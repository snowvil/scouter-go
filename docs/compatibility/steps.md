# 전체 Step wire 계약

정본은 Scouter `2.21.3`, commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의
`scouter.common/src/main/java/scouter/lang/step/StepEnum.java`, `Step.java` 및
각 concrete `read/write`다. 아래 범위는 원본 `StepEnum.Type` 23종 전체이며,
실제 Agent/Client 접속이나 Profile 저장·합산·조회 의미까지 검증한 상태는 아니다.

## 공개 API와 순서

```go
step, err := protocol.DecodeStep(raw, protocol.Limits{})
raw, err = protocol.EncodeStep(step, protocol.Limits{})
steps, err := protocol.DecodeSteps(profile.Profile, protocol.Limits{})
raw, err = protocol.EncodeSteps(steps, protocol.Limits{})
```

`ProfilePack.Profile`은 계속 opaque owned bytes다. 소비자가 `DecodeSteps`를
명시적으로 호출한다. 모든 Step은 `byte ID + concrete body`이며 공통 length나
sequence count가 없다. `Step.toObjectList/toObjects`와 같이 EOF까지 읽는다.
`DecodeStep`은 정확히 한 record만 읽고 trailing byte를 거절한다.
`Decoder.Step/Encoder.Step`도 제공하며 기존 sticky error 규칙을 따른다.

sequence의 입력 순서를 그대로 유지한다. index 정렬, summary 재배치, 문자열
dict 해석, parameterized message formatting, unknown body skip은 수행하지 않는다.
Java `Step.toBytes(Step[])`는 `ThreadCallPossibleStep.isIgnoreIfNoThreaded`라는
nonwire flag에 따라 producer 단계에서 일부 Step을 제외할 수 있다(`Step.java:49-65`).
Go `EncodeSteps`는 raw wire 및 Java `List<Step>` overload(`:72-81`)의 동작을
따르며 기록된 Step을 묵시적으로 제외하지 않는다.

## ID와 body

표의 `D`는 Scouter Decimal, `T`는 UTF8 Text/Blob length 규칙이다.
`S`는 `D parent, D index, D start_time, D start_cpu`의 Single prefix다
(`StepSingle.java:58-69`). 표에서 int 필드는 int32, summary elapsed/cputime과
txid/timestamp는 int64이며 모두 명시된 Decimal 또는 고정 폭을 사용한다.

| ID | Java/Go 의미 | Single prefix 이후 또는 summary body |
|---|---|---|
| 1 | Method | S, D hash/elapsed/cputime |
| 10 | Method2 | Method, D error |
| 2 | SQL | S, D hash/elapsed/cputime, T param, D error |
| 8 | SQL2 | SQL, byte xtype |
| 16 | SQL3 | SQL2, D updated |
| 3 | Message | S, T message |
| 9 | HashedMessage | S, D hash/time/value |
| 17 | ParameterizedMessage | S, D hash/elapsed/level, T paramString |
| 5 | Socket | S, Blob ipaddr, D port/elapsed/error |
| 6 | ApiCall | S, D txid/hash/elapsed/cputime/error, byte opt, opt=1이면 T address |
| 15 | ApiCall2 | ApiCall, byte async |
| 13 | Dispatch | ApiCall와 동일 body |
| 7 | ThreadSubmit | S, D txid/hash/elapsed/cputime/error |
| 14 | ThreadCallPossible | S, D txid/hash/elapsed, byte threaded |
| 12 | Dump | S, signed int16 stack count, fixed int32 stack[], fixed int64 threadId, T threadName/threadState, fixed int64 lockOwnerId, T lockName/lockOwnerName |
| 11 | MethodSum | D hash/count, D int64 elapsed/cputime |
| 21 | SQLSum | MethodSum, D error, T param/param_error |
| 31 | MessageSum | T message, D count |
| 42 | SocketSum | Blob ipaddr, D port/count, D int64 elapsed, D error |
| 43 | ApiCallSum | MethodSum, D error, byte opt=0 |
| 99 | Control | T message, D code |
| 51 | Span | CommonSpan body |
| 52 | SpanCall | CommonSpan, D txid, byte opt, opt=1이면 T address, byte async |

CommonSpan body는 `CommonSpanStep.java:61-107`을 따른다:

1. S, D hash/elapsed/error, D int64 timestamp, byte spanType.
2. D localEndpointServiceName, Blob localEndpointIp, fixed signed int16 localEndpointPort.
3. D remoteEndpointServiceName, Blob remoteEndpointIp, fixed signed int16 remoteEndpointPort.
4. Boolean debug/shared, Value annotationTimestamps, Value annotationValues, Value tags.

마지막 세 Value는 원본 Java cast에 따라 **List/List/Map**이어야 한다.
writer가 null을 NullValue로 쓸 수 있더라도 원본 reader의 cast는 실패한다.
Go는 잘못된 타입과 기본 zero-value Span을 `ErrInvalid`로 거절한다.
빈 Span을 만들 때는 empty List/List/Map Value를 명시한다.

address는 `opt=1`에서만 body에 존재한다. `opt=0/255` 등 그 외 byte는 address
없이 원본 그대로 유지한다. Parameterized level은 **signed int8을 Decimal로**
표현하며 ETX(`0x03`)가 포함된 paramString을 formatting 없이 보존한다.
SQL3 updated의 음수 sentinel(`-1/-2/-3`)도 signed int32로 보존한다.
Summary count는 scalar 값이며 allocation count로 취급하지 않는다.

Java 객체의 `spanPack`, `nameDebug`, `nameTemp`, `ApiCallStep2.start`,
`isIgnoreIfNoThreaded`, Parameterized tempMap은 wire에 없으므로 추가하지 않았다.
Summary에는 Single prefix가 없다. Span endpoint port는 unsigned port의 bit
패턴을 signed int16로 운반하며 `33000` fixture가 `-32536`으로 decode된다.

## 할당·오류·소유권

기존 `Limits`를 재사용한다. frame/Blob/UTF8/Value depth 제한에 더해
`MaxCollectionEntries`는 전체 sequence Step 수와 Dump stack 수를 각각 제한한다.
`MaxTotalValues`는 Step 1개, Dump stack item 1개, Span 내부 Value/array item을
동일 aggregate budget에서 함께 센다. Dump 배열은 signed int16 count,
남은 fixed int32 payload, count/aggregate 상한을 검사한 후에만 할당한다.

length/count 음수·초과, 잘린 body, int32/int8 overflow, typed nil pointer,
unknown ID/구현, 잘못된 Span Value 타입은 명시적 오류다. 상위 API는 오류 시
부분 Step/sequence/encoded bytes를 성공으로 반환하지 않는다. byte slice는
owned copy이며 decode 완료 후 원본 input buffer를 재사용할 수 있다.

원본의 관대한 malformed reader와 다른 의도된 경계:

- API_SUM의 Java reader는 nonzero opt를 읽지만 writer는 opt=0만 허용하고
  나머지를 IOException으로 거절한다(`ApiCallSum.java:62-86`). Go는 decode에도
  이 제한을 적용해 재인코딩 불가능한 기록을 거절한다.
- Parameterized level 및 Single/int 필드의 Decimal overflow는 Java cast의
  wrapping을 재현하지 않고 거절한다.
- 기존 primitive codec과 동일하게 malformed UTF8/unknown Decimal width를 거절한다.

Step 자체는 recursive하지 않다. Span Value 내부 nesting만 기존 depth 제한을
사용한다. 무제한 Step/배열 할당과 unknown Step의 silent skip은 제공하지 않는다.

## 실제 Java oracle

`tools/java-fixtures/StepOracle.java`는 원본 common 및 StepEnum을 실제 javac로
compile한 뒤 `DataOutputX.writeStep`, `DataInputX.readStep`, `Step.toObjectList`를
실행한다. 결과는 synthetic `step-*.bin` 57개다:

- 23개 ID의 nonzero/zero fixture 46개.
- ApiCall/ApiCall2/Dispatch/SpanCall의 opt0/255 fixture 8개.
- 정방향/역방향 EOF sequence, empty sequence 3개.

Java reader의 field 의미와 body 소비를 검사하며 Span MapValue는 Hashtable의
entry 순서와 구분해 key별 의미를 비교한다. Go 테스트는 모든 ID의 기대 field,
Java bytes와 exact reencoding, 모든 단일 record의 잘린 prefix, 옵션 분기,
EOF 순서, 제한, typed nil 및 pool input 재사용을 검증한다.
fixture 생성·SHA guard·manifest checksum은 공용 generator가 담당한다.

```sh
make fixtures-check
GOMAXPROCS=2 .tools/go/bin/go test ./internal/protocol -run 'Step|SpanStep'
GOMAXPROCS=2 .tools/go/bin/go test -race ./internal/protocol
GOMAXPROCS=2 .tools/go/bin/go test ./internal/protocol -run '^$' -fuzz '^FuzzDecodeSteps$' -fuzztime=10s -parallel=2
```

짧은 fuzz 통과는 입력 공간 전체나 운영 안전성을 증명하지 않는다. 실제 Agent
profile replay, Client의 profile decoding/표시, 장시간 malformed/fuzz, 공유 서버
자원 예산 및 저장 이관에서의 Step 의미 검증은 후속 단계다.

# internal/pulse 구현 계약

정본은 [docs/compatibility/pulse-contract.md](../../docs/compatibility/pulse-contract.md)와 Pulse plugin 원본(비공개, [baseline.json](../../docs/compatibility/baseline.json)의 `pulse_plugin.source_digest`로 고정)이다. 이 문서는 Go 구현의 범위, Java와 의도적으로 다른 동작, 아직 검증하지 않은 항목만 기록한다.

## 구현 범위

| 파일 | 내용 | Java 근거 |
|---|---|---|
| `javahash.go` | `JavaStringHashCode`(UTF-16 code unit, int32 overflow), `FloorMod`, `Partition`, `TransactionHash`(HashUtil CRC32, UTF-8 bytes) | plugin dispatcher, `scouter.util.HashUtil` |
| `application.go` | `javaSplit`(Java `String.split` trailing-empty 제거), `ExtractApplication` | plugin parser |
| `config.go` | Java 키 이름 그대로의 `Config`, `ParseJavaConfig`, `DefaultConfig`, 필터 함수(`IsExcludeTransaction`, `IsExcludeApplication`, `IsSpringBasedApplication`, `IsDropHTTPStatus`, `SpringURIFiltered`, `RebaseTransaction`, `RemoveAfterConditions`) | plugin 설정·Trace 모델 |
| `json.go` | Jackson 2.15.2 출력 재현 writer(escape 규칙, Base64, null) | golden 관찰 |
| `trace.go` | `Trace` 모델, `ConvertXLog`, Rebase/RemoveAfter, `MarshalJSON`(key 순서 고정) | plugin parser·Trace 모델 |
| `span.go` | `Span`/`ProfileStepData`, `ConvertProfileSteps`(mainValue/additionalValueList/step), `EncodeSingletonStep`, `ConvertProfile`, `MarshalJSON` | plugin parser·profile 모델 |
| `javaformat.go` | `ParameterizedMessageStep.buildMessasge`용 `String.format` 부분 구현, blank 판정, `StringUtil.split(char)` | ParameterizedMessageStep.java:115-132 |
| `dispatcher.go` | xlog/profile 별도 bounded queue + worker pool, drop-new offer, filtered txid map(10분 TTL), 단계별 통계, `Sink` interface, `Record`/`Header`, `BuildRecord`, bounded `Close` | plugin callback·dispatcher |

ingest 연결: `ingest.Config.OnXLog`/`OnProfile`가 `storeXLog`의 `PutXLog` 직전, `putProfile`(즉시·Version1·지연 child release·recovery 4경로 공통)의 `PutProfile` 직전에 호출된다(Java `XLogCore`/`ProfileCore`의 `PlugInManager.xlog/profile` → `XLogWR/XLogProfileWR.add` 순서). hook은 engine lock 안에서 실행되므로 dispatcher `Offer*`만 호출해야 한다.

Kafka producer(`internal/pulse/kafka`)와 server flag/stat 연결은 이 패키지 밖이다. `Sink`는 사용하는 쪽 interface이며 테스트는 메모리 sink를 쓴다.

## Kafka producer 압축과 protocol 버전 (`internal/pulse/kafka`)

| 항목 | Java plugin (kafka-clients 4.0.0) | Go |
|---|---|---|
| `compression.type` | `"lz4"` 하드코딩(scouter.conf 키 없음) | `kafka.Config.Compression` 기본 `lz4`, `none` 선택 가능. `gzip`/`snappy`/`zstd`는 Java에서는 유효 값이지만 Go는 `ErrInvalidConfig`로 거절(미구현을 조용히 비압축으로 바꾸지 않음). 이름은 Java처럼 소문자 정확 일치 |
| LZ4 frame | `Lz4BlockOutputStream` 기본값: magic `04 22 4D 18`, FLG `0x60`(v01, block independence), BD `0x40`(64KiB), HC=(XXH32(FLG BD)>>8)&0xFF, block checksum·content size·content checksum 없음, 줄지 않는 block은 bit31 raw 저장, end mark 0 | 같은 frame 형식(`lz4.go`). 압축 bytes 자체는 lz4-java와 다를 수 있다(다른 match finder, LZ4 형식상 허용) |
| 비압축 fallback | 항상 lz4 frame(필요 시 raw block) | batch 전체의 LZ4 frame이 평문 records보다 작지 않으면 그 batch만 codec 0(none)으로 전송. 전송 크기가 `BufferBytes`/`MaxRequestBytes`가 계산하는 비압축 크기를 넘지 않게 하기 위한 의도된 차이이며 소비자가 보는 record는 같다 |
| 압축 위치 | `RecordAccumulator` append 시 | broker worker가 request를 만들 때(수집 경로·`Enqueue`는 압축하지 않음). 연결별 16KiB hash table은 첫 압축 batch에서 할당, scratch는 최대 batch 크기로 상한 |
| ApiVersions | 최신 v4, 미지원 시 broker 응답의 v0 형식 오류를 보고 낮춰 재시도 | v3(header v2, `client_software_name=scouter-go-kafka`, `client_software_version=dev`), `UNSUPPORTED_VERSION`이면 같은 연결에서 v0 재시도 |
| Metadata / Produce | Metadata 0..13, Produce 3..12 | Metadata 1..13, Produce 3..12 중 broker와의 최고 공통 버전. 9 이상은 flexible(header v2/v1, compact 문자열·배열·records, tagged field) |
| tagged field | 알려진 tag는 해석(Produce v10+ `CurrentLeader`/`NodeEndpoints`로 leader 갱신) | 알려진 tag 포함 모두 skip. count는 `Limits.MaxTaggedFields`(기본 64)와 남은 bytes, 크기는 남은 bytes로 할당 전에 검증. leader 변경은 오류 시 metadata 재조회로 처리 |
| Metadata v13 top-level `error_code` | rebootstrap 등 처리 | 0이 아니면 해당 fetch 실패로 보고 다음 후보 broker 사용 |
| Produce v8+ `record_errors`/`error_message` | 예외 메시지에 포함 | `error_message`를 오류 이벤트에 덧붙이고 `record_errors`는 버림 |

검증(2026-10-15): fake broker 버전 행렬(Metadata 1..13 × Produce 3/5/7/8/9/10/12, unknown tagged field 주입, 구형 broker의 ApiVersions v3 거절→v0 재시도), 경계·fuzz(`FuzzLZ4RoundTrip`, `FuzzMetadataResponse` v1..13, `FuzzProduceResponse` v3..12, `FuzzApiVersionsResponse` v0/v3, `FuzzSkipTags`), Java oracle(`TestJavaLZ4Oracle`: kafka-clients 4.0.0 `MemoryRecords`+`ensureValid`가 Go lz4/none batch 5종과 LZ4 frame 11종을 동일하게 해제, 역방향으로 Java frame·lz4 batch를 Go 테스트 decoder가 해제, lz4-java XXH32 벡터 일치, 빈 입력·5 byte 입력 frame은 byte 단위 동일), 실제 Kafka 4.0.0 KRaft broker(협상 ApiVersions v3/Metadata v13/Produce v12, non-flexible 강제 시 v0/v8/v8; 7개 시나리오 각 96 record 소비 일치, broker segment의 batch codec lz4 35·none 1).

## Java golden 대조 결과

`TestJavaGoldenRecords`는 `testdata/pulse-golden.json`(124 case, 고정 plugin 원본, Scouter 2.20.0, Jackson 2.15.2, Asia/Seoul)을 config profile별로 `sequence` 순서대로 하나의 Dispatcher에 재생한다(Java의 static filtered-txid map·TextCache 공유를 재현). 결과 **124/124 PASS**: sent 98건은 topic/partition/key/header 순서·bytes/JSON bytes/profile step bytes·mainValue·additionalValueList까지 일치, filtered 20건은 같은 단계, exception 6건은 Go에서 미전송(추출 실패 filtered 또는 worker error)으로 일치. 파일 부재 시 `t.Skip("golden not generated")`. golden은 비공개 plugin 원본에서 생성한 고정 자료이고 생성 도구는 이 저장소에 없다(재생성은 비공개 저장소에서만 가능). 위 세 application 이름 필터는 Go 기본값이 비어 있으므로(아래 "의도된 차이") golden 생성 시 모든 profile의 `scouter.conf`에 중립 이름(`excluded-app`, `gateway-app|/|3`, `gateway-app,plain-app-a,plain-app-b,plain-app-c`)을 명시하고, Go test는 metadata에 기록된 같은 conf를 `ParseJavaConfig`에 넣는다.

golden으로 확정된 사실: Trace JSON key 순서는 `traceId … ignoreGlobalConsequentSampling, objectDictContent, transactionDictContent, threadDictContent, errorDictContent, xtype`(getter에서 정해지는 이름, 마지막). Jackson은 보조평면 문자를 `😀` surrogate pair escape로, 제어문자를 대문자 hex `\u001F`로 쓰고 `/ < >`와 BMP 비ASCII는 raw다. plugin의 rebase는 내용이 그대로여도 `transactionDictId`를 재계산하고 global suffix는 매치 시에만 재계산한다.

## 의도된 차이

| 항목 | Java | Go | 이유 |
|---|---|---|---|
| application 추출 실패 | 길이 2 split → `ArrayIndexOutOfBoundsException`, dictionary miss → `NullPointerException`으로 task 실패(로그만, filtered 미계수) | 모두 `application-extract` 단계 filtered로 계수 | 정상 계약과 결함 후보 분리; 미전송 결과는 동일 |
| dictionary 조회 오류 | `TextRD.getString`이 예외를 삼켜 null | lookup error를 task 오류로 `Errors` 계수·이벤트 보고 | silent error 금지. ctx 취소도 같은 경로 |
| profile step decode 실패/미지원 type | `RuntimeException` → worker 로그 | `Errors` 계수·이벤트 | 동일하게 미전송 |
| 통계 | 10초마다 로그 후 reset | 단조 증가 counter, 단계별 filtered 분리, `Errors`/`ShutdownDiscarded` 추가 | 운영 집계 요구 |
| `sent` 의미 | producer callback 성공(acks=0) | `Sink.Enqueue` 수락 | broker 영속 증거가 아님. producer 자체 stat은 kafka 패키지 담당 |
| filtered txid map 상한 | 무제한 `ConcurrentHashMap`, 10,000개 이상일 때만 만료 항목 제거 | 10,000 이상에서 만료 제거 + `MaxFilteredTraces`(기본 100,000) 초과 시 가장 오래된 항목 제거 | 메모리 상한 |
| 종료 | `shutdownNow`, drain 없음 | `Close(ctx)`가 offer를 거절하고 ctx 만료까지 drain, 잔여는 `ShutdownDiscarded` | 소유자·종료 경로 명확화 |
| pack 소유권 | 참조 공유 | `Offer*`에서 clone(상한 검사 뒤에만 복사, drop은 복사 없음) | 재사용 buffer race 방지, ingest hook 계약(pack 보유 금지), UDP adopt decode의 `Profile`이 datagram·재조립 buffer 전체를 붙잡는 것 방지 |
| queue 상한 | 개수(`ArrayBlockingQueue` 4096)만 | 개수 + queue별 byte 상한(XLog 16MiB, Profile 64MiB). 넘치면 drop-new, `Dropped`와 그 부분집합 `DroppedByBytes` 계수 | 큰 profile이 쌓일 때 메모리 상한(아래 "queue byte 상한") |
| drop 로그 | drop마다 plugin debug 로그(debug일 때만) + 10초 상태 로그 | queue별 10초에 1회 `dropped` Event(사유, 개수·byte 사용량, 누적 drop 수). 계수는 모든 drop. 간격은 단조 시간(`Clock().Sub(start)`, 기본 `time.Now`) 기준이고 시계가 한 간격 넘게 뒤로 가면 창을 초기화한다. `Options.TryEvent`가 false(전달 실패)를 돌려주면 창을 넘기지 않아 다음 drop에서 다시 보낸다(`OnEvent`만 쓰면 전달 성공으로 간주) | 지속 포화 시 로그 폭주 방지 |
| worker panic | worker loop가 task마다 `Exception`을 잡아 로그 후 다음 task | task 단위 `recover`: `Errors` 계수, queue별 10초 1회 `error` Event(`worker panic: …`), byte 반납, worker 유지 | 변환 결함 하나가 서버 전체를 종료하지 않게 함. Java는 `Error`는 잡지 않지만 Go panic은 구분하지 않고 모두 잡는다 |
| `plugin_pulse_kafka_topic_partition_count` ≤ 0 | task마다 `ArithmeticException` | `ParseJavaConfig` 오류 | 설정 시점 거절 |
| 애플리케이션 이름 필터 기본값 | `ext_plugin_pulse_exclude_applications`, `ext_plugin_pulse_rebase_transaction_applications`, `ext_plugin_pulse_non_spring_based_applications` 기본값이 특정 배포의 서비스 이름 | 키가 없으면 빈 값: 제외·rebase 대상 없음, 모든 application을 Spring 기반으로 취급(URI filter 적용) | 특정 배포의 이름이라 공개 저장소에 두지 않는다. 운영은 세 키를 `scouter.conf`(또는 `--pulse-conf` 파일; 이 경우 그 파일만 읽고 `scouter.conf`와 병합하지 않는다)에 명시한다. 키가 없으면 기동 시 키별 `WARNING: pulse: <key> is not set`(`internal/server/pulse.go` `pulseAppFilterWarnings`; non-Spring 키는 Spring URI filter가 켜졌을 때만), 의도적 비움은 `key=`로 명시해 경고를 없앤다. 키가 있을 때의 파싱 의미는 원본과 같다 |
| rebase 설정 key 중복 | `Collectors.toMap` 예외로 static init 실패(plugin 전체 불능) | `ParseJavaConfig` 오류 | 명시적 실패 |
| SPAN endpoint OBJECT 조회 | event 날짜로 조회해 plugin 내부 span 모델의 endpoint 서비스 이름만 설정(직렬화 안 됨) | 조회 생략 | 출력 영향 없음. storage 오류 경로만 달라짐 |
| `XLogProfilePack.elapsed` | wire에 없고 서버도 설정 안 함(항상 0); oracle은 메모리에서 17 설정 | `ConvertProfile(..., elapsed, ...)` 인자, worker는 0 | golden 대조용 통로 |
| ipaddr/message null | wire 기반 pack은 `new byte[0]`/`""` | Go `nil`→`null`, 빈 slice→`""` | Go wire decode는 빈 slice이므로 운영 출력은 Java와 같음 |

## queue byte 상한 (2026-10-23)

- 대상: dispatcher의 XLog/Profile queue 각각. task 하나의 byte는 pack 구조체 크기(`reflect` Size) + 보유하는 가변 길이 필드(XLog: `IPAddress`·`Extra` clone과 공유 string `CountryCode`·`Text1..5`, Profile: `Profile` clone)다. 접수 시 예약하고 worker가 처리(성공·filter·오류·sink 실패·panic·강제 종료 폐기 모두)를 마친 뒤 반납한다. 그래서 정의가 다르다: `Pending`은 channel에 대기 중인 task 수(처리 중 제외), `PendingBytes`는 대기 중 + 처리 중 task의 byte다. 세지 않는 것은 처리 중 task의 worker 변환 메모리(Step decode, Trace/Span 모델, JSON·Base64 buffer, Kafka record)다. 이것은 동시에 처리되는 task 수, 곧 queue별 `ext_plugin_pulse_kafka_dispatch_worker_count`(기본 2, Go 쪽 상한 없음)에 비례한다.
- `DroppedByBytes`는 offer 시점의 근사 분류다. 동시 offer나 worker 반납과 경쟁하면 개수·byte 중 먼저 검사에 걸린 쪽으로 센다. `Dropped` 합계는 정확하다.
- 순서: 개수 포화 검사 → byte 예약(CAS, 대기 없음) → clone → non-blocking send. 어느 단계든 실패하면 즉시 drop하므로 수집 경로(engine lock 안의 hook)는 기다리지 않고, drop된 offer는 복사·할당을 하지 않는다(`TestDispatcherDropDoesNotClone`).
- 기본값 근거: 한 profile의 최대 크기는 codec `MaxBlobBytes`와 storage `MaxRecordBytes`의 4MiB이므로 개수 상한만으로는 4096 × 4MiB = 16GiB까지 붙잡을 수 있었다. Profile 64MiB는 평균 16KiB 이하 profile 4096개를 모두 담아(정상 부하에서는 개수 상한이 먼저 걸림) 최대 크기 profile도 16개 이상 담는다. XLog task는 구조체 312B(amd64·arm64) + 짧은 필드라 4096개가 수 MiB이므로 16MiB는 비정상적으로 긴 `Text*`/`Extra`에서만 걸린다. 같은 프로세스의 Kafka producer `BufferBytes` 기본 64MiB(Java `buffer.memory` 512MiB 대신, 공유 호스트)와 합쳐 Pulse가 queue에 붙잡는 메모리는 기본 약 144MiB(+worker 변환 메모리)로 제한된다.
- producer 쪽: `kafka.Producer`는 이미 encode된 record bytes를 `BufferBytes`로 묶고(Enqueue 시 초과면 즉시 `ErrBufferFull`), `Stats.PendingBytes`로 사용량, `Stats.DroppedBufferFull`(`Dropped`의 부분집합)로 byte 사유 drop을 구분한다. 계산은 encode 크기 기준이라 batch slice의 여유 cap과 압축 scratch는 포함하지 않는다. dispatcher의 `Failed`는 이 거절을 sink 실패로 센다.
- Kafka 장애만으로는 dispatcher queue가 쌓이지 않는다: worker의 `Sink.Enqueue`는 broker를 기다리지 않고 buffer가 차면 즉시 실패한다. dispatcher queue가 쌓이는 경우는 worker가 느린 경우(dictionary 조회 지연 등)다.
- 노출: `QueueStats.PendingBytes`, `MaxPendingBytes`, `DroppedByBytes`는 `/health`의 `pulse.dispatcher`(Stats를 그대로 JSON으로 직렬화)와 종료 로그 `pulse=…`에 나온다. producer `Stats`는 `/health`의 `pulse.kafka`와 종료 로그 `kafka=…`에 나온다.
- 상한은 `Options.MaxXLogQueueBytes`/`MaxProfileQueueBytes`(0이면 기본값)다. Java 키 집합(`JavaKeys`)은 바꾸지 않았고, 서버는 Go 전용 재기동 키 `go_pulse_xlog_queue_mb`/`go_pulse_profile_queue_mb`(`internal/config/golimits.go`)로 이 값을 정한다.

## profile text 날짜 규칙(재현한 Java 동작)

main hash(METHOD/SQL/APICALL/THREAD_SUBMIT/DISPATCH/THREAD_CALL_POSSIBLE/HASHED·PARAMETERIZED_MESSAGE/SPAN/SPANCALL)와 DUMP stack hash는 plugin의 text preload가 **현재 날짜**로 미리 읽는다. miss면 그 hash가 null 값으로 캐시되어 이후 event 날짜 조회가 실행되지 않으므로 `**unlabeled**:<type>:<hash>`가 확정된다. Go도 현재 날짜 조회만 수행한다(golden `crossday-profile-apicall-filedb-event-date`/`-today`로 확인). trace/profile의 object/service/hmsg/error 조회는 event 날짜다. Java의 프로세스 전역 text 캐시가 이전 task의 hit을 재사용해 다른 날짜의 값을 돌려주는 상태 의존 동작은 재현하지 않는다(아래 미검증).

## 가정

- JVM default charset은 UTF-8(oracle metadata `default_charset=UTF-8`). `HashUtil.hash(String)`은 UTF-8 bytes CRC32.
- `toLowerCase()`는 non-Turkish 기본 locale. Go `strings.ToLower`와 `String.toLowerCase()`는 `İ` 등 특수 문자에서 다를 수 있다.
- Scouter `Configure.getValue`는 키 부재 시 null, 존재하면 그대로의 문자열. Go는 map 존재 여부로 구분하며 빈 문자열은 Java `"".split(",")`처럼 처리한다.
- `String.format`은 `%s %S %b %B %h %H %n %%`와 인덱스/`<`/`-`/width/precision만 성공으로 재현하고 나머지는 Java 예외와 같이 원문 반환. width/precision은 rune 단위(Java는 UTF-16 code unit).
- Step decode 상한은 `StepLimits`(entries 65536, values 1M)다. 저장은 Step을 해석하지 않고 profile bytes를 보존하되 `storage.Config.MaxRecordBytes`(기본 4MiB)를 넘으면 `ErrLimit`으로 거절한다(`internal/storage/profile.go`, `packwriter.go`). transport/codec의 길이 상한도 별도로 적용된다.

## 미검증

- Pulse 수집 서비스 수신, UUID 경계, broker 장애·내구성 옵션(`acks` 등): 통합 gate에서 확인한다. Kafka producer는 격리 broker(4.0.0)에서만 검증했다.
- Kafka 2.4 미만(ApiVersions v3 미지원) 실제 broker에서의 v0 재시도: fake broker와 4.0.0 broker의 미지원 버전 응답 형식(v0 layout + `UNSUPPORTED_VERSION`)으로만 확인했다.
- Kafka 4.1+ broker(Produce v13 이상 topic id 기반 등): 최고 공통 버전 협상으로 v12까지만 사용하며 실제 broker 검증은 없다.
- LZ4 압축의 CPU 비용·압축률과 Pulse on/off 처리량 비교: 미측정.
- 10분 TTL 만료는 fake clock 단위 테스트만 있고 Java oracle에는 없음(oracle `not_generated`).
- queue 포화(개수·byte)/worker exception/shutdown 잔여는 Go 단위 테스트만 있고 Java 대조 없음. byte 추정과 실제 heap 사용량의 차이는 미측정.
- Java 전역 text 캐시의 cross-task hit 재사용(이전 task에서 찾은 값이 날짜와 무관하게 재사용되는 경우).
- `%s` 이외 Formatter 동작, Turkish locale, 비정상 UTF-8 입력(Go protocol은 decode 시 거부).
- 운영 부하(TPS)와 공유 호스트 예산.

## 후속 보완 (2026-10-14)

- Offer와 Close의 동시 호출은 `offerMu`(RW)로 직렬화해 닫힌 channel send를 막는다.
- 강제 종료(`Close` deadline 초과) 중 `context.Canceled`로 끝난 task는 `Failed`/`Errors`가 아니라 `ShutdownDiscarded`로 센다.
- 필터 단계 Event는 `ext_plugin_pulse_debug=true`일 때만 만든다(Java plugin의 filter 로그와 동일 gate). sink/error/dropped/shutdown Event는 항상.
- 서버의 dictionary 조회는 Java `TextCache`(날짜 무관 최근 수신 LRU)를 흉내 내어 event 날짜 miss 시 수신 날짜(오늘)로 한 번 더 조회한다(`internal/server/pulse.go`). perm fallback은 storage가 type별로 영구/일별을 분리 저장하므로 Java와 결과가 같다. invalid UTF-8 text는 storage가 저장 단계에서 거부하므로 SERVICE miss → extension 단계 filtered가 된다(Java는 U+FFFD 치환 전송; Java agent는 유효 UTF-8만 보낸다).
- 지연(child) profile은 Java `ProfileCore.add`처럼 release 시각을 `time`으로 쓴다(ingest `releaseProfiles`/`recoverPack`).
- Pulse 설정 오류는 Java plugin처럼 격리된다: 서버는 기동하고 stream만 꺼지며 사유는 시작 로그와 `/health`의 `pulseError`에 남는다.
- `strings.TrimSpace`는 Unicode 공백을, Java `trim()`은 `<= U+0020`만 제거한다. 설정값에 NBSP 등 비ASCII 공백이 있는 경우만 다르다(미검증).
- `javaFormat`의 `%0$s`는 Java 17+에서 예외→원문 반환이지만 Go는 일반 인자로 처리한다(영향 미미, 미검증).

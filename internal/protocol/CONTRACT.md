# Scouter wire codec 계약

기준 원본: Scouter `2.21.3`, commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`.
이 패키지는 순수 byte codec이다. UDP/TCP framing, Agent/Client 접속, 명령별 조회,
인증, 저장/이관, profile 집계·표시, 서버 처리량은 이 패키지의 검증 범위에 포함되지 않는다.

## 구현 범위

- big-endian Boolean/byte/int16/int32/int64, signed int3/long5, IEEE754 Float/Double.
- Scouter Decimal: 폭 `0/1/2/3/4/5/8`, signed 2's complement. protobuf varint가 아니다.
- Blob/Text: `0..253`은 byte 길이, `0xff + uint16`, `0xfe + int32`; Text는 UTF-8.
- null Blob/Text와 empty는 wire에서 같으므로 empty로 복원한다.
- Pack 16종: Map(10), XLog(21), DroppedXLog(22), Profile V1(26)/V2(27),
  Span(31), SpanContainer(32), Text(50), PerfCounter(60), Status(61), Stack(62),
  Summary(63), Batch(64), InteractionPerfCounter(65), Alert(70), Object(80).
  추가 9종의 필드·Batch projection 경계는 [Pack 계약](../../docs/compatibility/packs.md)을 따른다.
- Value 17종: Null, Boolean, Decimal, Float, Double, Double/LongSummary, Text,
  TextHash, Blob, IP4, List, Int/Float/Text/LongArray, Map.
- StepEnum 23종과 EOF sequence: [Step 계약](../../docs/compatibility/steps.md).
  `DecodeSteps/EncodeSteps`는 순서를 보존하며 count·할당·중첩 상한을 적용한다.
- Profile bytes는 owned opaque payload다. Java에서 보내지 않는 `elapsed`나 internal
  processing flags는 새 wire 필드로 추가하지 않는다.
- XLog의 구형 선택 필드는 12개 append group 경계에 따라 읽는다.
  `TailVersion=0`은 원래 필수 필드만, `XLogCurrentTail=12`는 현재 전체 필드를 쓴다.
  decode된 `TailVersion`을 그대로 encode해 구형 fixture의 byte를 보존한다.
  최신 known group 이후의 unknown `Extra`는 owned bytes로 보존한다.
  원본에서 missing tail의 `countryCode`/`text1..5`는 null이며 Go string은 empty다.
  downstream JSON/조회 mapping은 `TailVersion`으로 필드 presence를 판별해 null과
  실제 전송된 empty string을 구분해야 한다.

정본 경로는 원본 `scouter.common/src/main/java/scouter/io/DataInputX.java`,
`DataOutputX.java`, `scouter/lang/pack/{MapPack,ObjectPack,PerfCounterPack,TextPack,
XLogPack,XLogProfilePack,XLogProfilePack2}.java`, `scouter/lang/value/*.java`다.

## Map 순서와 소유권

`MapPack`의 Java table은 `LinkedHashMap`이다. `MapValue`의 Java table은
`Hashtable`이므로 insertion order를 보장하지 않으며 Java decode→encode도 동일
key/value를 다른 순서로 보낼 수 있다. Go는 모든 map을 `[]Entry`로 표현해 수신한
wire 순서를 보존한다. 중복 key를 decode하면 기존 위치의 value를 갱신한다.
동일 key가 반복된 `[]Entry`의 encode는 모호한 의미를 피하기 위해 거절한다.

`Decoder`는 동작 중에만 입력 slice를 빌린다. 호출자는 decode가 끝날 때까지
입력을 수정하지 않아야 한다. 반환 Pack/Value/Blob/Extra의 byte slice와 string은
복사해 소유하므로 입력을 pool로 반납한 뒤에도 사용할 수 있다. zero-copy Blob
참조와 입력 buffer pool은 적용하지 않는다.

할당 감소를 위한 내부 기법과 그 소유권 규칙은 다음과 같다. 모두 출력 byte와
오류 종류를 바꾸지 않는다.

- `Encoder.Bytes`는 내부 buffer를 복사 없이 caller에게 넘긴다. Encoder는 넘긴
  slice에 다시 쓰지 않으며, `Bytes` 이후의 추가 write는 사본에서 이어진다(이후
  `Bytes`는 전체 출력을 반환). 반환 slice의 cap은 len으로 제한된다.
- XLog/Batch body는 중첩 Encoder 없이 같은 buffer에 쓰고 길이 prefix를 나중에
  채운다(`beginBlob/endBlob`). decode도 같은 Decoder를 body로 재범위화한다
  (`enterBody/leaveBody`). `MaxBytes`/`MaxBlobBytes` 검사는 그대로 적용된다.
- 32개 이하 Map entry는 중복 key 검사를 map 할당 대신 선형 탐색으로 하고, 메모리
  입력(stream 모드 제외)에서는 한 map의 key들을 하나의 소유 string의 substring으로
  만든다. 따라서 한 map의 key 하나를 오래 보관하면 그 map의 key bytes 전체(해당
  map의 key 합계로 상한)가 함께 유지된다.
- Step record는 record마다 할당한다. chunk(slab) 할당은 allocs/op만 줄이고
  ns/op 이득이 없으며 backing array 공유 규칙을 추가하므로 적용하지 않았다.
- `DecodePack/EncodePack/DecodeValue/EncodeValue/DecodeSteps/EncodeSteps`는
  codec 상태를 stack에 두며, Encoder는 256 byte(+Step당 24 byte) 용량 hint로
  시작한다. hint는 `MaxBytes`로 clamp되는 용량이며 검증 상한이 아니다.

Decoder가 빌린 slice(`textView`, `blobView`)는 다음 read 전까지만 유효하다.
비동기 소비자나 queue에는 소유 복사본(`Blob`, `Text`, Pack 필드)만 넘긴다.

## 제한과 오류

`Limits{}` 기본값: frame 8 MiB, Blob 4 MiB, collection 4096 items,
aggregate Value/Step/array item count 16384, Value depth 32. 호출자는 실제 수집 계약과
공유 호스트 자원 예산을 검증한 뒤 명시적으로 바꿀 수 있다. 이 숫자를 RSS 또는
서버 운영 자원 보장으로 해석하지 않는다.

할당 전에 length/count, 남은 input byte, aggregate count, nesting depth를 검사한다.
음수 length/count, int32 필드에 들어온 int64 overflow, int3/long5 encode overflow,
잘린 body 및 partial XLog append group을 오류로 반환한다.
오류 종류는 `ErrTruncated`, `ErrLimit`, `ErrInvalid`, `ErrUnsupported`, `ErrTrailing`이다.
primitive `Decoder`/`Encoder`는 첫 오류를 유지하며 `Err` 확인이 필수다.
`DecodePack/DecodeValue/EncodePack/EncodeValue`는 오류를 확인해 partial 성공을 막는다.

`ReadPack(io.Reader, Limits)`와 `ReadValue`는 공통 길이 헤더가 없는 TCP stream에서
정확히 한 객체만 소비한다. read-ahead 없이 같은 codec을 사용하며 `MaxBytes`는
객체마다 적용한다. buffer/deadline/connection 종료는 transport가 소유한다.
수신 실패 시 `ErrTruncated`와 원래 I/O 오류를 함께 보존한다. 오류 후에는 다음
객체의 경계를 알 수 없으므로 연결을 닫아야 한다. 이 API는 EOF까지 모아서
decode하지 않으며, 반환 Blob/Value는 입력 버퍼와 분리되어 있다.

원본의 malformed 입력 관용과 의도적으로 다른 경계:

- 원본 `readDecimal`은 모르는 폭도 int64로 읽지만, Go는 실제 Java writer가 내보내는
  폭만 허용한다.
- 원본은 UTF-8 오류를 replacement text로 바꾸지만, Go는 잘못된 UTF-8을 거절한다.
- 원본은 `Decimal`을 Java int로 cast할 수 있지만 Go는 범위 초과를 거절한다.
- 모든 unknown Pack/Value 및 JVM extension Pack은 `ErrUnsupported`다.
  Pack 공통 length가 없어 unknown body를 안전하게 건너뛸 수 없다.

## Java oracle와 검증

`scripts/generate-fixtures.sh`는 `docs/compatibility/baseline.json`의 source SHA를
확인하고 modified `scouter.common`을 거절한다. 기본 원본은 `../scouter`,
`SCOUTER_SOURCE`로 경로를 바꿀 수 있다. 원본 common의 전체 Java source와
`tools/java-fixtures/*Oracle.java`를 temporary directory에 `javac --release 11`
로 compile하며 원본 repository에 산출물을 쓰지 않는다.

fixture는 모두 synthetic이다. 실제 계정/SQL/profile을 읽지 않는다. 초기 54개는
Decimal 25개 경계, Blob 6개 경계, primitive/UTF8, Value 전체, Pack 7종, XLog
append version 0..12를 포함한다. 추가 Pack fixture 12개와 Step fixture 57개로
전체 기본 type을 대조한다. manifest는 source SHA/version, 파일 size/SHA256을 기록한다.
`--check`는 재생성한 전체 결과와 저장 fixture를 비교하며 repository를 수정하지 않는다.

Java source의 실제 decode와 semantic 검사, Go의 기대 필드 검사 및
Java 생성 byte와 Go 재인코딩 byte 일치를 수행한다. MapValue는 Java oracle에서
key별 semantic 비교를 사용한다. 초기 Profile fixture의 blob은 임의 synthetic
byte로 그대로 유지하며, 유효한 Step 검증은 별도 Step fixture를 사용한다.

```sh
scripts/generate-fixtures.sh --check
GOMAXPROCS=2 .tools/go/bin/go test ./internal/protocol
GOMAXPROCS=2 .tools/go/bin/go test -race ./internal/protocol
GOMAXPROCS=2 .tools/go/bin/go test ./internal/protocol -run '^$' -fuzz '^FuzzDecodePack$' -fuzztime=10s -parallel=2
GOMAXPROCS=2 .tools/go/bin/go test ./internal/protocol -run '^$' -fuzz '^FuzzDecodeValue$' -fuzztime=10s -parallel=2
GOMAXPROCS=2 .tools/go/bin/go test ./internal/protocol -run '^$' -bench . -benchmem
```

microbenchmark는 codec 비용만 측정한다. 결과를 서버 TPS로 환산하지 않는다.

## 출처와 라이선스

Scouter 원본 계약과 Java oracle에 사용하는 원본 source의 고지:
Copyright 2015 the original author or authors, https://github.com/scouter-project/scouter.
Licensed under the Apache License, Version 2.0: https://www.apache.org/licenses/LICENSE-2.0.
Go codec은 위 byte 계약을 기준으로 독립 구현했으며 참고 Go 구현을 복사하지 않았다.
Java 원본 source는 수정·재배포하지 않고 사용자가 제공한 checkout에서 임시 compile한다.

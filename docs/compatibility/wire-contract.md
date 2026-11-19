# Wire 계약 기준

정본은 `baseline.json`의 Scouter commit이며 아래 경로는 Scouter source root 기준이다. 이 문서는 transport 구현 완료를 의미하지 않는다.

## Primitive

`scouter.common/src/main/java/scouter/io/DataInputX.java`, `DataOutputX.java`:

- 고정 정수는 big-endian, signed two's complement. Decimal은 폭 byte `0/1/2/3/4/5/8` 뒤 signed payload이며 protobuf varint가 아니다.
- Blob 길이 0–253은 한 byte, 254–65535는 `ff + unsigned short`, 그 이상은 `fe + int32`이다. null과 empty는 `00`으로 합쳐진다.
- Text는 Blob 형태의 UTF-8이며 Java modified UTF가 아니다.
- Pack/Value/Step는 각각 type byte + type별 body다. 공통 길이가 없으므로 알 수 없는 type을 임의로 skip할 수 없다.
- MapPack은 LinkedHashMap이지만 MapValue의 내부 table은 Hashtable이다(`MapValue.java:45`). Go는 wire entry 순서를 보존한다. Java MapValue decode→encode의 순서가 달라질 수 있으므로 Java 의미 비교는 key별로, Go의 원본 bytes 재인코딩은 수신 순서로 검증한다.
- 원본은 Decimal의 0–5 이외 marker를 8-byte로 읽는다. Go의 malformed-marker 정책은 표준 encoder가 내보내는 8과 비정상 marker를 구분해 명시한다.

## UDP

정본: `scouter.common/src/main/java/scouter/net/NetCafe.java:24`, `scouter.server/src/main/scala/scouter/server/netio/data/NetDataProcessor.scala:71`.

| 종류 | magic | body |
| --- | --- | --- |
| single | CAFE `43414645`, JAVA `4a415641` | Pack |
| batch | CAFN `4341464e`, JAVN `4a41564e` | int16 count + 연속 Pack |
| fragment | CAFM `4341464d`, JMTU `4a4d5455` | int32 objHash + int64 pkid + int16 total + int16 index + Blob |

fragment index는 0-based이며 재조립 후 하나의 Pack이다. 원본은 pkid-only key, 1000개 cache, 생성 후 1000ms timeout을 사용한다. Go에서 sender isolation/bytes limit를 보강할 때 wire와 잘못된 입력 처리의 차이를 기록한다. default port는 UDP/TCP 6100이지만 테스트는 격리 포트를 사용한다.

## TCP

정본: `NetCafe.java:39`, `TcpFlag.java:21`, `scouter.server/src/main/scala/scouter/server/netio/service/net/TcpServiceWorker.scala:72`.

| 종류 | handshake | 요청/응답 |
| --- | --- | --- |
| Client | `cafe2001` | Text command + int64 session + command별 body |
| Agent V1 | `cafe1001` + int32 objHash | Text command + Pack; `(03 + Pack)* + 04` |
| Agent V2 | `cafe1002` + int32 objHash | int32 length + (Text command + Pack); `(03 + int32 length + Pack)* + 04` |
| Agent request | `cafe1011` + int32 objHash | int32 command + command별 stream |

Client response는 `(03 + handler별 Pack/Value/Blob)* + 04`다. `44`는 invalid session, `01`은 OK. Client/V1에 V2의 length envelope를 추가하지 않는다. `CLOSE`는 command 뒤 session을 읽지 않는다.

`SERVER_TIME`은 request body가 없고 `SERVER_VERSION`은 MapPack을 읽는다. `COUNTER_REAL_TIME`은 Value를, `TRANX_PROFILE_FULL`은 Blob을 반환한다. 명령 상수나 handler 개수만으로 request/response 호환을 판정하지 않는다.

## 인증과 타입별 주의점

- LOGIN/SERVER_VERSION/SERVER_TIME만 session-free이다. invalid session은 flag 후 연결을 닫는다.
- `__local` 로그인은 socket peer의 loopback 여부를 검사한다. request body의 IP를 신뢰하지 않는다.
- secure login password의 고정 salt + SHA-256 형식은 원본 `CipherUtil` 기준이다. plain SHA-256으로 대체하지 않는다.
- XLog는 Blob 안의 순차 필드이며 뒤에 추가된 field group을 구형 packet에서는 생략한다.
- Profile은 Step blob을 포함한다. 초기 codec에서 opaque bytes로 보존하더라도 Step 해석이나 Pulse JSON 호환이 완료된 것은 아니다.
- `PerfCounterPack.time`은 fixed int64, `TextPack.hash`는 fixed int32다. 전부 Decimal로 바꾸면 깨진다.

## 앞으로 필요한 transport evidence

Java sender/Client의 실제 bytes, TCP short read·연속 명령·partial write, fragment out-of-order·중복·만료·타 sender 충돌, authentication/permission, UDP drop와 종료 drain을 검증해야 한다. 현재 codec의 지원 목록과 test 결과는 README와 validation report에서 확인한다.

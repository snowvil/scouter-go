# 2C 수집·저장·조회와 실행 서버 검증

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

2026-10-13 로컬 검증. branch=`codex/bootstrap-server-port`, Go1.27.2/darwin-arm64/Apple M1 Pro, `GOMAXPROCS=2`. Scouter2.21.3 SHA=`dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5` 원본을 변경하지 않았다. Java 통신 oracle는 JDK18.0.2 `--release11`, DB oracle는 원본 Scala2.10.4 compiler를 설치된 JDK8로 실행하고 javac `--release8`을 사용했다. 모든 입력과 DB는 synthetic/임시이며 운영 서버에 접속하지 않았다.

## 구현과 commit

| 단위 | commit | 완료 범위 |
|---|---|---|
| 파일 index | `2c42ce2` | 원본 hash/time index, signed Long5, Java 양방향·파일 bytes 검증 |
| 기본 조회 | `febeeed` | 로그인/session, Object/Text/counter/XLog/profile 및 명령별 body/cursor |
| 파일 DB | `a5a53c2` | Object/Text/realtime·daily counter/XLog/profile, plain/GZipStore, 실제 Java reader/writer 양방향 |
| 수집 | `9ff9263` | Object heartbeat, counter 최근값·5분 표본, bounded XLog ring, Profile receiver 날짜 |
| 실행·연계 | `91752d1` | executable, 종료·lock·Agent pool, 실제 Java 통신 클래스 수집/조회/재시작 |

runtime은 기본 명령 27개와 OBJECT_ENV relay로 총 28개 dispatch를 제공한다. 전부의 운영 호환성을 주장하지 않는다. 지원 목록과 parameter·응답 계약은 [query-contract.md](../compatibility/query-contract.md), 실행은 [runtime.md](../compatibility/runtime.md), 저장은 [storage-contract.md](../compatibility/storage-contract.md)를 따른다.

## 실제 실행 결과

| 명령·검사 | 결과·근거 |
|---|---|
| `make check` | PASS: gofmt/vet/전체 test/build, `dist/scouter-go` 생성 |
| `make race` | PASS: 전체 package. storage19.871s/index9.189s; 실제 DB oracle도 실행 |
| `make fixtures-check` | PASS: 원본 Java synthetic codec/transport fixture141개 재생성 일치 |
| `make inventory-check` | PASS: 고정 원본 inventory와 fingerprint 일치 |
| `make fuzz FUZZTIME=5s` | PASS: codec5/transport2/index1/compression1, target9개, 2workers |
| 실제 Java calendar oracle | PASS: Asia/Seoul·New_York DST·base 이전, 원본 고정24시간 날짜 계산 |
| 실제 Java DB oracle | PASS: Java writer→Go reader 및 Go writer→원본 Java/Scala reader, plain/gzip 모두 |
| embedded counters.xml 비교 | PASS: 원본 resource와 byte-identical, SHA256=`6b3aa0b6b46e0e26f62f4c0bcb86e9b0f7bae16255e2cfdbdb2910df6f5ccab2` |
| `git diff --check` | PASS |

다음 명령은 exit0이며 `integration_passed=true` report를 생성했다.

```sh
python3 scripts/java-integration.py \
  --server ./dist/scouter-go \
  --report <report.json>
```

검증 binary SHA256=`f0c4c4595843539f0443ef963ebc73e904d75e9d752c3873ab7fcf03d5aa26f1`. report는 저장소에 넣지 않는 실행 산출물이며 원본 source/resource/harness/binary checksum, 실제 assertion, 종료 통계를 담는다. 원본 Java source232개/resource6개를 직접 컴파일하여 class387개를 만들었고 외부 JAR는 사용하지 않았다. UI preference adapter 하나를 제외한 Host 송신/TCP worker 및 Client login/socket/query 클래스는 정본 그대로 실행한다.

Client 명령25종 경로를 검사했다. 잘못된/정상 로그인과 XML/status bootstrap, dated/default Object, daily ERROR empty·missing·duplicate dictionary, counter MULTI/ALL/PAST_ALL·FIVE_MIN288개 배열, realtime cursor FIRST·error bypass·latest gap0, history V2 metadata LAST·page2+1·exhausted cursor를 검증했다. 일반 history와 V2 union의 XLog bytes/order가 같다. Profile V1/V2 raw chunk는 원본 Step decoder로 확인했다. 실제 Host TcpWorker V1/ReqestHandlingProxy/HostAgentEnv의 OBJECT_ENV round-trip도 통과했다. 세부 원본 경로·assertion은 [harness 문서](../../tools/java-integration/README.md)에 있다.

| 종료 단계 | 수집 완료/XLog | UDP datagram/Pack | TCP accepted/request | 저장·parse·reject·TCP 오류/active |
|---|---|---|---|---|
| Seed | 12/3 | 14/12 | 4/41 | 모두0 |
| 동일 DB 재시작 | 0/0 | 0/0 | 3/31 | 모두0 |

SIGTERM 두 번 모두 exit0, 재시작 조회에는 Agent 데이터를 재송신하지 않았다. TCP 횟수는 원본 ServerManager 주기 호출에 따라 달라지므로 harness는 고정 수치로 판정하지 않는다.

DB oracle는 원본 Object overwrite, permanent/daily Text, XLog time/txid/gxid·순서, Profile chunk와 압축 mode override, realtime dictionary 및 daily counter5개 timetype/4개 Value type을 검사한다. index oracle는 실제 Java/Scala index bytes4개와 양방향 reader를 검사한다. Logger/Configure/root routing 등 infrastructure stub의 범위는 [DB oracle 문서](../../tools/java-storage/README.md)에 명시한다.

## 발견·수정과 회귀

- signed40 offset/압축 Int3 처리, DST 날짜, Object 기본 deadtime8초, 빈 counter metadata 상한, dictionary 저장 실패 rollback을 원본과 맞췄다.
- 하루100001 realtime record가 있어도 좁은 시간 조회가 전체 day 상한에 막히지 않도록 index range scan을 적용하고 회귀 검증했다.
- gate 대기 취소, gzip/query context, index callback의 slice 변경에 따른 dedup 오염, queued Agent 요청 취소가 진행 중인 연결을 닫는 문제를 검증·수정했다.
- lock 최초 exclusive 생성/소유 inode 보호, 저장 flush와 독립적인 heartbeat, 오류 logger 포화 계수를 검증했다.
- Java harness resource 누락으로 인한 실패는 원본 resource 복사로 수정했다. 영구 SERVICE empty는 원본 disk writer가 생략하므로 재시작 empty 검증 입력을 daily ERROR로 정정했다. 이를 server 결함 수정으로 보고하지 않는다.

## 미실행·가정·남은 gate

원격 GitHub Actions는 실행하지 않았다. workflow에는 pinned source checkout 뒤 JDK8 compiler/JDK17, 전체 검사와 DB/실제 통신 gate를 연결했으나 원격 runner 결과는 별도다. benchmark/지속TPS/운영 DB 이관·rollback/crash recovery는 이번 기능 검증의 범위가 아니므로 실행하지 않았다.

Java JVM charset=UTF8와 동일 timezone이 전제다. 전체 Eclipse RCP UI, OSHI sampling, Java instrumentation Agent V2 process는 실행하지 않았다. Agent V2는 기존 synthetic wire fixture/socket 검증과 구별한다. 원본 TextCache의 영구 empty 일시 조회는 Go 미지원 차이다. 100-entry dictionary batch 경계와 실제 Java ring wrap은 이번 소규모 harness에 포함하지 않았다.

나머지 Pack 수집 의미, 전체229 handler, LDAP/raw login/custom XML/JVM plugin, Zipkin/B3, stack 저장, retention, Pulse/Kafka/Collector는 후속 단계다. gzip record별 block 재압축과 단일 수집 gate는 고TPS 경로로 검증하지 않았다. 이 결과는 기본 흐름의 synthetic 호환성 증거이며 전체 서버 교체·무손실 운영·목표 처리량의 증거가 아니다.

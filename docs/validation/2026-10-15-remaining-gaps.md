# 3C 잔여 기능 구현·통합 검증 기록

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

branch `codex/remaining-gaps`, 기록 기준 HEAD `3335cce`(2026-10-15). 단계 정의는 [implementation-plan](../implementation-plan.md) 3C 행, 키별 동작은 [config-behavior](../compatibility/config-behavior.md), 기능별 계약과 의도된 차이는 각 `internal/*/CONTRACT.md`가 정본이다. 이 문서의 수치는 개발 머신의 로컬 측정과 합성 데이터 결과다. 운영 규모·호스트 사양·목표 처리량은 담지 않는다.

결론: 3C 기능을 모두 구현해 서버 runtime에 연결했다. 기능별 Java oracle 대조, 독립 리뷰 2회(High 0, Medium 3·Low 15, 모두 수정 또는 문서화)와 통합 시험을 마쳤다. 통합 시험에서 `make replay` FAIL 1건이 나왔고, 원인은 로그 분류였으며 수정했다. 표준 gate 최종 결과는 §6 표에 있다. 실제 Agent 환경 배포는 이 기록 시점 뒤에 했고 결과는 §10·§11에 있다.

## 1. 범위와 제외

| 구분 | 내용 | 계약 |
|---|---|---|
| Pulse Kafka | producer lz4 압축 기본값(`none` 선택 가능), flexible version(ApiVersions v3, Metadata v1–13, Produce v3–12) | [pulse CONTRACT](../../internal/pulse/CONTRACT.md) |
| GeoIP | legacy `.dat` reader(geoip-api 1.3.1 `getLocation` 재현), XLog 국가·도시 보강, `geoip_*` live 설정 | [geoip](../../internal/geoip/CONTRACT.md), [ingest](../../internal/ingest/CONTRACT.md) §3 |
| 수집 부수 경로 | Kubernetes pod 순번(`OBJECT_SET_KUBE_SEQ`), CPU 초과 자동 thread dump, ServerStat, AgentManager `INACTIVE_OBJECT`/`ACTIVATED_OBJECT`/`MAYBE_OBJ_DUP`, `addObjectTypeIfNotExist` | [ingest](../../internal/ingest/CONTRACT.md) §1·§2·§5·§6 |
| 실행 모드 | `-console`(TermMain 12개 명령), `<pid>.scouter` 종료 파일(`-exit-file`), SIGHUP graceful, SIGQUIT dump 후 계속 실행 | [console](../../internal/console/CONTRACT.md), [console.md](../compatibility/console.md) |
| SqlTables | `SQLSimpleParser.getCrudInfo` 이식과 queue·집합 상한, 영구 `text_table` | [sqltable](../../internal/sqltable/CONTRACT.md) |
| Logger | Java 형식 server/request log, Pack `toString`, `log_udp_*`·`log_udp_some_object`·`log_test_rate` 출력 | [logger](../../internal/logger/CONTRACT.md) |
| live 설정 | F0(`Snapshot.Live` → `Runtime.ApplySettings`)와 ingest·agent pool·service·transport·storage·retention·logger·webapp·account LDAP 소비자 | [config](../../internal/config/CONTRACT.md) §5 |

제외 확정(프로젝트 결정, 명시적 미지원): JVM 스크립트·plugin 실행(`.alert` 규칙 컴파일·실행, `.plug`, plugin JAR, 확장 handler chain), webapp의 web_ext(`/extweb/*`)·swagger·WebSocket `/basic`·webroot·`/v0/temp/test/*`. 빈 성공으로 대체하지 않는다.

연결하지 않은 키(대응 구조가 없거나 Java도 기동 값): [config-behavior](../compatibility/config-behavior.md) §5.1a.

## 2. 작업 방식

기능별로 구현 범위를 나눠 진행한 뒤 통합하고 gate를 실행했다. 원본 계약 조사, binary 통합 검증, 독립 정적 리뷰 2회는 구현과 분리해 진행했다.

## 3. 기능별 Java oracle 근거

모두 고정 원본 `dee47fb5`(또는 그 원본이 쓰는 배포 jar)를 수정 없이 실행해 만든 golden이며, Go 테스트가 커밋된 golden과 비교한다. 재생성 비교는 각 make target이다.

| 기능 | oracle | 규모 | 재생성 비교 |
|---|---|---|---|
| GeoIP reader | geoip-api 1.3.1 `LookupService(GEOIP_MEMORY_CACHE).getLocation`(JDK 8, JDK 17 재생성도 byte 동일) | golden 27(집계). 합성 `.dat` 25개(정상·경계·손상)에 대한 조회 685건과 country code 표 | `make geoip-oracle-check` |
| ingest 부수 경로 | 원본 `PodSeqManager`, `ObjectCpuChecker`, `GeoIpUtil`, `AgentManager`/`ObjectMap`/daemon(JDK 8) | kube 31, cpu 29, geoip 31, alerts 39 step | `make ingest-oracle-check` |
| SqlTables | `SQLSimpleParser.getCrudInfo`(JDK 8) | 2107건 | `make sqltable-oracle-check` |
| Logger | 원본 `Logger`, `RequestLogger`, Pack/Value `toString` | golden 파일 6개(`logger`, `numbers`, `packstring`, 시각 2개 zone, `manifest`) | `make logger-oracle-check` |
| 관리·설정 | `tools/java-management`(3B oracle에 live LOGIN·XLog 하한 등 추가) | golden 452 | `make management-oracle-check` |
| storage 설정 | `tools/java-storage/ConfigStorageOracle.java`: 원본 `TextWR`/`TextPermWR`/`TextRD`, `TextPermIndex`/`TextTable`/`XLogIndex`/`KeyValueStoreIndex`, `AutoDeleteScheduler` | 일별 text off→on→off 전환 뒤 파일 17개 byte 동일·조회 15 key, index 크기별 파일 byte 동일·상호 읽기, purge 4단계 설정 변경 뒤 남은 파일 목록 동일 | `make storage-check` |
| Kafka lz4 | kafka-clients 4.0.0 `MemoryRecords`+`ensureValid`(`TestJavaLZ4Oracle`) | Go lz4/none batch 5종·LZ4 frame 11종을 Java가 해제, 역방향 Java frame·batch를 Go가 해제, XXH32 벡터 일치 | `go test ./internal/pulse/kafka` |
| Kafka 실제 broker | 로컬 격리 Kafka 4.0.0 KRaft | 7 시나리오 각 96 record 소비 일치, 협상 ApiVersions v3/Metadata v13/Produce v12(non-flexible 강제 시 v0/v8/v8), broker segment batch codec lz4 35·none 1 | `scripts/kafka-local.py` |

콘솔 출력 형식은 `ConsoleFormatOracle`(JDK 8·17, 각 246줄)로 숫자·패딩·txid helper만 대조했다. 명령 출력 줄 단위의 Java 콘솔 대조는 하지 않았다(§7).

## 4. 독립 리뷰

둘 다 정적 리뷰이며 명령은 실행하지 않았다. High는 두 리뷰 모두 0건이다.

**리뷰 1**(ingest·geoip·sqltable·logger·console): Medium 2, Low 8

| ID | 지적 | 처리 |
|---|---|---|
| M1 | SIGQUIT 미처리(Go 기본은 dump 후 exit 2) | `d365907`: Java처럼 goroutine dump를 쓰고 계속 실행 |
| M2 | kube 상한 예약이 확정 heartbeat까지 거절 | `5eaf09b`: 분기별 예약으로 확정 heartbeat는 상한에서도 통과 |
| L1 | 기동 시 적재한 객체의 첫 heartbeat를 저장하지 않음 | `05a0d8d` |
| L2 | 콘솔·종료 파일 기동 실패 시 logger를 닫지 않음 | `d365907`: 기동 실패를 한 경로로 닫음 |
| L3 | SqlTables `Close` timeout이 sink 호출을 끊지 못함 | `75ec8ca`(쓰기 전 ctx 확인) 뒤 `33a5544`(merge `3335cce`): storage `Store.PutTextContext`가 Store gate 대기를 ctx로 취소하고 쓰기 전에 ctx를 확인한다. 이미 시작한 레코드 쓰기는 파일 무결성을 위해 끝까지 쓴다 |
| L4 | GeoIP city text 쓰기 실패가 XLog 실패로 이어짐 | `9a9235f`: XLog는 보강 없이 유지 |
| L5 | agent call 취소 집계 오류 | `da7d973`: worker ctx 종료 뒤 꺼낸 호출은 abandon |
| L6 | `ACTIVATED_OBJECT`가 상한 검사보다 먼저 발생 | `05a0d8d`: heartbeat 수락 뒤 생성 |
| L7 | `log_test_rate` 처리 중 lock 안에서 디스크 조회 | `75ec8ca`: 서비스명 조회 100ms 상한 |
| L8 | 비터미널 stdin `O_NONBLOCK` 경계 | 문서화(`internal/console/CONTRACT.md`) |

**리뷰 2**(config·transport·storage·service·webapp·account·kafka): Medium 1, Low 7

| ID | 지적 | 처리 |
|---|---|---|
| M1 | Client pool 대기자에 시간 상한이 없고 zombie 대기자가 남음 | `7d8e126`: 대기 시간 상한 so-timeout, 대기자 수 `MaxConnections/2`, 대기 중 EOF는 포기로 처리하고 그 명령은 실행하지 않음 |
| L1 | webapp pool idle과 서버 so-timeout 경합 | `8ed4c96`: 응답 전 EOF/reset이면 1회 재시도, 부작용 명령 제외 |
| L2 | 8초 이상 간격 폴러의 idle 종료가 오류로 집계 | `37c6b42`: `scouter-replay`가 idle 종료 뒤 조용히 재연결(`idle_reconnects`) |
| L3 | servlet 전용 flag가 webapp 설정을 pin | `d365907` |
| L4 | webapp pool timeout 음수일 때 기동 실패 | `d365907`: 0 이하는 Java처럼 idle 연결을 매번 폐기 |
| L5 | S152/S161 rate limit이 Java보다 느슨 | `75ec8ca` |
| L6 | 리뷰 1 L2와 같음 | `d365907` |
| L7 | `config-behavior.md`가 배선 전 상태 | 이번 문서 갱신에서 처리 |

그 밖의 후속 수정: `a40041f`(stats 줄 client wait 필드), `b467836`(콘솔 `ingest.geoip_text_errors`), `59ad5f8`(reload 로그가 실제 적용 키만 표시), `dcc9d94`(§5.2).

## 5. 통합 시험 결과

### 5.1 PASS

| 항목 | 결과 |
|---|---|
| `make java-integration` | PASS |
| 실제 binary의 scouter.conf reload | PASS |
| Pulse lz4 end-to-end | PASS. 서버 stats XLog Sent 3600, Profile Sent 936, kafka SentRecords 4536, SentBatches 3065, Failed·Dropped 0. 독립 consumer 소비 meta 3600·profile 936, key 중복 0, traceId·key 불일치 0, partition 규칙 불일치 0. broker segment는 meta 2248 + profile 817 = 3065 batch이며 전부 lz4. 조건: 로컬 Kafka 4.0.0, topic 30 partition, replay rate 300, 10초 + 2초, objects 10 |
| Pulse 순서 | dispatcher workers=1이면 partition 안 순서 역전 0. 기본 workers=2(acks=0)에서는 meta 4·profile 2건 역전이 있었다. worker pool 구조에서 나오는 결과이며 계약 위반이 아니라고 판단했다 |
| 종료 경로 | PASS(경로별 세부는 이 기록에는 남기지 않았다) |

### 5.2 replay FAIL과 수정

- 증상: `make replay`가 `pass=false`. 데이터 결과는 scheduled=verified=2400, missing·extra·dup 0으로 정상이었다.
- 원인: `cmd/scouter-replay/server.go`의 `infoLogLine` 허용 목록에 없는 정상 로그를 오류로 판정했다. 대상은 HEAD에서 새로 생긴 S145 GeoIP(파일 없음), 종료 파일 안내, 종료 시 `sqltable=`·`logger=` 통계 줄과, `408b2cc`부터 있던 `config: ./conf/scouter.conf not found` 줄이다.
- 수정: `dcc9d94`. 해당 줄을 info로 분류하고, replay가 띄우는 서버를 `-exit-file=false`로 실행한다. `37c6b42`(idle 재연결)는 리뷰 2 L2 수정이며 이 FAIL과는 별개다.
- 재실행: 수정 뒤 기본 옵션 `make replay`에서 `pass=true`(scheduled=verified=2400, missing·extra·dup 0, query_errors 0)였다. 최종 gate에서 다시 확인했다(§6).

### 5.3 logger oracle drift(D-L4)

- 현상: `make logger-oracle-check`가 CPU 경합 상태에서 간헐적으로 달라졌다. busy-loop 8개 조건에서 20회 중 3회 재현했다.
- 원인: 원본 `Logger.scala`의 기동 경합이다. 첫 `println`이 daemon의 `pw` close와 겹치면 그 줄이 server log 대신 stdout으로 간다. Go 동작 문제가 아니라 oracle 생성 조건 문제다(원본 결함 후보 D-L4, §8).
- 수정: `c579306`. oracle이 daemon의 첫 sleep을 기다린 뒤(warm-up) 기록한다. 같은 경합 조건에서 22/22 PASS, 평상시 `--check`는 약 6초다.

### 5.4 로컬 성능 비교

같은 개발 머신에서 3C 이전 build와 3C build를 `scouter-replay`로 비교했다. 조건은 rate 1000, 30초, objects 50, `GOMAXPROCS=2`이다.

| 지표 | 이전 build | 3C build |
|---|---|---|
| 완료 XLog | 35000 | 35000(동일) |
| CPU 시간 평균 | 19.43s | 20.40s |
| RSS | 비슷함 | 비슷함 |

CPU 차이(약 5%)는 반복 측정의 잡음 범위로 판단했다. 마이크로벤치나 운영 처리량 주장이 아니며, 공유 호스트 예산·장시간 부하·포화 조건은 측정하지 않았다(§7). 비교 대상 이전 build의 commit과 반복 횟수는 이 기록에는 남기지 않았다.

## 6. 표준 gate

통합 시 `codex/remaining-gaps` `3335cce`(코드 최종 상태; 이후 commit은 문서·CONTRACT만)에서 실행했다. 로컬 macOS(darwin/arm64), `GOMAXPROCS=2`, `SCOUTER_SOURCE`=고정 원본(dee47fb), JDK 8 Zulu.

| 명령 | 결과 |
|---|---|
| `make check` | PASS — fmt-check·vet·test·build, 25개 package ok |
| `make race` | PASS — 25개 package ok, DATA RACE 없음 |
| `make fuzz` | PASS — 19개 target × `FUZZTIME=10s`(geoip `FuzzFromBytes`, kafka `FuzzLZ4RoundTrip`·`FuzzSkipTags`, sqltable `FuzzCrudInfo` 포함) |
| `make fixtures-check` | PASS — Java oracle fixture 141개 |
| `make inventory-check` | PASS |
| `make storage-check` | PASS — Java storage oracle(ConfigStorageOracle 포함) skip 없음 |
| `make management-oracle-check` | PASS — 452 golden 재생성 일치 |
| `make geoip-oracle-check` | PASS — 27 golden 재생성 일치 |
| `make ingest-oracle-check` | PASS — kube 31 / cpu 29 / geoip 31 / alerts 39 step golden 일치 |
| `make sqltable-oracle-check` | PASS — CRUD golden 12658줄 일치 |
| `make logger-oracle-check` | PASS — 6 golden 일치 |
| `make pulse-fixtures-check` | PASS — Pulse golden 124 case. `PULSE_PLUGIN_SOURCE` 지정 필요(기본 상대 경로에 Pulse plugin 소스가 없으면 실패 — 환경 문제) |
| `make java-integration` | PASS — 원본 통신 클래스 seed·restart·tcp-agent-v1·extra UDP·graceful SIGTERM 재기동 조회 |
| `make replay` | PASS — `pass=true scheduled=2400 verified=2400 missing=0 extra=0 dup=0` |

## 7. 미검증 항목

- 실제 MaxMind `GeoLiteCity.dat`(합성 DB만 사용).
- 실제 Java Agent에서 `OBJECT_SET_KUBE_SEQ` 순번 적용과 `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS` dump 실행.
- Java `-console` 서버와의 명령 출력 줄 단위 대조, jline Tab 후보 배치·EOF 실제 동작.
- Java webapp JSON 응답 대조.
- 실제 LDAP 서버 bind.
- Windows 실행(`flock`, termios).
- 장시간 부하와 queue·pool 포화 조건, 공유 호스트 자원 예산.
- pty raw mode에서 Ctrl-\(SIGQUIT)의 dump 후 계속 실행.
- Kafka 4.1+·2.4 미만 실제 broker, LZ4 CPU 비용과 Pulse on/off 처리량.
- 실제 Agent 환경 배포(이 기록 시점 기준. 이후 결과는 §10·§11).

## 8. 원본 결함·취약 동작 후보(정상 계약과 분리)

| ID | 내용 | Go 처리 | 근거 |
|---|---|---|---|
| D-L4 | 원본 Logger 기동 경합으로 첫 줄이 stdout으로 갈 수 있음 | owner goroutine 하나가 파일을 열고 닫아 경합 없음. oracle은 warm-up | logger CONTRACT, §5.3 |
| D-L1 | `log_rotation_enabled=false`면 open마다 `server.log`를 비움 | append | logger CONTRACT |
| D-L2 | `request-*.log` 경로가 기동 때 `log_dir`에 고정 | live `log_dir`을 따름 | logger CONTRACT |
| D-L3 | 정리 중 예외로 Logger thread 종료 | 경고·집계 후 계속 | logger CONTRACT |
| D-S1 | `log_udp_text`·S112가 원문 SQL을 남김 | `sql`·`login` Text 마스킹, S112는 hash·길이만 | logger·sqltable CONTRACT |
| GeoIP 192.168 | signed byte 비교 때문에 192.168.x.x를 사설로 보지 않고 조회 | **원본대로 재현** | ingest CONTRACT §3, ingest oracle |
| GeoIP 예외 | `getLocation` RuntimeException이 XLogCore loop까지 올라가 그 XLog 전체(그룹 성능·plugin·cache·저장)가 빠짐 | XLog는 처리하고 GeoIP 보강만 건너뜀(`ingest.geoip_errors`). city text 쓰기 실패도 XLog 유지(`9a9235f`) | geoip·ingest CONTRACT |
| D-G1 | GeoIP 파일 교체 뒤에도 IP cache를 비우지 않음 | 새 DB가 열리면 cache를 비움 | ingest CONTRACT §3 |
| D-N1 | `net_tcp_service_pool_size` 증가가 JDK 9+에서 반영되지 않음(JDK 8은 core > max) | 증가·감소 즉시 반영(JDK 8 의미) | transport CONTRACT §5 |
| D-N2 | 대기 중 끊긴 Client의 명령을 나중에 실행, 대기열 무제한 | 끊김을 감지해 포기 처리, 대기 시간·대기자 수 상한 | transport CONTRACT §3·§5 |
| sampling 값 ≤ 0 | `xlog_sampling_matcher_*` 0 이하에서 무제한 map 또는 0 나눗셈 | 기본값 사용 | ingest CONTRACT §7 |
| SqlTables 파싱 비용 | 닫히지 않은 괄호 반복에서 초다항식 시간(상한 없음) | 입력·작업량·node 상한, 초과는 파싱 실패 | sqltable CONTRACT §1 |
| 콘솔 D-C1~D-C8 | stdin EOF busy loop, 콘솔 모드 heartbeat 없음, 인자 오류 NPE 등 | 한 줄 오류·heartbeat 유지 등 | [console.md](../compatibility/console.md) §7, console CONTRACT |

## 9. 남은 위험

- SqlTables `Close`는 이미 시작한 레코드 쓰기(디스크 I/O)를 중간에 끊지 않는다. Store gate 대기는 ctx로 취소된다(리뷰 1 L3).
- Client idle 종료가 Java처럼 8초가 되면서 8초보다 긴 간격으로 폴링하는 Client API 소비자는 재연결이 필요하다([runtime](../compatibility/runtime.md) "운영자 안내").
- 종료 파일은 실행 셸의 현재 디렉터리에 생긴다. 그 디렉터리를 정리하면 서버가 정상 종료한다.
- 기본 Pulse dispatcher(workers=2)에서는 partition 안 순서가 보장되지 않는다(plugin 계약과 같음).

## 10. 실제 Agent 환경 배포와 후속 수정 (2026-10-16)

`codex/remaining-gaps`를 master에 병합한 빌드를 실제 Java Agent와 Scouter Client가 붙는 환경에 배포해 확인했다. 그 환경의 규모·처리량·설정값은 기록하지 않고, 발견한 결함의 원인과 수정, 회귀 테스트만 남긴다.

### 10.1 배포 중 발견·수정

| 발견 | 원인 | 수정 | 확인 |
|---|---|---|---|
| 종료 파일·GeoIP 기본 경로가 실행한 사용자의 홈 디렉터리 기준이 됨 | `scripts/scouter-go.sh`가 서버 디렉터리로 이동하지 않고 실행해, Java `stop.sh`(`rm -f *.scouter`)와 상대 경로 의미가 어긋남 | 실행 subshell에서 `cd "$HERE"`(`20ef65e`) | `<pid>.scouter`가 서버 디렉터리에 생성, S145 경로가 `conf/` 기준 |
| Scouter Client에서 SQL bind parameter가 모두 `[******]` | 단일 계정 모드(`--user/--password`) LOGIN이 첫 구현 때부터 그룹 정책 18개를 모두 false로 보냄(당시 관리 명령 미지원). Client `ProfileText`는 `AllowSqlParameter`가 false면 가림 | Java 기본 admin 그룹 정책을 보내되 account store가 필요한 3개(`AllowEditGroupPolicy`·`AllowAddAccount`·`AllowEditAccount`)만 false(`877922a`, `7851804`). `TestSingleCredentialAdminPolicy` | Client 프로토콜 LOGIN probe로 `AllowSqlParameter=true` 확인 |
| 같은 유형 재검토(독립 리뷰) | 고정 false·빈 응답은 추가 없음. 대신 Go 자원 상한이 Java 기능을 막는 곳: XLog 범위 조회 10만 건 상한(필터 전 scan), Client 응답 8MiB 누적·30초 총 deadline, Agent blob 8MiB 수집, Agent V2 4MiB·TRANX_PROFILE 4MiB | XLog/Summary/Alert 범위 조회 streaming(필터·max를 scan 안에서), Client 응답은 진전 기준 timeout과 단일 값 256MiB, heap dump는 chunk 중계, 큰 단일 응답은 명령별 상한, Agent 요청 deadline ≥ so_timeout. `--user/--password`가 `conf/account.xml`을 무시할 때와 retention off일 때 기동 WARNING | 전체 객체 대상 `TRANX_LOAD_TIME_GROUP`이 상한 없이 끝까지 전송됨을 실제 Client 프로토콜로 확인. heap dump·jar export는 로컬 e2e(`TestE2EHeapDumpDownloadOverEightMiB`, `TestE2EDownloadJarOverFourMiB`) |
| 재기동마다 기동 직후 수집 정체와 UDP queue reject | goroutine dump: 첫 `PutXLog`가 오늘 XLog index를 열며 **정상 종료 뒤에도** 전체 crash-recovery 검증(`legacyindex.validateRecover`)을 engine·store lock 안에서 수행. 이어서 첫 realtime counter 쓰기도 `counter/real` index 전체 검증 | 정상 종료 marker `.go-clean`과 index별 Seal(크기·mtime·header CRC). Seal과 정확히 일치할 때만 O(1) 검사로 열고, crash·변경·손상은 기존 전체 검증. Store·KV·Counter index 적용(`TestOpenSealedSkipsWalkOnlyForExactPair`, `TestOpenSealedFallsBackOnMismatch`, `TestSealsCarriedOnlyThroughCleanRuns`) | marker가 stop 때 기록, start 때 삭제됨을 확인. 재기동 후 reject 0 |
| 범위 조회가 수집을 막음 | 일치가 없는 scan은 batch가 차지 않아 store gate를 한 번 잡은 채 구간 전체를 읽음 | gate 보유를 256단계 또는 2ms로 제한하고 이어 읽기, 큰 bucket 정렬은 gate 밖(`TestRangeScanGateHoldIsBounded`) | 장구간 조회 중 수집 대기가 줄고 reject 0 |
| java-integration이 드물게 FIVE_MIN 검사 실패 | 실행이 5분 경계를 넘으면 서버 자동 5분 표본화가 bucket을 하나 더 씀(Java와 같은 동작) | 하네스 서버만 `_auto_5m_sampling=false`(`b9554d6`) | 이후 gate PASS |

### 10.2 최종 gate (`d0e8ad5`, 로컬)

check·fixtures·inventory·storage·management/geoip/ingest/sqltable/logger/pulse oracle·java-integration·replay·fuzz PASS. 전체 `go test -race -timeout 60m ./...`에서는 load average가 높은 상태(40~80)에서 시간 상한 테스트 2건(`TestRangeScanGateHoldIsBounded` 150ms 상한, `TestLocalStoreActualJavaHandlersAndDB` 2초 i/o deadline)이 실패했고 DATA RACE는 0건이었다. 두 테스트는 `-race` 단독 실행에서 PASS했다.

### 10.3 남은 위험

- 장구간 범위 조회 중 수집 대기가 짧게 남는다(GC·디스크 경합 후보, 미확인).
- crash 뒤 재기동은 여전히 그날 index 전체를 수집 경로에서 검증한다(기동 단계 미리 열기는 후속 후보).
- tagcount top100 cache 생성(5분 주기)이 store gate 안에서 파일 전체를 읽는다(측정 전).
- `conf/account.xml`의 여러 계정·그룹 정책 조합은 실제 Client로 검증하지 않았다.
- 4MiB를 넘는 jar export의 실제 Java Agent·Client 대조, 실제 GeoLiteCity.dat는 미검증이다.

### 10.4 계정 체계·heap dump·세션 유지 정책

- **Java 계정 체계**: `scripts/scouter-go.sh`가 기본으로 `--user/--password`를 넘기지 않아 `conf/account.xml`·`account_group.xml`을 쓴다(`0489768`). `SCOUTER_USER`/`SCOUTER_PASSWORD`를 지정할 때만 단일 계정 모드다. 계정 파일 모드의 LOGIN 응답은 admin이 `type=admin`, email, admin 그룹 정책(18개 모두 true)이고 guest는 Java 기본 guest 정책이다(로컬 확인).
- **heap dump**: 실제 Java Agent 하나에서 `OBJECT_CALL_HEAP_DUMP` → `OBJECT_LIST_HEAP_DUMP` → `OBJECT_DOWNLOAD_HEAP_DUMP` → `OBJECT_DELETE_HEAP_DUMP`를 1회 확인했다. hprof가 2MiB chunk로 끝까지 전송되고 크기가 목록 값과 일치했으며, 전송 중 수집 지표(reject·ingest 지연·RSS) 변화가 없었다. `OBJECT_DOWNLOAD_JAR`은 agent jar(4MiB 미만)로 동작만 확인했다. 4MiB를 넘는 앱 jar는 원본 Agent가 jar 전체를 대상 JVM heap으로 읽어(`FileUtil.readAll`) 작은 heap 앱에 OOM 위험이 있어 실제 Agent로는 실행하지 않았다(로컬 e2e만).
- **세션 유지 정책(의도된 차이)**: Java `LoginManager`는 마지막 인증 후 3시간(internal 7일) 동안 세션을 유지하고 연결 종료로 정리하지 않는다. 그래서 연결마다 `internal=true`로 LOGIN하고 유휴 연결을 재사용하는 Client API 소비자가 있으면 Login List에 닫힌 연결의 세션이 쌓인다. Go는 **연결 기반 만료**를 둔다(`internal/service/CONTRACT.md` §1).
  - internal 포함 모든 session은 살아 있는 Client TCP 연결이 붙잡는 동안 TTL 없이 유지한다. 연결은 마지막으로 LOGIN한 session 하나만 붙잡는다(`61e00ef`).
  - 마지막 연결이 닫힌 뒤 G(=2×`net_tcp_client_so_timeout_ms`, 30초~10분)가 지나면 제거한다. 서버는 session 만료를 이유로 연결을 닫지 않는다.
  - 첫 구현은 internal session을 연결 규칙에서 제외하고 Java 7일 TTL만 적용했다. 그 규칙에서는 재연결마다 생긴 internal session이 계속 남아, 위 규칙으로 고쳤다. 회귀 테스트 `TestInternalSessionFollowsConnections`, `TestHeldSessionOutlivesTTL`은 이전 규칙에서 실패한다.
  - 원본 Client는 `INVALID_SESSION`이면 세션을 0으로 두고 SessionObserver가 5초 안에 silent re-login한다(`TcpProxy.java:166-168`, `SessionObserver.java:42-68`). 사용 중인 session은 항상 살아 있는 연결이 붙잡고 있으므로, 이 정책에서 서버가 강제 재로그인을 일으키는 경로는 없다.
  - 실제 Agent 환경 확인: 개정 정책에서 Client API 소비자의 세션 수는 열린 연결 수를 따라갔고 누적되지 않았다. 서버 로그에 `invalid Scouter session`은 없었다.
- **전환 주의**: 유휴 연결을 재사용하는 Client API 소비자는 `net_tcp_client_so_timeout_ms`가 그 소비자의 유휴 재사용 시간보다 짧으면 서버가 이미 닫은 연결을 다시 써서 EOF로 실패한다(Java와 같은 동작, Java 기본값 8000).

## 11. 실환경 확인 요약 (첫 배포·재기동 반복, 2026-10-15)

3C 이전 빌드를 실제 Java Agent·Scouter Client 환경에 처음 배포했을 때의 결론이다. 환경 정보는 남기지 않는다.

- **text 재전송 daemon 부재**: Java Agent는 text를 한 번만 보내고 sent-set에 보관한다. Java 서버는 `TextCacheReset`이 기동 30초 뒤와 날짜 변경 때 live Agent에 `OBJECT_RESET_CACHE`를 보내는데 Go에 이 daemon이 없어, 재기동 뒤 service 이름이 비고 Pulse가 dictionary miss로 걸렀다. `internal/server/text_cache_reset.go`를 추가했다(`TestTextCacheResetterCallsOnStartAndDateChange`, `TestTextCacheResetSendsCommandOverAgentConnection`).
- **LOGIN의 server 이름**: 고정 `1` 대신 원본처럼 문자열 `server_id`(기본 hostname)를 보낸다(`--server-id`).
- **`CHECK_JOB`**: Client가 2초마다 보내는 이 명령을 미지원 오류로 닫아 Client가 재접속을 반복했다. Java와 같은 "job 없음" 응답으로 고쳤고, 이후 `REMOTE_CONTROL*` enqueue를 구현했다(`TestCheckJobRequiresOwnSession`, [agent-read-query](../compatibility/agent-read-query.md)).
- **재기동 직후 UDP reject**: Agent 전체가 다시 붙는 순간 당시 기본값(queue 1024, worker 2)이 넘쳤다. 이후 기본값을 queue 8192·수신 버퍼 4MiB로 올렸다([최대 처리량 기록](2026-10-17-xlog-max-benchmark.md) §13.2·§16.1).
- **재기동 반복**: SIGTERM·SIGKILL 정지와 기동을 다운 길이를 바꿔 가며 여러 번 반복해도 Agent는 서버 listen 뒤 수 초 안에 모두 다시 붙었다. Agent의 TCP worker thread는 원본 상한 10에서 재사용됐고, Agent 프로세스의 thread·RSS·CPU·로그에 변화가 없었다. 서버 다운 중 반열림 소켓은 남지 않았다.
- **SIGKILL 뒤**: 다음 기동에서 index relink로 복구됐고 조회 오류가 없었다. 기동 30초 안에 다시 내려가면 reset은 다음 기동에서 나간다(Java `TextCacheReset`와 같다). 날짜 경계의 reset도 Agent 쪽 부작용 없이 나갔다.
- 서버→Agent 바이트 경로 대조와 Agent 쪽 영향 분석은 [agent-side-effects](../compatibility/agent-side-effects.md)에 있다.

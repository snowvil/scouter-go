# Scouter Go

Scouter Server(Java/Scala)를 단일 Go 실행 파일로 대체하는 프로젝트입니다. 기존 Java Agent·Host Agent·Scouter Client를 수정하지 않고 연결하며, 원본 파일 DB 형식과 조회 의미를 보존합니다. Pulse plugin의 Kafka 전파 기능은 서버 내장 기능(Pulse)으로 들어 있습니다.

- 정본: Scouter `2.21.3`(commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`)과 Pulse plugin 원본입니다. Pulse plugin 원본은 비공개이며, 그 원본에서 만든 golden을 내용 digest와 함께 고정해 둡니다. 기준은 [baseline.json](docs/compatibility/baseline.json)에 있습니다.
- 목표: 같은 데이터를 받아 같은 결과를 돌려주면서, 더 적은 CPU·메모리로 더 많은 완료 transaction을 처리하는 것입니다. 운영 규모·목표 처리량은 환경마다 다르므로 이 저장소에 적지 않습니다([성능 기준](docs/performance.md)).
- [참고 Go 구현](https://github.com/scouter-project/scouter-server-go)은 설계 참고자료로만 평가했습니다([평가](docs/reference-evaluation.md)).

## 현재 상태

**주요 서버 기능은 구현되어 있으며, 실제 Agent·Client 상호운용은 아래에 기록한 범위에서 검증했습니다. 운영 전환 검증은 남아 있습니다.** 전체 포팅 완료나 목표 처리량 달성을 뜻하지 않습니다. 단계별 상태는 [구현 순서](docs/implementation-plan.md)가 정본입니다.

문서의 소스 대조 기준은 2026-11-07 점검입니다. 로컬 검증과 과거 실제 Agent 환경 검증의 구분은 [문서 정합성 점검 기록](docs/validation/2026-11-07-source-docs-sync.md)에 있습니다.

### 되는 것

- **수집**: UDP 단일·묶음·분할, TCP Agent V1(Host Agent)·V2(Java Agent)·stack upload. 원본 built-in Pack 16종, Value 17종, Step 23종.
- **저장**: Java 파일 DB와 같은 형식(XLog·profile·counter·text·object·summary·alert·KV·visitor·tag, GZipStore 압축 포함). Java가 쓴 DB를 Go가 읽고 Go가 쓴 DB를 Java가 읽는 양방향 검증을 했습니다([저장 계약](docs/compatibility/storage-contract.md)).
- **조회**: 계정 파일 모드에서 원본 Client handler 229개 전체를 등록합니다(READ 184, 상태 변경·관리 45). 단일 계정 모드에서는 계정 파일 관리 명령을 등록하지 않습니다. JVM에 의존하는 READ 5개는 JVM 실행 대신 정적 descriptor·load message·goroutine 정보로 답합니다([관리 명령](docs/compatibility/management-commands.md)).
- **부가 기능**: 계정 파일·LDAP 로그인, `scouter.conf` 3초 리로드, HTTP servlet(`/counter`·`/register`·`/telegraf`)과 Telegraf 수집, scouter.webapp REST API(`/scouter/*`), GeoIP, Kubernetes pod 순번, CPU 초과 자동 thread dump, SqlTables, Java 형식 server/request log, `-console` 콘솔, `<pid>.scouter` 종료 파일.
- **Pulse**: 원본 plugin의 topic·key·header·JSON·filter 계약(Java golden 124/124), 표준 라이브러리 Kafka producer(lz4 기본). 애플리케이션 이름 필터 3종의 기본값만 원본과 다릅니다(아래 [운영 동작 차이](#운영-동작-차이)). 계약은 [Pulse 계약](docs/compatibility/pulse-contract.md)에 있습니다.
- **이관 도구**: `scouter-db`의 inspect·migrate(증분 `--precopy`/`--finish` 포함)·verify·rollback([이관 계약](docs/compatibility/migration.md)).
- **운영 안전장치**: 연결·queue·cache·Client 응답·Agent relay·HTTP 요청에 상한과 포화 계수, `/health` JSON, 주기 stats 줄, 종료 상한(`--shutdown-timeout`), crash 뒤 checkpoint 복구. 상한이 없는 곳(Pulse worker 변환 메모리, store close 시간 등)은 [runtime 계약](docs/compatibility/runtime.md)과 각 `CONTRACT.md`에 적었습니다.

### 검증 수준

증거는 단계가 다릅니다. 앞 단계 통과를 뒤 단계 통과로 보지 않습니다.

| 단계 | 내용 | 기록 |
|---|---|---|
| Java oracle | 고정 원본으로 만든 codec·저장·관리 명령·Pulse·GeoIP 등 golden과 Go 결과 대조 | [validation](docs/validation/) 각 기록 |
| Java 통신 클래스 통합 | 원본 Client 통신 클래스로 56개 명령을 Go 서버에 실행(synthetic) | [Phase 2D](docs/validation/2026-10-13-phase2d.md) |
| 실제 Agent 상호운용 | 실제 `scouter.agent.java`·Host Agent와 원본 Client 클래스로 수집·relay·실패 경로·재기동·서버 발신 명령을 확인. JDK 8·11 각각 56/56 check | [Agent 상호운용](docs/validation/2026-10-24-java-agent-interop.md) |
| 실환경 | 실제 Java Agent와 Scouter Client가 붙은 환경에서 장기 운용·재기동 반복, 이후 Java 서버를 교체해 운영 | [실환경 확인 요약](docs/validation/2026-10-15-remaining-gaps.md), [단계별 결과](docs/improvement-candidates-2026-10-21.md), [전환 성능 평가](docs/reports/java-to-go-cutover-performance.md) |
| 운영 gate | 합의된 SLO·자원 한도·내구성 기준으로 판정한 검증은 없다. 한 환경의 교체 사례는 [전환 성능 평가](docs/reports/java-to-go-cutover-performance.md)에 있다 | — |

### 남은 gate

- 운영 DB 복사본으로 이관·rollback 리허설([runbook](docs/operations/cutover-runbook.md) §3)
- 운영 규모 부하와 목표 처리량, 공유 호스트 자원 예산 안의 지속 부하, 전원 손실 내구성
- 실제 Client GUI의 관리 명령 실동작, Batch Agent, `net_collector_tcp_session_count` 2 이상, 실제 LDAP·Telegraf 서버, Java webapp JSON 응답 대조
- Pulse Collector 실연동·UUID 경계와 Kafka broker 장애 행렬
- Windows 실행(Unix `flock`·termios 기반이라 검증하지 않음)

남은 개선 후보와 단계별 결과는 [운영 전환 전 개선 대상](docs/improvement-candidates-2026-10-21.md)에 있습니다.

## Java 서버와의 차이

원본과 같은 바이트·저장·조회 의미를 지키는 것이 원칙입니다. 아래는 원본과 일부러 다르게 둔 동작, 원본보다 나아진 점, Java 서버에 없는 기능입니다. 항목별 근거는 [runtime 계약](docs/compatibility/runtime.md) "운영자 안내", [runbook](docs/operations/cutover-runbook.md) §2.1, 각 패키지의 `internal/*/CONTRACT.md` "의도된 차이"에 있습니다.

### 운영 동작 차이

전환 전에 꼭 확인할 항목입니다.

| 항목 | Java 서버 | Go 서버 |
|---|---|---|
| retention | `mgr_purge_enabled` 키가 없으면 on | 키나 `--purge`가 없으면 **off**(기동 시 WARNING). 원본처럼 지우려면 `mgr_purge_enabled=true`를 명시합니다 |
| listener 주소 | 키가 없으면 `0.0.0.0` | flag 기본 `127.0.0.1:6100`. conf에 `net_*_listen_port`만 있고 `_ip`가 없으면 `127.0.0.1`을 유지합니다. `scripts/scouter-go.sh`는 `0.0.0.0:6100`을 flag로 넘깁니다(아래 [Linux 서버 배포](#linux-서버-배포)) |
| Pulse 애플리케이션 이름 필터(`ext_plugin_pulse_exclude_applications`·`_rebase_transaction_applications`·`_non_spring_based_applications`) | 키가 없으면 plugin의 배포 고유 기본값 | 키가 없으면 빈 값(제외·rebase 없음)이고 키마다 기동 WARNING(non-Spring 키는 Spring URI filter가 켜졌을 때만). 운영 Java conf에 키가 없으면 plugin 기본값을 비공개 conf로 옮깁니다 |
| JVM plugin·script | `.alert` 규칙, `.plug`, plugin JAR, 확장 handler chain 실행 | 실행하지 않습니다(아래 [명시적 미지원](#명시적-미지원)) |
| 연결·메모리 상한 | Agent·Client 연결 수, Client 응답·relay 메모리, HTTP 동시 요청에 상한 없음 | 상한이 있고 넘으면 사유별 로그와 `/health` 계수를 남깁니다. TCP 상한에 걸린 Agent는 1초 뒤 재접속하며 애플리케이션 thread에는 영향이 없습니다. HTTP는 3초 대기 뒤 `503`+`Retry-After`([sizing](docs/operations/sizing.md)) |
| Client 응답 예산 초과 | 메모리가 허락하는 만큼 응답 | 64KiB 이상 item의 전역 예산을 넘으면 그 명령만 실패합니다. Java Client에는 결과가 비거나 잘려 보입니다 |
| `SERVER_STATUS`·`SERVER_THREAD_*`·`SERVER_ENV` | JVM 값 | Go runtime 값 |
| Pulse 전송 | `sent`는 producer callback 성공. Kafka 압축 gzip·snappy·lz4·zstd | `sent`는 producer queue 수락(broker 영속 증거 아님). 압축은 lz4·none만 받고 나머지는 Pulse 설정 오류 |
| 설정·계정 파일 | `scouter.conf`를 ISO-8859-1로 읽음. 계정 파일 parse 실패 시 빈 계정으로 기동 | `scouter.conf`를 UTF-8로 읽음. 계정 파일 parse 실패 시 기동하지 않고 파일을 덮어쓰지 않음 |
| 로그 | SQL·login 원문 출력. rotation off이면 open마다 server.log를 비움 | SQL·login 원문을 마스킹. rotation off에서도 append |
| `/health` 감시 key | — | 11단계에서 `DelayedRejected`가 3개 key로 나뉘었습니다. 외부 감시 규칙은 [monitoring](docs/operations/monitoring.md)을 따릅니다 |

### 개선

원본 동작은 유지하면서 장애 영향과 자원 사용을 줄인 변경입니다. 측정 조건과 수치는 [개선 대상](docs/improvement-candidates-2026-10-21.md) §7~§20과 [성능 구조 검토](docs/performance-structural-review-2026-10-20.md)에 있습니다.

- **Agent 영향 차단**: TCP 연결 예산을 handshake·Agent·Client로 나눠 Client 폭주가 Agent 접속을 막지 않습니다. descriptor 부족(EMFILE)으로 Accept가 실패해도 TCP 서버가 멈추지 않고 backoff 뒤 다시 받으며, 기동 시 open file 한도에 맞춰 예산을 줄입니다.
- **수집 경로 분리**: 저장 I/O를 kind별 writer로 옮겨 수집 engine lock 밖에서 씁니다. 조회와 purge가 쓰기를 막지 않도록 index·txid 조회·압축 해제 구간을 저장 gate 밖으로 뺐습니다.
- **데이터 보존**: cache 예산을 넘어도 XLog·counter·alert·status·지연 sampling child는 저장합니다. visitor는 object 500개를 넘어도 방문을 잃지 않습니다. 기존 Java DB에서 counter 이름이 4096개를 넘으면 그날 조회·쓰기가 실패하던 경로를 고쳤습니다. text cache reset은 병렬로 실행하고 실패하면 재시도합니다.
- **crash 복구와 종료**: 정상 종료는 `.go-clean`, 실행 중에는 `.go-checkpoint`를 남겨 crash 뒤에는 checkpoint 이후 꼬리만 검증합니다. kill -9 직후에도 Go가 남긴 `lock.dat`는 바로 인계합니다. 종료는 `--shutdown-timeout` 상한 안에서 단계별로 끝나고, 넘으면 멈춘 단계를 남기고 exit 3으로 끝납니다.
- **원본 결함 수정**: webapp bearer 위조·session fixation 등 보안 결함, 잘린 UDP datagram의 부분 parse, 무제한 UDP 재조립을 고쳤습니다(각 `CONTRACT.md`의 "의도된 차이").
- **효율**: visitor HLL 캐시 구조, gxid 집합, counter cache, tag 반영 경로, 압축 writer를 고쳐 같은 부하의 CPU·RSS를 줄였습니다. 효과는 Linux VM에서 개선 전후 바이너리를 쌍으로 비교해 확인했습니다([A/B 방법](docs/performance.md)). 2026-10-17 로컬 비교에서는 같은 판정 기준의 최대 지속 완료 XLog/s가 Java 배포본보다 높았지만 메모리는 더 썼습니다. 이후 개선 뒤에는 Java와 다시 비교하지 않았습니다([기록](docs/validation/2026-10-17-xlog-max-benchmark.md)).

### 추가 기능

Java 서버에 없는 기능입니다.

- **단일 정적 바이너리**: cgo 없이 빌드해 대상 서버에 JVM·Go·공유 라이브러리가 필요 없습니다. Pulse가 내장이라 plugin JAR 배포도 없습니다. 외부 Go 모듈 의존성이 없습니다(Kafka producer 포함 표준 라이브러리만 사용).
- **`/health`와 `/debug/pprof`**: `--http`로 켜는 운영 HTTP. 수집·저장·연결·예산·Pulse 계수를 JSON으로 보이며 servlet·webapp과 별도 동시 요청 예산을 씁니다([monitoring](docs/operations/monitoring.md)).
- **stats 줄**: `--stats-interval`(기본 10초)마다 처리량·지연·대기·거절을 한 줄로 남깁니다.
- **`go_*` 설정 키**: Go 전용 상한을 `scouter.conf`에서 바꿉니다. HTTP admission·Agent relay·Client 응답·Pulse queue·text reset·visitor 예산·`go_maxprocs` 등이며, 상당수는 재기동 없이 반영됩니다(키 목록과 범위는 [config CONTRACT](internal/config/CONTRACT.md) §6).
- **`scouter-db`**: 원본 DB를 수정하지 않는 검사(inspect), 검증된 복사 이관(migrate, 증분 `--precopy`/`--finish`), verify, rollback.
- **`scouter-replay`**: loopback 부하·재시작 검증 report와, Java Agent TCP 세션 다수를 흉내 내는 `agents` 연결 규모 시험([성능 기준](docs/performance.md), [연결 규모 기록](docs/validation/2026-10-22-connection-scale.md)).
- **배포·운영 스크립트**: checksum을 확인하고 교체하는 `scripts/deploy.sh`, POSIX sh `scripts/scouter-go.sh`, 두 빌드의 효율을 쌍으로 비교하는 `scripts/bench-ab.sh`.
- **기타**: servlet IP 허용 목록(opt-in, `--scouter-http-check-allow-ips`), 콘솔 `debug queue`, SIGQUIT goroutine dump(서버는 계속 실행).

### 명시적 미지원

- JVM 스크립트·plugin 실행: `.alert` 규칙 컴파일·실행, `.plug` 스크립트, plugin JAR, 확장 handler chain(MapPack·standalone SpanPack 수집 포함). `.alert`/`.conf` 파일의 저장·조회·descriptor는 지원합니다.
- webapp의 extweb·swagger·WebSocket `/basic`·webroot·`/v0/temp/test/*`.
- `--user/--password` 단일 계정 모드의 raw(secureLogin=false) login. 계정 파일 모드는 Java처럼 동작합니다.

미지원 기능은 빈 성공 응답으로 대체하지 않습니다.

## 빌드와 실행

### 준비

Go 1.27.2를 씁니다. Go가 없으면 `python3 scripts/bootstrap_go.py`(Python 3.12+, curl 필요)가 공식 archive의 SHA-256을 확인해 저장소의 `.tools/go`에만 설치합니다. `Makefile`은 `.tools/go/bin/go`가 있으면 그것을 씁니다.

```sh
python3 scripts/bootstrap_go.py
make check        # gofmt, vet, test, build
make build        # dist/scouter-go, dist/scouter-db, dist/scouter-replay
```

### 로컬 실행

```sh
./dist/scouter-go --db ./data --udp 127.0.0.1:6100 --tcp 127.0.0.1:6100 --http 127.0.0.1:6180
```

- 설정: `--conf`(기본 `./conf/scouter.conf`, 없으면 flag만 사용)로 Java `scouter.conf`를 읽고 3초마다 리로드합니다. 우선순위는 flag > `scouter.conf` > 기본값입니다([키별 동작](docs/compatibility/config-behavior.md)).
- 계정: 기본은 conf 디렉터리(또는 `--account-dir`)의 `account.xml`/`account_group.xml`입니다. `--account-dir` 없이 `--user`/`--password`를 주거나 `--conf ""`로 conf를 끄면 그 계정 하나만 쓰는 단일 계정 모드입니다(기본 `admin/admin`).
- 운영 HTTP: `--http`를 줄 때만 `GET /health`와 `/debug/pprof`를 엽니다. loopback이 아닌 주소면 기동 시 WARNING을 남깁니다. Java servlet(`net_http_server_enabled`나 `enabled=true`인 Telegraf XML, 기본 `0.0.0.0:6180`)과 같은 포트·겹치는 host를 쓰면 두 listener와 해결 방법을 로그에 남기고 기동하지 않습니다.
- 전체 flag는 `scouter-go -h`로 확인합니다.

### Linux 서버 배포

```sh
make build-linux                     # dist/linux-amd64/ (LINUX_ARCH=arm64 가능), SHA256SUMS 포함
scripts/deploy.sh [--arch arm64] [--no-build] [--restart] [--dry-run] <대상 디렉터리 | [user@]host:디렉터리>
```

- 바이너리는 cgo 없는 정적 실행 파일이라 대상 서버에 Go나 공유 라이브러리가 필요 없습니다.
- `deploy.sh`는 `scouter-go`·`scouter-db`·`scouter-replay`·`SHA256SUMS`·`scouter-go.sh`를 임시 이름으로 복사하고 대상에서 checksum을 확인한 뒤 교체합니다. 대상의 `database/`·`logs/`·`conf/`·`scouter-go.env`는 건드리지 않습니다.
- 대상 서버에서는 `scouter-go.sh {start|stop|restart|status|args}`로 운영합니다(POSIX sh). 설정은 `SCOUTER_*` 환경변수나 같은 디렉터리의 `scouter-go.env`로 바꿉니다. 변수 목록은 [스크립트 머리말](scripts/scouter-go.sh)에 있습니다.
  - Java 기동 스크립트처럼 스크립트 디렉터리에서 서버를 실행하므로 `conf/`·`database/`·`logs/`가 그 아래에 있고, `<pid>.scouter` 종료 파일도 거기에 생깁니다(Java `stop.sh`의 `rm -f *.scouter`로도 멈춤). `conf/scouter.conf`가 없으면 flag 기본값으로 기동합니다(`SCOUTER_CONF`·`SCOUTER_CONF_DIR`로 다른 경로를 정했다면 그 파일이 있어야 합니다).
  - 스크립트는 listener·DB·log·plugin 경로를 flag로 넘기므로 `scouter.conf`의 `net_*_listen_*`·`db_dir`·`log_dir`·`plugin_dir`보다 `SCOUTER_UDP`·`SCOUTER_TCP`·`SCOUTER_DB` 등이 우선합니다. `args`로 실제 인자를 확인합니다.
  - `/health`는 기본 `127.0.0.1:6180`입니다. `SCOUTER_HTTP`를 정하지 않았고 Java servlet이 같은 6180을 쓰면(`scouter.conf`의 `net_http_server_enabled=true`나 최상위 `<enabled>`가 true인 `conf/scouter-telegraf.xml`, `net_http_port` 기본값) `127.0.0.1:6181`로 옮기고 그 사실을 로그에 남깁니다. 스크립트는 평범한 `key=value` 줄만 읽으므로 `${...}` 치환 등으로 겹치면 서버가 위 오류로 기동을 거부합니다. 이때는 `SCOUTER_HTTP`를 정합니다. `SCOUTER_HTTP=""`는 `/health`를 끄고, `SCOUTER_CONF=""`는 conf 파일 없이(계정 파일도 쓰지 않음) 기동합니다.
  - `stop`은 SIGTERM을 보내고 `SCOUTER_STOP_TIMEOUT`(기본 30초) 뒤에 SIGKILL합니다. 이 값을 설정하면 서버의 `--shutdown-timeout`을 그보다 10초 짧게(최소 5초) 함께 넘깁니다. 정수 초가 아니면(예: `30s`) 스크립트가 실행을 거부합니다.

### 종료

SIGINT·SIGTERM·SIGHUP·종료 파일 삭제·콘솔 `quit`은 모두 graceful 종료입니다. 수락한 데이터를 저장하고 fsync한 뒤 store를 닫으며, 오류 없이 끝나면 exit 0이고 DB에 `.go-clean`을 남깁니다. writer 종료 대기 상한을 넘겨 남은 record가 있으면 exit 0이어도 `.go-clean`을 쓰지 않습니다. `--shutdown-timeout`(기본 20초)을 넘으면 멈춘 단계와 남은 queue, marker 상태(`marker=written|absent`)를 한 줄로 남기고 exit 3으로 끝납니다. marker가 없으면 다음 기동은 crash checkpoint로 index를 검증합니다. 첫 신호보다 1초 넘게 늦은 SIGINT/SIGTERM은 marker 없이 즉시 종료합니다(exit 128+신호 번호). SIGQUIT은 goroutine dump만 쓰고 서버는 계속 실행합니다. 확인 절차는 [runbook](docs/operations/cutover-runbook.md) §9에 있습니다.

## 운영 전환

| 문서 | 내용 |
|---|---|
| [전환 runbook](docs/operations/cutover-runbook.md) | 사전 확인, 이관 리허설, Agent 전환 동작, 전환 방식(canary·일괄), rollback, 중단 기준, 종료 절차 |
| [설정 산정](docs/operations/sizing.md) | 연결·수신·CPU·메모리·디스크 설정을 정하는 방법, Go 전용 상한 키 |
| [감시 기준](docs/operations/monitoring.md) | `/health`·stats 줄·종료 로그 항목, 손실·포화 징후, 경보 기준 |
| [이관 계약](docs/compatibility/migration.md) | `scouter-db` inspect·migrate·verify·rollback과 증분 이관 |

`scouter-db`는 원본 DB에 쓰지 않습니다. 이관은 검증된 바이트 복사이며 Java와 Go가 같은 DB에 동시에 쓰지 않도록 `lock.dat`와 `.go-writer.lock`을 확인합니다. migrate와 `--finish`는 Java가 멈춘 지 60초가 지났거나 `--java-stopped`를 줄 때만 진행합니다. 사용법은 `scouter-db help`로 봅니다.

## 검증

표준 명령의 실제 결과로 완료를 판정합니다. 도구나 원본이 없어 skip된 검사는 통과가 아닙니다.

| 명령 | 내용 | 필요한 것 |
|---|---|---|
| `make check` | gofmt, vet, 전체 test, build | Go |
| `make race` | 전체 test를 `-race`로. test binary당 상한 30분(`RACE_TIMEOUT`) | Go |
| `make fuzz` | codec·transport·index·설정·GeoIP·LZ4 등 fuzz target별 10초(`FUZZTIME`) | Go |
| `make bench` | protocol codec microbenchmark. 서버 TPS가 아님 | Go |
| `make storage-check` | Java/Scala가 쓴 DB와 Go가 쓴 DB의 양방향 읽기 | JDK 11+, 고정 원본, Scala compiler용 JDK 8(`SCOUTER_SCALA_JAVA=<jdk8>/bin/java`, macOS는 설치된 JDK 8을 찾음) |
| `make fixtures-check`, `make inventory-check` | Java oracle fixture 재생성 비교, 원본 handler inventory 비교 | JDK 11+, 고정 원본(`SCOUTER_SOURCE`, 기본 `../scouter`) |
| `make java-integration` | 원본 Client 통신 클래스로 56개 명령 통합 실행 | JDK, 고정 원본 |
| `make management-oracle-check` 등 | 기능별 Java oracle 비교(`geoip-`, `ingest-`, `sqltable-`, `logger-oracle-check`). Pulse golden은 비공개 원본에서 만든 고정 자료라 재생성 명령이 없습니다 | 각 script가 요구하는 JDK·원본 |
| `python3 scripts/java-agent-interop.py` | 실제 Java Agent·Host Agent 상호운용(약 6분 반) | JDK 8 또는 11, 원본, Agent jar([사용법](tools/java-agent-interop/README.md)) |
| `make replay REPLAY_ARGS=...` | loopback 부하·재시작 검증 report(`dist/replay-report.json`) | Go |
| `scripts/bench-ab.sh --base BIN --new BIN` | 두 빌드의 완료 XLog 1,000건당 CPU를 쌍으로 비교 | bash, Python 3 |

기본 검증은 `GOMAXPROCS=2`로 실행합니다. GitHub Actions([ci.yml](.github/workflows/ci.yml))는 pull request와 `master`·`codex/**` branch push에서 `check`, `race`, `fuzz`(5초), `storage-check`, `java-integration`, `inventory-check`, `fixtures-check`를 실행합니다. 커밋된 golden과 Go 결과의 대조는 `make check`에 포함되고, `*-check` 명령은 golden 자체를 원본에서 다시 만들어 비교합니다. 원본 저장소는 수정하지 않으며 javac 출력은 임시 경로에 씁니다. 측정 방법과 처리량 정의는 [성능 기준](docs/performance.md)을 따릅니다.

## 문서 지도

- [문서 색인](docs/README.md): 설계·계약·검증·운영 문서 전체 목록
- [AGENTS.md](AGENTS.md): 구현·호환성·성능·검증 규칙
- [CONTRIBUTING.md](CONTRIBUTING.md): branch, toolchain, commit, release gate
- [ONBOARDING.md](ONBOARDING.md): 처음 참여할 때 읽을 순서
- 패키지 계약 `internal/*/CONTRACT.md`: 구현 범위와 원본과의 의도된 차이

## 라이선스

[Apache-2.0](LICENSE). 원본 protocol 출처는 [NOTICE](NOTICE)에 기록합니다. synthetic fixture에 운영 데이터는 사용하지 않습니다.

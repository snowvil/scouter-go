# 운영 전환 runbook

Java Scouter Server를 Go 서버로 바꿀 때의 순서, 확인 지점, 중단·rollback 기준이다. 운영 규모, 호스트 사양, 주소는 이 문서에 적지 않는다. 그 값은 전환 담당자가 [sizing](sizing.md)으로 정해 별도 작업 기록에 남긴다. 감시 항목과 경보 기준은 [monitoring](monitoring.md)에 있다.

## 1. 원칙

- **같은 DB에 Java와 Go를 동시에 쓰지 않는다.**
  - Go는 기동할 때 `lock.dat`(Java `DBCtr` 규칙)과 `.go-writer.lock`을 확인한다.
  - `scouter-db migrate`/`rollback`도 둘을 확인한다([migration](../compatibility/migration.md)).
- **원본 Java DB에는 쓰지 않는다.**
  - 이관은 검증된 바이트 복사다.
  - Java 원본 DB는 rollback 기준이므로 전환이 끝날 때까지 그대로 보존한다.
- **Agent 쪽에는 Scouter Agent conf 외의 변경을 요구하지 않는다.** Agent 동작은 아래 §4의 원본 근거대로다.
- **손실 구간을 미리 정하고 기록한다.**
  - Java Agent는 서버가 없는 동안 UDP 데이터를 버퍼링하지 않는다.
  - 서버를 바꾸는 동안 들어온 XLog·counter는 받는 서버가 없으면 사라진다.
- **단계마다 중단 기준(§7)을 정하고, 넘으면 rollback(§6)한다.**

## 2. 사전 확인

### 2.1 원본 설정과 사용 기능

| 항목 | 확인 방법 | Go에서 달라지는 점 |
|---|---|---|
| alert script(`.alert`), `.plug`, plugin JAR | 서버 `plugin_dir`, `ext_plugin_*` 키 | Go는 JVM plugin과 임의 script를 실행하지 않는다. 내장 Pulse만 지원한다 |
| `mgr_purge_enabled`와 보관 일수 키 | 서버 `scouter.conf` | Go retention은 이 키(또는 `--purge`)를 따른다. 키가 없을 때 Java는 on, Go는 off다(의도된 차이). 원본과 같은 보관을 원하면 `mgr_purge_enabled=true`를 명시한다 |
| Pulse 애플리케이션 이름 필터 `ext_plugin_pulse_exclude_applications`, `ext_plugin_pulse_rebase_transaction_applications`, `ext_plugin_pulse_non_spring_based_applications` | 운영 Java 서버 `scouter.conf`, Go가 읽을 `scouter.conf` 또는 `--pulse-conf` 파일 | 원본 plugin은 키가 없으면 특정 배포의 이름을 기본값으로 쓰지만 Go 기본값은 빈 값이다(제외·rebase 없음, 모든 application에 Spring URI filter 적용). 운영 Java conf에 세 키가 없으면 지금 Java가 쓰는 plugin 기본값(원본 plugin 설정의 값)을 비공개 conf에 옮긴다. 의도적으로 비우려면 `key=`로 명시한다(결과는 키 생략과 같지만 기동 경고 `WARNING: pulse: <key> is not set`이 없다. non-Spring 키는 `ext_plugin_pulse_enable_spring_uri_filter=false`이면 경고하지 않는다). `--pulse-conf`를 쓰면 그 파일만 읽고 `scouter.conf`의 Pulse 키와 병합하지 않는다 |
| `net_tcp_client_so_timeout_ms` | 서버 `scouter.conf` | 30000 이하이면 30초까지 쉰 연결을 재사용하는 Client API 소비자의 세션이 유휴 중에 끊긴다 |
| `net_tcp_service_pool_size` | 서버 `scouter.conf` | 동시 Client 세션 수다. `--max-connections`(Client 연결 상한)보다 크면 경고한다 |
| 알 수 없는 키 | Go 기동 로그 `config: unknown key ...` | 무시하고 로그만 남긴다. 오타·폐기 키를 확인한다 |
| Agent 종류 | Client의 object 목록(objType) | Java Agent(V2 TCP), Host Agent(V1 TCP), Batch Agent, kube 모드의 전환 동작이 다르다(§4) |
| Client 버전 분포 | Client 로그인 기록(request log) | 같은 프로토콜을 쓴다. 오래된 Client는 스테이징에서 먼저 확인한다 |

### 2.2 호스트

| 항목 | 확인 |
|---|---|
| open file 한도 | `ulimit -n`(hard). Go는 기동할 때 이 한도에 맞춰 TCP 연결 예산을 낮추고 경고를 남긴다(`WARNING: the open file limit ...`). 경고가 나오면 한도를 올리고 다시 기동한다 |
| UDP 수신 버퍼 | `net.core.rmem_max`. 너무 작으면 커널에서 UDP가 버려진다. Go가 실제로 받은 크기는 `/health`에서 본다([monitoring](monitoring.md)) |
| 포트 | UDP·TCP 수집 포트, servlet(`net_http_port`), 운영 HTTP(`--http`). Java와 같은 포트를 쓰려면 Java를 멈춘 뒤에만 기동한다 |
| 디스크 | 이관 대상 크기 × 1.01 + 여유(`scouter-db migrate --dry-run`이 계산), 그리고 보관 기간의 증가분 |
| 자원 공유 | 다른 서비스와 호스트를 같이 쓰면 `GOMAXPROCS`(`go_maxprocs`)와 `GOMEMLIMIT`를 정한다([sizing](sizing.md)) |

## 3. 이관 리허설 (운영 DB 복사본)

이관은 운영 DB **복사본**에서 먼저 끝까지 해 본다. 시간과 공간을 재고, 결과를 판정한다.

1. Java를 멈추지 않고 DB 디렉터리 전체를 복사본으로 만든다(파일 시스템 snapshot 또는 복사). 진행 중인 오늘 파일은 불완전할 수 있다. 리허설의 목적은 형식·시간·공간 확인이다.
2. `scouter-db inspect --db <복사본> --timezone <원본 JVM timezone> --report inspect.json`
   - `error`가 있으면 원인을 분류한다. torn tail과 gzip count 뒤처짐은 `--accept-recoverable` 대상이고, 그 밖의 error는 이관을 막는다.
3. `scouter-db migrate --src <복사본> --dst <새 Go DB> --dry-run`으로 공간을 확인한다.
4. `scouter-db migrate --src <복사본> --dst <새 Go DB> --report migrate.json`
   - 소요 시간을 기록한다. 실제 전환의 정지 시간이 이 시간에 비례한다(§5 방식 B).
5. Go 서버를 `<새 Go DB>`로 Agent가 송신하지 않는 별도 UDP·TCP 포트에 기동하고 Client로 과거 날짜를 조회한다. 다른 포트로 기동해도 수집 listener는 열리므로 리허설 Agent를 연결하지 않는다.
   - 대상: XLog 목록·profile·counter 과거값·summary·alert·object 목록
   - 같은 날짜를 Java 서버로 조회한 결과와 비교한다.
6. Go를 정지하고 `scouter-db verify --src <복사본> --dst <새 Go DB>`를 실행한다. Go가 쓴 변경은 `post_migration.changes`로 분류된다.
7. `scouter-db rollback --dst <새 Go DB>`로 rollback 절차도 확인한다.

**판정**
- inspect의 거부 오류가 0이고, 조회 비교에서 차이가 없거나 문서화된 의도된 차이뿐이어야 한다.
- 이관 시간이 허용 정지 시간 안이어야 한다.

## 4. Agent 전환 동작 (원본 근거)

| Agent | 서버 주소를 바꾸면 | 근거 |
|---|---|---|
| Java Agent | conf를 3초마다 다시 읽는다. UDP 대상은 즉시 바뀐다. TCP(V2) 세션은 다음 서버 요청(KEEP_ALIVE)이 끝난 뒤 주소 변경을 보고 닫히며, 새 주소로 다시 연결한다. 애플리케이션 재기동이 필요 없다 | `scouter.agent.java` `Configure.run`(3초 reload), `DataUdpAgent`(`ConfObserver`), `TcpWorker.processV2`(주소·포트가 같은 동안만 유지) |
| Host Agent | UDP는 즉시 바뀐다. TCP(V1) 세션은 주소 변경을 보지 않아 기존 서버와의 연결이 끊길 때까지 남는다 | `scouter.agent.host` `TcpWorker.process`(objHash만 확인) |
| Batch Agent | 실행할 때마다 conf를 읽는다 | `scouter.agent.batch` `Configure` |
| 공통 | 연결 실패 시 약 1초+3초 간격으로 재시도한다. 서버가 같은 주소·포트로 다시 뜨면 conf 변경 없이 다시 붙는다(연결 규모 시험: 재시작 뒤 약 5초 안에 전원 재연결) | `TcpRequestMgr`, [connection scale](../validation/2026-10-22-connection-scale.md) |

Host Agent의 TCP 세션은 서버 → Agent 요청(host top, 디스크 정보 등)에만 쓰인다. 수집(UDP)은 주소를 바꾸면 바로 새 서버로 간다. 기존 Java 서버를 멈추면 Host Agent의 TCP 세션도 새 서버로 옮겨 간다.

## 5. 전환 방식

### 방식 A: 새 DB로 병행 기동 후 Agent 단계 이전 (canary)

1. Go 서버를 Java와 **다른 포트·다른 DB**로 기동한다. 과거 데이터는 Java에 남는다.
2. 일부 Agent(영향이 적은 서비스부터)의 `net_collector_ip`/`net_collector_udp_port`/`net_collector_tcp_port`를 Go로 바꾼다.
3. 확인 기간 동안 [monitoring](monitoring.md)의 항목을 본다. Client로 두 서버를 각각 조회해 같은 Agent 군의 값이 맞는지 본다.
4. 문제가 없으면 Agent를 단계적으로 옮긴다. 마지막에 Java를 멈춘다.

- 장점: 손실 구간이 conf 반영 시간(수 초)뿐이다. 문제가 생기면 conf를 되돌려 Agent별로 즉시 rollback할 수 있다.
- 단점: 전환 기간과 그 뒤에도 과거 데이터는 Java DB에 있다. 과거 조회는 Java 서버(조회 전용으로 남김)나 이관된 DB로 해야 한다. Client가 두 서버를 함께 봐야 하는 기간이 생긴다.

### 방식 B: 이관한 DB로 일괄 전환

1. 리허설(§3)로 이관 시간과 `--finish` 시간을 안다.
2. **사전 복사**(Java 실행 중): `scouter-db migrate --precopy --src <Java DB> --dst <Go DB>`. 전환 직전까지 여러 번 실행하면 바뀐 파일만 다시 복사해 차이가 줄어든다. 원본은 읽기만 하지만 Java와 같은 디스크를 읽으므로 I/O가 적은 시간에 실행한다.
3. 사전 복사 반복 작업(cron 등)을 멈춘다. Java를 멈추고 **Java 프로세스가 끝났는지(PID 소멸) 확인한다.** Java는 종료하면서 데이터를 flush하므로 `lock.dat`이 갱신되지 않는다는 것만으로는 쓰기가 끝났다고 볼 수 없다. **손실 구간이 시작된다.**
4. **마무리**: `scouter-db migrate --finish --src <Java DB> --dst <Go DB> --report migrate.json`. 사전 복사 뒤 바뀐 파일만 복사·검사하고 나머지는 재사용한다. `lock.dat`가 남아 있고 60초가 지나지 않았으면 거부한다. PID 소멸을 확인했다면 `--java-stopped`로 진행한다. 같은 dst에 다른 이관 명령이 돌고 있으면 거부한다.
5. Go를 Java와 **같은 포트**로 기동한다. Agent는 conf 변경 없이 재연결한다(§4). **손실 구간이 끝난다.**
6. `--finish`는 바뀌지 않은 파일을 재hash하지 않고 파일 상태(크기·시각·inode)로 판단한다. 손실 구간 밖에서 `scouter-db verify --src <Java DB> --dst <Go DB>`로 원본 전체를 재hash해 확인한다([migration](../compatibility/migration.md) "증분 이관").

- 장점: 과거와 현재 데이터가 한 DB에 있고, Agent conf를 바꾸지 않는다. 손실 구간은 마지막 사전 복사 이후 바뀐 파일(주로 오늘 날짜와 영구 text)의 크기에 비례한다(synthetic 4.27GB 측정: 전체 이관 98초 → `--finish` 10초).
- 단점: 정지 창이 필요하다. 사전 복사 없이 `scouter-db migrate`만 쓰면 손실 구간이 전체 이관 시간이다.

### 선택 기준

- 허용 손실 구간이 `--finish` 시간보다 짧으면 방식 A로 Agent를 옮기고, 과거 데이터는 이관한 DB를 조회 전용 Go 서버로 연다.
- 정지 창이 허용되면 방식 B가 가장 단순하다.
- 어느 쪽이든 Pulse(Kafka) 소비자는 같은 topic·key·header로 받는다. 병행 기간에는 Java Pulse plugin과 Go 내장 Pulse가 **같은 topic에 중복 발행하지 않게** 한쪽만 켠다.

## 6. Rollback

| 상황 | 절차 | 데이터 |
|---|---|---|
| 방식 A, Agent 일부 이전 중 | 해당 Agent conf를 Java로 되돌린다(Java Agent는 수 초 안에 반영) | Go가 받은 기간의 데이터는 Go DB에만 있다 |
| 방식 B, 전환 직후 | Go를 멈추고 `scouter-db rollback --dst <Go DB>`(원본 불변을 증명하고 Go DB를 옆으로 옮김) → Java를 원래 DB로 기동 | Go가 받은 데이터는 Java DB에 없다(옆으로 옮긴 Go DB에 남음) |
| Go가 쓴 DB를 Java로 계속 사용 | Go를 멈추고 `scouter-db inspect`. `zip.torn_member`, `index.unreachable_records`, `index.relink_refused`가 있으면 Java를 기동하지 않는다([migration](../compatibility/migration.md) "verify와 rollback") | Go가 원본 형식으로 쓰므로 Java가 읽을 수 있다(양방향 oracle 검증, 운영 규모 미검증) |

rollback 뒤에는 원인 분석을 위해 Go 로그(`server-*.log`, stats 줄), `/health` 기록, 그리고 옆으로 옮긴 Go DB를 보존한다.

## 7. 중단 기준

전환 중 아래 중 하나라도 해당하면 진행을 멈추고 원인을 확인한다. 회복되지 않으면 rollback한다. 임계값은 [monitoring](monitoring.md)을 따른다.

- 수집 손실이 계속된다: `udp.Rejected`, `ingest.Rejected`, XLog·profile queue `Dropped`, writer `Dropped`가 증가한다.
- 저장 오류가 생긴다: `StorageErrors`, `WriteErrors`, `maintenance.errors`, `textWriteErrors`.
- Agent 연결이 불안정하다: `tcp.AgentConnections`가 예상 Agent 수보다 계속 적거나, 거절 로그(`refused`)나 `agentOrphans.Rejected`가 증가한다.
- Client 조회에서 Java와 다른 결과가 나오는데, 문서화된 의도된 차이가 아니다.
- 메모리·CPU가 정한 예산([sizing](sizing.md))을 계속 넘는다.

## 8. 전환 기록

전환마다 다음을 남긴다(운영 기록, 공개 저장소 밖).

- 전환 방식과 손실 구간(시작·끝 시각)
- 이관 report(`migrate.json`, `verify` 결과)
- 기동 설정(flag, `scouter.conf`, 환경 변수, open file 한도)
- 전환 전후 [monitoring](monitoring.md) 항목의 값
- 발견한 차이와 처리

## 9. Go 서버 종료

전환·rollback·재기동에서 Go 서버를 멈출 때의 절차다([runtime](../compatibility/runtime.md) "종료 상한").

1. `scripts/scouter-go.sh stop`(SIGTERM) 또는 `<pid>.scouter` 삭제로 멈춘다. SIGTERM에는 `shutdown: SIGTERM received; graceful shutdown within 20s`, 종료 파일 삭제에는 `was deleted; shutting down`이 포함된 로그가 남는다(20초는 서버 기본 상한).
2. 정상 종료는 exit 0이다. 마지막 줄들(`ingest=…`, `ingest writers closed`, `storage writers closed`, `logger=`)에서 남은 queue(`Pending`, `Len`)가 0인지, `Abandoned`·`ShutdownDiscarded`가 0인지 확인하고 DB root의 `.go-clean`도 확인한다. context를 보지 않는 ingest writer가 쓰기에 남아 있으면 exit 0이어도 경고와 함께 marker를 생략한다. 이때 다음 기동은 crash checkpoint로 index를 검증하므로 exit code만으로 drain 완료를 판단하지 않는다.
3. 20초(`--shutdown-timeout`) 안에 끝나지 않으면 서버가 스스로 exit 3으로 끝낸다. `shutdown: budget … exceeded` 줄의 `stage`가 멈춘 단계(예: `store/sync`, `background`)다. 나머지 필드는 아직 끝나지 않은 Client 명령·고아 relay 수와 남은 queue다(필드 목록은 [monitoring](monitoring.md) §1 "종료 로그"). 같은 줄의 `marker=absent`이면 `.go-clean`이 없으므로 **같은 DB로 재기동하면 crash checkpoint로 index 꼬리를 검증·복구**한다. 재기동 후 `/health`의 `indexOpens.*.CleanMarker=false`, `Checkpointed` 증가, `Failed`·`CheckpointRejected` 0을 확인한다([monitoring](monitoring.md) §5). 멈춘 단계가 `store`·`counters`·`background`(주기 sync)이면 디스크 상태(I/O 오류, 가득 참, NFS 응답)를 먼저 확인한다.
4. 급히 끝내야 하면 첫 신호 1초 뒤에 SIGTERM을 한 번 더 보낸다(exit 143. store가 이미 닫힌 뒤가 아니면 marker 없음). SIGKILL과 같은 복구 경로이지만 어느 단계였는지 한 줄을 남긴다. 1초 안에 반복된 신호와 SIGHUP은 무시한다. 종료 파일 삭제나 콘솔 `quit` 뒤 처음 보낸 SIGTERM은 graceful 종료를 그대로 둔다.
5. 스크립트의 SIGKILL(기본 30초, `SCOUTER_STOP_TIMEOUT`)은 서버의 상한과 보고 줄 쓰기(최대 2초)마저 끝나지 않을 때만 쓰인다.
   - `SCOUTER_STOP_TIMEOUT`을 설정하면 `scouter-go.sh`가 `--shutdown-timeout`을 `max(SCOUTER_STOP_TIMEOUT − 10, 5)`초로 함께 넘긴다(예: 30 → 20s, 20 → 10s, 12 이하 → 5s). 10초는 보고 줄 쓰기(최대 2초)와 stop 루프의 1초 간격 확인을 넉넉히 덮는 여유다. 5초 하한은 정상 종료(보통 1초 안팎)와 ingest writer 상한(5초)이 상한 안에 들게 하는 최소값이다.
   - 설정하지 않으면 flag를 넘기지 않고 서버 기본값 20초를 쓴다. 스크립트 기본 30초와 같은 결과다.
   - `SCOUTER_EXTRA_ARGS`에 `--shutdown-timeout`(또는 `-shutdown-timeout`, `=` 형식 포함)이 있으면 그 값을 그대로 쓰고 연동하지 않는다. `SCOUTER_STOP_TIMEOUT`이 정수 초가 아니면(예: `30s`) 스크립트는 어떤 명령도 실행하지 않고 exit 2로 거부한다. 그대로 두면 stop이 SIGKILL을 보내지 못하기 때문이다. 앞의 0은 무시한다(`08`은 8초).
   - `SCOUTER_STOP_TIMEOUT`이 7초 이하이면 5초 상한과 보고 줄 쓰기가 SIGKILL보다 늦을 수 있다. 그때는 보고 줄 없이 SIGKILL로 끝나고, 재기동은 같은 crash checkpoint 경로다.
   - 실제로 넘긴 값은 `scouter-go.sh args`와 기동 로그의 `scouter-go.sh start:` 줄에서 확인한다.
6. Java로 rollback하기 전에는 Go 종료가 exit 0이 아니었더라도 §6의 `scouter-db inspect`를 거친다.

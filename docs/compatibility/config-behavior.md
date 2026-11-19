# scouter.conf 키별 실제 동작 계약 (Java ↔ Go)

정본은 `baseline.json`에 고정된 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. Go 측 근거는 3C 잔여 기능 작업 시점의 코드다. 파일 파싱·`${}` 치환·LIST/SET 계약은 [`internal/config/CONTRACT.md`](../../internal/config/CONTRACT.md)와 [management-commands.md](management-commands.md) §3에 있다. 이 문서는 **각 키가 서버 동작을 언제, 어떻게 바꾸는지**만 다룬다.

근거 경로 약어: `C:`=`scouter.server/src/main/java/scouter/server/Configure.java`, `J/`=`scouter.server/src/main/java/scouter/server/`, `S/`=`scouter.server/src/main/scala/scouter/server/`, `CM/`=`scouter.common/src/main/java/scouter/`, `W/`=`scouter.webapp/src/main/java/scouterx/webapp/`. Go 경로는 저장소 루트 기준이다.

원본 동작은 모두 **코드 판독 결과**다. 실행 재현을 한 항목은 따로 "실행 확인"이라고 적었다.

## 0. 분류 기준

**반영 시점(Java)**

| 표기 | 의미 |
|---|---|
| 기동 | 객체(Scala `object`/static) 초기화 때 한 번 읽는다. reload해도 바뀌지 않는다. |
| 연결/파일 open | 새 연결 accept, 새 날짜 파일 open처럼 새 자원을 만들 때 읽는다. 기존 자원은 이전 값을 유지한다. |
| ConfObserver | `applyConfig` 끝에서 `ConfObserver.exec()`(`C:853`)가 등록된 Runnable을 실행해 반영한다(§2). |
| 사용 시마다 | 요청·패킷·주기 loop마다 `Configure.getInstance().<field>`를 읽는다. 다음 reload(최대 3초) 뒤 바로 반영된다. |
| 미사용 | 서버 코드에 소비처가 없다. 값만 LIST에 보인다. |

**상태(Go)**

| 상태 | 의미 |
|---|---|
| 일치 | 반영 시점까지 Java와 같다. |
| 부분 | 값은 쓰지만 반영 시점(예: Java 즉시, Go 재기동)이나 기본값이 다르다. |
| 표시 전용 | Java는 이 값으로 동작을 바꾸지만 Go는 Snapshot·LIST_CONFIGURE_SERVER에 값만 보이고 고정값이나 기본값으로 동작한다. |
| 미구현 | 이 키가 켜고 끄는 기능 자체가 Go에 없다. |
| 구조 차이 | Java 내부 구조(stage queue, thread pool, gzip block cache)를 조절하는 키다. Go에는 그 구조가 없다. 대응을 만들지 않는 것이 기본이다. |
| 제외 | 프로젝트 결정으로 범위에서 뺐다(JVM plugin·script 실행, extweb, swagger). |

공통 사실(Go): `config.Store`가 3초마다 파일을 다시 읽고 `Current()` Snapshot을 바꾼다. 바뀐 Snapshot은 `cmd/scouter-go`의 `OnChange`가 `Runtime.ApplySettings(Snapshot.Live())`로 넘기고, `internal/server/settings.go` `wireSettings`와 `wiring.go` `applyLive`가 ingest·agent pool·service·transport·storage·retention·logger·webapp·account LDAP·SqlTables·debug 출력 소비자에 원자 저장으로 반영한다(GeoIP 파일 재로드만 owner goroutine). 소비자별 API와 반영 시점은 [config CONTRACT](../../internal/config/CONTRACT.md) §5.3이다. 재기동 키는 바뀌면 `restart required` 로그만 남기고, 명시한 flag가 있는 키는 flag 값으로 고정된다(§5.2). scouter.conf 없이 실행하면 reload가 없으므로 retention·logger·webapp·LDAP은 기동 값으로 동작한다. 기동 때 한 번만 읽고 연결하지 않은 키와 그 이유는 §5.1a 끝에 모았다.

## 1. 키별 표

열: 키 | Java 기본값(field 초기값, `applyConfig` 기본값이 다르면 함께) | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작(근거) | 상태 | Go 패키지.

### 1.1 서버 식별·로그

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `server_id` | host 이름(`C:139`) | `S/netio/service/handle/LoginService.scala:63,127` | 사용 시마다 | LOGIN 응답 `server_id`를 요청마다 conf에서 읽는다 | live. reload마다 `service.Service.SetSettings`로 다음 LOGIN `server_id`에 반영한다. `--server-id`를 주면 flag 고정, 둘 다 없으면 host 이름(`internal/server/wiring.go` `applyLive`) | 일치 | service |
| `log_test_rate` | 0 | `S/core/ProfilePreCore.scala:107,141,150,159` | 사용 시마다 | 0보다 크면 profile 처리 경로마다 `rnd.nextInt(100) < rate`인 경우 `"<size>\t[ProfileCoreCase…] Profile2 svcName=…"`을 Logger에 쓴다. 개발용 "TODO remove logging"이다 | live. `ingest.Config.ProfileTestLog` → `Logger.Println(ProfileTestLine)`(ProfilePreCore 분기마다 `rand.IntN(100) < rate`). 서비스명은 TextCache 대신 저장소에서 읽고 100ms를 넘으면 `null`로 쓴다(`wiring.go` `logProfileTest`) | 일치(서비스명 조회 상한은 의도된 차이) | ingest |
| `log_tcp_action_enabled` | false | `S/netio/service/net/TcpServiceWorker.scala:77-148`, `TcpAgentWorker.scala:110` | 사용 시마다 | `Agent : <addr> open [<objHash32>] #<n>`, `Client : <addr> open #<n>`, `Unknown : <addr> drop`, `Client : <addr> closed [<예외> workers=<n>]`(NPE·timeout은 stack trace 추가), `Agent : <addr> close`. ID 없는 `Logger.println(msg)`라 `mgr_log_ignore_ids`로 막을 수 없다 | live. `TCPServer.SetActionLog` → logger `Println`. 문구는 Java와 같고 `Agent … close`는 연결당 1회, stack trace는 쓰지 않는다([transport CONTRACT](../../internal/transport/CONTRACT.md) §4·§5) | 일치(문구 외 차이는 의도된 차이) | transport, server |
| `log_udp_multipacket` | false | `S/netio/data/NetDataProcessor.scala:101-109` | 사용 시마다 | 분할 패킷 조립 완료 때 Logger `[S160] recv <Pack class> total=<n> object=(<objHash>)<objName> /<ip>` | live. `UDPReceiver.SetLogUDPMultipacket` → logger `PrintlnID("S160", …)`. objName은 `Engine.ObjectNameCached` | 일치 | transport |
| `log_expired_multipacket` | **true** | `S/netio/data/MultiPacketProcessor.scala:76-78`, `MultiPacket.scala:56-64` | 사용 시마다 | 1초마다 1초 지난 미완성 조립을 버리고 Logger `[S150] MultiPacket total=<n> recv=<m> object=(<objHash>)<objName> /<ip>`(같은 ID 10초 1회) | live. `UDPReceiver.SetLogExpiredMultipacket` → `S150`(transport가 10초 1회 적용). `udp.expired` 통계도 유지 | 일치 | transport |
| `log_udp_packet`, `log_udp_counter`, `log_udp_interaction_counter`, `log_udp_xlog`, `log_udp_profile`, `log_udp_text`, `log_udp_alert`, `log_udp_object`, `log_udp_status`, `log_udp_stack`, `log_udp_summary`, `log_udp_batch`, `log_udp_span` | 모두 false | `S/netio/data/NetDataProcessor.scala:133-228`(형식은 §1.8) | 사용 시마다 | 해당 Pack을 처리한 **뒤** `System.out`(Logger 파일이 아님)에 한 줄 쓴다. `log_udp_batch`는 추가로 `J/netio/req/net/ReqCommandFactory.java:9`, `TcpSendStack.java:20`, `J/db/BatchZipDB.java:20`에서 Logger에 쓴다 | live. `ingest.Config.PackLog` → `logger.UDPLog`/`FormatPack` → `Logger.Stdout`. `log_udp_packet`은 dispatch 전, 나머지는 처리 뒤. `sql`·`login` Text는 마스킹한다(D-S1). `log_udp_batch`의 Logger 추가 줄(`ReqCommandFactory`·`TcpSendStack`·`BatchZipDB`)은 없다 | 부분(D-S1 마스킹은 의도된 차이, batch 추가 줄 미구현) | ingest |
| `log_udp_some_object` | `""` | `J/CounterManager.java:283-316` | 사용 시마다 | ObjectPack objType이 값과 같으면 object type 자동 등록 과정을 `System.out`에 `DEBUG objType: …`(detected, will be added, new XML) 형식으로 쓴다 | live. `ingest.Config.ObjectTypeLog`와 runtime `addObjectTypeIfNotExist`가 `DEBUG objType:` 줄을 stdout에 쓴다. `new XML =` 본문은 Go가 저장한 counters.site.xml 원문이다([config CONTRACT](../../internal/config/CONTRACT.md) §8) | 일치(XML 본문 차이) | ingest |
| `log_service_handler_list` | false | `J/netio/service/ServiceHandlingProxy.java:83-85` | 기동 | handler 등록 때 `ServiceHandler <cmd>=><Invocation>`을 Logger에 쓴다 | 없음. 값만 보인다 | 미구현(P3) | service |
| `log_index_traversal_warning_count` | 100 | `S/db/io/IndexKeyFile.scala:95-96`, `IndexKeyFile2.scala:85,121,155` | 사용 시마다 | hash chain 탐색이 값보다 길면 `[S152] [warn] Too many index deep searching. <key>`(KV는 `S161`, 10초 1회) | live. `Store`/`Counters.SetIndexTraversalWarningCount`, 줄은 `Config.Log`/`Counters.SetLog` → logger `PrintlnID`(S152/S161, storage가 10초 간격 적용) | 일치 | storage |
| `log_rotation_enabled` | true | `S/Logger.scala:157-178,186` | ConfObserver + 파일 open | true면 `log_dir/server-yyyyMMdd.log`에 append한다. false면 `log_dir/server.log`를 `new PrintWriter(File)`로 열어 **open마다 내용을 지운다**(§4 D-L1) | live. `logger.Logger.SetConfig`. true면 `server-yyyyMMdd.log` append, false면 `server.log`에 **append**한다(D-L1, [logger CONTRACT](../../internal/logger/CONTRACT.md)) | 일치(D-L1은 의도된 차이) | server |
| `log_keep_days` | 31 | `S/Logger.scala:185-231`, `J/http/HttpServer.java:68` | 사용 시마다(Logger), 기동(Jetty) | Logger thread가 1시간마다(첫 실행은 기동 1시간 뒤) `log_dir`의 `server-yyyyMMdd.*`·`request-yyyyMMdd.*` 중 `오늘 - 파일날짜 > keep_days`를 지운다. rotation=false거나 값 ≤0이면 지우지 않는다. Jetty `http-request-yyyy_mm_dd.log` retainDays는 기동 값 | live(server/request log 정리, 1시간 주기, `--log-keep-days`가 고정). servlet listener의 HTTP request log 보관은 기동 값(`internal/httpapi`) | 일치 | server |
| `log_sql_parsing_fail_enabled` | false | `S/core/SqlTables.scala:115-117` | 사용 시마다 | SQL table 파싱 실패 때 Logger `[S112] <원문 SQL>\n<예외>`. **원문 SQL을 로그에 남긴다** | live. SqlTables 실패 때 `S112`를 쓰되 원문 SQL 대신 hash·길이만 남긴다(`internal/sqltable`, D-S1) | 일치(원문 미기록은 의도된 차이) | ingest |
| `_trace` | false | `S/Logger.scala:39-43,111-123`, `S/core/app/SpanStepBuilder.scala:81-86`, `S/core/SqlTables.scala:83`(읽기만 함), plugin builtin 2곳(제외) | 사용 시마다 | Logger.trace를 켜고, 모든 Logger 출력을 stdout에도 쓴다. zipkin span→step 변환 상세를 Logger에 쓴다 | live. `Logger.SetConfig`로 server log 줄을 stdout에도 쓴다. `SpanStepBuilder`의 zipkin 변환 상세 trace 줄은 없다 | 부분 | server, ingest |
| `log_dir` | `./logs` | `S/Logger.scala:157-191`, `S/netio/service/handle/ServerInfo.scala:87,110`, `J/logs/RequestLogger.java:38`, `J/http/HttpServer.java:59,66` | ConfObserver(Logger), 사용 시마다(SERVER_LOG_*), 기동(RequestLogger는 static final, Jetty) | Logger writer를 닫고 새 경로로 다시 연다. SERVER_LOG_LIST/DETAIL은 요청마다 conf를 읽는다. `request-*.log`는 **기동 때 경로**에 계속 쓴다 | live(`--log-dir`가 고정). logger가 다음 줄부터 새 디렉터리에 쓰고(이전 파일은 owner가 닫음) `request-*.log`도 따라간다(D-L2 의도된 차이). SERVER_LOG_* root는 `FileMetadata.SetLogDir`. servlet HTTP request log 경로는 기동 값 | 일치(D-L2는 의도된 차이) | server, service |
| `mgr_log_ignore_ids` | `{}`(StringSet, `,` 구분) | `S/Logger.scala:90-92` | 사용 시마다 | `Logger.println(id, …)` 형태의 ID 로그 중 집합에 든 ID를 버린다. ID 없는 로그·`System.out`은 막지 못한다 | live. `Logger.SetConfig`의 `IgnoreIDs`로 ID 있는 줄만 거른다. ID 없는 줄과 Go 표준 `log` 줄은 막지 못한다(Java도 ID 없는 줄은 막지 못함) | 일치 | server |

### 1.2 네트워크(UDP·TCP)

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `net_udp_listen_ip`, `net_udp_listen_port` | `0.0.0.0`, 6100 | `S/netio/data/net/DataUdpServer.scala:35-41,62-83` | 기동(수신 예외 뒤 재open 때 현재 값 사용) | DatagramSocket bind. bind 실패면 3초마다 재시도한다 | 기동 때 `--udp`>conf(`main.go:258`). conf에 없으면 Go 기본 `127.0.0.1:6100`(Java와 다름, CONTRACT §6) | 일치(기본값 차이는 기존 결정) | transport |
| `net_tcp_listen_ip`, `net_tcp_listen_port` | `0.0.0.0`, 6100 | `S/netio/service/net/TcpServer.scala:35-55` | 기동 | ServerSocket(backlog 50) | 기동 때 `--tcp`>conf(`main.go:259`) | 일치(기본값 차이는 기존 결정) | transport |
| `net_tcp_client_so_timeout_ms` | 8000 | `TcpServer.scala:37,43`, `LoginService.scala:80` | 연결 accept, 사용 시마다(LOGIN) | **accept한 모든 socket**에 SO_TIMEOUT을 건다(Agent socket은 handshake 뒤 덮어씀). Client 연결은 명령 사이 8초 무입력이면 `SocketTimeoutException`으로 닫힌다. read마다 다시 계산하고 write에는 timeout이 없다. LOGIN 응답 `so_time_out`에 값을 넣는다 | live. `TCPServer.SetClientSoTimeout`: **이후 accept한 Client 연결**의 read마다 deadline(명령 사이 대기 포함, 기본 8초 무입력이면 종료). 이전 Go의 고정 `IdleTimeout` 60초는 Client 연결에 쓰지 않는다(stack upload·Agent handshake 대기에만 남음). handshake 5초와 명령 총 deadline `RequestTimeout` 30초는 Go 안전장치로 유지한다. Client service pool 대기 상한도 이 값이다. LOGIN `so_time_out`은 `service.Settings`로 같은 live 값을 보낸다(고정 8000이 아님). webapp pool timeout 키가 없으면 이 값을 기본으로 쓴다 | 일치(총 deadline·handshake 5초는 의도된 차이) | transport, service |
| `net_tcp_agent_so_timeout_ms` | 60000 | `S/netio/service/net/TcpAgentWorker.scala:18` | 연결(Agent 등록 때) | Agent TCP 연결의 read timeout. 요청 응답 read가 60초 막히면 연결을 닫는다. keepalive 동안만 3000으로 바꿨다가 되돌린다(`:84-104`) | live. `TCPServer.SetAgentSoTimeout`: 이후 등록한 Agent 연결의 응답 read마다 deadline. 요청 전체는 `min(ctx, RequestTimeout)` | 일치(총 deadline은 의도된 차이) | transport |
| `net_tcp_agent_keepalive_interval_ms` | 5000 | `TcpAgentWorker.scala:83`, `TcpAgentManager.scala:30-56` | 사용 시마다 | 5초 주기 manager가 **마지막 write 후** 값 이상 지난 session에 `KEEP_ALIVE`를 보내고 3000ms 안에 응답을 읽는다. 실패하면 닫는다 | live. agent pool이 마지막 write 뒤 이 값이 지난 idle 연결에 `KEEP_ALIVE`를 보내고 3초 응답을 기다린다(`internal/server/agent_checkout.go`) | 일치 | server |
| `net_tcp_get_agent_connection_wait_ms` | 1000 | `TcpAgentManager.scala:69-72` | 사용 시마다 | objHash별 session queue(최대 50)에서 대기 시간 안에 idle 연결을 꺼낸다. 없으면 null(Agent 요청 실패) | live. agent pool checkout 대기 시간. objHash당 연결 상한 8은 Go 자원 상한이다 | 일치(연결 상한은 의도된 차이) | server |
| `net_udp_packet_buffer_size` | 65535 | `DataUdpServer.scala:45-47,66` | 기동(수신 loop 재시작 때) | 수신 buffer 크기. 더 큰 datagram은 **잘린 채** 처리되어 parse 오류(`S159`)가 된다 | 기동 때 파일에 키가 있을 때만 `UDPConfig.MaxDatagramBytes`로 쓴다. 65507 초과·1 미만은 65507(로그). `MSG_TRUNC` datagram은 parse하지 않고 거절한다. 재기동 키 | 일치(clamp·잘린 datagram 거절은 의도된 차이) | transport |
| `net_udp_so_rcvbuf_size` | 4194304(AIX/HP-UX 0) | `DataUdpServer.scala:67,72-75` | 기동 | 값 > 0이면 `setReceiveBufferSize` | 파일에 있을 때만 기동 때 적용(`main.go:260-262`; runtime이 `transport.UDPConfig.ReadBuffer`로 넘기고 transport가 설정, 실패하면 기동 실패. 요청·실효 크기는 `/health` `udp.ReadBufferRequested`/`ReadBufferEffective`). 없으면 OS 기본(CONTRACT §6) | 일치(기본값 차이는 기존 결정) | transport |
| `_net_udp_worker_thread_count` | 3 | `S/netio/data/NetDataProcessor.scala:40-52` | 기동 | 수신 queue(고정 2048, 넘치면 `S158`)를 소비하는 decode thread 수 | 기동 때 `--udp-workers`가 없고 파일에 키가 있을 때만 worker 수로 쓴다. 1 미만은 3, 256 초과는 256(로그). 키가 없으면 Go 기본 2(의도된 차이: Go worker는 engine lock 하나로 직렬화되어, Java처럼 3개로 늘리면 lock 전달만 늘어난다). UDP queue는 `--udp-queue`(기본 8192, payload 32MiB 상한)로 Java 고정 2048과 다르다. Java는 그 뒤에 stage queue(`xlog_queue_size` 등)가 더 있다. 재기동 키 | 일치(키가 없을 때 기본값 차이, 의도) | transport, server |
| `net_tcp_service_pool_size` | 100 | `TcpServer.scala:27-33` | 기동 + ConfObserver | Client 연결 하나가 thread 하나를 점유한다(Agent 연결은 handshake 뒤 반환). 초과 연결은 **무제한 queue에서 대기**한다(`CM/util/ThreadUtil.java:177-199`). reload 때 `setCorePoolSize`로 바꾸며 JDK에 따라 결과가 다르다(§2, 실행 확인) | live. `TCPServer.SetServicePoolSize`: handshake 뒤 **Client 세션만** FIFO permit을 기다린다(Agent 연결은 대기하지 않음). 증가·감소 즉시 반영(JDK 8 의미, D-N1). 대기는 Client so-timeout(0이면 `RequestTimeout`)과 대기자 수 `max(1, --max-connections/2)`로 제한하고(`--max-connections`는 2026-10-21부터 Client 연결 상한이며 Agent는 `--max-agent-connections`로 따로 센다), 대기 중 끊긴 연결의 명령은 실행하지 않는다(D-N2). 1 미만은 거절, `--max-connections`보다 크면 경고([transport CONTRACT](../../internal/transport/CONTRACT.md) §3) | 일치(§5.1 T5 결정 (b), 대기 상한은 의도된 차이) | transport |

### 1.3 HTTP·webapp

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `net_http_server_enabled`, `net_http_port` | false, 6180 | `J/http/HttpServer.java:61-62` | 기동 | Jetty를 `0.0.0.0:<port>`에 연다(Jetty 9.4.6 기본: thread 200, 대기 queue·연결 수 상한 없음) | 기동 때 `--scouter-http`>conf(`main.go:263-269`). Telegraf XML enabled=true도 켠다(`main.go:319-325`). Go는 연결 수(1024, 7/8 이상이면 keep-alive 회전)·servlet 동시 64·webapp 동시 64(각 3초 대기)·`/health`+pprof 동시 16 상한을 두고 넘치면 503(`internal/server/http_admission.go`, [runtime](runtime.md) 운영자 안내). 상한은 Go 전용 `go_http_*` 키로 바꾼다(재기동, [config CONTRACT](../../internal/config/CONTRACT.md) §6) | 일치(admission 상한은 의도된 차이) | server |
| `net_http_api_enabled` | false | `HttpServer.java:79,112` | 기동 | webapp(`/scouter/*`) 탑재 | 기동 때 `--webapp`>conf | 일치 | server |
| `net_http_api_gzip_enabled` | true | `HttpServer.java:100`, `W/main/WebAppMain.java:97` | 기동 | GzipHandler | 기동(servlet `--scouter-http-gzip`, webapp) | 일치 | server |
| `net_http_api_session_timeout` | 86400 | `W/main/WebAppMain.java:141`, `W/layer/service/UserTokenService.java:49-50` | 기동 | session·token 만료 | 기동(webapp session·token 만료) | 일치 | webapp |
| `net_http_api_cors_allow_origin`, `net_http_api_cors_allow_credentials` | `*`, `true` | `W/framework/filter/CorsFilter.java:43-44` | 사용 시마다(요청) | CORS header | live. `webapp.Server.SetSettings`, 다음 요청부터 | 일치 | webapp |
| `net_http_api_auth_ip_enabled`, `net_http_api_auth_ip_header_key`, `net_http_api_auth_session_enabled`, `net_http_api_auth_bearer_token_enabled` | false, null(applyConfig `""`), false, false | `W/framework/filter/AuthFilter.java:56-83`, `W/framework/util/ZZ.java:41-42` | 사용 시마다(요청) | 인증 방식·IP 판정 | webapp: live(`webapp.Server.SetSettings`). servlet listener는 `net_http_api_auth_ip_header_key`를 기동 때 한 번 읽는다(Go 전용 opt-in 허용 목록 검사) | 일치(webapp). servlet 쪽은 기동 값 | webapp |
| `net_http_api_allow_ips` | `localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1` | `C:701-707`(reload마다 `allowIpExact/Match` 재계산), webapp AuthFilter | 사용 시마다 | allow list | webapp: live. servlet listener 허용 목록 검사는 Go 전용 opt-in(`--scouter-http-check-allow-ips`)이며 기동 값이다 | 일치(webapp). servlet 쪽은 기동 값 | webapp, server |
| `net_webapp_tcp_client_pool_size` | 30(applyConfig 12) | `W/framework/client/server/Server.java:73` | 기동(서버 연결 생성) | webapp→collector 연결 pool 크기 | 기동, 기본 12(`internal/webapp`) | 일치 | webapp |
| `net_webapp_tcp_client_pool_timeout` | 60000(applyConfig 기본은 **현재 `net_tcp_client_so_timeout_ms`**, 기본 8000) | `W/framework/client/net/ConnectionPool.java:49,56` | 사용 시마다 | idle 연결 폐기 기준 | live. 키가 없으면 현재 `net_tcp_client_so_timeout_ms`(기본 8000)를 쓴다. 0 이하는 idle 연결을 매번 폐기한다(Java 비교식 `lastUsed < now-(timeout-1)`과 같은 결과). idle pool에서 꺼낸 연결이 응답 byte 전에 EOF/reset이면 새 연결로 1회 다시 보낸다. 요청 쓰기가 실패한 경우는 항상, 그 밖에는 부작용 없는 명령만(SET_/SAVE_ 계열, `OBJECT_REMOVE_INACTIVE`, `OBJECT_THREAD_CONTROL`, `OBJECT_HEAPHISTO` 제외) | 일치 | webapp |
| `net_webapp_tcp_client_so_timeout` | 30000 | applyConfig가 읽지 않는다(StandAlone 전용) | 미적용 | 항상 30000 | 값만 보인다 | 일치 | — |
| `net_http_extweb_dir`, `net_http_api_swagger_enabled`, `net_http_api_swagger_host_ip` | `./extweb`, false, `""` | `W/main/WebAppMain.java:146` 등 | 기동 | extweb·swagger | 값만 보인다 | 제외 | — |

### 1.4 디렉터리·object

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `db_dir` | `./database` | `S/db/DBCtr.scala:30`, `S/tagcnt/core/CountEnv.scala:40,48` | 사용 시마다(새 파일 open) | 이미 연 writer·index는 이전 경로를 계속 쓰고 새로 여는 파일만 새 경로로 간다. 실행 중 변경은 DB를 두 곳으로 쪼갠다(원본 취약 동작 D-C1) | 기동 고정, 재기동 키(`main.go:239`, `keys.go:160`) | 일치(재기동 유지가 의도된 차이) | storage |
| `plugin_dir` | `./plugin` | `J/plugin/alert/AlertRuleLoader.java:70,102,118,128`, `J/plugin/PlugInLoader.java:68` | 사용 시마다 | alert rule·plugin script 로드 root | 기동 고정, 재기동 키. alert script 파일 저장·조회 root로만 쓴다 | 부분(실행은 제외) | server |
| `plugin_enabled` | true | 서버 소비처 없음 | 미사용 | 효과 없음 | 값만 보인다 | 일치 | — |
| `client_dir`, `temp_dir` | `./client`, `./tempdata` | 서버 소비처 없음. `temp_dir`은 webapp adaptor getter만 있고 호출처가 없다(`W/framework/configure/EmbeddedConfigureAdaptor.java:116-117`) | 미사용 | 효과 없음 | 값만 보인다 | 일치 | — |
| `object_deadtime_ms` | 8000 | `S/core/AgentManager.scala:48-72` | 사용 시마다(1초 loop) | `now > wakeup + deadtime`이면 inactive 처리(알림 포함). ObjectPack tag `deadTime`이 0이 아니면 그 값. objType `batch`는 제외 | live. ingest 1초 판정·`Objects()` alive·`RemoveInactiveObjects`에서 읽는다. `deadTime` tag가 0이 아니면 그 값, `batch`는 제외([ingest CONTRACT](../../internal/ingest/CONTRACT.md) §6·§7) | 일치 | ingest |
| `object_zipkin_deadtime_ms` | 180000(applyConfig 300000) | `AgentManager.scala:51,59-60` | 사용 시마다 | objType이 `zipkin`이거나 `z$`로 시작하면(`CM/lang/counters/CounterConstants.java:53-54`) 이 값을 dead time으로 쓴다 | live. objType이 `zipkin`이거나 `z$`로 시작하면 이 값(applyConfig 기본 300000) | 일치 | ingest |
| `object_inactive_alert_level` | 0 | `AgentManager.scala:162-182` | 사용 시마다 | inactive 전환 때(sub object가 아닌 type, `:205-217`) `INACTIVE_OBJECT`, primary object 재활성 때(`:112-114`) `ACTIVATED_OBJECT` AlertPack(objType `scouter`)을 이 level로 만든다 | live. `INACTIVE_OBJECT`·`ACTIVATED_OBJECT` alert 생성 때 읽는다(ingest CONTRACT §6) | 일치 | ingest |

### 1.5 저장(압축·retention·text·index)

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `compress_xlog_enabled`, `compress_profile_enabled` | false, false | `S/db/xlog/XLogDataWriter.scala:53-63`, `XLogProfileDataWriter.scala:54-63`, reader 2곳 | 파일 open(날짜별) | 날짜 디렉터리에 `.service.conf`/`.profile.conf`가 있으면 그 값, 없으면 현재 conf 값으로 정하고 파일에 기록한다. reload 뒤 **새 날짜**부터 바뀐다 | live(`--compress-*`가 고정). `Store.SetCompression`: `.conf`가 없는 날짜의 첫 쓰기부터 반영하고 기존 날짜 `.conf`가 우선한다 | 일치 | storage |
| `_compress_write_buffer_block_count`, `_compress_read_cache_block_count`, `_compress_read_cache_expired_ms`, `_compress_dailycount_header_cache_size`, `_compress_write_thread` | 3, 3, 60000, 3, 2 | `J/db/io/zip/GZipStore.java:100`, `J/db/io/zip/IOChannel.java:41-46,63,95,115-129,146,159` | 사용 시마다 / ConfObserver(read cache 행 수, write thread pool 교체) | gzip block 쓰기 buffer·읽기 cache·비동기 쓰기 thread | `_compress_read_cache_block_count`(해제 block cache 수)와 `_compress_dailycount_header_cache_size`(날짜별 writer 위치 수)는 live(`Store.SetCompressReadCacheBlocks`/`SetCompressDailyCountCacheSize`, Go 상한 8/64). `_compress_write_buffer_block_count`·`_compress_read_cache_expired_ms`·`_compress_write_thread`는 대응 구조가 없어 연결하지 않는다(Go는 record마다 gzip member를 바로 쓰고 비동기 thread·시간 만료 cache가 없다, [storage CONTRACT](../../internal/storage/CONTRACT.md) §5) | 부분(2개 일치, 3개 구조 차이) | — |
| `mgr_purge_enabled` | true | `J/core/AutoDeleteScheduler.java:65-123` | ConfObserver + 사용 시마다(1분 loop) | §2.1 | live(`--purge`가 고정). `Retention.Apply`가 다음 cycle부터 적용. **키가 없으면 Go 기본 off**(의도된 차이). scouter.conf가 없으면 기동 값 | 일치(기본 off는 의도된 차이) | storage, server |
| `mgr_purge_disk_usage_pct`, `mgr_purge_profile_keep_days`, `mgr_purge_keep_days`, `mgr_purge_xlog_keep_days`, `mgr_purge_xlog_without_profile_keep_days`, `mgr_purge_counter_keep_days`, `mgr_purge_realtime_counter_keep_days`, `mgr_purge_tag_counter_keep_days`, `mgr_purge_visitor_counter_keep_days`, `mgr_purge_daily_text_days`, `mgr_purge_sum_data_days` | 80, 10, 10, 30, 30, 70, 70, 70, 70, 140, 60(파생식 `C:772-792`) | `AutoDeleteScheduler.java:65-103,132-336` | ConfObserver + 사용 시마다 | §2.1. 파생 기본값은 reload마다 다시 계산한다 | live(`--mgr-purge-*`가 고정). `storage.JavaRetentionConfig`로 파생식을 reload마다 다시 계산해 `Retention.Apply`. Java 삭제 이력 clear는 Go가 이력을 두지 않아 대응이 필요 없다(원본 `AutoDeleteScheduler` oracle로 결과 파일 집합 일치) | 일치 | storage, server |
| `mgr_text_db_daily_service_enabled`, `mgr_text_db_daily_api_enabled`, `mgr_text_db_daily_ua_enabled` | false ×3 | `S/db/TextPermWR.scala:37-47`(`isA`), 호출처 `S/db/TextWR.scala:93-109`, `S/db/TextRD.scala:28-53` | 사용 시마다(**쓰기와 읽기 모두**) | §3 | live. `Store.SetTextDaily`: 쓰기·읽기마다 경로를 고르고 일별 miss는 영구 사전으로 fallback한다. D-T1은 Java와 같게 둔다(storage CONTRACT §2, 원본 `TextWR`/`TextRD` oracle) | 일치 | storage, server |
| `_mgr_text_db_index_default_mb`, `_mgr_text_db_index_service_mb`, `_mgr_text_db_index_api_mb`, `_mgr_text_db_index_ua_mb`, `_mgr_text_db_index_login_mb`, `_mgr_text_db_index_desc_mb`, `_mgr_text_db_index_hmsg_mb` | 1 ×7 | `S/db/text/TextPermIndex.scala:32-38,104-110` | 기동(object val) + 파일 생성 | `00000000/text/text_<div>.hfile`을 **새로 만들 때** hash 영역 크기(MB). 기존 파일은 파일 길이를 따른다 | 기동 때 `storage.Config.IndexSizes`로 새 `.hfile` hash 영역 크기를 정한다(0..64 MiB). 재기동 키 | 일치 | storage |
| `_mgr_text_db_daily_index_mb` | 1 | `S/db/text/TextTable.scala:30,89-90` | 기동 + 파일 생성 | `<date>/text/text.hfile` 생성 크기 | 기동 `IndexSizes.TextDaily`, 재기동 키 | 일치 | storage |
| `_mgr_kv_store_index_default_mb` | **8** | `S/db/kv/KeyValueStoreIndex.scala:28,93-94` | 기동 + 파일 생성 | KV store hfile 생성 크기 | 기동 `IndexSizes.KV`, 키가 없으면 Java와 같은 8 MiB. 재기동 키 | 일치 | storage |
| `_mgr_xlog_id_index_mb` | 1 | `S/db/xlog/XLogIndex.scala:62,72` | 파일 open(날짜별 XLogIndex 생성) | txid/gxid index 생성 크기. 다음 날짜부터 반영 | live. `Store.SetXLogIDIndexMB`: 이후 새로 만드는 `xlog_tid`/`xlog_gid`부터 | 일치 | storage |

### 1.6 수집·조회

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `_auto_5m_sampling` | true | `S/core/Auto5MSampling.scala:41-62` | 사용 시마다(5분 경계 tick) | false면 5분 표본 저장을 건너뛴다(누적 map은 계속 채움) | live. 5분 tick마다 읽고 false면 쓰지도 비우지도 않는다 | 일치 | ingest, server |
| `xlog_queue_size` | 10000 | `S/core/XLogPreCore.scala:32`, `XLogCore.scala:38`, `S/db/XLogWR.scala:39` | 기동 | stage queue 상한. 넘치면 버리고 `S116-0`/`S116`/`S144` | 연결됨(재기동 키): 기동 Snapshot의 값(`internal/config/live.go:197`, 1 미만은 10000, 상한 2^20)을 `internal/server/runtime.go:347`이 `storage.Config.XLogQueueSize`로 넘겨 `XLogWR` queue 상한이 된다(포화 drop은 `/health` `xlogQueue.Dropped`, `S144`; `internal/storage/CONTRACT.md` §8.3). 실행 중 변경은 `restart required` 로그만 남긴다. XLogPreCore·XLogCore 단계 queue는 없고 UDP worker가 engine에서 메모리 처리 후 XLog writer에 넘긴다. 앞단 상한은 UDP datagram queue(`--udp-queue`, 기본 8192, payload 32MiB) | 일치(writer queue), 단계 queue는 구조 차이 | config, server, storage |
| `profile_queue_size` | 1000 | `ProfilePreCore.scala:40`, `ProfileCore.scala:34`, `XLogProfileWR.scala:37` | 기동 | 같은 방식(`S110*`, `S142`) | 연결됨(재기동 키): `xlog_queue_size`와 같은 경로(`live.go:198`, 1 미만은 1000)로 `storage.Config.ProfileQueueSize` → `XLogProfileWR` queue 상한(`/health` `profileQueue`, `S142`; `internal/storage/CONTRACT.md` §8.3). ProfilePreCore·ProfileCore 단계 queue는 없다 | 일치(writer queue), 단계 queue는 구조 차이 | config, server, storage |
| `span_queue_size` | 1000 | `S/core/SpanCore.scala:35`, `S/db/ZipkinSpanWR.scala:40` | 기동, **applyConfig가 읽지 않아 항상 1000** | — | 값만 보인다(Java도 applyConfig가 읽지 않음). span 저장은 ingest span writer queue(개수 1000 고정 = Java 실효값, byte 상한 16 MiB, [ingest CONTRACT](../../internal/ingest/CONTRACT.md) §13)이고 SpanCore 단계 queue는 없다 | 일치(실효값) | — |
| `xlog_realtime_lower_bound_ms` | 0 | `S/netio/service/handle/XLogService.scala:176-177` | 사용 시마다 | `TRANX_REAL_TIME_GROUP`의 `limit`(최소 elapsed)을 `max(conf, limit)`로 올린다 | live. `service.Settings`: `TRANX_REAL_TIME_GROUP` limit = `max(conf, limit)` | 일치 | service |
| `xlog_pasttime_lower_bound_ms` | 0 | `XLogService.scala:265-266` | 사용 시마다 | `TRANX_LOAD_TIME_GROUP`의 `limit`에 같은 규칙 | live. `TRANX_LOAD_TIME_GROUP` limit에 같은 규칙 | 일치 | service |
| `req_search_xlog_max_count` | 500 | `XLogService.scala:472-476`(`XLOG_LOAD_BY_TXIDS`), `:602-605`(`SEARCH_XLOG_LIST`) | 사용 시마다 | TXIDS는 증가 **후** `>=` 비교라 최대 `값-1`건을 돌려준다. SEARCH는 `값`건 | live. TXIDS off-by-one 재현, 0 이하면 결과 없음(Java와 같음) | 일치 | service |
| `xlog_sampling_matcher_gxid_keep_memory_count` | 500000 | `J/core/cache/XLogDelayingCache.java:51-53` | 기동(생성자) | 처리된 driving gxid 기억 상한 | 기동 때 한 번(`ingest.Config.SamplingGxidKeepCount`, XLogDelayingCache 생성자 대응) | 일치 | ingest |
| `xlog_sampling_matcher_xlog_keep_memory_count` | 100000 | `XLogDelayingCache.java:136-146` | 새 bucket 생성 때 | 지연 XLog bucket의 gxid 상한 | live. 새 bucket을 만들 때 읽는다. 0 이하는 기본값(의도된 차이: Java는 무제한 map) | 일치 | ingest |
| `xlog_sampling_matcher_xlog_keep_memory_millis` | 5000 | `XLogDelayingCache.java:41,84-91` | 사용 시마다 | 지연 XLog bucket 회전 주기 | live. 지연 XLog마다 회전 단위를 계산한다. 0 이하는 기본값(Java는 0 나눗셈) | 일치 | ingest |
| `xlog_sampling_matcher_profile_keep_memory_count` | 5000 | `J/core/cache/ProfileDelayingCache.java:174` | 새 bucket 생성 때 | 500ms bucket당 profile gxid 상한 | live. 새 bucket을 만들 때 읽는다. 0 이하는 기본값 | 일치 | ingest |
| `xlog_sampling_matcher_profile_keep_memory_secs` | 5 | `ProfileDelayingCache.java:97-101` | 사용 시마다 | 유지 bucket 수 = 값×1000/500 | live. bucket 회전마다 `secs*1000/500`(Java int 연산, 0 이하 결과는 10) | 일치 | ingest |
| `geoip_enabled` | true | `S/core/XLogCore.scala:39-44`, `J/plugin/IXLog.java:42-46`(plugin, 제외) | 사용 시마다 | WEB/APP service XLog에 countryCode·city hash를 채우고, 새 city 이름을 영구 사전 `city`에 기록한다(`S/geoip/GeoIpUtil.scala:21-58`) | live. XLog GeoIP 단계마다 읽는다(WEB/APP service만, [ingest CONTRACT](../../internal/ingest/CONTRACT.md) §3) | 일치 | ingest |
| `geoip_data_city_file` | `./conf/GeoLiteCity.dat` | `S/geoip/GISDataUtil.scala:20-42` | 기동 + ConfObserver | 경로가 바뀌면 `LookupService(MEMORY_CACHE)`로 다시 연다. 파일이 없으면 `S145`, 성공은 `S146`. 이미 채운 `locationCache`(1만 IP)는 비우지 않는다 | 기동 때 열고 없으면 `S145` 후 GeoIP를 끈다. 값이 바뀌면 settings owner goroutine이 다시 연다(reload lock 밖). 실패하면 이전 DB 유지. 새 DB가 열리면 IP cache를 비운다(D-G1 의도된 차이) | 일치(D-G1·재로드 위치는 의도된 차이) | ingest |
| `sql_table_parsing_enabled` | true | `S/core/SqlTables.scala:65-78`, 호출처 `S/core/TextCore.scala:38-41` | 사용 시마다 | §3.4. **dead code가 아니다** | live. `sqltable.Options.Enabled`를 sql TextPack마다 읽는다. 결과는 영구 `text_table`에 저장([sqltable CONTRACT](../../internal/sqltable/CONTRACT.md)) | 일치 | ingest, storage |
| `tagcnt_enabled` | true | `S/core/XLogCore.scala:70-72`, `S/core/AlertCore.scala:41-43`, `LoginService.scala:79` | 사용 시마다 | XLog·Alert tag count 수집을 켜고 끈다. LOGIN `menu.tag_count`에 값을 넣는다 | live. XLog·Alert tag count를 사용 시마다 gate하고 LOGIN `menu.tag_count`도 같은 값(기본 true) | 일치 | ingest, service |
| `visitor_hourly_count_enabled` | true | `S/core/VisitorCore.scala:71-77` | 사용 시마다 | false면 시간대별 방문자 HLL 갱신을 건너뛴다 | live. `Store.SetVisitorHourlyCount`(이전 `Config.VisitorHourlyDisabled`는 삭제) | 일치 | storage, ingest |
| `ext_link_name`, `ext_link_url_pattern` | `scouter-paper`, `http://my-scouter-paper-ip:6188/…` | `LoginService.scala:81-82` | 사용 시마다(LOGIN) | LOGIN 응답에 그대로 넣는다(Client 3rd-party UI 메뉴) | live. LOGIN 응답에 Java 기본값 그대로 넣는다(`service.Settings`) | 일치 | service |

### 1.7 telegraf·LDAP

| 키 | Java 기본값 | Java 소비처 | 반영 시점 | Java 동작 | Go 현재 동작 | 상태 | Go 패키지 |
|---|---|---|---|---|---|---|---|
| `input_telegraf_config_file` | `./conf/scouter-telegraf.xml` | applyConfig가 읽지 않는다 | 미적용 | 항상 기본 경로 | `--telegraf-config` flag(`main.go:36`) | 일치 | — |
| `input_telegraf_enabled`, `input_telegraf_debug_enabled`, `input_telegraf_delta_counter_normalize_default`, `input_telegraf_delta_counter_normalize_default_seconds`, `telegraf_object_deadtime_ms` | true, false, true, 30, 35000 | `J/http/handler/TelegrafInputHandler.java:117,134,147,173`, `J/http/model/InfluxSingleLine.java:317,450-453` | 사용 시마다(XML이 덮음) | 결함 D13 포함 동작은 `http-contract.md` §5.1 | reload 때 `Runtime.ReloadTelegraf`가 다시 합성한다(`main.go:92-96`, `runtime.go:777-793`, `internal/telegraf/config.go:185-189`) | 일치(D13은 의도된 차이) | server |
| `input_telegraf_$measurement$_enabled`, `_debug_enabled`, `_tag_filter`, `_counter_mappings`, `_objFamily_base`, `_objFamily_append_tags`, `_objType_base`, `_objType_prepend_tags`, `_objType_append_tags`, `_objType_icon`, `_objName_base`, `_objName_append_tags`, `_host_tag`, `_host_mappings`(14개 sample 키) | true, false, `""`, `""`, `""`, `""`, `""`, `scouter_obj_type_prefix`, `""`, `""`, `""`, `""`, `host`, `""` | `C:838-851`, 실제 measurement 키는 `applyTelegrafInputConfig`(`C:860-`) | reload | sample 키 자체는 동작 없음. `input_telegraf_$<m>$_*` 실제 키가 measurement map을 만든다 | 위와 같다 | 일치 | server |
| LDAP 비-field 키 `account_use_ldap`, `account_ldap_*` 7개 | — | `AccountManager.authorizeAccount`(management-commands.md §2.3) | 사용 시마다(LOGIN) | LOGIN마다 conf에서 읽는다 | live. 값이 바뀌면 `account.Manager.SetLDAP`, 다음 LOGIN/CHECK_LOGIN부터. scouter.conf가 없으면 기동 값 | 일치 | server, account |

Pulse 키(`ext_plugin_pulse_*`)는 [pulse-contract.md](pulse-contract.md)를 따르며 Go는 재기동 키다(`main.go:209,276-280`).

### 1.8 `log_udp_*` 출력 형식(`System.out`)

모두 처리 **뒤** 한 줄이며 시각·ID 접두가 없다. `DateUtil.timestamp`는 JVM 기본 timezone의 `yyyyMMdd HH:mm:ss.SSS`, `Hexa32.toString32`는 Scouter 32진 문자열이다.

| 키 | 접두 | 본문(`toString`) |
|---|---|---|
| `log_udp_packet` | 없음(type 분기 **전**, 모든 Pack) | 각 Pack의 `toString()` |
| `log_udp_counter` | `DEBUG UDP COUNTER: ` | `PerfCounter <timestamp> <objName> <timetype 이름> <MapValue>`(`CM/lang/pack/PerfCounterPack.java:53-60`) |
| `log_udp_interaction_counter` | `DEBUG UDP INTERACTION COUNTER: ` | `PerfInteractionCounterPack{time=…, objName='…', interactionType='…', fromHash='…', toHash='…', period=…, count=…, errorCount=…, totalElapsed=…, customData=…}` |
| `log_udp_xlog` | `DEBUG UDP XLOG: `(DroppedXLog는 `DEBUG UDP DROPPED XLOG: `) | `XLOG <timestamp(endTime)> objHash=<h32> service=<h32> txid=<h32> caller=<h32> gxid=<h32> elapsed=<n> error=<n>` / `DroppedXLogPack: g:<gxid> t:<txid>` |
| `log_udp_profile` | `DEBUG UDP PROFILE: ` | `Profile <timestamp> objHash=<h32> profile=<bytes>` / `Profile2 <timestamp> objHash=<h32> txid=<h32> gxid=<h32> profile=<bytes>` |
| `log_udp_text` | `DEBUG UDP TEXT: ` | `<xtype> <hash32> <text>`(**원문 SQL 포함**) |
| `log_udp_alert` | `DEBUG UDP ALERT: ` | `ALERT <timestamp> objType=… objHash=<h32> level=… title=… message=… tags=…` |
| `log_udp_object` | `DEBUG UDP OBJECT: ` | `OBJECT  objType=… objHash=<h32> objName=…[ addr=…][ <version>][ alive][ <wakeup Timestamp>][ tags]` |
| `log_udp_status` | `DEBUG UDP STATUS: ` | `Status <timestamp> objType=… objHash=<h32> key=… data=<MapValue>` |
| `log_udp_stack` | `DEBUG UDP STACK: ` | `Stack <timestamp> objHash=<h32> stack=<n>bytes` |
| `log_udp_summary` | `DEBUG UDP SUMMARY: ` | `Summary <timestamp> objHash=<h32> objType=… stype=… <MapValue>` |
| `log_udp_batch` | `DEBUG UDP Batch: ` | `Batch <timestamp(startTime)> objHash=<h32> JobId=… PID=… elapsed=…` |
| `log_udp_span` | `DEBUG UDP SPAN CONTAINER: ` | `SpanContainerPack  gxid=<h32> spanCount=<h32>` |

`TelegrafInputHandler`가 만든 Pack도 `NetDataProcessor.add(pack)`(`NetDataProcessor.scala:63-69`)로 같은 경로를 지나므로 같은 출력이 나온다.

## 2. ConfObserver

`S/ConfObserver.scala:22-47`: 이름별 Runnable을 `StringKeyLinkedMap`(삽입 순서)에 넣는다. `exec()`는 `applyConfig` 끝(`C:853`)에서 순서대로 실행하고 Runnable마다 `Throwable`을 `printStackTrace`로 삼킨다.

- 실행 thread: 3초 주기 Configure thread(`C:569-574`) 또는 `SET_CONFIGURE_SERVER`를 처리한 서비스 thread(`reload(true)`). 둘 다 `synchronized reload` 안이다. Runnable이 오래 걸리면(GeoIP 파일 로드) reload 전체가 그만큼 막힌다.
- 실행 조건: 파일 `lastModified`가 바뀌고 읽을 수 있을 때만 실행한다(`C:638-662`). 값이 같아도 실행하며, 각 Runnable이 이전 값과 직접 비교한다.
- 첫 `applyConfig`는 `Configure` 생성자 안에서 돈다. 그때는 등록된 Runnable이 없다.

### 2.1 등록 5곳(6건)과 순서

등록 순서는 각 객체가 처음 초기화되는 순서다(코드 판독 추정, 실행 미확인). `Main.java:42`의 `Logger.println`이 Logger를 먼저 초기화하고, Logger가 `Configure.getInstance()`(`Logger.scala:153`)를 호출한 직후 등록한다.

| 순서 | 이름 | 등록 위치 | Runnable 동작 |
|---|---|---|---|
| 1 | `Logger` | `S/Logger.scala:154-164` | `log_dir` 또는 `log_rotation_enabled`가 바뀌면 writer를 닫는다. 다음 로그 때 새 경로·방식으로 연다(`:166-183`) |
| 2 | `TcpServer` | `S/netio/service/net/TcpServer.scala:29-33`(`Main.java:54`) | `net_tcp_service_pool_size != corePoolSize`면 `setCorePoolSize`. **실행 확인**(Zulu 8.64 / Zulu 17.36, `ThreadPoolExecutor(2,2,10000,ms,LinkedBlockingQueue)` 동일 구성): JDK 8은 core만 4로 올라 동시 실행 4(max 2보다 큼), JDK 17은 `IllegalArgumentException`으로 그대로 2(예외는 ConfObserver가 삼킴). 줄이기는 두 JDK 모두 반영된다 |
| 3 | `scouter.server.core.AutoDeleteScheduler` | `J/core/AutoDeleteScheduler.java:65-90`(`Main.java:59`) | purge 키 10개 중 하나라도 바뀌면 `applyConf()`로 비교용 사본을 갱신하고 삭제 이력 set 8개를 비운다. 실제 purge loop(1분)는 사본이 아니라 **conf를 직접** 읽으므로 새 값은 다음 loop에 바로 쓰인다. 이력 clear는 이미 지운 날짜를 다시 후보로 만든다 |
| 4 | `_compress_write_thread` | `J/db/io/zip/IOChannel.java:118-129`(static, IOChannel 첫 사용 때) | 값이 바뀌면 새 fixed pool로 교체하고 이전 pool은 `shutdown`(대기 중 작업은 끝까지 실행) |
| 5 | `scouter.server.db.io.zip.IOChannel` | `IOChannel.java:40-46`(생성자) | read cache 행 수를 `_compress_read_cache_block_count`로 바꾼다 |
| 6 | `GISDataUtil` | `S/geoip/GISDataUtil.scala:21-26`(GeoIP 첫 조회 때) | `geoip_data_city_file`이 바뀌면 `load()`로 LookupService를 새로 만든다. `geoip_enabled`는 보지 않는다 |

4~6은 압축 저장·GeoIP를 처음 쓸 때 등록되므로, 압축을 쓰지 않거나 GeoIP 조회가 없으면 등록되지 않는다.

## 3. 일별 text DB(`mgr_text_db_daily_*_enabled`)

### 3.1 쓰기 경로

1. `TextCore`(`S/core/TextCore.scala:34-43`)가 TextPack마다 오늘 날짜(`DateUtil.yyyymmdd()`, JVM 기본 timezone)로 `TextWR.add`를 호출한다. `sql` type이면 먼저 `SqlTables.add`.
2. `TextWR.add`(`S/db/TextWR.scala:112-124`)는 `TextCache`에 넣고 `TextDupCheck`로 (div, date, hash) 중복을 거른 뒤 queue(100000)에 넣는다.
3. `TextWR.process`(`:92-110`)는 **처리 시점에** `TextPermWR.isA(div)`를 평가한다.
   - `isA`(`S/db/TextPermWR.scala:37-47`): `error`→false(항상 일별), `service`/`apicall`/`ua`→각 `!mgr_text_db_daily_*_enabled`, 그 밖(`sql`, `login`, `desc`, `group`, `city`, `table` …)→true(항상 영구).
   - 영구: `TextPermWR.add` → `<db_dir>/00000000/text/text_<div>`의 `.hfile/.kfile`(키 = 4바이트 hash, 값 = data 위치)과 `.data`(`int length` + UTF-8 bytes, `S/db/text/TextPermData.scala:96-104`). 빈 문자열은 버린다(`TextPermWR.scala:71-73`).
   - 일별: `<db_dir>/<yyyymmdd>/text/text`의 `.hfile/.kfile`(`TextWR.scala:142-174`). 키 = `HashUtil.hash(div)` 4바이트 + hash 4바이트, 값 = UTF-8 bytes를 kfile에 직접 저장한다(`S/db/text/TextTable.scala:59-79`). 5분 idle이면 닫는다.

### 3.2 읽기 경로

`TextRD.getString(date, div, hash)`(`S/db/TextRD.scala:28-53`): `TextCache` → `isA(div)`면 영구 사전만 → 아니면 그 날짜 일별 table → **miss면 영구 사전으로 fallback**. `HashText`(GET_TEXT 계열, `S/netio/service/handle/HashText.scala:53,78,109,142`)와 `XLogService` 검색(`:620-632`)이 이 함수를 쓴다.

### 3.3 reload 의미

`isA`는 쓰기·읽기마다 다시 평가한다. 따라서 값을 바꾸면 그 순간부터 저장 위치와 조회 위치가 함께 바뀐다.

- 영구→일별로 바꾸면 과거 영구 항목은 fallback으로 계속 보인다.
- 일별→영구로 바꾸면 그동안 일별에만 쓴 항목은 cache에서 밀려난 뒤 조회되지 않는다(원본 취약 동작 D-T1).
- 같은 hash가 영구 사전에 이미 있으면 일별 모드에서도 일별 table에 다시 쓴다(중복 저장, 정상 동작).

### 3.4 SqlTables(`sql_table_parsing_enabled`)는 실제로 호출된다

- 호출: `TextCore`가 xtype `sql`인 TextPack마다 `SqlTables.add(yyyymmdd, hash, text)`를 부른다(`S/core/TextCore.scala:38-41`).
- 동작(`S/core/SqlTables.scala:35-124`): 꺼져 있으면 즉시 반환한다. 켜져 있으면 queue1(500, 원문 포함)에 넣고, 넘치면 원문을 버린 채 queue2(10000)에 넣는다. 그것도 넘치면 `S111`.
- 전용 thread는 날짜별 처리 hash 집합(최근 2일, 날짜당 1만)과 실패 집합(1만)을 확인한다. 원문이 없으면 `TextRD.getString(date, "sql", hash)`로 읽은 뒤 `new SQLSimpleParser().getCrudInfo(sql)`(`CM/util/SQLSimpleParser.java`) 결과를 `TextWR.add(date, "table", hash, …)`로 저장한다. `table`은 `isA`가 true라 **영구 사전 `text_table`**에 들어간다.
- 예외가 나면 실패 집합에 넣고, `log_sql_parsing_fail_enabled`면 원문을 로그에 남긴다.
- jsqlparser import(`:22-26`)는 쓰지 않는다.
- 결론: dead code가 아니다. Client가 `table` 텍스트를 조회하는 화면 경로는 이번에 조사하지 않았다(§7).

## 4. 원본 결함·취약 동작 후보(정상 계약과 분리)

| ID | 내용 | 근거 | 재현 |
|---|---|---|---|
| D-L1 | `log_rotation_enabled=false`면 `server.log`를 `new PrintWriter(File)`로 열어 **open마다 비운다**. 날짜가 바뀔 때(Logger thread가 날짜 단위 변경 시 close), 재기동 때, ConfObserver가 다시 열 때 이전 내용이 사라진다 | `S/Logger.scala:145-149,174-179` | rotation=false로 기동 → 로그 생성 → 재기동 → `server.log` 앞 내용 소실 확인 |
| D-L2 | `request-yyyyMMdd.log` 경로는 `RequestLogger` class 초기화 때의 `log_dir`로 고정된다. `log_dir`을 바꾸면 server log와 request log가 다른 디렉터리에 남는다 | `J/logs/RequestLogger.java:38` | log_dir 변경 후 `SERVER_DB_DELETE` 실행 → 이전 dir에 기록 |
| D-L3 | Logger 관리 thread는 `startDaemon(name)`(1회 실행 + catch) 안의 `while(true)`라, `clearOldLog`에서 예외(`listFiles()`가 null인 경우 NPE)가 나면 thread가 끝난다. 이후 날짜 전환 close가 멈춰 이전 날짜 파일에 계속 쓴다 | `S/Logger.scala:136-152,191-194`, `S/util/ThreadScala.scala:8-21` | 기동 1시간 시점에 `log_dir`이 없게 만든다(경로를 바꾸고 아직 로그가 없는 경우) |
| D-N1 | `net_tcp_service_pool_size` 증가가 JDK 9+에서 반영되지 않는다(예외를 삼킴). JDK 8에서는 core > max가 된다 | §2.1 순서 2, 실행 확인 | §6 V-TPE |
| D-N2 | Client 연결이 pool thread를 다 쓰면 새 연결은 accept된 채 무제한 queue에서 기다린다. 응답이 없어 Client는 timeout까지 멈춘다 | `TcpServer.scala:28,42-46`, `ThreadUtil.java:177-199` | pool_size=1, Client 2개 연결 |
| D-C1 | 실행 중 `db_dir` 변경은 열린 파일과 새 파일을 다른 root에 둔다 | `S/db/DBCtr.scala:30` | 복사본 DB에서만 재현 |
| D-T1 | 일별→영구 전환 뒤 일별에만 있던 service/api/ua 텍스트가 조회되지 않는다 | §3.3 | 일별 on으로 수집 → off → cache가 빈 새 프로세스에서 GET_TEXT |
| D-G1 | GeoIP 파일을 바꿔도 IP→Location cache(1만)를 비우지 않는다 | `S/geoip/GeoIpUtil.scala:19,38-47` | 파일 교체 전후 같은 IP |
| D-S1 | `log_sql_parsing_fail_enabled`·`log_udp_text`가 원문 SQL을 로그나 stdout에 남긴다 | §1.1, §1.8 | — Go 결정: `log_udp_text`의 `sql`·`login` Text는 `<sql masked len=… hash=…>` 형식으로 가리고, `S112`는 hash·길이만 쓴다([logger CONTRACT](../../internal/logger/CONTRACT.md) D-S1) |

## 5. Go 구현 작업 목록

### 5.0 공통 선행: 설정 변경 전달(F0, P0)

현재 구조: `config.Store`는 `atomic.Pointer`로 불변 Snapshot을 교체한다. `Options.OnChange(Change)`는 reload lock 안에서 동기 호출되며 Reload·SaveText 재호출이 금지된다. main의 callback은 `Runtime.ApplySettings(store.Current().Live())`로 live 설정을 전달하고, Telegraf 소스가 바뀌면 `ReloadTelegraf`도 호출한다(`cmd/scouter-go/main.go` `onChange`). `ApplySettings`는 값 교체·setter 호출·크기 1 channel 신호만 처리하며 I/O는 owner goroutine이 맡는다(`internal/server/settings.go`).

아래는 조사 당시의 설계안이며 1~5는 모두 구현됐다. 현재 함수명·소비처는 [config CONTRACT](../../internal/config/CONTRACT.md) §5.2(F0)·§5.3을 따른다:

1. **typed 설정 묶음**: config에 `Snapshot.Live()`(가칭)를 추가한다. Java `getInt`/`getBoolean` 규칙과 applyConfig 파생 기본값으로 아래 live 키를 typed struct로 돌려준다. `Snapshot.Server()`(`internal/config/snapshot.go:278-330`)를 확장해도 된다.
2. **owner 적용 함수**: `internal/server.Runtime.ApplySettings(live)`(가칭)를 둔다. Open 때 한 번, OnChange 때마다 호출한다. 이 함수는 원자 값 저장과 크기 1의 coalescing channel 신호만 한다. reload lock 안에서 I/O를 하지 않는다. Java ConfObserver는 GeoIP 로드를 lock 안에서 하지만 Go는 owner goroutine에서 한다(의도된 차이).
3. **소비자 경계**: 각 패키지는 setter나 getter 함수만 받는다. 예: `ingest.Engine.SetObjectDeadTimes`, `transport.TCPServer.SetClientTimeout`, `service.Config.Settings func() service.Settings`. config 패키지 의존을 늘리지 않는다(AGENTS.md: 인터페이스는 사용하는 쪽).
4. **flag 고정 규칙**: flag로 명시한 키는 reload가 바꾸지 않는다. 처음 바뀔 때 한 번 로그를 남긴다(`main.go:78-79`의 `explicit`를 재사용).
5. **분류 갱신**: live로 바꾼 키는 `keys.go`의 `.restart()`를 지운다. `effectiveOverlay`(`internal/server/metadata.go:121-160`)가 live 값을 보이게 한다. `internal/config/CONTRACT.md` §5 표를 함께 고친다.

검증: `config` 단위 테스트(임시 conf → `Reload(true)` → 소비자 setter 값 확인), `-race`, flag 고정 테스트. 내부 구조라 Java oracle 대상이 아니다.

### 5.1 패키지별 작업

우선순위: P1 = Client·Agent가 관찰하는 동작이 바뀌거나 운영자가 흔히 바꾸는 키. P2 = 기능 추가가 필요한 키. P3 = 디버그 출력.

| ID | 우선 | 패키지 | 키 | 작업(Java 반영 시점 재현) | 의존 | 검증(Java oracle) |
|---|---|---|---|---|---|---|
| V1 | P1 | service | `server_id`, `ext_link_name`, `ext_link_url_pattern`, `net_tcp_client_so_timeout_ms`(`so_time_out`), `tagcnt_enabled`(`menu.tag_count`) | LOGIN마다 live 값으로 채운다. 기본값 불일치 3건(`ext_link_*` 빈 문자열, `tag_count` false)을 고친다(`service.go:307-308`) | F0 | 가능: 같은 conf로 Java 서버·Go 서버에 LOGIN하고 MapPack을 비교한다(기존 `management_oracle_test.go` 방식). conf 변경 후 3초 대기 → 재LOGIN |
| V2 | P1 | service | `xlog_realtime_lower_bound_ms`, `xlog_pasttime_lower_bound_ms`, `req_search_xlog_max_count` | 요청마다 `max(conf, limit)`. max count는 TXIDS `i+1 >= max`, SEARCH `>= max`(off-by-one 유지) | F0 | 가능: Java/Go에 같은 XLog 적재 후 limit·max 경계값(0, 1, 499, 500, 501) 비교 |
| V3 | P2 | service | `log_dir`(SERVER_LOG_LIST/DETAIL) | 요청마다 live `log_dir`을 읽는다 | F0, L1 | 가능: log_dir 변경 후 목록 비교 |
| T1 | P1 | transport | `net_tcp_client_so_timeout_ms` | Client 명령 사이 idle 읽기 timeout을 이 값으로 한다. Java처럼 **새 연결부터** 적용한다. `RequestTimeout`(30초 총 deadline)은 Go 안전장치로 유지할지 결정 필요(결과: 유지, transport CONTRACT §5) | F0 | 가능: Java·Go에 Client TCP 연결 후 무입력 → 연결 종료 시각 비교(8000ms 기본, 변경값) |
| T2 | P1 | transport | `net_tcp_agent_so_timeout_ms` | Agent 응답 read의 무입력 timeout. 새 Agent 연결부터 적용 | F0 | 부분 가능: Java 서버에 가짜 Agent로 응답 지연 → 연결 종료 시각 |
| T3 | P1 | server | `net_tcp_agent_keepalive_interval_ms`, `net_tcp_get_agent_connection_wait_ms` | `claimIdle` 기준·`checkout` timer를 live 값으로 한다(`agent_checkout.go:16,52`). keepalive 기준을 "마지막 write"로 맞출지 결정. 3000ms 응답 대기는 고정 | F0 | 가능: 가짜 Agent로 KEEP_ALIVE 수신 간격 측정 |
| T4 | P2 | transport, server | `net_udp_packet_buffer_size`, `_net_udp_worker_thread_count` | flag가 없을 때 기동 시 `UDPConfig.MaxDatagramBytes`(≤65507로 clamp)·`Workers`에 반영. 잘린 datagram은 계속 거절한다(의도된 차이) | — | Go 단위 테스트. oracle 불필요 |
| T5 | 결정 | transport | `net_tcp_service_pool_size` | 선택지: (a) 구조 차이로 두고 `--max-connections` 유지, (b) Client 세션 동시 처리 semaphore(대기, 상한 있는 대기열)로 매핑하고 live 조정. Java JDK별 차이(D-N1)는 따르지 않는다. **결과: (b) 채택**, 대기 시간·대기자 수 상한 추가(transport CONTRACT §3) | F0 | (b)면 동시 Client 수 측정. oracle은 JDK에 따라 달라 기준으로 쓰지 않음 |
| T6 | P3 | transport | `log_tcp_action_enabled`, `log_udp_multipacket`, `log_expired_multipacket` | §1.1 문구로 Logger 출력(`S150`은 10초 rate limit) | L1 | 가능: Java 서버 로그 줄과 정규식 비교 |
| I1 | P1 | ingest | `object_deadtime_ms`, `object_zipkin_deadtime_ms`, `object_inactive_alert_level` | dead time을 atomic live 값으로 한다. zipkin/`z$` 구분을 추가한다. `INACTIVE_OBJECT`/`ACTIVATED_OBJECT` alert 생성(1초 판정, primary object 조건, `MAYBE_OBJ_DUP` 포함 여부 확인)을 구현한다 | F0 | 가능: Java·Go에 ObjectPack 송신 후 중단 → alert pack·시각 비교 |
| I2 | P1 | ingest | `tagcnt_enabled` | XLog·Alert tag count 수집을 사용 시마다 gate | F0, V1 | 가능: off 상태 수집 후 TAGCNT 조회 결과 비교 |
| I3 | P1 | ingest, storage | `visitor_hourly_count_enabled` | `VisitorHourlyDisabled`를 live로 바꾸고 연결한다(`store.go:36`, `visitor.go:307`) | F0 | 가능: VISITOR_HOURLY 계열 조회 비교 |
| I4 | P2 | ingest, server | `_auto_5m_sampling` | `Sample5M` 호출 전 live 값 확인(`runtime.go:553-557`) | F0 | 가능: 5분 경계 뒤 FIVE_MIN counter 존재 여부 |
| I5 | P2 | ingest | `xlog_sampling_matcher_*` 5개 | Java 반영 시점 그대로: gxid count는 기동, xlog/profile count는 새 bucket 생성, millis·secs는 사용 시마다(`sampling.go`). cache byte 상한(`cacheDelta`)은 유지 | F0 | Go 단위 테스트 + 기존 sampling oracle 확장 |
| I6 | P2 | ingest(+storage text) | `sql_table_parsing_enabled`, `log_sql_parsing_fail_enabled` | SqlTables 이식: `SQLSimpleParser.getCrudInfo` 포팅, queue1/queue2·처리/실패 집합 상한, `table` 영구 사전 저장. 실패 로그의 원문 SQL 마스킹 여부 결정 필요(D-S1, 결과: hash·길이만 기록) | S1(text 경로) | 가능: `SQLSimpleParser`를 고정 commit에서 직접 실행해 SQL 표본→CRUD 문자열 golden을 만든다(비밀이 없는 합성 SQL만) |
| I7 | P2 | ingest | `geoip_enabled`, `geoip_data_city_file` | XLogCore 조건(WEB/APP service만), city 영구 사전 기록, 경로 변경 시 owner goroutine에서 재로드. 같은 IP cache 무효화는 의도된 개선 후보(D-G1) | F0 | 가능: 같은 GeoLiteCity.dat로 Java/Go XLog countryCode·city 비교 |
| I8 | P3 | ingest | `log_udp_*` 13개, `log_udp_some_object`, `log_test_rate`, `_trace`(span) | §1.8 형식으로 stdout 출력. Java `toString` 이식이 필요하다 | L1(trace), F0 | 가능: scouter.common Pack `toString`을 고정 commit에서 실행해 golden 생성 |
| S1 | P1 | storage, server | `mgr_text_db_daily_*_enabled` | 쓰기·읽기마다 live 값으로 경로 선택, 일별 miss 시 영구 fallback(`TextRD.scala:40-41`). 현재 `storage.TextContext`·`TextCached`에 반영됨 | F0 | `TestJavaStorageInteropConfigTextDaily`가 daily on/off 전환·영구 fallback·D-T1을 대조 |
| S2 | P1 | storage, server | `mgr_purge_enabled`, `mgr_purge_*` 11개 | live 값으로 `RetentionConfig`를 다시 만들고, 바뀌었으면 삭제 이력 set을 비운다. Go 기본 off(키 없음) 결정은 유지 | F0 | 가능: 복사본 DB에서 conf 변경 후 1 cycle 삭제 대상 비교(`tools/java-storage` 확장) |
| S3 | P2 | storage | `compress_xlog_enabled`, `compress_profile_enabled` | 새 날짜 writer open 때 live 값. 기존 날짜 `.conf` 우선은 유지 | F0 | 가능: 날짜 경계 전후 `.service.conf` 내용 비교 |
| S4 | P3 | storage | `_mgr_text_db_index_*_mb`, `_mgr_text_db_daily_index_mb`, `_mgr_kv_store_index_default_mb`, `_mgr_xlog_id_index_mb` | 새 hfile 생성 크기만 반영(기동 값). KV 기본 8 MiB 차이 정리 | — | 가능: 새 파일 크기 비교(Java가 Go 파일을 읽는지 포함) |
| S5 | P3 | storage | `log_index_traversal_warning_count` | legacyindex chain 탐색 길이 초과 경고(`S152`/`S161` 문구) | L1 | Go 단위 테스트 |
| L1 | P1 | server(신규 logger) | `log_dir`, `log_rotation_enabled`, `log_keep_days`, `mgr_log_ignore_ids`, `_trace` | Java Logger 재현: `server-yyyyMMdd.log` append(줄 형식 `yyyyMMdd HH:mm:ss [ID] msg`, ID 없음은 `yyyyMMdd HH:mm:ss msg`), 날짜 전환 reopen, 1시간 주기 정리(`server-`·`request-` 접두, 8자리 날짜), `_trace`면 stdout 병행, ID 필터. 날짜는 `--timezone` 기준. rotation=false 비우기(D-L1)는 append로 바꾸는 것을 제안. `request-yyyyMMdd.log`(SERVER_DB_DELETE, REMOTE_CONTROL, REMOTE_CONTROL_ALL, TRANX_LOAD_TIME_GROUP + AgentCall 등록 명령, 형식 `yyyyMMdd HH:mm:ss.SSS user(ip) cmd`)도 포함할지 결정 필요. 현재 Go `log` 출력과 SERVER_LOG_* 연동 방식도 정해야 한다(결과: D-L1 append, request log 포함, Go 표준 `log` 줄도 `StdLogWriter`로 server log에 기록, logger CONTRACT) | F0 | 가능: Java 서버 로그 파일명·줄 형식·삭제 대상 비교. 날짜 경계는 시계 주입 테스트 |
| W1 | P2 | webapp | `net_http_api_cors_*`, `net_http_api_auth_*`, `net_http_api_allow_ips`, `net_webapp_tcp_client_pool_timeout` | 요청마다 live 값 사용, pool timeout 기본값을 `net_tcp_client_so_timeout_ms`로 파생 | F0 | 가능: Java webapp과 응답 header·인증 결과 비교 |
| A1 | P2 | server, account | LDAP 키 | LOGIN마다 live LDAP 설정 | F0 | management-commands.md §2.3 oracle 재사용 |

Pulse(`internal/pulse`)에 해당하는 작업은 없다. Pulse 키는 재기동 키로 유지하고 F0가 Pulse 설정을 건드리지 않게 한다.

### 5.1a 작업별 연결·검증 현황(3C 작업 시점)

모든 작업은 runtime 배선(config 분류·`Live` 확장, ingest hook)으로 서버에 연결됐다. "검증"은 연결 경로를 실제로 지나는 테스트와 Java oracle 근거다. 실행 결과는 [검증 기록](../validation/2026-10-15-remaining-gaps.md)에 둔다.

| 작업 | 상태 | 검증 근거 |
|---|---|---|
| F0 | 연결됨 | `TestApplySettingsFromConfigReload`, `TestApplySettingsHooksAndCoalescing`, `TestLiveWiringEndToEnd`(conf 변경 → 소비자, flag 고정 유지) |
| I1, I2, I4, I5 | 연결됨 | ingest alerts oracle(원본 AgentManager, 39 case), `TestIngestSettingsConversion` |
| T3 | 연결됨 | `TestApplySettingsFromConfigReload`(agent pool timing 반영), `agent_pool_test.go` |
| I7 | 연결됨 | geoip golden 27, ingest geoip oracle 31. 실제 GeoLiteCity.dat는 미검증 |
| I6 | 연결됨 | sqltable oracle 2107건, `TestLiveWiringEndToEnd`(off면 파싱 안 함) |
| I8, L1 | 연결됨 | logger golden 6종, `TestDebugOutputHooks`, `TestLiveWiringEndToEnd`(log_dir 전환, request log 줄, `log_udp_text` 마스킹) |
| V1, V2, V3 | 연결됨 | LOGIN·XLog 하한·검색 상한 Java oracle, `TestLiveWiringEndToEnd`(LOGIN 값 변경) |
| W1 | 연결됨 | `TestLiveWiringEndToEnd`(webapp 설정 변경), webapp 단위 테스트. Java webapp JSON 대조는 미검증 |
| A1 | 연결됨 | `TestLiveWiringEndToEnd`(LDAP 켬 → LOGIN이 LDAP bind). 실제 LDAP 서버는 미검증 |
| T1, T2, T4, T5, T6 | 연결됨 | transport 단위·race 테스트, `TestLiveWiringEndToEnd`(pool 크기 변경) |
| S1~S5, I3(storage 쪽) | 연결됨 | `ConfigStorageOracle`(text daily·index 크기·purge), `TestStorageLogRateLimit`, `TestLiveWiringEndToEnd`(retention off → on) |

**연결하지 않은 키와 이유**(값은 Snapshot·LIST에 보인다):

| 키 | Go 동작 | 이유 |
|---|---|---|
| servlet listener(`internal/httpapi`)의 `net_http_api_allow_ips`, `net_http_api_auth_ip_header_key`, `net_http_api_gzip_enabled`, HTTP request log 보관(`log_keep_days`)·경로(`log_dir`) | 기동 값 | Java도 Jetty·servlet 구성을 기동 때 만든다(`HttpServer.java:61-112`). servlet 허용 목록 검사는 Go 전용 opt-in이라 live 대응 원본이 없다. 같은 키의 webapp·logger 소비는 live다 |
| `net_http_api_enabled`, `net_http_api_session_timeout`, `net_webapp_tcp_client_pool_size` | 기동 값 | Java도 기동 때만 읽는다(`HttpServer.java:79,112`, `WebAppMain.java:141`, `Server.java:73`) |
| `net_http_server_enabled`, `net_http_port`, listen ip/port, `net_udp_so_rcvbuf_size`, `net_udp_packet_buffer_size`, `_net_udp_worker_thread_count`, `db_dir`, `plugin_dir`, `_mgr_*_index_*_mb`, Pulse 키 | 재기동 키(`restart required` 로그) | Java도 기동 때만 읽거나(socket·index object val) 실행 중 변경이 위험하다(`db_dir` D-C1) |
| `_compress_write_buffer_block_count`, `_compress_read_cache_expired_ms`, `_compress_write_thread` | 표시 전용 | 대응 구조 없음: Go GZipStore는 record마다 member를 바로 쓰고 쓰기 buffer block·비동기 압축 thread·시간 만료 cache가 없다 |
| `span_queue_size` | 표시 전용 | Java도 applyConfig가 읽지 않아 항상 1000이다. Go ingest span writer queue는 1000 고정([ingest CONTRACT](../../internal/ingest/CONTRACT.md) §13). `xlog_queue_size`·`profile_queue_size`는 storage writer queue에 연결된 재기동 키다(위 표) |
| `log_service_handler_list` | 표시 전용 | 미구현(P3 디버그 출력) |
| `plugin_enabled`, `client_dir`, `temp_dir`, `net_webapp_tcp_client_so_timeout`, `input_telegraf_config_file` | 표시 전용 | Java 서버에도 소비처가 없다(미사용·미적용) |
| `net_http_extweb_dir`, `net_http_api_swagger_*` | 표시 전용 | 제외 확정(web_ext·swagger) |

### 5.2 바꾸지 않을 키(근거)

- 구조 차이: `_compress_*` 5개는 대응 구조가 없다. 대응을 만들면 queue/cache가 늘어 KISS에 어긋난다. `xlog_queue_size`·`profile_queue_size`는 storage writer queue 상한(재기동 키), `span_queue_size`는 Java처럼 읽지 않으며 Go span writer queue가 실효값 1000으로 고정이다. Core 단계 queue(XLogPreCore 등)는 없다.
- 재기동 유지(의도된 차이): `db_dir`(D-C1), listen ip/port, `net_udp_so_rcvbuf_size`, `net_http_*` 기동 키. Java도 기동 때만 읽거나 실행 중 변경이 위험하다.
- 미사용: `plugin_enabled`, `client_dir`, `temp_dir`, `net_webapp_tcp_client_so_timeout`, `input_telegraf_config_file`.
- 제외: extweb, swagger, plugin 실행(`plugin_dir`의 script 실행 부분), `IXLog`·`PluginInvocation`·`BuiltInPluginManager`의 `_trace`.

## 6. fixture·검증 후보

- V-TPE(실행 확인): `ThreadPoolExecutor(2,2,10000ms,LinkedBlockingQueue)`에 `setCorePoolSize(4)` 후 작업 6개를 넣는다. 결과는 Zulu 8 `grow ok core=4 max=2`, `active=4 queued=2`이고 Zulu 17 `grow java.lang.IllegalArgumentException core=2`, `active=2 queued=4`였다.
- V-LOGIN: conf A(`ext_link_name=x`, `tagcnt_enabled=false`, `net_tcp_client_so_timeout_ms=5000`, `server_id=s1`)로 Java 서버 LOGIN golden을 만든다. 파일 변경 뒤 다시 LOGIN golden B를 만든다.
- V-TEXT: 복사본 DB에서 daily on/off 전환 4가지 조합의 GET_TEXT·파일 bytes golden.
- V-SQLT: `SQLSimpleParser.getCrudInfo` 입력·출력 표(합성 SQL 30개 내외, select/insert/update/delete/merge/subquery/파싱 실패).
- V-PACKSTR: §1.8 Pack `toString` golden(합성 값).
- V-LOGFMT: Java 서버를 짧게 실행해 `server-yyyyMMdd.log` 첫 줄들의 형식과 `mgr_log_ignore_ids` 효과를 확인한다.

## 7. 조사하지 못한 항목

- ConfObserver 등록 순서는 코드 판독 추정이다. 실제 Java 서버를 띄워 `observers` 순서를 출력하지 않았다.
- `log_udp_*`·Logger 줄 형식은 코드 판독 결과다. Java 실행 golden은 아직 없다.
- Client가 `table` text(SqlTables 결과)를 어느 화면·명령으로 조회하는지 확인하지 않았다.
- webapp 키는 filter·adaptor 호출 위치까지만 확인했다. 각 요청 경로의 세부 반영 순서는 [webapp-api.md](webapp-api.md) 범위다.
- §1 Java 열·§2~§4는 최초 조사 기준이며, Go 열과 상태 열만 3C 작업 시점 기준으로 갱신했다. Go 열의 파일·함수 이름은 갱신했지만 줄 번호가 남은 행(예: `main.go:259`)은 조사 시점 줄이다.

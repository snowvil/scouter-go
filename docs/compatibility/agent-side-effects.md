# Java Agent 측 부작용 분석: 서버 결함이 계측 대상 애플리케이션에 미치는 영향

## 정본과 범위

- 원본: `$SCOUTER_SOURCE`(고정 원본 checkout), commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5` (`git rev-parse HEAD`로 확인, `baseline.json`과 동일). 아래 경로는 이 저장소 기준이며 `scouter.agent.java/src/main/java/scouter/agent/`는 `agent/`, `scouter.common/src/main/java/scouter/`는 `common/`, `scouter.server/src/main/scala/scouter/server/`는 `server/`로 줄인다.
- 비교 대상 Go 서버: 이 저장소 `internal/transport/tcp.go`, `internal/server/runtime.go`, `internal/server/agent_checkout.go`, `internal/server/agent_blob.go`, `internal/server/text_cache_reset.go`, `internal/service/agent_queries.go`(줄 번호는 분석 시점 기준).
- 질문: 서버(collector)가 잘못된 패킷, 미지원·위험 명령, 느린 응답, 끊김, 재접속 유도, 완전 부재 상태일 때 `-javaagent`로 심어진 `scouter.agent.java`가 어떻게 동작하고, 그것이 애플리케이션 스레드·메모리·가용성에 어떤 부작용을 주는가.
- 판단의 근거는 소스 정적 분석이다. 2026-10-24에 실제 Java Agent·Host Agent(2.20.0, 네트워크 계약은 기준 commit과 같음)로 수집·relay·고아 relay·Agent 실패·text reset·KEEP_ALIVE·재연결·수집 서버 주소 변경을 재현했다([Java Agent 상호운용](../validation/2026-10-24-java-agent-interop.md), 45/45 PASS). 11단계([개선 후보](../improvement-candidates-2026-10-21.md)의 단계 번호. 아래의 12·13단계와 C4·C8 같은 항목 ID도 같은 문서의 것이다)에서 서버 발신 명령 `OBJECT_SET_KUBE_SEQ`·`TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`도 실제 Agent로 확인했다(같은 문서 §2.8, 53/53 PASS). 서버→Agent frame 길이 불변식은 Go fuzz·경계 테스트로 고정했다(같은 문서 §8). 나머지는 끝의 "미검증·재현 fixture 후보"를 참조한다. 운영 규모·호스트 사양·TPS는 기록하지 않는다.

## 1. Agent 스레드 모델

| 구성 요소 | 스레드 | daemon | 예외 처리 | 근거 |
|---|---|---|---|---|
| TCP 세션 관리자 `TcpRequestMgr` | `SCOUTER-TCP` 1개, 1초 루프 | 예 | `catch (Throwable t) {}` 전부 삼킴, 로그 없음 | `agent/netio/data/net/TcpRequestMgr.java:14-22, 27-57` |
| TCP 명령 처리 `TcpWorker` | `ThreadUtil.createExecutor("SCOUTER", 10, 10000, true)` 고정 10 thread + **무제한** `LinkedBlockingQueue` | 예 | `run()`이 `catch (Throwable t) {}`로 삼키고 `finally close()` — 로그 없음 | `TcpRequestMgr.java:24`, `common/util/ThreadUtil.java:177-199`, `agent/netio/data/net/TcpWorker.java:25-34` |
| 명령 dispatch `ReqestHandlingProxy` | TcpWorker 스레드에서 동기 호출 | — | handler 예외는 `catch (Exception e) { e.printStackTrace(); }` → **애플리케이션 stderr**. `Error`는 잡지 않아 TcpWorker까지 전파 | `agent/netio/request/ReqestHandlingProxy.java:45-57, 100-110` |
| UDP 배치 송신 `UDPDataSendThread` | 1개, queue 비면 `net_udp_collection_interval_ms`(100ms) sleep | 예 | `run()`에 try/catch 없음 (`DataUdpAgent.write`가 IOException을 내부에서 잡으므로 보통 안전) | `agent/netio/data/UDPDataSendThread.java:34-42, 73-91` |
| UDP 큐 | `Queue<byte[]>(1024)` ring buffer. 가득 차면 **가장 오래된 항목을 덮어쓰고 null 반환**(drop, 비블로킹) | — | — | `UDPDataSendThread.java:47, 53-61`, `common/util/Queue.java:31-41` |
| 설정 reload `Configure` | `extends Thread`, 3초 루프, 날짜 단위 변경 시 `DataProxy.reset()` | 예 | — | `agent/Configure.java:53, 72, 894-907` |
| 로거 | 호출 스레드에서 동기 `System.out.println` 또는 파일. `log_dir` 기본값 `""` → **기본 설정에서는 애플리케이션 stdout** | — | — | `agent/Logger.java:97-114`, `agent/Configure.java:574, 1266` |

**애플리케이션 스레드가 직접 네트워크 I/O를 하는가 — 한다(UDP).** `DataProxy.sendDirect`는 호출 스레드에서 `DataUdpAgent.write` → `DatagramSocket.send`를 동기 실행한다(`agent/netio/data/DataProxy.java:214-219`, `agent/netio/data/net/DataUdpAgent.java:95-115`). 이 경로를 쓰는 호출자:

- XLog: `TraceMain.endHttpService` → `DataProxy.sendXLog` (`agent/trace/TraceMain.java:697`, `DataProxy.java:194-201`) — 트랜잭션 종료 스레드
- Profile: `ProfileCollector.process`가 step 버퍼가 차면 `sendProfile` (`agent/trace/ProfileCollector.java:58-66`, `DataProxy.java:232-291`) — 트랜잭션 스레드
- SQL text: `TraceSQL` → `sendSqlText` → `sendDirect` (`agent/trace/TraceSQL.java:167`, `DataProxy.java:58-67`)
- Referer, Alert, Counter: `DataProxy.java:95-103, 136-147, 294-315`

반대로 service/method/apicall/error/user-agent/login/web/group/hash-message/stack-element 텍스트와 heartbeat는 `udpCollect.add` → 1024 ring queue → 배치 스레드 경유다(`DataProxy.java:74-80, 104-134, 148-177, 317-373`).

**TCP는 애플리케이션 스레드를 쓰지 않는다.** 모든 서버→agent 명령은 `SCOUTER-n` 풀 스레드에서 처리되며, 그 스레드가 블로킹되어도 트랜잭션 처리에는 직접 영향이 없다. 단, 일부 handler는 JVM 전체에 영향을 주는 작업(STW heap walk, `System.gc`, `Thread.stop`)을 실행한다(§3).

부팅 순서: `JavaAgent.premain`이 `TcpRequestMgr.getInstance()`를 먼저 호출하고(`agent/JavaAgent.java:66`), handler 등록 `ReqestHandlingProxy.load`는 `AsyncRunner`의 `AgentBoot.boot()`에서 비동기로 실행된다(`agent/AgentBoot.java:38`). 그 사이에 도착한 명령은 모두 "unknown cmd"(A130)로 처리된다.

## 2. 서버가 보낸 바이트가 잘못된 경우

Java agent는 V2 framing만 사용한다(`TcpWorker.java:29, 93-134`). Host agent(`scouter.agent.host`)는 V1 스트리밍 `process()`를 사용한다(`scouter.agent.host/src/main/java/scouter/agent/netio/data/net/TcpWorker.java:29`).

V2 수신 경로: `in.readIntBytes()` → `new DataInputX(buff)` → `readText()` → `readPack()` (`TcpWorker.java:114-118`).

| 서버가 보낸 것 | agent 동작 | 애플리케이션 영향 | 근거 |
|---|---|---|---|
| 프레임 길이 `int` 음수 | `new byte[len]` → `NegativeArraySizeException` → `run()`의 Throwable catch → `close()` → ≤1초 후 재접속 | 없음(로그도 없음) | `common/io/DataInputX.java:71-80`, `TcpWorker.java:30-33`, `TcpRequestMgr.java:31-36` |
| 프레임 길이가 매우 큼 | 원본 Agent는 선언 길이를 검증하지 않고 할당한다. 할당에 실패하면 `OutOfMemoryError` → Throwable catch → 재접속 | **있음.** 큰 할당은 GC 압박이나 OOM으로 이어질 수 있다 | `DataInputX.java:71-80`, `TcpWorker.java:54, 114`, `Configure.java:95` |
| 프레임 안의 text/blob 길이가 프레임보다 큼 | `ByteArrayInputStream` 기반이라 `EOFException` → 재접속 | 없음 | `DataInputX.java:45-47, 90-110, 142-145` |
| 알 수 없는 Pack type byte | `PackEnum.create`가 `RuntimeException("Unknown pack type")` → 재접속 | 없음 | `common/lang/pack/PackEnum.java:55-66`, `DataInputX.java:265-269` |
| MapPack/ListValue count가 거대 | count만큼 루프하지만 프레임 버퍼 안에서 `EOFException` → 재접속. 프레임 길이가 상한이므로 count 자체로 큰 할당은 없음 | 없음 | `common/lang/pack/MapPack.java:174-182`, `common/lang/value/ListValue.java:158-164` |
| Pack type은 맞지만 handler가 기대한 `MapPack`이 아님 | handler의 `(MapPack) param` 캐스트 → `ClassCastException` → `InvocationTargetException` → `e.printStackTrace()` **애플리케이션 stderr**, 응답 `NoNEXT` | stderr 1건/요청 | `ReqestHandlingProxy.java:49-55`, `agent/netio/request/handle/AgentThread.java:50`, `AgentHeapDump.java:61` 등 |
| Host agent(V1)에 선언 길이가 매우 큰 blob | 위 "프레임 길이가 매우 큼"과 동일한 `read(len)` 할당. V1은 프레임 경계가 없어 어떤 파싱 오류도 스트림 desync → 예외 → 재접속 | V1 host agent도 동일 메모리 위험 | `DataInputX.java:98-101`, host `TcpWorker.java:65-91` |

재접속 비용: `TcpRequestMgr.run`은 1초마다 `LIVE.size() < sessionCount`이면 새 `Socket`을 `connect(timeout 3000)`하고 풀에 제출한다. 연결 실패 시 루프 안에서 3초 추가 sleep. 성공 후 즉시 끊기면 1초마다 반복(`TcpRequestMgr.java:31-40`, `TcpWorker.java:44-63`). 1회당 비용은 소켓 1개 + 8KB 입력/8KB 출력 버퍼 + 4바이트 handshake 2개. 실패는 agent 로그에 남지 않는다.

**Go 서버가 보내는 프레임은 위 위험을 구조적으로 피한다.** `AgentConn.RequestLimited`는 body(명령 text+파라미터)를 먼저 encode한 뒤 V2는 `WriteIntBytes`로 길이+본문을, V1은 본문을 inline으로 쓰며(2026-10-24부터 V1도 먼저 encode), `Stream.WriteIntBytes`는 그 명령의 item 상한(`AgentLimits.ItemBytes`: 일반 relay 8MiB, 큰 응답 명령 256MiB, `internal/server/agent_blob.go`) 초과를 `ErrLimit`으로 거부한다(`internal/transport/tcp.go` `RequestLimited`, `internal/transport/stream.go` `WriteIntBytes`). 파라미터는 Client 요청 decode 상한(변경 없음) 안의 `MapPack`이다. 따라서 선언 길이가 실제 본문과 다른 프레임은 생성되지 않는다. 명령 파라미터는 `KEEP_ALIVE`/`OBJECT_RESET_CACHE`에 빈 `MapPack`, 조회 relay에는 Client가 보낸 `MapPack`이다(`runtime.go:143`, `text_cache_reset.go:132`, `agent_queries.go` `HandleAgentReadCommand`).

회귀 방지(2026-10-24, 11단계 C8): `internal/transport/agent_frame_fuzz_test.go`가 이 불변식을 고정한다.

- 방법: `RequestLimited`가 실제로 쓴 byte를 `DataInputX`를 따른 독립 reader로 읽는다.
- `FuzzAgentRequestFrame`, V2: 선언 길이 = 뒤따르는 byte, 4+길이 ≤ item.
- `FuzzAgentRequestFrame`, V1: 명령을 정확히 소비한다.
- `FuzzAgentRequestFrame`, 거절: V1·V2 모두 wire 0 byte이고, 연결은 `RequestLimited`가 스스로 닫는다.
- `TestAgentRequestFrameAtRelayLimit`: 8MiB 경계를 확인한다.
- 임시 mutation 확인: 5개 중 새 테스트 4개, 기존 테스트 2개를 잡았다. "쓰기 후 상한 검사"와 "V1 text 선 flush"는 기존 테스트가 놓친 것이다.
- V1 부분 쓰기 수정: 4KiB를 넘는 명령 text가 Pack 거절 전에 socket에 나가던 경로를 고쳤다. V1도 V2처럼 text+Pack을 먼저 encode하고 상한을 확인한 뒤 쓴다([검증 §8](../validation/2026-10-24-java-agent-interop.md)).

## 3. 서버가 보낼 수 있는 명령 전체와 애플리케이션 영향

`ReqestHandlingProxy.load`는 `scouter.agent.netio.request.handle` 패키지와 `-Dscouter.handler` 시스템 속성의 클래스에서 `@RequestHandler`를 수집한다(`ReqestHandlingProxy.java:65-98`). 7개 handler 클래스의 전체 명령은 다음과 같다. "Go 전달" 열: **서버 발신**은 Go가 스스로 보내는 명령, **relay**는 인증된 Client 요청 시 `AgentReadCommands()` 또는 `OBJECT_ENV`로 전달하는 명령(`internal/service/agent_queries.go` `AgentReadCommands`), **차단**은 Go가 보내지 않는 명령.

| 명령(wire literal) | handler | 애플리케이션 영향 | Go 전달 |
|---|---|---|---|
| `KEEP_ALIVE` | `ReqestHandlingProxy.process:101-102` (handler 없이 null 반환 → `NoNEXT`만 응답) | 무해 | **서버 발신** (idle 5초마다) |
| `OBJECT_RESET_CACHE` | `AgentEnv.getAgentCacheReseto:80-84` → `DataProxy.reset()` | 무해에 가까움. 텍스트 사전 13개 set을 비워 이후 트랜잭션에서 TextPack을 다시 보냄(§4) | **서버 발신** (기동 30초 후·날짜 변경 시). Java는 Agent마다 순차 호출(응답 대기 so_timeout 60초)이고 Go는 최대 16개 병렬, 호출당 relay deadline까지 응답을 기다린다(고아 relay를 만들지 않음, 2026-10-23) |
| `OBJECT_ENV` | `AgentEnv.getAgentEnv:52-64` `System.getProperties()` 전체 | 읽기. 시스템 속성에 비밀값이 있으면 그대로 노출 | relay |
| `OBJECT_INFO` | `AgentEnv.getAgentInfo:68-78` | 읽기 | 차단 |
| `OBJECT_SYSTEM_GC` | `AgentEnv.systemGc:44-50` `System.gc()` | **위험**: 명시적 Full GC(STW) | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `OBJECT_DUMP_FILE_LIST` | `AgentEnv.getDumpFileList:86-103` `dump_dir.listFiles()` | 읽기(디스크) | relay |
| `OBJECT_DUMP_FILE_DETAIL` | `AgentEnv.getDumpFileDetail:105-126` 파일을 4KB blob 연속 스트림 | 부하(디스크 읽기, 파일 크기 비례) | relay(blob, chunk 단위 즉시 중계, chunk당 16MiB 상한) |
| `OBJECT_CLASS_LIST` | `AgentEnv.getLoadedClassList:128-208` `Instrumentation.getAllLoadedClasses()` 전체 배열 + 필터 | 부하(로드된 클래스 수 비례 배열 할당·문자열 비교, 100개/page 응답) | relay |
| `OBJECT_HEAPHISTO` | `AgentHeapHisto.heaphisto:28-36` → `ToolsMainFactory.heaphisto:38-70` → `HotSpotVirtualMachine.heapHisto("-all")` (`scouter.agent.batch/.../ToolsMain.java:82-83`), 최대 100000줄 | **부하(STW)**: HotSpot heap histogram은 safepoint에서 전체 힙을 순회한다. 힙 크기에 비례한 정지 | relay |
| `TRIGGER_HEAPHISTO` | `AgentHeapHisto.tirgger_heaphisto:38-41` → `DumpUtil.triggerHeapHisto` 파일 기록 | **부하(STW)** + `dump_dir` 파일 생성 | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `OBJECT_THREAD_DETAIL` | `AgentThread.threadDetail:48-135` `ThreadMXBean.getThreadInfo(id, 500)` | 읽기(단일 스레드 stack 500 frame; SQL 원문·bind 변수 노출) | relay |
| `OBJECT_THREAD_CONTROL` | `AgentThread.threadKill:160-196` `interrupt`/`stop`/`resume`/`suspend` | **위험**: 애플리케이션 스레드 강제 중단·정지 | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `OBJECT_THREAD_LIST` | `AgentThread.threadList:197-219` `ThreadUtil.getThreadList()` | 읽기(전체 스레드 정보) | relay |
| `OBJECT_ACTIVE_SERVICE_LIST` | `AgentThread.activeThreadList:221-295` 활성 TraceContext 순회 + 스레드별 CPU time | 읽기(활성 트랜잭션 수 비례) | relay |
| `OBJECT_THREAD_DUMP` | `AgentThread.threadDump:296-304` → `ToolsMainFactory.threadDump` (attach API) | 부하(전체 스레드 safepoint dump) | relay |
| `TRIGGER_ACTIVE_SERVICE_LIST` / `TRIGGER_THREAD_LIST` / `TRIGGER_THREAD_DUMP` | `AgentThread:305-318` → `DumpUtil.trigger*` `dump_dir` 파일 기록 | 부하 + 파일 생성 | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS` | `AgentThread:320-325` → `DumpOnCpuExceedanceWorker` queue(2) | 부하 + 파일 생성(비동기, queue 초과 시 drop) | **서버 발신** — Java `ObjectCpuChecker`(`server/core/app/ObjectCpuChecker.scala`)와 같이 `ADC` tag로 autodump를 켠 javaee 계열 agent의 REALTIME `ProcCpu`가 `ADC_TH`를 `ADC_D` 동안 넘으면 보낸다(`internal/ingest/objcpu.go`). 호출은 queue(100, 초과 시 drop·계수)를 거쳐 별도 worker가 수행한다(`internal/ingest/agent_calls.go`). 2026-10-24 실제 Agent 검증: 파라미터 `{objHash, TRIGGER_DUMP_REASON: TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED}`를 확인했다. Agent는 원본 CPU 초과 가드(`dump_cnt`·`dump_interval`)를 따라 dump했고 `NoNEXT`만 답했다. Host Agent 2.20 `ProcCpu`는 0~1 비율이라 정수 `ADC_TH` ≥ 1에서는 원본·Go 모두 보내지 않는다([검증 §2.8](../validation/2026-10-24-java-agent-interop.md)). Client relay는 차단 |
| `PSTACK_ON` | `AgentThread.turnOn:327-337` `MakeStack.pstack_requested = now + time` | 부하: 지정 시간 동안 주기적 stack 수집 활성화(`agent/counter/task/MakeStack.java:63-66`) | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `OBJECT_CALL_HEAP_DUMP` | `AgentHeapDump.callHeapDump:50-116` `HotSpotDiagnosticMXBean.dumpHeap(file, live=true)`, 10초 재호출 가드, JVM CWD 하위 `heapdump/` 디렉터리 | **위험**: Full GC + 힙 크기만큼 디스크 쓰기, STW | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `OBJECT_DELETE_HEAP_DUMP` | `AgentHeapDump.deleteHeapDump:148-163` | **위험**: Agent 측 dump 파일 삭제 | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `OBJECT_LIST_HEAP_DUMP` | `AgentHeapDump.listHeapDump:165-189` | 읽기 | relay |
| `OBJECT_DOWNLOAD_HEAP_DUMP` | `AgentHeapDump.downloadHeapDump:197-223` 파일을 2MB blob 연속 스트림 | 부하(디스크 읽기, 파일 크기 비례; 실패 시 stderr stack trace) | relay(blob, chunk 단위 즉시 중계, chunk당 16MiB 상한) |
| `OBJECT_LOAD_CLASS_BY_STREAM` | `AgentClassHandle.loadClassAsStream:35-57` `getAllLoadedClasses` 선형 탐색 + 클래스 바이트 읽기 | 읽기~부하(클래스 수 비례) | relay |
| `OBJECT_CLASS_DESC` | `AgentClassHandle.getClassInfo:58-75` | 읽기~부하(클래스 수 비례) | relay |
| `OBJECT_CHECK_RESOURCE_FILE` | `AgentClassHandle.checkJarFile:92-115` Client가 준 resource 위치의 JAR 정보 조회 | 읽기. 원본 Agent는 resource 값의 형식을 검증하지 않는다. Go는 인증된 Client 요청만 그대로 relay한다 | relay |
| `OBJECT_DOWNLOAD_JAR` | `AgentClassHandle.downloadJar:116-138` `FileUtil.readAll(file)` → JAR 전체를 힙에 올려 단일 `BlobValue`로 응답 | **부하(메모리)**: JAR 크기만큼 힙 할당(단일 `MapPack`이므로 Go는 이 명령에 256MiB 응답 상한을 둔다. 넘으면 로그를 남기고 소켓 종료) | relay |
| `OBJECT_FILE_SOCKET` | `AgentFileSocket.getFileList:36-60` `RunExec("lsof -Pp <pid>")` | 부하: **외부 프로세스 fork/exec** | relay |
| `SOCKET` | `AgentFileSocket.getSocketList:62-97` `SocketTable` 순회, `stackOrder` 플래그 설정 | 읽기(이후 socket open 시 stack 수집이 켜지는 부수 효과) | relay |
| `GET_CONFIGURE_WAS` | `AgentConfigure.getAgentConfigure:41-55` 설정 파일 원문 | 읽기(설정 파일에 비밀값이 있으면 노출) | relay |
| `SET_CONFIGURE_WAS` | `AgentConfigure.setAgentConfigure:57-67` 설정 파일 **덮어쓰기** + `reload(true)` → `ConfObserver.run()` | **위험**: hook 서명 변경 시 `AgentTransformer.reload`(`agent/AgentTransformer.java:107-114`), UDP 소켓 재오픈(`DataUdpAgent.java:41-46`), collector 주소 변경 시 TCP 세션 종료(`TcpWorker.java:111-112`) | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `LIST_CONFIGURE_WAS` | `AgentConfigure.listConfigure:69-77` | 읽기 | relay |
| `REDEFINE_CLASSES` | `AgentConfigure.redefineClasses:81-122` `Instrumentation.redefineClasses` | **위험**: 클래스 재정의(원본 바이트코드로 되돌림) | **relay(변경)** — 2026-10-16 `ManageCommands`(관리 명령 구현)부터 인증된 Client 요청을 전달한다 |
| `CONFIGURE_DESC` / `CONFIGURE_VALUE_TYPE` | `AgentConfigure:167-189` | 읽기 | 차단 |
| 그 외 알 수 없는 문자열 | `ReqestHandlingProxy.process:107` `Logger.println("A130", ...)` → `NoNEXT` | 로그 1줄/요청(기본 설정에서 앱 stdout) | — |

Host agent handler(`scouter.agent.host/.../handle/`): `OBJECT_ENV`, `OBJECT_SET_KUBE_SEQ`(HostAgentEnv), `HOST_DISK_USAGE`(DiskPerf, oshi), `HOST_TOP`(TopRequest, oshi), `GET/SET/LIST_CONFIGURE_WAS`, `CONFIGURE_DESC`, `CONFIGURE_VALUE_TYPE`(HostAgentConfigure). Host agent는 애플리케이션 JVM 밖의 별도 프로세스이므로 서비스 부작용은 없다. `OBJECT_SET_KUBE_SEQ`는 **서버 발신**이다: Java `PodSeqManager.pushSeqToAgent`(`server/kube/PodSeqManager.java:151-161`)와 같이 `useKubeSeq` tag와 `podName`이 있는 object의 pod seq가 정해지면 `{"seq": podSeq}`를 보낸다(`internal/ingest/kube.go`). Java는 AgentManager 스레드에서 동기 호출하지만 Go는 queue(1024, 초과 시 drop·계수)에 넣고 별도 worker가 호출한다(`internal/ingest/agent_calls.go`). 2026-10-24에 실제 Host Agent로 확인했다(kube 환경변수 + 33자 host 이름). 첫 heartbeat는 등록이 멈추고 seq 0이 push된 뒤 `/interop-kube-pod_0`으로 등록됐다. 새 objHash의 V1 세션으로 relay도 됐다([검증 §2.8](../validation/2026-10-24-java-agent-interop.md)). Host agent에는 `OBJECT_RESET_CACHE` handler가 없어 `NoNEXT`만 답한다(`TcpWorker.java:77-82`).

Go가 relay하는 명령 중 애플리케이션 JVM에 유의미한 부하를 줄 수 있는 것은 `OBJECT_HEAPHISTO`(STW heap walk), `OBJECT_THREAD_DUMP`, `OBJECT_CLASS_LIST`/`OBJECT_LOAD_CLASS_BY_STREAM`/`OBJECT_CLASS_DESC`(전체 클래스 배열), `OBJECT_DOWNLOAD_JAR`(JAR 전체 힙 적재), `OBJECT_FILE_SOCKET`(프로세스 fork), `OBJECT_DOWNLOAD_HEAP_DUMP`/`OBJECT_DUMP_FILE_DETAIL`(디스크 읽기)이다. 이는 Java 서버도 동일하게 relay하는 Client 기능이며, Go는 인증된 Client 요청에만 전달한다(`internal/service/agent_queries.go` `HandleAgentReadCommand`). Go가 스스로 보내는 명령은 `KEEP_ALIVE`, `OBJECT_RESET_CACHE`, `OBJECT_SET_KUBE_SEQ`(kube 순번 push), `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`(CPU 초과 dump)의 네 가지이며, 모두 Java 서버도 보내는 명령이다(위 표의 **서버 발신**).

모든 relay(Client 요청, `OBJECT_RESET_CACHE`, 수집 부수 호출)는 명령을 쓰기 **전에** 전역 relay 메모리 예산(GOMEMLIMIT의 1/4, 없으면 1GiB, 하한 768MiB; 256MiB 응답 명령은 동시 2개, `internal/server/agent_relay_budget.go`)을 잡는다. Client 명령은 `net_tcp_get_agent_connection_wait_ms`(기본 1초, Client so-timeout 8초보다 짧음)까지 기다린다. 그 안에 자리가 나지 않으면 그 명령은 Agent에 보내지 않고, Java `AgentCall`이 빈 세션을 못 찾았을 때와 같은 null 결과를 준다. 그래서 떠난 Client의 명령(`OBJECT_HEAPHISTO`, `SET_CONFIGURE_WAS` 등)이 늦게 전달되어 재시도와 겹치는 일이 없다. 같은 명령 안에서 한 번 거절되면 나머지는 기다리지 않는다. 서버 발신 중 kube 순번·CPU dump는 10초 기다린 뒤 실패로 세고, `OBJECT_RESET_CACHE`는 기다리지 않고 나중에 재시도한다. 이 상한은 Agent에 프레임을 보내거나 세션을 닫지 않는다(2026-10-23, Java에는 없는 상한, [runtime](runtime.md) 운영자 안내).

## 4. `OBJECT_RESET_CACHE` 이후 재전송량

`DataProxy.reset()`(`DataProxy.java:178-193`)은 다음 set을 `clear()`한다(괄호는 `setMax` 상한): serviceName(10000), errText(10000), apicall(10000), methodName(10000), sqlText(10000), referer(1000), userAgent(1000), descTable(1000), loginTable(10000), webNameTable(1000), groupAgent(500), hashMessage(10000), stackElement(20000). `objNameSet`(10000, `DataProxy.java:81`)과 `sqlHash`(5000, `:45`)는 비우지 않는다.

- 재전송은 **즉시 burst가 아니라 lazy**다. 각 텍스트는 다음에 다시 쓰일 때 `contains`가 false여서 TextPack 1개를 보낸다. 재전송 상한은 set 합계 85,500건이며 실제로는 다시 등장한 고유 텍스트 수만큼이다.
- 경로별 비용: SQL/referer는 `sendDirect`(호출 스레드 동기 UDP, `DataProxy.java:58-67, 95-103`), 나머지는 `udpCollect.add`(ring queue 1024, 초과 시 가장 오래된 것 drop). reset 직후 100ms 안에 1024개를 넘는 고유 텍스트가 큐에 들어가면 일부 TextPack이 유실되어 서버에 이름이 비어 보일 수 있다(앱 영향 없음, 데이터 품질 문제).
- UDP 패킷: TextPack 1개 ≤ `net_udp_packet_max_bytes`(60000)이면 단일/묶음 패킷, 초과하면 `writeMTU` 분할(`DataUdpAgent.java:99-100, 116-151`). 긴 SQL 원문은 분할 송신된다.
- `reset()`은 synchronized가 아니지만 각 `IntLinkedSet.clear()`는 synchronized다(`common/util/IntLinkedSet.java:248-254`). 동시 reset은 최악의 경우 같은 텍스트를 두 번 보내는 정도다.

Go 서버의 호출 시점은 Java `TextCacheReset`과 같다: 기동 30초 후 1회, 이후 2초 폴링에서 날짜(`scoutertime.Date`, 서버 `Location`) 변경 시 live agent 전체를 호출하고(Java는 순차·응답 대기 so_timeout, Go는 최대 16개 병렬·호출당 relay deadline까지 대기, relay 예산이 차 있으면 대기 없이 나중에 재시도, 2026-10-23) agent의 echo(`param` 반환, `AgentEnv.java:83`)는 버린다(`internal/server/text_cache_reset.go` `tick`, `run`).

**의도된 차이: 같은 날짜 안의 재시도(2026-10-21).** Java `TextCacheReset.scala:36-46`은 날짜 단위마다 live agent를 한 번만 부르고, TCP 세션이 없으면 `AgentCall`이 S501만 남기고 null을 반환한다(`AgentCall.scala:46-71`). Agent는 텍스트를 sent-set에 넣은 뒤 UDP로 한 번만 보내므로, 서버가 내려가 있던 동안 잃은 텍스트는 그 agent가 reset을 받을 때(대개 자정)까지 이름이 빈다. 재기동 30초 시점에 TCP가 아직 붙지 않은 agent가 매번 일부 있었다. Go는 한 pass에서 놓친 objHash를 그 날짜 동안 보관하고 다시 보낸다.

- 보관 대상: 세션 없음·busy(`net_tcp_get_agent_connection_wait_ms` 안에 빈 세션 없음)·오류. 세션이 명령을 받았으면 응답 Pack이 없어도(host agent의 `NoNEXT`만 응답) 성공이다(`agentPool.requestDiscard`, `internal/server/agent_checkout.go`). 이전에는 이 경우를 "missing"으로 세었다.
- 재시도 조건: 2초 tick마다 pass 순서대로 보며, 그 objHash에 열린 세션이 있고(`agentPool.connected`) 다음 시도 시각이 지난 것만 부른다. 세션 없음·busy로 놓친 것은 세션이 보이는 즉시(pool은 둘을 구분해 돌려주지 않는다), 오류는 10초 뒤부터 부르고, 재시도가 실패할 때마다 간격을 10·20·40·80초로 늘린다.
- 상한: 보관 수 ≤ live 수이며 절대 16384(`ingest.DefaultMaxObjects`와 같음, 초과분은 `overflow`로 세고 재시도하지 않음), tick당 호출 4회(동시에, 호출당 relay deadline까지; relay 예산 거절은 시도 횟수에 넣지 않고 10초 뒤 다시), objHash당 재시도 5회 후 포기. 날짜 단위가 바뀌면 보관 목록을 버리고(`discarded`) 새 pass가 처리한다.
- 로그: pass 요약 줄 `text cache reset: date … live … called … missing … errors …`는 그대로 두고, 보관이 있으면 `text cache reset: date … retry pending … overflow …`를 덧붙인다. 재시도 결과는 `text cache reset: date … retry called … failed … gave up … discarded … pending …` 한 줄로 합쳐 최대 1분에 한 번(날짜 변경 pass 직후에는 이전 날짜분을 즉시) 남긴다. 재시도의 agent별 오류는 줄마다 쓰지 않는다.
- 앱 영향: 중복 reset은 §4의 lazy 재전송을 한 번 더 일으킬 뿐이다. 재시도는 pass에서 놓친 agent에만, 날짜당 최대 5회, 전체 초당 2회 이하로 간다.
- 검증: `internal/server/text_cache_reset_test.go`(미연결 후 연결 시 같은 날 reset, busy·오류 후 간격 재시도, 최대 시도 후 포기, 날짜 변경 시 목록 폐기, 보관·라운드 상한, 실제 agent pool을 통한 늦은 연결 e2e). 실제 Java Agent로 새 빈 DB 재기동 시 reset 전 text 미전송과 reset 후 재전송을 확인했다([Java Agent 상호운용](../validation/2026-10-24-java-agent-interop.md) §4). 날짜 전환 reset은 미검증.

## 5. 서버 응답이 느리거나 없는 경우

| 상황 | agent 동작 | 애플리케이션 영향 | 근거 |
|---|---|---|---|
| 서버가 명령을 보내지 않음(KEEP_ALIVE 부재) | `readIntBytes`가 `so_timeout` 60초에 `SocketTimeoutException` → close → 1초 후 재접속. 60초마다 1회 재접속 반복 | 없음 | `TcpWorker.java:54, 114`, `Configure.java:95` |
| 서버가 명령을 보내고 응답을 읽지 않음/도중 소켓 닫음 | agent는 응답을 `BufferedOutputStream`에 쓰고 `flush`. 상대가 닫혔으면 소켓 버퍼가 찬 뒤 `IOException` → close → 재접속. 응답이 작으면 flush가 성공한 뒤 다음 `readIntBytes`에서 EOF | 없음(handler는 이미 실행 완료 — 부하 명령이면 부하는 그대로 발생) | `TcpWorker.java:121-128, 30-33` |
| 서버 TCP 포트 미오픈 | `connect(timeout 3000)` 실패 → `prepare()` false → 3초 sleep → 다음 루프. 약 4초 간격 재시도 | 없음 | `TcpWorker.java:51-62`, `TcpRequestMgr.java:33-39` |
| 서버 UDP 부재 | `DatagramSocket.send`는 비연결 소켓이라 ICMP unreachable을 예외로 올리지 않는다(Java는 `connect`된 DatagramSocket에서만 `PortUnreachableException`). 송신 syscall 비용만 들고 패킷은 버려진다 | 없음. 호출 스레드 블로킹은 로컬 송신 버퍼가 가득 찬 극단적 경우에만 발생 | `DataUdpAgent.java:95-115` (`send` 호출, connect 없음) |
| collector 호스트명 DNS 실패 | `setTarget`에서 `getByName` 예외 → `server_host == null` → `write`가 false 반환, 재시도 없음(설정 변경 시 `ConfObserver`로만 재해석) | 없음(데이터만 유실) | `DataUdpAgent.java:48-58, 97-98` |

## 6. 시간·날짜

- Agent는 JVM 기본 timezone의 `DateUtil.getDateUnit()`으로 날짜 변경을 감지해 3초 루프에서 `DataProxy.reset()`을 호출한다(`Configure.java:894-907`).
- Go 서버는 `config.Location`의 날짜가 바뀔 때 `OBJECT_RESET_CACHE`를 보낸다(`text_cache_reset.go` `tick`). agent TZ와 서버 Location이 다르면 하루 두 번(각각의 자정) reset이 일어난다. 같은 TZ면 최대 수 초 차이로 두 번 reset된다. 어느 경우든 §4의 lazy 재전송이 두 번 일어나는 것 이상의 영향은 없다.

## 7. Go 서버 특이 동작 평가

### (a) hash당 agent 연결 8개 제한

- Go: `agentPool.handle`이 같은 `ObjectHash`에 8개 이상이면 `ErrLimit`을 반환하고 `serveConn`의 defer가 소켓을 닫는다(`runtime.go:106-111`, `tcp.go:186-196`). Java 서버는 objHash당 `RequestQueue(50)`이고 초과 시 `put`이 false를 반환하지만 소켓은 유지된다(`server/netio/service/net/TcpAgentManager.scala:58-68`, `common/util/RequestQueue.java:89-98`).
- Agent 기본 `net_collector_tcp_session_count = 1`(최솟값 1, `Configure.java:93, 1025`) → 영향 없음.
- 9 이상으로 설정된 agent: 9번째부터는 handshake 직후 EOF → `run()` Throwable catch → `close()` → `LIVE.remove` → 1초 후 재접속. **초과 세션 수만큼 1초마다 재접속이 무한 반복**된다. Agent 쪽은 로그가 없어 보이지 않고, Go 쪽은 연결마다 `TCPStats.Errors`·`LimitErrors`를 올리며 `tcp.connection ... ErrLimit` 오류 이벤트는 limit 분류로 10초에 1건(사이 생략 수를 `(<n> more limit errors since the last line)`으로 붙임, transport CONTRACT §2.3)만 남긴다. 앱 영향은 초당 소켓 1개 + 약 16KB 버퍼 garbage 수준이며, 서버 쪽에 TIME_WAIT이 누적된다(Go가 먼저 닫음). 11 이상이면 `SCOUTER` 풀(10 thread) 밖에서 대기하는 worker가 열린 소켓을 들고 큐에 쌓이는데, 그 소켓은 Go 쪽 KEEP_ALIVE 3초 무응답으로 닫히므로 LIVE는 결국 수렴한다. 단 `LinkedBlockingQueue`가 무제한이라 이론상 대기 task는 쌓일 수 있다(`ThreadUtil.java:178`).
- 운영 확인 사항: 기존 Java 서버와 달리 "조용히 반복 재접속"이므로 이런 설정의 agent가 있는지 사전 확인이 필요하다(설정 조사는 이 문서 범위 밖).

### (b) KEEP_ALIVE 주기

- Java 서버: `TcpAgentManager` 5초 scan, `isExpired`는 마지막 write로부터 `net_tcp_agent_keepalive_interval_ms`(5000) 경과, `sendKeepAlive(3000)`은 so_timeout 3초로 응답 대기(`TcpAgentManager.scala:30-56`, `TcpAgentWorker.scala:83-104`, `server/Configure.java:207`).
- Go: 5초 ticker, `claimIdle`은 `lastUsed`로부터 5초 경과 조건, 3초 context timeout(`runtime.go:131-146`, `agent_checkout.go:49-57`).
- 실효 주기 5~10초, 3초 대기 모두 동일. Agent의 `so_timeout` 60초보다 충분히 짧아 agent 쪽 timeout 재접속은 발생하지 않는다. KEEP_ALIVE에 대한 agent 응답은 `NoNEXT` 1바이트뿐이며 handler를 호출하지 않는다(`ReqestHandlingProxy.java:101-102`).

### (c) 요청 실패 시 소켓 종료

- Go `AgentConn.Request`는 어떤 오류든 `a.Close()`(`tcp.go:455-459`), 넘겨받은 context가 끝나도 `AfterFunc`로 닫는다(`:452`). Java `TcpAgentWorker.write/readPack/sendKeepAlive`도 Throwable에 `close()`한다(`TcpAgentWorker.scala:37-38, 54-55, 101-102`). 정책 동일. 단 2026-10-21부터 server Pack relay는 Client 명령 context가 아니라 Agent 연결 수명 context를 넘긴다(§7(d) "호출자가 먼저 떠난 relay"). Client가 떠나거나 Client 진전 상한에 걸려도 Agent 세션은 닫히지 않는다.
- Agent 재접속 비용은 §2와 같고(1초 내 재접속), 실패 명령의 handler는 이미 실행됐으므로 "부하 명령 → Go timeout(read마다 `net_tcp_agent_so_timeout_ms`, 총 deadline은 so_timeout+`RequestTimeout`(기본 60초+30초), 큰 응답 명령은 so_timeout+10분, blob stream은 총 deadline 없음) → 소켓 종료 → 재요청"이 반복되면 부하가 반복된다. 이는 Client 조작에 의존하며 Java 서버도 같다. 이전에는 Client 명령의 30초 무진전 감시가 이 deadline보다 먼저 Agent 소켓을 닫아, 30초를 넘는 heap histogram·thread dump는 재시도마다 STW 작업이 되풀이됐다(C4, 2026-10-21 수정).
- 세부 차이: Go는 닫힌 슬롯을 `release`에서 `done`을 닫아 제거하고(`agent_checkout.go:58-74`), 다음 checkout은 새 연결을 최대 1초 기다린다(`:15-47`). Java는 `net_tcp_get_agent_connection_wait_ms`로 기다린다. 앱 영향 없음.

### (d) 응답 상한(명령별)

Java `AgentCall`/`TcpAgentWorker`에는 크기·총 시간 상한이 없고 read마다 `SO_TIMEOUT`만 있다(`AgentCall.scala:74-91`, `TcpAgentWorker.scala:17, 69-77`). Go는 read마다 등록 때의 `net_tcp_agent_so_timeout_ms`를 그대로 쓰고, 메모리 상한만 명령별로 둔다(`internal/server/agent_blob.go`, `internal/transport/tcp.go` `AgentLimits`).

| 응답 형태 | 명령 | 상한 | 총 deadline |
|---|---|---|---|
| Blob stream | `OBJECT_DOWNLOAD_HEAP_DUMP`, `OBJECT_DUMP_FILE_DETAIL` | chunk 하나 16MiB(Java agent는 2MiB·4KiB chunk). 개수·총량 상한 없음 | 없음(read마다 so_timeout, Client 쪽은 64KiB write 조각마다 `RequestTimeout`. 13단계부터 Client 무진전 감시는 stream 전체에서 보류) |
| 큰 단일 `MapPack` | `OBJECT_DOWNLOAD_JAR`(jar 전체 `BlobValue`), `OBJECT_HEAPHISTO`, `OBJECT_THREAD_DUMP` | 응답 256MiB(Client 응답 값 상한 `transport.MaxResponseValueBytes`와 같음) | so_timeout + 10분 |
| 그 밖의 relay | 나머지 조회·관리 relay, KEEP_ALIVE, 서버 발신 호출 | 응답 8MiB, Pack 4096개 (모든 Agent 응답 item: collection·값 262,144개) | so_timeout + `RequestTimeout`(30초) |

- **Blob stream은 받은 chunk를 바로 Client stream에 쓴다**(`HeapDumpService.scala:57-72`, `DumpService.scala:118-131`와 같은 순서). 서버가 쥐는 메모리는 chunk 하나다. Client가 느리면 Client 소켓 쓰기가 막히고, 그동안 서버가 Agent를 읽지 않으므로 Agent 쓰기도 TCP backpressure로 멈춘다. so_timeout은 Agent 입력을 기다리는 read에만 걸리므로 느린 Client 때문에 끊기지 않는다(`TestAgentBlobRelaySlowClient`).
- **끊김·취소**: Client 쓰기 실패나 Agent read 실패·상한 초과는 요청을 끝내고 Agent 세션을 닫는다. Java도 handler가 던지면 `TcpAgentWorker.read`가 `close()`한다. 이미 Client에 쓴 chunk는 남고 `NoNEXT` 없이 연결이 끝난다. Java Client는 이를 실패로 본다.
- **상한 초과**: 응답을 버리지 않고 요청을 실패시킨다. Agent 세션을 닫고 `agent relay <cmd> objHash=<hash> refused: response over the Go relay limit (item=… total=…)` 로그를 남긴다. Client 연결은 `NoNEXT` 없이 끝난다. 이때 Agent 쪽 스트리밍 handler(`AgentHeapDump.java:219-221`, `AgentEnv.java:121-123`)는 소켓이 닫힌 뒤 `IOException`을 받아 **애플리케이션 stderr에 stack trace 1건**을 남기고 재접속한다. Java 서버에서도 Client가 끊기면 같은 일이 생긴다.
- 남은 차이:
  - Java에는 큰 `MapPack`의 256MiB 상한과 Pack relay의 총 deadline이 없다. Go가 이 둘을 두는 이유는 메모리와 goroutine 종료 경로를 보장하기 위해서다.
  - `net_tcp_agent_so_timeout_ms=0`이면 Java는 read를 무한히 기다린다. Go는 Pack relay를 총 deadline(`RequestTimeout`)으로 끝내고, Blob stream은 read마다 `RequestTimeout`으로 끝낸다.
  - Java Client의 자체 read timeout은 이 문서의 범위 밖이다.

#### 호출자가 먼저 떠난 relay(고아 relay, C4, 2026-10-21)

- 재현(수정 전): 실제 `Runtime`(기본 `RequestTimeout` 30초, Agent so_timeout 60초)에 가짜 V1 Agent를 붙이고 Client가 `OBJECT_HEAPHISTO`를 보낸 뒤 Agent가 40초 뒤 응답하게 했다. 30초에 Client 명령의 무진전 감시(`withProgress`)가 handler context를 끝냈고, 그 context를 받은 `AgentConn.RequestLimited`의 `AfterFunc`가 Agent 소켓을 닫았다(`tcp.connection: "context deadline exceeded"`, 40초 시점 pool에 세션 없음). 큰 응답 명령의 so_timeout+10분 deadline은 Client 경로에서 실효가 없었다. 테스트: `internal/server/agent_pool_orphan_test.go` `TestE2EAgentRelaySlowerThanClientProgressKeepsSession`(수정 전 FAIL, 수정 후 PASS, 약 40초).
- 원본: `AgentCall.call`은 service thread에서 Agent 응답을 끝까지 읽는다(`AgentCall.scala:46-71`). Client가 자기 read timeout(기본 8초, LOGIN 응답의 `so_time_out`)으로 떠나도 서버는 모르고 계속 읽은 뒤 Client write에서 실패하며, Agent 세션은 `finally`의 `TcpAgentManager.add`로 pool에 돌아간다. 그동안 그 worker는 queue에 없으므로 같은 objHash 재요청은 `net_tcp_get_agent_connection_wait_ms` 뒤 null(빈 응답)을 받는다.
- Go(`internal/server/agent_pool_relay.go` `agentPool.relay`, `runtime.go` `request`, `agent_checkout.go` `requestDiscard`): Pack relay는 Agent 연결 수명 context로 실행한다. 호출자 context(Client 진전 상한, Client 이탈, 서버 종료 등. 2026-10-23부터 text cache reset은 호출당 deadline을 두지 않아 고아를 만들지 않는다)가 먼저 끝나면 호출자에게는 즉시 그 오류를 돌려주고, Agent 요청은 고아로 남아 자기 deadline(read마다 so_timeout, 총 so_timeout+30초, 큰 응답 명령 so_timeout+10분)까지 응답을 끝까지 읽어 버린다. 정상 종료면 세션을 pool에 돌려주고(재접속 없음), 오류·timeout이면 Java처럼 세션을 닫는다. 그동안 slot은 busy라 재요청은 원본처럼 빈 세션을 못 찾아 빈 응답이 된다(부하 명령이 다시 가지 않음).
- 상한: 동시 고아 32개, 고아가 들고 있는 relay 예산 합계 512MiB(일반 relay 8MiB, 큰 응답 명령 256MiB, 큰 응답은 전체 relay에서 동시에 2개까지). 2026-10-23부터 이 512MiB는 전역 relay 메모리 예산 안의 몫이다. 고아는 자기 relay가 명령 전에 잡은 permit을 그대로 들고 있으므로 따로 더 예약하지 않는다. 고아가 된 뒤 받은 Pack은 보관하지 않는다. 상한을 넘는 relay는 이전처럼 즉시 Agent 세션을 닫는다(`rejected`). 서버 종료 시 연결 handler가 busy 세션을 닫고 반환을 기다리므로 고아 goroutine이 연결보다 오래 남지 않는다.
- 계수·로그: `agentPool.orphans`, `/health`의 `agentOrphans`(`Active`, `Bytes` 예약 byte, `Orphaned`, `Completed`, `Failed`, `Rejected`). 예약은 wire 기준 item 예산이며 V2 decode 중에는 body와 decode 결과를 함께 들므로 실제 peak는 약 2배다. 로그는 최대 1분에 한 줄 `agent relay <cmd> objHash=<h> answered … after its caller had left; the answer is discarded and the agent session kept (…)` / `… orphan failed …; the agent session is closed` / `… closed: its caller left and the orphan relay limits (32 relays, 512MiB) are full`이며 그 사이 생략한 줄 수와 누적 계수를 함께 쓴다. 주기 stats 줄에는 넣지 않았다.
- Blob stream(`OBJECT_DOWNLOAD_HEAP_DUMP`, `OBJECT_DUMP_FILE_DETAIL`)은 대상이 아니다. Client context를 그대로 써서 Client가 떠나면 Agent 세션을 닫는다. 원본도 handler의 Client write 실패가 `TcpAgentWorker.read`의 catch로 가서 `close()`하므로 같다. 총량 상한이 없는 stream을 고아로 끝까지 읽으면 GB 단위 전송이 남기 때문이다.
- ~~남은 차이: Agent 응답이 `RequestTimeout`(30초)보다 늦으면 Client는 응답 없이 연결이 끝난다.~~ 12단계(2026-10-24)에서 해소했다. 아래 "Agent 대기 중 Client 진전 감시 보류"를 본다.

#### Agent 대기 중 Client 진전 감시 보류(12단계, 2026-10-24)

- 재현(수정 전): Client `so_time_out`이 30초보다 긴 설정(`net_tcp_client_so_timeout_ms` 60000 등)에서 heap histogram·thread dump·jar download처럼 Agent 응답이 30초를 넘는 relay가 원본에서는 성공하고 Go에서는 30초에 응답 없이 Client 연결이 끝났다(실제 원본 Client로 재현, [Java Agent 상호운용](../validation/2026-10-24-java-agent-interop.md) §2.3). 원인은 Client 명령마다 거는 무진전 감시(`withProgress`, `RequestTimeout`)가 Agent를 기다리는 동안에도 돌았기 때문이다.
- 원본: service thread가 Agent 응답을 끝까지 읽는다(`AgentCall.scala:46-66`, `TcpServiceWorker.scala:111-130`). 그동안 Client 이탈을 감지하지 않고, Agent read마다 so_timeout만 있다.
- Go: Pack relay를 기다리는 구간(`internal/service` `awaitAgent` → `transport.Stream.AwaitUpstream`)에서는 무진전 감시를 보류한다(`progressContext.hold`). 단일 대상·fan-out(대상마다 따로 보류, 대상 사이 응답 쓰기는 진전)·변경 relay·응답을 버리는 변경 relay(`OBJECT_RESET_CACHE`, `OBJECT_SYSTEM_GC`)·`CONFIGURE_DESC`/`CONFIGURE_VALUE_TYPE`의 objHash relay(`internal/service/server_metadata.go`)가 대상이다. 일관성을 위해 Client 경로에서 도달하지 않는 `query.go` `OBJECT_ENV`와 `packet_queries.go` `packetBatchRelay`도 같은 helper로 감쌌다(dispatcher가 `HandleAgentReadCommand`를 먼저 불러 `OBJECT_ENV`·`OBJECT_BATCH_ACTIVE_LIST`·`BATCH_ACTIVE_STACK`을 처리하므로, §12 마지막 항목). 대기 상한은 relay 자기 deadline 그대로다(read마다 Agent so_timeout, 총 so_timeout+30초, 큰 응답 명령 so_timeout+10분). 그래서:
  - Agent가 so_timeout 안에 답하면 30초가 지나도 Client가 응답을 받는다. 고아가 생기지 않고 Client 연결과 Agent 세션 모두 다음 명령을 처리한다.
  - Agent가 so_timeout보다 늦으면 원본처럼 Agent 세션을 닫고 Client에는 빈 응답(`NoNEXT`)을 보낸다(Java `AgentCall` null).
  - 보류가 끝나면 진전 시각을 갱신해 일반 감시(30초)를 처음부터 다시 건다.
- Client 이탈 감지(Go 전용): 보류 동안 Client socket을 별도 goroutine이 `bufio.Reader.Peek`으로 probe한다(byte를 소비하지 않음, read deadline 없음). EOF·RST(keepalive 실패의 ETIMEDOUT 포함)면 명령 context를 `transport.ErrClientGone`(`context.Canceled`로도 매칭)으로 즉시 끝내고, relay는 기존 고아 경로로 넘어가 응답을 끝까지 읽고 세션을 유지한다. 원본은 Client 이탈을 응답 write 때에야 알지만 Agent 쪽 결과(세션 유지)는 같다. 대기 중 Client가 보낸 byte는 bufio에 남아 다음 명령으로 읽힌다(Scouter Client는 동기식이라 보통 없다). 대기가 끝나면 probe를 중단하고 reader 소유권과 명령 안 read so-timeout을 되돌린다(`internal/transport/tcp.go` `awaitUpstream`, `probePeer`; 서비스 pool 대기의 probe와 같은 helper).
- 불변: relay 예산·고아 상한(동시 32개·512MiB)·relay deadline. 서버 종료 시 부모 context로 즉시 끝난다(shutdown budget 영향 없음).
- ~~Blob stream(`OBJECT_DOWNLOAD_HEAP_DUMP`, `OBJECT_DUMP_FILE_DETAIL`)은 대상이 아니다. 첫 chunk가 오기 전이나 chunk 사이가 `RequestTimeout`(30초)을 넘으면 Client 명령이 끝나고 Agent 세션이 닫힌다.~~ 13단계(2026-10-25)에서 해소했다. 아래 "Blob stream relay의 Agent 대기 보류"를 본다.
- action log: 대기 중 Client가 떠나면 `Client : <peer> closed`(짧은 형식)를 쓴다. 원본은 Client 이탈을 다음 write/read 때에야 알고 그 예외에 따라 `closed` 또는 `closed <exception> workers=N`을 쓰므로 줄 형식이 다를 수 있다. 연결 오류 통계는 `ClosedErrors`로 센다.
- 남은 위험: FIN·RST 없이 사라진 half-open Client는 probe의 Peek만으로는 알 수 없다. 실제로는 TCP keepalive(Go `net.Listen` 기본: 15초 idle 뒤 15초 간격 probe 9회, 약 2.5분. Go가 Linux·macOS 모두 세 값을 socket에 직접 설정하므로 OS 기본값과 무관하다. 13단계 판단은 아래 "half-open Client와 TCP keepalive")가 실패하면 probe read가 ETIMEDOUT을 받아 `ErrClientGone`으로 끝나는 것이 보통 먼저다. keepalive가 꺼진 환경이라면 상한은 relay deadline이며, 대상 하나 기준(일반 so_timeout+30초, 큰 응답 명령 so_timeout+10분)이다. fan-out은 대상마다 따로 기다리므로 최악은 대상 수 × (so_timeout+30초)다. 그동안 그 Client 세션과 service pool slot이 남는다.
- 검증: `internal/transport/client_upstream_test.go`(RequestTimeout 100~150ms: 보류 중 상한의 4배 대기 뒤 응답 전달, Client close·RST 시 1초 안에 `ErrClientGone`, 대기 중 Client가 보낸 다음 명령 정상 파싱, 서버 종료 시 즉시 종료, 보류 해제 뒤 일반 감시 재개; 보류 없는 이전 동작으로 되돌리면 5개 중 4개 FAIL, 서버 종료는 회귀 방지. 추가 확인: wait panic 뒤 probe 중단·read so-timeout 복원(defer 전 FAIL), 중첩 호출이 바깥 probe를 멈추지 않음(가드 전 FAIL·DATA RACE), 대기 뒤 같은 명령 read에 so-timeout 복원(복원 제거 시 FAIL), 다음 명령 byte를 보낸 뒤 close·RST도 `ErrClientGone`, 짧은 대기 3000회 반복 뒤 goroutine 증가 없음(회귀 방지), RST 이탈의 `ClosedErrors` 1·action log `closed` 짧은 줄(문구 변경 전 FAIL)), `internal/service/agent_wait_test.go`(단일·fan-out·변경·응답을 버리는 변경·`CONFIGURE_DESC`/`CONFIGURE_VALUE_TYPE` relay가 상한의 4배 대기 뒤 응답, Client 이탈 시 1초 안에 Agent 호출 context 종료; wrap 전 FAIL; fan-out 두 번째 대상 대기 중 Client 이탈 → 첫 응답만 전달, 둘째 호출은 `ErrClientGone`, 셋째 대상 미호출(`TestAgentRelayFanOutClientLeavesMidway`, wrap 전 FAIL)), `internal/server/agent_pool_orphan_test.go` 실제 기본값 e2e: `TestE2EAgentRelaySlowerThanClientProgressKeepsSession`(40초 응답을 Client가 받음, 고아 0, 같은 Client 연결·Agent 세션 재사용), `TestE2EAgentRelayPastAgentSoTimeoutAnswersEmpty`(Agent so_timeout 35초 초과 → 35초에 `NoNEXT` 빈 응답·세션 닫힘·고아 0·Client 연결 유지), `TestE2EAgentRelayClientLeavesFirstOrphans`(Client close 뒤 1초 안에 고아, 응답 뒤 `Completed` 1·세션 유지). 세 e2e 모두 wrap 전 FAIL(30.0초에 Client 연결 종료, 고아 미발생). 실제 Java Client·Agent 상호운용은 미검증이다.
- 검증: `agent_pool_orphan_test.go` — 고아가 응답을 끝까지 읽고 같은 세션 재사용·고아 중 재요청은 빈 세션(`TestAgentPoolOrphanReadsToEndAndKeepsSession`), read timeout이면 세션 종료(`…ReadTimeoutClosesSession`), 개수·byte 상한 초과 시 즉시 종료와 로그(`…LimitsCloseAtOnce`), `requestDiscard` 호출자 deadline(`TestAgentPoolDiscardOrphanKeepsSession`), 서버 종료 시 즉시 정리(`…EndsOnShutdown`), 실제 기본값 40초 e2e. 실제 Java Agent로 Client가 먼저 떠난 경우(응답 6초), RequestTimeout(30초)보다 늦은 응답(40초, 고아가 끝까지 읽고 세션 유지, 중간 재요청은 빈 응답), 단일 대상 relay 중 socket 끊김(빈 응답, 1초 내 재연결)을 재현했다([Java Agent 상호운용](../validation/2026-10-24-java-agent-interop.md) §3).

#### Blob stream relay의 Agent 대기 보류(13단계, 2026-10-25)

- 재현(수정 전): 실제 `Runtime`(기본 `RequestTimeout` 30초, Agent so_timeout 60초)에서 `OBJECT_DOWNLOAD_HEAP_DUMP`의 첫 chunk가 35초 뒤 오면 30초에 Client 연결이 `NoNEXT` 없이 끝나고 Agent 세션이 닫혔다(`TestE2EHeapDumpFirstChunkAfterClientProgressLimit` 수정 전 FAIL: `Client command ended before the first chunk: EOF`). Agent so_timeout을 35초로 둔 경우에도 Agent read timeout(35초)보다 Client 진전 상한(30초)이 먼저 끝냈다(`TestE2EHeapDumpPastAgentSoTimeoutClosesSession` 수정 전 FAIL: `Client end after 30.01s`). 대기 중 Client가 연결을 닫아도 아무도 Client socket을 읽지 않아 30초까지 Agent 세션을 붙잡았다(`TestE2EHeapDumpClientLeavesClosesAgentSession` 수정 전 FAIL).
- 원본: `HeapDumpService.downloadHepDump`·`DumpService.getDumpFileDetail` handler는 `inx.readByte()`/`readBlob()`마다 Agent so_timeout만 받고, 받은 chunk를 Client에 바로 쓴다(`HeapDumpService.scala:58-71`, `DumpService.scala:119-131`, `TcpAgentWorker.scala:15-18, 69-77`). 총 시간·Client 쪽 write timeout은 없다.
- Go(`internal/service/agent_queries.go` `agentReadQuery`): Blob relay 호출 전체를 `transport.Stream.AwaitUpstream`으로 감싼다. chunk read 구간만 보류하는 방식(read는 보류 안, write는 밖)도 검토했지만 택하지 않았다. chunk read는 transport `AgentConn.RequestLimited` 루프 안(flag read 포함)이라 그 경계마다 보류를 켜고 끄려면 transport API를 늘려야 하고, write에 일반 진전 감시를 다시 걸면 진전 갱신이 item 하나(2MiB chunk)를 다 쓴 뒤에만 일어나 64KiB 조각마다 진전하는 느린 Client(약 70KB/s 미만)를 30초에 끊는다. 보류 안에서도 모든 대기에 개별 상한이 있다.
  - Agent 대기: read마다 so_timeout(0이면 `RequestTimeout`). 총 deadline 없음(이전과 같음, 원본과 같음).
  - Client write: 64KiB 조각마다 `RequestTimeout` write deadline(`progressWriter`). 읽지 않는 Client는 보류 중에도 끝난다(`TestAgentBlobRelayStalledClientEndsRelay`, 수정 전후 PASS).
  - Client 이탈: probe가 EOF·RST를 즉시 감지해 ctx를 `ErrClientGone`으로 끝내고, `RequestLimited`의 `AfterFunc`가 Agent 세션을 닫는다. Blob stream은 고아로 만들지 않는다(총량 상한이 없는 stream을 아무도 받지 않는데 끝까지 읽지 않음). 원본은 다음 Client write 실패 때 `TcpAgentWorker.read`의 catch로 close하므로 Agent 쪽 결과(세션 종료)는 같고 시점만 이르다.
  - 경합: probe goroutine은 Client reader(`bufio.Reader`·`idleReader`)만, handler goroutine은 writer(`bufio.Writer`·`progressWriter`)만 쓴다. `net.Conn`의 동시 read/write는 안전하다. `-race`로 확인했다.
- 결과: 첫 chunk 전·chunk 사이 대기가 30초를 넘어도 Agent so_timeout 안이면 전달되고, Agent 세션은 pool로 돌아간다. so_timeout을 넘으면 원본처럼 Agent 세션을 닫는다. 이미 쓴 chunk 뒤에 `NoNEXT`를 쓰지 않는 Go의 의도된 차이(§12 "의도된 차이(Blob stream)")는 그대로다. 작은 chunk는 다음 flush(200ms 간격, 응답 끝, 오류)까지 bufio(4KiB)에 남을 수 있다. 원본도 `BufferedOutputStream`이라 같은 성격이다.
- 남은 위험(성능, 호환성 결함 아님): Client 진전 감시를 stream 전체에서 보류하므로 Blob 다운로드를 끊는 느린 Client 하한이 낮아졌다. 이전에는 chunk(2MiB) 하나를 `RequestTimeout`(30초) 안에 다 써야 해 약 70KB/s였고, 이제는 64KiB write 조각마다 `RequestTimeout`이라 이론값은 약 2.2KB/s다. 실제로는 Client 수신 window가 다시 열릴 때까지 조각 write가 기다리므로 그보다 높다(macOS loopback에서 deadline당 64KiB×4 읽기로는 deadline을 넘김, 테스트 `throttledReader` 주석). 그래서 수 GB heap dump를 매우 느린 Client가 받으면 그동안 Agent 세션 1개, relay 예산 permit 16MiB, service pool slot 1개를 오래 점유한다. 원본도 Client write에 상한이 없어 thread와 Agent 세션을 무기한 잡으므로 Go가 더 느슨해진 것은 아니다. 확인 지표: `/health` `agentRelayBudget`(`Bytes`·`Waiting`·`Refused`), `tcp`의 `ClientSessions`·`ClientWaiting`·`ClientWaitTimeouts`(service pool이 차면 대기·거절 증가), Agent 세션 수.
- 검증: `internal/service/agent_blob_wait_test.go`(`RequestTimeout` 100ms: 두 Blob 명령에서 첫 chunk·chunk 사이 400ms 대기 뒤 전부와 `NoNEXT` 전달·다음 명령 정상, wrap 전 FAIL `flag=0 want=3 err=EOF`; 다음 chunk 대기 중 Client close → 1초 안에 `ErrClientGone`, wrap 전 FAIL; 읽지 않는 Client → 3초 안에 write deadline(`os.ErrDeadlineExceeded`)으로 relay 종료, 회귀 방지; `RequestTimeout` 300ms에서 64KiB씩 10ms 간격으로 읽는 느린 Client가 8MiB chunk(약 1.6초, 상한의 5배)와 다음 chunk·`NoNEXT`를 받음, wrap 전 FAIL `flag=0 want=3 err=EOF`, 수정 후 `-race -count=10` PASS), `internal/server/agent_blob_e2e_test.go`(실제 기본값: 35초 뒤 첫 chunk 전달·세션 유지, so_timeout 35초 초과 → 34~45초에 Client EOF·세션 제거, Client close → 1초 안에 Agent 세션 EOF·고아 0·relay 예산 `Bytes` 0 복귀. 세 개 모두 wrap 전 FAIL, 수정 후 `-race -count=2` PASS). 실제 Java Client·Agent 상호운용은 미검증이다.

#### half-open Client와 TCP keepalive(13단계 판단, 2026-10-25)

- 원본: 서버는 accept한 socket에 `setKeepAlive`를 하지 않는다(`TcpServer.scala:40-43`은 `setSoTimeout`만). Java Client는 자기 쪽에 `setKeepAlive(true)`를 켜지만 OS 기본(Linux 7200초 idle)이라 실효가 거의 없다(`ClientTCP.java:54`). 원본 service thread는 Client 상태와 무관하게 Agent 응답을 끝까지 기다리므로 half-open이든 정상 종료든 같은 시간 동안 thread를 잡는다.
- Go 현재: `internal/server/runtime.go`의 `net.Listen`이 accept한 모든 TCP 연결에 keepalive를 켠다. Go 1.23+는 idle·간격·횟수를 socket에 직접 설정한다(macOS에서 accept한 socket의 `SO_KEEPALIVE` on, `TCP_KEEPALIVE` 15, `TCP_KEEPINTVL` 15, `TCP_KEEPCNT` 9를 getsockopt로 확인. 같은 호스트 sysctl 기본은 7200000/75000ms/8. Linux는 Go `tcpsockopt_unix.go`가 `TCP_KEEPIDLE`/`TCP_KEEPINTVL`/`TCP_KEEPCNT`를 같은 값으로 설정하는 것을 코드로만 확인, 미측정). 서버가 쓰지 않는 대기 중 half-open 감지는 약 15+15×9=150초다. 보낸 data가 ACK되지 않고 남으면 keepalive 대신 재전송 timeout이 적용되지만, Blob relay에서는 send buffer가 차면 write 조각 deadline(30초)이, Agent가 멈추면 Agent so_timeout이 먼저 끝낸다.
- 판단: **명시 설정을 하지 않는다(보류).** (1) Go는 이미 원본보다 이르게 끝낸다(FIN·RST 즉시, half-open 약 150초 또는 relay deadline 중 먼저). (2) 보류가 길어지는 경우는 Client so_time_out을 기본 8초보다 크게 둔 설정뿐이다. 기본 Java Client는 8초 뒤 스스로 연결을 닫으므로(FIN) probe가 즉시 감지한다. (3) 일반 relay deadline(so_timeout+30초=90초)이 150초보다 짧아 keepalive를 낮춰도 이득은 큰 응답 명령(so_timeout+10분)과 fan-out(대상 수×90초)의 드문 half-open뿐이다. (4) 같은 listener의 Agent 연결까지 바꾸지 않으려면 Client 연결마다 따로 설정해야 하고, 짧은 probe는 손실이 있는 망에서 살아 있는 Client를 끊을 위험이 있다. 운영에서 half-open으로 service pool slot이 오래 남는 사례(`/health` `tcp.ClientSessions`가 줄지 않음)가 확인되면 Client 연결에만 `TCPConn.SetKeepAliveConfig`(예: idle 15초, 간격 10초, 3회 → 약 45초)를 상수로 거는 것을 다시 검토한다.
- 측정: loopback에서 half-open을 만들려면 packet filter(pf/iptables)가 필요해 이번에는 실제 감지 시간을 재지 않았다(상대 프로세스 SIGSTOP은 커널이 ACK하므로 half-open이 아니다). Linux 측정 절차 후보: Client 연결 후 `iptables -A INPUT -p tcp --sport <client port> -j DROP`(서버 쪽에서 Client 패킷 차단) → 30초를 넘는 Agent 대기 명령 → 서버 action log `Client : <peer> closed`와 `ErrClientGone` 시각 측정(예상 약 150초).

## 8. 결론: 서버 결함이 애플리케이션에 영향을 주는 경로(가능성 순)

| 순위 | 경로 | 앱 영향 | Go 서버가 막는가 | 남은 위험 |
|---|---|---|---|---|
| 1 | Client 요청 relay로 전달되는 부하·위험 명령(`OBJECT_HEAPHISTO`, `OBJECT_THREAD_DUMP`, `OBJECT_CLASS_LIST`, `OBJECT_DOWNLOAD_JAR`, `OBJECT_FILE_SOCKET`, 그리고 2026-10-16부터 relay하는 변경 명령 21종: heap dump·GC·thread control·`SET_CONFIGURE_WAS`·`REDEFINE_CLASSES`·TRIGGER_* 등) | STW 정지, 힙 할당, 프로세스 fork | 부분. 인증된 Client만 보낼 수 있다. 읽기·변경 명령 모두 Java 서버와 같이 전달한다(`internal/service/agent_queries.go` `AgentReadCommands`, `agent_mutations.go` `AgentMutationCommands`). 요청 빈도 제한은 없다 | Client 계정이 있는 사용자의 반복 호출. Java 서버와 동일한 위험이며 Go 전환으로 새로 생기는 것은 아니다 |
| 2 | 거대 프레임 길이 → agent `new byte[len]` | GC 압박, OOM 전파 | 예. Go는 본문을 먼저 만들고 명령의 item 상한(8MiB/256MiB) 안에서 길이를 쓴다(`stream.go` `WriteIntBytes`). 길이와 본문이 불일치하는 프레임을 만드는 코드 경로가 없다 | Go `Stream`/encoder의 회귀 버그. 2026-10-24부터 `FuzzAgentRequestFrame`·`TestAgentRequestFrameAtRelayLimit`이 "선언 길이 == 실제 길이, 길이+4 ≤ item 상한, 거절 시 wire 0 byte(V1·V2)"를 고정한다(§2 끝) |
| 3 | 명령별 응답 상한(§7(d): blob chunk 16MiB, 큰 `MapPack` 256MiB, 그 밖 8MiB/4096) 초과로 인한 소켓 종료 | stderr stack trace, 세션 재접속 | 의도된 제한. heap dump·dump 파일은 chunk 단위로 중계해 크기 제한이 없다 | 256MiB를 넘는 jar·heap histogram·thread dump는 Go 서버로 받을 수 없다. 고아 relay 상한(동시 32개·512MiB)을 넘으면 이전처럼 세션을 닫는다(§7(d)) |
| 4 | `net_collector_tcp_session_count > 8`인 agent의 무한 재접속 | 초당 소켓·버퍼 garbage, 서버 TIME_WAIT, agent 쪽 무로그 | 아니오(의도된 제한이지만 agent 쪽 증상이 조용함) | 해당 설정 agent 존재 여부 사전 확인 |
| 5 | 잘못된 Pack type/파라미터 타입 → handler `ClassCastException` | 요청당 stderr stack trace 1건 | 예. Go는 항상 `MapPack`을 보낸다 | 없음 |
| 6 | 알 수 없는 명령 → Agent 로그 1줄/요청(A130, 기본 stdout) | stdout I/O | 예. Go는 고정 명령만 보내고 relay는 인증 필요 | Agent에 handler가 없는 명령을 relay하면 Agent 로그가 요청마다 1줄 늘어난다. Java 서버와 동일 |
| 7 | `OBJECT_RESET_CACHE` 재전송 | lazy TextPack 재전송, 큐 1024 초과 시 텍스트 유실 | Java와 동일 시점(30초 후·날짜 변경). 그 pass에서 놓친 agent에는 같은 날 연결되면 다시 보낸다(§4, 의도된 차이, objHash당 최대 5회) | agent/서버 TZ 불일치 시 하루 2회. 재시도로 놓친 agent에 한 번 더. 데이터 품질 문제로 앱 영향 없음 |
| 8 | KEEP_ALIVE 부재/느린 응답/서버 부재 | 60초 또는 4초 주기 재접속, UDP는 송신 후 버려짐 | 해당 없음(agent 설계가 fire-and-forget) | 없음 |

Go가 스스로 agent에 보내는 명령은 `KEEP_ALIVE`, `OBJECT_RESET_CACHE`, `OBJECT_SET_KUBE_SEQ`(kube pod seq 지정 시), `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`(agent가 켠 CPU autodump 조건 충족 시)이며 모두 Java 서버도 같은 조건에서 보낸다(§3). 앞의 셋은 handler 부하가 없다. `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`는 thread dump와 파일 생성 부하가 있지만 agent 설정(`ADC` tag)이 켠 경우에만 가고, agent 쪽 queue(2)가 넘치면 버린다. 뒤의 둘은 2026-10-24에 실제 Agent로 확인했다(원본 파라미터·가드 그대로, [검증 §2.8](../validation/2026-10-24-java-agent-interop.md)). 따라서 **Go 서버 자체의 결함이 애플리케이션 스레드를 멈추거나 메모리를 소모시키는 경로는 "프레임 길이 불변식 위반"(2번) 하나**이며 현재 코드는 이를 구조적으로 막는다. 나머지는 Client 조작에 의한 것(1, 3, 6)이거나 agent 설정에 의한 것(4, CPU autodump)이다.

## 9. Agent 자체 결함(서버 무관, 참고)

- `DataUdpAgent.openDatagramSocket` 실패(`net_local_udp_port` 충돌 등) 시 `datagram == null`인데 `write`는 `IOException`만 잡아 `NullPointerException`이 호출자로 전파된다(`DataUdpAgent.java:84-86, 109-113`). `sendDirect`도 `IOException`만 잡는다(`DataProxy.java:214-219`). 트랜잭션 스레드에서는 `TraceMain:702-704`의 Throwable catch로 흡수되지만, `UDPDataSendThread.run`에는 catch가 없어 배치 송신 스레드가 종료될 수 있다(`UDPDataSendThread.java:73-91`). 미재현.
- `TcpWorker.run`과 `TcpRequestMgr.run`의 Throwable 무시로 TCP 쪽 장애는 agent 로그에 전혀 남지 않는다. 서버 전환 후 agent 상태 확인은 서버 로그(`tcp.connection` 이벤트)와 live 연결 수로만 가능하다.

## 10. 미검증·재현 fixture 후보

2026-10-24 실제 Java Agent 시험에서 4번(응답 상한 초과)을 재현했다. 1번은 같은 날 Go 테스트로 고정했다. 나머지는 실행하지 않았다(소스 정적 분석). 테스트로 옮길 수 있는 항목:

1. **프레임 길이 불변식(Go)**: `AgentConn.Request`에 8MB 근처·초과 파라미터를 넣어 (i) 초과 시 아무 바이트도 쓰지 않고 `ErrLimit`, (ii) 이하 시 선언 길이 == 실제 본문 길이를 단언. fuzz 대상: `Stream.WriteIntBytes`, `protocol.Encoder`. **완료(2026-10-24)**: `internal/transport/agent_frame_fuzz_test.go`(`FuzzAgentRequestFrame` 60초 PASS, `TestAgentRequestFrameAtRelayLimit`, mutation 확인은 [검증 §8](../validation/2026-10-24-java-agent-interop.md)).
2. **Java agent의 frame 길이 미검증(원본 Agent 한계)**: Agent는 서버가 선언한 V2 frame 길이를 상한 없이 그대로 할당한다(§2). 실행 재현은 하지 않았다. Go 서버 쪽 보장은 1번의 `FuzzAgentRequestFrame`·`TestAgentRequestFrameAtRelayLimit`이다.
3. **session_count > 8 재접속 루프**: agent `net_collector_tcp_session_count=10`으로 Go에 접속 → Go `/health` `tcp.LimitErrors`가 초당 2씩 늘고(`tcp.connection ErrLimit` 이벤트는 10초에 1건, `(<n> more limit errors since the last line)`의 n이 약 20), agent `LIVE.size()`가 8에 수렴하는지 확인.
4. **응답 상한 초과**: 9MB 파일을 `dump_dir`에 두고 `OBJECT_DUMP_FILE_DETAIL` relay → Go가 소켓을 닫고 agent stderr에 `IOException` trace 1건, 1초 내 재접속. Java 서버 대비 기능 차이 기록. **재현 결과(2026-10-24)**: 9MiB 응답은 Go가 거절하고 세션을 닫았고 Agent는 0.9초 뒤 재연결했다. Agent stderr에 stack trace는 없었다(7MiB는 정상 전달).
5. **OBJECT_RESET_CACHE 재전송량**: reset 직후 고유 SQL 2000개를 실행하는 replay에서 서버가 받은 `TextPack(SQL)` 수 == 2000(sendDirect 경로는 유실 없음), service name 2000개 100ms 내 → 1024 초과분 유실 여부 확인.
6. **KEEP_ALIVE 왕복**: Go idle 5~10초마다 `KEEP_ALIVE` + agent `NoNEXT` 1바이트 응답을 pcap으로 확인, 60초 so_timeout 재접속이 발생하지 않음을 확인(Phase 2D 통합 검증 포함 여부는 미확인).


## 11. 2026-10-16 갱신: Agent 변경 명령 relay

관리 명령 구현([management-commands](management-commands.md))으로 §3 표의 "차단"이던 변경 명령 21종(`OBJECT_CALL_{HEAP_DUMP,BLOCK_PROFILE,MUTEX_PROFILE,CPU_PROFILE}`, `OBJECT_DELETE_HEAP_DUMP`, `OBJECT_RESET_CACHE`, `OBJECT_SYSTEM_GC`, `OBJECT_THREAD_CONTROL`, `PSTACK_ON`, `TRIGGER_{ACTIVE_SERVICE_LIST,HEAPHISTO,THREAD_DUMP,THREAD_LIST,BLOCK_PROFILE,MUTEX_PROFILE}`, `SET_CONFIGURE_WAS`, `REDEFINE_CLASSES`, `CUBRID_SET_ALERT_CONFIGURE`, `DB_KILL_PROCESS`, `SET_QUERY_INTERVAL`, `USE_DATABASE`)은 Java 서버와 같이 인증된 Client 요청을 Agent에 그대로 전달한다. Java와 같이 서버는 group policy를 검사하지 않으며(Client가 검사), Go는 요청마다 명령·objHash·session·peer를 로그에 남긴다(`internal/service/agent_mutations.go`). 따라서 §8 표의 1번 경로(Client 조작에 의한 부하)는 이제 변경 명령까지 포함하며, 이는 Java 서버와 동일한 위험이다. Go가 스스로 보내는 명령은 이 relay와 별개로 §8 끝의 네 가지(`KEEP_ALIVE`, `OBJECT_RESET_CACHE`, `OBJECT_SET_KUBE_SEQ`, `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`)다. (2026-10-21 정정: 이전 판은 앞의 둘뿐이라고 적었으나 `internal/ingest/agent_calls.go`가 뒤의 둘도 보낸다.)

## 12. 2026-10-21 갱신: 여러 agent에 보내는 조회 relay의 실패 처리

대상: `OBJECT_ACTIVE_SERVICE_LIST_GROUP`(objHash 목록)과 `objHash == 0`이고 `objType`이 있는 `OBJECT_ACTIVE_SERVICE_LIST`/`OBJECT_BATCH_ACTIVE_LIST`(그 type의 live agent 전체).

- 원본: `TcpAgentWorker.write/readByte/readPack`은 I/O 오류(read timeout 포함)를 모두 잡아 소켓을 닫고 0/null을 돌려주므로 `AgentCall.call`은 예외 없이 null을 반환한다(`TcpAgentWorker.scala:21-68`, `AgentCall.scala:46-71`). handler는 null이면 `{objHash}`만 담은 `MapPack`을 쓰고 다음 agent로 넘어간다(`ThreadList.scala:98-111, 131-146`, `BatchService.scala:110-123`). 즉 실패한 agent도 결과에 placeholder 하나를 남기며 연결이 없는 agent와 구분되지 않는다.
- Go(`internal/service/agent_queries.go` `agentReadQuery`): 이전에는 agent 하나의 `AgentRequest` 오류로 요청 전체를 끝냈다(Client는 `NoNEXT` 없이 끊긴 연결을 받음). 이제 원본과 같이 그 agent 자리에 `{objHash}` placeholder를 쓰고 나머지 agent를 계속 부른다. 건너뛴 수는 프로세스 누적 계수(`agentRelaySkips`)로 세고 로그 `service: <cmd> skipped objHash=… after agent failure …`는 최대 1분에 한 줄(그 사이 건너뛴 수를 함께 표기)이다.
- 그대로 요청 전체를 끝내는 오류: 요청 context 취소·만료, Go relay 크기 상한(`ErrLimit`, Agent 세션을 닫고 Client 요청을 실패시키는 §7(d) 정책), agent 호출이 없는 backend(`ErrUnsupported`), Client stream 쓰기 오류. Agent 응답을 decode하지 못한 경우(알 수 없는 Pack·Value type)는 2026-10-21부터 `ErrUnsupported`가 아니라 Agent 실패로 바꿔 원본 readPack 예외처럼 null로 처리한다(`TestAgentPoolUndecodableAnswerIsAgentFailure`).
- 남은 차이: (1) 원본은 응답 중간에 끊기면 그때까지 읽은 마지막 `MapPack`을 쓸 수 있으나(`readByte` 실패 시 직전 `readPack` 결과가 남음) Go pool은 오류 시 Pack을 돌려주지 않아 placeholder만 쓴다. (2) (2026-10-21 해소) 대상이 agent 하나인 relay도 아래 "단일 대상 relay"처럼 원본과 맞췄다. (3) (2026-10-24 해소, 12단계) fan-out 중 응답 없이 멈춘 agent가 있으면 Client 명령의 30초 무진전 감시(`RequestTimeout`)가 agent 요청 deadline(so_timeout+30초)보다 먼저 요청 전체를 끝냈다. 이제 대상마다 Agent를 기다리는 동안 그 감시를 보류하므로(§7(d) "Agent 대기 중 Client 진전 감시 보류") 원본처럼 so_timeout 뒤 그 agent를 skip(placeholder)하고 다음 대상으로 간다. Client가 자기 read timeout으로 연결을 닫으면 그 시점에 요청이 끝나고 진행 중인 relay는 고아가 된다.
- 검증: `internal/service/agent_queries_test.go` `TestAgentReadFanOutSkipsFailedAgent`(3개 중 1개 실패 → 3개 응답, 실패 자리는 placeholder만, 로그 rate-limit), `TestAgentReadFanOutStopsOnRequestErrors`(취소·`ErrLimit`·`ErrUnsupported`는 fan-out과 단일 대상 모두 중단). 실제 Java agent 상호운용은 미검증.

### 단일 대상 relay의 agent 실패(2026-10-21)

- 원본: objHash 하나에 보내는 Pack relay handler는 모두 `AgentCall.call(o, cmd, param)`을 부르고, agent I/O 실패(read timeout 포함)는 `TcpAgentWorker`가 삼켜 세션을 닫고 null을 돌려준다(`TcpAgentWorker.scala:21-68`, `AgentCall.scala:46-71`). handler는 null을 연결 없음과 똑같이 다룬다.
  - 아무것도 쓰지 않음(`if (p != null)`): `HOST_TOP`·`HOST_PROCESS_DETAIL`·`HOST_DISK_USAGE`·`HOST_NET_STAT`·`HOST_WHO`·`HOST_MEMINFO`(`HostService.scala:29-105`), `OBJECT_ENV`·`OBJECT_CLASS_DESC`·`OBJECT_CLASS_LIST`·`OBJECT_HEAPHISTO`·`OBJECT_FILE_SOCKET`·`OBJECT_SOCKET`·`OBJECT_LOAD_CLASS_BY_STREAM`·`REDIS_INFO`·`OBJECT_CHECK_RESOURCE_FILE`·`OBJECT_DOWNLOAD_JAR`(`AgentInfo.scala:113-296`), `OBJECT_THREAD_DETAIL`·`OBJECT_THREAD_LIST`·`OBJECT_THREAD_DUMP`·`OBJECT_THREAD_CONTROL`(`ThreadList.scala:67-171`), `BATCH_ACTIVE_STACK`(`BatchService.scala:145-149`), `GET/SET/LIST_CONFIGURE_WAS`·`REDEFINE_CLASSES`(`ConfigureService.scala:80-131`), DB 계열 15개(`DatabaseService.scala:46-258`), `CUBRID_GET/SET_ALERT_CONFIGURE`(`CubridService.scala:351-366`), `TRIGGER_*`·`OBJECT_DUMP_FILE_LIST`·`DUMP_APACHE_STATUS`(`DumpService.scala:39-149`), heap dump·profile 호출(`HeapDumpService.scala:38-117`), `DEBUG_AGENT`(`ServerDebug.scala:45-48`), `PSTACK_ON`(`StackAnalyzerService.scala:92-95`).
  - 결과를 버림: `OBJECT_RESET_CACHE`(`AgentInfo.scala:147-156`), `OBJECT_SYSTEM_GC`(`DumpService.scala:133-139`).
  - `{objHash}` placeholder: objHash를 지정한 `OBJECT_ACTIVE_SERVICE_LIST`(`ThreadList.scala:113-124`), `OBJECT_BATCH_ACTIVE_LIST`(`BatchService.scala:125-136`).
- Go(`internal/service/agent_queries.go` `agentReadQuery`, `agent_mutations.go` `agentMutation`): 이전에는 단일 대상 relay의 agent 오류로 요청을 실패시켜 Client가 `NoNEXT` 없이 끊긴 연결을 받았다. 이제 fan-out과 같은 `skippableAgentError` 규칙으로 agent 오류를 Java의 null로 바꿔, 위 분류대로 빈 응답(`NoNEXT`만) 또는 placeholder로 정상 종료한다. 계수 `agentRelaySkips`와 1분 1줄 로그(`service: <cmd> skipped objHash=… after agent failure, answered no item|the {objHash} placeholder like Java's null AgentCall result …`)는 fan-out과 공유한다.
- 그대로 요청을 실패시키는 오류: 요청 context 취소·만료(Client 진전 상한 포함), Go relay 크기 상한(`ErrLimit`), agent 호출이 없는 backend(`ErrUnsupported`), Client stream 쓰기 오류.
- 의도된 차이(Blob stream): `OBJECT_DOWNLOAD_HEAP_DUMP`·`OBJECT_DUMP_FILE_DETAIL`은 원본에서 handler 예외를 `TcpAgentWorker.read`가 삼키므로(`TcpAgentWorker.scala:69-77`) 중간에 끊겨도 그때까지의 chunk 뒤에 `NoNEXT`가 가고, Client(`XLogProxy.getHeapdumpByteArray`)는 잘린 hprof 파일을 성공으로 저장한다. Go는 이를 결함 후보로 보고 기존대로 `NoNEXT` 없이 끝내 Client가 실패로 보게 한다(`TestAgentReadPartialBlobBeforeUpstreamFailure`).
- `CONFIGURE_DESC`·`CONFIGURE_VALUE_TYPE`의 objHash≠0 경로(`internal/service/server_metadata.go:208-215`)도 같은 규칙을 따른다. 원본 `ConfigureService.scala:176-183, 199-206`과 같이 agent 오류를 null로 보고 아무것도 쓰지 않는다(`skippableAgentError`, `noteAgentRelaySkip`, `TestAgentReadSingleTargetFailureIsJavaNull`). (2026-10-24 정정: 이전 판은 이 경로가 요청을 실패시킨다고 적었지만 현재 코드는 skip한다.) `query.go`의 `OBJECT_ENV`, `packet_queries.go` `packetBatchRelay`는 dispatcher가 `HandleAgentReadCommand`를 먼저 부르므로 Client 경로에서 쓰이지 않는다.
- 검증: `internal/service/agent_queries_test.go` `TestAgentReadSingleTargetFailureIsJavaNull`(single-target read relay 40개·`OBJECT_ENV`·mutation 21개 전부: agent 오류 → 빈 응답 또는 placeholder, 같은 연결로 계속), `TestAgentReadFailureBoundaries`(Blob 오류·`ErrLimit`은 실패 유지), `agent_mutations_test.go`(`agent failure` → 빈 응답, `relay limit` → 실패), `service_test.go` `TestAgentRelayAndError`. 실제 Java Client 화면 확인은 미검증.

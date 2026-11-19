# Agent 읽기 relay 계약

정본은 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. `query-scope.json`의 `agent_read_relay` 43개와 기존 `OBJECT_ENV`를 처리하며 management mutation 45개는 이 recognizer에 포함하지 않는다. 아래 경로는 읽기 전용 Scouter 원본 기준이다.

## 공통 요청·응답

모든 요청 body는 MapPack이며 scalar target key는 `objHash`이다. 부가 키/Value/entry 순서를 그대로 upstream에 전달한다. 누락 scalar hash는 0이다. `AgentCall.scala:40–72`는 등록 ObjectPack 또는 TCP 연결이 없으면 null을 반환한다. 응답 Pack을 끝까지 읽은 후 **마지막 Pack이 MapPack일 때만** 하나를 반환한다. 앞선 MapPack이 있어도 마지막 Pack이 다른 형식이면 반환하지 않는다. 일반 명령의 null은 response item 0개이며, transport dispatcher가 최종 NoNEXT를 작성한다. Backend 오류/cancel은 성공 응답으로 숨기지 않는다. 등록 객체 확인과 offline connection 처리는 Engine/Agent pool이 소유한다.

`OBJECT_DUMP_FILE_DETAIL`과 `OBJECT_DOWNLOAD_HEAP_DUMP`는 `AgentCall` callback overload(`AgentCall.scala:74–91`)를 사용한다. 각 `HasNEXT + Blob` chunk를 같은 순서로 Client에 반환하며 MapPack wrapper나 Agent V2 intBytes wrapper를 붙이지 않는다. `DumpService.scala:118–130`, `HeapDumpService.scala:57–69`가 근거다. upstream framing 오류가 나도 완전히 읽은 chunks는 반환한다. Client에 해당 chunks를 쓴 뒤 오류로 연결을 닫으며 성공 NoNEXT를 보내지 않는다. Go는 제한된 chunk snapshot을 사용하므로 원본의 chunk마다 즉시 전달하는 timing과는 다르다.

## 분기·순서

- `OBJECT_THREAD_DETAIL`: `ThreadList.scala:60–72`의 `containsKey("id")`와 같이 id 키가 없으면 upstream call을 하지 않는다. 키가 존재하고 ValueNull이면 call한다.
- `OBJECT_ACTIVE_SERVICE_LIST`, `OBJECT_BATCH_ACTIVE_LIST`: scalar hash가 nonzero면 한 객체를 조회한다. 0이면 objType이 null인 경우 응답 0개, 그 외 `AgentManager.scala:256–265`의 type별 alive 목록 순서로 조회한다. 각 null 응답은 `{objHash: hash}` MapPack으로 반환하며 Map 응답의 objHash를 교체/추가한다. `ThreadList.scala:88–126`, `BatchService.scala:100–137`.
- `OBJECT_ACTIVE_SERVICE_LIST_GROUP`: objHash List 순서와 중복을 유지한다. upstream command를 `OBJECT_ACTIVE_SERVICE_LIST`로 바꾸며 원래 List-bearing MapPack을 그대로 보낸다. 응답 fallback/objHash 장식은 위와 같다. `ThreadList.scala:128–146`.
- LinkedHashMap 교체는 해당 entry 위치를 보존한다. backend snapshot을 clone한 후 장식하므로 다른 target과 호출자의 MapPack을 변경하지 않는다.
- `OBJECT_SOCKET` Java 상수의 실제 literal은 `SOCKET`이다(`scouter.common/src/main/java/scouter/lang/conf/RequestCmd.java:82`). `OBJECT_DOWNLOAD_JAR`, `OBJECT_LOAD_CLASS_BY_STREAM`은 이름과 달리 서버 handler 기준 마지막 MapPack relay이다.

## API·경계

`AgentReadCommands()`는 43개 literal의 새 slice를 반환한다. `Service.HandleAgentReadCommand(ctx, transport.ClientRequest, *transport.Stream) (recognized bool, err error)`는 미인식 명령의 body를 읽지 않는다. 인식 명령은 auth 확인 후 MapPack을 읽는다. 기존 `AgentBackend.AgentRequest`와 optional `AgentBlobBackend.AgentBlobRequest(ctx, hash, command, param) ([][]byte,error)`를 사용한다. Blob interface만 가진 backend도 Blob 명령을 처리할 수 있다. dispatcher는 Batch packet handler/기존 OBJECT_ENV 분기보다 이 recognizer를 먼저 호출한다.

기존 protocol의 allocation/length guard, transport frame limit과 함께 target/response items는 4096개로 제한한다. Blob byte 합계/Pack byte 합계는 Agent pool 및 Client Stream의 8MiB 기본 상한이 적용된다. Int32 범위를 벗어난 hash, scalar/list 타입 오류는 malformed로 종료한다. 원본 `MapPack.getInt`는 Number를 int로 변환하고 다른 타입은 0을 반환하지만 여기서는 기존 Go parameter guard를 유지한다. Group의 누락/null objHash는 원본 `MapPack.getList`(`MapPack.java:190–192`)와 `ThreadList.scala:131–132`의 null dereference 대신 malformed로 종료한다. 이러한 guard는 원본의 무제한/타입 cast 예외를 안전하게 종료하는 의도된 차이다. 원본 registry enum의 완전한 해시 순서 재현은 이 helper 범위 밖이며 backend가 제공하는 객체 순서를 보존한다.

## 명령 매핑

| wire literal | 원본 handler 경로·줄 | body/response |
|---|---|---|
| `ACTIVE_QUERY_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:41–52` | MapPack → final MapPack/null |
| `BATCH_ACTIVE_STACK` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/BatchService.scala:140–151` | MapPack → final MapPack/null |
| `CUBRID_GET_ALERT_CONFIGURE` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/CubridService.scala:346–357` | MapPack → final MapPack/null |
| `DB_EXPLAIN_PLAN` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:118–129` | MapPack → final MapPack/null |
| `DB_PROCESS_DETAIL` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:106–117` | MapPack → final MapPack/null |
| `DB_PROCESS_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:130–141` | MapPack → final MapPack/null |
| `DB_VARIABLES` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:142–153` | MapPack → final MapPack/null |
| `DEBUG_AGENT` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ServerDebug.scala:38–51` | MapPack → final MapPack/null |
| `DUMP_APACHE_STATUS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DumpService.scala:141–152` | MapPack → final MapPack/null |
| `EXPLAIN_PLAN_FOR_THREAD` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:238–249` | MapPack → final MapPack/null |
| `GET_CONFIGURE_WAS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ConfigureService.scala:75–86` | MapPack → final MapPack/null |
| `GET_QUERY_INTERVAL` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:166–177` | MapPack → final MapPack/null |
| `HOST_DISK_USAGE` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HostService.scala:55–67` | MapPack → final MapPack/null |
| `HOST_MEMINFO` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HostService.scala:95–107` | MapPack → final MapPack/null |
| `HOST_NET_STAT` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HostService.scala:68–81` | MapPack → final MapPack/null |
| `HOST_PROCESS_DETAIL` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HostService.scala:42–54` | MapPack → final MapPack/null |
| `HOST_TOP` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HostService.scala:29–41` | MapPack → final MapPack/null |
| `HOST_WHO` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HostService.scala:82–94` | MapPack → final MapPack/null |
| `INNODB_STATUS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:214–225` | MapPack → final MapPack/null |
| `LIST_CONFIGURE_WAS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ConfigureService.scala:99–110` | MapPack → final MapPack/null |
| `LOCK_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:94–105` | MapPack → final MapPack/null |
| `OBJECT_ACTIVE_SERVICE_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ThreadList.scala:88–127` | MapPack hash/type → decorated Maps |
| `OBJECT_ACTIVE_SERVICE_LIST_GROUP` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ThreadList.scala:128–148` | MapPack List(objHash) → decorated Maps |
| `OBJECT_BATCH_ACTIVE_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/BatchService.scala:100–139` | MapPack hash/type → decorated Maps |
| `OBJECT_CHECK_RESOURCE_FILE` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:275–286` | MapPack → final MapPack/null |
| `OBJECT_CLASS_DESC` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:120–132` | MapPack → final MapPack/null |
| `OBJECT_CLASS_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:133–146` | MapPack → final MapPack/null |
| `OBJECT_DOWNLOAD_HEAP_DUMP` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HeapDumpService.scala:57–72` | MapPack → Blob sequence |
| `OBJECT_DOWNLOAD_JAR` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:287–298` | MapPack → final MapPack/null |
| `OBJECT_DUMP_FILE_DETAIL` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DumpService.scala:118–132` | MapPack → Blob sequence |
| `OBJECT_DUMP_FILE_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DumpService.scala:106–117` | MapPack → final MapPack/null |
| `OBJECT_FILE_SOCKET` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:223–236` | MapPack → final MapPack/null |
| `OBJECT_HEAPHISTO` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:209–222` | MapPack → final MapPack/null |
| `OBJECT_LIST_HEAP_DUMP` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/HeapDumpService.scala:45–56` | MapPack → final MapPack/null |
| `OBJECT_LOAD_CLASS_BY_STREAM` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:251–262` | MapPack → final MapPack/null |
| `SOCKET` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:237–250` | MapPack → final MapPack/null |
| `OBJECT_THREAD_DETAIL` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ThreadList.scala:60–74` | MapPack; id presence guard → final MapPack/null |
| `OBJECT_THREAD_DUMP` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ThreadList.scala:149–161` | MapPack → final MapPack/null |
| `OBJECT_THREAD_LIST` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/ThreadList.scala:75–87` | MapPack → final MapPack/null |
| `REDIS_INFO` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:263–274` | MapPack → final MapPack/null |
| `SCHEMA_SIZE_STATUS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:190–201` | MapPack → final MapPack/null |
| `SLAVE_STATUS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:226–237` | MapPack → final MapPack/null |
| `TABLE_SIZE_STATUS` | `scouter.server/src/main/scala/scouter/server/netio/service/handle/DatabaseService.scala:202–213` | MapPack → final MapPack/null |

기존 `OBJECT_ENV`는 `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:106–117`의 공통 마지막 MapPack 계약으로 보정한다.

## 제외된 mutation 명령

다음은 scope inventory의 mutation 45개이며 읽기 relay 목록에 추가하지 않는다. 이 명령들은 이후 별도 handler로 모두 구현 등록했다(Agent relay 변경 명령 21개 포함, [management-commands](management-commands.md)). Handler의 mutation 분류는 `query-scope.json`의 source_path/source_line에서 확인한다.

`ADD_ACCOUNT`, `ADD_ACCOUNT_GROUP`, `CUBRID_SET_ALERT_CONFIGURE`, `DB_KILL_PROCESS`, `DEFINE_OBJECT_TYPE`, `EDIT_ACCOUNT`, `EDIT_GROUP_POLICY`, `EDIT_OBJECT_TYPE`, `OBJECT_CALL_BLOCK_PROFILE`, `OBJECT_CALL_CPU_PROFILE`, `OBJECT_CALL_HEAP_DUMP`, `OBJECT_CALL_MUTEX_PROFILE`, `OBJECT_DELETE_HEAP_DUMP`, `OBJECT_REMOVE`, `OBJECT_REMOVE_INACTIVE`, `OBJECT_REMOVE_IN_MEMORY`, `OBJECT_RESET_CACHE`, `OBJECT_SYSTEM_GC`, `OBJECT_THREAD_CONTROL`, `PSTACK_ON`, `REDEFINE_CLASSES`, `REMOTE_CONTROL`, `REMOTE_CONTROL_ALL`, `SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS`, `SAVE_ALERT_SCRIPTING_CONTETNS`, `SERVER_DB_DELETE`, `SET_CONFIGURE_COUNTERS_SITE`, `SET_CONFIGURE_SERVER`, `SET_CONFIGURE_TELEGRAF`, `SET_CONFIGURE_WAS`, `SET_CUSTOM_KV`, `SET_CUSTOM_KV_BULK`, `SET_CUSTOM_TTL`, `SET_GLOBAL_KV`, `SET_GLOBAL_KV_BULK`, `SET_GLOBAL_TTL`, `SET_QUERY_INTERVAL`, `TRIGGER_ACTIVE_SERVICE_LIST`, `TRIGGER_BLOCK_PROFILE`, `TRIGGER_HEAPHISTO`, `TRIGGER_MUTEX_PROFILE`, `TRIGGER_THREAD_DUMP`, `TRIGGER_THREAD_LIST`, `USE_DATABASE`.

`CHECK_JOB`(`ManageService.java:113–126`)은 Client가 2초마다 보내는 remote-control job 폴링이다. 초기 Go는 이 명령을 미지원 오류로 닫아 Client 연결이 2초마다 끊겼고(실제 Client 환경에서 관찰), Java의 "job 없음" 응답(아무 Pack도 쓰지 않음)으로 고쳤다. 이후 생산자인 `REMOTE_CONTROL`/`REMOTE_CONTROL_ALL`과 counters 변경의 `REFETCH_COUNTER_XML` enqueue를 구현했으므로 queue에 job이 있으면 원본처럼 2-item 응답을 보낸다(`internal/service/remote_control.go`). 요청 연결의 인증 session과 다른 session의 job은 꺼내지 않는다(의도된 차이, [management-commands](management-commands.md) §6.2·§9-13).

## 검증과 미검증

`agent_queries_test.go`는 scope43/mutation45 분리, 44개(기존 ENV 포함) TCP body/response, 마지막 Map/비Map/empty, live selector/중복 순서, fallback, id presence, clone ownership, malformed/auth/cancel/unsupported/error/4096 상한을 검증한다. Blob은 0/253/254/65535/65536byte 길이를 검증하며 Blob-only backend도 허용한다. 빈 upstream 응답 이후 context 취소도 성공으로 숨기지 않는다. 기본 framing 근거는 기존 `internal/transport/tcp_test.go`의 `TestAgentV2NoNextHasNoLengthAndCustomBlobResponse`, 원본 `scouter.server/src/main/scala/scouter/server/netio/service/net/TcpAgentWorker.scala:41–76` 및 `scouter.common/src/main/java/scouter/io/DataInputX.java:90–110`, `DataOutputX.java:111–134`이다. TCP Agent V2 Pack read만 intBytes를 사용하며 callback Blob은 같은 DataInputX에서 직접 읽는다.

실행 결과: 초기 통합 `.tools/go/bin/go test ./internal/service -count=1` PASS(64.992s), `.tools/go/bin/go test -race ./internal/service -run '^TestAgent(Read|Relay)' -count=1` PASS. 최종 취소/Blob-only 테스트 추가 후에는 service 비테스트 전체 파일과 `service_test.go`, `aggregation_test.go`, `agent_queries_test.go`를 명시한 `go test -race … -run '^TestAgent(Read|Relay)' -count=1` PASS(1.876s), 같은 명시 파일의 `go vet` PASS, 변경한 Go 파일 `gofmt -l` 결과 없음이다. 이 마지막 실행은 범위 검증이며 전체 package 통합 결과는 아래 "통합 pool 검증"을 따른다. 이 결과는 아래 실제 Agent gate와 구분한다.

이 문서는 실제 Java/Host Agent의 command별 payload 실행을 완료했다고 주장하지 않는다. 실제 Agent가 연결된 end-to-end relay 및 GUI Client 조회는 별도 gate다. Fake response는 고정된 synthetic 데이터만 사용하며 JVM 환경·DB schema·파일 다운로드의 의미를 검증하지 않는다. 전체 registry enum 순서와 upstream Agent 버전별 null/예외 차이도 실제 상호운용 검증에 남는다.

## 통합 pool 검증

Runtime은 hash별 최대 8개 Agent 연결과 전체 TCP 연결 상한을 적용한다. 정본 TcpAgentManager의 RequestQueue(50)보다 작은 의도된 자원 제한이며 초과 연결은 명시적 ErrLimit으로 종료한다. 1초 checkout 대기, idle FIFO 반환, 5초 scan/idle threshold와 3초 KEEP_ALIVE timeout을 적용한다. `agent_pool_test.go`의 7개 실제 TCP socket 회귀가 race 반복5회 PASS(21.704s)이며 동시 두 연결, waiter 취소 시 owner 보존, 회수·재접속, 무응답 timeout, upstream EOF의 partial Blob을 검증한다.

최종 전체 service race PASS(210.619s), 실제 Java 통신 클래스의 56명령 통합은 [Phase 2D 검증](../validation/2026-10-13-phase2d.md)을 따른다. 실제 HostTcpWorker V1의 OBJECT_ENV relay를 포함하며 43개 추가 명령 모두의 실제 OS/JVM 기능 실행을 뜻하지 않는다.

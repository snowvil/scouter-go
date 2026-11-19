# 관리·변경 명령 계약 (mutation 39 + JVM 의존 READ 5)

정본은 `baseline.json`의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다(원본 저장소 `git rev-parse HEAD`로 확인). 이 문서는 `query-scope.json`에서 `category=="mutation"`이고 `status`가 없는 39개와 `local_read` 중 JVM 의존 5개(`GET_ALERT_REAL_COUNTER_DESC`, `GET_ALERT_SCRIPT_LOAD_MESSAGE`, `GET_PLUGIN_HELPER_DESC`, `SERVER_THREAD_DETAIL`, `SERVER_THREAD_LIST`)를 다룬다. READ 25개는 [server-metadata-query](server-metadata-query.md), Agent 읽기 relay 43개는 [agent-read-query](agent-read-query.md)가 정본이다. 모든 판단은 소스 정적 분석이 기본이다. 실제 실행으로 확인한 것은 다섯 가지다. ① §2.4·§4.3의 XML/JAXB/ParamText 출력: JDK 18과 JDK 8, 원본 로직 복사 ② §5 descriptor 2종: 공식 `scouter-server-2.21.3.jar` reflection, JDK 8/18 ③ §2.4 null attribute 동작 ④ §6.3 원본 `ObjectWR` delete Order NPE(원본 Scala 컴파일·실행) ⑤ §9-11 Client 빈 MapPack desync(원본 `ServiceHandlingProxy` dispatch 실행). 실행 조건은 §11에 있다.

경로 약어: `server/` = `scouter.server/src/main/scala/scouter/server/`, `serverj/` = `scouter.server/src/main/java/scouter/server/`, `common/` = `scouter.common/src/main/java/scouter/`, `client/` = `scouter.client/src/scouter/client/`, `agentj/` = `scouter.agent.java/src/main/java/scouter/agent/`. handler 파일은 `server/netio/service/handle/` 아래다.

## 0. 공통 계약

- framing: Client는 `writeText(cmd) + writeLong(session) + [body]`를 보내고 `HasNEXT(1) + item` 반복 후 `NoNEXT`를 읽는다(`server/netio/service/net/TcpServiceWorker.scala:111-130`, `client/net/TcpProxy.java:142-175`). handler는 item 앞에 `HasNEXT`만 쓰고 `NoNEXT`는 worker가 쓴다. 이 문서의 "응답 없음"은 item 0개(=`NoNEXT`만)를 뜻하며 연결은 유지된다.
- 인증: free command는 `LOGIN`, `SERVER_VERSION`, `SERVER_TIME`뿐이다(`common/net/RequestCmd.java:334-344`). 44개 전부 유효 session이 필요하고 실패 시 `INVALID_SESSION`(`TcpServiceWorker.scala:117-123`). **서버는 group policy(`AllowHeapDump` 등)를 검사하지 않는다.** Go도 같고(Client 호환), Agent 변경 relay는 요청마다 명령·objHash·session·peer를 로그에 남긴다. policy는 LOGIN 응답의 `policy` MapValue로 Client에 전달되어 Client UI에서만 적용된다(`LoginService.scala:73-76`, `client/server/GroupPolicyConstants.java:21-38`). handler의 `login: Boolean` 인자는 어떤 handler도 사용하지 않는다.
- 예외: handler 예외는 `ServiceHandlingProxy.Invocation.exec`가 `printStackTrace`로 삼키고(`server/netio/service/ServiceHandlingProxy.java:40-53`) worker는 `NoNEXT`를 쓴다. 따라서 cast 실패·NPE·IOException은 모두 "응답 없음"으로 보이고 Client는 `getSingle`에서 null을 받는다(`TcpProxy.java:100-106`). handler가 item 일부를 쓴 뒤 예외가 나면 그 item까지만 전달된다.
- body 읽기: `din.readPack().asInstanceOf[MapPack]`과 `din.readMapPack()`(`common/io/DataInputX.java:270-272`)은 동일하게 MapPack cast이며 다른 Pack이면 ClassCastException → 응답 없음. body가 없는 명령에서 MapPack을 읽으면 다음 명령 바이트를 소비하므로 Go는 명령별 body 종류를 정확히 지켜야 한다.
- MapPack getter 의미(`common/lang/pack/MapPack.java:80-126,190-192`): `getText`는 TextValue가 아니면 null, `getInt/getLong`은 NumberValue(Decimal/Float/Double)가 아니면 0, `getBoolean`은 BooleanValue가 아니면 false, `getList`는 unchecked cast(ListValue가 아니면 ClassCastException). `put(key, String)`은 `TextValue`, `put(key, long)`은 `DecimalValue`, `put(key, boolean)`은 `BooleanValue`(`:132-142`). null String은 `writeText(null)` → 길이 byte 0 하나(`common/io/DataOutputX.java:134-141`)로 빈 Text와 동일 wire다.
- Agent relay 공통: `AgentCall.call(o, cmd, param)`(`server/netio/AgentCall.scala:40-72`)은 ObjectPack null(S502) 또는 TCP agent 세션 없음(S501)이면 null, 세션이 있으면 `cmd + (param ?: new MapPack())`를 쓰고 `HasNEXT`가 이어지는 동안 Pack을 읽어 **마지막 Pack이 MapPack일 때만** 반환한다(다른 Pack 타입은 S149 로그 후 null). 세션 획득 대기는 `net_tcp_get_agent_connection_wait_ms`(기본 1000ms, `TcpAgentManager.scala:69-72`), Agent socket timeout은 `net_tcp_agent_so_timeout_ms`(기본 60000ms, `TcpAgentWorker.scala:18`). 읽기 중 예외는 `TcpAgentWorker.readByte/readPack`이 삼키고 세션을 닫으며 0/null을 돌려 결과는 null이다(`TcpAgentWorker.scala:41-68`). `finally`에서 세션을 pool에 되돌린다(닫힌 세션도 반환되며 5초 keepalive 루프가 정리, `TcpAgentManager.scala:30-56`).

## 1. 명령별 요약 표

| 명령 | handler 경로:줄 | request body | response | 실패·예외 | 부작용 |
|---|---|---|---|---|---|
| ADD_ACCOUNT | AccountService.scala:37-54 | MapPack `id,pass,email,group`:Text | `HasNEXT + MapPack{result:Boolean}` | id 중복/DOM 예외 → `result=false`. **id null이면 `result=true`**: `StringKeyLinkedMap.get/put(null)`은 null 반환·무시(`common/util/StringKeyLinkedMap.java:103-105,176-178`), DOM은 `id=""`로 직렬화(JDK 8/18 실행 확인) → 실제로 추가되지 않는다 | §2 accountMap 갱신(파일 미기록, §9-1). email null이면 이후 LIST_ACCOUNT NPE(§9-12) |
| EDIT_ACCOUNT | AccountService.scala:56-73 | 동일 | 동일 | id 없음/null → false. 파일의 해당 `<Account>`에 `<Email>`이 없으면 NPE가 `editAccount` catch → false | 동일 |
| EDIT_GROUP_POLICY | AccountService.scala:119-125 | MapPack `{<group>: MapValue{<policy>:Boolean}}` | `HasNEXT + BooleanValue`(Pack 아님) | DOM/IO 예외 → false | §2.4 |
| ADD_ACCOUNT_GROUP | AccountService.scala:127-133 | MapPack `name:Text, policy:MapValue{<policy>:Boolean}` | `HasNEXT + BooleanValue` | name 또는 policy null → false; policy가 MapValue가 아니면 ClassCastException → 응답 없음 | §2.4 |
| SET_CONFIGURE_SERVER | ConfigureService.scala:61-73 | MapPack `setConfig:Text` | `HasNEXT + MapPack{result:Text "true"/"false"}` | setConfig null → `text.getBytes()` NPE → `saveText` catch → `"false"` | scouter.conf 덮어쓰기 + `reload(true)` §3 |
| SET_CONFIGURE_TELEGRAF | ConfigureService.scala:233-245 | MapPack `tgConfigContents:Text` | 동일 | null → `"false"` | `./conf/scouter-telegraf.xml` 덮어쓰기 + `reload(true)` |
| SET_CONFIGURE_COUNTERS_SITE | ConfigureService.scala:256-268 | MapPack `contents:Text` | 동일 | contents null → NPE → 응답 없음; XML parse 실패 → `"false"` | §4.3 저장·정규화·engine reload·`notifyAllClients` |
| DEFINE_OBJECT_TYPE | ConfigureService.scala:148-154 | MapPack `name,disp,family,icon:Text, sub-object:Boolean` | `HasNEXT + BooleanValue` | 이미 존재 → false. family 미존재 → `setObjectTypeAttribute` NPE를 `appendObjectType` catch → **false**(§4.2). (DEFINE은 `<Types>`에 append한 뒤 정규화하므로 `<Types/>` NPE에 걸리지 않는다) | counters.site.xml 정규화 저장 + reload |
| EDIT_OBJECT_TYPE | ConfigureService.scala:156-162 | 동일 | 동일 | family 미존재/name null → NPE를 `editOrAppendObjectType` catch → false | **파일 미기록·engine 불변인데 true**(§9-2) |
| SAVE_ALERT_SCRIPTING_CONTETNS | AlertScriptingService.scala:60-69 | MapPack `counterName,contents:Text` | `HasNEXT + MapPack{success:Boolean}` | contents null → NPE in `saveText` catch → false | `<plugin_dir>/<counterName>.alert` 덮어쓰기 §5 |
| SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS | AlertScriptingService.scala:71-80 | 동일 | 동일 | 동일 | `<plugin_dir>/<counterName>.conf` |
| SERVER_DB_DELETE | ManageService.java:57-74 | MapPack `file:List<Text>` | `HasNEXT + MapPack{size:Decimal}`; `file` 없음 → 빈 MapPack | 개별 삭제 실패 무시 | `db_dir` 아래 대상 재귀 삭제 §6.1 |
| REMOTE_CONTROL | ManageService.java:75-97 | MapPack `toSession,fromSession:Decimal, command:Text, +임의 키` | `HasNEXT + MapPack{success:Boolean}` | fromSession/toSession이 유효 session이 아니면 `getUser(..).ip()` NPE → **enqueue 후** 응답 없음 | session queue enqueue §6.2 |
| REMOTE_CONTROL_ALL | ManageService.java:98-112 | MapPack `fromSession:Decimal, command:Text, +키` | **응답 없음(항상)** | fromSession 무효 → enqueue 후 NPE(차이 없음) | 모든 로그인 session queue enqueue |
| CHECK_JOB | ManageService.java:113-126 | MapPack `session:Decimal` | job 있으면 `HasNEXT + TextPack{text=command}` 다음 `HasNEXT + MapPack(원래 param)`; 없으면 응답 없음 | – | queue dequeue(소비) |
| OBJECT_REMOVE | AgentInfo.scala:158-169 | **`din.readValue()` ListValue<Decimal>**(MapPack 아님) | `getFullAgentList`(OBJECT_TODAY_FULL_LIST와 동일, §6.3) ObjectPack 반복 | ListValue 아님 → ClassCastException → 응답 없음. NullValue → NPE → 응답 없음 | 메모리 제거 + `ObjectWR.remove` enqueue(**원본은 writer NPE로 디스크 삭제가 일어나지 않음**, §6.3·§9-6) |
| OBJECT_REMOVE_IN_MEMORY | AgentInfo.scala:196-207 | 동일 | 동일 | 동일 | 메모리 제거만 |
| OBJECT_REMOVE_INACTIVE | AgentInfo.scala:47-50 | **body 없음** | **응답 없음** | – | `alive==false` 객체 메모리 제거 |
| OBJECT_RESET_CACHE | AgentInfo.scala:147-156 | MapPack `objHash` | **응답 없음(항상)** | – | Agent `DataProxy.reset()` |
| OBJECT_SYSTEM_GC | DumpService.scala:133-139 | MapPack `objHash` | **응답 없음(항상)** | – | Agent `System.gc()` |
| OBJECT_CALL_HEAP_DUMP | HeapDumpService.scala:33-43 | MapPack `objHash:Decimal, fName:Text, time:Decimal` | Agent 최종 MapPack `{success:Boolean, msg:Text}` 또는 없음 | relay null → 없음 | Agent hprof 생성 |
| OBJECT_CALL_BLOCK_PROFILE | HeapDumpService.scala:85-95 | `objHash,fName,time` | 최종 MapPack/없음 | Java agent에 handler 없음 → 없음 | Go agent pprof |
| OBJECT_CALL_MUTEX_PROFILE | HeapDumpService.scala:97-107 | 동일 | 동일 | 동일 | 동일 |
| OBJECT_CALL_CPU_PROFILE | HeapDumpService.scala:109-119 | 동일 | 동일 | 동일 | 동일 |
| OBJECT_DELETE_HEAP_DUMP | HeapDumpService.scala:73-83 | `objHash, delfileName:Text` | `{success,msg}`/없음 | – | Agent 파일 삭제 |
| OBJECT_THREAD_CONTROL | ThreadList.scala:162-173 | `objHash, id:Decimal, action:Text(interrupt/stop/resume/suspend)` | Agent thread detail MapPack(§8 key + `Service Txid/Service Name/Service Elapsed/SQL/Subcall`) | – | Agent thread 제어 |
| PSTACK_ON | StackAnalyzerService.scala:87-97 | `objHash, time:Decimal`(off는 time 없음) | Agent는 **param echo**(같은 MapPack) | – | Agent pstack 타이머 |
| TRIGGER_ACTIVE_SERVICE_LIST | DumpService.scala:34-44 | `objHash` | `{name:Text}`(dump 파일명) 또는 빈 MapPack | – | Agent dump 파일 |
| TRIGGER_HEAPHISTO | DumpService.scala:46-56 | `objHash` | 동일 | – | 동일 |
| TRIGGER_THREAD_DUMP | DumpService.scala:58-68 | `objHash` | 동일 | – | 동일 |
| TRIGGER_THREAD_LIST | DumpService.scala:70-80 | `objHash` | 동일 | – | 동일 |
| TRIGGER_BLOCK_PROFILE | DumpService.scala:82-92 | `objHash` | Go agent 전용 | Java agent handler 없음 | – |
| TRIGGER_MUTEX_PROFILE | DumpService.scala:94-104 | `objHash` | 동일 | 동일 | – |
| SET_CONFIGURE_WAS | ConfigureService.scala:87-97 | `objHash, setConfig:Text` | `{result:Text "true"/"false"}` | – | Agent conf 파일 덮어쓰기 |
| REDEFINE_CLASSES | ConfigureService.scala:122-133 | `objHash, class:List<Text>`(RedefineClassDialog는 `classNames` 키를 보내 Agent가 무시) | `{success:Boolean[, error:Text]}`; 대상 class 없으면 빈 MapPack | – | JVM redefine |
| CUBRID_SET_ALERT_CONFIGURE | CubridService.scala:358-368 | `objHash, key:Text, value:Text` | `{result:Text}` | – | Cubrid agent 설정 |
| DB_KILL_PROCESS | DatabaseService.scala:154-164 | `objHash` + agent별 키 | 최종 MapPack/없음 | Client 호출자 없음 | DB 세션 kill |
| SET_QUERY_INTERVAL | DatabaseService.scala:178-188 | `objHash, interval:Decimal(0=off)` | 최종 MapPack/없음 | – | DB agent 수집 주기 |
| USE_DATABASE | DatabaseService.scala:250-260 | `objHash` + agent별 키 | 최종 MapPack/없음 | Client 호출자 없음 | – |
| GET_ALERT_SCRIPT_LOAD_MESSAGE | AlertScriptingService.scala:82-103 | MapPack `loop:Decimal(long), index:Decimal(int)` | `HasNEXT + MapPack{loop:Decimal, index:Decimal, messages:List<Text>}` | cache null은 불가(항상 객체) → 정상 입력이면 항상 1개. 현재 loop와 같은 loop + 음수 index → `queue(-1)` AIOOBE → 응답 없음(§5) | – |
| GET_ALERT_REAL_COUNTER_DESC | AlertScriptingService.scala:105-122 | **서버는 body를 읽지 않음**. Java Client는 빈 MapPack을 보냄(§9-11) | descriptor마다 `HasNEXT + MapPack{desc,methodName,returnTypeName:Text, parameterTypeNames:List<Text>}` 25개 | – | – |
| GET_PLUGIN_HELPER_DESC | AlertScriptingService.scala:124-141 | 동일(서버 미독, Client 빈 MapPack) | 동일 구조 22개 | – | – |
| SERVER_THREAD_LIST | ThreadList.scala:50-58 | **body 없음** | `HasNEXT + MapPack{id,name,stat,cpu:List}` | – | – |
| SERVER_THREAD_DETAIL | ThreadList.scala:39-48 | MapPack `id:Decimal` | `HasNEXT + MapPack`(§8); id 0/없음이면 **빈 MapPack** | – | – |

## 2. 계정

### 2.1 파일·로드·reload

- 경로: `Configure.CONF_DIR = "./conf/"`(`serverj/Configure.java:70`) + `account.xml`, `account_group.xml`(`server/account/AccountManager.scala:50-58`). 읽을 수 없으면 classpath `/scouter/server/account/*.xml`(`scouter.server/src/main/resources/scouter/server/account/`)을 최대 4회 재시도로 복사한다(`:71-143`).
- 기본 resource: `account.xml`은 `<Accounts>` 아래 `<xs:attribute name="id" type="xs:ID"/>`와 `admin`/`guest` 두 `<Account id pass group><Email>` 요소(pass는 salted SHA256 hex). `account_group.xml`은 `admin`(18개 policy 전부 true), `guest`(`AllowThreadDump`, `AllowSystemGC`만 true) `<Group name><Policy><AllowX>bool</AllowX>…`. policy 이름 18개는 `GroupPolicyConstants.java:21-38`과 동일하다. 두 파일 모두 tab 들여쓰기, 끝 newline 없음.
- 5초 daemon(`CoreRun.running` 동안)이 `lastModified` 변화 시 재파싱한다(`AccountManager.scala:61-70`). Scala object 초기화 순서상 `loadGroupFile()/loadAccountFile()`(`:59-60`)이 `lastModified*`를 먼저 대입한다. 그 뒤 `var lastModified* = 0L`(`:61-62`)이 0으로 덮으므로 **첫 tick에 두 파일을 한 번 더 읽는다**(무해). 변경 API는 성공 후 `lastModified* = file.lastModified()`를 다시 읽는다. 파일이 바뀌지 않았으므로(§2.4) mtime은 그대로이고 reload는 트리거되지 않는다. 다른 이유로 파일 mtime이 바뀌면 메모리에만 있던 변경은 사라진다. 파싱 실패는 `printStackTrace` 후 기존 map을 유지한다(`:102-104,139-141`). 파싱 결과는 `StringKeyLinkedMap`(삽입 순서, `AccountFileHandler.scala:42-63`, `GroupFileHandler.scala:45-79`). `Email` 요소가 없으면 email=null이다. `Account` field 기본값은 모두 `""`다(`common/lang/Account.java:28-31`). 그러나 파일 파싱과 ADD/EDIT handler가 null로 덮을 수 있고, `toBytes`는 null field에서 NPE를 낸다(§9-12). `<Group>`에 `<Policy>`가 없으면 `item(0)` null → NPE → 파싱 전체 실패(기존 map 유지). policy 값은 `String.toBoolean`이라 `true/false` 외 문자열은 예외 로그 후 건너뛴다(`GroupFileHandler.scala:64-72`).
- LIST_ACCOUNT 응답의 `Account.toBytes`(`common/lang/Account.java:33-48`)는 각 field를 `out.write(len)`(하위 1 byte) + platform charset bytes로 쓴다. 255 byte 초과 wrap은 [server-metadata-query](server-metadata-query.md) 결정대로 Go에서 거절한다.

### 2.2 로그인·비밀번호

- Client는 `CipherUtil.sha256(password)`를 보내므로 서버가 받는 `pass`는 이미 **salt `qwertyuiop!@#$%^&*()zxcvbnm,.` 앞붙임 SHA-256 hex(소문자)** 다(`client/net/LoginMgr.java:34-47`, `common/util/CipherUtil.java:59-83`). ADD/EDIT_ACCOUNT도 같은 값을 보낸다(`client/popup/AccountDialog.java:318-323,356-361`). 서버는 `account.password.equals(pass)` 문자열 비교만 한다(`AccountManager.scala:194-201`). `LoginMgr.login(..., secureLogin=false)` 경로는 평문을 보내며 서버는 구분하지 않는다.
- `CipherUtil.sha256` 정확한 형식: `MessageDigest("SHA-256").update(salt.getBytes()); digest(plain.getBytes())`(platform charset)이다. 각 byte를 2자리 소문자 hex로 이어 64자를 만든다. plain이 null이면 null, 예외 시 plain 그대로 반환한다(`CipherUtil.java:59-83`). 로그인 대화상자에는 "Secure Login" 체크박스가 있다(`client/popup/LoginDialog2.java:135-143`). 그러나 실제 호출은 3인자 `LoginMgr.login(...)`(`:324`)이라 **체크 여부와 관계없이 항상 SHA-256 hex를 보낸다**. 4인자 overload를 호출하는 곳은 이 Client 소스에 없다. 재로그인 경로(`SessionObserver.java:57`, `OpenServerAction.java:46`, `Application.java:138`)는 저장된 `server.getPassword()`(이미 hash된 값)를 `silentLogin`으로 그대로 보낸다. 결과적으로 LDAP 모드에서도 bind password는 SHA-256 hex다(§2.3). 원본 Java Client로 평문 LDAP 서버에 로그인하려면 LDAP 측에 hash가 저장되어 있어야 한다는 뜻이다(운영 미확인).
- `NetConstants.LOCAL_ID = "__local"`(`common/net/NetConstants.java:26`). **id**가 `__local`이면 `ServiceWorker.remoteIpByWorker`(접속 socket의 실제 remote IP)가 `localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1` 중 하나일 때만 `group=""`(Account 기본값)인 Account로 통과한다(`server/LoginManager.scala:38-43`). 비밀번호는 검사하지 않는다. 비 loopback이면 session 0 → `error="login fail"`이다. `internal`(Text, 대소문자 무시 `"true"`, `LoginService.scala:50-51`)이 true면 session TTL과 `okSession` keepalive 연장 모두 7일이다. 아니면 `CacheTable` 기본 3시간이고, `okSession`의 `getKeepAlive`가 연장한다. 단 `TcpServiceWorker.scala:110-126`이 연결마다 첫 비-free 명령에서만 `okSession`을 부르고 이후 명령은 `sessionOk` latch로 건너뛰므로, 연장 시점은 "요청마다"가 아니라 "연결의 첫 인증마다"다(`LoginManager.scala:26,33,60-65,71-82`). 5초 daemon이 만료 항목을 정리한다(`:27-32`). session key는 `KeyGen.next()`. `LoginUser.ip`는 요청 MapPack의 `ip`(Client 자기 보고값)이며 접속 IP가 아니다(`:54`).
- **의도된 차이(Go session 유지 규칙)**: Java는 연결 종료·로그아웃 때 session을 지우지 않아, 재연결마다 새로 LOGIN하는 소비자가 사용하지 않는 session을 3시간 동안 `GET_LOGIN_LIST`에 남긴다. Go는 session(internal 포함)을 그 id를 마지막으로 쓴(명령 header 또는 LOGIN) **살아 있는 Client 연결**이 하나라도 있으면 TTL 없이 유지하고, 마지막 연결이 끝난 뒤 유예 시간 G = 2 × live `net_tcp_client_so_timeout_ms`(최소 30초·최대 10분) 동안 새 요청이 없으면 제거한다. `internal="true"`로 연결마다 LOGIN하는 Client API 소비자는 Java에서 재연결마다 사용하지 않는 session을 7일 동안 남긴다. LOGIN·연결 인증·`CHECK_SESSION`·`GET_LOGIN_LIST`·REMOTE_CONTROL 목록이 같은 판정을 쓰며, 제거된 session으로 요청하면 `INVALID_SESSION`이다. Java Client는 `INVALID_SESSION`을 받으면 session을 0으로 지우고(`client/net/TcpProxy.java:166-168`) `SessionObserver`가 5초 주기로 silent re-login하므로(`client/threads/SessionObserver.java:42-68`) 자동 복구된다(원본 코드 판독, 실제 Client 실행 미확인). 정본은 [service 계약](../../internal/service/CONTRACT.md) §1, 연결 추적은 [transport 계약](../../internal/transport/CONTRACT.md) §3.1이다.
- `__local` 로그인 시 LOGIN 응답은 `type=""`(user.group)다. `policy`는 생략된다(`groupPolicyMap.get("")` null). `email`도 생략된다(`getAccount("__local")` null, `LoginService.scala:68-76`).

### 2.3 LDAP

`account_use_ldap=true`(`AccountManager.scala:145-192`)면 모든 LOGIN/CHECK_LOGIN이 JNDI로 간다.

| 키 | 기본값 | 의미 |
|---|---|---|
| `account_use_ldap` | false | LDAP 전환 |
| `account_ldap_provider_url` | null | `Context.PROVIDER_URL`. null이면 `Properties.setProperty`(Hashtable) NPE다. **이 호출은 `try` 밖(`:149-153`)이라 예외가 handler까지 전파**되어 LOGIN/CHECK_LOGIN은 응답 없음이 된다. pass가 null인 요청도 같다 |
| `account_ldap_auth=` | `"simple"` | **키 끝에 `=`가 포함된 오타 키** 그대로. `Context.SECURITY_AUTHENTICATION` |
| `account_ldap_principal_domain` | null | `SECURITY_PRINCIPAL = id + domain`(null이면 `"idnull"` 문자열) |
| `account_ldap_basedn` | null | subtree search base |
| `account_ldap_email_id` | `""` | 성공 시 email attribute 이름 |
| `account_ldap_group_id` | `""` | 성공 시 group attribute 이름 |
| `account_ldap_debug` | false | 원본은 id·받은 pass·props를 로그에 출력한다. Go는 비밀번호를 로그에 쓰지 않는다([account CONTRACT](../../internal/account/CONTRACT.md)) |

절차(`AccountManager.scala:145-192`):

1. `INITIAL_CONTEXT_FACTORY=com.sun.jndi.ldap.LdapCtxFactory`로 `new InitialDirContext(props)`를 만든다. bind 정보는 `SECURITY_PRINCIPAL = id + principal_domain`, `SECURITY_CREDENTIALS = pass`(Client가 보낸 값 그대로, §2.2)다. JNDI는 이 생성자에서 bind를 수행하므로 bind 실패는 `AuthenticationException` → catch → null이다.
2. 같은 bind 세션으로 `ctx.search(basedn, "(cn=" + id + ")", SUBTREE_SCOPE)`를 실행한다. 원본은 id를 escape하지 않고 filter 문자열에 결합한다. Go는 id를 literal 값으로 다룬다([account CONTRACT](../../internal/account/CONTRACT.md)). basedn null이면 JNDI 예외 → catch → null.
3. 결과의 **첫 entry만** 본다(`hasMore` 1회). 그 entry의 attribute id를 `getIDs()` 순서로 돌며 attribute 첫 값의 `toString()`이 id와 **대소문자 구분 equals**인 것이 하나라도 있으면 성공이다.
4. 성공 시 `Account{id, email=attrs.get(email_id).get(), group=attrs.get(group_id).get()}`를 만든다. attribute가 없으면 NPE를 catch하고 이후 field는 기본 `""`로 남는다. email 실패 시 group도 설정되지 않는다.
5. entry가 없거나 일치 attribute가 없으면 null이다. `finally`에서 `ctx.close()`. connect/read timeout 설정은 없어 JNDI 기본값(무제한)이다.

LDAP 키는 `Configure`의 public field가 아니라 `conf.getValue/getBoolean`으로 `property`를 직접 읽는다. 따라서 ① 매 로그인마다 최신 값이 반영되고 ② `LIST_CONFIGURE_SERVER`/`CONFIGURE_DESC`에는 나타나지 않는다. LDAP 모드는 `accountMap`을 쓰지 않는다. 그래서 LOGIN 응답 `email`은 `AccountManager.getAccount(id)`(로컬 파일) 기준이고, `policy`는 LDAP group 이름으로 `groupPolicyMap`을 조회한다(없으면 생략).

### 2.4 변경 API와 파일 출력

- `addAccount`: `accountMap`에 있으면 false; `AccountFileHandler.addAccount` 후 map put(`AccountManager.scala:215-230`). `editAccount`: 없으면 false; `editAccount` 후 map put(`:231-246`). `addAccountGroup`: `name`/`policy` null이면 false, `GroupFileHandler.addAccountGroup` 성공 시 map put(`:247-261`). `editGroupPolicy`: `GroupFileHandler.editGroupPolicy` 성공 시 **`loadGroupFile()` 재파싱**(`:262-270`).
- DOM 조작: addAccount는 `<Accounts>` 첫 요소 끝에 `<Account id pass group><Email>text</Email></Account>`를 append한다(`AccountFileHandler.scala:65-80`). null attribute는 `""`로 직렬화되고 null email은 `<Email/>`이 된다. editAccount는 첫 매칭 id의 `pass,group` attribute와 그 요소의 첫 `<Email>` text를 교체한다. 매칭이 없으면 Exception이다(`:82-104`). 이 closure 안의 `return`은 Scala non-local return(`NonLocalReturnControl`, `Exception` 아님)이라 `AccountManager`의 catch에 걸리지 않고 정상 종료된다. addAccountGroup은 `<Groups>` 끝에 `<Group name><Policy>`를 붙이고 policy 요소를 `<policy>true|false</policy>`로 추가한다(`policyMap.getBoolean` 결과, Boolean 아닌 값은 false, `GroupFileHandler.scala:102-124`). policy 순서는 **`MapValue.keys()` = `Hashtable` 열거 순서**이며 요청 순서가 아니다(`common/lang/value/MapValue.java:45,59-61`). 메모리 `groupPolicyMap`에는 요청 MapValue 객체를 그대로 넣는다. 따라서 Boolean이 아닌 값도 GET_GROUP_POLICY_ALL/LOGIN `policy`에 원형대로 노출된다. editGroupPolicy는 요청 MapPack의 group 키마다 매칭 `<Group>`의 `<Policy>` 아래 기존 요소 text 교체·없으면 append(`CastUtil.cString(value)` → `"true"/"false"`), 매칭 group이 없으면 무시(`:81-100,126-153`).
- **`XmlUtil.writeXmlFileWithIndent`는 commit `cc9bda37`(2022-11-02 "fix counters-site.xml on saving") 이후 `StreamResult(StringWriter)`로 변환 결과 문자열만 반환하고 `file` 인자를 쓰지 않는다**(`server/util/XmlUtil.scala:32-43`). 계정 4개 명령 모두 반환값을 버리므로 **디스크 파일은 변경되지 않는다**(§9-1).
- 변환기 출력(JDK 18 Xalan 내장, indent=2, 로컬 실행으로 확인): 첫 줄 `<?xml version="1.0" encoding="UTF-8" standalone="no"?>`, attribute는 DOM `NamedNodeMap` 순서 = **이름 알파벳순**(`group, id, pass`), 기존 whitespace text node가 보존된 채 indent가 추가되어 원본 tab 줄이 빈 줄처럼 남고 새 요소만 깔끔히 2-space 들여쓰기된다. `<xs:attribute …/>` 등 알 수 없는 노드는 그대로 보존된다. 변환 결과 문자열은 `</Accounts>` 뒤에 newline 1개로 끝난다(oracle 출력 byte 확인. 원본 입력 파일 자체는 끝 newline 없음).
- **JDK 8(Zulu 8) 실측 출력은 다르다**: 기존 tab 줄을 그대로 두고 indent를 추가하지 않는다. 새 `<Account>`는 0열에서 시작하고 자식 `<Email/>`은 4칸, 닫는 `</Account>`는 2칸이다. 두 JDK 모두 첫 줄 XML 선언(`standalone="no"`, 선언 뒤 newline), 알파벳순 attribute, 마지막 닫는 태그 뒤 newline 1개를 출력한다. 결론: 원본 출력은 JDK 의존이므로 byte 일치 fixture 대상이 아니다. 계약은 "Java `DocumentBuilder`가 다시 읽어 같은 Account/Group map을 얻는다"로 정의한다.

## 3. 설정(`serverj/Configure.java`)

### 3.1 파일·reload

- 파일: `System.getProperty("scouter.config", "./conf/scouter.conf")`(`:578-585`). Telegraf 파일은 `input_telegraf_config_file` field 기본값 `./conf/scouter-telegraf.xml`(`:445,587-589`)인데 **`applyConfig`가 이 키를 읽지 않아 항상 기본 경로**다.
- `Configure`는 Thread로 3초마다 `reload(false)`(`:569-574`). `reload(force)`는 synchronized, force=false면 `last_check+3000ms` 이내 호출을 무시(`:593-600`). `reloadConfig`는 `file.lastModified()==last_load_time`이면 false, 다르면 `Properties.load`(ISO-8859-1 + `\uXXXX`) → `ConfigValueUtil.replaceSysProp` → `applyConfig` → `applyTelegrafInputConfig`(`:638-663`). 파일이 없으면 `last_load_time`만 갱신하고 기존 값 유지. SET_CONFIGURE_SERVER는 `saveText` 성공 후 `reload(true)`로 즉시 적용한다. 생성자 `Configure()`도 `reload(false)`를 호출한다(`:552-554`). `Properties.load` 예외(예: 잘못된 `\uXXXX`)는 `printStackTrace` 후 **그 지점까지 적재된 temp로 진행**한다. 그래서 이후 키는 기본값으로 재적용된다(`:646-660`).
- Telegraf 파일 reload(`reloadTgConfig`, `:593-619`): `reload()` 끝에서 항상 호출된다. 파일이 없고 이번에 scouter.conf가 새로 적용됐으면 `telegrafInputConfigMap = telegrafInputConfigMapDeprecated`(conf의 `input_telegraf_$m$_*`)다. 파일을 읽을 수 있고 mtime이 바뀌었으면 JAXB `TgConfig` unmarshal 후 `applyTelegrafInputConfigNew`(`:1043-1114`)를 실행한다. 이 함수는 `input_telegraf_enabled/_debug_enabled/_delta_counter_normalize_default/_seconds`와 `telegraf_object_deadtime_ms`를 **파일 값으로 덮고**, enabled면 `net_http_server_enabled=true`를 강제한다. 다만 HTTP 서버는 기동 시에만 열린다(§3.3). measurement 설정은 파일 우선, 파일에 없는 measurement는 conf deprecated 값으로 보충한다. **unmarshal 실패 시 `last_load_time_tg`를 갱신하지 않고 이전 `telegrafOriginalConfig`로 다시 적용**하므로 3초마다 재시도와 stack trace가 반복된다.
- SET_CONFIGURE_TELEGRAF는 내용 검증 없이 `FileUtil.saveText(getTgFile(), text)`로 쓴다(platform charset, `common/util/FileUtil.java:156-170`). 그다음 `reload(true)`한다(`Configure.java:633-636`). GET_CONFIGURE_TELEGRAF는 `FileUtil.load(file, "utf-8")`로 읽는다(`:621-631`). **쓰기는 platform charset, 읽기는 UTF-8로 비대칭**이다. 저장한 XML이 깨져도 응답은 `"true"`다.
- `replaceSysProp`(`common/lang/conf/ConfigValueUtil.java:39-55`): 결과 Properties에 **환경변수 전체와 System properties 전체를 먼저 넣고** 파일 키를 덮는다. 값은 `StringUtil.trim` 후 `ParamText("${", "}")`로 `${NAME}`을 env/sysprop에서 치환한다. **정정(2026-10-15, 원본 `scouter.common` 컴파일 후 `replaceSysProp(Properties)` 직접 실행, Zulu 8)**: 정의 여부와 무관하게 `${...}`가 의도한 변수 값이 아닌 문자열로 치환된다(overload 선택 결함). 이전 기록의 `scouter.lang.ref.STRING@<hash>`는 `getText(Map)`을 직접 호출했을 때만 나오며 Configure 경로에서는 나오지 않는다. `x${`처럼 닫는 괄호가 없으면 그 뒤를 버리고, 값은 trim된다. Go 결정: 정의된 `${name}`은 환경변수 값, 미정의는 원문 유지, 파일에 없는 키는 같은 이름의 환경변수 fallback(`internal/config/CONTRACT.md` §3).
- `loadText`는 파일 전체를 platform charset `new String(bytes)`로 돌려주고 실패 시 null(`:1168-1180`). `saveText`는 parent mkdirs 후 `text.getBytes()`로 전체 덮어쓰기, 예외 시 false(`:1182-1198`). 원자적 교체·백업 없음.
- `getValue`는 `StringUtil.trim(property.getProperty)`, `getInt/getLong/getBoolean`은 parse 실패 시 default(`:1130-1166`). `getBoolean`은 `Boolean.parseBoolean`(대소문자 무시 `true`만 true).
- `getKeyValueInfo`(`:1209-1228`, `ConfigValueUtil.getConfigDefault` `ConfigValueUtil.java:57-73`): `new Configure(true)`(빈 생성자, field 초기값)와 현재 instance의 **public non-static field 전체**를 reflection(`getFields()` 순서)으로 읽어 `key/value/default` List를 만든다. `ignoreSet = {property, telegrafInputConfigMap, allowIpExact, allowIpMatch}`만 제외하고 `telegrafInputConfigMapDeprecated`, `telegrafOriginalConfig`, `WORKABLE`(static이라 제외)… 등 설정이 아닌 field도 노출된다. 값 변환 `toValue`: null→NullValue, Float/Double→Float/DoubleValue, Number→DecimalValue, Boolean→BooleanValue, 배열→`[a, b]`의 괄호 제거 Text, 그 외 `toString()` Text(`StringSet`은 `toString` 문자열)(`ConfigValueUtil.java:155-175`).

### 3.2 field 초기값과 applyConfig 기본값 불일치

`LIST_CONFIGURE_SERVER`의 `default`는 field 초기값이고 실제 미설정 시 값은 `applyConfig`의 두 번째 인자다. 다른 항목: `object_zipkin_deadtime_ms` 180000 vs 300000(`:283,726`), `net_webapp_tcp_client_pool_size` 30 vs 12(`:236,698`), `net_webapp_tcp_client_pool_timeout` 60000 vs `net_tcp_client_so_timeout_ms`(8000)(`:238,699`), `net_http_api_auth_ip_header_key` null vs `""`(`:245,702`), `mgr_purge_keep_days`/`mgr_purge_xlog_keep_days`/`mgr_purge_counter_keep_days`는 파생식이 달라 사용자가 일부만 바꾸면 값이 달라진다(`:313-334,775-792`). `applyConfig`가 읽지 않는 field: `input_telegraf_config_file`, `span_queue_size`, `net_webapp_tcp_client_so_timeout`, `net_http_api_allow_ips`의 `allowIpExact/allowIpMatch`(파생). 서버 코드 어디서도 참조하지 않는 field: `plugin_enabled`, `temp_dir`, `client_dir`, `net_http_extweb_dir`.

### 3.3 핫 리로드 범위

`applyConfig`는 매 reload마다 모든 field를 다시 대입하지만 소비자가 값을 언제 읽는지에 따라 실제 효과가 갈린다(소비처는 `scouter.server/src`·`scouter.webapp/src` grep 결과). 키별 전체 표와 Go 상태는 [config-behavior](config-behavior.md) §1이 정본이며 이 표는 요약이다(2026-10-15 정정: `net_http_api_*`의 요청마다 읽는 키, `xlog_sampling_matcher_*`의 키별 반영 시점, `log_keep_days`의 두 소비처).

| 분류 | 키 | 근거 |
|---|---|---|
| 기동 시에만 읽음(재시작 필요) | `net_udp_listen_ip/port`, `net_udp_so_rcvbuf_size`, `net_udp_packet_buffer_size` | `DataUdpServer.scala:36-37` open 시점 |
| | `net_tcp_listen_ip/port` | `TcpServer.scala:36-41` ServerSocket 생성 |
| | `_net_udp_worker_thread_count` | `NetDataProcessor.scala:40` val |
| | `xlog_queue_size`, `profile_queue_size`, `span_queue_size` | `XLogCore.scala:38`, `XLogPreCore.scala:32`, `XLogWR.scala:39`, `ProfileCore/ProfilePreCore/XLogProfileWR`, `SpanCore.scala:35` val queue |
| | `_mgr_text_db_index_*_mb`, `_mgr_kv_store_index_default_mb` | `TextPermIndex.scala:32`, `KeyValueStoreIndex.scala:28` object val |
| | `_mgr_xlog_id_index_mb` | `XLogIndex.scala:62` instance val(일별 파일 open 시 읽으므로 다음 날짜부터 반영) |
| | `net_http_server_enabled`, `net_http_port`, `net_http_api_enabled`, `net_http_api_gzip_enabled`, `log_keep_days`(Jetty `http-request-*.log` retainDays만) | `HttpServer.java:61-112` 기동 시(`setRetainDays(conf.log_keep_days)`는 `:68`) |
| | `net_http_api_session_timeout` | `scouter.webapp/.../main/WebAppMain.java:141` session handler 생성 시 |
| | `xlog_sampling_matcher_gxid_keep_memory_count` | `XLogDelayingCache.java:53` 생성자(싱글턴 1회) |
| 새 자원 생성 시 | `xlog_sampling_matcher_xlog_keep_memory_count` | `XLogDelayingCache.java:145` 새 bucket map 생성 때 `setMax` |
| | `xlog_sampling_matcher_profile_keep_memory_count` | `ProfileDelayingCache.java:174` 새 bucket map 생성 때 `setMax` |
| | `db_dir` | `DBCtr.scala:30`은 매번 읽지만 열린 writer/index는 기존 경로 유지 → 혼합 상태(변경 금지 권장) |
| ConfObserver로 반영 | `mgr_purge_*` 전체 | `AutoDeleteScheduler.java:65-90` 값 비교 후 `applyConf` + deleted 집합 clear |
| | `_compress_read_cache_block_count`, `_compress_write_thread` | `IOChannel.java:41-45,119-128`(thread pool 교체) |
| | `log_dir`, `log_rotation_enabled` | `Logger.scala:154-163` writer 재오픈 |
| | `net_tcp_service_pool_size` | `TcpServer.scala:29-33` `setCorePoolSize` |
| | `geoip_data_city_file` | `GISDataUtil.scala:21-26` 재로드 |
| 사용 시마다 읽음(즉시 반영) | `object_deadtime_ms`, `object_zipkin_deadtime_ms`, `object_inactive_alert_level` | `AgentManager.scala:49-52` 1초 루프 |
| | `xlog_realtime_lower_bound_ms`, `xlog_pasttime_lower_bound_ms`, `req_search_xlog_max_count` | `XLogService.scala:177,475,603` |
| | `tagcnt_enabled`, `geoip_enabled`, `sql_table_parsing_enabled`, `visitor_hourly_count_enabled`, `mgr_text_db_daily_*_enabled`, `compress_xlog/profile_enabled`(일별 writer open 시, 날짜 properties에 고정) | `XLogCore.scala:42,70`, `IXLog.java:43`, `SqlTables.scala:66`, `VisitorCore.scala:74`, `TextPermWR.scala:42`, `XLogDataWriter.scala:53-61` |
| | `log_udp_*`, `log_tcp_action_enabled`, `mgr_log_ignore_ids`, `_trace` | `NetDataProcessor`, `TcpAgentWorker`, `TcpServiceWorker`, `Logger` |
| | `log_keep_days`(server/request log 정리) | `Logger.scala:140-142,185-231` 1시간 주기 `clearOldLog`마다 읽는다. 같은 키의 Jetty request log 보관일은 위 기동 행 |
| | `xlog_sampling_matcher_xlog_keep_memory_millis`, `xlog_sampling_matcher_profile_keep_memory_secs` | `XLogDelayingCache.java:85-86`(XLog마다 회전 단위 계산), `ProfileDelayingCache.java:101`(회전마다 bucket 수 계산) |
| | `net_http_api_cors_allow_origin`, `net_http_api_cors_allow_credentials`, `net_http_api_auth_ip_enabled`, `net_http_api_auth_ip_header_key`, `net_http_api_auth_session_enabled`, `net_http_api_auth_bearer_token_enabled`, `net_http_api_allow_ips` | webapp `framework/filter/CorsFilter.java:43-44`, `AuthFilter.java:56-83`가 요청마다 읽는다. allow ip 집합은 reload 때 `Configure.applyConfig`가 다시 만든다 |
| | `plugin_dir` | `AlertRuleLoader.java:70,102,118,128`, `PlugInLoader` 5초 루프 |
| | `server_id`, `ext_link_name`, `ext_link_url_pattern`, `net_tcp_client_so_timeout_ms`(LOGIN 응답·accept 시) | `LoginService.scala:63,80-82`, `TcpServer.scala:43` |
| | `net_tcp_agent_so_timeout_ms`(agent accept 시), `net_tcp_agent_keepalive_interval_ms`, `net_tcp_get_agent_connection_wait_ms` | `TcpAgentWorker.scala:18,83`, `TcpAgentManager.scala:71` |
| | `telegraf_object_deadtime_ms`, `input_telegraf_*`(파일 reload 시 `applyTelegrafInputConfigNew`가 덮어씀) | `TelegrafInputHandler.java:173`, `Configure.java:1043-1114` |

ConfObserver(`server/ConfObserver.scala:22-47`)는 이름별 Runnable을 `StringKeyLinkedMap`에 등록하고 `applyConfig` 끝(`:853`)에서 등록 순서대로 실행하며 예외는 개별 `printStackTrace`로 삼킨다. 등록자는 위 표의 5곳(`AutoDeleteScheduler`, `IOChannel`×2, `Logger`, `TcpServer`, `GISDataUtil`)뿐이다.

### 3.4 주요 기본값

| 키 | 기본값(applyConfig) |
|---|---|
| `server_id` | `SysJMX.getHostName()` |
| `plugin_dir` | `./plugin` |
| `net_http_server_enabled` / `net_http_port` / `net_http_extweb_dir` | false / `NetConstants.SERVER_HTTP_PORT` / `./extweb` |
| `net_http_api_enabled` / `_swagger_enabled` / `_swagger_host_ip` / `_cors_allow_origin` / `_cors_allow_credentials` | false / false / `""` / `*` / `true` |
| `net_http_api_auth_ip_enabled` / `_auth_ip_header_key` / `_auth_session_enabled` / `_session_timeout` / `_auth_bearer_token_enabled` / `_gzip_enabled` / `_allow_ips` | false / `""` / false / 86400 / false / true / `localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1` |
| `input_telegraf_enabled` / `_debug_enabled` / `_delta_counter_normalize_default` / `_seconds` / `telegraf_object_deadtime_ms` | true / false / true / 30 / 35000 (`scouter-telegraf.xml`이 있으면 파일 값이 덮어씀, 그리고 enabled면 `net_http_server_enabled=true` 강제 `:1052-1056`) |
| `input_telegraf_$measurement$_*` | `_enabled` true, `_debug_enabled` false, `_tag_filter/_counter_mappings/_objFamily_base/_objFamily_append_tags/_objType_base/_objType_append_tags/_objType_icon/_objName_base/_objName_append_tags/_host_mappings` `""`, `_objType_prepend_tags` `scouter_obj_type_prefix`, `_host_tag` `host` |
| `mgr_text_db_daily_service_enabled` / `_api_enabled` / `_ua_enabled` | false / false / false |

## 4. counters-site(`serverj/CounterManager.java`)

### 4.1 파일과 engine

- 기본 XML은 classpath `/scouter/lang/counters/counters.xml`, custom은 `./conf/counters.site.xml`(`:57,82-89`). 기동 시 둘 다 `CounterEngine.parse`하고 `xmlCustomContent`는 파일 bytes 그대로 보관(GET_XML_COUNTER가 Blob으로 반환).
- `CounterEngine.parse(byte[])`(`common/lang/counters/CounterEngine.java:66-224`): DOM으로 `<Family>` 전부(attribute `name`,`master` 외는 `setAttribute`로 보존; 자식 `<Counter>`의 `name,disp,unit,icon,all,total`), 이어서 `<ObjectType>` 전부(`name,disp,family→familyMap.get,icon,sub-object`; 같은 name이 이미 있으면 기존 객체에 Counter만 추가)를 `familyMap/objTypeMap`(`StringKeyLinkedMap`)에 누적한다. 실패 시 `printStackTrace` 후 false. `clear()`는 두 map clear(`:226-229`). TAG/ATTR 상수(`:46-61`): `Counters, Familys, Types, Family, Counter, ObjectType`, `name, master, disp, unit, icon, family, all, total, sub-object`.
- `reloadEngine`은 `engine.clear(); parse(default) && parse(custom)`이며 non-atomic이라 reload 중 조회는 빈 engine을 볼 수 있다(`:492-495`).

### 4.2 DEFINE/EDIT_OBJECT_TYPE

- 요청 key는 literal `name`, `disp`, `family`, `icon`(Text), `sub-object`(BooleanValue)(`client/configuration/actions/DefineObjectTypeAction.java:222-228`). 서버는 `getText`/`getBoolean`으로 읽어 `ObjectType{name, displayName, icon, family=engine.getFamily(family), subObject}`를 만든다(`:264-280,322-335`).
- `addObjectType(MapPack)`: `engine.getObjectType(name)!=null`이면 false. `appendObjectType`(custom DOM에 `<Counters><Types>`가 없으면 생성 후 `<ObjectType>` append)(`:348-368`) → `XmlUtil.writeXmlFileWithIndent`로 문자열 생성 → `saveAndReloadCountersSiteXml(result)`(`:307-320`). `setObjectTypeAttribute`는 `name, disp, family(=objType.getFamily().getName()), icon, sub-object` 순으로 attribute를 넣고 `listObjectTypeCounters()`를 `<Counter>`로 추가한다(`:419-435`). **family가 engine에 없으면 `getFamily()` null → NPE**가 `appendObjectType`의 catch(Exception)에서 잡혀 null → false다(응답 `BooleanValue(false)`). 단 `editObjectType`도 같은 catch 경로라 false.
- `editObjectType(MapPack)`: `editOrAppendObjectType`이 같은 name의 `<ObjectType>` attribute를 교체(없으면 append)한 뒤 `writeXmlFileWithIndent`를 호출하지만 **반환 문자열을 버리고 파일을 읽어 reload**하므로 디스크·engine 모두 변경되지 않은 채 true를 돌려준다(`:337-346`, §9-2).
- Client는 성공 시 `REMOTE_CONTROL_ALL{command:"REFETCH_COUNTER_XML", fromSession}`을 별도 전송한다(`DefineObjectTypeAction.java:236-247`). 서버 DEFINE/EDIT handler 자체는 `notifyAllClients`를 호출하지 않는다.

### 4.3 SET_CONFIGURE_COUNTERS_SITE·normalize

- 순서: `new CounterEngine().parse(contents.getBytes("utf-8"))`(임시 engine, 실패 시 false) `&&` `saveAndReloadCountersSiteXml(contents)` → 성공 시 `RegisterHandler.notifyAllClients()`(`ConfigureService.scala:256-268`).
- `saveAndReloadCountersSiteXml`(`:505-523`): `normalizeCountersXml` 성공 문자열(JAXBException이면 원문)을 `FileUtil.saveText(customFile.getCanonicalPath())`(platform charset, 덮어쓰기)로 저장 → 파일 재읽기 → `reloadEngine` 결과 반환. `normalizeCountersXml`의 **NPE 등 JAXBException이 아닌 예외는 잡히지 않아** handler 예외 → 응답 없음.
- `normalizeCountersXml`(`:525-558`, JAXB `Counters0/Types0/ObjectType0/Familys0/Family0/Counter0` `:560-643`): `Types` null이면 빈 list; `types.objectType` null(`<Types/>`만 있을 때)이면 **NPE**(로컬 실행 확인). ObjectType은 `name` 기준 `Collectors.toMap`(뒤 항목 우선) → `HashMap.values()` 순서(**입력 순서 아님, name hash 순**). Family는 이름에 `zws-metric.`/`zps-metric.`를 포함하면 제거 후 같은 dedupe. **`ObjectType0`에는 `Counter` field가 없어 `<ObjectType>` 아래 `<Counter>`는 모두 삭제**되고, `Family`의 unknown attribute도 삭제된다.
- JAXB RI 2.3 formatted 출력은 두 조합으로 실행했고 출력이 **byte 동일**했다. ① JDK 18 + glassfish jaxb-runtime 2.3.2(원본 pom의 jaxb-impl 2.3.0.1은 JDK 18에서 Injector 오류로 실행 불가) ② JDK 8 + 원본 배포 `scouter-server-2.21.3` lib의 jaxb-impl 2.3.0.1. 출력 형식: 첫 줄 `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`, 4-space indent, `<Types>` 다음 `<Familys>`, attribute는 **field 선언 순서** = `ObjectType disp, family, icon, name, sub-object` / `Family master, name` / `Counter all, disp, icon, name, total, unit`. 입력에 없던 attribute는 field 초기값(`""`, `Counter.all`은 `"true"`)으로 **항상 출력**된다. 예: 입력 `<ObjectType name="t2" family="myfam"/>` → `<ObjectType disp="" family="myfam" icon="" name="t2" sub-object=""/>`, 빈 입력 `<Counters/>` → `<Counters>\n    <Types/>\n    <Familys/>\n</Counters>\n`.
- `RegisterHandler.notifyAllClients()`(`serverj/http/handler/RegisterHandler.java:177-188`): `AsyncRun` queue에 넣어 비동기로 `RemoteControl("REFETCH_COUNTER_XML", now, new MapPack(), commander=0)`을 현재 로그인 session 전부에 `RemoteControlManager.add`한다. Client는 CHECK_JOB으로 받아 `GET_XML_COUNTER`를 재조회한다(`client/remote/handle/ActionControl.java:58-...`).

### 4.4 counters.site.xml 레이아웃 fixture 후보

입력(Family 2개 중복·zws-metric 제거·ObjectType 중복·ObjectType 하위 Counter 포함)과 출력은 §11의 일회성 oracle 실행에서 확인했다(산출물은 저장소에 없다). Go 구현은 "정규화 후 JAXB 레이아웃"을 결정적 순서(§10 결정 필요)로 쓰고, Java `CounterEngine.parse`가 읽을 수 있음을 oracle로 증명하면 된다.

### 4.5 CounterManager 전체 메서드(`serverj/CounterManager.java`)

| 메서드(줄) | 동기화 | 의미 | 디스크 기록 | 호출자 |
|---|---|---|---|---|
| `getInstance`(67-76) | DCL | 싱글톤. 생성자에서 `init` | – | 전역 |
| `init`(82-89) | – | classpath `counters.xml` → `xmlContent` + parse. `./conf/counters.site.xml`을 읽을 수 있으면 `xmlCustomContent` + parse | 없음 | 생성자 |
| `readAndParseXml`(91-101) | – | resource 읽기 실패 시 로그만 남김 | 없음 | `init` |
| `getCounterEngine/getXmlContent/getXmlCustomContent`(103-113) | – | 현재 engine/원본 bytes. custom은 파일 없으면 null | – | GET_XML_COUNTER, 조회 전반 |
| `safelyAddFamily`(125-154) | sync | 메모리 캐시 `unsafeDoc`(최초 1회 `getCustomDocument` + 누락 `Counters/Familys/Types` 생성, `initUnsafeDoc` 215-237)에서 같은 name `<Family>`를 **교체**하고 없으면 append → `saveCustomContent` | **있음**(정규화 후 저장, 197-213) | Telegraf 입력(`TelegrafInputHandler.java:233,260`) |
| `safelyAddObjectType`(161-178) | sync | 같은 방식으로 `<ObjectType>` 교체/append | 있음 | Telegraf 입력(`:234`) |
| `findElementByTypeAndName`(180-195) | – | `unsafeDoc`에서 tag+name 첫 요소 | – | 위 2개 |
| `saveCustomContent`(197-213) | – | Xalan 문자열 → `normalizeCountersXml`(JAXBException이면 원문) → `saveText(canonicalPath)` → 재읽기 → `reloadEngine` | 있음 | safely* |
| `addFamily`(239-248) | sync | `getCustomDocument`에 `<Family>` append 후 `writeXmlFileWithIndent`, **반환값 버림** → 파일 재읽기 → reload | **없음**(§9-2와 같은 결함). 호출자 없음 | – |
| `addFamilyAndObjectType`(250-262) | sync | Family+ObjectType append, 반환값 버림 | **없음**인데 true | HTTP `RegisterHandler.java:121`(성공 시 `notifyAllClients`) |
| `addObjectType(MapPack)`(264-280) | – | key literal `name, disp, family, icon, sub-object`(`CounterEngine.ATTR_*`)로 `ObjectType` 생성. 이미 engine에 있으면 false | → 아래 | DEFINE_OBJECT_TYPE |
| `addObjectTypeIfNotExist(ObjectPack)`(282-305) | – | engine에 objType이 없고 `tags.detected`(`ScouterConstants.TAG_OBJ_DETECTED_TYPE`)가 기존 type이면 그 family/icon/subObject를 복사해 등록. `log_udp_some_object`와 같은 type이면 stdout debug | → 아래 | **Agent heartbeat마다** `AgentManager.active`(`AgentManager.scala:87`) |
| `addObjectType(ObjectType)`(307-320) | sync | `appendObjectType` → Xalan 문자열 → `saveAndReloadCountersSiteXml` | 있음 | 위 2개 |
| `editObjectType(MapPack)`(322-335) | – | 같은 key로 ObjectType 생성(존재 검사 없음) | → 아래 | EDIT_OBJECT_TYPE |
| `editObjectType(ObjectType)`(337-346) | sync | `editOrAppendObjectType`, 반환 문자열 버림 → 재읽기·reload → true | **없음**(§9-2) | EDIT_OBJECT_TYPE, `RegisterHandler.java:164` |
| `appendObjectType`/`editOrAppendObjectType`/`appendFamily`(348-468) | – | 누락 root/`Types`/`Familys` 생성 후 append/교체. 예외는 `Logger.printStackTrace` 후 null | – | 위 |
| `setObjectTypeAttribute`(419-435) | static | attribute `name, disp, family, icon, sub-object("true"/"false")` + `listObjectTypeCounters()` → `<Counter name disp unit icon all total>` | – | 위 |
| `getCustomDocument`(470-485) | – | custom 파일을 읽을 수 있으면 DOM parse + `normalize()`, 아니면 빈 `newDocument()`. 예외 시 null | – | 위 |
| `reloadEngine`(492-495) | – | `clear` 후 default·custom parse. **non-atomic**. custom bytes가 null(파일 없음)이면 `ByteArrayInputStream(null)` NPE를 parse가 catch → false(이미 default는 적재됨) | – | 위 |
| `readCountersSiteXml`(497-503) | – | 파일 있으면 UTF-8 text, 없으면 `""` | – | GET_CONFIGURE_COUNTERS_SITE |
| `saveAndReloadCountersSiteXml`(505-523) | **없음** | §4.3 | 있음 | SET_CONFIGURE_COUNTERS_SITE, `addObjectType` |
| `normalizeCountersXml`(525-558) | – | §4.3 | – | 위 |

- 동시성: `saveAndReloadCountersSiteXml`은 synchronized가 아니다. SET_CONFIGURE_COUNTERS_SITE와 heartbeat 자동 등록(`addObjectTypeIfNotExist`)이 겹치면 마지막 쓰기가 이긴다(lost update 가능). `safely*`는 기동 후 최초 1회 읽은 `unsafeDoc`을 계속 쓴다. custom 파일이 없을 때 `initUnsafeDoc`은 빈 `<Types/>`를 만든다. 이 상태에서 `safelyAddFamily`가 먼저 호출되면 정규화 NPE(§4.3)가 `saveCustomContent` 밖으로 전파된다(정적 분석, Telegraf 범위). 그래서 SET_CONFIGURE_COUNTERS_SITE로 바꾼 내용이 다음 Telegraf 자동 등록 때 **이전 DOM으로 덮인다**(§9-3).
- `CounterEngine` 상수(`common/lang/counters/CounterEngine.java:46-61`): `TAG_COUNTERS="Counters"`, `TAG_FAMILYS="Familys"`, `TAG_TYPES="Types"`, `TAG_FAMILY="Family"`, `TAG_COUNTER="Counter"`, `TAG_OBJECT_TYPE="ObjectType"`, `ATTR_NAME="name"`, `ATTR_MASTER="master"`, `ATTR_DISPLAY="disp"`, `ATTR_UNIT="unit"`, `ATTR_ICON="icon"`, `ATTR_FAMILY="family"`, `ATTR_ALL="all"`, `ATTR_TOTAL="total"`, `ATTR_SUBOBJECT="sub-object"`.
- `notifyAllClients`의 `AsyncRun`(`serverj/util/AsyncRun.java:21-48`)은 단일 thread와 `RequestQueue(128)`로 구성된다. `put`은 가득 차면 **새 작업을 버리고 false**를 반환하며 호출자는 결과를 무시한다(`common/util/RequestQueue.java:88-97`). 하나의 `RemoteControl` 객체(빈 param, commander 0)를 모든 session queue가 공유한다. `getLoginUserList()`의 null 원소 NPE는 `AsyncRun`이 로그만 남기고 계속 진행한다.

## 5. Alert script(`serverj/plugin/alert/AlertRuleLoader.java`)

- 파일명: `<plugin_dir>/<counterName>.alert`, `.conf`(`:51-52,102,118,128`). 원본은 `counterName`을 검증하지 않으며 null이면 `null.alert`다. Go는 separator·`.`·`..`가 든 이름을 거절한다(§9-9). `saveRuleContents/saveConfigContents`는 `FileUtil.saveText`(parent mkdirs, platform charset 덮어쓰기, 예외 시 false)(`common/util/FileUtil.java:156-170`). 조회는 `canRead`면 bytes→`new String`, 아니면 `""`(`:99-110`).
- 5초 루프(`:66-79`): `plugin_dir`가 읽기 가능하면 `checkModified` → `checkNewRule`. `checkNewRule`은 `*.alert`마다 확장자 제거 이름(`lastIndexOf('.')`)이 `alertRuleTable`에 없으면 compile(`createRule`), 성공 시 `.conf`도 로드(`:133-150`). `checkModified`는 등록된 rule마다 파일이 없으면 `clear`(메시지 `Alert rule cleared : <name>`), mtime이 다르면 재컴파일, `.conf` mtime이 다르면 재로드(`:157-181`).
- 컴파일(`:217-273`): javassist로 `scouter.server.alert.impl.<name>` 클래스의 `process(RealCounter)` body를 `"{" + RealCounter + " $counter=$1;" + <파일 내용> + "\n}"`로 설정. 성공 메시지 `Alert rule detected : <file name>`; `CannotCompileException`은 `(mtime, hash(filename))` 서명을 `compileErrorFiles`에 넣어 같은 파일을 재시도하지 않고 메시지 `Alert rule compile error : <ee.getMessage()>`; 그 외 예외는 `error : <e.getMessage()>`. 메시지는 모두 `AlertScriptLoadMessageCache.put`.
- `AlertConf`(`AlertConf.java:21-26`): `history_size`, `silent_time`, `check_term` 모두 기본 0(`.conf` 없거나 키 없음/빈 값이면 0, `CastUtil.cint`로 parse)(`:197-214,284-289`).
- `AlertScriptLoadMessageCache`(`server/core/cache/AlertScriptLoadMessageCache.scala:21-31`) = `AlertScriptLoadMessageLoopCache(50)`(`AlertScriptLoadMessageLoopCache.scala:23-82`): 용량 50 ring. `put`은 `queue(index)=msg; index++; index>=50이면 loop++, index=0`. `get(last_loop,last_index)`는 `end_loop-start_loop`가 0이면 `[start_index, end_index)`(단 `start_index<end_index`일 때만), 1이면 `start_index<=end_index`면 `[end_index,50)+[0,end_index)`(전체 한 바퀴) 아니면 `[start_index,50)+[0,end_index)`, 2 이상이면 전체 한 바퀴. null slot은 건너뛴다. 응답 `loop/index`는 현재 cursor이며 Client는 다음 요청에 그대로 돌려준다. Client 흐름(`client/configuration/views/AlertScriptingView.java:211-227,636-667`): 저장(Ctrl+S) 시 먼저 현재 cursor(최초 `(0,0)`)로 `first=true` 조회를 보내 cursor만 갱신하고 메시지는 버린다(비동기). 이어서 `.conf` 저장 → `.alert` 저장 → 1·3·5·7초 시점에 4회 재조회해 console에 append한다. 5초 loader 주기 안에 compile 메시지를 잡기 위한 설계다. `getList`는 lock 없이 `index/loop`를 읽고, 응답 cursor는 목록 생성 뒤 다시 읽으므로 사이에 들어온 메시지는 누락되거나 중복될 수 있다(`:40-42,75-81`). 경계: `end_loop - start_loop`가 음수(서버 재시작 후 Client의 오래된 loop)면 `case _`로 한 바퀴 전체를 돌려준다. 같은 loop의 `start_index` 음수는 `queue(-1)` AIOOBE → 응답 없음이다. `start_index >= 50`은 같은 loop면 빈 목록, 직전 loop면 `[0,end_index)`만 반환한다. **cache 객체는 항상 존재하므로 응답 없음 경우는 없다**(server-metadata-query 표의 "cache 없음은 item 없음"은 보수적 표현). 메시지가 없으면 `messages`는 빈 List.
- `GET_ALERT_REAL_COUNTER_DESC`(`RealCounter.java:466-513`): `getDeclaredMethods()` 중 non-static public이고 `@Deprecated`/`@Internal`이 없는 메서드. `parameterTypeNames`는 `@ParamDesc`가 있으면 그 문자열 **1개**, 없으면 parameter class `getName()` 목록. `desc`는 `@ConfigDesc` 값(없으면 null → 빈 Text). **`getDeclaredMethods()` 순서는 명세상 비결정적**이며 서버는 첫 호출 결과를 static에 캐시한다(`RealCounter.java:472-475`). 공식 `scouter-server-2.21.3.jar`(baseline HEAD와 `v2.21.3` 사이 server 소스 차이 없음, `git diff --stat v2.21.3 HEAD`는 `scouter.webapp/pom.xml`만 출력)를 reflection으로 실행했다. 원소 집합은 아래 표와 정확히 일치했고, **순서는 JDK 8과 JDK 18에서 달랐다**(§5.1). Java Client는 받은 descriptor를 `"$counter."/"$$." + methodName + "(" + params.join(", ") + ")"` 키의 `TreeMap`에 넣는다. 그래서 **화면 순서는 서버 순서와 무관하게 signature 사전순**이며, 같은 signature는 뒤 항목이 덮는다(`client/configuration/views/AlertScriptingView.java:579-626`). 따라서 Go는 아래 표 순서(소스 선언 순서)로 고정해도 Java Client 동작이 같다. 전체 25개:

| methodName | returnTypeName | parameterTypeNames | desc |
|---|---|---|---|
| getCounter | java.lang.String | [] | get current counter name. |
| getObjHash | int | [] | get current object hash value. |
| getObjName | java.lang.String | [] | get current object name. |
| getObjType | java.lang.String | [] | get current object type name. |
| getObjHashListString | java.lang.String | [java.lang.String] | get comma separated object hashes string of the given object type. |
| getObjHashListWithParentsString | java.lang.String | [java.lang.String] | get comma separated object hashes string of the given object type. |
| getIntValue | int | [] | get value of current counter as int. |
| getFloatValue | float | [] | get value of current counter as float. |
| getHistorySize | int | [] | get the history size of the counter. |
| getOverCount | int | [`int value, int sec`] | get how many times exceed the given value of the counter. |
| getOverCount | int | [`float value, int sec`] | get how many times exceed the given value of the counter. |
| getAvgToInt | int | [`int fromAgoSec, int durationSec`] | get average value of the counter in the given duration as int. |
| getLatestAvg | float | [`int durationSec`] | get latest average value of the counter in the given duration. |
| getLatestAvgToInt | int | [`int durationSec`] | get latest average value of the counter in the given duration as int. |
| getAvg | float | [`int fromAgoSec, int durationSec`] | get average value of the counter in the given duration. |
| getHistoryOldestTime | long | [] | get oldest time of the counter's history. |
| getHistoryCountInSec | int | [int] (ParamDesc 없음) | get how many values (of the current counter) exist in the seconds. |
| getFloatValue | float | [`String counter`] | get the counter's value as float. |
| getFloatValue | float | [`String objectFullName, String counter`] | get the another objects counter's value as float. |
| getIntValue | int | [`String counter`] | get the counter's value as int. |
| getIntValue | int | [`String objectFullName, String counter`] | get the another objects counter's value as int. |
| info | void | [`String title, String message`] | alert on info level. |
| warn | void | [`String title, String message`] | alert on warn level. |
| error | void | [`String title, String message`] | alert on error level. |
| fatal | void | [`String title, String message`] | alert on fatal level. |

제외: `@Deprecated` 17개(`counter, objHash, objName, objType, value, intValue, floatValue, historySize, overCount×2, getAvgtoInt, getLatestAvgtoInt, historyOldestTime, historyCount, floatValue(String), intValue(String), warning`), `@Internal` 10개(`setValue, setAlertTime, historySize(int), addValueHistory, silentTime×2, checkTerm×2, lastAlertTime, counterNames`), static `getRealCounterDescription`.

- `GET_PLUGIN_HELPER_DESC`(`serverj/plugin/PluginHelper.java:704-745`, 같은 규칙) 22개:

| methodName | returnTypeName | parameterTypeNames | desc |
|---|---|---|---|
| log | void | [java.lang.Object] | logging |
| println | void | [java.lang.Object] | System.out.println |
| urlEncode | java.lang.String | [java.lang.String] | url encoding |
| urlDecode | java.lang.String | [java.lang.String] | url decoding |
| toShortenUrl | java.lang.String | [java.lang.String, java.lang.String] | make shorten url. |
| getNumberFormatter | java.text.NumberFormat | [] | get NumberFormatter set fraction 1 |
| getNumberFormatter | java.text.NumberFormat | [`int fractionDigits`] | get NumberFormatter set the fraction |
| formatNumber | java.lang.String | [float] | format the number as fraction 1 |
| formatNumber | java.lang.String | [`float f, int fractionDigits`] | format the number as given fraction |
| formatNumber | java.lang.String | [double] | format the number as fraction 1 |
| formatNumber | java.lang.String | [`double v, int fractionDigits`] | format the number as given fraction |
| formatNumber | java.lang.String | [int] | format the number as fraction 1 |
| formatNumber | java.lang.String | [`int v, int fractionDigits`] | format the number as given fraction |
| formatNumber | java.lang.String | [long] | format the number as fraction 1 |
| formatNumber | java.lang.String | [`long v, int fractionDigits`] | format the number as given fraction |
| alertInfo | void | [`int objHash, String objType, String title, String message`] | alert as info level |
| alertWarn | void | 동일 | alert as warn level |
| alertError | void | 동일 | alert as error level |
| alertFatal | void | 동일 | alert as fatal level |
| toMap | java.util.HashMap | [`String[] keyValueArray`] | make hashMap easily |
| httpGet | void | [`String url, Map<String, String> paramMap`] | request http get |
| httpGet | void | [`String url, Map<String, String> paramMap, int timeoutMillis`] | request http get |

나머지 public 메서드(`getParamsString, alert, get*String×30, invokeMethod×3, newInstance×5, getFieldValue, makeArgTypes×8, makeArgs×8`)는 전부 `@Internal`, `getInstance`는 static이다(`:74-76,234-681`).

### 5.1 descriptor 실제 반환 순서(실측)

같은 jar·같은 class file에서 JDK별 `getDeclaredMethods()` 순서(번호는 응답 item 순서):

| JDK | RealCounter 순서 |
|---|---|
| Zulu 8 | error, getCounter, getObjHash, getObjName, getObjType, getObjHashListString, getObjHashListWithParentsString, getIntValue(String,String), getIntValue(String), getIntValue(), getFloatValue(String), getFloatValue(String,String), getFloatValue(), getHistorySize, getOverCount(int), getOverCount(float), getAvgToInt, getLatestAvg, getLatestAvgToInt, getAvg, getHistoryOldestTime, getHistoryCountInSec, info, warn, fatal |
| Zulu 18 | getObjHash, getObjName, getObjType, getObjHashListString, getObjHashListWithParentsString, getIntValue(), getIntValue(String,String), getIntValue(String), getFloatValue(), getFloatValue(String), getFloatValue(String,String), getHistorySize, getOverCount(int), getOverCount(float), getAvgToInt, getLatestAvg, getLatestAvgToInt, getAvg, getHistoryOldestTime, getHistoryCountInSec, fatal, info, error, warn, getCounter |

| JDK | PluginHelper 순서 |
|---|---|
| Zulu 8 | println, log, toMap, urlEncode, urlDecode, toShortenUrl, getNumberFormatter(), getNumberFormatter(int), formatNumber(float), formatNumber(double), formatNumber(double,int), formatNumber(int), formatNumber(int,int), formatNumber(long), formatNumber(float,int), formatNumber(long,int), alertInfo, alertWarn, alertError, alertFatal, httpGet(3인자), httpGet(2인자) |
| Zulu 18 | urlEncode, urlDecode, toShortenUrl, getNumberFormatter(), getNumberFormatter(int), formatNumber(float,int), formatNumber(double), formatNumber(double,int), formatNumber(int,int), formatNumber(int), formatNumber(long,int), formatNumber(long), formatNumber(float), alertInfo, alertWarn, alertError, alertFatal, httpGet(2인자), httpGet(3인자), println, log, toMap |

결론: 원본 서버 응답 순서는 JVM 구현·버전에 의존하므로 byte 일치 fixture로 고정하지 않는다. 테스트는 ① 원소 집합(4 field 값) 일치와 ② Go 응답 순서의 결정성만 검증한다.

## 6. 관리

### 6.1 SERVER_DB_DELETE

- 각 `file` 항목이 가리키는 `db_dir`(`DBCtr.getRootPath()`, `server/db/DBCtr.scala:30`) 아래 위치를 재귀 삭제한다(`deleteFiles`: 디렉터리면 자식 먼저, `exists()==false`면 무시, `delete()` 결과 무시)(`ManageService.java:57-74,143-153`). `SERVER_DB_LIST`가 주는 이름은 `"/20261015"`, `"/20261015/xlog"`처럼 separator로 시작하고 하위 디렉터리도 포함하며(`:38-48,127-142`), Client는 이 이름을 그대로 돌려보낸다. 원본은 `file` 값이 LIST가 준 이름인지 검증하지 않는다(§9-4). Go는 삭제 전에 모든 이름을 검증하고 하나라도 어긋나면 아무것도 지우지 않는다([storage-contract](storage-contract.md) "DB 경로 삭제"). Client는 LIST로 고른 디렉터리 이름만 보낸다(`client/views/ServerFileManagementView.java:332-345`). 응답 `size`는 요청 List 길이이지 실제 삭제 수가 아니며, Client는 이를 `"N directories is deleted."`로 표시한다.
- 원소 읽기: `ListValue.getString(i)`는 TextValue면 값, 다른 Value면 `toString()`, null이면 null이다(`common/lang/value/ListValue.java:95-103`). null 원소는 문자열 `null`로 취급된다. `file`이 ListValue가 아니면 ClassCastException → 응답 없음.
- 원본 `deleteFiles`는 symlink를 구분하지 않는다. Go는 symlink를 따라가지 않는다(§9-4). 읽을 수 없는 디렉터리는 `listFiles()`가 null → NPE → 그때까지만 삭제되고 응답 없음이다. 개별 `delete()` 실패는 무시한다. 삭제 중인 날짜의 열린 writer/index는 서버가 알지 못한 채 계속 쓴다. 오늘 날짜를 지우면 이후 쓰기는 삭제된 inode나 재생성 파일로 가는데, 이 동작은 플랫폼에 따라 다르다.

### 6.2 REMOTE_CONTROL / REMOTE_CONTROL_ALL / CHECK_JOB

- `RemoteControl(commnad, time, param=요청 MapPack 전체, commander=fromSession)`(`server/management/RemoteControl.scala:23-32`). CHECK_JOB은 이 `param`(toSession/fromSession/command 포함 원본)을 두 번째 item으로 돌려준다.
- `RemoteControlManager.add(toSession, control)`(`RemoteControlManager.scala:37-51`): session별 `scouter.util.Queue(5)`(내부 배열 6). `Queue.push`(`common/util/Queue.java:31-41`)는 가득 찼을 때 **새 항목을 쓰고 가장 오래된 항목을 버린 뒤 null 반환** → `add`는 false(S148 로그)지만 명령은 queue에 남아 있다. `getCommand`는 dequeue 1개(`:53-63`). 5초 daemon이 `LoginManager.getUser(session)==null`인 session queue를 제거(`:69-78`). session 만료 전엔 queue 상한 5로 메모리 제한.
- REMOTE_CONTROL 응답은 `add` 결과(`success:Boolean`). 로그 문자열 생성에서 `LoginManager.getUser(commander).ip()`/`getUser(session).ip()`를 호출하므로 두 session 중 하나라도 유효하지 않으면 NPE → 응답 없음(이미 enqueue는 완료). REMOTE_CONTROL_ALL은 `getLoginUserList()` 전체(요청자 자신 포함)에 enqueue하고 응답을 쓰지 않는다(`:98-112`). `getLoginUserList`는 `sessionTable.size()` 크기 배열을 만들어 keys 순회로 채우므로 순회 중 만료되면 null 원소가 들어가 `users[i].session()` NPE 가능(`LoginManager.scala:87-95`).
- CHECK_JOB은 요청 `session` 값의 queue를 그대로 dequeue한다. 요청자 자신의 session인지는 확인하지 않는다(§9-13). Go는 요청 연결의 인증 session과 같을 때만 dequeue한다. `commandTable`(`LongKeyLinkedMap`)은 임의 `toSession`으로도 항목이 생기지만, 5초 daemon이 비로그인 session 항목을 지워 상한이 생긴다.
- Client가 보내는 command 값: `EXIT_CLIENT`, `RESTART_CLIENT`, `POPUP_MESSAGE`(추가 키 `message`, `keeptime`), `REFETCH_COUNTER_XML`(`client/remote/RemoteCmd.java:20-25`, `client/views/LoginUserView.java:288-289,310-320,390-402`). Client는 2초마다 CHECK_JOB을 보낸다(`client/remote/CheckMyJob.java:125-166`).

### 6.3 OBJECT_REMOVE / OBJECT_REMOVE_IN_MEMORY / OBJECT_REMOVE_INACTIVE

- 요청 body는 Pack이 아니라 `din.readValue()`로 읽는 ListValue이며 각 원소는 `getLong(i).toInt`(NumberValue 아니면 0)(`AgentInfo.scala:160-164`). Client/webapp에 호출자는 없다(검색 결과 없음).
- `AgentManager.removeAgents(list, permanent)`(`server/core/AgentManager.scala:379-386`): 각 objHash를 `objMap.remove`(`ObjectMap.scala:83-…`: objectTable/objectNameTable/type 집합에서 제거) 후 `permanent`면 `ObjectWR.remove(objHash)`. `OBJECT_REMOVE_INACTIVE`는 `alive==false`인 ObjectPack만 메모리에서 제거(`:219-228`).
- `ObjectWR.remove`(`server/db/ObjectWR.scala:117-122`)는 `RequestQueue(DBCtr.MAX_QUE_SIZE=10000)`에 `createRm` Order를 넣는다. 가득 차면 S132 로그 후 버린다.
- **원본 실제 동작: 디스크 삭제가 일어나지 않는다.** `Order.add` 기본값은 `true`이고(`:90-95`) `createRm`은 `delete=true, hash`만 설정한다(`:104-109`, 2015년 최초 커밋부터 동일). writer loop(`:53-88`)는 `if (ord.add)`를 먼저 실행해 `ord.pack(null).objHash`에서 NPE를 낸다. 이 NPE를 `catch Throwable → printStackTrace`가 삼키므로 같은 Order의 `if (ord.delete)` 분기에 도달하지 못한다. 결과적으로 OBJECT_REMOVE는 디스크 측면에서 OBJECT_REMOVE_IN_MEMORY와 같고, 오늘 object DB가 없으면 open(디렉터리 생성)만 일어난다. **실행 확인(2026-10-15, 원본 `ObjectWR/DBCtr/ObjectIndex/ObjectData/IndexKeyFile/RealKeyFile/MemHashBlock`을 수정 없이 Scala 2.10.4·JDK 8로 컴파일, §11)**: `ObjectWR.add(pack)` 후 index 위치 0 → `ObjectWR.remove(hash)` 후 stderr에 `java.lang.NullPointerException at scouter.server.db.ObjectWR$$anonfun$1.apply$mcV$sp(ObjectWR.scala:69)` 1건, index 위치는 그대로 0(삭제 안 됨) → 같은 hash로 `Order{add=false, delete=true}`를 직접 queue에 넣으면 위치 -1(delete marker 기록). 즉 결함은 `createRm`이 `add=true`를 남기는 것뿐이고 delete 분기 자체는 동작한다. 실행 직후 디렉터리에는 `obj.data`, `obj.kfile`만 있었고 `obj.hfile`은 flush/close 전이라 아직 없었다.
- 의도된 동작(삭제 분기가 실행된다면): **오늘 날짜 DB**(`<db_dir>/<yyyymmdd>/object/obj.{data,hfile,kfile}`, `:156-181`)에서 `index.get(hash) >= 0`일 때 `ObjectIndex.delete(hash)`를 호출한다. `IndexKeyFile.delete`(`server/db/io/IndexKeyFile.scala:145-164`)는 hash chain에서 key가 같은 **모든 미삭제 record의 deleted flag를 true**로 바꾸고 `.data` slot은 남긴다. 과거 날짜 파일은 건드리지 않는다. 이후 같은 objHash의 ObjectPack이 오면 `index.get`이 -1이라 새 slot을 append한다(`:69-75`). 삭제는 비동기라 바로 이어지는 `getFullAgentList`가 아직 record를 읽을 수 있다.
- 살아 있는 Agent는 다음 heartbeat에서 `AgentManager.active`로 메모리에 다시 등록된다. 따라서 세 명령 모두 실질적으로는 "죽은 객체 정리"용이다.
- 응답 `getFullAgentList`(`AgentInfo.scala:171-194`): `ObjectRD.getObjectList(today)`(`.kfile` 첫 record부터 순서, deleted 제외, `ObjectRD.scala:78-116`, `IndexKeyFile.scala:187-206`)의 각 ObjectPack에 대해 메모리에 없으면 DB pack의 `tags.status="dead"`, `alive=false`; 있으면 메모리 pack에 `tags.status="active"|"inactive"`를 **넣은 채**(메모리 객체 변형) 전송, item마다 flush. 기동 시 `AgentManager.read()`가 오늘 DB를 메모리에 다시 올리므로(`:43-46`) IN_MEMORY 제거는 재시작 후 되살아난다. `OBJECT_REMOVE_INACTIVE`/`OBJECT_RESET_CACHE`/`OBJECT_SYSTEM_GC`/`REMOTE_CONTROL_ALL`은 응답 item이 0이므로 Client `getSingle`은 null이고 Client 코드는 이를 정상으로 취급한다.

## 7. Agent relay 변경 명령 21개

모든 handler는 `readPack cast` 또는 `readMapPack`(동일 의미)으로 MapPack을 읽고 `objHash`로 `AgentManager.getAgent`를 찾아 `AgentCall.call`한다. 응답은 `p != null`일 때만 `HasNEXT + writePack(p)`이며, `OBJECT_RESET_CACHE`(`AgentInfo.scala:147-156`)와 `OBJECT_SYSTEM_GC`(`DumpService.scala:133-139`)는 반환값을 버려 **항상 응답 없음**이다. relay null(객체 없음·세션 없음·timeout·Agent handler 미등록으로 Agent가 "unknown cmd" 빈 응답)은 응답 없음이다. 요청 MapPack은 추가 키·순서 그대로 Agent로 전달된다. 서버 측 policy·objType 검사는 없다.

| 명령 | 서버 읽기 | Java agent handler(`agentj/netio/request/handle/`) | Agent 응답 |
|---|---|---|---|
| OBJECT_CALL_HEAP_DUMP | readMapPack | `AgentHeapDump.java:50-96` | `{success:Boolean, msg:Text}`; 10초 내 재요청은 `success=false, msg="please wait 10 sec. from last request..."`. 파일명 `yyyyMMdd-HHmmss<fName>.hprof` |
| OBJECT_DELETE_HEAP_DUMP | readMapPack | 같은 파일(`delfileName`) | `{success, msg}` |
| OBJECT_CALL_BLOCK/MUTEX/CPU_PROFILE | readMapPack | **없음**(golang pprof 전용, `RequestCmd.java:72-74` 주석) | Java agent는 unknown cmd → 응답 없음 |
| OBJECT_RESET_CACHE | readPack cast | `AgentEnv.java:80-84` | param echo(서버가 버림) |
| OBJECT_SYSTEM_GC | readMapPack | `AgentEnv.java:44-50` | 빈 MapPack(서버가 버림) |
| OBJECT_THREAD_CONTROL | readMapPack | `AgentThread.java:160-196` | `ThreadUtil.getThreadDetail(id)` + 활성 서비스면 `Service Txid(Text hexa32), Service Name, Service Elapsed(Decimal), SQL?, Subcall?` |
| PSTACK_ON | readPack cast | `AgentThread.java:327-336` | **param echo**(`time<=0`이면 off) |
| TRIGGER_ACTIVE_SERVICE_LIST / THREAD_LIST / THREAD_DUMP | readMapPack | `AgentThread.java:305-318` → `DumpUtil.trigger*` | `{name:<dump file name>}`; 실패 시 빈 MapPack |
| TRIGGER_HEAPHISTO | readMapPack | `AgentHeapHisto.java:38-41` | 동일 |
| TRIGGER_BLOCK/MUTEX_PROFILE | readMapPack | 없음(golang 전용) | 응답 없음 |
| SET_CONFIGURE_WAS | readPack cast | `AgentConfigure.java:57-67`(host/batch agent도 동일 키) | `{result:Text "true"/"false"}` |
| REDEFINE_CLASSES | readPack cast | `AgentConfigure.java:81-121` | `class` List 기준; `{success:Boolean[, error:Text]}`; 매칭 class가 없으면 **빈 MapPack** |
| CUBRID_SET_ALERT_CONFIGURE | readPack cast | cubrid agent(이 저장소 밖) | Client는 `result` Text 기대(`client/cubrid/actions/AlertSettingDialog.java:239-251`) |
| DB_KILL_PROCESS / SET_QUERY_INTERVAL / USE_DATABASE | readMapPack | DB agent(이 저장소 밖) | 최종 MapPack |

서버는 요청 값을 해석하지 않고 그대로 relay하며, 파일 이름 값의 검증은 Agent 몫이다(§9-14). Go 서버도 같다. 삭제 실패 시 Agent 응답 `msg`는 `"file '<name>' is not exist..."`, 성공 시 `"Successfully deleted..."`다. heap dump 성공 msg는 `"Successfully request heap dump..."`, 미지원 VM은 `"dumpHeap only works on a Sun Java 1.6+ VM"`이다(`agentj/netio/request/handle/AgentHeapDump.java:79-93,148-163`).

Client 요청 키 근거: `client/heapdump/actions/{HeapDumpAction,BlockProfileAction,MutexProfileAction,CpuProfileAction}.java`(`objHash,fName,time`), `HeapDumpDeleteAction.java:64-66`, `client/model/AgentDataProxy.java:131-137`, `client/context/actions/OpenCxtmenuSystemGcAction.java:45-48`, `client/views/ObjectThreadDetailView.java:275-280`, `client/stack/actions/TurnOn/TurnOffStackAction.java`, `client/context/actions/OpenCxtmenu*Action.java`, `client/configuration/views/ConfigureView.java:533-541`(`setConfig`는 `\`를 `\\`로 escape), `client/popup/RedefineClassDialog.java:210-222`, `client/views/ObjectClassListView.java:644-647`, `client/context/actions/OpenCxtmenuDbChangeIntervalAction.java:141-155`.

## 8. SERVER_THREAD_LIST / SERVER_THREAD_DETAIL(`common/util/ThreadUtil.java`)

- `getThreadList()`(`:153-175`): `ThreadMXBean.getAllThreadIds()` 순서로 `id:List<Decimal(long)>`, `name:List<Text>`, `stat:List<Text(Thread.State.name)>`, `cpu:List<Decimal(ms = getThreadCpuTime/1_000_000)>` 네 List를 이 순서로 가진 MapPack 1개. ThreadInfo가 null인 id는 건너뛴다. 항상 non-null이므로 응답은 항상 1개.
- `getThreadDetail(id)`(`:72-104`): `id==0` 또는 ThreadInfo null이면 **빈 MapPack**(item은 전송됨). key 순서/타입: `Thread Id`(Decimal), `Thread Cpu Time`(Decimal ms), `Thread User Time`(Decimal ms), `Blocked Count`, `Blocked Time`, `Waited Count`, `Waited Time`, `Lock Owner Id`(Decimal, 없으면 -1), `Lock Name`(Text, null 가능→빈 Text wire), `Lock Owner Name`(Text), `Thread Name`(Text), `Stack Trace`(Text, frame을 `line.separator`로 연결, 최대 500 frame), `State`(Text). 요청 `id`는 `getLong`(Number 아니면 0).

## 9. 의도된 차이 후보(원본 결함·취약 동작, 정상 계약과 분리)

정상 계약은 §0~§8이다. 아래는 원본이 실제로 그렇게 동작하지만 Go가 **재현하지 않을 후보**다. 각 항목은 재현 방법과 권장안을 함께 적는다. 채택 결과는 §10 끝의 "결정 결과"에 있다.

| # | 원본 동작 | 근거 | 재현 방법 | 권장 Go 동작 |
|---|---|---|---|---|
| 1 | 계정 변경 4종이 디스크에 기록되지 않는다. `writeXmlFileWithIndent`는 문자열만 반환한다. ADD/EDIT_ACCOUNT·ADD_ACCOUNT_GROUP은 메모리만 바뀌고, 재시작이나 파일 mtime 변경 시 사라진다. EDIT_GROUP_POLICY는 곧바로 `loadGroupFile()`에 덮여 **true를 반환하지만 효과가 없다** | §2.4, `XmlUtil.scala:32-43`, `AccountManager.scala:262-270` | ADD_ACCOUNT 후 `conf/account.xml` 내용·mtime 불변, 재시작 후 LOGIN 실패. EDIT_GROUP_POLICY 후 GET_GROUP_POLICY_ALL 불변 | 실제 파일 기록(원자적 교체). 응답 형식은 동일 |
| 2 | EDIT_OBJECT_TYPE과 HTTP `addFamilyAndObjectType`이 파일·engine을 바꾸지 않고 true를 반환한다 | §4.2, §4.5, `CounterManager.java:239-262,337-346` | EDIT 후 GET_XML_COUNTER `custom` 불변 | DEFINE과 같은 저장 경로로 실제 반영 |
| 3 | normalize가 ObjectType 하위 `<Counter>`·Family unknown attribute를 삭제하고 순서를 HashMap 순서로 바꾼다. 입력에 `<Types/>`만 있으면 NPE → 응답 없음이다. Telegraf `safely*`는 기동 시 캐시한 `unsafeDoc`으로 덮어써 SET_CONFIGURE_COUNTERS_SITE 변경을 되돌릴 수 있다 | §4.3, §4.5 | `contents="<Counters><Types/></Counters>"` → 응답 없음(JDK 8/18 실행 확인) | 하위 Counter 보존, 입력 순서 유지, 빈 `Types` 허용, 단일 소유자의 최신 문서 기준 갱신 |
| 4 | SERVER_DB_DELETE가 `file` 값을 검증하지 않고 symlink를 구분하지 않는다 | §6.1 | 원본 소스 판독(실행 재현 없음). Go 거부 행렬은 `TestDeleteDatabasePathsRefusalMatrix`([storage-contract](storage-contract.md) "DB 경로 삭제") | `os.Root` 고정, LIST가 줄 수 있는 이름(`/yyyymmdd[/sub]`)만 허용, symlink 미추적, 오늘 날짜는 거절 또는 writer 정지 후 삭제 |
| 5 | REMOTE_CONTROL: 무효 session이면 로그 문자열 NPE로 **enqueue 후 응답 없음**. REMOTE_CONTROL_ALL은 응답이 없다. `Queue` 포화 시 가장 오래된 항목을 버리고 false | §6.2 | 존재하지 않는 `toSession`으로 REMOTE_CONTROL → item 0 | 무효 session은 enqueue 없이 `success=false`. REMOTE_CONTROL_ALL은 원본대로 응답 없음(Client가 null 기대) |
| 6 | **OBJECT_REMOVE의 디스크 삭제가 일어나지 않는다**(`Order.add=true` 기본값으로 writer NPE). 응답 목록에 삭제 대상이 `dead`로 남는다. **원본 클래스 실행으로 확인**(§6.3, §11) | §6.3, `ObjectWR.scala:66-95,104-109` | 격리 서버에서 죽은 objHash로 OBJECT_REMOVE → 서버 stderr NPE(`ObjectWR` thread) + 같은 응답과 다음 OBJECT_TODAY_FULL_LIST에 해당 객체가 `status=dead`로 남음. 단위 재현: §11 ObjectWR 단위 oracle | 원본의 의도(오늘 object index delete marker)를 구현하고, 응답 전에 삭제 완료를 반영. **원본과 관측 결과가 달라지므로** 결정 필요(§10-9) |
| 7 | 로그인·계정: `__local` loopback 로그인은 비밀번호를 검사하지 않음, LDAP debug가 받은 pass를 로그에 기록, LDAP filter에 id를 escape 없이 결합, `account_ldap_auth=` 오타 키, LDAP null 설정 NPE가 try 밖에서 발생, `Account.toBytes` 255 wrap | §2.2, §2.3 | `account_ldap_provider_url` 미설정 + `account_use_ldap=true` → LOGIN item 0. Go LDAP 동작은 `internal/account/ldap_test.go`(`TestLDAPBindSearchAndAccountRules` 등) | `__local`은 loopback 한정 유지 여부 결정, 비밀번호 로그 금지, RFC 4515 escape, 오타 키는 읽기 호환만, 설정 오류는 login fail 응답 |
| 8 | 설정: `${X}`가 의도한 변수 값이 아닌 문자열로 치환(overload 선택 결함, §3.1 정정), 닫는 괄호 없으면 잘림, env/sysprop 전체 혼입, `saveText` 비원자 쓰기, 설정이 아닌 public field 노출, 미반영 키, Telegraf 저장 charset 비대칭·XML 미검증 | §3.1, §3.2 | §3.1의 `replaceSysProp` 직접 실행(정정 항목) | 미정의 변수는 원문 유지, 원자적 교체, 저장 전 파싱 검증 |
| 9 | AlertRuleLoader: `counterName`을 검증하지 않고 파일 이름으로 쓴다. compile error 파일은 mtime이 바뀔 때까지 재시도하지 않는다. 수정 후 compile이 실패하면 `alertRuleTable`에 null이 들어가 다음 주기에 `Alert rule cleared` 메시지와 함께 규칙이 제거된다(이전 정상 규칙도 사라짐) | §5, `AlertRuleLoader.java:117-130,157-181,217-273` | 원본 소스 판독(실행 재현 없음). Go는 separator·`.`·`..`가 든 이름을 거절한다(`validAlertCounterName`, `internal/service/metadata_backend.go`) | 이름 검증(separator·`..` 거절). 실행은 미지원 결정이므로 load message에는 "실행 미지원" 사실만 기록 |
| 10 | handler 예외를 삼켜 cast/NPE가 모두 "응답 없음"이 되고 Client는 null(대개 "Network Error")을 받는다 | §0 | 잘못된 body 타입 | 명시적 오류 로그 + 원본과 같은 item 0(wire 호환). 단 #5·#7은 권장 응답 사용 |
| 11 | **Java Client가 body 없는 명령에 빈 MapPack을 보낸다.** GET_ALERT_REAL_COUNTER_DESC/GET_PLUGIN_HELPER_DESC는 `tcp.process(cmd, new MapPack(), …)`(`AlertScriptingView.java:597`)로 `0x0A 0x00` 2 byte를 더 보내지만 서버는 읽지 않는다. 같은 pooled 연결의 다음 명령은 이 2 byte부터 `readText`한다(길이 10 → `0x00` + 다음 명령 앞 9 byte). 그 결과 handler 없는 명령이 되고 `ServiceHandlingProxy.process`가 `RuntimeException("no handler")`을 던져(`ServiceHandlingProxy.java:97-104`) **서버가 연결을 닫는다**. `loadApiDesc`는 두 명령을 연속 실행하므로, pool이 같은 연결을 돌려주면 두 번째 GET_PLUGIN_HELPER_DESC가 EOF로 실패하고 **PluginHelper 목록이 비는 것**이 예상 증상이다. **dispatch 단계 실행 확인**(§11 dispatch desync oracle, 원본 `ServiceHandlingProxy.load/process`에 `GET_ALERT_REAL_COUNTER_DESC + session + 빈 MapPack(0x0A 0x00) + SERVER_TIME + session` byte stream을 worker 루프와 같은 순서로 넣음): 1회차는 정상 처리, 2회차 `readText`가 `"\x00\x0bSERVER_T"`(길이 10)를 명령으로 읽고 `readLong`이 `IME`+session 일부를 섞어 `5281953804277776384`를 만들며 `process`가 `RuntimeException: no handler  cmd=\x00\x0bSERVER_T`를 던진다. 실제 socket close와 GUI 증상은 미검증이다 | `TcpProxy.java:46-66,142-175`, `TcpServiceWorker.scala:111-155` | 격리 서버에 Java Client로 접속해 Alert Scripting 뷰를 연다 → API 목록에 `$$.` 항목 누락 여부와 서버 로그 `no handler` 확인. 또는 raw socket으로 `GET_ALERT_REAL_COUNTER_DESC + 빈 MapPack` 뒤 정상 명령 전송 | **결정 필요**: (a) 원본처럼 body 미독(위 증상 재현) 또는 (b) 두 명령에 한해 뒤따르는 빈 MapPack(`0x0A 0x00`)을 짧은 deadline 안에 peek해서 있으면 소비. (b)는 길이 10인 명령 이름(예: `OBJECT_ENV`)이 바로 이어지면 첫 byte `0x0A`가 겹쳐 오판할 수 있다. 다만 두 번째 byte까지 보면 정상 명령 이름은 `0x00`으로 시작하지 않는다. 권장: (b)를 2 byte(`0x0A 0x00`) 완전 일치 + 이미 수신 버퍼에 있을 때만 소비로 제한하고, 상호운용 테스트로 확인 |
| 12 | ADD/EDIT_ACCOUNT에 `email`(또는 다른 field) 키가 없으면 Account field가 null이 된다. 이후 LIST_ACCOUNT의 `toBytes`가 NPE를 내 **그 계정 이전 item까지만** 전송된다. 파일의 `<Email>` 누락도 같다 | §2.1, `Account.java:33-48` | `ADD_ACCOUNT{id,pass,group}` → LIST_ACCOUNT 부분 응답 | null field는 `""`로 정규화 |
| 13 | CHECK_JOB이 요청 `session`이 요청 연결의 session인지 확인하지 않는다 | §6.2 | 원본 소스 판독. Go 동작은 `TestCheckJobRequiresOwnSession` | 요청 연결의 인증 session과 같을 때만 dequeue. 다르면 원본처럼 item 0 |
| 14 | relay 명령의 파일 이름 값을 서버가 검증하지 않는다(Agent에 위임) | §7 | 원본 소스 판독(실행 재현 없음) | 서버에서 파일 이름 값을 제한할지 결정(Agent 호환에는 영향 없음) |
| 15 | GET_ALERT_SCRIPT_LOAD_MESSAGE 음수 index → AIOOBE → 응답 없음. cursor 읽기 비원자 | §5 | `{loop=<현재>, index=-1}` | 범위 밖 cursor는 전체 ring 반환, 단일 lock 안에서 목록·cursor 생성 |

## 10. Go 구현 완료 조건·fixture 후보·결정 필요 사항

완료 조건(명령별 테스트로 옮길 항목):
- wire: §1 표의 body 종류(MapPack/ListValue/없음)와 response 종류(MapPack/BooleanValue/TextPack+MapPack/ObjectPack 반복/없음)를 지킨다. key 순서와, `result`가 Text `"true"/"false"`인 명령(SET_CONFIGURE_SERVER/TELEGRAF/COUNTERS_SITE)과 Boolean인 명령을 구분한다. body 없는 명령이 다음 명령을 소비하지 않아야 한다(§9-11 결정 반영).
- 계정: salted SHA256 hex 문자열 비교, `__local` loopback 규칙(접속 IP 기준), session TTL 3시간/internal 7일과 keepalive 연장, 5초 mtime reload, `admin/guest` 기본 파일 생성, XML 읽기 호환(`<xs:attribute>` 무시, tab/빈 줄 허용, Email 누락 허용). 쓰기 후 Java `DocumentBuilder` + `AccountFileHandler/GroupFileHandler.parse` 동등 oracle로 확인한다. 255 byte wrap 거절은 기존 결정을 유지한다. id null ADD는 거절한다(§9-12와 함께).
- 설정: `SET_CONFIGURE_SERVER` 저장 후 즉시 reload. §3.3 표의 핫 리로드 가능 키만 런타임에 반영하고, 기동 전용 키는 "재시작 필요"로 로그를 남긴다. `${ENV}` 치환, LDAP 키는 매 로그인 시 property에서 읽는다.
- counters-site: 정규화 규칙(dedupe/zws·zps 제거) + Java `CounterEngine.parse` oracle. 성공 시 모든 로그인 session queue에 `REFETCH_COUNTER_XML`(빈 param, commander 0)을 enqueue하고 CHECK_JOB으로 수신되는지 확인한다. 자동 등록(heartbeat `detected` 태그)과 SET 명령의 직렬화(단일 writer)도 검증한다.
- alert: `.alert/.conf` 저장·조회, load message ring(50, loop/index cursor 의미, §5 경계 케이스), descriptor 25/22개의 원소 집합 일치와 고정 순서. 실행(javassist compile)은 명시적 미지원이되 저장은 성공으로 응답한다(결정: 실행 미지원).
- 관리: DB delete는 `db_dir` 하위 정규화 경로만 허용하고 `size`는 요청 수로 응답한다. REMOTE_CONTROL은 session당 queue 5 + 오래된 항목 drop + 5초 만료 정리, CHECK_JOB은 2-item 응답이다. OBJECT_REMOVE는 ListValue body + full list 응답이고, 디스크 delete marker는 §9-6 결정에 따른다.
- relay: 21개 모두 기존 `AgentBackend.AgentRequest` 경로를 쓴다. `OBJECT_RESET_CACHE`/`OBJECT_SYSTEM_GC`는 응답을 폐기하고, 그 외는 최종 MapPack만 전달한다. 요청 MapPack은 key 순서 그대로 전달한다.
- JVM 5개: descriptor 2개와 load message는 정적 데이터/ring으로 구현한다. `SERVER_THREAD_*`는 처음에는 미지원을 유지하기로 했으나 이후 goroutine 정보를 같은 스키마로 돌려주도록 구현했다(`internal/service/server_threads.go`, [server-metadata-query](server-metadata-query.md)).

fixture 후보:
- XML(계정/그룹): JDK 18·JDK 8 oracle 출력(§11, 산출물은 저장소에 없다). byte 비교용이 아니라 **Go 파서의 읽기 호환 입력**(tab/빈 줄/알파벳 attribute)으로 쓴다.
- counters 정규화: 같은 JAXB·Xalan oracle의 정규화 출력과 `<Types>` 없는 입력의 출력(JDK 8 + 원본 배포 jaxb-impl 2.3.0.1과 JDK 18 + jaxb-runtime 2.3.2가 **byte 동일**), 빈 `<Types/>` 입력의 오류(두 JDK 모두 NPE).
- descriptor: §5 표 2개(4 field), JDK별 순서는 §5.1. reflection 결과 원본은 저장소에 넣지 않았다.
- 위 oracle 산출물은 저장소에 없으므로 `tools/java-management` 또는 `testdata`로 재생성하는 것을 권장한다. 비밀번호 hash는 원본 기본 파일 값을 쓰지 말고 `000…`/`111…` 같은 더미 hex를 쓴다.

결정 필요:
1. 계정·EDIT_OBJECT_TYPE을 "실제 파일 기록"으로 구현할지(원본 결함 수정, §9-1·2), 원본과 같이 메모리/무효과로 둘지. 권장: 기록.
2. XML 쓰기 형식: 원본 출력은 JDK 8/18에서 서로 다르다(§2.4). 권장은 Go `encoding/xml` 정규 출력 + Java 파서 oracle이다. attribute 순서(원본 출력은 알파벳순)도 함께 정한다. 권장: 알파벳순(두 JDK 공통).
3. counters.site.xml 정규화 출력의 ObjectType/Family 순서(입력 순서 유지 권장)와 ObjectType 하위 Counter 보존 여부(보존 권장, 원본은 삭제).
4. `SERVER_DB_DELETE` 경로 검증 방식(§9-4)과 오늘 날짜·열린 writer와의 충돌 처리.
5. `${X}` 치환: 원본의 overload 선택 결함(§3.1)은 재현하지 않는다. **결정: 정의된 변수는 환경변수 값, 미정의는 원문 유지**(원본 실행 근거, 2026-10-15).
6. descriptor 응답 순서: 원본은 JVM 의존이고 Java Client는 TreeMap으로 정렬한다. 권장: §5 표 순서로 고정.
7. REMOTE_CONTROL 무효 session(§9-5): 권장은 enqueue 없이 `success=false`.
8. LDAP: JNDI simple bind + `(cn=id)` subtree + attribute-value==id 규칙을 Go로 재현할 때 필요한 LDAP client 의존성(표준 라이브러리에 없음 → 외부 의존성 승인 필요), TLS/StartTLS, escape, timeout 정책.
9. **OBJECT_REMOVE 디스크 삭제(§9-6)**: 원본 관측 동작(삭제 안 됨)을 재현할지, 의도대로 오늘 index에 delete marker를 남길지. 후자면 Java `ObjectRD`가 marker를 존중하는지 oracle로 확인한다.
10. **descriptor 명령의 Client 빈 MapPack(§9-11)**: 원본 desync 재현 또는 제한적 소비.
11. CHECK_JOB session 소유 검사(§9-13) 도입 여부.

결정 결과(구현 반영, 근거는 [검증 기록](../validation/2026-10-16-management-gaps.md) §9 allowlist와 패키지 계약): 1 계정 4종·EDIT_OBJECT_TYPE 모두 실제 파일 기록, 2 계정 XML은 JDK 8 Xalan 출력과 byte 일치(`internal/account`), 3 counters.site.xml은 입력 순서·속성 보존(의미 비교, `internal/counters`), 4 경로 사전 검증 후 거절·열린 handle 정리([storage-contract](storage-contract.md) "DB 경로 삭제"), 5 위 기록대로, 6 고정 선언 순서, 7 enqueue 없이 `success=false`, 8 표준 라이브러리 LDAP v3 client(`internal/account`), 9 오늘 index에 delete flag 기록([storage-contract](storage-contract.md) "Object 영구 삭제"), 10 Client가 덧붙인 빈 MapPack을 이미 버퍼에 있을 때만 제한적으로 소비((b)안, `transport.Stream.ConsumeBuffered`), 11 요청 연결의 session 것만 dequeue.

## 11. 조사하지 못한 항목·실행 근거

- 실행한 것(저장소 밖 일회성 oracle, 산출물은 저장소에 없다):
  - null id append oracle: 원본 기본 `account.xml`에 null id Account append → Xalan 변환. JDK 8/18 모두 `setAttribute(null)` 허용, `id=""`·`<Email/>` 출력. JDK별 공백 차이 확인.
  - descriptor reflection oracle: `scouter-server-2.21.3.jar`(+ 같은 배포 lib)에서 `Class.forName(..., false, …)`로 static 초기화 없이 `RealCounter`/`PluginHelper`를 reflection 했다. 원본 `getRealCounterDescription`과 같은 필터를 JDK 8/18에서 실행했다. 결과는 25/22개로 §5 표와 내용이 일치했고 순서만 달랐다.
  - JAXB·Xalan oracle(JDK 8): 아래 JDK 18 oracle을 JDK 8 + 원본 배포 jar(jaxb-impl 2.3.0.1 등)와 원본 `ParamText` 소스로 재컴파일·실행했다. counters/paramtext 결과는 JDK 18 결과와 동일하고, 계정 XML은 공백이 다르다.
- 실행한 것(앞선 조사): JAXB·Xalan oracle을 JDK 18(Zulu 18.0.2)로 실행했다. DOM/Xalan은 원본 `XmlUtil`·`AccountFileHandler`·`GroupFileHandler` 로직을 복사했고, JAXB는 원본 `CounterManager` 내부 클래스와 `jaxb-api 2.3.1 + jaxb-runtime 2.3.2`를 썼다.
- 실행한 것(2026-10-15 추가, 저장소 밖 일회성 oracle):
  - `python3 scripts/java-management.py --source <원본> --classes-dir <출력 디렉터리>`로 관리 oracle classpath를 만든 뒤, ObjectWR 단위 oracle을 원본 `server/db/{ObjectWR,DBCtr}.scala`, `db/obj/{ObjectIndex,ObjectData}.scala`, `db/io/{IndexKeyFile,RealKeyFile}.scala`, `serverj/db/io/MemHashBlock.java`(모두 무수정)와 함께 `scouter.deploy/lib/scala/scala-compiler.jar` 2.10.4·JDK 8(Zulu 1.8)로 컴파일해 seam `ObjectWR/DBCtr`보다 앞선 classpath로 실행했다. 실행 결과: `location_after_add=0`, `location_after_remove=0`, `location_after_manual_delete_order=-1`, stderr 3줄(`NullPointerException … ObjectWR.scala:69`). 임시 `db_dir`은 JVM마다 새로 만든 temp 디렉터리다.
  - dispatch desync oracle을 같은 classpath·JDK 8로 실행했다. 원본 `ServiceHandlingProxy`가 handler를 dispatch하므로 `GET_ALERT_REAL_COUNTER_DESC` 응답 25 item 뒤에 남은 `0x0A 0x00`이 다음 명령 이름으로 읽히는 것을 확인했다.
- 정적 분석만 한 것(미실행): Scala handler의 실제 TCP 실행(`TcpServiceWorker` socket 종료 포함), LDAP 실제 bind, Java Client GUI 상호운용, AsyncRun/RemoteControl 포화 동작.
- 미조사: cubrid/DB agent 측 handler(저장소 밖), webapp REST가 같은 명령을 어떻게 감싸는지([webapp-api](webapp-api.md) 범위), `ObjectMap.remove`의 type 집합 세부, Telegraf `TgConfig` XML schema 전체(별도 Telegraf 범위).

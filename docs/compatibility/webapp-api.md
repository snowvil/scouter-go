# scouter.webapp REST API 인벤토리

정본은 `baseline.json`의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. 원본 경로는 `scouter.webapp/src/main/java/scouterx/webapp/` 기준이며 `:N`은 줄 번호다. 목적은 Go 서버가 같은 REST API를 **직접** 제공할 때 필요한 계약(경로·파라미터·호출 TCP 명령·응답 JSON)을 고정하는 것이다. Java webapp는 Scouter Server와 **TCP Client 프로토콜**(`NetCafe.TCP_CLIENT`, `RequestCmd`)로 통신하는 별도 프로세스(standalone, 포트 6188) 또는 Server JVM 안의 embedded 모드(6180, [http-contract](http-contract.md) §1)로 동작한다. Go가 내장할 때는 TCP 왕복 없이 `internal/service` handler를 직접 호출하면 되지만, 아래의 request MapPack 구성·응답 해석 규칙이 곧 계약이다.

Go 구현 상태는 §4.1이 정본이다. `internal/webapp`이 `--webapp`(또는 `net_http_api_enabled=true`)으로 servlet listener의 `/scouter/*`에 93개 endpoint(controller A 38 + B 55)를 제공한다. `--http`는 이와 별개인 `/health`·pprof 운영 listener다(`internal/server/http.go`). `/v0/temp/test/*`, WebSocket `/basic`, swagger, webroot, extweb은 제외 확정이다.

## 1. 부트스트랩·공통 규칙

| 항목 | 원본 | 계약 |
|---|---|---|
| 실행 모드 | `main/WebAppMain.java:79-116`, `:278-287` | standalone `main()`은 `StandAloneConfigure`(`./conf/scouter.conf`, `net_collector_ip_port_id_pws` 기본 `127.0.0.1:6100:admin:admin`, 포트 `net_http_port` 기본 6188)로 collector에 TCP 로그인 후 Jetty 기동. embedded는 Server가 `setWebAppContext()`를 호출하며 `EmbeddedConfigureAdaptor`가 `127.0.0.1:{net_tcp_listen_port}`에 `__local`/`__local` 계정(`NetConstants.LOCAL_ID`)으로 로그인한다(`framework/configure/EmbeddedConfigureAdaptor.java:110-113`). |
| context | `:126-152` | context path `/`, session handler(`maxInactiveInterval = net_http_api_session_timeout`), Jersey `ServletContainer`를 **`/scouter/*`**에 등록(`jersey.config.server.provider.packages=scouterx.webapp`, swagger 켜면 `,io.swagger.jaxrs.listing` 추가 → `/scouter/swagger.json`). 정적: `/*` → classpath `webroot/`(dir listing 금지), `/extweb/*` → `net_http_extweb_dir`(dir listing 허용), `/swagger` → swagger `Bootstrap` servlet(`swagger/Bootstrap.java`, basePath `/scouter`, host = `net_http_api_swagger_host_ip:port`). |
| filter(`/scouter/*`만) | `:154-159` | 순서: `LoggingInitServletFilter`(MDC `requestId` = `#`+random32+counter), `CorsFilter`, `NoCacheFilter`(`Cache-Control: no-cache`, `Expires: 0`, `Pragma: No-cache`), `ReleaseResourceFilter`(`TextModel.endScope()`). |
| CORS | `framework/filter/CorsFilter.java:39-66` | `net_http_api_cors_allow_origin`이 공백이 아니면 `Access-Control-Allow-Origin`(origin=`*`이고 credentials=`true`면 요청 `Origin` 헤더 값으로 치환), `Access-Control-Allow-Credentials`, `Access-Control-Max-Age: 600`, `Access-Control-Allow-Headers: Content-Type, Authorization`, `Access-Control-Allow-Methods: POST, GET, OPTIONS, PUT, DELETE, PATCH`. `OPTIONS`는 헤더만 쓰고 chain을 타지 않는다(200 빈 본문). origin 설정이 공백이면 CORS 헤더를 쓰지 않고 `OPTIONS`도 Jersey로 넘어가 Jersey 기본 OPTIONS 처리(Allow 헤더/WADL)를 받는다. 헤더는 `addHeader`라 중복 설정 시 누적된다. |
| 인증 | `framework/filter/AddAuthFilterFeature.java:33-40`, `AuthFilter.java:50-95` | `scouterx.` 패키지 resource의 method 중 `@NoAuth`가 없는 모든 method에 `AuthFilter` 등록. 순서: (1) `net_http_api_auth_ip_enabled`면 `ZZ.getRequestIp`(`net_http_api_auth_ip_header_key` 헤더의 첫 `,` 토큰 trim, 없으면 remoteAddr)가 `allowIpExact`에 있거나 `allowIpMatch`(StrMatch)에 맞으면 **즉시 통과**, 아니면 `violation=true`. (2) `net_http_api_auth_bearer_token_enabled`면 `Authorization` 헤더가 있을 때 `bearer ` 접두(대소문자 무시) 제거 후 `UserToken.fromBearerToken` → `validateToken` → 통과; 헤더가 없고 session 인증도 꺼져 있으면 `SESSION_EXPIRED`(401). (3) `net_http_api_auth_session_enabled`면 `HttpSession.userId` 없으면 `LOGIN_REQUIRED`(401). (4) `violation`이면 `SESSION_EXPIRED`(401). 세 옵션이 모두 false(기본)면 **인증 없음**. 세부: bearer 헤더가 있으면 session 검사 없이 통과/실패가 결정된다. session 검사는 `servletRequest.getSession()`(create=true)이라 미인증 요청마다 **새 session과 `Set-Cookie: JSESSIONID`가 생긴 뒤** 401이 난다. IP 검사(1)가 실패한 경우 유효 bearer는 (2)에서 바로 return하므로 **IP 위반이어도 통과**하지만, 유효 session은 (3)을 지나 (4)에서 401이 된다(비대칭). CORS preflight(`OPTIONS`)는 servlet filter 단계에서 끝나므로 AuthFilter를 거치지 않는다. |
| session 로그인 | `layer/controller/UserController.java:64-72` | `POST /scouter/v1/user/login` body `{"serverId":int?,"user":{"id","password"}}`. `AccountConsumer.login`(`layer/consumer/AccountConsumer.java:39-50`)이 `CHECK_LOGIN` MapPack `{id, pass=sha256(password)}` → `BooleanValue`; false면 `LOGIN_FAIL`(404). 성공 시 servlet session 생성·`userId` attribute. 쿠키 이름은 Jetty 기본 `JSESSIONID`. |
| bearer 토큰 | `UserController.java:80-88`, `layer/service/UserTokenService.java`, `framework/session/UserToken.java` | `POST /scouter/v1/user/loginGetToken` → `{"success":true,"bearerToken":"V1.<token>.<hexa32(serverId)>.<userId>"}`. token은 Tomcat `SessionIdGenerator`(`lib3/tomcat/SessionIdGenerator.java`). 발급 시 사용자별 token 목록을 collector의 세션 전용 custom KV에 `ttl=sessionExpireSec`(= timeout×1.1)로 저장한다(저장 형식의 정본은 원본 `UserToken`·`UserTokenService`이며 Go는 같은 형식을 읽고 쓴다). 검증은 메모리 `UserTokenCache`(최대 10,000) → 없으면 KV 조회; 만료면 `SESSION_EXPIRED`; 마지막 사용 뒤 `timeout×0.1`초가 지났으면 사용 시각을 갱신해 cache와 KV에 다시 쓴다(touch). `UserToken.fromBearerToken`은 `StringUtils.split(token, ".", 4)` 결과 길이를 검증하지 않아 형식 오류 시 `ArrayIndexOutOfBounds` → 500(결함 후보 W1). `V1` 접두도 검사하지 않으며 4번째 조각(userId)은 `.`를 포함할 수 있다. KV에 해당 userId 항목이 아예 없으면 `tokens.split` NPE → 500(W11). |
| 응답 wrapper | `view/CommonResultView.java:40-81` | `{"status":<HTTP status>,"requestId":<MDC>,"resultCode":0,"message":"success","result":<T>}`. `success()`는 result `true`. 실패는 `fail(status, resultCode, message, null)`. |
| **숫자 직렬화** | `framework/mapper/JerseyMapperProvider.java:31-33`, `CommonResultView.java:42` | Jackson `WRITE_NUMBERS_AS_STRINGS=true`. 모든 숫자 필드가 **JSON 문자열**(`"status":"200"`, `"objHash":"-12345"`)로 나간다. streaming 응답(`jsonStream/jsonArrayStream`)도 같은 mapper. Boolean은 boolean 그대로. |
| streaming 응답 | `CommonResultView.java:83-129` | `jsonStream`: `{"status":"200","resultCode":"0","message":"","result":{...}}`, `jsonArrayStream`: `result`가 배열. 같은 `WRITE_NUMBERS_AS_STRINGS` mapper(`:42`)의 generator라 `writeNumberField`도 **문자열**로 나간다. `requestId` 없음. 본문은 chunked로 흘리며 중간 예외는 HTTP 200이 이미 나간 뒤 끊긴다. |
| 필드 순서·null | `CommonResultView.java:40-48` | Lombok `@Getter` + Jackson 기본 설정이므로 필드 선언 순서 `status, requestId, resultCode, message, result`로 직렬화되고 null도 생략하지 않는다(`"result":null`, `requestId`는 `/scouter/*` 요청이면 항상 MDC 값). Jackson 기본 순서 규칙에 의존하므로 golden으로 확인 필요(미검증). |
| 오류 매핑 | `framework/exception/mapper/GenericExceptionMapper.java:47-120`, `ErrorState.java:30-46` | `ErrorStateBizException`/`ErrorStateException` → `status=errorState.status`, `resultCode=같은 숫자`, `message=errorMessage`(VALIDATE_ERROR는 `"fail to validate input parameters. : " + ex.message`). `WebApplicationException`(Jersey 404/405/415 등) → 그 status. 그 외 Throwable → 500/500, message=`ex.message [caused by] cause.message`. `javax.validation.ValidationException`은 `ValidationExceptionMapper`(400, `[ValidationException] path message...`)가 받지만 `ConstraintViolationException`으로 캐스팅하므로 `ServerManager.getServerIfNullDefault`가 던지는 일반 `ValidationException`은 ClassCastException → Jersey 500(결함 후보 W2). ErrorState: SESSION_CLOSED/CLIENT_SOCKET_CLOSED/INTERNAL_SERVER_ERROR/COLLECTOR_NOT_CONNECTED/COLLECTOR_INVALID_SESSION=500, ILLEGAL_KEY_ACCESS=400, LOGIN_REQUIRED=401, LOGIN_FAIL=404, SESSION_EXPIRED=401, NOT_IMPLEMENTED=501, VALIDATE_ERROR=400. |
| `serverId` | `framework/client/server/ServerManager.java:208-225`, `Server.java:69` | `serverId = HashUtil.hash(ip + port)`(문자열 결합, 예 `"127.0.0.16100"`). query `serverId` 누락 → 0 → `getServer(0)==null` → 연결 서버가 정확히 1개면 default, 아니면 `ValidationException`. multi-server는 `net_collector_ip_port_id_pws`에 `,`로 나열. Go 단일 서버 내장 시 `serverId`는 자기 자신 1개로 고정하되 값은 같은 hash 규칙으로 노출해야 `/v1/info/server`·bearer token의 `hexa32(serverId)`가 호환된다. |
| 시간 파라미터 | `request/CounterRequest.java:69-98` 등 | `yyyymmdd`(path), `startYmdHms/endYmdHms`=`yyyyMMddHHmmss`(`ZoneId.systemDefault()`), `startHms/endHms`=`HHmmss`(yyyymmdd와 결합, `SimpleDateFormat` 기본 TZ), `startYmdHm/endYmdHm`=`yyyyMMddHHmm`, `startYmd/endYmd`=`yyyyMMdd`(counter avg) 또는 **`yyyyMMddHH`**(visitor group, 이름과 달리 시 단위), `*TimeMillis`=epoch ms. 문자열형과 millis가 공존하면 VALIDATE_ERROR. `DateTimeFormatter`(`CounterRequest`, `SummaryRequest`, `VisitorGroupRequest`) 파싱 실패는 잡지 않아 `DateTimeParseException` → 500(W12), `SimpleDateFormat`(`PageableXLogRequest`, `SearchXLogRequest`)은 lenient라 `startHms=250000`처럼 범위 밖 값도 다음 날로 넘겨 받아들이고 형식 오류만 400 `date is invalid!`. range 상한: counter 과거 조회 6시간(`CounterRequest.java:42,85`), latest 3600초(`LatestCounterRequest.java:38,56`), stat·summary·xlog는 상한 없음. |
| 목록 파라미터 | `framework/util/ZZ.java:75-117` | `objHashes`, `counters`, `keys`, `txidList`, `dictKeys`: `,` 구분, 선두 `[`가 있으면 양끝 1글자 제거(`[a,b]`). 공백 trim 없음, 정수 파싱 실패는 `NumberFormatException` → 500. |
| 페이징 | `request/PageableXLogRequest.java:56-131` | `pageCount` 기본 10,000(setter가 0 이하를 기본값으로 바꿈), `@Max` 30,000(초과 400). `lastTxid`/`lastXLogTime`은 둘 다 0이거나 둘 다 비0. 다음 페이지 요청은 직전 응답의 `lastTxid`·`lastXLogTime`을 그대로 넘기며 `hasMore=false`면 종료한다. query 미지정 시 setter가 기본값(0)으로 호출된다는 Jersey `@BeanParam` 주입 규칙에 의존한다(미검증). |
| Content-Type | 모든 GET에 `@Consumes(APPLICATION_JSON)` | 본문 없는 GET은 영향 없음. GET에 다른 `Content-Type`을 붙이면 Jersey 415. |
| 날짜 TZ | `Server.java:111-117`, `LoginMgr.java:67-77` | webapp는 LOGIN 응답의 `time`으로 collector와의 clock delta를 저장하고 `server.getCurrentTime()`을 latest counter·XLog loop cache에 사용한다. `timezone`은 저장만 하고 API에 쓰지 않는다. |

### 1.1 webapp 내부 캐시(Go 내장 시 대체 필요)

| 구성 | 원본 | 역할 |
|---|---|---|
| `AgentModelThread` | `framework/client/model/AgentModelThread.java:63-127` | 10초마다 서버별 `GET_XML_COUNTER`(counterEngine 갱신)와 `OBJECT_LIST_REAL_TIME`을 호출해 objHash→`AgentObject{objType,objName,serverId,objPack}` 맵을 유지. `SCounter`, `SActiveService`, `SAlert`, `CounterView`의 objName/displayName/unit, interaction counter의 parent host 조회가 여기에 의존한다. 캐시에 없는 objHash는 `SCounter` 생성자에서 NPE → 500(W3), `SActiveService`는 skip, `SAlert`/`SActiveServiceStepCount`는 `"UNKNOWN"`. |
| `TextProxy`/`TextModel` | `framework/client/model/TextModel.java`, `TextProxy.java` | type별 LRU(service 20k, method 40k, …)와 `GET_TEXT_100`(MapPack `{date, type, hash[]}` → MapPack `{hexa32(hash): text}`), `TextLoader`는 `GET_TEXT_ANY_TYPE`(`{type[], hash[]}` → TextPack 스트림). 미해결은 `**unlabeled**:<type>:<hash>`, hash 0은 `""`. 요청 scope 안의 실패 hash는 재조회하지 않는다. |
| `XLogLoopCache` + `XLogRetrieveThread` | `framework/cache/XLogLoopCache.java`, `framework/client/thread/XLogRetrieveThread.java` | 서버별 1초 주기로 `TRANX_REAL_TIME_GROUP(_LATEST)`(objHash 없음=전체, count 10,000)를 당겨 40,000칸 ring에 (pack, loop, index)로 적재. `/v1/xlog-data/realTime`은 이 ring에서 읽는다(최대 20,000건, `endTime > now-2000ms`인 항목부터 중단). 즉 **webapp 자체 loop/index**이며 collector의 loop/index와 다르다. |
| `ConnectionPool`/`TcpProxy` | `framework/client/net/*` | pool 크기 `net_webapp_tcp_client_pool_size`, idle timeout `_pool_timeout`, so timeout은 LOGIN 응답 `so_time_out`(없으면 15000). `INVALID_SESSION`(0x44) 수신 시 session=0으로 두고 `ServerSessionObserver`가 5초 주기로 재로그인. |

## 2. Endpoint 표 (22 controller, 101 endpoint)

모든 path는 `/scouter` 접두 뒤에 붙는다. "인증"은 `@NoAuth`가 없으면 `필요`. 응답은 특별히 적지 않으면 `CommonResultView<...>`. `serverId`는 모든 endpoint에서 query int 선택(§1 규칙)이며 표에서 생략한다. 숫자 JSON 필드는 모두 문자열로 직렬화된다(§1).

### 2.1 ActiveServiceController `/v1/activeService` (`layer/controller/ActiveServiceController.java:50-175`)

| method path | 파라미터 | TCP 명령 (request) | 응답 model | 비고 |
|---|---|---|---|---|
| GET `/stepCount/ofType/{objType}` | path objType | `ACTIVESPEED_REAL_TIME` `{objType}` → MapPack 스트림 | `List<SActiveServiceStepCount>{objHash,objName,step1Count,step2Count,step3Count}` ← `objHash, act1, act2, act3` | objName은 AgentModelThread, 없으면 `UNKNOWN` |
| GET `/ofType/{objType}` | path objType | `OBJECT_ACTIVE_SERVICE_LIST` `{objType}` → objHash별 MapPack | `List<SActiveService>` (`model/scouter/SActiveService.java:37-116`): objHash,objName,threadId(`id`),threadName(`name`),threadStatus(`stat`),threadCpuTime(`cpu`),txidName(`txid` 문자열),txid(=hexa32 decode),activeDate(오늘 yyyymmdd),elapsed,serviceName(`service`),ipaddr(`ip`),note(`sql` 또는 `subcall`),mode(`SQL`/`SUBCALL`/`NONE`) | elapsed 내림차순 정렬; cache에 없는 objHash pack은 skip |
| GET `/ofObject/{objHash}` | path objHash | `OBJECT_ACTIVE_SERVICE_LIST` `{objHash}` | 동일 | |
| GET `/control/thread/{threadId}/ofObject/{objHash}` | path objHash, threadId; query `action` 필수 | `OBJECT_THREAD_CONTROL` `{objHash, id, action}` → MapPack | `ThreadContents{serviceThread, time(Service Elapsed), keyValueDataList[{key,value}]}` key 정렬, `Stack Trace` 제외, Text/Decimal/Double/Float만 | Agent thread 제어(mutation) |
| GET `/thread/{threadId}/ofObject/{objHash}` | path; query `txidName`(hexa32) 또는 `txid` | `OBJECT_THREAD_DETAIL` `{objHash, id, txid}` → MapPack | `ActiveThread`(`model/ActiveThread.java:57-99`): txidName(`Service Txid`), elapsed(`Service Elapsed`), serviceName(`Service Name`), note/mode(`SQL`→SQL, `Subcall`→SUBCALL), sqlActiveBindVar, threadId(`Thread Id`), threadName, threadStatus(`State`), threadCpuTime, threadUserTime, blockCount(`Blocked Count`), blockTime, waitedCount, waitedTime, lockOwnerId, lockName, lockOwnerName, stackTrace(`Stack Trace`) | |

### 2.2 AlertController `/v1/alert` (`AlertController.java:39-114`)

| method path | 파라미터 | TCP 명령 | 응답 | 비고 |
|---|---|---|---|---|
| GET `/realTime/{offset1}/{offset2}` | path loop(≥0), index(≥0); query objType | `ALERT_REAL_TIME` `{loop, index, objType}` → MapPack(meta `loop,index`) + AlertPack 스트림 | `RealTimeAlertView{offset1,offset2,alerts[SAlert{time,objType,objHash,objName,level(enum 이름),title,message,tagMap}]}` | objName은 AgentModelThread/`UNKNOWN` |
| GET `/scripting/{counterName}` | path | `GET_ALERT_SCRIPTING_CONTETNS` `{counterName}`→`contents`; `GET_ALERT_SCRIPTING_CONFIG_CONTETNS`; `GET_ALERT_REAL_COUNTER_DESC`·`GET_PLUGIN_HELPER_DESC`(빈 MapPack 요청, desc 스트림) | `ScriptingLoadData{ruleText,configText,configHash,ruleTextHash,realCounterDescMap,pluginHelperDescMap}`; map key `"$counter.method(paramTypes)"`/`"$$.method(...)"`, value `ApiDesc{desc,methodName,returnTypeName,fullSignature}` | 빈 contents는 기본 템플릿(`AlertScriptingConsumer.java:47-56`) |
| POST `/set/config/scripting/{counterName}` | body `SetConfigRequest{values}` | 현재 config 로드 후 `hash(values)`가 다를 때만 `SAVE_ALERT_SCRIPTING_CONFIG_CONTETNS` `{counterName, contents}` → `success` | `ScriptingSaveStateData{status(200/404),message}`; 변경 없으면 status 0 | mutation |
| POST `/set/rule/scripting/{counterName}` | 동일 | `SAVE_ALERT_SCRIPTING_CONTETNS` | 동일 | mutation |
| GET `/read/scripting/{loop}/{index}` | path | `GET_ALERT_SCRIPT_LOAD_MESSAGE` `{loop,index}` → `{loop,index,messages[]}` | `ScriptingLogStateData{loop,index,message[]}` | |

### 2.3 ConfigureController `/v1/configure` (`ConfigureController.java:40-135`, `layer/consumer/ConfigureConsumer.java`, `ConfigureKVHandleConsumer.java`)

| method path | 파라미터 | TCP 명령 | 응답 | 비고 |
|---|---|---|---|---|
| GET `/server` | | `GET_CONFIGURE_SERVER`(`serverConfig`), `LIST_CONFIGURE_SERVER`(`key/value/default` ListValue), `CONFIGURE_DESC`, `CONFIGURE_VALUE_TYPE`, `CONFIGURE_VALUE_TYPE_DESC`(빈 MapPack) | `ConfigureData{contents, configStateList[{key,value,def}], descMap, valueTypeMap(ValueType enum 이름), valueTypeDescMap(ValueTypeDesc), status=200}` | `$…$` 포함 key는 변수 제거 key도 추가 |
| GET `/object/{objHash}` | path | `GET_CONFIGURE_WAS`(`agentConfig`), `LIST_CONFIGURE_WAS`, 나머지 3개 `{objHash}` | 동일 | Agent relay |
| POST `/set/server` | body `{values}` | `SET_CONFIGURE_SERVER` `{setConfig=values(`\`→`\\`)}` → `result`=="true"면 `LIST_CONFIGURE_SERVER` | `ConfigureData{configStateList,status 200/404}` | mutation |
| POST `/set/object/{objHash}` | path, body | `SET_CONFIGURE_WAS` `{setConfig, objHash}` → `LIST_CONFIGURE_WAS` | 동일 | mutation |
| PUT `/set/kv/server` | body `SetConfigKvRequest{key,value}` | `GET_CONFIGURE_SERVER` 텍스트에서 `^key\s*=.*\n?` 정규식 치환(없으면 `\n\n#auto-added\nkey=value\n` append) → `SET_CONFIGURE_SERVER` | `Boolean` | mutation; key는 regex escape 없음(W4) |
| PUT `/set/kv/ofType/{objType}` | path, body | `OBJECT_LIST_REAL_TIME`로 objType·alive 객체마다 `GET_CONFIGURE_WAS`→치환→`SET_CONFIGURE_WAS` | `List<ConfObjectState{objHash,success}>` (dead 객체는 false) | mutation |

### 2.4 CounterController `/v1/counter` (`CounterController.java:54-207`, `CounterConsumer.java`)

| method path | 파라미터 | TCP 명령 | 응답 | 비고 |
|---|---|---|---|---|
| GET `/realTime/{counters}/ofType/{objType}` | path counters(목록), objType | `COUNTER_REAL_TIME_ALL_MULTI` `{objType, counter[], objHash[](빈)}` → MapPack `{objHash[], counter[], value[]}` | `List<SCounter{objHash,objName,name,displayName,unit,value}>` | displayName/unit은 webapp CounterEngine, objName은 AgentModelThread(없으면 NPE→500, W3) |
| GET `/realTime/{counters}` | query objHashes 필수, path counters | 동일, `objType=null` | 동일 | |
| GET `/{counter}/ofType/{objType}` | `CounterRequestByType`: path counter, objType; query startYmdHms/endYmdHms 또는 startTimeMillis/endTimeMillis(범위 ≤ 6시간) | 날짜 단위로 분할하여 `COUNTER_PAST_TIME_ALL` `{objType, counter, stime=max(dayStart+1000,start), etime=min(dayStart+86400000-1000,end)}` → objHash별 MapPack `{objHash, time[], value[]}` | `List<CounterView{objHash,objName,startTimeMillis,endTimeMillis,name,displayName,unit,timeList[],valueList[]}>` objHash별 병합 | |
| GET `/{counter}` | `CounterRequestByObjHashes`: query objHashes | `COUNTER_PAST_TIME_ALL` `{objHash[] , counter, stime, etime}` | 동일 | |
| GET `/stat/{counter}/ofType/{objType}` | `CounterAvgRequestByType`: query startYmd/endYmd(yyyyMMdd) 필수 | `COUNTER_PAST_LONGDATE_ALL` `{objType, sDate, eDate, counter}` → objHash별 MapPack `{objHash,time[],value[]}` | `List<AvgCounterView{objHash,objName,fromYmd,toYmd,name,displayName,unit,timeList,valueList}>` | 5분 단위 일 평균 |
| GET `/stat/{counter}` | query objHashes | `COUNTER_PAST_LONGDATE_ALL` `{objHash[], …}` | 동일 | |
| GET `/{counter}/latest/{latestSec}/ofType/{objType}` | path latestSec(≤3600) | `end=server.currentTime`, `start=end-latestSec*1000`으로 위 과거 조회 | `List<CounterView>` | |
| GET `/{counter}/latest/{latestSec}` | query objHashes | 동일 | 동일 | |

Jersey 경로 매칭 주의: `/realTime/{counters}`와 `/{counter}/latest/{latestSec}`, `/stat/{counter}`는 literal 우선 규칙으로 구분된다.

### 2.5 CustomKvStoreController `/v1/kv/space` (`CustomKvStoreController.java:45-126`, `CustomKvStoreConsumer.java`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/{keySpace}/{key}` | path | `GET_CUSTOM_KV` `{keySpace,key}` → Value | `String`(value.toString(), 없으면 null) |
| PUT `/{keySpace}` | body `SetKvRequest{serverId?,key,value,ttl=-1}` | `SET_CUSTOM_KV` `{keySpace,key,value,ttl}` → BooleanValue; false면 RuntimeException(500) | `true` |
| PUT `/{keySpace}/{key}/:ttl` | body `SetKvTTLRequest{serverId?,ttl}` | `SET_CUSTOM_TTL` `{keySpace,key,ttl}` | `true` |
| GET `/{keySpace}/{keys}/:bulk` | path keys 목록 | `GET_CUSTOM_KV_BULK` `{keySpace, key=ListValue}` → MapPack key→Text | `List<KeyValueData{key,value}>` |
| PUT `/{keySpace}/:bulk` | body `SetKvBulkRequest{serverId?,kvList[{key,value}],ttl=-1}` | `SET_CUSTOM_KV_BULK` `{keySpace, kv=MapValue, ttl}` → MapPack key→Boolean | `List<KeyValueData{key,value(boolean)}>` |

`ttl`은 초이며 `-1`(`ParamConstant.TTL_PERMANENT`)이 영구다. `:ttl`·`:bulk`는 literal path segment다.

### 2.6 DictionaryController `/v1/dictionary` (`DictionaryController.java:41-78`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/{yyyymmdd}` | query `dictKeys` = `type:hash,type:hash`(예 `service:12345,sql:-678`) | type별로 `GET_TEXT_PACK` `{date, type, hash[]}` → TextPack 스트림 | `jsonArrayStream`: `{"status":"200","resultCode":"0","message":"","result":[{"textType","dictKey","text"}...]}` |

### 2.7 GlobalKvStoreController `/v1/kv` (`GlobalKvStoreController.java:45-112`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/{key}` | path | `GET_GLOBAL_KV` **TextValue(key)**(MapPack 아님) → Value | `String` |
| PUT `` (root) | body `SetKvRequest` | `SET_GLOBAL_KV` `{key,value,ttl}` | `true` |
| PUT `/{key}/:ttl` | body `SetKvTTLRequest` | `SET_GLOBAL_TTL` `{key,ttl}` | `true` |
| GET `/{keys}/:bulk` | path keys | `GET_GLOBAL_KV_BULK` `{key=ListValue}` | `List<KeyValueData>` |
| PUT `/:bulk` | body `SetKvBulkRequest` | `SET_GLOBAL_KV_BULK` `{kv=MapValue, ttl}` | `List<KeyValueData>` |

### 2.8 HostObjectRequestController `/v1/object/host` (`HostObjectRequestController.java:32-79`)

| method path | TCP 명령 | 응답 |
|---|---|---|
| GET `/realTime/top/ofObject/{objHash}` | `HOST_TOP` `{objHash}` → MapPack list `PID,USER,CPU,MEM,TIME,NAME` | `List<ProcessObject{pid,user,cpu(float),mem,time,name}>` (null이면 result null) |
| GET `/realTime/disk/ofObject/{objHash}` | `HOST_DISK_USAGE` `{objHash}` → list `Device,Total,Used,Free,Pct,Type,Mount` | `List<HostDiskData{device,total,used,free,pct(long→double),type,mount}>` |

### 2.9 InfoController `/v1/info` (`InfoController.java:42-79`)

| method path | 인증 | 소스 | 응답 |
|---|---|---|---|
| GET `/server` | **@NoAuth** | `ServerManager.getAllServerList()` | `List<ServerView{id,name(LOGIN의 server_id 또는 ip:port),connected(session!=0),serverTime(now-delta),version}>` |
| GET `/counter-model` | 필요 | webapp CounterEngine(`GET_XML_COUNTER` default+custom 파싱) | `CounterModelData{families[{name,counters[{name,displayName,unit,totalizable,icon,master}]}], objTypes[{name,familyName,displayName,icon,subObject}]}` — Lombok `isMaster/isSubObject` boolean은 Jackson 속성 `master`/`subObject` |

### 2.10 InteractionCounterController `/v1/interactionCounter` (`InteractionCounterController.java:58-178`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/realTime/ofType/{objType}` | path | `INTR_COUNTER_REAL_TIME_BY_OBJ` `{objType, objHash[]}` → InteractionPerfCounterPack 스트림 | `List<InteractionCounterData{objName,interactionType,fromObjHash,fromObjName,fromObjCounters[],toObjHash,toObjName,toObjCounters[],period,count,errorCount,totalElapsed}>`; objName은 `GET_TEXT_ANY_TYPE`(OBJECT) |
| GET `/realTime` | query objHashes 필수 | 동일 + 결과 from/to objHash의 parent host(`hash(parentName)`)에 `COUNTER_REAL_TIME_ALL_MULTI` counters `Cpu`, javaee family 객체에 `ActiveSpeed` | 위 + `fromObjCounters/toObjCounters[{name,value}]` |

### 2.11 ObjectController `/v1/object` (`ObjectController.java:47-206`, `ObjectConsumer.java`)

| method path | TCP 명령 | 응답 | 비고 |
|---|---|---|---|
| GET `` (root) | `OBJECT_LIST_REAL_TIME`(body 없음) → ObjectPack 스트림 | `List<SObject{objType,objFamily,objHash,objName,address,version,alive,lastWakeUpTime(wakeup),tags{}}>` | objFamily는 webapp CounterEngine |
| GET `/remove/inactive` | 연결된 **모든** 서버에 `OBJECT_REMOVE_INACTIVE` | `true` | mutation |
| GET `/remove/inactive/server` | 지정 서버에 `OBJECT_REMOVE_INACTIVE` | `true` | mutation |
| GET `/threadList/{objHash}` | `OBJECT_THREAD_LIST` `{objHash}` → list `id,name,stat,cpu,txid,elapsed,service` | `List<ThreadObjectData{id,name,stat,cpu,txid(String),elapsed(String),service}>` | |
| GET `/threadDump/{objHash}` | `OBJECT_THREAD_DUMP` `{objHash}` → list `threadDump` | `String`(줄마다 `\n`) | |
| GET `/heapHistogram/{objHash}` | `OBJECT_HEAPHISTO` `{objHash}` → list `heaphisto`(공백 분리 `no count size name`) | `List<HeapHistogramData{no,count,size,name(JVM descriptor→canonical)}>` | 토큰<4 줄 skip |
| GET `/env/{objHash}` | `OBJECT_ENV` `{objHash, userSession=session, userIp=로컬 IP}` → MapPack | `List<VariableData{name,value}>` | |
| GET `/socket/{objHash}` | `OBJECT_SOCKET`(literal `"SOCKET"`, `scouter.common/.../net/RequestCmd.java:82`) `{objHash}` → list `key,host(Blob ip),port,count,service(hash),txid,order,stack` | `List<SocketObjectData{key,host,port,count,service(텍스트),txid,standby(order),stack}>` | service는 `GET_TEXT_100`(service, 오늘) |

### 2.12 SimpleViewController `""` (`layer/controller/others/SimpleViewController.java:42-73`)

| method path | 인증 | 동작 |
|---|---|---|
| GET `/s/{key}` | **@NoAuth**, `@Produces(TEXT_HTML)` | `GET_CUSTOM_KV` `{keySpace="shortner"(ScouterConstants.SHORTENER_KEY_SPACE), key}` → URI면 `303 See Other` Location, 아니면 404 빈 본문 |

### 2.13 PrivateKvStoreController `/v1/kv-private` (`PrivateKvStoreController.java:47-151`)

CustomKvStore와 동일 명령이며 private 전용 예약 keySpace를 쓰고 key에 사용자별 접두를 붙인다(형식의 정본은 원본 `PrivateKvStoreController`; userId는 `WebRequestContext`의 인증 토큰; 인증 비활성 시 `userToken==null`이라 NPE→500, W5 — `ErrorState.LOGIN_REQUIRED.newBizException()`을 throw하지 않고 생성만 한다).

| method path | 명령 |
|---|---|
| GET `/{key}` | `GET_CUSTOM_KV` |
| PUT `` | `SET_CUSTOM_KV` |
| PUT `/{key}/:ttl` | `SET_CUSTOM_TTL` |
| GET `/{keys}/:bulk` | `GET_CUSTOM_KV_BULK`(응답 key에서 접두 제거; 접두 불일치 검사도 throw 누락) |
| PUT `/:bulk` | `SET_CUSTOM_KV_BULK`(접두 부착) |

### 2.14 ProfileController `/v1/profile`, 2.15 ProfileDataController `/v1/profile-data` (`ProfileController.java:40-65`, `ProfileDataController.java`, `ProfileConsumer.java:38-72`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/v1/profile/{yyyymmdd}/{txid}` | path txid(long), yyyymmdd | `TRANX_PROFILE` `{date, txid, max=10}` → XLogProfilePack 1개 | `List<Step>`(`scouter.lang.step.Step.toObjectList(profile bytes)`; Step 하위 클래스 필드가 그대로 Jackson 직렬화) |
| GET `/v1/profile-data/{yyyymmdd}/{txid}` | 동일 | 동일 | `List<ProfileStepData{mainValue,additionalValueList,step}>`; mainValue는 step type별 text dictionary 해석(`model/ProfileStepData.java:108-170`), span step은 `SSpanStep/SSpanCallStep`로 변환 |

### 2.16 ShortenController `/v1/shortener` (`ShortenController.java:45-79`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/{key}` | path | `GET_CUSTOM_KV` `{keySpace=shortner,key}` | `String` |
| POST `` | query `url` | `SET_CUSTOM_KV` `{shortner, hexa32(hash(url)), url, ttl=-1}` | `String` = `<requestURL 앞부분>/s/<hexa32>` |

### 2.17 SummaryController `/v1/summary` (`SummaryController.java:49-313`, `SummaryConsumer.java:63-245`)

14 endpoint = 7종 × {`/ofType/{objType}`, `/ofObject/{objHash}`}. 공통 파라미터 `SummaryRequest`: query `startTimeMillis/endTimeMillis` 또는 `startYmdHm/endYmdHm`(yyyyMMddHHmm). 날짜 단위로 쪼개 매일 `{date, stime, etime, objType?(null이면 미포함), objHash}`로 한 번씩 호출하고 `summaryKey`(id)로 합산한다.

| path prefix | TCP 명령 | 응답 item(`Summary{itemMap: {id: item}}` — JSON은 `itemMap` 객체) |
|---|---|---|
| `/service` | `LOAD_SERVICE_SUMMARY` lists `id,count,error,elapsed,cpu,mem` | `ServiceSummaryItem{summaryKey,summaryKeyName(service text),count,errorCount,elapsedSum,cpuSum,memorySum}` |
| `/sql` | `LOAD_SQL_SUMMARY` `id,count,error,elapsed` | `SqlSummaryItem{…,errorCount,elapsedSum}`(sql text) |
| `/apiCall` | `LOAD_APICALL_SUMMARY` | `ApiCallSummaryItem`(apicall text) |
| `/ip` | `LOAD_IP_SUMMARY` `id,count` | `IpSummaryItem{summaryKeyName=IPUtil.toString(id)}` |
| `/userAgent` | `LOAD_UA_SUMMARY` | `UserAgentSummaryItem`(userAgent text) |
| `/error` | `LOAD_SERVICE_ERROR_SUMMARY` `id,count,error,service,message,txid,sql,apicall,fullstack` | `ErrorSummaryItem{error,service,errorMessage,txid,sql,apiCall,fullStack}`(각 text 해석) |
| `/alert` | `LOAD_ALERT_SUMMARY` `id,title,count,level` | `AlertSummaryItem{summaryKeyName=title,count,level(enum)}` |

### 2.18 TempTestController `/v0/temp/test` (`TempTestController.java:26-105`)

8 endpoint(`GET ""`(session attribute `testId` 생성, 평문 `OK`), `/more`(@NoAuth, `OK-More`), `/todo`, `/todoAsResult`, `/exception`, `/exception/state`, `/exception/biz`, `/map`). TCP 호출 없음, 개발용. Go는 제공하지 않아도 된다(의도된 차이로 기록).

### 2.19 UserController `/v1/user` (`UserController.java:48-104`)

| method path | 인증 | body | TCP 명령 | 응답 |
|---|---|---|---|---|
| POST `/login` | @NoAuth | `LoginRequest{serverId?(기본 default server),user{id,password}}` | `CHECK_LOGIN` `{id, pass=sha256(password)}` → BooleanValue | `true` + servlet session(`JSESSIONID`) |
| POST `/loginGetToken` | @NoAuth | 동일 | `CHECK_LOGIN` + `SET_CUSTOM_KV`(세션 저장) | `BearerTokenView{success:true,bearerToken}` |

### 2.20 VisitorController `/v1/visitor` (`VisitorController.java:25-132`, `VisitorConsumer.java`)

| method path | 파라미터 | TCP 명령 (request) | 응답 |
|---|---|---|---|
| GET `/realTime/ofObject/{objHash}` | path | `VISITOR_REALTIME` `{objHash}` → DecimalValue | `Long` |
| GET `/realTime/ofType/{objType}` | path | `VISITOR_REALTIME_TOTAL` `{objType}` | `Long` |
| GET `/realTime` | query objHashes 필수(빈 값 400) | `VISITOR_REALTIME_GROUP` `{objHash=ListValue}` (null이면 0) | `Long` |
| GET `/{yyyymmdd}/ofObject/{objHash}` | path | `VISITOR_LOADDATE` `{objHash, date}` | `Long` |
| GET `{yyyymmdd}/ofType/{objType}` (선행 `/` 없음, Jersey가 보정) | path | `VISITOR_LOADDATE_TOTAL` `{objType, date}` | `Long` |
| GET `/ofObject/{objHashes}` | `VisitorGroupRequest`: path objHashes 목록; query startYmd/endYmd **(yyyyMMddHH)** 또는 startYmdH/endYmdH(ms) | `VISITOR_LOADDATE_GROUP` `{objHash[], sDate=startYmd, eDate=endYmd}` → MapPack `{time,value}` | `VisitorGroup{time(String),value}` |
| GET `/hourly/ofObject/{objHashes}` | 동일 | `VISITOR_LOADHOUR_GROUP` `{objHash[], stime=startYmdH, etime=endYmdH}` → MapPack 스트림 | `List<VisitorGroup>` |

`/ofObject` 경로는 `validate()`가 `startYmd`를 `yyyyMMddHH`로 파싱해 `startYmdH`에 넣지만 consumer는 `sDate/eDate`에 **원문 `startYmd`**를 넣는다(서버 handler가 어떤 형식을 기대하는지는 [aggregation-query](aggregation-query.md)/visitor 문서 기준; 결함 후보 W6).

### 2.21 XLogController `/v1/xlog` (`XLogController.java:58-227`, `XLogConsumer.java`)

| method path | 파라미터 | TCP 명령 | 응답 |
|---|---|---|---|
| GET `/realTime/{xlogLoop}/{xlogIndex}` | path loop/index(≥0); query objHashes 필수 | loop==0&&index==0이면 `TRANX_REAL_TIME_GROUP_LATEST` 아니면 `TRANX_REAL_TIME_GROUP`, `{index, loop, count=10000, objHash[]}` → MapPack(meta `loop,index`) 먼저, XLogPack 스트림 | `jsonStream` result `{"xlogLoop","xlogIndex","xlogs":[SXLog…],"count"}` |
| GET `/{yyyymmdd}` | `PageableXLogRequest`: query startTimeMillis/endTimeMillis 또는 startHms/endHms, objHashes 필수, pageCount, lastTxid, lastXLogTime | `TRANX_LOAD_TIME_GROUP_V2` `{date, stime, txid=lastTxid, etime, lastBucketTime=lastXLogTime, pageCount, objHash[]}` → XLogPack 스트림 후 MapPack(`hasMore,lastTxid,lastXLogTime`) | result `{"xlogs":[…],"hasMore","lastTxid","lastXLogTime","count"}` |
| GET `/{yyyymmdd}/{txid}` | path | `XLOG_READ_BY_TXID` `{date, txid}` → XLogPack | `SXLog` (없으면 null) |
| GET `/{yyyymmdd}/gxid/{gxid}` | path | `XLOG_READ_BY_GXID` `{date, gxid}` → 스트림 | `List<SXLog>` |
| GET `/search/{yyyymmdd}` | `SearchXLogRequest`: 시간 2종 택일, objHash(long), service, ip, login, desc, text1..5 (null 아닌 것만 put) | `SEARCH_XLOG_LIST` `{date, stime, etime, service?, objHash?, ip?, login?, desc?, text1..5?}` | `List<SXLog>` |

`SXLog`(`model/scouter/SXLog.java:160-200`): endTime, objHash, service(hash), txid, threadNameHash, caller, gxid, elapsed, error(hash), cpu, sqlCount, sqlTime, ipaddr(`a.b.c.d`), kbytes, userid, userAgent, referrer, group, apicallCount, apicallTime, countryCode, city, xType(int), login, desc, hasDump(byte), text1..5, queuingHostHash, queuingTime, queuing2ndHostHash, queuing2ndTime, profileCount, profileSize. hash는 해석하지 않은 원본 값이다.

### 2.22 XLogDataController `/v1/xlog-data` (`XLogDataController.java:67-295`)

| method path | 파라미터 | 소스 | 응답 |
|---|---|---|---|
| GET `/realTime/{offset1}/{offset2}` | path; query objHashes 필수 | **webapp `XLogLoopCache`**(§1.1; 최대 10,000건 요청, 20,000 상한, `endTime > now-2000` 보류) | result `{"xlogs":[XLogData…],"offset1","offset2","count"}` |
| GET `/{yyyymmdd}` | `PageableXLogRequest` | `TRANX_LOAD_TIME_GROUP_V2` | result `{"xlogs":[XLogData…],"hasMore","lastTxid","lastXLogTime","count"}` |
| GET `/{yyyymmdd}/{txid}` | path | `XLOG_READ_BY_TXID` | `XLogData` |
| GET `/{yyyymmdd}/multi/{txidList}` | path txidList 목록 | `XLOG_LOAD_BY_TXIDS` `{date, txid=ListValue}` → 스트림 | `List<XLogData>` |
| GET `/{yyyymmdd}/gxid/{gxid}` | path | `XLOG_READ_BY_GXID` | `List<XLogData>` |
| GET `/search/{yyyymmdd}` | `SearchXLogRequest` | `SEARCH_XLOG_LIST` | `List<XLogData>` |

`XLogData.of`(`model/XLogData.java:161-221`): `GET_TEXT_ANY_TYPE`로 service/hashMessage(threadName, queuingHost)/error/userAgent/referrer/group/city/login/desc를 먼저 적재 후 endTime, objHash, service(text), txid, threadName(text), caller, gxid, elapsed, error(text), cpu, sqlCount, sqlTime, ipAddr, allocatedMemory(=kbytes), internalId(=userid), userAgent, referrer, group, apicallCount, apicallTime, countryCode, city, xLogType(`XLogTypes.Type` 이름: WEB_SERVICE/APP_SERVICE/BACK_THREAD/ASYNCSERVLET_DISPATCHED_SERVICE/ZIPKIN_SPAN/UNKNOWN), login, desc, hasDump, text1..5, queuingHost, queuingTime, queuing2ndHost, queuing2ndTime. Jackson 속성명은 Lombok getter 기준이라 `xLogType`은 `xlogType`, `ipAddr`은 `ipAddr`.

## 3. WebSocket

`@ServerEndpoint("/basic")`(`layer/websock/BasicSocket.java:14-52`, 등록 `main/WebAppMain.java:118-124`): 접속 시 텍스트 `"You are now connected to scouterx.webapp.layer.websock.BasicSocket"` 전송, 수신 텍스트를 그대로 echo. endpoint는 context root 기준 **`/basic`**이며 `/scouter/*` 밖이라 filter(CORS·no-cache·로깅)와 AuthFilter가 적용되지 않는다(무인증). session idle timeout 7일(`setDefaultMaxSessionIdleTimeout(7*24*3600*1000)`). 실데이터 push는 없다(테스트 페이지 `webroot/test/ws/`). embedded 모드에서는 `net_http_api_enabled=true`일 때만 HttpServer가 reflection으로 등록한다([http-contract](http-contract.md) §1). Go 호환 대상에서 제외해도 Client 기능 손실이 없다(의도된 차이로 기록).

### 3.1 정적 자원·기타 경로

| path | 원본 | 동작 |
|---|---|---|
| `/*` | `WebAppMain.java:145,168-176` | classpath `webroot/`를 `DefaultServlet`(dirAllowed=false, pathInfoOnly=true)로 제공. webapp SPA·테스트 페이지. filter 없음 |
| `/extweb/*` | `:146,178-186` | `net_http_extweb_dir`(기본 `./extweb`) 디렉터리, **dirAllowed=true**(디렉터리 목록 노출), 무인증 |
| `/swagger` | `:147,161-166` | swagger `Bootstrap` servlet(`init`에서 `BeanConfig`만 설정, `doGet` 없음 → GET은 405, `swagger/Bootstrap.java:21-49`). `/scouter/swagger.json`은 `net_http_api_swagger_enabled`일 때 Jersey가 제공(인증 filter는 `scouterx.` 패키지 resource에만 걸리므로 swagger listing은 **무인증**) |
| 요청 로그 | `:188-203`(standalone) | `./logs/http-request-yyyy_mm_dd.log` 고정 경로(embedded는 Server의 `{log_dir}`, [http-contract](http-contract.md) §1) |

Go는 `webroot`·`extweb`·swagger를 호환 대상으로 보지 않는다(제외 권고). `extweb` 디렉터리 목록 노출은 원본 취약 동작 후보(W13)로 분리한다.

## 4. Go 구현 가능성 분류

기준: `internal/service/service.go:111-153` `SupportedCommands()`와 각 파일의 명령 목록(2026-10-15 시점 working tree). 이 시점 `AccountCommands()`·`ConfigureCommands()`·`ManageCommands()`는 빈 scaffold였다. 이후 관리 명령 45개가 모두 등록되어(`command_registry_test`: mutation 45/45, unsupported 0) 아래 표의 "불가" 행은 현재 모두 collector 지원 명령으로 중계된다(현재 상태 열에 표시, §4.1). `docs/compatibility/query-scope.json`에서 의존 명령의 category는 `SET_CONFIGURE_SERVER`·`SET_CONFIGURE_WAS`·`OBJECT_REMOVE_INACTIVE`·`OBJECT_THREAD_CONTROL`·`SAVE_ALERT_SCRIPTING_*`=`mutation`, `OBJECT_SOCKET`(literal `SOCKET`)=`agent_read_relay`, `GET_ALERT_SCRIPT_LOAD_MESSAGE`·`CHECK_LOGIN`·`VISITOR_LOADHOUR_GROUP`=`local_read`이다. "가능"은 Go가 해당 TCP 명령 handler를 가진 경우이며 REST 계층(인증·JSON·캐시)은 별도 구현이 필요하다. "Agent relay"는 Go가 Agent TCP pool을 통해 중계하는 명령이다(`OBJECT_ENV`는 backend가 `AgentBackend`일 때만 목록에 들어간다, `service.go:113-115`).

| 분류 | endpoint | 의존 명령 (Go 상태) |
|---|---|---|
| 가능 (local) | `/v1/counter/*` 8개 | `COUNTER_REAL_TIME_ALL_MULTI`, `COUNTER_PAST_TIME_ALL`, `COUNTER_PAST_LONGDATE_ALL` (ExtendedCounter) |
| 가능 (local) | `/v1/xlog/*` 5개, `/v1/xlog-data/*` 6개 | `TRANX_REAL_TIME_GROUP(_LATEST)`, `TRANX_LOAD_TIME_GROUP_V2`, `XLOG_READ_BY_TXID`, `XLOG_READ_BY_GXID`, `XLOG_LOAD_BY_TXIDS`, `SEARCH_XLOG_LIST`(SupportedCommands 포함), text dictionary(`GET_TEXT_ANY_TYPE`). `/xlog-data/realTime`은 webapp ring cache 대체 설계 필요 |
| 가능 (local) | `/v1/profile`, `/v1/profile-data` | `TRANX_PROFILE`(xType 5 span은 TraceBackend 필요) |
| 가능 (local) | `/v1/summary/*` 14개 | `LOAD_*_SUMMARY` 7개(SummaryQueries) |
| 가능 (local) | `/v1/visitor/*` 7개 | `VISITOR_*` 7개 |
| 가능 (local) | `/v1/kv/*`, `/v1/kv/space/*`, `/v1/kv-private/*`, `/v1/shortener/*`, `/s/{key}` | `GET/SET_GLOBAL_KV*`, `GET/SET_CUSTOM_KV*`(LocalStore, oracle 검증됨) |
| 가능 (local) | `/v1/dictionary/{yyyymmdd}` | `GET_TEXT_PACK` |
| 가능 (local) | `/v1/alert/realTime`, `/v1/interactionCounter/*`, `/v1/activeService/stepCount/ofType` | `ALERT_REAL_TIME`, `INTR_COUNTER_REAL_TIME_BY_OBJ`, `ACTIVESPEED_REAL_TIME` |
| 가능 (local) | `/v1/object` 목록, `/v1/info/server`, `/v1/info/counter-model`, `/v1/user/login`, `/v1/user/loginGetToken` | `OBJECT_LIST_REAL_TIME`, `LOGIN`/`SERVER_STATUS`, `GET_XML_COUNTER`, `CHECK_LOGIN`, `SET_CUSTOM_KV` |
| 가능 (local, 읽기만) | `/v1/configure/server`, `/v1/alert/scripting/{counterName}`의 contents 2종 | `GET/LIST_CONFIGURE_SERVER`, `CONFIGURE_DESC/VALUE_TYPE/VALUE_TYPE_DESC`, `GET_ALERT_SCRIPTING_(CONFIG_)CONTETNS` |
| 가능 (Agent relay) | `/v1/activeService/{ofType,ofObject,thread}/...`, `/v1/object/{threadList,threadDump,heapHistogram,env,socket}`, `/v1/object/host/*`, `/v1/configure/object/{objHash}` | `OBJECT_ACTIVE_SERVICE_LIST`, `OBJECT_THREAD_DETAIL`, `OBJECT_THREAD_LIST/DUMP`, `OBJECT_HEAPHISTO`, `OBJECT_ENV`(AgentBackend 필요), `OBJECT_SOCKET`, `HOST_TOP`, `HOST_DISK_USAGE`, `GET/LIST_CONFIGURE_WAS` (AgentReadCommands) |
| 2026-10-15 분류 "불가 (Go 미구현 mutation)" → 현재 가능(local·relay) | `/v1/configure/set/server`, `/set/object/{objHash}`, `/set/kv/server`, `/set/kv/ofType/{objType}` | `SET_CONFIGURE_SERVER`(local), `SET_CONFIGURE_WAS`(Agent relay) |
| 〃 → 현재 가능(local) | `/v1/object/remove/inactive`, `/remove/inactive/server` | `OBJECT_REMOVE_INACTIVE` |
| 〃 → 현재 가능(Agent relay) | `/v1/activeService/control/thread/...` | `OBJECT_THREAD_CONTROL` |
| 2026-10-15 분류 "불가 (JVM script)" → 현재 가능(Go 의미) | `/v1/alert/set/config/scripting`, `/set/rule/scripting`, `/read/scripting/{loop}/{index}`; `/v1/alert/scripting/{counterName}`의 desc map 2종 | `SAVE_ALERT_SCRIPTING_*`(파일 저장), `GET_ALERT_SCRIPT_LOAD_MESSAGE`(저장·변경 사실 ring), `GET_ALERT_REAL_COUNTER_DESC`·`GET_PLUGIN_HELPER_DESC`(고정 소스 정적 표). script 컴파일·실행은 제외 확정. 결정: 네 명령 중 하나라도 collector가 지원하지 않으면 `/scripting/{counterName}` 전체를 501로 응답하고 빈 desc map으로 위장하지 않는다(`internal/webapp/CONTRACT.md`) |
| 제외 확정 | `/v0/temp/test/*` 8개, WebSocket `/basic`, swagger UI(·`/scouter/swagger.json`), webroot, `/extweb/*` | 개발/테스트용·정적 자원. Go는 404 |

합계(2026-10-15 재집계 당시): 가능 83(local 72 + relay 11), 불가 10(mutation 7 + JVM script 3; `/scripting/{counterName}`은 local로 셌으나 desc map 부분 불가), 제외 권고 8(+WebSocket·정적 자원) = **101**. 현재는 불가 10개가 모두 가능으로 바뀌어 구현 93(= 101 − TempTest 8, §4.1)이고 제외 8은 제외 확정이다. 이전 초안의 "local 74 + relay 13"은 합이 105가 되어 오류였다. collector가 지원하지 않는 명령은 빈 성공 응답으로 대체하지 않고 `NOT_IMPLEMENTED`(501) 형식의 `CommonResultView`로 명시한다.

controller별 집계(`@GET/@POST/@PUT` 수 = `grep -cE '^\s*@(GET|POST|PUT|DELETE|PATCH)\b'`, 총 101, 2026-10-15 분류. "불가" 열은 현재 모두 구현됨):

| controller | 수 | local | relay | 불가 | 제외 |
|---|---|---|---|---|---|
| ActiveService | 5 | 1 (stepCount) | 3 (ofType, ofObject, thread) | 1 (control) | |
| Alert | 5 | 2 (realTime, scripting 부분) | | 3 (set/config, set/rule, read/scripting) | |
| Configure | 6 | 1 (server) | 1 (object) | 4 (set/server, set/object, set/kv/server, set/kv/ofType) | |
| Counter | 8 | 8 | | | |
| CustomKvStore | 5 | 5 | | | |
| Dictionary | 1 | 1 | | | |
| GlobalKvStore | 5 | 5 | | | |
| HostObjectRequest | 2 | | 2 | | |
| Info | 2 | 2 | | | |
| InteractionCounter | 2 | 2 | | | |
| Object | 8 | 1 (목록) | 5 (threadList, threadDump, heapHistogram, env, socket) | 2 (remove/inactive 2종) | |
| SimpleView | 1 | 1 | | | |
| PrivateKvStore | 5 | 5 | | | |
| Profile / ProfileData | 1 / 1 | 2 | | | |
| Shorten | 2 | 2 | | | |
| Summary | 14 | 14 | | | |
| TempTest | 8 | | | | 8 |
| User | 2 | 2 | | | |
| Visitor | 7 | 7 | | | |
| XLog | 5 | 5 | | | |
| XLogData | 6 | 6 | | | |
| **합계** | **101** | **72** | **11** | **10** | **8** |

주의: `/v1/configure/object/{objHash}`는 `CONFIGURE_DESC`·`CONFIGURE_VALUE_TYPE`도 `{objHash}`로 호출한다. Go는 objHash≠0이면 앞의 둘을 Agent로 중계하고 `CONFIGURE_VALUE_TYPE_DESC`는 빈 MapPack을 돌려준다(`internal/service/server_metadata.go:175-200`). Java 대비 차이는 [server-metadata-query](server-metadata-query.md) 기준으로 확인한다. `/v1/interactionCounter/realTime`·`/v1/object`·`SAlert` 등은 webapp `AgentModelThread` 캐시(§1.1)를 전제하므로 Go 내장 시 ObjectCache 직접 조회로 대체해야 한다.


### 4.1 Go 구현 상태 (2026-10-16, `internal/webapp`)

- 프레임워크: Java webapp처럼 자기 서버의 Client TCP 주소에 bounded pool(기본 12)로 접속해 명령을 중계한다(embedded `__local` 로그인, 계정 파일이 없으면 `--user/--password`). CORS·NoCache·requestId·인증 3종(IP/bearer/session, 원본의 비대칭 포함)·`CommonResultView`(숫자 문자열)·streaming·오류 매핑·gzip을 구현했다. `Supported`에 `service.SupportedCommands()`를 연결해 미지원 명령은 501 `NOT_IMPLEMENTED`로 응답한다(빈 성공 없음).
- controller A(User 2, Info 2, Object 8, Host 2, ActiveService 5, Configure 6, Alert 5, Counter 8 = 38)와 B(XLog 5, XLogData 6, Profile 1, ProfileData 1, Summary 14, Visitor 7, Dictionary 1, GlobalKv 5, CustomKv 5, PrivateKv 5, Shorten 2+`/s/{key}`, SimpleView 1, InteractionCounter 2 = 55, `bRoutes` 테스트로 고정)를 구현했다(합계 93. 이전 판의 "61"은 합산 오류였다). `/v0/temp/test/*`, WebSocket `/basic`, swagger는 404(제외 권고대로).
- 관리 명령이 TCP handler에 등록되면서 §4의 "불가 (Go 미구현 mutation)" 7개는 가능으로 바뀌었고, alert scripting 3개는 저장·load message·descriptor 정적 표로 응답한다(script 실행은 제외 확정, [server-metadata-query](server-metadata-query.md)).
- live 설정: `internal/webapp`은 `net_http_api_cors_*`·`net_http_api_auth_*`·`net_http_api_allow_ips`·`net_webapp_tcp_client_pool_timeout`을 요청마다 읽는 `Server.SetSettings`를 제공한다. 서버 runtime이 scouter.conf reload마다 반영한다. `net_http_api_enabled`·`net_http_api_session_timeout`·`net_webapp_tcp_client_pool_size`는 Java처럼 기동 값이다. pool timeout 키가 없으면 `net_tcp_client_so_timeout_ms`(기본 8초)를 쓰고, 0 이하는 idle 연결을 매번 폐기한다. idle pool에서 꺼낸 collector 연결이 응답 byte 전에 EOF/reset이면 새 연결로 1회 다시 보낸다. 요청 쓰기가 실패한 경우는 항상, 그 밖에는 부작용 없는 명령만이다(SET_/SAVE_ 계열, `OBJECT_REMOVE_INACTIVE`, `OBJECT_THREAD_CONTROL`, `OBJECT_HEAPHISTO` 제외, [config-behavior](config-behavior.md), [webapp CONTRACT](../../internal/webapp/CONTRACT.md)).
- `/xlog-data/realTime`은 webapp 자체 ring 대신 collector의 `TRANX_REAL_TIME_GROUP` loop/index를 그대로 노출한다(`internal/webapp/CONTRACT.md`).
- 검증: fake collector(loopback TCP)로 endpoint JSON 형태·인증·pool·오류 매핑을, 실제 `server.Open`으로 `/v1/info/server`·`/v1/counter/realTime` end-to-end를 확인했다. Java webapp를 실제로 띄워 JSON golden을 대조하지는 않았다(미검증). 기동은 `--webapp` 또는 `net_http_api_enabled=true`([runtime](runtime.md)).
- 추가 결함 후보 W15: 원본 `/configure/set/kv/server`는 collector가 읽지 않는 빈 MapPack을 보내 스트림이 어긋난다(`ConfigureController`→`SET_CONFIGURE_SERVER` 경로). Go는 body 없이 보낸다.

## 5. 원본 결함·취약 동작 후보

| ID | 위치 | 현상 |
|---|---|---|
| W1 | `framework/session/UserToken.java:73-76` | bearer token `.` 분할 결과 검증 없음 → 형식 오류 시 500 |
| W2 | `ServerManager.java:208-217`, `ValidationExceptionMapper.java:45` | multi-server에서 `serverId` 누락 시 `ValidationException`이 mapper에서 ClassCastException → 500(의도는 400) |
| W3 | `model/scouter/SCounter.java:46-51`, `CounterConsumer.java:153-154` | AgentModelThread 캐시에 없는 objHash(새 객체, 최대 10초 지연·죽은 객체) → NPE 500 |
| W4 | `ConfigureKVHandleConsumer.java:36,80` | config key를 regex escape 없이 `Pattern.compile` → `.`/`$` 포함 key가 다른 줄을 치환하거나 PatternSyntaxException |
| W5 | `PrivateKvStoreController.java:131-149` | `ErrorState...newBizException()`을 throw하지 않아 미인증 시 NPE 500, 접두 불일치 key가 통과 |
| W6 | `VisitorController.java:103`, `VisitorConsumer.java:135-151` | `/ofObject/{objHashes}`의 `startYmd/endYmd`는 `yyyyMMddHH`로 검증되지만 `sDate/eDate`에 원문을 전달 |
| W7 | `AuthFilter.java:56-65` | IP 인증이 켜져 있고 allow-list에 맞으면 bearer/session 검사 없이 통과한다. 원본은 `net_http_api_auth_ip_header_key`로 지정한 헤더 값의 출처를 검증하지 않는다. Go도 같은 순서를 유지한다(Client 호환, §4.1). 이 키는 헤더를 덮어쓰는 신뢰할 수 있는 proxy 뒤에서만 쓴다 |
| W8 | `CounterController.java` | `/{counter}/ofType/{objType}` 6시간 제한은 `validate()`가 호출되는 endpoint에만 적용되고 `/stat/*`는 날짜 범위 제한이 없다 |
| W9 | `CommonResultView.java:42` | 숫자가 문자열로 직렬화되어 `status`/`objHash`/`txid`가 모두 문자열. 호환을 위해 Go도 동일하게 내보내야 한다(변경하면 기존 클라이언트 스크립트가 깨짐) |
| W10 | `XLogDataController.java:129-131` | realtime은 webapp 프로세스 로컬 ring(40,000)에 의존해 offset이 webapp 재시작마다 초기화되고 서버 loop/index와 무관 |
| W11 | `layer/service/UserTokenService.java:99-107` | 메모리 cache에 없는 bearer token 검증 시 세션 KV에 그 사용자의 저장 값이 없으면 `tokens.split` NPE → 401이 아니라 500. 저장 값 중 형식이 깨진 항목은 `mergeStoredTokensWith`(`:118-121`)에서 null filter 전 `isNotExpired` 호출로 NPE |
| W12 | `request/CounterRequest.java:90-98`, `SummaryRequest.java:83-90`, `VisitorGroupRequest.java:62-70` | `DateTimeParseException`을 잡지 않아 잘못된 날짜 문자열이 400이 아니라 500 |
| W13 | `main/WebAppMain.java:178-186` | 원본은 `/extweb/*`에 인증과 디렉터리 목록 제한을 두지 않는다(`dirAllowed=true`). Go는 `/extweb/*`를 제공하지 않는다(404) |
| W14 | `framework/filter/AuthFilter.java:83-87` | session 인증 활성 시 원본은 미인증 요청에도 session을 만든 뒤 401을 낸다 |
| W15 | `layer/controller/ConfigureController.java`(`/v1/configure/set/kv/server`) | collector가 읽지 않는 빈 MapPack을 보내 스트림이 어긋남. Go는 body 없이 보냄 |
| W16 | `layer/service/UserTokenService.java:45-84`, `CustomKvStoreController.java:57-121` | 원본은 bearer 검증에서 저장된 세션 값의 시각 범위를 확인하지 않고, 세션 저장용 keySpace를 REST custom KV API의 대상에서 제외하지 않는다. Go는 범위를 벗어난 시각을 401로 거절하고(`TestBearerFutureFootprintRejected`), REST custom KV에서 세션 예약 keySpace를 400으로 거절한다(`TestBCustomKvReservedKeySpaces`). TCP Client 명령의 KV 계약은 원본과 같다. |
| W17 | `UserController.java:64-72`, `CorsFilter.java:39-66`, `ActiveServiceController`(`GET .../control/thread/...`) | 원본은 로그인 때 session id를 새로 발급하지 않고, 쿠키에 SameSite/Secure를 지정하지 않으며, `*`+credentials=true 설정에서 요청 Origin을 그대로 돌려주고, thread 제어가 GET endpoint다. Go: 로그인마다 새 session id, `SameSite=Lax`; CORS Origin 처리와 GET endpoint는 원본 유지(Client 호환) |

## 6. Go 구현 시 결정 필요 사항

1. 경로 접두 `/scouter` 유지 여부와 포트(Java는 Server embedded 6180 / standalone 6188). 기존 REST 소비자가 `/scouter/v1/...` 경로를 쓰므로 유지가 안전하다.
2. 숫자 문자열 직렬화(W9) 호환 유지 여부.
3. `serverId` 값: 단일 내장 서버의 `hash("127.0.0.1" + port)`(embedded 규칙)로 고정할지, 설정 가능하게 할지. bearer token의 `hexa32(serverId)`와 `/v1/info/server.id`가 이에 묶인다.
4. 인증: 기본(3옵션 false)은 무인증이다. Go 기본값을 Java와 같게 둘지, loopback 외 바인딩 시 최소 session/bearer를 강제할지. bearer 저장소(collector custom KV)를 원본과 같은 형식으로 쓰면 Java webapp와 토큰 호환이 된다.
5. `/v1/xlog-data/realTime`의 offset 의미(webapp ring vs 서버 loop/index 직접 노출).
6. 불가 endpoint의 응답 형식(501 `NOT_IMPLEMENTED` 권고) 및 `/v1/alert/scripting/{counterName}` 부분 응답 정책.
7. `AgentModelThread` 대체: Go는 ObjectCache를 직접 읽을 수 있으므로 10초 지연·W3 NPE를 재현하지 않고 즉시 조회로 바꾸는 것이 의도된 차이가 된다.
8. TZ: Java는 JVM 기본 TZ로 `yyyyMMddHHmmss`를 해석한다. Go는 `internal/scoutertime`의 서버 TZ를 사용하고 문서화한다.

## 7. 완료 조건·fixture 후보

- 응답 wrapper golden: `{"status":"200","requestId":"#xxx0","resultCode":"0","message":"success","result":true}`(숫자 문자열) 및 오류 `{"status":"401","requestId":"#xxx1","resultCode":"401","message":"login required.","result":null}`(오류도 `requestId` 포함, 필드 순서는 §1 "필드 순서·null" 가정). streaming은 `{"status":"200","resultCode":"0","message":"","result":…}`(`requestId` 없음).
- 인증 매트릭스: 3옵션 조합 × (`@NoAuth` endpoint, 일반 endpoint) × (IP allow 일치/불일치, bearer 유효/만료/형식오류, session 유무). bearer 토큰 포맷 `V1.<token>.<hexa32>.<userId>`와 저장 값의 병합·만료 제거.
- 파라미터 파서: `ZZ.splitParam*`(`[a,b]`, `a,b`), 시간 4종 형식과 공존 오류(400 message 문자열 일치), `pageCount` 기본/상한, `lastTxid/lastXLogTime` 짝.
- 각 endpoint별 request MapPack golden(§2의 key 집합·타입: `objHash`는 ListValue vs Decimal, `GET_GLOBAL_KV`는 TextValue)과 응답 JSON 필드 집합(§2 model). 서버 handler 응답은 기존 `internal/service` 테스트 fixture를 재사용하고, JSON 변환만 추가 검증한다.
- streaming endpoint(dictionary, xlog realTime/pageable, xlog-data)의 바깥 wrapper(`status/resultCode/message` + `result`)와 meta 필드 순서(`xlogs` 배열 뒤 `hasMore/lastTxid/lastXLogTime/count`).
- text 해석: hash 0 → `""`, 미해결 → `**unlabeled**:<type>:<hash>`.
- CORS preflight(`OPTIONS` 200 + 헤더 5종), no-cache 헤더 3종. origin 설정 `*`+credentials `true`일 때 요청 `Origin` 반사.
- 인증 비대칭(IP 위반 + 유효 bearer = 통과, IP 위반 + 유효 session = 401), session 인증 시 미인증 요청의 `Set-Cookie`(W14) 여부는 Go에서 재현할지 의도된 차이로 둘지 결정 후 테스트로 고정.
- 오류 매핑: `ErrorStateBizException`(VALIDATE_ERROR만 message 접미), `ErrorStateException`, Jersey 404/405/415, 일반 예외 500 `message [caused by] cause`, `ValidationException`(W2) — 각 status·resultCode·message 문자열 golden.
- endpoint 수 회귀: Go route table이 §4 집계(101 중 93 구현(§4.1), 제외 8은 404, collector 미지원 명령은 501)와 일치하는지 표 기반 테스트.

## 8. 미조사·가정

- Jersey/Jackson 버전별 Lombok boolean(`isMaster`) 속성명, `@Builder` 클래스의 getter 생성은 Lombok 규칙으로 추정했다(빌드 산출물로 확인하지 않음).
- swagger `swagger.json` 내용과 `webroot` 정적 자원 목록은 호환 대상으로 보지 않았다.
- 각 TCP 명령의 서버 측 응답 상세는 기존 문서([query-contract](query-contract.md), [aggregation-query](aggregation-query.md), [agent-read-query](agent-read-query.md), [local-store-query](local-store-query.md), [server-metadata-query](server-metadata-query.md))를 따르며 여기서는 webapp이 읽는 key만 적었다.
- 실제 Java webapp 실행으로 JSON 출력을 캡처하지 않았다(미검증). golden 생성 시 Java webapp를 격리 환경에서 띄워 수집해야 한다.
- Jersey `@BeanParam` setter가 query 부재 시 기본값으로 호출되는지(pageCount·serverId 기본값 적용 경로), `LocalDateTime.parse`가 `yyyyMMddHH`만으로 시각을 해석하는지는 라이브러리 동작 추정이다.
- §2 각 endpoint의 응답 model 필드는 이전 조사를 유지했고, 이번 재검증은 부트스트랩·filter·인증·오류 매핑·요청 검증·consumer TCP 명령/param key·endpoint 수에 한정했다. model 클래스별 Jackson 속성명(Lombok getter)은 표본만 확인했다.

### 8.1 검증 이력

- 2026-10-15: 원본(`dee47fb5`) 대조. 확인: controller 22·endpoint 101(`grep -cE '^\s*@(GET|POST|PUT|DELETE|PATCH)\b'` 합계 101), consumer별 `RequestCmd`·param key, `AuthFilter`/`CorsFilter`/`CommonResultView`/`GenericExceptionMapper`/`ValidationExceptionMapper`/`ErrorState`/`ZZ`/`UserToken`/`UserTokenService`/`LoginRequest`/요청 검증 클래스, `AgentModelThread`(20×500ms), `XLogLoopCache`(40,000). 수정: streaming 숫자도 문자열, bearer touch 조건(경과 10%), §4 집계(83+10+8=101, 이전 87/105 오류), Agent relay 행의 표 깨짐, `OBJECT_SOCKET` literal. 추가: 필드 순서·null, 인증 비대칭·session 생성, CORS 공백 origin, 날짜 파싱 오류·range 상한, 페이징 연속 규칙, §3.1 정적 자원, controller별 집계표, W11~W14, fixture 4종.

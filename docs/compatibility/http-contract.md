# 내장 HttpServer 계약 (counter / register / telegraf)

정본은 `baseline.json`의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. 아래 원본 경로는 모두 `scouter.server/src/main/java/scouter/server/` 기준이며 `:N`은 줄 번호다. 이 문서는 Java Server가 Jetty로 직접 제공하는 세 servlet(`/counter/*`, `/register/*`, `/telegraf/*`)과 그 설정·부작용을 다룬다. 같은 Jetty에 embedded되는 `scouterx.webapp` REST API는 [webapp-api](webapp-api.md)에 있다. Go 서버는 이 세 servlet을 별도 servlet listener(`net_http_server_enabled`/`net_http_port` 또는 `--scouter-http`)에 구현했다(`internal/httpapi`). `--http`는 `/health`·pprof 전용의 다른 listener이며 두 listener가 같은 port를 쓰고 host가 겹치면(같은 IP·wildcard) 기동을 거부한다.

## 1. HttpServer 부트스트랩 (`http/HttpServer.java`)

| 항목 | 원본 | 계약 |
|---|---|---|
| 기동 조건 | `HttpServer.java:61`, `Main.java:55` | `Configure.getInstance()` 이후 `HttpServer.load()`가 daemon thread를 띄우고 `net_http_server_enabled`가 true일 때만 `new Server(net_http_port)`. 기본 `net_http_server_enabled=false`, `net_http_port=6180`(`NetConstants.SERVER_HTTP_PORT`, `scouter.common/.../net/NetConstants.java:23`). |
| telegraf XML이 켜면 강제 기동 | `Configure.java:1053-1055` | `scouter-telegraf.xml`의 `<enabled>true</enabled>`를 읽으면 `net_http_server_enabled = true`로 덮어쓴다. `scouter.conf`가 false여도 HTTP 서버가 뜬다. 파일이 없으면 덮어쓰지 않는다. |
| 요청 로그 | `:65-74` | Jetty `NCSARequestLog`, 파일 `{log_dir}/http-request-yyyy_mm_dd.log`(`yyyy_mm_dd` 자리를 `yyyy_MM_dd`로 치환), `retainDays=log_keep_days`, append, extended(NCSA combined: referer·user-agent 포함), cookie 미기록, timezone은 JVM 기본. |
| context | `:79-95` | `net_http_api_enabled=true`면 reflection으로 `scouterx.webapp.main.WebAppMain.setWebAppContext()`를 호출해 webapp의 `ServletContextHandler`(context path `/`, session 지원, `/scouter/*` Jersey)를 받는다. 클래스 부재/예외는 로그 `Error while loading webapp api context!` 후 **무시**하고 빈 `ServletContextHandler(SESSIONS)`(`/`)로 계속 기동한다. |
| servlet 등록 | `:96-98` | 같은 context에 `/counter/*`→`CounterServlet`, `/register/*`→`RegisterServlet`, `/telegraf/*`→`TelegrafInputServlet`. webapp의 `/*` DefaultServlet보다 구체 path가 우선한다. webapp filter(인증·CORS·no-cache)는 `/scouter/*`에만 걸리므로 **세 servlet에는 인증·IP 제한·CORS가 없다**(Go는 opt-in 허용 목록 검사를 제공한다, §7-1). |
| gzip | `:100-108` | `net_http_api_gzip_enabled`(기본 true)면 `GzipHandler`가 context를 감싼다: 포함 method GET/POST/PUT/DELETE, `minGzipSize=1024`. 응답 압축은 요청의 `Accept-Encoding: gzip`과 Jetty 기본 MIME 조건을 따른다. 세 servlet 응답은 본문이 없어 실질 영향이 없다. |
| WebSocket | `:112-122` | `net_http_api_enabled`면 `WebAppMain.setWebSocketServer(context)`를 reflection으로 호출. 실패는 로그 후 무시. |
| 기동 실패 | `:124-129` | `server.start()` 예외는 `Logger.println("HTTP", 10, ...)`만 남기고 thread 종료. 서버 프로세스는 계속 동작한다(포트 충돌 시 HTTP만 조용히 없음). |
| `getRemoteAddr` | `:133-145` | `X-FORWARDED-FOR` 헤더가 있으면 `StringUtil.split(ip, ',')`(빈 토큰 제거, trim 없음)의 첫 요소, 없거나 토큰 0개면 `request.getRemoteAddr()`. 원본은 헤더 값을 검증하지 않고 ObjectPack.address로 쓴다(D9). telegraf에서만 사용한다. |

관련 `Configure.java` 키(`:216-258`, 적용 `:689-714`):

| 키 | 기본값 | 사용처 |
|---|---|---|
| `net_http_server_enabled` | false | HttpServer 기동 |
| `net_http_port` | 6180 | Jetty 포트 |
| `net_http_extweb_dir` | `./extweb` | webapp `/extweb/*` 정적 파일 root |
| `net_http_api_enabled` | false | webapp context/WebSocket 로딩 |
| `net_http_api_swagger_enabled` / `_host_ip` | false / `""` | webapp swagger |
| `net_http_api_cors_allow_origin` / `_credentials` | `*` / `true` | webapp CorsFilter |
| `net_http_api_auth_ip_enabled`, `_ip_header_key` | false, `""` | webapp AuthFilter |
| `net_http_api_auth_session_enabled`, `net_http_api_session_timeout` | false, 86400 | webapp session 인증 |
| `net_http_api_auth_bearer_token_enabled` | false | webapp bearer |
| `net_http_api_gzip_enabled` | true | GzipHandler |
| `net_http_api_allow_ips` | `localhost,127.0.0.1,0:0:0:0:0:0:0:1,::1` | `allowIpExact`(`,` split Set) + `allowIpMatch`(`*` 포함 항목만 `StrMatch`) |
| `net_webapp_tcp_client_pool_size/_pool_timeout/_so_timeout` | 30 / 60000 / 30000 | webapp TcpProxy pool |

`allowIpExact/allowIpMatch`는 **webapp `AuthFilter`에서만** 평가된다(`scouter.webapp/.../framework/filter/AuthFilter.java:56-65`). `/counter`, `/register`, `/telegraf`는 IP 검사를 하지 않는다. `StrMatch`(`scouter.common/.../util/StrMatch.java`)는 `*`·`**`=ANY, 선두/말미/중간 `*` 1개까지의 glob(STR/END/MID/STR_MID/STR_END/MID_MID/MID_END), `*`가 없거나 길이<2면 equals다. 빈 target은 항상 false.

## 2. `/counter/*` (`http/servlet/CounterServlet.java`, `http/handler/CounterHandler.java`)

- method: `doPost`만 override(`CounterServlet.java:34`). GET 등은 `HttpServlet` 기본 405. 경로 suffix(`/counter/anything`)는 무시한다.
- body: `req.getReader()`로 읽는 JSON(json-simple `JSONParser`). 요청 `Content-Type`에 charset이 없으면 servlet 기본 ISO-8859-1로 디코딩되므로 비ASCII는 `charset=UTF-8` 명시가 필요하다.
- 최상위가 JSONArray면 각 JSONObject 요소를 순서대로 처리(`CounterHandler.java:82-89`), JSONObject면 단일 처리, 그 외(숫자·문자열 등)는 `SC-8001 Incorrect body` 로그. 어떤 경우든 **응답은 200·빈 본문**이다(status를 설정하지 않음). 파싱/처리 중 Throwable은 `SC-8000` 로그 후 **남은 배열 요소 처리를 중단**한다(`:95-97`); 이미 enqueue한 앞 요소는 그대로 반영된다.
- 요소 구조(`:100-112`):
  - `object`(필수, 없으면 요소 무시): `host`, `name`, `type`, `address`.
  - `objName = "/" + host + ("/" + name if name non-empty)`(`:154-163`). `host`가 null이면 문자열 `"/null"`이 된다.
  - `objHash = HashUtil.hash(objName)`(`scouter.common/.../util/HashUtil.java:56-66`, `str.getBytes()` **플랫폼 기본 charset**의 CRC32 변형).
  - `ObjectPack{objHash, objName, objType=type, address=address}`; version/tags 없음(`:114-125`).
  - 주소: `InetAddress.getByAddress(IPUtil.toBytes(address))`(`:127-131`). `IPUtil.toBytes`(`scouter.common/.../util/IPUtil.java:44-65`)는 `a.b.c.d`가 아니거나 null·파싱 실패면 `{0,0,0,0}`, 옥텟이 0..255 밖이면 **null**을 돌려 `getByAddress(null)`이 `UnknownHostException("addr is of illegal length")` → `SC-8000` 로그, 해당 요소부터 중단. 주소 검사는 ObjectPack enqueue **전**(`:103-105`)이라 이 경우 해당 요소의 ObjectPack도 들어가지 않는다.
  - `counters`(선택): `[{name, value}]`. `value`는 `Number`여야 하며 `floatValue()`로 `FloatValue`에 넣는다(`:133-145`). 정수도 float으로 저장된다. `PerfCounterPack{time=System.currentTimeMillis(), timetype=REALTIME(3), objName}`; objHash는 없고 `NetDataProcessor`가 `hash(objName)`으로 주입한다.
  - 요소 내 순서(`:100-112`): ObjectPack을 **먼저** enqueue한 뒤 counters를 만든다. 따라서 `value`가 문자열/`null`/boolean이거나 `name`이 문자열이 아니면(ClassCastException·NPE) ObjectPack은 이미 반영되고 PerfCounterPack만 빠진 채 남은 배열 요소 처리가 중단된다. `type`·`host`·`name`·`address`가 문자열이 아니어도 ClassCastException으로 같은 중단이 일어난다.
- 전달: `DataOutputX.write(NetCafe.CAFE)` + `writePack(pack)` 바이트를 `NetDataProcessor.add(bytes, addr)`(`netio/data/NetDataProcessor.scala:56-61`)에 넣는다. 수신 queue는 UDP와 공유하는 `RequestQueue(2048)`(`:53`, `putNotifySingle`)이며 포화 시 `S158 overflow recv queue!!` 로그 후 drop. HTTP 응답은 drop 여부와 무관하게 200이다. 이후 처리는 UDP 수신과 동일: ObjectPack→`AgentManager.active`(`core/AgentManager.scala:82-`; address가 비어 있으면 `addr.getHostAddress()`로 대체 `NetDataProcessor.scala:197-199`), PerfCounterPack→`PerfCountCore.add`(time==0이면 now, timetype==0이면 REALTIME, `COMMON_OBJHASH`/`COMMON_TIME` 주입 `:139-148`).
- 등록 부작용 없음: ObjectType/counter 정의를 만들지 않는다. 정의되지 않은 objType·counter도 저장은 되지만 Client는 `counters.xml`에 없는 항목을 표시하지 못한다. 정의는 `/register`로 먼저 올려야 한다.
- 예시 body는 `CounterHandler.java:43-73` 주석의 2-object 배열이다.

## 3. `/register/*` (`http/servlet/RegisterServlet.java`, `http/handler/RegisterHandler.java`)

- `doPost`만(`RegisterServlet.java:34-40`). 성공 `201 Created`, 그 외 `500`. 본문 없음.
- body는 **단일 JSONObject**(`RegisterHandler.java:70`; 배열이면 ClassCastException → 500):
  `{"object":{"type":<필수>,"display":<선택>}, "counters":[{"name","unit","display"?,"total"?,"all"?}, ...]}`. `total`/`all`은 JSON boolean이어야 한다(문자열이면 ClassCastException → 500). `counters`·`object`가 없으면 NPE → 500. 모든 예외는 `SC-8001 Http body parsing error` 로그 후 false(500)이다(`:170-173`).
- 선택 필드는 `containsKey`로 판정한다(`:79-83`, `:98-108`). 즉 `"display": null`은 "없음"이 아니라 **null displayName**으로 들어가고, `unit` 누락은 `null` unit이 된다(기본값 대입 없음). `"display"` 기본값 대입은 key 자체가 없을 때만 일어난다.
- `objType`이 `CounterEngine`에 없을 때(`:75-125`):
  - `ObjectType{name=type, displayName=display||type, icon=""}`, `Family` 새로 생성. family 이름은 같은 이름의 family가 없으면 `type`, 있으면 `type + "." + (Math.random()*100)`(예 `redis.37.2187…`, 비결정적).
  - 각 counter: `Counter{name, unit, displayName=display||name, icon="", total=total||true, all=all||true}`를 family에 추가, **첫 counter가 master**(`:117-119`).
  - `CounterManager.addFamilyAndObjectType(family, objectType)`(`CounterManager.java:250-262`): `counters.site.xml`(`Configure.CONF_DIR + CUSTOM_FILENAME`) DOM에 `<family>`·`<objectType>`을 **append**(같은 이름 기존 요소를 제거하지 않음) → 파일 저장 → `reloadEngine()`. 성공 시 `notifyAllClients()` 후 true.
- `objType`이 이미 있을 때(`:126-168`):
  - 기존 family/icon/subObject를 복사한 ObjectType, displayName은 `display||기존`. `objectType.getCounter(name)`(objectType 자체 counter + family counter, `scouter.common/.../lang/ObjectType.java:103-109`)로 **이미 해석되는 이름은 건너뛰고**, 새 이름만 objectType-level counter로 추가(unit/display/total/all 규칙 동일).
  - `CounterManager.editObjectType(objType)`(`CounterManager.java:337-345`, `editOrAppendObjectType :371-417`): 같은 `name`의 `<objectType>` 요소를 찾아 attribute를 덮어쓰고 counter 요소를 **append**(기존 child는 남음). 없으면 append. 저장·reload·`notifyAllClients()`.
- `notifyAllClients()`(`:177-188`): `AsyncRun`으로 `RemoteControl("REFETCH_COUNTER_XML", now, new MapPack(), commander=0)`을 `LoginManager.getLoginUserList()`의 **모든 Client session**에 `RemoteControlManager.add`(`management/RemoteControlManager.scala`, session별 queue 5개 상한, 초과 시 `S148` 로그·drop). Client는 `CHECK_JOB` polling으로 수신한다([agent-read-query](agent-read-query.md)의 CHECK_JOB 참조). 세션 없는 사용자는 5초 주기 cleanup으로 제거된다.
- 예시 body는 `RegisterHandler.java:38-61` 주석. 주석 JSON의 trailing comma는 json-simple이 허용하지 않을 수 있으니 fixture는 정규 JSON으로 만든다.

## 4. `/telegraf/*` (`http/servlet/TelegrafInputServlet.java`, `http/handler/TelegrafInputHandler.java`)

### 4.1 요청 처리 (`TelegrafInputHandler.handlerRequest :116-159`)

1. `input_telegraf_enabled`가 false면 즉시 return(200, 빈 본문). 어떤 경로든 응답은 **200·빈 본문**이며 status/body를 쓰지 않는다. 파싱 예외가 servlet까지 전파되면 Jetty 500.
2. `receivedTime = now`. `request.getReader().readLine()`으로 줄 단위 읽기(CR/LF 제거, charset은 Content-Type 의존).
3. 줄 수 상한: `lineCount++ > 1000` 검사가 readLine **앞**에 있으므로 1001줄 이상인 payload는 `TG010 ... some line could be dropped!` 로그 후 **payload 전체를 버린다**(1000줄 이하만 처리). 부분 처리가 아니다.
4. 중복 제거: `uniqueLineString.put(toLineStringKey(line), line)`(`InfluxSingleLine.java:110-169`). key = measurement + tag set + **첫 field key**(mode 0은 첫 공백까지, mode 1은 다음 ` `·`=`·`,` 전까지; `\`는 다음 문자를 literal로). 같은 key의 뒤 줄이 앞 줄을 덮어쓴다. 즉 한 payload에 같은 series가 여러 timestamp로 오면 마지막 줄만 남는다. 처리 순서는 `HashMap` 순서(비결정적).
5. 각 줄 `InfluxSingleLine.of(line, configure, receivedTime)`; null이면 skip. `input_telegraf_debug_enabled`면 `TG002`(수신 원문)·`TG003`(파싱 결과) 로그, 아니면 measurement debug일 때 `TG006`(원문, `of` 내부 `:317-319`, measurement 설정이 있으면 enabled/filter 검사 전에 출력)·`TG004`(결과, line이 null이 아닐 때만).
6. `count(line, HttpServer.getRemoteAddr(request))`. remote address가 옥텟 범위를 벗어난 dotted 문자열(예 `X-FORWARDED-FOR: 300.1.1.1`)이면 `IPUtil.toBytes`가 null을 돌려 `InetAddress.getByAddress(null)`이 `UnknownHostException`(IOException)을 던지고, 이것이 servlet까지 전파되어 **500·남은 줄 미처리**가 된다(D9 연장). 단, 이 검사는 objType·counter가 이미 등록된 줄에서만 도달한다. 같은 payload의 줄 사이에는 트랜잭션이 없으며 앞 줄의 enqueue는 취소되지 않는다.
7. servlet은 `doPost`만 구현한다(`TelegrafInputServlet.java:32-35`). GET 등은 405. `/telegraf/` 뒤 suffix(예 `/telegraf/write`)는 무시되므로 Telegraf `outputs.http`/`outputs.influxdb` 어느 URL이든 POST면 같은 처리다. `Content-Encoding: gzip` 요청 본문은 해제하지 않는다(Jetty `GzipHandler`의 request inflate는 Jetty 9.4 기본 비활성 — 미검증) → Telegraf `content_encoding = "identity"` 필요.

### 4.2 line protocol 파서 (`InfluxSingleLine.of :171-337`)

mode 전이: 0 measurement → `,` → 1 tags → ` ` → 2 fields → ` `(따옴표 밖) → 3 timestamp.

| 구간 | 규칙 | 비고 |
|---|---|---|
| measurement | `\`는 다음 문자 literal, `,`에서 tags로 전이. **공백을 구분자로 보지 않는다** | tag가 없는 줄(`cpu value=1 ts`)은 measurement가 `cpu value=1 ts` 전체가 되어 설정에 없으므로 **항상 drop**(결함 후보 D1) |
| tags | `key=value`를 `,`로 구분, `\`는 다음 문자 literal, ` `에서 종료. `=`로 key/value 전환, `,`마다 put. 종료 시 key 길이>0인 마지막 pair put | `,`에서는 key 길이 검사 없이 put하므로 `m,,a=1`은 빈 key `""` tag가 생긴다. 종료 ` `에서만 빈 key를 버린다. 같은 key가 반복되면 마지막 값. 따옴표 처리 없음 |
| fields | `"`로 quote 토글(`sink='"'`). **quote 안의 문자는 어디에도 append되지 않고** 따옴표 밖 `\`만 escape. quote 안의 `\`는 무시되므로 `\"`가 quote를 **닫는다**. ` `·`=`·`,`는 quote 밖에서만 의미. `,`에서는 빈 key도 put, ` `에서는 key 길이>0만 put | 문자열 field 값은 항상 빈 문자열이 된다. 값이 비어 있고 그 field가 counter mapping에 있으면 `valueStr.charAt(length-1)`이 예외 → `of`의 catch → **줄 전체 drop**(D2). mapping에 없는 문자열 field는 무해. `s="a\"b c"`처럼 escape된 따옴표 뒤 공백이 있으면 그 지점에서 timestamp 구간으로 넘어가 줄이 drop된다(D12) |
| timestamp | 남은 모든 문자(공백 포함). `Long.parseLong(ts) / 1_000_000`(ns→ms, 0 방향 절삭). | timestamp가 없거나 숫자가 아니면(뒤 공백 포함) 예외 → 줄 drop(D3). 단위는 ns만 가정한다(Telegraf `precision` 기본 1ns). 음수 timestamp도 허용 |

field 값 → `addNumField(:87-105)`: mapping(`tConfig.getCounterProtocol(fieldKey)`)이 없으면 무시. 마지막 문자가 `i`면 `Long.parseLong`→`DecimalValue`, `0-9`면 `Float.parseFloat`→`FloatValue`(정수 표기도 float, 큰 정수는 정밀도 손실), 그 외(`t`/`true`/`T`/`F`/`u` unsigned/`NaN`/문자열)는 skip. `-5i`·`1e10` 허용.

measurement 결정 후(`:311-336`): `telegrafInputConfigMap.get(measurement)` 없음 → null(로그 없음). `tConfig.isEnabled()` false → null. `isValidConfig()`(`ScouterTgMtConfig.java:261-266`: `objFamilyBase`가 `""` 또는 `"X$"`면 invalid) → null. `isTagFilterMatching(tags)` 불일치 → null. 이 네 검사는 try 밖이므로 `isTagFilterMatching`의 예외(빈 match 문자열 `charAt(0)`, D4)는 **servlet까지 전파되어 500이며 같은 payload의 남은 줄도 처리되지 않는다**.

`isTagFilterMatching`(`ScouterTgMtConfig.java:227-259`): filter가 비어 있으면 true. 모든 (tag, matchList) 항목을 OR로 평가. `!`로 시작하면 부정(`!cpu-total`은 값이 `cpu-total`이 아닐 때 true이며 **tag가 없는 경우도 true**). 긍정은 `matchStr.equals(value)` 정확 일치만, glob 없음.

### 4.3 object/counter 이름 결정 (`ScouterTgMtConfig.java`)

`X = "X$"`(`:33`). `setObjFamilyBase`·`setObjNameBase`는 저장 시 `"X$" + base`로 접두사를 붙인다(`:89-91`, `:137-139`). `objTypeBase`는 접두사 없음.

| 값 | 규칙 (`:177-222`) | sample(cpu, host=h1, 태그 `scouter_obj_type_prefix` 없음) |
|---|---|---|
| host | `tags.get(hostTag)`; 없으면 `"unknown"`; `hostMapping.get(host)`가 있으면 치환 | `h1` |
| family | `"X$" + objFamilyBase` + 각 `objFamilyAppendTags`마다 `"_" + tags.get(tag)`(없으면 문자열 `null`) | `X$HOST_METRIC` |
| objType | 각 `objTypePrependTags` 값이 비어 있지 않으면 `value + "_"` 앞에 붙임 + `objTypeBase` + append tags(`"_" + value`, 없으면 `null`) | `HOST_NEW` (prefix 태그가 `p`면 `p_HOST_NEW`) |
| objName | `"/" + host + "/" + "X$" + objNameBase` + append tags | `/h1/X$LINUX_NEW` (mysql sample은 base가 빈 문자열이라 `/h1/X$_<server>`) |
| objHash | `HashUtil.hash(objName)` | |
| objTypeIcon | 설정값 그대로 | `linux` |

Client는 `X$` 접두사의 family/objName을 그대로 본다. `objTypeIcon`은 Client 아이콘 파일명이다.

### 4.4 등록·수집 (`TelegrafInputHandler.count :161-175`)

1. `counterManager.getCounterEngine().getObjectType(line.objType)`가 없으면 `registerObjTypeQueue`(상한 1024)에 넣고 **이 줄은 저장하지 않는다**. queue가 차면 `RequestQueue.put`이 false를 돌려 **로그 없이 버린다**(`scouter.common/.../util/RequestQueue.java:89-98`, Q6). 같은 objType의 줄이 여러 번 들어가도 thread의 double check(`:203-206`)가 두 번째부터 skip한다.
2. 있으면 `hasNewCounterThenRegister`(`:177-192`): 줄에 포함된 mapped numeric field마다 `CounterProtocol.isNewOrChangedCounter`(`http/model/CounterProtocol.java:249-264`). normal counter(`getTaggedName(tags)`)를 `objectType.getCounter`(objectType counter → family counter 순, `scouter.common/.../lang/ObjectType.java:103-109`)로 찾아 없으면 new. 있고 `fresh`이면 `checkChangedDeeper`(`:298-309`)로 tagged displayName·unit·icon·total을 비교해 하나라도 다르면 changed. delta counter(`taggedName + "_$delta"`, unit `unit + "/s"`)는 normal이 new가 아닐 때만 같은 방식으로 검사한다. `fresh`는 CounterProtocol 인스턴스(설정 reload마다 새로 생성)별 플래그로, 생성 후 15초가 지난 뒤 **처음 `isNewOrChangedCounter`가 호출될 때** false가 된다(`:33-35`, `:67-71`, `:262`). 즉 설정을 바꾼 직후 15초 동안만 display/unit 변경을 감지하고 그 뒤에는 이름 존재 여부만 본다. 하나라도 new/changed면 그 CounterProtocol의 `toCounters(tags)`(normal, delta) 각각을 `addCounterQueue`(1024, 포화 시 무로그 drop)에 넣고 **true를 돌려 이 줄도 저장하지 않는다**(다른 field는 이미 등록돼 있어도 줄 전체 미저장).
3. 아니면 `InetAddress.getByAddress(IPUtil.toBytes(remoteAddress))`(IPv6/호스트명/`X-FORWARDED-FOR` 비IPv4 → `0.0.0.0`) 주소로 `NetDataProcessor.add(line.toObjectPack(remoteAddress, telegraf_object_deadtime_ms), address)` 후 `add(line.toPerfCounterPack(), address)`.

`toObjectPack`(`InfluxSingleLine.java:387-398`): `{objHash, objName, objType, address=remoteAddress 문자열, tags={deadTime: telegraf_object_deadtime_ms}}`. `deadTime` tag는 `AgentManager`의 dead 판정(`AgentManager.scala:59-66`, 0이면 기본 deadtime)에 쓰인다. version 없음.

`toPerfCounterPack`(`:400-464`): `{time=receivedTime(서버 수신 시각, line timestamp 아님), timetype=REALTIME, objName}`. 각 mapped field에 대해:

- normal counter(`deltaType != DELTA`): `normalizeSec > 0 && !hasDeltaCounter`(즉 NONE 타입)면 `MeterCounterManager.getMeterCounter(objHash, name)`에 add 후 `getAvg(normalizeSec)`를 `DoubleValue`로, 아니면 원 값(`DecimalValue`/`FloatValue`) 그대로. BOTH 타입의 normal 값은 정규화하지 않는다.
- delta counter(`DELTA`/`BOTH`): `CounterTimeCache.get(CounterKey(objHash, deltaName, REALTIME))`(`core/cache/CounterTimeCache.scala`, keep 120초·최대 1,000,000행)의 prev가 있을 때만 값 생성. `DECIMAL`이면 long 차, 아니면 float 차; `deltaPerSec = delta * 1000f / (timestampOrigin - prev.time)`(**line timestamp ms 기준**). 이후 `normalizeSec > 0`이면 MeterCounter avg(normalizeSec), 아니면 `input_telegraf_delta_counter_normalize_default`(true)일 때 avg(`input_telegraf_delta_counter_normalize_default_seconds`=30). `FloatValue`로 put. 마지막에 `CounterTimeCache.put(key, (value, timestampOrigin))`. 첫 sample·120초 이상 끊긴 뒤 첫 sample은 delta 값이 없다. 같은 timestamp가 두 번 오면 0으로 나눠 `Infinity/NaN`이 저장된다(D5).
- `CounterTimeCache`의 값은 `NumberValueWithTime{value: NumberValue(원 값, Decimal/Float), time: long(line timestamp ms)}`(`http/model/NumberValueWithTime.java:26-42`)다. key는 `CounterKey(objHash, deltaCounterName(tag 치환 후), REALTIME)`이고 `CacheTable.put(key, v, 120000)`(`core/cache/CounterTimeCache.scala:29-35`, 최대 1,000,000행)로 put마다 만료가 120초로 갱신된다. prev와 현재 값의 타입이 다르면(예 정수 `i` → 실수) 현재 값의 `getValueType()` 기준으로 차를 구한다.
- `MeterCounter`(`core/app/MeterCounter.java:32-60`)는 `MeteringUtil(2000, 31)`: **2초 bucket 31개**. `add(v)`는 현재 bucket에 `value += v; count++`. `getAvg(period)`는 현재 bucket부터 최근 `period`개(`MeteringUtil.check`로 최대 30, `scouter.common/.../util/MeteringUtil.java:68-72,87-95`)를 훑어 **Σvalue / Σcount**(sample 수 가중 평균, 0건이면 0)를 돌려준다. 실제 창은 `period × 2초`(normalizeSeconds=30 → 60초)다(Q1). `MeterCounterManager`(`core/app/MeterCounterManager.java:29`)는 (objHash, counterName)별 `LinkedMap` 최대 10,000개(초과 시 삽입 순서상 가장 오래된 항목 제거, `get`은 순서를 바꾸지 않음 → 계속 쓰이는 counter도 밀려나 평균이 초기화될 수 있다).
- 생성된 PerfCounterPack의 data 값 타입 요약: 정규화 normal → `DoubleValue`, 비정규화 normal → 수신 타입(`DecimalValue`/`FloatValue`), delta → 항상 `FloatValue`. 이후 `NetDataProcessor`가 `objHash`(`DecimalValue`)와 `time`(`DecimalValue`) 공통 key를 추가한다(`NetDataProcessor.scala:139-148`).

### 4.5 백그라운드 등록 thread (`TelegrafInputHandler.run :78-114`)

단일 daemon thread가 `registerObjTypeQueue` → `addCounterQueue` → `changeNotifyQueue`를 순서대로 비운다(각 항목 사이 100ms sleep, queue poll 100ms).

- `registerNewObjType0`(`:202-243`): objType이 이미 생겼으면 skip. `Family{name=line.family}`에 **이 줄에 있던 mapped numeric field**의 counters(normal/delta)만 추가, 첫 counter가 master(`HashMap` 순서라 비결정적). `ObjectType{name, displayName=name, icon=objTypeIcon, family}`. `counterManager.safelyAddFamily(family)`(`CounterManager.java:125-158`: `counters.site.xml` 메모리 DOM에서 같은 이름 `<family>`를 **통째로 교체**) + `safelyAddObjectType`(`:161-176`, 같은 이름 `<objectType>` 교체) → `saveCustomContent`로 파일 저장·정규화·engine reload. 둘 다 성공하면 `RegisterHandler.notifyAllClients()`. 같은 family를 공유하는 measurement(sample의 cpu/mem→`X$HOST_METRIC`)는 나중 등록이 family counter 목록을 덮어쓰고, 이후 `addCounter0`이 누락분을 다시 추가하는 churn이 생긴다(Q2).
- `addCounter0`(`:245-265`): `prevAddedCounter`(CacheTable 10,000행·5초)의 같은 이름 counter와 `semanticEquals`(name/displayName/unit/icon/total, `scouter.common/.../lang/Counter.java:126-135`)면 `[counter+] ignored by equals` 로그 후 skip. 아니면 objectType의 family에 `addCounter` + `safelyAddFamily`, 성공 시 cache에 넣고 `changeNotifyQueue.put(1)`(상한 10).
- `changeNotifyQueue`가 비면 누적 count>0일 때 한 번 `notifyAllClients()`.

이 과정의 결과로 Telegraf 수집은 counter 정의 생성·`counters.site.xml` 쓰기·Client `REFETCH_COUNTER_XML` 알림·새 counter가 등장한 줄의 drop이 동반된다.

## 5. Telegraf 설정 (`Configure.java`, `support/telegraf/*`)

### 5.1 로딩 순서

`Configure` thread가 3초마다 `reload(false)`(`Configure.java:569-574`, `:593-600`). `reloadConfig()`가 `scouter.conf`의 mtime 변경 시 `applyConfig()`와 **deprecated** `applyTelegrafInputConfig()`(`:859-1037`)를 실행하고, `reloadTgConfig(configRenewed)`(`:602-619`)가 `input_telegraf_config_file`(기본 `./conf/scouter-telegraf.xml`)을 처리한다:

- 파일이 없고 conf가 갱신됐으면 `telegrafInputConfigMap = telegrafInputConfigMapDeprecated`(deprecated 키만 유효).
- 파일이 있고 mtime이 바뀌었고 읽을 수 있으면 JAXB `unmarshal` → `telegrafOriginalConfig`; JAXBException은 stack trace만 남기고 **이전 `telegrafOriginalConfig`로** `applyTelegrafInputConfigNew()`를 실행한다(mtime은 갱신되지 않아 3초마다 재시도).
- `applyTelegrafInputConfigNew()`(`:1043-1113`): XML 전역 값으로 `input_telegraf_enabled/_debug_enabled/_delta_counter_normalize_default/_seconds/telegraf_object_deadtime_ms`를 **덮어쓰고**, enabled면 `net_http_server_enabled=true`. measurement별 `ScouterTgMtConfig`를 만들고, XML에 없는 measurement는 deprecated map에서 보충한다(우선순위: XML > deprecated). `hostMappings`는 `Collectors.toMap(telegraf→scouter)`, `counterMappings`는 `toMap(tgFieldName→CounterProtocol.of)`로 변환되므로 **중복 key가 있으면 `IllegalStateException`이 Configure thread로 전파**된다(D6). `tagFilters`는 같은 tag를 merge한다.
- `GET_CONFIGURE_TELEGRAF`/`SET_CONFIGURE_TELEGRAF`(`getTgConfigContents :621-631`, `saveTgConfigContents :633-636`)는 파일 원문을 읽고/쓰며 적용은 위 polling이 한다. 파일 부재 시 `TgConfig.getSampleContents()`를 돌려준다([server-metadata-query](server-metadata-query.md)).
- 진입점과 예외 전파: `Configure` 생성자(`:552-554`)와 3초 루프(`:569-574`), `SET_CONFIGURE_SERVER`(`netio/service/handle/ConfigureService.scala:67`)·`SET_CONFIGURE_TELEGRAF`(`:239`) 저장 성공 후 `reload(true)`가 같은 `reload`를 호출한다. `run()` 루프에는 catch가 없어 telegraf 설정 처리 중 RuntimeException(D6·D14·D15)이 나면 **Configure thread가 종료**되어 이후 `scouter.conf`·XML 변경이 반영되지 않는다. 기동 시(생성자)에 나면 `Configure.getInstance()` 자체가 실패한다.
- 갱신 순서의 함정(D13): XML이 이미 적용된 뒤 `scouter.conf`만 바뀌면 `applyConfig()`(`:830-836`)가 `input_telegraf_enabled/_debug_enabled/_delta_counter_normalize_default/_seconds/telegraf_object_deadtime_ms`를 **scouter.conf 값으로 되돌리고**, XML mtime은 그대로라 `applyTelegrafInputConfigNew()`가 다시 돌지 않는다. 같은 이유로 deprecated measurement 키 변경은 `telegrafInputConfigMapDeprecated`만 바꾸고 실제 사용 map(`telegrafInputConfigMap`)에는 XML이 다시 바뀔 때까지 반영되지 않는다. `net_http_server_enabled`도 scouter.conf 값으로 되돌아가지만 HttpServer는 기동 시 한 번만 읽으므로 실행 중 서버에는 영향이 없다.
- `input_telegraf_config_file`은 `CONF_DIR("./conf/") + "scouter-telegraf.xml"`(`:70`, `:445`) 상대 경로이며 `GET/SET_CONFIGURE_TELEGRAF` 원문 I/O는 UTF-8(`FileUtil.load(tgFile, "utf-8")`), JAXB unmarshal은 XML 선언의 encoding을 따른다.

### 5.2 XML 스키마 (`TgConfig.java`, `TgmConfig.java`, `TgCounterMapping.java`)

JAXB 기본 매핑(public field, 요소 이름 = 필드 이름, root `TelegrafConfig`). 요소 부재 시 Java 필드 기본값이 유지된다. list는 `@XmlElementWrapper`(wrapper 이름 = 필드 이름) + 항목 요소(`tag`/`measurement`/`hostMapping`/`tagFilter`/`counterMapping`)이고, `TagFilter.match`만 wrapper 없이 `<match>`를 반복한다. Go는 빈 wrapper(`<objTypePrependTags/>`)를 빈 list로, 반복 wrapper를 마지막 list로 처리한다. 알 수 없는 요소는 무시하고 enum 밖 `deltaType`은 null에 해당하는 값으로 두어 NONE처럼 처리한다. JAXB 필드 대조는 `internal/telegraf/oracle_test.go`의 `TestJAXBOracle`과 `testdata/jaxb/`가 맡으며 JDK 8·고정 source가 없으면 skip한다. 구현 상세와 과거 검증 근거는 [telegraf CONTRACT](../../internal/telegraf/CONTRACT.md)를 따른다.

필수·null 주의(`TgmConfig.java:40-107`, `TgCounterMapping.java:33-41`):

- `TagFilter.match`는 초기값이 없다(`TgmConfig.java:53-56`). `<tagFilter><tag>cpu</tag></tagFilter>`처럼 match가 없으면 null 값이 `Collectors.toMap`의 merge에 들어가 NPE(D6).
- `HostMapping.telegraf`/`scouter` 중 하나라도 없으면 `toMap` 값 null NPE 또는 null key, 같은 `telegraf`가 반복되면 `IllegalStateException(Duplicate key)`(D6).
- `counterName` 누락 → `CounterProtocol.setName(null)`의 `toCharArray` NPE. `displayName` 누락/빈 값은 counterName으로 대체. `tgFieldName` 중복 → `IllegalStateException`(D6), 누락 → null key(어떤 field와도 매칭되지 않음).
- `measurementName` 누락 → map key null. 같은 `measurementName`이 두 번 나오면 같은 `ScouterTgMtConfig`를 뒤 항목 값으로 덮어쓴다(merge 아님, `Configure.java:1063-1067`).

```
<TelegrafConfig>                                  TgConfig (:35-58)
  <enabled>true</enabled>                         boolean, 기본 true
  <debugEnabled>false</debugEnabled>              기본 false
  <deltaCounterNormalizeDefault>true</…>          기본 true
  <deltaCounterNormalizeDefaultSeconds>30</…>     기본 30
  <objectDeadtimeMs>35000</objectDeadtimeMs>      기본 35000
  <measurements>
    <measurement>                                 TgmConfig (:36-108)
      <measurementName>cpu</measurementName>      String, 기본 null (map key)
      <enabled>true</enabled>                     기본 true
      <debugEnabled>false</debugEnabled>          기본 false
      <objFamilyBase>HOST_METRIC</objFamilyBase>  기본 "" (비면 invalid → 전부 drop)
      <objFamilyAppendTags><tag>…</tag>…</objFamilyAppendTags>   기본 []
      <objTypeBase>HOST_NEW</objTypeBase>         기본 ""
      <objTypePrependTags><tag>scouter_obj_type_prefix</tag></objTypePrependTags>  기본 [scouter_obj_type_prefix]
      <objTypeAppendTags><tag>…</tag></objTypeAppendTags>        기본 []
      <objTypeIcon>linux</objTypeIcon>            기본 ""
      <objNameBase>LINUX_NEW</objNameBase>        기본 ""
      <objNameAppendTags><tag>…</tag></objNameAppendTags>        기본 []
      <hostTag>host</hostTag>                     기본 "host"
      <hostMappings><hostMapping><telegraf>h</telegraf><scouter>s</scouter></hostMapping></hostMappings>  기본 []
      <tagFilters><tagFilter><tag>cpu</tag><match>cpu-total</match><match>!cpu0</match></tagFilter></tagFilters>  기본 []
      <counterMappings>
        <counterMapping>                          TgCounterMapping (:33-55)
          <tgFieldName>usage_user</tgFieldName>   필수(map key)
          <counterName>cpu_user_$cpu$</counterName>   `$tag$`는 tag 값으로 치환
          <displayName>cpu_user_$cpu$</displayName>   비면 counterName
          <unit>%</unit>                          기본 ""
          <totalizable>false</totalizable>        기본 false
          <normalizeSeconds>30</normalizeSeconds> 기본 30; <4 → 0(정규화 없음), >60 → 60
          <deltaType>NONE</deltaType>             NONE|DELTA|BOTH (scouter.lang.DeltaType)
        </counterMapping>
      </counterMappings>
    </measurement>
  </measurements>
</TelegrafConfig>
```

`CounterProtocol.of(mapping)`(`CounterProtocol.java:51-65`)와 `setName/setDisplayName`(`:97-154`): `$tag$` 구간을 `*`로 바꾸고 tag 이름을 순서대로 기억한다. `generateTaggedName`(`:180-199`)은 `*`를 `tagMap.get(tag)`로 치환(없으면 `null`). delta 이름은 `taggedName + "_$delta"`, delta displayName도 같은 규칙, unit은 `unit + "/s"`(`:176-203`). `hasNormalCounter = deltaType != DELTA`, `hasDeltaCounter = DELTA || BOTH`(`:241-247`).

`getSampleContents()`(`TgConfig.java:90-652`)는 measurement 7개(`cpu`, `mem`, `redis`, `redis_keyspace`, `nginx`, `mysql`, `mysql_innodb`)를 담는다. 전역 `debugEnabled`는 **true**다. 요약:

| measurement | family/objType/objName base, icon | 특이 |
|---|---|---|
| cpu | HOST_METRIC / HOST_NEW / LINUX_NEW, linux | tagFilter `cpu=cpu-total`; usage_user/system/steal/iowait/nice → `cpu_*_$cpu$`, `%`, NONE, 30 |
| mem | HOST_METRIC / HOST_NEW / LINUX_NEW, linux | used_percent→`mem-used_percent`(%), free→`mem-free`(byte), total→`mem-total`(byte), NONE |
| redis | redis / redis / redis, redis | objName append `port`; keyspace_hits·used_memory NONE; expired_keys·evicted_keys(displayName 오타 `expired_keys`)·total_connections_received·total_commands_processed BOTH |
| redis_keyspace | redis / redis / redis, redis | objName append `port`; keys→`keys-$database$`, expires→`expires-$database$`, BOTH |
| nginx | nginx / nginx / nginx, nginx | objName append `port`; active/reading/writing/waiting NONE; accepts/handled/requests DELTA; unit 빈 문자열 |
| mysql | mysql / mysql / `""`, mysql | objName append `server`; connections·innodb_buffer_pool_pages_*·innodb_data_pending_*·innodb_row_lock_current_waits NONE; bytes_received/sent·com_*·innodb_data_reads/writes·innodb_row_lock_waits DELTA. `innodb_buffer_pool_pages_total`의 counterName/displayName이 `innodb_buffer_pool_pages_dirty`로 중복 정의됨(sample 결함) |
| mysql_innodb | mysql / mysql / mysql, mysql | objName append `server`; dml_deletes/updates/inserts/reads DELTA |

### 5.3 deprecated `scouter.conf` 키 (`Configure.java:444-547`, 파서 `:859-1037`)

| 키 | 기본 | 파싱 |
|---|---|---|
| `input_telegraf_config_file` | `./conf/scouter-telegraf.xml` | XML 경로 |
| `input_telegraf_enabled` / `_debug_enabled` | true / false | XML이 있으면 XML 값으로 덮어씀 |
| `input_telegraf_delta_counter_normalize_default` / `_seconds` | true / 30 | 동일 |
| `telegraf_object_deadtime_ms` | 35000 | 동일 |
| `input_telegraf_$<m>$_enabled` / `_debug_enabled` | true / false | `Boolean.parseBoolean` |
| `input_telegraf_$<m>$_tag_filter` | `""` | `tag:value,tag:!value` — `,` split 후 `:` split이 정확히 2개가 아니면 `CFG003` 로그·무시 |
| `input_telegraf_$<m>$_counter_mappings` | `""` | `,` split(빈 토큰 제거) 후 `:` split(빈 토큰 보존, `splitByWholeSeparatorPreserveAllTokens`): `{field}:{counter}:{display?}:{unit?}:{total?}:{normalizeSec?}`. field 접두 `&`=DELTA, `&&`=BOTH. display 비면 counter, unit 기본 `""`, total 기본 **true**(`Counter.java:32` 기본, XML은 false), normalizeSec는 **미지정 시 0**(XML 기본 30과 다름), 파싱 실패 0, 4 미만 0·60 초과 60. 토큰이 1개(`:` 없음)면 `split[1]` `ArrayIndexOutOfBoundsException`, field가 빈 문자열이거나 `&` 한 글자면 `charAt` 예외(D14) |
| `input_telegraf_$<m>$_objFamily_base` / `_objFamily_append_tags` | `""` / `""` | append는 `,` split |
| `input_telegraf_$<m>$_objType_base` / `_objType_prepend_tags` / `_objType_append_tags` / `_objType_icon` | `""` / `scouter_obj_type_prefix` / `""` / `""` | prepend가 비면 기본값으로 대체. measurement 첫 키를 만날 때 기본 prepend list를 `Arrays.asList(input_telegraf_$measurement$_objType_prepend_tags)`로 **분할 없이 1원소**로 넣으므로(`:880`), 리터럴 키 `input_telegraf_$measurement$_objType_prepend_tags=a,b`를 바꾸면 기본 prepend tag 이름이 `"a,b"` 하나가 된다. objType_base 미지정 시 `ScouterTgMtConfig` 기본 `"X$"`가 남는다(`ScouterTgMtConfig.java:45`) |
| `input_telegraf_$<m>$_objName_base` / `_objName_append_tags` | `""` / `""` | |
| `input_telegraf_$<m>$_host_tag` / `_host_mappings` | `host` / `""` | mappings `tgHost:scHost,...`, 2토큰 아닌 항목 무시 |

key 형식은 `input_telegraf_$` + measurement + `$_` + postfix이며 measurement 이름에 `$_`가 들어갈 수 없다(`:870-873`). `input_telegraf_$`로 시작하지만 `$_`가 없는 키(오타 등)는 `substring(0, -1)` `StringIndexOutOfBoundsException`으로 설정 적용 전체가 중단된다(D15). 알 수 없는 postfix는 measurement 객체만 만들고 무시한다. measurement 이름 `measurement` 자체도 보통 키처럼 파싱된다. deprecated에서도 `_objFamily_base`가 없으면 family base가 `"X$"`로 남아 `isValidConfig()` false → 해당 measurement 전부 drop이다.

## 6. 원본 결함·취약 동작 후보 (정상 계약과 분리)

| ID | 위치 | 현상 | 재현 |
|---|---|---|---|
| D1 | `InfluxSingleLine.java:192-209` | tag 없는 line(`m value=1 1700000000000000000`)은 measurement에 공백 이하가 포함되어 설정 lookup 실패 → 조용히 drop | tag 없는 줄 POST 후 counter 미저장 확인 |
| D2 | `:250-305`, `:93-94` | 문자열 field 값이 빈 문자열로 파싱되고, 그 field가 mapping에 있으면 `charAt(-1)` 예외 → 줄 전체 drop(stack trace만) | mapping된 field에 `"abc"` 전송 |
| D3 | `:332` | timestamp 누락/비숫자/ns 이외 단위 → `NumberFormatException` drop 또는 ms 오해석 | `precision=s`로 전송 시 delta 계산 분모가 0 또는 음수 |
| D4 | `ScouterTgMtConfig.java:237` | XML `<match></match>`/`<match/>`(빈 문자열)이면 `charAt(0)` `StringIndexOutOfBoundsException`이 servlet으로 전파 → 500, 같은 payload 나머지 줄 미처리. deprecated `tag:`는 `StringUtil.split`이 빈 토큰을 버려 2토큰이 아니므로 `CFG003`으로 걸러져 도달하지 않는다 | tagFilter에 빈 match 설정 |
| D5 | `InfluxSingleLine.java:435,439` | 같은 timestamp가 재전송되면 `(ts - prev.ts) == 0`으로 나눠 `Infinity/NaN` 저장(float 나눗셈) | 같은 줄 2회 POST(dedup은 payload 내에서만 적용) |
| D6 | `Configure.java:1072-1103`, `:569-574` | `hostMapping.telegraf`·`counterMapping.tgFieldName` 중복(`IllegalStateException`), `tagFilter`의 `<match>` 누락·`hostMapping` 값 누락(NPE), `counterName` 누락(NPE)이 reload로 전파. `run` 루프에 catch가 없어 **Configure thread 종료 → 이후 모든 설정 reload 정지**(기동 시면 서버 기동 실패) | 원본 소스 판독 |
| D7 | `TelegrafInputHandler.java:126-143` | 1001줄 이상 payload는 전체 폐기(로그는 "some line could be dropped") | Telegraf `metric_batch_size` > 1000 |
| D8 | `CounterHandler.java:127-131`, `IPUtil.toBytes` | `address` 옥텟 범위 초과 → `toBytes` null → `UnknownHostException`으로 배열 나머지 요소 중단; 비IPv4/호스트명/null → `0.0.0.0`(ObjectPack.address 문자열은 원문 유지) | `"address":"300.1.1.1"` |
| D9 | `HttpServer.java:133-145` | 원본은 `X-FORWARDED-FOR` 값을 검증하지 않고 ObjectPack.address로 쓴다(trim 없음). 세 servlet에는 인증·IP 제한이 없다. Go는 허용 IP 판정(켠 경우)을 TCP peer 또는 지정 header로 하고 `X-Forwarded-For`는 address 표시에만 쓴다([runtime](runtime.md), `TestAllowIPs`) | 원본 소스 판독 |
| D10 | `RegisterHandler.java:90` | family 이름 충돌 시 `type + "." + Math.random()*100` 비결정 이름 | 같은 이름 family 존재 상태에서 register |
| D11 | `RegisterServlet`/`CounterServlet` | body charset 미지정 시 ISO-8859-1 디코딩, JSON 비ASCII 깨짐 | `Content-Type: application/json` 없이 한글 전송 |
| D12 | `InfluxSingleLine.java:260-272` | quoted string field 안의 `\"`에서 quote가 닫혀 뒤 문자가 field 값으로 들어가고, 이어지는 공백에서 timestamp 구간으로 넘어가 줄 drop | `m,host=h s="a\"b c",v=1 1` |
| D13 | `Configure.java:602-619`, `:830-836` | XML 적용 후 `scouter.conf`만 바뀌면 전역 telegraf 값이 scouter.conf 값으로 되돌아가고 XML은 재적용되지 않음. deprecated measurement 키 변경도 XML mtime이 바뀔 때까지 미반영 | XML `enabled=false` + scouter.conf `input_telegraf_enabled` 미지정 상태에서 scouter.conf 다른 키 수정 → 수집 재개 확인 |
| D14 | `Configure.java:925-949` | deprecated `counter_mappings` 토큰에 `:`가 없거나 field가 빈 문자열/`&` 한 글자면 AIOOBE/SIOOBE → 설정 적용 중단(D6과 같은 thread 종료) | 원본 소스 판독 |
| D15 | `Configure.java:870-874` | `input_telegraf_$`로 시작하고 `$_`가 없는 키 → `substring(0,-1)` 예외 → 설정 적용 중단 | 원본 소스 판독 |
| Q1 | `MeterCounter.java:32`, `MeteringUtil.search` | normalizeSeconds N의 실제 평균 창은 2N초(최대 60초) | 값 계단 변화 후 수렴 시간 측정 |
| Q2 | `TelegrafInputHandler.java:233`, `CounterManager.safelyAddFamily` | 같은 family를 공유하는 measurement 등록이 family counter 목록을 통째 교체 → 이후 addCounter0가 다시 추가하며 `counters.site.xml`이 반복 저장되고 Client에 REFETCH가 여러 번 간다 | sample cpu→mem 순서로 첫 수신 |
| Q3 | `TelegrafInputHandler.java:163-170` | 새 objType/새 counter가 포함된 줄은 저장되지 않는다(등록만). 첫 1~2 주기 데이터 유실 | 신규 measurement 첫 전송 |
| Q4 | `InfluxSingleLine.java:137` | payload 내 같은 series 여러 timestamp 중 마지막만 처리(dedup) | 한 payload에 10초 간격 2줄 |
| Q5 | `toPerfCounterPack :401` | PerfCounterPack.time은 서버 수신 시각. Telegraf가 지연 전송하면 저장 시각이 line timestamp와 어긋난다 | `flush_interval` 크게 설정 |
| Q6 | `TelegrafInputHandler.java:53-55`, `RequestQueue.put` | 등록 queue(objType 1024, counter 1024, notify 10) 포화 시 로그·계수 없이 drop. 등록 thread는 항목마다 100ms sleep이라 초당 최대 약 10건만 처리 → 신규 counter가 많은 measurement는 등록 완료까지 해당 줄들이 계속 미저장 | 신규 tag 값이 많은 counterName(`$tag$`) 첫 수신 |

## 7. Go 구현과 fixture 범위

§1–§6은 원본 계약 조사다. Go 구현은 `internal/httpapi`(servlet 3종, `--scouter-http`)와 `internal/telegraf`에 있으며 범위·의도된 차이는 [httpapi CONTRACT](../../internal/httpapi/CONTRACT.md), [telegraf CONTRACT](../../internal/telegraf/CONTRACT.md)를 따른다. 과거 Java golden 대조 결과는 [검증 기록](../validation/2026-10-16-management-gaps.md) §9에 있다. 아래는 구현에 사용한 fixture 범위와 채택된 동작이며 이번 문서 정리에서 재실행한 결과는 아니다:

1. 부트스트랩: `net_http_server_enabled=false` + XML `enabled=true`이면 Java처럼 `0.0.0.0:<net_http_port>`에 listener를 열고 기동 로그를 남긴다. 세 servlet의 IP 검사는 원본처럼 기본 off이며 `--scouter-http-check-allow-ips` 또는 `--scouter-http-allow-ips`로 켠다. `--http`와 servlet listener가 같은 port에서 주소까지 겹치면 bind 전에 오류로 거절한다(`cmd/scouter-go/main.go` `listenerConflict`). 서로 다른 hostname의 DNS 해석은 이 사전 검사에 포함하지 않는다.
2. `/counter` fixture: `CounterHandler.java:43-73` 예시 → ObjectPack 2개 + PerfCounterPack 2개(FloatValue, REALTIME, objName `/VM123/my_server1`, objHash=`hash("/VM123/my_server1")`), address `10.10.10.10`. Java oracle로 objHash와 `DataOutputX.writePack` 바이트를 생성해 Go `protocol` 디코딩과 비교한다. 경계: `name` 누락(`/VM123`), `host` null(`/null`), 비Number value(중단), 배열 중간 요소 오류 시 앞 요소만 반영, 응답 항상 200 빈 본문.
3. `/register` fixture: 신규 type → `counters.site.xml`에 `<family name="redis" master="aof_rewrite_scheduled">` + counter 3개(total/all 기본 true, 예시의 `used_cpu_user` total=false) + `<objectType name="redis" family="redis" display="Redis" icon="" subobject="false"/>`; 201. 기존 type에 새 counter만 추가; `counters` 누락 → 500. `REFETCH_COUNTER_XML`이 로그인 session 모두에 enqueue되는지(CHECK_JOB 응답).
4. `/telegraf` parser 단위 fixture(line → measurement/tags/fields/timestamp):
   - `cpu,cpu=cpu-total,host=h1 usage_user=1.5,usage_system=2i 1700000000000000000` → tags 2, fields {usage_user:"1.5"(Float), usage_system:"2i"(Decimal 2)}, ts 1700000000000ms.
   - escape: `cpu,host=a\ b,path=/x\,y usage_user=1 1` → host `a b`, path `/x,y`.
   - quoted string field: `m,host=h s="a b",v=1 1` → s 빈 문자열, v Float 1; s가 mapping되면 줄 drop(D2).
   - boolean/unsigned/NaN skip, 음수 `-5i`, 지수 `1e3`.
   - D1/D3/D4 재현 line.
   - dedup key: `cpu,host=h1 usage_user=1 1`과 `cpu,host=h1 usage_user=2 2`는 같은 key(마지막 승).
5. 이름 결정 fixture: sample 설정 + tags `{host:h1, cpu:cpu-total}` → family `X$HOST_METRIC`, objType `HOST_NEW`(prefix 태그 `p` 있으면 `p_HOST_NEW`), objName `/h1/X$LINUX_NEW`, counter `cpu_user_cpu-total`; redis_keyspace `{host:h1, port:6379, database:db0}` → objName `/h1/X$redis_6379`, counters `keys-db0`, `keys-db0_$delta`(unit `ea/s`). hostMapping 치환, 없는 tag → `null` 문자열, `unknown` host.
6. delta/normalize fixture: 두 sample (v=100i@t, v=160i@t+10s) → 첫 sample delta 없음, 둘째 `6.0/s`; normalizeSec=0·default normalize 켜짐이면 MeterCounter 평균(단일 값이면 6.0). Go는 원본처럼 2초 bucket 31개에서 최근 최대 30개를 사용해 normalizeSeconds N을 2N초 창으로 처리한다(`convert.go` `meter`, `TestNormalCounterNormalization`).
7. 등록 부작용 fixture: 새 objType 첫 줄 drop → 등록 후 다음 줄 저장. `counters.site.xml` family 교체 semantics(Q2)와 Client 알림 횟수.
8. 한도: 1000줄/payload, register/addCounter queue 1024, prevAddedCounter 5초, CounterTimeCache 120초, MeterCounter map 10,000. Go는 각 상한과 포화 동작(명시적 거절/계수)을 정의한다.
9. 설정 fixture: `getSampleContents()` 원문을 JAXB(Java oracle)로 읽은 결과와 Go XML decoder 결과 비교(measurement 7개, `objTypePrependTags`, `tagFilters`, `deltaType`). 경계: 빈 wrapper(`<objTypePrependTags/>`), `<match/>`, match 누락, 중복 `tgFieldName`/`hostMapping`, 중복 `measurementName`, enum 밖 `deltaType`, 알 수 없는 요소. `telegraf.Load`·`Compose`는 읽기·파싱·적용 오류를 반환하면서 기본 TgConfig + deprecated measurements fallback을 제공한다(`TestLoad`). TCP 설정 저장은 별도 사전 검증으로 잘못된 XML을 거절한다(`internal/server/metadata.go` `ValidateTelegraf`).
10. deprecated 키 fixture: `counter_mappings`의 `&`/`&&`, 토큰 수 2~6, normalizeSec 미지정(0)·`3`(→0)·`90`(→60), total 미지정(true); `tag_filter`의 `tag:!v`, `tag:`(무시); `host_mappings` 2토큰 아닌 항목 무시; `$_` 없는 키(D15)는 Go에서 해당 키만 무시·경고.
11. 설정 우선순위: XML measurement > deprecated measurement(같은 이름이면 XML만), XML 전역 값이 scouter.conf 전역 값을 덮는다. Go `Compose`는 두 소스 중 하나가 바뀌어도 전체를 재합성해 D13(scouter.conf만 변경 시 되돌림)을 재현하지 않는다(`TestComposeRebuildsBothSources`, [config CONTRACT](../../internal/config/CONTRACT.md) §5.1).
12. 응답·오류 fixture: `/counter` 항상 200 빈 본문(파싱 실패 포함), `/register` 201/500, `/telegraf` 200(비활성·1001줄 초과 포함)과 D4/remote address 예외 시 500. method는 POST만(그 외 405).

## 8. 미조사·가정

- Jetty `GzipHandler` 기본 MIME 포함 목록과 `NCSARequestLog` 세부 필드 순서는 Jetty 9 버전 의존이라 원문으로 확인하지 않았다.
- `HashUtil.hash(String)`은 `getBytes()` 플랫폼 charset이다. 운영 JVM이 UTF-8이라고 가정한다(`file.encoding` 미확인).
- `AgentManager.active`의 Telegraf ObjectPack 처리(`updated` 카운트, address 변경 시 저장)는 UDP 경로와 동일하다고 보고 별도 추적하지 않았다.
- `PlugInManager.active`·Pulse 플러그인이 HTTP 유입 ObjectPack/PerfCounterPack에도 호출되는 점은 코드상 동일 경로이나 실제 실행으로 검증하지 않았다(미검증).
- JAXB RI의 빈 wrapper·enum 밖 값은 `TestJAXBOracle`의 대조 범위다. Jetty `GzipHandler`의 request body inflate 여부는 라이브러리 버전 의존이며 미검증이다.
- json-simple `JSONParser`가 trailing comma·중복 key를 어떻게 처리하는지(중복 key는 마지막 값으로 추정)는 라이브러리 원문을 확인하지 않았다.
- 원본 Java Server 전체를 실행한 재현 결과는 이 문서에 없다. 별도 클래스 oracle·golden의 과거 결과와 의도된 차이는 위 구현 계약·검증 기록을 따르며, 이를 전체 servlet 상호운용 검증으로 확대하지 않는다.

### 8.1 검증 이력

- 2026-10-15: 기존 초안의 줄 번호·동작을 원본(`dee47fb5`)과 대조. 수정: `/counter` 주소 오류는 NPE가 아니라 `UnknownHostException`(ObjectPack enqueue 전), D4 줄 번호(`:237`)와 deprecated 경로 비도달, `isNewOrChangedCounter` 줄 번호와 `fresh` 의미, `MeterCounter` 줄 번호·평균식. 추가: `/register` `containsKey` 의미, tag/field 파서의 빈 key·`\"` 처리(D12), `NumberValueWithTime`/`CounterTimeCache` 구조, 생성 pack 값 타입, Configure 예외 전파·갱신 순서(D13~D15), XML 필수·null 규칙, deprecated 파서 경계, queue 포화(Q6), fixture 9~12.

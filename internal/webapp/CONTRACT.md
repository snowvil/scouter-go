# internal/webapp 계약

정본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `scouter.webapp`(`scouterx/webapp/...`). endpoint 인벤토리와 원본 근거는 `docs/compatibility/webapp-api.md`를 따른다. 이 문서는 Go 구현의 구조, 고정 결정, 의도된 차이만 기록한다.

## 구조

- Java webapp처럼 collector의 **Client TCP 프로토콜**(magic `0xcafe2001`, `Text cmd + Int64 session + body`, `0x03` item / `0x04` 종료 / `0x44` INVALID_SESSION)로 접속한다. REST 계층은 "명령 ↔ JSON 변환"만 하고 조회 의미는 TCP handler(`internal/service`)가 보장한다.
- `client.go`: bounded pool. 열린 연결(사용 중 + idle)은 `PoolSize`(net_webapp_tcp_client_pool_size, 기본 12) 이하이며 슬롯이 없으면 요청 ctx가 끝날 때까지 기다린다. idle 연결은 `PoolIdleTimeout`(net_webapp_tcp_client_pool_timeout, 기본 8s = Java applyConfig 기본인 `net_tcp_client_so_timeout_ms` 8000ms) 이상 지났을 때(0 이하이면 항상 폐기, Java `lastUsed < now - (timeout - 1)`과 같음) 또는 collector가 이미 닫은 경우(1ms read probe) 폐기한다. 종료 플래그를 정상 수신한 연결만 pool로 돌아간다. idle pool에서 꺼낸 연결이 응답 0바이트 상태에서 EOF·reset·EPIPE를 만나면(서버 idle so-timeout과의 경합) 새 연결로 **1회** 재시도한다. 요청 write 자체가 실패한 경우는 모든 명령을 재시도하고, write 후 0바이트 EOF는 서버가 실행했을 수 있으므로 부작용 명령(`SET_`/`SAVE_`/`ADD_`/`EDIT_`/`DELETE_`/`REMOVE_`/`RESET_` 계열, `OBJECT_REMOVE_INACTIVE`, `OBJECT_THREAD_CONTROL`, `OBJECT_HEAPHISTO`)은 재시도하지 않는다. 새로 dial한 연결은 재시도하지 않는다(Java webapp에는 재시도가 없다 — 의도된 차이).
- LOGIN은 공유 session 하나(`LoginMgr`): `{id, pass=sha256(salt+pw), version, hostname, internal="true", ip}`. 첫 사용 시 lazy login. LOGIN은 한 번에 하나만 실행하고 `login.mu`를 I/O 동안 잡지 않으며, 진행 중인 LOGIN을 기다리는 호출은 자기 ctx 만료 시 즉시 반환한다. collector 거절/연결 실패 후 1초 동안은 같은 오류를 재사용한다(backoff). `INVALID_SESSION`을 받으면 session을 지우고 **즉시 1회 재로그인 후 재시도**한다(collector는 실행 전에 거절하므로 중복 실행 없음). Java는 5초 주기 observer가 재로그인하고 그 사이 요청은 500이다.
- 응답 item 종류는 명령별 표(`valueResponse`)로 정한다. Value 응답: `CHECK_LOGIN, GET_CUSTOM_KV, SET_CUSTOM_KV, SET_CUSTOM_TTL, GET_GLOBAL_KV, SET_GLOBAL_KV, SET_GLOBAL_TTL, VISITOR_REALTIME, VISITOR_REALTIME_TOTAL, VISITOR_REALTIME_GROUP, VISITOR_LOADDATE, VISITOR_LOADDATE_TOTAL`(원본 `getSingleValue` 호출처 전수). 나머지는 Pack.
- 상한: 응답 1건당 `MaxResponseItems`(1,000,000), `MaxResponseBytes`(64MiB), 요청 body `MaxBodyBytes`(4MiB), servlet session `MaxSessions`(10,000, LRU), bearer cache 10,000(UserTokenCache와 동일), text cache type별 원본 한도(service 20k 등, type 수 64), `GET_TEXT_ANY_TYPE` 1회 2,000건(codec collection 상한 4,096 미만), collector 대기 `CallTimeout`(30s, `net_webapp_tcp_client_so_timeout`). Java처럼 read마다 거는 시간 제한이라 응답 item을 받을 때마다 다시 잰다. 계속 도착하는 긴 응답(몇 시간 범위의 objType counter 조회)은 끝까지 받고, 응답이 멈춘 collector는 30초 뒤 실패한다. 요청 ctx가 끝나면 바로 끊는다(`TestCallTimeoutPerResponseItem`). 이전에는 명령 전체를 30초로 묶어 Java에서 되던 긴 조회가 실패했다. 응답 decode는 collection 2^18·값 2^19개까지 받는다(하루치 2초 counter point 43,200개, `TestCounterRangeLargeResponse`). pool 슬롯은 each 콜백이 panic해도 defer로 반환되고 그 연결은 닫힌다.
- **each 콜백 안에서 다른 collector 호출을 하지 않는다.** 슬롯을 쥔 채 중첩 호출하면 pool이 고갈될 수 있다. controller는 item을 모은 뒤 text/object를 조회한다.
- **live 설정(`settings.go`)**: Java webapp은 CORS·인증·allow IP·pool timeout을 요청마다 `ConfigureAdaptor`로 읽는다(`CorsFilter.java:43-44`, `AuthFilter.java:56-83`, `ZZ.java:41-42`, `ConnectionPool.java:49,56`). Go는 `Server.SetSettings(webapp.Settings)`(원자 교체, 사용하는 쪽 정의)로 다음 요청부터 반영한다. 한 요청의 CORS 판정과 AuthFilter 판정은 각각 하나의 Settings 값을 본다. `Config`의 같은 필드는 초기값이다. 키 대응: `CORSAllowOrigin`=`net_http_api_cors_allow_origin`, `CORSAllowCredentials`=`net_http_api_cors_allow_credentials`, `AuthIPEnabled`=`net_http_api_auth_ip_enabled`, `AllowIPs`=`net_http_api_allow_ips`(설정 때 exact/glob으로 한 번 parse), `AuthIPHeaderKey`=`net_http_api_auth_ip_header_key`, `AuthSessionEnabled`=`net_http_api_auth_session_enabled`, `AuthBearerEnabled`=`net_http_api_auth_bearer_token_enabled`, `PoolIdleTimeout`=`net_webapp_tcp_client_pool_timeout`(키가 없으면 소유자가 현재 `net_tcp_client_so_timeout_ms`를 넣는다). 인증이 켜진 상태에서 세 옵션이 모두 꺼지면 비 loopback listener에 경고를 다시 남긴다. `PoolSize`·`SessionTimeout`·`Gzip`은 Java도 기동 때만 읽어 재기동 값이다.
- `Config.Supported`(nil이면 모두 지원으로 간주)가 false인 명령은 TCP 호출 없이 `501 NOT_IMPLEMENTED`(`"This API is not yet implemented."`)로 응답한다. runtime wiring은 `service.SupportedCommands()`로 채운다. 지원 판정 없이 collector가 연결을 닫으면(Go 서버의 미지원 경로) 500이다.

## 공통 HTTP 계약

- context path `/scouter`, route는 `/scouter/v1/...`. `/scouter` 밖은 plain 404(webroot·`/extweb`·`/swagger` 미제공), `/scouter/swagger.json`·`/scouter/v0/temp/test/*`는 미등록 → JSON 404.
- 필터 순서: requestId(`"#" + base32(random 2000..31999) + 0부터 증가 counter`) → CORS(origin 공백이면 생략, `*`+credentials `true`면 요청 `Origin` 반사, OPTIONS는 헤더만 쓰고 200 빈 본문) → NoCache(`Cache-Control: no-cache`, `Expires: 0`, `Pragma: No-cache`) → gzip(GET/POST/PUT/DELETE, 1024B 이상, `Accept-Encoding: gzip`) → route 매칭(미매칭 404/405를 `HTTP 404 Not Found`/`HTTP 405 Method Not Allowed` CommonResultView로) → 인증 → handler.
- 응답 wrapper: `{"status","requestId","resultCode","message","result"}` 순서, null 유지, **정수·실수는 JSON 문자열**(Jackson `WRITE_NUMBERS_AS_STRINGS`), 실수는 Java `Float/Double.toString` 형식(`1.0`, `1.0E7`), boolean은 그대로. 실패는 `status=resultCode=ErrorState status`, VALIDATE_ERROR만 `"fail to validate input parameters. : " + 상세`.
- 인증(`AuthFilter`): 세 옵션 모두 false면 무인증. IP(allow exact + StrMatch glob, `net_http_api_auth_ip_header_key` 첫 `,` 토큰; Go는 원문과 `net.ParseIP` 정규화형(IPv6 축약/전개, IPv4-mapped→IPv4)을 모두 비교) 일치 시 즉시 통과 → bearer(`bearer ` 접두 대소문자 무시) → session(`JSESSIONID`) → IP 위반이면 401. IP 위반 + 유효 bearer는 통과, IP 위반 + 유효 session은 401(원본 비대칭 유지). @NoAuth는 ServeMux가 고른 pattern으로 판정한다: `POST /scouter/v1/user/login`, `POST /scouter/v1/user/loginGetToken`, `GET /scouter/v1/info/server`, `GET /scouter/s/{key}`. 세 옵션이 모두 false이고 `Config.ListenAddr`가 loopback이 아니면 `New`(그리고 그 상태로 바꾸는 `SetSettings`)가 경고를 기록한다.
- bearer: 토큰 형식은 `V1.<32 hex 대문자>.<hexa32(serverId)>.<userId>`다. 저장 위치·저장 형식·ttl·touch 주기는 원본 `UserTokenService`/`UserToken`과 같아 Java webapp이 발급한 토큰을 그대로 검증한다. 만료된 토큰은 401이다. Go는 저장 항목의 시각 값 범위를 추가로 검증한다. webapp 내부용 예약 keySpace `__WEB_SESSION__`·`___WEB_SESSION_PRIVATE___`(대소문자 무시)에 대한 REST custom KV 접근은 B(`ctl_b_kv.go` `bReservedKeySpace`)가 400 VALIDATE_ERROR로 거부한다.
- `serverId` query는 int 검증만 하고 단일 collector로 해석한다(원본 단일 서버 동작과 같음). serverId 값은 `HashUtil.hash(host + port)`(CRC32) of `CollectorAddr`.
- 시간: `yyyyMMddHHmmss`는 `Config.Location`(collector TZ)으로 해석하고, 날짜 분할은 `DateTimeHelper` BASE_TIME(2000-01-01 00:00 loc) 단위다. latest 계열 end는 `now + LOGIN clock delta`(`Server.getCurrentTime`).
- 결과 순서: `HashMap<Integer,...>.values()`(counter 과거/stat)와 `HashSet<String/Integer>`(counter 이름, objHashes)는 Java HashMap bucket 순서를 재현한다(tree bin 미모델). Java HashMap 순서가 원래 미정인 `tags`/`tagMap`/env는 wire 순서, `map[string]` 직렬화는 key 정렬.

## A endpoint (38)

User 2, Info 2, Object 8, HostObjectRequest 2, ActiveService 5, Configure 6, Alert 5, Counter 8. 501 가능(collector 미지원 시): `/object/remove/inactive`, `/object/remove/inactive/server`, `/activeService/control/thread/...`, `/configure/set/server`, `/configure/set/object/{objHash}`, `/configure/set/kv/server`, `/configure/set/kv/ofType/{objType}`, `/alert/scripting/{counterName}`(desc 2종 포함 4명령 중 하나라도 미지원이면 전체 501 — 빈 desc map으로 위장하지 않음), `/alert/set/config/scripting`, `/alert/set/rule/scripting`, `/alert/read/scripting`. mutation endpoint는 body 검증 전에 지원 여부를 먼저 확인한다.

## B endpoint (55)

XLog 5, XLogData 6, Profile 1, ProfileData 1, Summary 14, Visitor 7, Dictionary 1, GlobalKvStore 5, CustomKvStore 5, PrivateKvStore 5, Shorten 2, SimpleView 1(`/s/{key}`, @NoAuth), InteractionCounter 2(목록은 `ctl_b_misc_b_test.go` `bRoutes`, 등록은 `ctl_b_routes.go` `registerB`). 모두 local 명령이며 501 endpoint는 없다(collector가 명령을 지원하지 않으면 공통 규칙대로 501/500).

- Jersey가 literal 문자 수로 구분하는 template 중 Go 1.22 pattern이 충돌하는 곳은 dispatcher 하나로 등록한다: `/v1/xlog/{a}/{b}/{c}`(realTime > gxid), `/v1/xlog-data/{a}/{b}/{c}`(realTime > multi > gxid), `PUT /v1/kv/{a}/{b}`(`space/{keySpace}`는 custom store, `{key}/:ttl`은 global). Jersey가 `/v1/kv/space...`를 custom controller로 보내므로 global key `space`는 404다.
- 직렬화: `SXLog`/`XLogData`/Step(23종)/`SSpanStep`의 Jackson 속성 순서·이름(`xtype`, `xlogType` 등 getter-only 속성은 끝), byte[]→Base64, `opt`≠1이면 address null, Java `Float/Double.toString`, `HashMap`/`HashSet`/`Hashtable`(MapValue) 반복 순서는 `testdata/b_java_oracle.json`(고정 commit 원본 + jackson-databind 2.12.6.1, `testdata/b_oracle/run.sh`) golden과 일치한다.
- 상한: Summary는 하루 단위로 collector를 1회씩 부르므로 기간을 **366일**로 제한한다(초과 400, 원본 무제한). KV PUT body는 `Config.MaxBodyBytes`(기본 4MiB)를 따른다(초과 400). Profile Step 해석은 `MaxBytes` 4MiB(Go collector의 profile 상한), step 65,536개, 중첩 값 1,048,576개(`internal/pulse` StepLimits와 같은 기준)이며 초과 시 500으로 명시 실패한다(절단하지 않음).
- 메모리: B 응답은 모두 **본문 전체를 메모리에서 만든 뒤** 쓴다. 원본 `jsonStream`/`jsonArrayStream` endpoint(xlog realTime·pageable, xlog-data realTime·pageable, dictionary)도 collector 응답 전체를 모은 뒤 쓰므로, collector 오류가 첫 바이트 전에 나면 잘린 200 대신 오류 wrapper가 된다. 요청 1건의 최대 점유는 collector 응답(호출당 `MaxResponseItems`/`MaxResponseBytes`) + view 구조체 + JSON 본문(대략 wire 크기의 수 배)이다. 기준 크기: realTime 10,000 XLog, pageable `pageCount` ≤ 30,000, search·gxid·multi는 collector 응답 상한까지, Summary는 최대 366일치 item을 key별로 합산한 map, Profile은 위 Step 상한. peak 메모리는 동시 요청 수에 비례하며, runtime이 webapp handler 앞에 동시 64개 gate(빈자리 최대 3초 대기 뒤 503 + `Retry-After: 1`, `internal/server/http_admission.go`)를 둔다. collector 호출은 따로 pool 12로 제한된다. servlet 쪽(`internal/httpapi`)은 `MaxInFlight` 64를 쓴다.
- `/v1/xlog-data/realTime/{offset1}/{offset2}`: 원본은 webapp 프로세스의 40,000칸 `XLogLoopCache` offset(재시작 시 초기화, W10)이다. Go는 collector `TRANX_REAL_TIME_GROUP(_LATEST)`의 loop/index를 그대로 offset1/offset2로 노출하고(0/0이면 최신 10,000건), 사전 생성 대기(endTime > now-2000ms 보류)는 하지 않는다.
- custom KV(`/v1/kv/space/{keySpace}`)는 webapp 내부용 예약 keySpace 두 개(`__WEB_SESSION__`, `___WEB_SESSION_PRIVATE___`)를 400으로 거부한다(Go 추가 검증). private KV 미인증은 401(원본 NPE 500, W5), 응답 key 접두 제거는 원본처럼 길이로만 자른다.
- 원본 유지: `/v1/visitor/ofObject/{objHashes}`는 원본처럼 `sDate`/`eDate`를 보낸다(collector는 `startDate`/`endDate`를 읽으므로 원본도 실패, W6). `/v1/visitor/hourly/...`는 time/value ListValue를 `getText`/`getLong`으로 읽어 `{"time":null,"value":"0"}`이 된다(원본 결함 후보 WB1). Profile 응답 pack이 없으면 원본처럼 500 `"null"`.
- 그 밖의 차이: 목록 파라미터 숫자 오류·누락(원본 NPE/NumberFormatException 500)과 날짜 형식 오류는 400, Interaction의 미등록 객체 counter는 건너뜀(W3), KV bulk 응답 pack 누락은 `[]`, body의 알 수 없는 필드는 무시, dictionary는 type 안 순서를 `HashSet<String>` 순서로 고정(원본은 identity HashSet이라 미정).

## 의도된 차이(원본 결함 후보 포함)

| ID | 원본 | Go |
|---|---|---|
| W1 | bearer 형식 오류 → ArrayIndexOutOfBounds 500 | 401 SESSION_EXPIRED |
| W1b | `UserTokenCache`가 bearer의 userId와 cache 항목의 userId가 같은지 검증하지 않음 | cache 항목의 userId가 bearer userId와 다르면 401 |
| W2 | multi-server serverId 누락 ClassCastException 500 | 단일 collector라 발생하지 않음 |
| W3 | AgentModelThread 캐시 miss → SCounter/CounterView NPE 500 | miss 시 최대 초당 1회 즉시 재조회, 그래도 없으면 objName `"UNKNOWN"`(displayName은 미지 type 규칙: `""`, unit `null`) |
| W4 | config key를 regex로 컴파일, replacement의 `$`/`\` 해석 | key `QuoteMeta`, 값은 literal 치환 |
| W11 | KV에 userId 항목 없음 → NPE 500, 깨진 저장 항목 NPE | 401, 깨진 항목은 건너뜀 |
| W12 | `DateTimeParseException` 500 | 400 VALIDATE_ERROR |
| W14 | session 인증 시 미인증 요청마다 session 생성·`Set-Cookie` 후 401 | session을 만들지 않고 401 |
| W16 | 저장된 bearer 항목의 시각 값 범위를 검증하지 않음. webapp 내부용 예약 keySpace를 REST custom KV에서 구분하지 않음 | 범위를 벗어난 항목은 401, 예약 keySpace는 REST에서 400(B) |
| W17 | 로그인 성공 때 session id를 새로 발급하지 않음(요청이 제시한 기존 session 유지), 쿠키에 SameSite 없음 | 로그인 성공마다 새 session id 발급(제시된 session은 폐기), 쿠키 `SameSite=Lax` |
| W15 | `/configure/set/kv/server`가 `GET_CONFIGURE_SERVER`에 collector가 읽지 않는 빈 MapPack body를 보내 스트림이 어긋남 | body 없이 전송 |
| - | Jersey query/path 정수 변환 실패 404 | 400 VALIDATE_ERROR(A·B 공통 기준) |
| - | bean validation 메시지(`[ValidationException] <method>.argN.<field> may not be null`) | 같은 형식으로 생성(정확한 path 문자열은 미검증) |
| - | `/v1/info/server` `serverTime = now - delta`(부호 오류) | `now + delta`(collector 시각) |
| - | CORS 없는 OPTIONS → Jersey Allow + WADL | 200 + `Allow` 헤더, 본문 없음 |
| - | GET에 JSON 외 Content-Type → 415 | 검사하지 않음 |
| - | Jetty `JSESSIONID` 쿠키 | `JSESSIONID`, `Path=/`, `HttpOnly`, `SameSite=Lax`, 메모리 저장(재시작 시 소멸, Java와 동일) |
| - | `CounterEngine` family 미정의 object type → NPE | familyName `"UNKNOWN"`, counter-model `familyName:null` |
| - | WebSocket `/basic`, webroot, `/extweb`(W13), swagger, `/v0/temp/test/*` | 미제공 |

## 미검증

- Java webapp 실제 출력 golden 비교(필드 순서·Lombok boolean 속성명·HashMap 순서)는 실행하지 않았다. 순서는 원본 소스의 선언 순서와 HashMap 규칙에서 도출했다.
- Java DecimalFormat/Double.toString 경계값(지수 경계, 17자리 반올림)은 표본 테스트만 있다.
- 실제 Java Client/브라우저 상호운용, 운영 부하는 미검증이다.

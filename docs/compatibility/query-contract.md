# 기본 Client 조회 계약과 초기 구현

정본은 [baseline.json](baseline.json)의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5` (`2.21.3`)다. 아래 원본 경로/줄은 이 SHA 기준이다. [server-contract.md](server-contract.md)의 전체 229개 명령 중 초기 2C 관측 흐름을 구현하며, 나머지 명령은 명시적으로 실패한다. 전체 서버 대체 완료나 운영 상호운용 검증을 의미하지 않는다.

## 구현 경계

[`internal/service/backend.go`](../../internal/service/backend.go)의 `Backend`는 현재/날짜별 Object snapshot, dictionary, realtime/history/daily counter, realtime/history/txid XLog 및 순서가 유지된 raw Step profile chunk를 조회한다. handler는 ingest/storage 구체 타입에 의존하지 않는다. 반환 slice와 Pack/Value는 detached snapshot이어야 한다. `Text`의 found bool은 미존재와 존재하는 빈 문자열을 구분한다. `HistoryResult.LastBucketTime`은 paging index bucket이며 XLog의 EndTime이 아니다.

`New(Config, Backend)`는 Backend와 명시적 Username/Password를 요구한다. Password는 raw 설정이며 Java 기본 Client가 전송하는 salted SHA256 hex와 비교한다. 설정값/요청 pass를 로그나 응답에 기록하지 않는다. Config 기본값은 ServerID=1, Location=time.Local, Clock=time.Now, SessionTTL=3h, MaxSessions=1024다. runtime에서 원본 기본 계정을 선택하는 것은 별도 설정 결정이며 service는 묵시적 계정을 생성하지 않는다. CounterXML은 생성 시 복사한다. 원본 default XML embedding은 runtime 소유이며 빈 XML 설정에서 GET_XML_COUNTER를 성공시키지 않는다.

`Handlers()`는 `transport.TCPHandlers.Client/Authorize`를 연결한다. Agent dispatcher/connection pool은 runtime 소유다. 선택적 `AgentBackend.AgentRequest(ctx,objHash,command,MapPack)`가 있으면 OBJECT_ENV를 지원하고 원본 MapPack을 그대로 전달한다. 다른 관리/Agent 명령을 자동 활성화하지 않는다. [`SupportedCommands()`](../../internal/service/service.go)는 실제 dispatch 명령만 반환한다.

모든 Client 요청은 Text(command)+int64(session) 다음에 아래 표의 명령별 body를 사용한다. HasNEXT는 각각의 응답 item 앞에만 기록하고 마지막 NoNEXT는 transport가 기록한다. unsupported/잘못된 parameter/backend 오류를 빈 성공 응답으로 변환하지 않고 연결 오류로 반환한다. 인증 실패는 handler body 읽기 전에 InvalidSession flag와 연결 종료다. 이 framing은 `scouter.server/src/main/scala/scouter/server/netio/service/net/TcpServiceWorker.scala:110-129`와 Go transport 계약을 따른다.

## 인증과 Client 초기화

| wire 명령 | request body | response |
|---|---|---|
| LOGIN | MapPack: id/pass, 선택적 ip/hostname/version/internal | MapPack: 실패 session=0,error="login fail"; 성공 session,time,server_id(Text),type,version,client_version,email,timezone,policy(MapValue),menu(MapValue),so_time_out,ext_link_name,ext_link_url_pattern |
| CHECK_SESSION | MapPack: session | MapPack: session,validSession(Decimal 0/1), 실패 error="login fail" |
| SERVER_VERSION | MapPack (빈 Map도 body 필요) | MapPack: version |
| SERVER_TIME | 없음 | MapPack: time |
| SERVER_STATUS | 없음 | MapPack: used,total,time |
| GET_XML_COUNTER | 없음 | MapPack: default(BlobValue) |

원본 근거: `scouter.server/src/main/scala/scouter/server/netio/service/handle/LoginService.scala:42-86,113-123`, `scouter.server/src/main/scala/scouter/server/netio/service/handle/ServerInfo.scala:41-61,76-81`, `scouter.server/src/main/scala/scouter/server/netio/service/handle/ConfigureService.scala:135-145`. LOGIN/SERVER_VERSION/SERVER_TIME만 free command다 (`scouter.common/src/main/java/scouter/net/RequestCmd.java:334-343`). 그 외 명령은 valid session이 필요하며 transport의 원본 connection sessionOk latch를 유지한다. 따라서 session TTL은 새 연결 인증 및 CHECK_SESSION에 적용되고, 이미 인증된 연결의 매 명령마다 재인증하는 동작은 아니다.

고정 salt는 `scouter.common/src/main/java/scouter/util/CipherUtil.java:59-77`에 정의되며 SHA256(salt+password)의 lowercase hex다. GET_SERVER_ID 같은 salt 협상은 없다. Java는 기본 charset의 getBytes를 쓰고 Go는 UTF-8을 쓴다. UTF-8 Java 실행 환경/ASCII credential이 초기 검증 가정이다. `scouter.client/src/scouter/client/net/LoginMgr.java:34-46`의 secureLogin=false raw password 모드는 `--user/--password` 단일 credential 모드에서 지원하지 않는다. 계정 파일 모드(`--account-dir`)는 Java `AccountManager`처럼 받은 `pass` 문자열을 저장값과 그대로 비교한다([account CONTRACT](../../internal/account/CONTRACT.md)).

일반 session 3h keepalive와 internal 7d, loopback 특수 계정은 `scouter.server/src/main/scala/scouter/server/LoginManager.scala:26-85`에 있다. Go는 crypto/rand int64 nonzero session, mutex 보호, bounded session 수, 로그인 시 만료 항목 정리, 인증/CHECK_SESSION 시 TTL touch를 사용한다. **2026-10-16 갱신**: `--account-dir`(기본 `--conf` 디렉터리)이 있으면 Java `AccountManager`처럼 `account.xml`/`account_group.xml`을 읽어 복수 계정·group policy·`__local` loopback 특수 계정·internal 7d session·`account_use_ldap` LDAP bind를 지원한다(`internal/account/CONTRACT.md`, [management-commands](management-commands.md) §2). `--user/--password`를 명시하면 종전의 단일 credential 모드다. 어느 모드에서도 credentials 검증을 우회하지 않는다.

원본 LOGIN은 요청 Map을 수정하여 pass까지 응답한다. Go는 새 응답 Map을 만들어 id/session 및 필요한 metadata만 보낸다. 이는 의도한 credential 비반환 차이이며 byte-identical LOGIN 전체 응답을 주장하지 않는다. type은 계정의 group이며 policy는 `account_group.xml`의 해당 group 값이다(단일 credential 모드에서는 admin label에 미지원 policy false). Version은 개발 중 Go 서버임을 표시하며 전체 Scouter 지원을 광고하지 않는다. SERVER_STATUS used/total은 Go HeapAlloc/HeapSys bytes로 대응하고 JVM committed/used heap과 메모리 의미가 다르다. XML custom은 이후 `counters.site.xml`(`internal/counters`)로 지원하며 GET_XML_COUNTER의 `custom`에 실린다. JVM 환경 노출은 하지 않고 `SERVER_ENV`는 Go runtime 값을 보인다.

실제 Eclipse Client는 LOGIN 직후 GET_XML_COUNTER(null body)를 호출한다 (`scouter.client/src/scouter/client/net/LoginMgr.java:109-142`). 이 명령은 LOGIN body 뒤에 추가 빈 Map을 소비하지 않는다. SERVER_STATUS도 null body이며 Java Client의 주기적인 상태 갱신을 지원한다.

## Object와 dictionary

| 명령 | request MapPack 또는 body | response / 빈 결과 |
|---|---|---|
| OBJECT_LIST_REAL_TIME | 없음 | current ObjectPack 각각, inactive도 포함 |
| OBJECT_INFO | date(빈/없음=오늘),objHash | 날짜별 ObjectPack; 미존재는 item 없음 |
| OBJECT_LIST_LOAD_DATE | date | MapPack objType/objHash/objName parallel List; 빈 date는 item 없음 |
| OBJECT_ENV | objHash와 원본 추가 field | optional AgentBackend relay의 Pack stream; Agent 오류 전달 |
| GET_TEXT | date(없음=오늘),type,hash(List Decimal) | 하나의 MapPack: Hexa32(hash) → Text; missing hash list는 item 없음, lookup miss는 key 없음 |
| GET_TEXT_100 | 같은 request | MapPack 최대 100 unique key 단위; 결과 없으면 item 없음 |
| GET_TEXT_PACK | 같은 request | lookup hit마다 TextPack(type,hash,text) |
| GET_TEXT_ANY_TYPE | date,type(List Text),hash(List Decimal) | 같은 index의 type/hash 조회, hit마다 TextPack; 길이 불일치는 명시 오류 |

원본 근거: `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:52-74,83-117`, `scouter.server/src/main/scala/scouter/server/db/ObjectRD.scala:60-75`, `scouter.server/src/main/scala/scouter/server/netio/service/handle/HashText.scala:38-115,123-149`.

Hash는 signed int32다. 응답 key는 `scouter.common/src/main/java/scouter/util/Hexa32.java:38-49`: 0..9는 decimal, 10 이상은 x+base32, 음수는 z+abs(base32)다. 예: 10→xa, -32→z10, int32 min→z2000000. UTF-8 문자열 및 존재하는 빈 문자열을 보존한다. GET_TEXT의 명시적 빈 date는 원본처럼 빈 date로 Backend에 전달한다. 동일 hash 반복은 MapPack LinkedHashMap의 최초 위치와 마지막 값을 보존한다. daily/permanent dictionary fallback은 Backend 책임이다 (`scouter.server/src/main/scala/scouter/server/db/TextRD.scala:28-52`).

원본 OBJECT_INFO missing object는 HasNEXT 뒤 null Pack write 예외 후보가 있으며 별도 재현 전이다. Go의 no-item 처리는 의도한 차이다. 원본 OBJECT_LIST_REAL_TIME의 alive object `tags.counter` master counter 보강은 `scouter.server/src/main/scala/scouter/server/netio/service/handle/AgentInfo.scala:88-95`; backend의 current snapshot 보강 책임이며 dictionary XML engine 전체 지원과 별도다. handler에서 임의 master counter 이름을 추측하지 않는다.

## Counter

| 명령 | request MapPack | response |
|---|---|---|
| COUNTER_REAL_TIME | objHash,counter,timetype | cache hit의 **Value**, miss는 item 없음 |
| COUNTER_REAL_TIME_ALL | objType,counter | live object hit의 objHash/value parallel List MapPack |
| COUNTER_REAL_TIME_MULTI | objHash,counter(List Text) | counter/value parallel List MapPack, miss 제외 |
| COUNTER_PAST_TIME | objHash,counter,stime,etime | time/value List MapPack, 빈 series도 MapPack |
| COUNTER_PAST_TIME_ALL | counter,stime,etime,objHash(List) 또는 objType | 요청 object마다 objHash,time,value MapPack; 빈 series 포함 |
| COUNTER_TODAY | objHash,counter,timetype | 오늘 midnight+slot delta의 time/value List MapPack |
| COUNTER_PAST_DATE | date,objHash,counter | FIVE_MIN=3의 time/value List MapPack; Backend nil이면 item 없음 |

원본 근거: `scouter.server/src/main/scala/scouter/server/netio/service/handle/CounterService.scala:39-104,222-315`, `scouter.server/src/main/scala/scouter/server/netio/service/handle/DailyCounterService.scala:42-72,399-427`, `scouter.common/src/main/java/scouter/lang/CounterKey.java:115-120`. COUNTER_REAL_TIME의 timetype 누락은 0이며 자동 REALTIME=1 변환을 하지 않는다. ALL/MULTI는 원본처럼 realtime=1을 사용한다. ALL은 alive+objType 필터를 유지한다. PAST_TIME은 stime 날짜 한 개의 자료를 조회하며 날짜를 가로지르는 확장은 하지 않는다. Bulk history에서 명시 objHash 목록이 우선하며 결과는 입력 hash 순서다. 실제 자료 정렬, 중복 packet, NULL 및 5분 자동 표본 의미는 Backend 계약이다.

`scouter.common/src/main/java/scouter/lang/TimeTypeEnum.java:83-96`의 getTime은 1→2000ms,2→60000ms,3→300000ms,5→3600000ms,6→86400000ms다. TEN_MIN=4 상수는 있지만 getTime에 case가 없어 0이다. Go는 이 daily timetype를 malformed parameter로 거절하며 10분 지원을 추정하지 않는다. Java COUNTER_TODAY의 null slot은 NullValue다. Go Backend도 이 tagged null을 보존해야 하며 0으로 치환하지 않는다.

## XLog, cursor와 profile

| 명령 | request MapPack | response item 순서 |
|---|---|---|
| TRANX_REAL_TIME_GROUP | loop,index,limit,objHash(List 선택) | **첫 MapPack loop/index**, 이후 XLogPack |
| TRANX_REAL_TIME_GROUP_LATEST | loop,index,count,objHash(List 선택) | 첫 MapPack loop/index, 이후 XLogPack |
| TRANX_LOAD_TIME_GROUP | date,stime,etime,limit,max,reverse,objHash(List) | XLogPack만; 빈 objHash는 item 없음 |
| TRANX_LOAD_TIME_GROUP_V2 | date,stime,etime,lastBucketTime,txid,pageCount,objHash(List) | XLogPack 이후 **마지막 MapPack hasMore/lastXLogTime/lastTxid** |
| XLOG_READ_BY_TXID | date,txid,gxid(선택) | txid XLogPack 또는 item 없음 |
| TRANX_PROFILE | date,txid,gxid/xlogType(선택),max | legacy ProfilePack; Profile만 채움 |
| TRANX_PROFILE_FULL | 같은 Map, max 무시 | ordered raw Step **Blob** chunk마다 item |

원본 근거: `scouter.server/src/main/scala/scouter/server/netio/service/handle/XLogService.scala:41-97,148-162,171-248,259-392,413-447`, `scouter.common/src/main/java/scouter/lang/constants/ParamConstant.java:53-73`.

Realtime ring cursor는 다음 write 위치다. objHash 없음/빈 목록은 전체 object다. 필터로 결과가 없어도 최신 loop/index를 보낸다. realtime은 elapsed>=limit 또는 error이며 object 필터를 함께 적용한다. LATEST는 최근 raw count를 선택한 후 object filter이므로 count보다 적은 응답이 가능하다. ring rollover/overrun 정책은 `scouter.server/src/main/scala/scouter/server/core/cache/XLogLoopCache.scala:36-97,148-167,190-203`에 있으며 Backend 책임이다.

History V1은 elapsed>limit로 strict 비교하고 error 예외가 없다. time 범위는 inclusive이며 max/reverse를 유지한다. 기본 lower bound는 0이다. `xlog_realtime_lower_bound_ms`·`xlog_pasttime_lower_bound_ms`(limit = max(bound, limit))는 `service.Settings`로 받으며 서버 runtime이 scouter.conf reload마다 반영한다. V2에는 elapsed 필터가 없다. 초기 service는 LATEST count<=0, V2 pageCount<0을 parameter 오류로 거절한다. 빈 object list, pageCount=0, 한쪽만 zero인 lastBucketTime/txid는 false/0/0 metadata를 보낸다. 이전 cursor item은 제외하고 pageCount를 채운 뒤 추가 item 존재로 hasMore를 판정한다. cursor의 lastXLogTime은 500ms index bucket이다. bucket 내부 record 순서는 최신 append link부터다 (`scouter.server/src/main/scala/scouter/server/db/io/IndexTimeFile.scala:75-89,216-263`). XLog EndTime을 그대로 cursor로 쓰면 resume 의미가 달라진다.

Profile Backend는 저장된 chunk 순서를 유지한다. 일반 profile은 max>0일 때 **non-null chunk 수**로 제한하고 다음 chunk 직전에 StepControl(code=0,message=" ** Profile Truncated ** ")을 붙인다 (`scouter.server/src/main/scala/scouter/server/db/XLogProfileRD.scala:70-110`). max<=0은 전체다. FULL은 max를 무시하고 chunk를 개별 Blob으로 반환한다 (`scouter.server/src/main/scala/scouter/server/db/XLogProfileRD.scala:113-140`). Backend nil chunk list는 profile 미존재, empty non-nil list는 존재하는 빈 profile을 뜻한다. nil 개별 chunk는 쓰지 않는다. 단일 aggregate profile은 일반 chunk 누적 시 4MiB를 제한하며 최종 응답은 transport/codec byte budget도 적용한다.

ZIPKIN_SPAN=5 (`scouter.common/src/main/java/scouter/lang/pack/XLogTypes.java:3-10`) profile 합성, gxid parent fallback/caller rewrite와 XLOG_READ_BY_GXID는 초기에는 미지원이었고 이후 [trace-query.md](trace-query.md)에서 구현했다. profile만 빈/없음 date를 오늘로 보완하며 history/txid 조회는 날짜 누락을 오늘 자료로 바꾸지 않는다. normal txid hit는 지원하며 txid miss+gxid 요청을 빈 정상 결과로 숨기지 않는다. XType=4 BACK_THREAD2는 일반 profile이다. 실제 Client는 일반 profile을 ProfilePack으로, FULL을 Blob으로 읽는다 (`scouter.client/src/scouter/client/model/XLogProxy.java:43-86`).

날짜 계산은 [`internal/scoutertime/date.go`](../../internal/scoutertime/date.go)의 Date/ParseDate를 사용한다. 원본 `scouter.common/src/main/java/scouter/util/DateTimeHelper.java:44-87,101-108,131-135`는 2000-01-01 timezone offset을 기준으로 고정 86400000ms day를 계산하며 DST의 현지 자정 변경을 적용하지 않는다. 예를 들어 America/New_York 여름 04:30Z는 현지 날짜가 바뀌어도 Java 기준으로 전날에 속한다. Go는 2000..2099의 명시 날짜 범위만 지원하며 범위 밖의 원본 clamp/예외 동작을 그대로 재현하지 않고 parameter 오류로 거절한다.

## 검증과 남은 gate

`internal/service/service_test.go`는 synthetic credential/data만 사용하고 실제 loopback TCP framing으로 다음을 검사한다: LOGIN 성공/실패와 pass 비반환, session capacity/expiry/concurrent touch/InvalidSession, no-body 명령 연속 요청, dictionary signed hash 및 빈 text, counter Value/history, daily timezone/Null, realtime 첫 cursor/history 마지막 cursor, profile chunk/StepControl/FULL Blob, Agent relay field 보존/오류, unsupported/malformed 연결 종료. DST 고정 날짜와 B3 caller rewrite 미지원 오류도 검사한다. 아직 이 테스트의 Go codec 왕복을 Java oracle byte 검증이라고 부르지 않는다.

```sh
.tools/go/bin/go test ./internal/service -count=1
.tools/go/bin/go test -race ./internal/service -count=1
.tools/go/bin/go vet ./internal/service
```

실제 원본 Java TcpProxy/Host TcpWorker interop harness는 `scripts/java-integration.py`와 `tools/java-integration`에 구현했다. 기본 명령25종의 배열·empty·순서·cursor 및 수집→종료→동일DB 재시작 조회가 통과했다. [검증 결과](../validation/2026-10-13-core-server.md)는 synthetic 범위이며 실제 Eclipse GUI 화면, 운영 DB, session 만료 후 실제 Client 재연결과 shared-host 조회 병행 부하는 별도 gate다. 실제 Java DB reader/writer의 양방향 검증은 통신 harness와 독립적으로 실행했다.

Synthetic golden은 정본 DataOutputX로 LOGIN failure Map(credential 제외 의도 차이), Counter Value 및 list/map/null, realtime cursor-first/history cursor-last, ProfilePack 및 Blob chunks를 만들 수 있다. malformed list 타입, wrong body/no-body 혼합, cursor rollover/overrun, empty result, duplicate txid, retention 만료, backend error, 자정/DST, profile 크기 초과를 정상 fixture와 분리한다. 미지원 권한/명령, 7d internal session, raw login/LDAP/custom XML, Zipkin은 후속 계약 작업이며 초기 관측 명령이 이 기능들을 대체하지 않는다.

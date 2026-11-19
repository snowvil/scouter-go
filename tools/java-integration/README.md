# 실제 Java 통신 클래스 상호운용 검증

이 harness는 고정 원본의 Host Agent 송신·TCP worker와 Desktop Client 로그인·socket·query 클래스를 **수정하거나 복사하지 않고** 임시 directory에 직접 컴파일한다. 데이터는 synthetic이며 외부 서버·운영 Agent·운영 DB를 사용하지 않는다. 실제 Eclipse RCP 화면이나 Java instrumentation 전체를 실행하는 검증은 아니다.

```sh
python3 scripts/java-integration.py
python3 scripts/java-integration.py --server dist/scouter-go --report /tmp/scouter-interop-report.json
python3 scripts/java-integration.py --compile-only
```

기본 원본은 `../scouter`이며 `SCOUTER_SOURCE` 또는 `--source`로 지정한다. macOS에서는 설치된 JDK18을 우선 사용하며 `--java-home <JDK>`로 지정할 수 있다. `javac --release 11`을 사용한다. 원본 common resource도 수정 없이 임시 classes에 복사한다(`Version.getProperties()`가 `/scouter/v.properties`를 필요로 한다). `--server`를 생략하면 `.tools/go/bin/go` 또는 PATH의 Go로 `./cmd/scouter-go`를 임시 directory에 새로 빌드한다. 모든 server·classes·DB·Host config·로그는 임시 directory에 두며 기본적으로 제거한다. 실패 증거를 보존하려면 `--keep-temp`를 사용한다. Java/Go worker CPU는 2개로 제한한다.

## 원본 경로와 검증 경계

모든 경로는 Scouter 정본 repository 기준이다. baseline은 `docs/compatibility/baseline.json`의 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. script는 commit SHA, common/Host/Client source의 clean 상태, 컴파일할 각 source와 사용할 resource의 Git blob을 확인한다. `--report`는 각 원본 source/resource의 길이·SHA-256·Git blob, JDK version, harness checksum, server binary checksum과 실제 성공한 assertion을 기록한다.

| 경로 | 실행하는 실제 계약 |
|---|---|
| `scouter.agent.host/src/main/java/scouter/agent/netio/data/net/DataUdpAgent.java:112` | 실제 DatagramSocket 단일 CAFE 송신. `:137` CAFM 분할, `:183` CAFN 묶음 송신 |
| `scouter.agent.host/src/main/java/scouter/agent/netio/data/HostAgentDataProxy.java:58` | error TextPack. `:95` counter 묶음. `:115` ObjectPack heartbeat |
| `scouter.agent.host/src/main/java/scouter/agent/netio/data/net/TcpWorker.java:42` | 실제 socket 준비. `:63` TCP_AGENT V1 handshake와 server 명령 response |
| `scouter.agent.host/src/main/java/scouter/agent/netio/request/ReqestHandlingProxy.java:65` | 실제 annotation scanner/handler 등록. `:100` 실제 dispatch |
| `scouter.agent.host/src/main/java/scouter/agent/netio/request/handle/HostAgentEnv.java:35` | 실제 OBJECT_ENV 응답. 전체 환경을 출력하지 않고 synthetic marker만 확인 |
| `scouter.client/src/scouter/client/net/ClientTCP.java:58` | 실제 Socket connect. `:65` TCP_CLIENT handshake |
| `scouter.client/src/scouter/client/net/TcpProxy.java:142` | session·명령별 body·response flag·Pack/Value/Blob decoding. `:188` 실제 LOGIN 전송 |
| `scouter.client/src/scouter/client/net/LoginMgr.java:34` | 실제 salted SHA-256 로그인. `:110` counter XML parsing, `:131` GET_XML_COUNTER body 없음 |
| `scouter.client/src/scouter/client/server/ServerManager.java:74` | 실제 Server registry와 주기적 SERVER_STATUS body 없음 |

공통 Java source 213개, Host source 8개, Client source 11개를 명시적으로 전달한다. `-sourcepath ""`와 임시 classes만의 classpath로 implicit source lookup 및 외부 JAR를 배제한다. JDK18.0.2의 `--release 11` 컴파일 결과는 원본 source 232개, harness/adapter/factory 4개, class 389개다. 이 수치는 컴파일 증거이며 통합 성공과 별개다.

UI adapter는 `scouter.client.preferences.PManager` 하나다. 원본 PManager가 `IPreferenceStore`/`Activator`에 의존하므로 메모리 string preference store를 제공한다. 원본 `ServerPrefUtil`, `PreferenceConstants`, `Server`, `ServerManager`, `LoginMgr`, `ClientTCP`, `TcpProxy`는 그대로 컴파일한다. 빈 결과를 반환하는 원본 `DummyTcpProxy`가 사용되면 harness는 실패한다. 네트워크/login stub이나 synthetic 수동 Client socket은 없다.

## assertion

1. 임시 Go 서버의 ephemeral UDP/TCP loopback 주소를 ready JSON `{udp,tcp}`에서 읽는다. 실제 `LoginMgr`로 잘못된 비밀번호의 authentication 실패와 정상 로그인, nonzero session, timezone, host counter XML parsing, SERVER_STATUS를 확인한다.
2. 실제 `HostAgentDataProxy`/`DataUdpAgent`로 ObjectPack, error/service TextPack, realtime counter 2개, XLog, Profile V1/V2 두 chunk를 전송한다. 기존 8개 Pack 순서를 유지하고 empty error TextPack 1개, FIVE_MIN counter 1개, XLog 2개를 추가하여 총 12개 Pack/XLog 3개다. Profile 첫 chunk는 512-byte 설정을 넘겨 실제 CAFM 분할을 실행한다. XLog/Profile 내용은 Java instrumentation이 만든 trace가 아니라 harness의 synthetic common Pack이다.
3. 원본 `TcpProxy`로 realtime object/counter, dated OBJECT_INFO, GET_TEXT, COUNTER_PAST_TIME, XLOG_READ_BY_TXID, TRANX_PROFILE, TRANX_PROFILE_FULL을 조회한다. 원본 Step decoder와 byte equality로 Profile chunk 순서와 내용을 확인한다. 원본 `TcpProxy`가 예외를 내부에서 삼키더라도 socket 유효성과 필요한 결과를 assert하여 빈 성공을 금지한다.
4. 실제 Host `TcpWorker` V1 연결에 대해 Client OBJECT_ENV → Go agent pool → 원본 request handler round-trip을 수행하고 synthetic property marker를 확인한다. 시스템 environment/property 값을 로그나 fixture로 저장하지 않는다.
5. Go 서버에 SIGTERM을 보내 exit 0을 확인하고 같은 임시 DB로 재시작한다. 새 JVM의 원본 Client로 로그인하여 dated object/text/counter/XLog/profile history를 다시 확인한다. 재시작 단계에는 Agent 데이터를 재송신하지 않는다.

2C 추가 query assertion도 같은 원본 `TcpProxy`와 정본 `RequestCmd` 상수를 사용한다.

| 명령 | 배열·경계·순서 assertion |
|---|---|
| `OBJECT_INFO`, `OBJECT_LIST_LOAD_DATE` | explicit date와 default-today 조회, `objHash/objName/objType` 배열 길이 seed1/restart2(implicit Batch 포함) 및 같은 Host identity |
| `GET_TEXT_100` | daily error type의 `[missing,empty,error,error]` → MapPack 1개, 두 entry의 순서 `[empty,error]`; duplicate replacement, empty 보존, missing 생략 |
| `GET_TEXT_PACK`, `GET_TEXT_ANY_TYPE` | TextPack stream의 순서·type/hash zip·duplicate 보존, missing hash/잘못된 type 생략, empty string 보존. missing-only stream은 Pack 0개; `GET_TEXT`는 empty MapPack 1개 |
| `COUNTER_REAL_TIME_MULTI`, `COUNTER_REAL_TIME_ALL` | MULTI 요청 `[Cpu,missing,Cpu]` → `counter/value` 두 entry, 중복·순서 보존; ALL `objHash/value` 길이 1 및 25.25 |
| `COUNTER_REAL_TIME_OBJECT_ALL`, `COUNTER_REAL_TIME_OBJECT_TYPE_ALL`, `COUNTER_PAST_TIME_GROUP` | 원본 NetDataProcessor.scala:137-148가 수신 때 넣는 Decimal objHash/time과 원래 Float Cpu의 cache3개 field 및 과거 indexed row2개의 metadata. OBJECT_ALL의 counter/value 배열과 TYPE_ALL의 object-name/MapValue를 확인; PAST_TIME_GROUP은 original readBulk의 offset-zero minute gate 때문에 빈 배열 |
| `COUNTER_PAST_TIME`, `COUNTER_PAST_TIME_ALL` | `time/value` 두 sample `[t,t+2000]`/`[12.5,25.25]`; ALL explicit object 순서 보존, missing object도 빈 두 배열 유지; objType selector 비교 |
| `COUNTER_PAST_DATE`, `COUNTER_TODAY` | 실제 Host UDP `TimeTypeEnum.FIVE_MIN` Pack으로 저장한 288개 `time/value` 배열. Asia/Seoul 자정 + `i*300000`, 채운 한 bucket 37.5, 나머지 287개 NullValue |
| `TRANX_REAL_TIME_GROUP`, `TRANX_REAL_TIME_GROUP_LATEST` | metadata FIRST의 Decimal `loop=0/index=3`; GROUP limit200은 error가 있는 elapsed123의 XLog 1과 elapsed250의 XLog 3, cursor resume은 metadata만. LATEST count2는 XLog 2/3; gap=0인 동일 cursor도 원본처럼 최근 두 record 반환 |
| `TRANX_LOAD_TIME_GROUP`, `TRANX_LOAD_TIME_GROUP_V2` | 일반 history의 XLog 3개와 V2 page2+resume1의 byte/order 동일성. metadata LAST의 Boolean `hasMore`, Decimal `lastXLogTime/lastTxid`; cursor는 정확한 endTime이 아닌 500ms bucket. 끝 cursor 재개는 metadata만, 값 false/0/0 |

원본 계약 근거는 `AgentInfo.scala:52/65`, `HashText.scala:38/63/95/123`, `CounterService.scala:57/85/222/260`, `DailyCounterService.scala:42/399`, `XLogService.scala:171/214/314/385` 및 `scouter.common/src/main/java/scouter/lang/constants/ParamConstant.java:55/71`이다(Scala 경로는 `scouter.server/src/main/scala/scouter/server/netio/service/handle/` 기준). realtime gap=0의 최근 count 유지 의미는 `scouter.server/src/main/scala/scouter/server/core/cache/XLogLoopCache.scala:124`, 500ms paging bucket은 `scouter.server/src/main/scala/scouter/server/db/io/IndexTimeFile.scala:245`에 있다.

script는 종료 로그의 통계를 JSON report에도 기록한다. 기본28개 명령 검증은12Pack seed로 먼저 실행하며2D seed29Pack를 추가한 최종 통계는 ingest 완료41/XLog5, UDP Pack41이다. 추가 datagram 기대값은 원본 단일 write의 encoded payload 길이/MTU512와 ordered CAFN write당1개로 계산하여 seed 로그에 남기고 기존14개에 더해 assert한다. 재시작에는 ingest 완료·UDP 수신 모두 0이어야 한다. 두 종료에서 storage/parse/reject/handler/TCP 오류 0과 active connection 0을 확인한다. TCP 요청/accept 횟수는 원본 ServerManager의 주기 호출에 따라 변하므로 실제 값을 기록하고 고정 숫자로 assert하지 않는다.

query의 eventual poll은 최대 10초이며 command별 누락·잘못된 타입·내용·순서는 실패한다. JVM 90초, ready 20초, graceful shutdown 15초의 상한을 둔다. `--compile-only`는 report의 `integration_passed=false`와 미실행 이유를 남긴다.

## 남은 검증

- 전체 Eclipse RCP Client는 `META-INF/MANIFEST.MF:28`의 JavaSE-21 및 SWT/JFace/Eclipse bundle이 필요하다. 이 harness는 JDK11 bytecode로 빌드 가능한 원본 communicator subset만 사용한다. UI 동작을 증명하지 않는다.
- 전체 Host Agent의 OSHI counter sampling, native/OS 상태 수집은 실행하지 않는다. `scouter.agent.host/pom.xml:29`의 OSHI dependency와 별도 실행 환경이 필요하다.
- 실제 Java instrumentation Agent의 TCP_AGENT V2는 이 harness에서 실행하지 않는다. 기존 transport oracle의 synthetic V2 fixture 검증과 구분한다. Host V1 연결은 실제 worker와 handler로 검증한다.
- 고부하·UDP 손실/재정렬·복수 Agent·운영 인증·기존 운영 DB 이관·Pulse integration은 별도 gate다. synthetic local 수집/조회/재시작 성공을 전체 포팅 완료나 TPS 증거로 해석하지 않는다.
- GET_TEXT_100의 100-entry batch 경계와 realtime ring wrap은 작은 이번 데이터로 실행하지 않는다. 배열/metadata/type 및 cursor 재개 의미를 검증한다.
- empty string은 daily error dictionary로 검증한다. 원본 `TextPermWR.scala:71`은 영구 dictionary의 empty 저장을 생략하지만 `TextWR.scala:114`의 먼저 실행한 TextCache에는 남을 수 있다. 영구 service empty의 일시 cache 의미는 이번 영속 empty assertion과 구분한다.


## 2D 실제 Pack/query 확장

`ExtraInteropHarness`는 같은 실제 Host `DataUdpAgent`와 원본 Client `TcpProxy`를 사용한다. Java instrumentation/전체 RCP 실행 범위는 확대하지 않는다. 통합 source232개와 원본 resource6개는 그대로이며 synthetic Pack factory `tools/java-storage/ExtraDataset.java`를 함께 컴파일한다. 이 factory는 저장 oracle와 같은 입력을 제공하고 네트워크/handler를 대체하지 않는다.

기존12Pack 뒤29Pack(최종41Pack/XLog완료5)을 추가한다: Status1, Alert1, Interaction3, Stack2, Summary13(10종류와 EndUser3반복), Batch2, SpanContainer2/B3XLog2, pending childProfile/XLog 각1, Dropped1. Batch가 기존에 없는 `/interop/batch` object를 실제 수집 경로에서 등록하며 B3 records는 이 object를 사용하므로 기존 Host XLog cursor/조회 배열을 유지한다. MapPack와 standalone SpanPack은 정본 built-in 수집 route가 없으므로 성공 지원 대상으로 assert하지 않는다.

| 추가 명령/입력 | 검증 의미 |
|---|---|
| `COUNTER_REAL_TIME_GROUP`, `COUNTER_PAST_DATE_GROUP`, `COUNTER_TODAY_GROUP`, `OBJECT_TODAY_FULL_LIST` | 원본 요청 body/Pack 타입, realtime value25.25, daily288배열/1개37.5/287Null, Host+implicitBatch object와 status tag |
| `STATUS_AROUND_VALUE`, `ALERT_LOAD_TIME`, `ALERT_REAL_TIME` | 입력time1을 server receive clock으로 대체, content/tag 보존; Alert metadataFIRST loop0/index1, first=true는 metadata only |
| `INTR_COUNTER_REAL_TIME_BY_OBJ` | 동일 key의 최신 count2 replacement 및 순서. 과거 payload timestamp도 receive TTL 동안 존재; 10s TTL과5s cleanup 후 eventual empty |
| `GET_STACK_ANALYZER`, `GET_STACK_INDEX` | 실제 `StackPack.setStack` 압축을 원본 `getStack`으로 풀어 문자열/order 비교. index는 Pack으로 잘못 해석하지 않고 원본 `INetReader.readLong` primitive2개 |
| `BATCH_HISTORY_LIST`, `BATCH_HISTORY_DETAIL` | 실제 `BatchPack.readSimple` Blob decoder와 원본 Pack decoder 사용. simple의 detail nil, SQL detail1개, 물리 position0와4+첫Pack길이 |
| Summary10명령 | 실제 parallel lists 입력 APP/SQL/APICALL/IP/UA/ERROR/ALERT/NAV/AJAX/ENDUSER_ERROR. EndUser longID 반복의 마지막 row replacement, Error host=stacktrace/count omitted의 원본 결함 동작 포함 |
| `TRANX_PROFILE/FULL` xtype5 | original Step decoder로4Step hierarchy와 CLIENT child 제외/max 무시, time/elapsed wire0, FULL single identical Blob. shared-ID input은3Step/timestamp tie/URL/endpoint/annotation/debug 확인 |
| `XLOG_READ_BY_GXID`, `XLOG_LOAD_BY_GXID`, `XLOG_LOAD_BY_TXIDS`, `QUICKSEARCH_XLOG_LIST`, `SEARCH_XLOG_LIST`, `XLOG_READ_BY_TXID` | GXID/date selector, txid input order/miss omission/duplicate 유지, Quicksearch 중복, chronological search, missing nested Span의 stored ancestor XLog fallback |
| pending Profile2/XLog→Dropped driver | child history/profile 없음, 완료XLog수 증가 없음. enqueue/Completed Pack수와 완료XLog5를 별도 assert |

저장 oracle의 실제 원본 SpanStepBuilder/Counter/Summary handler byte 검증과 이 socket 통합은 별도 gate다. 추가 Pack factory로 보내는 데이터는 전체 instrumentation의 실제 trace 생성 결과가 아닌 synthetic common Pack이다. `--compile-only` 성공은 위 조회/재시작 실행 성공을 뜻하지 않는다. 총 고유 명령56개(원본 LoginMgr 내부 LOGIN/XML 포함)를 호출하며 empty 성공 대신 response 타입/배열/fields/metadata를 확인한다.

`REALTIME_SERVICE_GROUP`은 기존 Host WEB XLog3개로 group`/**`, count3, error1, Float elapsed413/3을 확인한다. startup1초/cache1초를 bounded eventual query로 기다리며 missing objHash list는 응답0개다.


순서 검증에 의존하는 Stack/Batch/Interaction replacement/EndUser repeated Summary/pending-child→Dropped는 원본 `DataUdpAgent.write(List<byte[]>)` CAFN API로 묶어 한 datagram 내 순서를 고정한다. 서로 다른 datagram의 socket 송신 순서를 서버 worker의 처리 순서로 가정하지 않는다. 기존 Profile 첫 chunk의 CAFM 분할은 유지하며 원본 Client 조회로 첫 chunk의 영속 완료를 확인한 뒤 V2 chunk를 보낸다. service dictionary와 realtime cursor용 XLog에도 원본 query readiness barrier를 둔다. 이는 UDP 재정렬/손실 복구 보장이 아닌 synthetic fixture의 확정적인 선행 조건이다.

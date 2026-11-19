# Packet·Summary 조회 계약

정본은 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `scouter.server/src/main/scala/scouter/server/netio/service/handle`이다. 이 단위는 아래 23개 명령의 dispatcher와 조회 변환을 구현한다. `PacketCommands()`가 등록 목록을 반환하고 `Service.HandlePacketCommand(ctx, request, stream)`이 `(recognized, error)`를 반환한다. 기존 dispatcher가 request MapPack을 읽기 전에 호출한다. 모든 요청은 MapPack이며 항목 앞 HasNEXT는 handler, 성공 종료 NoNEXT는 transport가 작성한다.

Backend는 `packet_backend.go`의 optional interfaces를 사용한다. 미제공 기능은 `ErrUnsupported`, 조회 실패는 실제 error를 반환한다. `RealtimeGroupBackend.RealtimeServiceGroup(ctx, []int32)`은 완성된 MapPack 또는 빈 결과(nil)를 반환한다. 결과는 Backend 소유 객체와 분리한 snapshot이며 Agent 응답에 objHash를 넣을 때도 원본 MapPack을 수정하지 않는다.

## 명령과 framing

`P`는 MapPack, `[]`는 ListValue다. 시간 조회 순서는 Backend가 원본 index/cache 순서로 제공한다. 조회 범위의 실제 날짜·index 선택은 storage가 담당한다.

| 명령 | request | response와 원본 근거 |
|---|---|---|
| REALTIME_SERVICE_GROUP | P{objHash[]} | empty set이면 없음; 그 외 P{name[],count[],elapsed[],error[]}. GroupService:41, XLogGroupPerf의 합산/순서는 Backend 책임 |
| STATUS_AROUND_VALUE | P{objHash,key,time} | time에서 1분 앞으로 조회하여 첫 일치 StatusPack만. StatusService:30 |
| ALERT_REAL_TIME | P{objType,loop,index,first} | cursor P{loop,index} 먼저, first=true이면 끝; 아니면 AlertPack 순서. empty objType은 전체. AlertService:47 |
| ALERT_LOAD_TIME | P{date,stime,etime,count,level,object,key} | AlertPack; level 정확 일치, object는 날짜별 objName substring, key는 title/message substring. null 필터 생략, 빈 문자열 필터 활성. count 1..1000 외 500; 원본의 count+1개 동작 보존. AlertService:76/108 |
| ALERT_TITLE_COUNT | P{date,stime,etime} | title별 P{title,level,count:MapValue{hhmm→count}}. 첫 level(signed byte), 같은 분의 뒤 count는 덮어쓰기. AlertService:135 |
| GET_STACK_ANALYZER | P{objName,from,to 또는 date,hour} | StackPack raw body; valid from>0/to>from 우선, 아니면 날짜+hour의 1시간. StackAnalyzerService:32 |
| GET_STACK_INDEX | 위와 동일 | **Pack이 아닌 int64 timestamp 8 bytes**. StackAnalyzerService:59 |
| BATCH_HISTORY_LIST | P{objHash,from,to,filter,response} | 현재 Object 없거나 잘못된 범위면 없음. job substring, elapsed 최솟값. **Pack type 없는 writeSimple Blob**. BatchService:44, BatchDB:73/105 |
| BATCH_HISTORY_DETAIL | P{objHash,startTime,elapsedTime,position} | 현재 objName, startTime+elapsedTime의 날짜, physical position으로 BatchPack 1개 또는 없음. BatchService:68 |
| BATCH_HISTORY_STACK | P{objHash,time,filename} | Blob chunk stream. BatchArchiveBackend가 있어야 한다. 미제공 시 명시적 unsupported. BatchService:87 |
| OBJECT_BATCH_ACTIVE_LIST | P{objHash 또는 objType} | 명시 hash 또는 해당 type의 live Agent별 MapPack; unavailable Agent도 P{objHash} 1개. BatchService:100 |
| BATCH_ACTIVE_STACK | P{objHash,…} | Agent MapPack 1개, unavailable이면 없음. BatchService:140 |
| INTR_COUNTER_REAL_TIME_BY_OBJ | P{objType 또는 objHash[]} | **InteractionPerfCounterPack** stream. 명시 hashes가 live type selector를 덮어쓰며 순서·중복 보존. missing selector면 없음. InteractionCounterService:43 |
| LOAD_SERVICE_SUMMARY | P{date,stime,etime,objType,objHash} | P{id[],count[],error[],elapsed[],cpu[],mem[]}. SummaryService:61/323 |
| LOAD_SQL_SUMMARY | 위와 동일 | P{id[],count[],error[],elapsed[]}. SummaryService:61/327 |
| LOAD_APICALL_SUMMARY | 위와 동일 | P{id[],count[],error[],elapsed[]}. SummaryService:61/331 |
| LOAD_IP_SUMMARY | 위와 동일 | P{id[],count[]}. SummaryService:132/335 |
| LOAD_UA_SUMMARY | 위와 동일 | P{id[],count[]}. SummaryService:132/339 |
| LOAD_SERVICE_ERROR_SUMMARY | 위와 동일 | P{id[],error[],service[],message[],count[],txid[],sql[],apicall[],fullstack[]}. SummaryService:179/343 |
| LOAD_ALERT_SUMMARY | 위와 동일 | P{id[],title[],count[],level[]}. title별 CRC32 id, 첫 signed byte level. SummaryService:269/347 |
| LOAD_ENDUSER_NAV_SUMMARY | P{date,stime,etime,objType} | P{id[],uri[],ip[],count[],18 navigation timing lists}. SummaryEndUserNav:61 |
| LOAD_ENDUSER_AJAX_SUMMARY | 위와 동일 | P{id[],uri[],ip[],count[],duration[],userAgent[]}. SummaryEndUserAjax:44 |
| LOAD_ENDUSER_ERROR_SUMMARY | 위와 동일 | P{id[],host[],stacktrace[],userAgent[],uri[],message[],file[],lineNumber[],columnNumber[]}. **count 없음**. SummaryEndUserError:48 |

PSTACK_ON은 상태를 바꾸는 관리 명령으로 이 단위에 포함하지 않는다. BATCH_HISTORY_STACK은 Engine의 BatchArchiveBackend와 storage 조회에 연결되었으며, Runtime의 TCP_SEND_STACK handler도 PutBatchArchiveContext에 연결되었다. 실제 zip 경로·파일 검증 및 저장 형식의 최종 검증은 storage 단위에서 진행한다. Optional interface를 제공하지 않는 별도 Backend에서는 명시적 unsupported를 유지한다.

## Summary 집계 의미

APP/SQL/APICALL만 null·빈 objType을 모두 전체로 취급한다. 나머지는 null만 전체이며 빈 문자열은 정확한 type selector다. 기본 Summary는 objHash=0이면 전체, EndUser는 원본처럼 objHash를 사용하지 않는다. 빈 결과도 원본 column 순서의 빈 lists를 갖는 MapPack 1개다.

처음 등장한 id 순서와 linked map의 FIFO eviction을 보존한다. 기본/EndUser 최대 50000 groups, service error 최대 10000 groups다. count/error는 Java int32 overflow, elapsed/cpu/mem/timing은 int64 합산을 사용한다. service error는 첫 error/service/txid, 첫 nonzero message/sql/apicall/fullstack을 유지한다. Alert는 title별 합계와 첫 level이다. navigation timing field 순서는 `navigationStart, unloadEventStart, unloadEventEnd, fetchStart, domainLookupStart, domainLookupEnd, connectStart, connectEnd, requestStart, responseStart, responseEnd, domLoading, domInteractive, domContentLoadedEventStart, domContentLoadedEventEnd, domComplete, loadEventStart, loadEventEnd`다.

실제 원본 handler 실행으로 확인한 EndUser 결함도 보존한다. lookup은 id.getInt이고 insert는 id.getLong이므로 32-bit 범위를 넘는 id가 합산되지 않고 기존 항목을 대체할 수 있다. ERROR의 host는 host list가 아닌 stacktrace에서 읽으며 count를 출력하지 않는다. 이를 정상 집계로 수정하여 호환이라고 주장하지 않는다.

ALERT_TITLE_COUNT의 title 열거는 Java HashMap의 일반 bucket 순서를 재현하지만 collision tree의 JDK별 순서까지 보장하지 않는다. 내부 count MapValue는 unordered map 의미를 보존하며 Java Hashtable의 byte 열거 순서와 동일하다는 검증은 아직 없다. HashUtil title CRC32는 원본 JVM default charset이 UTF-8인 환경을 전제로 한다.

## 상한·취소·차이

원본 RD handler처럼 저장소를 읽는 대로 처리하는 조회에는 **읽은 양의 상한이 없다**. Summary(LOAD_*_SUMMARY, ALERT_TITLE_COUNT), ALERT_LOAD_TIME, DEBUG_SERVER의 alert 수, XLog 과거 조회(TRANX_LOAD_TIME_GROUP/_V2, SEARCH_XLOG_LIST)는 storage가 한 batch(512건·4 MiB, 읽기 16 MiB)씩 store gate 안에서 읽고 gate 밖에서 handler에 넘긴다. 객체·elapsed filter와 max/pageCount/count는 scan 안에서 적용하며 max에 닿으면 읽기를 멈춘다. 상한은 Go가 들고 있는 양에만 둔다: 집계 group은 원본 값(50000/10000, FIFO eviction), ALERT_TITLE_COUNT의 (title, 분) cell 100000, slice를 반환하는 나머지 Backend 결과의 records/rows/chunks와 selector 100000. 이전의 "filter 전 구간 전체 100000건·64 MiB" 상한은 제거했다. Client 응답에는 명령당 누적 상한이 없고 응답 item 하나(encode 결과)가 `transport.MaxResponseValueBytes`(256 MiB)+1 MiB를 넘을 때만 ErrLimit이다. 명령 총 시간 상한도 없으며 Stream 진전이 `RequestTimeout`(기본 30초) 동안 없을 때만 끝난다(transport CONTRACT §2). 모든 Backend 호출과 긴 조회 루프에 request context를 전달/확인한다. Backend는 자체 lock/I/O 경계에서 취소를 처리해야 하며 동기 disk syscall 강제 중단은 보장하지 않는다.

잘못된 날짜·역전 범위·Summary 평행 list 길이 오류는 명시적 malformed로 처리한다. 원본 RD가 callback 예외를 잡고 partial 집계를 반환하는 일부 malformed DB 동작과 차이가 있다. TCP handler 실패 시 이미 작성된 부분 응답은 원본처럼 flush하고 성공 NoNEXT를 붙이지 않는다. 이 동작은 transport의 회귀 race 테스트로 검증되었다.

현재 GET_STACK_INDEX는 StackBackend가 반환한 StackPack.Time을 사용한다. 원본은 index만 읽을 수 있어 data block 손상 시 동작과 I/O 비용이 다를 수 있다. 저장 writer가 timestamp와 Pack.Time을 일치시키는 정상 데이터가 검증 범위다.

## 검증

Go loopback은 raw int64/simple Blob framing, forward first match, Alert cursor/count+1, minute overwrite, signed byte/int32 overflow, missing/empty selector, duplicate hashes, Batch detail/relay ownership, backend error·취소·권한, 열별 empty lists, malformed parallel lists를 확인한다.

`TestSummaryOriginalScalaRawBodies`는 `python3 scripts/java-storage.py --generate-counter-handlers DIR`으로 **원본 Scala SummaryService/EndUser class**를 실행한다. RD 입력 seam만 synthetic data를 공급한다. APP/AJAX/NAV/ERROR 4개 raw response body를 실제 Go TCP 응답과 비교했다. 나머지 19개 명령 전체가 actual handler byte 검증되었다는 뜻이 아니다. 실제 Agent/Client session 및 공용 서버의 운영 부하 검증은 별도 통합 gate다.

```sh
GOMAXPROCS=2 .tools/go/bin/go test ./internal/service -run 'TestPacket|TestSummary' -count=1
GOMAXPROCS=2 .tools/go/bin/go test -race ./internal/service -run 'TestPacket|TestSummary|TestExtendedCounterOriginalScalaTodayWireOracle' -count=1
```

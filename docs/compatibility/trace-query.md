# XLog·B3·Zipkin trace 조회 확장

정본: Scouter 2.21.3, commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5` ([baseline.json](baseline.json)). 원본은 읽기 전용이며 아래 경로/줄은 이 SHA 기준이다. 이 문서는 [query-contract.md](query-contract.md)의 초기 normal txid/profile 지원 이후 확장을 설명한다. 실제 운영/전체 Eclipse GUI/고부하 검증을 의미하지 않는다.

## 구현과 연결

[`TraceBackend`](../../internal/service/trace_backend.go)는 세 개의 optional query를 제공한다: `XLogsByGxID(ctx,date,gxid)`, `SpansByGxID(ctx,date,gxid)`, `XLogsByTime(ctx,date,from,to)`. 반환 데이터는 detached snapshot이다. GXID는 signed int64이며 UUID로 바꾸지 않는다. GXID 결과는 index 최신 append link 순서, Span 결과는 그 순서의 Container를 각 Blob wire 순서로 펼친 목록, time 결과는 elapsed=0도 포함한 raw chronological 목록이다.

[`HandleTraceCommand`](../../internal/service/trace.go)는 새 5개 selector를 처리하며 authenticated request만 허용한다. 5개 명령은 service dispatcher에 등록돼 있고(`service.go`의 명령 목록, `query.go`의 분기) Engine이 TraceBackend query를 store로 전달한다. 기존 [`xlog.go`](../../internal/service/xlog.go)는 같은 TraceBackend로 normal txid의 B3 caller 보정, gxid fallback 및 Zipkin profile 합성을 처리한다. Backend가 TraceBackend를 제공하지 않는 기능은 ErrUnsupported다. 명령 등록·Agent 수집·sampling preCore/Dropped 처리는 dispatcher·ingest 소유이며 이 파일들에서 임의로 구현하지 않는다.

`Config.SearchXLogMaxCount`는 원본 `req_search_xlog_max_count`에 대응한다. 기본값 500, 0은 기본 선택, 음수는 생성 오류다. backend/storage 오류, resource limit, 잘못된 Pack/parameter를 빈 성공으로 감추지 않는다. 정상 미존재는 각 명령의 no-item 계약을 따른다. transport가 최종 NoNEXT를 기록한다.

## 12개 handler

모든 body는 MapPack이다. 표의 모든 조회는 session 인증이 필요하다. response는 별도 표시가 없으면 HasNEXT+XLogPack stream이며 cursor/paging metadata는 없다.

| 명령 | request key | response·선택·순서 | 원본 XLogService.scala 줄 |
|---|---|---|---|
| TRANX_PROFILE | date(빈/없음=오늘),txid,gxid,xlogType,max | normal legacy ProfilePack 또는 합성 Span profile | 41-70,100-113,148-155 |
| TRANX_PROFILE_FULL | date,txid,gxid,xlogType; max 무시 | normal ordered Blob chunks, Span은 단일 Step Blob | 73-97,115-133,158-162 |
| TRANX_REAL_TIME_GROUP | loop,index,limit,objHash[] 선택 | 첫 MapPack loop/index 다음 XLog; elapsed>=limit OR error | 171-204 |
| TRANX_REAL_TIME_GROUP_LATEST | loop,index,count,objHash[] 선택 | 첫 MapPack loop/index 다음 최근 raw count에서 object filter | 214-248 |
| TRANX_LOAD_TIME_GROUP | date,stime,etime,limit,max,reverse,objHash[] | elapsed>limit; max/reverse; 빈 object 목록은 no-item | 259-297 |
| TRANX_LOAD_TIME_GROUP_V2 | date,stime,etime,lastBucketTime,txid,pageCount,objHash[] | XLog 다음 마지막 MapPack hasMore/lastXLogTime/lastTxid | 314-392 |
| XLOG_READ_BY_GXID | date,gxid | 해당 날짜의 GXID index 순서, caller 변형 없음 | 395-410 |
| XLOG_READ_BY_TXID | date,txid,gxid 선택 | txid hit의 조건부 caller 보정 또는 Span ancestry fallback | 413-462 |
| XLOG_LOAD_BY_TXIDS | date,txid(List Decimal) | 입력 ID 순서; miss 제외, 같은 ID 반복 유지; caller 변형 없음 | 464-488 |
| XLOG_LOAD_BY_GXID | stime,etime,gxid | start 날짜의 전체 GXID 결과 후 end 날짜(다를 때)의 전체 결과 | 491-526 |
| QUICKSEARCH_XLOG_LIST | date,txid,gxid | txid!=0 단일 hit 먼저, gxid!=0 그룹 결과 뒤; 중복 유지 | 529-563 |
| SEARCH_XLOG_LIST | stime,etime,objHash,service,ip,login,desc,text1..text5 | raw time 조회에 optional 문자열/object 필터, match limit | 566-686 |

표의 정본 파일 전체 경로는 `scouter.server/src/main/scala/scouter/server/netio/service/handle/XLogService.scala`다. `TRANX_REAL_TIME`/`TRANX_LOAD_TIME`은 `scouter.common/src/main/java/scouter/net/RequestCmd.java:112-123`의 상수지만 정본 서버에 등록된 ServiceHandler가 없다. 이름만 보고 새로운 selector나 alias를 만들지 않는다.

GXID 순서는 timestamp 정렬이 아니다. `scouter.server/src/main/scala/scouter/server/db/xlog/XLogIndex.scala:115-125` → `scouter.server/src/main/scala/scouter/server/db/io/IndexKeyFile.scala:126-142`가 head→previous link 순으로 읽는다. `XLogRD.scala:207-243`는 이 순서에서 unreadable/null record를 제외한다. GXID=0은 저장 index를 만들지 않는다 (`XLogIndex.scala:77-83`).

LOAD_BY_GXID는 stime/etime로 날짜 두 개만 선택하며 EndTime 필터를 하지 않는다. 중간 날짜가 있어도 추가로 조회하지 않는다. SEARCH 역시 start/end 날짜만 사용한다. Go raw query는 선택한 날짜 파일의 끝/시작으로 구간을 제한하여 기존 Store의 날짜/시간 상한을 지키며, 원본이 첫 날짜 파일과 마지막 날짜 파일만 읽는 결과 의미를 유지한다. 이 원본 한계를 다중 날짜 자동 확장으로 바꾸지 않는다. LOAD_BY_TXIDS는 **시도 count를 증가한 뒤 count>=limit면 종료**하므로 기본 500에서 최대 499개 입력을 조회한다. miss도 count에 포함된다. 이 off-by-one 형태는 코드 근거로 재현한 계약이며 결과 개수 500으로 자동 교정하지 않는다.

V2의 lastXLogTime은 XLog EndTime도 epoch 500ms floor도 아니다. `scouter.server/src/main/scala/scouter/server/db/io/IndexTimeFile.scala:249-258`에서 stime 또는 lastBucketTime을 seed로 500ms씩 증가하는 traversal cursor다. stime의 비정렬 offset을 유지한다. metadata는 마지막 item이며 resume 요청은 lastBucketTime=직전 lastXLogTime,txid=직전 lastTxid다. 기존 ring·history 필터와 NULL/빈 결과는 query-contract의 normal 계약을 유지한다.

## B3 caller와 txid fallback

단일 XLOG_READ_BY_TXID만 다음 변형을 수행한다 (`XLogService.scala:420-459`). GXID/복수 TXID/quick/search에는 원본 TODO만 있고 caller 보정이 없다.

1. 직접 txid hit가 없고 request gxid!=0이면 해당 GXID의 모든 Span을 txid map으로 만든다. requested txid부터 parent를 찾아 그 ID의 저장 XLog를 조회한다. 이 fallback hit는 다시 caller를 보정하지 않는다.
2. 직접 hit이며 `(XType==ZIPKIN_SPAN(5) || B3Mode) && Caller!=0 && Caller!=GxID`인 경우, XLog의 GxID로 Span map을 읽고 Caller부터 parent를 찾아 응답의 Caller만 교체한다. Backend snapshot은 수정하지 않는다.
3. map에 ID가 없으면 parent=0이다. SERVER/CONSUMER뿐 아니라 CLIENT/PRODUCER도 parent 후보다. `scouter.common/src/main/java/scouter/lang/pack/SpanTypes.java:52-69`의 shared-ID 의미를 보존한다. map은 flat Span 순서의 마지막 동일 txid 값으로 교체한다.

원본 Span DB 조회 nil은 getSpansMap.foreach 예외를 거쳐 single txid 조회를 no-item으로 끝내지만, 존재하는 non-nil empty 목록은 caller=0 변형이 가능하다. Go는 이 nil/empty 차이를 유지한다. 일반 storage/backend 오류는 명시 오류로 전달한다. ancestry cycle은 Go에서 malformed로 종료하며 원본의 무한 재귀/stack failure를 재현하지 않는다.

## Span profile 분기와 Step 합성

`XLogService.scala:55-65,86-96`에서 request gxid==0이면 먼저 저장 XLog를 읽어 **gxid와 xlogType 모두** 덮어쓴다. 따라서 request xlogType=5여도 저장 XLog가 normal이면 normal profile이다. gxid!=0이면 요청 xlogType을 사용한다. B3Mode만으로 Span profile로 전환하지 않는다.

ZIPKIN_SPAN=5는 GXID의 SpanContainer를 펼쳐 [`BuildSpanProfile`](../../internal/service/trace.go)로 Step을 합성한다. Span은 저장 profile Blob의 대체 저장 형식이 아니라 별도 span DB다. 정본 `scouter.server/src/main/scala/scouter/server/core/app/SpanStepBuilder.scala:34-139` 기준:

- SERVER/CONSUMER는 XLoggable map, 나머지는 별도 map에 txid별 마지막 값을 넣는다. CLIENT/SERVER 같은 ID를 중복 오류로 거절하지 않는다.
- 원본 입력 목록을 다시 순회하면서 non-XLoggable child만 caller의 non-XLoggable/XLoggable relation에 붙인다. sibling TreeSet comparator는 timestamp만 비교한다. 같은 timestamp의 나중 sibling은 탈락한다. txid나 입력 index를 tie-breaker로 추가하지 않는다.
- 요청 txid의 XLoggable root를 preorder로 방문한다. 시작 root 이후 CLIENT/SERVER/PRODUCER/CONSUMER boundary는 그 Step을 포함한 뒤 subtree를 차단한다. 동일 txid의 CLIENT boundary도 원본의 startTxid 비교를 따른다.
- 초기 timestamp는 root의 timestamp다. index는 preorder index, StartTime은 int32(timestamp-initial), parent는 최종 txid→Step map의 index 또는 -1이다. 같은 txid Step이 둘이면 마지막 index가 parent lookup에 사용된다.
- SERVER/CONSUMER/UNKNOWN은 SpanStep, CLIENT/PRODUCER는 SpanCallStep이다. elapsed/error/name hash/timestamp/endpoints/IP/port/debug/shared/annotations/tags를 보존한다. CLIENT/PRODUCER의 tags에 http.url key가 있으면 Opt=1/address를 설정하며 Async 기본 0이다.
- nameDebug TextRD 조회는 wire 필드가 아니므로 Go 합성에 dictionary dependency를 추가하지 않는다. 정본 Scala oracle은 이 debug-only TextRD 경계만 seam으로 제공한다.

field 변환 근거는 `scouter.common/src/main/java/scouter/lang/step/SpanStep.java:42-64`, `SpanCallStep.java:63-94`, `CommonSpanStep.java:61-82`다. endpoint 예시는 fixture의 합성 데이터만 사용하며 운영 값을 fixture/log에 넣지 않는다.

합성 normal response는 legacy ProfilePack(Time=0,TxID=request txid,ObjHash=root ObjHash,Service=root Name,Profile=Step bytes)다. Scala가 대입하는 elapsed는 `scouter.common/src/main/java/scouter/lang/pack/XLogProfilePack.java:82-88` wire에 없다. FULL은 같은 Step bytes를 **단일 Blob**으로 보낸다. max는 Span 합성에서 무시한다. normal 저장 profile에서는 기존 chunk max와 StepControl truncation을 유지한다.

없는 Span/root에 대한 일반 TRANX_PROFILE은 no-item이다. FULL은 원본 uncaught branch에 맞춰 오류로 연결을 끝낸다. 순환 hierarchy는 malformed, 입력/출력 relation은 최대 100000, encoded 합성 profile은 최대 4MiB 및 codec/transport budget을 적용한다. 이는 원본의 무한 재귀/무제한 응답 대신 명시적 제한을 둔 차이다.

## SpanContainer 저장

[`internal/storage/span.go`](../../internal/storage/span.go)는 기존 Store 파일/index/lock/codec helper를 사용한다. 경로는 yyyyMMdd/span/span.data, span_tim, span_gid다. `scouter.server/src/main/scala/scouter/server/core/SpanCore.scala:35-57`와 `scouter.server/src/main/scala/scouter/server/db/ZipkinSpanWR.scala:73-75,85-89`처럼 Container를 그대로 저장하며 XLog를 파생 생성하지 않는다. timestamp=0 보완은 ingest 책임이다. 독립 SpanPack 수집 확장은 자동 지원하지 않는다.

`scouter.server/src/main/scala/scouter/server/db/span/ZipkinSpanDataWriter.scala:50-56`와 `ZipkinSpanDataReader.scala:58-67`는 **signed short length + encoded SpanContainerPack**이며 압축을 사용하지 않는다. Go는 encoded record>32767를 쓰기 전에 거절한다. 원본 writer의 short wrap/reader negative array 예외를 재현하지 않는다. index key는 timestamp/GXID의 big-endian int64, value는 signed 5-byte physical offset이다. GXID0은 index 기록을 생략한다 (`ZipkinSpanIndex.scala:58-71`).

조회는 GXID index 최신 link부터 Container를 읽고 각 Blob을 EOF까지 SpanPack으로 decode한다. declared SpanCount는 보존하지만 조회 loop 상한으로 쓰지 않는다. matching Container가 없으면 nil, matching empty Container가 있으면 non-nil empty Span 목록을 반환하여 B3 보정의 nil/empty 차이를 유지한다. 원본 `scouter.common/src/main/java/scouter/lang/pack/SpanPack.java:221-235`도 EOF를 따른다. 잘린 record/unknown nested Pack은 오류다. record/byte/count 상한과 context lock/IO/decode 체크를 적용한다. append 이후 index를 완료하는 중 취소되면 record가 저장되어도 caller는 cancellation을 받을 수 있으므로 exactly-once ack를 주장하지 않는다.

raw time 조회는 기존 Store.XLogsContext를 재사용한다. 이 경로는 기존 legacyindex.ScanRange가 기록한 **원본 비정렬 마지막 bucket 누락 수정**을 함께 따른다. 기존 DB format/정렬과 이 의도한 source bug 차이를 구분한다. 조회 보존 기간, disk full/partial index recovery와 shared-host 성능은 별도 gate다.

## 검증

synthetic unit/wire 테스트는 normal/empty selector, duplicate GXID/TXID와 순서, 0ms elapsed 검색, NULL/empty pattern, 원본 제한 star 문법, IPv4 fallback, configured match/attempt limit, B3 caller rewrite/fallback, normal-versus-Span 분기, 단일 FULL Blob, timestamp tie/shared ID/boundary/parent/metadata, malformed/cancel/limit/ownership/restart를 검사한다.

[`trace_test.go`](../../internal/service/trace_test.go)의 `TestTraceScalaSpanOracle`는 `scripts/java-storage.py --generate-spans`를 임시 디렉터리에 실행한다. 정본 **실제 Scala SpanStepBuilder + Step.toBytes** 결과와 Go 합성 bytes를 simple/shared 두 dataset으로 비교한다. JDK가 없으면 explicit skip이며 PASS 증거로 간주하지 않는다. input Container와 expected Step은 oracle가 생성하며 Go round-trip만으로 원본 호환을 주장하지 않는다.

```sh
.tools/go/bin/go test ./internal/service ./internal/storage -run '^Test(Trace|Span|XLogsByGxID)' -count=1
.tools/go/bin/go test -race ./internal/service ./internal/storage -run '^Test(Trace|Span|XLogsByGxID)' -count=1
.tools/go/bin/go vet ./internal/service ./internal/storage
```

실제 Java reader/writer span DB 양방향, 실제 TcpProxy에서 모든 selector/B3/profile, 실제 Host/Agent 연결·재시작 검증은 tools/java-storage 및 tools/java-integration의 별도 결과로 확인한다. 단위/oracle 통과와 runtime 명령 등록 완료/운영 검증을 분리한다.

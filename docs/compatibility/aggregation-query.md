# Visitor / TagCount 조회·저장 계약

정본은 `baseline.json`의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. 아래 근거 경로는 이 정본 저장소 기준이다. TPS/운영 호환 완료를 의미하지 않는다.

## Visitor

`scouter.server/src/main/scala/scouter/server/netio/service/handle/VisitorService.scala:34-127`의 7 commands를 구현한다.

| 명령 | request MapPack | HasNEXT 뒤 body |
|---|---|---|
| VISITOR_REALTIME | objHash | DecimalValue |
| VISITOR_REALTIME_TOTAL | objType | DecimalValue |
| VISITOR_REALTIME_GROUP | objHash list | merged DecimalValue |
| VISITOR_LOADDATE | date, objHash | DecimalValue |
| VISITOR_LOADDATE_TOTAL | date, objType | DecimalValue |
| VISITOR_LOADDATE_GROUP | startDate, endDate, objHash list | 날짜마다 MapPack(date,value) |
| VISITOR_LOADHOUR_GROUP | stime, etime, objHash list | 날짜마다 MapPack(time list,value list) |

GROUP은 cardinality 합산 대신 register max 병합이다. 실시간은 현재 cache 우선, 일별·시간별 과거 조회는 파일 우선이다. 없는 데이터는 0이다. 날짜 범위는 양 끝 포함, 고정 24시간 증가다. hourly는 stime을 정시로 내리지 않고 고정 1시간씩 증가한다. 역전된 날짜 범위는 0 records, 역전된 hourly 범위는 빈 lists MapPack 1개다. `timetype`은 없다.

수집은 `XLogCore.scala:54-61`, `VisitorCore.scala:50-76`에 따라 등록된 WEB_SERVICE object의 userid!=0만 처리한다. 날짜/hour는 XLog.endTime이 아니라 서버 처리 시각이다. `visitor_hourly_count_enabled` 기본 true(`Configure.java:550,824`)다. Go storage는 `Store.SetVisitorHourlyCount(enabled)`(주입 전 true)로 받는다(이전 `Config.VisitorHourlyDisabled`는 삭제, [storage CONTRACT](../../internal/storage/CONTRACT.md)). 서버 runtime이 scouter.conf reload마다 반영한다.

`VisitorDB.scala:41,86-112`의 rsd=20은 **int log2m 생성자**다. HLL register 2^20, word 174763, bytes 699060이다. userid는 `MurmurHash.hashLong`으로 32bit hash를 얻고, `HyperLogLog.offerHashed(int)`로 register를 갱신한다. word당 5bit register 6개, union은 max, cardinality는 원본 alpha/small-range/Math.round를 사용한다. 근거: `scouter.server/src/main/java/scouter/server/util/cardinality/{HyperLogLog,MurmurHash,RegisterSet}.java`.

파일은 `<date>/visit/<objType>.usr`, `<date>/visit/<Hexa32(objHash)>.usr`, `<date>/visit_hourly/<Hexa32(objHash)>_<hour>.usr`. bytes는 big-endian int32 log2m, int32 wordCount, int32 words다. object hash 파일 이름은 일반 hex가 아니라 Hexa32의 x/z radix32 표현이다.

원본 hourly getNewObject가 hour suffix를 두 번 붙여 load하는 결함을 재현한다(`VisitorHourlyDB.scala:74,88-93`). oracle은 정상 single-suffix 파일에 userid1을 저장한 뒤 **실제 원본 VisitorHourlyDB**에 userid2를 offer하여 cardinality=1을 확인한다. 원본의 rollover cache clear-before-flush 순서도 유지한다(`VisitorDB.scala:47-53`, `VisitorHourlyDB.scala:42-47`). 실제 시각을 바꾸는 자정 rollover 실행 oracle은 아직 없으며 해당 순서는 소스+Go 재현 테스트 근거다.

## TagCount

`scouter.server/src/main/scala/scouter/server/netio/service/handle/TagCountService.scala:45-203`의 5 commands를 구현한다.

| 명령 | HasNEXT 뒤 body | 빈 결과 |
|---|---|---|
| TAGCNT_DIV_NAMES | group별 TextValue | service,alert 등록 순서 |
| TAGCNT_TAG_NAMES | name별 TextValue | unknown group: 0 records |
| TAGCNT_TAG_VALUES | Text tagName, int32 kinds, float32 total, int32 n, n*(Value,float32 count) | 요청 name별 kinds/total/n=0 |
| TAGCNT_TAG_VALUE_DATA | int16 length, float32[1440] | 없는 Value: 0 records |
| TAGCNT_TAG_ACTUAL_DATA | XLogPack/AlertPack 원본 bytes | 없는 데이터: 0 records |

VALUES의 tagName은 list, VALUE_DATA의 tagName은 text다. VALUES/VALUE_DATA의 빈 date만 오늘로 바꾼다. DIV_NAMES/TAG_NAMES의 objType은 원본에서 사용하지 않는다. TAG_NAMES는 등록 순서가 아니라 실제 StringSet bucket 순서다. 이 순서는 실제 TagCountConfig/StringSet oracle으로 검증한다.

service는 XLogTagCount.scala:62-145, alert는 AlertTagCount.scala:58-62의 typed Value와 조건을 따른다. tagcnt_enabled 기본 true다. service는 endTime, alert는 AlertCore에서 서버 수신 시각으로 덮어쓴 time을 사용한다. countryCode는 TextValue, login/desc는 DecimalValue, object/service/error 등은 TextHashValue이며 이 타입을 서로 대체하지 않는다.

First는 tag별 **최초 100개 distinct Value**를 1440개의 float32 minute buckets에 보관한다. 이후 신규 Value는 Next의 시간별 60 buckets로 넘어간다(`first/TagValueEntry.scala:164-180`). 이미 First인 Value는 계속 First로 간다. VALUES는 cache 우선, cache가 없으면 First만 합산한다(`TagCountProxy.scala:45-87`). fallback sort는 int(B.count-A.count)로 비교하므로 차이가 1 미만이면 tie다. VALUE_DATA는 First가 없을 때만 Next를 조회한다(:90-95).

Top100 cache는 5분마다 Next totalCount header와 First sum을 합친다. 원본 TopN의 binary insertion, equal-count mid insertion, 작은 값 제거, reverse 출력 순서를 재현한다. cache가 만들어지기 전 overflow Value가 VALUES 목록에 없는 동작을 유지한다.

Top100 cache 생성은 원본 makeTop100(`core/Top100FileCache.scala:125-169`)처럼 스트리밍한다. First를 읽어 tag별 key·HashSet hash·1440 bucket 합만 보관하고, Next를 파일 순서(`next/IndexFile.scala:148-168`)로 읽으며 tag별 sumOfValue/kindsOfValue와 limit 100 TopN(`TopN.java:44-108`, `ValueCount.java:39-44`)만 유지한다. 이후 Next가 있는 tag에 First 값을 Java HashSet bucket 순서로 추가한다(`FirstTagCountDB.scala:200-222`). First만 있는 tag는 원본처럼 cache를 만들지 않는다. 메모리는 TopN·First key와 record 1개 수준이며 파일 크기나 MaxQueryBytes에 비례하지 않는다. 한 파일 scan은 1<<24 record, 보관 key 추산은 64MiB를 넘으면 ErrLimit이며 context 취소를 반환한다. 결과 bytes는 이전 전체 적재 구현과 같다(`tagcount_stream_test.go`의 reference 비교).

파일 경로는 `<date>/tagcnt/<objType>/`이다.

| 파일 | 형식 |
|---|---|
| first.kfile | CA FE; record: deleted byte, hashLink long5, float32[1440], int16 keyLength, key |
| next.kfile | CA FE; record: hashLink long5, float32 totalCount, hourPosition long5[24], int16 keyLength, key |
| next.data | CA FE; 시간별 float32[60] |
| first.hfile / next.hfile | CA FE, 1024 bytes reserved header, offset4 int32 count, long5 buckets |
| top100.<signed decimal tagKey>.dat | int32 kinds,float32 total,int32 n,n*(Value,**float64** count) |

key=long8 tagKey+Value wire. tagKey=CRC32(group) 상위32bit+CRC32(name) 하위32bit. hfile bucket은 `(CRC32(key)&0x7fffffff)%capacity`다. 원본 String hash bytes는 Java default charset이므로 oracle은 UTF-8 컴파일/runtime으로 검증한다.

ACTUAL_DATA는 해당 날짜의 ObjectPack 등록 objType으로 object hashes를 정한다. service filter는 알려진 key/value 중 **하나라도 일치하면 true인 OR**다. 빈 MapValue는 false, null filter는 true다. service txid record 자체는 제외하고 다음 record부터 출력한다. reverse는 기존 timed index 순서를 사용하고 max는 callback 뒤 확인한다. max<=0도 첫 callback 뒤 종료되는 경계를 유지한다. service 지원 filter는 error,user-agent,object,nation,city,service 및 service sum keys,referer,group이다. login/desc는 원본 filter에서 지원하지 않는다. alert는 object,level,title을 지원한다. 각 출력은 decode/re-encode한 bytes가 아니라 기존 XLog/Alert data reader의 raw bytes다.

## 한도·소유권·의도된 차이

- 모든 API는 Store.mu.LockContext로 직렬화하고 context 취소를 반환한다. 반환 Value/series/raw slices는 호출자가 소유한다. 파일은 기존 Store.files cache의 Sync/Close/eviction 소유권을 사용한다.
- Visitor entry 상한은 일별 objType 100(원본과 같음), 일별 object와 hourly object 각 32768이다(원본은 500). 새 entry는 파일을 읽지 않는 partial entry다. 방문의 register만 메모리에 올리고, 10초 flush가 파일에 register별 max로 합친다. 그래서 수집 경로는 object 수와 무관하게 visitor 파일을 읽거나 쓰지 않는다. flush는 resident HLL 합계가 예산(256MiB) 안이면 entry를 resident(전체 HLL 메모리 보관)로 승격한다. 넘으면 방문·쓰기·pass 시점에 가장 오래 쓰이지 않은 entry부터 partial로 강등한다. dirty entry도 쓰지 않은 register를 delta로 남겨 무손실로 강등한다. flush를 기다리는 delta 합계가 64MiB를 넘으면 그 방문을 바로 파일에 쓴다(write-through). 상한을 넘으면 FIFO로 가장 오래된 entry를 내보내며, dirty면 저장한 뒤 제거한다. 원본은 500개를 넘으면 dirty HLL을 저장하지 않고 버려 방문을 잃는다. Go는 파일 bytes가 모든 방문을 HLL 하나에 넣은 결과와 같도록 보존한다(의도된 차이). object 500개 이하이고 objType 100개 이하이면 원본과 같은 파일을 만든다. objType이 100개를 넘으면 원본은 축출된 objType의 방문을 잃는다. 예외: 같은 시간에 32768개를 넘어 축출된 hourly key가 시간당 32768개를 넘게 다시 들어오면 원본처럼 double suffix load로 파일을 바꾼다. 세부는 `internal/storage/CONTRACT.md` §8.1을 따른다. `.usr` 파일은 방문자 수와 무관하게 항상 699,060 bytes다. 원본 `HyperLogLog.getBytes`가 sparse 표현 없이 dense `RegisterSet` word 174,763개를 모두 쓰기 때문이며(`HyperLogLog.java:203-210`, `RegisterSet.java:56-65`, `VisitorDB.scala:41`의 `rsd = 20`은 Int라 `HyperLogLog(int log2m)`로 log2m=20), Go도 같은 크기와 bytes를 쓴다. 따라서 디스크 사용량은 원본과 같다. 사용자 ID가 있는 object 하나당 daily 파일 1개, 방문이 있던 시간마다 hourly 파일 1개다. 배포 자원 예산과 부하 gate 검증이 필요하다.
- Tag table cache는 30개, metadata 추산 상한 64MiB, pending float arrays 및 key bytes는 min(MaxQueryBytes,64MiB), cache 재생성 대기 경로는 1000개다. 포화 시 pending flush 또는 명시적 ErrLimit을 반환한다. 원본처럼 무한 queue 대기하지 않는다. file cache eviction에서도 dirty counters를 잃지 않는다.
- legacy 조회는 누락 파일을 만들지 않는다. 원본 reader가 rw open/mkdir하는 부작용은 재현하지 않는다. 기존 symlink와 경로 traversal, 잘못된 날짜/길이/offset/파일 크기는 거절한다.
- Tag First/Next key file은 전체를 적재하지 않고 record 단위로 스트리밍한다(scan 상한 1<<24 record. top100 재구성이 이 상한이나 보관 상한에 닿으면 그 경로는 purge·재기동 전까지 다시 재구성하지 않는다, storage CONTRACT §8). VALUES fallback은 요청 tag record만 보관하며 MaxQueryRecords를 넘으면 ErrLimit, VALUE_DATA는 첫 일치 record에서 scan을 멈춘다. ingest의 First key 적재는 metadata 64MiB 상한을 record마다 확인한다. 이 경로들은 Store.mu를 잡은 채 파일 끝까지 읽을 수 있으므로 매우 큰 Next 파일은 ingest를 그만큼 지연시킨다.
- 조회 record/bytes는 기존 MaxQueryRecords/MaxQueryBytes를 적용한다. Visitor 날짜/hour 범위는 최대 366일이다. 무제한 범위 대신 ErrLimit을 반환한다. corrupt 데이터/취소/저장 오류를 빈 성공으로 바꾸지 않는다.
- Store.Close는 모든 aggregation dirty 항목을 flush하고 기존 파일 sync를 수행한다. 서버 종료가 원본 마지막 dirty 유실보다 데이터를 더 보존할 수 있다. Store.Sync와 주기 flush는 시작 시점의 dirty 항목을 한 번씩만 쓰고, 그 사이 새로 dirty가 된 항목은 다음 flush에서 쓴다.
- Client에 tag count·top100이 보이는 시점: 원본처럼 5초 flush와 5분 rebuild 주기를 따르되, 실제 쓰기는 runtime maintenance loop가 1초 tick마다 하므로 최대 약 1초와 그 loop의 앞선 작업 시간만큼 더 늦어질 수 있다(`internal/storage/CONTRACT.md` §8).

## 검증

`go test ./internal/storage -run '^TestJavaAggregationInterop$' -count=1 -v`는 실제 원본 HyperLogLog/MurmurHash/RegisterSet, TagCountConfig/StringSet, First/Next IndexFile/KeyDataFile/KeyFile/DataFile, ValueCountTotal, TopN 및 VisitorHourlyDB를 임시 classes로 컴파일한다. 손작성 Java format 복제품을 사용하지 않는다. 설정·Logger·daemon 종료만 seam으로 둔다.

`scripts/java-aggregation.py`는 common/server/Scala jars 전체 tracked files를 pinned Git blob과 비교하고 각 파일 길이/SHA-256/Git blob 및 harness SHA-256을 임시 source-guard.json에 기록한다. 원본 저장소는 읽기 전용이다.

검증 범위: HLL byte/merge 양방향, First/Next Java→Go/Go→Java, typed Value, 100/101 경계, 실제 fractional comparator, 실제 equal-count TopN cache bytes, hourly suffix 원본 사례, Go TCP 7 Visitor/5 Tag response body, raw/cursor/OR/reverse/max, corrupt/symlink/context 취소, Sync/Close/restart 및 cache eviction.

이 문서는 소스·fixture·로컬 TCP 증거다. 실제 배포, Scouter GUI 시각 검증, 운영 DB 복사본 이관, 공유 호스트 지속 부하 및 목표 처리량 증거는 남은 gate다.

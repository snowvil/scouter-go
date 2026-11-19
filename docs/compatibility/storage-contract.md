# Object/Text/XLog/Profile 저장 계약

정본은 `baseline.json`에 고정한 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`(2.21.3)다. 아래 경로는 정본 기준이며 Go는 원본 파일 이름·바이트·offset/index를 사용한다. 별도 custom DB 형식을 Scouter 호환이라고 취급하지 않는다. 이 문서는 Object/Text/XLog/Profile과 공통 날짜·복구 경계다. Counter 구현은 `internal/storage/counter.go`에 분리한다.

## 원본 형식과 조회 의미

다중 바이트 정수는 big-endian이다. Long5는 signed40bit 위치이고 파일 offset 0도 유효한 data 위치다. index chain의 0은 연결 종료이며 첫 `.kfile` record는 2다.

| 데이터 | 상대 DB 경로 | payload와 index | 정본 근거 |
|---|---|---|---|
| Object | `{date}/object/obj.data`, `.hfile`, `.kfile` | `int32(4096)` + `writePack(ObjectPack)`의 4096바이트 zero padding. index key=int32 objHash, value=Long5 offset. 같은 objHash는 기존 data slot만 덮어쓴다. | `ObjectWR.scala:49,66-74,126-132,156-180`; `obj/ObjectData.scala:65-93`; `obj/ObjectIndex.scala:52-74` |
| 일별 Text | `{date}/text/text.hfile`, `.kfile` | key=int32 CRC32(div)+int32 textHash, index Blob value=UTF8 text 자체. 최초 키만 저장. | `TextWR.scala:92-107,152-173`; `text/TextTable.scala:61-79` |
| 영구 Text | `00000000/text/text_{div}.data`, `.hfile`, `.kfile` | `.data`=`int32 len`+UTF8. index key=int32 textHash, value=Long5 offset. 최초 키만 저장, 빈 문자열 무시. | `TextPermWR.scala:37-46,58-61,71-74,98-121`; `text/TextPermData.scala:81-102`; `text/TextPermIndex.scala:75-87` |
| 비압축 XLog | `{date}/xlog/xlog.service`, `.service.conf` | signed int16 길이 + `writePack(XLogPack)` bytes. `.conf`의 `compress_xlog_enabled`가 파일 모드다. time/txid/gxid별 `xlog_tim`, `_tid`, `_gid` index, value=Long5. gxid=0은 index에 쓰지 않는다. | `xlog/XLogDataWriter.scala:53-80`; `xlog/XLogDataReader.scala:54-76`; `XLogWR.scala:58-76,109-136`; `xlog/XLogIndex.scala:39-42,70-90` |
| 비압축 Profile | `{date}/xlog/xlog.profile`, `.profile.conf` | int32 길이 + raw Step bytes. ProfilePack 전체를 쓰지 않는다. `xlog_profile` index key=int64 txid, value=Long5. 같은 txid의 모든 offset을 숫자 오름차순 정렬해 concatenate. | `core/ProfileCore.scala:39-42`; `xlog/XLogProfileDataWriter.scala:54-82`; `xlog/XLogProfileIndex.scala:73-98`; `XLogProfileRD.scala:70-110` |

위 Scala DB 파일들의 앞부분은 `scouter.server/src/main/scala/scouter/server/db/`다. `Configure.java:728-729,794-808`의 기본값은 두 압축 옵션 false, Text daily service/api/ua false, hash index 크기 1MiB다. `error`는 항상 일별, `service`/`apicall`/`ua`는 해당 옵션에 따르고 나머지는 영구다(`scouter.common/.../TextTypes.java:23-41`). Java `HashUtil.hash`는 IEEE CRC32이며 String overload는 platform default charset를 쓴다(`scouter.common/src/main/java/scouter/util/HashUtil.java:56-65`). Go는 div/name에 UTF8 bytes를 사용하므로 Java 운영 JVM charset를 UTF8로 고정하는 것이 상호운용 전제다.

`.kfile`은 `CA FE` 두 바이트로 시작하고 record는 `deleted bool` + `prev Long5` + `key unsigned-short length + bytes` + `value Blob`이다(`db/io/RealKeyFile.scala:36-51,101-135`). Blob 길이는 0..253 한 바이트, 255+uint16, 254+int32다(`scouter.common/src/main/java/scouter/io/DataOutputX.java:111-129`). `.hfile`은 header 1024바이트(CAFE, offset4 int32 count), 이후 5바이트 bucket pointer array다. hash slot은 `(CRC32(key)&0x7fffffff) % floor(payloadSize/5)`(`db/io/MemHashBlock.java:31-35,63-86,104-115`). index 충돌은 key equality로 chain을 탐색한다. Get은 최신 matching key, GetAll은 matching chain 전체를 읽는다(`db/io/IndexKeyFile.scala:54-62,76-94,126-142`). Object delete marker와 기존 duplicated physical records는 index 계층의 계약이다.

time `.hfile`은 172800개 Long5 bucket(500ms, 864000 payload bytes)이다(`MemTimeBlock.java:35-41,80-83`). key=int64 epoch milliseconds, value=Long5 data offset이며, V1 조회는 bucket 순회 중 timestamp와 **signed byte lexicographic value** 순으로 정렬·동일 pair 중복 제거한다(`IndexTimeFile.scala:48-55,59-83,216-240,266-282,314-323`). V2는 bucket chain 최신 append부터 조회하며 중복을 유지한다(`IndexTimeFile.scala:75-83,245-263`). `XLogs`는 V1, `XLogsV2`는 V2 순서를 사용한다. `XLogRecord.BucketTime`은 호출자가 시작한 bucket cursor(500ms씩 증가)다.

## 압축 DB

압축 XLog/Profile은 **하나의 공유 저장소** `{date}/xlog/data`를 사용한다(`db/io/zip/GZipCtr.java:29-39`). `xlog.N`은 최대32MiB uncompressed logical block의 GZIP 파일이고, `count.dat`은 int64 next logical offset이다(`CountBoard.java:34-68`, `IOChannel.java:49-60,99-105,130-159`). logical offset은 `N*33554432+blockWithinOffset`이다(`Block.java:104-117`). block record는 `signed Long5 next` + `signed Int3 payloadLength` + raw bytes이며 next=0은 종료, 양수면 다음 record bytes를 결합한다(`GZipStore.java:87-122,136-164`). record가 block에 안 들어가면 다음 block으로 이동하므로 block 사이에 logical 빈 구간이 생길 수 있다. Go는 이 구조의 reader/writer를 구현하며 새 record는 next=0으로 쓴다. gzip 압축 결과의 binary 동일성은 요구하지 않고 Java GZIPInputStream이 해제한 record와 offset/조회 결과를 비교한다.

**Go writer의 multi-member 형식(의도된 차이).** Java는 block 전체를 한 GZIP member로 다시 저장한다(`IOChannel.saveBlockBytes`). Go는 record마다 독립 GZIP member 하나를 `xlog.N` 끝에 append한다(`internal/storage/zip.go`). Java `CompressUtil.unZip`의 `GZIPInputStream`은 이어 붙은 member를 끝까지 해제하므로 같은 논리 bytes가 나온다. Java가 만든 단일 member block 뒤에도 그대로 이어 쓴다. count.dat 값과 logical offset 의미는 바뀌지 않는다. 압축률은 Java보다 낮다. member마다 header·trailer 18바이트가 붙고 record 사이 사전 공유가 없기 때문이다. replay(seed 42) block 실측 결과, 전체 압축률은 0.192였다. 같은 논리 bytes를 Java처럼 block 전체 gzip(level 6)으로 압축하면 0.145다. 약 110B XLog member는 원본보다 약 1.2배 커진다. 압축 수준은 BestSpeed다. Go reader와 migrate inspect는 member 단위로 해제한다(`storage.DecodeZipBlock`). 파일 크기 상한은 5×32MiB, 논리 상한은 32MiB다. 근거: `TestJavaReadsGoGzipMembers`(원본 Java unZip·index·GZipStore reader로 Go member 확인, Java DB에 Go가 이어 쓴 뒤 Java verify), `TestZip*`.

모드는 매 파일 `.service.conf`/`.profile.conf`의 기존 설정이 새 process 설정보다 우선한다. 존재하지 않을 때만 configured mode를 기록한다. 압축 `.conf`를 false로 바꾸거나 개별 payload를 gzip한 `.service` 파일은 이 계약이 아니다.

## 날짜와 수명·복구

Java `DateTimeHelper`는 JVM default timezone의 **2000-01-01 midnight**를 BASE_TIME으로 잡고, 이후 고정86400000ms 단위로 날짜 테이블과 time bucket을 계산한다. DST wall-clock calendar 계산과 다르다(`scouter.common/src/main/java/scouter/util/DateTimeHelper.java:44-49,66-88,131-135,295-316,341-348`). Go `JavaDate`/`JavaMillis`와 index 계층은 동일 Location/base를 사용한다. JVM timezone과 Go Location을 맞춰야 하며 UTC/Asia/Seoul 외 DST zone도 비교해야 한다. Object/Text 날짜는 ingest가 서버 수신 시각으로 전달한다. XLog는 Pack EndTime을 쓰되 0이면 receiver 시간을 적용한다. ProfileCore는 수신 날짜를 사용하므로 Go ingest도 ProfilePack 시간을 receiver 시간으로 보정한 뒤 storage에 전달한다.

원본 hash/time memory header는 4초 flush이며 Close에서 dirty header를 flush한다(`MemHashBlock.java:47-55`; `MemTimeBlock.java:54-56`; `scouter.common/src/main/java/scouter/io/FlushCtr.java:77-92`). 데이터 append와 index 갱신은 별도이며 WAL/atomic transaction이 없다. header 저장은 FileOutputStream overwrite이고 예외를 삼킨다(`scouter.common/src/main/java/scouter/util/FileUtil.java:146-153`). 압축 count는 block의 async 저장을 예약하기 전에 바뀐다(`IOChannel.java:99-104,130-140`). 조사한 DB 코드에서 자동 tail truncate/index rebuild/recovery는 확인되지 않았다. 원본의 정상 close/reopen과 비정상 종료 내구성은 서로 다른 gate다.

Go는 하나의 Store gate로 메서드, Close, Sync를 직렬화하며 반환값은 caller가 소유한다. index 최대32개, data file 최대16개로 제한하고 최근 사용이 가장 오래된 handle부터 eviction하며 eviction 시 Sync/Close한다. index eviction 전에는 그 index가 참조하는 data file을 먼저 sync한다. 모든 index는 자기 디렉터리 subtree(`{date}/{kind}/...`, 압축 `{date}/xlog/data/` block 포함)의 data file만 참조하므로 그 subtree의 열린 data file이 대상이다(열린 data file 전체가 아니다). 날짜별 `.service.conf`/`.profile.conf` 압축 모드는 Java처럼 writer open 시점 의미로 한 번 읽어 cache하며 retention이 해당 날짜를 지우면 버린다. Visitor `.usr` snapshot은 Java `FileUtil.save`처럼 fsync하지 않고 handle cache를 거치지 않는다. 처음과 불일치가 의심될 때는 temp 파일+rename으로 통째로 쓰고, 그 뒤에는 바뀐 4KiB page만 제자리에 다시 쓴다. 새 visitor entry는 파일을 읽지 않고 방문의 register만 메모리에 모았다가 flush가 파일에 합치며, resident 예산 안에서만 전체 HLL을 메모리에 둔다. 그래서 object가 Java 상한 500을 넘어도 방문을 잃거나 수집 경로에서 파일을 읽고 쓰지 않는다(`internal/storage/CONTRACT.md` §8.1). Runtime은 `--sync-interval`(기본4초)마다 `SyncData`를 호출한다. tag count는 Store gate가 아니라 자체 tag gate·handle cache(최대 64개)·inbox로 처리하고, maintenance loop가 짧은 hold로 나누어 쓴다(`internal/storage/CONTRACT.md` §8). text dictionary는 원본 TextWR처럼 Store의 writer goroutine이 queue에서 꺼내 쓰고, 대기 중인 text는 바로 조회된다(§8.2). gate 안에서는 index header snapshot만 잡는다. data fsync·key fsync·header 기록은 gate 밖에서 한다. data를 먼저 fsync하고, snapshot header는 snapshot 이전에 append된 record만 가리킨다. 주기 Sync가 gate 밖에서 fsync하는 동안 eviction·archive 교체·retention으로 cache를 떠나는 handle은 즉시 cache에서 빠지되 close는 그 Sync가 fsync를 마친 뒤 순서대로 수행한다(`closeQueue`). 따라서 eviction은 진행 중인 fsync pass를 기다리지 않고, fsync 중인 handle이 닫히지도 않는다. Sync가 없을 때는 victim을 제자리에서 Sync/Close한다. 같은 경로가 close 대기 중에 다시 열리면 새 handle을 열지 않고 대기 중인 handle을 cache에 되돌리며, close가 실행 중이면 그 한 건의 close만 기다린 뒤 새로 연다. 그래서 옛 handle의 마지막 header가 새 handle로 쓴 record를 덮어쓰지 않는다. 대기 목록은 32건으로 제한하며 초과분은 Sync 중에도 제자리에서 닫는다. cache의 data file은 fsync 후(eviction·archive 교체·Close) 또는 purge 중에만 닫히므로 Sync pass가 닫힌 handle에서 `os.ErrClosed`를 받으면 오류가 아니다. Close는 `syncRun`을 먼저 잡아 진행 중인 Sync와 그 deferred close가 끝난 뒤 최종 header를 쓴다. Counters도 같은 규칙이다. Visitor snapshot은 runtime maintenance loop가 10초마다 쓰며 주기 Sync 대상이 아니다. 수집 경로의 visitor eviction·write-through 쓰기와 그 밖의 snapshot 쓰기는 같은 `visitorIO` 순서로 직렬화한다. Close는 data file sync 후 index sync/close한다. gzip은 member를 block에 write한 뒤 count.dat 8바이트를 제자리에 덮어쓴다(count.dat 최초 생성만 임시 파일+rename). 두 handle은 data file cache에 있어 주기 Sync·Close·eviction 때 fsync되며 record마다 fsync하지 않는다. 디코딩한 block은 최대 4개를 조회용으로 cache하고 writer append가 같은 cache를 갱신한다. 논리 크기 기준 4×32MiB이며 append 여유 용량(`cap`) 때문에 실제 heap은 이를 다소 넘을 수 있다. 주기 Sync는 dirty index마다 header snapshot(hash 1MiB, time 864KiB)을 일시 복사한다. cache는 파일 identity·크기·mtime이 같을 때만 쓰므로 열린 Store 뒤에서 파일을 바꾸는 것은 지원하지 않는다. 원본 파일 형식은 유지하지만 여러 파일 변경의 crash atomicity, rename directory fsync, Java와 동시 writer는 보장하지 않는다. Java/Go를 같은 DB에 동시에 쓰지 않는다. 손상·truncation·count/block 불일치·압축 linked cycle은 명시 error다. 예외는 Go writer 자신의 crash 형태다. ① member write 뒤 count 저장 전에 중단되어 count 뒤에 완전한 next=0 record만 남고 다음 block이 없으면, 다음 append가 count를 전진시킨다(`zip-count-advanced`). ② count block의 마지막 member가 잘렸으면(입력이 header·deflate·trailer 안에서 끝나고 그 길이가 9MiB 이하) 쓰기 경로가 마지막 완전 member 경계로 truncate한다(`zip-torn-member`). 둘 다 `RecoveryEvent`로 보고하며, 모든 검사를 통과한 뒤에만 파일을 고친다. 중간 member 손상(header·CRC·deflate 오류), gzip이 아닌 꼬리, count가 완전 member의 논리 길이보다 큰 상태는 수정하지 않고 ErrCorrupt다. 조회는 완전한 member의 record만 반환하고, 잘린 member 안의 record는 부분 bytes가 아니라 오류다. 따라서 torn tail은 count 이전 record의 조회에 영향을 주지 않는다. Java GZIPInputStream은 header 안에서 잘린 꼬리를 무시하고 deflate 안에서 잘리면 block 전체를 null로 읽는다. 범위는 [recovery-retention](recovery-retention.md)을 따른다. Java reader의 일부 silent partial-return과 의도적으로 다른 실패 계약이다. 원본 DB 복사본을 사용하고 손상 시 자동 수리/덮어쓰기하지 않는다.

## Go API와 기본 상한

```go
store, err := storage.Open(storage.Config{
    Directory: "./database",
    Location: time.UTC, // Java JVM timezone과 동일해야 한다.
})
if err != nil { return err }
defer store.Close()
if err = store.PutObject("20261013", objectPack); err != nil { return err }
logs, err := store.XLogs("20261013", startMillis, endMillis, false)
```

Config는 `Directory`, `Location`, `CodecLimits`, `CompressXLog`, `CompressProfile`, `TextDailyService`, `TextDailyAPI`, `TextDailyUA`, `HashSizeBytes`, `MaxRecordBytes`, `MaxQueryRecords`, `MaxQueryBytes`, `IndexSizes`, `Log`, `MaxQuerySpan`, `OnRecover`, `DeferVisitorFlush`다. `CompressXLog`/`CompressProfile`/`TextDaily*`는 live setter(`SetCompression`, `SetTextDaily`)의 초기값이다. `IndexSizes`(`IndexSizesMB`)는 `_mgr_text_db_index_*_mb`, `_mgr_text_db_daily_index_mb`, `_mgr_kv_store_index_default_mb`의 기동 값(MiB, 0..64, 0은 기본값)으로 새 `.hfile`의 hash 영역 크기만 정한다. `Log`는 Java ID 경고(S152/S161 index 탐색)를 받는 sink이며 nil이면 버린다. 이전의 `VisitorHourlyDisabled`는 삭제했고 `visitor_hourly_count_enabled`는 `Store.SetVisitorHourlyCount`로 받는다. scouter.conf 키별 API와 주입 시점은 [storage CONTRACT](../../internal/storage/CONTRACT.md)가 정본이며, 서버 runtime이 기동 값(`IndexSizes`, `Log`)과 reload마다의 setter를 연결한다(`internal/server/wiring.go`). Location은 필수다. 기본 hash index1MiB(text·object·XLog 등, KV `kv2` index는 Java `_mgr_kv_store_index_default_mb` 기본값과 같은 **8MiB**), 단일record4MiB, 조회100000records/64MiB/24h다. 결과가 상한을 넘으면 partial success 대신 ErrLimit을 반환한다. Pack codec 기본 상한도 적용한다. 압축 record의 signed Int3 길이는 최대8388607바이트, signed Long5 offset은 최대549755813887이다. 비압축 XLog는 Java signed-short reader 때문에 payload32767바이트 이하이고 Object는 padding 포함4096바이트 이하이다. 날짜 조회는 요청 날짜 내부의 양 끝 inclusive epoch 범위로 제한한다. 서로 다른 날짜는 호출자가 나눠 조회한다. 설정 음수·잘못된 날짜·path separator가 포함된 div·invalid UTF8 text는 거부한다.

- `PutObject(date,*ObjectPack)` / `Objects(date)`
- `PutText(date,*TextPack)` / `Text(date,div,hash)`
- `PutXLog(*XLogPack)` / `XLog(date,txid)` / `XLogs(date,start,end,reverse)` / `XLogsV2(date,start,end,lastBucket)`
- `PutProfile(*ProfilePack)` / `Profile(date,txid)` / `ProfileChunks(date,txid)`
- `Sync()` / idempotent `Close()`

취소 가능한 조회는 `XLogContext`, `XLogsContext`, `XLogsV2Context`, `ProfileChunksContext`에 request context를 전달한다. 기존 함수는 `context.Background()` wrapper다. gate 대기는 context select로 취소하고, index callback·각 bucket·Profile chunk·GZIP 입력/해제 read·Pack decode 전후에 취소를 확인한다. 취소 시 partial 결과 대신 context 오류를 반환한다. 이미 실행 중인 disk syscall, 신규 index Open의 전체파일 validation, index 내부의 nonmatching hash-chain 순회/정렬은 현재 context로 강제 중단하지 못한다. Close는 활성 저장 작업이 gate를 해제한 후 진행하므로 blocked disk I/O를 강제로 종료한다는 보장이 아니다.

## Object 영구 삭제 (`RemoveObject`)

원본: `AgentManager.removeAgents(list, permanent=true)`가 hash마다 `ObjectWR.remove(objHash)`를 호출한다(`core/AgentManager.scala:379-386`). `ObjectWR`의 writer thread는 **처리 시점의 오늘 날짜**(`curDbw = open(DateUtil.yyyymmdd())`) object DB에서 `index.get(hash) >= 0`이면 `ObjectIndex.delete(hash)`를 호출한다(`db/ObjectWR.scala:53-88,104-122,156-181`). `IndexKeyFile.delete`는 bucket chain을 head부터 따라가며 key가 같고 아직 삭제되지 않은 **모든** record의 첫 바이트(deleted flag)를 `writeBoolean(true)`=0x01로 바꾼다(`db/io/IndexKeyFile.scala:145-164`, `RealKeyFile.scala:57-62,87-92`). record는 chain에 남고 `.hfile` header count와 `obj.data` 4096byte slot은 바뀌지 않는다. 과거 날짜는 건드리지 않는다. 이후 같은 hash의 ObjectPack은 `index.get`이 -1이므로 새 slot을 append한다(`ObjectWR.scala:69-75`).

Go: `Store.RemoveObject(ctx, hash int32) error`는 Store Java timezone의 오늘 날짜 `{date}/object/obj`에 위 delete flag를 쓴다(`legacyindex.Index.Delete`). ingest의 `objectStoreRemover` seam이 이 시그니처를 type assertion으로 사용한다. 테스트·oracle용 `RemoveObjectsOn(ctx, date, hashes) (live int, err error)`는 날짜를 지정하며 한 호출 최대 10000개(초과 ErrLimit)다. Store gate 안에서 실행하며 Objects/PutObject와 직렬화된다. Object 전용 in-memory cache는 없으므로 별도 무효화가 필요 없다. flag는 key file에 쓰고 다른 index 쓰기와 같이 다음 주기 `SyncData`/`Close`의 key file fsync로 디스크에 반영한다(호출마다 fsync하지 않음, Java도 fsync 없음).

| 항목 | Java | Go | 구분 |
|---|---|---|---|
| 실제 삭제 수행 | 고정 source의 `Order.add` 기본값이 `true`이고 `createRm`은 `delete`만 켠다. writer는 add 분기에서 `pack == null`의 `p.objHash` NPE를 내고 loop가 삼켜 **delete 분기에 도달하지 못한다**(`ObjectWR.scala:66-69,77-81,91-94,104-109`). 디스크 변화 없음. | API와 `IndexKeyFile` 형식이 뜻하는 delete flag를 실제로 쓴다. | 원본 결함 후보와 분리한 의도된 차이 |
| 동기성 | `RequestQueue(DBCtr.MAX_QUE_SIZE)` 비동기, 포화 시 S132 로그 후 유실 | 동기 호출, 오류 반환, 다음 `Objects`에 즉시 반영 | 의도된 차이 |
| object DB가 없는 날짜 | `open`이 빈 `obj.*`를 만들고 get=-1 | 아무 파일도 만들지 않고 no-op(Java reader에는 둘 다 빈 목록) | 의도된 차이 |
| 살아 있지 않은 hash | no-op | no-op | 동일 |

검증: `TestRemoveObjectsOnMarksIndexOnly`(오늘/다른 날짜, 재오픈, data slot 불변, 재추가 append), `TestRemoveObjectUsesCurrentDate`, `TestRemoveObjectsOnWithoutDBAndErrors`, `TestRemoveObjectsConcurrentWithPutAndQuery`(-race), legacyindex `TestDeleteMarksEveryLiveMatchLikeJava`/`TestDeleteRejectsReadOnlyAndClosed`. Java reader 쪽 의미: `ObjectRD.getObjectList`/`read`는 `ObjectIndex.open`·`ObjectData.open` 후 `idx.read(handler, reader.read)`로 `.kfile`을 첫 record부터 걸으며 deleted flag가 켜진 record를 건너뛰고, `getObjectPack`은 `idx.get(hash) < 0`이면 null이다(`db/ObjectRD.scala:34-56,78-116`, `IndexKeyFile.scala:54-62,166-206`). 따라서 Go가 마킹한 hash는 Java 목록·단건 조회에서 빠지고 나머지는 그대로 읽힌다. 원본 oracle `TestJavaStorageInteropObjectRemove`(`tools/java-storage/ObjectRemoveOracle.java`)는 ObjectRD와 같은 `ObjectIndex.open`/`ObjectData.open`/`read`/`get` 경로와 원본 `ObjectData`/`ObjectIndex`로 Java 기록+삭제→Go 조회·삭제→Java 조회, Go 기록+삭제→Java 조회·삭제→Go 조회·재추가→Java 조회를 수행하고, 같은 Java DB 복사본 두 개에 Java와 Go가 같은 hash를 삭제한 뒤 `obj.kfile`/`obj.hfile`/`obj.data` bytes가 같음을 확인한다. writer thread 자체(위 NPE 경로)는 oracle 범위가 아니며 source 근거로만 기록한다.

## DB 경로 삭제 (`DeleteDatabasePaths`, SERVER_DB_DELETE)

원본: `ManageService.deleteDbFiles`(`netio/service/handle/ManageService.java:57-74,143-153`)는 `SERVER_DB_LIST`가 준 이름(`"/20261015"`, `"/20261015/xlog"` 형식, 디렉터리만 나열, `:127-142`)을 받아 없으면 무시하고, 디렉터리면 자식부터 재귀 삭제하며 `delete()` 결과를 무시한다. 원본은 받은 이름을 검증하지 않고 열린 writer를 처리하지 않는다. Go는 검증한다.

Go: `Store.DeleteDatabasePaths(ctx, relPaths []string) (deleted int, err error)`와 `DatabaseDeleter{Store, Counters}.Delete`. `deleted`는 실제로 DB tree를 떠난 기존 경로 수다(응답 `size`=요청 수 계산은 service 몫). 의도된 차이:

- **전체 사전 검증**: 하나라도 거부되면 아무것도 지우지 않는다. 거부(ErrInvalid): 빈 이름, `/`로 시작하지 않는 이름, `..` 구성요소(정규화 전 검사), NUL·backslash·invalid UTF-8, DB root 자체, 첫 구성요소가 유효한 `yyyymmdd` 날짜나 `00000000` 하위가 아닌 이름(host 절대경로, `lock.dat`, `.go-writer.lock`, `.purge-trash`, 알 수 없는 top-level 포함), `00000000` 전체, 존재하는 symlink 구성요소(원본은 검사하지 않는다, management-commands §6.1·§9-4). 1024개 초과·4096바이트 초과 이름은 ErrLimit.
- **열린 handle**: retention G1([recovery-retention](recovery-retention.md))과 같은 순서로 Store gate → `visitorIO` → Counters gate를 잡고, 대상 아래의 index/data/gzip/압축모드/TTL handle과 counter table을 cache에서 빼고 닫는다. 대상 아래의 pending tag 집계와 현재 visitor 날짜의 `visit`(daily)/`visit_hourly`(hourly) 집합은 flush하지 않고 버린다. 그 뒤 `.purge-trash`로 rename하고 gate 밖에서 unlink한다. trash로 옮길 수 없으면(EXDEV, trash 생성 실패) 같은 디렉터리의 숨은 이름 `.purge-quarantine-…`로 rename한 뒤 gate 밖에서 지운다. 두 rename이 모두 실패하면 대상을 남기고 오류를 반환한다. gate 안에서는 unlink하지 않는다(Java 제자리 삭제와 의도된 차이, mount point·읽기 전용 부모는 삭제되지 않는다). 이후 같은 날짜 쓰기는 새 파일을 만들고 조회는 오류 없는 빈 결과다.
- **Counters 필수**: counter 파일을 담은 대상(`{date}`, `{date}/counter`와 그 하위)은 Counters가 없으면 명시적으로 거부한다(열린 table이 unlink된 파일에 계속 쓰는 silent loss 방지). runtime은 `Store.AttachCounters(c)`(같은 root만 허용)로 연결하거나 `DatabaseDeleter{Store, Counters}`를 쓴다.
- **실패 보고**: 개별 삭제·close·trash 정리 실패를 계수에서 빼고 `errors.Join`으로 반환한다. 정리하지 못한 trash는 retention 주기의 `cleanTrash`가 재시도한다.
- 경로 검증(Lstat)은 gate 밖에서 하므로 Store 밖 프로세스가 동시에 DB tree를 바꾸는 경우는 보장 범위가 아니다(Store 단일 writer 전제).

검증: `TestDeleteDatabasePathsClosesHandlesAndRecreatesOnWrite`, `TestDeleteDatabasePathsRefusalMatrix`, `TestDeleteDatabasePathsNeedsCountersForCounterFiles`, `TestDeleteDatabasePathsDiscardsVisitorSetOfDeletedDirectory`, `TestDeleteDatabasePathsConcurrentWithIngestAndQuery`(-race). Java handler 실행 oracle은 없다(원본은 파일 재귀 삭제뿐이며 형식 계약이 없음).

## 영구 Text div 상한 (R7, 의도된 차이)

원본은 UDP로 받은 `TextPack.xtype`(div)의 종류 수에 상한을 두지 않고 div마다 영구 index를 만든다. Go는 원본에 없는 div의 영구 index 수를 제한한다. 근거 경로는 정본 기준이다.

**원본 동작**
- 서버는 div를 거르지 않는다. 모든 TextPack은 `NetDataProcessor.scala:186` → `TextCore.add`(`TextCore.scala:46-52`) → `TextWR.add(yyyymmdd, pack.xtype, ...)`(`TextCore.scala:42`)로 간다. 허용 목록이 없다.
- `TextPermWR.isA`는 `error`만 false, `service`/`apicall`/`ua`는 `mgr_text_db_daily_*_enabled`에 따르고 **그 밖의 모든 div는 true**다(`TextPermWR.scala:37-47`). 그래서 모르는 div는 항상 영구다(`TextWR.scala:92-97`). 일별 `{date}/text/text`에는 들어가지 않는다.
- 영구 text는 div마다 `00000000/text/text_<div>` 파일 묶음(`.hfile`·`.kfile`·`.data`)을 연다(`TextPermWR.scala:89-108`). 모르는 div의 hash index 크기는 `_mgr_text_db_index_default_mb`(기본 1MiB, `Configure.java:353,798`; `TextPermIndex.scala:102-111`)다. 빈 text는 무시한다(`TextPermWR.scala:71-73`). `00000000`은 retention 대상이 아니다.
- 원본이 쓰는 div는 `scouter.lang.TextTypes`의 17개뿐이다(`TextTypes.java:22-38`): `error apicall method service sql object referer ua group city table maria login desc web hmsg stackelem`.
  - Java Agent: `DataProxy.java:65,79,92,101,112,122,132,155,165,175,330,341,351,362,371`(sql·service·object·referer·ua·method·apicall·error·desc·login·web·group·hmsg·stackelem).
  - Host Agent: `HostAgentDataProxy.java:64`(error). Batch Agent는 TextPack을 보내지 않는다(`scouter.agent.batch`에 TextPack 사용 없음).
  - 서버 자체: object(`AgentManager.scala:185-189`, `ObjectRD.scala:26`), table(`SqlTables.scala:111`), city(`GeoIpUtil.scala:52`), group(`XLogGroupUtil.scala:77`, `IXLog.java:90`).
  - Client·webapp·Pulse plugin은 조회만 한다(`TextTypeEnum.java:28-44`, `HashText.scala:99-112`, Pulse plugin의 profile 변환기). `maria`는 쓰는 곳이 없고 조회 목록에만 있다.
  - 사용자 정의 div를 만드는 정상 경로는 원본에 없다. 제3자 Agent가 다른 div를 보낼 가능성만 남는다.

**Go 동작** (`internal/storage/text.go` `admitTextDiv`)
- 위 17개 div는 지금과 같이 상한 없이 처리한다(일별·영구 분기, 최초 값 우선, 빈 text 무시 모두 그대로).
- 그 밖의 div(이하 미지 div)는 영구 index가 있거나 이미 허용된 div면 그대로 저장한다. 새 미지 div는 영구 index를 가진 미지 div가 `DefaultMaxUnknownTextDivs`(64)개 미만일 때만 허용한다. 64는 원본 div 수(17)의 약 4배로 제3자 Agent의 사용자 div 여유를 둔 값이다.
- 개수에 드는 div: `00000000/text`에 `text_<div>.hfile`/`.kfile`/`.data`가 있는 미지 div(Java나 이전 실행이 만든 것 포함), 쓰기 queue에 text가 남아 있는 미지 div, 마지막 디렉터리 읽기 뒤에 허용한 미지 div다. 디렉터리는 첫 미지 div가 올 때 읽고, 상한에 닿은 상태에서 새 미지 div가 오면 최대 10초(`textDivRescan`)에 한 번 다시 읽는다. 그래서 허용됐지만 파일을 만들지 못한 div(쓰기 실패, queue 포화로 버림)와 파일이 지워진 div는 다음 재읽기 때 자리를 비운다. 허용 직후 아직 queue에 들어가기 전인 text는 마지막 읽기 뒤 허용 기록으로 남겨 세므로 재읽기가 그 자리를 빼앗지 않는다. 기존 미지 div는 개수가 상한을 넘어도 계속 저장한다.
- 디렉터리 읽기가 실패하면(권한, 파일로 바뀐 경로 등) 그 text는 storage 오류이고 상한 계수에는 넣지 않는다. 같은 오류를 10초 동안 기억해 미지 div pack마다 engine lock 안에서 다시 읽지 않는다. 알려진 div는 디렉터리를 읽지 않는다.
- 상한을 넘은 새 미지 div의 text는 queue에 넣지 않고 `ErrTextDivLimit`(`ErrLimit`)로 거절한다. 파일을 만들지 않고 조회에도 보이지 않는다. 빈 text는 이전처럼 오류 없이 무시한다.
- div 길이: 파일 이름 한도 255byte 안에 `text_<div>.hfile`이 들어가도록 244byte(`maxTextDivBytes`)를 넘는 div는 쓰기·조회 모두 `ErrInvalid`다. Java는 그런 파일을 열지 못해 text를 잃는다(예외 출력).
- 계수: storage `TextDivsLimited()`, ingest `Stats.TextDivsLimited`(`/health` `ingest`)와 `Stats.Rejected`. `StorageErrors`에는 넣지 않는다. 로그는 ingest가 10초에 한 줄(div 이름과 누적 수).
- 설정: Go 전용 `go_text_max_unknown_divs`(기본 64, 1..4096, live, 다음 text부터, [config CONTRACT](../../internal/config/CONTRACT.md) §6). `Store.SetMaxUnknownTextDivs(n)`로 연결한다(0=기본, 음수=Java처럼 무제한이지만 설정 키로는 고를 수 없다). 줄여도 이미 세어진 div는 계속 쓴다.
- **쓰레기 div가 자리를 채웠을 때 회복**: `/health`의 `TextDivsLimited` 증가와 ingest 로그(`text of div "..." not stored`)로 알 수 있다. ① 정상 div라면 `go_text_max_unknown_divs`를 올린다(재기동 불필요). ② 쓰레기 div라면 `00000000/text/text_<div>.hfile`·`.kfile`·`.data` 세 파일을 모두 지운다(하나라도 남으면 계속 센다). Client의 DB 삭제(`SERVER_DB_DELETE`, 세 경로를 파일 단위로 지정, 열린 handle은 닫힌다)로 실행 중에 지우거나, 서버를 멈추고 지운 뒤 기동한다. 실행 중에 지우면 다음 재읽기(최대 10초 뒤 상한에서 새 미지 div가 올 때) 때 자리가 빈다. 원인 송신지는 출처 allowlist나 방화벽으로 막는다(ingest CONTRACT §12 "R7").
- 일별 text(`{date}/text/text`)는 날짜별 공용 파일이고 미지 div는 들어가지 않으므로 이 상한과 무관하다. 알려진 div의 영구 `.data` 크기에는 원본과 같이 상한이 없다(남은 위험).

**원본과 차이**: Java는 미지 div 수에 상한이 없다. Go는 65번째부터 새 미지 div의 text를 버린다. 원본 Agent·서버·Client가 쓰는 div에는 영향이 없다.

검증: `TestUnknownTextDivsBounded`(수정 전 실패), `TestKnownTextDivsUnbounded`, `TestExistingUnknownTextDivKept`, `TestUnknownTextDivsConcurrentBound`(-race, 정확히 64), `TestUnknownTextDivScanErrorCached`, `TestUnknownTextDivsRescanFreesPlaces`(미기록 div·`00000000/text` 삭제 후), `TestTextDivLength`, ingest `TestUnknownTextDivsLimitedInIngest`, server `TestGoLimitsFromConfig`(live 연결).

## Realtime counter 이름 사전 `{date}/counter/real.head` (R6·R7, 의도된 차이)

**원본 동작**
- 형식: `RealtimeCounterDBHeader`(`db/counter/RealtimeCounterDBHeader.scala:58-163`)는 offset 512에 int32 body 길이를 쓴다. 이어서 int32 count와 (text 이름, int32 id) 쌍을 `StringIntMap` 순서(id 순서 아님)로 쓴다.
- 파일 크기: `openHeaderFile`이 2048byte 미만 파일을 padding한다.
- 쓰기: `RealtimeCounterWR`(`RealtimeCounterWR.scala:49-53`)는 pack마다 `intern(keySet)`으로 새 이름을 한꺼번에 넣는다. 새 이름이 있으면 `saveTags`가 길이 4byte와 body를 따로 제자리에 쓴다(fsync 없음, `:142-145`). 그래서 kill이나 ENOSPC 때 다음 형태가 남을 수 있다.
  - 길이는 새 값인데 body는 이전 값인 파일.
  - 2048byte 미만 파일 또는 0byte 파일.
- 읽기: 생성자(`:78-96`)는 길이가 양수일 때만 그만큼 읽는다. 짧게 읽힌 부분은 0이다. count개 entry만 읽고, 남는 byte는 무시한다. 예외가 나면 그때까지 읽은 entry만 남는다.
- 같은 이름이 여러 entry에 있으면 파일 순서상 마지막 id를 쓴다.
- 이름 수 상한이 없다. 사전은 날짜 전체가 공유하므로 크기는 Agent 수가 아니라 서로 다른 이름 수를 따른다.
- 이름 출처: 기본 counter 외에 CustomJmx·plugin counter가 이름을 더한다. Telegraf 입력은 tag 값을 넣은 이름을 만들 수 있다(`InfluxSingleLine.java:406-457`). 이런 이름은 출처 수에 비례해 는다.

**Go 동작** (`internal/storage/counter.go`, 상세는 [storage CONTRACT](../../internal/storage/CONTRACT.md) §8.6)
- 읽기는 Java처럼 동작한다. 파일을 0 padding된 것으로 보고, 길이나 count가 양수일 때만 읽으며, count개만 읽고 남는 byte는 무시한다. 같은 이름이면 파일 순서상 마지막 id를 쓴다. 그래서 위 torn 형태가 모두 열린다.
- 읽기 상한: body 64MiB, 이름 1,048,576개다. 넘으면 명시적 `ErrLimit`이다.
- 이전 동작: 4096개를 넘는 Java 사전, 또는 torn 형태가 있으면 그 날짜 realtime table을 열지 못했다. 그날의 조회와 쓰기가 모두 실패했고, pack마다 index walk를 다시 했다.
- 읽을 수 없는 사전: index를 열기 전에 실패한다. 파일이 바뀔 때까지 같은 오류를 기억한다.
- 쓰기: Java 이름을 포함한 날짜 사전 전체가 65,536개, body 16MiB 안일 때만 새 이름을 더한다(이전 4096).
  - 상한에 닿으면 새 이름의 entry만 빼고 같은 pack의 나머지 값은 저장한다. 이전에는 pack 전체를 `ErrLimit`으로 거절했다.
  - 뺀 entry 수는 `Counters.NamesLimited()`로 센다(`/health` `maintenance.counterNamesLimited`). 로그는 `S133`으로 10초에 한 번 남긴다.
- 저장 순서: 새 이름은 Java처럼 제자리에 저장한다. 읽은 마지막 entry 바로 뒤에 새 entry를 붙여 fsync하고, 그다음 길이·count 8byte를 써서 fsync한다.
  - Java가 쓴 entry의 byte와 순서는 바뀌지 않는다.
  - 새 파일은 temp → fsync → rename → 디렉터리 fsync로 만든다.

**원본과 차이**
- Java는 이름을 빼지 않는다. Go는 날짜 사전이 65,536개에 이르면 그 뒤 새 이름의 entry를 저장하지 않는다. Java가 쓴 사전이 이미 이 수보다 크면 그대로 읽고, 아는 이름은 계속 저장한다.
- 읽기 상한을 넘거나 entry가 손상된(id 범위 밖, 중복 id, 잘못된 UTF-8) Java 사전은 읽지 못한다(명시적 오류). Java는 예외 전까지의 entry로 계속한다.
- 같은 이름이 여러 id에 있는 사전에서 새 id는 Go가 count+1이다. Java는 서로 다른 이름 수 + 1이라 이미 쓰인 id와 겹친다(원본 결함 후보).
- 새 이름 저장 하나에 fsync가 2번이다(Java 0번, 이전 Go 1번 + rename). 비용은 CONTRACT §8.6에 있다.

**검증**
- `TestJavaStorageInteropRealtimeCounterNames`(`make storage-check`): 원본 class로 이름 5000개를 쓴다. Go가 읽고 새 이름을 더하고, Java가 읽고 다시 더한 결과를 Go가 읽는다(수정 전 Go 첫 조회 `ErrLimit`).
  - Go가 만든 날짜(createNames, 실패한 append가 남긴 꼬리, 부분 저장 record)는 `RealtimeCounterDBHelper.open`과 `RealtimeCounterRD.read`/`readBulk`가 부르는 원본 호출로 Java가 읽는다. `RealtimeCounterRD` 객체 자체는 oracle 컴파일 대상이 아니다.
- 단위 테스트:
  - `TestRealtimeCounterJavaDictionaryBeyondFormerLimit`(수정 전 Put `ErrLimit`)
  - `TestRealtimeCounterNamesJavaTornForms`(8가지, 수정 전 모두 실패)
  - `TestRealtimeCounterNamesLoadFailureBeforeIndex`(수정 전 index를 열고 walk)
  - `TestRealtimeCounterNamesRepeatedName`
  - `TestRealtimeCounterNamesLimitStoresKnownNames`, `TestRealtimeCounterNamesBodyBound`
  - `TestRealtimeCounterNamesSaveFailureRollsBack`, `TestRealtimeCounterNamesSaveFaults`
  - `TestRealtimeCounterNamesTornAppend`, `TestRealtimeCounterNamesRecreatedHead`, `TestRealtimeCounterNamesReadBounds`

## 검증 gate

Go 단위 테스트는 고정Object slot/덮어쓰기, 일별·영구Text/최초값, Java 모드override, XLog bucket경계와 V1/V2 순서, Profile 조각연결, owned 결과, 조회상한, corrupted/truncated data, cache eviction, concurrent lifecycle를 검사한다. Java가 만든 실제 DB를 Go가 읽고 Go DB를 Java Scala 원본 reader가 읽는 **양방향 oracle**는 별도 통합 검증으로 기록해야 한다. index와 Pack bytes fixture만으로 모든 DB의 상호운용을 완료 선언하지 않는다.

추가 운영 gate는 실제 Java/Host Agent와 Scouter Client, 기존 운영 DB 복사본, restart/crash/rollback, retention 삭제와 열린 파일 상호작용, 공유 서버 자원 한도/조회 병행 부하다. gzip writer는 record마다 member를 append하며 block을 재압축하지 않는다. 처리량은 [Phase 2E 검증](../validation/2026-10-13-phase2e.md)의 압축 replay 측정을 따른다. 전원 손실 시 count.dat와 block 사이의 fsync 순서는 보장하지 않는다(§recovery-retention G6). WAL, 다중 writer lock, 실제 Agent 연계는 이 저장 단위의 완료 주장에 포함하지 않는다. retention과 제한된 crash 복구는 [recovery-retention](recovery-retention.md)에 별도로 기록한다.

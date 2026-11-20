# storage 설정 키 계약

정본은 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. 파일 형식·날짜·복구 계약은 [storage-contract](../../docs/compatibility/storage-contract.md), retention 세부는 [recovery-retention](../../docs/compatibility/recovery-retention.md), 키별 Java 반영 시점은 [config-behavior](../../docs/compatibility/config-behavior.md) §1.5·§3을 따른다. 이 문서는 `scouter.conf` 키가 storage에서 **언제, 어떤 API로** 반영되는지만 다룬다.

storage는 `internal/config`를 import하지 않는다. live 값은 아래 setter로 받는다. setter는 원자 저장만 하고 Store gate를 잡지 않으므로 config reload callback(reload lock 안)에서 호출해도 긴 조회에 막히지 않는다. 주입 전에는 표의 "주입 전" 값으로 동작한다.

## 1. 키와 API

| 키 | Java 반영 시점 | Go API | 주입 전 | 비고 |
|---|---|---|---|---|
| `mgr_text_db_daily_service_enabled`, `_api_enabled`, `_ua_enabled` | 쓰기·읽기마다(`TextPermWR.isA`) | `Store.SetTextDaily(service, api, ua bool)` | `Config.TextDaily*`(기본 false) | §2 |
| `compress_xlog_enabled`, `compress_profile_enabled` | 날짜별 writer open | `Store.SetCompression(xlog, profile bool)` | `Config.CompressXLog/CompressProfile` | `.conf`가 없는 날짜의 첫 쓰기에서만 쓰인다. 기존 날짜 `.service.conf`/`.profile.conf`가 우선한다. `.conf`가 없는 날짜의 읽기 fallback도 이 값이다(Java reader와 같다) |
| `visitor_hourly_count_enabled` | Visitor마다(`VisitorCore.scala:74`) | `Store.SetVisitorHourlyCount(enabled bool)` | true | `Config.VisitorHourlyDisabled`는 삭제했다 |
| `_mgr_text_db_index_{default,service,api,ua,login,desc,hmsg}_mb`, `_mgr_text_db_daily_index_mb`, `_mgr_kv_store_index_default_mb` | 기동(object val) + 새 파일 생성 | `Config.IndexSizes`(`IndexSizesMB`, MiB) | text 1(`Config.HashSizeBytes`), KV **8** | 새 `.hfile`의 hash 영역 크기. 기존 파일은 파일 길이를 따른다(Java `MemHashBlock`과 같다) |
| `_mgr_xlog_id_index_mb` | 날짜별 `XLogIndex` 생성 | `Store.SetXLogIDIndexMB(mb int) error` | 1(`Config.HashSizeBytes`) | 이후 새로 만드는 `xlog_tid`/`xlog_gid`만 바뀐다. `xlog_tim`(864000B 고정)·object·profile·span·tag index는 키가 없어 `Config.HashSizeBytes` |
| `log_index_traversal_warning_count` | 조회마다 | `Store.SetIndexTraversalWarningCount(n)`, `Counters.SetIndexTraversalWarningCount(n)` | 100 | §3 |
| (로그 sink) | — | `Config.Log`, `Store.SetLog`, `Counters.SetLog` (`LogFunc(id, intervalSeconds, message)`) | nil(버림) | §3 |
| `mgr_purge_enabled`, `mgr_purge_*` 11개 | ConfObserver + 1분 loop | `Retention.Apply(enabled bool, JavaRetentionConfig(set)) (changed bool)`, `Retention.Settings()` | `Retention.Config`, 켜짐 | §4. Go 기본 off(키 없음)는 배선 쪽 결정으로 유지 |
| `_compress_read_cache_block_count` | ConfObserver(`IOChannel` readCache 행 수) | `Store.SetCompressReadCacheBlocks(n)` | 4 | §5 |
| `_compress_dailycount_header_cache_size` | 사용 시마다(`IOChannel.check`) | `Store.SetCompressDailyCountCacheSize(n)` | 8 | §5 |
| `_compress_write_buffer_block_count`, `_compress_read_cache_expired_ms`, `_compress_write_thread` | — | 없음 | — | §5 구조 차이 |

`*_mb` 값 범위는 0..`MaxIndexMB`(64)다. `Open`은 범위 밖 `IndexSizes`를 `ErrInvalid`로 거절하고 `SetXLogIDIndexMB`는 오류를 반환하고 이전 값을 유지한다. 0은 "미설정"(기본값)이다. Java는 0이면 `MemHashBlock` capacity 0으로 매 조회가 `ArithmeticException`이 되고 2048 이상이면 int overflow가 난다. Go는 이를 재현하지 않는다(의도된 차이).

## 2. 일별 text DB(S1)

- 쓰기(`PutText`)는 `TextWR.process`다. `error`는 항상 일별, `service`/`apicall`/`ua`는 live 값, 그 밖(`sql`, `table`, `city`, `login`, `desc`, `group`, …)은 항상 영구다. 일별은 `{date}/text/text.{hfile,kfile}`(key=CRC32(div)+hash, value=UTF-8), 영구는 `00000000/text/text_{div}.{hfile,kfile,data}`다. 먼저 쓴 값이 남는다(hasKey). 영구는 빈 문자열을 버린다. 일별은 빈 문자열도 쓴다.
- 읽기(`Text`/`TextContext`)는 `TextRD.getString`에서 TextCache를 뺀 것이다. 영구 div는 `text_{div}`만, 일별 div는 그 날짜 table → **miss면 `text_{div}`**(`TextRD.scala:40-41`)다.
- **D-T1(일별→영구 전환 뒤 일별 전용 텍스트 미조회)은 Java와 같게 둔다.** 근거: Java는 읽기마다 `isA`를 평가해 영구 사전만 본다. Go가 일별 table까지 찾으면 같은 설정에서 Java 서버와 다른 GET_TEXT 결과가 나와 이관·병행 비교 기준이 깨진다. 다시 일별로 켜면 보인다(파일은 지우지 않는다).
- 영구 사전 쓰기 API: SqlTables `table`, GeoIP `city`는 별도 API 없이 `PutText(date, &TextPack{XType: "table"|"city", ...})`가 `text_table`/`text_city`에 쓴다(`isA`=true). 테스트 `TestTextTableAndCityArePermanent`.
- 의도된 차이: Java `TextRD`는 일별 div 조회 때 그 날짜의 text 파일을 만든다(`TextWR.open`). Go 읽기는 파일을 만들지 않고 없으면 miss로 본다. TextCache·TextDupCheck(메모리 중복 제거)는 storage에 없다.

## 3. index traversal 경고(S5)

Java `IndexKeyFile.get`/`IndexKeyFile2.get`은 chain을 끝까지 돌고도 key를 못 찾으면, `IndexKeyFile2.updateOrPut`은 chain을 끝까지 돌면(폭이 다른 match에서 바로 append하는 경우 제외), `setTTL`은 못 찾으면 `looping > log_index_traversal_warning_count`일 때 경고한다. looping은 삭제 record를 포함한 chain record 수다. Java가 `hasKey`/`getAll`을 쓰는 곳(Text 쓰기의 중복 확인, Profile·gxid 조회)은 경고하지 않는다.

| Go 경로 | Java | id | 메시지 key |
|---|---|---|---|
| `PutObject`의 기존 slot 조회 | `ObjectWR` `index.get` | S152 | `DataInputX.toInt(key,0)` |
| `Text` 조회, `XLog(txid)` | `TextTable`/`TextPermIndex.get`, `XLogIndex.getByTxid` | S152 | 같음(8byte key는 앞 4byte) |
| daily counter 쓰기·조회 | `DailyCounterWR`/`DailyCounterRD` `index.get` | S152 | 같음 |
| KV 조회 miss | `IndexKeyFile2.get` | S152 | `new String(key,"UTF8")` |
| KV 쓰기, TTL 변경 miss | `updateOrPut`, `setTTL` | S161 | 같음 |

메시지는 `[warn] Too many index deep searching. <key>`이고 `LogFunc(id, 10, msg)`로 낸다. Java `Logger.println(id, 10, …)`처럼 id별 10초 간격을 storage가 먼저 적용한다(`mgr_log_ignore_ids`는 sink 담당). sink는 대부분의 경로에서 Store gate와 index lock 안에서 불리므로 빨리 반환하고 Store를 호출하면 안 된다. 단건 `XLog(txid)`는 `legacyindex.KeyChain.LookupContext`로 walk하므로 gate·lock 밖에서, 다른 조회와 동시에 불린다(§6). 그래도 요건은 같다(빨리 반환, Store 호출 금지, 동시 호출에 안전). 구현: `legacyindex.Index.LookupContext`·`KeyChain.LookupContext`·`TTLIndex`의 `SetDeepSearch(DeepSearchFunc)` 관찰자.

## 4. retention(S2)

`Retention.Apply`는 `mgr_purge_enabled`와 `JavaRetentionConfig`로 만든 값을 원자적으로 바꾸고 다음 `RunOnce`부터 쓴다. 꺼져 있으면 `RunOnce`는 아무것도 하지 않는다(Java loop의 `if (conf.mgr_purge_enabled)`). Java ConfObserver는 값이 바뀌면 삭제 이력 set 8개를 비운다. Go는 이력을 두지 않고 매 cycle 디스크의 날짜를 다시 나열하므로 비울 것이 없고 결과 파일 집합이 같다(아래 oracle). `Apply`의 반환값은 Java 비교 조건(값 변화)과 같은 의미로 로그 용도다. `Apply`를 한 번도 부르지 않으면 `Retention.Config`를 켜진 상태로 쓴다(기존 호출자 호환).

## 5. 압축 저장 `_compress_*`

Go GZipStore는 record마다 gzip member를 바로 append한다([storage-contract](../../docs/compatibility/storage-contract.md) "압축 DB"). 대응하는 상한이 있는 키만 연결했다.

- `_compress_read_cache_block_count` → 해제 block cache 수(기본 4, 32MiB 논리 크기 각각). Java `CacheTable.setMaxRow`는 0 이하를 무제한(만료 60s)으로 본다. Go는 만료가 없으므로 0 이하와 `MaxZipCacheBlocks`(8) 초과를 8로 둔다. 줄이면 다음 GZip 작업에서 즉시 LRU로 줄인다.
- `_compress_dailycount_header_cache_size` → 날짜별 writer 위치(count.dat) 수(Store 기본 8. 서버 설정 기본값 3이면 2). Java `check()`는 `size >= n-1`이면 비우므로 n-1개만 남고 n≤1이면 무한 loop다. Go는 `max(n-1, 1)`, 최대 `MaxZipWriters`(64)다. 밀려난 날짜는 다음 쓰기에서 count.dat를 다시 검증해 연다.
- 대응 없음(만들지 않음): `_compress_write_buffer_block_count`(Go는 쓰기 buffer block이 없다. 쓰자마자 member를 기록), `_compress_write_thread`(비동기 압축 thread pool 없음), `_compress_read_cache_expired_ms`(cache 시간 만료 없음. 크기 상한과 파일 identity 검사로만 무효화). 만들면 queue/cache와 비동기 소유권이 늘어 KISS에 어긋나고 crash 계약이 바뀐다.

### 5.1 writer load의 block 재decode 방지 (2026-10-24)

Go writer는 날짜의 첫 append 전에 count.dat를 그 block과 대조한다(`loadZipWriter`). 이 검증은 Store gate 안에서 block 전체(최대 논리 32MiB)를 decode한다. Java `IOChannel.getLastWriteBlock`은 count.dat만 읽고 검증하지 않는다. 이 검증은 Go가 추가한 안전장치다. 검증 결과, 복구 규칙(`zip-torn-member`, `zip-count-advanced`), count.dat 의미(논리 next offset), 파일 byte는 바뀌지 않는다. 바뀐 것은 같은 block을 반복해서 decode하지 않는다는 점뿐이다. 이전에는 아래 세 경우에 레코드마다 decode가 반복되었다. pack writer는 hold 한 번에 최대 256건을 레코드별로 쓰므로 hold 하나가 수 초 이상 걸릴 수 있었다.

- **writer 재적재가 cache를 검증 입력으로 씀**: decode 대신 해제 block cache 항목을 쓰는 조건은 셋이다. (1) 같은 파일이다(`sameZipFile`: identity, 크기, mtime). (2) 항목의 `end`가 파일 크기와 같다. (3) 항목이 있다. `end`는 마지막 완전한 member 뒤의 파일 offset이고, 항목 data는 `[0, end)`의 decode다. `DecodeZipBlock`은 끝까지 완전한 member로 된 파일에서 `Torn`=0, `End`=크기를 내고, 그때 `Data`는 각 member의 해제 byte를 이은 것이다. cache 항목이 그 값과 byte 단위로 같은 이유는 만든 곳마다 다르다.
  - 읽기 decode(`zipDataFetch`, `useFetchedLocked`): 같은 파일 상태의 decode 결과 자체다. 끝에 torn member가 있으면 `end` < 크기이므로 재사용하지 않는다.
  - writer append(`appendZip`): `[0, w.end)`와 같은 data에 이번 member의 해제 byte(head 8 + payload)를 이은 것이다. member는 그 byte를 압축한 gzip member이고, 파일에는 `w.end`부터 기록된다.
  - writer load: decode 결과이거나, torn member를 잘라낸 뒤의 `[0, End)`다.
  - 검증 입력이 같으므로 결과(거부, 복구, writer 위치)도 decode할 때와 같다. 남은 위험은 기존 읽기 cache와 같다. 크기와 mtime이 같은 외부 덮어쓰기는 알아채지 못한다.
- **member 쓰기 실패**(ENOSPC·EIO 등): 쓰다 만 member를 `w.end`로 잘라낸다(이전과 같음). 그 앞의 byte는 바뀌지 않았으므로 cache 항목은 지우지 않고 stat만 갱신한다. 조건은 셋이다. 잘라내기 뒤 크기가 `w.end`이고, 같은 파일(`os.SameFile`)이며, 쓰기 전에 성공 경로와 같은 검사(cache 크기·`end` == `w.end`)를 통과한 항목이어야 한다. writer는 이전처럼 버리고, 다음 append가 다시 적재·검증한다. 그 적재는 decode 없이 cache로 한다. 잘라내기나 stat이 실패하면 이전처럼 cache도 버리고, 다음 적재는 decode한다.
  - 남은 위험: 쓰기 전 mtime은 비교하지 않는다. 비교하려면 레코드마다 fstat이 하나 더 필요하고, 잘라내기가 mtime을 바꾸므로 뒤에서는 비교할 수 없다. 성공 경로도 append마다 크기만 본다. 그래서 크기를 바꾸지 않은 외부 덮어쓰기는 두 경로 모두 알아채지 못한다.
  - count.dat 쓰기 실패는 cache가 이미 새 member를 담고 있으므로 같은 방식으로 재사용된다. 다음 적재가 decode 없이 count를 진행시킨다(`zip-count-advanced`).
- **검증 거부 기억**(`zipState.failed`): 적재가 `ErrCorrupt`·`ErrLimit`로 거부되면(count가 block보다 큼, member 손상, 복구할 수 없는 count 뒤 tail, later block 등) 날짜별로 거부 오류를 기억한다. 열쇠는 적재가 읽은 것 전부다. count.dat 값, 실제로 읽은 count.dat·count block handle의 stat, data 디렉터리(목록: later block)와 다음 block(rollover orphan, 경로로 엶) 경로의 stat이다(없으면 없음). Store file cache는 경로가 바뀐 뒤에도 이전 파일의 handle을 쥐고 있을 수 있다. 그래서 mv로 파일을 바꾼 수동 복구는 그 handle이 cache에서 밀려나 다시 열린 뒤에 읽히고 검증된다. 그 전에는 기억이 없어도 같은 파일을 읽어 같은 오류가 난다. 열쇠가 같은 동안 다음 적재는 decode 없이 같은 오류를 돌려주고, 파일은 이전처럼 건드리지 않는다. 하나라도 바뀌면(수동 복구, 잘라냄, mtime 변경, count.dat 변경, block 추가·삭제) 다시 검증한다. purge·DB 삭제(`zipReleaseLocked`)는 그 날짜의 기억을 지운다. 적재가 성공하면 기억을 지운다. 상한은 writer 위치 수와 따로 고정한 `zipLoadFailures`(= `MaxZipWriters` 64, LRU)다. 거부된 날짜가 writer 상한보다 많이 번갈아 써져도 다시 decode하지 않게 하려는 것이다.
  - I/O 오류(파일 읽기, stat, `ReadDir`, 잘라내기, count.dat 쓰기)는 기억하지 않는다. 파일 변경 없이 풀릴 수 있기 때문이다(ENOSPC 해소 등). 그래서 잘라내기까지 실패하는 지속 I/O 오류에서는 레코드마다 decode가 남는다.
  - count.dat 자체의 형식 오류(8 byte 아님, 범위 밖)는 decode 전에 나는 오류라 비용이 작아 기억하지 않는다.
- **gate 안 decode는 남는다**: 재기동 뒤 첫 적재(날짜당 1회, XLog·profile·objType이 같은 날짜 저장소를 공유), cache에서 밀려난 block의 재적재, 쓰기 실패에서 cache가 없던 경우다. 정상 경로에서 이 비용은 날짜당 1회이고 pack queue가 흡수한다. gate 밖 decode(zipFetch 방식)는 pack writer batch의 순서·오류 전파를 바꿔야 해서 하지 않았다.
- 검증: `zip_reload_test.go`
  - `TestZipWriterReloadReusesCachedBlock`: writer 1개로 두 날짜를 번갈아 쓸 때 재적재 decode 0회.
  - `TestZipWriterWriteFailureReloadWithoutDecode`: member를 반만 쓰고 ENOSPC를 3번 낸 뒤 decode 0회. 같은 offset에서 재개하고 파일이 정확함.
  - `TestZipWriterLoadFailureRemembered`: count가 block보다 큰 상태에서 5회 append해도 decode 1회이고 파일 불변. count.dat 변경, block 잘라냄, mtime 변경, later block 삭제, purge 뒤에는 다시 검증하고, EIO는 기억하지 않음.
  - `TestZipLoadFailuresBounded`: 상한 64.
  - `TestZipLoadFailureKeyFollowsHandles`: mv로 count.dat·block을 바꾸면 handle이 다시 열린 뒤 검증하고 성공한다.
  - `TestZipLoadFailuresBeyondWriterBound`: writer 1개, 거부 날짜 3개를 3바퀴 돌아도 decode 3회.
  - `TestZipLoadSkipsTornReadCache`: 읽기가 cache한 torn block은 재사용하지 않고 decode한 뒤 `zip-torn-member`.
  - `TestZipLoadReusesFetchedTail`: fetch와 tail 결합으로 만든 cache를 decode 없이 재사용.
  - `TestZipLoadDecodesExternallyChangedBlock`: 외부 append·mtime 변경 뒤에는 decode.
  - `TestZipCountWriteFailureReloadWithoutDecode`: decode 0회, `zip-count-advanced` 1회.
  - `TestZipWriteAndTruncateFailureDecodesAgain`: 잘라내기까지 실패하면 cache를 버리고 decode.
  - `TestZipRolloverWriteFailureResumes`: rollover block의 첫 member 실패 뒤 decode 없이 재개.

## 6. 범위 조회 streaming(Client 조회)

Java `XLogRD`/`SummaryRD`/`AlertRD`/`StatusRD`의 `readByTime*`는 handler에 레코드를 읽는 대로
넘기고, handler가 filter·max를 적용하며, 총 건수·byte 상한이 없다. Go는 같은 순서로 streaming한다.

- `ScanXLogsContext(ctx, XLogScan, visit)`: V1(`ScanRange` 순서, reverse 포함)과 V2(`Paged`,
  500ms bucket chain 순서, `XLogRecord.BucketTime`). `Match`(store gate 안, 레코드마다 한 번, 순서대로)와
  `Max`(match 수)로 scan 안에서 멈춘다. `ScanSummariesContext`, `ScanAlertsContext`와 내부
  `streamTimedPacks`는 Status/Summary/Alert의 forward 순서다.
- batch: store gate(`s.mu`)를 잡은 동안 최대 512건 또는 보관 4 MiB까지 batch를 채우고, gate를 놓은 뒤
  `visit`을 호출한다. ingest는 Client socket write를 기다리지 않는다. 다음 batch는 cursor에서 이어 읽는다.
- **gate 보유 상한**: gate를 한 번 잡을 때마다 index chain 레코드·data 레코드·bucket 단계를 합쳐
  `scanHoldSteps`(256)개 또는 `scanHoldTime`(2ms) 중 먼저 닿는 쪽에서 gate를 놓는다(시각은 16단계마다
  확인하고, 한 번 잡으면 최소 1단계는 진행한다). `Match`가 모두 버리는 조회도 같다. 구간 크기나 bucket
  하나의 레코드 수와 무관하게 ingest 대기는 한 번의 보유 시간(ms 수준)으로 묶인다. 이전에는 filter가
  버린 레코드가 batch를 채우지 않아 읽은 byte 16 MiB까지 한 번에 잡았고 V1 bucket은 통째로 읽었다
  (일치가 없는 넓은 구간 조회 하나가 수집을 초 단위로 막을 수 있었다).
  - V1·timed: bucket chain을 `legacyindex.WalkRangeStep`으로 chain 위치에서 이어 걸으며 compact
    `legacyindex.TimeEntries`(레코드당 16 byte + value byte)에 모은다. 다 모으면 `RangeBucket`과 같은 순서
    (time, signed byte value 오름차순, TreeSet 중복 제거, reverse, 끝점 filter)로 정렬해 읽는다. 1024개를
    넘는 bucket의 정렬은 gate 밖(`settlePending`)에서 한다. bucket당 상한은 기존 index 상한
    `MaxResultRecords`(100000)/`MaxResultBytes`(64MiB) 그대로이고 xlog·timed index의 메모리는 bucket당
    약 2.1MiB 이하다. 정렬 순서를 만들려면 bucket 전체가 필요하므로 메모리 단위는 chunk가 아니라 이
    compact bucket이다. 수집을 시작한 뒤 그 bucket head 앞에 붙은 레코드는
    보지 않는다(이전의 한 번 snapshot과 같은 결과).
  - V2: chain 위치(`Record.Position`/`PreviousPosition`)에서 `WalkBucketFrom`으로 이어 걷는다. scan이
    지나간 bucket의 head 앞에 새로 붙은 레코드는 보지 않는다(이미 head를 지난 Java reader와 같음).
  - 중간에 index가 없어지면(purge) 끝, Store가 닫히면 `ErrClosed`, ctx 취소는 레코드마다 확인한다.
- 총 건수·총 byte 상한 없음. slice를 반환하는 API(`XLogsContext`, `XLogsV2Context`, `StatusesContext`,
  `SummariesContext`, `AlertsContext`, `QueryListContext`)는 **수집 결과**에만
  `MaxQueryRecords`/`MaxQueryBytes`(timed는 `extraQueryBudget`) 상한을 둔다. filter가 버린 레코드는
  세지 않는다(이전: filter 전 scan 건수·byte).
- realtime counter 이력(`Counters.RealtimeContext`)은 scan byte 64MiB 상한을 없앴다. 레코드당 8MiB와 결과
  100000 point 상한은 유지한다.
- realtime counter 조회(`RealtimeContext`, `CounterRowsContext`, `CounterBulkRowsContext`)도 XLog scan과 같은
  상한으로 Counters gate(`c.mu`)를 나누어 잡는다(`scanRealtimeHolds`). PerfCounterPack 수집
  (`Counters.PutContext`, ingest writer, §8.4)은 hold 한 번(ms 수준)만 기다린다. 이전에는 24시간 조회 하나가 scan
  내내 gate를 잡아, 그동안 engine lock 안에서 기다리는 counter 수집 때문에 XLog 수집 전체가 멈출 수 있었다.
  - hold마다 `scanHoldSteps`(256) 또는 `scanHoldTime`(2ms)까지만 진행하고, batch는 512건/4MiB다.
  - gate 안에서는 index 순회와 data 레코드 읽기만 한다. index 순회는 V1과 같은 `rangeCursor`
    (`WalkRangeStep`+`TimeEntries`)를 쓴다. 레코드 decode·이름 매핑·결과 append는 gate 밖에서 scan 순서대로
    한다.
  - 결과(레코드, 순서, 100000 point·`counterCatalogRecords`·64MiB `ErrLimit`, 오류)는 한 번에 잡던 때와 같다
    (`TestRealtimeCounterScansMatchOneHold`, 이전 구현 oracle). 오류는 scan 순서상 먼저 실패한 레코드의 것이다.
  - 이름 사전은 hold 시점 slice를 gate 밖으로 넘긴다. `putRealtime`은 id를 덧붙이기만 하고 저장 실패
    rollback도 같은 hold 안에서 하므로 안전하다. 이름→id map(`counterTable.ids`)은 gate 안에서만 쓴다(§8.6).
  - bulk의 cursor는 (60초 chunk, chunk 안 물리 위치)다. chunk의 끝 위치는 그 chunk를 시작한 hold에서 정한다.
    endpoint 탐색 1회(chunk당 최대 121 bucket chain)를 1단계로 센다.
  - scan 중 추가된 레코드: 수집을 시작한 bucket(bulk는 chunk)에 이후 붙은 레코드는 보지 않는다. 아직 도달하지
    않은 bucket의 레코드는 본다.
  - hold 사이 eviction(table 8개 상한)으로 다시 열린 table은 같은 파일이라 이어 읽는다. 그 날짜의
    purge·DB 삭제(날짜별 횟수 `markPurgedLocked`)가 있었거나, table이 없거나, data 파일이 바뀌면
    (`os.SameFile` 불일치 또는 크기 감소) 오류 없이 끝나고 그때까지의 결과를 반환한다. 날짜별 횟수 때문에
    재생성된 파일이 inode를 재사용해도 구별한다. 다른 날짜의 purge는 영향이 없다.
  - Close 뒤에는 `os.ErrClosed`다. ctx 취소는 레코드마다 확인한다.
  - `DailyCounterNamesContext`(5m index 물리 전체 scan, 살아 있는 레코드 100000/64MiB 상한)는 아직 한
    hold다(남은 위험: legacyindex에 물리 위치 재개 API가 없음).
- **여러 객체의 realtime counter 이력**(`RealtimeMultiContext`, `COUNTER_PAST_TIME_ALL`·`COUNTER_PAST_TIME_TOT`):
  index 값에 data 위치와 objHash가 함께 있으므로, 범위의 index를 한 번만 걸으며 요청한 objHash의 data만 읽는다.
  이전에는 객체마다 같은 범위의 index를 처음부터 걸었다(objType 전체 조회가 객체 수만큼 느림). Java도
  `RealtimeCounterRD.readBulk`로 범위를 한 번 읽는다. 객체별 결과(순서, 반복·역순 시각, 값)는 `RealtimeContext`와
  같다(`TestRealtimeMultiMatchesPerObject`, `RealtimeContext`도 이 경로를 쓴다). 결과는 point마다 시각과
  encode된 값만 담는다(point당 약 13 byte). 객체당 100000 point와 전체 값 256MiB를 넘으면 `ErrLimit`이다.
  응답은 객체 하나씩 decode해 보낸다.
- **batch archive**(`PutBatchArchiveContext`/`BatchArchiveContext`): Store gate는 짧은 구간에만 잡는다.
  - 업로드에서 gate를 잡는 구간은 두 곳이다. (1) 검증, `MkdirAll`, 대상 디렉터리의 `.batch-archive-*` temp
    생성. (2) 대상 재검증(symlink·디렉터리로 바뀌면 `ErrInvalid`, 닫혔으면 `ErrClosed`), cache handle 교체,
    rename.
  - Agent body 읽기(최대 `MaxQueryBytes` 64MiB, 호출자 deadline), temp fsync, rename 뒤 디렉터리 fsync는
    gate 밖이다. 그래서 느린 업로드가 PutXLog를 막지 않는다(이전에는 최대 30초 deadline 동안 막을 수 있었다).
  - rename 전 실패·취소·짧은 업로드는 보이는 변화가 없고 temp를 지운다. rename 뒤 디렉터리 fsync 오류나 취소는
    publish 뒤에 오류로 보고될 수 있다(기존과 같음).
  - 업로드 중 purge·DB 삭제가 시간 디렉터리를 trash로 옮기면 rename이 실패한다(ENOENT). 아무것도 publish하지
    않고 디렉터리를 다시 만들지 않는다.
  - 같은 key 동시 업로드는 각자 temp에 쓰고 마지막 rename이 이긴다. 조회는 항상 한쪽의 완전한 archive를 본다.
  - gate가 직렬화하던 몫은 `batchArchiveConcurrent`(4)가 대신한다. 업로드와 조회가 각각 동시에 4개까지
    진행하고, 그 이상은 자리를 ctx가 끝날 때까지 기다린다. 그래서 temp 디스크는 최대 4×64MiB, 조회 메모리는
    최대 4×64MiB로 묶인다(`TestBatchArchiveConcurrencyBound`). 기다리는 쪽은 그 Agent·Client 요청뿐이다.
  - 조회는 gate 안에서 검증하고 전용 read-only handle을 연다. chunk 읽기는 gate 밖이다. 연 inode를 끝까지
    읽으므로 동시 교체·purge에도 결과가 섞이지 않는다. 조회는 Store data handle cache를 쓰지 않는다.
- **tag actual 목록**(`TagActualContext`, tag count에서 실제 XLog·alert 목록): XLog scan과 같은 `rangeCursor`와
  hold 상한으로 Store gate를 나누어 잡는다. 결과(저장된 byte, 순서, txid cursor, filter, Max, reverse, scan
  건수·byte 상한 `extraQueryBudget`, 오류)는 한 hold로 읽던 때와 같다(`TestTagActualHoldsMatchOneHold`, 이전
  구현 oracle). hold 사이 purge는 그때까지의 결과로 끝난다.
- **key 하나의 수집 조회**(`ProfileChunksContext`·`Profile`, `XLogsByGxIDContext`, `SpansByGxIDContext`,
  profile.go): 이전에는 index scan·정렬·record 읽기(profile 최대 64MiB, gzip 해제 포함)를 끝날 때까지 한 번의
  Store gate hold로 했다. 이제 다음처럼 나눈다.
  - index hold: key의 hash chain snapshot(`legacyindex.KeyChain`: bucket head, kfile 길이, 조회 전용
    read-only kfile handle)을 잡고 compression mode를 읽는다. 비압축이고 chain이 비어 있지 않으면 같은 hold에서
    data 파일(`xlog.profile`·`xlog.service`·`span.data`)을 조회 전용 read-only handle로 연다. chain 길이와
    무관하게 이 hold는 head 하나 읽기와 open 몇 번이다.
  - chain walk는 gate 없이 그 snapshot으로 한다. index 값(data 위치)을 모으고 상한은 기존과 같다(profile은
    `MaxQueryRecords` 초과 시 즉시 `ErrLimit`, gxid·span은 `MaxQueryRecords`개까지 모으고 하나 더 있으면 그
    앞 레코드를 다 처리한 뒤 `ErrLimit`). Store는 이 index들(`xlog_profile`·`xlog_gid`·`span_gid`)에 append만
    한다. `Store.index`가 이 셋과 `xlog_tid`(아래 단건 조회)를 `SetAppendOnly`로 열어 제자리 갱신(Update/Delete)은
    `ErrAppendOnly`로 실패한다(`TestKeyChainIndexesAreAppendOnly`). append는 snapshot 길이 뒤에 쓰고 그 앞 레코드만 link하므로 chain 레코드는 바뀌지 않는다. 그래서 walk
    결과는 index hold 시점에 한 번에 걸은 결과와 같다. handle이 inode를 유지하므로 그 사이 append·Sync·eviction·
    purge·DB 삭제·Close와 무관하다. profile의 compression mode 오류는 이전처럼 walk 오류 다음에 보고한다.
    조회 하나가 fd를 최대 둘(kfile, data) 잠시 쓴다. 이전에는 walk를 gate 안에서 했다(남은 위험이던 긴 chain:
    한 bucket에 30만 레코드인 gxid 조회가 수집을 약 140ms 막음, 아래 측정).
  - 비압축 record는 gate 없이 그 handle로 읽는다(batch archive 조회와 같은 방식). index가 가리킨 record는
    이미 다 쓰였고 다시 쓰이지 않으며, handle은 연 inode를 유지한다. 그래서 그 사이 purge·DB 삭제·재생성·
    Close가 있어도 결과가 섞이지 않고, 조회가 먼저 끝났을 때의 결과와 같다. 조회는 Store handle cache를
    쓰지 않으므로 eviction과도 무관하다. 조회 하나가 fd 하나를 잠시 쓴다.
  - 압축 record(GZipStore, block cache를 gate가 보호)는 hold마다 최대 `scanHoldSteps`(256)개 또는
    `scanHoldTime`(2ms, record마다 확인, hold마다 최소 1개)까지 읽는다. cache에 없는 block은 아래 "압축 block
    해제"처럼 hold를 끝내고 gate 밖에서 해제한 뒤 같은 record부터 다음 hold에서 읽는다. hold마다 먼저 Store가 닫혔으면
    `ErrClosed`, index hold 뒤 그 날짜의 xlog 디렉터리(또는 그 안)를 건드린 purge·DB 삭제가 있었으면
    (`packSeq`·`packDiscards`, `xlogPurgedSinceLocked`) **빈 결과(nil, 오류 없음)**로 끝난다. purge 뒤에
    시작한 조회의 결과(index 없음)와 같고, 재생성된 같은 offset의 다른 record를 섞지 않는다. hold 사이에
    purge·삭제 기록이 상한(64개·1시간, DB 삭제는 대상당 1개)을 넘게 쌓여 판단할 수 없으면 추측하지 않고
    index hold부터 다시 조회한다(`collectAttempts` 3회). 매번 그러면 재시도 가능한 `ErrPurgeOverlap`이다
    (이전: purge되지 않은 날짜도 빈 결과).
  - 결과(byte, 순서, 상한, 어느 record의 오류가 먼저인지)는 이전 한 hold 구현과 같다
    (`TestCollectQueriesMatchOneHold`, `TestCollectQueriesMatchOneHoldSharedChain`(bucket 1개: 모든 key가 한
    chain), 이전 구현 oracle, 압축·비압축, 상한, 잘린·없는 data 파일).
  - 단건 `XLogContext`(txid, Java `XLogRD.getByTxid`, `IndexKeyFile.get`: 가장 최근의 살아 있는 레코드)는
    chain 길이로 두 경로를 나눈다. 이전에는 chain 길이와 무관하게 index walk와 record 읽기를 한 hold로 했다
    (hash 충돌이나 작은 hash 영역으로 chain이 길면 그 walk 동안 수집이 멈춤, 아래 측정).
    - 짧은 chain(일반적인 경우, txid는 key가 거의 유일): hold 하나(`lockScan`, hold 관찰 포함)에서 chain을 다른
      hold와 같은 상한, 즉 `scanHoldSteps`(256) 레코드(삭제 레코드 포함) 또는 hold 시작부터 `scanHoldTime`(2ms) 중
      먼저 닿는 쪽까지만 걷는다(`legacyindex.Index.LookupWithin`, 시각은 16레코드마다 확인). 그 안에서
      hit·miss·오류로 끝나면 이전 구현 그대로 Store handle로 record를 읽는다(압축 block miss면 gate 밖 해제 뒤 새
      hold에서 조회를 처음부터). 상한에 닿으면 아무것도 보고하지 않고 긴 경로로 간다. page cache에 있는 256레코드
      miss는 약 125µs라(로컬, `BenchmarkTxidLookupChainLimit`) 보통은 레코드 수가 먼저 닿는다. 레코드마다 kfile
      `ReadAt`이 있으므로 cold page cache나 느린 디스크에서는 256레코드가 ms 단위(레코드당 디스크 읽기 하나)가 될 수
      있어 시간 상한을 둔다. gate 안 walk는 2ms에 최대 16레코드 읽기(확인 간격) 몫을 더한 시간으로 묶인다(range scan
      hold의 16단계 확인과 같은 성격).
    - 긴 chain(256 초과 또는 시간 상한 도달): 수집 조회와 같은 방식이다. 같은 조회의 새 hold에서 `xlog_tid` chain snapshot과
      compression mode를 잡고(비압축이고 chain이 비어 있지 않으면 `xlog.service` read-only handle도 연다),
      chain walk는 gate 없이 `KeyChain.LookupContext`로 한다. 비압축 record는 그 handle로 gate 없이, 압축
      record는 hold 하나에서 읽는다(block miss면 위처럼 gate 밖 해제 뒤 같은 record를 다음 hold에서). 조회 하나가
      fd를 최대 둘(kfile, `xlog.service`) 잠시 쓴다. KeyChain을 열고 닫는 비용(로컬 약 70–150µs) 때문에 짧은
      chain에는 쓰지 않는다(짧은 chain만으로 쓰면 hit 조회가 2.7µs에서 150µs로 느려졌다).
    - 결과(record, newest 우선, 없는 txid·index·날짜, 잘못된 index 값·다른 txid record의 `ErrCorrupt`, 잘린·없는
      data, mode 파일 오류는 lookup 뒤 record 읽기에서 보고, 취소)는 두 경로 모두 이전 한 hold 구현과 같다
      (`TestXLogLookupMatchesOneHold`, `TestTxidLookupShortAndLongChains`(255·256·257 경계, 257은 긴 경로 확인),
      `TestTxidMissRetryTurnsLong`(압축 miss 해제 중 같은 chain에 300개 append → 재조회가 긴 경로), 이전 구현 oracle,
      압축·비압축, bucket 1개. legacyindex `TestLookupWithinLimits`: 길이 == limit 경계, 지난 deadline).
    - S152: Java처럼 miss일 때만, chain을 끝까지 돈 레코드 수(삭제 레코드 포함, 빈 chain은 0)가
      `log_index_traversal_warning_count`보다 크면 같은 텍스트(`[warn] Too many index deep searching. <key 앞
      4byte int>`)로 id별 10초 간격으로 낸다(§3). 짧은 chain은 gate·index lock 안에서, 긴 chain은 snapshot이
      index의 `DeepSearchFunc`을 가져가 walk 뒤에 부르므로 gate·lock 밖에서 불린다(sink 요건은 §3 그대로,
      `TestTxidDeepSearchConcurrentWithInGateReports` -race). 256에서 멈춘 짧은 경로는 보고하지 않고 긴 경로가
      끝까지 걸은 길이로 한 번 보고한다. 발생 조건·텍스트는 oracle과 같다(`TestXLogLookupMatchesOneHold`,
      `TestKeyChainLookupMatchesLookupContext`).
    - 긴 경로의 hold 사이 purge·DB 삭제·Close: 비압축은 index hold의 handle로 읽으므로 조회가 먼저 끝났을 때의
      결과, 압축은 purge·삭제면 빈 결과(nil, 오류 없음), Close면 `ErrClosed`(`TestTxidLookupBetweenHolds`,
      `TestTxidLookupsConcurrentWithPurgeAndClose` -race). 짧은 경로는 이전과 같다
      (`TestCompressedReadDuringPurgeAndClose`). purge 기록이 상한을 넘어 판단할 수 없으면 다른 수집 조회처럼
      다시 조회하고 3회 모두 그러면 `ErrPurgeOverlap`이다(`TestTxidLookupPurgeOverlap`. 이전 한 hold 구현에는
      없던 오류. 압축 날짜의 긴 chain 단건 조회 사이에 purge·DB 삭제가 64개 넘게 쌓여야 생긴다).
    - `ErrPurgeOverlap`의 Client 영향은 profile·gxid 수집 조회와 같다. service handler가 오류를 그대로 반환하고
      transport는 이미 보낸 item만 flush한 뒤 NoNEXT 없이 연결을 닫는다(transport 계약). 그래서 단건
      `XLOG_READ_BY_TXID`류는 빈 결과, `XLOG_LOAD_BY_TXIDS`는 그 txid 앞까지만 받은 결과가 되고 Java Client는
      오류 표시 없이 받는다. 다시 조회하면 된다.
    - `XLOG_LOAD_BY_TXIDS`(txid 최대 `req_search_xlog_max_count` 500)는 txid마다 짧은 경로 hold 하나라 로컬에서
      500개 약 1.5ms(압축 약 2.4ms, cache warm)이고 hold 사이마다 수집이 들어갈 수 있다. 모든 txid에 긴 경로를
      썼다면 약 75ms(압축 약 39ms)였다.
- **압축 block 해제**(GZipStore 읽기, zip.go `zipFetch`): cache에 없는 block(논리 최대 32MiB)의 해제는 수십 ms
  (로컬 30MiB 약 30ms, -race 약 480ms)라 gate 안에서 하면 그동안 수집 쓰기가 멈췄다. hold를 끝낼 수 있는 읽기
  (key 하나의 수집 조회, V1·V2 범위 scan, 단건 `XLogContext`, tag actual 목록 `TagActualContext`)는 이제
  다음처럼 한다.
  - cache miss면 gate 안에서 경로와 stat만 기록하고(`zipMiss`) hold를 끝낸다. 해제는 gate 밖에서 자리를 얻은
    뒤 파일을 열어 같은 파일(`os.SameFile`, 크기가 줄지 않음)인지 확인하고 **그때의** 크기까지 한다(`fetchZip`;
    자리 대기 동안 쓰인 member도 gate 밖에서 해제된다). 파일은
    해제하는 동안만 연다(자리를 기다리는 동안 fd 없음). 다음 hold에서 같은 record를 다시 읽고 해제한 block을
    쓴다. V1(tag actual 포함)은 정렬된 bucket의 같은 위치, V2는 그 chain record 자체(`at = r.NextPosition`,
    `resume = r.Position`)에서 잇고, `Match`는 record마다 여전히 한 번이다. 범위 scan은 batch를 visit에 넘긴
    뒤 해제한다(느린 Client 쓰기 동안 자리·block을 들고 있지 않음).
  - 해제한 block은 아직 파일의 앞부분일 때만 쓴다: 같은 파일(`os.SameFile`), 크기가 줄지 않음, 그 뒤 block
    truncate 없음(`zipState.truncs`: rollover orphan 정리·쓰기 실패 되돌리기·torn member 정리), 그 날짜의
    purge·DB 삭제 없음(`xlogPurgedSinceLocked`, 판단 불가도 무효). append만 있었으면 앞부분은 그대로이고 index가
    가리킨 record는 miss의 stat 전에 다 쓰였으므로 결과는 그 hold에서 gate 안 해제를 한 결과와 같다.
    - 파일이 그대로면(`sameZipFile`) 그대로 cache에 넣는다(gate 안 해제가 넣었을 항목. 진짜 torn 마지막
      member는 전체 해제처럼 빠진다).
    - 해제를 시작한 뒤 파일이 커졌으면(쓰는 중인 오늘 block, 해제 시간과 다음 hold까지의 gate 대기 동안 쓰인
      member) 해제가 확인한 마지막 member 경계(`ZipBlock.End`)부터 현재 끝까지를 gate 안에서 해제해 붙이고 현재
      stat으로 cache에 넣는다. 해제 시점에 쓰이던 member(앞부분의 torn)는 그 경계부터 다시 온전히 읽는다. 꼬리의
      torn 마지막 member는 전체 해제와 같이 빠진다. 그 뒤로는 gate 안 해제 뒤처럼 writer(`appendZip`)가 cache를
      이어 갱신한다. 이전 수정본은 커진 block을 한 번 쓰고 버려 hot block이 record마다 다시 해제됐다
      (`TestHotBlockDecodedOncePerScan`: 2,000건 scan에서 234회 → 1회).
    - gate 안 꼬리는 `zipTailMaxBytes`(압축 256KiB)와 `zipTailMaxLogical`(논리 1MiB)로 묶는다. 로컬 측정에서
      XLog member(약 90B) 256KiB(약 2,800개) 해제가 약 0.8ms(member당 비용이 지배), 압축이 잘 되는 profile은
      약 1GB/s라 논리 1MiB가 약 1ms다. 그래서 꼬리 해제는 hold 한 번(`scanHoldTime` 2ms) 정도다. 이를 넘거나
      꼬리가 손상됐거나 합이 32MiB를 넘으면 그 block을 쓰지 않고 miss 1회로 센다(다시 gate 밖 해제,
      `TestLongTailDecodedOutsideGate`, `TestHotBlockTailUnderGateBounded`: 자리 대기 동안 block이 256KiB 넘게
      (로컬 약 670KiB) 커져도 gate 안 꼬리는 최대 약 46KiB, 이전 수정본은 자리 대기 동안 쓰인 전체).
    - 해제 오류는 파일이 그대로일 때만 그 오류로 보고한다.
    - 읽기가 miss 아닌 결과로 끝나면(성공·오류, 다른 조회가 이미 cache에 넣어 hit한 경우 포함) 남은 block과
      자리를 바로 놓는다(`TestCacheHitReleasesFetchedBlock`).
  - record 하나가 `zipFetchAttempts`(4)번 miss하면(계속 무효화되거나 cache 1 block에서 block을 넘는 Java
    chain) 이전처럼 gate 안에서 해제한다. 그래서 언제나 끝나고 결과는 gate 안 해제와 같다.
  - 자리(`zipDecodeConcurrent` 2개, Store 전체)는 해제 중이거나 해제를 마치고 다음 hold를 기다리는 block을
    센다. 다음 hold가 block을 가져가면 자리를 놓고, block은 cache(`_compress_read_cache_block_count` 상한)에
    들어가거나 버려진다. 그래서 cache 밖 메모리는 자리의 2×32MiB에, 가져간 hold 안에서 버려질 block과
    `zipFetchAttempts` 뒤 gate 안 해제(gate가 직렬화하므로 한 번에 하나, cache에 들어감) 몫이 GC 전까지 더해질 수
    있다. 조회 하나는 block을 하나만 들고, 자리를 기다리는 것은 들고 있지 않을 때뿐이라 서로 막지 않는다.
    기다림은 gate 밖이고 ctx로 끝나며 그때 fd·자리는 남지 않는다(`TestDecodeSlotWaitCancelled`).
  - fd: 수집 조회는 index hold부터 끝까지 kfile·data 최대 2개, 압축 해제는 해제하는 동안 block 파일 1개(동시에
    자리 수만큼)를 더 쓴다.
  - hold 사이 purge·DB 삭제·Close는 hold 사이에 끼었을 때와 같다(수집 조회·단건: 빈 결과·`ErrClosed`, scan:
    index 없음으로 끝·`ErrClosed`). 해제 중인 handle은 연 inode를 읽으므로 다른 파일의 record를 섞지 않는다.
  - 결과·경합: `TestCompressedBlockDecodeLeavesGateFree`(해제 중 gate가 비어 있음, 결과 동일),
    `TestCompressedReadFallsBackToInGateDecode`, `TestCompressedReadWhileBlockGrows`(해제 중 같은 block append,
    해제 범위 밖 record는 꼬리 해제로 읽음, cache가 파일과 같음), `TestHotBlockDecodedOncePerScan`,
    `TestCacheHitReleasesFetchedBlock`, `TestDecodeSlotWaitCancelled`, `TestCompressedReadDuringPurgeAndClose`,
    `TestCompressedDecodesAreBounded`, `TestTagActualDecodesOutsideGate`, `TestV1LargeBucketMiss`(1024 초과
    bucket, reverse), `TestV2HeadMissWithAppends`(head miss 중 같은 bucket append, 상태 있는 Match 한 번),
    `TestMissWithTornTailAndWriterTruncate`, `TestGrownTailTornOrDamaged`(꼬리 torn·손상), `TestRolloverDuringDecode`
    (해제 중 32MiB 넘어 다음 block으로 rollover), `TestTagActualCursorRecordMiss`(TxID 시작 record 자체의 miss),
    `TestModeChangeReleasesFetchedBlock`(조회 중 비압축으로 바뀌거나 mode 오류면 `xlogBytesFetch`가 block·자리를
    놓음), `TestCompressedReadsConcurrentWithWriters`(-race).
  - 남은 위험: 쓰기 쪽 `loadZipWriter`는 날짜 writer를 처음 쓸 때(재기동·eviction·purge 뒤) count block 하나를
    gate 안에서 해제한다(검증·torn 정리와 한 단위). 수집이 매우 많아 해제와 gate 대기 동안 늘 256KiB 넘게 쓰이면
    block을 쓰지 못하고 `zipFetchAttempts` 뒤 gate 안 전체 해제로 간다(이전 동작과 같은 최악).
  - 로컬 측정(개발 노트북 macOS, `GOMAXPROCS` 기본, 운영 수치 아님, 동시 `PutXLog` 최악 대기):
    - 30MiB block cold 읽기 3회: 이전 28.8–31.0ms(해제 27.1ms), 이후 0.25–7.0ms. -race 이후 2.1–6.4ms(해제 480ms).
    - bucket 하나에 gxid 30만 레코드: 이전 141ms(비압축)·155ms(압축), 이후 0.09–0.32ms. profile·span chain
      10만: 이전 46–56ms, 이후 0.12–0.33ms.
    - bucket 하나에 txid 30만 레코드의 단건 조회(가장 오래된 txid와 없는 txid, 둘 다 chain 전체 walk, 약
      140ms): 이전 144–165ms, 이후 0.24–4.6ms(`TestLongTxidChainIngestWait`, 3회 중 최소로 판정).
    - 짧은 chain hit 단건 조회(기본 hash 크기, 2만 건, `BenchmarkTxidLookup`): 이전 약 2.8µs·923B·8 allocs,
      이후 약 2.9µs·931B·9 allocs(비압축), 압축 4.7µs·12 allocs → 4.8µs·14 allocs.
- **purge·DB 삭제의 gate 범위**(retention.go `detach`, delete.go `detach`): gate 안에서는 집계 버리기,
  handle을 cache에서 빼기, 대상 rename만 한다. 물리 삭제(`RemoveAll`)는 언제나 gate 밖이다.
  - gate를 잡는 순서는 Store gate → `visitorMu` → `visitorIO` → Counters gate → tag gate다(`lockDetachGates`).
    수집은 레코드마다 Store gate를, visit마다 `visitorMu`를 잡으므로, 뒤쪽 gate(`visitorMu`·`visitorIO`·
    Counters·tag)가 바쁘면 앞쪽을 쥔 채 기다리지 않는다. 전부 놓고 바쁜 gate 하나만 기다린 뒤 처음부터 다시
    잡는다. 이전에는 Store gate와 `visitorMu`를 쥔 채 기다려서, visit의 snapshot 쓰기·visitor snapshot 쓰기·
    counter 쓰기·tag pass의 top100 쓰기가 걸려 있는 동안 수집이 함께 멈췄다. 64회(`detachGateRounds`) 다시
    잡고도 바쁘면 이전처럼 쥔 채 기다려 purge가 끝없이 밀리지 않게 한다
    (`TestDetachDoesNotHoldIngestGatesWhileWaiting`).
  - purge는 gate를 기다린 시간의 합이나 쥔 시간이 100ms(`detachSlow`) 이상이면 `PurgeAction.Gates`에 gate별
    대기 시간·round 수·쥔 시간을 적고, runtime이 `purge {date} {mode} gates: …` 한 줄로 남긴다. 쥔 채 기다린
    마지막 round였으면 그 사실도 적는다.
  - 대상은 `.purge-trash`로 rename한다. trash를 만들 수 없거나(ENOSPC, 파일이 막음 — trash `MkdirAll`도
    gate 밖) rename이 실패하면(EXDEV 등) 대상과 같은 디렉터리의 숨은 이름 `.purge-quarantine-…`으로 rename한다
    (`moveAway`, metadata만). 이 이름은 날짜가 아니라 Store 경로·retention 날짜·DB 삭제 이름으로 닿지 않는다.
    gate 밖에서 trash 항목처럼 지운다. 지우지 못한 quarantine 이름은 다음 `cleanTrash`가 gate 밖에서 다시
    지운다: root, 날짜(`00000000` 포함) 디렉터리 바로 안, 그 한 단계 아래(purge 대상과 대부분의 DB 삭제
    대상). 더 깊은 DB 삭제 대상의 것만 그 날짜의 다음 purge까지 남는다.
  - rename한 경로는 rename 전에 process 전역 목록(`removing`)에 올리고 주인이 지운 뒤 내린다. retention
    회차의 `cleanTrash`는 목록에 있는 경로(동시에 진행 중인 DB 삭제의 trash 항목)를 건너뛰어 주인의
    `RemoveAll`과 경합하지 않는다. 이미 사라진 경로는 오류가 아니다.
  - 두 rename이 모두 실패하면(mount point의 EBUSY, 읽기 전용 부모 등) 대상을 **그대로 두고** 오류를
    반환한다. 이전에는 gate 안에서 제자리 `RemoveAll`을 했다(큰 트리 삭제 동안 수집 정지). Java
    `deleteFiles`는 제자리 삭제하지만 Go는 다음 retention 회차가 다시 시도한다(의도된 차이). 계수:
    `Retention.DetachStats()`(quarantine 사용·제자리 남김), 로그는 호출자(runtime `purge failed`)의 오류다. DB
    삭제는 그 대상을 계수에서 빼고 오류를 반환한다.
  - cache에서 뺀 doomed handle은 rename 뒤 gate 안에서 정리한다(`doomedCloses.settle`).
    - 파일이 모두 옮겨졌으면 close(index Close는 kfile·hfile fsync)를 gate 밖에서 한다. 진행 중인 Sync
      pass가 있으면 그 pass가 닫는다(`closeQueue`, 그 pass의 Commit 중에 닫지 않도록).
    - 아무것도 옮겨지지 않았으면(rename 실패) 같은 handle을 cache에 되돌린다. 파일과 handle이 그대로이므로
      닫지 않는다. 그래서 fsync 없는 data close 뒤 index header fsync라는 순서 문제가 생기지 않고, 이후
      쓰기는 같은 handle로 이어진다.
    - 여러 파일 대상의 일부만 옮겨졌으면 gate 안에서 닫되, 남는 data 파일(닫는 것과 되돌리는 것 모두)을
      먼저 fsync한 뒤 index를 닫는다(data → header).
  - Sync pass 중 eviction으로 close 대기열에 있는 handle(key = 경로, 같은 경로를 다시 열면 재채택)은 그 파일이
    옮겨졌으면 key를 지워 재채택하지 않는다(`forgetPendingLocked`, Store·Counters 대기열). 이전에는 purge 뒤
    같은 경로의 조회·쓰기가 옛 handle(옮겨진 파일)을 다시 채택해 purge 전 데이터를 돌려주거나 쓰기를 잃었다
    (`TestPurgeForgetsPendingEvictions`). 그 handle은 기존대로 pass가 닫는다.
  - 남은 차이: rename이 실패한 purge·삭제도 그 날짜 집계(pending tag·visitor·text·queue의 XLog/profile)는
    이미 버렸고, purge 기록이 남아 진행 중인 압축 수집 조회는 빈 결과로 끝난다(purge 의도와 같은 보수적 결과).
  - 삭제 대상 계산, 오늘·`0000` 날짜 거부, 집계 버리기, 재생성 방지(`packDiscards`, text·tag·visitor
    버리기, `markPurgedLocked`)는 그대로다.
  - fault injection: `TestPurgeRenameFailureNeverUnlinksUnderGates`, `TestDeleteDatabasePathsRenameFailure`
    (rename 실패 시 unlink 없음·같은 handle 복원·파일 계속 사용 가능, trash만 실패 시 quarantine 후 gate 밖
    unlink, 모든 unlink 시점에 Store·Counters gate가 비어 있음), `TestPurgeMixedTargetSyncsDataBeforeIndexClose`,
    `TestRetentionQuarantinesWhenTrashUnusable`, `TestRetentionQuarantineLeftoverRetried`,
    `TestCleanTrashRemovesQuarantineInDateDirectories`, `TestCleanTrashSkipsRemovalsInFlight`(-race).
- **purge·DB 삭제의 gate 대기**: Store gate를 잡은 채 Counters gate와 tag gate를 lock 순서대로 기다린다.
  그동안 수집이 멈춘다. 만료 날짜가 있는 retention 회차와 Client의 DB 삭제에서만 생긴다. 대부분의 경로는
  hold 단위(최대 256단계/2ms)로 잡지만, 다음 경로는 한 번에 더 오래 잡을 수 있어 대기가 hold 한 번으로
  묶이지 않는다(남은 위험).
  - Counters gate: `DailyCounterNamesContext`(한 hold), seal 없는 옛 index를 여는 `c.table`(index walk).
  - tag gate: 조회 앞의 `tagDrainAllLocked`(batch 나머지와 inbox, 최대 약 2×16384 entry), table 30개
    상한 eviction의 table 전체 flush와 새 table의 first 파일 scan.
- gxid(`XLogsByGxIDContext`)·span·TRANX_PROFILE(`ProfileChunksContext`)은 Java도 결과 전체를 메모리에
  모으는 조회라 기존 수집 상한을 유지한다(gate 사용은 위 "key 하나의 수집 조회"). TRANX_PROFILE_FULL은 Java가 block마다 보내지만 Go는 같은
  수집 API(txid 하나의 block 합 `MaxQueryBytes` 64MiB)를 쓴다(남은 차이).

## 7. 정상 종료 marker `.go-clean`

설정 키가 아니라 Store 수명 계약이다. 정본 설명은 [recovery-retention](../../docs/compatibility/recovery-retention.md) §9 G7이고, 구현은 `clean.go`와 `legacyindex/seal.go`다.

- `Close`: 모든 index·data를 오류 없이 sync·close한 경우에만 marker를 쓴다. 진행 단계(`xlog writer`, `profile writer`, `text writers`, `gates`, `sync`, `visitor flush`, `index close`, `clean marker`, `checkpoints`)는 lock 없이 `CloseStage()`로 읽는다. marker를 썼는지는 `CleanMarkerWritten()`으로 읽는다. runtime 종료 상한(main `--shutdown-timeout`)이 넘으면 이 둘을 보고하고 프로세스를 끝낸다(S6). `SkipCleanMarker()`를 부른 뒤의 `Close`는 marker만 쓰지 않고 나머지(sync·Seal 없는 close·checkpoint)는 같다. 다음 Open은 checkpoint로 연다(`TestSkipCleanMarker`). runtime은 ingest writer가 쓰기에 갇힌 채 남았을 때 이를 부른다. `CloseStageHook`은 테스트 전용이다. marker 기록이 실패하면 Close가 오류를 반환한다. 이때 data는 이미 닫혔고, 다음 기동은 전체 검증을 한다.
- `Open`: marker를 읽어 메모리에 두고 즉시 삭제한다(디렉터리 fsync 포함). 형식이 틀린 marker는 지우고 무시한다.
- `index()`: Seal과 정확히 같은 쌍은 `legacyindex.OpenCheckpointed`(Seal 우선)로 O(1) 검사만 하고 연다. 열 때 Seal을 소비한다. 같은 run에서 eviction 뒤 다시 열 때는 아래 crash checkpoint를 쓴다.
- KV(TTL) index도 같다. `OpenTTLCheckpointed`로 열고, Store.Close가 `TTLIndex.CloseSealed`로 Seal한다.
- Counter index(`{date}/counter/real`, `5m`)는 `Counters`가 소유한다. 그래서 Seal을 다음 순서로 주고받는다.
  - `Store.AttachCounters`가 marker에서 읽은 counter Seal을 Counters로 넘긴다.
  - `Counters.Close`가 모든 table을 오류 없이 닫으면, Seal(data fsync 뒤 `CloseSealed`)을 보관한다.
  - 그 뒤의 `Store.Close`가 보관된 Seal을 marker에 넣는다(runtime 순서: Counters → Store). 아직 열지 않은 counter Seal은 그대로 이월한다.
  - attach하지 않았거나 Store보다 늦게 닫힌 Counters의 쌍은 Seal이 없으므로 전체 검증한다.
- 관찰: `Store.IndexOpenStats()`(Store index + KV)와 `Counters.IndexOpenStats()`는 `Walked`/`Sealed`/`Checkpointed`/`Created`/`Failed`, `CheckpointRejected`, `WalkedRecords`/`WalkedBytes`(open이 하나씩 검증한 record·key byte 합)를 돌려준다. Store 쪽은 기동 시 marker 유무와 시각도 준다. runtime은 이 값을 `/health`의 `indexOpens`로 낸다(로그는 남기지 않는다).

### 7.1 crash checkpoint `.go-checkpoint`, `.go-checkpoint-counter`

marker는 정상 종료 뒤에만 쓸모가 있다. crash(SIGKILL, OOM, 종료 hang 뒤 SIGKILL) 뒤에는 그날 index를 처음 열 때 Store gate(또는 Counters gate) 안에서 record 전체를 검증했다(아래 §7.2 측정: XLog 300만 건 25–29s). crash checkpoint는 그 비용을 마지막 checkpoint 뒤에 붙은 꼬리로 줄인다. 구현은 `clean.go`(log), `legacyindex/checkpoint.go`(검증)다.

- **내용**: index 쌍마다 `legacyindex.Checkpoint{KeyBytes, HashBytes, HeaderSum, TailCRC}`. 뜻은 "key 파일 앞 `KeyBytes`가 fsync됐고, 디스크의 hfile은 fsync된 이 header이며, 그 header는 `KeyBytes` 아래 record를 모두 가리킨다(bucket head보다 새 record가 없다)"이다. `HeaderSum`은 header의 4KiB page별 CRC32를 big-endian으로 이어 붙인 값의 CRC32다(부분 쓰기와 같은 page 단위라 전체 header를 다시 hash하지 않는다). `TailCRC`는 `KeyBytes` 바로 앞 최대 64byte의 CRC32로, 지우고 다시 만든 같은 경로의 쌍을 header와 함께 구별한다.
- **만드는 곳과 순서**: `SyncPoint.Commit`(key fsync → dirty header page 쓰기 → header fsync 성공 뒤, `BeginSync` 시점 길이), `Index.Sync`/`Close`/`CloseSealed`(같은 순서, 현재 길이). key·header의 fsync나 header 쓰기가 한 번이라도 실패한 handle은 그 뒤 checkpoint를 만들지 않는다(디스크 상태를 모름). Store·Counters는 data 파일 fsync가 실패하면 그 data를 가리킬 수 있는 쌍(Store는 실패한 파일이 index 디렉터리 아래에 있는 쌍, Counters는 그 table)의 checkpoint를 기록하지 않는다. `SyncData`, `Counters.Sync`, eviction close, `Store.Close`, `Counters.Close` 모두 같다(`TestNoCheckpointAfterDataFsyncFailure`).
- **기록 순서**: `SyncData`/`Counters.Sync`는 모든 header Commit과 지연 close(drain)가 끝난 **뒤** log를 쓴다(data → header → checkpoint). pass의 checkpoint는 gate를 다시 잡고, 그때도 cache에 남은 같은 handle의 것만 기록한다. pass 중 eviction된 handle은 자신의 close가 더 새 checkpoint를 기록하고, purge·DB 삭제로 풀린 handle은 다시 기록하지 않는다(`TestSyncDataSkipsReleasedHandles`). eviction close의 기록은 close 뒤 gate 안(`pendingClose.after`)에서 한다. 다음 경우에는 기록하지 않는다. purge·DB 삭제가 queue 항목의 key를 지운 경우(retention `forgetPendingLocked`), 그 경로가 다시 cache된 경우, key 파일이 없어진 경우다(`TestForgottenPendingEvictionRecordsNothing`). eviction close와 `Close`가 만든 checkpoint는 메모리 log에 넣고 다음 SyncData 또는 Close 끝에서 쓴다. 파일은 `.go-clean`과 같이 temp `.scouter-*` → fsync → rename → 디렉터리 fsync로 원자 교체한다. 바뀐 entry가 없으면 쓰지 않는다. log 쓰기 실패는 SyncData/Close 오류로 돌려준다(runtime `periodic sync` 로그·`maintenanceErrors`). 데이터는 이미 안전하고, 다음 crash 재기동이 전체 검증을 할 뿐이다.
- **위치·형식**: DB root의 `.go-checkpoint`(Store index + KV, Store 소유)와 `.go-checkpoint-counter`(Counters 소유). 두 소유자가 파일과 lock을 나누지 않는다. JSON `{format:1, writer:"scouter-go", written_at, indexes:{"<root 아래 slash 경로, 확장자 없음>": {key_bytes, hash_bytes, header_sum, tail_crc32, seq}}}`. 최대 4096 entry이며 넘으면 오래 갱신되지 않은 entry부터 버린다. `.go-clean`과 달리 Open이 지우지 않는다. entry는 header가 바뀔 때까지 crash와 무관하게 참이고, 바뀌면 `HeaderSum`이 맞지 않는다.
- **open**: Seal이 맞으면 Seal(`CheckSealed`). 아니면 checkpoint가 다음을 모두 만족할 때만 `KeyBytes`부터 꼬리만 검증·복구한다(`CheckTail`). `HashBytes` = hfile 크기, 2 ≤ `KeyBytes` ≤ kfile 크기, 디스크 header의 `HeaderSum` 일치, `TailCRC` 일치. 꼬리 검증은 기존 `validateRecover`와 같은 규칙이다(torn tail 잘라냄, orphan relink, relink check). 다음 경우는 아무것도 쓰기 전에 멈추고 전체 walk로 다시 판단한다. head가 `KeyBytes` 이상인 경우, 꼬리의 corruption, relink 거절(거절 메시지와 tombstone 대상이 앞 record에 따라 달라짐). 그래서 결과(record, 복구, 오류)는 항상 전체 walk와 같다. log가 없거나, 형식·writer가 다르거나, 경로가 root 밖이거나, 위 조건이 하나라도 틀리면 기존 전체 검증이다(`CheckpointRejected` 계수).
- **꼬리만 보는 근거**: key 파일은 붙여 쓰기만 한다. Go의 제자리 쓰기(deleted flag, 같은 길이 value, TTL 만료, tombstone)는 record 경계와 link를 바꾸지 않는다. Java의 제자리 쓰기도 index에서는 같다. 다만 Java `RealKeyFile.update`와 `RealKeyFile2.update`(`RealKeyFile2.scala:177-196`)는 원래 값 이하 길이의 **더 짧은** 값도 제자리에 쓸 수 있다. 그러면 blob 길이가 줄어 record 경계가 바뀐다. 이 전제가 성립하는 이유는 index 값이 항상 5byte data offset(`DataOutputX.toBytes5`, 예: `KeyValueStoreIndex.scala:69,73`)이라 같은 길이로만 갱신되기 때문이다. `IndexKeyFile.update`는 서버 안에 호출자가 없다. 다른 길이의 값을 제자리 갱신하는 writer가 생기면 이 근거가 깨진다. checkpoint 아래 record는 checkpoint를 만든 handle이 이미 검증한 상태다(전체 walk, Seal, 또는 이전 checkpoint 사슬). 그 아래의 orphan은 relink 거절이 남긴 tombstone뿐이다. tombstone은 꼬리 record의 relink 판단을 바꾸지 않는다(`TestCheckpointDamagedTailMatchesFullWalk/tombstones_before_the_checkpoint`). checkpoint 뒤 Java가 header를 flush하거나, 도구가 쌍을 다시 쓰거나, header page가 손상되면 `HeaderSum`이 달라진다. 마지막 64byte 안의 제자리 쓰기(최근 record의 delete·update)는 `TailCRC`를 바꿔 안전하게 전체 검증이 된다(느려질 뿐이다).
- **수명**: 새로 만든 쌍(`CheckCreated`)은 그 경로의 entry를 지운다. log를 읽을 때 key 파일이 없는 entry(purge된 날짜)는 버린다. purge·DB 삭제는 rename으로 옮긴 root마다 gate 안에서(`settleDoomedLocked`) `Store.forgetCheckpointsLocked(root)`와 `Counters.forgetCheckpointsLocked(root)`로 그 아래 entry를 지운다. 옮기지 못하고 남은 root는 파일과 entry를 그대로 둔다. root는 retention과 DB 삭제가 쓰는 절대 경로다(`filepath.Join(Directory, date, target)`: 날짜 디렉터리, `{date}/xlog` 같은 종류 디렉터리, `{date}/xlog/xlog_profile.kfile` 같은 파일 하나, 또는 `deleteTarget.path`). 상대 경로면 DB root 기준이다. 지우지 않으면 재기동 전까지 남아 매 persist에 다시 쓰였다. 남은 entry도 파일이 없거나 다른 쌍이면 위 검사에서 거절되지만, 쓰기 비용을 줄이려고 지운다. log는 entry가 바뀐 경우에만 다시 쓴다(`TestForgetCheckpointsLocked`).
- **같은 run의 eviction 재open**: eviction close(`Index.Close`의 sync)가 만든 checkpoint를 메모리 log에서 바로 쓴다. 그래서 재open은 꼬리 0건이다(`TestEvictionReopenUsesCheckpoint`, `TestCounterEvictionReopenUsesCheckpoint`). 이전에는 Seal을 소비한 뒤라 전체 검증을 했다.
- **호환**: Java는 DB root에서 디렉터리만 날짜로 본다(`AutoDeleteScheduler.getLongAgoDate`의 `isDirectory()`). 그래서 두 파일을 무시한다. `ManageService.collectDirectory`의 DB 크기 합계에는 파일 크기(수 KB)가 더해진다. `.go-clean`과 같다. Go retention은 유효한 `yyyymmdd` 디렉터리만 지운다. DB 삭제는 날짜 아래 경로만 지운다. migrate·`scouter-db`는 두 파일을 `.go-clean`처럼 runtime state로 보고 복사·비교·checksum에서 뺀다(`migrate/fsutil.go`). 조회 비교용 staging Store가 남긴 파일도 지운다(`migrate/query.go`). 복사본은 원본 DB의 검증 생략 판단을 물려받지 않는다.
- **gate 밖 open**: 이번 변경은 비용 자체를 줄였다. 검증을 gate 밖으로 옮기는 single-flight open은 하지 않았다. checkpoint가 없을 때(첫 SyncData 전 crash, log 손상, Java가 header를 쓴 뒤)는 여전히 gate 안 전체 검증이다(남은 위험).
- **로컬 측정**(개발 노트북 macOS APFS, `GOMAXPROCS=2`, 일회성 측정, 재현 테스트는 저장소에 없음, 운영 수치 아님): 비압축 XLog 30만 건을 쓰고 SyncData를 한 뒤 2,000건을 더 붙이고 Close 없이 복사했다(crash 이미지). 첫 `PutXLog`는 checkpoint가 있으면 27ms, 54ms였다(`Checkpointed:3`, 꼬리 6,000 record). log를 지우면 1.30s, 1.88s였다(`Walked:3`, 906k record, 19.9MB). 큰 날짜에서는 §7.2의 전체 검증 수치(25–29s) 대신 꼬리 크기(최대 SyncData 주기 4초 + 종료 지연 분량)에 비례한다.

### 7.2 header 부분 쓰기 (R1)

`SyncData`/`Counters.Sync`의 4초 주기 header 저장은 이전에 dirty index마다 header 전체(기본 1MiB, 최대 hash 크기)를 복사·쓰기·fsync했다. 원본 Java는 `MemHashBlock`을 4초마다 전체 저장한다(fsync 없음).

- 이제 index는 4KiB page 단위 dirty bitmap을 둔다(`Put`/`PutBatch`/TTL put의 bucket·count, 복구의 relink, 새 파일). `BeginSync`는 dirty page만 복사한다(SyncPoint가 소유하며 재사용하지 않는다). `Commit`은 key fsync 뒤 연속 page를 한 번의 `WriteAt`으로 오름차순 기록하고 header를 fsync한다. `Sync`/`Close`는 dirty와 진행 중 snapshot page를 현재 메모리 값으로 쓴다.
- 결과 바이트는 전체 쓰기와 같다. 디스크 header는 `BeginSync` 시점의 메모리 header와 byte 단위로 같다(`TestPartialHeaderWriteMatchesFullHeader`). 쓰지 않은 page는 마지막 성공 쓰기 이후 바뀌지 않았기 때문이다.
- `Commit`과 `Sync`/`Close`가 겹치는 경우, 또는 SyncPoint가 중첩되는 경우(`BeginSync` A → `BeginSync` B → B 먼저 Commit)는 header 쓰기 횟수(`headerSeq`)로 감지한다. 호출자는 한 번에 pass 하나만 돌리므로 어느 쪽도 생기지 않지만 계약으로 막는다. 늦은 `Commit`은 header를 쓰지 않고 checkpoint도 만들지 않으며, 자기 page를 다시 dirty로 표시해 다음 sync가 현재 값으로 쓰게 한다. 옛 snapshot이 새 header를 덮지 않고, page가 다음 Sync/Close까지 갇히지도 않는다. `BeginSync`는 dirty page에 더해 아직 Commit되지 않은 앞 SyncPoint가 잡은 page(page별 참조 수)도 복사한다. 그래서 먼저 Commit한 뒤 point의 checkpoint도 "`KeyBytes` 아래 record를 디스크 header가 모두 가리킨다"를 만족한다. 한 SyncPoint의 두 번째 Commit은 오류다(`TestSupersededCommitWritesNothing`, `TestNestedSyncPoints`). 수정 전에는 `TestNestedSyncPoints`가 두 순서 모두 실패했다.
- 실패한 page는 다시 dirty가 되고 다음 sync가 현재 값으로 다시 쓴다(`TestCommitOrderAndFailures`). 순서는 key fsync → header page 쓰기 → header fsync → checkpoint 그대로다.
- crash 일관성은 이전과 같다. 쓰기 도중 crash로 일부 page만 새 값이면, 새 head는 이미 fsync된 record만 가리키고, 옛 head 뒤 record는 기존 relink 복구 대상이다. 이전에도 1MiB 한 번의 쓰기가 page 경계에서 끊길 수 있었다. checkpoint는 header fsync 뒤에만 기록되므로 끊긴 header는 `HeaderSum` 불일치로 전체 검증된다.
- 로컬 측정(일회성 측정, 재현 테스트는 저장소에 없음, 4초당 record 수 → Commit이 쓰는 header byte): time index(844KiB) 10·1,000·10,000건 모두 8KiB(0.9%)였다. 1MiB hash index는 10건 42KiB(4.1%), 100건 334KiB(32.5%), 1,000건 이상 97.8–100%였다. 바쁜 hash index(XLog txid·gxid)는 줄지 않는다. time index와 저빈도 index(text·object·KV·summary·alert 등)의 복사·쓰기가 준다.
- 로컬 측정(개발 노트북 macOS APFS, `GOMAXPROCS=2`, 일회성 측정, 재현 테스트는 저장소에 없음, 비압축 XLog 300만 건): 한 날짜의 `xlog_tim`/`xlog_tid`/`xlog_gid` kfile은 각각 약 66MB, `xlog.service`는 약 159MB였다. 재기동 후 첫 `PutXLog`는 marker가 없으면(기존 동작이자 crash 경로) 2회 25.3s, 29.4s였다(`Walked:3`). 정상 종료 marker가 있으면 2회 8.8ms, 38.5ms였다(`Sealed:3`). `Store.Open`의 marker 읽기·삭제는 5–9ms였다.
- 같은 환경에서 counter 측정도 했다. 2초 주기 realtime counter 400만 건을 넣었고, `real.kfile`은 104MB였다. 재기동 후 첫 realtime `Counters.Put`은 Seal이 없으면 6.66s, 6.50s였다(`Walked:1`). Seal이 있으면 2.7ms, 3.3ms였다(`Sealed:1`). 100만 건(26MB)일 때는 1.44s, 1.85s → 2.7ms, 3.8ms였다.
- 같은 환경에서 tagcount `first` 파일(0.6MB)의 첫 load는 1–3ms였다.
- 위 값은 모두 운영 수치가 아니며, 다른 디스크·CPU에서는 값이 달라진다.

## 8. tag count 실행·lock 계약

설정 키는 아니지만 수집 지연에 직결되는 Store 실행 계약이다. 원본은 tag count를 자체 thread에서 처리한다. `XLogTagCount`는 queue 10000과 daemon, `MoveToNextCollector`는 5초 또는 1000 key마다 저장, `Top100FileCache`는 별도 thread다. 그래서 수집 thread는 enqueue만 한다. Go도 같은 구조다(`tagstore.go`). 로컬 측정에서는 tag flush가 수집 경로나 Store gate를 잡는 동안 UDP queue reject가 몰렸다(`docs/validation/2026-10-17-xlog-max-benchmark.md` §11·§12).

- **inbox**: `PutXLogTagsContext`/`PutAlertTagsContext`는 날짜·objType을 검증한 뒤 기여를 inbox에 붙이고 바로 돌아온다. inbox는 leaf lock `inboxMu`로 붙이거나 꺼낼 때만 잡는다. 수집은 tag gate, Store gate, tag 파일 I/O를 기다리지 않는다.
  - inbox 상한은 `tagInboxMax`(16384 XLog/alert)다.
  - 가득 차면 수집은 drain goroutine을 바로 깨우고(`TagDrainSignal`), 빈자리를 최대 `tagInboxWait`(100ms) 기다린다(`Backpressure`). ingest는 이 대기를 engine lock 밖에서 한다(§8.5). 기여에는 만들 때의 purge 세대가 찍혀, 그 뒤 자기 경로가 purge되었으면 inbox에 들어가지 않는다(§8.5).
  - 그래도 차 있으면 그 기여를 버리고 센다(`InboxDropped`, log S188). 원본 `XLogTagCount`도 queue가 가득 차면 S188을 남기고 버린다.
  - 한 번 시간이 초과되면 inbox가 비워질 때까지(saturated) 기다리지 않고 바로 버린다. 그래서 tag count가 멈춰도(디스크 정지, 긴 조회) XLog 수집은 포화 한 번에 최대 100ms만 늦어지고 그 뒤로는 늦어지지 않는다(`TestTagInboxStalledDropsWithoutSlowingIngest`).
  - 수집은 tag gate를 잡지 않으며 inbox를 직접 반영하지 않는다. 짧은 몰림은 대기로 흡수하고, 지속되는 정체는 원본처럼 tag만 잃고 XLog를 지킨다.
  - 도착 순서는 First 100 값을 정하므로 반영은 항상 도착 순서다. 일찍 멈춘 반영(ctx 취소)의 나머지는 다음 반영의 앞에 남는다. entry 하나는 통째로 반영되거나 전혀 반영되지 않는다. ctx는 entry 사이에서만 본다.
  - 메모리 상한: inbox는 최대 16384 entry, 진행 중 batch는 최대 2×16384 entry다(pass hold는 batch가 빈 뒤에만 inbox를 가져오고, tag goroutine과 조회의 반영은 batch 나머지 뒤에 inbox를 붙인다). entry는 path 문자열 하나와, XLog면 기여를 만들 XLog 필드(정수 15개와 국가 코드, entry 안에 값으로), alert면 기여 slice(4개)다. entry 하나는 160 byte다. inbox 배열은 최대 16384 entry(약 2.6MB)까지만 키우고, batch는 최대 2×16384 entry(약 5.2MB), 다시 쓸 배열(spare)은 하나에 최대 16384 entry다. 합쳐 최악 약 10MiB, 한가할 때 남는 것은 spare 하나(최대 약 2.6MB)다. XLog의 `PrepareXLogTags`는 할당하지 않는다(이전에는 XLog마다 기여 slice 약 430 byte를 engine lock 안에서 할당했다). 다 반영한 batch의 배열은 비운 뒤 다음 inbox가 다시 쓴다(`tagDrainedLocked`, 용량 16384 이하만. batch 나머지와 inbox를 이어 붙인 배열은 버린다). batch 나머지가 없으면 inbox를 복사하지 않고 그대로 batch로 쓴다. 대기 중인 tag `Write.Size()`는 entry 크기(`unsafe.Sizeof`)와 국가 코드, alert면 기여 slice를 더한 값이다.
  - Store가 닫히면 `ErrClosed`다.
- **pending 반영 자료 구조**(2026-10-24): XLog의 기여는 반영할 때 entry의 XLog 필드에서 만든다. 순서는 이전 Go와 같다(@total, object, service, service-time·kbyte, sqltime·sqlcount·apitime·apicount, service-error·error, group, user-agent, referer, city, nation, login, desc). 원본 `XLogTagCount.process`는 error-sum부터 nation까지를 SQL·API 합계보다 먼저 센다. First 100은 tag마다 도착 순서로 정해지므로 이 차이는 entry 중간의 tag가 실패할 때만 드러난다. 그때는 앞의 tag만 반영된다(`TestTagEntryFailureKeepsEarlierTags`). tag key(`tagKey(group, name)`의 crc32)는 이름이 고정이라 한 번만 계산해 둔다(`tagXLogKeys`). pending table은 record key를 encode한 문자열이 아니라 고정 크기 key(`tagValueKey`: tag, Value type, 숫자)로 찾는다. Text 값(국가 코드, alert title)의 숫자는 table이 그 text에 붙인 id다(`tagTable.texts`). id는 그 text를 쓰는 pending delta가 있는 동안만 유지하고, 마지막 delta가 flush·실패 drop되면 버린다. purge·eviction이면 table과 함께 버린다. 그래서 `texts`는 pending delta 수를 넘지 않고, pending byte 상한에 따로 더하지 않는다. id는 Store 안에서 다시 쓰지 않는다(`tagCountState.lastText`). flush pass는 hold 사이에 snapshot 항목을 이 key로 다시 찾는데, 그 사이 table이 eviction·purge 뒤 다시 만들어져 id가 1부터 다시 붙으면 다른 text의 새 delta가 snapshot 항목과 같은 key가 되어 정렬 위치가 아닌 곳에 쓰일 수 있기 때문이다(`TestTagTextIDsNotReusedAfterTableRemade`, table별 id로는 실패 확인). 그래서 key encode는 새 delta를 만들 때만 한다. table 조회와 top100 표시는 entry당 한 번이다(이전에는 tag마다). flush 순서는 encode한 key의 byte 순서 그대로다. tag 파일 byte, First 100 판정, pending·metadata byte 계산, 상한에서의 drop·`ApplyErrors`는 이전 구현과 같다(`TestTagApplyMatchesPreviousImplementation`: 이전 구현이 만든 파일·상태·조회 결과 digest와 비교). 로컬 bench(Apple M1 Pro, 200 object·2000 service, XLog당 tag 약 9개)에서 반영은 XLog당 약 1.2µs → 약 0.4µs, `PrepareXLogTags`는 약 290ns·426B·1 alloc → 약 130ns·0 alloc이다. 서버 CPU 효과는 Linux A/B로 따로 확인한다.
- **tag gate**(`tagState.mu`): pending table과 tag 파일 handle·크기 cache를 보호한다. cache는 `tagFileMax` 64개다. eviction 때 쓴 적 있는 handle만 fsync한 뒤 close하고, header magic은 handle마다 한 번 검증한다. tag 파일은 tag gate 안에서만 읽고 쓴다. 그래서 tag pass는 XLog·text·profile 쓰기를 지연시키지 않고, Store gate를 기다리지도 않는다.
- **engine tick**(`Engine.Tick` → `TagTickContext` → `tagTickLocked`, engine lock을 놓은 뒤, §8.4): disk I/O 없이 `inboxMu` 안에서 5초 flush와 5분 top100 rebuild를 due로 표시한다. engine clock `now`를 쓰고, tag pass를 기다리지 않는다.
- **실행 주체**: tag 전용 runtime goroutine 하나가 100ms마다, 그리고 inbox가 차서 `TagDrainSignal`이 오면 바로 `Store.FlushTagCountsIfDue(ctx)`를 부른다. visitor flush·sync·purge를 하는 `maintain` goroutine과 분리되어 있어, 긴 tag pass가 그 작업을 늦추지 않고 그 작업도 tag 반영을 늦추지 않는다.
  - 매번 inbox를 tag gate의 짧은 hold로 반영한다. 높은 rate에서는 inbox가 1초보다 훨씬 빨리 찼다(로컬 replay 40k XLog/s에서 1초 주기면 30초에 `Backpressure` 70회, 100ms 주기면 0회, 5회 반복).
  - due이면 incremental flush를 하고, 5분 cycle이면 rebuild를 한다.
  - 오류는 `maintenanceErrors`로 센다. flush가 실패하면 다음 5초 tick에 다시 한다. 100ms 호출마다 다시 하지 않으므로 계속 실패해도(ENOSPC, 손상) 시도와 log는 5초에 한 번이다(`TestTagFlushFailureRetriedAtNextTick`). 원본 `MoveToNextCollector`도 다음 저장 주기에 다시 한다. rebuild가 실패한 path는 다음 5분 cycle에 다시 한다. 단, 자원 상한(`ErrLimit`: scan 상한 1<<24 record, 보관 상한 64MiB)으로 끝난 rebuild는 다시 하지 않는다. tag 파일은 늘기만 하므로 다시 해도 파일 전체(수 GB)를 5분마다 읽고 같은 자리에서 실패한다. 그 path는 purge되거나 재기동할 때까지 rebuild 대상에서 빼고(`cacheLimited`, `/health` `tagCount.CacheLimited`), 오류는 한 번만 남긴다. count 기록은 계속되고 top100 파일은 마지막으로 성공한 내용으로 남는다(`TestTagTop100LimitIsNotRetried`).
  - 반영에 실패한 inbox entry는 `ApplyErrors`로 세고 건너뛴다. 동기 추가가 실패해도 XLog는 저장되던 기존 동작과 같다.
  - pass(flush, rebuild)가 진행 중일 때는 그 pass의 hold가 inbox를 반영한다.
- **hold 안의 반영**: flush와 rebuild의 hold는 쌓인 inbox를 hold 예산의 절반 안에서 먼저 반영하고(남은 batch를 이어서, batch가 끝나야 새 inbox를 꺼낸다), 그 뒤 기록을 최소 1개 처리한다. 반영이 아무리 많아도 pass는 hold마다 앞으로 나아가므로 끝난다. 그래서 긴 pass 동안에도 inbox가 차서 수집이 기다리는 일이 줄어든다. 로컬 replay 11k XLog/s 측정에서 pass가 약 1초 동안 반영을 멈춰 backpressure가 1~2회 났던 것을 막는다.
- **hold**: inbox 반영, flush, rebuild 모두 tag gate를 한 번 잡을 때마다 `scanHoldSteps`(256) 단계 또는 `scanHoldTime`(2ms) 중 먼저 닿는 쪽까지만 하고 놓는다. hold 사이에는 tag 조회, purge가 끼어든다. 수집 자체는 tag gate를 쓰지 않지만, pass는 디스크와 CPU를 나누어 쓰도록 hold 사이에 쉰다(아래 pass 휴식).
- **incremental flush**(`flushTagCounts`/`tagFlushPass`): 시작 시점의 pending key 목록을 `tagFlushLocked`와 같은 순서로 정렬한다. 순서는 table path, First 다음 Next, key 순이다.
  - record 하나의 read-modify-write(chain 탐색, 저장, pending 삭제, `pendingBytes` 갱신)는 한 hold 안에서 끝난다. 따라서 조회가 반쯤 반영된 record를 보지 않고, count를 잃거나 두 번 더하지 않는다.
  - 다른 경로가 이미 쓴 key는 건너뛴다. 다른 경로는 inline 상한 flush, `tagTableLocked` eviction, `Close`, purge다.
  - pass 중에 반영된 기여는 다음 pass가 쓴다.
  - 동시 기여가 없으면 결과 파일은 전체 flush와 byte 단위로 같다(`TestTagIncrementalFlushMatchesFullFlush`).
- **top100 rebuild**(`tagMakeCacheHolds`): due이면 flush를 먼저 하고 rebuild한다.
  - First, Next 순서로 `tagScanCursor`를 써서 hold로 나누어 파일 순서대로 읽는다. builder 상태는 hold 사이에도 유지하고, cache 파일은 마지막 hold에 쓴다.
  - scan이 시작된 뒤 파일 끝에 붙은 record는 보지 않는다. 그 사이 제자리 갱신된 record는 갱신 전이나 갱신 후 값으로 읽힌다. 원본 `Top100FileCache`도 쓰기 중인 파일을 읽는다.
  - hold 사이에 purge나 DB 삭제가 일어나면 조용히 멈추고 지워진 디렉터리를 다시 만들지 않는다. 다른 날짜 purge로 멈춘 path는 파일이 남아 있으면 다음 cycle에 다시 한다.
  - Client `MakeTagTop100Context`도 같은 hold 방식이다.
- **조회**(`TagValuesContext`, `TagValueSeriesContext`, `MakeTagTop100Context`): tag gate 안에서 먼저 inbox를 반영한다. 이 반영은 요청 ctx로 끊지 않으며, batch 나머지와 inbox(최대 약 2×16384 entry) 분량이다. 그 뒤 파일 scan은 flush처럼 hold로 나누어 읽는다(`tagScanInHolds`, `tagMakeCacheHolds`). 그래서 긴 조회가 tag gate를 오래 잡아 maintenance 반영과 purge를 막지 않는다. 기록 하나는 한 hold 안에서 읽는다. scan 도중 끝에 붙은 기록은 보지 않고, 제자리 갱신된 기록은 갱신 전이나 후 값으로 읽는다. 원본 reader도 writer thread와 동시에 읽는다. hold 사이에 purge가 일어나면 빈 결과다. 원본처럼 파일에 쓰이기 전의 count는 보이지 않는다. 화면에 보이는 시점은 5초 flush와 5분 rebuild 주기를 따르고, tag goroutine 지연만큼 늦어질 수 있다. `TagActualContext`는 XLog data를 읽으므로 Store gate를 hold로 나누어 쓴다(§6).
- **purge·DB 삭제**(`discardAggregationLocked`): Store gate와 tag gate를 잡고 대상 path 아래의 pending table, top100 표시, inbox와 진행 중 batch의 entry를 버린다. tag handle은 fsync 없이 닫는다. tag gate는 대상의 rename·삭제가 끝날 때까지 놓지 않는다(`lockDetachGates`). 그 사이 반영·flush·조회가 지워질 tag 파일을 다시 열면 안 되기 때문이다. 지워진 날짜를 flush가 다시 만들지 않는다.
- **SyncData**: tag record를 쓰지 않는다. 열린 tag handle은 gate 밖에서 fsync한다(닫힌 handle은 무시). 원본은 tag 파일을 fsync하지 않는다.
- **Sync·Close**:
  - `Store.Sync`(명시 호출)는 visitor flush, inbox 반영과 flush, SyncData를 이 순서로 한다.
  - `Close`(`syncLocked` → `tagClose`)는 inbox를 닫은 뒤 남은 batch와 inbox를 반영하고, 전체 flush를 하고, tag handle을 fsync 후 닫는다.
  - pass가 hold 사이에 있을 때 `Close`가 끝나면 pass는 다음 hold에서 `closed`를 보고 오류 없이 멈춘다.
- **lock 순서**: `syncRun` → `tagRun` → `mu`(Store gate) → `tags.mu`(tag gate) → `inboxMu`. tag gate 안에서는 Store gate를 잡지 않는다. `tagRun`은 한 번에 pass 하나(반영, flush, rebuild)만 허용하며 어느 gate 안에서도 잡지 않는다.
- **pending 상한**: inbox entry 하나를 반영하면 pending byte 상한(`MaxQueryBytes`, 최대 64MiB)을 넘을 수 있는 경우, 그 entry를 통째로 버리고 센다(`PendingDropped`, log S182 "queue is full"). 추정은 실제 증가량 이상이다(새 delta는 256 + key 길이, 새 hour는 240).
  - 원본과 다른 점(의도): 원본 `MoveToNextCollector.add`는 자기 queue가 차면 S182를 남기며 빈자리를 기다린다. 기다리는 쪽은 `XLogTagCount` daemon이라 수집은 막히지 않고, 대신 `XLogTagCount` queue가 차서 S188로 버려진다. Go는 기다리지 않고 pending byte 상한에서 같은 S182 log와 함께 entry(XLog 하나의 기여) 단위로 버린다. tag 반영이 tag gate를 오래 잡아 조회·purge를 막지 않게 하기 위해서다. 결과는 원본과 같이 일부 tag count를 잃고 XLog는 지킨다.
  - 통째로 버리므로 XLog 하나의 tag는 모두 반영되거나 모두 빠진다(`TestTagPendingBoundDropsWholeEntries`, 추정 경계 `TestTagNeedMaxCoversKey`).
  - 남은 일부 반영: entry 중간의 tag 추가 자체가 실패하면(First metadata 64MiB 상한, 32767 byte를 넘는 key, table 읽기 실패) 앞의 tag는 반영된 채 그 entry를 `ApplyErrors`로 센다. 원본도 tag마다 따로 세고 실패한 tag만 log를 남긴다.
  - 이전에는 그 자리에서 전체 flush를 했다(`InlineFlushes`, 이제는 추정이 빗나간 경우의 마지막 안전장치). 신규 tag 값 폭주(로컬 replay, text hit 0.0, 20k XLog/s)에서 그 flush가 CPU 약 27초와 디스크를 써 XLog 쓰기를 늦췄다.
- **pass 휴식**: flush와 rebuild의 hold가 끝나면 그 길이만큼 쉰다(`tagPassYieldFactor` 1, ctx로 끝남). 쉬는 동안에도 inbox 반영은 짧은 hold로 계속한다(`tagApplyDuringRest`, batch가 빈 뒤에만 inbox를 가져온다). 반영은 메모리 작업이고, 긴 pass 동안 inbox가 차서 수집이 기다리던 것을 막는다(로컬 replay 38,000 XLog/s에서 inbox 대기 32~34회·버린 기여 약 11만 건 → 0, `TestTagPassRestAppliesInbox`). 그래서 pass는 쓸 수 있는 디스크와 CPU의 절반 정도만 쓴다. 쓸 것이 적으면 pass는 hold 몇 개로 끝나 영향이 없다. 폭주 중에는 pass가 길어지고 pending이 상한에 닿아 tag가 버려진다. tag라는 보조 기능이 줄어들 뿐 XLog 수집은 지킨다.
  - pending delta는 손댄 hour의 60분 배열만 가진다(원본 `MoveToNextCollector`의 `Array[Float](60)`). 계산은 delta당 256 + key 길이, hour 하나당 240이다.
  - 이 byte 회계는 실제 heap 상한이 아니다. delta 구조체, map entry, key 사본 두 벌 때문에 작은 key에서는 대략 1.2~1.5배다.
- **counter**: `TagCountStats`는 `/health`의 `tagCount`, 종료 stats line의 `tagcount=`로 나간다. 항목은 `InlineFlushes`(0이 정상), `Backpressure`(빈자리를 기다린 기여), `InboxDropped`(inbox가 계속 차서 버린 기여), `PendingDropped`(pending 상한에서 버린 entry), `ApplyErrors`, `DroppedDeltas`, `Passes`, `MaxPassMillis`/`LastPassMillis`다.
- **header 쓰기 실패**: 새 tag 파일의 header(magic) 쓰기가 실패하면(ENOSPC) 빈 key·data 파일은 없는 파일로 보고 다음 쓰기에서 header를 다시 쓴다. hash 파일은 크기를 늘린 뒤 header 쓰기가 실패하면 다시 0으로 줄인다. 이전에는 남은 빈 파일이 계속 `ErrCorrupt`가 되어 그 날짜의 tag count가 디스크 공간이 회복돼도 쓰이지 않았다.
- **쓰기 실패**: 기록 하나의 쓰기가 count를 바꾸기 전에 실패하면 delta는 pending에 남아 다음 pass가 다시 쓴다. 이런 경우는 새 기록의 append(실패하면 잘라 낸다), Next 기록의 새 hour block append다. Next 기록은 새 hour를 먼저 append하므로 공간 부족은 count를 바꾸기 전에 실패한다. 일부 count가 이미 파일에 반영된 뒤 실패하면 delta를 버리고 `DroppedDeltas`로 센다. 이런 경우는 제자리 hour 가산 일부, head 쓰기, hash link다. 다시 쓰면 같은 count를 두 번 더하기 때문이며, 원본도 실패한 기록을 log(S204)만 남기고 넘어간다. 단위가 다르다. `DroppedDeltas`는 잃은 count 수가 아니라 delta(key 하나, 여러 hour) 수다. 원본은 hour마다 따로 써서 실패한 hour만 잃는다. Go는 delta를 통째로 버리는 대신, count가 바뀌기 전의 실패는 다시 쓰므로 그만큼 덜 잃는다. 실패 직전에 append한 block과 반쪽 기록은 잘라 낸다(자르기 실패도 오류로 반환). 일반 파일에서는 일어나지 않는 short write의 일부 byte 변경은 다시 쓸 때 반영되지 않을 수 있다. 전체 flush(Close·inline 상한)는 한 기록의 실패에서 멈추지 않고 나머지를 쓴 뒤 첫 오류를 반환한다. 제자리 갱신은 손상 입력(hour 위치가 data 파일 범위 밖, block이 있는데 data 파일이 없음, magic 불일치)을 쓰기 전에 `ErrCorrupt`로 거절한다.
- **순서 보장의 범위**: 아래 쓰기 순서는 process crash와 취소에 대한 보장이다. 전원 손실에는 해당하지 않는다. key 파일과 hash 파일 사이에 fsync barrier가 없으며, 원본도 같다.
- **기록 쓰기 순서와 취소**: 새 tag 기록은 key 파일에 먼저 붙이고, 그다음 hash bucket의 head를 그 위치로 바꾼다(원본 `IndexFile.put`: `keyFile.append` 후 `hashFile.put`). 기록 하나의 저장(`tagSaveRecord`)은 시작하면 ctx로 끊지 않는다. ctx는 기록 사이에서만 본다. 이전 순서(head 먼저)에 ctx 검사가 끼면, 종료 중 끊긴 flush가 key 파일 끝을 넘는 위치를 가리키는 bucket을 남겼다. 그러면 그 bucket의 조회와 다음 flush가 `ErrCorrupt`가 됐다(로컬 부하 측정 중 종료 시 1회 관측, `TestTagFlushCancelledAnywhereLeavesWholeRecords`로 재현·수정). 이미 쓰인 DB의 끊긴 chain은 `scouter-db inspect`의 `tagcount.*` 경고로 찾는다(`VerifyTagCountFiles`, 읽기 전용). 검사하는 불변식은 세 가지다. 기록마다 link는 같은 bucket의 직전 기록이고, bucket head는 그 bucket의 마지막 기록이고, header count는 비어 있지 않은 bucket 수다. 원본 Java가 쓴 파일도 이 불변식을 만족한다(`TestJavaAggregationInterop`).
- **tag 파일 크기 cache**: 크기는 `tags.ends`에 두고 handle과 함께 버린다. 쓰기 오류가 나면 버리고, header는 handle마다 한 번 검증한다. 열린 Store 뒤에서 tag 파일을 바꾸는 것은 지원하지 않는다.
- 파일 형식, record 내용·순서, 조회 결과, `tagcnt_enabled` live 의미는 바뀌지 않았다.

### 8.1 visitor snapshot 쓰기

- **visitor lock**(`visitorMu`): visitor 상태(HLL entry, 날짜·시 roll, flush 표시)는 Store gate가 아니라 전용 lock이 보호한다. 수집의 `PutVisitorContext`(XType 0이고 UserID가 있는 XLog마다)는 Store gate를 잡지 않으므로, XLog·profile writer의 gate hold 뒤에서 engine lock을 쥔 채 기다리지 않는다. visitor 파일은 handle cache를 쓰지 않으므로 경로는 cache 없이 검사한다(`walkSafePath`). lock 순서는 `mu` → `visitorMu` → `visitorIO`이고, purge·DB 삭제·Close는 Store gate를 쥔 채 `visitorMu`를 잡는다. 그래서 그 사이 방문이 밀려난 snapshot을 지울 날짜에 저장하지 않는다(이전과 같은 보장). engine tick의 visitor roll(`VisitorRollContext`)은 `visitorMu`만 잡고, Store gate는 tag tick(`TagTickContext`)만 잡는다(§8.4).

- **entry 상한과 메모리 예산(object 수천 대응)**: 원본은 daily objType 100, daily object 500, hourly object 500개를 `LinkedMap` 삽입 순서(FIFO)로 두고, 넘치면 가장 오래된 HLL을 저장하지 않고 버린다(`IntKeyLinkedMap._put`의 `overflowed`는 빈 함수). 다시 들어온 object는 마지막 flush의 파일을 다시 읽으므로 그 사이 방문이 빠진다. 이전 Go는 같은 상한에서 버리는 대신 저장했고, 사용자 ID가 있는 XLog를 보내는 object가 500을 넘으면 거의 모든 방문이 miss가 되어 방문마다 정렬·저장·읽기를 했다(object 4000·XLog 5k/s에서 서버 CPU 47~55%, visitor writer 기록의 약 70% Dropped, `docs/validation/2026-10-23-profile-scout.md` §4.3). 지금은 다음과 같다.
  - **partial entry**: 새 entry는 파일을 읽지 않는다. 방문은 메모리의 빈 HLL에 register만 올린다(partial). 수집 경로는 파일을 읽지도 쓰지도 않는다. 저장 경로(symlink·root 밖)는 entry를 만들 때 한 번 검사해 이전 miss load처럼 그 방문을 `ErrInvalid`로 거절한다. flush는 partial entry를 파일에 합친다(register별 max). 바뀌는 4KiB page만 읽고 다시 쓰고, 파일이 없으면 새로 만든다. hourly entry의 첫 쓰기는 원본처럼 double suffix 경로에서 읽은 base에 방문을 더해 통째로 쓴다(restart 뒤 첫 방문의 덮어쓰기 동작 유지).
  - **resident 승격·강등**: flush는 resident HLL 합계가 예산(`visitorResidentMax`, 기본 256MiB, dense HLL 약 380개 분량, `go_visitor_resident_mb`)에 dense HLL 하나를 더해도 들어가면 partial entry를 쓰면서 파일 전체를 한 번 읽어 resident로 바꾼다. 그 뒤 register를 바꾸지 않는 방문은 비용이 없고, 쓰기는 이전처럼 바뀐 page만 한다. 합계가 예산을 넘으면 마지막 방문이 가장 오래된 resident부터 예산의 7/8까지 partial로 강등한다. 넘는 시점은 방문이 resident HLL을 키울 때(sparse→dense, flush도 due로 표시), entry를 쓴 직후, pass 끝이다. dirty entry도 잃지 않고 강등한다. synced면 파일은 마지막 쓰기 이후 표시된 page 밖에서 HLL과 같고 register는 커지기만 하므로, 그 page의 word만 delta로 남긴다. unsynced면 HLL 전체를 delta로 두고 통째 쓰기를 기다린다. pass가 쓰는 중인 entry는 강등하지 않는다. 그래서 resident 합계는 방문 사이에도 예산을 넘지 않는다(`TestVisitorResidentBudgetHeldWhileHLLsGrow`. 수정 전에는 flush 사이에 약 7.3MiB 초과로 실패했다). 다만 진행 중인 쓰기와 강등 대상이 아닌 entry만큼은 넘을 수 있다. 강등하면 delta 합계가 `visitorDeltaMax`를 넘게 되는 dirty entry도 강등하지 않는다. 판단은 그 entry의 HLL 크기로 보수적으로 한다. 그런 entry를 강등해도 메모리는 거의 줄지 않는다(unsynced는 HLL 전체가 delta다). 오히려 뒤따르는 partial 방문이 모두 수집 경로에서 write-through된다. 그래서 그 entry는 기다린다(`demoteWait`). 그 결과 resident가 예산을 넘으면 flush를 due로 표시한다. 기다리는 동안 방문은 전체 정렬을 다시 하지 않는다. 예산을 넘긴 방문은 자기가 키운 entry 하나만, delta에 들어갈 때 강등한다(`TestVisitorBudgetWaitVisitsDemoteTheirEntry`). pass는 그 entry를 쓴 뒤 깨끗한 상태(delta 0)로 강등하고, pass 끝에서 다시 판단한다. 이 경우에만 resident가 그 entry가 쓰이고 강등될 때까지 예산을 넘는다(`TestVisitorBudgetCutWaitsForThePass`. 수정 전에는 1MiB/4MiB로 줄이면 delta가 약 8MiB가 됐다). 그동안 넘는 양의 상한은 기다리는 resident entry가 dense까지 자라는 양, 즉 resident entry 수 × 약 683KiB다. 기다리는 동안에는 승격하지 않으므로 resident entry 수는 늘지 않는다. pass의 쓰기가 실패하면 그 pass는 flush를 다시 due로 표시하지 않는다. 다음 10초 tick이 표시하므로, 실패하는 pass가 1초마다 반복되지 않는다(`TestVisitorBudgetWaitAfterFailedPass`). 크기는 dense 683KiB, sparse word당 20B(Go 1.27 map 실측 13~19B)로 추산한다.
  - **결과**: max는 순서와 무관하므로 파일 bytes는 entry 수와 예산에 상관없이 하루·한 시간 동안 하나의 HLL에 모든 방문을 넣은 결과와 같다(`TestVisitorManyObjectsNoIngestIOAndExactFiles`, `TestVisitorBudgetsGiveSameFiles`, Java oracle: Java가 쓴 snapshot에 partial 경로로 합친 결과가 Java merge와 같고, partial 경로로 쓴 파일을 Java가 검증한다, `TestJavaAggregationInterop`). realtime 조회는 `visitorMu` 아래에서 resident HLL과 partial delta만 합친다. 파일은 lock을 놓은 뒤 파일마다 `visitorIO` 아래에서 읽으므로 조회가 수집과 pass를 멈추지 않고 쓰는 중인 파일을 읽지 않는다. 원본은 object 500개(objType 100개)를 넘으면 방문을 잃으므로 이것은 의도된 보존 차이다. object 500개 이하이고 objType 100개 이하이면 원본과 같은 파일을 만든다. objType이 100개를 넘으면 원본은 축출된 objType의 방문을 잃고 Go는 저장한다.
  - **상한**: objType 100(원본과 같음), object daily·hourly 각 32768(`visitorObjectsMax`, partial entry 하나 약 200B). 상한을 넘으면 원본처럼 가장 먼저 만든 entry를 내보내되, dirty면 그 자리에서 쓴다(원본은 버린다). 다른 pass가 쓰는 중인 entry는 건너뛴다. 쓰기에 실패한 victim은 register를 지닌 채 남고 다음 victim을 시도한다. 세 victim이 실패하면 그 offer만 오류다. 같은 시간에 쓰인 뒤 축출된 hourly key가 다시 들어오면 그 파일에 합친다. double suffix load로 통째로 바꾸면 그 시간의 저장분을 잃기 때문이다. 이 기록은 시간당 objectsMax개까지이고 그 뒤로는 원본처럼 바꾼다. delta 합계가 `visitorDeltaMax`(64MiB, 평소에는 10초치 방문으로 수 MiB, `go_visitor_delta_mb`)를 넘으면 flush를 due로 표시하고 partial entry 방문을 그 자리에서 파일에 쓴다(write-through). write-through가 실패해도 register는 메모리에 남으므로 그 방문의 오류로 돌려주지 않는다. write-through 수는 `Store.VisitorStats().WriteThroughs`, 실패 횟수는 `WriteThroughFailures`로 센다. 이 Stats에는 entry 수와 resident·delta bytes, 예산도 있다. 첫 오류는 다음 pass가 돌려주고 runtime이 로그로 남긴다. 한 XLog의 objType·object·hourly offer는 서로 독립이라 하나가 거절돼도 나머지는 센다. ingest visitor batch도 거절된 방문 하나로 나머지를 버리지 않고 모든 방문을 시도한다(ctx가 끝나면 남은 방문은 시도하지 않고 failed로 센다). writer 집계에서 방문은 세 offer가 모두 성공해야 Written이다. 하나라도 거절되면, 그 오류 가운데 입력 오류(`ErrInvalid`·`ErrLimit` 등: 이름·경로·시각)가 있으면 WriteInvalid, 없으면 WriteErrors로 센다. 성공한 다른 offer는 HLL에 이미 반영됐으므로 실패로 센 방문도 일부 집계에는 들어 있을 수 있다(`TestVisitBatchCountsWrittenInvalidAndFailed`). flush가 멈춰도 delta는 늘지 않고(write-through), resident는 위 대기 상한 안에 머문다. 대신 visitor writer가 느려진다. 그러면 ingest queue 포화로 drop이 집계된다.
  - **실패**: partial entry의 파일이 완전한 snapshot이 아니면(크기·header·word 오류) 덮어쓰지 않고 `ErrCorrupt`를 돌려준다. delta는 메모리에 남아 다음 pass가 다시 시도한다. 한 entry의 실패로 pass를 멈추지 않고 다른 entry를 계속 쓴 뒤 첫 오류를 돌려준다(이전에는 첫 오류에서 멈췄다). 실패나 ctx 취소 뒤 재시도한 파일은 같은 bytes다. 쓰기가 실패해 unsynced가 된 entry가 그 뒤 강등돼도 같다(`TestVisitorFailedWritesRetryToSameBytes`, `TestVisitorCancelledPassKeepsRegisters`, `TestVisitorFailedWriteThenDemotedRetries`). dense HLL을 강등한 partial entry의 realtime 값도 같다(`TestVisitorDemotedDenseRealtime`). write-through 실패는 `TestVisitorWriteThroughFailureDeferred`로 검증한다. Close는 메모리의 dirty entry를 쓰고 돌아오며, 그 동안 visitorMu 밖에서 쓰던 pass를 기다리지 않는다. 그 pass 쓰기가 실패하면 pass가 Close 뒤에 한 번 더 쓴다. visitor snapshot은 독립 파일이라 Close 뒤 쓰기를 허용한다(`TestVisitorWriteFailedDuringCloseRetried`). 이 재시도의 오류는 Close가 아니라 pass 호출자(Sync·FlushVisitorsIfDue)가 받는다. 재시도도 실패하면 그 delta는 닫힌 Store와 함께 사라진다. hourly 첫 쓰기(double suffix base)가 실패하고 같은 entry가 그 사이 write-through로 쓰이면, 그 hourly 파일은 restart 전 저장분을 덮어쓰지 않고 합친다(원본 quirk와 다른 보존 쪽 차이). 두 예산은 `Store.SetVisitorBudgets(residentMB, deltaMB)`로 바꾼다(`go_visitor_resident_mb`, `go_visitor_delta_mb`, live, 0은 기본값). setter는 config reload 경로에서 불리므로 값을 atomic으로 저장만 하고 visitorMu를 기다리지 않는다(`TestVisitorSetBudgetsDoesNotTakeTheVisitorLock`). visitor state가 다음 방문, roll tick, `FlushVisitorsIfDue`, pass, `VisitorStats` 때 visitorMu 아래에서 적용한다. state를 만들기 전에 정한 값은 state를 만들 때 적용한다. resident를 줄이면 적용 시점(보통 1초 이내)에 강등한다. 이것은 메모리 안의 작업이다(위 delta 예외 포함). 늘리면 다음 pass부터 승격하고, 승격마다 파일 하나를 통째로 읽는다. 바뀐 뒤 delta가 예산을 넘으면 flush를 due로 표시한다. 그래도 그 pass 전의 partial 방문은 write-through된다. 예산을 바꿔도 파일 bytes는 같다(`TestVisitorSetBudgets`, `TestVisitorSetBudgetsConcurrent`). entry 상한(`visitorTypesMax`, `visitorObjectsMax`)은 코드 상수다. 실행 중에 object 상한을 줄이면 다음 새 entry 방문 하나가 초과분 전부를 visitorMu 아래에서 쓰게 되므로 키로 두지 않는다.
- **dirty 표시**: 원본 `VisitorDB.getNewObject`는 방문마다 dirty를 켠다. Go는 HLL register가 실제로 바뀐 방문만 dirty로 표시한다. 바뀌지 않은 HLL을 다시 쓰면 같은 bytes이므로 파일 내용은 같다(의도된 차이). 예외적으로 오늘 날짜 `.usr` 파일을 Store 밖에서 지우면, 원본은 다음 10초 flush에 다시 만들지만 Go는 register가 바뀌는 다음 방문 뒤에 만든다.
- **pass 범위**: 한 flush pass는 시작 시점에 dirty였던 entry만 한 번씩 쓴다. pass 중 다시 dirty가 된 entry는 다음 pass가 쓴다. 새 방문이 계속 들어와도 pass가 끝난다. tag flush는 별도 goroutine이 한다.
- **쓰기 방식**: 파일이 없거나 크기가 snapshot과 다르거나(resident만, partial은 `ErrCorrupt`), resident 쓰기가 실패한 뒤이거나, hourly entry의 첫 쓰기이면 temp 파일+rename으로 통째로 쓴다. hourly entry는 restart 뒤 double suffix 경로에서 읽혀(원본 동작 유지) 저장 경로의 파일과 내용이 다를 수 있으므로 처음에는 통째로 쓴다. 그 밖(`synced`)에는 바뀐 4KiB page만 제자리에 쓴다(resident는 표시된 page, partial은 delta가 닿는 page를 읽어 max). 파일 크기와 header는 바뀌지 않고 register는 커지기만 하므로, 쓰기가 중간에 끊겨도 파일은 일부 방문만 빠진 유효한 HLL이다. 원본은 truncate 후 통째로 다시 써서 끊기면 잘린 파일이 남을 수 있다. 조회는 파일마다 `visitorIO`를 잡고 읽으므로 진행 중인 page 쓰기를 섞어 읽지 않는다. pass는 디렉터리를 pass마다 한 번 검사하고 파일은 자신만 Lstat한다. 경로 문자열은 쓸 entry에만 만든다. 이전에는 entry마다 모든 경로 성분을 Lstat하고 dirty entry 전부의 경로를 미리 만들었다. Close는 Store gate 아래에서 dirty entry를 모두 쓴다(최대 objType 100+object 2×32768개). 그래서 종료 시간이 그 쓰기 수에 비례한다(entry당 open·pwrite·close, page 단위).
- **버퍼**: snapshot image는 `visitorIO`를 가진 쪽이 소유하는 버퍼 하나를 재사용한다. entry마다 약 700KiB를 할당하지 않는다.
- **이름 충돌**: objType 이름과 object hash 이름(Hexa32)이 같으면 원본처럼 `visit/{name}.usr` 하나를 같이 쓴다. 원본은 마지막 writer가 이기고, Go는 두 entry의 page가 섞일 수 있다. 어느 쪽도 정상 계약이 아닌 원본 충돌 동작이다.
- **top100 rebuild와 purge**: rebuild가 hold 사이에 있을 때 purge나 DB 삭제가 일어나면 rebuild를 조용히 멈춘다. 지워진 디렉터리에 cache를 다시 만들지 않는다. purge counter는 Store 전체 단위라 다른 날짜의 purge로 멈춘 path는 파일이 남아 있으면 다음 cycle에 다시 만든다.

### 8.2 text dictionary 쓰기

원본 `TextWR`는 queue와 daemon thread로 text를 쓰고, `TextCache.put`으로 바로 조회되게 한다. Go도 같다(`textqueue.go`).

- **PutText**: 날짜, div, UTF-8, 길이를 바로 검증한다. 오류는 동기로 반환한다. 대상은 넣는 시점의 `mgr_text_db_daily_*` 값으로 정한다. daily면 `{date}/text/text`, 아니면 `00000000/text/text_{div}`이고, 동기 쓰기 때와 같다. Java oracle(`TestJavaStorageInteropConfigTextDaily`)도 이 순서로 비교한다.
- **미지 div 상한(R7)**: 영구 대상이고 원본 17개 div(`TextTypes`)가 아닌 div가 영구 index도 허용 기록도 없으면, 영구 index를 가진 미지 div가 `DefaultMaxUnknownTextDivs`(64, `SetMaxUnknownTextDivs`) 미만일 때만 받는다. 넘으면 queue에 넣지 않고 `ErrTextDivLimit`(`ErrLimit`)을 반환하고 `TextDivsLimited`로 센다. 기존 index가 있는 미지 div는 계속 받는다. 개수는 `00000000/text` 디렉터리 읽기(첫 미지 div, 상한에서 최대 10초마다)와 pending·최근 허용으로 다시 센다. 읽기 오류는 10초 기억한다. 상한은 `go_text_max_unknown_divs`(live)다. div는 244byte 이하(`maxTextDivBytes`)다. 세부와 원본 근거는 [storage-contract](../../docs/compatibility/storage-contract.md) "영구 Text div 상한"이다.
- **pending**: 정한 대상과 key로 pending에 넣고 writer queue(`textQueueMax` 8192)에 보낸다. 같은 key가 pending에 있으면 넣지 않는다. 파일과 같이 첫 값이 이긴다.
- **queue가 가득 찼을 때**:
  - 상한은 개수 8192와 byte `textQueueMaxBytes` 32MiB다. text 하나는 queue가 비어 있으면 상한보다 커도 받는다.
  - 상한에 닿으면 PutText는 빈자리를 최대 `textQueueWait`(100ms) 기다린다. ingest는 이 대기를 engine lock 밖에서 한다(§8.5).
  - 그래도 차 있으면 그 text를 버리고 센다(`TextDropped`: `/health` maintenance `textDropped`, 종료 stats line; log S140). 원본 `TextWR`도 queue가 가득 차면 S140을 남기고 버린다.
  - 한 번 시간이 초과되면 writer가 queue를 개수와 byte 모두 절반 이하로 줄일 때까지(saturated) 기다리지 않고 바로 버린다. 그래서 text 저장이 멈춰도 수집은 포화 한 번에 최대 100ms만 늦어지고, 경계에서 대기와 버림이 번갈아 생기지 않는다(`TestTextQueueStalledDropsWithoutSlowingIngest`).
  - 버린 text는 작은 메모리 cache(`textRecentMax` 8192개, `textRecentMaxBytes` 4MiB, 먼저 들어온 것부터 밀려남)에 남아 조회에 보인다. 원본도 `TextCache`에 먼저 넣고 queue에 보내므로 버린 text가 화면에 보인다. 파일에는 쓰이지 않으며, Agent가 같은 text를 다시 보내면 그때 저장된다. purge·삭제는 대상 아래의 cache 항목도 버린다.
  - ctx가 끝나면 그 text는 저장하지 않고 ctx 오류다. Close는 기다리는 호출을 `ErrClosed`로 깨운다. Sync barrier는 시간 제한 없이 기다린다.
- **writer**: Store가 소유한 goroutine 하나다. `Open`이 시작하고 `Close`가 끝내므로, Open한 Store는 반드시 Close해야 한다.
  - Store gate를 한 번 잡을 때 queue에 있는 text를 `scanHoldSteps`(256)개 또는 `scanHoldTime`(2ms)까지 받아, 사전(index 경로)마다 묶어 쓴다(`putTextBatchLocked`). 각 text의 `hasKey` 확인 뒤, 새 text의 data를 한 번에, index 레코드를 한 번에 쓴다. 같은 묶음 안의 같은 key는 첫 값만 쓴다. 파일 결과는 하나씩 쓸 때와 byte 단위로 같다(`TestTextBatchMatchesSingleWrites`). 로컬 replay에서 XLog 1,000건당 서버 CPU 약 3% 감소(mix 15,000/s). 그래서 수집 engine lock은 text 쓰기를 기다리지 않고, text가 몰려도 gate를 text마다 따로 잡지 않는다.
  - hold가 끝나면 그 길이만큼 gate를 잡지 않고(`textYieldFactor` 1, gate 점유율 ≤1/2), queue가 개수나 byte 기준으로 절반을 넘으면 쉬지 않는다(`textBacklogYieldFactor` 0). 예전 값(3, backlog 때 1)은 XLog·profile 쓰기가 engine lock 안에 있어 text hold가 그것을 늦추던 때의 것이다. 지금은 XLog·profile이 자기 writer에서 쓰이고(§8.3) 수집의 group 조회는 읽기 cache를 쓰므로, 큰 휴식은 text queue만 채워 수집을 기다리게 했다. 로컬 replay 22,000 mix XLog/s에서 UDP queue가 7,000~8,000까지 늘고 reject가 났는데, 바꾼 뒤 약 300에 머물렀다. 신규 text 100% 폭주(20,000 XLog/s)에서도 XLog 손실은 0이었다(3/3).
  - pending 여부는 gate 안에서 다시 확인한다. purge·삭제가 gate 안에서 버린 text는 쓰지 않는다.
  - 저장 오류는 `TextWriteErrors`(`/health` maintenance `textWriteErrors`, 종료 stats line)로 센다. 재시도는 없다. 그 key는 다음 PutText 때 다시 저장된다. 원본 TextWR도 오류를 log만 남긴다.
- **조회**: `TextContext`는 원본 `TextRD` 순서(daily 다음 permanent)대로 pending을 먼저 보고 파일을 읽는다. 첫 번째로 볼 곳의 pending은 Store gate 없이 먼저 본다. 방금 도착한 text를 찾는 수집 group 조회가 writer의 hold를 기다리지 않게 하기 위해서다. gate 없이 답하는 부분은 `TextCached(date, div, hash) (text, found, known)`로 따로 부를 수 있다(known=false면 `TextContext`가 파일을 읽을 경우). ingest는 engine lock 안에서는 `TextCached`만 쓰고, 모르면 lock 밖에서 `TextContext`로 읽어 cache에 남긴다(ingest CONTRACT §11 "service text 미리 읽기").
- **읽기 cache**(원본 `TextCache` 역할): 파일에서 읽은 text를 (index 경로, key)별로 기억한다(`rememberRead`, byte 상한 `Config.TextCacheBytes`, 기본 128MiB, `go_text_cache_mb`. 넘으면 앞에서부터 내보내되, 앞에 온 뒤로 한 번이라도 읽힌 항목은 한 바퀴 더 둔다(second chance). 그래서 한 번 쓰이고 마는 text가 얼마나 지나가도 XLog마다 읽는 text는 남는다. 한 번 넣을 때 한 바퀴 더 두는 항목은 64개까지다(`textReadSpare`): 모든 항목이 쓰이는 중이면 넣기 한 번이 cache 전체를 도는 것을 막는다. 상한의 1/8보다 큰 text는 넣지 않는다. 이전에는 65,536개·16MiB FIFO였다). 첫 번째로 볼 곳의 cache도 Store gate 없이 보므로, XLog마다 하는 group 이름 조회가 writer hold를 기다리지 않는다. 저장된 text는 바뀌지 않으므로(첫 값이 이긴다) 낡지 않는다. 경로별로 따로 두어 daily·permanent 순서와 D-T1은 그대로다. purge·DB 삭제는 대상 아래의 항목을 버린다(`TestTextReadCacheSkipsGate`).
- **"없음" cache**: 파일에 없던 ref를 기억한다(`rememberMissing`, 최대 262,144개). text가 아직 오지 않은 service의 XLog마다 group 조회가 Store gate 안에서 파일을 다시 읽던 것을 막는다(로컬 replay 25,000 mix XLog/s에서 group 조회의 gate 대기 16.1초 → 2.9초). 그 ref의 text가 queue에 들어오는 순간 항목을 지우므로, 나중에 도착한 text를 숨기지 않는다. 파일 읽기와 기록은 gate 안에서 하므로 그 사이에 쓰인 text는 pending에 있다. 조회 순서(첫 곳의 pending·cache·"없음", daily가 없으면 permanent)는 원본 `TextRD`와 같다(`TestTextMissingCache`).
- **재제출 건너뛰기**(원본 `TextDupCheck`): writer가 새로 쓴 text는 읽기 cache에 기록하고, 읽기 cache에 있는(저장이 확인된) ref의 PutText는 queue에 넣지 않는다. 파일에서는 첫 값이 이기므로 결과는 같다. group 이름처럼 매번 다시 제출되는 text로 writer가 Store gate를 쓰던 것이 줄어든다(`TestTextStoredNotQueuedAgain`).
- **파일 읽기는 한 번에 하나**(`textQueue.slot`): cache가 답하지 못한 `TextContext`는 Store gate 앞이 아니라 slot 앞에서 줄을 선다. gate는 FIFO라, 이전에는 cache를 놓친 조회(Pulse 변환 worker, Client)가 모두 gate에 줄을 서 writer가 그 뒤에서 기다렸다. 이제 writer 앞에 서는 text 읽기는 하나다. slot을 얻은 뒤 cache를 다시 보므로 같은 text를 여럿이 찾아도 파일은 한 번 읽는다(`TestTextConcurrentMissesReadTheFileOnce`). ctx가 끝나면 줄에서 나온다. 날짜나 div가 유효하지 않은 조회는 slot과 gate를 기다리지 않고 바로 `ErrInvalid`다(`TestTextInvalidLookupDoesNotWaitForGate`).
- **기다림에 상한이 있는 조회**(`TextWithin`, `textfetch.go`): 수집 경로용이다. cache가 답하지 못하면 Store의 fetcher goroutine 하나가 읽고(대기열 4096, 같은 text는 한 건. fetcher는 slot에 줄 서지 않고 gate를 바로 기다린다. 그래서 gate 앞의 text 읽기는 slot 하나와 fetcher 하나, 최대 둘이다), 호출자는 `wait`까지만 기다린다. 그 안에 답이 없으면 `known=false`로 돌아오고 읽기는 계속되어 cache에 남는다. 한 번 시간이 넘으면 1초 동안(`textSlowHold`) 뒤따르는 조회는 기다리지 않고 읽기만 맡긴다. 1초가 지나면 다시 기다려 보고, 여전히 느리면 그중 하나가 시간을 넘겨 다시 1초를 연다(호출자당 1초에 대기 한 번). 재기동 직후처럼 cache가 비어 있고 한꺼번에 많은 text를 찾으면 읽기가 fetcher 뒤에 밀려, store가 느리지 않아도 잠깐 이 상태가 될 수 있다(밀린 읽기가 끝날 때까지와 그 뒤 1초). 이때 `GO-TEXT` 로그를 10초에 한 번 남긴다. page cache가 비워졌거나 디스크가 느려 gate hold가 길어져도 수집 goroutine이 gate 뒤에 서지 않게 하는 것이 목적이다(`TestTextWithin`, ingest CONTRACT "service text 미리 읽기"). Close는 fetcher의 읽기를 취소하고 대기 중인 요청을 `ErrClosed`로 끝낸다(`TestTextWithinDuringClose`). 계수는 `/health` `textCache`(`TextCacheStats`)다.
- **purge·DB 삭제**: 대상 아래의 pending text와 버린 text cache를 버리고, writer는 그 항목을 건너뛴다.
- **Sync·Close**: `Store.Sync`는 그때까지 queue에 들어온 text가 쓰일 때까지 기다린다. `Close`는 받기를 멈추고, 이미 받은 text를 모두 쓴 뒤 writer가 끝나면 gate를 잡는다. Close 뒤의 PutText는 `ErrClosed`다.
- **crash**: queue에 있던 text(보통 수 ms 분량)는 잃는다. 원본 queue와 같은 성격이다.
- **원본과 다른 점(의도)**:
  - daily/permanent 대상을 원본 `TextWR.process`는 처리 시점에, Go는 PutText 시점에 정한다. `mgr_text_db_daily_*`를 바꾸는 순간 queue에 있던 text만 저장 위치가 다를 수 있다. 바꾼 뒤 들어온 text부터 새 설정을 따르는 것은 같다.
  - queue가 가득 차면 원본은 바로 버리고, Go는 최대 100ms 기다린 뒤 버린다. 포화 중에는 Go도 바로 버린다.
  - 같은 key에 다른 text가 연달아 오면, 원본 `TextCache`는 마지막 값을 보여 주고 파일에는 첫 값이 남는다. Go는 처음부터 첫 값을 보여 준다. hash 충돌에서만 생긴다.

### 8.3 XLog·profile 쓰기

원본은 수집 thread에서 디스크에 쓰지 않는다. `XLogCore`는 encode한 XLog를 `XLogWR`(queue `xlog_queue_size` 기본 10000, writer thread 하나)에 넘기고, `ProfileCore`는 profile을 `XLogProfileWR`(`profile_queue_size` 기본 1000)에 넘긴다. queue가 가득 차면 S144·S142를 남기고 버린다. Go도 같다(`packwriter.go`).

- **수집 경로**: `EnqueueXLogContext`·`EnqueueProfileContext`가 검증·encode(profile은 복사)한 뒤 writer queue에 넣는다. 검증 오류(`ErrInvalid`, `ErrLimit`)는 바로 돌려준다. 수집 engine lock은 XLog·profile 디스크 쓰기를 기다리지 않는다. 로컬 측정(mix 9,000 XLog/s)에서 engine lock 안 CPU의 대부분이 이 `pwrite`였고, UDP worker들이 그동안 engine lock을 기다렸다.
- **queue 상한**: 개수는 `xlog_queue_size`·`profile_queue_size`(기동 때 한 번 읽음, 재기동 키), byte는 종류마다 `packQueueMaxBytes` 64MiB다. 레코드 하나는 queue가 비어 있으면 상한보다 커도 받는다.
- **가득 찼을 때**: 최대 `packQueueWait`(100ms) 기다린 뒤 버리고 센다(`/health` `xlogQueue`·`profileQueue`의 `Dropped`, log S144·S142). 한 번 시간이 초과되면 writer가 queue를 개수와 byte 모두 절반 이하로 줄일 때까지 기다리지 않고 바로 버린다. 그래서 writer가 느려져도 XLog·profile enqueue는 포화 한 번에 최대 100ms(XLog와 profile이 같이 포화되면 각각 한 번, 최대 200ms)만 수집을 늦추고, 그 뒤로는 늦추지 않는다(`TestPackQueueStalledDropsWithoutSlowingIngest`). 포화 해제 판단과 sender의 포화 표시가 겹치면 대기가 한 번 더 생길 수 있다(무해). ingest는 이 대기를 engine lock 밖에서 한다(§8.5).
  - 범위: 이 격리는 queue가 차는 경우다. 디스크 자체가 멈추면 writer가 Store gate를 잡은 채 쓰기에서 멈추므로, Store gate를 쓰는 다른 수집(visitor, object, alert, status, summary 저장)도 그 hold 뒤에서 기다린다. 이전 동기 경로와 같은 성격이며, Store gate 분리는 다음 단계 과제다.
- **writer**: 종류마다 Store가 소유한 goroutine 하나다(`Open`이 시작하고 `Close`가 끝낸다). hold 하나의 레코드는 gate를 잡기 전에 queue에서 꺼낸다(`scanHoldSteps` 256개 또는 `holdBatchBytes` 1MiB까지, queue가 비면 거기까지). 압축 설정이 켜져 있으면 꺼낸 레코드의 gzip member도 gate 밖에서 만든다(`packQueue.prepare`, writer 자신의 `gzipMember`와 arena). 압축은 압축 쓰기 CPU의 대부분인데, gate 안에서 하면 gate를 쓰는 모두가 그 시간을 기다렸다. member는 gate 안에서 만들 때와 같은 bytes다(`TestPackWriterMembersMatchInGateCompression`). 날짜의 conf 파일이 live 설정과 다르게 고정돼 있으면 gate 안에서 이전처럼 판정한다(준비 안 된 레코드는 거기서 압축, 비압축 날짜는 member를 쓰지 않음). 그 뒤 gate를 잡아 꺼낸 레코드를 쓰고 놓는다. gate를 기다리는 동안 writer가 들고 있는 레코드(최대 256개)는 queue 개수 상한에 들지 않지만 byte 상한에는 쓰일 때까지 계상된다. gate는 FIFO로 넘어가므로 hold 사이에 조회, text writer, 다른 writer가 들어온다. 한 hold의 레코드는 날짜가 같은 연속 구간마다 묶어 쓴다(`putXLogBatchLocked`·`putProfileBatchLocked`). data 레코드를 한 번에 쓰고(`appendDataBatch`), 그다음 index마다 레코드를 한 번에 쓴다(`legacyindex.PutBatch`). 파일에는 레코드를 하나씩 쓸 때(XLogWR처럼 data 다음 time·txid·gxid index)와 같은 byte가 같은 순서로 남는다. index 레코드는 언제나 자기 data 뒤에 쓰인다. 레코드 하나가 크기 상한을 넘으면 그 레코드만 실패한다(`TestPackBatchOversizeFailsAlone`). 쓰기 오류(ENOSPC 등)는 그 묶음 전체의 오류로 센다. 압축 모드는 레코드별로 쓴다. 묶음 쓰기로 로컬 replay에서 XLog 1,000건당 서버 CPU가 mix 약 5%, XLog 전용 약 7% 줄었다. 비압축 파일은 동기 쓰기와 byte 단위로 같다(`TestPackWritersMatchSyncWrites`). 압축 모드에서는 XLog와 profile이 같은 날짜 압축 저장소를 쓰므로 두 writer가 끼어드는 순서대로 위치가 정해진다. 원본의 두 writer thread도 같다. 쓰기 오류는 `WriteErrors`로 세고 원인과 함께 log를 남긴다(XLog `SZ143`, profile `S141`, 각 10초에 한 번, 재시도 없음. 원본은 실패마다 stack trace를 남긴다). 이전에는 같은 오류가 수집 오류(`ingest.StorageErrors`·`Rejected`)로 셌지만, 이제 수집은 enqueue 성공으로 끝나므로 `xlogQueue`·`profileQueue`의 `WriteErrors`로 본다.
- **purge·DB 삭제**: 레코드보다 나중에 일어난 purge·삭제가 그 날짜의 xlog 디렉터리나 그 안의 파일을 지웠으면 writer는 그 레코드를 쓰지 않고 센다(`Discarded`). 동기 쓰기 때 삭제 전에 받은 레코드가 디렉터리와 함께 지워진 것과 같고, 지운 디렉터리를 다시 만들지 않는다(text writer와 같은 정책). 삭제 뒤에 받은 레코드는 이전처럼 쓴다. 기록은 최근 64개, 1시간까지만 본다.
- **보이는 시점**: queue에 있는 레코드는 쓰이기 전에는 조회되지 않는다(보통 ms). 원본도 XLog 조회는 파일에서 읽고, 실시간 XLog는 메모리 cache(Go: engine ring)에서 준다.
- **Sync·Close**: `Store.Sync`는 그때까지 queue에 들어온 레코드가 쓰일 때까지 기다린다. `Close`는 받기를 멈추고 이미 받은 레코드를 모두 쓴 뒤 gate를 잡는다(`TestPackQueueCloseWritesAccepted`). 디스크가 멈추면 Close도 그만큼 기다린다(원본 `closeAll`도 같다). Close 뒤의 enqueue는 `ErrClosed`다. runtime은 Close 뒤 queue 통계를 한 줄 남긴다.
- **crash**: queue에 있던 레코드는 잃는다. 원본 queue와 같다.
- **동기 API**: `PutXLog`·`PutProfile`은 그대로 동기다(이관 도구와 테스트).
- **원본과 다른 점(의도)**: queue가 가득 차면 원본은 바로 버리고, Go는 최대 100ms 기다린 뒤 버린다. 포화 중에는 Go도 바로 버린다.

### 8.4 ingest 비동기 writer가 쓰는 진입점 (2026-10-21)

ingest는 counter·object·status·alert·summary·stack·batch·span·visitor를 kind별 writer goroutine에서 저장한다([ingest CONTRACT](../ingest/CONTRACT.md) §13). 그래서 storage에 ctx와 lock 범위를 나눈 진입점 세 개를 두었다.

- `PutObjectContext(ctx, date, p)`: `PutObject`와 같은 기록을 쓰되 Store gate를 `LockContext`로 잡는다. 종료 deadline이 지나 ctx가 취소되면 gate를 기다리던 쓰기가 아무것도 쓰지 않고 끝난다(`TestPutObjectContextWaitsForGateWithContext`). `PutObject`는 이 함수를 `context.Background()`로 감싼다.
- `VisitorRollContext(ctx, now)`: visitor 날짜·시간 경계 roll만 한다. `visitorMu`만 잡고 Store gate는 잡지 않는다. ingest의 visitor queue가 이것을 방문 offer와 같은 FIFO로 실행해 "older now must not roll" 순서를 지킨다.
- `TagTickContext(ctx, now)`: tag count tick(Store gate 필요)이다. ingest Tick이 engine lock을 놓은 뒤 부른다. 그래서 visitor queue가 Store gate 때문에 막히지 않는다(`TestVisitorRollWithoutGateTagTickWithGate`).
- roll과 tag tick을 Store gate 한 번에 하던 이전 진입점 `VisitorSyncContext`는 런타임에서 쓰지 않아 test helper(`visitor_test.go`)로 옮겼다.

### 8.5 ingest의 기다리지 않는 offer와 lock 밖 submit (2026-10-22)

ingest engine lock은 모든 UDP 수집을 직렬화한다. 그래서 text·XLog·profile queue와 tag inbox의 "가득 찼을 때 최대 100ms 대기"(§8, §8.2, §8.3)를 lock 안에서 하면 그 대기 동안 다른 worker의 수집이 모두 멈춘다. 대기·버림·계수 정책(기존 정책)은 그대로 두고, 대기하는 곳만 lock 밖으로 옮길 수 있게 진입점을 나눴다(`ingestwrite.go`).

- `Prepare*`: `PrepareText(date, p)`, `PrepareXLogEncoded(p, b)`, `PrepareProfile(p)`, `PrepareXLogTags(objType, p)`, `PrepareAlertTags(objType, p)`가 기존 queue 호출과 같은 검증을 하고 `Write`를 돌려준다. queue를 건드리지 않는다. 오류는 기존 호출이 돌려주던 것과 같다(빈 permanent text·빈 profile은 빈 `Write`). text의 daily/permanent 대상, XLog·profile의 purge sequence(§8.3 purge)는 Prepare 때 정한다.
- `Offer(w) (done, err)`: 기다리지 않는다(queue의 leaf lock만). 들어감·같은 key 중복(text)·포화 중 버림이면 done=true다. 가득 찼지만 포화 전이면 queue에 넣지 않고 done=false다(예약했던 bytes는 되돌림). 오류는 `ErrClosed`뿐이다.
  - **text 보류**: done=false인 text는 queue bytes 없이 pending에 남는다(`textItem.held`, "없음" cache에서도 지운다). 그래서 Submit 전에도 `TextCached`/`TextContext`가 그 text를 찾는다. PutText가 lock 안에서 빈자리를 기다리던 동안 보이던 것과 같다(queue가 bytes로 가득 찼을 때는 이전에도 대기 중 pending이 아니었다. 이제는 그 경우에도 보인다). Submit은 같은 item을 넣거나(pending은 그대로 그 item), 대기 끝에 버리면 pending에서 버린 text cache로 옮기고, `ErrClosed`·ctx 오류면 pending에서 뺀다. 보류 중 purge·삭제가 그 pending을 지웠으면 Submit은 쓰지 않고 끝낸다(버림으로 세지 않음). writer는 이전처럼 `pending[ref] == item`인 것만 쓴다.
  - **tag purge 세대**: Prepare가 tag 기여에 tag purge 세대(`tagState.purges`, atomic이라 lock 없이 읽음)를 찍는다. inbox에 넣을 때(`tagEnqueue`) 그 뒤 purge·삭제가 자기 경로를 덮었으면 버린다(세지 않음, inbox에 이미 있던 기여를 purge가 버리는 것과 같다). purge는 세대를 올리고 기록을 tag gate와 `inboxMu`를 둘 다 잡은 채 남기므로, inbox 쪽은 `inboxMu` 아래에서 둘을 같이 본다. Prepare와 Submit 사이에 purge가 기록 길이(`tagPurgeLogMax` 64)보다 많이 일어나 판정할 수 없으면 버리지 않는다(다른 경로의 기여까지 세지 않고 버리는 것보다, 이전처럼 그 디렉터리를 다시 만들 수 있는 쪽을 택함. 100ms 안에 purge·삭제 64번은 현실적이지 않다).
- `Drop(w)`: 기다릴 수 없는 호출자(ingest의 panic 경로)가 w를 그 queue의 drop으로 버린다(text: `TextDropped`·버린 text cache, XLog·profile: `Dropped`, tag: `InboxDropped`, 각 원본 로그 id). `Write.Size()`는 w가 기다리는 동안 쥔 메모리 추정이다.
- `Submit(ctx, w)`: 기존 호출과 같은 대기·버림·계수다. 기존 `PutTextContext`/`EnqueueXLog*Context`/`EnqueueProfileContext`/`Put*TagsContext`는 이제 Prepare + Submit이고 동작은 같다(tag의 ctx 확인은 날짜 검증 뒤로 옮겨졌다: 둘 다 오류인 입력에서 오류 종류만 다를 수 있다).
- `Write.Queue()`는 대상 queue(`TextQueue`/`XLogQueue`/`ProfileQueue`/`TagQueue`)다. 같은 queue의 Write를 한 goroutine이 Offer·Submit 순서대로 넣으면 그 순서로 들어간다. ingest는 한 hold에서 어떤 queue에 deferred가 생기면 그 queue의 뒤 Write를 Offer하지 않고 뒤에 붙인다.
- 원본과의 차이(그대로): 원본은 가득 차면 바로 버린다. Go는 포화 전 한 번 최대 100ms 기다린다. 이제 그 대기는 쓰는 goroutine만 멈춘다.
- 검증: `ingest_lock_test.go`(package `storage_test`, ingest engine과 함께, `export_test.go`의 `HoldGate`로 Store gate를 잡아 writer를 멈춤): `TestIngestFullStorageQueueWaitsOutsideEngineLock`(text·XLog·profile queue를 채우고 한 worker가 기다리는 동안 다른 worker의 수집이 50ms 안에 끝남, 기다린 쪽은 100ms 뒤 1건 버림), `TestIngestColdServiceTextReadOutsideEngineLock`, `TestIngestDelayedServiceTextReadAhead`, `TestIngestDeferredXLogsKeepTheirOrder`, `TestIngestDeferredTextVisibleToOtherCollectors`, `TestIngestCloseDuringDeferredSubmit`, `TestIngestServiceTextReadAheadCanceled`, `TestIngestServiceTextReadErrorRejectsAsBefore`. `ingestwrite_test.go`(package `storage`): `TestOfferHeldTextReadableThenSubmitted`, `TestHeldTextPurgedBeforeSubmitIsNotWritten`, `TestDropHeldText`, `TestTagEntryPurgedBeforeSubmitIsDiscarded`, `TestTagEntryOlderThanPurgeLogIsKept`; `ingest_lock_test.go`의 `TestIngestManyReleasedXLogsWithBriefFullQueue`, `TestIngestLargeProfileWithFullQueueWaits`(둘 다 hold당 상한에서 바로 버리던 구현에서 실패 확인). 수정 전 코드에서 실패함을 확인한 것: `TestIngestFullStorageQueueWaitsOutsideEngineLock`·`TestIngestColdServiceTextReadOutsideEngineLock`(다른 worker 약 80ms, gate를 잡은 동안 1초 timeout), `TestIngestDeferredTextVisibleToOtherCollectors`(보류 없이 group 0), `TestTagEntryPurgedBeforeSubmitIsDiscarded`(세대 확인 없이 purge 뒤 inbox에 들어감).

### 8.6 realtime counter 이름 사전 `real.head` (R6·R7 일부, 2026-10-23)

원본 형식과 차이는 [storage-contract](../../docs/compatibility/storage-contract.md) "Realtime counter 이름 사전"이 정본이다. 여기에는 구현 계약만 적는다(`counter.go`).

- **읽기**(`loadNames`): Java 생성자(`RealtimeCounterDBHeader.scala:78-96`)처럼 읽는다.
  - 파일이 0으로 채워져 있다고 보고 읽는다. Java는 2048byte 미만 파일을 padding하고, 짧게 읽힌 buffer는 0으로 남기 때문이다.
  - offset 512의 int32 길이가 4 미만(0, 음수 포함)이면 빈 사전이다. count가 0 이하여도 빈 사전이다.
  - count개 entry만 읽고, 그 뒤 byte는 길이 안이라도 무시한다.
  - 그래서 다음 Java torn 형태가 모두 열린다(`TestRealtimeCounterNamesJavaTornForms`, 수정 전 8가지 모두 실패).
    - `saveTags`가 길이만 새 값으로 쓰고 body는 이전 그대로인 경우(파일 끝 또는 0 padding).
    - 2048byte 미만 파일, 0byte 파일, 길이만 있는 파일.
  - `headBody`는 파일의 길이 값이 아니라 실제로 읽은 끝(`d.Offset()`)이다. 그래서 다음 append는 마지막 entry 바로 뒤에 쓰고, 새 길이가 꼬리를 덮는다.
- **읽기 상한**: body 64MiB(`realtimeHeadMaxBody`, 이름 하나도 이 크기까지), 이름 1,048,576개(`realtimeNamesReadMax`). 넘으면 `ErrLimit`이다.
  - count가 body에 들어갈 수 없으면(entry 최소 5byte) 할당 전에 `ErrInvalid`다.
  - id 범위 밖, 중복 id, 잘못된 UTF-8도 `ErrInvalid`다. Java라면 예외 직전까지 읽은 entry를 남기고 계속한다.
  - 근거: 이름 하나에 이름 byte에 더해 slice·map 약 100byte heap이 든다. 그래서 상한에서 table당 약 160-220MB다. 이 규모면 Java도 새 이름마다 body 전체를 다시 쓰므로, 운영에서 현실적이지 않다고 본다.
- **같은 이름이 여러 id**: 파일 순서상 마지막 entry의 id로 찾는다. Java `tagStrInt.put` 순서와 같다(`TestRealtimeCounterNamesRepeatedName`, 이전 Go는 큰 id를 골랐다).
  - 새 이름의 id는 Go가 count+1이다. Java는 서로 다른 이름 수 + 1이라 이미 쓰인 id와 겹친다(원본 결함 후보). 그런 사전에서는 다음 id가 Java와 Go 사이에 갈라진다.
- **load 실패**: 사전을 index 쌍보다 먼저 읽는다. 그래서 읽을 수 없는 사전은 index walk와 다른 table eviction 없이 실패한다.
  - 실패는 `.head` 경로와 파일 크기·mtime으로 최대 64개(`headFailuresMax`)까지 기억한다(`Counters.headFailures`, gate 안).
  - 같은 파일이면 pack마다 다시 읽지 않고 같은 오류를 돌려준다. 파일이 바뀌면 다시 읽는다(`TestRealtimeCounterNamesLoadFailureBeforeIndex`).
  - 조회는 `.data`가 없는 날짜를 이전처럼 빈 결과로 본다.
- **쓰기 상한**(`putRealtime`): Java가 쓴 이름을 포함한 날짜 사전 **전체**가 `Counters.namesMax`(기본 `realtimeNamesDefaultMax` 65,536)개, body `realtimeNamesMaxBody`(16MiB) 안일 때만 새 이름을 더한다. 설정 키는 없다.
  - 근거: 사전은 날짜 공유라 서로 다른 이름 수를 따른다. Telegraf tag 치환 이름처럼 출처 수에 비례하는 이름도 받으면서, Go가 쓴 사전은 늘 읽기 상한 안에 둔다.
  - 인증 없는 UDP로 채울 수 있는 양(R7)과 메모리(table당 이름 16MB + 약 6.5MB, 열린 table 최대 8개)를 묶는다.
- **상한에 닿은 pack**: 새 이름의 entry만 빼고 나머지 entry는 pack 순서대로 저장한다. entry가 모두 빠지면 record와 index를 쓰지 않는다. 오류는 반환하지 않으므로 ingest `WriteInvalid`로 세지 않는다.
  - `Counters.NamesLimited()`가 뺀 entry 수를 센다(atomic). runtime은 `/health` `maintenance.counterNamesLimited`와 종료 stats `counterNamesLimited=`로 낸다.
  - 로그는 `Counters.SetLog` sink로 `S133`(원본 `RealtimeCounterWR` 오류 id)을 10초에 한 번 남긴다. 날짜, 사전 크기, 이번 pack에서 뺀 수, objName(200자), 첫 이름(64자, quote)을 담는다.
  - Java 사전이 이미 상한보다 크면 그대로 읽는다. 아는 이름은 계속 저장하고 새 이름만 뺀다.
- **저장**(`saveNames`): 새 이름이 생긴 pack마다 한 번 저장한다. pack 안의 새 이름은 한 번에 붙인다. 레코드가 새 id를 가리키기 전에 끝난다. 순서는 다음과 같다.
  1. 새 entry만 읽은 끝(516+`headBody`) 뒤에 쓰고 fsync한다.
  2. offset 512의 길이·count 8byte(한 sector)를 쓰고 fsync한다.
  - 2 전에 중단되면 파일은 이전 사전 그대로다. 그 뒤 entry는 Java·Go reader 모두 무시하고, 다음 저장이 덮어쓴다.
  - `.head`가 없으면(새 날짜, 또는 열린 table 아래에서 지워진 경우) 사전 전체를 temp 파일 → fsync → rename → 디렉터리 fsync로 만든다(2048byte padding, 디렉터리 fsync 실패는 오류, `TestRealtimeCounterNamesRecreatedHead`).
  - 실패하면 그 pack은 저장하지 않는다. `names`·`ids`를 저장 전으로 되돌리고 오류를 반환한다(이전과 같다).
  - 2의 쓰기는 됐지만 fsync가 실패하면 디스크 사전이 메모리보다 클 수 있다. 그 이름을 가리키는 레코드는 없고, 다음 저장이 같은 자리를 덮는다(`TestRealtimeCounterNamesSaveFaults`: 네 단계 각각 실패 주입).
  - 이전에는 pack마다 사전 전체를 temp 파일에 쓰고 rename했다(디렉터리 fsync 없음). 쓰기량이 이름 수의 제곱에 비례했다.
  - **비용**: 새 이름 pack 하나에 Counters gate 안 fsync가 2번이다(이전 1번 + rename). macOS의 `File.Sync`는 `F_FULLFSYNC`라 fsync가 비용을 결정한다. 측정 조건은 Apple M1 Pro, `-count=3`, 다른 테스트가 함께 돌던 조건이다(잡음 있음). Linux에서는 측정하지 않았다.

    | bench | 이전(HEAD) | 이후 |
    |---|---|---|
    | `BenchmarkCounterPutRealtimeNewName`(사전 3000개, 새 이름 1개) | 6.9-8.1ms | 10.3-10.4ms |
    | `BenchmarkCounterRealtimeDayRollover`(하루 첫 pack: 객체 100개, 공유 이름 40개 + 객체마다 2개, 저장 100번, index 생성 포함) | 0.61-0.67s | 0.91-1.06s |

    - 작은 사전에서는 fsync 1번이 더 늘어 약 1.4배 느리다. 대신 이전 방식의 쓰기량은 사전 크기에 비례했다(65,536개면 저장마다 MB 단위).
    - 새 이름 저장은 하루 시작과 새 Agent·counter가 처음 올 때 몰린다. 이미 pack 단위로 묶어 붙이고, 그 이상(여러 pack 묶기)은 하지 않았다.
    - fsync를 1번으로 줄이면 전원 손실 때 길이만 새 값으로 남을 수 있다(같은 파일 안 쓰기 순서 보장 없음). 그래서 줄이지 않았다.
- **id cache(R6)**: `counterTable.ids`(이름→id)는 `loadNames`가 한 번 만들고 `putRealtime`이 갱신한다. Counters gate 안에서만 읽고 쓴다. 조회는 이전처럼 hold 시점 `names` slice만 gate 밖으로 가져간다.
  - 이전에는 pack마다 사전 전체로 map을 다시 만들었다. 레코드 byte는 같다(Decimal count, (Decimal id, Value) pack 순서).
  - 측정 조건: `BenchmarkCounterPutRealtimeKnownNames`, 20 counter pack, 아는 이름, data·index 쓰기 포함, Apple M1 Pro, `-count=6 -benchtime=20000x`, 중앙값. ns/op는 서버 처리량으로 환산하지 않는다.

    | 사전 크기 | 이전 | 이후 |
    |---|---|---|
    | 300개 | 12.1µs, 14,320B, 14 alloc | 6.9µs, 712B, 11 alloc |
    | 4000개 | 134.8µs, 219,162B, 28 alloc | 7.1µs, 728B, 11 alloc |
- 검증: storage-contract 해당 절의 테스트, `-race` `Counter|Realtime|Daily|Java`. 실패 주입 hook은 `counterHeadFault`(test 전용, 단계 이름 `entries`/`entries-sync`/`length`/`length-sync`)다.
- 남은 범위: 레코드 하나의 entry 수는 여전히 4096 이하만 읽는다(`deliver`·`counterCatalogMap`). 수신 codec 기본 `MaxCollectionEntries`와 같은 값이다. Java가 4096개 넘는 MapValue pack을 받아 쓴 레코드는 읽지 못한다.

## 9. 검증

- realtime counter hold(`counter_scan_test.go`): 이전 one-hold 구현 oracle과 realtime·rows·bulk 결과·오류 동일(5500건, dense bucket 포함), hold 사이 다른 goroutine의 PerfCounterPack 수집 완료, 연속 ingest 중 결과 동일, eviction·purge·재생성·inode 재사용(날짜별 purge 횟수)·다른 날짜 purge·Close·취소.
- key 하나의 수집 조회(`profile_test.go`): 이전 한 hold 구현 oracle과 profile·gxid XLog·span 결과·오류 동일(압축·비압축, 건수·byte 상한, 잘린·없는 data 파일), hold 사이 다른 goroutine의 PutXLog 완료(비압축은 index hold 3번뿐, 압축 hold당 최대 256 record), 24MiB profile 조회 중 PutXLog 대기, hold 사이 purge(ALL, PROFILE+XLOG)·DB 삭제·purge 후 같은 offset 재생성·Close(비압축은 조회 시작 시점 결과, 압축은 빈 결과 또는 `ErrClosed`), 조회·쓰기·purge 동시 실행(-race), purge 기록 밀림 시 재조회로 같은 결과·매번 밀리면 `ErrPurgeOverlap`(`TestCompressedCollect*`).
- gate 밖 chain walk·block 해제(`collect_gate_test.go`): bucket 1개 chain에서 oracle과 동일, 긴 chain walk·cold 30MiB block 해제 중 PutXLog 최악 대기가 gate 안에서 할 때의 1/2·3/4 미만(이전 구현에서 실패 확인), gate 밖 해제 무효화 시 4회 뒤 gate 안 해제, 해제 중 append·purge·DB 삭제·Close, 동시 해제 상한, 읽기·쓰기·eviction·purge 동시 실행(-race). `legacyindex/keychain_test.go`와 `FuzzIndexFiles`: `KeyChain` walk가 `ScanKeyContext`와 같은 레코드·순서·오류, snapshot 뒤 append·Close·rename·삭제 무관, 쓰기와 동시 walk(-race).
- batch archive(`batch_archive_test.go`): body를 읽는 동안 다른 goroutine의 PutXLog 완료, 동시 업로드 마지막 rename 승리, 업로드 중 purge·대상 변경·Close 시 publish 없음, 조회 handle의 inode 유지, 업로드·조회 동시 수 상한.
- 기능 격리(`tagcount_flush_test.go`, `text_context_test.go`): tag inbox·text queue 정지 시 수집 대기 상한과 drop, pending 상한의 entry 단위 drop과 추정 경계, pass hold의 drain 상한, text 포화 hysteresis, 버린 text cache 상한·purge, 대기 중 Close.
- tag flush·rebuild(`tagcount_flush_test.go`, §8): 한 hold에 1 record로 쪼갠 incremental flush·rebuild와 기존 전체 flush·한 hold rebuild의 first/next/hfile/data/top100 파일 byte 동일, hold 사이 기여(이미 쓴 key·남은 key·새 key, inline 상한 flush 포함)의 무손실·무중복과 hold 사이 record+pending 합 일치, 기본 hold 상한에서 다른 goroutine의 gate 획득, engine tick은 due 표시만 함, hold 사이 Close 뒤 pass 정지와 무손실.
- 단위(`live_test.go`, `legacyindex/deepsearch_test.go`): text 전환·fallback·D-T1, `table`/`city` 영구, 압축 새 날짜만, visitor hourly, index 생성 크기·기존 파일 유지·KV 8MiB, S152/S161 조건·rate limit·hasKey 무경고, retention Apply, zip cache 상한, setter 동시 호출(-race).
- Java oracle(`config_java_test.go`, `make storage-check` 대상, `tools/java-storage/ConfigStorageOracle.java`):
  - `TestJavaStorageInteropConfigTextDaily`: 원본 `TextWR.process`·`TextPermWR`·`TextRD.getString`. 일별 off→on→off 전환 쓰기 뒤 Java/Go DB 17개 파일 byte 동일, 세 설정(off/on/일부)과 두 날짜(table 있음/없음)에서 15개 key 조회 결과 동일(fallback·D-T1 포함). TextCache를 피하려고 (설정, 날짜)마다 새 process로 읽는다.
  - `TestJavaStorageInteropConfigIndexSizes`: 원본 `TextPermIndex`/`TextTable`/`XLogIndex`/`KeyValueStoreIndex`를 키 값(2..9, KV 8, xlog id 3)으로 생성한 파일과 Go 파일 byte 동일, Java가 기본 설정으로 Go 파일을 읽고 Go가 Java 파일을 읽는다.
  - `TestJavaStorageInteropConfigPurge`: 원본 `AutoDeleteScheduler`(생성자 등록 ConfObserver 실행, `run()` 한 회차 메서드 순서)로 4단계 설정 변경(끄기 포함) 후 남은 파일 목록이 `Retention.Apply`+`RunOnce`와 같다. 디스크 사용률 purge(`mgr_purge_disk_usage_pct`)는 실제 디스크에 의존해 0으로 두었다(기존 `TestRetentionDiskUsageThresholds`가 Go 단위로 검증).
  - harness 경계: writer thread는 `DBCtr.running=false`로 바로 끝나고 `TextPermWR` writer loop 본문(hasKey→write→set)과 `FlushCtr` 주기 flush 1회를 harness가 수행한다. 원본 `TextPermIndex.closeAll`은 reference가 남은 index를 닫지 않아 header를 쓰지 않는다(운영 서버는 FlushCtr 4초 주기 flush로 기록). 이는 원본 동작이며 harness가 flush를 대신한다.

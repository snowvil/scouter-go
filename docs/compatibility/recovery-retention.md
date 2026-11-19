# Retention·종료·재시작 복구 계약

정본은 `baseline.json`에 고정한 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. 근거 경로의 약어는 `S/`=`scouter.server/src/main/scala/scouter/server/`, `J/`=`scouter.server/src/main/java/scouter/server/`, `C/`=`scouter.common/src/main/java/scouter/`이다. 이 문서의 원본 동작은 모두 **코드 판독 결과**이며 Java 실행 재현은 아직 하지 않았다. 파일 형식은 [storage-contract.md](storage-contract.md), lock/종료의 Go 측 계약은 [runtime.md](runtime.md)를 따른다.

결론: 원본에는 자동 tail truncate, index rebuild, count 보정, fsck가 **없다**. 대신 손상을 묵시적으로 "초기화"하는 경로가 두 개 있다. `.hfile` 길이가 1024 미만이면 새 파일로 간주하고, `count.dat` 길이가 8 미만이면 0으로 간주한다. 정상 종료도 WR queue를 drain하지 않고 index header flush를 보장하지 않는다.

## 1. Retention (`AutoDeleteScheduler`)

### 설정과 실효 기본값

`Configure` 필드 초기값과 `apply()`의 `getInt` 기본값이 서로 다르다. 실제 기동 시에는 `apply()`의 값이 적용된다(`J/Configure.java:306-337,772-792`). 단위는 모두 **일**이고, `0` 이하이면 해당 day-limit purge를 끈다(`J/core/AutoDeleteScheduler.java:180,200,...,323`의 `> 0` 검사).

| 키 | 실효 기본값(키 미설정 시) | 대상(mode) |
|---|---|---|
| `mgr_purge_enabled` | true | 전체 on/off. 매 주기 확인한다(`:108`). |
| `mgr_purge_disk_usage_pct` | 80 | 디스크 사용률 모드. `0` 이하이면 끈다(`:133`). |
| `mgr_purge_profile_keep_days` | 10. 값이 0이면 deprecated `mgr_purge_keep_days`(기본 10)로 바꾼다. | PROFILE |
| `mgr_purge_xlog_keep_days` | `profile*3`(=30). 값이 0이면 deprecated `mgr_purge_xlog_without_profile_keep_days`(기본 profile*3)로 바꾼다. | XLOG |
| `mgr_purge_counter_keep_days` | `mgr_purge_keep_days*7`(=70). profile_keep_days가 아니라 deprecated 키에서 파생한다. | **ALL(날짜 디렉터리 전체)** |
| `mgr_purge_realtime_counter_keep_days` / `_tag_` / `_visitor_` | counter 값(=70) | REAL_COUNTER / TAG / VISITOR |
| `mgr_purge_daily_text_days` | `max(tag*2, xlog*2)`(=140). 실제 삭제 기준은 `max(text, tag, xlog)`다(`:279-280`). | TEXT |
| `mgr_purge_sum_data_days` | 60. reload 기본값이 현재 필드값이라서 키를 지워도 이전 값이 유지된다(`Configure.java:792`). | SUM |

ALL mode가 counter 보존일(70)에 날짜 디렉터리 전체를 지운다. 따라서 기본 설정에서 실제 보존 기간은 profile 10, xlog 30, sum 60, 나머지 70일이다. text 140일은 실효가 없다. 위 키 중 하나라도 바뀌면 ConfObserver가 값을 다시 읽고 삭제 이력 set을 비운다(`:65-90`).

### 실행·판정

- daemon thread로 기동 직후 1회 실행하고, 이후 `CHECK_INTERVAL`=1분마다 실행한다(`:27,50-58,106-125`). 순서는 disk → profile → xlog → tag → text → sum → visitor → realtime → all이다.
- 오늘 날짜는 `DateUtil.yyyymmdd()`로 구하고, 기준일은 `yyyymmdd(now - N*86400000)`이다. JVM default timezone과 고정 24h `DateTimeHelper`를 쓴다(`C/util/DateUtil.java:35,45,73`). `int(dir) <= 기준일`인 디렉터리를 삭제하므로, N=10이면 오늘을 포함해 10개 날짜를 남긴다.
- 후보 선택(`getLongAgoDate`, `:401-421`)은 `db_dir` 직속 **디렉터리** 중 이름에 `"0000"`이 없고, `Integer.valueOf`로 파싱되며, 삭제 이력 set에 없는 것의 숫자 최솟값이다. `00000000`(영구 text, `kv2`), `lock.dat`, 숫자가 아닌 이름은 제외된다. 후보가 오늘이면 중단하고(`:184` 등), 기준일보다 크면 중단한다.
- 디스크 모드(`:127-176`)는 used%=`100 - usable*100/total`로 계산한다. used > `pct-(100-pct)*0.4`(80→72)이면 PROFILE, > `pct-(100-pct)*0.2`(76)이면 XLOG, > `pct`(80)이면 ALL을 수행한다. 오늘을 제외한 가장 오래된 날짜부터 1개씩 지우고 매번 다시 측정한다.
- 삭제 방식(`:341-399`): 재귀 `File.delete()`이며 **반환값을 무시**한다. 예외는 `S207` 로그만 남긴다. 실패해도 `S206 Auto deletion` 로그를 출력하고 이력 set에 추가하므로 재시작이나 설정 변경 전까지 재시도하지 않는다.

| mode | 삭제 경로(`{d}`=`db_dir/yyyymmdd`) | 남는 것 |
|---|---|---|
| PROFILE | `{d}/xlog/xlog.profile`, `xlog_profile.hfile`, `.kfile` | `xlog.profile.conf`, 압축 profile(공유 `{d}/xlog/data`) |
| XLOG | `{d}/xlog` 전체(압축 data 포함), `{d}/span` | |
| TAG / TEXT / SUM / VISITOR | `{d}/tagcnt`, `{d}/text`, `{d}/sum`, `{d}/visit` | `{d}/visit_hourly`(`S/db/VisitorHourlyDB.scala:114`) |
| REAL_COUNTER | `{d}/counter/real.data`, `.hfile`, `.kfile` | `real.head`(`S/db/counter/RealtimeCounterDBHeader.scala:156`), 일별 counter |
| ALL | `{d}` 전체 | |

## 2. 삭제 중 열린 파일

AutoDeleteScheduler는 어떤 WR/RD/GZip 객체도 close하지 않고, 이들과 lock으로 조율하지도 않는다. 오늘이 아닌 날짜를 열어 둘 수 있는 소유자는 다음과 같다.

- `XLogWR.dailyContainer`: XLog EndTime 날짜별로 연다. 5분마다 30분 idle을 검사해 close하므로 최대 약 35분간 열려 있다(`S/db/XLogWR.scala:35,42-50`). 이 map은 동기화되지 않은 `mutable.Map`인데 watcher와 writer 두 thread가 함께 접근한다.
- Profile/RealtimeCounter/DailyCounter WR: 현재 날짜 하나만 열고 날짜가 바뀔 때 close한다(`S/db/XLogProfileWR.scala:66-70`; `RealtimeCounterWR.scala:41-45`; `DailyCounterWR.scala:47-51`). TextWR은 10초마다 5분 idle을 검사한다(`TextWR.scala:39-50,70-78`).
- `GZipStore.writingBlockTable`(≤3일), `IOChannel` CountBoard(≤2), readCache(1분)(`J/Configure.java:291-299`; `J/db/io/zip/IOChannel.java:62-69`).
- RD는 조회마다 open/close한다. `XLogIndex`와 `XLogDataReader`는 참조 계수 공유 table을 쓴다(`S/db/xlog/XLogIndex.scala:44-58`; `xlog/XLogDataReader.scala:31-44`).

POSIX 결과(추정): unlink는 성공한다. 열린 handle은 unlink된 inode에 계속 쓰고, 그 데이터는 close 시 사라진다. 이후 같은 날짜를 새로 열면 `mkdirs`가 디렉터리를 되살리고(`XLogWR.scala:113-115`; `J/db/io/zip/GZipCtr.java:41-45`), 그 날짜가 이미 이력 set에 있으므로 재시작 전까지 다시 purge하지 않는다. Windows에서는 열린 파일의 delete가 조용히 실패한다(미검증). 이 경합은 보존일이 1일인 day-limit 모드나, 자정 직후의 디스크 모드에서 실제로 생긴다.

## 3. 종료

- 종료 계기는 두 가지다. `<pid>.scouter` 파일이 사라지면 `ShutdownManager.shutdown()` 후 `System.exit(0)`을 호출하고, signal이 오면 shutdown hook이 같은 함수를 호출한다(`J/Main.java:60-64,70-92`). `-console` 모드에는 이 loop와 heartbeat가 없다(`:67-68`).
- Go(구현): 일반 모드는 같은 이름의 종료 파일(`<pid>.scouter`, 실패하면 `$HOME/<pid>.scouter.run`, 둘 다 실패하면 exit 1)을 만들고 1초마다 확인해 사라지면 SIGTERM과 같은 graceful 종료를 한다. 정상 종료 시 파일을 지운다(`deleteOnExit` 대응). `-exit-file=false`로 끌 수 있고 `-console` 모드는 Java처럼 만들지 않는다. SIGINT·SIGTERM·SIGHUP·콘솔 `quit`도 같은 경로다(`cmd/scouter-go/exitfile.go`, `console.go`). Go 종료 순서는 [runtime](runtime.md) "종료와 writer 배타성"을 따른다.
- `ShutdownManager`는 등록 순서대로 1회 실행한다(`S/ShutdownManager.scala:32-44`). 등록된 동작은 `DBCtr.running=false`, `CoreRun.running=false`, `CountEnv.running=false`, KV index/reader/writer close, `GZipStore.shutdown`→`flush`뿐이다.
- WR thread는 `while (DBCtr.running) { queue.get() }` 구조다. `RequestQueue.get()`은 timeout 없이 대기하므로(`C/util/RequestQueue.java:31-39`), 다음 item이 도착해야 loop를 빠져나와 `closeAll()`에 도달한다(`XLogWR.scala:52-83`). JVM은 hook이 끝나면 종료되므로 **queue 잔여분은 버려지고** close는 best-effort다.
- `FlushCtr`는 daemon이며 dirty header를 4초 간격으로 저장한다. `shutdown()`을 호출하는 곳이 없고 종료 시 전체 flush도 없다(`C/io/FlushCtr.java:52-96`). WR close가 실행되지 않으면 최근 ≤4초의 hash/time head 갱신을 잃는다. KV(IndexKeyFile2)는 close 시 flush된다.
- 비압축 XLog/Profile writer는 record마다 `flush()`를 호출하므로 앱 버퍼 손실은 없다(`S/db/xlog/XLogDataWriter.scala:76-81`). fsync는 어디에도 없다.
- 압축 모드: shutdown flush는 `count.dat`을 동기 기록한 뒤 gzip block 저장을 executor에 비동기로 맡긴다(`IOChannel.java:99-104,130-141`). JVM 종료와 경합하므로 block 저장이 빠질 수 있다.

## 4. 재시작·비정상 종료 후 open

| 경우 | 원본 동작 | 근거 |
|---|---|---|
| (a) append 위치 | data는 open 시 **파일 길이**, kfile은 매 append 시 `raf.length()`, gzip은 **`count.dat` 값**에서 이어 쓴다. block 길이는 무시한다. 저장된 counter와 파일 길이를 대조하지 않는다. | `S/db/io/RealDataFile.scala:31`; `RealKeyFile.scala:131-137`; `IOChannel.java:49-61` |
| (b) 오래된 `.hfile` | rebuild하지 않는다. 마지막 flush 이후 append된 kfile record는 head에서 도달할 수 없어 get/getAll이 놓친다. 새 put은 오래된 head에서 chain을 이어 가므로 영구 orphan이 된다. 전체 scan(`read`)에서는 보인다. | `IndexKeyFile.scala:54-64,166-185` |
| (c) 끝부분 partial record | data: 다음 기록을 그 뒤에 이어 쓴다. data flush가 index보다 먼저이므로 index는 대개 이 record를 가리키지 않는다. kfile: 다음 append가 partial 뒤에 붙고, 전체 scan은 어긋나서 `S125` 로그와 함께 중단되거나 잘못 parse한다(추정). | `XLogDataWriter.scala:76-81`; `IndexKeyFile.scala:181-184` |
| (d) header 길이·magic | hfile·kfile·count.dat의 magic을 검사하지 않는다. hfile이 1024 미만이면 **빈 새 header**로 바꾼다. 1024 이상이면 실제 길이로 capacity를 정하므로 원래와 크기가 다르면 bucket이 어긋나 조용히 miss한다. MemTimeBlock은 capacity가 172800으로 고정이라 짧은 파일에서는 큰 slot 접근이 AIOOBE가 된다. `readAll`이 null을 반환하면 NPE가 난다. | `J/db/io/MemHashBlock.java:63-82`; `MemTimeBlock.java:62-84`; `C/util/FileUtil.java:173-184` |
| (e) `count.dat`↔`xlog.N` | count가 block 내용보다 크면 merge 시 0으로 채운다. 그 구간을 가리키는 index는 빈 결과가 된다(NPE를 catch). count가 block보다 작으면 merge에서 **old가 new를 덮어쓴다**. old block을 못 읽으면 START 이후 bytes만 저장하고, 이를 offset 0 block으로 읽으므로 위치가 어긋난다. 8바이트 미만 count는 0으로 보고 block 0을 덮어쓴다. | `J/db/io/zip/Block.java:108-112,130-140`; `GZipStore.java:150-163`; `CountBoard.java:39-48` |
| (f) index가 data EOF 밖 | 비압축은 `readShort` EOF → RuntimeException이다. `getByTxid`는 **null**을 반환하고, 범위 조회는 이미 전달한 record를 남긴 **부분 결과**가 된다(로그는 stack trace만). 끝이 잘린 record는 `read`(readFully 아님) 때문에 0으로 채운 버퍼를 조용히 반환한다. txid 일치 검증이 없다. | `xlog/XLogDataReader.scala:65-83`; `S/db/XLogRD.scala:35-53,95-122` |

## 5. 쓰기 실패·disk full

- `RealDataFile.write`는 offset을 먼저 증가시킨 뒤 쓴다(`RealDataFile.scala:57-64`). 쓰기 예외가 나면 offset과 물리 위치가 어긋날 수 있다(추정). 예외는 WR의 `catch Throwable`에서 stack trace만 남기고, 해당 record는 index되지 않는다(`XLogWR.scala:79-81`).
- `RealKeyFile.write` 예외는 전파되며 partial tail이 남을 수 있다. head 갱신은 append 뒤에 하므로 일어나지 않는다(`RealKeyFile.scala:101-113`).
- header 저장 `FileUtil.save`는 truncate 후 write하고 예외를 삼킨다. 그 뒤 `dirty=false`가 되어 재시도하지 않는다. disk full이면 0바이트 hfile이 남을 수 있고, 재시작 때 (d)의 빈 header가 된다(`MemHashBlock.java:47-50`; `FileUtil.java:146-153`).
- `CountBoard.set`이 실패해도 메모리 count는 이미 증가해 있다(`CountBoard.java:55-64`). block 저장 실패는 `S209` 로그만 남긴다(`IOChannel.java:130-141`).
- WR open이 실패하면 OftenAction이 **queue 전체를 clear**한다(`XLogWR.scala:64-69`; `XLogProfileWR.scala:71-76`; `DailyCounterWR.scala:52-58`). queue가 가득 차면 새 item을 버린다(`RequestQueue.java:89-98`; `xlog_queue_size`=10000). ENOSPC 감지나 back-pressure는 없다.

## 6. `lock.dat`

| 항목 | Java `DBCtr`/`Main` | Go `acquireLock`(`internal/server/runtime.go`) |
|---|---|---|
| 생성 | `mkdirs` → mtime < now-5000이면 path로 delete → `createNewFile`. 성공하면 `deleteOnExit`, 실패하면 main을 반환한다(`S/db/DBCtr.scala:45-62`; `J/Main.java:46-48`). | `.go-writer.lock` flock → 같은 5초 규칙 → `O_EXCL` 생성 |
| heartbeat | 1초마다 `canWrite`이면 `setLastModified`를 호출한다. 소유권(inode)을 검증하지 않고, `-console`에서는 heartbeat가 없다(`DBCtr.scala:39-44`; `Main.java:88-95`). | 1초마다 Chtimes를 호출하고 SameFile이 아니면 오류로 종료한다(`Runtime.startHeartbeat`, `writerLock.touch`). `-console` 모드에서도 유지한다(의도된 차이, D-C2 방지). |
| 삭제 | 정상 JVM 종료 시 deleteOnExit가 지운다. kill -9이면 남고, 5초 뒤 stale로 판정된다. | 같은 inode일 때만 삭제한다(`writerLock.Close`). |

## 7. 원본 결함·취약 동작 후보 (정상 계약과 분리)

1. purge가 열린 handle과 조율하지 않는다. 손실 쓰기, 날짜 부활, 이력 set 때문에 재purge가 누락될 수 있다(§2). 재현: `mgr_purge_xlog_keep_days=1`로 두고 어제 EndTime XLog를 계속 보내면서 1분 주기를 넘긴 뒤, `{어제}/xlog`의 재생성 여부와 내용을 확인한다.
2. 삭제 실패를 성공으로 로그하고 재시도하지 않는다. PROFILE은 압축 모드에서 효과가 없고, `visit_hourly`, `real.head`, `xlog.profile.conf`가 남는다.
3. 정상 종료에서도 queue를 drain하지 않고, header flush를 보장하지 않으며, gzip block을 비동기로 저장한다(§3).
4. 압축 모드에서 block은 10초 idle, 32MiB 가득 참, buffer eviction, 종료 시점에만 저장된다(`GZipStore.java:67-85,100-104`). 그래서 지속 부하 중 crash가 나면 날짜당 최대 32MiB를 잃는다. 4초마다 flush된 index는 그 구간을 가리키고, 재시작 후 같은 logical offset에 새 record를 쓴다. 결과적으로 **이전 txid 조회가 다른 XLog를 반환**할 수 있다(검증 없음, (f)). 재현: `compress_xlog_enabled=true`, 지속 수집 → kill -9 → 재시작 → 새 수집 → 이전 txid 조회.
5. hfile을 비원자적으로 덮어쓴다. crash나 disk full로 1024 미만이 되면 index가 묵시적으로 초기화된다(§4 d, §5).
6. 쓰기 전 offset 증가, short read 묵시 반환, `dailyContainer` 비동기화 map, Block merge 방향(§4 e).
7. lock: Java는 소유권을 검증하지 않고 `-console`에서 heartbeat가 없다. stale 판정과 delete 사이에 TOCTOU가 있어 두 writer가 함께 돌 수 있다.

## 8. Go 설계 함의

**원본과 동일하게 할 것**: 설정 키 이름, 실효 기본값 파생(deprecated 키, `keep_days*7`, text의 `max`), mode별 대상 경로, 숫자 날짜 비교와 `0000` 제외, 오늘 보호, JVM timezone과 같은 Location 및 고정 24h 계산, 1분 주기, 디스크 임계식. 저장 파일 형식, 그리고 원본 DB에 자동 truncate를 하지 않는 것.

**의도적 차이 후보(조사 시점 제안, 채택 결과는 §9)**
- G1: purge를 Store gate 안에서 수행한다. 대상 날짜의 cached index/file/gzip handle을 Sync/Close한 뒤 삭제한다. 삭제 이력은 영구 set으로 두지 않고 매 주기 다시 계산해, 되살아난 날짜도 다시 purge되게 한다.
- G2: 삭제 실패를 계수·보고하고 다음 주기에 재시도한다. 남는 파일(§1 표)을 원본처럼 둘지 정리할지 결정해 문서화한다.
- G3: 종료 시 bounded deadline 안에서 queue를 drain하고 enqueue/완료/잔여 drop을 계수한 뒤 Sync/Close한다. engine의 drain 여부는 미확인이다.
- G4: **주기적 header sync**. 현재 Go `.hfile`은 Sync, Close, 32개 초과 eviction 때만 기록된다(`internal/storage/legacyindex/index.go:195-245,567-580`; `internal/storage/store.go:152-189,308-350`). runtime에서 주기적으로 `Store.Sync`를 호출하는 곳은 찾지 못했다. 그래서 crash가 나면 index를 연 뒤의 head 갱신을 모두 잃을 수 있어 Java(≤4s)보다 나쁘다. ≤4초 주기 Sync 또는 open 시 kfile 순차 scan으로 head를 재구성(bucket별 마지막 record)하는 방안을 제안한다.
- G5: 손상 처리. 현재 Go는 partial kfile tail, magic, 크기, head 경계 오류를 `ErrCorrupt`로 판정해 그 날짜의 읽기·쓰기를 모두 실패시킨다(`index.go:60-145,330-365`). Java는 계속 append한다. 제안은 원본 자동수리 금지를 유지하고, 복사본 대상 오프라인 repair 도구(유효 경계까지 truncate, hfile 재구성, 보고서)를 따로 두는 것이다.
- G6: gzip. Go는 record마다 block과 count를 atomic rename으로 쓴다(`internal/storage/xlog.go:73-95,139-211`). block과 count 사이에서 crash가 나면 `len(block) > count`가 되어 이후 그 날짜 append가 `ErrCorrupt`로 영구 중단된다. Go가 만든 block chain이 유효하면 count를 block 길이로 올리는 보정을 후보로 둔다. Java가 만든 count>block 상태는 거부 또는 read-only로 처리한다. temp `.scouter-*` 잔여 정리도 필요하다.
- G7: 현재 Go는 data EOF 밖, 짧은 read, 0 영역을 오류로 보고한다(`store.go:258-287`; `xlog.go:215-265`). Java의 null·부분·빈 결과와 다른 의도적 차이로 유지하되, Client 응답 매핑(빈 결과 vs 오류)은 service 계약에서 정한다.
- G8: lock은 Go의 더 엄격한 동작을 유지한다. Java `-console`과 소유권 미검증 한계 때문에 "동시 writer 금지"를 운영 절차로 고정한다.

## 9. Go 채택 결정 (Phase 2E)

| 후보 | 결정과 구현 | 근거·검증 |
|---|---|---|
| retention 계약 | 키 이름, `Configure.apply()` 파생, mode별 대상 경로, `int(dir) <= yyyymmdd(now-N)`, 오늘 보호, `0000` 포함 이름 제외, 디스크 임계식, 1분 주기와 기동 직후 1회를 원본대로 따른다(`internal/storage/retention.go`). | `TestJavaRetentionDefaults`, `TestRetentionDayLimitTargetsAndBoundaries`, `TestRetentionDiskUsageThresholds` |
| 의도된 차이: 기본 꺼짐 | Java 기본은 켜짐이지만 Go는 `--purge`를 명시해야 한다. 이관·rollback 검증 기간에 되돌릴 수 없는 삭제를 막기 위해서다. 운영 전환 시 Java 설정과 같은 `--mgr-purge-*`를 지정한다. | `cmd/scouter-go/main.go` |
| 의도된 차이: 후보 이름 | Java `Integer.valueOf`는 `123` 같은 디렉터리도 날짜로 비교해 지운다. Go는 유효한 8자리 날짜만 후보로 삼는다. | 같은 테스트의 `123/keep` |
| G1 열린 handle | 대상이 남아 있는 날짜만 처리한다(이미 지운 날짜는 action 없음). Store gate→Counters gate 순서로 잠근 뒤 대상 날짜의 pending tag/visitor 집계를 **flush하지 않고 버린다**(byte 예산 반환). 다른 날짜의 flush 실패(예: ENOSPC)가 공간 회수를 막지 않게 하기 위해서다. 이어서 대상 경로의 index/data handle, 압축 모드 cache, counter table을 sync·close·제거하고, 대상을 같은 파일시스템의 `.purge-trash`로 rename한다(실패하면 같은 디렉터리의 `.purge-quarantine-…`로 rename). 실제 `RemoveAll`과 doomed handle close는 gate를 푼 뒤 수행하므로 대용량 삭제가 ingest를 막지 않는다. 두 rename이 모두 실패하면 gate 안에서 지우지 않고 대상을 남긴 채 `purge failed`로 보고하며 다음 주기에 재시도한다(Java 제자리 삭제와 의도된 차이, `Retention.DetachStats`). 남은 trash·quarantine은 다음 주기에 재시도한다. 삭제 이력은 두지 않으므로, 늦은 데이터로 되살아난 날짜도 다음 주기에 다시 지운다. | `TestRetentionClosesOpenHandlesAndRepurgesResurrectedDate`, `TestRetentionDiscardsPurgedAggregation`, `TestRetentionTrashCleanupRetried`, `TestPurgeRenameFailureNeverUnlinksUnderGates`, `TestRetentionQuarantineLeftoverRetried`, `TestRetentionConcurrentWithIngestAndQuery`(-race) |
| G2 삭제 실패 | 실패를 `PurgeAction.Err`와 오류로 반환하고 `maintenanceErrors`로 센다. rename 실패 시 원본 데이터는 그대로 남고 다음 주기에 재시도한다. `visit_hourly`, `real.head`, 압축 profile이 남는 것은 원본과 같다. | `TestRetentionFailureReportedAndRetried` |
| G3 종료 잔여 | queue drain은 추가하지 않았다. UDP 종료 시 queue에 남은 datagram을 `ShutdownDiscarded`로 분리했다. 단위별 항등식은 다음과 같다. datagram 단위: `Datagrams = Rejected(크기·포화) + queue 투입`이고, queue 투입은 `ShutdownDiscarded + decode 시도`다. decode 결과는 malformed, expired, 미완성 fragment 보관, Pack N개 중 하나다. Pack 단위: `Packets + HandlerErrors + AbortedPackets`. 단일 Pack datagram만 쓰는 테스트에서는 `Datagrams = Packets + HandlerErrors + Rejected + ShutdownDiscarded`가 성립한다. | `TestUDPQueueAndHandlerCancellation` |
| G4 header 손실 | ① `--sync-interval`(기본 4s) 주기 fsync. ② 쓰기 Open이 header flush 이후 append된 record를 검증한 뒤 relink한다(`legacyindex/recover.go`). 모든 orphan의 prev가 해당 bucket chain을 잇고 data 검사(G4 한계)를 통과해야만 적용한다. 아니면 relink 전체를 거부하고 Java 의미로 열어 `index-relink-refused`로 경로당 1회 보고한다. 거부는 파일에 남긴다. 거부된 살아 있는 orphan의 deleted flag에 **2**를 쓴다(tombstone). Java `DataInputStream.readBoolean`은 0이 아닌 값을 모두 true로 읽으므로 Java의 get/getAll/full-scan 의미는 "삭제된 record"로 같다(원본 oracle `verify-double-crash`로 확인). Go는 flag 2를 tombstone으로 구분해 이후 어떤 open에서도 relink 후보로 삼지 않는다. 그래야 같은 날짜에 data가 다시 쌓여 offset이 재사용된 뒤 재오픈(eviction·재기동)해도 잃어버린 record가 다른 data 위로 되살아나지 않는다. Java가 delete한 record(flag 1)는 chain 구성원으로 계속 취급하며(원본 fixture의 deleted record가 chain 안에 있음), tombstone만 남은 bucket은 saved head를 유지하고, 앞선 orphan이 모두 tombstone인 bucket에서 saved head로 되돌아가는 새 record는 fork가 아니라 chain 재시작으로 받아들인다. 복구된 hash/time/TTL header는 Java가 flush한 header와 바이트가 같다. | `TestRecoverStaleHeader`, `TestOriginalJavaStorageInterop`, `TestTTLActualScalaBidirectionalFiles`, 로컬 replay 측정 |
| G5 손상 | 자동 처리는 두 형태뿐이다. ① 위 relink, ② 어느 head도 참조하지 않고 형식이 타당한 마지막 torn kfile record의 truncate. 그 밖의 magic/size/head/cycle/link/중간 손상은 해당 index를 계속 오류로 둔다. 원본 DB(src)는 이관 도구가 `O_RDONLY`로만 읽고, 수리는 Go writer가 소유한 DB에서만 일어난다. | `TestRecoverTornTail`, `TestRecoverRefusesInconsistentFiles`, `TestRecoverMidFileGarbageIsNotTruncated` |
| G4 한계 | relink 전에 index가 가리키는 data가 디스크에 남아 있는지 확인한다(`legacyindex.OpenChecked`, `internal/storage/relink_check.go`). XLog time/txid/gxid와 Profile index는 `xlog.service`/`xlog.profile`의 길이 prefix와 record 끝이 data 길이 안에 있어야 하고, 압축 모드에서는 논리 offset이 `count.dat` 미만이어야 한다. Object와 영구 Text index도 같은 방식이다. 하나라도 어긋나면(전원 손실로 kfile page만 남은 경우) relink 전체를 거부하고 Java 의미로 열어 `index-relink-refused`로 보고하므로, 조회는 오류 대신 sync된 범위만 돌려준다. 그 밖의 index(status/sum/alert/span/counter/KV)는 kfile 구조만 검사한다. 주기 Sync는 data를 fsync한 뒤에만 그 시점까지의 header snapshot을 쓴다. | `TestRelinkRefusedWhenDataNotDurable`, `TestRefusedRelinkStaysRefusedAfterDataRegrows`(plain/gzip), `TestRefusedRelinkIsTombstoned`, `TestOpenCheckedRelinkCheck`, `TestSyncPointHeaderSnapshotOrdering`, Java `verify-double-crash`. 실제 전원 손실 재현은 없음 |
| G6 gzip count | Go writer는 record마다 GZIP member를 block에 append한 뒤 count.dat를 제자리에 덮어쓴다. record마다 rename·fsync를 하지 않고, 주기 Sync가 두 handle을 fsync한다. 쓰기 경로가 처음 날짜를 열 때 모든 검사를 마친 뒤에만 복구한다. ① count block 마지막 member가 잘렸으면(9MiB 이하, header·deflate·trailer 안에서 끝남) 완전한 member 경계로 truncate한다(`zip-torn-member`). ② count 뒤 tail이 완전한 next=0 record뿐이고 다음 block이 없으면 count를 전진시킨다(`zip-count-advanced`). rollover crash가 남긴 다음 block 1개는 고아로 허용한다. 비어 있거나(truncate 직후 crash) 마지막 member가 잘린 것도 허용하며, 다음 rollover가 truncate 후 다시 쓴다. Java가 만든 count>block, 중간 member 손상, gzip이 아닌 꼬리, linked tail, 다음 block 2개 이상은 `ErrCorrupt`다. | `TestZipCount*`, `TestZipTornMemberTruncatedByWriter`, `TestZipCorruptMembersRejectedUntouched`, `TestZipCountRecoveryEmptyOrTornOrphan` |
| G6 한계 | process crash(SIGKILL)에서는 write 순서(member→count) 때문에 count가 완전 member보다 앞서지 않는다. 큰 member write가 중간에 끊긴 torn member는 위 ①로 복구한다. 전원 손실·OS crash에서는 count.dat page가 block page보다 먼저 기록될 수 있다. 그러면 count>논리 길이가 되어 그 날짜의 압축 append가 `ErrCorrupt`로 멈춘다. offset 재사용으로 다른 record를 반환하지는 않는다. 이 순서 보장(block fsync 뒤 count 기록)은 Store Sync 경로의 별도 작업이다. | 미측정(전원 손실 재현 없음) |
| G7 정상 종료 뒤 재검증 생략 | 이전에는 재기동 후 첫 쓰기가 그날 index를 열면서 **정상 종료 뒤에도** 모든 record를 검증했다(`validateRecover`, O(파일 크기)). 그동안 Store gate와 ingest engine lock을 잡고 있었다. 그래서 하루치가 쌓인 index에서는 기동 직후 수집이 한동안 멈추고 UDP queue reject가 생겼다(실제 Agent 환경에서 관찰). 이제 `Store.Close`는 모든 index·data를 오류 없이 sync·close한 경우에만 DB root에 `.go-clean`을 원자적으로 쓴다(temp `.scouter-*` → fsync → rename → 디렉터리 fsync). marker에는 종료 시각, 형식 version, 그리고 이번에 깨끗이 닫힌 index 쌍마다 `legacyindex.Seal`(kfile·hfile 크기, ns mtime, 메모리 header 전체의 CRC32)을 담는다. `Store.Open`은 다른 작업보다 먼저 marker를 메모리로 읽고 삭제한 뒤 디렉터리를 fsync한다. 삭제에 실패하면 Open이 실패한다. index를 열 때 그 쌍이 Seal과 **정확히** 같고 mtime이 marker 시각 이하이면 record walk와 복구를 생략한다. 대신 magic·count·크기·header CRC·bucket head 범위만 검사한다(`legacyindex.OpenSealed`, `CheckSealed`). 다음 경우에는 지금처럼 전체 검증과 복구를 한다. marker가 없는 경우(SIGKILL·전원 손실·Close 실패, 즉 marker 기록 전 crash), marker 뒤에 Go·Java·도구가 쓴 쌍, 잘리거나 header가 손상된 쌍, Seal에 없는 쌍이다. 처음 결정은 "index mtime ≤ marker 시각"이었지만 Seal 일치로 강화했다. mtime 비교만으로는 crash한 run이 남긴 쌍을 다음 정상 run이 열지 않고 종료하면, 그 쌍의 mtime이 marker보다 오래되어 검증 없이 신뢰되는 구멍이 있다(`TestSealsCarriedOnlyThroughCleanRuns`). 정상 run이 열지 않은 쌍의 Seal은 파일이 여전히 일치할 때만 다음 marker로 넘기며, 최대 1024개다. 적용 범위는 Store index(XLog·profile·text·object·status·sum·alert·span), KV(TTL) index, 그리고 `AttachCounters`로 연결한 Counters의 counter index(`real`·`5m`)다. counter Seal은 Attach 때 Counters로 넘긴다. Counters가 Store보다 먼저 정상 종료하면 그 Seal을 Store marker에 넣는다(runtime 종료 순서). Store index를 고친 뒤에도 실제 Agent 환경에서 기동 직후 ingest 정체가 남았다. 이는 counter realtime index의 전체 검증으로 **추정**한다. 로컬에서 104MB `real.kfile`을 검증하는 데 6.5–6.7s가 걸렸다. 실제 환경의 파일 크기로는 확인하지 않았다. 회귀 테스트는 `TestCleanRestartSkipsCounterAndKVValidation`, `TestCounterAndKVCrashPathsAreFullyValidated`다. `.go-writer.lock` flock과 `lock.dat` 규칙은 바꾸지 않았다. runtime은 `Store.Close`가 반환한 뒤에야 writer lock을 푼다. 한계는 mtime 해상도다(Linux는 coarse clock으로 수 ms). Close 직후 그 해상도 안에서 크기와 header를 유지한 채 kfile을 제자리에서 바꾸면 감지하지 못한다. writer lock 때문에 그 사이 다른 writer는 시작할 수 없다. migrate는 `.go-clean`을 복사·비교·checksum 대상에서 빼고, 조회 비교용 Store가 staging에 남긴 marker를 지운다. | `TestCleanRestartSkipsFullValidation`(수정 전 실패: `Walked:3`), `TestCrashWithoutMarkerIsFullyValidated`, `TestPairChangedAfterMarkerIsFullyValidated`, `TestTruncatedPairWithMarkerIsFullyValidated`, `TestDamagedHeaderWithRestoredMTimeIsFullyValidated`, `TestSealsCarriedOnlyThroughCleanRuns`, `TestMalformedMarkerIsIgnored`, `TestMarkerEntriesAreBounded`, `legacyindex/TestOpenSealed*`. 로컬 측정은 [storage CONTRACT](../../internal/storage/CONTRACT.md) §7 |
| G7 crash 뒤 꼬리만 검증 | marker 없는 재기동(SIGKILL·전원 손실)도 전체 walk를 줄인다. `SyncData`/`Counters.Sync`/eviction close/`Close`가 key fsync → header 쓰기 → header fsync를 모두 마친 쌍마다 `legacyindex.Checkpoint`(key 길이, hash 길이, 4KiB page CRC 합, key 마지막 64byte CRC)를 DB root의 `.go-checkpoint`(Store index·KV)와 `.go-checkpoint-counter`(Counters)에 원자 교체(temp → fsync → rename → 디렉터리 fsync)로 기록한다. 기록 실패는 SyncData/Close 오류다. index를 열 때 우선순위는 Seal → checkpoint → 전체 walk다. checkpoint가 디스크와 맞으면 checkpoint 뒤에 붙은 record만 기존 `validateRecover` 규칙으로 검증·복구한다. 불일치, bucket head가 꼬리에 있음, 꼬리 손상, relink 거절이면 아무것도 쓰기 전에 전체 walk로 다시 연다. 그래서 결과는 언제나 전체 walk와 같다(`FuzzCheckpointTail`). 근거는 key 파일이 붙여 쓰기만 하고 제자리 쓰기가 record 구조를 바꾸지 않는다는 점이다. 마지막 64byte 안의 제자리 쓰기는 전체 walk가 된다(안전하지만 느림). 첫 SyncData 전 crash, log 손상, Java가 쓴 뒤의 쌍은 여전히 전체 walk다. 최대 4096 entry이고 migrate·`scouter-db`는 두 파일을 runtime state로 제외한다. Java는 root의 날짜 디렉터리만 보므로 영향이 없다. 로컬 측정은 [storage CONTRACT](../../internal/storage/CONTRACT.md) §7.1 | `legacyindex/TestCheckpointTailOnlyAfterCrash`, `TestCheckpointMismatchFallsBackToFullWalk`, `TestCheckpointDamagedTailMatchesFullWalk`, `FuzzCheckpointTail`, `TestCrashRestartValidatesOnlyTail`, `TestCrashRestartCounterAndKVTail`, `TestCheckpointLogFallbacks`, `TestEvictionReopenUsesCheckpoint` |
| G7 짧은 read | 유지한다. data EOF 밖, 짧은 read, 0 영역은 오류이며 Java의 null·부분 결과와 다르다. | 기존 storage 테스트 |
| G8 lock | Java 규칙(5초 안에 갱신된 `lock.dat`이면 기동 거부)을 유지한다. 단 2026-10-22부터 Go writer는 `lock.dat`에 `scouter-go writer` 내용을 쓰고, 기동 시 `.go-writer.lock` flock을 잡은 상태에서 그 내용의 fresh `lock.dat`를 만나면 직전 Go writer가 죽은 것이므로 바로 넘겨받는다. 이전에는 SIGKILL 뒤 재기동이 약 5초 거부됐다(replay 실측 5.1~5.2초). Java는 `lock.dat`를 비어 있게 만들고(`createNewFile`) mtime만 보므로, 내용이 없거나 다른 fresh `lock.dat`(살아 있는 Java 서버)는 여전히 거부한다. heartbeat는 `-console` 모드에서도 유지한다(Java 콘솔 모드는 heartbeat가 없어 5초 뒤 다른 서버가 lock을 지울 수 있다, [console](console.md) D-C2). | `TestCrashedGoWriterLockTakenOverAtOnce`(수정 전 FAIL), `TestWriterLockAndFreshJavaLock`, 로컬 replay 측정. 콘솔 heartbeat는 `Runtime.startHeartbeat`가 모드와 무관하게 실행된다는 코드 판독 근거(전용 테스트 없음) |
| retention live 설정(3C) | storage `Retention.Apply(enabled, JavaRetentionConfig)`가 `mgr_purge_enabled`·`mgr_purge_*`를 원자적으로 바꾸고 다음 `RunOnce`부터 쓴다. Java ConfObserver의 삭제 이력 clear는 Go가 이력을 두지 않으므로 할 일이 없고 결과 파일 집합은 같다. 서버 runtime이 reload마다 이 값을 넘긴다(`internal/server/wiring.go`, `--purge`·`--mgr-purge-*`는 flag 고정). scouter.conf 없이 실행하면 기동 값을 유지한다. Go 기본 off는 그대로다 | `TestJavaStorageInteropConfigPurge`(원본 `AutoDeleteScheduler` 4단계 설정 변경), [storage CONTRACT](../../internal/storage/CONTRACT.md) §4 |
| 종료 파일(3C) | 위 §3 "Go(구현)"대로 `<pid>.scouter`를 구현했다 | `TestExitFileDeletionShutsDown`, `TestExitFileRemovedOnSignalAndOptional`, `TestExitFileUncreatableExits1`, `TestCreateExitFileFallback`, `TestWatchExitFile`, `TestShutdownSignals`(`cmd/scouter-go/console_test.go`) |

migrate inspect의 `index.auto_recoverable`/`index.relink_refused` 미리보기는 runtime과 같은 data 검사(`AnalyzeRecoveryChecked`)로 판단한다. inspect는 같은 조건을 `zip.torn_member`(error)와 `zip.auto_recoverable_block`(info)로 표시하고, `--accept-recoverable`은 표시된 block의 `zip.count_mismatch`·`zip.untracked_block`·`zip.torn_member`만 수락한다(`TestInspectTornZipMember*`, `TestInspectEmptyOrTornRolloverOrphan`).

모든 자동 복구는 `storage.Config.OnRecover`와 `Counters.OnRecover`로 보고된다. runtime은 이를 `storage recovery <kind> <path>: <detail>` 로그와 종료 통계의 `recoveries`로 남긴다. 원본 결함 후보(§7)의 Java 실행 재현은 하지 않았다.

## 10. 남은 테스트·fixture 후보

- 설정 파생 표 테스트: 키 미설정, deprecated 키만 설정, 0/음수, reload 시 키 삭제(sum 유지).
- 날짜 선택: `00000000`, `kv2`, `abc`, 미래 날짜, 오늘, N 경계(`<=`)가 섞인 tmp 트리에서 삭제 집합과 mode별 잔여 파일을 정확히 비교한다. 디스크 모드는 사용률 함수를 주입해 72/76/80 임계를 검증한다.
- 열린 handle purge: 날짜 d의 index와 gzip을 연 상태에서 purge한 뒤 쓰기·조회하고, G1에서 정의한 결과를 확인한다.
- crash 행렬(복사본): kfile tail -k bytes, 이전 hfile snapshot 복원, hfile <1024, count.dat ±, block 누락·손상, index offset > EOF. 각 경우 Java 원본 reader(`tools/java-storage` 확장)의 결과(예외, null, 부분)와 Go 결과를 함께 기록한다.
- 종료: 부하 중 SIGTERM을 보낸 뒤 enqueue = 완료 + drop + 잔여가 성립하는지, reopen 후 조회 가능 건수를 확인한다.
- lock 상호운용: Java `createLock`과 Go의 5초 경계, Java console 모드, 강제 종료 후 stale 판정.

## 미확인

tagcnt/visitor/summary/alert/status/KV2 writer의 재시작 append 위치와 종료 flush 세부, Windows 삭제 결과, `deleteOnExit`와 hook의 실제 순서, 위 결함 후보의 실제 재현(모두 미실행). Go UDP 수신은 종료 시 queue를 drain하지 않고 `ShutdownDiscarded`로 센다(`internal/transport/udp.go` Serve). §8의 Go 라인 번호는 조사 시점 기준이다.

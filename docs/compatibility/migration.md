# 기존 DB 검사·이관·rollback 계약

정본은 SHA `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 Java/Scala 파일 DB다. Go는 원본 파일 형식을 그대로 읽고 쓰므로([storage-contract](storage-contract.md)) **이관은 형식 변환이 아니라 검증된 바이트 복사**다. 구현은 `internal/storage/migrate`, CLI는 `cmd/scouter-db`다. 운영 DB로는 검증하지 않았다. 증거는 원본 Java writer로 만든 synthetic DB다([Phase 2E 기록](../validation/2026-10-13-phase2e.md)).

## 원칙

- 원본(src)은 `O_RDONLY`로만 연다. lock 파일, header flush, mtime 변경을 포함해 어떤 쓰기도 하지 않는다. 테스트는 매 검사 뒤 SHA-256·크기·mtime·mode 불변과 새 파일 미생성을 확인한다.
- 원본 DB에는 자동 수리를 하지 않는다. 손상·불일치는 `error`/`warning`/`info` code로 보고하고 빈 성공으로 바꾸지 않는다. Go가 내용을 검사하지 못하는 범주는 `unchecked`/`structure-only`로 따로 표시한다.
- Java와 Go를 같은 DB에 동시에 쓰지 않는다. migrate(`--finish` 포함)와 rollback은 `lock.dat` mtime이 5초 이내(Java stale 규칙)이거나 `.go-writer.lock`에 Go writer의 flock이 있으면 거부한다. 예외는 `migrate --precopy`다. src를 읽기만 하고 dst를 만들지 않으므로 fresh `lock.dat`(Java 실행 중)를 허용하고 Go writer만 거부한다.
- 복사 결과를 확정하는 migrate와 `--finish`는 `lock.dat`가 **없거나 60초 넘게 갱신되지 않았을 때만** 진행한다. 그 밖에는 `--java-stopped`로 Java 프로세스 종료(PID 소멸)를 확인했다고 명시해야 한다. Java 종료 경로는 1초 heartbeat loop를 끝낸 뒤 `ShutdownManager.shutdown()`으로 GZipStore block flush, index·KV close를 동기 수행한다(`scouter.server/src/main/java/scouter/server/Main.java:88-95`). 정상 종료면 `lock.dat`는 JVM 종료 때 지워진다(`DBCtr.scala:56` `deleteOnExit`). 따라서 5초 stale은 "아직 쓰는 중"일 수 있다. kill -9 같은 비정상 종료면 `lock.dat`가 남는다.
- 같은 dst의 migrate·`--precopy`·`--finish`·rollback은 형제 파일 `<dst>.migrate.lock`의 `flock(LOCK_EX|LOCK_NB)`을 작업 전 구간 잡는다. 동시에 실행하면 나중 것이 거부된다. 그래서 반복 precopy가 finish의 staging을 덮어쓰거나 두 finish가 manifest를 덮어쓰는 일이 없다. lock 파일은 지우지 않는다(지우면 대기 중인 locker와 경합한다). `--dry-run`은 lock 파일을 만들지 않는다.
- `<dst>`, `<dst>.partial`, 상태 파일(`.partial.json`, `.precopy.json`, `.manifest.json`, `.migrate.lock`)이 symlink면 거부한다. symlink면 staging이나 상태 쓰기가 src 같은 다른 위치로 향할 수 있다. `--report`는 src·dst·`<dst>.partial` 안에 둘 수 없다.

## 명령

```sh
scouter-db inspect  --db <DB 복사본> --timezone Asia/Seoul --report inspect.json
scouter-db migrate  --src <Java DB> --dst <새 Go DB> --dry-run
scouter-db migrate  --src <Java DB> --dst <새 Go DB> --report migrate.json [--fresh]
scouter-db migrate  --precopy --src <Java DB> --dst <새 Go DB> [--dry-run] [--fresh]   # Java 실행 중, 반복 가능
scouter-db migrate  --finish  --src <Java DB> --dst <새 Go DB> --report migrate.json [--dry-run] [--accept-recoverable]
scouter-db verify   --src <Java DB> --dst <Go DB> [--manifest <dst>.manifest.json]
scouter-db rollback --dst <Go DB> --report rollback.json
```

`--timezone`은 원본 JVM default timezone과 같아야 한다. 다르면 time index bucket과 날짜 검사가 `index.head`/`time.date_mismatch` 오류로 드러난다. exit code는 0 성공, 1 문제 발견·거부·실패, 2 사용법 오류다. `--report` 경로가 DB 안이면 거부한다.

## inspect

DB root를 walk해 날짜 디렉터리, `00000000` 영구 디렉터리, 기타를 분류한다. 파일마다 크기·SHA-256·범주·record 수·상태를 기록하고, 문제는 `(code, path)`별로 상세 10개와 나머지 수를 남긴다.

| 범주 | 검사 |
|---|---|
| `.kfile`/`.hfile`, TTL `.k2file` | magic, header·count, record 경계, chain cycle, head가 record 경계인지, Java count 규칙, head에서 도달 불가능한 record(`index.unreachable_records`, header flush 전 비정상 종료 형태) |
| time index | key의 Java 날짜(고정 24h, 지정 timezone)가 디렉터리 날짜와 같은지 |
| XLog/Profile/Object/Text/Status/Alert 등 data | index offset이 EOF 이내인지, Pack decode, objHash·key 일치, 잘린 tail과 미색인 tail(`data.truncated_tail`, `data.unindexed_tail`) |
| 압축 `xlog/data` | `count.dat`과 `xlog.N` 블록 일치, gzip 해제, record 경계, linked cycle |
| KV `kv2` | live/expired/deleted 통계(`--now` 기준) |
| counter data, tagcnt, visitor, stack/batch | SHA-256만 기록(`unchecked`), counter index는 구조 검사 |

profile 쪽 XLog가 없으면 `profile.xlog_missing` warning이다. Profile은 수신 시각으로 저장되므로 원본상 정상일 수 있다.

## migrate

1. src와 dst의 동일·중첩·symlink 별칭을 거부한다. dst는 없거나 빈 디렉터리여야 한다. live writer를 확인하고, src inspect에 error가 있으면 거부한다.
2. 파일 시스템 여유 공간을 확인한다(복사량×1.01+16MiB). `--dry-run`은 여기서 끝나며 아무것도 쓰지 않는다.
3. `<dst>.partial`에 파일별 복사+fsync를 한다. 복사 중 stream SHA와 재읽기 SHA를 src inspect SHA와 비교한다. `lock.dat`, `.go-writer.lock`, retention 잔여 `.purge-trash/**`, Go atomic write 임시 파일(`.scouter-*`, `.counter-head-*`, `.batch-archive-*`)은 복사하지 않고 manifest `excluded`에 기록한다. 이들은 inspect에서 index/data로 분류하지 않는다. 대신 각각 `purge.trash_leftover`, `temp_leftover` warning이 된다(lock은 `lock.*`).
4. staging inspect 결과를 src와 비교한다. 그다음 staging을 Go `storage.Open`으로 열어 같은 조회 결과를 src 읽기 전용 경로와 비교한다. 전체 비교는 Objects, XLogs V1, Status, Alert이고, 표본(기본 1000) 비교는 XLog txid, Profile chunk, 일별·영구 Text, KV다. summary/span/counter/tag/visitor/stack/batch 조회는 `query.not_compared`로 표시한다. 조회 전후 stat으로 staging에 쓰기가 없었음을 확인한다.
5. src 전체를 재hash하고 lock을 재확인한다. manifest(`verified`)를 기록하고 원자적 rename으로 `<dst>`를 확정한 뒤 부모 디렉터리를 fsync하고 manifest를 `completed`로 바꾼다.

### 자동 복구 가능한 원본 오류 (`--accept-recoverable`)

원본에 다음 오류만 있으면 기본 migrate는 거부하고 `--accept-recoverable`을 안내한다.

- torn index tail: `index.truncated`와 같은 경로의 `index.auto_recoverable`이 함께 있는 경우. 읽기 전용 open은 torn tail에서 실패한다. 그래서 inspect는 dry run의 절단 위치 이전 완전한 record를 별도로 순회해, 일반 index와 같은 key·날짜·data offset·decode 검사를 수행한다. 여기서 나온 `data.*` 등의 오류는 수락 대상이 아니다.
- gzip count 뒤처짐: `zip.count_mismatch`/`zip.untracked_block`이 난 block이 `zip.auto_recoverable_block`으로 표시된 경우. N=`count/32MiB`, start=`count%32MiB`일 때 조건은 다음과 같다.
  - block N이 있으면 길이가 start 이상이고, start 이후가 완전한 next=0 record뿐이다.
  - 고아 block은 N+1 하나만 허용한다. N+2 이상이 있으면 N+1 유무와 무관하게 거부한다.
  - N+1은 첫 record가 rollover 필요조건 `start+8+len(first) > 32MiB`를 만족하고, 전체가 완전한 next=0 record여야 한다.

이 옵션을 주면 원본 바이트를 **그대로** 복사하고 manifest의 `accepted_problems`에 기록한다. staging inspect 비교는 같은 오류를 그대로 기대한다. 복구 대상 index를 쓰는 조회 비교는 `query.not_compared`로 건너뛴다. Go storage가 쓰기 모드로 열면 그 index를 복구해 복사본이 바뀌기 때문이다.

dst는 Go writer가 처음 열 때 복구된다. 잘린 tail은 마지막 완전한 record 경계로 truncate하고, count.dat은 block 끝으로 전진한다. 이 변경은 `verify`에서 `post_migration.changes`로 보이고, 수락된 오류는 `src.inspect_errors`/`dst.inspect_errors`로 세지 않는다. 그 밖의 error가 하나라도 있으면 옵션과 무관하게 거부한다.

```sh
scouter-db migrate --src <Java DB> --dst <새 Go DB> --dry-run --accept-recoverable
scouter-db migrate --src <Java DB> --dst <새 Go DB> --accept-recoverable --report migrate.json
# Go 서버를 dst로 한 번 기동·정지한 뒤
scouter-db verify  --src <Java DB> --dst <새 Go DB>
```

stale header 때문에 head에서 도달할 수 없는 record(`index.unreachable_records`)는 warning이며 원래부터 migrate를 막지 않는다. 각 bucket의 새 record가 저장된 head에 정확히 이어지면 `index.auto_recoverable`이고, Go writer가 relink한다. 이중 crash처럼 fork가 있으면 `index.relink_refused` warning이다. 이때 Go는 relink하지 않고 원본 Java 의미대로 연다(도달 불가 유지, 파일 불변). 쓰기 Open 자체가 실패할 상태는 `index.not_recoverable` warning을 덧붙인다.

실패하면 `<dst>`는 생기지 않는다. `<dst>.partial.json`에 `failed`와 원인을 남긴다. 재실행하면 검증된 staging 파일을 재hash해 재사용한다. src 크기나 SHA가 바뀌었으면 처음부터 다시 복사한다. `--fresh`는 staging을 버린다.

## 증분 이관 (`--precopy`/`--finish`)

전체 migrate는 Java를 멈춘 뒤 src 전체를 세 번 이상 읽고 모든 record를 decode한다. 그동안 Agent 데이터가 사라지므로 손실 구간이 전체 이관 시간과 같다. 증분 이관은 Java가 도는 동안 복사·검사를 미리 해 두고, Java를 멈춘 뒤에는 **바뀐 파일만** 복사·검사한다. 원칙은 그대로다. src에는 쓰지 않고, Java와 Go가 같은 DB에 동시에 쓰지 않으며, 검증 없이 성공을 보고하지 않고, 실패하면 dst를 만들지 않으며, 다시 실행할 수 있다.

```sh
scouter-db migrate --precopy --src <Java DB> --dst <새 Go DB> --report precopy.json   # 여러 번 반복 가능
# 반복 precopy 작업(cron 등) 중지 → Java 정지, 프로세스 종료(PID 소멸) 확인
scouter-db migrate --finish  --src <Java DB> --dst <새 Go DB> --report migrate.json [--java-stopped] [--rehash-staging]
```

### Java writer 조사와 사전 복사 범위 결정

원본(`S`=`scouter.server/src/main/scala/scouter/server`)에서 Java가 언제 어느 파일에 쓰는지 확인했다.

| writer | 쓰는 날짜·파일 | 근거 |
|---|---|---|
| XLog, Zipkin span | pack **EndTime/time의 날짜**별 container. 5분마다 30분 idle만 close한다. 늦게 도착한 XLog는 며칠 전 날짜도 다시 연다 | `S/db/XLogWR.scala:34-61`, `S/db/ZipkinSpanWR.scala:34-62` |
| Profile, Status, Summary, Alert, realtime counter | 직전 pack과 날짜가 다르면 그 pack 시각의 날짜를 연다. agent 시각이 섞이면 과거 날짜도 쓴다 | `S/db/XLogProfileWR.scala:66-70`, `StatusWR.scala:39-42`, `SummaryWR.scala:42-45`, `AlertWR.scala:47-50`, `RealtimeCounterWR.scala:43-48` |
| daily counter, object | daily counter는 `counterData.date`, object는 서버 현재 날짜. 둘 다 고정 slot을 **제자리 갱신**한다(크기 불변) | `S/db/DailyCounterWR.scala:47-51`, `counter/DailyCounterData.scala:95-149`, `S/db/ObjectWR.scala:57-62`, `obj/ObjectData.scala:80-91` |
| text | daily text는 data 날짜별 table(10초마다 5분 idle close). 영구 div는 `00000000/text`에 계속 append한다 | `S/db/TextWR.scala:39-50,78-100` |
| visitor, tag count | 현재 날짜/pack 시각 날짜. visitor는 파일 전체를 덮어쓴다 | `S/db/VisitorDB.scala:45-48`, `S/tagcnt/first/FirstTagCountDB.scala:138-145`, `S/tagcnt/next/NextTagCountDB.scala:104,153` |
| index header, gzip block, KV | `.hfile` header는 4초마다 같은 크기로 덮어쓴다. gzip block은 10초 idle·32MiB·종료 때 저장한다. KV(`00000000/kv2`)는 수시로 갱신한다 | [storage-contract](storage-contract.md), [recovery-retention](recovery-retention.md) §4 |

결정: **날짜나 이름으로 사전 복사에서 빼거나 "변하지 않음"으로 둘 수 있는 파일은 없다.** 늦은 XLog·과거 시각 pack이 어느 날짜든 다시 쓸 수 있고, header·object·counter·visitor는 크기를 유지한 채 바뀐다. 그래서 precopy는 기존 제외 대상(lock, Go runtime state, `.purge-trash`, 임시 파일)만 빼고 모든 파일을 복사한다. 모든 파일을 "변할 수 있음"으로 보고, finish가 stat으로 변경을 가려낸다. 크기만으로는 부족하고 mtime·ctime이 필요한 이유가 위의 제자리 갱신이다.

mtime을 보존하며 쓰는 경로도 찾았다. Java DB 코드에서 `setLastModified`는 `DBCtr.updateLock`의 `lock.dat`뿐이다(`S/db/DBCtr.scala:39-44`). `lock.dat`는 제외 대상이다. `scouter.server`·`scouter.common` main source에는 `MappedByteBuffer`/`FileChannel.map`이 없다. 따라서 모든 DB 쓰기는 `RandomAccessFile`/`FileOutputStream`의 write이고, 커널이 mtime과 ctime을 갱신한다.

### 사전 복사 (`--precopy`)

- lock: `lock.dat`가 fresh(Java 실행 중)여도 허용한다. `.go-writer.lock` flock(Go writer)이 잡혀 있으면 거부한다. 결과의 `java_active`가 관찰한 상태다.
- 대상: dst는 없거나 비어 있어야 하고, manifest가 없어야 한다. 전체 migrate가 남긴 `<dst>.partial.json`이 있으면 거부한다(`--fresh`로 버린다).
- 복사: src를 walk해 파일마다 `O_RDONLY|O_NOFOLLOW`로 연다. 읽기 전 fstat(크기·ns mtime·ctime·inode·mode)을 잡고, stream SHA-256으로 `<dst>.partial`에 복사하고 fsync한 뒤 다시 fstat한다. staging mtime을 src mtime으로 맞추고 copy를 재읽기해 SHA를 확인한다. 기록은 읽기 전 stat과 stream SHA다.
- **racy**: 복사 중 stat이 바뀌었거나, mtime이 복사 시작 60초(`--racy-window`, 기본 60s) 전보다 늦은 파일은 `racy`로 표시한다. 이유는 두 가지다. 같은 timestamp tick 안의 다음 쓰기는 stat을 바꾸지 않을 수 있다(Linux coarse clock, 일부 FS의 1초 해상도). 또 커널은 write 경로에서 데이터를 복사하기 **전에** mtime을 갱신한다(ext4/xfs의 `file_update_time`). 그래서 크기가 같은 제자리 쓰기가 page fault·writeback 대기로 멈춘 사이에 읽으면, 새 mtime과 옛 바이트를 함께 볼 수 있다. 처음에는 2초였지만 이런 지연에 여유를 두려고 60초로 늘렸다. git의 racy-clean 규칙과 같은 생각이다. racy 파일은 다음 round와 finish가 stat과 무관하게 다시 복사한다. Java가 계속 쓰는 오늘·영구 파일이 여기에 해당한다.
- 상태: `<dst>.precopy.json`에 파일별 기록(위 stat, SHA, staging mtime, racy)과 group별 inspect·조회 비교 결과를 저장한다. dst는 만들지 않는다. 상태는 통째로 다시 쓰므로 복사 중에는 10초 또는 1GiB마다 저장한다. 파일 수에 비례해 저장하면 파일이 많을 때 O(n²)가 된다. 저장되지 않은 기록은 crash 뒤 그 파일을 다시 복사하게 할 뿐이다.
- 진행 중 Java retention이 날짜를 지우면, walk 도중이든 walk와 복사 사이든(ENOENT) round를 실패시키지 않고 제거로 반영한다. finish에서는 src가 멈춰 있어야 하므로 오류다.
- 부하: precopy는 운영 중인 Java와 디스크를 나눠 쓴다. Linux에서는 `nice -n 10 ionice -c3 scouter-db migrate --precopy ...`, macOS에서는 `taskpolicy -b`로 우선순위를 낮춰 실행한다.
- 반복: 다음 round는 stat이 기록과 같고 racy가 아니며 staging 파일의 크기·mtime이 기록과 같은 파일을 읽지 않고 재사용한다. 새 파일, 바뀐 파일, racy 파일만 복사하고, 사라진 파일은 staging에서 지운다. 실행할수록 차이는 "그 사이 Java가 쓴 파일"로 줄어든다.
- 검사 단위(group): inspect의 모든 검사와 조회 비교는 한 group 안의 파일만 읽는다. 날짜 디렉터리는 `<date>/<첫 하위 디렉터리>`(xlog는 gzip block·profile·conf와 함께), 그 밖은 확장자를 뺀 경로다(`00000000/text/text_<div>`, `00000000/kv2/kv_<space>`의 `.kfile`/`.hfile`/`.data`가 한 group). precopy는 racy 파일이 없는 group만 staging에서 inspect하고, 원본 오류가 없거나 자동 복구 대상뿐인 group은 Go storage 조회 비교까지 해 결과를 캐시한다. 복사 중인 group의 결과는 다음 finish에서 다시 계산한다.

### 마무리 (`--finish`)

1. Java 정지 확인: `lock.dat`가 5초 안에 갱신됐거나 Go writer flock이 있으면 거부한다. `lock.dat`가 남아 있고 60초가 안 됐으면 `--java-stopped` 없이는 거부한다(원칙 참조). precopy 상태가 없거나, `--timezone`/`--sample`/`--query-limit`/`--now`가 precopy와 다르면 거부한다. KV TTL 판정 시각(`--now`)은 첫 precopy 시각으로 고정한다.
2. src를 walk해 기록과 비교한다. 크기·ns mtime·ctime·inode·mode가 모두 같고 racy가 아니며 staging stat도 같으면 **src를 읽지 않고** 재사용한다. `--rehash-staging`을 주면 재사용할 staging copy도 기록 SHA로 재hash한다(staging만 읽는다). 다르면 다시 복사한다. 다르거나 새 파일, racy 파일은 다시 복사한다. 복사 중 stat이 바뀌면 writer가 살아 있는 것으로 보고 실패한다. 사라진 파일은 staging에서 지운다. 바이트가 바뀌기 전에 그 group의 캐시를 지우고 상태를 저장한다.
3. 결과가 없는 group(바뀐 group과 precopy가 미룬 group)만 staging에서 inspect한다. 바뀌지 않은 group은 precopy 결과를 재사용해 전체 보고서로 합친다. 합친 보고서는 src 전체 Inspect와 파일 목록·summary·problem(code·path·offset·반복 수)이 같다(`TestGroupInspectionEqualsFullInspect`). 판정은 전체 migrate와 같다. error가 있으면 거부하고, 자동 복구 대상뿐이면 `--accept-recoverable`일 때만 진행한다.
4. 조회 비교 결과가 없는 group만 비교한다. 독립 reader와 Go `storage.Open`이 모두 staging copy를 읽는다. 비교 전후 staging stat은 같아야 한다.
5. src 재확인: 전체를 다시 stat해 기록과 비교하고, 이번 finish가 복사한 파일만 재hash한다. 그다음 lock을 재확인한다. manifest(`verified`) → 원자적 rename → `completed` 순서는 전체 migrate와 같다. 끝나면 precopy 상태를 지운다.

manifest는 전체 migrate와 같은 형식이다. 파일 목록(크기·SHA·mode·mtime)·제외 목록·디렉터리·inspect summary·조회 비교 수가 같은 src의 전체 migrate와 일치한다(`TestPrecopyFinishMatchesFullMigrate`). 여기에 `incremental`(precopy round 수, finish의 복사·재사용·group·src 읽은 byte·단계별 ms)을 더한다. 그래서 `verify`/`rollback`은 바뀌지 않는다. 둘 다 manifest 기준으로 src 전체를 재hash한다.

### 최종 보증 비교

| 항목 | 전체 migrate | precopy + finish |
|---|---|---|
| src = staging | 복사 stream SHA = inspect SHA, staging 재읽기 SHA | 파일마다 마지막 복사의 stream SHA = staging 재읽기 SHA. 그 뒤 src 불변은 stat 5종 일치와 racy 제외로 판단한다 |
| inspect | src 전체 + staging 전체를 비교 | 바뀐 group은 staging, 나머지는 precopy 때 같은 바이트로 한 결과다. src inspect와 staging inspect의 비교는 같은 바이트에 대한 결정적 함수이므로 생략한다 |
| 조회 비교 | 독립 reader는 src, Go Store는 staging | 둘 다 staging(바이트가 src와 같음). Go Store와 독립 parser의 해석 차이를 보는 목적은 같다 |
| 마지막 src 확인 | 전체 재hash + lock | 전체 stat + finish가 복사한 파일 재hash + lock |

차이는 "바뀌지 않은 파일은 재hash 대신 stat으로 판단한다"는 한 가지다. 근거와 위험은 다음과 같다.

- 근거: Java는 mtime을 보존하는 쓰기를 하지 않는다(위 조사). `cp -p`·`rsync -t`·`tar` 같은 도구가 mtime을 되돌려도 ctime은 사용자 공간에서 설정할 수 없고 inode도 바뀌므로 감지한다(`incremental_test.go`의 `cp -p`+rename 식 inode 교체 시나리오). mtime 해상도 안의 연속 쓰기는 racy 규칙으로 막는다.
- 운영 전제: src와 staging이 darwin 또는 linux의 로컬 파일시스템에 있어야 한다. 파일시스템은 ns 또는 1초 이하 mtime과 ctime·inode를 제공해야 하고, 시스템 시계를 precopy와 finish 사이에 되돌리지 않아야 한다. 그 밖의 플랫폼에서는 `--precopy`/`--finish`를 거부한다(ctime·inode 없이는 크기·mtime·mode만 남는다). 원격 FS(NFS 등)는 서버 시계와 차이가 racy window보다 작을 때만 전제가 성립하며, 검증하지 않았다.
- 남는 위험: 위 전제를 깬 경우, 그리고 racy window보다 길게 멈춘 제자리 쓰기다.
- 확인 범위: `scouter-db verify`는 Go 기동 뒤 손실 구간 밖에서 **src 쪽**을 재hash로 증명한다(src = manifest). dst에는 Go가 이미 쓰고 있으므로, 재사용한 staging copy가 manifest와 같았다는 점까지는 증명하지 못한다. staging copy는 precopy 때 재읽기로 확인했고, finish 때는 크기·mtime만 확인한다. 이 부분까지 바이트로 확인하려면 finish에 `--rehash-staging`을 준다. 비용은 staging 전체 읽기다(src는 읽지 않는다).
- 바뀐 파일은 통째로 다시 복사한다. append된 꼬리만 복사하지 않는다. `.kfile`은 delete flag를 제자리 갱신하고, `.hfile`·gzip block·visitor·object·counter도 덮어쓰기 때문이다. 접두부가 같다는 증명에도 결국 전체 읽기가 필요하다. 따라서 finish 비용은 "그 사이 바뀐 파일 크기"에 비례한다. 보통 오늘 날짜 group과 `00000000`의 text·KV가 여기에 해당하고, 바뀐 group은 decode 검사도 다시 한다.

### 실패·중단·재실행

- finish 전에 반복 precopy 작업을 멈춘다. 멈추지 않아도 `<dst>.migrate.lock` 때문에 동시에 돌지는 않는다. 하지만 finish가 lock을 먼저 잡지 못하면 거부되므로, 손실 구간에 재시도가 필요해진다.
- precopy/finish가 실패하면 dst는 생기지 않는다. `<dst>.partial`과 `<dst>.precopy.json`(`status: failed`, `error`)이 남는다. 다시 `--precopy`나 `--finish`를 실행하면 기록과 stat을 다시 비교해 이어서 진행한다. 중간에 끊긴 복사는 기록이 바뀌기 전이라 다음 실행에서 다시 복사한다. staging 파일이 기록과 다르면(크기·mtime) 그 파일만 다시 복사한다.
- group 결과는 그 group의 바이트를 바꾸기 전에 지우고 저장한다. 그래서 어느 시점에 중단돼도 남은 캐시는 현재 staging 바이트를 설명한다.
- `--precopy --fresh`는 staging과 두 상태 파일을 지우고 처음부터 시작한다. 전체 migrate는 precopy 상태가 있으면 거부한다(`--fresh`로 버린다). `--finish --fresh`는 사용법 오류다.
- `--dry-run`은 둘 다 아무것도 쓰지 않는다. precopy는 복사량과 여유 공간을, finish는 바뀐 파일 수·byte를 보고한다.
- finish가 판정에서 거부되면(원본 오류) staging과 상태는 남는다. Java를 다시 기동해 운영을 되돌리고, 다음 precopy부터 다시 진행할 수 있다. rollback(전환 뒤 Java 복귀)은 전체 migrate와 같다.

증거: `internal/storage/migrate/incremental_test.go`(plain·gzip, Java 실행 중 precopy, round 사이 append·header·새 날짜·삭제·같은 크기 내용 변경·mtime 복원 쓰기, lock 조건, 3개 지점 중단 후 재실행, staging 변조, 원본 오류 판정, finish src 읽기 byte = 바뀐 파일 크기×2, finish가 src를 inspect하지 않음, 동시 실행 거부, 60초 미만 `lock.dat`와 `--java-stopped`·`lock.dat` 부재, 복사 도중 쓰기(precopy racy·finish 실패), walk 도중과 walk·복사 사이 ENOENT, 복사 도중 precopy 중단 후 재실행, `cp -p`+rename 식 inode 교체, finish 직전 쓰기, 재사용 group에 symlink 등장, `--rehash-staging`, symlink staging 거부). macOS(APFS)와 Linux arm64 VM(Lima, 로컬 root FS)에서 `go test -race -short`로 통과했다, `cmd/scouter-db` `TestCommandIncremental`, 그리고 `TestJavaOracleMigration`이다. 마지막 테스트는 원본 Java writer로 만든 DB 5종(base plain·gzip, extra, ttl, local)에서 precopy + finish가 전체 migrate와 같은 manifest·바이트를 만들고 finish가 아무것도 다시 읽지 않음을 확인한다. 이 경우 src는 변경하지 않았다. 측정은 [증분 이관 측정](../validation/2026-10-23-incremental-migration.md). 실제로 실행 중인 Java 서버 옆에서의 precopy는 검증하지 않았다. 변경 시나리오의 DB는 Go storage writer로 만들었고, Java 실행 중 상태는 fresh `lock.dat`와 round 사이 Go writer 쓰기로 흉내 냈다.

## verify와 rollback

`verify`는 manifest 기준으로 src 불변, dst checksum, 양쪽 inspect를 비교한다. 이관 뒤 Go가 dst에 기록한 변경은 오류가 아니라 `post_migration.changes`로 분류한다.

rollback에는 두 가지 의미가 있다.

- **원본 복귀(`scouter-db rollback`)**: Java가 src로 돌아가는 경로다. dst와 src에 live writer가 없음을 확인하고, src가 manifest와 전체 일치하는지 재hash로 증명한다. 그다음 `<dst>`를 `<dst>.rolledback-<UTC>`로 rename한다. 삭제하지 않는다. 이관 뒤 Go가 수집한 데이터는 src에 없다. 이 손실 구간은 전환 계획에서 별도로 다룬다.
- **Go가 쓴 DB를 Java로 계속 사용**: 먼저 Go를 정지하고 `scouter-db inspect`를 실행한다. `zip.torn_member`, `index.unreachable_records`, `index.relink_refused`가 하나라도 있으면 Java를 기동하지 않는다. Go 쓰기 경로의 복구는 그 날짜에 다시 쓸 때만 일어나므로, Go를 한 번 더 기동해 복구를 끝내거나(압축 block의 torn member는 Java reader가 block 전체를 null로 읽고 Java writer가 덮어쓴다) 해당 날짜를 제외한다. Go writer가 원본 형식을 쓰므로 Java reader가 Go DB를 읽을 수 있음을 양방향 oracle로 검증했다(`--verify`, `--verify-extra`, `--verify-ttl`). 운영에서는 Go를 정지하고 dst 복사본을 inspect한 뒤 Java를 기동한다.

## 남은 한계

- 운영 DB 복사본, 대용량의 시간·메모리는 검증하지 않았다. 전체 migrate는 src를 세 번 이상 전부 읽고 모든 index record의 data를 decode한다. 증분 이관은 Java 정지 뒤 이 비용을 바뀐 파일로 줄인다. synthetic 측정만 있다.
- lock 확인부터 rename까지의 경합은 직전 재확인으로 창만 줄인다. Java `lock.dat` 자체가 엄격한 lock이 아니다([recovery-retention](recovery-retention.md) §6).
- manifest `verified` 기록 뒤 rename 전에 crash하면, 남은 manifest 때문에 재실행이 거부되므로 수동 정리가 필요하다. rename 뒤 `completed` 기록 전에 crash하면 verify/rollback이 거부된다.
- Unix 전용(`flock`, `statfs`)이다. Windows는 검증하지 않았다.
- 2026-10-23부터 inspect는 `.purge-trash`·`.purge-quarantine-*` 안의 tag count 파일을 chain 검사하지 않는다. 이전에는 retention 잔여물에도 `tagcount.*` warning을 냈다.

# Java 인덱스 저장 계약

정본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`.
[RealKeyFile](https://github.com/scouter-project/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/io/RealKeyFile.scala),
[IndexKeyFile](https://github.com/scouter-project/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/io/IndexKeyFile.scala),
[IndexTimeFile](https://github.com/scouter-project/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/io/IndexTimeFile.scala),
[MemHashBlock](https://github.com/scouter-project/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/java/scouter/server/db/io/MemHashBlock.java),
[MemTimeBlock](https://github.com/scouter-project/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/java/scouter/server/db/io/MemTimeBlock.java),
[HashUtil](https://github.com/scouter-project/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.common/src/main/java/scouter/util/HashUtil.java).
형식/알고리즘을 Go로 재구현했으며 원본 Apache-2.0 저작권과 출처를 코드에 유지한다.

- `.kfile`: `CA FE`, 각 레코드는 deleted boolean 1byte + signed Long5 prev + unsigned short key 길이/bytes + Blob value. Blob prefix는 0..253 직접 길이, `FF`+uint16, `FE`+int32. Prev/head는 0 sentinel 또는 2 이상 signed 40bit 파일 offset이다. append chain은 이전 offset이 현재보다 작아야 한다.
- `.hfile`: `CA FE`, offset4 int32 count, 1024byte reserved header, signed Long5 bucket array. 기존 hash capacity는 파일 길이에서 결정하고 5byte 미만의 trailing padding은 보존한다. hash는 `HashUtil.hash(byte[])`와 같은 CRC32 IEEE, bucket은 `(hash & 0x7fffffff) % capacity`다.
- 시간 인덱스는 172800 bucket(864000byte), timezone의 2000-01-01 midnight 기준 고정 24시간 modulo/500ms를 사용한다. DST에 따라 매일 midnight를 다시 계산하지 않는다. 양수이지만 base 이전 timestamp는 bucket0이다.
- hash count는 점유 bucket 수다. Java 시간 count는 새 bucket 첫 입력 시 1 추가 + 매 입력 시 1 추가하므로 append-only에서 **record 수 + 점유 bucket 수**다. 삭제된 Java 파일의 저장 count는 재계산하지 않는다.
- Get/GetAll은 exact key의 최신 append부터, Records/Scan은 삭제 제외 물리 append 순서다. absent Get은 nil, 존재하는 empty Blob은 non-nil empty slice다. Record/slice는 호출자가 소유한다.
- `KeyChain(key)`는 hash chain 하나의 snapshot이다. 읽기 lock 안에서 bucket head와 kfile 길이를 읽고 kfile을 read-only로 따로 연다(같은 파일인지 `os.SameFile`로 확인). `ScanContext`는 lock 없이 그 handle로 `ScanKeyContext`와 같은 레코드·순서·chain 검사·오류를 낸다. `LookupContext`는 lock 없이 `Index.LookupContext`와 같은 값(가장 최근의 살아 있는 레코드)·오류를 내고, miss면(빈 chain은 0) snapshot 때 가져간 index의 `DeepSearchFunc`을 `DeepGet`과 walk 길이(삭제 레코드 포함)로 부른다. 이 호출은 index lock 밖이다. `Index.LookupWithin(ctx, key, limit, deadline)`은 index 읽기 lock 안에서 chain을 최대 limit 레코드(삭제 포함, 음수는 0)까지, deadline이 0이 아니면 그 시각 전까지(16레코드마다 확인)만 걷는 `LookupContext`다. 그 안에서 끝나면 값·오류·`DeepGet` 보고가 `LookupContext`와 같고, 더 길면 아무것도 보고하지 않고 `done=false`로 끝난다(호출자는 KeyChain으로 다시 걷는다). 짧은 chain을 KeyChain handle을 열지 않고 상한 있는 시간에 끝내는 용도다. append(Put/PutBatch)는 snapshot 길이 뒤에 쓰고 그 앞만 link하므로 snapshot chain은 바뀌지 않고, handle이 inode를 유지하므로 Close·rename·삭제와도 무관하다. Update·Delete는 제자리 쓰기라 walk가 나중에 닿은 레코드에서 새 값·flag가 보인다. `SetAppendOnly`로 표시한 index는 Update·Delete를 `ErrAppendOnly`로 거절하므로 snapshot이 정확하다(open 시 복구는 영향 없음. Store는 xlog_profile·xlog_gid·span_gid·xlog_tid를 이렇게 연다). time index는 지원하지 않는다. 한 goroutine이 소유하고 `Close`가 handle을 닫는다. `WalkBucketFrom`은 방문한 레코드 r 자체부터 다시 걸을 때 `at = r.NextPosition`, `resume = r.Position`을 받는다.
- ScanRange는 Java V1 bucket별 timestamp, **signed byte value** 순서 및 동일 `(time,value)` 중복 제거를 유지한다. ScanBucket(false)는 V2 최신 append부터 중복을 그대로 전달한다. Callback error는 즉시 전파한다. callback은 읽기 lock에서 호출하므로 같은 Index에 재진입하지 않는다.
- Range는 inclusive endpoint를 포함하는 모든 bucket을 방문한다. Java의 500ms 증가 loop에서 비정렬 endpoint 마지막 bucket을 누락하는 동작은 재현하지 않는다. 범위 최대 24시간이며 같은 bucket에 다른 날짜의 데이터가 섞여 있으면 Java처럼 bucket 내부 timestamp 정렬을 적용한다. 상위 저장 계층이 날짜 파일을 분리한다.
- Update는 최신 exact key의 동일 Blob 길이만 변경한다. Java update의 bucket collision key 확인 누락과 짧은 Blob overwrite로 후속 sequential scan이 깨지는 동작은 재현하지 않는다. 이 방식은 Object의 기존 5byte data offset 갱신에 충분하다.

## 자원과 수명

단일 path의 한 Index owner만 허용한다. positional IO와 RWMutex로 동일 owner의 경쟁을 보호하며 process 간 writer lock은 제공하지 않는다. 키 최대65535byte, value16MiB, hash 영역128MiB, 편의 API/정렬 bucket은 최대100000record/64MiB다. `Scan`, `ScanKey`, `ScanBucket(false)`는 record 하나씩 전달하므로 상위 계층이 자신의 조회 상한에서 중단할 수 있다. Open은 전체 레코드 구조와 head 경계를 검사하여 truncated tail을 거절한다(쓰기 Open은 증명된 crash 형태만 복구한다. `recover.go`). 예외로 `OpenSealed`는 쌍이 `CloseSealed`가 돌려준 `Seal`(두 파일 크기·ns mtime·header CRC32)과 정확히 같으면 record walk 대신 magic·count·크기·header CRC·bucket head 범위만 검사한다(`CheckSealed`). 하나라도 다르면 일반 Open과 같은 전체 검증을 한다. `OpenCheck()`가 어느 경로였는지 알려 준다. Seal의 저장과 신뢰 범위는 상위 Store의 정상 종료 marker가 정한다([recovery-retention](../../../docs/compatibility/recovery-retention.md) §9 G7). `OpenCheckpointed`/`OpenTTLCheckpointed`는 Seal이 맞지 않을 때 `Checkpoint`(fsync된 key 길이, hfile 크기, header page sum, key 길이 앞 64byte CRC)가 디스크와 맞으면 그 뒤 꼬리만 검증·복구한다(`CheckTail`). 꼬리만으로 전체 walk와 같은 판단을 할 수 없는 경우(head가 꼬리에 있음, corruption, relink 거절)에는 아무것도 쓰기 전에 전체 walk로 다시 연다. `OpenInfo()`가 경로·walk한 record/byte 수·거절 이유를 알려 준다. Checkpoint는 writable handle의 Sync·SyncPoint.Commit·Close가 key fsync → header 쓰기 → header fsync 뒤에 만들고 `Checkpoint()`로 돌려준다. 저장 위치와 신뢰 범위는 Store의 crash checkpoint log가 정한다([storage CONTRACT](../CONTRACT.md) §7.1). chain의 음수/전방/자기 링크는 오류다.

Sync는 `.kfile` fsync 후 `.hfile` 기록/fsync 순서다. header는 마지막 쓰기 이후 바뀐 4KiB page만 쓴다(`BeginSync`도 그 page만 복사). 디스크 결과는 전체 쓰기와 byte 단위로 같다. 중첩되거나 Sync/Close에 밀린 SyncPoint는 header를 쓰지 않고 자기 page를 다시 dirty로 표시한다. `BeginSync`는 아직 Commit되지 않은 point가 잡은 page도 포함한다([storage CONTRACT](../CONTRACT.md) §7.2). Close는 Sync 후 두 파일을 닫고 idempotent다. 닫힌 handle의 다른 호출은 ErrClosed다. 오류를 삼키지 않는다. 두 파일을 원자적으로 commit하지 않으므로 process crash/power loss와 header 일부 쓰기 이후의 자동 복구를 보장하지 않는다. 새 파일의 directory fsync도 수행하지 않는다.

## 검증

`go test ./internal/storage/legacyindex`, `go test -race ./internal/storage/legacyindex`는 경계/충돌/Blob 폭/long key/count/timezone/삭제/순서/소유권/중단/경쟁을 검증한다. `FuzzIndexFiles`는 작은 malformed 파일의 panic/hang 회귀를 검사한다.

`TestOriginalJavaStorageInterop`는 source SHA/clean을 확인하고 실제 common codec와 Scala 인덱스를 임시 경로에 컴파일한다. Java 생성 파일을 Go로 읽고 Go 파일 네 개의 bytes를 Java 파일과 대조한 뒤 **원본 Java/Scala reader**로 다시 조회한다. synthetic 데이터만 사용한다. Logger/Configure만 로깅/경고 설정을 격리하는 stub이며 DB/codec/hash/date 클래스는 수정하지 않는다. 원본 deploy compiler는 Scala2.10.4로 JDK18 compiler mirror 초기화가 실패하므로 macOS에 설치된 JDK8로 compiler만 실행한다. javac는 JDK11+ `--release 8`을 사용한다. 다른 OS에서는 `SCOUTER_SCALA_JAVA=/path/to/jdk8/bin/java`로 지정한다. 도구/원본이 없으면 이 통합 테스트는 skip이며 호환 검증 성공으로 취급하지 않는다.

재사용 compiler launcher: `python3 internal/storage/legacyindex/testdata/run_oracle.py --classes-dir <임시 디렉터리>`. source/classes 출력은 원본 디렉터리를 수정하지 않는다. 이 검증은 기존 운영 DB 전체, 중간 crash 복구, 지속 TPS를 입증하지 않는다.

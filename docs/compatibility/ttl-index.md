# TTL index 파일 계약

원본 SHA는 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. `internal/storage/legacyindex/ttl.go`는 IndexKeyFile2/RealKeyFile2/MemHashBlock의 TTL index를 구현한다. 원본 DB는 읽기 전용이며 테스트는 합성 데이터와 임시 디렉터리를 사용한다. `.kfile` 일반 index와 `.k2file` TTL index는 다른 형식이다.

## 파일 형식과 기준 source

| 항목 | 형식 | 정본 |
| --- | --- | --- |
| key 파일 | `<path>.k2file`, 시작 `CA FE`, 첫 record position 2 | `../scouter/scouter.server/src/main/scala/scouter/server/db/io/RealKeyFile2.scala:40`, `:231` |
| record | deleted Boolean(1), expire signed long5(5), previous position signed long5(5), key ushort length(2)+bytes, value Blob | 같은 파일 `:46`, `:158` |
| head 파일 | `<path>.hfile`, `CA FE`, 1024-byte reserved header, offset 4의 big-endian int32 count, 이후 5-byte head 배열 | `../scouter/scouter.server/src/main/java/scouter/server/db/io/MemHashBlock.java:31`, `:63` |
| bucket | `(CRC32(key)&0x7fffffff) % floor(hashBytes/5)`; 끝의 나머지 padding 유지 | 같은 파일 `:79`, `:84`; `../scouter/scouter.common/src/main/java/scouter/util/HashUtil.java:56` |
| count | bucket의 기존 head가 0일 때만 증가; 삭제·만료·duplicate로 감소하지 않음 | `MemHashBlock.java:104` |
| Blob | 0..253 길이는 1바이트, 254..65535는 FF+ushort, 그 이상은 FE+signed int32 길이 | `../scouter/scouter.common/src/main/java/scouter/io/DataOutputX.java:88` |

Go는 기존 index의 `readLong5`, `writeLong5`, `blobPrefix`, CRC bucket, 파일 Sync/Close, payload read primitive를 재사용한다. TTL record의 expire 필드 때문에 기존 Index.meta를 사용하지 않고 별도 metadata reader를 둔다. 반환 value는 소유 복사본이며 caller가 보관하거나 수정할 수 있다. nil은 missing, 길이 0의 non-nil slice는 저장된 빈 Blob이다.

OpenTTL의 `hashSizeBytes`는 원본 Scala 생성자의 MB 단위를 wrapper에서 변환한 **바이트 크기**다. 기존 파일을 열면 `.hfile` 실제 길이가 capacity를 결정한다. count/header padding과 물리 previous link를 보존한다. 잘못된 magic·count·head 위치·record 경계·길이·cycle은 명시적 오류다. 원본 MemHashBlock의 작은 파일 초기화 동작과 달리 불완전한 기존 파일 쌍을 새 파일로 재설정하지 않는다.

## 시간과 조회·삭제

expire/now는 밀리초가 아닌 epoch **초**이다. 만료 비교는 `expire < now`이며, expire와 now가 같은 초에는 살아 있다(`RealKeyFile2.scala:69`, `:73`). 원본 `DataInputX.LONG5_MAX_VALUE`는 `(Integer.MAX_VALUE * 256L)`인 `549755813632`이다. true signed 40-bit 최대 `549755813887`과 구분한다(`../scouter/scouter.common/src/main/java/scouter/io/DataInputX.java:39`). Go의 `TTLNoExpiry`는 이 원본 상수이며 API의 negative expires를 해당 값으로 저장한다. 명시적 nonnegative expires는 true signed 40-bit 최대 이내에서 저장한다.

GetContext는 bucket head부터 역순으로 key를 비교한다. 첫 matching record가 deleted 또는 expired이면 nil이며, 이전 matching record로 fallback하지 않는다(`../scouter/scouter.server/src/main/scala/scouter/server/db/io/IndexKeyFile2.scala:133`). DeleteContext는 첫 matching record에만 deleted flag를 쓰고, 이미 deleted이면 false이다. expired이지만 deleted가 아니면 삭제할 수 있다(`:207`). SetTTLContext도 첫 matching record만 대상으로 하며 deleted/expired이면 false이다(`:97`). 과거 record의 TTL 또는 flag를 바꾸지 않는다.

## 갱신과 물리 append

PutContext는 expire와 previous bucket head를 포함한 새 record를 append하고 head를 새 position으로 바꾼다(`IndexKeyFile2.scala:39`). UpdateOrPutContext는 원본 `:63`의 현재 코드처럼 bucket의 matching record를 모두 갱신한 후 **항상 새 record를 append**한다. deleted/expired 과거 matching도 갱신 과정에서 flag=false와 새 expire로 다시 기록된다. 이것을 최신 key의 단순 덮어쓰기로 축약하지 않는다.

원본 updateOrPut의 “update 때 TTL 보존” 주석(`:55`)과 달리 RealKeyFile2.update는 전달된 ttl로 expire를 다시 계산한다(`RealKeyFile2.scala:177`, `:194`). Go는 주석보다 현재 코드의 갱신 의미를 따른다. 호출 wrapper가 relative TTL을 absolute expire로 변환한다.

정상 KeyValueStore의 value는 별도 `.data` 파일 위치를 나타내는 고정 5바이트이다. 같은 길이에서는 기존 record의 previous link·position을 유지하며 원본과 같은 물리 bytes를 쓴다. 원본은 작은 value로 줄일 때 남은 bytes를 truncate하지 않아 다음 물리 record 경계를 깨뜨릴 수 있다. Go는 Blob 크기나 비정규 prefix 폭이 다르면 기존 record를 줄여 쓰지 않고 새 record를 append한다. 커지는 경우 원본도 즉시 append한다. 이 차이는 정상 고정 길이 KV offset 경로와 분리한 의도된 오류 회피다.

SetTTL/삭제/in-place rewrite는 root head를 바꾸지 않는다. append는 기존 head를 physical link로 기록하며 count는 occupied bucket 기준으로 갱신한다. expiry/deletion은 공간을 회수하지 않는다. 이 구현은 compaction 기능을 추가하지 않는다.

## API, 소유권, 취소와 제한

```go
OpenTTL(path string, hashSizeBytes int) (*TTLIndex, error)
OpenTTLContext(ctx context.Context, path string, hashSizeBytes int) (*TTLIndex, error)
GetContext(ctx context.Context, key []byte, nowUnixSeconds int64) ([]byte, error)
PutContext(ctx context.Context, key, value []byte, expiresUnixSeconds int64) (int64, error)
UpdateOrPutContext(ctx context.Context, key, value []byte, expiresUnixSeconds int64) (int64, error)
DeleteContext(ctx context.Context, key []byte) (bool, error)
SetTTLContext(ctx context.Context, key []byte, nowUnixSeconds, expiresUnixSeconds int64) (bool, error)
Sync() error
Close() error
```

각 path의 writer owner는 한 프로세스의 한 TTLIndex여야 한다. cross-process lock은 없다. 내부 RWMutex로 동시 Get/Put/TTL/Delete/Sync/Close를 보호한다. context 취소는 lock 대기와 collision chain의 nonmatching record까지 확인한다. OpenTTLContext는 기존 파일 검증 중에도 취소를 확인한다. 파일 I/O 한 번 자체를 context로 중단하지는 않는다.

key는 65,535바이트, value는 16MiB, hash bytes는 128MiB, key 파일/position은 signed 40-bit 범위로 제한한다. 단일 chain 탐색은 100,000 record까지이며 초과하면 ErrLimit이다. 원본의 traversal warning만 출력하고 계속 탐색하는 동작과의 명시적 차이다. 상한 안에서는 원본처럼 chain 길이가 `log_index_traversal_warning_count`(기본 100)를 넘으면 S152(조회 miss)·S161(쓰기·TTL miss) 경고를 storage `Log` sink로 낸다([storage CONTRACT](../../internal/storage/CONTRACT.md) §3). nil key/value, 불완전·손상 파일, 종료, 취소, 파일 쓰기 오류를 성공으로 바꾸지 않는다.

append 쓰기 실패는 이전 길이로 truncate를 시도하고 rollback 실패 시 index fault를 유지한다. in-place rewrite/TTL 쓰기도 이전 bytes 복원을 시도한다. UpdateOrPut가 일부 matching record를 갱신한 뒤 취소·오류를 반환할 수 있으므로 작업 전체를 원자적 변경으로 주장하지 않는다. 후속 retry는 새 record를 더 만들 수 있다. Sync는 key 파일을 먼저 fsync한 후 dirty hash header를 저장·fsync한다. Close는 Sync와 두 파일 close 오류를 반환하며 반복 Close는 성공이다. 이 순서는 두 파일의 crash atomicity를 보장하지 않는다.

Open은 전체 record/header를 검증하고 hash block을 메모리로 읽는다. 매 KV 조회마다 Open/Close하면 데이터 크기에 비례하는 비용이 있으므로 Store owner가 bounded index cache와 Sync/Close lifecycle을 관리한다. 미사용 인덱스를 무제한으로 열어 두지 않는다. 공유 호스트의 실제 비용은 별도 측정 대상이다.

## 검증 범위

```sh
.tools/go/bin/go test ./internal/storage/legacyindex -run '^TestTTL' -count=1
.tools/go/bin/go test -race ./internal/storage/legacyindex -run '^TestTTL' -count=1
.tools/go/bin/go vet ./internal/storage/legacyindex
```

범위 테스트는 physical position/previous link/root count, expiry 동일초·이후초, newest expired/deleted의 fallback 금지, 빈 Blob/missing 구분, duplicate in-place update+append, resize 안전 처리, Blob prefix 경계 0/253/254/65535/65536, reopen/owned 결과, key/value/time 상한, 손상 header/chain/length, 취소된 Open·lock 대기·collision scan, 동시 접근을 검증한다. 기존 `FuzzTTLIndexFiles` seed도 유지한다.

`TestTTLActualScalaBidirectionalFiles`는 `scripts/java-storage.py --generate-ttl <빈DIR>`와 `--verify-ttl <GoDB복사본>`을 실제 호출한다. `tools/java-storage/TTLOracle.java`는 고정 원본 RealKeyFile2/IndexKeyFile2/MemHashBlock을 compile·실행하며 Go 구현을 oracle로 대신 사용하지 않는다. 도구의 `ttl-metadata.json`에서 source SHA, hash 크기 1MiB, expiry sentinel, 충돌 key, 물리 position을 확인한다.

- 원본 생성 DB를 Go로 읽어 hash collision, 최신 lookup, expired/deleted, empty Blob, 32비트 초과 expiry와 동일초 경계를 검증한다.
- Go가 원본과 동일한 put/delete/updateOrPut/absolute-expire 시나리오를 작성한 뒤 `.k2file`과 `.hfile` 전체 바이트를 비교한다. matching 3개 record 갱신 후 append와 header padding까지 비교 대상이다.
- Go가 작성한 DB의 추가 복사본을 원본 verifier로 열어 get/getAll, physical record walk, flag/sentinel/EOF/reopen을 검증한다. 원본 getAll은 만료된 nondeleted record도 포함한다. Go GetContext의 최신 만료 lookup과 혼동하지 않는다.
- 별도 원본 생성 DB 복사본에 Go SetTTL/Delete/UpdateOrPut를 수행한다. SetTTL은 최신 expire 5바이트만, Delete는 최신 flag 1바이트만 바뀌는지 비교하고, 갱신은 과거 matching을 다시 기록한 뒤 새 record를 append하는지 검증한다. 생성 DB 원본은 변경되지 않았음을 파일 바이트로 재확인한다.

실제 oracle 초기 실행은 PASS(19.39초)이며 skip 없이 generate와 verify를 실행했다. 최종 TTL 전체 범위 `-race`는 PASS(15.628초, actual oracle 자체 13.96초), `go vet ./internal/storage/legacyindex`도 PASS했다. 원본 oracle compiler가 SHA와 관련 source의 dirty 상태를 검증하는 것도 확인했다. 이 증거는 합성 DB의 양방향 파일 호환 검증이다. 기존 실제 DB 복사본의 양방향 조회·복원·rollback, 비정규·손상 파일의 원본 동작 비교, crash recovery와 sustained KV 부하는 별도 gate이다.

# Go 참고 구현 재평가: Java 대비 효율 주장·메모리 기법·기능 대조

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

2026-10-14 읽기 전용 조사다. [2026-10-13 평가](reference-evaluation.md)의 "채택 5/보류 5"와 성능 근거 한계를 반복하지 않고, README의 Java 대비 효율 주장 신빙성, 메모리 최적화 기법의 도입 가능성, 기능 대조를 확장한다. 참고 코드를 복사하지 않았다.

> 이후 변경: 아래 기능 대조의 "우리" 쪽 상태는 2026-10-14 당시 값이다. 그 뒤 관리·변경 명령 45개, 계정 XML·LDAP, Pulse 내장(3·3B), GeoIP(3C)를 구현했다. 현재 상태는 [implementation-plan.md](implementation-plan.md)를 따른다. 본문은 당시 평가로 보존한다.

| 항목 | 값 |
|---|---|
| 참고 구현 | [`scouter-project/scouter-server-go`](https://github.com/scouter-project/scouter-server-go). 고정 commit `8a869592f51ee1f8d5e98a61dc3f1486e00b78b0`. GitHub API 기준 `main` HEAD도 같은 SHA(2026-04-16)라 README는 고정본과 동일하다(diff 없음) |
| 우리 정본 | Scouter 2.21.3 `dee47fb5`, branch `codex/phase2e-storage-ops`, 이 저장소의 Pulse 내장 작업 커밋 `56870e1` |
| 표기 | 참고 구현 경로는 `R:`, 우리 경로는 `G:` 접두어. 라인은 1-based |
| 조사 방법 | raw.githubusercontent에서 README·docs·benchmark·관련 소스를 받아 읽음. 참고 구현의 benchmark·테스트는 **실행하지 않음** |

## 1. README "Java 대비 효율 비교" 주장별 신빙성

README `R:README.md:5-15` 표의 7행과 `R:README.md:27`의 "~70% 절감"을 하나씩 본다. Java 열의 측정 방법(버전은 "Scouter 2.21.2", JVM 옵션, 부하, 호스트)은 README 어디에도 없다.

| # | 주장 (`R:README.md`) | 측정 방법 공개 여부 | Java 동일 조건 측정 | 우리 판단 | 근거 |
|---|---|---|---|---|---|
| 1 | 런타임 메모리 Java 400~800MB vs Go 50~150MB, "70~80% 절감" (:9) | 없음. Go 값은 `GOMEMLIMIT=1GiB` 조건만 표기. 부하·object 수·보존 일수·측정 시점 없음 | 없음. "JVM 힙 + Metaspace"는 `-Xmx` 설정값에 좌우되며 실측 조건 미기재 | **비교 대상 부재**. 범위 하한/상한을 조합하면 62~88%라 "70~80%"는 어림값. Features의 "~70%"(:27)와도 불일치 | `R:README.md:9,27`, `R:scripts/start.sh:22`(`GOMEMLIMIT` 기본 1GiB) |
| 2 | 배포 크기 ~34MB JAR+JRE vs 7.4MB 단일 바이너리 (:10) | 바이너리 크기는 빌드로 재현 가능 | JAR 합계는 원본 배포물에서 확인 가능 | **근거 있음(정성적)**. 다만 운영 성능·메모리와 무관한 포장 크기다 | `R:README.md:10` |
| 3 | 시작 시간 "수 초" vs ~50ms (:11) | 측정 스크립트·조건 없음 | Java 값이 "수 초"라는 서술뿐 | **검증 불가**. Go 프로세스 기동이 빠른 것은 일반적 사실이나 50ms가 listener·DB open·cache warm-up 중 어디까지인지 미정의 | `R:README.md:11` |
| 4 | GC 정지 G1 "수십~수백 ms" vs sub-ms STW (:12) | 없음 | 없음. Java 수치는 G1 일반론이며 Scouter 서버 측정이 아님 | **비교 대상 부재**. Go GC의 STW가 짧은 것은 runtime 특성이지만 이 서버의 pause 분포 측정이 없다. 긴 tail은 GC가 아니라 fsync·lock stall에서도 생긴다 | `R:README.md:12` |
| 5 | XLog 수집 처리량 ~27ns/op 단일, ~11ns/op 병렬, 0~1 alloc/op (:13) | benchmark 코드 공개 | Java 열 "-" | **microbenchmark 한정**. `BenchmarkXLogWR_Add`는 256B 고정 bytes를 10,000 용량 channel에 `Add`하는 시간이다. 저장·index·flush를 기다리지 않고, 큐가 차면 `default:` 분기에서 **무통보 drop**된 호출도 같은 ns/op에 포함된다. `AddAndWait`는 `StopTimer` 뒤 `Close`라 drain 시간이 제외된다 | `R:internal/db/xlog/xlog_bench_test.go:262-286,288-311`, `R:internal/db/xlog/xlog_wr.go:23,45,92-97` |
| 6 | 디스크 쓰기 768MB/s (1KB 레코드, 0 alloc/op), "buffered + pwrite" (:14) | benchmark 코드 공개 | Java 열 "-" | **microbenchmark 한정**. `RealDataFile.Write`는 8KiB `bufio.Writer` append이고 `Flush`는 `bufio` flush다. fsync·index·네트워크·decode가 없다. pwrite는 읽기 경로(`ReadAt`)이지 쓰기 경로가 아니다 | `R:internal/db/io/io_bench_test.go:83-107`, `R:internal/db/io/real_data_file.go:16,35,87-90`, `R:docs/xlog-pipeline.md:199-201` |
| 7 | 인메모리 해시 조회 14ns/op (:15) | benchmark 코드 공개 | Java 열 "-" | **microbenchmark 한정**. 미리 채운 100K key의 `MemHashBlock.Get`이며 디스크 chain 탐색이 없다 | `R:internal/db/io/io_bench_test.go:42-58` |

판단 기준을 정리하면 다음과 같다.

- **동일 입력**: Java 열은 측정 입력이 없다. Go 열의 5~7은 synthetic 고정 bytes다.
- **동일 내구성**: 참고 구현의 benchmark 어디에도 `fsync`가 없다(`R:real_data_file.go` 전체에 `Sync` 호출 없음). 원본 Java도 header를 fsync 없이 저장하므로 이 자체가 차이는 아니지만, "내구성 동일 조건 비교"는 성립하지 않는다.
- **동일 기능 범위**: 참고 구현은 StackAnalyzer/Batch/TagCount service handler가 없고 command 직접 등록이 126개다(§3). 기능이 적은 서버의 메모리를 기능이 많은 Java 서버와 비교한 것이다.
- **동일 호스트**: README에 호스트·OS·Go 버전·측정일이 없다. 미확인.
- **ns/op → TPS 환산 오류**: README 표는 환산하지 않는다. 그러나 "수집 처리량" 행 제목에 ns/op를 쓴 것은 독자가 1/27ns ≈ 3,700만 건/s로 읽게 만들 소지가 있다. 이 값은 enqueue 호출 시간이지 완료 XLog/s가 아니다.

### 우리 실측과 직접 비교하지 않는 이유

| 구분 | README 수치 | 우리 측정 ([2E](validation/2026-10-13-phase2e.md), [개선](validation/2026-10-14-phase2e-improvements.md)) |
|---|---|---|
| 지표 정의 | 단일 함수 ns/op (enqueue, bufio write, map lookup) | **재시작 뒤 TCP 조회로 재확인된 고유 XLog 수 / (첫 예정 송신~서버 종료, drain 포함)** |
| 경로 | 메모리 내 함수 호출 | UDP 수신→재조립→decode→dictionary→저장→3중 index→주기 fsync→조회 병행→SIGTERM drain→재시작 재조회 |
| 손실 처리 | 큐 포화 drop이 ns/op에 포함 | reject·누락은 별도 집계, 누락 1건이면 FAIL |
| 환경 | 미기재 | M1 Pro 8코어 loopback, `GOMAXPROCS=2`, Go 1.27.2, JDK 18/8, 압축 on/off, `--sync-interval` 4s/0 |
| 결과 성격 | Go 열만 있는 비교표 | 2000 tx/s·200 obj 46000/46000, soak 99.5%, RSS 0.8~0.9GiB. 목표 처리량·목표 호스트·Java 동일 조건 비교 **미검증** |

두 수치는 같은 축에 놓을 수 없다. 참고 구현의 ns/op를 우리 TPS로 나누거나 곱해 개선율을 만들지 않는다.

### README 각 행에 대응하는 우리 쪽 지표와 재현 가능 여부

| README 행 | 우리 쪽 동등 지표 | 현재 상태 | 공정 비교에 필요한 조건 ([performance.md](performance.md) §비교 실험) |
|---|---|---|---|
| 런타임 메모리 | replay report의 max RSS 시계열(200 obj: 767~899MiB, soak 600s 증가 추세 없음) | Go만 측정. Java 원본 같은 corpus 측정 없음 | 같은 corpus·object 수·보존 일수·조회 병행, Java는 `-Xmx`·GC 옵션 명시, 둘 다 RSS(heap 아님)로 기록 |
| 배포 크기 | `dist/scouter-go` 등 3개 binary SHA256 기록, 크기는 미기록 | 재현 가능 | `ls -l dist/` 추가면 충분. 성능 지표로 취급하지 않음 |
| 시작 시간 | replay의 kill→ready(SIGKILL 뒤 재기동 5.1s, `lock.dat` 5초 규칙 포함) | 정상 기동 시간은 미기록 | "ready"를 listener bind·DB open·ready-file 생성 중 어디로 정의할지 고정 후 Java·Go 동일 정의 측정 |
| GC 정지 | 없음. history p95/p99(143/243ms @2000 tx/s)는 GC가 아니라 gate·fsync 지배 | `--profile-dir`로 pprof는 가능, GC trace 없음 | `GODEBUG=gctrace=1`·Java GC log를 같은 부하에서 수집, p99 지연과 분리해 보고 |
| XLog 수집 처리량 | 완료 XLog/s(drain 포함)와 reject·누락 | 2000 tx/s·200 obj PASS, 상한 미탐색 | ns/op와 호환 불가. Java는 같은 replay 도구로 같은 정의의 TPS 측정 필요 |
| 디스크 쓰기 | 없음(record 단위 write + 주기 fsync) | 측정 안 함 | 비교 가치 낮음. fsync 정책별 손실 창·stall이 실제 관심사 |
| 인메모리 해시 조회 | `legacyindex` bucket 계산은 같은 CRC32 % 규칙(`G:internal/storage/legacyindex/index.go:241`) | 단독 benchmark 없음 | 비교 가치 낮음. 조회 지연은 chain 탐색·disk read가 지배 |

## 2. 메모리 최적화 기법 도입 가능성

README `R:README.md:17-23`의 5개 기법과 docs·코드에서 확인한 기법을 열거한다. 우리 지침은 원본 파일 형식 유지, 묵시적 drop 금지, 상한·소유권 명시, 호환성 > 성능이다. "mmap"은 참고 구현 소스·docs에서 발견하지 못했다(미사용으로 판단).

| 기법 | 참고 구현 (`R:`) | 우리 현재 (`G:`) | 판정 | 예상 효과 / 위험 |
|---|---|---|---|---|
| Zero-allocation 직렬화 (`DataOutputX` buffer 모드, `Reset` 재사용) | `internal/protocol/dataoutx.go:16-44,40`. "escape analysis 검증 완료"의 산출물은 README에 없음(미확인) | `make bench` 153B XLog: Decode 450.9ns/10 allocs, Encode 739.5ns/11 allocs. owned copy 사용, pool 미적용(`docs/validation/2026-10-13-foundation.md:36-44`) | **도입 가능** | Pack encode/decode의 allocs/op 감소. 바이트 출력은 불변이므로 Java fixture 141개로 회귀 검증 가능. 위험: scratch 재사용 시 async 소비자(Pulse `Offer*`는 clone, 저장 큐)에 빌린 slice가 새면 race. `internal/protocol`은 순수 codec이므로 pool 소유권을 caller에 두어야 한다 |
| Zero-copy 역직렬화 (`ReadBlobRef`가 입력 buffer를 alias) | `internal/protocol/datainx.go:250-268`. 주석이 "buffer 수명 밖 보존 금지"를 caller에 위임 | 모든 Blob을 copy(owned) | **조건부** | decode 할당 감소. 위험: UDP 수신 buffer 재사용과 결합하면 realtime ring·Pulse·storage에 넘어가는 slice가 덮어써진다. 도입 시 "ref는 handler 반환 전까지만 유효" 규칙과 `-race` 재현 테스트가 선행 조건. 저장 record와 ring entry는 계속 copy |
| `sync.Pool` 버퍼 재사용 (zstd decode, XLogPack 직렬화) | `internal/db/compress/zstd.go:9-23,79-97`, `internal/db/xlog/xlog_data.go:50-64,99-109`. pool 반환 규칙이 "압축 시에만 반환" 등 조건부라 오용 가능성이 주석에 드러남 | `sync.Pool` 미사용. gzip block 조회 cache 최대 4개(논리 4×32MiB, `cap` 여유 포함), 주기 Sync마다 header snapshot(hash 1MiB, time 864KiB) 일시 할당(`docs/compatibility/storage-contract.md` §날짜와 수명·복구) | **조건부** | 후보는 ① 주기 Sync의 header snapshot buffer(index당 1회/4s, 상한 32 index), ② gzip member reader/`DecodeZipBlock` 출력, ③ TCP 응답 encode buffer. 효과는 GC 빈도 감소이며 RSS 감소는 아니다. pprof(`--profile-dir`) alloc 상위에 나타난 뒤에만 적용한다 |
| `RWMutex` 읽기 최적화 (`MemHashBlock`/`MemTimeBlock`) | `R:README.md:22`. 참고 구현은 index/data 객체마다 lock | 단일 Store gate가 메서드·Close·Sync를 직렬화. fsync는 gate 밖(`G:internal/storage/store.go:107-114,446-455`). `GOMAXPROCS` 2→4 처리량 차이 없음(개선 기록 §4) | **조건부(측정 우선)** | 측정된 다음 병목이 "단일 Store gate"이므로 gate 분할(날짜·파일 종류별)이 RWMutex보다 먼저다. 위험: 날짜 전환·eviction·retention과 조회의 순서 계약이 gate 하나에 묶여 있어 분할 시 recovery·relink·tombstone 의미를 다시 검증해야 한다 |
| `GOMEMLIMIT` 1GiB 기본 | `R:scripts/start.sh:22`. README :23은 "OOM 방지"로 서술 | 서버는 설정하지 않음. replay 도구만 `--gomemlimit` 전달(`G:cmd/scouter-replay/main.go:92`, `server.go:74-88`) | **도입 가능(운영 값 측정 후)** | soft limit라 RSS cap·OOM 방지가 아니다(AGENTS.md). 200 obj에서 RSS 0.8~0.9GiB 중 visitor HLL ~280MiB·index header는 live heap이라 limit를 낮추면 GC만 잦아진다. replay로 `GOMEMLIMIT` 유무·값별 RSS/CPU/reject를 같은 조건에서 비교한 뒤 start script 기본값을 정한다 |
| realtime ring buffer + objHash index | `internal/core/cache/xlog_cache.go:20-137`, `docs/xlog-pipeline.md:91-135`. 직렬화 bytes와 필터 metadata만 보관 | ring 20,480개(`G:internal/ingest/engine.go:92-94`), byte budget 64MiB에 포함(`engine.go:101-102,209-224`). objHash 보조 index 없음 | **조건부** | 선택 object 조회 scan 감소. 위험: Java `XLogLoopCache`의 loop/index cursor·multi-object 순서 계약을 유지해야 하며 2026-10-13 평가 §채택 4의 조건과 같다. 현재 history p95는 fsync·gate가 지배하므로 우선순위 낮음 |
| 비동기 batch writer (queue 10,000, batch 512, 포화 시 drop) | `internal/db/xlog/xlog_wr.go:23,45,50-98` | ingest가 저장 완료 후 ring에 넣는 동기 경로. UDP queue 256·worker 2 포화는 `Rejected`로 계수(`G:internal/transport/udp.go:41`, `docs/compatibility/runtime.md`) | **비도입(현 형태)** | 묵시적 drop 금지 지침 위반. "수락 ≠ 저장 완료" 의미 분리와 종료 drain·오류 전달을 갖춘 bounded batch는 2026-10-13 평가 §채택 3의 조건으로만 검토한다 |
| `bufio` 8KiB data append, `RealKeyFile` 16KB append buffer | `internal/db/io/real_data_file.go:35`, `docs/text-cache-database.md:173-181` | append offset cache(`b98062b`), record 단위 write, 주기 fsync. 비압축 data file은 OS write 뒤 4s fsync | **조건부** | syscall 수 감소. 위험: 읽기 전 flush 누락이 Java reader·같은 process 조회에 보이지 않는 record를 만든다. 적용 시 "조회 전 flush" 불변식과 crash 시 손실 창(현재 fsync 주기와 동일)을 기록 |
| zstd 압축(`[00][01][zstd]` body, `xlog.data`) | `internal/db/xlog/xlog_data.go:25,38-42`, `go.mod`(`klauspost/compress v1.18.4`) | 원본 GZipStore 형식 유지, record마다 gzip member(`G:internal/storage/zip.go:23-47`, BestSpeed). 압축률 0.192 vs Java 0.145 | **비도입** | 원본 `xlog.service`/GZipStore와 호환되지 않고 Java reader가 읽지 못한다. 저장 형식 변경은 이관·rollback 계약 전체를 다시 열어야 한다. 압축률 개선은 rollover 재압축(개선 기록 §6) 쪽이 호환 범위 안이다 |
| LRU text cache 100,000 entries | `R:internal/config/config.go:208-209`, `docs/text-cache-database.md:64` | dictionary·object·counter cache는 개수+byte 예산(4,096/64MiB/65,536) | **해당 없음(이미 상한 보유)** | entry 수 상한만으로는 긴 SQL text에 byte 상한이 없다. 우리 byte 예산 방식이 지침에 더 맞는다 |
| handle cache (우리 고유) | — | index 32·data 16 LRU, eviction 시 Sync/Close(`G:internal/storage/store.go:261,298`), tag 테이블 30·cache path 1,000(`G:internal/storage/tagcount.go:52-70`), hash header 1024+1MiB·time 864,000B를 index마다 메모리 보유(`G:internal/storage/legacyindex/index.go:32-33,163`) | 유지 | 32 index × 최대 1MiB = 약 32MiB 상한이 명시적이다. 참고 구현은 `MemHashBlock`도 같은 header를 메모리에 두므로 이 부분의 메모리 차이는 없다 |
| visitor HLL (우리 고유) | 참고 구현은 `internal/db/visitor` 3파일. rsd 계약 여부 미확인 | precision 20 → 2^20/6+1 = 174,763 words × 4B ≈ 683KiB/개, object당 2개(`G:internal/storage/visitor_hll.go:10-17`). 200 obj에서 ~280MiB | 유지 | Java `.usr` 파일 크기 계약이라 줄일 수 없다. 대신 idle object의 HLL eviction·lazy load는 호환 범위 안의 후보다(이미 eviction I/O 직렬화 `visitorIO` 있음) |

### 우리 RSS 구성 추정과 줄일 수 있는 범위

README의 50~150MB와 우리 0.8~0.9GiB(200 obj)를 직접 비교하기 전에, 우리 RSS가 어디에 쓰이는지 계약별로 나눈다. 아래는 상한·계약 기반 추정이며 heap profile 실측이 아니다(미확인 항목은 표기).

| 구성 | 상한 또는 계약 | 추정 크기 (200 obj) | 줄일 수 있는가 |
|---|---|---|---|
| visitor HLL | object당 2개 × 약 683KiB, Java `.usr` 크기 계약 | 약 267~280MiB(2E 기록 §3 해석) | 크기 자체는 불가. idle object eviction·lazy load는 가능(호환 영향 없음) |
| index header | index당 1024+1MiB(hash) 또는 864,000B(time), cache 32개 | 최대 약 32MiB | 불가(원본 형식). 주기 Sync snapshot 일시 복사는 pool 후보 |
| gzip block cache | 논리 4×32MiB + `cap` 여유 | 압축 on일 때만 최대 128MiB+ | 압축 모드 전용. block 수·크기는 설정 후보 |
| ingest cache | object 4,096·counter 65,536·byte 예산 64MiB(ring 20,480 포함) | ≤64MiB | 이미 상한. 예산 flag 노출 가능 |
| tag count pending | table 30·cache path 1,000·metadataBytes 집계 | 미확인 | 스트리밍화 완료(`d4a4317`) |
| UDP queue | 256 datagram × ≤65,507B | ≤16MiB | worker/queue flag 노출 시 함께 기록 |
| Go runtime·GC 여유 | `GOMEMLIMIT` 미설정 → GOGC 100 기본 | live heap과 비슷한 크기 | `GOMEMLIMIT` 측정 후 결정(권고 3) |
| 나머지(goroutine stack, TCP buffer, 조회 결과) | 조회 100,000 record/64MiB 상한 | 미확인 | `--profile-dir` heap profile로 확인 |

따라서 참고 구현과의 메모리 차이 중 큰 부분은 Java 계약을 그대로 유지한 구조(visitor HLL, index header)와 GC 여유분이며, 기법 부재가 아니다. visitor HLL이 참고 구현에서 같은 크기 계약을 따르는지는 미확인이다.

## 3. 기능 대조

README `R:README.md:25-33`은 지원 기능표가 아니라 7줄 bullet이다. 코드 기준으로 보완했다. 참고 구현의 service 직접 등록은 `Register(protocol.X)` 126개(`R:internal/netio/service/*.go` 22파일)이고 agent proxy는 동적 등록이다(`handler_agent_proxy.go:20`).

| 영역 | 참고 구현 | 우리 구현 (2026-10-14) | 대조 |
|---|---|---|---|
| Client READ 명령 | 직접 등록 126개 중 우리 READ scope 184개에 속한 것 79개, 초기 28개에 속한 것 27개 | READ 184개 중 179개 구현 등록, JVM 의존 5개 명시적 unsupported(`docs/implementation-plan.md` 2D) | 우리가 넓다. 참고 구현에는 StackAnalyzer/Batch/TagCount handler가 없다(2026-10-13 평가) |
| 관리·변경(mutation) 명령 | 45개 중 20개 등록: `ADD_ACCOUNT`, `ADD_ACCOUNT_GROUP`, `EDIT_ACCOUNT`, `EDIT_GROUP_POLICY`, `CHECK_JOB`, `OBJECT_REMOVE{,_INACTIVE,_IN_MEMORY}`, `SAVE_ALERT_SCRIPTING{,_CONFIG}_CONTETNS`, `SERVER_DB_DELETE`, `SET_CONFIGURE_{SERVER,COUNTERS_SITE,TELEGRAF}`, `SET_{CUSTOM,GLOBAL}_KV{,_BULK}`, `SET_{CUSTOM,GLOBAL}_TTL` | 45개 미구현(READ 단계 범위 제외) | **참고 구현에만 있음**. 각 handler의 Java 의미(권한 검사, 파일 갱신 형식, account XML) 일치 여부는 미확인 |
| REST API / pprof | `/api/v1/{objects,counter/realtime,xlog/realtime,text,server/info}`, `/health`, `/debug/pprof/*`, 기본 off(`R:internal/http/server.go:85-102`, `README.md:142-143`). 2026-10-13 평가에서 realtime counter endpoint 테스트 2건 FAIL | 없음. pprof는 `--profile-dir`로 종료 시 파일 기록 | **참고 구현에만 있음**. 원본 Java webapp API와 호환 여부는 미확인 |
| 설정 핫 리로드 | 5초 polling `ModTime` watcher(`R:cmd/scouter-server/main.go:105-106`, `internal/config/watcher.go:12-29`), account XML watcher(`main.go:241`) | 없음(flag 고정) | **참고 구현에만 있음** |
| retention | `DayContainerPurger` keepHours 기본 48(`R:internal/db/purger.go:22-24`), `AutoDeleteScheduler` keepDays 30, `DataPurgeScheduler`(`main.go:302-331`) | Java `mgr_purge_*` 파생 규칙·disk 임계·trash·열린 handle close·실패 재시도(`docs/compatibility/recovery-retention.md`). 기본 off(의도된 차이) | 양쪽 있음. 우리는 Java 규칙 호환, 참고 구현은 자체 규칙 |
| 기존 DB 이관·rollback | 없음(자체 `xlog.data`/zstd 형식) | `scouter-db inspect/migrate/verify/rollback`, 손상 22종 검출, Java oracle 이관 테스트 | **우리에만 있음** |
| crash 복구 | 없음(확인 범위). `Close`가 queue drain 안 함 | 검증된 relink, torn tail truncate, gzip count 전진, tombstone, `RecoveryEvent` 계수. SIGKILL replay 15001/15001 | **우리에만 있음** |
| Java oracle 검증 | Go roundtrip 테스트만. CI 설정(`.github`) 없음(tree 기준) | Java fixture 141개, 양방향 storage interop, 원본 reader로 replay DB 전수 대조, Java 통신 클래스 56명령 통합, race/fuzz 11 target | **우리에만 있음** |
| 측정 도구 | benchmark 함수 | `scouter-replay`(pacing·조회 병행·SIGTERM/SIGKILL·재조회 집계·RSS/CPU 시계열) | **우리에만 있음** |
| Pulse(Kafka) | 없음 | 3단계 진행 중: plugin golden 124/124, 표준 라이브러리 Kafka producer, dispatcher(`56870e1`), runtime `--pulse-conf` 연결은 당시 미완료 | **우리에만 있음** |
| GeoIP, account XML, 서비스 그룹 집계 | `internal/geoip`, `internal/login/account_xml.go`, `core/xlog_group_perf.go` | 서비스 그룹은 `docs/compatibility/service-group.md` 범위. GeoIP·account XML 미구현 | 참고 구현에만 있음(GeoIP는 원본 Java `GeoIpUtil` 의존 기능, 우선순위 낮음) |
| 기타 | Windows disk util, `{PID}.scouter` 파일 삭제 시 종료 | `flock`+`lock.dat` 5초 규칙, Unix만 검증 | 차이 기록 |

### 도입 가치가 있는 참고 기능(호환성 영향 낮고 측정 가능한 순)

| 순위 | 후보 | 호환성 영향 | 측정·검증 방법 |
|---|---|---|---|
| 1 | opt-in HTTP `/health`·`/debug/pprof`(localhost 기본) | 없음(Scouter wire와 무관). Java webapp API 호환을 주장하지 않음 | replay 중 live profile 수집으로 다음 병목(gate·fsync) 분석 시간 단축. `--profile-dir` 종료 시 기록보다 운영 친화적 |
| 2 | 관리 명령 중 KV/TTL 6종(`SET_{CUSTOM,GLOBAL}_KV{,_BULK}`, `SET_*_TTL`) | 중간. `docs/compatibility/local-store-query.md`의 READ 계약 위에 쓰기 추가. Java oracle 필요 | 원본 `KeyValueStoreRW` fixture로 파일 bytes 대조, READ 재조회 일치 |
| 3 | `OBJECT_REMOVE*` 3종 | 중간. ingest object cache와 `obj.data` slot 의미 | Java 통신 클래스 통합에 명령 추가, 재시작 뒤 object 목록 비교 |
| 4 | 설정 리로드 | 낮음(flag→파일 전환이 선행). 리로드 가능한 키(purge 임계 등)와 불가 키(DB 경로·timezone)를 분리 | 단위 테스트 + 운영 절차 문서 |
| 5 | 계정 관리 5종, alert script 저장 2종 | 높음(account XML 형식·권한, script 실행은 JVM) | 저장 형식만 Java oracle, 실행은 unsupported 유지 |

## 4. 결론

**신빙성 한 줄 판단**: README의 "Java 대비 효율 비교"는 Java 측 측정이 전혀 없는 **한쪽 열짜리 비교표**이며, Go 열의 수치는 저장 완료·fsync·조회·drop 집계가 빠진 microbenchmark이거나 조건 미기재 값이라 **서버 효율 비교로서 신빙성이 없다**(정성적 사실인 바이너리 크기·빠른 기동·짧은 GC STW만 일반론으로 타당).

근거 요약: ① Java 열에 버전 외 측정 조건이 없고 "2.21.2"라 우리 정본 2.21.3과도 다르다. ② 27ns/op는 10,000 용량 channel enqueue이며 포화 시 무통보 drop이 포함된다. ③ 768MB/s는 8KiB `bufio` append이고 fsync가 없다. ④ 14ns/op는 메모리 map 조회다. ⑤ 메모리 50~150MB는 기능 범위(handler 126개, Stack/Batch/TagCount 없음)·부하·보존 조건이 다른 서버의 값이다. ⑥ 2026-10-13 평가의 `go test` FAIL 2건과 CI 부재로 수치 재현 체계도 확인되지 않았다.

### 우리 프로젝트 적용 권고 상위 5개

| # | 권고 | 호환성 영향 | 검증 방법 |
|---|---|---|---|
| 1 | Store gate 분할 또는 eviction 경로의 gate 안 fsync 대기 제거를 다음 성능 작업으로 고정 | 없음(파일 형식·순서 동일). recovery·tombstone·retention 순서 계약 재검증 필요 | `scouter-replay` 2000 tx/s·200 obj·P2/P4 동일 조건 전후, `GOMAXPROCS` 2→4 차이 발생 여부, `make race`, storage/migrate Java oracle |
| 2 | `internal/protocol` encode/decode의 allocs/op 감소(scratch 재사용·`append` 기반 write). zero-copy Blob ref는 소유권 규칙 확정 전 보류 | 없음(바이트 동일). 빌린 slice를 ring·Pulse·storage에 넘기지 않는 규칙 유지 | `make bench` 전후(ns/op·allocs/op), fixtures 141개, `make fuzz`, race. 서버 TPS로 환산하지 않음 |
| 3 | `GOMEMLIMIT` 운영 기본값을 측정으로 결정하고 start script·runtime.md에 "soft limit" 의미와 함께 기록 | 없음 | replay `--gomemlimit` 미설정/1GiB/2GiB 동일 조건 RSS·CPU·reject·history p99 비교(200 obj soak 포함) |
| 4 | UDP `Workers`/`QueueCapacity`를 flag로 노출하고 reject 계수를 종료 로그·replay report에 유지. 묵시적 drop·무제한 queue로 바꾸지 않음 | 없음 | 2000 tx/s·200 obj에서 queue 256→1024, worker 2→4 전후 reject·RSS 비교 |
| 5 | opt-in HTTP `/health`·`/debug/pprof`(기본 off, localhost) 추가 후 관리 명령 중 KV/TTL 6종부터 Java oracle로 구현 | HTTP: 없음. KV/TTL: 파일 형식 oracle 필요 | HTTP는 단위 테스트·포트 충돌 없음 확인. KV/TTL은 원본 writer fixture bytes 대조와 READ 재조회 |

비권고: zstd `xlog.data` 형식, 포화 시 drop하는 async batch writer, entry 수만 세는 LRU text cache. 세 가지 모두 원본 파일 호환·묵시적 drop 금지·byte 예산 지침과 충돌한다.

### 미확인 사항

- README Go 수치의 측정 호스트·Go 버전·측정일, "escape analysis 검증 완료"의 산출물.
- 참고 구현 관리 명령 20개의 Java 의미 일치 여부, HTTP API와 원본 webapp API의 호환 여부, visitor HLL rsd 계약.
- 참고 구현 benchmark를 이 호스트에서 재실행한 결과(이번 조사에서 실행하지 않음).
- 우리 RSS 구성 표의 크기는 상한·계약 기반 추정이며 heap profile 실측이 아니다.

### 이 문서의 근거 범위

| 구분 | 수행 | 미수행 |
|---|---|---|
| 참고 구현 | README·docs 4건·benchmark 2파일·protocol/db/core/http/config/service 소스 열람, GitHub tree(231 blob)·main HEAD SHA 확인, service 등록 명령 126개를 우리 `query-scope.json`과 교차 | `go test`/`go bench` 실행, Java 원본과의 동일 조건 측정 |
| 우리 구현 | AGENTS.md, 기존 평가, performance, architecture, storage-contract, runtime, implementation-plan, 2E 기록 2건, `store.go`/`visitor_hll.go`/`tagcount.go`/`legacyindex/index.go`/`engine.go`/`udp.go`/`zip.go`/`pulse/CONTRACT.md` 열람 | 새 측정, 코드 변경, 테스트 실행 |

## 5. 권고 적용 상태 (2026-10-14)

| # | 권고 | 상태 | 커밋 / 근거 |
|---|---|---|---|
| 1 | Store gate stall 제거 | **적용**. eviction·archive·retention이 진행 중인 fsync pass를 기다리지 않고 close를 pass 뒤로 미루는 close queue; index eviction fsync를 해당 subtree로 한정. seam 테스트 1.06s → 9ms. gate 분할은 측정상 불필요 | `9886a0a`, `internal/storage/close_queue_test.go` |
| 2 | protocol allocs/op 감소 | **적용**. EncodePack 11~12 → 1~2 allocs(xlog 880→307 ns/op), DecodePack xlog 10→8, counter 35→16. 출력 bytes 불변(fixture 141). `Value` struct 272B는 공개 API라 보류 | `internal/protocol` 커밋, `alloc_test.go`, `bench_test.go` |
| 3 | `GOMEMLIMIT` 기본값 측정 | **측정 완료, 기본값 미설정 유지**. 1·2GiB는 RSS·누락·p99 차이 없음(max RSS 828~847MiB), 512MiB는 RSS 592~612MiB로 줄지만 reject 181·324·p99 172~519ms로 편차가 커 운영 기본값으로 제안하지 않음 | [권고 적용 기록](validation/2026-10-14-recommendations.md) §3·4 |
| 4 | UDP worker/queue flag | **적용**(`--udp-workers`, `--udp-queue`, `--udp-read-buffer`) 후 **queue 기본값 256 → 1024**. 2000 tx/s·200 obj 누락 0(2/2), 3000·4000 tx/s도 누락 0; worker 4 단독은 악화, read buffer는 효과 없음 | `ee61387`, [권고 적용 기록](validation/2026-10-14-recommendations.md) §3·4 |
| 5a | opt-in HTTP `/health`·pprof | **적용**(`--http`, 기본 off, loopback 외 바인드 경고) | `ee61387` |
| 5b | KV/TTL 관리 명령 6종 | **적용**. 원본 handler oracle 45 case response bytes·kv2 파일 동일, mutation 45 → 39 미구현 | `8e9acf5`, `docs/compatibility/local-store-query.md` |

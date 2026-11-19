# 최대 부하 근처 병목 프로파일링 준비 (초안)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

상태: **초안**. 최대 부하 근처 측정과 원인 판정(후보 3 확인)은 [최대 처리량 측정 기록](2026-10-17-xlog-max-benchmark.md)에 있다. 제품 코드는 바꾸지 않았다. 이 문서는 (1) 부하 중 profile 수집 스크립트, (2) XLog 수집 경로의 직렬화 지점과 병목 후보 정적 분석, (3) 저부하 smoke 결과를 담는다. 이 문서 작성 시점에는 최대 부하 근처 측정을 하지 않았다.

TPS는 AGENTS.md 정의대로 **저장 후 재조회로 확인된 완료 XLog 수/초**이며 scouter-replay report(`replay/report.json`)의 값만 쓴다. 이 스크립트의 profile 수치나 microbenchmark ns/op를 TPS로 환산하지 않는다.

## 1. 스크립트 `scripts/bench-profile.sh`

scouter-replay가 server를 직접 띄우므로(fresh temp DB, loopback) 스크립트는 `--server-arg=--http=127.0.0.1:<임시포트>`를 덧붙이고, 부하 구간에 `/debug/pprof`를 수집한다.

```sh
make build GO=<go>                                   # dist/scouter-go, dist/scouter-replay
GO=<go> scripts/bench-profile.sh [옵션] [-- scouter-replay 추가 인자]
# smoke (저부하)
GO=<go> scripts/bench-profile.sh --rate 800 --warmup 1 --duration 9 --cpu-seconds 6 --delay 2
GO=<go> scripts/bench-profile.sh --contention --rate 800 --warmup 1 --duration 9 --cpu-seconds 6 --delay 2
# 최대 부하 근처 (측정용 예시)
GO=<go> scripts/bench-profile.sh --rate <R> --warmup 5 --duration 45 --cpu-seconds 30 \
  -- --objects 40 --gomaxprocs 4 --server-arg=--stats-interval=5s
```

| 옵션 | 기본값 | 의미 |
|---|---|---|
| `--out DIR` | `artifacts/bench-profile/<UTC>` | 출력 디렉터리(비어 있어야 함, `artifacts/`는 gitignore) |
| `--rate/--warmup/--duration` | 1000/5/45 | replay에 그대로 전달. 부하 길이 = warmup+duration |
| `--delay`, `--cpu-seconds` | warmup, 30 | server 기동 후 대기, 수집 창. `delay+cpu-seconds ≤ warmup+duration`을 검사 |
| `--contention` | off | 아래 모드 참조 |
| `--list REGEX` | `ingest.*Engine.*AcceptFrom`, `storage.*Store.*PutXLog$` | CPU(및 block) profile `-list` 대상, 반복 가능 |
| `--http-port`, `--server`, `--replay`, `--go`, `--nodecount` | 빈 포트, `dist/*`, `$GO` | |

`--` 뒤 인자는 replay로 전달한다(`--rate/--duration/--warmup/--work-dir/--report`는 거부). server flag는 replay의 `--server-arg=...`로 넘긴다(예: `--server-arg=--udp-workers=4`, `--server-arg=--sync-interval=0`, `--server-arg=--conf=<복사본 scouter.conf>`).

### 모드

- 기본(cpu): `/debug/pprof/profile?seconds=N` CPU profile, 창 중간의 goroutine dump(debug=2)와 `/health`, 창 끝의 heap(`gc=1`)·allocs·goroutine profile과 `/health`.
- `--contention`: server에 `--profile-dir=<out>/shutdown`을 주어 기동 시 `SetBlockProfileRate(1ms)`·`SetMutexProfileFraction(10)`을 켠다(`cmd/scouter-go/profile.go:31-32`). server에는 이 rate를 HTTP나 flag로 따로 켜는 방법이 없다(`internal/server/http.go:58-62`는 표준 handler만 노출). 같은 창에서 `block?seconds=N`, `mutex?seconds=N` delta를 받는다. 이때 server가 CPU profiler를 프로세스 수명 동안 이미 쓰고 있으므로 HTTP CPU profile은 받을 수 없고, 종료 시 기록되는 전체 수명 `shutdown/<pid>/cpu.pprof`(기동·drain 포함)를 요약한다. replay 재기동 검증 server의 pid 디렉터리도 생기며, 시작 순서상 첫 번째가 부하를 받은 server다.
- `syncx.Mutex`(engine·Store gate)는 channel 기반이다(`internal/syncx/mutex.go:15-17`). 그래서 이 lock의 대기는 **mutex profile이 아니라 block profile**에 `syncx.(*Mutex).LockContext`/`Lock` 아래 `selectgo`/`chansend1`로 나타난다. mutex profile에는 `sync.Mutex`(Reassembler, legacyindex.Index, pulse offerMu 등)와 runtime lock만 잡힌다.
- 수집 비용 때문에 두 모드의 TPS는 서로 비교하지 않는다. 같은 모드끼리만 비교한다.

### 출력

```
conditions.txt          git sha/dirty, go version, host CPU/코어 수, 모드, 창, replay 명령, replay_exit
http/cpu.pprof          (기본 모드) 수집 창 CPU
http/block.pprof, http/mutex.pprof   (contention) 수집 창 delta
http/heap.pprof, allocs.pprof, goroutine.pprof, goroutine-mid.txt(debug=2), goroutine-end.txt(debug=1)
http/health-{start,mid,end}.json      ingest/udp/tcp 카운터, QueueLen/QueueCap, (stats-interval이 지났다면) stats
shutdown/<pid>/{cpu,block,mutex,heap}.pprof   (contention)
replay/report.json, replay.log, server-*.log   replay 원본 결과
summary/*.txt           pprof -top(-cum), block-gate(syncx 대기만), block-udp-idle, list.txt(-list, 샘플 없는 줄 제거)
(stdout)                파일 목록, 각 summary 앞부분, replay report 발췌, 창 중간/끝 /health의 UDP queue·ingest 카운터·ingestLatency
```

`block-gate`는 `-focus='syncx\.\(\*Mutex\)'`로 engine/Store gate 대기만 남긴다. 전체 block profile은 logger·maintain·config watch 같은 유휴 `select`가 대부분이라 그대로는 읽기 어렵다. `block-udp-idle`에서 `UDPReceiver.Serve.func2`(worker loop, `internal/transport/udp.go:443-480`)의 `chanrecv2`는 worker가 빈 queue를 기다린 시간이고, `func3`은 fragment sweeper(`udp.go:484-504`)다. func 번호는 compiler가 붙이므로 코드가 바뀌면 달라질 수 있다.

## 2. XLog 수집 경로 정적 분석 (HEAD `be9ff6d`)

### 2.1 단계별 동시성·lock

| # | 단계 | 실행 주체 / 동시성 | lock | 하는 일 | 근거 |
|---|---|---|---|---|---|
| 1 | UDP 수신 | **단일 goroutine** | 없음 | `ReadMsgUDPAddrPort` 1 datagram/syscall, datagram마다 `append` 복사 할당, non-blocking enqueue. 가득 차면 `Rejected`++ 후 폐기 | `internal/transport/udp.go:506-531` (복사 524, 거절 527-529) |
| 2 | datagram queue | `chan datagram` cap 기본 1024 (`--udp-queue`) | channel | 수신→worker 전달 | `udp.go:76`, `:438`; `cmd/scouter-go/main.go:53-55` |
| 3 | transport worker | 기본 **2** (`--udp-workers`, `_net_udp_worker_thread_count`가 conf에 있으면 그 값, 상한 256) | CAFM일 때 `Reassembler.mu`(sync.Mutex) | decode(`protocol.NewDecoder`/`Pack`), 묶음 CAFN은 pack마다 handler 호출 | `udp.go:441-480`, decode `:186-260`, 조립 `:261-305`; `main.go:387-394` |
| 4 | handler | worker goroutine | — | `engine.AcceptFrom` 호출, 소요시간을 atomic histogram에 기록(lock 대기 포함) | `internal/server/runtime.go:416-421`, `internal/server/stats.go:62-75` |
| 5 | ingest engine | worker 수만큼 경쟁 | **engine `syncx.Mutex` 1개**, pack 처리 전체 동안 보유 | `clonePack`(encode+decode), type dispatch, XLog sampling/delay, group, storage 쓰기, Pulse offer, realtime ring, visitor, tagcount | lock `internal/ingest/engine.go:286-290`, clone `:260-266,295`, dispatch `:320-432` |
| 6 | XLog 처리 | engine lock 안 | engine | sampling 판단·지연(`delayPack`은 encode), driving tx면 지연 child decode 후 저장 | `internal/ingest/sampling.go:55-104`, `:203-233` |
| 7 | storeXLog | engine lock 안 | engine | `processGroup`(service text 조회 가능), ring용 `EncodePack`, `OnXLog`(Pulse), `store.PutXLog`, ring 갱신, `PutVisitorContext`, `PutXLogTagsContext` | `sampling.go:339-382`, group `internal/ingest/group.go:53-` |
| 8 | Store.PutXLog | engine lock + **Store gate(`syncx.Mutex`)** | 둘 다 | 또 한 번 `EncodePack`, data `pwrite` 1회, index `_tim/_tid/_gid` 각 `Index.Put`(`sync.Mutex` + key file `pwrite`) | `internal/storage/xlog.go:138-189`, `internal/storage/store.go:553-596`, `internal/storage/legacyindex/index.go:302-352` |
| 9 | Store.PutProfile | engine lock + Store gate | 둘 다 | data `pwrite` + `_profile` index `pwrite`, 비 Version2·즉시 처리 profile | `internal/storage/profile.go:10-49`, 호출 `sampling.go:164-172` |
| 10 | Pulse enqueue | engine lock 안 (Pulse 설정 시에만) | `offerMu.RLock` | XLog/Profile 얕은 복사+byte slice 복제, non-blocking channel send, 가득 차면 drop | `runtime.go:353-354`, `internal/pulse/dispatcher.go:223-258` |
| 11 | realtime cache | engine lock 안 | engine | `ring[index] = encoded bytes`(기본 20480칸), 조회 `XLogsRealtime`는 **engine lock 안에서 ring 항목을 DecodePack** | `sampling.go:349-368`, `engine.go:205-207`, `:587-660` |
| 12 | visitor / tagcount | engine lock 안, 호출마다 Store gate 재획득 | 둘 다 | HLL offer, tag contribution 최대 ~20개 `tagAddLocked`; pending이 상한을 넘으면 그 자리에서 `tagFlushLocked`(디스크 읽기·쓰기) | `internal/storage/visitor.go:250-270`, `internal/storage/tagcount.go:157-226`, flush `:115-118` |

XLog 1건이 engine lock 안에서 하는 일(GxID≠0, 즉시 처리, tagcnt 기본 on): pack encode 3회(clone, ring, PutXLog)+decode 1회, Store gate 획득 3회(PutXLog, PutVisitor, PutXLogTags), `pwrite` 4회(data 1 + index 3). ProfilePack이 따로 오면 다른 `AcceptFrom` 호출로 encode/decode 1회씩(clone)과 `pwrite` 2회가 더 든다.

### 2.2 engine lock 밖에서 같은 lock을 잡는 주기 작업

| 작업 | 주기 | 잡는 lock | 근거 |
|---|---|---|---|
| `Engine.Tick` → `Store.VisitorSyncContext` → `tagTickLocked` | 1s, 그중 5s마다 `tagFlushLocked` | **engine + Store gate를 동시에 보유한 채 tag flush 디스크 I/O** | `internal/server/runtime.go:675-683`, `internal/ingest/packet_state.go:273-331`, `visitor.go:448-457`, `tagcount.go:285-299` |
| `Store.SyncData` | `--sync-interval` 기본 4s | Store gate 안에서 `tagFlushLocked`와 index header snapshot; fsync는 gate 밖 | `internal/server/runtime.go:741-742`, `store.go:686-720`; 기본값 `main.go:52` |
| `Store.FlushVisitorsIfDue` | 1s tick, 10s마다 | gate 밖에서 snapshot I/O | `runtime.go:748-749`, `visitor.go:274-287` |
| `Engine.Sample5M` | 5분 | engine lock 안에서 counter put | `engine.go:750-784` |
| Client 조회 `XLogsRealtime` | 요청마다(replay `--query-concurrency` 기본 2, 1s 간격) | engine lock, ring decode | `engine.go:587-660`, `cmd/scouter-replay/query.go:62-152` |
| Client 조회 history scan | 요청마다 | Store gate를 256 step/512 record/4MiB 단위로 나눠 보유 | `internal/storage/xlog_scan.go:43-46`, `:175-260` |

### 2.3 병목 후보 (우선순위)와 측정 방법

1. **engine 단일 lock이 수집 전체를 직렬화**. worker 수와 무관하게 `AcceptFrom` 본문은 한 번에 하나만 돈다(`engine.go:286-320`). lock 안에 동기 `pwrite`가 XLog당 4회 들어 있어 디스크 지연이 그대로 처리량 상한이 된다. 최대 처리량 ≈ 1 / (XLog당 lock 보유시간).
   - 측정: (a) contention 모드 `summary/block-gate.txt`에서 `ingest.(*Engine).AcceptFrom` 아래 `LockContext` 대기 합 ÷ (worker 수 × 창 길이). 이 비율이 1에 가까우면 worker는 사실상 lock 대기 중이다. (b) 기본 모드 CPU `Total samples/Duration`이 GOMAXPROCS×100%보다 훨씬 낮은데 `health-*.json`의 `udp.Rejected`/`QueueLen`이 늘면 CPU가 아니라 직렬화 한계다. (c) `--server-arg=--udp-workers=1/2/4`로 같은 rate의 TPS·reject를 비교한다. 늘려도 그대로면 lock 한계다.
2. **lock 안 동기 파일 쓰기(syscall)**. smoke에서 CPU 샘플의 52%가 `os.(*File).WriteAt`/`pwrite`였다(§3). macOS APFS와 Linux 대상 파일시스템의 비용은 다르므로 대상 OS에서 다시 확인해야 한다.
   - 측정: `summary/list.txt`의 `PutXLog` 줄별 cum(`appendData` vs `idx.Put`), `cpu-cum.txt`의 `syscall.pwrite`. Linux에서는 `perf trace -s`나 `strace -c -f`로 syscall 수/지연을 함께 본다(부하 격리 환경에서만).
3. **tag flush가 engine lock과 Store gate를 함께 잡은 채 디스크 I/O**(`Engine.Tick` 5s 주기, `SyncData` 4s 주기, pending 상한 초과 시 수집 경로 안). 그동안 모든 worker가 멈추고 queue가 찬다. 주기적 reject spike의 후보다.
   - 측정: `--server-arg=--stats-interval=5s`로 `/health.stats.ingestLatency.maxUs`(lock 대기 포함)를 본다. CPU `-list 'storage.*tagFlushLocked'`, block-gate에서 `Engine.Tick`/`SyncData` 시점 대기를 확인한다. A/B: 복사본 scouter.conf에 `tagcnt_enabled=false`를 넣어 `--server-arg=--conf=...`로 비교한다(기능을 끄는 비교이므로 진단용이다).
4. **pack encode/decode 반복과 할당**. XLog당 encode 3회+decode 1회, datagram마다 복사 할당, CAFM 조립 복사, tagcount 1440×float32 배열. smoke allocs에서 `protocol.ownedCopy` 13%, `PutXLogTagsContext` 17%, `tagFlushTableLocked` 23%였다. GC CPU와 lock 보유시간을 함께 늘린다.
   - 측정: `allocs-space.txt`, CPU의 `runtime.gcBgMarkWorker`/`mallocgc` cum, `--gomemlimit`·GOGC 조건 기록.
5. **단일 UDP reader goroutine**. datagram당 syscall 1회와 할당 1회다. CAFN 묶음(`--batch` 기본 32)이면 영향이 작지만 `--batch 1`이나 작은 datagram mix에서는 상한이 될 수 있다. kernel SO_RCVBUF가 넘치면 server 카운터에는 안 잡히고 replay 검증의 missing으로만 보인다.
   - 측정: CPU에서 `UDPReceiver.Serve`(reader) cum이 1코어(창 길이와 같은 초)에 가까운지 본다. `--server-arg=--udp-read-buffer=<bytes>` A/B, `report.json`의 `udp_accounting`과 missing을 비교한다.
6. **realtime 조회가 engine lock 안에서 ring을 decode**(`engine.go:591-660`). 조회 1회가 마지막 cursor 이후 항목을 lock 안에서 decode하므로, 고부하에서는 조회 1회에 수천~2만 건(ring 20480칸)을 lock 안에서 처리할 수 있다. 조회 병행 시 수집 처리량이 떨어지는 후보다.
   - 측정: `--query-concurrency 0`과 기본 2 비교, block-gate에서 `XLogsRealtime` 보유 중 대기, CPU `-list 'ingest.*XLogsRealtime'`.
7. **Pulse offer**는 non-blocking이고 lock 안에서 복사만 한다(`dispatcher.go:223-258`). 병목 가능성은 낮다. replay 기본 구성에서는 Pulse가 꺼져 있어 측정되지 않는다. Kafka 경로는 `scripts/kafka-local.py` 같은 격리 broker로 별도 측정해야 한다.

측정 순서 제안: rate를 단계적으로 올려 reject/missing이 처음 나타나는 지점 R을 찾는다. 그 근처에서 기본 모드(CPU) 1회와 contention 모드 1회를 같은 조건으로 실행한다. 이어서 후보 1·3·5·6의 A/B(`--udp-workers`, `tagcnt`, `--udp-read-buffer`, `--query-concurrency 0`)를 같은 rate로 실행한다. 조건(GOMAXPROCS, payload mix, profile 크기, text hit ratio, objects, batch, sync interval)은 `conditions.txt`와 `report.json`에 남는다.

## 3. smoke 결과 (로컬, 저부하)

조건: Apple M1 Pro 8코어(공유 장비), macOS, go1.27.2, HEAD `be9ff6d`, replay 기본 payload(objects 20, batch 32, profile-ratio 0.3, text-hit 0.95, query-concurrency 2), server 기본(udp-workers 2, sync-interval 4s), GOMAXPROCS 미설정. rate 800/s, warmup 1s + duration 9s, delay 2s, 창 6s. 부하 10초, 전체 실행 약 16초(drain·재기동 검증 포함). contention 실행에는 `-- --server-arg=--stats-interval=3s`를 추가해 passthrough도 확인했다. 아래 표는 다른 부하와 겹치지 않게 직렬로 실행한 최종 2회 결과다. 개발 중 같은 조건으로 2회 더 실행했고 결과는 비슷했다(800.06/800.10/s, missing 0). 매 실행 후 server 프로세스가 남지 않은 것을 확인했다.

| 항목 | 기본 모드 | contention 모드 |
|---|---|---|
| replay exit | 0 | 0 |
| steady verified rate (report) | 800.07/s | 800.04/s |
| drain 포함 대표 TPS (report) | 714.1/s | 707.7/s |
| missing / not_sent / server UDP reject | 0 / 0 / 0 | 0 / 0 / 0 |
| 수집 파일 | cpu, heap, allocs, goroutine(pprof·debug=1·debug=2), health×3, summary 7개 | + block, mutex(창 delta), shutdown/<pid>/{cpu,block,mutex,heap}×2(부하 server, 재기동 server), summary 13개 |

기본 모드 CPU(6.13s 창, 샘플 1000ms ≈ 0.16코어, `pprof -top -cum` 발췌):

```
 cum%
 63.0%  syscall.rawsyscalln (flat 63.0%)
 44.0%  os.(*File).WriteAt / syscall.pwrite
 39.0%  transport.(*UDPReceiver).Serve.func2   (UDP worker)
 37.0%  ingest.(*Engine).AcceptFrom / acceptLocked
 23.0%  storage.(*Store).appendData
 20.0%  ingest.(*Engine).storeXLog
```

contention 모드:
- `-list`(부하 server 전체 수명 CPU 1.83s): `AcceptFrom` 930ms가 모두 `acceptLocked`(`engine.go:320`). `PutXLog` 450ms 중 `appendData` 330ms(`xlog.go:167`), `idx.Put` 120ms(`xlog.go:184`).
- block(6s 창): engine `LockContext` 대기 447ms(`engine.go:286`), `PutXLog`의 Store gate `Lock` 15ms(`xlog.go:139`), history 조회(`XLogsHistory`)의 gate 대기 0.12s. UDP worker 유휴 `chanrecv2` 10.53s(2 worker × 6s = 12s 중 약 88% 유휴).
- mutex(6s 창): 합계 2.3ms, runtime 내부 lock만 잡혔다.
- `/health.stats.ingestLatency`(3s 창, handler 기준이라 lock 대기 포함): count 5668, p50 47µs, p95 207µs, **p99 2303µs, max 37281µs**. 저부하에서도 수십 ms짜리 정체가 있다. 후보 3(주기 tag flush·sync가 gate를 보유하는 구간)을 최대 부하 근처에서 확인할 근거다. 원인 판정은 아직 하지 않았다. 스크립트 출력부 수정 뒤 기본 모드를 `--stats-interval=3s`로 한 번 더 실행했다(exit 0, verified 800.07/s, missing 0). 창 끝 `/health` snapshot에서 UDP queue가 **222/1024**였고 ingestLatency max는 36546µs였다. 800/s에서도 순간적으로 worker가 멈춰 queue가 쌓이는 구간이 있다. 최대 부하 근처에서는 이 정체가 queue 포화(reject)로 이어질 수 있다.

800/s에서는 worker가 대부분 유휴이고 lock 경쟁은 작다. **이 수치는 저부하 smoke이고 최대 부하 근처의 병목 판정 근거가 아니다.**

## 4. 알려진 한계

- 최대 부하 근처 측정, Linux 대상 측정, Pulse 활성 경로 측정은 하지 않았다(미검증).
- contention 모드는 CPU profile이 전체 수명(기동·drain 포함)이라 기본 모드 CPU와 직접 비교할 수 없다.
- block profile rate 1ms 표본이라 짧은 lock 대기는 과소 집계된다. mutex fraction 10은 1/10 표본이다.
- 수집 창은 server 기동(`/health` 응답) 기준이다. replay의 실제 첫 전송과는 작은 차이가 있다(`--delay`로 흡수).
- `block-udp-idle`의 `Serve.func2/func3` 이름은 compiler가 붙인 이름이라 코드 변경에 따라 달라질 수 있다.
- 스크립트는 macOS 기본 bash 3.2와 `curl`, `python3`(빈 포트 선택, report 발췌)을 사용한다. Linux에서는 실행 확인하지 않았다.

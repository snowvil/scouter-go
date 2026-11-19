# 다음 개선 후보 실측 조사 (2026-10-23)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

HEAD `32d87da` 빌드로 완료 XLog당 CPU, lock 점유, 손실 직전 동작을 프로파일로 쟀다. 목적은 다음 라운드 개선 후보를 근거와 함께 순위화하는 것이다. 코드는 바꾸지 않았다.

## 결론

1. **object가 500개를 넘으면 visitor cache가 thrash한다(신규, 1순위).**
   - 조건: object 4000개, XLog 5,000/s.
   - CPU: visitor 경로가 서버 CPU의 47~55%를 쓴다(`offerVisitorLocked`).
   - 손실: visitor writer가 기록의 약 70%를 버렸다(`ingestWriters` visitor `Dropped` 119,870~130,757, 실행당 XLog 175,000건).
   - object 8000개, XLog 200/s에서도 visitor 경로가 서버 CPU의 42%였다.
   - 원인: object별 daily/hourly entry 상한이 500이다. 그래서 거의 모든 방문이 miss가 되고, miss마다 다음을 한다.
     - 전체 entry를 정렬한다(`sort.Slice`, CPU 10~11%).
     - dirty victim을 저장한다(Java dense 형식 파일 쓰기와 rename, 23~34%).
     - 새 파일을 읽는다(8~9%).
   - object 200개 시나리오에서는 drop 0, CPU 4~8%다. 그래서 지금까지의 replay 측정에서는 보이지 않았다.
2. **처리된 gxid 집합(`addProcessed`)이 GC 비용의 큰 몫이다(신규, 2순위).**
   - 구조: 기본 500,000개를 `container/list` 요소로 들고 있다.
   - 비중: XLog 전용 30,000/s 실행이 끝날 때 live heap object의 96%(약 116만 개 중 111만 개), live heap 48MiB(40%)를 차지한다.
   - 같은 빌드에서 이 상한을 conf로 1000으로 낮춘 A/B(아래 §5.2) 결과는 이렇다.
     - 완료 XLog 1,000건당 서버 CPU: XLog 전용 −6.2%(6쌍 중 5쌍 감소), mix −9.6%(4/4쌍)
     - RSS: 약 100MiB 감소
   - 상한을 낮추는 것 자체는 의미가 바뀌므로 해법이 아니다. 같은 FIFO 의미를 pointer 없는 구조로 바꾸면 이 차이의 대부분을 얻을 것으로 본다(추정).
3. **손실 직전의 실패는 CPU나 lock 포화가 아니다.** 원인 미확정의 일시적 UDP queue 포화다.
   - 증상: XLog 전용에서 60k·70k는 실패했고 80k는 3회 중 2회 통과했다. 비단조다.
   - 실패한 실행은 모두 한 순간(1초 표본 하나)에 UDP queue reject 1,956~9,137건이 몰렸다.
   - 그 순간에도 lock 포화는 없었다. engine held는 27~47%, 서버 CPU는 약 1코어였다.
   - mix 40k 실패는 부하 생성기 지연 때문이다(`send_lag_over_1s` 76,697). 서버 한계로 보지 않는다.
4. **정상 규모(object 200)의 CPU 분포는 이전 조사와 같다.**
   - tag 반영·flush: XLog 전용 20~25%, mix 15%
   - engine accept: 14~22%
   - XLog/profile writer: 11~18%
   - GC mark: 7~16%
   - UDP 수신: 5~11%
   - 포화된 lock은 없다. 최대는 XLog 전용 80k에서 tag gate held 26%, engine held 21%였다.
5. **R5(Agent별 KEEP_ALIVE·pool 전역 lock)는 우선순위가 낮다.** (`R5`~`R7`, `6단계`, `7라운드` 같은 이름표는 [개선 대상](../improvement-candidates-2026-10-21.md)의 항목 번호와 단계 이름이다. `7라운드`는 그 문서의 7단계다.)
   - 8000 idle Agent에서 서버 CPU는 1코어의 12~13%다.
   - 그중 pool lock(`claimIdle`·`release`·`signalLocked`)은 profile의 약 3%, 1코어 기준 0.4% 이하다.
   - 나머지는 KEEP_ALIVE write·read syscall과 goroutine wakeup이다. 프로토콜이 정한 비용이다.
6. **R6(counter 이름 사전 재구성)도 낮다. 실제 비용은 다른 곳에 있다.**
   - counter 16,000 pack/s에서 사전 map 재구성은 CPU의 0.3%다(replay counter 이름 5개 기준).
   - 같은 조건에서 engine lock 안의 `cacheCounterLocked`가 8.5%를 쓰고, engine held를 8%에서 25%로 올린다.

## 1. 측정 조건

| 항목 | 값 |
|---|---|
| 빌드 | `32d87da`(HEAD, 미커밋 변경 제외: 별도 detached worktree), `GOOS=linux GOARCH=arm64 CGO_ENABLED=0`, go1.27.2 |
| 환경 | Lima VM, Ubuntu 24.04 arm64, 4 vCPU, 4GiB. 서버와 부하기가 같은 VM(loopback). `net.core.rmem_max=8388608` |
| 서버 | replay 관리 모드(새 DB, 기본 설정, GOMAXPROCS·GOMEMLIMIT 미설정, `--sync-interval` 기본), `--lock-stats`, replay `--sample-health` |
| 부하(공통) | `scouter-replay --objects 200 --senders 2 --batch 1 --query-concurrency 2 --text-cardinality 2000 --seed 9 --warmup 5s --duration 30s` |
| mix | 기본 profile 비율 0.3, 크기 0:0.1·1024:0.6·8192:0.25·131072:0.05 |
| XLog 전용 | `--profile-ratio 0` |
| TPS 정의 | 완료 XLog(저장 뒤 재조회로 검증한 고유 XLog) |
| CPU 지표 | 서버 프로세스 utime+stime ÷ 완료 XLog × 1000(초, 기동·warmup·drain 포함, 1차 서버만) |
| lock 점유 | warmup 이후 송신 구간의 `/health` locks 차이. held·waited는 구간 대비 %. waited는 대기 합이라 100%를 넘을 수 있다 |
| 프로파일 | `--profile-dir`(전체 수명 CPU, block 1ms, mutex 1/10, 종료 시 heap). R5는 `/debug/pprof` 60초 창 |
| 겹침 확인 | 회차 직전 VM load·상위 프로세스를 기록했다(`*.pre`). 측정 중 다른 측정은 없었다 |

- 이 VM의 회차 간 변동은 크다(같은 시나리오에서 ±15%, §2).
- 개선 효과는 같은 시간대에 번갈아 잰 쌍 비교(§5.2)로만 판단한다.
- 프로파일 비율은 회차 간 ±2%p 안에서 일치했다.

## 2. 시나리오별 수치

| 시나리오 | 회차 | 통과 | 서버 CPU/1k XLog(초) | RSS 최대(MiB) | engine held/waited | Store gate held(xlog/profile writer) | tag gate held |
|---|---|---|---|---|---|---|---|
| mix 15,000/s | plain 2 | 2/2 | 0.0458, 0.0414 | 279, 255 | 8.7/5.8, 6.8/4.5 | 12.4(5.0/6.5), 10.6(4.1/5.7) | 9.9, 9.4 |
| mix 15,000/s | profile 2 | 2/2 | 0.0442, 0.0453 | 233, 300 | 8.3/5.0, 7.6/5.0 | 12.0, 10.7 | 10.3, 10.7 |
| XLog 30,000/s | plain 2 | 2/2 | 0.0237, 0.0175 | 263, 252 | 12.4/9.1, 8.6/6.5 | 9.1(7.9/0), 6.7(5.8/0) | 13.4, 10.3 |
| XLog 30,000/s | profile 2 | 2/2 | 0.0250, 0.0196 | 269, 268 | 12.3/10.3, 9.2/7.7 | 8.9, 7.1 | 14.3, 11.3 |
| mix 35,000/s | profile 2 | 2/2 | 0.0259, 0.0254 | 320, 319 | 13.5/10.5, 12.8/10.0 | 18.4(6.9/9.1) | 15.8, 14.7 |
| XLog 80,000/s | profile 2 + plain 1 | 2/3 | 0.0128, 0.0130, 0.0124 | 760, 755, 756 | 19.7~20.7 / 18.0~22.5 | 13.8~14.2(11.9~12.3/0) | 25.8~25.9 |

- 손실 직전 rate(ladder, plain 1회씩, 이어서 반복 측정):

| 종류 | 통과 | 실패 |
|---|---|---|
| XLog 전용 | 50k, 80k(3회 중 2회) | 60k(2/2: UDP reject 4,559·1,956), 70k(UDP reject 9,137), 80k 1회(reject 3,067) |
| mix | 25k, 30k, 35k(3/3) | 40k(UDP reject 569, 생성기 `send_lag_over_1s` 76,697) |

- 모든 실행에서 kernel `RcvbufErrors`는 0이었다. XLog 손실은 모두 서버 UDP queue reject(queue 8192)다.
- 서버 CPU는 XLog 80k에서도 실행 전체 평균 약 0.8~0.9코어였다. 4 vCPU에서 포화가 아니다.
- RSS는 XLog 50k 이상에서 약 750MiB로 커졌다. 그러나 같은 시점 heap profile의 inuse는 139MiB(30k에서 113MiB)였다.
  - 차이는 GC heap 목표, 파일 매핑 등으로 추정한다.
  - RssAnon과 RssFile은 나눠 재지 않았다(미검증).

## 3. CPU 상위 경로 (object 200, 서버 CPU profile의 cum%)

| 경로 | mix 15k(2회) | mix 35k(2회) | XLog 30k(2회) | XLog 80k(2회) |
|---|---|---|---|---|
| UDP 수신 goroutine(recvmsg 포함) | 10.9 / 10.3 | 9.5 / 10.3 | 6.7 / 7.5 | 5.4 / 5.9 |
| └ recvmsg syscall | 5.4 / 5.3 | 5.6 / 5.8 | 4.6 / 4.9 | 3.8 / 4.3 |
| UDP worker 전체(decode+accept) | 19.6 / 18.5 | 21.6 / 20.4 | 26.0 / 25.7 | 28.4 / 27.8 |
| └ decode | 3.6 / 3.7 | 3.1 / 3.0 | 5.3 / 4.6 | 4.4 / 4.2 |
| └ engine accept(engine lock 안) | 14.4 / 13.7 | 16.1 / 15.2 | 19.9 / 20.3 | 21.5 / 21.5 |
| 　└ storeXLog | 8.2 / 8.0 | 8.8 / 8.2 | 11.7 / 11.7 | 11.9 / 12.4 |
| 　└ PrepareXLogTags | 2.4 / 3.0 | 3.1 / 3.5 | 5.0 / 4.3 | 4.0 / 4.0 |
| 　└ EncodePack | 3.6 / 3.8 | 3.3 / 2.7 | 4.0 / 4.1 | 4.1 / 4.5 |
| 　└ addProcessed | 1.9 / 1.6 | 2.7 / 1.7 | 3.2 / 2.7 | 3.8 / 3.8 |
| tag 반영+flush(`FlushTagCountsIfDue`) | 15.4 / 15.7 | 15.7 / 15.2 | 21.5 / 19.8 | 24.5 / 24.6 |
| └ tag 반영(drain·apply) | 8.2 / 8.8 | 10.8 / 10.9 | 13.8 / 13.2 | 18.8 / 19.4 |
| └ tag flush pass | 7.3 / 6.9 | 4.9 / 4.4 | 7.7 / 6.6 | 5.6 / 5.1 |
| XLog/profile writer | 16.7 / 16.3 | 15.9 / 17.9 | 11.3 / 11.7 | 12.0 / 12.5 |
| └ pwrite 전체 | 13.8 / 12.8 | 11.6 / 13.3 | 7.7 / 6.9 | 5.9 / 7.1 |
| visitor writer | 3.7 / 3.0 | 4.0 / 4.6 | 5.8 / 6.6 | 8.3 / 8.0 |
| Client 조회(replay 조회 worker 2) | 4.9 / 6.0 | 4.1 / 3.6 | 6.2 / 5.9 | 4.8 / 4.7 |
| GC mark worker | 11.7 / 13.5 | 16.2 / 16.3 | 9.3 / 10.3 | 7.6 / 7.4 |
| └ idle mark | 7.4 / 8.4 | 10.9 / 10.9 | 5.6 / 6.9 | 4.6 / 4.5 |
| mallocgc | 8.1 / 7.3 | 5.9 / 6.0 | 7.4 / 6.7 | 6.0 / 7.3 |
| scheduler `findRunnable` | 6.5 / 5.9 | 5.2 / 4.5 | 4.5 / 4.7 | 3.2 / 2.2 |

세부(XLog 30k 기준, line profile):

- **tag**:
  - `tagAddLocked` 9.5%
    - key encode(`tagScratchKey`, `Encoder.Reset` 포함) 2.8%
    - `tagKey`의 crc32 3.2%
    - path map 조회(`cachePaths`·`tables`) 약 1.9%
  - `2026-10-17-xlog-max-benchmark.md` §17.4와 `docs/performance-structural-review-2026-10-20.md` §9에서 key 미리 계산을 이미 시도했다. 프로파일에서는 줄었지만 쌍 비교에서는 효과가 없었다. 메모리 지연이 드러날 뿐이다.
- **engine lock 안**:
  - `PrepareXLogTags` 5.0%. alloc 14.5%(`append([]tagContribution(nil), …)` 복사)
  - `EncodePack` 3.0%
  - `addProcessed` 3.2%(map과 list 갱신, XLog마다 할당 2회)
- **할당**(누적 alloc_space):
  - XLog 전용: `Decoder.xlog` 17%, `PrepareXLogTags` 15%, `Encoder.init` 13%, `tagEnqueue` 7%
  - mix: UDP datagram 복사(`UDPReceiver.Serve`) 33%, 분할 조립 출력(`Reassembler.addFragment`) 22%. profile byte량(약 42MB/s)에 비례한다.
  - 큰 []byte는 noscan이라 mark 비용은 작다. 이전 GOGC 시험에서도 CPU 이득이 없었다(`docs/validation/2026-10-17-xlog-max-benchmark.md` §17.4).
- **live heap**(종료 시 inuse, XLog 30k):
  - `addProcessed`와 list 48MiB, object 수 기준 96%
  - visitor HLL word 48MiB(대부분 noscan)
  - index page 9MiB
- **contention**:
  - block profile(1ms 표본)에서 engine lock 대기는 30초 창에 1.3~2.6초, Store gate 대기(packWriter)는 mix에서 1.6초였다.
  - mutex profile은 visitor writer 쪽이 가장 컸다(XLog 30k 7.6ms, 1/10 표본).
  - 모두 포화 수준이 아니다.

## 4. 대규모 object·연결 (R5, R6, visitor)

### 4.1 R5: 8000 Agent TCP 세션

조건은 `scouter-replay agents`, 20초에 걸쳐 연결하고 120초 유지했다. CPU는 연결 완료 뒤 60초 창의 서버 utime+stime이다. 같은 VM에서 부하기가 함께 돌았다.

| 시나리오 | 회차 | 서버 CPU(1코어=100%) | RSS | KEEP_ALIVE 수(120초) |
|---|---|---|---|---|
| idle8000(UDP·Client 없음) | 3 | 12.7, 12.2, 11.2 | 274~280MiB | 약 110,500 |
| A8000(+UDP heartbeat·counter 2초, XLog 200/s, Client 2, probe) | 3 | 68.6, 69.1, 73.0 | 430~451MiB | 약 107,400 |
| idle0(Agent 없음, 비교용) | 1 | 0.3 | 16MiB | — |

idle8000 profile(60초 창, 서버 CPU 6.7초):

- `agentPool.handle`은 42.8%다.
  - KEEP_ALIVE 왕복(`RequestPack`) 33%: write syscall 17.5%, read 6.7%
  - `context.WithTimeout` 2.8%
  - ticker select 3.4%
  - pool lock 경로(`claimIdle` 0.03초, `release` 0.16초, `signalLocked` 포함)는 합계 약 3%다. 1코어 기준으로는 0.4% 이하다.
- scheduler·futex wakeup은 33%다. 위 경로와 겹친다.
- 부하기의 goroutine pprof 표본 수집(`--http`)이 서버 CPU의 10.5%를 썼다. 측정 도구의 비용이다.
- mutex profile에서 pool lock 경합은 60초에 `release` 8.9ms(1/10 표본)다. 무시할 수준이다.
- **판단**: R5의 "전역 lock" 비용은 실측상 작다.
  - 줄일 여지는 연결별 ticker 대신 Java `TcpAgentManager`처럼 5초 scan 하나로 바꾸는 것뿐이다. 효과는 12% 중 수 %p로 추정한다.
  - 우선순위는 낮다.

A8000의 서버 CPU 69% 중 42%가 visitor 경로였다(§4.3).

### 4.2 R6: counter pack 빈도

조건: object 4000, XLog 5,000/s, `--profile-ratio 0`. counter 간격을 2초(2,000 pack/s)와 250ms(16,000 pack/s)로 비교했다. plain 각 2회, profile 각 1회다.

| counter 간격 | 서버 CPU/1k XLog | engine held/waited | 비고 |
|---|---|---|---|
| 2초 | 0.394(실패: UDP reject 184), 0.292, 0.282(profile) | 34.8/39.9(실패 회차), 8.6/6.6, 7.5/6.6 | visitor writer Dropped 12.2만~13.1만 |
| 250ms | 0.321, 0.321, 0.340(profile, 실패: reject 3,013) | 25.5/29.0, 24.4/27.5, 24.2/28.7 | 같음 |

- counter pack 14,000/s를 늘리면 서버 CPU가 실행당 약 5~10초 늘어난다. pack당 약 10~18µs다(추정, 회차 변동 큼).
- 250ms profile 기준 비용:
  - `Counters.PutContext` 3.5%, 그중 `putRealtime` 3.0%. 이름 사전 map 재구성(326~334행)은 약 0.19초(0.3%)다.
  - 나머지는 파일 `Seek`·`Write`·index `Put`(약 1.1초)이다.
  - 사전 재구성 비용은 이름 수에 비례한다. 실제 Agent처럼 이름이 수십 개면 몇 배가 될 수 있다(미검증).
- 더 큰 비용은 engine lock 안의 `cacheCounterLocked`다. 8.5%를 쓴다.
  - entry마다 `EncodeValue`와 복사를 한다.
  - string key map 5개(`current`·`samples`·`counterSizes`·`sampleSizes`·`counterExpiry`)를 갱신한다.
  - engine held가 8%에서 25%로 오른다.
- Tick과 Sample5M:
  - 현재 코드에서 Sample5M은 `takeSamples`만 lock 안에서 하고 저장은 writer pass로 넘긴다(`internal/ingest/engine.go:1067-1098`).
  - 5분 경계를 포함한 측정은 하지 않았다(미검증).
- object 4000 조건에서는 counter보다 visitor 비용(47~55%)이 압도적이었다(§4.3).

### 4.3 visitor cache thrash (신규)

| 조건 | visitor 경로 CPU | visitor writer 결과 |
|---|---|---|
| object 200, XLog 30k/80k | 5.8~8.3% | Enqueued=Written, Dropped 0 |
| object 8000, XLog 200/s(A8000) | 42%(`offerVisitorLocked` 41.6%: save 22.8%, `sort.Slice` 9.5%, load 8.4%) | Dropped 0(23,919건 모두 기록) |
| object 4000, XLog 5,000/s(R6 2초) | 54.6%(save 33.6%, sort 11.1%, load 9.4%) | Enqueued 51,895, Written 13,818, **Dropped 123,141**, Pending 38,077(c2s-plain-2) |

- **코드**: `internal/storage/visitor.go:435-513`
  - object별 daily와 hourly entry 상한은 각각 500, objType daily는 100이다.
  - miss일 때의 처리는 이렇다.
    1. 같은 종류의 key를 모두 세어 slice로 모은다.
    2. `sort.Slice`로 가장 오래된 entry를 찾는다.
    3. dirty이면 저장한다(Java dense HLL 형식 파일 쓰기와 rename).
    4. 새 파일을 읽는다.
- **원본**: `docs/compatibility/aggregation-query.md:68`에 따르면 Java도 같은 500 상한과 FIFO 축출이다. 다만 Java는 dirty 축출을 저장하지 않고 버린다. Go는 저장하므로 miss마다 쓰기가 한 번 더 있다(의도된 보존 차이).
- **영향**:
  - 사용자 ID가 있는 XLog를 보내는 object가 500을 넘으면 방문이 대부분 miss가 된다.
  - CPU가 XLog rate에 비례해 늘어난다. 이 VM에서 방문 1건당 수백 µs~1ms 이상이다(A8000: 16.7 CPU초 ÷ 60초 ÷ 약 200건/s).
  - writer가 따라가지 못하면 visitor 기록을 버린다(Dropped). 즉 방문자 수가 과소 집계된다.
- **replay의 기존 시나리오(object 200)에서는 드러나지 않았다.**

## 5. 개선 후보 순위

### 5.1 순위

| 순위 | 후보 | 근거(실측) | 예상 효과 | 호환성 위험 | 변경 범위 | 측정 방법 |
|---|---|---|---|---|---|---|
| 1 | **visitor cache를 object 수에 맞게**: entry 상한을 메모리 예산 기반으로 올린다(sparse HLL은 작다). 축출 순서는 정렬 대신 O(1) 구조로 바꾼다. 축출 저장을 10초 flush에 맡기는 방안을 검토한다 | §4.3: object 4000에서 CPU 47~55%, Dropped 약 70%. object 8000·200/s에서 CPU 42% | object 500 초과 환경에서 visitor CPU 대부분 제거, Dropped 0. object 200 환경은 변화 없음 | 파일 형식·cardinality 의미는 그대로다. 메모리 상한은 새로 정해야 한다(dense 승격 시 entry당 최대 약 700KB). Java 대비 축출 동작 차이는 이미 기록돼 있다 | `internal/storage/visitor.go`, 상한 설정(config) | `--objects 4000 --rate 5000 --profile-ratio 0`: visitor writer Dropped·CPU/1k. A8000 서버 CPU. object 200 A/B 8쌍(회귀 없음) |
| 2 | **처리된 gxid 집합을 pointer 없는 구조로**: `container/list`와 `map[int64]*list.Element` 대신 고정 ring(`[]struct{txid int64; discard int32}`)과 `map[int64]uint32`(또는 open addressing)를 쓴다 | §3 live heap object 96%. §5.2 상한 축소 A/B: XLog −6.2%(5/6), mix −9.6%(4/4), RSS −100MiB | CPU 약 5~9%, RSS 약 60~100MiB 감소(상한 축소 A/B를 상한으로 본 추정) | FIFO 축출 순서, 기존 entry의 갱신(위치 불변), keep count conf 의미를 그대로 둬야 한다. 순수 내부 구조 변경이다 | `internal/ingest/sampling.go` `addProcessed`와 조회부 | `scripts/bench-ab.sh` 8쌍(XLog 30k, mix 15k), heap inuse_objects 확인, 기존 sampling 테스트 |
| 3 | **고rate 일시 UDP queue 포화의 원인 규명**: 실패 순간을 잡는 trace(`/health` Rejected 증가 시 `/debug/pprof/trace` 수집) | §2: XLog 60k·70k 실패, 80k 1/3 실패. 매번 한 순간에 몰렸다(t≈10초 또는 송신 종료 직전). lock·CPU는 포화가 아니다 | 손실 직전 rate의 안정화(현재 비단조). CPU 효과는 없음 | 원인에 따라 다르다. queue 크기 상향은 메모리 상한 변경이다 | 측정 먼저(replay·스크립트). 이후 원인 모듈 | 같은 rate 반복 5회 이상에서 reject 0 여부, trace의 STW·lock hold 구간 |
| 4 | **counter cache 갱신을 engine lock 밖으로 또는 단순화**: entry당 map 5개 갱신과 `EncodeValue`. R6 사전 재구성은 cache 하나로 같이 정리 | §4.2: counter 16k/s에서 `cacheCounterLocked` 8.5%, engine held 8→25%. 사전 재구성은 0.3% | counter 비중이 큰 환경에서 engine lock 점유 감소(수 %p), CPU 수 % | counter cache 조회 의미(10초 만료, 5분 sample)를 유지해야 한다 | `internal/ingest`(engine), `internal/storage/counter.go` | R6 시나리오 A/B, engine held |
| 5 | **tag 반영 자료 구조**(구조 변경): pending map key·hour 배열 접근 축소, XLog당 기여 복사 제거(`PrepareXLogTags`의 alloc 15%) | §3: XLog 전용 20~25%, 80k에서 tag gate held 26% | 큼(최대 10%대). 단, 과거 미세 변경은 효과가 없었다(`performance-structural-review-2026-10-20.md` §9) | tag count 파일·순서 계약이 엄격하다(tag 쓰기 순서 결함 이력) | `internal/storage/tag*.go` | A/B 8쌍 이상, tag 계약 테스트 |
| 6 | R5: 연결별 5초 ticker 대신 scan goroutine 하나 | §4.1: pool lock은 1코어의 0.4% 이하. 전체 KEEP_ALIVE 비용 12%의 대부분은 I/O | 8000 연결에서 1코어의 1~2%p(추정) | Java `TcpAgentManager` 5초 scan과 더 가깝다 | `internal/server/runtime.go` handle | idle8000 60초 창 CPU 3회 |
| 7 | UDP datagram·조립 버퍼 재사용(mix) | §3 alloc 55%가 profile byte량에 비례 | 작음(noscan, GOGC 시험에서 CPU 이득 없음) | 버퍼 소유권(AGENTS.md: 비동기 소비자에 빌린 slice 금지) | `internal/transport` | 보류 |

### 5.2 처리된 gxid 집합 상한 A/B (같은 빌드, conf만 다름)

- a: 기본값 500,000. b: `xlog_sampling_matcher_gxid_keep_memory_count=1000`(`--server-arg=--conf=…`). b의 서버 로그에는 "config not found"가 없다. conf가 적용된 것을 확인했다.
- 순서는 쌍마다 번갈아 정했다(AB, BA, …).
- 각 plain 30초 + warmup 5초, 그 밖의 조건은 §1과 같다.

| 시나리오 | 쌍 | 쌍 차이(b vs a) 중앙값 | 평균 / SD | b가 적은 쌍 | 중앙값 CPU/1k a → b | RSS a → b |
|---|---|---|---|---|---|---|
| XLog 30,000/s | 6 | −6.2% | −3.9% / 4.3% | 5/6 | 0.0193 → 0.0183 | 258~272 → 157~169MiB |
| mix 15,000/s | 4 | −9.6% | −8.5% / 3.0% | 4/4 | 0.0341 → 0.0310 | 230~237 → 143~151MiB |

- 이 결과는 "pointer가 많은 50만 entry를 들고 있는 비용"의 상한이다.
- pointer 없는 구조도 map은 남는다(noscan). 따라서 실제 효과는 이보다 작을 수 있다. 구현 뒤 bench-ab 8쌍으로 확인한다.
- engine held는 거의 같았다(XLog 9.0→8.6%).

## 6. 측정 한계

- 서버와 부하기가 같은 4 vCPU VM에 있다.
  - mix 40k 실패는 생성기 지연 때문이다.
  - XLog 고rate에서 생성기 CPU는 실행당 약 13초였다.
  - 절대 처리량은 이 VM의 수치일 뿐이며 운영 처리량을 뜻하지 않는다.
- 같은 시나리오의 회차 간 CPU/1k 변동이 크다(XLog 30k에서 0.0175~0.025). 판단은 §5.2 같은 번갈음 쌍으로 한다. 단일 프로파일의 차이는 근거로 쓰지 않았다.
- CPU/1k에는 기동·warmup·drain이 포함된다. 프로파일은 서버 전체 수명이다(R5만 60초 창).
- replay 부하의 한계:
  - 모든 XLog가 driving이고 사용자 ID를 가진다.
  - counter 이름은 5개다.
  - object 수 4000·8000 조건은 visitor 비용을 키우는 쪽으로 단순하다.
  - 실제 Agent의 사용자 ID 비율, counter 이름 수, object별 XLog 분포는 재지 않았다.
- 미측정 항목:
  - R6의 Tick·Sample5M 5분 경계 지연
  - RSS 구성(RssAnon·RssFile)
  - UDP queue 포화 순간의 trace(5초 trace 2회는 포화 순간을 잡지 못했다)
- R5 시나리오에서는 부하기(`agents --http`)의 goroutine 표본 수집이 서버 CPU에 포함된다(idle에서 약 10%).
- block profile은 1ms 이상 대기만 표본한다. mutex profile은 1/10 표본이다.

## 7. 재현

- **명령 예**:
  - `dist/scouter-replay --server dist/scouter-go --rate 30000 --duration 30s --warmup 5s --seed 9 --sample-health --objects 200 --senders 2 --batch 1 --query-concurrency 2 --text-cardinality 2000 --profile-ratio 0 --server-arg=--lock-stats [--server-arg=--profile-dir=DIR]`
  - R6·visitor: 위 명령에 `--objects 4000 --rate 5000 --counter-interval 2s|250ms`
  - R5: `scouter-replay agents --agents 8000 --spread 20s --duration 120s [--udp … --xlog-rate 200 --clients 2 --probe-interval 5s]`

## 8. 고rate UDP queue 일시 포화 trace (2026-10-23~24, 후보 3)

### 8.1 방법

- **빌드**: `da03b8d`
- **조건**: XLog 전용, §1과 같은 replay 조건. 60,000/s 5회, 80,000/s 5회, trace 없는 대조 60,000/s 3회.
- **감시와 수집**:
  - 실행 중 `/health`를 100ms마다 읽어 UDP `Rejected`와 `QueueLen`, lock 합계를 기록했다.
  - 서버 `/debug/pprof/trace?seconds=2`를 끊김 없이 이어 받았다.
  - reject가 늘어난 100ms 구간과 겹치는 trace만 남겼다.
- **분석**: `go tool trace -d=parsed`의 상태 전이로 다음을 집계했다.
  - UDP worker·reader의 상태 시간
  - worker가 하나도 실행되지 않은 구간
  - 그 구간에 실행된 goroutine
  - STW·GC 구간, 5ms 이상 syscall
- **환경 주의**: 이 회차 동안 Mac 호스트에서 다른 테스트가 돌았다. VM vCPU가 밀렸을 수 있다. 빈도 수치는 참고만 한다.

### 8.2 결과

| rate | trace | reject 발생 | reject 수 |
|---|---|---|---|
| 60,000/s | 있음 | 2/5(x60-2, x60-4) | 9,507 / 4,061 |
| 80,000/s | 있음 | 0/5 | — |
| 60,000/s | 없음(대조) | 0/3 | — |

reject는 실행마다 한 번, 100~300ms 안에 몰렸다. 두 사례의 흐름은 이렇다.

- **x60-2**(송신 시작 뒤 약 28.4초):
  1. UDP queue가 100ms 단위로 7 → 1,039 → 7,202 → 8,170으로 찼다. 그다음 표본에서 reject 9,507이 나왔다.
  2. XLog writer queue `Len`은 0~196이었다. writer 역압이 아니라 ingest(worker) 진행이 멈춘 것이다.
- **x60-4**(약 19초): queue가 4,534 → 8,073이 된 뒤 두 표본에 걸쳐 reject 1,827, 2,234가 나왔다.

trace 관찰(포화 직전 약 200~600ms):

- **두 사례 공통: tag drain이 길게 연속 실행됐다.**
  - 경로: `FlushTagCountsIfDue` → `tagDrain` → `tagApplyLocked`
  - x60-4: 약 290ms 동안 거의 쉬지 않았다(10~41ms 조각 연속).
  - x60-2: 45ms 연속 실행 등이 있었다.
- **x60-2: visitor writer의 HLL `densify`가 몰렸다.**
  - sparse → dense 전환이다. 약 190ms 중 약 150ms를 11~34ms 조각으로 연속 실행했다.
  - object 200개의 HLL이 비슷한 시점에 sparse 상한(8192 word)을 넘기 때문으로 추정한다.
- **UDP worker가 실행 대기에 묶였다.**
  - x60-4에는 worker 2개(기본 `DefaultUDPWorkers=2`)가 **Runnable인데 실행되지 못한** 구간이 있었다. 길이는 44.8ms였고, 그동안 worker 실행은 0이었다. 실제로 실행된 것은 tag drain과 reader syscall 정도였다.
  - x60-2에서는 worker가 선점된 뒤 19.6ms 대기했고, reader도 Runnable로 28.7ms 기다렸다.
- **nonblocking syscall이 비정상적으로 길었다.**
  - UDP reader의 `recvmsg`가 10~27ms 걸렸다(x60-4).
  - XLog writer의 `pwrite`(`appendDataBatch`·`PutBatch`)가 11~15ms(최대 54.9ms, x60-2) 걸렸다.
  - x60-2에서는 주기 동기화의 fsync가 같은 구간에 몰렸다. `syncKeys`·`writePages`·`closeQueue.syncFile`이 6~29ms, `.go-checkpoint` `replaceFile` fsync가 228ms였다.
  - nonblocking `recvmsg`가 수십 ms 걸린 것은 thread가 OS에서 실행되지 못했음을 뜻한다(추정).
- **GC는 원인이 아니다.**
  - STW는 0.16~0.34ms다.
  - mark assist는 최대 8~9ms(x60-4, 포화 뒤 구간)였다.
- **engine lock**:
  - x60-2 후반에 worker 하나가 engine lock을 기다렸다(`syncx.(*Mutex).LockContext`).
  - 같은 시각에 lock을 가진 worker가 실행 중이었다. 장시간 잡고 I/O를 기다린 holder는 보이지 않았다.
- **trace 해석 함정**:
  - trace 끝의 약 220ms(`pprof.Trace` 정지 처리 구간)에는 상태 전이가 기록되지 않았다. 그래서 그 구간은 "계속 Running"처럼 보인다. 해석에서 뺐다.
  - x60-2의 reject는 trace 정지 직후에 나왔다. trace 정지 비용이 기여했을 가능성도 배제하지 못했다.

### 8.3 해석과 다음 측정

- **확정하지 못했다.** 관찰된 것과 맞는 가설은 이렇다.
  1. 순간적으로 CPU 수요가 몰린다. tag drain의 연속 apply, visitor densify 몰림, writer `pwrite`·fsync, GC mark가 겹치고, 같은 VM의 부하 생성기와 호스트 쪽 경합도 더해진다.
  2. 그러면 UDP worker 2개가 100~200ms 동안 거의 진행하지 못한다.
  3. 60,000/s에서 UDP queue 8192개는 약 0.14초 분량이라 넘친다.
- 단일 lock이 막은 증거는 없다. lock 점유도 포화가 아니었다(§2).
- **원인 분리 측정(제안)**:
  - 같은 60,000/s를 10회 이상, `scripts/bench-isolated.sh`의 taskset으로 서버와 생성기 CPU를 분리하고 Mac 호스트가 조용할 때 잰다.
  - conf로 다음 변형을 각각 같은 횟수 잰다. 각 변형에서 reject가 난 실행의 비율을 비교한다.
    - `tagcnt_enabled=false`(tag drain 제거)
    - `visitor_hourly_count_enabled=false`(visitor hourly 제거)
    - `--sync-interval` 연장(주기 fsync 제거)
- **개선 후보**(원인 확정 뒤 선택):
  - tag drain을 짧게 끊어 CPU를 양보한다(hold 사이 `runtime.Gosched`, 또는 drain 속도 상한).
  - visitor densify 시점을 분산하거나, 미리 dense 크기로 할당한다.
  - UDP queue 상한을 datagram 개수 대신 byte로 바꿔 여유 시간을 늘린다. 이 경우 메모리 상한을 바꾸게 된다.

## 9. 6단계 A/B (2026-10-23, `32d87da` vs `da03b8d`)

조건은 VM, `scripts/bench-ab.sh`, replay는 양쪽 모두 `da03b8d` 빌드, §1의 replay 조건이다. 모든 회차가 통과했다.

| 시나리오 | 쌍 | 쌍 차이 중앙값 | 평균 / SD | new가 적은 쌍 | CPU/1k 중앙값 base → new | RSS 중앙값 |
|---|---|---|---|---|---|---|
| mix 15,000/s | 10 | −0.35% | +0.13% / 2.97% | 5/10 | 0.03348 → 0.03330 | 234 → 231MiB |
| XLog 30,000/s | 8 | −0.03% | +0.62% / 3.23% | 4/8 | 0.01951 → 0.01969 | 263 → 269MiB |

판단: 6단계(조회 경로 gate 밖 처리, counter 이름 저장, R7, 상한 키)의 수집 CPU 변화는 측정 해상도(약 ±4%) 안이다.

## 10. 7라운드 A/B와 visitor 규모 (2026-10-24, `da03b8d` vs `d07fded`)

조건은 VM이고, Mac 호스트에서는 다른 작업이 돌지 않았다. replay는 양쪽 모두 `d07fded` 빌드이고, 모든 회차가 통과했다.

### 10.1 정상 규모 쌍 비교 (`scripts/bench-ab.sh`, §1의 replay 조건)

| 시나리오 | 쌍 | 쌍 차이 중앙값 | 평균 / SD | new가 적은 쌍 | CPU/1k 중앙값 base → new | RSS 최대 중앙값 |
|---|---|---|---|---|---|---|
| mix 15,000/s | 10 | **−8.45%** | −8.40% / 2.21% | 10/10 | 0.03336 → 0.03026 | 235 → 182MiB |
| XLog 30,000/s | 8 | **−6.60%** | −5.91% / 1.90% | 8/8 | 0.01937 → 0.01813 | 270 → 212MiB |

- 쌍별 차이:
  - mix: −3.70, −11.81, −10.09, −9.06, −8.12, −7.41, −7.20, −8.77, −7.57, −10.31%
  - XLog: −7.02, −2.99, −6.19, −3.80, −8.01, −4.49, −7.64, −7.14%
- 크기는 §5.2의 상한 A/B(gxid 집합 상한 축소: XLog −6.2%, mix −9.6%, RSS 약 −100MiB)와 같은 수준이다. 처리된 gxid 집합을 pointer 없는 구조로 바꾼 것이 주된 효과로 본다. counter cache 변경의 몫은 분리하지 않았다.

### 10.2 visitor 규모 (object 4000, XLog 5,000/s, `--profile-ratio 0`, 사용자 ID 있음, 번갈아 3회씩)

| 빌드 | CPU/1k XLog | RSS 최대 | visitor writer(Enqueued / Written / Dropped / Pending) | `/health` visitors |
|---|---|---|---|---|
| base `da03b8d` | 0.2797, 0.2853, 0.2809 | 189~194MiB | 54,361~56,749 / 16,510~18,570 / **118,287~120,675** / 약 38,000 | (필드 없음) |
| new `d07fded` | 0.0948, 0.0924, 0.0943 | 168~173MiB | 175,036 / 175,036 / **0** / 0 | Entries 8,001, Resident 약 7.2MB / 상한 256MiB, Delta 0 / 64MiB, WriteThroughFailures 0 |

- 결과:
  - CPU/1k는 약 66% 줄었다(0.28 → 0.094).
  - visitor 기록은 모두 저장됐다(Dropped 0).
  - §4.3의 thrash가 해소됐다.
- 측정 범위: 이 시나리오의 visitor 집계 정확도(저장된 cardinality와 원본의 비교)는 여기서 재지 않았다.

## 11. UDP 일시 포화: CPU 분리 조건별 실험 (2026-10-24, `d07fded`)

### 11.1 방법

- **CPU 분리**: 서버는 `taskset -c 0-2`로 묶었다(Go가 affinity를 읽어 GOMAXPROCS 3). replay, `/health` 감시, trace 수집은 `taskset -c 3`으로 묶었다.
  - `scripts/bench-isolated.sh`는 최대 rate 탐색용이라 쓰지 않았다. 같은 taskset 분리를 고정 rate 반복에 직접 적용했다.
- **부하**: XLog 전용 60,000/s, §1의 replay 조건과 같다.
- **조건**: 4가지를 한 회차씩 번갈아 10회 돌렸다(총 40회).
  - 기본
  - `tagcnt_enabled=false`
  - `visitor_hourly_count_enabled=false`
  - `--sync-interval=10m`
- **수집**: 모든 회차에서 §8과 같이 수집했다. 100ms `/health` 감시와 2초 trace를 연속으로 받고, reject 구간의 trace만 남겼다.

### 11.2 결과

| 조건 | reject 실행 | reject 수 | CPU/1k | RSS 최대 | UDP queue 최대(1초 표본) |
|---|---|---|---|---|---|
| 기본 | 1/10(base-7) | 61 | 0.0138~0.0146 | 659~707MiB | 609 |
| `tagcnt_enabled=false` | 0/10 | 0 | 0.0132~0.0142 | 628~674MiB | 1,097 |
| `visitor_hourly_count_enabled=false` | 1/10(hroff-9) | 2,584 | 0.0131~0.0141 | 418~437MiB | 2,631 |
| `--sync-interval=10m` | 1/10(sync-4) | 18 | 0.0138~0.0151 | 656~710MiB | 3,754 |

- **빈도**: 40회 중 3회였다. CPU 분리 전(§8, `da03b8d`)에는 trace를 받은 60k 실행 5회 중 2회였다(reject 9,507·4,061). 빈도와 크기 모두 크게 줄었지만 **0은 아니다**.
- **조건 간 차이**: 조건마다 0~1회라 통계적으로 구분되지 않는다. 이 실험으로는 tag count, hourly visitor, 주기 fsync 중 어느 것도 단독 원인으로 지목할 수 없다.
- **reject 3회의 trace**:
  - **sync-4(18건)**:
    - 2초 trace가 끝나고 다음 trace가 시작된 직후(약 40ms)에 났다. 직전 trace의 정지 처리가 2.44초 걸렸다.
    - trace 수집 자체가 유발한 것으로 본다.
  - **base-7(61건)**:
    - trace 시작 뒤 약 280ms에 났다.
    - worker가 실행되지 않은 구간은 10~14ms였다.
    - UDP reader의 nonblocking `recvmsg`가 12~21ms 걸렸다.
    - densify는 없었고, tag drain이 실행 중이었다.
  - **hroff-9(2,584건)**:
    - trace 한가운데였다(송신 시작 후 31.3초).
    - worker 2개가 선점된 채 29~42ms 실행되지 못했다.
    - 그동안 서버 CPU 3개를 다음이 썼다.
      - XLog index `pwrite` 27.8ms
      - tag 파일 `pwrite` 19.3ms
      - trace advancer 17.1ms
      - GC mark worker
      - visitor `setWord` 25.8ms
    - 이어서 GC mark termination STW가 15.68ms 이어졌다. 평소는 0.2~0.4ms다.
- **visitor `densify`**: 세 reject 구간 모두에서 관찰되지 않았다. §8의 x60-2에서는 약 150ms를 차지했다. 7라운드 visitor partial entry로 수집 경로의 densify 몰림은 사라진 것으로 본다.

### 11.3 판단과 남은 확인

- **이전 실패의 주원인**: §2·§8에서 60k·70k가 실패한 주원인은 두 가지가 겹친 것으로 판단한다.
  - 같은 VM의 부하 생성기·호스트와의 CPU 경합
  - visitor densify·tag drain·GC가 순간적으로 몰려 CPU를 많이 쓴 것
- CPU를 분리하고 7라운드를 적용하면 40회 중 3회, 소량으로 줄었다.
- **남은 잔여 원인**: 서버 CPU 안에서 짧은 선점이 일어나 queue 여유 약 0.14초를 넘는 경우다. 원인 후보는 이렇다.
  - writer `pwrite`
  - GC mark termination의 긴 STW(1회 15.7ms)
  - trace 수집 자체(2회는 trace 경계에서 났다)
- **남은 확인 사항**:
  1. trace 없이 같은 조건 20회 이상 반복한다. trace가 유발한 몫을 빼고 실제 빈도를 잰다.
  2. GC mark termination STW가 15ms까지 길어지는 조건을 확인한다. 우선 GC 직전에 thread가 syscall이나 OS 대기에 있는지 본다.
  3. UDP queue 상한을 datagram 수 대신 byte 기준(예: 여유 1초)으로 바꾸는 효과와 메모리 비용을 평가한다. 상한 변경이므로 별도 결정이 필요하다.
  4. 같은 측정을 core가 더 많은 격리 호스트에서 반복한다. VM 4 vCPU에서는 서버 3 CPU와 생성기 1 CPU로 나누는 것이 한계다.

## 12. trace 없는 반복과 GC STW 분포 (2026-10-24, `1796ead`)

### 12.1 방법

- §11과 같은 CPU 분리(서버 `taskset -c 0-2`, 생성기·감시 `taskset -c 3`)와 같은 부하(XLog 전용 60,000/s, 30초, warmup 5초)를 썼다.
- runtime trace를 받지 않았다. `/health` 100ms 감시만 했다.
- 서버에 `GODEBUG=gctrace=1`을 주어 GC마다 STW 두 구간(sweep termination, mark termination)의 wall time을 기록했다.
- 24회를 연속으로 돌렸다.

### 12.2 결과

| 항목 | 값 |
|---|---|
| UDP reject가 생긴 실행 | **0/24** |
| 저장 확인(재기동 뒤 전체 조회) | 24회 모두 손실·중복 0 |
| CPU/1k | 0.0126~0.0141 |
| RSS 최대 | 658~678MiB |
| GC 횟수 / STW 구간 | 1,262회 / 2,524구간 |
| STW p50 / p99 / p99.9 / 최대 | 0.079 / 0.46 / 2.9 / **3.0ms** |
| STW 5ms 초과 | 0 |

- 모든 실행의 replay 판정이 `pass=false`였다. gctrace 출력이 서버 stderr의 경고 줄로 집계됐기 때문이다(실행마다 약 450줄). 저장 확인과 재기동은 모두 정상이었다.

### 12.3 판단

- **§11의 reject 원인**: §11 reject 3회 가운데 2회는 trace 경계에서 났다. 15.7ms STW는 trace 수집 중이었다. trace를 빼면 같은 조건 24회에서 reject가 0이고, STW도 3ms를 넘지 않았다. 따라서 §11의 잔여 reject와 긴 STW는 주로 trace 수집이 만든 것으로 판단한다.
- **byte 기준 UDP queue 상한(§11.3 3번)**: 이 조건에서는 근거가 없어 보류한다. queue 여유(약 0.14초)는 3ms STW보다 충분히 크다.
- **남은 확인**: core가 더 많은 격리 호스트에서 반복(§11.3 4번)과 실제 운영 burst 형태는 이 실험으로 대신할 수 없다.

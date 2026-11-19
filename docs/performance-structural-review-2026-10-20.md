# 구조적 성능 개선 검토 (2026-10-20)

미세 최적화가 측정 해상도 아래로 내려온 뒤(`docs/validation/2026-10-17-xlog-max-benchmark.md` §17), 더 큰 개선 후보를 고르기 위해 검토했다. 조사 범위는 네 가지다.

- 원본 Java tag count 계약
- Go Store gate 구조
- 외부 기법과 참고 구현(`scouter-server-go@8a86959`)
- 측정 방법

수치는 Linux VM(Lima, Ubuntu 24.04 arm64, vCPU 4, 메모리 4GiB, loopback, `net.core.rmem_max` 8MiB)에서 쟀다. 효과 추정은 측정 전까지 추측으로 표시한다.

## 1. 결론

1. **먼저 benchmark를 바로잡는다.**
   - 표준 replay(`--text-hit-ratio 0.95`)는 text 참조의 5%마다 새 hash를 만든다. 그래서 service, SQL, error 종류가 끝없이 늘어난다.
   - 그 결과 tag 집계와 text writer 비용이 실제보다 크게 잡혔다. tag가 서버 CPU의 39%로 보인 것은 이 workload 때문이다.
   - 실제 Agent 부하에서는 tag key가 새로 생기는 속도가 이 replay 조건보다 훨씬 느리다.
2. **현실적인 cardinality(`--text-hit-ratio 0.999`)에서 본 분포**: XLog 전용 30,000/s 기준이다.
   - 큰 순서는 ingest 경로(35%: XLog 저장 21%, 그중 visitor 7.5%, tag 기여 7%), XLog·profile writer 14%, tag 15%, GC 8%, UDP 수신 6%다.
   - 같은 rate에서 XLog당 CPU가 약 24% 낮다.
3. **측정 해상도가 부족하다.**
   - 같은 빌드끼리 잰 회차 간 변동은 10~13%(CV)다. Apple Silicon의 P/E core 혼용이 원인으로 추정된다.
   - VM에는 하드웨어 counter가 없어 명령어 수로 잴 수 없다.
   - 대신 번갈아 잰 쌍의 차이는 SD 약 2%로 안정적이다. 판단은 **8쌍 이상, 쌍 차이의 중앙값**으로 한다.
4. **"Store gate 사용률 약 90%"는 근거가 없다.**
   - 저장소 문서와 계측 코드 어디에도 출처가 없다. gate 분리는 holder별 점유 계측으로 다시 확인한 뒤에 정한다.
5. **권고 순서**:
   - (0) 측정 기반 정비: 현실적 workload 옵션, 쌍 비교 스크립트, gate·engine lock 점유 계측
   - (1) 현실 workload로 효율과 최대치를 다시 잰다
   - (2) 계측 결과에 따라 Store gate 단계적 분리(text → xlog), 또는 ingest 임계 구역 축소(visitor·tag 기여를 engine lock 밖으로)
   - tag 주기 연장, mmap, io_uring은 근거가 약하거나 위험이 커서 보류한다

## 2. 측정 전제의 교정

### 2.1 replay workload의 cardinality

- `cmd/scouter-replay/generator.go`의 `text()`는 hit가 아니면 항상 새 문자열과 새 hash를 만든다(`pool.created++`). 기본 hit 비율은 0.95다.
  - 30,000 XLog/s라면 초당 약 1,500개의 새 service가 생기고, 각각 tag key 약 7개(service, *-sum 등)를 새로 만든다.
- key 반복률(5초 pass를 묶은 창 안의 distinct key ÷ delta, 기본 workload, 90초 실행):

| 창 | XLog 전용 30,000/s | mix 15,000/s |
|---|---|---|
| 5초(현재 주기) | 100% (pass당 delta 약 128,000) | 100% (약 106,000) |
| 10초 | 74% | 67% |
| 30초 | 53% | 39% |
| 60초 | 45% (distinct 약 70만) | 30% |

- 실제 Agent 환경: 날짜별 tag 파일 크기를 기록 크기(Next 약 140B, First 약 5.8KB)로 나눠 distinct key 수를 추정했다. 하루치가 이 replay의 짧은 구간보다 훨씬 적었다. 즉 기본 workload의 cardinality는 실제보다 크게 높다.
- 현실적 cardinality(`--text-hit-ratio 0.999`, 30초 프로파일 창의 CPU 표본):

| 항목 | XLog 전용 30,000/s | mix 15,000/s |
|---|---|---|
| 전체 CPU 표본 | 16.7초 (기본 workload 22.0초) | 16.7초 (21.9초) |
| ingest(UDP worker, engine lock) | 35% | 24% |
| └ XLog 저장(`storeXLog`) | 21% (visitor 7.5%, tag 기여 7.1%, encode 3.3%, writer 투입 2.3%) | 13% |
| XLog·profile writer | 14% | 18% |
| tag(반영 + flush) | 15% | 7% |
| GC | 8% | 13% |
| UDP 수신 | 6% | 9% |
| Client 조회(replay의 병행 조회) | 7% | 7% |
| syscall 합계 | 18% | 29% |

- **조치**:
  - replay에 cardinality를 정하는 옵션을 추가한다. 예: service·SQL·error 종류 수의 상한과 분포(Zipf), text hit 비율을 따로 지정.
  - 표준 시나리오는 그 옵션으로 "현실적 mix"와 "cardinality 폭주"(지금의 기본값, 방어 성능용)를 나눠 잰다.
  - 지금까지의 §16·§17 수치는 폭주 쪽 조건이다.

### 2.2 측정 잡음과 판단 방법

- A/A(같은 빌드, XLog 전용 30,000/s, 6회): CPU 고정 없이 CV 13.0%였다. 서버를 vCPU 0–1에, replay를 2–3에 고정하면 CV 10.6%였지만, 서버가 vCPU 2개로 부족해 6회 중 3회 실패했다. 고정은 효과가 작다.
- VM에는 PMU가 노출되지 않는다(`perf stat -e instructions` 불가). 그래서 명령어 수 지표는 쓸 수 없다.
- 번갈아 잰 쌍의 차이는 안정적이다. §17.4의 7쌍에서 이상치 1개를 빼면 쌍 차이의 SD가 약 2%였다.
- **판단 규칙**: 기준과 변경을 번갈아 8쌍 이상 잰다. 쌍 차이의 중앙값과 부호 일치 수로 판단한다. 단일 회차 프로파일끼리의 비교로는 판단하지 않는다(§17.4에서 9% 차이가 잡음이었다).
- **조치**: 쌍 차이와 요약 통계를 내는 비교 스크립트를 `scripts/`에 둔다(§8의 `scripts/bench-ab.sh`). 조건은 VM, rmem 설정, cardinality 옵션을 기록한다.

## 3. 원본 Java tag count 계약 (요약)

근거는 원본 commit `dee47fb5`다.

- **파이프라인**: XLogTagCount(queue 10000, 차면 S188 drop) → FirstTagCountDB(queue 50000, 분 단위 메모리 누적, 5초 flush, 막히면 S183) → MoveToNextCollector(queue 30000, (key, hour)별 60분 배열, **5초 또는 1000 key**마다 넘김) → NextTagCountDB(queue 2000 map, 즉시 RMW, S185) → Top100FileCache(5분).
  - 출처: `S/tagcnt/*`, `S/core/CoreRun.scala:7`.
- **쓰기**:
  - First: (key, 분)마다 chain을 두 번 탐색한 뒤 4B RMW한다.
  - Next: (key, hour)마다 0보다 큰 분만 4B RMW하고, head의 total을 RMW한다.
  - hfile은 메모리(`MemHashBlock`)에 두고 4초마다 통째로 저장한다. fsync는 없다.
- **조회 시점**: 파일만 읽는다. series 경로는 약 5초 뒤에 보이고, top100 cache 경로는 최대 5분 뒤에 보인다. 아직 쓰지 않은 First value는 tagmap에서 오므로 count 0으로 즉시 보인다.
- **Go와의 관계**: Go는 이미 원본 수준(5초)이다. tick 기반 flush, Close 때 전체 flush, rebuild 전 flush로 원본 결함 후보 일부를 해소했다. 그래서 "원본만큼 주기를 늘리기"는 해당하지 않는다.
- **원본 결함 후보**(정상 계약과 분리):
  - D1: item이 도착할 때만 flush하므로 idle tail이 반영되지 않고, 종료하면 잃는다.
  - D2: `Top100FileCache.logSet`을 동기화 없이 공유한다.
  - D3: rebuild 전에 flush하지 않는다.
  - D4: `TextPermWR`는 queue가 차면 queue 전체를 비운다.
- **CONTRACT §8에 보탤 사실**:
  - FirstTagCountDB queue 50000과 S183
  - NextTagCountDB queue 2000과 S185
  - 도착할 때만 flush하는 동작(D1)
  - rebuild 전 flush 없음(D3)
  - First 쓰기가 분 단위라는 점
- **변경 여지 판정**:

| 후보 | 판정 |
|---|---|
| 조회 시 pending 합산 | 허용(원본보다 최신인 의도된 차이). top100 cache 경로에는 섞지 않는다 |
| flush 주기를 30~60초로 | 단독이면 위험(보이는 시점이 원본보다 늦다). pending 합산과 함께면 허용. 단 process crash 손실 창과 메모리가 커진다 |
| hour 단위로 보류했다가 쓰기 | 불가 또는 위험 |
| 새 key를 모아 한 번에 append | 허용(byte 동등하게 만들 수 있다) |
| key→기록 위치 메모리 index | 허용(원본도 hfile을 메모리에 둔다) |

## 4. Store gate 구조 (요약)

- **Store.mu가 보호하는 것**: 공유 handle cache 하나(`files` 16, `indexes` 32, LRU, `ends`, `appendBuf`, `closes`)와 `modes`·`zip`(압축), `closed`, 통계다. `legacyindex.Index`에는 자체 lock이 있다.
- **이미 분리된 선례**: tag(`tagState`: 자체 gate, handle 64, LRU), counter(`Counters.mu`), visitor(`visitorMu`/`visitorIO`).
- **분리할 때 함께 바꿀 것**:
  - (위험 높음) handle cache는 domain마다 따로 둔다. 공유 cache에 leaf lock만 거는 방식은 eviction fsync와 close가 다른 domain의 쓰기와 겹칠 수 있다.
  - (높음) purge와 DB 삭제는 대상과 겹치는 모든 domain gate를 고정 순서로 잡고 rename까지 쥔다.
  - (중간~높음) SyncData와 Close는 domain별 snapshot과 drain으로 한다.
  - (높음) xlog와 profile은 압축 저장소(`zip`, `modes`)를 공유하므로 한 domain이어야 한다.
  - (중간) lock 순서 규칙을 갱신한다.
- **설계안**:

| 안 | 내용 | 변경 범위(추정) | 위험 |
|---|---|---|---|
| A | text domain만 분리 | 파일 7~9개, 함수 25~35개 | 중간 |
| B | text / xlog(+profile, zip, modes, packDiscards) / misc 3 domain | 파일 13~16개, 함수 55~75개 | 중간~높음 |
| C | 날짜×종류 gate | B보다 큼 | 높음, 이득 작음 |
| D | writer 전용 handle(Java XLogWR 방식) | 파일 15개 이상 | 높음 |

- **권고**: B를 목표로 단계적으로 간다.
  - (0) holder별 gate 점유와 대기를 계측한다.
  - (1) 동작 변화 없는 `handleCache` 타입 추출
  - (2) A
  - (3) 측정에서 경합이 남으면 xlog domain
- **기대 효과는 미확인이다.** §15.1의 측정(engine lock 대기 13.8초 중 Store gate 대기 3.2초)은 writer를 분리하기 전의 것이다. 효과는 mix 시나리오와 디스크 정지 격리(object, alert, summary 저장이 writer hold 뒤에서 기다리지 않게 하기, CONTRACT:280)에 집중될 것으로 본다.

## 5. 외부 기법과 참고 구현 (요약)

| 기법 | 판단 | 근거 |
|---|---|---|
| 참고 구현의 tag count | 형식이 다르다(날짜·tag별 JSON을 5분마다 통째로 씀). 가져올 것은 "메모리에서 길게 모아 드물게 쓴다"는 생각뿐 | `scouter-server-go` `internal/tagcnt/core.go`, `store.go` |
| 참고 구현의 XLog·profile·text | 우리보다 앞선 점이 없다. 무통보 drop, Java 비호환 zstd profile이 기본값, hfile을 truncate한 뒤 통째로 쓰는 위험이 있다 | `internal/db/*` |
| io_uring | 제외. Docker 기본 seccomp가 차단하고, Go 라이브러리는 정체됐거나 beta다 | docs.docker.com seccomp, moby#47532 |
| pwritev/preadv | `x/sys/unix`에 있지만 연속 범위만 쓸 수 있다. 단독 효과는 작다 | pkg.go.dev |
| tag 파일 mmap(MAP_SHARED) | 효과는 크지만 위험이 중간이다(truncate·확장 시 SIGBUS, remap, RSS 집계). 현실 workload에서 tag 몫이 7~15%라 보류 | mmap(2), `runtime/debug.SetPanicOnFault` |
| tag 사용자 공간 write-back | pending delta가 이미 그 역할이다. 주기 연장(§3)과 같은 문제다 | — |
| GC | Green Tea는 Go 1.26부터 기본이고, Go 1.27의 작은 할당 개선도 기본이다. arena는 쓸 수 없다. `GOGC=off`+`GOMEMLIMIT`은 공유 호스트에서 쓰지 말라고 공식 가이드가 권한다. 주 레버는 할당량 감소다(§17.3이 그 예) | go.dev/doc/go1.26, gc-guide |
| UDP | SO_REUSEPORT, GRO, busy poll은 효율에 이득이 없다. 운영에는 `rmem_max` 8MiB 이상과 `Udp RcvbufErrors` 감시를 권한다 | udp(7), §17.2 |

## 6. 후보 평가와 진행안

| # | 후보 | 기대 효과 | 위험 | 선행 조건 | 판단 |
|---|---|---|---|---|---|
| 0 | 측정 기반 정비: replay cardinality 옵션, 쌍 비교 스크립트, holder별 gate·engine lock 점유 계측 | 이후 모든 판단의 해상도와 대표성 | 낮음 | — | **먼저** |
| 1 | 현실 workload로 효율과 최대치 재측정(VM, 8쌍) | 우선순위 확정 | 낮음 | 0 | **다음** |
| 2 | Store gate 단계적 분리(handleCache 추출 → text → xlog) | mix 상한과 디스크 정지 격리. 효율 이득은 작을 수 있다 | 중간~높음 | 0의 점유 계측에서 경합 확인 | 조건부 |
| 3 | ingest 임계 구역 축소: visitor HLL offer와 tag 기여 생성을 engine lock 밖의 전용 goroutine으로(원본도 tag는 XLogTagCount 비동기 queue) | engine lock 점유 약 1/2 감소(현실 workload에서 저장 21% 중 visitor·tag 14.6%). CPU 효율보다 core가 많을 때의 상한에 이득 | 중간(포화 시 drop 정책, 순서) | 0, 1에서 engine lock이 상한임을 확인 | 조건부 |
| 4 | tag: key 위치 메모리 index, 새 key 묶음 append, (조회 합산과 함께) 주기 연장 | 현실 workload에서 tag 몫 7~15% 중 일부 | 중간 | cardinality 폭주 방어가 목표일 때 | 보류 |
| 5 | 할당량 감소 지속(alloc_space 상위) | 각 1~3% | 낮음 | 8쌍 판단 | 수시 |
| 6 | mmap, io_uring, GOGC 조정, recvmmsg | — | 중간~높음 | — | 제외·보류 |

## 7. 미확인 사항

- 현실 workload에서의 최대 지속 처리량과 그 상한 원인. 1단계에서 잰다.
- Store gate·engine lock의 holder별 점유. 계측 코드가 아직 없다.
- 실제 운영의 key와 service cardinality 분포. 실제 Agent 환경의 파일 크기로만 추정했다.
- Go 1.27에서 `nogreenteagc` opt-out이 아직 있는지(GC 몫 대조용).
- Scouter Client가 조회 합산으로 응답되는 pending-only value를 어떻게 표시하는지(§3 후보를 진행할 때 확인).

## 8. 1단계 결과: 측정 기반 정비와 재측정 (2026-10-20)

- **도구**(`docs/performance.md` "비교 측정 도구"):
  - `scouter-replay --text-cardinality`
  - `scripts/bench-ab.sh`(쌍 비교)
  - `scouter-go --lock-stats`(`/health` `locks`)
  - replay `--sample-health`가 `locks`도 기록한다.
- **조건**: Linux VM(§0 조건). `--text-cardinality 2000`, object 200, sender 2, batch 1, 조회 병행 2.

### 8.1 측정 방법의 해상도

- **A/A**(같은 binary 두 개, mix 15,000/s, 8쌍): 쌍 차이 중앙값 +2.7%, SD 4.8%였다. 8쌍 중 6쌍이 같은 쪽으로 기울었다.
- **도구 commit의 A/B**(추적은 꺼 둠):
  - 첫 회는 +3.8%(SD 6.8%, 7/8쌍 증가)였다.
  - 다시 재니 −0.6%(SD 4.3%, 5/8쌍 감소)였다.
  - 첫 회의 차이는 A/A 범위 안이다. 꺼진 추적의 비용(lock마다 atomic load 1회)은 측정되지 않는다.
- **판단 규칙 보정**:
  - 8쌍으로 구분할 수 있는 효과는 약 ±4%보다 큰 것이다.
  - 그보다 작은 효과는 20~30쌍이 필요하거나, 근거로 삼지 않는다.
  - 결론을 내기 전에 같은 A/B를 한 번 더 재서 방향이 유지되는지 본다.

### 8.2 lock 점유 (held = 구간 중 잡혀 있던 시간 비율, waited = 기다린 시간 합 ÷ 구간)

| 시나리오 | 통과 | engine lock held / waited | Store gate held (XLog / profile / text writer) | tag gate held |
|---|---|---|---|---|
| mix 15,000/s | 예 | 8.9% / 5.6% | 10.8% (4.3 / 5.6 / 0.2) | 7.3% |
| mix 25,000/s | 예 | 13.0% / 8.7% | 14.4% (5.4 / 8.0 / 0.2) | 10.4% |
| mix 35,000/s | 예 | 20.4% / 14.6% | 20.1% (7.5 / 11.3 / 0.1) | 17.2% |
| XLog 전용 30,000/s | 예 | 14.3% / 11.5% | 7.8% (6.6 / 0 / 0.1) | 11.3% |
| XLog 전용 60,000/s | 예 | 24.8% / 22.5% | 11.4% (9.6 / 0 / 0) | 19.9% |
| XLog 전용 90,000/s | 아니오(0.8% 손실) | 39.0% / 38.9% | 18.5% (15.7 / 0 / 0.1) | 34.5% |

- **포화에 가까운 lock이 없다.** 현실 cardinality에서는 Store gate가 mix 35,000/s에서도 20%다. "약 90%"는 재현되지 않았다.
- 90,000/s의 손실은 VM의 CPU 포화 때문이다(replay와 서버가 vCPU 4개를 나눠 쓴다. §17.2와 같은 양상). lock이 원인이 아니다.
- **결론**: 이 조건에서 처리량의 상한은 CPU다.
  - Store gate 분리(§4)와 ingest 임계 구역 축소(§6 #3)는 지금 근거가 없어 보류한다. core가 많은 호스트에서 다시 확인한다.
  - 우선순위는 XLog당 CPU(§2.1 분포: ingest 35%, writer 14%, tag 15%, GC 8%)다.
- XLog당 CPU는 rate가 오를수록 낮아진다. mix 15,000/s에서 0.037, 35,000/s에서 0.028이다(1,000건당 초). 고정 비용(주기 작업, 조회 병행)이 분산되기 때문이다. 비교는 같은 rate끼리만 한다.

## 9. 2단계: 현실 workload에서 XLog당 CPU (2026-10-20)

- **프로파일**(현재 master, XLog 전용 30,000/s, `--text-cardinality 2000`, 30초 창 16.9초): self time 상위는 다음과 같다.
  - syscall 15%
  - GC mark 약 9%
  - tag key의 `crc32String` 3.5%
  - `time.now` 3.4%: tag hold의 단계별 시계 2/3, UDP worker의 datagram 시계, ingest 지연 통계
  - `Limits.normalized` 1.7%: `Encoder.Reset`마다 다시 계산
  - visitor HLL offer는 4.5%다. 대부분 register word의 map·배열 접근이고, hash가 아니다.
- **시도**: 세 가지를 한 묶음으로 넣었다.
  - tag key를 고정 이름표에서 미리 계산했다(crc32 제거).
  - `Encoder.Reset`에서 이미 정규화된 limits는 다시 계산하지 않게 했다.
  - tag 반영 단계의 hold 시계를 8단계마다 한 번만 읽게 했다.
- **결과**:
  - 8쌍 A/B: XLog 전용 30,000/s에서 −0.2%(SD 2.2%, 7쌍), mix 15,000/s에서 +2.8%(SD 4.0%). 둘 다 효과가 없다.
  - 프로파일에서 `crc32String`과 `normalized`는 사라졌다. 그러나 `tagAddLocked`가 1.69초에서 2.27초로 늘어 전체는 1% 줄었을 뿐이다.
  - tag 반영은 map 조회·갱신이 이어지는 구간이고, 시간 대부분이 메모리 지연이다. 계산을 빼도 다음 메모리 접근을 기다리는 시간이 드러날 뿐 CPU 시간은 줄지 않는다.
  - 그래서 넣지 않았다.
- **판단**:
  - 현실 workload에서 XLog당 CPU를 줄이는 미세 변경은 측정 해상도(8쌍 약 ±4%, 이 시나리오에서는 SD 2.2%라 약 ±2%) 아래로 내려왔다.
  - 남은 큰 몫은 세 가지다.
    - tag 반영(15%): map 중심의 메모리 지연. 줄이려면 자료 구조를 바꿔야 한다.
    - GC(9%)
    - syscall(15%): 레코드는 이미 hold 단위로 묶어 쓴다.
  - §8.2에서 lock 점유가 낮았다. 따라서 처리량은 core 수에 비례해 늘 가능성이 크다(추정). 다음 확인 대상은 core 수에 따른 확장성이다.

## 10. GOMAXPROCS: 같은 일에 쓰는 CPU (2026-10-20)

- **확장성**(XLog 전용, `--text-cardinality 2000`, 손실 없이 지속한 rate, 각 1회):
  - `GOMAXPROCS=1`: 50,000/s까지 통과
  - `GOMAXPROCS=2`: 80,000/s까지 통과
  - 같은 40,000/s에서 서버 CPU는 1일 때 15.9초, 2일 때 20.9초였다.
  - §8.2처럼 lock 점유가 낮으므로, 처리량 상한은 core 수를 따라간다.
- **쌍 비교**(8쌍, 완료 XLog 1,000건당 서버 CPU, 기준은 VM의 기본값인 vCPU 4개):

| 설정 | XLog 전용 30,000/s | mix 15,000/s |
|---|---|---|
| `GOMAXPROCS=2` | −11.6% (SD 1.5%, 8/8쌍) | −8.9% (SD 2.1%, 8/8쌍) |
| `GOMAXPROCS=1` | −31.1% (SD 0.9%, 7/7쌍) | — |

- **RSS**: 같다.
- **지연**: ingest p50·p99는 같거나 낮다(p99 19~39µs). Client 조회 p99는 36~39ms에서 67~107ms로 늘었다. P가 적어 조회가 수집·tag 작업과 CPU를 나누기 때문이다.
- **해석**(추정):
  - P가 많으면 Go runtime이 빈 CPU를 GC mark worker(idle 모드)와 scheduler spinning에 쓴다. 처리량은 같고, 그 CPU는 다른 서비스와 공유하는 호스트에서 실제로 빼앗는 자원이다.
  - mix 15,000/s 프로파일에서 `gcDrainMarkWorkerIdle`은 1.96초(30초 창의 13%)였다.
- **적용**(2026-10-20 결정):
  - 운영은 처음에 기본값(코어 수, cgroup CPU 제한이 있으면 그 값)으로 시작한다. 코드 기본값은 바꾸지 않는다.
  - 나중에 바꾸는 방법은 두 가지다.
    - scouter.conf의 Go 전용 키 `go_maxprocs`(live, 재기동 없음. 지우면 기동 값으로 돌아간다. `internal/config/CONTRACT.md` §6)
    - 기동 환경 변수 `GOMAXPROCS`(배포 스크립트의 `SCOUTER_GOMAXPROCS`)
  - 우선순위는 conf 키 > 환경 변수 > runtime 기본값이다.
  - 호스트를 전용으로 쓰면 코어 수가 맞다. 쉬는 CPU를 GC가 쓰는 것은 손해가 아니기 때문이다. 다른 서비스와 공유하면 상한을 두는 편이 낫다.
  - cgroup CPU 제한(예: systemd `CPUQuota`)은 syscall을 포함한 process 전체를 제한하고, Go 1.25부터 runtime이 그 값을 GOMAXPROCS에 반영한다(`containermaxprocs`).
  - 정할 때는 최대 수집 rate의 여유와 조회 지연을 함께 본다. P 하나가 이 VM에서 XLog 전용 약 50,000/s를 처리했다.
  - 실제 Agent 부하에서도 `GOMAXPROCS`를 낮추면 같은 일에 쓰는 CPU가 줄어드는 같은 방향을 확인했다. 한 번 바꿔서 잰 비교라 시간대 영향을 배제하지는 못했다.

# Agent → Server XLog 최대 지속 처리량 로컬 측정 (2026-10-17)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

결론: 로컬 개발 장비(공유 부하)에서 Go 서버의 최대 지속 완료 XLog/s는 **payload의 "새 distinct 값" 비율에 따라 크게 달라진다**. 텍스트 사전이 안정된 XLog 전용 batch 1 부하에서는 7,539~10,000 XLog/s였다. 신규 텍스트·tag 값이 계속 생기는 부하(text hit 0.95)에서는 1,312~2,218 XLog/s였다. 같은 장비·같은 판정 기준에서 Java Scouter 2.20.0 배포본(-Xmx512m)은 350~843 XLog/s였다. 상한에서의 실패는 평균 CPU 포화가 아니었다. 주기적 집계 flush가 수집 경로를 멈추는 동안 UDP queue가 넘쳐 생긴 burst 거절이었고, 1차 원인은 **XLog tag count 집계·flush가 engine lock과 Store gate 안에서 실행되는 구조**다(§5). 이 문서의 수치는 이 장비의 측정값이다. 운영 처리량 예측이나 목표 수치가 아니다.

## 1. TPS 정의와 판정 기준

- TPS는 AGENTS.md 정의대로 **송신 후 서버에 저장되고 Client TCP 조회(`TRANX_LOAD_TIME_GROUP`)로 다시 확인된 완료 XLog 수/초**(`steady_verified_rate_tps`)다. packet/s, datagram/s, 송신 측 XLog/s는 따로 표시한다.
- driver `scripts/bench-xlog-max.py`([설계](bench-xlog-max-driver.md)): 지수 증가 후 이분 탐색, steady 30s + warmup 5s, rate별 새 서버·새 DB.
- 한 시도의 성공 조건: 누락·중복 0, `steady_verified ≥ 0.99×rate`, 생성기 오류·overrun 없음, Go UDP reject 0, drain ≤ 2s, Go queue backlog 증가 ≤ 64 datagram. 아래 두 기준은 1차 측정 이후 바꿨다(§7).
  - `netstat` UDP drop은 호스트 전체 카운터라, XLog 누락이 함께 있을 때만 실패로 본다(`--kernel-loss-fails`로 엄격 모드 가능).
  - backlog 기본 허용치는 0에서 64로 올렸다.
- 표의 "최대"는 **성공한 가장 높은 rate**이고 "실패 상한"은 그 바로 위에서 실패한 rate다. 각 탐색은 한 번씩 실행했으며 rate마다 반복하지 않았다. 반복 탐색값은 함께 적어 산포를 보인다.

## 2. 조건

| 항목 | 값 |
|---|---|
| 장비 | Apple M1 Pro 노트북(8코어, 다른 앱과 공유, 측정 중 load average 약 3~6), macOS, APFS, loopback 전용 |
| Go 서버 | 이 branch의 `dist/scouter-go`(go1.27.2), 기본 설정(udp-workers 2, udp-queue 1024, sync-interval 4s, tagcnt/visitor 기본 on), GOMAXPROCS 미지정 |
| Java 서버 | 원본 배포본 Scouter 2.20.0, Zulu JDK 8, `-Xmx512m`(배포본 startup과 같음), 격리 임시 DB, 시나리오마다 새 프로세스([실행기](../../scripts/bench-java-server.md)) |
| 생성기 | `scouter-replay` senders 2, objects 200(음수 objHash 포함), Client 조회 병행 2(1s 간격). batch 1 = XLog/Profile마다 단일 CAFE datagram(실제 Java Agent `DataProxy.sendDirect`와 같은 방식) |
| payload "mix" | profile ratio 0.3, profile 크기 0B 10% / 1KiB 60% / 8KiB 25% / 128KiB 5%(128KiB는 CAFM 분할) |
| text hit | 0.95 = 텍스트 참조의 5%가 새 hash(신규 TextPack과 신규 tag 값 발생), 1.0 = 사전 안정 |
| Java 판정 | 외부 모드. Java는 송신 종료 후 수 초 지나서야 XLog가 조회에 보인다(§7). 그래서 quiescence 후 `--verify-delay=20s`를 기다렸다가 검증했다. Java에서는 서버 UDP reject와 queue를 관측할 수 없어 `unknown`으로 둔다 |

생성기 한계(sink-only, 서버 없음): batch 1·objects 200에서 sender 1개는 약 58,700 tx/s, sender 2개는 약 62,000 tx/s, sender 4개는 약 68,300 tx/s(약 166k datagram/s)였다. 아래 모든 측정값은 이보다 훨씬 낮으므로 생성기는 상한이 아니다.

## 3. 결과: 최대 지속 완료 XLog/s

| 시나리오 | 조건 | Go 최대(실패 상한) | Java 2.20.0 최대(실패 상한) |
|---|---|---|---|
| batch 1, mix, text hit 0.95 | Agent 송신 방식, 신규 값 지속 | **1,500**(1,531) / 반복 1,312(1,359) | **843**(875) / 반복 550(600) |
| batch 1, XLog 전용, text hit 0.95 | profile 없음 | **2,093**(2,125) / 반복 2,218(2,250) | **812**(843) |
| batch 32, mix, text hit 0.95 | CAFN 묶음 | **5,093** | **593**(625) |
| batch 1, XLog 전용, text hit 1.0 | 사전 안정 | **10,000**(10,031) / 반복 7,539(7,578) | **550**(600) |
| batch 1, mix, text hit 1.0 | 사전 안정 | **6,562**(6,593) | **350**(400) |

Go 변형(batch 1, mix, text hit 0.95, 각 1회):

| 변형 | 최대 | 해석 |
|---|---|---|
| 기본(위 첫 줄) | 1,500 | 기준 |
| `--udp-workers 4 --udp-queue 8192 --udp-read-buffer 4MiB` | 1,875 | 노이즈 범위 안. worker·queue 확대로는 해결되지 않는다 |
| profile 압축(`--compress`) | 1,250 | 이득 없음 |
| Client 조회 병행 0 | 1,250 | 조회 병행은 상한 요인이 아니다 |
| GOMAXPROCS 2 / 4 | 1,406 / 1,125 | 코어 수로 늘지 않는다(직렬 구간 한계) |
| objects 20 | 1,718 | 객체별 visitor HLL 수 감소의 효과는 작다 |
| `visitor_hourly_count_enabled=false` | 1,218 | 단독으로는 상한 요인이 아니다 |

자원(최대 rate 근처, 30s+5s 부하 동안 서버 프로세스):

| 서버·시나리오 | CPU | RSS 최대 |
|---|---|---|
| Go, mix 0.95 @1,500 | user+sys 22.5s ≈ 평균 0.62코어 | 약 865MiB |
| Go, XLog 0.95 @2,062 | 21.1s ≈ 0.59코어 | 약 885MiB |
| Go, batch 32 mix @5,093 | 34.4s ≈ 0.96코어 | 약 834MiB |
| Go, XLog 1.0 @10,000 | 30.5s ≈ 0.85코어 | 약 865MiB |
| Java, mix 0.95 @843 | `ps` 중앙값 33%(순간 최대 320%) | 약 576MiB |
| Java, XLog 0.95 @812 | 중앙값 23% | 약 564MiB |

- Go RSS의 대부분은 visitor HLL이다. live heap 338MB 중 275MB가 HLL이고(객체 200개 × daily·hourly, HLL 1개 약 700KiB로 Java `VisitorDB`와 같은 구조), 나머지는 GC 여유분이다. `visitor_hourly_count_enabled=false`이면 RSS가 약 780MiB에서 약 455MiB로 줄었다. Java는 같은 HLL을 512MiB heap 상한 안에 둔다. 이 측정에서 Go는 **처리량은 높지만 메모리는 Java보다 많이 썼다**. "같은 호환성에서 더 적은 자원"이라는 목표 중 메모리 항목은 아직 충족하지 못했다. 다음 후보는 `GOMEMLIMIT` 조건 측정과 flush snapshot 버퍼 재사용이다.
- 두 서버 모두 평균 CPU에 여유가 있는 상태에서 실패했다. 상한은 CPU 포화가 아니라 직렬 구간의 정체에서 생긴다(§5).

## 4. 원본 Java Agent 송신 경로로 교차 확인

`tools/java-loadgen`(고정 원본 `DataProxy`/`UDPDataSendThread`/`DataUdpAgent`를 수정 없이 컴파일)으로 Go 서버에 보냈다. 조건은 objects 200, threads 4, profile ratio 0.3 × {1KiB×3, 8KiB}, service 100개(사전 안정), 30s, JVM 2 CPU다. 판정은 서버 종료 stats 대조다. `ingest.XLogs == 송신 XLog`, `udp.Datagrams == 송신 datagram`, `udp.Packets == 예상 packet`을 비교하며, 조회 검증은 하지 않았다.

| 목표 rate | 송신 XLog/s | 서버 저장 XLog | UDP reject | 대조 |
|---|---|---|---|---|
| 1,000 | 1,000 | 29,999 / 29,999 | 0 | 3개 모두 일치 |
| 1,500 | 1,500 | 45,000 / 45,000 | 0 | 일치 |
| 2,000 | 2,000 | 59,999 / 59,999 | 0 | 일치 |
| 3,000 | 3,000 | 89,999 / 89,999 | 0 | 일치 |
| 무제한 | 62,374(송신 81k datagram/s) | 912,067 / 1,871,231 | 1,152,408 | 불일치(과부하) |

- 원본 Agent 송신 계층의 wire(CAFE 단일, CAFN text/heartbeat 묶음)와 Go 수신은 3,000 XLog/s까지 손실 없이 일치했다.
- 무제한 과부하에서 Go 서버는 30초 동안 약 128만 packet(XLog 약 91만, 약 30k XLog/s)을 처리했고 나머지는 queue 포화로 거절해 `Rejected`로 셌다. 서버는 정상 종료했다. 이 값은 서버 카운터 기준이며 조회 검증 TPS가 아니다. 과부하 지속 상태라 "지속 가능 최대"로 쓰지 않는다.
- replay의 사전 안정 결과(§3, 7.5k~10k)와 원본 Agent 경로 결과가 같은 방향이다. 사전이 안정된 실제 Agent 부하에서는 신규 값 부하보다 상한이 훨씬 높다.

## 5. 상한 원인 분석

### 5.1 관측

- 실패 시도의 `/health` 1초 sample에서 `udp.Rejected`는 지속적으로 늘지 않고 **4~10초 간격 burst**로 늘었다(예: S1 1,750/s 시도에서 178, 599, 183). 평균 queue는 대부분 0~100이다.
- CPU profile(3,000 tx/s, XLog 전용, text hit 0.95, 30s):
  - syscall 77%, visitor HLL 파일 flush 49%(gate 밖, 10s 주기).
  - 수집 경로 `acceptLocked` 25% 중 `PutXLogTagsContext`→`tagFlushTableLocked`(pwrite) 15%, `PutXLog` 8%, `tagEnsureFile` 등의 `fstat` 7%.
  - block profile에서는 engine `LockContext` 대기가 컸다.
- `PutText`(신규 TextPack 저장)는 0.14s 미만으로 profile에 나타나지 않았다. 신규 텍스트 자체가 아니라 **신규 tag 값이 만드는 tag count 레코드**가 비용의 출처다.

### 5.2 A/B 확인 (고정 rate, batch 1, mix, text hit 0.95, 반복)

| 조건 | 결과 |
|---|---|
| 2,000/s, tagcnt on ×3 | 3/3 실패: 누락 666/200/483, UDP reject 2,166/499/1,150 |
| 2,000/s, `tagcnt_enabled=false` ×3 | 3/3 통과: 누락 0, reject 0 |
| 4,000/s, tagcnt off ×2 | 2/2 실패(reject 919/132) |
| 4,000/s, tagcnt off + visitor hourly off ×2 | 1/2 통과(reject 205/0), RSS 약 455MiB |

text hit 0.95와 1.0을 4,000/s(XLog 전용)에서 비교했다. 1.0은 조회 병행 0·2 모두 통과(reject 0)했고, 0.95는 둘 다 reject 452/593으로 실패했다.

### 5.3 판단

1. **1차 원인: XLog tag count가 수집 직렬 구간 안에서 실행된다.** `Engine.Tick`(1s)이 engine lock을 잡고 `tagTickLocked`로 5s마다, `SyncData`는 Store gate 안에서 4s마다 pending tag 레코드 전체를 디스크에 쓴다. 레코드마다 1440×float32를 찾고 쓰며, pending 상한을 넘으면 `tagAddLocked`가 수집 경로 안에서 바로 flush한다. 이 동안 모든 UDP worker가 lock을 기다리고 1024칸 queue는 1초 미만에 찬다. 신규 tag 값이 많을수록 flush가 길다. 원본 Java는 `XLogTagCount`(queue 10000 + daemon thread)와 `MoveToNextCollector`(queue 30000 + daemon thread, 5s 또는 1000 key마다 저장)로 **수집 thread에서 분리**해 처리한다. 저장 형식과 집계 의미는 같게 두고 실행 위치만 수집 경로 밖으로 옮기는 것이 첫 개선 후보다.
2. **2차 원인: visitor HLL flush.** gate는 HLL 1개를 복사하는 동안만 잡지만(`visitor.go` `flushVisitors`), 10s마다 약 280MB를 temp 파일에 쓰고 rename한다. I/O와 CPU를 같은 장비에서 경쟁하며 RSS도 키운다. 원본 Java `VisitorDB`도 같은 주기로 전체를 다시 쓰므로 형식 호환을 유지한 채 dirty 범위·주기를 줄이는 방안을 검토해야 한다.
3. **배제·약한 요인:** worker·queue 확대, GOMAXPROCS, 조회 병행, profile 압축은 상한을 의미 있게 바꾸지 못했다. batch 32가 3.4배 높은 이유는 datagram당 고정비(수신 syscall·clone·handler 호출)가 줄기 때문이다. 다만 실제 Java Agent는 XLog를 묶지 않으므로 batch 1이 Agent 현실 조건이다.

개선 구현과 전후 비교는 §11에 있다. 위 판단 중 "flush를 수집 경로 밖으로 옮기면 된다"는 부분은 반만 맞았다. 실제로는 pending 상한에 의한 inline flush와, 짧은 hold를 연달아 잡는 pass의 gate 점유가 원인이었다(§11.2).

## 6. Java 결과 해석의 한계

- Java는 rate에 대해 단조적이지 않았다. 같은 조건의 반복에서 843과 550이 나왔고, 사전 안정 mix에서는 400에서 실패한 뒤 350에서 통과했다. 이 측정의 Java 상한은 "약 350~850 XLog/s 범위"로 읽어야 한다.
- Java 실패는 커널 UDP drop이 0이어도 XLog 누락으로 나타났다. 원인은 Java 내부 queue drop일 수 있으나 관측 수단이 없어 확인하지 못했다.
- Java는 2.20.0 배포본이다. 계약 정본(2.21.3 source, `dee47fb`)과 버전이 다르다. JDK 8과 `-Xmx512m`은 배포본 기본값이고, heap을 늘린 조건은 측정하지 않았다.

## 7. 무효화한 실행과 교훈

| 실행 | 문제 | 조치 |
|---|---|---|
| Go 1차 행렬 | 15~93 tx/s에서도 "실패"가 나왔다. backlog 허용치 0이 queue 흔들림(1.0→1.6)을 실패로 잡았고, 호스트 전체 `netstat` drop(다른 프로세스 영향)이 XLog 누락 없이도 실패로 판정됐다 | driver 기준 수정(0be6192). 결과 폐기 후 재측정 |
| Java 1차 행렬 | 1 tx/s에서도 누락이 나왔다. Java는 cursor가 멈춘 뒤 수 초 지나서 XLog index를 반영하므로, quiesce 1s에서는 2,400건 중 583건이 조회되지 않았다 | `--quiesce=20s`는 drain 판정을 망가뜨렸다. 대신 replay `--verify-delay`를 추가했다(1e0afff). drain 1.03s, 2,400/2,400 확인 |

교훈: 호스트 전체 카운터와 서버별 카운터를 구분한다. 외부 서버는 "조회에 보이기까지의 지연"을 drain과 분리해 검증한다.

## 8. 재현

```sh
make build
# Go: 시나리오 1개(예: Agent 방식 batch 1 mix)
python3 scripts/bench-xlog-max.py --kind go --objects 200 --senders 2 --duration 30 --warmup 5 \
  --max-rate 60000 --batch 1 --query-concurrency 2 --initial-rate 500 --output <새 디렉터리>
#   사전 안정: --replay-arg=--text-hit-ratio=1.0 / XLog 전용: --profile-ratio 0
#   설정 A/B: --server-arg=--conf=<tagcnt_enabled=false 등을 넣은 conf>
# Java: SCOUTER_SERVER_DIST=<원본 배포본 server/> scripts/bench-java-server.py run --udp-port <p> --tcp-port <p> --jvm-arg=-Xmx512m --ready-file <f> 로 띄운 뒤
python3 scripts/bench-xlog-max.py --kind external --server-udp 127.0.0.1:<p> --server-tcp 127.0.0.1:<p> \
  --server-pid <pid> --objects 200 --senders 2 --duration 30 --warmup 5 --batch 1 --query-concurrency 2 \
  --replay-arg=--verify-delay=20s --output <새 디렉터리>
# 원본 Agent 송신 경로 교차 확인
SCOUTER_SOURCE=<고정 원본> python3 tools/java-loadgen/run.py --server dist/scouter-go --rate 3000 --duration 30 \
  --objects 200 --threads 4 --profile-ratio 0.3 --profile-sizes 1024,1024,1024,8192 --jvm-cpus 2 --report <파일>
# 부하 중 profile
GO=<go> scripts/bench-profile.sh --rate 3000 --warmup 5 --duration 40 --cpu-seconds 30 --contention --out <새 디렉터리> \
  -- --objects 200 --senders 2 --batch 1 --profile-ratio 0 --query-concurrency 0
```

시나리오는 하나씩 직렬로 실행했고, 측정 중에는 다른 부하 생성기를 돌리지 않았다. 결과 원본(report/decision/summary JSON)은 저장소에 넣지 않았다.

## 9. 남은 위험과 미검증

- 단일 노트북, 공유 부하, macOS APFS 조건이다. 같은 조건의 반복 간 차이가 약 ±15~25%였다. Linux 대상 파일시스템, CPU quota, 다른 서비스와 자원을 공유하는 목표 호스트 조건은 미검증이다.
- 각 rate를 한 번씩만 실행했다. 탐색 결과는 "그 실행의 성공 하한"이다.
- Pulse(Kafka) 활성 경로, 장시간 soak, retention 병행, 실제 다수 Agent JVM(소스 port 다수, 객체별 텍스트 사전)은 측정하지 않았다.
- 서버 코드는 바꾸지 않았다. 표준 gate 중 `make check`와 `cmd/scouter-replay` race만 실행했다. `make race/fuzz/bench/fixtures-check/inventory-check` 전체는 미실행이다(§10).

## 10. 이 branch의 도구 변경과 확인

- `cmd/scouter-replay`: `--sink-only`, `--senders`, 외부 서버 모드, 커널 UDP 통계, `--sample-health`, `--verify-delay`.
- `scripts/bench-xlog-max.py`: driver. `--kernel-loss-fails`, `--backlog-growth` 기본 64, `--replay-arg`.
- `scripts/bench-java-server.py`, `tools/java-loadgen/`, `scripts/bench-profile.sh`.
- 문서: [driver](bench-xlog-max-driver.md), [profiling 준비](2026-10-17-bench-profiling.md).
- 확인(commit 전): `make check` PASS, `go test -race ./cmd/scouter-replay/` PASS, `python3 -m unittest discover -s cmd/scouter-replay -p '*_test.py'` 5 OK.

## 11. 개선 전후 (branch `codex/ingest-perf`)

### 11.1 변경 (파일 형식·record 내용과 순서·조회 결과 유지)

| commit | 내용 |
|---|---|
| tag flush 분리 | engine tick은 5s flush·5min top100 rebuild를 due로 표시만 한다. runtime maintenance loop가 Store gate를 기록 단위의 짧은 hold(≤256 기록/2ms)로 나누어 쓴다. `SyncData`도 같은 incremental flush를 쓰고, `Close`는 전체 flush를 유지한다 |
| pending 시간 단위 | pending delta를 key마다 하루치 1440분(5.76KB)이 아니라 손댄 hour의 60분(240B)만 둔다. 원본 `MoveToNextCollector`와 같은 단위다 |
| gate 점유율 상한 | tag pass는 hold 뒤 그 길이의 3배 동안 gate를 잡지 않는다(점유율 ≤1/4) |
| visitor | register가 바뀐 방문만 dirty로 표시한다. 한 번 통째로 쓴 snapshot은 바뀐 4KiB page만 제자리에 쓴다. 버퍼를 재사용하고 pass마다 entry를 한 번만 쓴다 |

계약은 `internal/storage/CONTRACT.md` §8·§8.1, `docs/compatibility/storage-contract.md`, `aggregation-query.md`에 기록했다.

### 11.2 원인 추적 순서 (2000/s 고정, batch 1, mix, text hit 0.95, 30s)

| 빌드 | 통과 | UDP reject | 서버 CPU(user+sys) | 확인한 원인 |
|---|---|---|---|---|
| 기존 master | 0/3 | 314~2,389 | 약 25s | §5 |
| + flush를 engine lock 밖·짧은 hold로 | 0/3 | 326~1,866 | 약 29~30s | `tagAddLocked`의 pending 상한 inline 전체 flush가 여전히 수집 경로 안에서 실행(CPU profile 1.6s, ingest latency 최대 237ms) |
| + pending 시간 단위 | 0/4 | 46~895 | 약 16~17s | inline flush는 사라졌다. 그러나 `--sync-interval=0`도 실패, `tagcnt_enabled=false`만 통과. 원인은 pass의 hold가 연달아 gate를 잡아 FIFO로 넘겨줄 때 수집이 hold당 한 단계만 지나가는 것 |
| + gate 점유율 상한 | **4/4** | **0** | 약 17s | — |

### 11.3 최대 지속 완료 XLog/s (같은 driver·기준, 각 1회)

| 시나리오 | 개선 전 | 개선 후 |
|---|---|---|
| batch 1, mix, text hit 0.95 | 1,500 / 반복 1,312 | **3,812** |
| batch 1, XLog 전용, text hit 0.95 | 2,093 / 반복 2,218 | **7,781** |
| batch 1, XLog 전용, text hit 1.0 | 10,000 / 반복 7,539 | 10,031 |
| batch 1, mix, text hit 1.0 | 6,562 | 6,937 |

- 신규 값이 계속 생기는 부하에서 약 2.5~3.5배 늘었다. 사전이 안정된 부하는 원래 tag flush가 짧아 거의 변하지 않았다(노이즈 범위). 이 조건의 추가 개선은 §12에 있다.
- 서버 RSS 최대값은 같은 2000/s 조건에서 실행마다 약 640~870MiB로 흩어졌다(개선 전 약 860~890MiB). GC 시점 영향이 커서 이 측정으로는 메모리 개선을 주장하지 않는다. 메모리는 §12에서 따로 다룬다.
- 후속 보완 뒤(SyncData에서 tag 쓰기 제거, 밀린 양이 많으면 yield 생략, ctx로 취소되는 yield, counter 추가) 같은 2000/s 조건 2회: 모두 통과, UDP reject 0, `tagcount={InlineFlushes:0 Passes:6 MaxPassMillis:≈1500}`.
- 한계: 같은 공유 노트북 측정이고, 각 탐색은 1회다. Linux·장시간 soak·실제 Agent로는 확인하지 않았다.

## 12. 추가 개선 (branch `codex/ingest-perf-b`)

### 12.1 변경 (파일 형식·조회 결과 유지)

| 항목 | 내용 |
|---|---|
| tag 파일 syscall 축소 | 크기는 handle과 수명을 같이 하는 cache에 두고 header는 handle마다 한 번만 검증한다. record head와 key를 한 번에 읽는다 |
| tag count 분리 | 원본 `XLogTagCount`·`MoveToNextCollector`처럼 수집은 기여를 inbox에 넣기만 한다(가득 차면 버리지 않고 직접 반영). pending, tag 파일 handle, 크기는 tag 전용 gate가 보호한다. tag pass는 Store gate를 잡지 않으므로 gate 점유율 상한(yield)은 없앴다 |
| text 비동기 저장 | 원본 `TextWR`·`TextCache`처럼 PutText는 검증 후 pending(즉시 조회)과 bounded queue(개수 8192, 32MiB)에 넣는다. Store가 소유한 writer가 gate hold당 최대 256개를 쓴다. 수집 engine lock은 text 쓰기를 기다리지 않는다 |
| visitor HLL 메모리 | nonzero word 8192개까지는 sparse map으로 둔다. 파일 이미지와 추정값은 같다 |

계약과 원본 대비 의도된 차이는 `internal/storage/CONTRACT.md` §8, §8.1, §8.2에 있다. 차이는 queue가 가득 찰 때 버리지 않고 기다리는 것, text 대상이 PutText 시점에 정해지는 것 등이다.

### 12.2 결과 (같은 driver·기준, 각 1회, 측정 중 다른 작업 없음)

| 시나리오 | §11 개선 후(bb03619) | 추가 개선 후 | 서버 RSS 최대(추가 개선 후, 통과 rate) |
|---|---|---|---|
| batch 1, mix, text hit 0.95 | 3,812 | 3,502 | 167MiB |
| batch 1, XLog 전용, text hit 0.95 | 7,781 | **11,355** | 392MiB |
| batch 1, XLog 전용, text hit 1.0 | 10,031 | **12,500** | 253MiB |
| batch 1, mix, text hit 1.0 | 6,937 | 7,054 | 166MiB |

3,600/s 고정(mix, text hit 0.95, 2회씩 교차, 두 빌드 모두 상한을 넘는 부하):

| 빌드 | UDP reject | tag pass 최대 | RSS 최대 |
|---|---|---|---|
| bb03619 | 1,721 / 368 | 약 2.8s | 859 / 903MiB |
| 추가 개선 후 | 371 / 374 | 약 0.4s | 200 / 152MiB |

- XLog 전용 부하는 25~46% 늘었다. mix 부하는 노이즈 범위 안이다. mix의 남은 상한은 tag·text가 아니라 profile(128KiB까지, CAFM 분할)과 XLog 저장 경로(data와 index pwrite, Store gate 직렬화)다. 원본도 data 기록마다 flush한다. data append를 묶으면 index가 아직 쓰이지 않은 data를 가리킬 수 있어 하지 않았다.
- 메모리: 서버 RSS 최대가 약 860~900MiB에서 약 150~390MiB로 줄었다. 같은 장비의 Java 2.20.0(`-Xmx512m`, §3) 약 500~600MiB보다 낮다. 측정은 objects 200, 35초 부하이므로, 방문자가 많은 객체의 HLL은 장시간 운영에서 dense(약 700KiB)로 바뀐다.
- 한 번 무효로 한 측정이 있다. tag count 분리를 적용한 직후의 탐색 한 회는 같은 장비에서 테스트를 동시에 돌려 mix 결과가 1,959로 나왔다. 그 결과는 폐기하고 위 표에는 넣지 않았다.

### 12.3 남은 후속 과제

§13에서 다룬 것이 있다. 남은 것은 §13.4에 있다.

## 13. 3차 개선과 결함 수정 (branch `codex/ingest-perf-c`)

### 13.1 결함: 종료 중 끊긴 tag flush가 hash chain을 깨뜨림

부하 측정 중 서버 종료 1회에서 `Close`가 `corrupt Scouter DB`(exit 1)를 반환했다. 서버 실행 404회 중 1회였고, 재기동 뒤 XLog 413,000건은 모두 조회로 확인됐다. 원인은 이렇다.

- 새 tag 기록을 쓸 때 hash bucket의 새 head를 먼저 쓰고 key 기록을 나중에 썼다.
- 쓰기마다 ctx를 확인했다.
- 그래서 종료 중 취소된 flush가 key 파일 끝을 넘는 위치를 가리키는 bucket을 남겼다. 그 bucket의 조회와 이후 flush는 `ErrCorrupt`가 된다.

이 결함은 성능 작업 이전의 원래 코드에도 약한 형태로 있었다. 원래의 5초 flush도 engine tick의 ctx로 끊길 수 있었다.

수정은 두 가지다.
- 원본 `IndexFile.put`처럼 key 기록을 먼저 붙이고 그다음 bucket을 연결한다.
- 기록 하나는 시작하면 끊지 않는다.

`TestTagFlushCancelledAnywhereLeavesWholeRecords`는 수정 전 코드에서 같은 오류로 실패한다. 이미 쓰인 DB의 끊긴 chain은 `scouter-db inspect`의 `tagcount.*` 경고로 찾는다(§13.3).

### 13.2 변경 (파일 형식·조회 결과 유지)

| 항목 | 내용 |
|---|---|
| tag 기록 제자리 갱신 | 기존 기록은 delta가 손댄 hour(240B)만 읽고 더해 쓴다. 이전에는 First 1440분 전체 또는 Next의 모든 hour block을 다시 썼다. 전체 rewrite와 byte 동일하다(`TestTagInPlaceUpdateMatchesFullRewrite`, Java가 쓴 파일 위에서도 같은 비교). 쓰기 실패 시 count가 바뀌기 전이면 재시도하고, 일부라도 반영됐으면 delta를 버리고 센다(`DroppedDeltas`). 이중 가산은 없다 |
| 할당 감소 | 수집 경로의 tag 기여를 압축 표현으로 바꿨다(`protocol.Value` 대신 kind·number·text). key encoder를 재사용하고, chain 탐색에서 key가 다른 기록은 count를 decode하지 않는다. 11k XLog/s 측정에서 할당 8.2GB→4.3GB, GC CPU 2.09s→0.95s(30s 창) |
| tag 조회 hold 분할 | Client tag 조회도 파일 scan을 tag gate hold로 나눈다. 다른 날짜 purge에는 영향받지 않는다 |
| hold 안의 inbox 반영 | flush·rebuild hold가 쌓인 inbox를 먼저 반영한다 |
| UDP queue | 기본 1024→8192 datagram, payload 32MiB 상한을 추가했다. 바쁜 장비에서는 scheduler 지연(trace: 6초에 worker 357ms, reader 212ms 대기)이 1024칸을 넘겼다. mix 4800/s와 XLog 12800/s에서 1024는 4/4 실패, 8192는 4/4 통과였고 RSS 차이는 없었다 |
| text writer 양보 | Store gate hold 뒤 그 길이의 3배 동안 쉰다(queue 절반 이상이면 쉬지 않음). A/B는 노이즈 범위였다 |

### 13.3 tag chain 검사(읽기 전용)

`VerifyTagCountFiles`와 `scouter-db inspect`의 `tagcount.*` 경고가 검사하는 불변식은 세 가지다. 기록마다 link는 같은 bucket의 직전 기록이고, bucket head는 그 bucket의 마지막 기록이고, header count는 비어 있지 않은 bucket 수다. 원본 Java가 쓴 파일도 이 불변식을 만족한다. 단, 원본 Java는 hash 표를 4초마다 저장하므로, 실행 중 복사본이나 Java crash 뒤의 DB에서도 경고가 나온다.

### 13.4 결과 (같은 driver·기준, 각 1회, 측정 중 다른 작업 없음)

| 시나리오 | §12 추가 개선 후 | 3차 개선 후 | 서버 RSS 최대 |
|---|---|---|---|
| batch 1, mix, text hit 0.95 | 3,502 | **8,682** | 323MiB |
| batch 1, XLog 전용, text hit 0.95 | 11,355 | **24,703** | 532MiB |
| batch 1, XLog 전용, text hit 1.0 | 12,500 | **45,474** | 298MiB |
| batch 1, mix, text hit 1.0 | 7,054 | **26,601** | 277MiB |

- 처음 측정(§3)과 같은 조건에서 비교한다. 신규 값 mix는 1,312~1,500에서 8,682로, XLog 전용은 2,093~2,218에서 24,703으로 늘었다. 같은 장비의 Java 2.20.0은 350~843이다.
- 큰 몫은 UDP queue 확대다. 남은 짧은 정체와 공유 장비의 scheduler 지연이 queue를 넘기던 것을 흡수했다. 앞선 개선은 그 정체의 길이와 빈도를 줄였다.
- tag counter: 네 시나리오 모두 `InlineFlushes`·`ApplyErrors`·`DroppedDeltas`가 0이었다. 2만 XLog/s 이상에서는 inbox(16384)가 차서 `Backpressure`가 24~70회 났다. 수집이 직접 반영했고 손실은 없었다. 이후 100ms 주기 inbox 반영을 넣었다(`codex/ingest-perf-d`). 40k XLog/s(XLog 전용, 텍스트 고정)에서 `Backpressure`는 70→0회였다(각 5회). 통과는 이전 5/5, 이후 4/5였다. 1회 실패는 한 10초 창에 UDP reject 10,718건이 몰린 경우였다. 그 창의 ingest 최대 지연은 21ms로 정상이었으므로 공유 장비 scheduler 지연으로 보인다.
- 한계: 공유 노트북(load 3~6) 측정이고, 같은 장비에서 생성기(2 sender)가 함께 돈다. 각 탐색은 1회이고 30초다. Linux 파일시스템, 장시간 soak, 실제 다수 Agent는 미검증이다.

### 13.5 실제 Agent 환경 확인 항목

1. 배포 전 기존 DB 복사본에 `scouter-db inspect`를 실행해 `tagcount.*` 경고를 확인한다. 수정 전 서버의 종료 중 끊긴 flush 흔적이 남았는지 보는 것이다. 경고가 있으면 해당 날짜·objType의 tag 조회 이상 여부를 Client에서 확인한다.
2. 실제 Agent 부하에서 `/health`의 `tagCount`(`InlineFlushes`, `Backpressure`, `InboxDropped`, `PendingDropped`, `ApplyErrors`, `DroppedDeltas`, pass 시간)와 `maintenance.textWriteErrors`·`textDropped`, `udp.Rejected`를 확인한다.
3. Client에서 tag count·top100·텍스트(service/SQL 이름) 표시와 갱신 시점을 확인한다. tag는 5초 flush와 5분 rebuild 주기, 텍스트는 즉시 조회된다.
4. RSS와 CPU를 같은 시간대의 Java 서버와 비교한다(장시간).
5. 재기동 직후와 종료 시 로그에 `corrupt`나 `tag count flush` 오류가 없는지 확인한다.


## 14. 기능 격리 (branch `codex/isolation`)

목표: 서버 내부 기능의 문제가 Agent·Client로 전파되지 않고, 한 기능의 정체가 다른 기능(특히 XLog 수집)의 병목이 되지 않게 한다. 계약은 `internal/storage/CONTRACT.md` §6·§8·§8.2, `internal/ingest/CONTRACT.md` §11에 있다.

### 14.1 변경 (파일 형식·record 내용·조회 결과 유지)

- text queue·tag inbox: 가득 차면 최대 100ms 기다린 뒤 버리고 센다(`textDropped`, `InboxDropped`, log S140/S188). 원본 `TextWR`·`XLogTagCount`도 가득 차면 버린다. 한 번 시간이 초과되면 포화가 풀릴 때까지 기다리지 않는다. 그래서 그 기능이 멈춰도 수집은 포화 한 번에 최대 100ms만 늦어진다. text 포화는 queue가 절반 이하로 줄어야 풀린다. 버린 text는 작은 cache(8192개/4MiB)에 남아 조회에 보인다(원본 `TextCache`).
- tag pending 상한: 그 자리 전체 flush 대신 entry(XLog 하나의 tag) 단위로 버린다(`PendingDropped`, log S182). flush·rebuild pass는 hold 사이에 쉬고, hold 안의 inbox 반영은 예산 절반으로 묶는다.
- tag 집계를 전용 runtime goroutine으로 옮겼다(100ms 반영, 5초 flush, 5분 rebuild). flush 실패는 다음 5초 tick에 다시 한다.
- text writer는 Store gate hold 사이에 쉰다. 수집의 group 이름 조회는 writer hold를 기다리지 않는다.
- realtime 조회 decode, 5분 sample·alert summary·group 저장, service group 이름 조회를 engine lock 밖으로 옮겼다.
- realtime counter 조회는 Counters gate를 hold로 나누어 잡는다(이전: 24시간 조회 하나가 counter 수집을 막아 engine lock을 통해 XLog 수집까지 멈출 수 있었다). purge는 날짜별 횟수로 감지한다.
- batch archive 업로드·조회의 body I/O를 Store gate 밖으로 옮기고, 업로드·조회 동시 수를 각각 4로 묶었다(이전: 느린 업로드 하나가 deadline 동안 XLog 쓰기를 막을 수 있었다).
- 결함 수정: ENOSPC 등으로 header 쓰기에 실패해 빈 채로 남은 tag 파일이 디스크 회복 뒤에도 계속 `ErrCorrupt`였다. 이제 다시 header를 쓴다.

### 14.2 폭주 (XLog 전용, text hit 0.0, 20,000 XLog/s 고정, 30초, batch 1, 조회 병행 2)

신규 service·tag 값이 계속 들어오는 최악 조건이다.

| 빌드 | XLog 손실(70만 중) | UDP reject | 버린 tag entry | 버린 text |
|---|---|---|---|---|
| 대기 방식(격리 전) | 321k~356k | — | 0 | 0 |
| 격리 1차(02f2005) | 8k~122k | — | — | 0 |
| **격리 최종** | **0 (2/2)** | **0** | 535k~541k(`PendingDropped`) | 0 |

- 이전 빌드의 reject·drop 수는 이 표에 옮기지 않았다(—). 최종 빌드는 보조 기능인 tag count만 줄고 XLog는 모두 저장됐다. 버린 수는 `/health`의 `tagCount`에 남는다. `InlineFlushes`·`ApplyErrors`·`DroppedDeltas`는 0이다.

### 14.3 최대 지속 완료 XLog/s (§8 driver·기준, 각 1회, 측정 중 다른 작업 없음)

| 시나리오 | 3차 개선 후(§13.4) | 격리 후 | 버린 tag/text |
|---|---|---|---|
| batch 1, mix, text hit 0.95 | 8,682 | **10,968** | 0 / 0 |
| batch 1, XLog 전용, text hit 0.95 | 24,703 | **32,847** | tag 39,837 / 0 |
| batch 1, XLog 전용, text hit 1.0 | 45,474 | **53,792** | 0 / 0 |
| batch 1, mix, text hit 1.0 | 26,601 | **29,375** | 0 / 0 |

- 회귀는 없다. 늘어난 몫은 수집 경로가 tag·text 정체를 기다리지 않게 된 효과로 보인다. 다만 공유 장비의 1회 측정이라 변동 폭(수 %~10%대)을 감안한다.
- XLog 전용 text hit 0.95에서 tag pending 상한 drop은 16,000/s까지 0이었고, 32,000/s 이상에서 시도마다 0~4만 건 생겼다. 신규 tag 값이 초당 약 1,600개 이상 쌓이면 5초 flush가 따라가지 못하는 지점이다. 그 이하 부하에서는 tag를 잃지 않는다.
- 평소 부하(위 mix 최대 처리량 포함)에서 `InboxDropped`와 `textDropped`는 0이었다.

### 14.4 남은 위험

- gxid·span·profile 수집 조회는 Store gate를 조회 동안 잡는다(txid·gxid 하나의 결과). `TagActualContext`는 이후 hold로 나눴다.
- purge·DB 삭제는 Store gate를 잡은 채 Counters·tag gate를 기다린다. 일부 경로(`DailyCounterNamesContext`, seal 없는 옛 counter index 열기, 조회 앞의 tag inbox 반영, tag table eviction)는 한 번에 더 오래 잡는다. 만료 날짜가 있는 retention 회차와 DB 삭제에서만 생긴다.
- tag 폭주 시 잃는 count는 Java와 지점이 다르다. Java는 `XLogTagCount` queue(S188)에서 잃고, Go는 주로 pending 상한(S182)에서 잃는다.
- Linux 파일시스템, 장시간 soak, 실제 다수 Agent는 실제 Agent 환경에서 확인한다(§13.5에 `textDropped`, `InboxDropped`, `PendingDropped` 추가).

### 14.5 실제 Agent 환경에서 찾아 고친 기존 결함

격리 변경을 실제 Agent 환경에 배포해 §13.5 항목을 확인했다. 재시작 직후 UDP reject와 `tagcount.*` 경고는 없었고, `textDropped`·`InboxDropped`·`PendingDropped`·`ApplyErrors`·`DroppedDeltas`도 0이었다. 무거운 Client 조회 중에도 수집 지연은 늘지 않았다. 그 환경의 규모와 조회 시간 수치는 적지 않는다. 확인 중에 이번 격리 변경과 무관한 기존 결함 다섯 가지를 찾아 고쳤다.

1. **큰 Client 응답이 끊김**: Client 응답 encoder가 요청 decoder의 collection 상한(4,096개/값 16,384개)을 그대로 썼다. 긴 구간의 `COUNTER_PAST_TIME_ALL` 응답이 encode에 실패했고, 서버가 연결을 닫았다(webapp 500, Client 과거 counter 차트 실패). Java에는 이 상한이 없다. 이제 Client 응답은 응답 byte 상한만 받고, webapp도 collector 응답을 2^18/2^19까지 decode한다.
2. **webapp 호출 timeout 의미**: `net_webapp_tcp_client_so_timeout`(30초)은 Java에서 read마다 거는 socket timeout인데, Go는 명령 전체에 걸었다. 그래서 계속 응답이 오는 긴 조회가 30초에 실패했다. 이제 응답 item마다 다시 잰다.
3. **index 조회가 syscall에 묶임**: chain 레코드 하나를 읽을 때 `ReadAt`을 3~4번 했다. 이제 레코드 head와 작은 payload를 한 번에 읽는다(결과·오류 동일, 맞지 않으면 기존 경로).
4. **objType counter 조회가 객체 수만큼 범위를 반복해 읽음**: `COUNTER_PAST_TIME_ALL`이 객체마다 같은 범위의 realtime index를 처음부터 걸었다. Java는 `readBulk`로 한 번 읽는다. index 값에 objHash가 있으므로 이제 범위를 한 번만 걸으며 요청한 객체의 data만 읽는다(객체별 결과는 동일).
5. **webapp이 응답 전체를 decode된 채 보관**: objType 전체의 긴 구간 응답을 모두 `Value`로 들고 있다가 JSON을 만들어, 조회 한 번에 process heap이 크게 올랐다. 이제 pack이 도착하는 대로 시각·값 배열로 줄인다(JSON 동일).

세 수정(3~5)을 거치며 objType 전체 counter 조회 시간과 조회 중 최고 RSS가 모두 크게 줄었다.

남은 항목: Client 화면의 tag count·top100·텍스트 표시(사람이 Client로 확인), 장시간 RSS·CPU 비교.

## 15. XLog·profile writer와 visitor lock (branch `codex/isolation`)

### 15.1 분석

- mix 9,000 XLog/s의 경합 profile에서, UDP worker는 30초 동안 lock을 13.8초 기다렸다. 그중 Store gate 대기는 3.2초뿐이었고, 나머지는 engine lock 대기였다. engine lock 안 CPU 9.1초 중 8.7초는 XLog·profile `pwrite`였다.
- 그래서 Store gate만 파일 종류별로 쪼개는 방법은 효과가 없다. 쓰기는 여전히 engine lock 안에서 하나씩 실행되기 때문이다.
- 원본은 수집 thread에서 쓰지 않는다. `XLogCore`→`XLogWR`(`xlog_queue_size`), `ProfileCore`→`XLogProfileWR`(`profile_queue_size`)로 넘기고, queue가 가득 차면 S144·S142로 버린다.

### 15.2 변경 (파일 형식·쓰기 순서·조회 결과 유지)

- XLog·profile은 수집에서 encode한 뒤 writer goroutine에 넘긴다(storage CONTRACT §8.3). 비압축 파일은 동기 쓰기와 byte 단위로 같다. `xlog_queue_size`·`profile_queue_size`를 적용했다(재기동 키). purge·삭제 때 queue에 남은 레코드는 디렉터리와 함께 버린다.
- visitor 상태에 전용 lock을 주었다(storage CONTRACT §8.1). writer만 분리하면 XLog마다 visitor 갱신이 writer의 gate hold 뒤에서 engine lock을 쥔 채 기다려 더 나빠졌다(block profile 8.8초).

### 15.3 결과 (같은 장비, 기준 빌드와 번갈아 고정 rate 30초, 각 2회)

| 조건 | 기준 p50 / p99 / max | 변경 후 p50 / p99 / max |
|---|---|---|
| mix, text hit 0.95, 9,500/s | 23µs / 약 330µs / 9~30ms | **8µs / 약 55µs** / 9~17ms |
| mix, text hit 0.95, 10,500/s | 22µs / 약 300µs / 10~13ms | **7µs / 약 47µs** / 11~13ms |
| mix, text hit 1.0, 25,000/s | 29µs / 약 95µs / 73~79ms | **10µs / 약 55µs / 5~10ms** |

- 수집 지연(engine lock 안에서 pack 하나를 처리하는 시간)이 3~6배 짧아졌다. counter·object·alert 처리가 XLog 디스크 쓰기를 기다리지 않는다.
- 최대 지속 처리량은 차이를 가리지 못했다. 이 장비의 상한은 커널 UDP 수신 버퍼 손실이고(호스트 전체 기준 "full socket buffers"), 같은 조건에서도 두 빌드가 번갈아 수십~수백 건씩 잃는다. 서버 CPU는 약 5% 늘었다(goroutine 간 전달 비용).
- 폭주(text hit 0.0, 20,000/s, 70만 건)는 손실 0이었고, writer queue drop도 0이었다.

## 16. 최대 성능 재측정과 추가 개선 (2026-10-18, branch `codex/isolation`)

### 16.1 측정 방법의 교정

- 앞 절까지 로컬 최대치의 상한은 서버가 아니었다. 커널 UDP 수신 버퍼 손실(호스트 전체 "full socket buffers")이 모든 시나리오의 실패 원인이었다.
- Go CLI는 `net_udp_so_rcvbuf_size` 키가 없으면 OS 기본(macOS 약 768KiB, Linux 보통 약 208KiB)을 썼다. 원본 `DataUdpServer`는 키가 없어도 4MiB를 건다. 이제 Go도 기본 4MiB다(0은 OS 기본). OS 상한(Linux `net.core.rmem_max`)이 더 작으면 원본처럼 그 값으로 줄어든다.
- 탐색 판정에는 손실 외에 `ingest_backlog_growth`도 있다. 측정 동안 UDP queue가 계속 늘면, 손실이 없어도 그 rate는 지속 불가로 본다.
- 공유 노트북(측정 중 load 4~13) 1회 측정이다. 같은 빌드도 회차마다 10~20% 움직였으므로, 개선 판단은 같은 고정 rate에서 빌드를 번갈아 돌린 A/B로 했다.

### 16.2 개선과 근거 (각 단계 A/B)

| 단계 | 원인(측정) | 변경 | 효과 |
|---|---|---|---|
| 수신 버퍼 | 커널 UDP 버퍼 손실 | 기본 4MiB(원본과 같음) | 시나리오별 최대치 +39~98% |
| UDP 경로 복제 | 모든 pack을 engine lock 안에서 encode·decode로 복제 | UDP receiver가 decode한 pack은 복제하지 않음(`AcceptOwnedFrom`). 왕복 동일성은 `FuzzDecodePack`이 검사 | 같은 rate에서 서버 CPU 약 13% 감소 |
| UDP worker 수 | Java 기본 3을 시험 | Go 기본 2 유지(의도): worker가 engine lock 하나로 직렬화되어, 3개째는 lock 전달만 늘림 | 16,000 mix/s에서 queue 최대 223~262(2개) vs 425~687(3개) |
| tag pass 휴식 | pass 중 inbox가 차서 수집이 회당 최대 100ms 대기(30초에 32~34회), 기여 약 11만 건 버림 | pass가 쉬는 동안에도 inbox를 짧은 hold로 반영 | 38,000 XLog/s에서 대기·버림 0, UDP queue 안정 |
| text writer 휴식·읽기 cache | text queue가 차서 수집 대기, group 이름 조회의 Store gate 대기 | text 휴식 축소, 읽기 cache(원본 `TextCache` 역할) | 22,000 mix/s에서 실패 2/2 → 통과, 폭주 손실 0(3/3) |

### 16.3 최대 지속 완료 XLog/s (§8 driver, batch 1, 조회 병행 2, 각 1회)

| 시나리오 | 오늘 시작(§14) | 수신 버퍼 4MiB | 최종 |
|---|---|---|---|
| mix, text hit 0.95 | 10,968 | 19,593 | **26,226** |
| XLog 전용, text hit 0.95 | 32,847 | 48,406 | **85,859** |
| XLog 전용, text hit 1.0 | 53,792 | 74,960 | **95,121** |
| mix, text hit 1.0 | 29,375 | 58,140 | **54,843** |

- 최종 열은 text 쪽 변경 전(XLog 전용 두 줄)과 후(mix 두 줄)의 측정이다. 두 측정 사이에 바뀐 것은 text 쪽뿐이라 XLog 전용 결과에는 영향이 작다. mix 1.0은 회차 변동 범위 안이다(53,554~58,140).
- 실패 사유: mix는 UDP queue 증가(수집 처리 상한), XLog 전용은 queue 증가와 커널 UDP 손실이 함께 나타났다.
- 신규 text 100% 폭주(20,000 XLog/s, 70만 건): XLog 손실 0(3/3). tag는 pending 상한에서 약 53만 entry를 버린다(보조 기능).

### 16.4 다음 개선 후보 (측정 근거)

- mix 상한 근처의 남은 대기는 Store gate를 두고 XLog·profile·text writer가 서로 기다리는 시간이다. 파일 종류별 gate 분리가 다음 후보다. handle cache·압축 저장소를 공유하므로 설계가 크다.
- 할당: decode 복사·UDP 수신 복사·encode가 GC를 키운다(30초에 수 GB). 수신 버퍼 pool, encode 재사용이 후보다.
- 운영: Linux `net.core.rmem_max`가 배포판 기본처럼 작으면 4MiB 요청이 그 값으로 줄어든다. 원본 Java도 같은 상한을 받으므로, 순간 몰림이 잦으면 호스트 설정으로 올리는 것을 검토한다.
- 공유 장비의 측정 변동이 커서, 운영 판단에 쓸 최대치는 격리 Linux 호스트에서 잰다. 절차는 `isolated-benchmark.md`(`make bench-bundle`, `scripts/bench-isolated.sh`)에 있다. 그 뒤 text 쪽 개선(68998dd: 없음 cache, TextDupCheck에 해당하는 재제출 건너뛰기)도 들어갔으며, 효과는 격리 측정으로 확인한다.

## 17. 효율 지표로 계속한 개선 (2026-10-19)

격리 호스트가 없어서, 공유 장비에서도 흔들리지 않는 지표로 개선을 판단했다. 지표는 같은 고정 rate에서 완료 XLog 1,000건당 서버 CPU 초다. 기준과 변경을 번갈아 3회씩 쟀고, 장비 부하가 load 24까지 오른 회차에서도 회차 간 변동은 ±0.3% 안이었다. 최대 처리량은 서버가 쓸 수 있는 CPU에 비례하므로, 이 값이 줄면 상한도 그만큼 오른다.

| 변경 | mix 15,000/s (1,000건당 CPU 초) | XLog 전용 30,000/s |
|---|---|---|
| 시작(68998dd) | 0.1073~0.1077 | — |
| UDP pack이 datagram을 그대로 가리킴(복사 없음, `AdoptInput`) | 0.1028~0.1048 | — |
| 레코드 버퍼 재사용 1MiB, tag 새 레코드 버퍼 재사용, XLog encode 한 번 | 0.1002~0.1012 | 0.0505~0.0514 → 0.0502~0.0506 |
| writer 묶음 쓰기(파일마다 한 번, `appendDataBatch`·`PutBatch`) | 0.091~0.095 | 0.047~0.048 |

- 합계: mix에서 XLog당 서버 CPU 약 13% 감소, XLog 전용에서 약 7%(묶음 쓰기 기준) 감소.
- 모든 변경에서 파일은 byte 단위로 같고, 순서 보장(index는 자기 data 뒤)도 유지된다(단위 테스트·`FuzzDecodePack`·Java storage interop).
- 실제 Agent 환경: 배포 뒤 reject·오류·drop은 없었다. 실시간 XLog 표본을 저장소에서 다시 조회해 XLog와 profile이 모두 읽히는 것을 확인했다.
- 남은 CPU의 대부분은 syscall(pwrite, UDP recvmsg, pread)이다. 레코드당 syscall은 이미 묶었으므로, 다음 후보는 UDP 수신 일괄화(Linux `recvmmsg`, 표준 라이브러리 밖)와 tag flush의 레코드당 읽기·쓰기다.

### 17.1 시도했으나 넣지 않은 변경: tag flush 읽기 cache

mix 15,000/s 프로파일에서 tag flush는 서버 CPU의 약 12%다. 그중 pread가 약 1.9초(30초 창)였다. 읽기를 없애는 두 가지를 구현하고, 파일 byte 동등성 테스트와 mutation 검사까지 통과시킨 뒤 같은 방법(기준과 번갈아 3회)으로 쟀다. 둘 다 효과가 측정 변동보다 작아서 넣지 않았다.

| 시도 | 내용 | 1,000건당 CPU 초 (기준 → 변경, 3회 평균) | 원인 |
|---|---|---|---|
| 기록 위치·hour block cache (각 32,768 entry) | 이미 있는 key의 hash chain 탐색과, 더하기 전 block 읽기를 생략 | 0.0914 → 0.0934 | 한 pass에서 갱신하는 기존 key가 약 77,000개라 cache보다 많다. FIFO가 다음 pass 전에 모두 밀어내서 hit가 약 32,000에서 멈췄다. 다 담으려면 약 100MB가 필요해 공유 호스트에 맞지 않는다. |
| hash 파일 mirror (table당 1MiB, 합계 8MiB 상한) | 새 key를 포함한 모든 bucket 읽기를 메모리에서 처리 | 0.0916 → 0.0915 | tag 읽기는 1.95 → 1.16초, flush는 4.82 → 4.28초(전체의 약 1.5%)로 줄었다. 그러나 효율과 pass 시간(최대 2.9~3.3초 → 2.6~3.1초) 모두 측정 변동 안이다. |

- 남은 tag 비용은 쓰기다. 새 기록마다 data block, key 기록, hash link를 합쳐 3~5회 쓰고, 갱신은 hour마다 쓴다. 기록 사이를 묶어도 상한은 약 1.7%로 추정된다. 그만큼을 얻으려면 실패 시 잘라 내기(cut)의 의미가 기록 여러 개로 넓어지므로 보류했다.
- 로컬(macOS)에서 효율로 잴 수 있는 큰 후보는 이것으로 줄었다. 남은 큰 몫은 UDP 수신(recvmsg 약 14%)이다. `recvmmsg` 일괄 수신은 Linux 전용이고, macOS에서는 `x/net`이 한 번에 하나씩 읽어 효과를 잴 수 없다. Linux 부하 환경에서 A/B로 판단해야 한다.

### 17.2 Linux VM 측정과 UDP 일괄 수신(recvmmsg): 넣지 않음

- **측정 환경**: 같은 노트북의 Linux VM(Lima, Apple Virtualization)이다. Ubuntu 24.04 arm64, vCPU 4, 메모리 4GiB를 썼다. 서버와 replay는 모두 VM 안의 loopback에서 돌렸다. 격리 호스트가 아니므로 절대 수치보다 빌드 간 A/B로 판단한다.
  - VM의 `net.core.rmem_max`는 기본 208KiB다. 이 값에서는 서버가 요청한 4MiB 수신 버퍼가 잘려, mix 15,000/s에서도 kernel `RcvbufErrors`로 XLog 약 800건을 잃었다. 8MiB로 올린 뒤 쟀다.
  - 같은 조건에서 서버 CPU는 macOS보다 낮다. mix 15,000/s 기준 1,000건당 약 0.053초다(macOS 약 0.091초).
- **구현**: Linux에서만 `golang.org/x/net/ipv4`의 `ReadBatch`(recvmmsg)로 한 번에 최대 16개를 읽었다. 다른 OS는 기존의 하나씩 읽기를 유지했다. 단위 테스트(IPv4·IPv6 sender, MSG_TRUNC, 순서)는 VM에서 race까지 통과했고, 한 호출에 16개를 받는 것도 확인했다.
- **효율**(기준과 번갈아 3회, 1,000건당 CPU 초): mix 15,000/s는 0.0533 → 0.0540, XLog 전용 30,000/s는 0.0267 → 0.0270이다. 모두 변동 안이다. 이 rate에서는 수신 goroutine이 도착보다 빨리 비우므로, 한 번에 읽는 datagram이 대부분 1개다.
- **고rate**(XLog 전용, 각 4회): 70,000/s에서 기준은 2/4, 변경은 3/4 통과했다. 90,000/s에서는 기준이 4번 모두 실패했고, 변경은 3/4 실패했다. 잃은 수도 겹친다(기준 3,880~15,662, 변경 0~11,484). 손실은 kernel이 아니라 worker queue 포화(`udp Rejected`)에서 났다. 그러니 상한은 수신 syscall이 아니라 worker·저장 쪽이다.
- **판단**: 효과가 측정되지 않으므로 의존성과 코드를 넣지 않았다. 이 VM은 이후 Linux 측정 환경으로 쓴다.

### 17.3 XLog마다 반복하던 계산 제거 (Linux VM에서 판단)

- **근거**: VM에서 XLog 전용 90,000/s로 프로파일하고 할당을 봤다(30초 창). VM은 replay 2개와 서버(1.5 core)로 vCPU 4개가 찬다. 따라서 이 rate의 손실은 서버 설계가 아니라 VM 크기의 한계다. 남은 개선 방법은 XLog당 CPU를 줄이는 것이다.
  - tag 기여 slice가 XLog마다 20칸으로 잡혔다. byte 할당의 28%(3.28GB)다.
  - `tagKey`가 tag마다 문자열을 `[]byte`로 복사했다(1,050만 객체).
  - 날짜 문자열 `Format`이 레코드마다 돌았다(710만 객체).
  - visitor가 같은 객체 이름 문자열을 XLog마다 세 번 만들었다(900만 객체).
  - UDP pack마다 sender 주소 문자열을 만들었다(300만 객체). 이 값은 ObjectPack에만 쓰인다.
- **변경**(결과 값은 모두 같음):
  - `scoutertime`은 마지막 location의 기준 시각과 마지막 날의 이름을 기억한다. 둘 다 key의 순수 함수다.
  - tag 기여는 stack 배열에 모은 뒤 정확한 길이로 한 번만 복사한다. field 순서를 바꿔 56B가 48B가 됐다.
  - `tagKey`는 복사 없이 crc32를 계산한다. `FuzzCRC32String`으로 `crc32.ChecksumIEEE`와 같은지 확인한다.
  - tag table 경로는 cache한다(1,024쌍 상한).
  - visitor는 객체·객체 유형별 entry 이름을 그날 cache한다(4,096개 상한).
  - sender 주소 문자열은 ObjectPack일 때만 만든다.
- **결과**(기준과 번갈아 3회, 1,000건당 CPU 초):

| 환경 | mix 15,000/s | XLog 전용 30,000/s |
|---|---|---|
| Linux VM | 0.0504~0.0515 → 0.0478~0.0486 (약 5%↓) | 0.0259~0.0263 → 0.0238~0.0242 (약 8%↓) |
| macOS | 0.0935~0.0947 → 0.0909~0.0940 (변동 안) | — |

- macOS에서는 CPU의 대부분이 syscall이라 이 변경의 몫이 작다. 서버 배포 대상인 Linux에서 판단했다.
- 같이 고친 테스트 결함이 있다. `TestApplySettingsFromConfigReload`는 막 받은 XLog를 비동기 writer가 쓰기 전에 읽었고, writer가 이길 때만 끝났다. 수신이 빨라지자 5초를 넘겨 가끔 실패했다. 이제 그 txid가 저장될 때까지 기다린다(30회 통과, 중앙값 0.5초에서 0.15초로).

### 17.4 Linux VM에서 본 남은 비용과 넣지 않은 것 (§17.3 이후)

- **분포**(VM 프로파일, 30초 창):
  - XLog 전용 30,000/s: tag count가 서버 CPU의 약 39%다(flush pass 30%, 반영 15%). 그다음은 UDP worker의 ingest 22%, GC 약 10%다.
  - mix 15,000/s: tag 약 20%, text writer 14%, GC 13%다.
- **tag key·table 기준 경로 cache**: 프로파일에서는 `tagKey`(0.73초)와 `filepath.Join`(0.49초)이 사라졌다. 그러나 7쌍 A/B의 효율은 0.0247 → 0.0245로 변동 안이었다. 단일 회차끼리의 차이(최대 20%)는 프로파일 비교에도 같은 크기로 들어 있다. 그래서 넣지 않았다.
- **남은 후보는 하나하나 작다**(추정 1~2%, 측정 해상도 아래):
  - tag 파일 이름 문자열 연결(`path+".kfile"` 등)은 할당의 약 15%이고, CPU로는 약 1~1.5%다.
  - text writer의 존재 확인(`Index.Get`, 원본 TextWR과 같은 검사)과 read cache의 map 비용이 있다.
- **GOGC**(mix 15,000/s, 번갈아 2회): 코드는 바꾸지 않고, 운영 환경 변수의 효과만 확인했다.

| GOGC | 1,000건당 CPU 초 | 최대 RSS |
|---|---|---|
| 100 (기본) | 0.048~0.051 | 365~417MiB |
| 200 | 0.046~0.060 | 535~793MiB |
| 400 | 0.048~0.050 | 약 1.1GiB |

- GC 비율을 올려도 CPU 절약은 측정 변동 안이고, 메모리만 2~3배가 된다. 공유 호스트에서는 기본값 100을 유지한다.

- 다음 단계의 구조적 후보 검토와 측정 전제 교정(replay cardinality, 쌍 비교 판단 규칙)은 `docs/performance-structural-review-2026-10-20.md`에 있다.

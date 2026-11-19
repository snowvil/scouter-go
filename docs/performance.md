# 성능 목표와 검증 계획

## 확정된 조건과 미확정 입력

| 항목 | 상태 |
| --- | --- |
| 현재 규모·처리량 | 운영 환경별 입력. 이 저장소에 기록하지 않으며 측정 전 조건으로만 받는다 |
| 목표 | 기존 Java 서버보다 적은 자원으로 더 높은 완료 transaction/s와 안정적인 조회 지연. 수치 목표는 환경별 합의 사항 |
| 서버 | 다른 애플리케이션과 자원을 공유할 수 있는 호스트. 코어·메모리·디스크 종류는 측정 기록에 조건으로 적는다 |
| Scouter 할당 CPU/RAM/I/O | 미확정. 전체 호스트 자원을 성능 예산으로 쓰지 않음 |
| 트래픽 corpus | `scouter-replay`의 seed 기반 synthetic 생성기는 구현되어 있다. 실제 비식별 운영 corpus는 별도 확보·검증이 필요하다 |
| SLO/내구성 | p99 허용값, fsync 정책, 허용 drop, 보존 기간, 동시 조회 부하 미확정 |

## 계수와 처리량 정의

패킷 수는 transaction 수가 아니다. 한 XLog와 여러 profile/text/counter가 함께 들어올 수 있다. 동일 txid가 재전송될 수도 있으므로 corpus의 기대 transaction 집합과 비교한다.

- 입력: generated transaction, sent/received datagram, bytes, 재조립 완료, malformed/expired/kernel drop.
- 처리: accepted XLog, rejected XLog, filtered XLog, queue 대기, 처리 오류.
- 저장: 완료된 append/index, 재조회 가능한 고유 txid, sync 완료, 복구 후 고유 txid.
- Pulse: queued/filtered/dropped/send-failed/callback-completed, broker/consumer 확인 수. acks=0 callback은 영속 저장 증거가 아니다.

대표 acceptance 처리량은 측정 구간의 **저장·인덱스가 완료되고 조회로 확인된 유효 XLog transaction 수/초**다. 종료 후 backlog를 처리한 시간을 denominator 밖으로 빼지 않는다. 안정 구간 throughput과 drain 포함 전체 throughput을 함께 보고한다. crash durability는 별도 재시작 검증으로 보고한다.

## 비교 실험

1. 동일 corpus/설정/retention/durability/자원 한도를 사용해 Java 원본과 Go를 따로 실행한다. 동일 DB에 동시 쓰지 않는다.
2. synthetic은 200개 app/800개 object, 양·음수 hash, text hit/miss, 여러 profile 크기, 정상/오류, 지연 도착과 자정 경계를 포함한다. 실제 비율은 사용자 corpus 확보 전까지 가정으로 기록한다.
3. 처리량을 단계적으로(예: 이전 단계의 약 2배씩) 올려 손실·지연이 생기는 지점까지 측정한다. 단계 값은 측정 조건으로 기록하며 운영 목표치가 아니다.
4. 후보 CPU quota/RAM 예산을 단계적으로 늘려 임계점을 찾는다. 예: 2/4/8 core에서 비교하되 공유 서버에 무단 부하를 주지 않는다.
5. core-only와 Pulse-enabled를 각각 측정하고, Pulse-enabled에서는 broker·Collector가 감당하는 처리량까지 따로 확인한다.

제안 시험 구간은 5분 warm-up + 30분 steady + 2시간 soak다. 운영 전환 기준으로 확정되기 전에는 제안값으로 취급한다. 부하 생성기는 수집 서버와 분리하고 coordinated omission을 피하도록 예정 송신 시각부터 end-to-end latency를 측정한다.

## 필수 시나리오

| 시나리오 | 확인할 결과 |
| --- | --- |
| 정상 ingest + 동시 과거 조회 | TPS, p50/p95/p99, CPU, RSS, alloc/GC, IOPS, queue 증가 추세 |
| burst/queue full | reject/drop가 계수되고 무제한 메모리 증가가 없음 |
| 느린 디스크/disk full/index 실패 | 성공으로 오인하지 않고 오류·복구 상태를 보존 |
| SIGTERM | 새 입력 차단 → 수락된 작업 drain → writer flush/sync/close. 시간 초과 잔여 수 보고 |
| 강제 종료/재시작 | durability 설정별 손실 창과 tail/index 복구, 중복/누락 비교 |
| 날짜 변경/late data/retention | 시간대·날짜별 조회·파일 수명·삭제 안전성 |
| Kafka outage/slow broker | core 영향, queue 포화, retry/drop, 순서, 복구 후 consumer lag |
| 다수(수백 이상) connection/reconnect | FD/goroutine 수, session/agent reconnect, timeout, 자원 회수 |

## 측정 산출물

source SHA, Go/JDK/OS 버전, CPU quota, GOMAXPROCS/GOMEMLIMIT, 메모리/RSS, disk 종류·가용량, page-cache 상태, corpus hash/크기 분포, queue/cache 설정, flush/fsync 정책, concurrency와 raw 결과를 함께 저장한다.

`make bench`의 codec ns/op·B/op·allocs/op는 해당 함수만의 결과다. 참고 Go README의 enqueue benchmark나 로컬 codec 수치를 목표 처리량 달성 증거로 쓰지 않는다. 단순 benchmark 개선은 live Agent/Client/Pulse 성능을 보장하지 않는다.

Go runtime 메모리 한도 의미는 [공식 GC guide](https://go.dev/doc/gc-guide#Memory_limit), race/fuzz 도구는 [race detector](https://go.dev/doc/articles/race_detector)와 [fuzzing](https://go.dev/doc/security/fuzz/)을 따른다.

## Phase 2E 로컬 baseline과 도구

재현 가능한 측정 도구는 `cmd/scouter-replay`(`make replay REPLAY_ARGS=...`)다. 자식 서버를 임시 DB와 loopback 주소로 띄운다. Java `DataUdpAgent`와 같은 framing으로 Object/Text/Counter/XLog/Profile을 예정 송신 시각 기준으로 pacing해 보낸다(coordinated omission 회피). 동시에 TCP Client 조회를 병행한다. 종료(SIGTERM drain 또는 SIGKILL) 후에는 같은 DB로 재시작해, 다시 조회되는 고유 txid를 기대 집합과 비교한다. report JSON에는 다음을 기록한다.

- payload mix·seed·profile 크기·dictionary hit
- 서버 flag·`GOMAXPROCS`/`GOMEMLIMIT`·fsync 정책·압축, 파일시스템과 여유 공간
- 송신 lag, 조회 지연, UDP/ingest 계수와 종료 잔여, CPU(rusage)·RSS 시계열
- 재시작 시간과 복구 이벤트

외부 loopback 서버는 `--server-udp`·`--server-tcp`로 연결한다. 이 모드는 서버를 종료·재기동하지 않고 quiescence 뒤 TCP 조회만 하므로 영속성, 종료 계수, 서버 reject/backlog와 디스크 상태를 입증하지 않는다. 처리량 denominator는 송신 시작부터 quiescence까지이며 `--verify-delay` 대기와 조회 시간은 제외한다. Java 실행·종료 절차와 예시는 [기준치 실행기](../scripts/bench-java-server.md)를 따른다.

로컬(M1 Pro 8코어, loopback 공유) 결과와 측정으로 고친 병목은 [Phase 2E 기록](validation/2026-10-13-phase2e.md)과 [개선 기록](validation/2026-10-14-phase2e-improvements.md)에 있다. 개선 뒤 object 200개·2000 tx/s에서 누락 0, 압축 모드 1000 tx/s 누락 0, 10분 soak 99.5%를 측정했다. 이 수치는 목표 호스트 결과가 아니며 운영 처리량 목표의 달성 증거가 아니다. 2026-10-14 [권고 적용 기록](validation/2026-10-14-recommendations.md)에서 Store gate stall 제거·codec allocs 감소·UDP queue 기본값 1024(4000 tx/s·200 obj 누락 0, 20s)를 측정했고 `GOMEMLIMIT`는 기본값 미설정으로 두었다. 주기 fsync 비용, gzip writer, 장기 soak RSS 증가가 다음 병목 후보다.

### 비교 측정 도구 (2026-10-20)

- `scouter-replay --text-cardinality N`: service·SQL·method·error·UA·referer text를 종류마다 최대 N개만 만들고, 그 뒤로는 재사용한다. 기본값 0은 기존 동작이다. 이때는 hit가 아닌 참조마다 새 text가 생기므로 tag와 text dictionary가 끝없이 커진다(cardinality 폭주 시나리오).
  - 실제 서비스처럼 종류가 유한한 workload는 이 옵션으로 잰다. 근거는 [구조 검토](performance-structural-review-2026-10-20.md) §2.1이다.
- `scripts/bench-ab.sh --base BIN --new BIN`: 두 빌드를 번갈아 쌍으로 돌린다(기본 8쌍, 순서는 AB, BA를 교대). 판단은 완료 XLog 1,000건당 서버 CPU의 쌍 차이(중앙값과 부호 일치 수)로 한다. 같은 빌드끼리도 회차마다 10~13% 흔들리므로, 단일 회차나 단일 프로파일은 근거로 쓰지 않는다. 같은 바이너리 두 개를 비교한 A/A 측정(mix 10쌍)에서도 쌍 중앙값 ±1.5%와 부호 일치 8/10이 나왔으므로, 효과로 인정하는 기준은 대략 2% 이상이면서 10쌍 기준 부호 일치 9/10 이상이다(8쌍이면 8/8에 가깝게). 더 작은 차이는 쌍을 늘리거나 profile로 확인한다([개선 대상](improvement-candidates-2026-10-21.md) §18).
- `scouter-go --lock-stats`(측정 전용): `/health`에 `locks`를 추가한다. 항목은 engine lock(`engine`), Store gate(`store`), tag gate(`tags`)의 획득 수·점유·대기 합계와, Store gate 점유 중 각 writer의 몫(`store.xlogWriter`, `store.profileWriter`, `store.textWriter`)이다.
  - 구간의 두 `/health` 차이를 구간 길이로 나누면 점유율이다.
  - 획득마다 시계를 두 번 읽는 비용이 있어 기본으로는 꺼져 있다.

## 측정 기록

| 기록 | 내용 |
| --- | --- |
| [XLog 최대 지속 처리량](validation/2026-10-17-xlog-max-benchmark.md), [격리 호스트 절차](validation/isolated-benchmark.md) | 단일 호스트 loopback 최대 지속 rate와 격리 호스트용 절차 |
| [구조 검토](performance-structural-review-2026-10-20.md) | Linux VM의 비용 분해, `GOMAXPROCS`, lock 점유 |
| [대규모 연결](validation/2026-10-22-connection-scale.md) | Agent TCP 연결 수·재시작 폭주·handshake 예산 |
| [개선 후보 실측](validation/2026-10-23-profile-scout.md), [개선 대상](improvement-candidates-2026-10-21.md) §7~§20 | 단계별 A/B 결과와 다음 후보 |

모두 조건이 붙은 측정 결과이며 운영 처리량 예측이 아니다.

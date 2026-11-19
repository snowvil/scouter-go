# 주기 처리량·지연 로그(`--stats-interval`) 검증 기록

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

2026-10-14, branch `codex/stats-log`(master `e5f9d2c`에서 분기). 기존 Scouter 서버를 대체해 관찰할 때 대시보드 없이 서버 로그만으로 부하와 지연을 읽을 수 있게 하는 운영 보조 기능이다. wire/저장 형식과 조회 의미에는 영향이 없다.

## 변경

| 커밋 | 내용 |
|---|---|
| `3bc07fc` | `--stats-interval`(기본 10s, 0 off): 구간마다 한 줄 요약 로그, `/health`의 `stats` JSON. 고정 bucket lock-free 히스토그램(`internal/server/stats.go`), ingest(UDP pack 1개 처리)·client(명령 1건 handler) 지연 관측 지점(`runtime.go`), Linux `/proc/self/statm` RSS, replay 도구의 로그 분류 |
| `737f3a3` | 문서 정리(코드 변경 없음) |
| `b7276da` | 독립 리뷰에서 나온 항목 반영: pulse `fail`에 producer 전송 실패·drop 합산, `expired` 분리, 상한 2^27µs 주석 정정, `bits.LeadingZeros64`, max 지연 시 quantile 0 방지, flaky 테스트 조건, interval 0/음수·경계 테스트, 문서 의미 명시 |
| `a886a90` | `internal/pulse/kafka` `complete`가 counter 갱신 전에 batch를 release해 `Flush` 직후 `SentRecords`가 덜 세일 수 있던 순서 수정(gate에서 1/40 재현) |

출력 예(로컬 replay 300 tx/s·object 10):

```
stats 10s: xlog 300/s (5,984)  packs 741/s  udp 315/s reject 0 malformed 0 expired 0 | ingest p50 47µs p99 1.8ms max 42ms reject 0 drop 0 | client 2.0/s p50 12ms p99 39ms max 39ms active 2 err 0 | agents 10  heap 61MiB sys 102MiB gc 15 goroutines 24
```

각 항목의 의미는 [runtime](../compatibility/runtime.md)의 `--stats-interval` 행에 있다.

## 설계 근거

- 수집 경로 추가 비용은 `time.Now()` 2회와 atomic 갱신 3회(bucket, sum, max CAS)뿐이다. 할당·lock·대기가 없다. 히스토그램은 217 bucket × 8B로 2개 합쳐 약 3.5KiB다.
- bucket은 µs의 2의 거듭제곱마다 8개(상한 12.5%), 2^27µs(약 134s) 이상은 overflow bucket이다. quantile은 nearest-rank(ceil)로 bucket 상한을 보고한다.
- 구간 delta의 원천 counter(ingest/udp/tcp/pulse/kafka Stats)는 모두 단조 증가 atomic이며 감소·reset이 없어 uint64 뺄셈이 안전하다.
- stats goroutine은 `Run`의 sampling WaitGroup에 들어가며 `ctx` 취소로 즉시 끝난다. `ObjectCount`의 engine lock 대기는 Serve 종료 뒤·engine Close 전이라 교착이 없다.
- `GOMEMLIMIT`·RSS 의미는 바꾸지 않는다. `heap`/`sys`는 `runtime/metrics`(STW 없음), `rss`는 Linux에서만이며 다른 OS는 생략한다.

## 검증

- `internal/server`, `cmd/scouter-replay` `-race` PASS; stats 관련 테스트 `-race -count=20` PASS; kafka 두 테스트 `-race -count=40` PASS.
- 로컬 replay(`scouter-replay`, 기본 flag) 2회: 300 tx/s·10 obj·22s 7200/7200 PASS, 500 tx/s·20 obj·25s 13500/13500 PASS. 서버 로그에 10s마다 stats 줄이 찍히고 replay가 오류 줄로 세지 않는다.
- 독립 리뷰(정적)에서 나온 항목은 `b7276da`로 반영했다. 그 밖에 `bits.LeadingZeros64`·count atomic 제거·max 0 경로를 반영했다. 첫 구간 window가 Open 시각 기준인 점, overflow bucket quantile이 하한인 점, Client 지연이 종결 byte·flush를 제외하는 점은 문서에 남긴다.
- 전체 gate 결과는 아래 표에 기록한다.

| 명령 (`a886a90` archive) | 결과 |
|---|---|
| `make check` | **PASS** (221s) |
| `make race` | **PASS** (274s) |
| `make fixtures-check` | **PASS**, Java fixtures 141 |
| `make inventory-check` | **PASS** |
| `make storage-check` | **PASS** (13s) |
| `make fuzz` (11 target, 10s) | **PASS** (129s) |
| `make build` + java-integration | **PASS**, `integration_passed=True` |
| `make pulse-fixtures-check` | **PASS**, 124 case |

gate 빌드 `dist/scouter-go` SHA256 앞부분 `ced85c32cb95b0a2`. 직전 커밋(`737f3a3`) archive의 gate는 `make race`에서 `internal/pulse/kafka` `TestProducerMetadataAndProduceVersionMatrix`가 1회 실패했고(`a886a90`으로 수정) 나머지는 PASS였다.

## 남은 문제

- 수집 경로 추가 비용의 처리량 영향은 replay 전후 비교로 분리 측정하지 않았다(두 replay 모두 누락 0, 같은 조건의 이전 측정과 TPS 차이는 잡음 범위).
- Client 명령별(느린 명령 상위 N) 분리 출력, Agent request 지연, UDP queue 대기 시간은 포함하지 않았다. 필요하면 후속 단위로 추가한다.
- Linux `rss` 출력은 Linux 호스트에서 실행하지 않았다(코드 경로만 분리).

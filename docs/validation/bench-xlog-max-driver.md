# XLog 최대 지속 rate 탐색 driver

현재 CLI의 사용 범위와 판정 기준을 정리한다. 마지막 "개발 검증" 절은 2026-10-17 실행 이력이며 최신 소스의 재검증 결과가 아니다. Java 서버 준비는 [기준치 실행기](../../scripts/bench-java-server.md), 당시 Java/Go 비교 결과는 [최대 처리량 기록](2026-10-17-xlog-max-benchmark.md)을 따른다.

## 범위와 계약

- 구현 경로는 `cmd/scouter-replay/**`와 `scripts/bench-xlog-max.py`다. 생성기·탐색 driver의 결과를 서버 기능 전체의 검증으로 확대하지 않는다.
- 기존 `--senders N`은 이미 sender별 UDP 소켓·generator·batcher, transaction index 분할, 객체 heartbeat/counter 분배를 지원한다. 이를 그대로 재사용한다. XLog 객체 선택은 기존 txid 기반 계약을 유지한다.
- `--sink-only`: 로컬 UDP discard socket을 열고 같은 payload/framing/pacing으로 송신한다. `generator_only.transactions_sent_per_second`, datagram/s, byte/s를 기록하며 **서버 TPS·저장 검증을 수행하지 않는다**. `--rate`를 높여 송신 지연과 not_sent를 비교해야 생성기 한계를 알 수 있다.
- `--server-udp IP:port --server-tcp IP:port`: literal loopback만 허용한다. 서버 시작·종료·재시작·DB 접근을 하지 않고 quiescence 후 `TRANX_LOAD_TIME_GROUP`으로 예정 XLog 전체를 조회한다. `--server-pid`는 `ps` RSS/%CPU 수집에만 사용한다. PID와 서버 endpoint의 연관성은 운영자가 제공한 정보다.
- 외부 모드의 DB/compression/runtime/server-arg 옵션은 거절한다. 실제 Java 설정은 외부 서버를 띄운 쪽에서 기록한다. 전용 격리 DB를 사용하고, 다른 부하와 섞지 않는다. 같은 DB에서 별도 실행을 반복할 때는 `--seed`를 달리해 txid 재사용을 피한다.
- macOS `netstat -s -p udp`, Linux `/proc/net/snmp`의 실행 전후 증감은 `kernel_udp`에 기록한다. Linux `InErrors`가 포함하는 `RcvbufErrors`를 이중 합산하지 않는다. 명령 실패·누락 필드·카운터 초기화는 unavailable이며 0 손실로 간주하지 않는다. **호스트 전체 통계**라 다른 프로세스 영향이 섞일 수 있다.
- `--sample-health`: 관리하는 Go 서버에 loopback 임시 HTTP 포트를 열어 `/health`를 1초마다 조회한다. sample은 최대 7,200개이며 초과분을 따로 센다. 외부 `ps` sample도 같은 상한이다. driver는 누락/초과로 관측이 불완전하면 Go backlog 검증 실패로 분류한다.
- 기본 `make replay`의 restart 검증과 report 필드 의미를 유지한다. 외부 모드에서는 기존 throughput의 `*_server_exit*` 필드 분모 끝점이 quiescence이고 재시작 내구성을 증명하지 않음을 `definition`에 명시한다.

## driver 판정과 탐색

기본 steady 30초, warmup 3초, cooldown 5초다. 최초 rate에서 성공하면 두 배로 올리고 실패 상한을 만나면 성공 하한/실패 상한 사이를 이분 탐색한다. 최초 rate부터 실패하면 절반으로 낮춰 성공 하한을 찾는다. resolution·최소/최대 rate·최대 시도 수로 종료를 제한한다.

성공 조건:

1. replay pass, TCP 검증 complete, unique verified == scheduled, 누락·중복·extra·objHash mismatch 없음.
2. 생성기 송신/생성/socket 오류 없음, 예정 종료 후 overrun 기본 0.5초 이하.
3. `steady_verified_rate_tps >= 0.99 * rate`, 커널 UDP 손실 0.
4. Go UDP reject 0, steady health에서 ingest reject/storage error/dropped XLog 0.
5. Go queue의 마지막 steady 1/3 평균 - 처음 1/3 평균이 `--backlog-growth` 이하. 기본값 64이며, 완전한 sample이 3개 이상 필요하다. queue는 datagram 단위다.
6. quiescence 성공 및 송신 종료 후 drain wait가 기본 2초 이하.

외부 Java 서버에서 관측할 수 없는 UDP reject/backlog는 `unknown`으로 기록하고, 그 자체만으로 실패시키지 않는다. 외부 판정의 근거는 TCP 무누락·커널 손실·drain이다. Go와 Java의 관측 범위가 다르므로 같은 수준의 queue/reject 증거로 해석하지 않는다.

`steady_verified`는 steady에 **송신 예정이었던** XLog의 사후 조회 수다. transaction별 실제 저장 완료 시각은 알 수 없다. 최종 완료 TPS의 기존 drain-inclusive 값도 함께 확인해야 한다. 상한까지 성공하면 최대값이 아니라 **그 상한까지의 성공 하한**이다. 단일 실행·단조성 가정으로 탐색하므로 최종 후보를 반복 검증해야 한다.

출력: 시도별 `command.json`, `replay.log`, `report.json`, `decision.json`; 최종 `summary.json`, `summary.md`. 출력 디렉터리는 새 경로여야 하며 기존 결과를 덮어쓰지 않는다. `--dry-run`은 실행 없이 지수 증가 후보만 기록한다(실패 정보를 알 수 없으므로 이분 탐색 분기는 출력하지 않음).

## 정확한 실행 예

저장소 root에서:

```sh
# Go toolchain 준비는 README의 빌드 절을 따른다.
make build

# 생성기 송신능력 smoke: 서버 TPS가 아니다.
dist/scouter-replay --sink-only --rate 2000 --duration 10s --warmup 0s \
  --senders 2 --max-overrun 100ms --work-dir /tmp/replay-sink-new

# 두 sender, restart + 전체 TCP 검증.
dist/scouter-replay --rate 500 --duration 4s --warmup 0s --senders 2 \
  --sample-health --max-overrun 100ms --work-dir /tmp/replay-two-senders-new

python3 scripts/bench-xlog-max.py --dry-run --smoke --output /tmp/xlog-dry-new

# 구조 smoke. 기본 backlog-growth=64 대신 짧은 sample 변동 16만 명시적으로 허용.
python3 scripts/bench-xlog-max.py --smoke --senders 2 --backlog-growth 16 \
  --cooldown 1 --output /tmp/xlog-smoke-new

# 이미 준비된 전용 loopback 서버. 아래 endpoint는 실제 준비한 값으로 맞춘다.
dist/scouter-replay --server-udp 127.0.0.1:6100 --server-tcp 127.0.0.1:6100 \
  --duration 4s --rate 500 --senders 2 --seed 501 --work-dir /tmp/replay-external-new
# CPU/RSS를 수집하려면 해당 서버의 실제 PID를 --server-pid 인자로 추가한다.
```

최종 측정 매트릭스 예(모든 명령은 직렬 실행, 서버 종류 간에도 중복 부하 금지):

```sh
# CPU budget와 sender 수만 바꾸고 나머지 payload/검증 조건은 고정한다.
for procs in 2 4; do
  for senders in 1 2 4; do
    python3 scripts/bench-xlog-max.py --kind go --gomaxprocs "$procs" \
      --senders "$senders" --objects 200 --batch 32 --profile-ratio 0.3 \
      --profile-sizes '0:0.1,1024:0.6,8192:0.25,131072:0.05' \
      --query-concurrency 2 --duration 30 --warmup 3 --cooldown 5 \
      --initial-rate 500 --max-rate 20000 --max-attempts 20 --resolution 100 \
      --backlog-growth 0 --output "/tmp/xlog-final-go-p${procs}-s${senders}"
  done
 done

# 동일 시나리오의 압축 변형: 별도 새 output으로 --compress 추가.
# Java 기준치: scripts/bench-java-server.py로 전용 서버를 기동하고 endpoint/PID/설정을 기록한다.
python3 scripts/bench-xlog-max.py --kind external \
  --server-udp 127.0.0.1:6100 --server-tcp 127.0.0.1:6100 \
  --senders 2 --objects 200 --batch 32 --profile-ratio 0.3 \
  --profile-sizes '0:0.1,1024:0.6,8192:0.25,131072:0.05' \
  --query-concurrency 2 --duration 30 --warmup 3 --cooldown 5 \
  --initial-rate 500 --max-rate 20000 --max-attempts 20 --resolution 100 \
  --seed 100001 --output /tmp/xlog-final-java-s2
# 위 명령에도 실제 Java PID를 --server-pid 인자로 추가하면 ps sample을 수집한다.
```

최종 측정 전 같은 payload/send ratios로 sink `--rate`를 서버 후보보다 높여 생성기 여유를 별도로 입증해야 한다. 2,000/s 이하 개발 smoke를 생성기 최대 송신률 증거로 확대하지 않는다. `GOMEMLIMIT`·CPU quota·disk/fsync·압축·조회 병행·실제 서버 설정은 시나리오별 기록해야 하며 공유 호스트 전체 자원을 전용 예산으로 가정하지 않는다.

## 개발 검증 (2026-10-17, 로컬 공유 Mac)

- `go test ./cmd/scouter-replay/...`: PASS.
- `go test -race ./cmd/scouter-replay/...`: PASS.
- `go vet ./cmd/scouter-replay/...`, `gofmt -l cmd/scouter-replay`, `git diff --check`: PASS.
- `python3 -m unittest discover -s cmd/scouter-replay -p '*_test.py'`: 4개 PASS. 오류 원인, backlog 증가/외부 unknown, 지수 증가→이분 탐색, smoke 상한/loopback 검증.
- `make replay GO=<go>`: PASS, `pass=true`, scheduled/sent/verified 2,400/2,400/2,400, missing/extra/dup 0, kernel loss 0.
- sink 10초, rate 2,000, sender 2: PASS, 20,000/20,000 송신, kernel loss 0.
- sink 초기 `--max-overrun 0s`: FAIL, 20,000 중 19,999 송신. 100ms 허용으로 재실행했다.
- sender 2, 4초 500/s: PASS, 2,000/2,000 조회 검증, health sample 5개.
- driver dry-run: 지수 증가 후보 출력을 확인했다.
- driver 4초씩 500→1,000→2,000/s: 세 시도 PASS. `--backlog-growth 16`을 명시했고 실제 증가량 2.5/0/4.5였다. `smoke_only=true`, 최대 rate를 찾은 것이 아니라 제한 상한까지 동작을 확인했다.
- 별도 Go 서버를 external 모드로 두 번 조회: 각각 2초 100/s, 200/200 verified, restart 미시도, ps sample 각 4개. 각 replay 후 서버가 계속 실행 중임을 확인했고 테스트 harness가 마지막에 SIGTERM으로 정상 종료(0)했다.
- 모든 부하 smoke는 다른 부하와 겹치지 않게 직렬 실행했다. 실행 뒤 서버 잔여 프로세스가 없음을 확인했다.

미실행: Java 원본 서버 실측, Linux 실제 `/proc` 수집(파서 fixture만 검증), 30초 이상 지속 탐색·최대 생성기 송신률·운영 환경 검증. 서버 코드/codec를 변경하지 않아 전체 `make race/fuzz/fixtures-check/inventory-check`는 이번 범위에서 반복하지 않았다.

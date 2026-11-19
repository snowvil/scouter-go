# 대규모 Agent TCP 연결 시험 (2026-10-22)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

1단계 연결 예산(handshake·Agent·Client 분리, 거절 `SO_LINGER 0`, 사유별 10초 1줄 로그, accept
EMFILE backoff)과 3단계 Agent relay 고아 상한을 실제 수천 개 연결로 처음 시험했다.

## 결론

- 정상 규모(A): 2000·4000·8000 Agent 모두 거절 0, 서버 쪽 Agent 세션 끊김 0. 연결당 goroutine은
  1개(`agentPool.handle`), fd 1개. idle 세션 RSS는 연결당 약 33KiB, UDP·조회 병행 시 Agent당
  약 43KiB(세션+object/counter 상태). 8000 idle 세션의 KEEP_ALIVE 비용은 1코어의 약 16%.
- 재시작 폭주(B): SIGTERM 재시작 뒤 4000·8000 Agent 전원이 5.05초 안에 다시 붙었다. 거절 0.
  회복 시간은 서버 예산이 아니라 Java Agent의 "connect 실패 → 3초+1초" 대기가 정한다. SIGKILL
  재시작은 `lock.dat` 5초 규칙 때문에 9.1초(`1d91997` 기준, 이후 `2d60689`에서 Go writer가 남긴 fresh
  `lock.dat`를 즉시 인수하도록 수정됨, 재측정 안 함).
- handshake 예산(H): 8000 Agent가 1초 안에 동시에 붙어도 `--max-handshakes=64`에서 거절 32건,
  4.1초 만에 전원 연결. 기본값 2048은 loopback 기준으로 여유가 크다.
- 상한 초과(C): 초과 Agent 1000개가 1초마다 재시도해 초당 약 900건이 거절됐다. 로그는 10초에
  1줄로 12줄, 정확한 수는 `TCPStats`에 남았다. 기존 3000 세션·UDP 수집·Client 연결은 영향이 없었다.
- fd 한계(D): accept backoff는 바쁜 루프 없이(약 1회/초) 동작했고 기존 세션은 유지됐다. 그러나
  **Agent socket이 fd를 다 쓰면 저장 파일 open이 실패해 counter·visitor·tag·checkpoint 쓰기가
  계속 실패하고, 새 Client도 접속하지 못한다**(위험 R1, `1d91997` 기준). `3c855ed`에서 기동 시 fd 한도에
  맞춰 예산을 낮추도록 고친 뒤 다시 실행해 저장 쓰기 실패 0, Client probe 전부 성공을 확인했다(수정 후 재실행).
- 고아 relay(E): 고아는 32개에서 멈췄고 넘친 relay 32건은 해당 Agent 세션을 닫았다. Agent는 1초
  안에 다시 붙었다. 로그는 1분에 1줄.

## 측정 조건

| 항목 | 값 |
|---|---|
| 환경 | Lima VM, Ubuntu 24.04 arm64, 4 vCPU, 4GiB, swap 없음. 서버·부하기가 같은 VM(loopback) |
| 서버 | `cmd/scouter-go` commit `1d91997`(작업 트리 비테스트 변경 없음), `GOOS=linux GOARCH=arm64 CGO_ENABLED=0` |
| 서버 옵션 | `--conf= --stats-interval 5s --http 127.0.0.1:6380`, 새 DB, retention off, `--sync-interval` 기본 4s |
| 커널 | `somaxconn 4096`, `tcp_max_syn_backlog 256`, `tcp_syncookies 1`, `ip_local_port_range 32768-60999`, `rmem_max 8388608`(시험 전 설정) |
| fd | 서버·부하기 `ulimit -n 65536`(D는 서버만 2048) |
| Agent | `scouter-replay agents`, objHash당 세션 1개(`net_collector_tcp_session_count` 기본), 20% V1(`TCP_AGENT`), 80% V2 |
| UDP(A·B·C·D) | Agent마다 ObjectPack·PerfCounterPack 2초 간격, XLog 200/s(전체), profile 없음, text hit 99% |
| Client | 조회 worker 2개(1초마다 `OBJECT_LIST_REAL_TIME` + `TRANX_REAL_TIME_GROUP_LATEST`), 5초마다 새 연결 LOGIN probe |
| 시간 | 각 120초. A·idle은 첫 연결을 20초, B·C·D는 10초에 걸쳐 분산, H는 분산 없음 |

"settled" 세션은 연 지 1초(`--settle`)가 지난 세션이다. 예산 거절은 그 전에 끊기므로 회복 시간은
마지막 세션이 열린 시각으로 계산했다. CPU는 서버 utime+stime(100% = 1코어), 부하기와 같은 VM이다.
CPU 효율 A/B 판단에는 쓰지 않는다.

## 시나리오별 결과

| 시나리오 | 조건 | 결과 | 판정 |
|---|---|---|---|
| A2000 | 2000 Agent + UDP + Client | 20.8초에 전원 연결, 거절 0, 끊김 0. RSS 167MiB(기동 13MiB), fd 2051, goroutine 2042, thread 11, CPU 중앙값 60%. ingest p99 ≤ 63µs(5초 창), `OBJECT_LIST_REAL_TIME` p99 29ms, probe 23/23 | pass |
| A4000 | 4000 Agent | 거절 0, 끊김 0. RSS 252MiB, fd 4052, goroutine 4042, CPU 65%. ingest p99 ≤ 71µs, OBJECT_LIST p99 39ms, probe 23/23 | pass |
| A8000 | 8000 Agent | 거절 0, 끊김 0. RSS 419MiB(최대 450MiB), fd 8052, goroutine 8042, thread 13, CPU 92%(최대 172%). ingest p99 ≤ 111µs(5초 창 최대 160ms 1건), OBJECT_LIST(8000 object) p99 180ms, probe 23/23 | pass |
| idle8000 | 8000 Agent, UDP·Client 없음 | RSS 269MiB → 연결당 32.8KiB, CPU 16%. KEEP_ALIVE 간격 모두 약 10초 | pass |
| B4000 | 40초에 SIGTERM, 즉시 재기동 | 전원 EOF → 첫 재시도 refused(4000건, Agent당 1회) → 5.05초 만에 전원 재연결. handshake·Agent 거절 0. 로그 42줄(기동 2회분) | pass |
| B8000 | 같은 조건 8000 | 5.05초 만에 전원 재연결, refused 8000, 거절 0. 로그 41줄 | pass |
| Bkill8000 | 40초에 SIGKILL | `lock.dat` heartbeat로 재기동 5회 거부(약 6초) → 9.1초 만에 전원 재연결(`1d91997` 기준, 이후 `2d60689`에서 수정됨, 재측정 안 함). refused 15957, connect reset 43, 명령 전 reset 26(아래 참고). index relink 복구 줄 6개 | pass |
| H8000:64 | `--max-handshakes=64`, 8000 Agent가 1초 안에 접속 | handshake 거절 32, 로그 1줄, 4.1초에 전원 연결 | pass |
| C4000:3000 | `--max-agent-connections=3000`, 4000 Agent | 3000 연결 유지. 초과 1000개가 1초마다 재시도해 120초에 `AgentRejected` 110242. 로그 12줄(`N more rejected since the last line`). 기존 세션 끊김 0, KEEP_ALIVE 계속, UDP 200 XLog/s 유지, `ClientRejected` 0, probe 23/23, CPU 65%(A4000과 같음), ingest p99 ≤ 103µs | pass(2회째 실행) |
| D4000:2048 (`1d91997`) | 서버 `ulimit -n 2048` | `AcceptRetries` 121(약 1회/초), CPU 25%, `tcp.accept` 로그 10초 1줄(12줄). 기존 2006 세션 끊김 0. 접속 못 한 1994 Agent는 커널 accept queue에서 60초(so_timeout) 기다린 뒤 다시 접속. 저장 쓰기 실패 다수(R1), Client probe 13/14 timeout | **fail(R1)** → 수정 후 pass(아래) |
| E | 200 Agent가 명령에 45초 늦게 응답, 60 Client가 `OBJECT_ENV` 후 8초에 포기 | Client 진전 상한(30초) 뒤 고아 32개에서 정지, 넘친 relay 32건은 Agent 세션 close → Agent 1초 안에 재연결(최소 171/200). 고아 32건은 45초에 응답을 받고 세션 유지(completed). 고아 로그 2줄 | pass |

참고 수치:

- 연결당 메모리: idle 32.8KiB(RSS). A2000→A8000 기울기 (419−167)/6000 ≈ 43KiB/Agent. 문서
  추정치 14KiB는 Go heap+stack이고 RSS는 GC 여유분을 포함한다.
- KEEP_ALIVE 간격: idle에서 약 10초, 부하 시 p50 10.2초·최대 15.05초. 서버는 5초 ticker로 확인하고
  마지막 쓰기 후 5초가 지난 세션에만 보낸다. 다음 tick이 5초를 겨우 못 채우면 한 주기를 건너뛰어 사실상
  10초 간격이다. Java Agent의 so_timeout 60초보다 충분히 짧다. Java `TcpAgentManager`도 같은 5초 scan +
  5초 조건 구조라(TcpAgentManager.scala:30-45, TcpAgentWorker.scala:83) 비슷할 것으로 보이나 Java로는
  측정하지 않았다.
- 서버 로그: A·B·C·H는 기동/종료 줄 외에 연결 관련 줄이 예산 로그뿐이다. Agent마다 줄을 쓰지 않는다.
- DB 크기: XLog가 들어온 object마다 visitor HLL 파일이 하루 1개·시간당 1개(각 699,060B) 생긴다.
  8000 object의 120초 실행 DB가 약 10GiB(visitor 파일 약 15,000개)였다. Java `VisitorDB`/
  `VisitorHourlyDB`와 같은 형식·배치다(VisitorHourlyDB.scala:102-124). Agent 수가 많으면 디스크 계획에
  반영해야 한다. 첫 A8000은 VM 디스크가 차서 중단됐고(서버는 `no space left on device`를 rate limit해
  기록하며 계속 동작), 이후 스크립트는 시나리오마다 크기만 기록하고 DB를 지운다.

## 발견한 위험

### R1. fd 고갈 시 저장 쓰기와 Client 접속 실패 (D) — `3c855ed`에서 수정, 재실행 확인

재현(`1d91997`): `scripts/connection-scale.sh --out DIR D4000:2048`(서버 `ulimit -n 2048`, 4000 Agent).

- Agent socket이 fd를 거의 다 쓰면(fd 2048/2048) 저장 계층의 `open`이 `too many open files`로
  실패한다. 120초 동안 `ingest: daily writer failed (S122) ... 5m.kfile` 484,000건,
  `visitor writer failed (S199)` 21,591건, `tag count flush`, `periodic sync: index checkpoint`, request
  log open 실패가 이어졌다(줄은 rate limit됨). UDP 수집은 계속 받지만 일부 데이터가 저장되지 않는다.
  같은 상황의 stats 줄은 `/proc/self/statm`을 열지 못해 rss 항목이 빠졌다.
- accept가 막히면 Client 예산과 상관없이 새 Client 연결도 들어오지 못한다(probe 14회 중 13회 LOGIN
  timeout). 이미 연결된 Client 세션 2개는 정상이었다.
- 접속하지 못한 Agent는 커널 accept queue에 있어 Agent는 연결된 것으로 보고 KEEP_ALIVE를 60초 기다린
  뒤 끊고 다시 시도한다. Agent 애플리케이션 thread에는 영향이 없다.
- 기본 예산 합(2048+16384+1024)이 fd 한도보다 크면 예산 전에 fd가 먼저 바닥난다. Go 1.19+ 런타임은
  Linux에서 soft `RLIMIT_NOFILE`을 hard 한도까지 올리므로 systemd 기본(soft 1024, hard 524288)에서는
  잘 일어나지 않는다. hard 한도 자체가 낮게 설정된 호스트에서 일어난다.
- 수정(`3c855ed`): 기동 시 open file 한도에서 DB 예비분 `max(1024, 한도/4)`를 뺀 값에 맞춰
  Agent/handshake/Client 예산을 낮추고 WARNING을 남긴다(`ulimit -n 2048`이면 832/128/64).

#### 수정 후 재실행 (`3c855ed`, D4000:2048)

조건은 위와 같다(서버 `ulimit -n 2048`, 4000 Agent, UDP 200 XLog/s, Client worker 2 + probe 5초,
120초). 서버는 `3c855ed`를 별도 빌드로 만들었다(해당 commit만 포함).

| 확인 항목 | 결과 | 판정 |
|---|---|---|
| 기동 WARNING | fd 예산 줄 1줄: `the open file limit 2048 leaves too few descriptors ... lowered from 16384/2048/1024 to 832/128/64 ... Raise the limit (ulimit -n, systemd LimitNOFILE) to keep the defaults`. 함께 `net_tcp_service_pool_size=100 exceeds --max-connections=64` WARNING 1줄이 더 나온다(낮춘 Client 예산의 부수 경고) | pass(경고 2줄) |
| 저장 쓰기 실패 | `too many open files`·writer failed·flush·periodic sync·request log 오류 0줄. 종료 통계 `StorageErrors`·`WriteErrors`·`textWriteErrors`·`maintenanceErrors`·logger `FileErrors` 모두 0. fd 최대 894 | pass |
| 기존 세션·UDP | 832 세션 유지(명령 뒤 끊김은 종료 시 local_close뿐), KEEP_ALIVE 계속(최대 간격 15.0초), UDP 200 XLog/s·reject 0, ingest p99 ≤ 143µs, `AcceptRetries` 0 | pass |
| Client | probe 23/23 성공, 조회 worker 238/238 성공(OBJECT_LIST p99 60ms), `ClientRejected` 0, Client 연결 최대 3 | pass |
| 초과 Agent 거절 로그 | Agent 예산 줄 12줄(10초 1줄, `~31,700 more rejected since the last line`), handshake 예산 줄 1줄(`HandshakeRejected` 15). `AgentRejected` 357,892 | pass |

관찰: 초과 Agent 3168개가 1초마다 재시도해 초당 약 3000건이 거절됐다. CPU 중앙값은 89%(같은 VM의
부하기 포함)였다. 거절된 Agent는 수집 TCP 세션이 없지만, UDP 수집은 계속된다(object 4000 alive). 대신
서버에서 Agent로 보내는 요청(스레드 덤프, 텍스트 캐시 reset 등)은 그 Agent들에 닿지 않는다. 텍스트 캐시
reset은 3168개를 `missing`으로 retry 대기열에 넣었다. 운영에서는 WARNING을 보고 fd 한도를 올려야 한다.

### R2. 종료 시 writer drain 5초 초과 1회 (C 1회째, 재현 안 됨)

C4000:3000 1회째 종료 때 `ingest: writers not drained within 5s: 2655 queued records not written`과
`daily/visitor writer failed: context canceled`가 나왔다. 이 실행 중 VM이 약 2분 멈췄고(벽시계
16:00:41→16:02:43, 단조 시계는 6초 증가) 16시 정각을 넘어 visitor 시간 파일 4000개가 새로 생겼다. 같은
조건 2회째와 다른 시나리오(A8000 1.0초, B4000 1.9초 drain)에서는 재현되지 않았다. 시간 경계 +
수천 object에서 종료하는 경우를 따로 확인할 필요가 있다(이 시험에서는 `internal/ingest`를 고치지 않고
기록만 한다).

### R3. 텍스트 캐시 reset이 느린 Agent에 순차로 5초씩 쓴다 (E, 낮음)

E에서 Agent가 모든 명령에 45초 늦게 답하자 `OBJECT_RESET_CACHE` pass가 Agent마다 5초
(`textCacheResetCallWait`)씩 순서대로 timeout됐다. 시간이 지난 호출은 고아 relay가 되어 고아 slot을
최대 8개 차지했다. Java `TextCacheReset`도 순차 호출이고 응답 대기 상한이 Agent so_timeout(60초)이라
Go가 더 짧다. 다만 느린 Agent가 많으면 pass가 길어지고 그동안 고아 slot을 잡아 Client relay가 거절될
수 있다.

### R4. Client 명령 progress timeout 로그가 연결마다 1줄 (E, 낮음)

진전 상한(30초)으로 끝난 Client 명령마다 `tcp.connection: "context deadline exceeded"`가 rate limit
없이 1줄씩 나왔다(60 Client → 60줄). 대량 Client 이탈 때 로그가 늘어난다.

### 기타 관찰

- Bkill8000의 connect reset 43건과 명령 전 reset 26건은 SIGKILL 직후 1초 안에 재시도한 Agent가 아직 닫히는
  중인 listen socket에 붙었다가 reset된 것으로 보인다. 재기동 거부 경로는 `acquireLock`
  (runtime.go:326)이 listen(runtime.go:486)보다 앞이라 원인이 아니다. 추정이며 검증하지 않았다.
- Java `TcpAgentManager.agentTable`은 `setMax(5000)`(TcpAgentManager.scala:28)이라 objHash가 5000개를
  넘으면 오래된 Agent의 세션 큐가 빠진다. Go는 이 상한이 없고 연결 예산으로만 제한한다. 의도된 차이로
  기록할지는 미결정이다.

## Java Agent TCP 동작 근거 (Scouter `dee47fb5`)

| 동작 | 근거 | 도구 반영 |
|---|---|---|
| 1초 루프, 부족한 세션만 연결, connect 실패 시 3초 추가 대기 | `scouter.agent.java/.../net/TcpRequestMgr.java:29-40` | `--retry-interval 1s`, `--fail-pause 3s`, Agent별 1초 위상 |
| `net_collector_tcp_session_count` 기본 1 | `scouter.agent.java/.../Configure.java:93,1025` | `--session-count 1` |
| connect timeout 3000ms, read마다 so_timeout 60000ms | `TcpWorker.java:44-63`, `Configure.java:95-97,1026-1027` | `--connect-timeout`, `--so-timeout` |
| V2: `TCP_AGENT_V2`+objHash 한 번에 flush, 명령은 IntBytes(Text+Pack), 응답 `[0x03 IntBytes(Pack)] 0x04` | `TcpWorker.java:93-134`(Java Agent 기본) | 80% Agent |
| V1: `TCP_AGENT`, Text+Pack 직접, 응답 `[0x03 Pack] 0x04` | `TcpWorker.java:65-91`, host Agent `scouter.agent.host/.../TcpWorker.java:29,63-70` | 20% Agent(`--v1-ratio`) |
| `KEEP_ALIVE`는 null 반환 → `0x04`만 | `ReqestHandlingProxy.java:100-102` | 즉시 `0x04` |
| read/write 예외는 socket close → 다음 tick 재연결 | `TcpWorker.java:25-40` | 끊김 사유 분류 후 다음 tick |
| 서버: 5초 scan, 마지막 쓰기 후 5초 지난 세션에 KEEP_ALIVE, 응답 대기 3초 | `TcpAgentManager.scala:30-45`, `TcpAgentWorker.scala:83-104`, server `Configure.java:207` | 측정만 |

## 도구 사용법

`scouter-replay agents`(새 하위 명령, `cmd/scouter-replay/agents.go`). 서버를 띄우거나 멈추지 않는다.
주소는 loopback만 받는다.

```
scouter-replay agents --tcp 127.0.0.1:6300 --udp 127.0.0.1:6300 --http 127.0.0.1:6380 \
  --pid-file server.pid --agents 4000 --v1-ratio 0.2 --spread 10s --duration 120s \
  --xlog-rate 200 --clients 2 --probe-interval 5s --report agents.json
```

- Agent: `--agents`, `--session-count`, `--v1-ratio`, `--spread`(0이면 첫 1초 안에 전원 접속),
  `--connect-timeout`, `--so-timeout`, `--retry-interval`, `--fail-pause`, `--agent-delay`(KEEP_ALIVE 외
  명령 응답 지연).
- UDP: `--udp`, `--heartbeat-interval`, `--counter-interval`, `--xlog-rate`.
- Client: `--clients`, `--client-interval`, `--client-timeout`, `--probe-interval`, relay는
  `--relay-clients`, `--relay-command`(기본 `OBJECT_ENV`), `--relay-at`, `--relay-timeout`.
- 표본: `--http`(`/health` tcp·agentOrphans·stats, pprof goroutine 수), `--pid`/`--pid-file`(Linux
  `/proc` RSS·thread·fd·CPU, 재기동한 서버 추적), `--sample-interval`, `--settle`.
- 보고서: 전원 연결 시각, settled 감소 구간(episode: 시작·지속·최소·재시도·실패), connect 실패와 세션
  종료 사유(명령 전/후, eof·reset·refused·timeout), KEEP_ALIVE 간격, 초당 표본, 최댓값.

시나리오 실행: `scripts/connection-scale.sh --out DIR A8000 idle8000 B8000 Bkill8000 H8000:64
C4000:3000 D4000:2048 E`(서버를 새 DB로 띄우고 끝나면 SIGTERM, DB는 크기만 남기고 삭제. `KEEP_DB=1`로
유지). 요약: `scripts/connection-scale-summary.py DIR`(시나리오당 JSON 한 줄).

`client.call`은 이제 `param == nil`이면 body를 보내지 않는다(Java Client `TcpProxy.process(cmd, null)`,
`OBJECT_LIST_REAL_TIME`은 body 없음, AgentInfo.scala:76-83). 이전처럼 빈 MapPack을 보내면 서버가 다음
명령을 잘못 읽는다.

## 검증 명령

- `gofmt -l cmd/` 출력 없음, `go vet ./cmd/...` 통과.
- `go test -race -count=1 ./cmd/scouter-replay/...` 통과. 새 테스트: Go transport 서버와 V1·V2
  KEEP_ALIVE/relay 응답(`TestFleetAnswersGoServerAgentRequests`), Agent 예산 거절 계수와 재시도
  (`TestFleetRetriesAfterBudgetReject`), 재시도 시간표(`TestNextTickFollowsAgentGrid`), loopback 제한.
- `bash -n scripts/connection-scale.sh`, `python3 -m py_compile scripts/connection-scale-summary.py`.

## 미실행·한계

- `3c855ed` 재실행은 D만 했다. A·B·C·E·H는 `1d91997` 결과다.
- 이 시험은 실제 Java Agent·Java 서버와 하지 않았다(실제 Java Agent·Host Agent 상호운용은 [2026-10-24 시험](2026-10-24-java-agent-interop.md)에서 45/45 PASS)(Agent 동작은 소스 근거로 모사). Java 서버의
  KEEP_ALIVE 실효 간격과 5000 objHash 동작은 측정하지 않았다.
- loopback이라 RTT가 0에 가깝다. 실제 망에서는 handshake slot을 RTT만큼 오래 쥐고 SYN backlog
  (`tcp_max_syn_backlog 256`, syncookies) 영향이 다를 수 있다. 원격 호스트 다수에서 오는 연결은 미시험.
- 서버와 부하기가 같은 4 vCPU를 나눠 쓴다. CPU 수치는 관찰용이다.
- 각 시나리오 1회(C는 2회). A8000 1회째는 VM 디스크 부족으로 중단, 재실행 값을 썼다. C 1회째는 VM 정지
  2분이 끼어 2회째 값을 썼다.
- 부하기는 A 시리즈 실행 뒤 종료 중 write 오류 계수를 고쳐 다시 빌드했다(측정 값에는 영향 없음, A4000의
  `write_errors 4`는 종료 시 계수).
- 여러 objHash 세션(`session-count` > 1), objHash당 8개 상한 초과, `TCP_AGENT_REQ`(stack upload)는 시험하지
  않았다.

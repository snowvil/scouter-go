# 운영 설정 산정 (sizing)

운영 Go 서버의 설정값을 어떻게 정하는지 정리한다. 이 문서에는 특정 운영 환경의 규모·처리량·호스트 사양을 적지 않는다. 담당자는 아래 방법으로 값을 정해 전환 기록([runbook](cutover-runbook.md) §8)에 남긴다. 표의 측정값은 같이 적힌 조건에서 얻은 것이며 운영 예측치가 아니다.

## 1. 정하는 방법

1. **입력 확인.** 현재 Java 서버의 Agent 수(objType별), Client 동시 사용 수, 시간대별 XLog 수(완료 transaction), profile 비율과 크기 분포, 보관 일수, 디스크 증가량, 그리고 호스트를 같이 쓰는 다른 서비스의 사용량을 Java 서버와 OS에서 확인한다.
2. **스테이징 측정.** 실제 Agent 일부를 Go 스테이징에 붙여 `/health`와 stats 줄을 며칠 기록한다([monitoring](monitoring.md)). 부하 시험은 격리된 환경에서만 `scouter-replay`(XLog·profile·조회)와 `scouter-replay agents`(대규모 연결)로 한다. 운영망을 대상으로 부하를 만들지 않는다.
3. **여유분 적용.** 측정한 최대값에 여유를 두어 상한을 정한다. 상한에 걸릴 때의 동작은 아래 표의 "넘으면"을 따르며, 대부분 수집을 막지 않고 거절·drop을 세고 로그를 남긴다.
4. **전환 후 재확인.** 운영 첫 주에 같은 항목을 다시 보고 조정한다. 실시간 반영되는 값(`go_maxprocs`, `net_tcp_service_pool_size`, retention 키)은 재기동 없이 바꿀 수 있다.

## 2. 연결과 수신

| 설정 | 기본값 | 정하는 기준 | 넘으면 |
|---|---|---|---|
| `--max-agent-connections` | 16384 | Agent 수 × `net_collector_tcp_session_count`(Agent 기본 1)보다 넉넉히. 연결당 메모리는 idle 약 33KiB, UDP·조회 병행 시 Agent당 약 43KiB였다(Linux VM 측정, [connection scale](../validation/2026-10-22-connection-scale.md)) | 새 Agent 연결을 즉시 닫고 10초에 1줄 로그를 남긴다. Agent는 약 4초마다 재시도한다. 이미 연결된 Agent와 UDP 수집은 영향이 없다 |
| `--max-handshakes` | 2048 | 재시작 직후 동시에 재접속하는 Agent 수. loopback 시험에서는 이 값을 64로 낮추고 8000 Agent가 1초 안에 접속해도 4.1초 안에 모두 연결됐다([connection scale](../validation/2026-10-22-connection-scale.md)) | 거절된 Agent가 재시도한다 |
| `--max-connections` | 1024 | Client 연결 수(GUI, webapp, Client API 소비자) | 새 Client 연결 거절 |
| `net_tcp_service_pool_size` | 100 | 동시에 명령을 처리하는 Client 세션 수. stats 줄 `waiting`/`wait-timeout`이 생기면 올린다(실시간 반영) | Client가 대기 후 연결이 닫힌다 |
| open file 한도(`ulimit -n`) | OS | 위 세 연결 예산의 합 + DB 예비분 `max(1024, 한도/4)`보다 커야 한다 | 기동할 때 예산을 자동으로 낮추고 WARNING을 남긴다 |
| `--udp-read-buffer`(`net_udp_so_rcvbuf_size`) | 4MiB | 수신 burst. 커널 `net.core.rmem_max`가 이보다 작으면 커널이 잘라 적용한다. 실제 적용된 크기는 `/health`의 `udp`에서 본다 | 커널이 UDP를 버린다. Linux에서는 `udp.SocketDrops`가 증가한다 |
| `--udp-workers` | 2 | UDP decode·수집 병렬도. 수집 lock 하나를 공유하므로 늘려도 처리량 증가는 제한적이다. 바꾸면 측정으로 확인한다 | — |
| `--udp-queue` | 8192(payload 32MiB 상한) | burst 흡수. 측정 중 `udp` queue 대기 길이를 본다 | `udp.Rejected` 증가, 1초에 1줄 로그 |

## 3. CPU와 메모리

| 설정 | 기본값 | 정하는 기준 |
|---|---|---|
| `GOMAXPROCS`(env) 또는 `go_maxprocs`(scouter.conf, 실시간) | Go runtime 기본값(논리 CPU와 cgroup CPU 한도 반영) | 처음에는 기본값으로 시작한다. 공유 호스트에서 CPU를 제한해야 하면 낮춘다. 2로 낮추면 같은 부하에서 완료 XLog당 CPU가 약 9~12% 줄었다(Linux VM 쌍 비교, vCPU 4개 기본값 대비, [구조 검토](../performance-structural-review-2026-10-20.md) §10). 양수 `go_maxprocs`가 우선하며, 제거하거나 0이면 기동 시 env 값 또는 runtime 기본값으로 돌아간다 |
| `GOMEMLIMIT`(env) | 없음 | 서버에 줄 메모리 예산. soft limit이며 RSS hard cap이 아니다. 설정하면 Client 응답 예산의 기본값이 그 1/4이 된다(아래). 측정한 정상 RSS 최대값보다 넉넉하게 둔다 |
| `--engine-cache-mb` | 64 | 실시간 화면용 cache(counter, XLog ring, alert, status, group, sampling 지연 bucket)의 계상 예산. 넘어도 XLog·counter·alert·status는 저장되고 메모리 갱신만 건너뛴다(`*CacheSkipped`, group·alert summary 집계에서도 빠진다). 지연 sampling child XLog/profile은 원본 복구 규칙을 일찍 적용해 그 규칙에 따라 저장되거나 버려진다(지연 profile은 discardNone만 저장, XLog는 discardAll이면 버림. `DelayedRecoveredEarly`/`DelayedDiscardedEarly`). object와 kube 상태는 자기 bytes만 이 값과 비교하므로 계상 합계는 최대 이 값의 2배다. object가 이 값에 가까우면 XLog ring이 멈추고 sampling 지연이 사실상 꺼진다(저장은 계속) |
| `--max-objects`, `--max-counters` | 16384, 262144 | object 수와 실시간 counter key 수. object 상한에서는 가장 오래 비활성인 object부터 메모리에서 뺀다 |

### 메모리를 쓰는 곳과 상한

| 곳 | 상한 | 넘으면 |
|---|---|---|
| Agent 연결 | 연결 수 × 연결당 메모리(위 측정) | 연결 거절 |
| 수집 engine cache | 계상 기준 최대 `--engine-cache-mb` × 2. 실시간 cache(counter·XLog ring·alert·status·group·sampling 지연 bucket)가 `--engine-cache-mb`까지, object·kube 상태가 자기 bytes만으로 따로 `--engine-cache-mb`까지 쓸 수 있다. 계상 bytes는 encode 크기라 실제 heap은 항목에 따라 0.8~17배다([ingest CONTRACT](../../internal/ingest/CONTRACT.md) §12) | cache는 갱신만 건너뛴다(저장은 계속). 지연 child는 복구 규칙을 일찍 적용한다. object·kube는 자기 bytes가 넘을 때만 heartbeat를 거절한다. object·kube가 이 값에 가까우면 공유 예산이 남지 않아 XLog ring·status·counter cache가 멈추고 sampling 지연이 사실상 꺼진다 |
| XLog·profile 저장 queue | 개수·byte 상한([storage CONTRACT](../../internal/storage/CONTRACT.md) §8) | 최대 100ms 대기 후 drop(`xlogQueue.Dropped`, `profileQueue.Dropped`) |
| text 읽기 cache | 128MiB(`go_text_cache_mb`, 계상 기준. 실제 heap은 약 1.5~2배) | 오래 읽히지 않은 text부터 내보낸다. 내보낸 text는 다음 조회 때 파일에서 읽는다(`textCache.Evicted`, `textCache.FileReads`) |
| visitor HLL | resident 256MiB(`go_visitor_resident_mb`) + flush 대기 delta 64MiB(`go_visitor_delta_mb`) + entry(objType 100, object daily·hourly 각 32768, partial entry 약 200B) | resident 초과분은 오래 안 쓴 entry부터 partial로 강등한다. 파일 bytes는 같고, flush가 page를 읽어 합치며 realtime 조회가 파일을 읽는 IO가 는다. delta 초과분은 방문을 바로 쓴다(write-through, `/health` `visitors.WriteThroughs`). visitor writer가 느려지면 ingest queue drop으로 집계된다 |
| 수집 lock 밖으로 미룬 쓰기 | hold당 1024건 / 4MiB | lock 안에서 이전처럼 넣는다(`StoreWritesHeldInline`) |
| Pulse queue | XLog 16MiB, Profile 64MiB(개수 각 4096), producer 64MiB | drop(`pulse.dispatcher.*.DroppedByBytes`) |
| Client 응답 | `GOMEMLIMIT`/4(최소 257MiB) 또는 1GiB. 64KiB 미만 응답과 Agent relay로 중계하는 blob chunk(relay 예산으로 계상)는 제외. 전송 중인 응답과 대기 중인 응답이 각각 예산까지 차지할 수 있어 encode된 응답은 최대 약 2배까지 메모리에 있을 수 있고, encode 도중의 일시 메모리는 예산 밖이다 | 대기 후 그 명령만 실패(`tcp.ClientResponseRejected`). Java Client는 그때까지 받은 결과만 표시하고 다음 명령에서 재연결한다 |
| Agent relay | 전역 예산(`GOMEMLIMIT`/4 또는 1GiB, 하한 768MiB). 일반 응답 8MiB, 큰 응답 256MiB(동시 2개), blob chunk 16MiB. 고아 relay는 이 예산 안에서 동시 32개 | 명령을 Agent에 보내기 전에 기다리고, 그 안에 자리가 나지 않으면 보내지 않는다(Agent 세션 유지, `agentRelayBudget.Refused`). Client 명령은 Java의 null 결과처럼 빈 응답이 되고, 서버 발신 호출은 실패로 세며, text cache reset은 나중에 다시 시도한다. Client 명령은 예산 대기와 Agent 세션 대기를 합쳐 `net_tcp_get_agent_connection_wait_ms`(기본 1초)만 기다리고, 서버 발신 호출(kube 순번, CPU dump)은 `go_agent_relay_server_wait_ms`(기본 10초), text cache reset은 기다리지 않고 나중에 다시 부른다([runtime](../compatibility/runtime.md) "운영자 안내"). 고아 상한 초과는 그 세션을 닫는다([agent-side-effects](../compatibility/agent-side-effects.md) §7(d)) |
| HTTP·webapp 요청 | listener마다 연결 1024(open file 한도가 낮으면 줄어들며 최소 192). 상한의 7/8을 넘으면 응답마다 `Connection: close`를 붙여 idle keep-alive 연결을 회전시킨다. servlet(`/counter`, `/register`, `/telegraf`)·webapp(`/scouter/*`)은 각각 동시 64개, 대기 3초. `/health`·pprof는 별도 16개. HTTP로 push하는 호스트 수가 연결 상한에 가까우면 상한을 올린다 | 503(`Retry-After: 1`). `/health`의 `httpAdmission`, `scouterHttp`에서 계수를 본다. Java(Jetty)는 거절하지 않고 무한히 기다린다(의도된 차이) |

### 상한을 바꾸는 키 (scouter.conf, Go 전용)

Java 키 집합에는 없는 Go 전용 키다. 잘못된 값이나 범위 밖 값은 기동을 막지 않고 경고 후 기본값을 쓰며, 0은 기본값이다. 키별 허용 범위와 그 근거는 [runtime 계약](../compatibility/runtime.md)과 [config CONTRACT](../../internal/config/CONTRACT.md) §6에 있다. "재기동" 키는 바꾸면 `restart required` 로그를 남기고 다음 기동부터 적용된다.

| 키 | 기본값 | 반영 |
|---|---|---|
| `go_http_max_connections` | 1024(listener별). open file 한도에 맞춰 낮아질 수 있다. 4096이면 한도가 약 66k 이상이어야 한다 | 재기동 |
| `go_http_servlet_max_inflight`, `go_http_servlet_queue_wait_ms` | 64, 3000 | 재기동 |
| `go_http_webapp_max_inflight`, `go_http_webapp_queue_wait_ms` | 64, 3000 | 재기동 |
| `go_http_ops_max_inflight` | 16 | 재기동 |
| `go_agent_relay_budget_mb` | `GOMEMLIMIT`/4(최소 768) 또는 1024 | live |
| `go_agent_relay_max_large`, `go_agent_relay_server_wait_ms` | 2(256MiB relay 동시 수), 10000(서버 발신 호출의 예산 대기 ms. Client 명령은 `net_tcp_get_agent_connection_wait_ms`를 따른다) | live |
| `go_client_response_budget_mb` | `GOMEMLIMIT`/4(최소 257) 또는 1024 | 재기동 |
| `go_pulse_xlog_queue_mb`, `go_pulse_profile_queue_mb` | 16, 64 | 재기동 |
| `go_text_cache_reset_parallel` | 16 | live |
| `go_text_max_unknown_divs` | 64(원본에 없는 text 분류로 새 영구 index를 만들 수 있는 수) | live |
| `go_text_cache_mb` | 128(service·object 이름 등 text를 메모리에 둘 크기. URL에 변수가 들어가 service 이름 종류가 많은 환경은 늘린다) | 재기동 |
| `go_visitor_resident_mb`, `go_visitor_delta_mb` | 256, 64(visitor HLL을 통째로 둘 메모리, flush를 기다리는 방문 메모리) | live |
| `go_maxprocs` | 0(runtime 기본값 또는 기동 시 `GOMAXPROCS` env 값). 양수이면 env보다 우선 | live |

이 상한들의 합이 서버에 줄 메모리보다 크면, 동시에 최대치에 닿는 경우를 고려해 큰 항목(Client 응답, Pulse)을 먼저 줄인다. visitor 예산은 메모리를 CPU·IO와 맞바꾸는 값이다. 사용자 ID를 보내는 object가 많은 공유 호스트에서 `go_visitor_resident_mb`를 줄이면 메모리는 줄지만, flush가 바뀐 page를 읽고 다시 쓰고 realtime 방문자 조회가 object마다 파일(699,060B)을 읽는다. 반대로 object 수천의 daily·hourly HLL을 모두 메모리에 두려면 그 HLL 크기 합만큼 늘린다(dense면 HLL 하나 약 683KiB, `/health` `visitors.ResidentBytes`로 확인). 크게 줄일 때는 단계적으로 줄인다. 한 번에 크게 줄이면 강등했을 때 delta 예산을 넘게 되는 dirty entry는 flush가 쓴 뒤에야 강등된다. 그동안은 resident가 예산을 넘고(최대 resident entry 수 × 약 683KiB), 그 flush에 IO가 몰린다. 디스크 쓰기가 실패하면 10초마다 다시 시도하므로 그만큼 오래 넘는다. object entry 상한(32768)은 키로 노출하지 않는다. 실행 중에 줄이면 수집 경로에서 초과분을 한꺼번에 동기로 쓰고, 메모리 효과도 작기 때문이다. GC 여유를 위해 `GOMEMLIMIT`은 정상 RSS의 약 1.5~2배를 권한다(측정으로 조정).

## 4. 디스크

- **증가량**: 스테이징이나 Java 운영의 날짜별 디렉터리 크기로 잰다.
- **visitor 파일**: 사용자 ID가 있는 XLog를 받는 object마다 visitor HLL 파일이 하루 1개, 방문이 있던 시간마다 1개 생긴다. 방문자 수와 관계없이 파일 하나가 699,060B로 고정이다(Java `HyperLogLog(20)` dense 직렬화, Go도 같음). 하루 증가량은 `object 수 × (1 + 방문이 있던 시간 수) × 699,060B`이고, 보관 일수(`mgr_purge_visitor_counter_keep_days`)를 곱하면 visitor만으로 상당한 공간이 된다. object가 많으면 visitor 보관 일수를 먼저 정한다.
- **보관**: `mgr_purge_*` 키로 정한다. Go는 `mgr_purge_enabled`가 없으면 retention을 하지 않는다([runbook](cutover-runbook.md) §2.1).
- **압축**: `--compress-xlog`, `--compress-profile`는 원본 GZipStore 형식이다. 디스크를 줄이는 대신 조회와 쓰기에 CPU를 더 쓴다. 이미 압축된 DB의 형식을 그대로 따른다.
- **fsync**: `--sync-interval`(4초)은 data·index header를 주기적으로 fsync한다. crash 뒤 재기동 시간은 여기서 정해지는 checkpoint로 짧아진다([recovery-retention](../compatibility/recovery-retention.md) G7).

## 5. 기록할 것

산정 결과로 다음 값을 전환 기록에 남긴다.

- 위 표의 최종 설정값과 그 근거(측정 기간, 최대값, 여유 비율)
- `GOMEMLIMIT`, `GOMAXPROCS`, open file 한도, `rmem_max`
- 스테이징 측정 기간의 `/health` 최대값

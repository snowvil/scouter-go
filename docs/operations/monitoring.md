# 운영 감시 기준

Go 서버가 정상인지, 손실이나 자원 포화가 생겼는지 판단하는 항목과 기준이다. 수치 임계값은 운영 규모에 따라 담당자가 정한다([sizing](sizing.md)). 이 문서는 무엇을 어떤 방향으로 보는지를 정한다.

## 1. 수집 경로

| 경로 | 내용 |
|---|---|
| `GET /health` (`--http`, localhost 권장) | 수집·저장·연결·queue의 JSON snapshot. 누적 계수는 **주기 사이 증가량**, 현재 연결 수·queue 길이·메모리는 현재값을 본다. `stats`는 마지막 `--stats-interval` 구간 값이며 첫 tick 전이나 interval이 0이면 없다. 다른 HTTP 요청이 몰려도 별도 예산으로 응답한다 |
| stats 줄 (`--stats-interval`, 기본 10초) | 서버 로그의 한 줄 요약: `stats 10s: xlog N/s (누적) packs udp reject malformed expired [assembly] [sockdrop] \| ingest p50 p99 max reject drop \| client N/s p50 p99 max active err waiting abandoned wait-timeout wait-reject [\| pulse N/s fail pending] \| agents heap sys [rss] gc goroutines`. `[ ]` 부분은 조건부다: `assembly`는 0이 아닐 때, `sockdrop`은 커널 계수를 읽을 수 있을 때(Linux), `pulse`는 Pulse를 켰을 때, `rss`는 측정될 때만 나온다. 사람이 볼 때 쓴다. `client` p50/p99/max는 Client 명령 하나의 handler 전체 시간이다. Agent relay를 기다리는 시간도 들어가므로 heap dump 다운로드(Blob stream relay)처럼 수 분 걸리는 명령이 있던 구간은 p99·max가 분 단위가 된다. 이것만으로 서버 지연이라고 판단하지 말고 같은 시각에 그런 relay가 있었는지 먼저 확인한다(relay를 따로 집계하지는 않는다) |
| 서버 로그 (`log_dir/server-yyyyMMdd.log`) | 거절·오류는 사유별로 rate limit된 줄과 "N more ... since the last line" 누적 수로 남는다 |
| 종료 로그 | 종료할 때 queue별 enqueue·written·dropped와 남은 수를 한 번에 남긴다. `ingest=... udp=... tcp=...`로 시작하는 최종 계수 줄(종료 단계 `final stats`)에는 `agentOrphans`, `agentRelayBudget`, `visitors`(`WriteThroughs` 포함)도 `/health`와 같은 이름으로 들어간다. 신호를 받으면 `shutdown: SIGTERM received; graceful shutdown within 20s ...` 한 줄이 남는다. 종료 상한(`--shutdown-timeout`, 기본 20초)을 넘으면 `shutdown: budget ... exceeded; exiting 3` 줄과 exit 3으로 끝난다. 이 줄에는 멈춘 단계(`stage=`), 경과, 아직 handler 안에 있는 Client 명령 수(`clientCommands`, Agent 응답 대기 포함), Client가 떠난 뒤 남은 Agent relay 수(`agentOrphans`)와 모든 relay가 잡은 relay 예산 byte(`agentRelayBytes`), 남은 queue(`ingestPending`, `xlogQueue`, `profileQueue`, `pulseQueue`, `kafkaPending`), `.go-clean` 기록 여부(`marker=written|absent`)가 담긴다. 앞선 신호보다 1초 넘게 늦은 SIGINT/SIGTERM은 `shutdown: second signal ...; exiting now` 줄을 남기고 exit 128+신호 번호로 끝낸다. `marker=absent`이면 다음 기동은 crash checkpoint로 검증한다([runtime](../compatibility/runtime.md) "종료 상한"). ingest writer가 context를 보지 않는 쓰기에 갇히면 `ingest: <kind> writer still in a write ... N records left pending`과 `WARNING: ingest writers still hold N records: the clean-shutdown marker is not written` 줄이 남는다. 이때는 exit 0이어도 marker가 없다 |
| 콘솔(`-console`) | stats 키(`udp.assembly_rejected`, Linux `udp.socket_drops` 등) |

## 2. 즉시 확인 (손실·오류)

아래 오류·drop 계수가 **증가**하면 수집·저장 실패를 확인한다. `visitors.WriteThroughs`는 예산 포화에 따른 동기 쓰기 횟수로, 그 자체가 손실은 아니다. host 전체 계수는 이 프로세스의 손실과 구분한다.

| 항목 (`/health` 경로) | 의미 | 먼저 볼 것 |
|---|---|---|
| `udp.Rejected` | UDP 수집 queue가 가득 차 버린 packet | CPU 포화, 저장 지연(`ingest` p99), `--udp-queue` |
| `udp.SocketDrops`(Linux, `KernelCounters=true`. 읽지 못하면 `KernelCountersError`에 원인) | 커널 socket 수신 버퍼가 넘쳐 버려진 datagram | `udp.ReadBufferEffective`, `net.core.rmem_max` |
| `udp.HostRcvbufErrors`(Linux, `HostCounters=true`) | host 전체 UDP 수신 버퍼 오류(다른 프로세스 포함). `HostInErrors`는 이 값을 포함하므로 더하지 않는다 | 같은 host의 다른 UDP 서비스 |
| `udp.Malformed`, `udp.AssemblyRejected` | 해석 불가 packet, 분할 UDP 재조립 상한 초과 | Agent 버전, `net_udp_packet_buffer_size` |
| `ingest.Rejected`, `ingest.StorageErrors` | 수집 거절, 저장 실패 | 서버 로그의 오류 줄, 디스크 |
| `ingest.DroppedXLogs` | XLog 저장 queue에서 버려진 XLog | `xlogQueue` |
| `xlogQueue.Dropped`, `profileQueue.Dropped`, `*.WriteErrors` | XLog·profile writer 포화(S144/S142), 쓰기 오류 | 디스크 I/O, `ingest` p99 |
| `ingestWriters[*].Dropped`, `.WriteErrors`, `ingest.WriteDropped` | counter·object·alert 등 kind별 writer 포화와 오류 | 디스크 |
| `maintenance.errors`, `maintenance.textWriteErrors`, `maintenance.textDropped` | retention·sync 오류, text 저장 오류·포화 | 서버 로그 `periodic sync`, `purge failed` |
| `visitors.WriteThroughs`, `visitors.WriteThroughFailures` | visitor delta 메모리가 상한(`DeltaMax`, 기본 64MiB, `go_visitor_delta_mb`)에 닿아 수집 경로에서 바로 쓴 방문 수(늘면 visitor writer가 느려진다)와 그 쓰기 실패 수(방문은 메모리에 남고 다음 flush가 다시 쓴다) | flush 지연·디스크, 서버 로그의 visitor 오류, 예산(`ResidentMax`·`DeltaMax`). `ResidentBytes > ResidentMax`이면 예산을 줄인 뒤 dirty entry가 flush를 기다리는 중이다(강등 대기). 다음 flush 뒤에도 계속되면 visitor 쓰기 실패를 의심한다 |
| `pulse.dispatcher.{XLog,Profile}.Dropped`(`DroppedByBytes` 포함), `pulse.kafka.Dropped`, `pulse.sinkErrors` | Pulse 발행 손실 | Kafka 상태, worker 지연 |

## 3. 포화 징후 (주의)

증가하면 상한에 닿고 있다는 뜻이다. 데이터 손실은 아니지만 사용자 영향이나 손실로 이어질 수 있다.

| 항목 | 의미 |
|---|---|
| `tcp.AgentRejected`, `tcp.HandshakeRejected` | Agent 연결 예산 초과. Agent는 재시도하고, 그동안 서버 → Agent 요청이 닿지 않는다 |
| `tcp.ClientRejected`, `tcp.ClientWaiting`, `tcp.ClientWaitTimeouts` | Client 연결 상한, service pool 대기 |
| `tcp.ClientResponseRejected`, `tcp.ClientResponseWaiting` | Client 응답 메모리 예산 포화. 큰 조회가 실패한다 |
| `agentRelayBudget.Refused`, `.Waiting` | Agent relay 예산 포화. 그 조회는 Java의 null 결과처럼 빈 응답이 된다 |
| `agentOrphans.Rejected`, `.Failed` | Client가 떠난 뒤 남은 relay가 상한에 걸려 Agent 세션을 닫았거나 실패했다 |
| `httpAdmission.*`, `scouterHttp.Busy` | HTTP·webapp 요청 거절(503) |
| `ingest.XLogCacheSkipped`, `ingest.CounterCacheSkipped`, `ingest.AlertCacheSkipped`, `ingest.StatusCacheSkipped` | 실시간 cache 예산이나 개수 상한에 닿아 메모리 상태를 갱신하지 않았다. 개별 XLog·counter·alert·status 저장은 계속된다. 실시간 화면만이 아니라 메모리에서 만드는 집계 저장에도 빠진다: `XLogCacheSkipped`의 group meter는 매분 group 합계, `AlertCacheSkipped`의 alert summary는 5분 alert SummaryPack에서 그 건이 빠진다. `AlertCacheSkipped`는 alert 하나에 summary와 ring으로 최대 2 증가한다 |
| `ingest.DelayedRecoveredEarly`, `ingest.DelayedDiscardedEarly` | cache 예산이 모자라 sampling 지연 child XLog/profile에 driving XLog를 기다리지 않고 원본 복구 규칙을 일찍 적용해 저장 queue에 넘겼다/버렸다(XLog는 discardAll이면 버림, profile은 discardNone만 저장). 저장 queue가 포화면 그 뒤에 버려질 수 있다(`/health`의 `xlogQueue.Dropped`, `profileQueue.Dropped`). alert ring 자리를 만들려고 지연 profile을 일찍 복구한 것도 들어간다. 계속 늘면 `--engine-cache-mb`를 늘리거나 지연 bucket 설정(`xlog_sampling_matcher_*`)을 확인한다 |
| `ingest.DelayedAbandoned` | 지연 child의 복구가 오류(종료 중 ctx 취소, decode, 저장)로 멈춰, 처리 중이던 group에서 처리하지 못한 child 수. 종료 때 bucket에 남아 있던 지연 child는 세지 않는다(원본도 메모리 cache와 함께 사라진다). 종료 때가 아닌데 늘면 저장 오류를 확인한다 |
| `ingest.ObjectsLimited` | object 상한(`--max-objects`)에 닿았고 내보낼 inactive object가 없어 새 object를 등록하지 못했다. 그 heartbeat는 저장되지 않는다(`ingest.Rejected`에도 들어간다) |
| `ingest.ObjectsEvictedSilent` | object 상한에서 오래 조용한 batch·긴 dead time object를 실시간 목록에서 뺐다(다음 heartbeat에 재등록) |
| `maintenance.counterNamesLimited` | 날짜별 realtime counter 이름 사전이 새 이름 상한(65,536)에 닿아 새 이름의 값만 저장하지 않았다(기존 이름은 계속 저장). Telegraf tag처럼 이름이 계속 늘어나는 입력을 확인한다 |
| `ingest.TextDivsLimited` | 원본에 없는 text 분류가 상한(새 분류 64개)에 걸려 저장되지 않았다. 잘못된 Agent나 비정상 송신을 의심한다 |
| `ingest.StoreWritesHeldInline` | 저장 queue가 막혀 수집 lock 안에서 대기했다 |
| `ingest.ServiceTextSkipped`, `textCache.Slowdowns`, `textCache.Unanswered` | service text를 제때(50ms) 읽지 못해 XLog를 group 없이 저장했다(XLog·profile·Pulse 전송은 정상). 저장소 잠금이나 디스크가 느리다는 신호다. 로그 `GO-TEXT`가 같이 남는다. 주기적으로(예: 매시 같은 분) 나타나면 호스트가 page cache를 비우는 작업이나 대량 읽기 작업을 의심한다 |
| `textCache.Evicted`, `textCache.FileReads` | text 읽기 cache가 상한(`MaxBytes`, `go_text_cache_mb`)에 닿아 내보낸 수, 파일에서 읽은 수. 둘이 함께 계속 늘면 cache가 작다 |
| `tagCount.CacheLimited` | tag count top100 재구성이 크기 상한에 닿아 그만둔 경로 수(날짜·objType). 그 날짜의 top100은 마지막 성공 내용으로 남고 count 기록은 계속된다 |
| `tcp.AcceptRetries` | accept 실패(EMFILE 등) 재시도 |
| `tcp.TimeoutErrors`, `.ClosedErrors`, `.LimitErrors`, `.UnsupportedErrors`, `.UnauthorizedErrors`, `.OtherErrors` | 연결 오류의 분류별 누적. `Unsupported`는 Scouter가 아닌 접속(포트 스캐너 등)이다 |

## 4. 상태

| 항목 | 정상 |
|---|---|
| `tcp.AgentConnections` | 기대 Agent 수 × `net_collector_tcp_session_count`와 비슷하다. 재기동 직후 수 초 안에 회복한다 |
| `stats.ingest` p99 / max | 평소 µs 단위. 초 단위 max는 수집 정지다(재기동 직후는 [indexOpens](#5-재기동-직후) 확인) |
| text cache reset 로그 | 날짜가 바뀔 때 `text cache reset: date ... live N called N missing 0 errors 0`. `missing`·`errors`가 0이 아니면 일부 Agent가 text를 다시 보내지 않는다 |
| `stats.rss`, `stats.heap` | [sizing](sizing.md)에서 정한 메모리 예산 안 |
| `udp.ReadBufferEffective` | 요청값 이상(Linux는 2배로 보고). 작으면 `rmem_max`가 막고 있다 |

## 5. 재기동 직후

| 항목 | 의미 |
|---|---|
| `indexOpens.{store,counters}.Sealed` | 정상 종료 표시(`.go-clean`)로 검증을 생략한 index 쌍 |
| `.Checkpointed`, `.WalkedRecords` | crash 뒤 checkpoint 이후 꼬리만 검증한 쌍과 record 수. 직전 종료가 exit 3(종료 상한 초과)·두 번째 신호·SIGKILL이었으면 `CleanMarker=false`이고 이 경로가 정상이다 |
| `.Walked`, `.WalkedRecords`가 큼 | 전체 검증. 시간은 record 수에 비례하고, 이 동안 수집이 멈출 수 있다(배경은 [개선 대상](../improvement-candidates-2026-10-21.md) §10) |
| 서버 로그 `storage recovery index-relinked` | crash로 header가 놓친 record를 다시 연결했다(정상 복구) |
| 기동 로그 `WARNING: the open file limit ...` | open file 한도 때문에 연결 예산을 낮췄다 |

## 6. 경보 기준 정하기

1. **§2 항목**: 증가량 > 0이면 경보한다. 재기동 직후의 짧은 burst를 허용하려면 "N분 연속 증가"로 조건을 둔다.
2. **§3 항목**: 스테이징 측정 기간의 평소 증가량을 기준선으로 삼고, 그보다 계속 높으면 경보한다.
3. **§4 항목**: 범위를 벗어나면 경보한다(Agent 연결 수의 큰 감소, ingest max가 초 단위, RSS가 예산 초과).
4. 경보가 울리면 같은 시각의 stats 줄과 서버 로그의 rate limit 줄("N more ...")로 원인을 좁힌다.

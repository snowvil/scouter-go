# transport 계약

기준 원본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`. wire·framing 계약 정본은
[transport.md](../../docs/compatibility/transport.md)이고, 키별 반영 시점은
[config-behavior.md](../../docs/compatibility/config-behavior.md) §1.1·§1.2·§5.1(T1·T2·T4·T5·T6)이다.
이 문서는 패키지가 노출하는 설정 경계와 Java와의 의도된 차이만 적는다.

## 1. 설정 전달 경계

transport는 `internal/config`와 logger 패키지를 import하지 않는다. 배선 측이 아래 setter를
기동 때 한 번, 설정 변경 때마다 호출한다. setter는 원자 저장만 하므로 어떤 goroutine에서
불러도 되고 I/O를 하지 않는다. 호출 전에는 Java 기본값을 쓴다.

| 키 | API | Java 기본값(호출 전) | 반영 시점 |
|---|---|---|---|
| `net_tcp_client_so_timeout_ms` | `TCPServer.SetClientSoTimeout(time.Duration) error` | 8000ms | 이후 accept한 연결(Java `TcpServer` accept 때 `setSoTimeout`) |
| `net_tcp_agent_so_timeout_ms` | `TCPServer.SetAgentSoTimeout(time.Duration) error` | 60000ms | 이후 handshake를 마친 Agent 연결(Java `TcpAgentWorker` 생성 때) |
| `net_tcp_service_pool_size` | `TCPServer.SetServicePoolSize(int) error` | 100 | 즉시(증가·감소 모두) |
| `log_tcp_action_enabled` | `TCPServer.SetActionLog(bool)` | false | 로그 지점마다 |
| `log_udp_multipacket` | `UDPReceiver.SetLogUDPMultipacket(bool)` | false | 로그 지점마다 |
| `log_expired_multipacket` | `UDPReceiver.SetLogExpiredMultipacket(bool)` | **true** | 로그 지점마다 |
| `net_udp_packet_buffer_size` | `UDPPacketBufferSize(int) (size int, adjusted bool)` → `UDPConfig.MaxDatagramBytes` | 65535 → 65507 | 기동 |
| `_net_udp_worker_thread_count` | `UDPWorkerCount(int) (workers int, adjusted bool)` → `UDPConfig.Workers` | 3 | 기동 |

- timeout setter: 음수는 `ErrMalformed`로 거절하고 이전 값을 유지한다. `0`은 Java
  `setSoTimeout(0)`처럼 read마다의 제한이 없다는 뜻이다.
- `SetServicePoolSize`: 1 미만은 `ErrMalformed`로 거절하고 이전 값을 유지한다(Java는 기동 시
  0 이하로 `ThreadPoolExecutor`를 만들지 못하고, JDK 17은 실행 중 잘못된 값을 예외로 버린다).
- `UDPPacketBufferSize`: 65507 초과는 65507로, 1 미만은 65507로 바꾸고 `adjusted=true`를
  돌려준다. `UDPWorkerCount`: 1 미만은 Java 기본 3, `MaxUDPWorkers`(256) 초과는 256으로
  바꾸고 `adjusted=true`. `UDPConfig.normalized`는 `Workers > MaxUDPWorkers`를 오류로 거절한다.
  flag 우선 규칙과 `adjusted` 로그는 배선 측 책임이다.
- 로그 연결: `TCPServer.SetLogFunc(LogFunc)`, `UDPReceiver.SetLogFunc(LogFunc)`,
  `UDPReceiver.SetObjectNameFunc(ObjectNameFunc)`. nil이면 해당 줄을 내지 않는다.

## 2. TCP so-timeout(T1·T2)

- 소켓 아래에 `idleReader`를 두고 **실제 socket read마다** read deadline을
  `min(now+so_timeout, 총 deadline)`으로 다시 건다. Java SO_TIMEOUT처럼 read마다 갱신되며,
  bufio에 이미 들어온 bytes는 timeout과 무관하다.
- Client: 명령 사이 대기와 명령 body read 모두 연결 accept 때의 `net_tcp_client_so_timeout_ms`를
  쓴다. handshake 4 byte는 기존 `HandshakeTimeout`(5초) 총 deadline을 유지한다.
- Client 명령에는 **총 deadline이 없다**(Java와 같음). 대신 진전 기준 상한만 둔다
  (`client_progress.go`).
  - 명령 안의 socket read마다 so-timeout. so-timeout이 0이면 명령 안에서만 `RequestTimeout`
    (기본 30초)을 read마다 쓴다(명령 사이 대기는 Java처럼 무제한).
  - socket write는 64KiB 조각마다 write deadline `now+RequestTimeout`을 다시 건다. Client가
    읽지 않아 한 조각도 못 보내면 그 시간 뒤 write 오류로 명령과 연결을 끝낸다.
  - handler ctx는 Stream의 read·write 진전이 `RequestTimeout` 동안 없으면
    `context.DeadlineExceeded`로 끝난다. 계속 보내는 응답은 시간 제한 없이 이어진다.
  - 예외: handler가 `Stream.AwaitUpstream(wait)`으로 표시한 Agent 대기 구간에서는 이 감시를
    보류한다(Java service thread는 Agent 응답을 Agent so_timeout까지 기다린다). 대기 상한은 wait 자신의
    deadline(server Pack relay deadline)이다. server의 wait는 Agent에 쓰기 전 relay 예산 permit과 Agent
    세션 대기를 포함하며, Client 명령은 둘을 합쳐 live `net_tcp_get_agent_connection_wait_ms`까지만
    기다린다(Java `RequestQueue.get` 한 번과 같은 상한). 그동안 별도 goroutine이 Client socket을 `Peek`으로
    probe하고(소비하지 않음, read deadline 없음) EOF·RST면 ctx를 `ErrClientGone`(`context.Canceled`로도
    매칭, 원인 오류를 wrap)으로 즉시 끝낸다. 대기 중 Client가 보낸 byte는 다음 명령으로 남는다. 끝나면 probe를
    interrupt로 멈추고 명령 안 read so-timeout을 되돌린 뒤 진전 시각을 갱신해 감시를 처음부터 다시 건다.
    wait 안에서 Stream을 읽으면 안 된다(reader를 probe가 쓴다). 응답 item 쓰기는 된다(Blob
    stream relay는 chunk를 받는 대로 wait 안에서 쓴다). 쓰기는 handler goroutine만 하고 probe는 reader만
    쓰므로 bufio 경합이 없고 net.Conn read/write 동시 사용은 안전하다. 그 쓰기도 64KiB 조각마다 write
    deadline이 그대로라 읽지 않는 Client는 보류 중에도 `RequestTimeout` 뒤 끝난다. Stream처럼 동시 호출은
    금지이고, wait 안의 중첩 호출은 새 probe 없이 wait만 실행한다. wait가 panic해도 probe 중단과 read
    so-timeout 복원은 defer로 끝난다. half-open Client(FIN·RST 없음)는 TCP keepalive 실패(ETIMEDOUT →
    `ErrClientGone`) 또는 wait의 deadline까지 남는다. keepalive는 listener가 정한다: `net.Listen` 기본은
    Linux·macOS 모두 idle 15초, 간격 15초, 9회(`TCP_KEEPIDLE`/`TCP_KEEPALIVE`·`TCP_KEEPINTVL`·`TCP_KEEPCNT`를
    Go가 직접 설정해 OS 기본 7200/75초를 쓰지 않음)라 서버가 쓰지 않는 대기 중에는 약 150초에 감지된다.
    보낸 data가 ACK되지 않고 남아 있으면 keepalive 대신 재전송 timeout이 적용되지만, 그때는 write deadline
    또는 Agent read so_timeout이 먼저 끝낸다. 대기 중 이탈의 action log는 `Client : <peer> closed`(짧은
    형식), 연결 오류 분류는 closed다.
  - 응답 item을 쓴 뒤 직전 flush가 200ms 이상 지났으면 bufio(4KiB)를 flush한다. 느린 스트리밍
    조회가 찾은 결과를 명령 끝까지 붙잡지 않게 한다(Java XLog 과거 조회는 item마다 flush).
- Client 응답 크기: 명령당 누적 상한(이전 `MaxFrameBytes` 8MiB 누적)은 없다. **전송 대기 버퍼
  상한**만 둔다. 응답 item 하나를 encode해 socket writer에 넘기기 전까지 메모리에 들고 있으므로
  item 하나의 encode 결과가 `MaxResponseValueBytes`(256MiB) + 1MiB envelope를 넘으면
  `ErrLimit`이다. 응답 encoder(`ResponsePack`/`ResponseValue`/`ResponseBlob`)의 단일 값 상한
  (`MaxBlobBytes`, `MaxBytes`)은 Client 응답에 한해 `MaxResponseValueBytes`로 올린다. 그 밖의
  encode와 **요청 decode 상한은 `TCPConfig.CodecLimits`·누적 `MaxFrameBytes` 그대로**다.
  socket write는 blocking이므로 느린 Client는 handler를 멈춰 세운다(backpressure). 쌓이는 양은
  연결마다 bufio 4KiB + item 하나이고, 연결 전체 합은 §2.2의 응답 메모리 예산으로 묶는다.
- Agent: 응답 read마다 등록 때의 `net_tcp_agent_so_timeout_ms`를 쓴다. 요청 전체 deadline은
  `AgentLimits.Transfer`로 정하며 항상 so_timeout 이상이다. 기본은
  `min(ctx deadline, now+so_timeout+RequestTimeout)`이고, `Transfer<0`(Blob stream)이면 총 deadline이
  없다. 이때 so_timeout이 0이면 read마다 `RequestTimeout`을 쓴다. Java에는 총 deadline이 없다.
  timeout이면 연결을 닫는다(Java도 close). `RequestLimited`는 넘겨받은 ctx가 끝나도 연결을 닫으므로,
  호출자가 먼저 떠나도 Agent 세션을 지켜야 하는 Pack relay(`internal/server` `agentPool.relay`)는
  Client 명령 ctx 대신 **Agent 연결 수명 ctx**를 넘긴다. Client 명령 ctx가 진전 상한
  (`RequestTimeout`)이나 Client 이탈로 끝나면 Client 쪽 응답만 끝나고, Agent 요청은 "고아"로 남아
  자기 relay deadline까지 응답을 끝까지 읽고 버린 뒤 세션을 pool에 돌려준다(Java
  `TcpAgentWorker`와 같은 결과, C4). 고아 relay의 결과 로그(1분 1줄, 나머지는 계수만)는 세션·예산을
  반납하기 전에 쓰므로 종료 뒤에 로그가 남지 않는 대신, log 쓰기가 막히면 반납도 그만큼 늦어진다. 고아 수·예약 byte 상한과 계수는
  [agent-side-effects.md](../../docs/compatibility/agent-side-effects.md) §7(d)에 있다. Blob
  stream relay는 Client ctx를 그대로 써서 Client가 떠나면 Agent 세션을 닫는다(Java도 handler의 Client
  write 실패로 close). Blob stream 전체가 `AwaitUpstream` 안에서 돌아 Client 진전 감시가
  보류되므로 그 ctx는 진전 상한으로 끝나지 않고 Client 이탈(probe)·서버 종료로만 끝난다. 첫 chunk 전·chunk
  사이 대기는 Agent read마다 so_timeout만 받는다(Java와 같음).
- Agent 응답 크기(`AgentConn.RequestLimited`/`RequestPackLimited`의 `AgentLimits`): `ItemBytes`는
  HasNEXT item 하나(V1 Pack, V2 IntBytes, Blob chunk)와 그 안의 Blob/Text 값 상한이다.
  `TotalBytes`는 응답 전체 상한이고, 0이면 `ItemBytes`, 음수면 총량 상한 없음(item을 하나씩 넘기는
  stream)이다. 0값 `AgentLimits{}`(`Request`/`RequestPack`)는 응답 전체 `MaxFrameBytes`다. 상한을
  넘으면 할당 전에 `ErrLimit`으로 끝내고 연결을 닫는다. 명령별 값은 `internal/server/agent_blob.go`,
  의도된 차이는 [agent-side-effects.md](../../docs/compatibility/agent-side-effects.md) §7(d)에 있다.
- `TCP_AGENT_REQ`(stack upload)는 바꾸지 않았다(`IdleTimeout`·`RequestTimeout` 총 deadline).

## 2.1 TCP 연결 예산(Go 전용, C1)

Java `TcpServer`는 accept 수에 상한이 없고 모든 socket을 service pool의 무제한 queue에 넣는다.
Agent 연결은 handshake 뒤 thread를 반환하고 `TcpAgentManager`(objHash당 `RequestQueue(50)`,
objHash 5000개)에 남으므로 **Agent 연결 총수 상한이 없다**. Go는 모든 연결을 bounded로 두되
Agent와 Client가 서로의 예산을 쓰지 않도록 세 갈래로 나눈다.

| 예산 | `TCPConfig` 필드 | 기본값(normalized) | 대상 | 상한 확인 시점 |
|---|---|---|---|---|
| handshake | `MaxHandshakes` | 2048 (`DefaultMaxHandshakes`) | magic 4 byte를 아직 읽지 않은 socket | accept 직후 |
| Agent | `MaxAgentConnections` | 16384 (`DefaultMaxAgentConnections`) | `TCP_AGENT`(V1)·`TCP_AGENT_V2`·`TCP_AGENT_REQ`(stack upload) | magic 판독 직후 |
| Client | `MaxConnections` | 128 (`DefaultMaxConnections`, server는 `--max-connections` 1024) | `TCP_CLIENT`(세션·pool 대기 모두) | magic 판독 직후 |

- 연결은 accept에서 handshake slot을 잡고, magic을 읽으면 **handshake slot을 먼저 반납한 뒤**
  Agent/Client slot을 잡는다. 두 slot을 동시에 쥐지 않는다. 연결이 끝나면 쥔 slot을 반납한다.
  알 수 없는 magic은 slot 없이 닫는다(`Unknown : <addr> drop`).
- Agent의 objHash 4 byte는 Agent slot을 잡은 뒤 같은 `HandshakeTimeout` deadline으로 읽는다.
- 상한 초과: 읽기·쓰기·대기 없이 `SO_LINGER 0`으로 즉시 닫는다(peer는 reset, 서버에는 TIME_WAIT가
  남지 않음). Agent 입장에서는 서버가 내려간 것과 같다. Java Agent `TcpWorker`는 접속 직후
  read/write 예외로 소켓을 닫고 `TcpRequestMgr`가 1초 뒤 다시 시도한다(connect 자체가 reset으로
  실패하면 3초 더 쉰다). 이 경로는 Agent의 `SCOUTER` worker thread에서만 돌고 애플리케이션 thread와
  무관하다. 이미 등록된 objHash당 Agent 연결 상한(server pool 8개)은 별도로 유지된다.
- 계수: `TCPStats.HandshakeRejected`·`AgentRejected`·`ClientRejected`, 합계 `Rejected`(하위 호환).
  현재 점유 `Handshaking`·`AgentConnections`·`ClientConnections`, 상한 `MaxHandshakes`·
  `MaxAgentConnections`·`MaxConnections`. 거절은 `Errors`에 세지 않는다. `Active`는 accept 뒤 아직
  닫히지 않은 모든 socket(거절 직후 닫히는 중인 것 포함)이다.
- 로그: 예산마다 10초에 한 줄(Go 전용, `log_tcp_action_enabled`와 무관, 서버 종료 중 제외),
  `id=""`로 Java logger에 쓰고 같은 문장을 `ErrorHandler`(`tcp.connections.<handshake|agent|client>`,
  `ErrLimit`)에도 보낸다.
  `<TCP|Agent|Client> : <addr> rejected[ [<Hexa32 objHash>]], <예산> connections=<현재>/<상한> (Go connection limit, raise <flag>)[, <n> more rejected since the last line]`.
  flag는 `--max-handshakes`·`--max-agent-connections`·`--max-connections`다. objHash는 Agent magic과
  함께 이미 buffer에 들어온 경우에만 적는다(기다려 읽지 않는다). 줄 사이에 생략된 거절 수는 다음
  줄의 `n more rejected`로 넘긴다. 마지막 줄 뒤 거절이 더 없으면 그 수는 `TCPStats`에만 남는다.
- 기본값 근거(주석 `tcp.go` 참고): loopback에서 idle Agent socket 3000개를 측정하면 Go 메모리
  (HeapInuse+StackInuse)가 연결당 약 14KiB였다(bufio reader·writer 4KiB씩, goroutine stack 4KiB,
  Stream·AgentConn·netFD). 16384개 Agent는 최악 약 224MiB, handshake 2048개는 약 28MiB다.
  Agent는 첫 write에 magic과 objHash를 함께 보내므로 handshake slot은 왕복 한 번 동안만 쥔다.
  재기동 직후 수천 Agent가 동시에 재접속해도 handshake 상한에 걸리지 않게 2048로 두었다.
  socket마다 fd를 하나 쓰므로 `ulimit -n`은 세 상한의 합과 저장 파일 수보다 커야 한다. 그보다
  작으면 상한 전에 accept가 실패한다.
- Accept 일시 오류(2026-10-21): `EMFILE`·`ENFILE`·`ENOBUFS`·`ENOMEM`·`ECONNABORTED`·timeout이면
  listener를 멈추지 않는다. 5ms부터 두 배씩 최대 1초까지 쉬었다가 다시 받는다(`net/http`와 같은 방식).
  그동안 이미 연결된 Agent·Client 세션은 그대로 동작한다. 횟수는 `TCPStats.AcceptRetries`, 로그는
  `tcp.accept` 10초 1줄(ulimit 안내 포함). handshake 예산은 전역이며 peer별 상한이 없다. 아무것도
  보내지 않는 연결이 예산을 채우는 동안에는 새 연결이 거절된다(수용한 위험). 이미 연결된 세션은 영향이
  없고 로그에 peer가 찍히며, Agent는 재시도로 회복한다. 이전에는 오류 한 번에 TCP 서버 전체가 종료됐다
  (`TestTCPAcceptRetriesTemporaryErrors`). 그 밖의 Accept 오류는 이전처럼 Serve를 끝낸다.

## 2.2 Client 응답 메모리 예산(Go 전용, R2)

item 상한(§2)은 연결 하나의 양만 묶으므로 동시 Client 세션(`net_tcp_service_pool_size` 기본 100)이 모두
큰 응답을 느린 Client에 보내면 합계가 수십 GiB가 될 수 있었다. `TCPServer`는 모든 Client 연결이 함께 쓰는
byte 예산을 하나 둔다(`client_budget.go`).

- 대상: encode가 끝난 응답 item(`ResponsePack`/`ResponseValue`/`ResponseBlob`/`ResponseRaw`) 중
  **64KiB(`clientBudgetMinItem`) 이상**. HasNEXT를 쓰기 전에 `len(item)`을 잡고 socket writer가 반환하면
  (실패해도) 돌려준다. 64KiB 미만 item은 예산을 쓰지도 기다리지도 않는다(세션당 하나라 합계는 세션 수 ×
  64KiB). handler가 encode 전에 만드는 값(Pack, 조회 buffer)은 세지 않는다.
- 제외: `Stream.ResponseRelayBlob`. Agent Blob relay(heap dump·dump file, `internal/service` `agentReadQuery`)
  chunk는 server의 Agent relay 예산(chunk permit)이 이미 세므로 이 예산을 잡지 않는다. 여기서 기다리면
  그동안 Agent read가 멈추고, 거절되면 `RequestLimited`가 Agent 세션을 닫기 때문이다(이중 계상 제거).
- 상한: `TCPConfig.MaxClientResponseBytes`. 0이면 `DefaultClientResponseBytes()`: GOMEMLIMIT
  (`debug.SetMemoryLimit`)이 있으면 그 1/4, 단 item 하나 최대치(`MaxResponseValueBytes`+1MiB = 257MiB)
  이상. 없으면 `DefaultClientResponseBudget` 1GiB(최대 item 4개, 보통의 수 MiB 응답 수백 개). 음수는
  `ErrMalformed`. 나머지 3/4는 수집·저장 cache·Agent socket 몫이다. GOMEMLIMIT은 soft limit이므로 이
  예산도 RSS hard cap이 아니다.
- 예산보다 큰 item은 **아무도 예산을 쓰지 않을 때** 혼자 나간다. item 상한이 허용하는 응답은 여전히 보낼
  수 있다. 보내는 중인 합은 `max(상한, item 하나)`, 기다리는 합도 같은 값 이하라서 encode된 byte는
  최대 약 2×상한이다. encode 자체(Encoder의 증가 여유, 예산을 잡기 전 encode 중의 피크)는 예산 밖이다.
  GOMEMLIMIT이 512MiB 이하이면 하한 257MiB 때문에 상한이 GOMEMLIMIT의 1/4을 넘는다.
- 자리가 없으면 기다린다. 대기 상한은 그 연결의 pool 대기 상한(accept 때 client so-timeout, 0이면
  `RequestTimeout`)과 `RequestTimeout` 중 작은 값이고, 명령 ctx(진전 상한, 서버 종료·`Close`)가 끝나도
  끝난다. 대기 중 peer가 연결을 닫은 것은 읽지 않으므로 대기 상한까지 모른다. Scouter Client는
  자기 read timeout 뒤 읽지 않으므로 더 기다릴 이유가 없다. 이미 encode한 item이 기다리므로 대기 중
  item 합이 상한을 넘게 되면 기다리지 않고 바로 거절한다(첫 대기자는 항상 기다릴 수 있다).
- 순서: first fit. 반납될 때 들어갈 수 있는 대기자를 오래된 순으로 모두 들인다. 큰 item이 기다리는 동안
  들어갈 수 있는 작은 item은 먼저 나간다(큰 item은 대기 상한 뒤 실패할 수 있다).
- 거절: `ErrResponseBudget`(`errors.Is(err, ErrLimit)`)을 handler에 돌려준다. item은 한 byte도 쓰지
  않았다. handler가 그 오류를 반환하면 이미 보낸 item만 flush하고 NoNEXT 없이 연결을 닫는다(item 상한
  초과 `ErrLimit`과 같은 경로). Java Client `TcpProxy.process`(142-173행)는 read 예외를 잡아 `tcp.close()`만
  하고 **정상 반환**한다. 그래서 호출자는 그때까지 받은 item만으로 된 결과를 오류 표시 없이 받는다(첫 item이
  거절되면 빈 결과, 둘째 이후면 잘린 결과). 다음 명령은 새로 연결한다. Java 서버는 handler 예외를
  `ServiceHandlingProxy`(43-52행)에서 잡아 stack trace만 남기고 NoNEXT를 쓴 뒤 연결을 유지한다
  (`TcpServiceWorker.scala` 126-129행). 결과가 잘리는 것은 같고 연결 유지 여부만 다르다. 다른 Client와 Agent
  연결은 영향이 없다. 연결 오류는 `tcp.connection`(분류 limit, §2.3)으로 보고된다.
- 계수: `TCPStats.ClientResponseBytes`(지금 쥔 byte), `ClientResponsePeakBytes`(최대),
  `MaxClientResponseBytes`(상한), `ClientResponseWaiting`(지금 기다리는 item), `ClientResponseWaits`(기다린
  item 누적), `ClientResponseRejected`(거절 누적).
- 로그: 거절마다가 아니라 10초에 한 줄(Go 전용, `id=""`, `log_tcp_action_enabled`와 무관, 종료 중 제외).
  `Client : <addr> response of <n> bytes refused, response memory=<사용>/<상한> (Go Client response memory budget)[, <n> more refused since the last line]`.
- 배선: `internal/server`는 `go_client_response_budget_mb`(MiB, 재기동 적용)를 `MaxClientResponseBytes`로 넘긴다. 0이면 기본값이다.

## 2.3 연결 오류 보고(Go 전용, R4)

오류로 끝난 연결(`TCPStats.Errors`)은 `ErrorHandler`에 `tcp.connection`으로 보고한다. 사유를
timeout(`context.DeadlineExceeded`·`os.ErrDeadlineExceeded`·net timeout, Client 진전 상한 포함)·
closed(EOF·reset·EPIPE·`net.ErrClosed`)·limit(`ErrLimit`)·unsupported(`ErrUnsupported`: 모르는 magic·
Agent request, 스캐너·probe)·unauthorized(잘못된 Client session)·other로 나누고 분류마다 **10초에 한 건**만
보낸다. 스캐너 소음이 storage·handler 오류(other)의 줄을 가리지 않게 따로 둔다. 분류별 누적은
`TCPStats.TimeoutErrors`·`ClosedErrors`·`LimitErrors`·`UnsupportedErrors`·`UnauthorizedErrors`·`OtherErrors`이고
합은 `Errors`다. 그 사이 생략한 수는 다음 건의 오류에 `(<n> more <분류> errors since the last line)`으로 붙인다
(`errors.Is`는 원래 오류를 그대로 따른다). 마지막 건 뒤 생략분은 `Errors`에만 남는다. 이전에는 진전
상한으로 끝난 Client 명령마다 한 줄씩 나왔다(2026-10-22 연결 규모 시험 R4: 60 Client → 60줄).

## 3. Client 세션 pool(T5, 결정 (b))

- handshake 뒤 Client 예산(`TCPConfig.MaxConnections`) slot을 잡은 **Client 연결만**
  `net_tcp_service_pool_size` permit을 기다린다. 대기는 FIFO다. Client 예산 초과는 즉시
  닫는다(`ClientRejected`, §2.1).
- **대기자 수 상한**: `MaxClientWaiters(MaxConnections) = max(1, MaxConnections/2)`
  (기본 `--max-connections=1024`면 512, `TCPConfig` 기본 128이면 64). 대기자가 이미 상한이면
  새 Client 연결은 기다리지 않고 바로 닫고 `ClientWaitRejected`를 센다. 대기자가 Client slot을
  다 채워 새 Client가 들어오지 못하는 일을 막는다. Agent는 별도 예산이라 영향이 없다.
- **대기 시간 상한**: 연결 accept 때의 `net_tcp_client_so_timeout_ms`(기본 8000ms), 그 값이
  0(무제한)이면 `RequestTimeout`(기본 30초). 넘으면 연결을 닫고 `ClientWaitTimeouts`를 센다.
  Scouter Client(8초)·webapp(30초 CallTimeout)는 그 전에 포기하고 소켓을 닫으므로 더 기다려도
  죽은 요청만 남는다. 대기 deadline은 probe read에도 걸린다.
- **대기 중 peer EOF·read 오류는 buffer에 요청이 있어도 포기**로 본다(slot 반환,
  `ClientAbandoned`). Scouter Client는 half-close하지 않으므로 EOF는 포기한 연결이고, 그 연결이
  보낸 명령(변경 명령 포함)은 나중에 입장해도 실행하지 않는다. 이전의 "half-close 뒤 요청 처리"
  동작은 제거했다. permit을 받은 직후 probe가 이미 EOF를 본 경우에도 permit을 반납하고 포기로
  센다. 입장 뒤에 도착한 EOF는 일반 세션 규칙을 따른다.
- probe goroutine은 bufio `Peek`로 소비 없이 연결 상태를 본다. bufio buffer(4096 byte)가 가득
  차면 감지 없이 대기 시간 상한까지 기다린다.
- 대기 종료(timeout·rejected)는 Java에 없는 Go 줄을 `id=""`로 10초에 한 번만 낸다
  (`log_tcp_action_enabled`와 무관, 서버 종료 중에는 내지 않음):
  `Client : <addr> closed after waiting <ms>ms for a service pool slot (Go service pool wait limit)`,
  `Client : <addr> rejected, waiting clients=<상한> (Go service pool wait limit)`.
  `ClientAbandoned`는 줄을 내지 않는다. 세 경우 모두 `Errors`에는 세지 않는다.
- 서버 종료(ctx 취소·`Close`) 때 대기자는 즉시 끝난다.
- 증가는 대기자를 바로 입장시킨다. 감소는 새 입장만 막고 실행 중 세션은 끊지 않는다.
- 통계: `TCPStats.ClientSessions`, `ClientWaiting`, `ServicePoolSize`, `ClientAbandoned`,
  `ClientWaitTimeouts`, `ClientWaitRejected`.
- 남은 한계: 대기자 상한은 대기 중인 연결만 막는다. `net_tcp_service_pool_size`가
  `MaxConnections/2` 이상이면 실행 중 Client 세션과 대기자가 함께 Client slot을 모두 쓸 수 있다
  (Agent 예산과는 무관).

## 3.1 Client 연결별 Login session 추적(Go 전용)

Java `ServiceWorker`는 연결과 session을 잇지 않는다. Go는 사용하지 않는 Login session을
정리하려고(service 계약 §1) Client 연결이 쓰는 session id를 service에 알린다.

- `TCPHandlers.Sessions`(`ClientSessionObserver`, 선택): pool permit을 받은 Client 연결마다
  `ClientConn.ID`(서버 안에서 유일, 0 아님)를 붙이고, 명령 header의 0이 아닌 session id를
  **마지막으로 쓴 하나만**(`MaxClientConnSessions` = 1) 붙잡는다. Scouter Client는 서버당 session 하나를 쓰고 재로그인 뒤에는 pool 연결로 새 id를 보내므로, 새 id를 보낸 연결은 이전 id를 더 쓰지 않는다(처음 16개 LRU로 두었다가, 같은 연결로 재로그인하는 Client API 소비자의 대체된 session이 연결마다 여러 개 남아 1로 고쳤다). 새 id가 들어오면
  이전 id에 `ClientSessionReleased`, 새 id에 `ClientSessionBound(conn, session)`를 부르고, 연결이
  끝날 때(EOF·오류·`CLOSE`·`INVALID_SESSION`·서버 종료) `ClientSessionReleased`를 부른다.
  Bound 하나에 Released는 정확히 한 번이다. 이미 추적 중인 id는 다시 알리지 않는다.
- 추적은 `Authorize`보다 먼저 한다. 만료된 id를 붙잡을지는 observer(service)가 정한다.
  인증된 연결(latch)이 다른 session id를 보내도 그 id를 추적한다(Java처럼 명령은 실행된다).
- `ClientRequest.Conn.BindSession(id)`: LOGIN handler가 새 session을 그 연결에 바로 묶는다.
  handler가 반환하기 전에 불러야 한다. 연결 없이 만든 `ClientRequest`(0값 `Conn`)는 아무것도
  추적하지 않는다.
- 호출은 연결 goroutine에서 동기로 일어나며 연결별로 순서가 있다. observer는 빠르고
  nonblocking이어야 한다. Agent·`TCP_AGENT_REQ` 연결과 pool 대기 중 끝난 연결은 알리지 않는다.

## 4. Java Logger 줄(T6)

`LogFunc(id, message)`는 timestamp 없이 한 줄을 받는다. ID 없는 Java `Logger.println(msg)`는
`id=""`다. 호출은 동기이므로 함수는 동시성 안전, bounded, nonblocking이어야 한다. S150의
10초 rate limit(Java `Logger.checkOk`)은 **transport가 적용**한다. 호출 측은 줄 형식
(`yyyyMMdd HH:mm:ss [ID] msg`)과 `mgr_log_ignore_ids` 필터만 적용한다.

| 키 | id | message |
|---|---|---|
| `log_tcp_action_enabled` | `""` | `Client : <addr> open #<세션 수>` / `Client : <addr> closed` (EOF·timeout) / `Client : <addr> closed <원인> workers=<세션 수>` / `Agent : <addr> open [<Hexa32 objHash>] #<n>` / `Agent : <addr> close` / `Unknown : <addr> drop` |
| `log_udp_multipacket` | `S160` | `recv <Pack class> total=<n> object=(<objHash 10진>)<objName> /<ip>` |
| `log_expired_multipacket` | `S150` | `MultiPacket total=<n> recv=<m> object=(<objHash 10진>)<objName> /<ip>` |

- `<addr>`는 JDK 8 `InetSocketAddress.toString()`(`/ip:port`, IPv6는 괄호 없이 압축하지 않은
  8 group), `/<ip>`는 `InetAddress.toString()`이다. Hexa32와 주소 문자열은 Zulu JDK 8 실행
  결과를 test vector로 고정했다(`settings_test.go`).
- `CLOSE` 명령으로 끝난 Client는 closed 줄이 없다(Java `return`). 잘못된 session은
  `closed java.lang.RuntimeException: Invalid session key : <cmd> workers=<n>`이다.
- objName은 `ObjectNameFunc`(cache 조회만, storage에서 막히면 안 됨)로 얻고, 없으면 Java처럼
  `null`을 쓴다.
- S160은 조립된 Pack을 handler가 오류 없이 처리한 뒤에만 쓴다. S150은 sweeper 만료와 Decode
  중 만료 판정 두 경로 모두에서 쓴다.
- 서버 종료 중에는 action 줄을 쓰지 않는다.

## 5. 의도된 차이

| 항목 | Java | Go | 이유 |
|---|---|---|---|
| Client 명령 총 시간 | 제한 없음(read마다 SO_TIMEOUT, write 무제한) | 총 시간 제한 없음. 진전 없는 상태가 `RequestTimeout`(기본 30초)을 넘으면 끝냄: write 조각마다, so-timeout 0일 때 명령 안 read마다, handler의 Stream 진전 기준. 단 `AwaitUpstream` Agent 대기 구간(Pack relay와 Blob stream relay 전체)은 감시를 보류하고 relay deadline(Blob은 Agent read마다 so_timeout)과 write 조각 deadline만 적용(원본과 같음) | 읽지 않는 Client나 멈춘 handler가 세션을 무한히 잡지 않게 하는 안전장치. 계속 진전하는 스트리밍 응답과 Agent 응답 대기는 끊지 않는다 |
| Agent 대기 중 Client 이탈 | 감지하지 않음(다음 write/read 때 실패, 그 예외에 따라 `closed` 또는 `closed <exception> workers=N`) | Client socket probe로 EOF·RST를 즉시 감지해 명령을 `ErrClientGone`으로 끝내고 relay를 고아로 넘김. action log는 짧은 `closed` | 감시 보류 동안 떠난 Client의 세션·pool slot을 relay deadline까지 붙잡지 않게 함. Agent 쪽 결과(세션 유지)는 원본과 같다 |
| Client 응답 크기 | 제한 없음 | 명령 누적 제한 없음. 응답 item 하나(encode 결과)가 `MaxResponseValueBytes`(256MiB)+1MiB 이하 | encode한 item을 보내기 전까지 메모리에 들고 있으므로 그 버퍼만 묶는다 |
| Client 응답 메모리 합계(R2) | 없음(heap `-Xmx`만, 넘치면 OOM) | 64KiB 이상 item의 합을 `MaxClientResponseBytes`(GOMEMLIMIT/4 또는 1GiB)로 묶고, 넘치면 대기 상한 뒤 그 명령만 실패(§2.2) | 무거운 조회가 몰려도 프로세스 전체 OOM 대신 해당 명령만 실패 |
| 연결 오류 보고 | 해당 없음(`log_tcp_action_enabled` 줄만) | `tcp.connection` ErrorHandler 보고를 분류마다 10초 1건 + 생략 수(§2.3) | 대량 Client·Agent 이탈 때 로그 폭주 방지 |
| UDP 재조립 상한 초과 | 상한 없음(`MultiPacketProcessor`) | 거부하고 `AssemblyRejected`로 계수(`Malformed`와 별도, `udp.assembly`) | 무제한 메모리 금지, 손상 datagram과 구분 |
| Client 응답 flush | handler마다 다름(XLog 과거 조회는 item마다) | 직전 flush가 200ms 이상 지난 item 뒤에 flush, 명령 끝에 flush | syscall 수와 전달 지연의 절충 |
| Agent 응답 총 시간 | read마다 60초, 총 시간 제한 없음 | read마다 so-timeout + 총 deadline `min(ctx, so_timeout+Transfer)`. server Pack relay의 ctx는 Agent 연결 수명이라 Client가 떠나도 이 deadline까지 읽는다(고아 relay) | 같은 안전장치. Client 진전 상한(30초)이 Agent 세션을 끊지 않게 함(C4) |
| pool 크기 증가(D-N1) | JDK 9+는 반영되지 않음, JDK 8은 core>max로 반영 | 항상 즉시 반영 | 결정: JDK 8 의미 |
| pool 크기 감소 | 초과 worker가 queue가 빌 때까지 계속 처리하고 10초 idle 뒤 종료 | 새 입장을 바로 막음 | 단순한 semaphore 의미 |
| pool 대상 | handshake를 포함한 모든 연결이 thread를 기다림(Agent 연결도 막힘) | handshake 뒤 Client 세션만 대기 | Agent 등록이 Client 포화에 막히지 않게 함 |
| 대기 중 끊김(D-N2) | 감지하지 않고 thread가 날 때까지 queue에 남음. 나중에 입장하면 buffer에 남은 명령을 실행 | probe로 감지해 slot 반환. buffer에 명령이 있어도 실행하지 않음(`ClientAbandoned`) | 자원 반환. Client가 이미 실패로 보고한 명령(변경 명령 포함)의 지연 실행 방지 |
| 대기 시간 | 무제한(queue 대기 중 SO_TIMEOUT 없음) | client so-timeout(0이면 `RequestTimeout`) 뒤 연결 종료(`ClientWaitTimeouts`) | 포기한 Client의 zombie 대기자가 slot을 채우지 않게 함 |
| 대기열 크기 | 무제한 `LinkedBlockingQueue` | `max(1, MaxConnections/2)`, 초과는 즉시 종료(`ClientWaitRejected`) | 무제한 queue 금지, Client slot 일부를 새 Client 몫으로 남김 |
| 연결 수 상한(C1) | 없음(accept 무제한, Agent는 objHash당 queue 50·objHash 5000개) | handshake 2048·Agent 16384·Client `MaxConnections` 세 예산(§2.1), 초과는 reset으로 즉시 종료 | 무제한 자원 금지. Agent 예산을 Client와 분리해 Client 포화·느린 handshake가 Agent 등록을 막지 않게 함 |
| 대기 종료 로그 | 없음 | Go 전용 줄 10초 1회 | 운영자가 포화를 알 수 있게 함 |
| handshake timeout | client so-timeout(8초) | `HandshakeTimeout`(5초) | 기존 Go 계약 유지 |
| timeout·NPE stack trace | stderr에 출력 | 출력하지 않음 | Logger 줄이 아님 |
| 기타 예외 문구 | Java 예외 `toString()` | Go 오류 문자열(제어문자 `?`, 256 byte 절단) | Java class 이름을 재현할 수 없음. peer 입력 log injection 방지 |
| `Agent ... close` | `close()` 호출마다 | 연결당 한 번 | 중복 제거 |
| Agent `#n` | `TcpAgentManager` idle queue 크기 | transport가 센 같은 objHash의 열린 Agent 연결 수 | transport는 server pool을 모른다 |
| 잘린 UDP datagram | 잘린 채 parse해 `S159` | 거절하고 `Rejected` 집계 | 부분 parse 금지 |
| UDP buffer 상한 | 65535 | 65507 clamp | IPv4 최대 payload |

## 5.1 UDP queue 통계

`UDPStats.QueueLen`/`QueueCap`은 worker queue(Java `NetDataProcessor` queue 대응)의 현재 길이와
용량이다. `Serve`가 queue를 소유하는 동안만 값이 있고, `Serve` 전과 worker drain·join 뒤에는
둘 다 0이다. channel `len`/`cap`과 atomic pointer로 읽으므로 어느 goroutine에서든 race 없이
부를 수 있다. Health JSON `UDP` 객체와 stats 로그 줄 `udp={...}`에 두 필드가 추가된다
(`scouter-replay` 파서는 추가 필드를 보존한다).

## 5.2 UDP socket 진단(Go 전용, R8)

- `UDPConfig.ReadBuffer`가 0보다 크면 `NewUDPReceiver`가 `SO_RCVBUF`로 설정한다(실패하면 생성 오류).
  0이면 호출자가 연 socket 그대로 둔다. `UDPStats.ReadBufferRequested`는 이 값, `ReadBufferEffective`는
  생성 때 `getsockopt(SO_RCVBUF)` 값이다. Linux는 승인 크기의 2배를 보고하고 `net.core.rmem_max`(권한
  없는 설정)로 잘린다. 요청이 4MiB인데 425984면 rmem_max가 212992라는 뜻이다. macOS는 요청값 그대로이고
  `kern.ipc.maxsockbuf`를 넘으면 설정 자체가 실패한다.
- Linux만 kernel 계수를 읽는다(`udp_diag_linux.go`). socket 부분과 host 부분은 따로 읽고 따로 실패한다.
  - socket(`KernelCounters=true`): 이 socket(fstat inode)의 `/proc/net/udp` 또는 `/proc/net/udp6` 행의
    `drops`(`SocketDrops`, 수신 buffer 포화 등 kernel이 버린 datagram)와 `rx_queue`(`SocketQueuedBytes`).
    rx_queue는 payload가 아니라 skb truesize 기준 수신 buffer 메모리라 payload 합보다 크다.
  - host(`HostCounters=true`): UDP `RcvbufErrors`·`InErrors`(`/proc/net/snmp` `Udp:`와, 있으면
    `/proc/net/snmp6` `Udp6*`의 합, 부팅 이후 누적, 같은 network namespace의 다른 socket 포함). kernel은
    수신 buffer 오류 때 두 계수를 함께 올리므로 **`InErrors`는 `RcvbufErrors`를 포함한다(더하지 않는다)**.
  - 못 읽은 부분은 false와 0이고, 첫 원인을 `KernelCountersError`에 적는다(예: 닫힌 socket이면
    `socket not found in /proc/net/udp{,6} (closed?)`, host 값은 그대로 나온다).
  - 수신 loop는 읽지 않고 `Stats()`가 1초에 최대 한 번 읽는다(`/proc/net/udp` 한 번 scan).
- macOS 등 Linux 밖: `KernelCounters=false`, `HostCounters=false`, `KernelCountersError="kernel UDP counters not supported on <GOOS>"`. macOS에는 socket별 drop 계수가 없고 host 전체 값(`netstat -s`
  의 "dropped due to full socket buffers")은 읽지 않는다. `SO_RXQ_OVFL`(Linux, datagram마다 cmsg)은
  수신 hot path에 비용을 더하므로 쓰지 않았다.
- 배선: `internal/server`는 `--udp-read-buffer`(`net_udp_so_rcvbuf_size`)를 `UDPConfig.ReadBuffer`로 넘긴다.
  stats 줄의 `malformed`에는 재조립 상한 초과가 빠지고 `assembly N`과 Linux `sockdrop N`이 따로 나온다.

## 6. 검증

- `go test -race ./internal/transport/...`: so-timeout 새 연결 적용·per-read 갱신·0(무제한)·음수 거절,
  Agent 응답 timeout과 trickle 응답, pool 대기·증가·감소·대기자 끊김·종료·Agent 비차단,
  대기 시간 상한(so-timeout, 0이면 `RequestTimeout`)·로그 rate limit, 명령을 보낸 뒤 close/half-close한
  대기자의 명령 미실행(handler 호출 0), 대기자 상한 즉시 거절과 Agent slot 확보, pool resize 중
  결과 집계(served+timeout+rejected), `sessionGate` 동시 resize·대기자 상한 race, action/S150/S160 문구와 rate limit, UDP helper.
  Client 연결별 session 추적(마지막 session 하나만 보유·새 id가 오면 이전 id release, LOGIN bind·EOF/CLOSE/INVALID_SESSION/종료 release 짝, Agent 연결 무관,
  동시 연결과 서버 종료 race: `client_sessions_test.go`).
- Agent 대기 보류(`client_upstream_test.go`): 진전 상한의 4배 대기 뒤 응답 전달, Client close·RST 시
  1초 안에 `ErrClientGone`, 대기 중 Client가 보낸 다음 명령 정상 파싱, 서버 종료 시 즉시 종료, 보류 해제 뒤 일반
  감시 재개(보류 없는 이전 동작에서 4개 FAIL 확인, 서버 종료 항목은 회귀 방지). 추가 검증: panic 뒤 복원,
  중첩 호출, 대기 뒤 read so-timeout, 다음 명령 byte 뒤 close·RST, 3000회 반복 goroutine 누수, RST 이탈의
  `ClosedErrors`·action log 줄. 서비스 pool 대기 probe는 같은
  `probePeer` helper를 쓰며 기존 pool 대기 테스트로 검증한다.
- 응답 메모리 예산(`client_budget_test.go`): 읽지 않는 Client가 쥔 예산 때문에 다음 큰 item이 대기 상한 뒤
  거절(응답 0 byte·연결 종료·`ErrLimit` 보고), 그동안 작은 item은 정상, 쥔 item은 끝까지 전달, 반납 시 대기자
  입장, 예산보다 큰 단독 item·대기 합 상한 즉시 거절·first fit·종료 시 대기 해제, GOMEMLIMIT 기본값.
  연결 오류 보고(`conn_errors_test.go`): 진전 상한 5건 → 보고 1건, 생략 수 전달, 분류. UDP 진단
  (`udp_diag_test.go`, Linux `udp_diag_linux_test.go`): 재조립 상한 4종이 `ErrAssemblyLimit`·
  `AssemblyRejected`(Malformed 0), 요청·실제 `SO_RCVBUF`, `/proc` parser, Linux에서 읽지 않는 4KiB buffer
  socket의 `SocketDrops`·`SocketQueuedBytes`·host `RcvbufErrors` 증가와 wildcard(dual-stack) socket 조회, 닫힌
  socket에서 host 값만 읽힘, `/proc` parser 비정상 입력. 추가 검증(2026-10-23): relay Blob chunk가 가득 찬
  예산에서 대기·거절 없이 나감(수정 전 FAIL), 둘째 item 거절 시 첫 item 뒤 NoNEXT 없이 EOF, 쓰기 실패 뒤 예산
  반납, 대기 중 `Close`로 즉시 종료, acquire/release/timeout race stress, unknown magic 분류·`reportConnError`
  동시 호출.
- Java 서버 실측 대조(Client idle 종료 시각, 로그 줄 정규식)는 하지 않았다. 문구는 원본 코드
  판독, Hexa32·주소 문자열은 JDK 8 실행 결과로만 확인했다.

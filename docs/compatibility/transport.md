# UDP/TCP 수신 계약

정본은 [baseline.json](baseline.json)의 Scouter `2.21.3`, commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. 아래 원본 경로·줄은 해당 root 기준이다. `internal/transport`는 socket 수신·framing·수명 경계를 구현한다. 서버 core, 229개 Client 명령, 로그인 DB, 저장, Agent connection manager, Pulse 전파의 전체 구현을 의미하지 않는다.

## 원본 근거

| 경계 | 원본 |
|---|---|
| UDP magic·single/batch/fragment | `scouter.common/src/main/java/scouter/net/NetCafe.java:24`, `scouter.server/src/main/scala/scouter/server/netio/data/NetDataProcessor.scala:71`, `scouter.agent.java/src/main/java/scouter/agent/netio/data/net/DataUdpAgent.java:93` |
| Java fragment cache·TTL | `scouter.server/src/main/scala/scouter/server/netio/data/MultiPacketProcessor.scala:51`, `scouter.server/src/main/scala/scouter/server/netio/data/MultiPacket.scala:34` |
| TCP handshake·Client session·CLOSE | `scouter.server/src/main/scala/scouter/server/netio/service/net/TcpServiceWorker.scala:78`, `scouter.common/src/main/java/scouter/net/RequestCmd.java:335`, `scouter.client/src/scouter/client/net/TcpProxy.java:141` |
| Agent V1/V2 명령과 응답 | `scouter.server/src/main/scala/scouter/server/netio/service/net/TcpAgentWorker.scala:20`, `scouter.agent.java/src/main/java/scouter/agent/netio/data/net/TcpWorker.java:76` |
| AgentRequest·stack upload | `scouter.server/src/main/java/scouter/server/netio/req/net/TcpAgentReqWorker.java:25`, `scouter.server/src/main/java/scouter/server/netio/req/net/TcpSendStack.java:15`, `scouter.agent.batch/src/main/java/scouter/agent/batch/netio/data/net/TCPStackZipWorker.java:123` |

## UDP wire와 처리

정수는 big-endian이다. Blob은 Scouter의 1/3/5byte 길이 prefix이며 Java signed short 범위를 유지한다. Pack에는 공통 길이가 없다.

| magic | body |
|---|---|
| `CAFE` / `JAVA` | Pack 1개 |
| `CAFN` / `JAVN` | int16 count + 연속 Pack count개 |
| `CAFM` / `JMTU` | int32 objHash + int64 pkid + int16 total + int16 index + Blob fragment |

`Reassembler.Decode(sender, datagram, now)`는 batch 전체를 검증한 뒤 Pack 목록을 반환한다. fragment가 미완성이면 빈 목록을 반환하고, index 순으로 합친 body에서 Pack 1개를 해석한다. 동일 fragment의 동일 바이트 재전송은 중복 슬롯을 늘리지 않는다. 완료된 packet 전체 재전송에 대한 exactly-once 보장은 제공하지 않는다.

원본의 pkid-only cache와 달리 `(sender IP, sender port, objHash, pkid)`를 key로 사용한다. IPv4-mapped 주소는 정규화한다. 서로 다른 송신자의 같은 pkid를 합치지 않는다. 다음 차이는 잘못된 입력에 대한 의도된 방어다.

- negative count, total=0, 잘못된 index, count 변경, 내용이 다른 duplicate, trailing bytes를 거부한다. batch count=0은 빈 목록으로 허용한다.
- assembly TTL은 생성 시점 기준이며 duplicate로 연장하지 않는다. 같은 key의 늦은 fragment가 만료 assembly를 발견하면 `ErrExpired`를 반환하고 해당 assembly를 제거한다.
- idle expiry는 별도 sweeper가 실행하며 늦은 fragment와 동일한 만료 event·계수를 사용한다.
- 단일 datagram·batch Pack 수·fragment 수·assembly 수·assembly byte·전체 buffered byte를 제한한다. 상한 초과는 묵시적 eviction 성공으로 바꾸지 않는다.

기본 UDPConfig는 workers 2, queue 8192(queued payload 32MiB 상한), datagram 65,507byte, batch/fragment 각 1,024개, assemblies 128개, assembly 8MiB, 전체 fragment payload 16MiB, TTL 1초다. payload budget 외에도 최대 `MaxAssemblies * MaxFragments` slice 슬롯과 `(queue + workers)` datagram 메모리가 있으므로 이를 RSS 상한으로 해석하지 않는다. 공유 서버의 실제 예산은 별도로 조정한다.

`NewUDPReceiver(*net.UDPConn, config, PacketHandler, ErrorHandler)`는 성공 시 socket 소유권을 받는다. `Serve(ctx)`가 읽기·고정 worker·sweeper를 실행한다. queue 포화는 즉시 datagram을 거부하고 보고한다. buffer와 decoded Pack 데이터는 detached 소유권으로 callback에 전달한다. Counter 시간 보정·object 활성화·text dictionary 갱신 등 core 의미는 PacketHandler 책임이다.

종료 시 socket을 닫고 worker context를 취소한다. queued datagram을 거부하고 worker/sweeper를 join한 뒤 fragment state를 해제한다. 종료 시 무조건 drain 성공을 주장하지 않는다. Stats 단위는 다음과 같다.

| Stats field | 단위 |
|---|---|
| Datagrams / Rejected / Malformed | 읽은 datagram / queue·size·종료 거부 datagram / decode 실패 datagram(재조립 상한 제외) |
| AssemblyRejected | 재조립 상한(`ErrAssemblyLimit`: assembly 수·전체 buffered byte·assembly byte·fragment 수)으로 거부한 fragment datagram. 2026-10-23부터 `Malformed`에서 분리, ErrorHandler operation `udp.assembly` |
| Packets / HandlerErrors / AbortedPackets | callback 성공 Pack / callback 실패 Pack / batch 중 취소로 dispatch하지 않은 나머지 Pack |
| Expired / AbortedAssemblies | 만료 assembly / 종료 시 남은 assembly |
| ReadBufferRequested / ReadBufferEffective | `UDPConfig.ReadBuffer`로 receiver가 요청한 `SO_RCVBUF`(0: 설정하지 않음) / `getsockopt(SO_RCVBUF)` 값. Linux는 승인 크기의 2배를 보고하고 `net.core.rmem_max`로 잘린다(요청 4MiB, rmem_max 212992면 425984) |
| KernelCounters, HostCounters, KernelCountersError, SocketDrops, SocketQueuedBytes, HostRcvbufErrors, HostInErrors | Linux만. socket 부분(`KernelCounters`): 이 socket의 kernel drop(`/proc/net/udp{,6}` drops)·수신 buffer 메모리(rx_queue, skb truesize 기준이라 payload보다 큼). host 부분(`HostCounters`): UDP `RcvbufErrors`·`InErrors`(`/proc/net/snmp` + 있으면 `snmp6`, 부팅 이후 누적, 같은 network namespace의 모든 UDP socket 합). `InErrors`는 `RcvbufErrors`를 포함하므로 더하지 않는다. 두 부분은 따로 실패하고 원인은 `KernelCountersError`에 남는다. 1초 cache. macOS 등에서는 둘 다 false이고 0이다 |

이 계수는 transaction TPS가 아니다. 저장 성공은 이 수신 계층이 측정하지 않는다. kernel drop은 Linux에서만 읽는다(위 표).

## TCP wire

handshake와 후속 body는 **같은 buffered reader**를 사용한다. 소켓 stream을 handshake 뒤 새 reader로 바꾸면 이미 읽어 둔 body가 유실될 수 있다.

| magic | handshake 뒤 역할 |
|---|---|
| `0xcafe2001` Client | Text command + fixed int64 session + 명령별 body |
| `0xcafe1001` Agent V1 | fixed int32 objHash. 서버가 Text command + Pack을 보내고 agent가 flag + Pack으로 응답 |
| `0xcafe1002` Agent V2 | fixed int32 objHash. 서버가 int32 length + (Text command + Pack)을 보내고 agent가 flag + int32 length + Pack으로 응답 |
| `0xcafe1011` AgentRequest | fixed int32 objHash. 이후 int32 request code와 request별 body |

`HasNEXT=0x03`, `NoNEXT=0x04`, `INVALID_SESSION=0x44`, stack `OK=0x01`이다. V2에서도 NoNEXT에는 length가 붙지 않는다. Agent V1/V2는 unsolicited telemetry 수집 경로가 아니다. `TCP_SHUTDOWN` 및 알 수 없는 magic/request는 지원하지 않으며 연결을 닫고 오류를 보고한다.

### Client dispatcher

`TCPHandlers.Client(ctx, ClientRequest, *Stream)`이 명령별 body를 정확히 소비한다. body 없음, Pack, Value, Blob 등을 분리하며 범용 envelope를 만들지 않는다. `Stream.ReadPack/ReadValue`는 protocol의 streaming API로 객체 1개만 소비한다. codec byte/Blob 상한은 현재 command의 남은 transport byte 예산으로도 제한한다. partial read와 coalesced command를 같은 stream에서 처리한다.

`CLOSE`는 Text 뒤 session/body가 없다. `LOGIN/SERVER_VERSION/SERVER_TIME`은 원본의 free command다. 첫 protected command의 session은 body 처리 전에 Authorize callback으로 검증한다. authorizer가 없거나 false이면 `0x44`를 보내고 닫는다. 원본처럼 인증 성공은 connection latch이므로 이후 명령의 session을 다시 검증하지 않는다. 실제 LOGIN의 자격증명·policy·session 저장은 dispatcher 책임이다. request의 Peer는 body가 주장하는 주소가 아니라 실제 socket peer다.

응답 item은 encode가 끝난 byte로 socket에 넘겨질 때까지 메모리에 있다. 64KiB 이상인 item(Agent Blob relay chunk `ResponseRelayBlob`은 relay 예산이 세므로 제외)은 모든 Client 연결이 함께 쓰는 응답 메모리 예산(`TCPConfig.MaxClientResponseBytes`, 기본 GOMEMLIMIT의 1/4 또는 1GiB)을 HasNEXT 전에 잡고, 자리가 없으면 연결의 pool 대기 상한(최대 `RequestTimeout`)까지 기다린 뒤 `ErrResponseBudget`(ErrLimit)으로 명령을 실패시킨다. 세부는 [transport CONTRACT](../../internal/transport/CONTRACT.md) §2.2.

dispatcher는 `ResponsePack/ResponseValue/ResponseBlob` 또는 명령에 맞는 Stream writer로 응답 item을 작성한다. transport는 callback이 정상 반환한 경우에만 최종 NoNEXT를 flush한다. callback 미등록·미지원 명령·body/handler 오류는 빈 성공 응답을 만들지 않는다. dispatcher가 명령 schema보다 덜 소비하거나 다음 명령까지 소비하는 오류는 transport가 범용 length로 검증할 수 없으므로 명령별 golden·통합 검증이 필수다.

### Agent request channel

`TCPHandlers.Agent(ctx, *AgentConn)`은 연결이 사용할 수 있는 동안 유지되며 반환하면 socket이 닫힌다. connection pool·objHash routing·KEEP_ALIVE 주기는 Runtime manager 책임이며 현재 연결 계약은 [실행 계약](runtime.md)과 [Agent relay](agent-read-query.md)를 따른다. callback이 socket을 registry에 넣고 즉시 반환하는 방식은 사용할 수 없다.

`AgentConn.Request(ctx, cmd, parameter, receive func(context.Context, *Stream) error)`는 custom 명령 응답을 처리한다. `RequestPack`은 일반 Pack 응답만 처리하고 receive에 `(context.Context, protocol.Pack)`을 전달한다. 특수 명령의 raw Blob/stream 응답을 V2 일반 Pack 응답이라고 가정하지 않는다.

request는 connection별 gate로 직렬화한다. gate 대기는 caller context로 취소할 수 있고, 대기 중 취소는 진행 중 요청의 socket을 닫지 않는다. gate를 획득한 뒤 RequestTimeout/caller deadline의 빠른 시각을 socket과 response callback context에 적용한다. 요청 중 취소·오류는 해당 연결을 닫아 불완전 응답을 다음 명령으로 오인하지 않는다. response callback은 취소에 협조해야 한다.

### TCP_SEND_STACK

AgentRequest의 `0xeded0001` body는 fixed int64 startTime + Text objName + Text filename + fixed int64 fileSize + raw fileSize bytes다. ZIP 바이트에 Blob/intBytes envelope가 없다.

`TCPHandlers.Stack(ctx, StackUpload, io.Reader)`에는 정확히 fileSize로 제한된 reader를 전달한다. 음수·초과 크기는 할당·callback 전에 거부한다. callback이 전부 소비하고 정상 반환한 후에만 OK 1byte를 flush한다. truncated body, 덜 소비한 body, deadline, callback 오류는 OK를 보내지 않는다. 여러 upload를 한 연결로 이어 보낼 수 있다.

transport는 ZIP 해석·파일 경로·영속 저장을 구현하지 않는다. fixture의 ZIP 표식 바이트는 framing 대조용 synthetic payload다. filename은 untrusted metadata이며 저장 handler가 안전한 경로와 완료/rollback을 책임진다. callback의 nil은 저장 계약이 완료되었다는 의미로 사용해야 한다.

## 수명·상한·오류 보고

기본 TCPConfig는 연결 예산 handshake 2048·Agent 16384·Client 128, command read/write 각각 8MiB, Text 64KiB, raw stack upload 64MiB, handshake 5초, idle socket I/O 60초, request 30초다. Client와 stack의 body callback 및 Authorize에도 동일 request deadline context를 전달한다. Agent lifetime callback의 idle 관리와 keepalive는 manager 책임이며 개별 Agent request에는 request deadline을 적용한다.

`NewTCPServer(listener, config, handlers, onError)`는 성공 시 listener 소유권을 받는다. 연결 예산은 세 갈래다. magic을 읽기 전 socket은 `MaxHandshakes`, Agent magic(`TCP_AGENT`·`TCP_AGENT_V2`·`TCP_AGENT_REQ`) 뒤에는 `MaxAgentConnections`, `TCP_CLIENT` 뒤에는 `MaxConnections` slot을 쥔다. handshake 상한을 넘은 연결은 worker를 만들지 않고, Agent·Client 상한을 넘은 연결은 magic 판독 직후 읽기·대기 없이 `SO_LINGER 0`으로 닫는다. 거절은 사유별로 `TCPStats`에 세고 예산마다 10초 1줄 로그(생략 수 누적)를 낸다.

원본과의 차이: Java `TcpServer`는 accept에 상한이 없고 Agent 연결은 handshake 뒤 `TcpAgentManager`에 남으므로 Agent 연결 총수 상한이 없다. Go는 무제한 자원을 두지 않으려고 Agent 예산을 bounded(기본 16384, idle 연결당 Go 메모리 약 14KiB 측정 기준 최악 약 224MiB)로 두되 Client 예산과 분리해 Client 포화나 느린 handshake가 Agent 등록을 막지 않게 했다. 상한에 걸린 Agent에는 서버가 내려간 것과 같은 동작(즉시 reset, Agent는 1초 뒤 재시도)만 보인다. 상세와 로그 형식은 [transport CONTRACT](../../internal/transport/CONTRACT.md) §2.1이다. `Serve(ctx)` 종료는 listener 및 등록된 connection 닫기, callback context 취소, worker join을 포함한다. `Close`는 중복 호출 가능하다. 단일 receiver/server 객체는 Serve를 한 번만 실행한다.

모든 callback은 동기식이며 취소에 협조해야 한다. Go는 context를 무시하는 사용자 callback을 강제 중단하지 않으므로 그런 callback은 join을 막을 수 있다. `ErrorHandler`는 receive/worker goroutine에서 실행되며 concurrency-safe·bounded·nonblocking이어야 한다. ErrorHandler에서 Serve 종료를 기다리거나 blocking network 작업을 하지 않는다.

## 검증

`TransportOracle.java`는 고정 Java `DataOutputX`를 사용해 `transport-*.bin` 18개를 만든다. 기본 fixture 경로는 `internal/protocol/testdata/java`이고 기존 생성기의 manifest/checksum 검증에 포함한다. 실제 socket 테스트는 `127.0.0.1:0`으로 격리한다.

검증 범위:

- 6개 UDP magic, Java byte fixture, batch, 역순·동일 duplicate fragment, sender 격리, count/index/size/queue 상한, TTL idle·same-key expiry, oversized/truncated 수신, 취소·socket 종료, concurrent reassembly.
- Client partial byte read/coalescing, LOGIN/CLOSE, bodyless 명령, protected session latch, invalid session, 미등록 handler, malformed header/body, callback deadline.
- Agent V1/V2의 서버 명령·flagged 응답 byte 일치, NoNEXT, custom Blob, V2 negative length, 대기 요청 취소·연결 유지, response callback deadline.
- stack metadata/raw byte/OK 일치, 덜 소비한 body·초과 크기·truncation 거부, 연속 upload, listener 포화·handshake timeout·종료 후 connection 수 0.
- race와 bounded `FuzzUDPDatagram/FuzzTCPHeader`; protocol streaming fuzz는 별도 gate다.

```sh
make fixtures-check
.tools/go/bin/go test -count=1 ./internal/transport
.tools/go/bin/go test -race -count=1 ./internal/transport
.tools/go/bin/go test ./internal/transport -run='^$' -fuzz='^FuzzUDPDatagram$' -fuzztime=10s -parallel=2
.tools/go/bin/go test ./internal/transport -run='^$' -fuzz='^FuzzTCPHeader$' -fuzztime=10s -parallel=2
```

Java golden·loopback은 운영 Agent/Client 전체 상호운용, core 저장/조회, 실제 ZIP 파일 저장, 다수 연결의 자원 사용, 목표 처리량과 OS별 운영 검증을 대신하지 않는다.

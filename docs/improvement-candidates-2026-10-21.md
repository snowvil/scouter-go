# 운영 전환 전 개선 대상 (2026-10-21)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

이 문서는 2026-10-21 조사와 이후 10-25까지의 단계별 실행 이력이다. §1~§6의 문제·제안은 조사 당시 상태이며 후속 수정은 §7~§20을 따른다. 현재 구현 상태와 남은 gate는 [구현 순서](implementation-plan.md), 최신 소스 대조는 [2026-11-07 점검 기록](validation/2026-11-07-source-docs-sync.md)을 확인한다.

운영 전환을 앞두고 개선할 곳을 찾았다. 기준은 세 가지이고, 이 순서로 중요하다.

1. 호환성. 서버가 오동작해도 Agent(고객 애플리케이션 JVM)가 영향을 받으면 안 된다.
2. 자원 절감과 과도한 경합 제거.
3. 기존 처리 구조의 문제.

## 0. 조사 방법과 교차 검증

| 분석 | 범위 | 방법 |
|---|---|---|
| A. Agent 영향 | 서버 상태(down, hang, 잘못된 응답, 재시작, UDP 유실, text·object 유실)별로 Agent에 생기는 일 | 원본 Agent 코드(`scouter.agent.java`, host, batch) 정적 분석 |
| B. 호환성 검증 공백 | 기능군별 증거 단계와 운영 전 공백 | 계약·검증 문서와 원장 대조 |
| C. 구조 위험 | 장애 전파, 상한, 경합, 주기 작업, 종료·복구 | Go 코드 독립 리뷰(읽기 전용) |
| D. 교차 검증 | A~C 같은 과제 | 같은 과제를 별도의 독립 검증으로 한 번 더 수행 |

- 아래 항목에는 **일치 수**(A~D 중 몇 개가 독립적으로 지적했는지)와 **코드 확인** 여부를 붙였다. 코드 확인은 지적된 파일:줄을 다시 읽어 사실을 확인했다는 뜻이다.
- 장애 영향은 재현 전 추론이다. fault injection과 실제 Agent 규모 시험은 하지 않았다.
- 실제 Agent 환경 장기 감시의 결론은 §5에 따로 정리했다.

## 1. 호환성과 Agent 영향 (최우선)

| # | 항목 | 일치 | 코드 확인 | 증상(추론) | 개선 방향 | 검증 |
|---|---|---|---|---|---|---|
| C1 | **TCP 연결 총상한 기본 1024를 Agent와 Client가 함께 쓴다.** 대기 Client가 최대 절반을 차지할 수 있고, 상한을 넘은 연결은 accept 직후 닫힌다(`cmd/scouter-go/main.go:49`, `internal/transport/tcp.go:152,250-276`). 원본 Java에는 Agent 연결 상한이 없다 | 4/4 | 예 | Agent 수가 이 값에 근접하면, 넘친 Agent가 로그 없이 1초마다 재접속을 반복한다. 그 Agent에는 서버 발신 명령(KEEP_ALIVE, `OBJECT_RESET_CACHE`, kube seq, CPU dump)이 닿지 않아 서비스명이 영구히 비게 된다. Client 접속도 거절될 수 있다 | Agent와 Client 예산을 분리하고 Agent 예약분을 둔다. 운영 규모에 맞게 기본값과 `ulimit -n`을 정한다. 거절 사유를 `/health`에 노출한다 | 격리 환경에서 상한+α개 가짜 V2 Agent와 Client burst, slow handshake로 Agent 연결이 유지되는지 본다 |
| C2 | **object 4096, counter key 65536, engine cache 64MiB가 설정할 수 없는 고정값이다.** 기동할 때 당일 object가 4096을 넘으면 Engine 생성이 실패한다(`internal/ingest/engine.go:222-257`). 메모리의 object는 `OBJECT_REMOVE`로만 지워진다(`object_remove.go:92`) | 3/4 | 예 | object가 하루에 계속 늘면(예: pod 이름 변경) 새 heartbeat와 counter가 거절되고 일부 Agent가 목록에서 빠진다. 최악의 경우 서버가 기동하지 못한다 | 설정 키로 뺀다. 기동 시에는 상한까지만 올리고 경고한다. 원본의 object 관리(자동 정리 여부)와 맞춘다 | 상한 근처 replay, object 레코드 4097개 기동 테스트 |
| C3 | **text 사전 영구 누락 연쇄.** Agent는 text를 "보냄"으로 표시한 뒤 UDP로 한 번만 보낸다. 서버가 그 UDP나 text queue 항목을 잃으면 다음 reset까지 이름이 빈다. 재기동 30초 뒤의 `OBJECT_RESET_CACHE`는 1회 순차 처리이고 실패나 미연결 Agent를 그날 다시 시도하지 않는다(`internal/server/text_cache_reset.go:47-79`) | 4/4 | 예 | 원본과 같은 의미지만 규모가 커지면 놓치는 Agent가 는다. 재기동·자정 직후 재전송 burst로 text drop이 날 수 있다 | 기동 뒤 처음 연결·등록되는 Agent에 1회 reset을 보낸다. 실패 Agent에는 bounded 재시도를 한다(의도된 차이로 기록) | Agent 일부 TCP 지연 접속, reset 중 offline·busy 시나리오에서 이름 누락을 확인 |
| C4 | **Client 명령의 30초 무진전 watchdog이 Agent relay를 끊을 수 있다.** Client 명령 context가 `withProgress(..., RequestTimeout=30s)`이다(`internal/transport/tcp.go:551`). Agent 응답을 기다리는 동안 진전이 없으면 Agent 소켓을 닫는다. 원본은 Client가 포기해도 Agent 응답을 끝까지 읽는다 | 1/4 | 일부(진전 판정은 재현 필요) | 큰 heap histo, thread dump가 30초를 넘으면 실패하고 Agent가 재접속한다. 재시도하면 STW 작업이 반복된다 | Agent relay 중에는 watchdog을 relay deadline에 맞춘다. 또는 문서를 정정한다 | 가짜 Agent의 40초 지연 응답으로 Java와 Go를 비교 |
| C5 | **그룹 relay 오류 전파 차이.** Agent 1개가 실패하면 Go는 그룹 전체를 실패로 끝낸다(`internal/service/agent_queries.go:150-152`). 원본은 그 Agent를 null로 넘기고 계속한다 | 1/4 | 예 | 그룹 active service 같은 화면에서 Agent 하나의 문제로 전체가 빈다 | 원본처럼 실패 Agent만 건너뛴다 | Agent 3개 중 1개를 끊고 Java와 Go의 응답 Pack 수 대조 |
| C6 | **서버가 스스로 보내는 명령의 실제 Agent 검증이 없다.** `OBJECT_SET_KUBE_SEQ`는 host agent의 object 이름을 바꾸고, `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`는 dump 파일을 만든다. 문서(`agent-side-effects.md`)의 "2종뿐" 서술은 낡았다. reason 값이 바뀌면 Agent가 가드 없이 dump를 실행한다 | 2/4 | 예 | 잘못 보내면 Agent에 파일과 부하가 생긴다 | 문서를 정정하고, reason 상수 golden 테스트를 둔다 | 실제 host agent(kube 모드)와 autodump 설정 Java agent로 확인 |
| C7 | **위험 관리 relay(GC, thread 제어, heap dump, 설정·클래스 변경)에 group policy 평가가 없다**(`internal/service/manage_commands.go:53-73`) | 1/4 | 예(원본 권한 모델과의 비교는 미확인) | 일반 계정이나 운영 실수로 다수 Agent에 GC나 dump가 번질 수 있다 | 원본 권한 모델을 확인한 뒤 명령별 권한, 대상, 빈도를 제한한다 | 계정별 허용·거절, 격리 JVM에서 부작용 확인 |
| C8 | **V2 framing 불변식.** Agent는 서버가 선언한 길이를 검증 없이 할당한다(`DataInputX.java:71-80`). Go는 encode 뒤 길이를 쓰고 8MiB 상한을 둬서 안전하지만, 회귀를 막는 단언이 없다 | 2/4 | 예 | 서버 버그 하나가 곧 앱 OOM으로 이어진다 | "선언 길이 = 실제 길이 ≤ 상한"을 fuzz로 단언한다(V1 포함) | encoder fuzz, Java `TcpAgentWorker.write`와 byte 대조 |

- **확인된 안전 사항**(4/4 일치):
  - Agent 애플리케이션 스레드는 서버 상태와 무관하다. 비연결 UDP send만 하고, TCP는 daemon 풀이 처리한다.
  - 서버 down, hang, UDP 유실이 앱 스레드를 막거나 Agent queue를 무한히 늘리지 않는다. UDP ring은 1024개, TCP는 `session_count`개다.
  - 거대 길이와 framing에 대해서는 Go가 원본보다 안전하다.
  - 다만 XLog·SQL·profile 송신은 앱 스레드에서 동기 syscall로 실행된다. 그래서 OS 송신 버퍼 수준의 비용은 남는다(D).

## 2. 구조와 기능 격리

| # | 항목 | 일치 | 코드 확인 | 증상(추론) | 개선 방향 |
|---|---|---|---|---|---|
| S1 | **engine lock 아래 동기 디스크 I/O.** 대상은 counter, object, status, alert, summary, stack, batch, span, visitor load/save, 새 counter 이름 fsync다(`internal/ingest/engine.go:322-350,427`, `packet_state.go:87-211`, `objects.go:150`) | 2/4 | 예 | 디스크 지연이나 Store·Counters gate의 장기 hold(profile·gxid 조회, purge) 동안 XLog를 포함한 모든 수집이 멈춘다. 이후 UDP reject와 실시간 조회 timeout이 생긴다 | 원본처럼 종류별 bounded async writer(포화 drop, 계수)를 둔다. engine lock은 메모리 상태 갱신에만 쓴다 |
| S2 | **cache 예산이 차면 XLog와 counter가 영속 저장 전에 거절된다**(`internal/ingest/sampling.go:355`, `engine.go:393-425`) | 1/4 | 예 | sampling 지연 bucket이나 gxid map이 예산을 채우면 XLog가 디스크에도 남지 않는다. 기능 격리 위반이다 | 영속 저장과 cache 예산을 분리한다. cache 거절 시 cache만 건너뛴다 |
| S3 | **저장 실패 시 실시간 상태도 갱신되지 않는다.** counter cache, alert ring, object liveness가 해당한다(`engine.go:427`, `packet_state.go:165-176`, `objects.go:100-152`) | 1/4 | 일부 | ENOSPC가 나면 감시 화면에서 counter와 alert가 사라지고, 자정 object 재기록이 실패하면 전 객체가 INACTIVE가 될 수 있다 | 메모리 갱신을 먼저 하고 영속 저장은 비동기로 하며 오류를 계수한다. 원본의 순서와 대조한다 |
| S4 | **crash 뒤 재기동하면 index를 gate 안에서 전수 검증한다.** 같은 run에서 eviction 뒤 다시 열 때도 같다(`internal/storage/store.go:529-573`, `counter.go:136-145`). 측정값은 CONTRACT §7에 있다(XLog 300만 건 25~29초, counter 400만 건 6.6초) | 2/4 | 예(기록) | kill -9, OOM, 종료 hang 뒤 SIGKILL로 재기동하면 일 데이터량에 비례하는 시간 동안 수집이 멈춘다 | SyncData마다 checkpoint seal을 남겨 꼬리만 검증한다. 또는 gate 밖에서 열고 검증한다 |
| S5 | **profile·gxid·span 조회가 Store gate를 끝까지 잡는다. purge는 여러 lock을 겹쳐 잡고, rename 실패 시 lock 안에서 RemoveAll을 한다**(`internal/storage/profile.go:180-189`, `retention.go:322-372`) | 3/4 | 예 | 큰 profile, 느린 디스크, 큰 삭제가 모든 writer를 세우고 S1을 거쳐 수집 전체로 번진다 | chunk마다 유한 hold로 나눈다. 물리 삭제는 gate 밖에서 한다 |
| S6 | **종료 대기에 deadline이 없다**(`engine.go:479`, `internal/storage/packwriter.go:436-456`) | 2/4 | 예 | 디스크 hang이면 SIGTERM이 끝나지 않는다. 결국 SIGKILL이 오고 S4로 이어진다 | 단계별 종료 예산을 두고, 남은 queue를 기록하고 끝낸다 |
| S7 | **visitor 객체 cache 상한 500에서 thrash가 생긴다.** 상한은 원본 `VisitorDB`(`setMax(500)`, 유형 100)와 같다. 다만 Go는 load/save(HLL 약 700KiB)를 engine lock 안에서 한다(`internal/storage/visitor.go:447-505`) | 2/4 | 예(원본 상한 일치) | UserID가 있는 XLog를 보내는 object가 500을 넘어 순환하면 XLog마다 큰 읽기와 쓰기가 생기고 수집 전체가 느려진다. 손상된 `.usr`은 XLog마다 다시 읽는다 | load/save를 별도 goroutine으로 옮긴다(포화 drop). 상한은 원본과 같게 두되 설정 가능하게 한다. 손상 파일은 격리한다 |

## 3. 자원과 경합

| # | 항목 | 일치 | 코드 확인 | 내용 | 개선 방향 |
|---|---|---|---|---|---|
| R1 | **4초 SyncData가 dirty index의 header(hash 표 전체)를 매번 복사하고 쓰고 fsync한다**(`internal/storage/legacyindex/syncpoint.go:17-41`) | 1/4 | 예 | 저부하에서도 hash 표 크기만큼(기본 index마다 1MiB) 할당과 쓰기가 4초마다 생긴다(추정). 저부하에서 주기 작업이 CPU 대부분을 쓰는 원인 후보다 | header를 dirty page(4KiB) 단위로 추적해 바뀐 부분만 쓴다 |
| R2 | **Client 조회와 Pulse queue에 전역 메모리 상한이 없다.** 조회는 요청마다 상한이 있지만 동시 100 session의 합계는 없다. Pulse queue 상한은 개수(4096×2)뿐이고 profile을 복제한다 | 2/4 | 예 | 무거운 조회가 몰리거나 Kafka 장애 중 큰 profile이 오면 OOM이 나고, 수집 공백 뒤 S4로 이어진다 | 전역 조회 byte semaphore, Pulse queue byte 상한, RSS 경보 |
| R3 | **HTTP/webapp 연결과 대기 요청에 admission 상한이 없다**(`internal/server/http.go:80-87`, `internal/webapp/server.go`). HTTP·Telegraf 수집은 engine lock을 잡는다 | 2/4 | 일부 | REST 요청이 몰리면 goroutine과 socket이 계속 늘고 engine lock 경합이 생긴다 | 연결 수, inflight, 대기 timeout을 두고 넘치면 429/503을 돌려준다 |
| R4 | **relay 응답의 전체 byte 예산이 없다**(일반 8MiB, large 256MiB, chunk 16MiB × service pool 100) | 1/4 | 예 | 큰 relay가 동시에 오면 메모리가 크게 늘 수 있다 | 동시 large relay 수와 byte permit을 둔다 |
| R5 | **Agent별 KEEP_ALIVE와 pool 전역 lock.** Agent 연결마다 5초에 한 번 왕복하고, release마다 전역 `p.mu`를 잡고 channel을 다시 만든다(`internal/server/agent_checkout.go:39-45`) | 2/4 | 예 | 규모에 비례해 초당 수백 번의 timer, context, 할당이 생긴다(측정 필요) | 규모 시험으로 측정한 뒤 필요하면 고친다 |
| R6 | **engine lock 안의 반복 비용.** PerfCounterPack마다 이름 사전(≤4096)을 map으로 다시 만든다(`internal/storage/counter.go:323-327`). Tick과 Sample5M이 같은 goroutine이다(`internal/server/runtime.go:714-735`) | 1/4 | 예 | counter pack 빈도에 비례하는 CPU와 할당, 5분마다 tick 지연 | 사전 cache, goroutine 분리 |
| R7 | **인증 없는 UDP 입력에 대한 자원 상한이 일부 경로에 없었다**(text 분류 수, object 수, 날짜별 counter 이름 수 등. `internal/storage/text.go:30-36`, `counter.go:331`) | 1/4 | 예 | 잘못 설정된 송신 하나로 정상 Agent 등록이 막히거나 디스크가 계속 늘 수 있다 | div 허용 목록 또는 일별 신규 상한과 계수, 출처 allowlist 옵션(원본과의 차이로 기록) |
| R8 | **진단 계수가 부족하다.** kernel UDP drop(`RcvbufErrors`)과 유효 `SO_RCVBUF`를 노출하지 않는다. 재조립 상한 초과를 malformed로 센다. index 전수 검증 횟수(`IndexOpenStats`)를 노출하지 않는다 | 3/4 | 예 | 손실 원인을 진단하기 어렵다 | `/health`에 노출한다 |

## 4. 운영 전환 준비 (코드 밖)

모두 B·D가 일치해서 지적했다.

- **증거 단계**: "운영 검증"까지 간 기능군이 없다. 가장 높은 단계는 실제 Agent·Client가 붙는 저부하 환경의 통합 검증이다. 명령별 증거 단계를 기록한 원장도 없다(`source-inventory.json`은 정책상 모두 `not_implemented`).
- **이관**: 운영 DB 복사본으로 이관과 rollback을 리허설한 적이 없다. 실제 Agent 환경 검증도 새 DB에서 했다.
- **절차 문서**: 전환 runbook, canary와 병행 운영 방식, Agent 원복 절차, rollback 구간의 데이터 처리(`migration.md:77`이 "전환 계획에서 다룬다"고 했지만 그 문서가 없다)가 없다.
- **sizing 문서**: 정할 값은 `--max-connections`, `ulimit -n`, `rmem_max`, UDP queue와 worker, `GOMAXPROCS`(§10), `GOMEMLIMIT`, service pool, object·counter 상한이다.
- **감시 기준**: `/health` 항목(udp Rejected, ingest Rejected, textDropped, tag dropped, maintenance errors, pulse failures)과 커널 `RcvbufErrors`의 경보 임계값이 없다. 감시 스크립트도 저장소에 없다.
- **운영 확인 목록**:
  - `.alert`·`.plug`·plugin JAR 사용 여부. Go는 실행하지 않는다.
  - `mgr_purge_enabled` 명시 여부. Go는 기본이 off다.
  - `net_tcp_client_so_timeout_ms`. 유휴 연결을 재사용하는 Client API 소비자가 있으면, 그 재사용 시간보다 짧을 때 그 소비자의 요청이 실패한다.
  - Agent 종류(host, batch, kube 모드)와 Client 버전 분포.
- **실제 Agent·Client로 아직 다루지 않은 것**:
  - GUI 관리 명령, tag·top100·text 화면, 변경 relay 21종
  - Host·Batch Agent, LDAP, Telegraf
  - `session_count` 2 이상
  - 운영 규모 부하
- **문서 불일치**:
  - `docs/compatibility/runtime.md:39`와 `transport.md:29`의 UDP 기본값이 코드(queue 8192, 수신 버퍼 4MiB)와 다르다.
  - `config-behavior.md`의 `xlog_queue_size` "연결 안 함" 서술이 낡았다.

## 5. 실제 Agent 환경 장기 감시 결과

환경의 규모·일자별 경위는 남기지 않고 결론만 적는다.

- **UDP reject**: 재기동 직후의 reject burst는 UDP queue를 8192로 올린 뒤 사라졌다.
- **조회 응답 collection 상한 오류**: 큰 `COUNTER_PAST_TIME_ALL` 응답이 요청 decoder의 collection 상한에 걸려 실패했다. Client 응답 상한을 분리해 고쳤고 재발하지 않았다([최대 처리량 기록](validation/2026-10-17-xlog-max-benchmark.md) §14.5).
- **text cache reset**: TCP 세션이 없는 object가 매회 missing으로 남았다. C3의 실제 사례다.
- **Agent 쪽**: Agent worker thread는 원본 상한 10을 넘지 않았다. 재접속 횟수는 서버 재기동과 앱 재배포 횟수를 따랐다.

## 6. 진행 순서 제안

1. **C1, C2**(설정·상한 문제, 변경 범위 작음) → **S2**(영속 저장 거절 제거) → **C3, C5**(text 재시도, 그룹 relay). 모두 호환성과 Agent 영향에 직결된다.
2. **S1 + S3**(engine lock 밖으로 저장 이동, 상태 갱신 순서). 변경이 가장 큰 구조 개선이며 S5·S7과 함께 설계한다.
3. **S4, S6**(재기동·종료 시간 단축) → **R1**(SyncData 부분 쓰기) → **R2·R3·R4**(전역 상한) → **R8**(진단 계수).
4. **C4, C6, C7**은 원본 동작 확인과 재현을 먼저 하고 결정한다.
5. §4의 운영 전환 준비는 코드 작업과 병행한다. 이관 리허설, runbook, sizing, 감시 기준, 운영 확인 목록을 운영 측과 함께 정한다.

## 7. 1단계 진행 결과 (2026-10-21)

| 항목 | 상태 | commit | 확인 |
|---|---|---|---|
| C1 TCP 연결 예산 분리 | 완료. handshake 2048, Agent 16384, Client `--max-connections` 1024. 상한에 걸리면 즉시 close하고, 사유별 10초 1줄 로그(peer, objHash, 수/상한, flag, 누적 거절 수)와 `/health` 수치를 남긴다 | 40baec8, 59779e2 | 단위·경계·race 테스트. 실제 Agent 환경: Agent·Client가 따로 집계되고 거절이 없었다 |
| (추가) Accept 일시 오류에도 TCP 서버 유지 | 완료. 원래는 EMFILE 한 번에 TCP 서버 전체가 멈췄다(C1 작업 중 발견). 지금은 backoff 뒤 계속 받는다 | 40baec8 | `TestTCPAcceptRetriesTemporaryErrors`, mutation 검사 |
| C2 object·counter 상한 | 완료. 기본 16384/262144, flag로 바꿀 수 있다. 기동 때 상한을 넘어도 실패하지 않는다. 상한에서는 Tick이 이미 inactive로 바꾼 object 중 가장 오래된 것을 메모리에서 내보낸다(Java `clearInactive`와 같은 범위) | 16bc83f | 단위·race 테스트. 실제 Agent 환경에서 축출·거절이 없었다 |
| S2 cache 예산과 영속 저장 분리 | XLog·realtime counter는 완료(16bc83f). 지연 sampling child, object·alert·status는 11단계에서 완료(0fe0cfd, §17). 당시의 `DelayedRejected`는 `DelayedRecoveredEarly`·`DelayedDiscardedEarly`·`DelayedAbandoned`로 바뀌었다 | 16bc83f, 0fe0cfd | 단위 테스트(디스크 재조회) |
| C3 text reset 재시도 | 완료(의도된 차이). Host Agent의 NoNEXT 응답을 called로 센다 | bac2451 | 실제 Agent 환경: 매회 `missing`으로 남던 Agent가 모두 called가 됐다. 빠지던 것은 Host Agent였다 |
| C5 그룹 relay | 완료. 실패한 agent 자리에 `{objHash}` placeholder를 두고 계속한다(원본과 같음) | bac2451 | 단위 테스트 |

- **독립 리뷰**: 차단·높음 0건. 중간 2건(축출 후보 범위, 지연 XLog 문서·계수)과 낮음 항목을 반영했다. 수용한 위험(handshake peer별 상한 없음, fan-out 중 멈춘 agent의 30초 감시)은 문서에 적었다.
- **검증**: `make check`, `go test -race ./...`, `make storage-check` 모두 exit 0.
- **실제 Agent 환경**(재시작 직후 구간): reject·저장 오류·drop이 없었다.
- **남은 것(다음 단계)**:
  - S2의 나머지 경로
  - S1·S3(engine lock 밖으로 저장 이동)
  - C4(30초 감시) 재현
  - 단일 대상 relay 오류 의미(원본은 빈 응답, Go는 실패)
  - 대규모 Agent 연결 통합 시험(격리 환경)

## 8. 2단계: 저장을 수집 lock 밖으로 (S1·S3, 2026-10-21)

- **변경**(66e7838, bd39bda):
  - counter·daily·object·status·alert·summary·stack·batch·span·visitor를 kind별 단일 goroutine FIFO writer가 저장한다. 원본 `*Core`→`*WR`처럼 queue 10000(span 1000)을 두고, byte 상한도 함께 둔다.
  - engine lock 안에서는 원본 순서대로 메모리를 갱신하고 enqueue만 한다. 그래서 저장이 실패하거나 queue가 차도 실시간 화면은 유지된다(S3).
  - queue가 가득 차면 기다리지 않고 버리며, 계수하고 10초에 1줄 로그를 남긴다.
  - writer는 오류가 나도 계속 돈다(원본 Stack·Batch writer는 오류 한 번에 멈춘다).
  - 종료할 때는 최대 5초 동안 queue를 비운다. 상세 계약은 [ingest CONTRACT](../internal/ingest/CONTRACT.md) §13.
- **원본 구조 조사**와 **독립 리뷰 2회**(높음 1·중간 4·낮음 7)를 반영했다.
  - 5분 sample 회차의 byte 상한 drop
  - 영구 object 삭제를 queue로 처리
  - visitor roll과 tag tick 분리
  - object 저장 실패 시 다음 heartbeat에서 재기록
  - 입력 오류(`WriteInvalid`) 분리
- **효율**(Linux VM, 기준과 번갈아 8쌍, `--text-cardinality 2000`):
  - visit를 XLog마다 queue에 넣던 첫 구현은 XLog 전용 30,000/s에서 **+4.8%**(8쌍 모두 증가)였다. 256개씩 묶은 뒤 **−3.9%**(비교 가능한 6쌍 모두 감소)가 됐다.
  - mix 15,000/s는 +2.4%(SD 6.7%)로, 8쌍 해상도(±4%) 안이다.
- **미재현 이상**: A/B 중 연속된 2회에서 부하 생성기 자체의 송신이 약 20초 밀렸고(약 2.5분 구간), 그동안 XLog를 잃었다. 같은 빌드로 12회를 더 돌렸을 때는 모두 손실 0, 송신 지연 1ms였다. 그래서 VM 호스트의 정지로 판단하지만 원인을 확정하지는 못했다. 실제 Agent 환경에서 계속 지켜본다.
- **lock 점유**(`--lock-stats`, mix 35,000/s): engine lock이 이전 측정(§8.2 구조 검토) 20.4%에서 13.0%로 줄었다.
- **실제 Agent 환경**(재시작 직후 구간):
  - writer 기록(counter, daily, visitor, object, summary)이 모두 저장됐다. 오류·drop·포기는 없었다.
  - 수집·UDP·TCP reject가 없었고, object는 모두 alive, text reset missing은 없었다.
- **남은 것**:
  - lock 안에 남은 I/O: group의 service text cache miss 읽기, 저장 queue 포화 시 최대 100ms 대기, 관리 명령 일부. ingest CONTRACT §11에 정리했다.
  - S4(crash 뒤 index 전수 검증), S5(profile 조회 gate hold·purge 중첩 lock), R1(SyncData 전체 header 복사)
  - C4 재현, 대규모 연결 통합 시험

## 9. 3단계: Agent relay 수명, crash 재기동, 조회·purge lock 분할 (C4·S4·R1·S5, 2026-10-21)

- **C4 Agent relay**(2fe98be):
  - **재현**: 수정 전에는 Agent 응답이 Client 무진전 감시(30초)보다 늦으면 그 시점에 Agent 세션이 닫혔다.
  - **고아 relay**: 이제 Client가 먼저 떠나도 relay가 "고아"로 남아 Agent 연결 수명 동안 응답을 끝까지 읽고 버린다. 세션은 유지한다(원본 `TcpAgentWorker`와 같음).
  - **상한**: 고아는 동시 32개, 예약 512MiB까지다. 넘으면 이전처럼 세션을 닫고, 계수하고, 1분에 1줄 로그를 남긴다. `/health`의 `agentOrphans`로 볼 수 있다.
  - **단일 대상 relay의 Agent I/O 오류**: 원본 `AgentCall`의 null처럼 처리한다. 응답은 빈 결과이고, 활성 목록은 `{objHash}` placeholder다. `CONFIGURE_DESC`/`CONFIGURE_VALUE_TYPE`의 Agent 대상도 포함한다.
  - **decode할 수 없는 Agent 응답**: backend 미지원(`ErrUnsupported`)이 아니라 Agent 실패로 처리한다.
  - **Agent 응답 값 개수 상한**: 독립 리뷰가 찾았다. 응답 item 하나의 collection·값 개수 상한을 요청 decoder 기준인 4096/16384에서 262,144로 올렸다. `OBJECT_ACTIVE_SERVICE_LIST`는 list 12개라서 활성 서비스가 1,366개만 넘어도 원본에 없는 세션 close가 났다.
- **S4 crash 재기동**(80bdee9):
  - **checkpoint 기록**: header fsync가 끝난 index 쌍마다 checkpoint를 남긴다. 기록 시점은 `SyncData`, `Counters.Sync`, eviction close, `Close`이고, 위치는 `.go-checkpoint`, `.go-checkpoint-counter`다.
  - **재기동 검증**: `.go-clean` 없이 재기동하면 checkpoint 뒤의 꼬리 record만 검증한다. 불일치나 손상이 있으면 쓰기 전에 전체 walk로 다시 연다.
  - **로컬 synthetic 측정**(XLog 30만 건 + 꼬리 2,000건): 첫 쓰기까지 1.3–1.9s가 27–54ms로 줄었다.
  - **checkpoint를 남기지 않는 경우**: data fsync가 실패한 쌍, purge로 풀린 handle, 잊힌 pending close.
- **R1**: SyncPoint가 dirty인 4KiB header page만 쓴다. time index는 부하와 관계없이 약 1%만 쓴다. 바쁜 hash index는 거의 전체를 쓰므로 효과가 작다.
- **S5 조회·purge**(80bdee9):
  - **수집 조회**: profile·gxid·span 수집 조회는 index walk 동안만 gate를 잡는다.
    - 비압축 data는 그 hold에서 연 fd로 gate 밖에서 읽는다.
    - 압축 data는 256 record 또는 2ms 단위로 hold를 나누고, 사이에 purge가 있었는지 확인한다.
    - 테스트에서 24MiB profile 조회 중 PutXLog 최악 대기는 9–23ms였다.
  - **purge·DB 삭제**: gate 안에서는 rename만 하고, `RemoveAll`과 handle close는 gate 밖에서 한다.
    - rename이 모두 실패하면 제자리 삭제 대신 대상을 남기고 오류를 보고한다. 원본(제자리 삭제)과 의도적으로 다르다.
- **독립 리뷰**: 두 영역을 따로 리뷰했고 P0·P1은 relay 1건이었다(위 값 개수 상한). P2는 다음을 반영했다.
  - relay: 거절 직전 완료 경합, decode 불가 응답
  - storage: SyncPoint 중첩, fsync 실패 시 checkpoint 미기록, purge 기록 밀림 시 조용한 빈 결과 → 재시도·명시 오류, 남은 대상의 data fsync 순서, evict 대기 handle 재채택, quarantine 잔여물, trash 정리 경합
- **gate**: `make check`, `go test -race ./...`, `make storage-check` 모두 exit 0(DATA RACE 0, Java interop SKIP 0).
- **효율**(Linux VM, 2단계와 번갈아 8쌍, `--text-cardinality 2000`):
  - mix 15,000/s +0.26%(SD 1.2%), XLog 전용 30,000/s −0.17%(SD 3.8%)로 변화 없음이다.
  - 이번 단계는 CPU가 아니라 정지·재접속 위험을 줄이는 변경이다.
- **실제 Agent 환경**(재시작 직후): 수집·UDP·TCP reject, writer·저장 오류, text reset missing, 고아 relay가 모두 없었고 Agent 연결이 유지됐다.
- **남은 것**:
  - lock 안에 남은 I/O: group text read, 저장 queue 포화 시 최대 100ms 대기
  - index hash chain walk의 분할: legacyindex에 재개 API가 필요하다.
  - 압축 block 해제는 아직 gate 안이다.
  - 전역 메모리 상한: Client 조회, Pulse, HTTP admission, relay byte(R2–R4)
  - 대규모 연결 통합 시험, 실제 Agent 환경의 crash(kill -9) 검증
  - 운영 전환 문서: runbook, 이관 리허설, sizing, monitoring

## 10. 4단계: 실제 crash·대규모 연결 검증과 수집 lock 정리 (2026-10-22~23)

- **실제 Agent 환경의 kill -9 재기동**(kill 직후 기동): 두 경우 모두 서버는 즉시 기동했다.
  - crash checkpoint가 있으면 꼬리 record만 하나씩 검증한다. 수집 정지는 1초 미만이었고 UDP reject가 없었다.
  - checkpoint가 없으면(3단계 전과 같음) 그날 record 전체를 검증한다. 그동안 수집이 멈춰 UDP reject가 났다. 정지 시간은 그날 쌓인 record 수에 비례한다.

- **검증 중 찾아 고친 결함**:
  - **crash 직후 재기동 거부**(2d60689): kill -9 직후 재기동이 `lock.dat` 5초 규칙에 걸려 실패했다. 이제 Go writer는 `lock.dat`에 표시를 남기고, `.go-writer.lock`을 잡은 새 writer는 그 표시가 있는 fresh `lock.dat`를 바로 넘겨받는다. Java는 `lock.dat`를 빈 파일로 만들고 mtime만 보므로, 살아 있는 Java 서버는 여전히 거부된다.
  - **`/health` 지연**(0f22f62): `/health`의 `indexOpens` 계수가 Store gate를 잡아, 긴 index 검증 동안 응답이 25초 늦었다. 계수를 leaf lock으로 옮겼다.
- **대규모 연결 시험**([결과](validation/2026-10-22-connection-scale.md), Linux VM, `scouter-replay agents`):
  - **정상 동작**: Agent 2000·4000·8000개에서 reject 0, 세션 끊김 0이었다. idle 세션 메모리는 연결당 약 33KiB다. 재시작 뒤 전원 재접속까지 약 5초가 걸렸고, 이 시간은 Agent의 재접속 대기가 정한다.
  - **상한 동작**: handshake·Agent 예산 초과, accept EMFILE backoff, 고아 relay 상한(32개)이 모두 설계대로 동작했다. 기존 세션·UDP 수집·Client 연결은 영향을 받지 않았다.
  - **fd 한도 결함**(3c855ed): open file 한도가 예산보다 낮은 호스트에서는 Agent socket이 fd를 다 써서 저장 파일 open이 실패했다. 이제 기동 시 DB 예비분 `max(1024, 한도/4)`를 남기도록 TCP 예산을 낮추고 WARNING을 남긴다. 다시 실행한 시험에서 저장 실패는 0이었다. 일반 서버 한도에서는 기본값이 그대로 유지된다.
- **수집 lock에 남은 대기·I/O 제거**(dbc885f):
  - storage queue가 가득 찼을 때의 최대 100ms 대기와 cache에 없는 service text 파일 읽기를 engine lock 밖으로 옮겼다. 그래서 한 worker의 대기가 다른 수집을 멈추지 않는다. 재현 테스트에서 이전에는 다른 worker가 80–101ms 또는 1초 이상 멈췄다.
  - **독립 리뷰 2회**(P1 3건, P2 다수)를 반영했다.
    - lock 밖으로 미룬 text를 다른 worker가 보게 해서, group 값이 0으로 저장되는 것을 막았다.
    - hold당 상한을 넘으면 lock 안에서 이전처럼 넣어, 대량 해제 때 손실이 이전보다 늘지 않게 했다. 테스트에서 이 손실은 212건에서 0건이 됐다.
    - Flush·Close가 lock 밖 쓰기를 기다리게 했다.
    - purge 뒤에 경로를 다시 만들지 않게 했다.
  - **계수**: `StoreWritesDeferred`, `StoreWritesHeldInline`, `StoreWritesHeldDropped`(panic 전용), `ServiceTextSkipped`. 마지막 계수는 이후 이름과 의미가 바뀐 현재 것이다. 지금은 engine lock 안에서 service text 파일을 읽지 않고, 답을 제때 얻지 못한 XLog를 group 없이 저장한 수를 센다(`TestEvictedServiceTextNotReadUnderLock`, [ingest CONTRACT](../internal/ingest/CONTRACT.md) §11).
  - **효율**(VM, 3단계와 번갈아 측정, `--text-cardinality 2000`): mix 15,000/s는 −1.24%(10쌍 중 7쌍 감소), XLog 전용 30,000/s는 +0.86%(SD 2.1%)로 측정 해상도 안이다. 같은 머신에서 다른 테스트와 겹쳐 돌린 첫 측정(+4.4%, 이상치 +35%)은 버렸다.
- **기타**:
  - 오늘 날짜에서만 실패하던 permanent 삭제 테스트를 고쳤다(014e413).
- **gate**:
  - `make check`, `make storage-check`: exit 0
  - `go test -race ./...`: Mac load 40–50에서 service의 Java 연동 테스트 1건이 Client i/o timeout으로 실패했다. 부하가 내려간 뒤 재실행에서 통과했고, DATA RACE는 0건이다.
- **남은 것**:
  - index hash chain walk를 나눠 실행하기(legacyindex에 재개 API 필요)
  - 압축 block 해제를 gate 밖으로 옮기기
  - R2–R4 전역 메모리 상한(Client 조회, Pulse, HTTP admission, relay byte)
  - text cache reset이 Agent별로 순차 진행되는 문제(느린 Agent마다 5초)
  - Client 진전 상한 로그의 rate limit
  - 운영 전환 문서(runbook, 이관 리허설, sizing, monitoring)

## 11. 5단계: 전역 자원 상한과 운영 전환 문서 (2026-10-23)

- **Client 응답 전역 예산**(R2, acab3c6):
  - **범위**: 64KiB 이상인 응답 item에만 적용한다. 예산은 `GOMEMLIMIT`/4(최소 257MiB)이고, `GOMEMLIMIT`이 없으면 1GiB다.
  - **넘치면**: Client 대기 상한까지 기다린 뒤 그 명령만 실패한다. Java Client는 그때까지 받은 결과만 보이고, 다음 명령에서 다시 연결한다.
  - **Blob 예외**: Agent에서 중계하는 blob chunk는 relay 예산으로 세므로 이 예산에서 빠진다. 리뷰에서 heap dump 중계가 대기하다 Agent 세션을 닫는 문제를 찾아 고쳤다.
- **Pulse queue byte 상한**(R2, cbf4193):
  - **상한**: XLog 16MiB, Profile 64MiB다. 개수 상한 4096은 그대로다.
  - **넘치면**: 기다리거나 복사하지 않고 drop하고, `DroppedByBytes`로 센다.
  - **worker panic**: task 단위로 격리했다. 이전에는 panic 한 번에 서버 전체가 종료됐다. 이 결함은 리뷰에서 찾았다.
- **HTTP admission**(R3, e560c71):
  - **연결**: listener마다 1024개다. 상한 근처에서는 keep-alive 연결을 회전시킨다.
  - **요청**: servlet과 webapp은 각각 동시 64개, 대기 3초이고, 넘으면 503이다.
  - **감시**: `/health`는 별도 예산이라 요청이 몰려도 응답한다.
  - **Java와의 차이**: Java(Jetty)는 요청을 거절하지 않고 무한히 대기시킨다(의도된 차이).
- **Agent relay 전역 예산**(R4, e560c71):
  - **예산**: `GOMEMLIMIT`/4 또는 1GiB, 하한 768MiB다.
  - **잡는 시점**: Agent에 명령을 보내기 전에 잡는다.
  - **Client 명령의 대기**: Java `net_tcp_get_agent_connection_wait_ms`(기본 1초)만 기다리고, 넘치면 Java null 결과처럼 응답한다. Agent 세션은 유지된다. 처음에는 10초 대기로 구현했는데, Client read timeout(8초)보다 길어 떠난 Client의 명령이 늦게 전송될 수 있다는 리뷰 지적으로 바꿨다.
- **text cache reset**:
  - 16개를 병렬로 호출하고, 호출마다 두던 5초 제한을 없앴다. 그래서 고아 relay를 만들지 않는다.
  - 예산이 차서 거절되면 재시도하되 시도 횟수에 넣지 않는다.
- **진단**(R8):
  - UDP 재조립 상한 초과를 malformed와 분리해 센다.
  - 요청한 SO_RCVBUF와 실제 적용된 크기를 보여 준다.
  - Linux에서는 socket drop과 host 전체 `RcvbufErrors`를 읽는다.
  - 연결 오류는 분류별로 10초에 1줄 로그와 분류별 계수를 남긴다.
  - `/health`에 Pulse producer 통계를 추가했고, stats 줄에 `assembly`·`sockdrop`를 추가했다.
- **독립 리뷰**: 세 영역을 따로 리뷰했고 P0는 없었다.
  - **P1 4건**: blob relay와 Agent 세션, 대기 시간과 Client timeout, keep-alive 점유, Client 응답 문서
  - **P2**: 다수 반영했다.
- **운영 문서**(d3eba37):
  - [runbook](operations/cutover-runbook.md): Agent 종류별 주소 전환 동작을 원본 소스로 확인해 적었다. Java Agent V2는 3초 conf reload 뒤 다음 KEEP_ALIVE에서 전환되고, Host Agent의 V1 TCP는 기존 연결이 끊길 때까지 남는다.
  - [sizing](operations/sizing.md), [monitoring](operations/monitoring.md)
- **gate**:
  - `make check`: 처음에는 replay의 stats 파서가 새 bool·문자열 필드 때문에 실패했다. 고친 뒤 exit 0이다.
  - `go test -race ./...`: 같은 파서 테스트 1건 외에 통과했다.
  - `make storage-check`, `make pulse-fixtures-check`: exit 0
- **효율**(VM, 4단계와 번갈아 측정): mix 15,000/s +0.23%(10쌍), XLog 전용 30,000/s +0.04%(8쌍)로 변화 없다.
- **남은 것**:
  - 증분 이관(방식 B의 손실 구간 단축)
  - index hash chain walk 분할, 압축 block 해제를 gate 밖으로
  - 인증 없는 UDP 입력의 자원 상한(R7)
  - HTTP 상한·서버 발신 대기의 설정 노출
  - 운영 DB 복사본으로 이관 리허설

## 12. 6단계: 증분 이관, 조회 gate 분할, R7, 기존 DB 호환, 상한 설정 (2026-10-23)

- **증분 이관**(`scouter-db migrate --precopy` / `--finish`):
  - **사전 복사**: Java가 도는 동안 모든 파일을 미리 복사한다.
  - **마무리 복사**: Java가 멈춘 뒤에는 상태(크기·ns mtime·ctime·inode·mode)가 바뀐 파일만 복사·검사한다.
  - **효과**(synthetic 4.27GB): 손실 구간이 전체 이관 98초에서 `--finish` 10초로 줄었다. 결과 manifest와 dst 바이트는 전체 이관과 같다.
  - **리뷰 반영**: 같은 dst의 동시 실행을 flock으로 막는다. Java 정지 판정은 `lock.dat`이 없거나 60초 이상 갱신되지 않을 때, 또는 `--java-stopped`를 줄 때다. Java는 종료하면서 flush하기 때문이다. racy 창은 60초로 늘렸고 `--rehash-staging`을 추가했다.
  - **runbook 방식 B 갱신**: [runbook](operations/cutover-runbook.md)
- **조회 gate 분할**:
  - **index walk**: profile·gxid·span index를 snapshot으로 떠서 gate 밖에서 걷는다. 이 세 index는 append-only로 보호한다.
  - **압축 block 해제**: gate 밖에서 한다. 동시 해제는 2개로 제한하고, gate 안에서는 늘어난 꼬리만 붙인다(256KiB·1MiB 상한).
  - **효과**(로컬): 긴 chain 조회 중 PutXLog 최악 대기가 141–155ms에서 1ms 수준으로, cold 30MiB block 해제 중에는 31ms에서 0.4–4ms로 줄었다.
  - **리뷰 반영**: 쓰기 중인 block을 record마다 다시 해제하던 회귀(2,000건 scan 기준 해제 234회 → 1회)와 slot 기아를 고쳤다.
- **R7 인증 없는 UDP 입력의 자원 상한**:
  - **text 분류**: 원본에 없는 분류로 새 영구 index를 만들 수 있는 수를 64개로 묶었다(`go_text_max_unknown_divs`). 원본 17개 분류는 무제한 그대로다.
  - **object 상한 고착**: batch나 deadTime이 1시간을 넘는 object로 상한이 막히면, 그중 오래 조용한 것을 실시간 목록에서만 뺀다.
- **기존 Java DB 호환**(counter 이름 사전):
  - **결함**: 날짜의 realtime counter 이름이 4096개를 넘는 Java DB를 Go가 읽지 못해, 그날 조회와 쓰기가 모두 실패했다. 수정 전에 재현했다.
  - **읽기**: Java 사전을 이름 100만 개, 64MiB까지 읽는다. Java가 비정상 종료로 남길 수 있는 손상 `.head`도 Java처럼 읽는다.
  - **쓰기**: 상한(사전 전체 65,536개)에 닿아도 새 이름만 빼고 기존 이름의 값은 저장한다.
  - **비용**: pack마다 이름 사전을 다시 만들지 않는다(이름 4000개 기준 135µs → 7µs). Java↔Go 양방향 oracle을 추가했다.
- **Go 전용 상한 키 14개**:
  - `go_http_*`, `go_agent_relay_*`, `go_client_response_budget_mb`, `go_pulse_*_queue_mb`, `go_text_cache_reset_parallel`, `go_text_max_unknown_divs`
  - 잘못된 값은 경고 후 기본값을 쓴다. 범위 근거는 [config CONTRACT](../internal/config/CONTRACT.md) §6에 있다.
- **독립 리뷰**: 영역마다 1~2회 받았고 P0는 없었다.
  - **P1**: 이관 2, 조회 gate 2(+재리뷰 1), R7 1, counter 1
  - **P2**: 대부분 반영했다.
- **gate**:
  - `make check`: `TestHotBlockTailUnderGateBounded`의 slot 보유 goroutine이 테스트가 끝난 뒤에도 `t.Error`를 불러 실패했다. goroutine을 기다리도록 고친 뒤 통과했다.
  - `go test -race ./...`, `make storage-check`, `make pulse-fixtures-check`: exit 0
- **효율**(Linux VM, 5단계와 번갈아 측정): mix 15,000/s −0.35%(10쌍), XLog 전용 30,000/s −0.03%(8쌍)로 변화 없다.
- **실제 Agent 환경**: 재기동 뒤 reject·writer drop·오류가 없고, 새 계수(TextDivsLimited, ObjectsEvictedSilent, counterNamesLimited)도 0이다.
- **다음 라운드 후보**([profile scout](validation/2026-10-23-profile-scout.md)):
  1. **object 500개 초과 시 visitor cache thrash**: CPU 42~55%를 쓰고 visitor 기록의 70%를 drop한다.
  2. **처리된 gxid 집합의 pointer 구조**: 상한 A/B에서 CPU −6~10%, RSS 약 −100MiB였다.
  3. **고rate UDP queue 일시 포화**: 원인 미확정
  4. **engine lock 안 counter cache 갱신**

## 13. 7단계: visitor cache, GC 대상 구조, counter cache (2026-10-24)

- **visitor cache thrash 해소**(53fa084):
  - **문제**: object가 500개를 넘으면 visitor cache가 계속 miss해 서버 CPU의 절반 가까이를 쓰고 visitor 기록의 약 70%를 버렸다.
  - **해법**: 새 entry는 파일을 읽지 않고 방문의 register만 메모리에 둔다. flush가 바뀐 4KiB page만 합쳐 쓴다. resident는 256MiB 예산 안에서만 두고, 넘으면 무손실로 강등한다.
  - **결과 bytes**: 모든 방문을 HLL 하나에 넣은 결과와 같다. Java oracle로 양방향을 확인했다.
  - **Java와의 차이**: Java는 500개를 넘으면 방문을 잃지만 Go는 보존한다(의도된 차이).
  - **리뷰**: 2회 받았고 P0는 없었다. 처음 구현에서는 resident 예산이 실효가 없었는데(P1) 이를 해소했다.
- **처리된 gxid 집합을 pointer 없는 구조로**(403fa2f): `container/list` 대신 map과 삽입 순서 ring을 쓴다. 의미는 LongIntLinkedMap LAST mode와 같고 oracle로 확인했다. live heap object가 약 100만 개에서 약 2천 개로 줄었다.
- **counter cache**: map 5개를 2개로 줄였고, 계상 크기는 저장하지 않고 계산한다. encoder는 재사용하되 인코딩 결과는 항상 복사해 넘긴다. pack 하나를 lock 안에서 처리하는 비용이 약 4.5µs에서 1.5µs로 줄었다.
- **효율**(Linux VM, 6단계와 번갈아 측정):

  | 시나리오 | CPU(쌍 중앙값) | 감소한 쌍 | RSS 중앙값 |
  |---|---|---|---|
  | mix 15,000/s | **−8.45%** | 10/10 | 235 → 182MiB |
  | XLog 전용 30,000/s | **−6.60%** | 8/8 | 270 → 212MiB |
  | visitor 규모(object 4000, 5,000/s, 3회씩) | CPU/1k 0.28 → 0.094(**−66%**) | — | visitor drop 약 9.7만 → 0 |

- **UDP queue 일시 포화**: 서버와 부하 생성기 CPU를 분리한 새 빌드에서 40회 중 3회 reject가 났다. 조건별로 기본 1/10(61건), tag 끔 0/10, hourly visitor 끔 1/10(2,584건), sync 간격 연장 1/10(18건)이다. 이전(60k, 2/5회, 4천~9천 건)보다 빈도와 크기가 크게 줄었고 조건 간 차이는 구분되지 않는다.
  - 3회 모두 visitor densify가 없었다.
  - 2회는 2초 trace의 시작·정지 경계에서 났다. trace 수집이 유발했을 가능성이 크다.
  - 1회는 UDP worker가 선점된 구간에 GC mark termination STW 15.7ms가 겹쳤다.
  - 이전 실패의 주원인은 같은 VM 안 생성기와의 CPU 경합에 densify·tag drain·GC가 겹친 것으로 판단한다.
  - 남은 확인: trace 없이 20회 이상 반복한 빈도, GC STW가 길어지는 조건, byte 기준 UDP queue 상한의 효과([profile scout](validation/2026-10-23-profile-scout.md) §11)
- **gate**: 시간에 민감한 테스트 2건이 부하에서 flaky해 고쳤다. e2e 테스트는 끼어든 KEEP_ALIVE에 응답하고, HotBlock 테스트는 race build에서 hold 판정을 완화한다. 그 뒤 race와 `storage-check`가 통과했다.
- **실제 Agent 환경**: reject·writer drop·오류가 없었다. 같은 실제 Agent 부하에서 수집 지연(p50·p99)도 줄었다.
- **다음 후보**:
  - 실제 Java Agent 상호운용 시험(진행 중)
  - tag 반영·flush 구조: 정상 규모 CPU의 20~25%
  - txid 단건 조회 chain walk의 gate
  - 운영 DB 복사본으로 이관 리허설(승인 필요)

## 14. 8단계: 실제 Java Agent 상호운용, tag 반영 경로 (2026-10-24)

- **실제 Java Agent 상호운용**(ffbd110, [결과](validation/2026-10-24-java-agent-interop.md)):
  - **방법**: prebuilt `scouter.agent.jar`와 `scouter.host.jar`(2.20.0)를 썼다. 네트워크 계약 코드는 기준 commit과 같고, harness가 실행마다 이를 확인한다. 샘플 JVM, 원본 TcpProxy Client, 기록 proxy를 Mac 루프백에서 Go 서버에 붙였다.
  - **결과**: JDK 8과 11로 최종 2회 돌렸고 둘 다 45/45 PASS다. 서버 결함은 없었다.
  - **확인한 항목**: 수집, relay 11종(9MiB 덤프 SHA 일치 포함), 고아 relay(Client 이탈, 30초 초과 응답), Agent 연결 끊김 시 Java null 응답, 응답 상한, KEEP_ALIVE, text reset, 재연결, conf로 수집 서버 주소 변경(Java Agent V2는 다음 KEEP_ALIVE 뒤 이동, Host Agent V1 TCP는 기존 서버가 멈출 때 이동)
- **tag 반영 경로**(8b2a3e4):
  - **변경**: tag마다 하던 key encode, hash, path 조회를 없앴다. pending key는 고정 크기 `{tag, 값, 종류}`이고, text 값은 Store 전체에서 단조 증가하는 id로 바꾼다. 준비 단계는 engine lock 안에서도 할당이 없다.
  - **결과 동일성**: 이전 구현의 golden digest와 같고, 일부러 넣은 결함 5종을 모두 잡는다.
  - **리뷰**: P0·P1은 없었다. text id 재사용 시 flush 순서가 어긋나는 문제와 spare 메모리 상한을 고쳤다.
- **효율**(Linux VM, 7단계와 번갈아 측정):

  | 시나리오 | CPU(쌍 중앙값) | 감소한 쌍 |
  |---|---|---|
  | mix 15,000/s | −2.51% | 10/10 |
  | XLog 전용 30,000/s | −1.06% | 7/8 |

  macOS bench(XLog당 −60%)보다 효과가 작다.
- **gate**: `make check`, `storage-check` exit 0. race에서는 변경하지 않은 counters 테스트가 부하로 timeout됐고, 재실행 10회는 통과했다.
- **실제 Agent 환경**: reject·오류·tag drop이 없었다.
- **다음 후보**:
  - tag flush의 record 위치·head cache: flush pass의 syscall을 5회에서 2~3회로 줄인다.
  - 운영 DB 복사본으로 이관 리허설(승인 필요)
  - txid 단건 조회 chain walk의 gate
  - GC STW가 길어지는 조건과 byte 기준 UDP queue 상한

## 15. 9단계: txid 단건 조회 gate, tag flush cache 폐기 (2026-10-24)

- **txid 단건 조회**(becdf41):
  - **문제**: `XLogContext`가 `xlog_tid` chain 전체를 Store gate 한 hold 안에서 걸었다. hash 충돌로 chain이 길면 그동안 수집이 멈췄다(bucket 하나에 30만 record면 hold 144–165ms).
  - **변경**: chain을 "256 record 또는 2ms 중 먼저"까지만 gate 안에서 걷는다(`Index.LookupWithin`, hold는 `lockScan`). 그보다 길면 KeyChain snapshot을 잡아 gate 밖에서 걷는다(`KeyChain.LookupContext`, 수집 조회와 같은 방식). 그래서 `xlog_tid`를 append-only로 연다.
  - **결과**:
    - 위 chain에서 동시 PutXLog의 최악 대기는 0.24–4.6ms다.
    - 짧은 chain hit은 이전과 같다(로컬 약 2.7µs, +1.5–2.5%).
    - 결과·오류·S152 경고는 이전 한 hold 구현(oracle)과 같다.
    - 새 오류 `ErrPurgeOverlap`은 압축 날짜의 긴 chain 경로에서만 생긴다(profile·gxid 조회와 같은 정책).
  - **리뷰**:
    - 1차 리뷰에서 P0·P1은 없었다.
    - 처음 구현은 모든 조회를 snapshot 경로로 보냈다. 측정해 보니 짧은 chain hit이 약 55배(2.7µs→150µs) 느려져서 gate 안 짧은 경로를 더했다.
    - 재리뷰에서 gate 안 walk의 시간 상한과 hold 관찰 누락을 고쳤다.
- **tag flush record cache**(bbe7740 → revert):
  - **변경**: flush가 갱신한 record의 위치, head, 마지막 hour block을 기억해 다시 읽지 않았다.
  - **macOS 결과**: bench에서는 창당 read 42,544→0, 82ms→52ms였다. 결과 파일은 byte 단위로 같았다.
  - **Linux VM 결과**: 캐시만 다른 두 바이너리를 XLog 전용 30,000/s로 8쌍 비교했다. CPU는 +1.06%(감소 3/8, sd 1.69%)로 이득이 없었고, RSS 최대는 216→237MiB(+21MiB, 8/8)였다. Linux는 pread가 싸서 줄인 syscall이 CPU로 이어지지 않았다. 캐시가 붙잡는 메모리(entry당 약 450B + key, GC 목표 2배)만 남는다.
  - **결정**: 자원 절감이 우선이라 revert했다. 8단계 tag 반영 경로처럼 macOS bench 효과가 Linux에서 크게 줄어드는 경우가 반복된다. 이후 저장 경로의 syscall 절감은 Linux VM A/B로 먼저 확인한다.
- **기존 테스트 결함**(63459b6): `TestTCPConnectionErrorsConcurrent`가 race 부하에서 실패했다.
  - **원인**: 로그가 허용된 호출의 `Swap(0)` 전에 다른 goroutine이 센 오류는 그 줄의 "N more"로 넘어간다. 동작은 맞지만 테스트가 남은 수만 확인했다.
  - **수정**: 이제 `more + unlogged`를 확인한다. `-race` 200회를 통과했다.
- **효율**(Linux VM, 8단계 바이너리와 번갈아 측정, cache revert 뒤 최종 바이너리):

  | 시나리오 | CPU(쌍 중앙값) | 감소한 쌍 | RSS 최대(중앙값) |
  |---|---|---|---|
  | mix 15,000/s | −1.62% | 8/10 | 185→184MiB |
  | XLog 전용 30,000/s | +0.11% | 4/8 | 216→214MiB |

  수집 경로는 바뀌지 않았으므로 변화 없음으로 본다(mix의 차이는 sd 2.66% 안이다). cache를 포함한 바이너리는 RSS가 mix 185→204MiB, XLog 212→240MiB였다.
- **gate**: `make check`, `storage-check`, `pulse-fixtures-check` exit 0. 전체 race는 위 transport 테스트만 실패했고, 수정 뒤 transport race는 통과했다.
- **실제 Agent 환경**: 1796ead를 배포하고 재기동했다. index 쌍은 모두 정상 종료 표시로 열렸다(Walked 0). Agent는 바로 다시 붙었다. reject·저장 오류·drop·socket drop은 없었다.
- **다음 후보**:
  - GC STW가 길어지는 조건과 byte 기준 UDP queue 상한
  - 운영 DB 복사본으로 이관 리허설(승인 필요)
  - `loadZipWriter`의 gate 안 decode
  - visitor 예산의 설정 키 노출

## 16. 10단계: UDP 일시 포화 재측정, 압축 writer 재decode, visitor 예산 키 (2026-10-24)

- **UDP 일시 포화 재측정**(f2f2f01, [profile scout](validation/2026-10-23-profile-scout.md) §12):
  - **방법**: §11과 같은 CPU 분리와 XLog 전용 60,000/s로 24회 돌렸다. trace는 받지 않았고, `GODEBUG=gctrace=1`로 STW를 기록했다.
  - **결과**: reject 0/24, 저장 손실 0이다. GC STW는 2,524구간에서 최대 3.0ms(p99 0.46ms)였다.
  - **판단**: §11의 잔여 reject와 15.7ms STW는 주로 trace 수집이 만든 것이다. byte 기준 UDP queue 상한은 근거가 없어 보류한다.
- **압축 writer의 count block 재decode**(6900ca3):
  - **분석**: 처음 후보는 `loadZipWriter`의 첫 decode를 gate 밖으로 옮기는 것이었다. 이것은 재기동 뒤 날짜당 1회, 0.1~0.3초라 가치가 낮다고 판단했다. 대신 같은 지점에서 레코드마다 전체 decode가 반복되는 증폭 세 가지를 찾았다.
    - load 거부를 기억하지 않았다.
    - member 쓰기가 실패하면 writer와 cache를 버렸다.
    - 서버 기본 writer 2개에서 날짜가 3개 이상 섞이면 다시 적재했다.

    pack writer hold 하나(최대 256건)가 수십 초가 될 수 있었다. Java는 다시 적재할 때 count.dat만 읽는다.
  - **변경**: 같은 파일이고 완전한 member로 끝나는 해제 cache로 검증한다. 쓰기 실패 뒤에는 잘라낸 cache를 유지한다. `ErrCorrupt`·`ErrLimit` 거부는 읽은 handle stat을 열쇠로 기억한다. I/O 오류는 기억하지 않는다. 파일 bytes와 count.dat 의미는 그대로다.
  - **리뷰**: P0·P1은 없었다. 기억 열쇠를 경로 stat에서 handle stat으로 바꿨다(mv 복구가 막히지 않게). 기억 상한도 writer 수와 분리했다.
- **visitor 예산 키**(82e416c): `go_visitor_resident_mb`(1~16384, 기본 256)와 `go_visitor_delta_mb`(16~1024, 기본 64)를 live 키로 추가했다. object 상한은 상수로 남겼다(live로 낮추면 수집 경로에서 대량 동기 쓰기가 생기고, 메모리 효과는 약 13MiB다).
  - **설계에서 찾은 위험**: resident를 크게 줄이면 dirty entry 강등으로 delta가 상한을 넘었다. 그러면 이후 방문이 수집 경로에서 write-through된다. 이제 그런 entry는 다음 pass가 쓴 뒤 강등한다.
  - **리뷰**: setter가 설정 reload 경로에서 visitorMu를 기다리던 문제를 고쳤다. 이제 atomic 저장만 하고 visitor 쪽이 적용한다. 쓰기 실패 pass가 1초마다 반복되던 문제와 대기 중 resident 상한도 고쳤다.
- **효율**(Linux VM, 9단계 최종 바이너리와 번갈아 측정): 두 변경 모두 기본 설정의 수집 경로를 바꾸지 않는다.

  | 시나리오 | CPU(쌍 중앙값) | 감소한 쌍 | RSS 최대(중앙값) |
  |---|---|---|---|
  | mix 15,000/s | −0.25% | 6/10 | 183→184MiB |
  | XLog 전용 30,000/s | −1.64% | 6/8 | 214→214MiB |

  변화 없음으로 본다(두 차이 모두 sd 2.1~3.6% 안이다).
- **gate**: `make check`, race 전체, `storage-check`, `pulse-fixtures-check` exit 0. 리뷰 뒤 문서·주석·테스트만 바꾼 다음 `make check`와 config·server race를 다시 통과했다.
- **실제 Agent 환경**: f2f2f01을 배포하고 재기동했다.
  - index 쌍은 모두 정상 종료 표시로 열렸다.
  - reject·저장 오류·drop·socket drop은 없었다.
  - `/health` `visitors`에 기본 예산(256/64MiB)과 `WriteThroughs` 0이 보인다.
- **다음 후보**:
  - 운영 DB 복사본으로 이관 리허설(승인 필요)
  - core가 더 많은 격리 호스트에서 60k 반복(§11.3 4번)
  - 대규모 object(수천)에서 visitor 예산을 줄였을 때의 IO 측정

## 17. 11단계: 예산과 영속 저장 분리 마무리, 서버→Agent 경로 고정, 종료 상한 (2026-10-24)

후보는 §1~§3의 남은 항목을 코드 기준으로 다시 조사해 정했다.
- **보류한 후보**:
  - C7(위험 relay 권한): 원본 Java 서버도 group policy를 LOGIN 응답에만 싣고 Client가 검사한다. Go도 같다.
  - R5(KEEP_ALIVE): 측정상 비용이 작다.
  - R6(counter 사전): 대부분 이미 끝났다.

- **S2 나머지**(0fe0cfd):
  - **문제**: engine cache 예산이 차면 alert·status·지연 sampling child XLog/profile이 저장 queue에 들어가기 전에 거절됐다. Java에는 byte 예산이 없으므로 원본보다 데이터를 잃었다.
  - **alert·status**: 항상 저장한다. 예산이 모자라면 실시간 cache만 건너뛴다. 건너뛴 status key는 지워서 오래된 값이 현재 값처럼 보이지 않게 한다.
  - **지연 child**: 같은 종류의 가장 오래된 지연 group부터 Java 복구 규칙을 일찍 적용한다(XLog는 discardAll이 아니면, profile은 discardNone만 저장). 복구해도 자리가 나지 않으면 들어온 pack에 바로 적용한다.
  - **alert ring**: 지연 profile을 먼저 복구해 popup이 계속 나오게 한다.
  - **object·kube**: 자기 bytes만으로 예산을 검사한다. 그래서 다른 cache가 차도 heartbeat가 거절되지 않는다. 계상 최악치는 예산의 2배다.
  - **`/health` key 변경**: `DelayedRejected`가 없어지고 `DelayedRecoveredEarly`·`DelayedDiscardedEarly`·`DelayedAbandoned`로 나뉘었다. 외부 감시 도구가 이 key를 읽으면 고쳐야 한다.
  - **리뷰(2회, P0·P1 없음)**:
    - 계수를 저장·폐기·중단으로 나눴다.
    - 복구해도 자리가 생기지 않으면 복구하지 않는다.
    - 크기가 늘지 않는 갱신은 항상 통과시킨다.
- **C8·C6 서버→Agent 경로**(fb8c4e3):
  - **C8 fuzz 단언**: Agent는 서버가 선언한 길이를 검증 없이 할당한다. 그래서 `FuzzAgentRequestFrame`이 Java `DataInputX`를 따르는 독립 reader로 다음을 단언한다.
    - 선언 길이 = 실제 길이 ≤ 상한
    - 거절이면 wire에 0 byte, 연결 종료
  - **V1 encode-first**: V1 요청도 V2처럼 먼저 encode한 뒤 쓴다. wire byte와 받아들이는 크기는 이전과 같다.
  - **C6 실제 Agent 확인**: `OBJECT_SET_KUBE_SEQ`(kube Host Agent 이름 변경)와 `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`(Agent 자체 CPU dump 가드 경로)를 실제 Agent로 확인했다.
  - **interop**: S2 반영 빌드로 JDK 8·11 모두 54/54 통과했다([결과](validation/2026-10-24-java-agent-interop.md)).
  - **원본 관찰**: Host Agent 2.20의 ProcCpu는 0~1 비율이다. 그래서 원본과 Go 모두 기본 임계값 90에서는 CPU dump가 사실상 발동하지 않는다.
- **S6 종료 상한**(00813b6):
  - **상한**: `--shutdown-timeout`(기본 20초)을 둔다. 넘으면 멈춘 단계·남은 queue·marker 여부를 한 줄로 남기고 exit 3으로 끝낸다. 다음 기동은 crash checkpoint로 복구한다.
  - **이후 신호**: 첫 신호 1초 뒤의 SIGINT/SIGTERM은 즉시 종료한다. 1초 안의 반복과 SIGHUP은 무시한다.
  - **갇힌 writer**: 1초만 더 기다리고 남긴다. 이때는 `.go-clean` 없이 store를 닫는다.
  - **스크립트 연동**: `scouter-go.sh`가 `SCOUTER_STOP_TIMEOUT`과 연동한다.
  - **종료 시간 측정**: 로컬에서 15,000 XLog/s 부하 중 정상 종료는 1.0~1.1초였다.
  - **리뷰**: P1 2건을 고쳤다.
    - 신호 처리가 stderr 쓰기에 막히면 상한이 켜지지 않았다.
    - 다른 계기로 종료가 시작된 뒤 처음 온 stop을 즉시 종료로 처리했다.
- **replay 도구**(2f8d607): 정상 종료의 첫 신호 줄을 정보성 줄로 본다. 이 수정 전에는 11단계 서버의 모든 A/B 실행이 판정 실패였다(데이터 검증은 정상).
- **효율**(Linux VM, 10단계 바이너리와 번갈아 측정, 고친 replay 사용):

  | 시나리오 | CPU(쌍 중앙값) | 감소한 쌍 | RSS 최대(중앙값) |
  |---|---|---|---|
  | mix 15,000/s | +1.08% | 2/10 | 185→184MiB |
  | XLog 전용 30,000/s | +0.06% | 4/8 | 215→214MiB |

  XLog 전용은 변화가 없다. mix는 sd 2.7% 안이지만 10쌍 중 8쌍이 늘었다. S2의 cache 계상 변경이 원인 후보라서 다음 단계에서 확인한다.
- **gate**: 전체 gate(`make check`, race 전체, `storage-check`, `pulse-fixtures-check`) exit 0이다. S6 재리뷰 반영 뒤 `make check`와 cmd·server·ingest·storage 관련 race도 exit 0이다.
- **실제 Agent 환경**: 2f8d607을 배포하고 재기동했다.
  - index 쌍은 모두 정상 종료 표시로 열렸다.
  - reject·저장 오류·drop·socket drop은 없었다.
  - 새 계수(`DelayedRecoveredEarly` 등)는 0이다.
  - 새 종료 로그는 다음 재기동부터 나온다(이번에 멈춘 것은 이전 바이너리다).
- **다음 후보**:
  - mix +1% 원인 확인(S2 계상 경로 profile)
  - 운영 DB 복사본으로 이관 리허설(승인 필요)
  - Agent 응답 30초 초과 시 Client 연결 종료 차이(결정 필요)
  - XLog ring·status·counter cache의 자리 확보(§17 S2의 다음 결정 대상)

## 18. 12단계: Agent 응답 대기 중 Client 진전 감시 보류, A/B 잡음 폭 측정 (2026-10-24~25)

- **후보 선정**:
  - 11단계 A/B의 mix +1.08%는 같은 조건 재측정에서 +0.40%(감소 4/10)로 재현되지 않았다.
  - 실제 Agent 부하에서 받은 CPU profile에서는 counter 저장과 압축 profile 저장이 큰 몫이었다. 둘 다 본래 하는 일의 비용이다.
  - daily counter의 syscall 절감은 9단계 경험(Linux에서 효과 없음)이 있어 제외했다.
  - 그래서 결정 대기 항목 가운데 코드 영향이 큰 "Agent 응답 30초 초과 시 Client 연결 종료"를 분석했다.
- **분석**:
  - 원본은 Client 명령 처리에 총 시간 상한이 없다. Agent 응답을 끝까지 읽어 전달한다(`AgentCall.scala`).
  - Go는 Client 30초 무진전 감시가 Agent 대기에도 걸려 연결을 끊었다.
  - 원본 기본 Client `so_time_out`(8초)에서는 차이가 없다. Client가 30초 넘게 기다리는 설정(예: 60초)에서는 heap dump·heap histo·thread dump·jar download가 원본에서 성공하고 Go에서 빈 화면이 됐다(실제 Client로 재현됨).
  - 운영 원칙(호환성 우선, Agent 영향 최소, 자원 상한 유지)에 맞는 B안을 택했다.
- **변경**(54c5864):
  - `Stream.AwaitUpstream`이 Agent relay를 기다리는 동안 Client 진전 감시를 보류하고, 끝나면 명령 read timeout을 복원한다.
  - Client 이탈은 Peek probe로 감지해 `ErrClientGone`으로 명령을 끝내고 기존 고아 경로로 넘긴다. 원본에는 없는 감지이며 문서화했다.
  - Agent가 so_timeout을 넘기면 원본처럼 빈 응답을 보내고 Agent 세션을 닫는다. Client 연결은 유지한다.
  - relay deadline·예산·고아 상한은 그대로다.
  - blob stream은 chunk마다 진전이 갱신되므로 대상에서 뺐다(첫 chunk 전 30초 초과는 남은 차이).
- **리뷰**: P0·P1은 없었다. P2 6건을 고쳤다. 대기 중 panic 시 probe 정리, 중첩 호출 방지, 도달하지 않는 relay 2곳 일관화, 문서 정밀도, action log 문구다.
- **실제 상호운용**(644b607, 11e9fba):
  - harness 기대값을 새 동작으로 바꿨다. 3b(Client 60초에 40초 응답 수신), 3c(Client 8초 이탈 → 고아), 3f(Agent so_timeout 초과 → 빈 응답)다. JDK 8·11 모두 56/56이다.
  - 3f가 Agent 세션을 연속으로 막아 첫 text reset이 마지막 재시도에 전달되던 것을, 4b 재기동 뒤로 옮겨 108초·여유 2회로 되돌렸다.
  - 남은 위험: 운영에서 큰 응답 명령의 고아가 세션을 오래 잡으면 그날 reset이 포기될 수 있다(원본보다 나쁘지 않다).
- **효율**(Linux VM, 11단계 바이너리와 번갈아 측정):

  | 시나리오 | CPU(쌍 중앙값) | 감소한 쌍 |
  |---|---|---|
  | mix 15,000/s | +0.91% | 2/10 |
  | XLog 전용 30,000/s | −1.79% | 5/8 |

  - **A/A 측정**: 같은 바이너리 두 개를 mix 10쌍으로 비교했더니 **−1.36%, 같은 방향 8/10**(sd 2.18%)이었다.
  - 이 측정 방식에서는 ±1.5% 정도의 쌍 중앙값과 8/10 부호 일치가 잡음만으로도 나온다. 그래서 11·12단계의 mix ±1%는 잡음으로 판정한다.
  - 앞으로 이 A/B에서 효과로 인정하는 기준은 대략 2% 이상이고, 부호 일치가 9/10 이상이어야 한다. 더 작은 효과는 쌍 수를 늘리거나 profile로 확인한다.
- **gate**: `make check`, race 전체, `storage-check`, `pulse-fixtures-check` exit 0.
- **실제 Agent 환경**: 11e9fba를 배포했다.
  - S6 종료 로그가 처음 찍혔고, 같은 초에 정상 종료했다(writer drop 0).
  - 재기동 뒤 index 쌍은 모두 정상 종료 표시로 열렸다. reject·오류·고아는 없었다.
- **다음 후보**:
  - half-open Client 감지 시간 측정
  - blob stream 첫 chunk 30초 차이
  - 운영 DB 복사본 이관 리허설(승인 필요)

## 19. 13단계: blob stream relay의 Agent 대기 보류, keepalive 판단 (2026-10-25)

- **blob stream relay**(65a96c1):
  - **문제**: heap dump 다운로드와 dump 파일 상세는 chunk를 받는 대로 Client에 쓴다. 그래서 12단계 대기 보류에서 빠졌고, 첫 chunk 전이나 chunk 사이가 30초를 넘으면 Client 명령이 끝나고 Agent 세션이 닫혔다. 원본은 Agent read마다 so_timeout만 적용한다.
  - **변경**: blob relay 전체를 `AwaitUpstream` 안에서 실행한다. 이에 맞춰 대기 중에는 Stream을 읽지 말아야 하고 쓰기는 허용한다는 계약으로 바꿨다.
  - **남는 상한**: 대기마다 따로 상한이 걸린다.
    - Agent read: so_timeout
    - Client write: 64KiB 조각마다 write deadline
    - Client 이탈: probe가 감지하고 Agent 세션을 닫는다(원본은 다음 write가 실패할 때 닫는다)
  - **부수 효과**: 2MiB chunk마다 30초 안에 다 받지 못하던 느린 Client(약 70KB/s 미만)도 이제 끊기지 않는다.
  - **남은 위험**: 아주 느린 Client가 큰 heap dump를 받으면 Agent 세션, relay permit, service slot을 오래 잡는다. 원본도 상한이 없다. `/health`의 `agentRelayBudget`와 `tcp.ClientWaiting`으로 본다.
  - **리뷰**: P0·P1은 없었다. P2 4건은 문서와 테스트였다(읽지 않는 Client의 종료 원인, 계속 진전하는 느린 Client, 이탈 뒤 예산 반납).
- **TCP keepalive(보류)**:
  - 원본 서버는 keepalive를 켜지 않는다.
  - Go listener 기본값은 15초/15초×9로, macOS에서 getsockopt로 확인했다. half-open Client는 약 150초 안에 끝난다.
  - 기본 Client(so_time_out 8초)는 스스로 FIN을 보내므로 probe가 바로 잡는다.
  - 값을 낮추면 손실이 있는 망에서 살아 있는 Client를 끊을 위험이 있고, 얻는 것은 드문 경우뿐이라 상수를 바꾸지 않았다.
  - 운영에서 half-open 사례가 확인되면 Client 연결에만 짧은 keepalive를 거는 방안을 다시 본다.
  - Linux 실측과 half-open 감지 시간은 재지 않았다.
- **gate**: `make check`, race 전체, `storage-check`, `pulse-fixtures-check` exit 0이다. 실제 Java Agent interop은 JDK 8과 JDK 11 모두 56/56이다(7MiB relay, 9MiB 거절 포함).
- **Linux keepalive 실측**(Linux VM, Ubuntu 24.04):
  - OS 기본값은 `tcp_keepalive_time` 7200초, `intvl` 75초, `probes` 9다.
  - Go 서버가 accept한 연결은 연결 1초 뒤 `ss -tnoi`에 `timer:(keepalive,13sec,0)`으로 나온다. 즉 Go가 idle을 15초로 덮어쓴다(macOS와 같음).
  - half-open 감지 시간 자체는 재지 않았다. 인증 전 연결은 handshake timeout으로 먼저 닫혀서, Agent relay 대기 중에 iptables DROP을 거는 절차가 필요하다([agent-side-effects](compatibility/agent-side-effects.md) §7(d)).
- **효율**: 수집 경로가 바뀌지 않아 VM A/B는 생략했다. §18의 A/A 기준으로 2% 미만 차이는 구분되지 않는다.
- **실제 Agent 환경**: 65a96c1을 배포했다. index는 정상 종료 표시로 열렸고, reject·오류·고아는 없었다.

## 20. 14단계: 9~13단계 교차 통합 리뷰와 후속 수정 (2026-10-25)

- **통합 리뷰**: 각 단계는 따로 리뷰했지만, 서로 겹치는 지점은 함께 본 적이 없었다. 이번에 교차 지점을 정적으로 다시 봤다.
  - 대상 조합:
    - 종료와 relay 대기·blob
    - 종료와 조기 recover·zip reload·visitor 예산
    - purge와 txid·zip 실패 기억
    - relay 예산·service slot
    - 계수
  - 결과: 교착, 무한 대기, 종료 이후 쓰기, 계수 이중화는 없었다. P2 6건이 나왔다.
- **기존 data race**(99a18c1):
  - 원인: HTTP 종료는 2초 뒤 handler를 기다리지 않는다. 그래서 늦게 끝난 `/health`가 `Close`가 nil로 바꾸는 `r.http`·`r.pulse`를 lock 없이 읽었다. 수정 전 `-race`로 재현했다.
  - 수정: `Close`는 `r.http`를 nil로 바꾸지 않는다. Pulse는 atomic 사본으로 읽고, servlet 포인터는 mutex 아래에서 읽는다.
- **후속 수정**(99a18c1, 9dd5f4c):
  - exit 3 종료 줄에 활성 Client 명령 수·고아 relay 수·relay 예산 byte를 넣었다. 최종 stats 줄에는 고아·relay 예산·visitor 계수를 넣었다. 최종 stats 줄 앞에는 "final stats" 단계를 둔다.
  - 고아 relay는 slot·permit을 반납하기 전에 결과 로그를 쓴다. 그래서 store를 닫은 뒤에 로그가 찍히지 않는다. 대신 로그 쓰기가 막히면 반납이 늦어지며, 이 trade-off를 문서에 적었다.
  - Client relay는 relay 예산 대기와 Agent 세션 대기를 합쳐 `net_tcp_get_agent_connection_wait_ms` 한 번만 기다린다. Java `RequestQueue.get`과 같다. 이전에는 12단계 이후 두 대기가 각각 이 값만큼 걸렸다.
  - `ReloadTelegraf`는 XML 파일 읽기를 servlet lock 밖에서 한다. 동시 reload는 reload lock으로 순서를 지킨다.
- **리뷰**: 후속 수정을 다시 리뷰했고 P0·P1은 없었다. lock 순서, Java 대기 의미, stats 줄 파서 호환을 확인했다. P2(테스트·주석·Telegraf lock·단계 표시)는 반영했다.
- **gate**: `make check`, race 전체, `storage-check`, `pulse-fixtures-check` exit 0. 후속 수정 뒤에도 `make check`와 server·cmd·transport·service race exit 0.
- **실제 Agent 환경**: 9dd5f4c를 배포했다.
  - 이전 바이너리(65a96c1)가 첫 신호 줄과 함께 정상 종료했다(writer drop 0).
  - 재기동 뒤 index 쌍은 정상 종료 표시로 열렸다. reject·오류는 없었다.

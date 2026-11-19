# 실제 Java Agent 상호운용 검증 (2026-10-24)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

지금까지 Agent 쪽 동작은 원본 소스를 흉내 낸 가짜 Agent(`scouter-replay`, 테스트 fake)로만 확인했다. 이번에는 실제 `scouter.agent.java`(`-javaagent`)와 실제 Host Agent를 Go 서버에 붙였다. 조회는 원본 Client 통신 클래스(`TcpProxy`/`LoginMgr`)로 했다. harness: `scripts/java-agent-interop.py`, `tools/java-agent-interop/`(사용법은 그 README).

## 결론

- **최종 실행 2회 모두 45/45 PASS다.** 샘플 JVM은 JDK 8 1회, JDK 11 1회다.
- **11단계(commit `cc65050`)에서 서버 발신 명령 시나리오 8개를 더해 다시 실행했고 2회 모두 53/53 PASS다**(§2.8). 이 문서의 `11단계`·`S2`·`C6`·`C8` 같은 이름표는 [개선 대상](../improvement-candidates-2026-10-21.md)의 항목 번호다. 기존 45개도 그대로 통과했다. 후속 보완으로 시나리오 7에 ADC tag 반영 확인 1개를 더하고 dump 판정을 강화했다. 이 판(54개)으로 S2 반영 빌드(commit `0fe0cfd`, `git archive`)에서 전체를 다시 실행했고, JDK 8(1.8.0_345)과 JDK 11(11.0.27) 모두 54/54 PASS였다(293.7초/290.8초, 잔여 프로세스 0). 그 실행의 시나리오 6 Host V1 relay 4종은 모두 정상이었고, 이로써 V1 encode-first 쓰기 경로를 실제 Agent로 확인했다. 시나리오 7의 kube seq push는 2/1회였고 `Failed`·`Dropped`는 0이었다. CPU dump trigger는 1회(14.3/15.3초)였고 dump 파일은 2개(간격 2.0초)였다.
  - `OBJECT_SET_KUBE_SEQ`: kube Host Agent가 Go가 준 seq 0으로 `/interop-kube-pod_0`이 된다.
  - `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`: Go가 원본 reason(`TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED`)으로 보내고, Agent는 원본 CPU 초과 가드대로 dump한다.
- **12단계(commit `54c5864`, Agent 대기 중 Client 진전 감시 보류)를 실제 원본 Client·Java Agent로 확인했다.** 시나리오 3을 새 동작에 맞게 고쳐 check가 56개가 됐고, `54c5864` 빌드(`git archive`)로 JDK 8·11 모두 56/56 PASS였다(379.7초/374.8초, 잔여 프로세스 0). 그 뒤 3f(Agent so-timeout 초과)를 4b 재기동 뒤로 옮긴 harness로 HEAD `644b607`(비테스트 코드는 `54c5864`와 같음)을 빌드해 다시 실행했고, JDK 8·11 모두 56/56 PASS였다(395.6초/385.7초). §2.3에 결과를 적었다.
  - Client so_timeout 60초 + Agent 40초 응답: Client가 40.0초에 응답을 받는다. 고아는 생기지 않고 Agent 세션과 Client 연결은 유지된다. 수정 전 빌드는 30.0초에 응답 없이 끊겼다.
  - Client so_timeout 8초(기본) + Agent 40초 응답: Client는 8.0초에 떠난다. 서버는 이탈을 0.2초 안에 감지해 고아로 넘기고, 고아는 40.0초에 응답을 끝까지 읽은 뒤 세션을 유지한다.
  - Agent 응답이 Go Agent so_timeout(15초로 설정)보다 늦으면 원본처럼 Client에 빈 응답(`NoNEXT`)을 보낸다. Client 연결은 유지하고 Agent 세션은 닫는다. 고아는 생기지 않는다.
- 서버→Agent frame 길이 불변식은 Go fuzz·경계 테스트로 고정했다(§8). 임시 mutation 5개 중 새 테스트는 4개(M1·M2·M4·M5), 기존 테스트는 2개(M1·M4)를 잡았다. M3은 wire 결과가 같은 동등 mutant다. V1 명령 쓰기는 V2처럼 먼저 encode하도록 고쳤다. 그래서 거절된 명령은 V1·V2 모두 wire에 0 byte를 남긴다.
- 실제 Agent로 처음 확인한 것은 다음과 같다.
  - 수집: XLog·profile·text·counter·heartbeat.
  - relay: 서버→Agent 11종, Host 4종.
  - 고아 relay 뒤에도 세션이 유지된다.
  - 단일 대상 relay의 Agent 실패는 Java null로 처리된다.
  - 응답 상한 8MiB를 넘으면 거절한다.
  - KEEP_ALIVE 주기, 재기동 뒤 text reset과 Agent의 text 재전송.
  - conf의 collector 주소를 바꾸면 세션이 옮겨 간다.
  - 서버를 재기동하면 Agent가 다시 연결한다.
- **서버 결함은 발견하지 못했다.** 실패로 끝난 개발 중 실행 4건은 모두 harness 결함이었다(§5).
- **runbook §4의 Agent 전환 동작을 실제 Agent로 확인했다.**
  - Java Agent: conf를 바꾸면 다음 KEEP_ALIVE 뒤 V2 세션이 새 주소로 옮겨 간다.
  - Host Agent: UDP만 바로 옮겨 간다. V1 TCP는 기존 서버가 멈출 때까지 남는다.
- 남은 위험(§4): 하나뿐인 Agent 세션을 느린 relay나 고아 relay가 쥐고 있으면 그동안 text reset이 미뤄진다. 11단계 실행에서는 재시도로 66초에 전달됐다. 12단계 첫 실행에서는 40초 relay 세 개가 이어져 190초(마지막 재시도)에 전달됐다. 3f를 4b 재기동 뒤로 옮긴 뒤에는 108초에 전달됐다(재시도 여유 2회).

## 1. 조건

- 호스트: macOS(arm64) 개발 장비, 127.0.0.1 loopback. 포트는 임시로 고른 빈 포트를 썼다(Scouter 기본 포트는 쓰지 않음). DB는 실행마다 새 임시 directory다.
- 기능 확인용이며 부하 측정이 아니다.
  - Go: `GOMAXPROCS=2`.
  - 각 JVM: `-XX:ActiveProcessorCount=2`, 샘플 `-Xmx128m`, Host `-Xmx64m`.
  - 샘플 앱: worker 3개가 200ms 간격으로 거래를 만든다(약 11 XLog/s).
- Go 서버: commit `6c08cb6`을 `git archive`로 풀어 깨끗한 tree에서 빌드했다. 작업 tree의 미커밋 변경은 들어가지 않았다. Go 설정은 모두 기본값이다(`RequestTimeout` 30초, KEEP_ALIVE 5초, `net_tcp_get_agent_connection_wait_ms` 1초).
- Client: 원본 common + Client 통신 source 224개를 JDK 18 `--release 11`로 컴파일했다. 원본은 baseline commit `dee47fb5`이며, Git blob을 대조하는 guard는 `scripts/java-integration.py`의 것을 재사용했다.
- 샘플 JVM: JDK 8(Zulu 8.64), JDK 11(Zulu 11.80, `-Djdk.attach.allowAttachSelf=true`).

### 1.1 Agent jar: prebuilt 2.20.0을 쓴 근거

- 이 장비에는 maven이 없다. Java Agent는 shade(asm·javassist relocation)와 xtra jar 조립이 필요해 javac만으로는 배포 jar와 같은 산출물을 만들 수 없다. 그래서 원본 소스 빌드 대신 로컬 참고 배포본의 공식 jar를 썼다.
  - `scouter.agent.jar`: 2.20.0, SHA-256 `96cc13e3…cfe88`, 2,401,020 B.
  - `scouter.host.jar`: 내부 `scouter-agent-host-2.20.0.jar`, SHA-256 `e8f92da0…61cdf`.
- 근거: `git diff --stat v2.20.0 dee47fb5`를 아래 경로에 대해 실행하면 **빈 결과**다.
  - Java Agent 통신·수집 경로: `scouter.agent.java/.../agent/netio`, `agent/counter`, `Configure.java`, `JavaAgent.java`, `AgentBoot.java`.
  - 공통 경로: `scouter.common/.../scouter/net`, `scouter/io`, `scouter/lang`.
  - Host Agent: `scouter.agent.host/src`.
- harness는 실행마다 이 diff를 다시 확인하고, 비어 있지 않으면 실패한다.
- 2.20.0 → 2.21.3 사이 Agent 변경은 계측 대상 라이브러리(ElasticSearch, Jedis, HttpClient5, reactive)와 `StrMatch`뿐이다. 이번 경로와는 관계없다.
- 가정: 배포 jar는 v2.20.0 tag의 공식 release 산출물이다(jar의 `pom.properties` version 2.20.0). jar의 class를 tag 소스 빌드와 byte 단위로 대조하지는 않았다.

### 1.2 구성

```
샘플 JVM (-javaagent) --UDP--> Go A
                      --TCP V2--> 기록 proxy --> Go A     (서버→Agent 명령, Agent 응답 HasNEXT/NoNEXT를 시각과 함께 기록)
원본 TcpProxy Client --TCP--> Go A / Go B
Host Agent (scouter.boot.Boot) --UDP/TCP V1--> Go B, 이후 conf 변경으로 Go A
```

- 느린 응답·요청 중 socket 끊김·큰 응답을 결정적으로 만들려고 test 전용 handler를 넣었다.
  - 방법: `-Dscouter.handler=zzinterop`과 `-Xbootclasspath/a`.
  - 동작: 원본 `ReqestHandlingProxy.load`가 정렬된 class 이름 순으로 handler를 등록하므로, 이 handler가 원본 `OBJECT_CHECK_RESOURCE_FILE` handler 하나만 대체한다. Agent 로그에 A129가 1건 남는다.
  - 나머지 명령, `TcpWorker`, `TcpRequestMgr`, `DataProxy`는 원본 그대로다.

## 2. 항목별 결과

수치는 최종 실행 2회(JDK 8 / JDK 11)의 값이다. 한 값만 적은 것은 두 실행이 같았다는 뜻이다.

### 2.1 수집

| 검증 | 결과 | 근거 |
|---|---|---|
| object heartbeat·type | PASS | `/interop-host/interop-sample`, type `java`, alive |
| XLog 저장·realtime 조회(`TRANX_REAL_TIME_GROUP_LATEST`) | PASS | 39/37건, error 4건, 2.5초 slow 1건 |
| service·error·method text | PASS | `interop.SampleService.handle(IJ)Ljava/lang/String;`, `java.lang.IllegalStateException: interop synthetic error`, `…stepLoad(J)…` |
| profile(`TRANX_PROFILE`, 원본 `Step.toObjects`) | PASS | step 4개(MethodStep 3, HashedMessageStep 1) |
| realtime counter | PASS | TPS(Float), ElapsedTime(Decimal), HeapUsed(Float), ActiveService(Decimal) |
| Go UDP/ingest 오류 | PASS | Malformed·Rejected·StorageErrors 0 |
| Agent TCP framing | PASS | magic `0xcafe1002`(V2), objHash 일치 |

### 2.2 서버→Agent relay

모든 relay에서 응답 decode와 Client 전달이 정상이었다. 기록 proxy에서도 명령마다 1회 전달되고 Agent 응답이 끝까지 완결되는 것을 확인했다(parse error 0).

| 명령 | 결과 | 근거(JDK 8 / JDK 11) |
|---|---|---|
| `OBJECT_THREAD_LIST` | PASS | id 116/117개, worker·idle thread 이름 포함 |
| `OBJECT_ENV` | PASS | key 65/67개, `-Dinterop.marker` 값 일치(값 자체는 기록하지 않음) |
| `OBJECT_ACTIVE_SERVICE_LIST` | PASS | 실행 중인 slow service 1건(`SampleService`), `complete` 포함 14개 key |
| `OBJECT_THREAD_DUMP` | PASS | 2,756/2,810줄, Agent 응답 168KB, 184/188ms |
| `OBJECT_HEAPHISTO` | PASS | 831/1,137줄, `java.lang.String` 포함 |
| `OBJECT_DUMP_FILE_LIST` / `OBJECT_DUMP_FILE_DETAIL` | PASS | 9MiB 파일을 4KB blob 2,304개로 받음. SHA-256 일치 |
| `GET_CONFIGURE_WAS` / `LIST_CONFIGURE_WAS` | PASS | conf 원문 포함 / 306개 key |
| `OBJECT_CLASS_LIST` | PASS | 100개/page |
| `OBJECT_SYSTEM_GC`(관리 relay) | PASS | Java처럼 결과를 버림(응답 Pack 0). Agent 로그에 A127이 남음 |

JDK 9 이상에서는 `-Djdk.attach.allowAttachSelf=true`가 없으면 `OBJECT_THREAD_DUMP`와 `OBJECT_HEAPHISTO`에 Agent가 `error` 값 하나만 담아 답한다. Agent는 "Fail to JVM attachment" Alert도 보낸다(`scouter/xtra/tools/ToolsMain.java`). 개발 중 실행에서 이 응답도 Go가 그대로 Client에 전달하는 것을 확인했다. 서버와 무관한 Agent 쪽 조건이다.

### 2.3 고아 relay·Agent 실패·응답 상한

| 검증 | 결과 | 근거 |
|---|---|---|
| Client가 자기 so-timeout(2초)으로 먼저 떠나고 Agent는 6초에 답함 | PASS | Client 2.0초에 끊김(`isValid=false`). Agent handler 정상 종료. 같은 proxy 연결이 다음 명령을 처리(재접속 없음) |
| Agent 응답(40초)이 Go `RequestTimeout`(30초)보다 늦지만 Client so-timeout(60초) 안(12단계) | PASS | Client가 40,039/40,021ms에 응답을 받음(item 1, tag 일치, 응답 뒤 socket 열림). `agentOrphans` Orphaned 변화 0, Active 0. Agent 세션 유지(같은 proxy 연결) |
| Client가 so-timeout(8초, 기본값)으로 먼저 떠나고 Agent는 40초에 답함(12단계) | PASS | Client 8,002/8,003ms에 끊김(`isValid=false`). harness가 Client 결과를 받은 직후 첫 조회에 이미 고아였다(Orphaned +1, Active 1, Bytes 8MiB). 고아가 40.01초에 응답을 끝까지 읽음 → Completed +1·Failed 0·Active 0. 세션 유지(같은 연결), 다음 명령 정상. Go 로그: `answered 40.01s after the request, after its caller had left; … the agent session kept` |
| 고아 relay 중 같은 Agent에 재요청 | PASS | 9.0초 시점 요청(다른 Client 연결)이 1.0초(`net_tcp_get_agent_connection_wait_ms`) 뒤 빈 응답을 받음. Agent handler는 호출되지 않음(handler 호출 수 +2 = 고아 1 + 이후 1). 부하 명령이 다시 가지 않는다 |
| 요청 중 Agent socket 끊김(단일 대상 relay) | PASS | Client는 `NoNEXT`만 받음(빈 응답, 연결 유지). Java `AgentCall` null과 같다. Agent는 0.1~0.7초 뒤 재접속, 다음 명령 정상. Go 로그: `skipped objHash=… answered no item like Java's null AgentCall result: EOF` |
| 7MiB 응답 | PASS | BlobValue 7,340,032 B 전달 |
| 9MiB 응답(일반 relay 상한 8MiB 초과) | PASS | Client 요청 실패(`NoNEXT` 없이 끝남). Go 로그: `refused: response over the Go relay limit (item=8MiB …)`, Agent 세션 닫힘. Agent는 0.9초 뒤 재접속, 다음 명령 정상. Agent stderr에 stack trace 없음(`TcpWorker`가 삼킴) |
| Agent 응답(40초)이 Go Agent so-timeout보다 늦음(12단계, 4b 재기동 뒤에 실행) | PASS | 실행 중 서버 conf `net_tcp_agent_so_timeout_ms=15000`(3초 reload), socket 끊김으로 Agent 재등록 뒤 요청. Client(so-timeout 60초)는 15,005/15,005ms에 `NoNEXT`만 받음(item 0, 연결 유지). 기록 proxy에서 서버가 그 Agent 세션을 15.01/15.0초에 닫음(`by=server`), 그 세션의 응답 기록 0. 고아 0. Agent는 handler가 끝난 40.0초 뒤 새 세션으로 재접속(conf는 기본값으로 되돌림), 다음 명령 정상. 원본 `TcpAgentWorker` read timeout → close → `AgentCall` null과 같다 |

"(12단계)" 표시 행과 "고아 relay 중 같은 Agent에 재요청" 행은 HEAD `644b607` 빌드의 최종 실행 값이다(JDK 8 / JDK 11, 3f 위치 이동 뒤). Client 8,002/8,003ms·재요청 9.0초는 `54c5864` 빌드 실행과 같았다. 나머지 행은 11단계 실행 값이며, 12단계 실행에서도 모두 PASS였다. 3f는 기동 30초 뒤 첫 `OBJECT_RESET_CACHE`의 재시도와 겹치지 않도록 시나리오 4b(새 DB 재기동, 그 reset 전달 확인) 뒤에서 실행한다(§4-1).

이력(수정 전 빌드, commit `6c08cb6`·`0fe0cfd`): Agent 응답(40초)이 Go `RequestTimeout`(30초)보다 늦으면 Client(so-timeout 60초)는 30.0초에 응답 없이 끊겼다. 고아 relay가 40.0초에 응답을 끝까지 읽었다(`agentOrphans` Active 1·Bytes 8MiB → Completed 1·Failed 0, 세션 유지). 원본 서버라면 40초에 응답을 전달한다. 12단계에서 Agent 대기 중 Client 무진전 감시를 보류하고 Client 이탈은 socket probe로 감지하도록 바꿔 이 차이를 없앴다([agent-side-effects](../compatibility/agent-side-effects.md) §7(d) "Agent 대기 중 Client 진전 감시 보류").

#### 2.3.1 12단계 수동 확인: 원본 Client 연결 하나를 계속 쓰는 경우

harness의 Client driver는 요청마다 새 socket을 연다. 그래서 같은 Client 연결 재사용은 별도 driver로 확인했다. 이 driver는 원본 `TcpProxy` 하나를 쥐고 같은 socket으로 명령을 이어 보낸다. 서버 so-timeout은 서버 conf로 바꾸고, `LOGIN`이 돌려준 `so_time_out`이 원본 Client socket에 적용된다. 서버는 54c5864와 비테스트 Go 코드가 같은 작업 tree 빌드이고, Agent는 실제 Java Agent 2.20.0이다. 수치는 JDK 8 / JDK 11 값이다.

| 확인 | 결과 | 근거 |
|---|---|---|
| 1. 서버 `net_tcp_client_so_timeout_ms=60000`(LOGIN so_time_out 60000), Agent 40초 | PASS | 40,018/40,024ms에 응답(item 1). 같은 `TcpProxy`로 보낸 다음 명령은 3ms에 정상. Client local port는 두 명령 내내 같았다. 0.2초 간격 `/health` 조회에서 `agentOrphans` Active는 최대 0이었고 Orphaned·Completed 변화도 0이었다. Agent 세션 유지(재접속 0) |
| 2. 기본 conf(LOGIN so_time_out 8000), Agent 40초 | PASS | Client 8,005/8,003ms에 `SocketTimeoutException`으로 socket을 닫음. 서버는 그 뒤 0.15/0.19초 안에(조회 간격 0.2초) 고아로 넘김(Active 1, Bytes 8MiB, Orphaned 1). 40.01/40.02초에 Completed 1·Failed 0·Active 0. Agent 세션은 고아 전후와 재요청 뒤까지 같은 연결. 재요청(새 Client socket)은 5/6ms에 정상. Agent handler 호출은 두 번(40초 요청, 재요청)뿐 |
| 3. 서버 `net_tcp_agent_so_timeout_ms=15000` + Client 60초, Agent 40초 | PASS | Client가 15,003ms에 빈 응답(`NoNEXT`)을 받고 같은 socket을 유지. 서버가 15초에 Agent 세션을 닫음. Go 로그 `skipped objHash=… answered no item like Java's null AgentCall result: … i/o timeout`. 고아 0. Agent는 handler가 끝난 뒤(약 41초) 재접속했다. 그 사이 약 26초 동안 이 Agent의 세션은 0개였다. 원본에서도 `TcpWorker`가 handler 안에 막혀 있으므로 같다. 재접속 뒤 같은 Client socket으로 보낸 명령은 정상 |

- 관찰: 확인 1 동안 서버의 Accepted가 1 늘었다. 원인은 원본 Client의 `ServerManager` thread다. 이 thread는 2초마다 `SERVER_STATUS`를 보낸다(`ServerManager.java:80`). driver가 pool의 `TcpProxy`를 쥐고 있으면 이 thread가 새 socket을 연다. 원본 Client의 정상 동작이며, driver의 연결은 local port로 같음을 확인했다.
- 공통: 샘플 JVM 생존, A130 0, Exception 0, 서버 정상 종료(exit 0).

### 2.4 KEEP_ALIVE·text reset·재기동

| 검증 | 결과 | 근거 |
|---|---|---|
| 유휴 세션의 KEEP_ALIVE | PASS | 70초 동안 8~10회, 간격은 정확히 5.0초 또는 10.0초(Java와 같은 5초 scan + 5초 유휴 조건). Agent 응답은 `NoNEXT`만이며 0.3~2.9ms |
| 70초 유휴(Agent so-timeout 60초 초과) | PASS | 같은 연결 유지, 재접속 0 |
| 기동 뒤 첫 `OBJECT_RESET_CACHE` | PASS(재시도 경로) | 두 실행 모두 30초 시점에는 Agent의 유일한 세션이 고아 relay(40초 응답)로 busy였다. Go 로그 `live 1 called 0 missing 1`, `retry pending 1` 뒤 재시도로 기동 66초 뒤에 전달됐다. 개발 중 실행(시나리오 시점이 달라 30초에 busy가 아니었던 1회)에서는 29.98초에 전달됐다. 12단계 첫 실행(시나리오 3에 40초 relay 3개)에서는 두 JDK 모두 189.99초에 전달됐다. 3f를 4b 뒤로 옮긴 최종 실행에서는 108.0/108.01초다(§4-1). 재기동 뒤 reset은 29.98/29.99초였다 |
| SIGTERM 정상 종료(Agent 연결 상태) | PASS | exit 0 |
| **새 빈 DB로 재기동**: reset 전 text 누락 | PASS | 재기동 12초 뒤 새 XLog 127건의 service hash가 새 DB에 없음(found 0). Agent는 이미 보낸 text를 다시 보내지 않는다(sent-set) |
| 같은 재기동 뒤 reset과 재전송 | PASS | 재기동 29.99초에 `OBJECT_RESET_CACHE`. Agent는 param을 echo(item 1). 이후 service text가 저장돼 조회됨. Go 로그 `live 1 called 1 missing 0` |
| 직접 연결(proxy 없음) 재기동 뒤 재연결 | PASS | 5초 정지 → ready 후 2.88/2.94초, 12초 정지 → 0.80/0.83초. Agent의 connect 실패 뒤 3초 sleep + 1초 loop 주기와 맞음. 같은 DB의 object는 ready 1.9초 뒤 alive(heartbeat 수신 전에는 alive=false로 보임) |
| proxy 경유 재기동 뒤 재연결 | PASS | 0.28~0.31초. proxy가 upstream 거절을 connect 후 EOF로 보이게 하므로 직접 연결보다 빠르다. 실제 수치는 위 직접 연결 값을 쓴다 |

text reset을 날짜 전환 없이 재현하는 방법: 서버를 새 빈 DB로 재기동한다. Go는 기동 30초 뒤 reset을 보내므로 test hook이 필요 없다.

### 2.5 collector 주소 변경 (runbook §4)

| 검증 | 결과 | 근거 |
|---|---|---|
| Java Agent: 같은 서버의 다른 TCP 주소(proxy → 직접) | PASS | conf를 쓴 뒤 4.2~5.2초에 Agent가 옛 세션을 스스로 닫음(`close by agent`). 옛 세션의 마지막 frame은 KEEP_ALIVE 명령과 응답. 0.1초 뒤 새 주소로 연결 |
| Java Agent: UDP·TCP를 새 서버 B로 | PASS | 10.1~10.2초에 B의 Agent 연결 1, A는 0. object·XLog가 B에 저장되고, B 경유 `OBJECT_THREAD_LIST` relay가 정상. 전환 뒤 A의 XLog 증가 0 |
| Host Agent: conf를 A로 변경 | PASS | UDP는 3.6초 안에 A에 object로 나타남. 20초 뒤에도 V1 TCP는 B에 남아 B 경유 `OBJECT_ENV` relay 정상. A의 Agent 연결 0 |
| Host Agent: B 정지 | PASS | V1 TCP가 0.1~0.2초 만에 A로 연결. A 경유 relay 정상 |

runbook §4의 "다음 서버 요청(KEEP_ALIVE)이 끝난 뒤 주소 변경을 보고 닫힌다"를 frame 기록으로 확인했다. 전환에 걸리는 시간은 다음과 같다.

- 같은 서버: conf reload(최대 3초) + 다음 KEEP_ALIVE(유휴 5~10초)로 약 4~13초다. 측정값은 4.2~5.2초였다.
- 새 서버: 약 10초를 측정했다.

### 2.6 Host Agent

| 검증 | 결과 | 근거 |
|---|---|---|
| object·counter | PASS | `/interop-host-agent`, type `osx`, `Cpu`(Float), V1 TCP 1 |
| V1 relay | PASS | `OBJECT_ENV`(marker 일치, key 이름도 기록하지 않음), `HOST_TOP`(2.2초), `HOST_DISK_USAGE`, `LIST_CONFIGURE_WAS` |
| 예외 | PASS | Host Agent 출력에 Exception 0 |

### 2.7 Agent 프로세스 부작용

- 샘플 JVM은 끝까지 살아 있었다.
- A130(unknown cmd)은 0건이다. Go는 Agent가 모르는 명령을 보내지 않았다.
- A129(test handler 등록)는 1건이다.
- 출력과 Agent 로그의 Exception/Error는 0건이다. 9MiB 거절과 socket 끊김 때도 stack trace가 없었다.

### 2.8 서버 발신 명령 (11단계 C6)

Go가 Client 요청 없이 스스로 Agent에 보내는 명령 중 아직 실제 Agent로 확인하지 않았던 두 가지를 검증했다. 시나리오 7은 §2.6 뒤에 서버 A에서 실행한다.

- 샘플 JVM: conf를 바꿔 A(기록 proxy 경유)로 돌아온다.
- kube Host Agent: 새로 띄운 두 번째 Host Agent다. 환경변수 `KUBERNETES_SERVICE_HOST`를 주고(원본 `Configure.isKube()`는 비어 있는지만 본다), conf `test_host_name=interop-kube-pod-7d9f8b6c5d-x2x4z`(33자)를 준다. 원본은 18자를 넘고 `-` 조각이 3개 이상인 host 이름에서 끝의 두 조각을 뺀 `interop-kube-pod`를 pod 이름으로 쓴다(`scouter.agent.host/.../Configure.java:347-362`).
- 이 Host Agent는 샘플 JVM과 `counter_object_registry_path`(임시 directory)를 공유한다. 그래서 원본 `ProcPerf`가 샘플 JVM의 `ProcCpu`를 Java object 이름으로 보낸다. 원본 Java 서버의 `ObjectCpuChecker`가 읽는 counter가 이것이다.
- 모든 Agent의 등록 directory를 임시 directory로 바꿨다. 기본값 `/tmp/scouter`는 이 장비의 다른 Agent와 공유된다. 또 kube 이름을 쓰지 않는 Host Agent는 그 안의 `*.scouterkubeseq`를 모두 지운다(`AgentHeartBeat.java:82-85`). 그래서 첫 Host Agent는 별도 directory를 쓴다. 비-kube Agent 프로세스의 환경에서는 `KUBERNETES_SERVICE_HOST`를 뺀다.

| 검증 | 결과 | 근거(JDK 8 / JDK 11) |
|---|---|---|
| `OBJECT_SET_KUBE_SEQ`: `<pod>_<seq>` 등록 | PASS | 시작 4.6/4.1초 뒤 `/interop-kube-pod_0`(type `osx`) alive. pod 이름의 object는 이것 하나다(`_rnd<n>` 이름은 등록되지 않음). Go `KubeHeld` +1, `KubeSeqEnqueued`/`Done` +1, `Failed`·`Dropped` 0. 후속 보완 뒤 개발 실행에서는 +2/+2였다. 이름을 바꾸기 전 heartbeat가 한 번 더 오면 원본처럼 15초 hold 안에서 다시 push한다(`kube.go` `applyNewPodSeq`). Agent 등록 directory에 `0.scouterkubeseq` = `interop-kube-pod_0` |
| 이름이 바뀐 Agent로 relay | PASS | 새 objHash로 `OBJECT_ENV` 1건(marker 일치). 원본 `HostAgentEnv.setKubePodSeq`가 이름을 바꾸고(`HostAgentEnv.java:55`), `TcpWorker.process`는 objHash가 바뀌면 loop를 끝내 새 objHash로 다시 연결한다(`TcpWorker.java:74`) |
| `ProcCpu` 수신 | PASS | Java object(`/interop-host/interop-sample`)의 realtime `ProcCpu` FloatValue `4.9E-5`/`3.6E-5` |
| ADC tag 반영(후속 보완으로 추가) | PASS(개발 실행) | 서버 object의 tag가 `{ADC: true, ADC_TH: 90, ADC_D: 8000}`이 된 것을 `OBJECT_LIST_REAL_TIME`으로 확인했다. threshold 0으로 바꾼 뒤에도 `ADC_TH: 0` 반영(2.3초)을 trigger 판정 전에 확인했다 |
| `ADC_TH` 90에서는 보내지 않음 | PASS | ADC 켠 conf 적용 뒤 16초(`ADC_D` 8초 + 여유) 동안 `CPUDumpEnqueued` 증가 0, proxy에 명령 0, dump 파일 0. **threshold 비교의 변별력은 없다.** 관찰 1대로 `ProcCpu`가 0~1이라 90은 어떤 부하에서도 넘지 않는다. 이 행은 ADC tag가 반영된 상태에서 잘못된 전송이 없다는 것만 보인다 |
| `ADC_TH` 0: trigger 전송 | PASS | conf 적용 16.0/15.4초 뒤 1회 보냄. 그 시간은 conf reload(≤3초) + heartbeat + `ADC_D` 8초 + 판정 pack 주기다. proxy가 푼 파라미터는 `{objHash: <샘플 objHash>, TRIGGER_DUMP_REASON: "TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED"}`이며 원본 `ObjectCpuChecker.scala:27-28`과 같다. Agent 응답은 `NoNEXT`만(item 0, 3.9/1.7ms)이다. 원본 handler가 null을 돌려주기 때문이다 |
| Go 호출 queue | PASS | trigger 직후 threshold를 90으로 되돌려 반복을 막았다. `CPUDumpEnqueued` = `Done` = proxy가 본 명령 수 = 1, `Failed`·`Dropped` 0 |
| Agent 쪽 원본 가드 | PASS | `autodump_cpu_exceeded_dump_cnt=3`, `dump_interval_ms=2000`. dump 파일 2개(`scouter.threaddump.*.dump`, 각 170~200KB, 간격 2.01/2.03초), trigger 1회. reason이 CPU_EXCEEDED가 아니면 원본 `DumpOnCpuExceedanceWorker`는 가드 없이 trigger마다 `triggerThreadDump()` 1개만 만든다(`DumpOnCpuExceedanceWorker.java:37-41`). 따라서 판정은 **trigger 수 < 파일 수 ≤ 3 × trigger 수**와 dump_interval 간격이다. 파일이 trigger 수보다 많은 것이 가드 경로(`DumpUtil.autoDumpByCpuExceedance`의 dump_cnt loop)를 탔다는 근거다. 최초 판정(2 ≤ n)은 trigger가 2회 이상이면 가드 없는 경로도 통과시킬 수 있어 리뷰 반영으로 고쳤다 |
| Agent 부작용 | PASS | 샘플 JVM 생존. 시나리오 동안 Agent 출력에 Exception 0, A130 0 |

관찰(원본 동작, Go 결함 아님):

1. **Host Agent 2.20.0의 `ProcCpu`는 백분율이 아니라 기계 전체 대비 비율(0~1)이다.**
   - 원본 `ProcPerf`: `getProcessCpuLoadBetweenTicks(null)`(oshi 6.3.1, `ProcPerf.java:93-94`). 이전 snapshot이 null이면 process 수명 누적 부하를 돌려준다. 이 값을 다시 논리 core 수로 나눈다.
   - 관찰값은 `3.5E-5`~`4.9E-5`였다.
   - 그래서 원본 서버와 Go 모두 정수 `ADC_TH` ≥ 1에서는 CPU autodump를 보내지 않는다. 기본값 90도 마찬가지다.
   - 이번 검증은 threshold 0으로 전송 경로를 확인했다.
   - Go는 원본 비교식(`threshold < ProcCpu`)을 그대로 따르므로 동작이 같다.
2. **첫 trigger의 dump는 `dump_cnt`보다 하나 적게 생길 수 있다.**
   - `dump_cnt=3`인데 두 실행 모두 파일이 2개였다.
   - 추정 원인: 원본 `DumpUtil.getInstance()`가 dump thread를 시작한 직후 `trigger()`(`notifyAll`)를 부른다. 그래서 thread가 아직 `wait` 전이면 첫 알림이 사라진다(`DumpUtil.java:46-55, 192-203, 256`).
   - Agent 쪽 동작이며 서버와 무관하다.

## 3. 발견한 서버 결함

없음.

## 4. 관찰·남은 위험 (결함 아님)

1. **text reset이 고아 relay 뒤로 밀린다.**
   - 관찰: 30초 pass 시점에 Agent의 유일한 세션이 고아 relay로 busy였다. 그래서 reset은 같은 날짜 재시도(10·20·40·80초 간격, 최대 5회)로 66초에 전달됐다.
   - 12단계 첫 실행에서는 시나리오 3의 40초 relay 세 개(Client가 기다리는 relay, 고아, Agent so-timeout 초과 뒤 Agent가 재접속하기 전 약 26초)가 이어졌다. 그래서 두 JDK 모두 189.99초에 전달됐다. Go 로그의 재시도 합계는 `failed` 1+2+1 = 4였고 `gave up`은 0이었다. 따라서 5회 중 마지막 재시도로 전달된 것이다. 이 harness 판정("30초 또는 busy 뒤 같은 날짜 재시도")은 시나리오 3이 조금만 더 길어져도 `gave up`으로 실패할 수 있었다.
   - **해결(harness, 3f 위치 이동):** 3f를 시나리오 4b(새 DB 재기동) 뒤로 옮겼다. 4b가 끝날 때는 재기동 뒤 reset이 이미 전달된 상태다. 최종 실행(HEAD `644b607`)에서는 JDK 8·11 모두 첫 reset이 108.0/108.01초에 전달됐다. 30초 pass 시점의 busy는 3b(Client가 기다리는 40초 relay)와 3c(고아 40초)다. Go 로그의 재시도 합계는 `failed` 1+2 = 3 뒤 `called 1`, `gave up 0`이다. 즉 4번째 재시도 호출로 전달됐고, `gave up`까지는 실패 2회(80초 간격 포함)의 여유가 남는다. 재기동 뒤 reset은 29.98/29.99초였다. 11단계의 66초보다 늦은 것은 시나리오 3에 40초 relay가 하나(3c) 더 있기 때문이다.
   - 남은 위험: 큰 응답 명령(`OBJECT_HEAPHISTO` 등)의 고아는 최대 so_timeout+10분 동안 세션을 쥘 수 있다. 그러면 재시도 5회가 모두 busy로 끝나 그 날짜의 reset을 포기할 수 있다(`gave up`). 원본은 busy면 S501로 건너뛰고 자정까지 기다리므로 원본보다 나빠지지는 않는다.
   - 영향: 그동안 서버 재기동 중 잃은 text가 빈 이름으로 보인다.
2. **Client가 so-timeout으로 먼저 떠나도 Go는 Agent 응답까지 relay를 계속한다.** 원본과 같고 세션도 유지된다. Go가 Client 이탈을 감지하는 시점은 응답을 쓸 때다. (12단계 이후: Agent 대기 중 Client가 연결을 닫으면 probe가 즉시 감지해 명령을 끝내고 relay는 고아로 계속된다. 실제 원본 Client(so-timeout 8초)로 확인했다. 이탈 0.2초 안에 고아가 됐다(§2.3, §2.3.1). half-open Client는 TCP keepalive나 relay deadline까지 감지되지 않으며 실제로 시험하지 않았다.)
3. **같은 DB로 재기동하면 heartbeat가 오기 전까지 object가 alive=false로 보인다.** 이번에는 1.9초였다. 운영 화면에서는 재기동 직후 몇 초간 Agent가 죽은 것처럼 보일 수 있다. 원본 서버와의 비교는 하지 않았다.

## 5. 개발 중 실패한 실행 (harness 결함)

| 실행 | 실패 | 원인과 조치 |
|---|---|---|
| 1 | realtime counter 4개 모두 없음 | `COUNTER_REAL_TIME` 요청에 `timetype`을 빠뜨렸다(원본 Client 요청 형식 위반). `TimeTypeEnum.REALTIME`을 추가했다 |
| 2 | 첫 reset 66초 | 30초에 세션이 고아 relay로 busy였다(§4-1). 판정을 "30초 또는 busy 뒤 같은 날짜 재시도"로 바꿨다 |
| 3 | 재기동 뒤 object 확인 | 대기 없이 한 번만 조회했다. alive가 될 때까지 기다리도록 바꿨다 |
| 4(JDK 11) | thread dump·heap histogram `error`, 재기동 뒤 object alive=false | 앞의 것은 JDK 9+ self-attach 조건이라 `--agent-jvm-opt`를 추가했다. 뒤의 것은 heartbeat 전 alive=false를 기다리지 않은 탓이다 |

실행 1~2의 Go binary는 작업 tree를 그대로 빌드한 것이라 작업 tree의 미커밋 변경이 섞여 있었다. 그래서 이후 실행은 `git archive`로 깨끗한 tree를 빌드했다. 최종 결과(§2)는 깨끗한 빌드의 값이다.

## 6. 실행 명령과 출력

```sh
SCOUTER_SOURCE=<scouter baseline checkout> python3 scripts/java-agent-interop.py --report <dir>/final-jdk8.json
#   [09:08:18] checks: 45 pass, 0 fail; elapsed 240.6s   (exit 0)
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --agent-java-home <JDK11> \
    --agent-jvm-opt=-Djdk.attach.allowAttachSelf=true --report <dir>/final-jdk11.json
#   [09:12:17] checks: 45 pass, 0 fail; elapsed 237.9s   (exit 0)
python3 scripts/java-agent-interop.py --compile-only   # 원본 guard·컴파일만
```

11단계 재실행(commit `cc65050`, `git archive` 빌드, 시나리오 7 추가):

```sh
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --report <dir>/final-jdk8.json          # JDK 8 (1.8.0_345)
#   [17:05:01] checks: 53 pass, 0 fail; elapsed 300.0s   (exit 0)
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --agent-java-home <zulu-11> \
    --agent-jvm-opt=-Djdk.attach.allowAttachSelf=true --report <dir>/final-jdk11.json             # JDK 11 (11.0.27)
#   [17:10:07] checks: 53 pass, 0 fail; elapsed 294.3s   (exit 0)
```

12단계 재실행(commit `54c5864`, `git archive` 빌드, 시나리오 3 개편으로 56개):

```sh
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --server-rev 54c5864 --report <dir>/v2-jdk8.json   # JDK 8 (1.8.0_345)
#   [22:28:42] checks: 56 pass, 0 fail; elapsed 379.7s   (exit 0)
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --server-rev 54c5864 --agent-java-home <zulu-11> \
    --agent-jvm-opt=-Djdk.attach.allowAttachSelf=true --report <dir>/v2-jdk11.json                         # JDK 11 (11.0.27)
#   [22:34:59] checks: 56 pass, 0 fail; elapsed 374.8s   (exit 0)
```

3f를 4b 재기동 뒤로 옮긴 최종 실행(HEAD `644b607`, `git archive` 빌드):

```sh
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --report <dir>/v3-jdk8.json                       # JDK 8 (1.8.0_345)
#   [23:22:19] checks: 56 pass, 0 fail; elapsed 395.6s   (exit 0)
SCOUTER_SOURCE=<…> python3 scripts/java-agent-interop.py --agent-java-home <zulu-11> \
    --agent-jvm-opt=-Djdk.attach.allowAttachSelf=true --report <dir>/v3-jdk11.json                         # JDK 11 (11.0.27)
#   [23:28:47] checks: 56 pass, 0 fail; elapsed 385.7s   (exit 0)
```

- 시나리오 3을 고치기 전 harness(54개)로 같은 코드 빌드를 실행하면 JDK 8·11 모두 52/54였다. 실패 2개는 수정 전 동작을 기대한 판정이다. 하나는 "Client ends ~30s"인데 실제로는 40.0초에 응답을 받았다. 다른 하나는 "고아 중 재요청"인데, 첫 요청이 40초에 끝나 재요청 시점에 고아가 없었다. 이 두 판정을 새 3b·3c로 바꾸고 3f를 더했다.
- 개발 중에는 `--server-calls-only`(수집 1 + 시나리오 7만, 판정에 쓰지 않음)로 1회 실행했다. 그 실행의 실패 1건은 harness 결함이었다. 이미 proxy 세션이 있는데 새 `magic`만 기다렸다. 판정을 "그 objHash의 열린 proxy 세션"으로 고쳤다.
- report의 `server_a_agent_calls`에는 서버 A의 kube·CPU dump queue 계수가 있다. 두 실행 모두 KubeSeq Enqueued/Done 1, CPUDump Enqueued/Done 1이고 Failed·Dropped는 0이다.

- 실행 뒤 남은 JVM·Go·proxy 프로세스는 0개다. 임시 directory는 제거했다.
- report JSON에는 다음이 들어 있다.
  - 검사별 근거.
  - jar SHA-256, 통신 경로 diff 결과.
  - 서버 commit과 binary SHA-256.
  - 첫 서버의 재기동 전 `/health` TCP·orphan 계수(TCP Errors 2 = Timeout 1(고아 relay의 Client 종료) + Limit 1(9MiB 거절)).
  - 관련 Go 로그 줄.

## 7. 미실행·한계

- **원본 소스 빌드 Agent(2.21.3)는 실행하지 않았다.** §1.1의 diff 근거로 2.20.0 배포 jar를 대신 썼다.
- **servlet·JDBC·HTTP client를 쓰는 앱은 아니다.** SQL·API call step, user/visitor, referer, `TCP_AGENT_REQ`(stack upload)는 만들지 않았다.
- **실제 부하 명령이 느려지는 상황(큰 heap의 histogram 등)은 재현하지 않았다.** 지연·실패는 test handler로 결정적으로 만들었다. relay 경로(일반 relay 상한·deadline)는 같지만, 큰 응답 명령 class(256MiB, so_timeout+10분)의 고아 relay는 실제 Agent로 시험하지 않았다.
- **원본 Java 서버와 나란히 비교하지 않았다.** 원본 동작은 소스 근거와 기존 문서로 비교했다.
- 다음은 시험하지 않았다.
  - `net_collector_tcp_session_count` > 1, objHash당 8개 상한 초과.
  - 여러 Agent, 원격 호스트, 실제 망 RTT.
  - 날짜 전환에 따른 reset(Agent 자체 reset과 서버 reset의 이중 발생).
- 실행은 JDK 8·11 각 1회(최종)다. 시간 값(재연결·전환 초)은 loopback·저부하 조건의 관찰값이며 운영 수치가 아니다.
- 시나리오 7(서버 발신 명령)의 한계:
  - 실제 Kubernetes가 아니다. 환경변수와 `test_host_name`으로 원본의 kube 이름 규칙만 재현했다.
  - 같은 pod 이름의 host 여럿, seq 충돌·재할당, 15초 hold 만료, object 비활성화 뒤 seq 반환은 실제 Agent로 시험하지 않았다(Go 단위 테스트만).
  - CPU autodump는 threshold 0으로만 전송을 확인했다(위 관찰 1). 여러 번 연속 trigger, Agent queue(2) 초과 drop, Go queue(100) 포화는 시험하지 않았다.
  - `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`는 기록 proxy(V2)로 파라미터를 확인했다. `OBJECT_SET_KUBE_SEQ`(V1)의 파라미터 `{seq}`는 wire에서 직접 보지 않았다. Agent가 seq 0으로 이름을 바꾼 결과로 확인했다.

## 8. 서버→Agent framing 불변식 (Go 테스트, 11단계 C8)

Java Agent는 서버가 선언한 길이만큼 검증 없이 할당한다. V2는 `readIntBytes`, V1은 Blob 길이다(`DataInputX.java:71-80, 90-110`). 그래서 길이가 틀린 frame이나 item 상한을 넘는 frame은 곧 Agent heap 할당이 된다([agent-side-effects](../compatibility/agent-side-effects.md) §2, §8 2번). 이 위험을 막는 회귀 테스트를 추가했다(`internal/transport/agent_frame_fuzz_test.go`).

- 방법: `net.Pipe`의 한쪽을 `TCPServer.serveConn`과 같은 `AgentConn`으로 감싼다. `RequestLimited`가 실제로 쓴 byte는 반대쪽 가짜 Agent가 읽는다. 가짜 Agent는 `DataInputX`를 따라 Go decoder와 독립으로 구현했다. 선언 길이가 item 상한을 넘으면 할당 전에 위반으로 기록한다.
- `FuzzAgentRequestFrame`: 입력은 version(V1/V2), item 상한(16B~128KiB), `MaxTextBytes`, 명령 문자열, MapPack 파라미터(Null·Boolean·Decimal·Text·Blob, 4KiB writer buffer를 넘는 값 포함)다. 단언은 다음과 같다.
  - V2: int32 선언 길이 = 뒤따르는 byte 수, 4+길이 ≤ item.
  - V1: Java 호환 reader가 명령(text+MapPack)을 정확히 소비하고 뒤에 남는 byte가 없다.
  - 성공 여부 = 독립 계산한 크기가 상한 안인지(거짓 `ErrLimit`도 실패).
  - 거절(`ErrLimit`/`ErrInvalid`)이면 V1·V2 모두 wire 0 byte다.
  - `RequestLimited`는 실패하면 스스로 연결을 닫고, 성공하면 닫지 않는다(`AgentConn.Closed()`).
  - 성공이면 Agent가 decode한 명령·entry가 보낸 것과 같다.
- `TestAgentRequestFrameAtRelayLimit`: 실제 일반 relay 상한 8MiB에서 V1·V2 모두 확인한다. 정확히 8MiB인 명령은 그대로 쓰이고(V2 선언 8MiB−4), 1 byte가 넘으면 `ErrLimit`이며 wire는 0 byte다.
- `TestAgentRequestFrameAtTextLimit`: 명령 text 상한(`TCPConfig.MaxTextBytes`)을 V1·V2 각각 확인한다. 길이가 상한과 같으면 그대로 쓰이고, 1 byte 넘으면 `ErrLimit`이며 wire 0 byte, 연결은 닫힌다.
- 잘못된 UTF-8 명령(V1·V2 seed)은 `ErrInvalid`로 거절되고 wire 0 byte다.

mutation 확인. 작업 tree가 아니라 `git archive HEAD`를 푼 임시 사본에서 비테스트 코드를 바꿨다. 그 시점에 작업 tree의 `tcp.go`·`stream.go`는 수정하지 않았다(`git status`/`git diff` 확인). 아래 V1 수정은 그 뒤에 한 별도 변경이다.

| mutation | 기존 `go test ./internal/transport/` | 새 테스트 |
|---|---|---|
| M1 `WriteIntBytes`가 길이+1을 씀 | FAIL(이미 잡음) | FAIL(Agent 쪽 선언 길이 초과 감지) |
| M2 `WriteIntBytes`가 길이·본문을 먼저 쓰고 상한을 나중에 검사 | **ok(놓침)** | FAIL(`ErrLimit`인데 8MiB+1 frame이 wire에 나감) |
| M3 `WriteIntBytes` 상한 1 byte 완화(`writeRoom()-3`) | ok | ok. 동등 mutant다. `Stream.Write`의 쓰기별 상한이 본문을 다시 막고, 버퍼에 남은 길이 4 byte는 연결을 닫을 때 버려진다. wire 결과가 바뀌지 않는다 |
| M4 V1에서 Pack을 text보다 먼저 씀 | FAIL | FAIL |
| M5 V1에서 text를 쓴 직후 flush | **ok(놓침)** | FAIL(거절된 요청의 text 11 byte가 wire에 나감) |

첫 판 실행 결과(작업 tree, HEAD `cc65050` + 새 테스트 파일):

```sh
gofmt -l internal/transport/                      # 출력 없음
go vet ./internal/transport/                      # 출력 없음
go test -race -count=1 ./internal/transport/      # ok 10.3s
go test -run '^$' -fuzz '^FuzzAgentRequestFrame$' -fuzztime 60s -parallel 4 ./internal/transport/
#   execs 3,709,156, new interesting 40 (total 149), PASS
```

V1 부분 쓰기(발견 → 수정):

- 현상: 이전 V1 경로는 명령 text를 쓰고(`WriteText`) 이어서 Pack을 썼다(`WritePack`). text가 writer buffer(4KiB)보다 크면 text가 먼저 socket에 나갔다. 그 뒤 Pack이 `ErrLimit`으로 거절되면 Agent는 text만 받고 연결이 닫힌 것을 봤다. probe에서 5,000자 명령일 때 wire 5,003 B였다.
- 영향: 메모리 위험은 아니었다(64KiB 안, 선언 길이 일치, Host Agent는 EOF로 재연결). 서버 명령 이름이 짧은 상수라 실제로 생기지도 않았다.
- 수정: `RequestLimited`(`internal/transport/tcp.go`)가 V1도 text+Pack을 한 번에 encode하고 상한을 확인한 뒤 쓴다. V1은 그 byte를 inline으로, V2는 `WriteIntBytes`로 쓴다. 받아들이는 범위는 같다. 이전에도 `Stream.Write`가 text+Pack 누적 ≤ item을 강제했다.
- 회귀 확인: fuzz seed(V1, 5,000자 명령, item 6,000, 5,000 B Blob)가 이전 코드에서는 실패하고(`V1 … 5003 bytes reached the Agent`) 새 코드에서는 통과한다.
- 동작 불변: HEAD + 이 수정을 `git archive` 사본에서 `go test ./internal/transport/ ./internal/server/ ./internal/service/ ./cmd/...`로 돌려 모두 ok였다. 그 binary로 시나리오 7(`--server-calls-only`, V1 `OBJECT_SET_KUBE_SEQ` push와 `OBJECT_ENV` relay 포함)을 돌려 18/18 PASS였다. 전체 interop은 다시 돌리지 않았다.

후속 보완 뒤 재검증(작업 tree):

```sh
gofmt -l internal/transport/                      # 출력 없음
go vet ./internal/transport/                      # 출력 없음
go test -race -count=1 ./internal/transport/      # ok 10.3s
go test -run '^$' -fuzz '^FuzzAgentRequestFrame$' -fuzztime 60s -parallel 4 ./internal/transport/
#   execs 3,686,550, new interesting 16 (total 165), PASS
```

fuzz 입력의 version 대응도 고쳤다(홀수 V1, 짝수 V2). 이전에는 seed의 표시와 실제 version이 반대였다. 무작위 입력은 두 version을 모두 덮으므로 판정 범위는 같다.

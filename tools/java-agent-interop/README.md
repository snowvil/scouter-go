# 실제 Java Agent 상호운용 harness

실제 `scouter.agent.java`(`-javaagent`)를 붙인 작은 샘플 JVM을 임시 Go 서버(127.0.0.1, 임시 DB)에 연결하고, 원본 Client 통신 클래스(`TcpProxy`/`LoginMgr`)로 조회·relay를 실행한다. 운영 Agent·운영 DB·외부 네트워크는 쓰지 않는다. 결과 기록은 `docs/validation/2026-10-24-java-agent-interop.md`.

```sh
SCOUTER_SOURCE=/path/to/scouter python3 scripts/java-agent-interop.py --report /tmp/agent-interop.json
python3 scripts/java-agent-interop.py --compile-only            # 컴파일과 원본 guard만
python3 scripts/java-agent-interop.py --server dist/scouter-go --keep-temp
python3 scripts/java-agent-interop.py --agent-java-home <JDK11> --agent-jvm-opt=-Djdk.attach.allowAttachSelf=true
python3 scripts/java-agent-interop.py --server-rev <commit>       # 빌드할 revision(기본 HEAD)
python3 scripts/java-agent-interop.py --server-calls-only          # 개발용: 수집 1 + 시나리오 7만(판정에 쓰지 않음)
```

`--server`가 없으면 `git archive <rev>`로 푼 깨끗한 tree에서 `./cmd/scouter-go`를 빌드한다(작업 tree의 미커밋 변경은 들어가지 않는다). 샘플 JVM JDK는 기본 zulu-8(있으면)이다. JDK 9+에서 `OBJECT_THREAD_DUMP`·`OBJECT_HEAPHISTO`는 Agent가 자기 JVM에 attach하므로 원본 안내대로 `-Djdk.attach.allowAttachSelf=true`가 필요하다(없으면 Agent가 `error` 값과 경고 Alert를 돌려준다).

전체 실행은 약 6분 반(KEEP_ALIVE 관찰 70초, 40초 지연 relay 3회, 재기동 4회, 서버 발신 명령 약 50초 포함)이다. Go·JVM은 각각 CPU 2개로 제한한다(`GOMAXPROCS=2`, `-XX:ActiveProcessorCount=2`). 공유 호스트의 다른 측정과 겹치지 않게 실행한다.

## 구성

| 구성 요소 | 경로 | 역할 |
|---|---|---|
| Agent | `--agent-jar`(기본 `<source>/../scouter-bin/agent.java/scouter.agent.jar`) | 공식 배포 jar를 임시 directory에 복사해 쓴다(agent directory 기준의 `dump`/`plugin` 기본 경로가 원본 밖에 생기지 않게). jar의 `pom.properties` 버전 tag와 baseline commit 사이 `git diff`가 Agent 통신 경로(`agent/netio`, `agent/counter`, `Configure`, `JavaAgent`, `AgentBoot`, `common/net`, `common/io`, `common/lang`)에서 비어 있지 않으면 실패한다 |
| 샘플 앱 | `sample/interop/*.java` | `interop.SampleService.handle`을 worker 3개가 200ms 간격으로 호출한다. Agent conf `hook_service_patterns`로 XLog, `hook_method_patterns`로 method profile이 생긴다. 7번째 호출마다 예외(error XLog), worker 0의 5번째 호출마다 2.5초(active service), idle thread 100개 |
| test 전용 handler | `hooks/zzinterop/InteropHooks.java` | `-Dscouter.handler=zzinterop` + `-Xbootclasspath/a`. 원본 `ReqestHandlingProxy.load`가 정렬된 class 이름 순으로 등록하므로 원본 `OBJECT_CHECK_RESOURCE_FILE` handler를 대체한다(agent 로그 A129). 요청 MapPack으로 느린 응답(`sleep`), 큰 응답(`big`), 요청 중 socket 종료(`close`)를 만든다. 나머지 명령은 원본 handler 그대로다 |
| Client driver | `client/AgentInteropClient.java` | 원본 common + Client 통신 클래스와 `tools/java-integration`의 `PManager` adapter로 컴파일. 요청마다 새 `TcpProxy` socket을 열어 요청별 so-timeout을 쓴다. 응답은 크기·타입·hash·부분 문자열 일치만 출력한다(환경변수 값 등은 기록하지 않음) |
| 기록 proxy | `scripts/java-agent-interop.py` `FrameProxy` | Agent TCP(V2)를 Go 앞에서 중계하며 서버→Agent 명령 frame과 Agent 응답(HasNEXT item 수·byte, NoNEXT)을 시각과 함께 `proxy-frames.jsonl`에 남긴다. upstream(Go)이 없으면 Agent 연결을 바로 닫는다(Agent에는 connect 성공 후 EOF로 보임) |

## 시나리오

1. 수집: object heartbeat·type, XLog(error·slow 포함) realtime 조회, service/error/method text, profile(원본 Step decoder), realtime counter, Go UDP/ingest 오류 0, Agent TCP magic `0xcafe1002`(V2).
2. relay: `OBJECT_THREAD_LIST`, `OBJECT_ENV`(marker만), `OBJECT_ACTIVE_SERVICE_LIST`, `OBJECT_THREAD_DUMP`, `OBJECT_HEAPHISTO`, `OBJECT_DUMP_FILE_LIST`, `OBJECT_DUMP_FILE_DETAIL`(9MiB 파일 blob stream SHA-256 일치), `GET_CONFIGURE_WAS`, `LIST_CONFIGURE_WAS`, `OBJECT_CLASS_LIST`, `OBJECT_SYSTEM_GC`(관리 relay). proxy 기록으로 명령 1회·응답 완결 확인.
3. 실패 경로(test handler).
   - Client가 so-timeout(2초)으로 먼저 떠난 relay(Agent 6초).
   - Go `RequestTimeout`(30초)보다 늦지만 Client so-timeout(60초) 안인 응답(40초): Client가 40초에 응답을 받고 고아가 생기지 않는다(Agent 대기 중 진전 감시 보류).
   - Client가 so-timeout(8초)으로 떠난 뒤 Agent가 40초에 답함: 서버가 이탈을 바로 감지해 고아 relay로 넘기고, 고아가 끝까지 읽고 세션을 유지한다. 그 사이 재요청은 빈 응답이다.
   - 요청 중 Agent socket 종료(Java null), 7MiB 통과·9MiB 일반 relay 상한 거절.
   - Agent 응답이 Go Agent so_timeout(실행 중 conf로 15초)보다 늦음: Client는 빈 응답을 받고 연결을 유지하며, Go는 Agent 세션을 닫는다. 고아는 생기지 않는다. 그 뒤 conf를 기본값으로 되돌린다. 이 항목은 기동 30초 뒤 첫 text reset의 재시도와 겹치지 않도록 시나리오 4b 재기동 뒤에 실행한다.
4. KEEP_ALIVE 주기·응답(유휴 70초), 기동 30초 뒤 `OBJECT_RESET_CACHE`, 새 빈 DB로 재기동했을 때 reset 전 text 누락과 reset 뒤 재전송, 직접 연결 재기동 후 재연결 시간(정지 5초·12초).
5. Agent conf `net_collector_tcp_port` 변경(같은 서버의 다른 주소), UDP·TCP를 새 서버로 변경.
6. Host Agent(`--host-jar`, 기본 `<source>/../scouter-bin/agent.host/scouter.host.jar`, `scouter.boot.Boot`): object·`Cpu` counter, V1 relay(`OBJECT_ENV` marker, `HOST_TOP`, `HOST_DISK_USAGE`, `LIST_CONFIGURE_WAS`), conf 변경 시 UDP만 이동하고 V1 TCP는 기존 서버가 멈출 때 새 주소로 이동.
7. 서버 발신 명령(서버 A).
   - `OBJECT_SET_KUBE_SEQ`: 두 번째 Host Agent에 `KUBERNETES_SERVICE_HOST`와 conf `test_host_name=interop-kube-pod-7d9f8b6c5d-x2x4z`를 준다. 첫 heartbeat 보류 → seq push → `/interop-kube-pod_0` 등록 → 새 objHash로 relay.
   - `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`: 샘플 JVM conf로 `autodump_cpu_exceeded_*`를 켠다. ProcCpu는 같은 등록 directory를 읽는 kube Host Agent의 `ProcPerf`가 보낸다.
   - threshold 90에서는 보내지 않는지, threshold 0에서는 `ADC_D` 뒤 1회 보내는지 확인한다. 기록 proxy로 파라미터(objHash·CPU_EXCEEDED reason)와 `NoNEXT` 응답을 확인하고, Go queue 계수와 Agent `dump_dir` 파일 수·간격(원본 가드)도 확인한다.

모든 Agent의 `counter_object_registry_path`는 임시 directory다. 기본 `/tmp/scouter`는 이 장비의 다른 Agent와 공유된다. 또 kube 이름을 쓰지 않는 Host Agent는 그 안의 `*.scouterkubeseq`를 모두 지운다(원본 `AgentHeartBeat.writeHostNameForKube`). 그래서 시나리오 6의 Host Agent는 kube Host Agent와 다른 directory를 쓴다. 비-kube Agent 프로세스 환경에서는 `KUBERNETES_SERVICE_HOST`를 뺀다.

## 한계

- prebuilt 2.20.0 jar를 baseline(2.21.3 소스) 대용으로 쓴다. 근거는 위의 통신 경로 diff가 비어 있다는 것이며, 계측(ASM) 대상 라이브러리 변경분은 이 harness와 무관하다.
- 샘플 앱은 servlet/JDBC를 쓰지 않는다. HTTP·SQL·API call step, user/visitor, referer 등은 생성하지 않는다.
- test handler 시나리오의 지연·실패는 결정적으로 만든 것이다. 실제 부하 명령(heap histogram 등)이 느려지는 상황은 같은 relay 경로를 쓰지만 별도로 재현하지 않았다.
- Host Agent는 공식 배포 jar(2.20.0)를 쓰며 baseline 대비 `scouter.agent.host/src` diff가 비어 있지 않으면 실패한다.

# 원본 Java Agent 송신 경로 XLog 부하 생성기

고정 원본 Scouter 2.21.3(commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`)의 Java Agent 송신 계층
`DataProxy`, `UDPDataSendThread`, `net/DataUdpAgent`와 `scouter.common` 전체를 **수정 없이** 임시 디렉터리에서
컴파일해, 실제 Agent와 같은 경로·묶음·분할 규칙으로 XLog를 보낸다. instrumentation(javaagent)은 쓰지 않고
`TraceMain.endHttpService`가 하는 호출 순서를 그대로 따른다.

```sh
export SCOUTER_SOURCE=/path/to/pinned/scouter
make build    # dist/scouter-go

# 임시 Go 서버(loopback, 임시 DB)에 보내고 종료 stats와 송신 수를 대조
python3 tools/java-loadgen/run.py --server dist/scouter-go \
  --rate 1000 --duration 10 --objects 10 --threads 4 \
  --profile-ratio 0.2 --profile-sizes 2048,16384 --seed 7 --jvm-cpus 2 \
  --txid-file /tmp/txids.txt --report /tmp/loadgen.json

# 이미 떠 있는 서버(127.0.0.1만 허용)에 보내기: 서버 쪽 대조는 호출자가 한다
python3 tools/java-loadgen/run.py --target 127.0.0.1:16100 --rate 500 --duration 10
```

종료 코드: 0 성공, 3 송신 실패 XLog가 있거나 수집 queue가 남음, 4 `--server` 대조 불일치 또는 서버 비정상 종료.

## 인자

| 인자 | 기본 | 의미 |
|---|---|---|
| `--target` / `--server` | 둘 중 하나 | 대상 UDP `127.0.0.1:port`(loopback 외 거부) / 임시 Go 서버 binary |
| `--objects` | 1 | Agent 객체 수(1..10000). `seq`마다 `(seq-1) % objects` 객체로 보낸다 |
| `--threads` | 1 | 송신 thread 수(1..256) |
| `--rate` | 100 | 전체 목표 XLog/s. thread마다 `threads/rate` 간격으로 균등 분배. `0`은 무제한 |
| `--duration` | 10 | 송신 시간(초, ≤3600) |
| `--profile-ratio` | 0 | profile을 붙일 XLog 비율(0..1) |
| `--profile-sizes` | 4096 | 쉼표 구분 profile byte 크기. `Step.toBytes` 길이가 이 값 이상이 될 때까지 `MethodStep`/`SqlStep3`를 번갈아 쌓은 template(크기마다 1개)을 쓴다 |
| `--services` | 100 | service 이름 수(1..500) |
| `--seed` | 1 | XLog 내용과 기본 `run_id`를 정한다 |
| `--run-id` | seed에서 결정 | txid 상위 32bit(1..2147483647) |
| `--set NAME=VALUE` | 없음 | `net_udp_packet_max_bytes`(512..60000), `net_udp_collection_interval_ms`, `profile_step_max_count`(≥128)만 변경 가능. 바뀐 값은 report `agent_conf_changed`에 남는다 |
| `--heap`, `--jvm-cpus` | 256m, 없음 | JVM `-Xmx`, `-XX:ActiveProcessorCount` |
| `--txid-file` | 없음 | 송신 성공한 txid를 한 줄에 하나씩(10진수) |
| `--report` | 없음 | 최종 JSON 저장(stdout에도 출력) |

## txid와 서버 쪽 대조

- `txid = (run_id << 32) | seq`, `seq = 1..txid.assigned`(연속). report의 `txid.first`/`txid.last`가 범위다.
  XLog datagram 송신이 실패한 seq만 `txid.failed_seqs`에 남으므로, 서버에 저장돼야 할 집합은 "범위 − failed_seqs"다.
- `gxid=0`, `endTime=0`(Agent도 넣지 않고 서버가 수신 시각을 넣는다), `discardType=DISCARD_NONE`, `xType=0`, `status=200`.
- XLog 내용(service, elapsed 1..1000ms, profile 여부·크기, sqlCount, userid)은 `seed`와 `seq`로만 결정된다(thread 배치와 무관).
- 객체 이름은 `/loadgen-<run_id>/agent-<NNNNN>`, objHash는 `HashUtil.hash(이름)`이다.
- 같은 DB에 같은 seed/run_id로 다시 보내면 txid가 겹친다. 반복 측정은 `--run-id`나 `--seed`를 바꾼다.

## 원본 Agent의 실제 송신 규칙(이 도구가 그대로 재현하는 것)

| pack | 원본 경로 | wire |
|---|---|---|
| `XLogPack` | `DataProxy.sendXLog` → `sendDirect` | XLog마다 단일 `CAFE` datagram. **묶음 전송하지 않는다** |
| `XLogProfilePack2` | `DataProxy.sendProfile(Step[], ctx)` → `profile_step_max_count`(1024) step 단위로 나눠 `sendDirect` | 단일 `CAFE`, pack이 `net_udp_packet_max_bytes`(60000)를 넘으면 `CAFM` 분할 |
| `TextPack` SERVICE/METHOD/HASH_MSG(thread 이름) | `UDPDataSendThread.add` | queue(1024)를 `net_udp_collection_interval_ms`(100ms)마다 비워 `CAFN` 묶음, 최대 크기 초과 시 분할 |
| `TextPack` SQL | `DataProxy.sendSqlText` → `sendDirect` | 단일 `CAFE` |
| `ObjectPack` | `DataProxy.sendHeartBeat` → `UDPDataSendThread` | `CAFN` 묶음. `AgentHeartBeat` `@Counter` 기본 2000ms마다 객체별 1개 |

- 텍스트 사전은 원본 `DataProxy`의 `IntLinkedSet`이 hash별로 처음 한 번만 보내고 이후 hash만 쓴다.
- 호출 순서는 `TraceMain.endHttpService`와 같다: `sendServiceName` → (`sendProfile`) → `sendHashedMessage(thread 이름)` → `sendXLog`.
  SERVICE text는 비동기 queue로, XLog는 즉시 나가므로 text가 XLog보다 늦게 도착할 수 있다(원본과 같음).
- 기본 profile template step은 작아서(약 25 byte) 1024 step 묶음이 60000 byte를 넘지 않는다. `CAFM` 분할을
  보려면 `--set net_udp_packet_max_bytes=8192`처럼 낮춘다(아래 smoke 참고).

## Seam과 원본 대비 차이

원본 Agent의 `Configure`·`Logger`·`TraceContext`는 javaagent 전체(ASM, conf reload thread, obj 탐지)에 묶여 있어
`seams/`로 대체한다. 송신 동작을 바꾸는 코드는 없다.

| seam | 대체 범위 |
|---|---|
| `scouter.agent.Configure` | 송신 계층이 읽는 필드만 둔다. 각 기본값은 `run.py`가 컴파일 전에 원본 `Configure.java`의 `getInt/getLong/getBoolean/getValue` 기본값과 문자열로 비교한다(불일치 시 중단). `obj_type`만 원본이 탐지값이라 `"java"`로 고정(XLog 경로에서는 읽지 않음) |
| `scouter.agent.Logger` | stderr 출력 |
| `scouter.agent.trace.TraceContext` | `DataProxy.sendProfile`이 읽고 쓰는 필드(이름·타입 동일)만 |

기본값에서 바꾼 Agent 설정(report `agent_conf_changed`):

- `net_collector_udp_port`: 대상 port. `net_collector_ip`: 대상 loopback 주소(기본값과 같은 127.0.0.1).
- `net_local_udp_ip=127.0.0.1`: 원본 기본은 null(모든 주소 bind). loopback만 쓰기 위해 고정.
- `--set`으로 바꾼 값.

의도한 차이와 한계:

- **여러 객체가 한 JVM을 공유한다.** 원본은 JVM당 객체 1개다. `Configure.getObjHash()`가 호출 thread에 묶인 objHash를
  돌려주게 해 XLog/Profile/`CAFM` header의 objHash는 객체별로 맞지만, UDP socket 1개와 텍스트 사전 1개를 공유한다.
  따라서 실제 N개 Agent보다 TEXT pack이 적고(같은 service text를 객체마다 보내지 않음) source port가 하나다.
- 송신 계측을 위해 `DataUdpAgent`의 private `DatagramSocket`을 reflection으로 `send()`만 세는 subclass로 바꾼다.
  wire byte와 호출 경로는 원본 그대로다. `send()` 예외는 원본처럼 삼켜지지만 계측에서 `send_errors`와
  XLog별 실패(`xlog.failed`, `txid.failed_seqs`)로 센다. 수신 측 kernel drop은 송신 측에서 보이지 않으며 `--server` 대조가 잡는다.
- PerfCounterPack/Alert/Summary/DroppedXLog/TCP Agent 연결은 보내지 않는다. `ObjectPack.version`은 원본 source
  resource의 미치환 값(`${project.version} ${build.datetime}`)이다.
- `--rate 0`(무제한) 경로는 이 smoke에서 실행하지 않았다. 이후 실행은 [xlog-max 기록](../../docs/validation/2026-10-17-xlog-max-benchmark.md) §4에 있다.

## 출력(JSON 주요 필드)

- `xlog.sent`/`failed`/`with_profile`/`sent_per_s`: **송신 측 XLog 수**. AGENTS.md의 TPS(저장·조회로 확인된 완료 XLog/s)가 아니다.
- `udp.datagrams`/`bytes`/`send_errors`, 종류별 `single_datagrams`(CAFE)·`multi_datagrams`/`multi_packs`(CAFN)·`mtu_fragments`/`mtu_packs`(CAFM),
  `packs_by_type`, `expected_server_udp_packets`(= CAFE + CAFN pack + CAFM 조립 pack, Go 서버 `udp.Packets`와 비교 대상).
  datagram/byte 수는 heartbeat·text를 포함한 전체이고 `send_phase_*_per_s`는 송신 구간 증가분이다.
- `cpu.send_phase_process_cpu_s`/`send_phase_cpu_pct_of_one_core`: 송신 구간 JVM process CPU(JIT·GC 포함), `available_processors`.
- `agent_conf`(실제 사용값), `agent_conf_changed`, `build`(원본 commit, 원본 파일, seam, 검사한 기본값, 서버 binary sha256).
- `--server`일 때 `server_check`: 서버 종료 stats(`ingest`/`udp`/`tcp`)와 세 대조
  `ingest.XLogs == xlog.sent`, `udp.Datagrams == udp.datagrams`, `udp.Packets == udp.expected_server_udp_packets`.

## Smoke 기록(개발 확인용, 성능 측정 아님)

조건: Apple M1 Pro 공유 장비, JDK zulu 18.0.2(`--release 11` 컴파일, `-XX:ActiveProcessorCount=2 -Xmx256m`),
Go 서버 `dist/scouter-go`(`--udp/--tcp 127.0.0.1:0 --conf "" --db <임시>`, GOMAXPROCS 미지정), 2026-10-17.
같은 시각에 같은 장비에서 다른 replay 부하가 돌고 있었으므로 CPU 값은 참고용이다.

| 조건 | 송신 XLog | 서버 XLogs | datagrams(송신/서버) | packets(예상/서버) | 기타 |
|---|---|---|---|---|---|
| rate 1000, 10s, objects 10, threads 4, profile 0.2 × {2048,16384}, seed 7 | 10000 (failed 0, 1000.06/s) | 10000 | 12048 / 12048 | 12265 / 12265 | profile 2005, TEXT 200, OBJECT 60, send CPU 1.39s(한 core의 13.9%), txid-file 10000줄 |
| rate 200, 2s, objects 5, threads 2, profile 0.3 × 30000, `net_udp_packet_max_bytes=8192` | 400 | 400 | 952 / 952 | 825 / 825 | CAFM 조각 378, 조립 pack 126 |

두 실행 모두 서버·JVM이 exit 0으로 종료했고 남은 프로세스가 없었다.

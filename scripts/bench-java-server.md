# Java Scouter 기준치 실행기

`bench-java-server.py`는 Python 3 표준 라이브러리, JDK(`java`/`javac`), `lsof`, `ps`를 사용한다.
배포 경로는 `--source` 또는 `SCOUTER_SERVER_DIST`(원본 배포본의 `server/` 디렉터리)로 지정한다.
기본 JDK는 macOS 시스템 JDK 디렉터리의 `zulu-8.jdk`다. 다른 설치는 `--java-home <JDK>`로 지정한다.
원본의 boot JAR와 `lib/`만 새 임시 디렉터리로 복사하며 원본에는 쓰지 않는다.
기존 DB·계정·conf·plugin 스크립트·로그는 복사하지 않는다. 빈 conf에서 기본 `admin/admin`을 생성한다.
복사된 `*pulse*.jar`를 제거하고 `ext_plugin_pulse_enable=false`, 빈 `plugin/`을 적용한다.

## 실행·종료·정리

무부하 smoke는 LOGIN 검증 직후 SIGTERM 종료와 임시 디렉터리 삭제까지 수행한다.
ready 파일은 **존재하지 않는 경로**를 사용하며, 종료 후에도 증거 JSON을 남긴다.

```sh
python3 scripts/bench-java-server.py run --smoke \
  --ready-file /tmp/java-smoke-01.json
```

기준치 실행은 foreground supervisor를 백그라운드로 띄워 관리한다.
`run`은 TCP 연결 성공만으로 준비 상태를 선언하지 않는다. 동봉 JAR의
`DataInputX/DataOutputX/MapPack`을 쓰는 임시 Java 클라이언트를 컴파일하고,
TCP magic `0xcafe2001`, `LOGIN`, SHA-256 암호, nonzero session, 응답 종료 flag를 검증한다.
세션 키/암호 hash는 출력하지 않는다. `lsof -nP -a -p <PID> -i`로 해당 Java PID의
TCP LISTEN과 UDP loopback bind를 확인한 후 JSON의 `state=ready`, `login=LOGIN_OK`를 기록한다.

```sh
python3 scripts/bench-java-server.py run \
  --udp-port 16100 --tcp-port 16100 \
  --jvm-arg=-Xmx512m --compress-xlog --compress-profile \
  --keep-dir --ready-file /tmp/java-bench-01.json > /tmp/java-bench-01.stdout 2>&1 &
JAVA_SUPERVISOR=$!

# 준비 대기: starting에서는 진행하지 않으며 stopped/timeout은 실패다.
python3 - <<'PY'
import json, time
from pathlib import Path
p = Path('/tmp/java-bench-01.json')
for _ in range(150):
    try:
        d = json.loads(p.read_text())
    except (FileNotFoundError, ValueError):
        d = {}
    if d.get('state') == 'ready':
        print('Java PID:', d['pid'], 'work:', d['work_dir'])
        break
    if d.get('state') == 'stopped':
        raise SystemExit('Java startup failed; inspect stdout/ready JSON')
    time.sleep(0.1)
else:
    raise SystemExit('Java readiness timeout; stop supervisor')
PY

# 측정 후, 실패한 측정에서도 반드시 실행한다.
python3 scripts/bench-java-server.py stop --ready-file /tmp/java-bench-01.json
wait "$JAVA_SUPERVISOR"
# --keep-dir일 때 필요한 DB/로그를 별도 보존한 후 정리한다.
python3 scripts/bench-java-server.py cleanup --ready-file /tmp/java-bench-01.json
```

`stop`은 ready JSON의 PID와 `ps`의 고유 `-Dbench.java.workdir=...`를 대조한 후
SIGTERM을 보낸다. supervisor는 최대 `--stop-timeout`(기본 5초) 대기하고 불가피하면
SIGKILL 후 reap하며, 이 경우 exit 1과 `forced_kill=true`를 남긴다. 정상 drain으로 해석하지 않는다.
`run`에 SIGINT/SIGTERM을 보내도 같은 정리가 수행된다. `cleanup`은 종료 확인 전 삭제를 거절한다.
SIGKILL로 supervisor 자체를 죽이면 정리가 보장되지 않으므로 이 방법은 사용하지 않는다.
기본 종료는 DB/로그를 삭제한다. `--keep-dir`은 실패 조사·저장 검증용이며 정리는 별도 명령이다.
Scouter 기본 포트 6100/6180은 거절하고, 모든 서버 bind는 127.0.0.1이다. HTTP와 purge는 끈다.
기동 전 TCP/UDP 포트 점유를 확인하며 포트 충돌은 실패한다.

기본 JVM 옵션 `-Xmx512m`은 원본 `startup.sh` 값이다. 해당 스크립트의 Java 절대 경로는
사용하지 않고 `--java-home`으로 선택한다. `--jvm-arg`는 반복 가능하며 하나라도 지정하면
기본 JVM 옵션 전체를 대체한다. 예: `--jvm-arg=-Xmx256m --jvm-arg=-XX:ActiveProcessorCount=2`.
`JAVA_TOOL_OPTIONS`, `_JAVA_OPTIONS`, `JDK_JAVA_OPTIONS`, `CLASSPATH`는 자식에서 제거한다.
JVM 옵션에 agent/네트워크 서비스를 추가하지 않는다. timezone은 Asia/Seoul로 고정한다.
압축은 기본 false이며 `--compress-xlog`, `--compress-profile`로 각각 활성화한다.

## 원본 버전과 누락 원인 분류

소스 근거는 고정 원본 `SCOUTER_SOURCE`,
commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`(2.21.3)이다.
2026-10-17 smoke에서 사용한 배포본은 실제 기동 banner와 JAR 파일명 모두 2.20.0이었다.
사용자가 지정한 배포본의 버전은 매 실행 기록으로 확인하며, 아래 소스 설명을 다른 버전 배포 바이너리의 모든 동작에 대한 증명으로 취급하면 안 된다.
공정한 버전 일치 비교가 필요하면 동일 source revision 배포본을 준비하고
`--source`로 지정해야 한다. ready JSON은 복사본 모든 JAR의 SHA-256, Java 버전,
실제 JVM 명령, 설정, 제거된 plugin, banner와 listener를 기록한다.

| 단계 | 용량 / 설정 | 포화·실패 근거 |
|---|---|---|
| OS UDP 수신 버퍼 | `net_udp_so_rcvbuf_size=4194304` 요청 (`--udp-rcvbuf` 변경 가능) | `DataUdpServer.open`의 `setReceiveBufferSize`; 커널의 실제 적용 크기는 별도 측정 필요 |
| datagram 배열 | `net_udp_packet_buffer_size=65535` | `DataUdpServer.recv`의 `DatagramPacket`; 이 크기는 SO_RCVBUF가 아님 |
| UDP dispatch | `_net_udp_worker_thread_count=3`, `RequestQueue[NetData](2048)` | `NetDataProcessor.add`: `S158 overflow recv queue!!` |
| XLog 전처리 / core / DB writer | 각 `xlog_queue_size=10000` | `XLogPreCore`: S116-0, `XLogCore`: S116, `XLogWR`: S144, 문구 `queue exceeded!!` |
| Profile 전처리 / core / DB writer | 각 `profile_queue_size=1000` | `ProfilePreCore`: S110-0/S110-0-1/S110-0-2, `ProfileCore`: S110, `XLogProfileWR`: S142, 문구 `queue exceeded!!` |
| 지연 XLog/Profile recovery | 각 queue 5 (요소는 묶음 map) | `XLogDelayingsRecoverCore`: S116-1, `ProfileDelayingsRecoverCore`: S110-1 |
| Text / counter core | `CoreRun.MAX_QUE_SIZE=10000` | `TextCore`: S115, `PerfCountCore`: S109, 문구 `queue exceeded!!` |
| Text DB writer | `DBCtr.LARGE_MAX_QUE_SIZE=100000` | `TextWR`: S140 `queue exceeded!!` |

경로 기준: Java 설정은 `scouter.server/src/main/java/scouter/server/Configure.java`,
Scala 클래스는 `scouter.server/src/main/scala/scouter/server/{netio/data,core,db}/` 아래에 있다.
`scouter.common/src/main/java/scouter/util/RequestQueue.java`의 `put`/`putNotifySingle`은
큐가 가득 차면 **새 요소를 넣지 않고 false를 반환**한다. 해당 caller는 로그만 남기며 재시도하지 않는다.
UDP queue drop은 datagram 단위이므로 CAFN 묶음 하나에서 여러 XLog가 누락될 수 있다.
CAFM 조각 누락은 조립 실패로 이어질 수 있다.

UDP decode 실패는 `S159 invalid data`, UDP receive 예외는 S151, bind 예외는 S157이다.
분할 패킷 만료는 S150 `MultiPacket total=... recv=...`로 분류한다.
`Logger.println(id, 10, ...)`는 로그를 제한하므로 로그 줄 수는 drop 개수가 아니다.
커널 UDP drop은 Java 큐 로그 없이 발생할 수 있다. 송신 실패/미전송, 커널 drop,
dispatch 포화, parse/fragment 실패, core 포화, writer 포화/저장 오류, 조회 미확인을 구분한다.
로그가 없다는 이유로 누락 원인을 확정하지 않는다.

## 외부 서버 모드 연결 예시

`scouter-replay --server-udp/--server-tcp`는 이미 실행 중인 loopback 서버로 송신하고 TCP 조회로 XLog를 확인한다. 두 주소를 함께 지정해야 하며, 선택 인자 `--server-pid`는 CPU/RSS 표본 수집에만 쓴다. 서버를 종료·재기동하지 않으며 DB·압축·Go runtime·`--server-arg` 설정을 받지 않는다.
Java index 반영을 기다려야 하면 `--verify-delay <duration>`을 지정한다. 이 대기는 quiescence 뒤 조회 전에 적용되며 처리량 denominator에는 포함되지 않으므로 값과 검증 시점을 함께 보고한다.

```sh
JAVA_PID=$(python3 -c 'import json; print(json.load(open("/tmp/java-bench-01.json"))["pid"])')
./dist/scouter-replay \
  --server-udp=127.0.0.1:16100 --server-tcp=127.0.0.1:16100 --server-pid="$JAVA_PID" \
  --restart=false --rate=200 --duration=5s --warmup=0s \
  --batch=32 --profile-ratio=0.3 --profile-sizes=1024:1 \
  --objects=2 --query-concurrency=1 --seed=7 \
  --report=/tmp/java-replay-01.json
python3 scripts/bench-java-server.py stop --ready-file /tmp/java-bench-01.json
```

외부 모드 처리량은 **quiescence 뒤 TCP 조회로 확인한 고유 XLog 수 / 송신 시작부터 quiescence까지의 시간**이다. 재기동 후 영속성·서버 reject/backlog·종료 계수·디스크 상태는 검증하지 않는다. 영속 저장을 포함한 acceptance TPS는 별도 재기동·저장 검증이 필요하다. 송신 rate, packet/s, byte/s,
profile/s는 별도 지표다. Java PID RSS/CPU, payload mix, 압축, queue/buffer 설정,
측정/검증 시간 구간, disk/fsync 정책, 공유 장비 경합을 기록해야 한다.
`GOMAXPROCS`/`GOMEMLIMIT`는 Java에 적용되지 않는다. JDK heap limit도 RSS hard cap이 아니다.
서버가 시작됐다는 smoke 결과만으로 ingest 성능·저장 호환성·운영 적합성을 선언하지 않는다.

## 개발 smoke 증거 (2026-10-17, 로컬 macOS)

- 기본 smoke 명령: `python3 scripts/bench-java-server.py run --smoke --ready-file <report.json>` → exit 0, `LOGIN_OK`, TCP/UDP `127.0.0.1:16100`, SIGTERM 후 `pid_exited=true`, `forced_kill=false`, `cleaned=true`.
- 분리 종료 검증: `run --ready-file <report.json> --keep-dir --compress-xlog --compress-profile --jvm-arg=-Xmx256m` → `LOGIN_OK`; 실행 중 `cleanup` 거절, `stop` exit 0, supervisor exit 0, 후속 `cleanup` exit 0 및 디렉터리 부재. 전체 2.7초, 부하 전송 없음.
- TCP 16100을 먼저 점유한 뒤 `run --smoke` → exit 1 (`Address already in use`), PID 0으로 Java 미기동, 임시 디렉터리 정리 확인.
- `run --udp-port 6100` → argparse exit 2. `ast.parse` 구문 검사 및 `git diff --check` 통과.
- JDK Zulu 8.64.0.19 / `1.8.0_345`, 실제 server banner `Scouter version 2.20.0`.
- `make check/race/fuzz/bench/fixtures-check/inventory-check` 미실행: Go 구현이나 fixture 변경 없이 실행기 두 파일만 추가한 범위이므로 해당 Java 프로세스 lifecycle 검증으로 제한했다. 외부 replay 통합, 저장·조회 완료 TPS, 강제 종료 fallback, JDK 17/18은 미검증이다.

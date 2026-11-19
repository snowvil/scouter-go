# 격리 호스트 최대 성능 측정 절차

공유 장비(다른 프로세스가 CPU를 쓰는 노트북)에서 잰 최대치는 회차마다 10~20% 움직였다(`2026-10-17-xlog-max-benchmark.md` §16). 운영 판단에 쓸 최대치는 다른 서비스가 없는 격리 Linux 호스트에서 잰다. 이 문서는 그 절차다. 호스트 사양과 결과 원본은 저장소에 넣지 않는다. 결과를 옮길 때는 측정 조건과 수치만 적는다.

## 원칙

- **격리**: 다른 서비스나 운영 트래픽이 없는 호스트에서만 잰다. 운영 서버와 같은 호스트에서는 돌리지 않는다.
- **loopback 전용**: 부하 생성기(`scouter-replay`)는 loopback 서버만 대상으로 한다. 서버도 생성기가 직접 띄운다. 네트워크로 부하를 보내지 않는다.
- **CPU 분리**: 생성기와 서버가 같은 호스트에서 돌므로, Linux에서는 `taskset`으로 CPU를 나눈다. 서버 쪽 CPU 수가 측정 조건의 핵심이다.
- **시스템 설정은 바꾸지 않는다**: 스크립트는 `net.core.rmem_max` 같은 커널 값을 기록만 한다. 서버는 원본 Java와 같은 UDP 수신 버퍼 4MiB를 요청하고, 커널 상한이 작으면 그 값으로 줄어든다(Java도 같다). 상한을 올려 보는 것은 운영자 판단이며, 올렸다면 조건에 함께 적는다.
- **반복**: 시나리오마다 최소 3회 재고 중앙값을 쓴다. 측정 중에는 다른 작업(빌드, 테스트, 다른 측정)을 돌리지 않는다.

## 준비

```sh
make bench-bundle LINUX_ARCH=amd64      # dist/bench-bundle-linux-amd64.tar.gz
```

묶음에는 `scouter-go`, `scouter-replay`, `bench-xlog-max.py`, `bench-isolated.sh`가 들어 있다. 호스트에는 `python3`와 `taskset`(util-linux)만 있으면 된다. 디스크는 측정 디렉터리 기준 수십 GB 여유를 둔다(시도마다 새 DB를 만든다).

## 실행

```sh
tar xzf bench-bundle-linux-amd64.tar.gz && cd bench-bundle-linux-amd64
./bench-isolated.sh --smoke --out ./r-smoke                                  # 도구 확인(수치 무의미)
./bench-isolated.sh --out ./r1 --server-cpus 0-3 --load-cpus 4-7             # 본 측정, 약 1~2시간
```

`--server-cpus`와 `--load-cpus`는 호스트 CPU를 둘로 나눈 목록이다. 서버 CPU 수를 바꿔 가며(예: 2, 4, 8) 재면, CPU 수에 따른 확장성을 볼 수 있다.

## 결과 읽기

`r1/summary.txt`에 시나리오마다 한 줄씩 나온다.

| 시나리오 | 내용 |
|---|---|
| `mix-text095` | XLog의 30%에 profile(0~128KiB), text 재사용률 95% |
| `xlog-text095` | XLog만, text 재사용률 95% |
| `xlog-text100` | XLog만, text 고정 |
| `mix-text100` | profile 섞음, text 고정 |
| `storm-N` | 신규 text 100%, 20,000 XLog/s 고정. XLog 손실은 0이어야 한다. tag는 버릴 수 있다(보조 기능, 셈) |

- 최대치는 "완료된 XLog/s"다. 저장 후 다시 조회해 확인한 수이며, packet/s가 아니다.
- 실패 사유는 다음과 같다.
  - `ingest_backlog_growth`: 수집 queue가 계속 늘어 지속 불가.
  - `server_udp_reject`: 앱 queue가 넘침.
  - `kernel_udp_loss`: 커널 소켓 버퍼 손실.
- 커널 손실이 사유라면 수신 버퍼 상한(`conditions.txt`의 `net.core.rmem_max`)과 함께 해석한다.
- `conditions.txt`에는 측정 조건을 남긴다. 커널, CPU 수, CPU 분리, 측정 전후 load, 커널 소켓 버퍼 상한, 서버 바이너리 해시다.

## Java와 비교(선택)

같은 호스트에서 원본 서버를 비교하려면 `scripts/bench-java-server.py`로 격리된 Java 서버를 띄우고, `bench-xlog-max.py --kind external`로 같은 시나리오를 잰다(`2026-10-17-xlog-max-benchmark.md` §8). Java 서버도 원본 배포본의 같은 설정으로 띄우고 조건을 함께 적는다.

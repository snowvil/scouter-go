# 개발과 릴리즈

규칙의 정본은 [AGENTS.md](AGENTS.md)다. 이 문서는 branch, 도구, 검증, commit 절차를 정리한다.

## Branch와 commit

- 기본 branch는 `master`이고, 작업은 `master`에서 만든 `codex/<topic>` branch에서 한다. 읽기 전용 Scouter/Pulse 원본의 branch 정책(develop 등)을 이 저장소에 적용하지 않는다.
- 호환성 단위마다 코드·fixture·계약 문서·검증 기록을 함께 갱신하고, 검증이 끝나면 바로 작은 commit을 만든다.
- commit 제목은 기존 이력처럼 `영역: 바뀐 동작` 형식의 영어 한 줄로 쓴다(예: `server: fix a /health race with Close`, `docs: stage 14 results`). 영역은 패키지나 `docs`, `tools`, `scripts`, `cmd`다.
- 원격 push, PR, merge, release, 배포는 별도로 요청받은 범위에서만 한다.
- 공개 저장소다. 넣지 않는 내용은 [AGENTS.md](AGENTS.md)의 "공개 저장소 규칙"을 따르고, commit 전에 `make public-check`를 실행한다(아래 "공개 전 검사").
- commit 작성자 email은 회사 도메인이 아닌 주소를 쓴다. GitHub noreply 주소는 `git config user.email "<id>+<login>@users.noreply.github.com"`으로 이 저장소에만 설정할 수 있다.

## 도구

| 도구 | 용도 | 준비 |
|---|---|---|
| Go 1.27.2 | 빌드·테스트 | `python3 scripts/bootstrap_go.py`(Python 3.12+, curl). 공식 archive의 SHA-256을 확인해 저장소 내부 `.tools/go`에 설치하며 시스템 설정과 shell profile은 바꾸지 않는다. `Makefile`은 `.tools/go/bin/go`가 있으면 그것을, 없으면 `PATH`의 `go`를 쓴다(`GOTOOLCHAIN=local`) |
| Python 3 | oracle·상호운용·측정 script | 시스템 Python |
| JDK 11+ | Java oracle fixture 생성, 원본 Client 통신 클래스 컴파일 | `JAVA_HOME` 또는 `PATH` |
| JDK 8 | 원본 Scala 2.10 compiler 실행(`storage-check`), Telegraf JAXB oracle, 실제 Agent 샘플 JVM | Scala는 `SCOUTER_SCALA_JAVA=<jdk8>/bin/java`(macOS는 설치된 JDK 8을 찾는다). JAXB oracle은 `JAVA8_HOME=<jdk8>`와 `SCOUTER_SOURCE`를 모두 지정해야 실행된다 |
| 고정 Scouter 원본 | Java oracle | 기본 `../scouter`, `SCOUTER_SOURCE`로 변경. commit은 [baseline.json](docs/compatibility/baseline.json) |
| Pulse plugin 원본 | Pulse golden의 출처 | 비공개 원본이며 golden 생성 도구도 이 저장소에 없다. 커밋된 golden은 고정 자료이고, `go test ./internal/pulse/`가 Go 구현을 그 golden과 대조한다 |

원본 저장소는 수정하지 않는다. fixture 생성과 javac 출력은 임시 디렉터리를 쓴다.

## 검증

변경 범위에 맞는 명령을 실행하고 실제 pass/fail을 기록한다. 도구나 원본이 없어 skip된 검사는 통과가 아니며 미검증으로 적는다.

```sh
make check              # gofmt, vet, 전체 test, build (모든 변경)
make race               # 동시성·종료 경로를 건드린 변경
make fuzz               # codec·framing·parser 변경 (target별 FUZZTIME, 기본 10s)
make bench              # codec microbenchmark (서버 TPS 아님)
make storage-check      # 저장 형식 변경: Java/Scala DB 양방향
make fixtures-check     # codec 변경: Java oracle fixture 재생성 비교
make inventory-check    # 원본 handler inventory 비교
make java-integration   # 원본 Client 통신 클래스로 56개 명령 통합
```

기능별 oracle은 `make management-oracle-check`, `geoip-oracle-check`, `ingest-oracle-check`, `sqltable-oracle-check`, `logger-oracle-check`다. 커밋된 golden과 Go 결과의 대조는 `make check`에 들어 있고, 이 명령들은 golden을 원본에서 다시 만들어 비교한다. Pulse golden은 비공개 원본에서 만든 것이라 이 저장소에서 다시 만들 수 없다.

`make check` 성공이 opt-in 외부 검증까지 모두 실행했다는 뜻은 아니다. Telegraf JAXB는 위 환경변수가 없으면, Kafka Java codec·실제 broker 테스트는 `SCOUTER_KAFKA_JAVA_CP`·`SCOUTER_KAFKA_BOOTSTRAP` 등 필요한 설정이 없으면 skip된다. 해당 기능을 검증할 때는 테스트의 요구 환경을 준비하고 verbose 결과에서 skip 여부를 확인한다.

Agent 통신 경로(transport, Agent relay, text reset, 서버 발신 명령)를 바꾸면 실제 Agent 상호운용을 실행한다. 작업 tree가 아니라 `git archive`로 푼 지정 revision을 빌드하므로 미커밋 변경은 들어가지 않는다. 사용법과 시나리오는 [harness README](tools/java-agent-interop/README.md)에 있다.

```sh
SCOUTER_SOURCE=../scouter python3 scripts/java-agent-interop.py --report <dir>/jdk8.json
SCOUTER_SOURCE=../scouter python3 scripts/java-agent-interop.py --agent-java-home <JDK11> \
  --agent-jvm-opt=-Djdk.attach.allowAttachSelf=true --report <dir>/jdk11.json
```

성능 변경은 `scripts/bench-ab.sh`로 이전 빌드와 쌍 비교한다. 같은 빌드끼리도 흔들리므로 단일 실행이나 단일 profile은 근거로 쓰지 않는다([성능 기준](docs/performance.md)). 부하는 격리된 환경에서만 만든다.

GitHub Actions([ci.yml](.github/workflows/ci.yml))는 `master`·`codex/**` push와 PR에서 `check`, `race`, `fuzz`(5초), `storage-check`, `java-integration`, `inventory-check`, `fixtures-check`를 실행한다.

source SHA가 바뀌면 `docs/compatibility/baseline.json`, 계약 diff, 재생성 fixture를 함께 검토한다. 정상·경계·malformed 입력과 성능 측정을 분리한다.

## 공개 전 검사

```sh
make public-check       # 추적 파일 전체 검사
scripts/public-check.sh --staged   # staged 내용만 검사(pre-commit hook용)
```

검사는 두 부분이다.

- 저장소에 든 일반 규칙: 로컬 절대 경로, private key와 알려진 token 형식, 허용 도메인 밖의 email, 추적된 설정·key·archive 파일.
- 로컬 deny list: 저장소 root의 `.public-denylist`(git이 무시한다) 또는 `PUBLIC_DENYLIST`가 가리키는 파일에 확장 정규식을 한 줄에 하나씩 적는다. 조직·서비스·호스트 이름, 내부 주소 대역, 계정 이름, 비공개 저장소 이름처럼 저장소에 적으면 그 자체로 공개되는 식별자를 여기에 둔다. 같은 정규식을 commit 작성자·committer에도 적용한다.

deny list가 없으면 일반 규칙만 돌고 그 사실을 알린다. 검사는 문장을 읽어야 드러나는 내용(환경의 규모, 사건 경위)을 잡지 못하므로, 문서를 고칠 때는 규칙을 직접 대조한다. pre-commit hook으로 쓰려면 `.git/hooks/pre-commit`에서 `scripts/public-check.sh --staged`를 호출한다.

보안 취약점은 공개 issue로 올리지 않는다. [SECURITY.md](SECURITY.md)를 따른다.

## 버전과 release gate

Go 제품 버전은 Scouter wire/source 기준 버전과 별도로 관리한다. 아직 배포 가능한 대체 서버 버전을 발행하지 않았다. 구현되지 않은 기능을 version 문자열로 지원하는 것처럼 표시하지 않는다.

릴리즈 전 필수 조건:

- 호환성 목록의 필수 항목 구현과 Java golden·실제 Agent·Client 검증
- 운영 DB 복사본의 조회·이관·복원 검증과 rollback 절차
- malformed 입력, 인증·권한, race, 종료·재시작·queue 포화 검증
- 합의된 자원 한도 안에서 지속 부하와 조회 병행 검증
- Pulse Kafka·Collector payload, drop·ordering, UUID 경계 검증
- 빌드 SHA·Go 버전·설정·OS·CPU·메모리·디스크 조건과 checksum, 변경사항 기록

현재 충족 상태는 [구현 순서](docs/implementation-plan.md)와 [README](README.md) "남은 gate"를 따른다.

새 외부 dependency는 용도, 라이선스, 운영 비용을 기록한다. 원본에서 가져온 코드의 저작권 고지와 출처를 유지한다.

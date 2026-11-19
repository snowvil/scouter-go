# Scouter Go 개발 시작 안내

Scouter Java/Scala 서버를 단일 Go 실행 파일로 대체하는 프로젝트다. 현재 구현 범위와 남은 운영 전환 gate를 먼저 확인하고, 수정할 경계의 계약·테스트를 읽은 뒤 작업한다.

## 읽을 순서

1. [README](README.md)의 현재 상태·Java 서버와의 차이·남은 gate와 [문서 색인](docs/README.md)을 읽는다.
2. [AGENTS.md](AGENTS.md)와 [CONTRIBUTING.md](CONTRIBUTING.md)에서 코드 경계, branch·commit·검증·릴리즈 규칙을 확인한다.
3. [구현 순서](docs/implementation-plan.md)에서 단계별 상태를 확인하고, 담당 `internal/*/CONTRACT.md`와 `docs/compatibility/` 문서를 읽는다.
4. [최근 소스·문서 점검](docs/validation/2026-11-07-source-docs-sync.md)과 관련 검증 기록을 확인한다. 날짜가 있는 개선·측정 기록은 해당 revision의 증거이며 현재 소스의 재검증 결과와 구분한다.

## 로컬 환경

필요한 도구는 Go 1.27.2, Python 3, Java oracle용 JDK 11+다. 원본 Scala compiler와 실제 Agent 샘플에는 JDK 8이 필요하다. 버전과 원본 경로 설정은 [CONTRIBUTING.md](CONTRIBUTING.md)의 도구 표를 따른다.

```sh
python3 scripts/bootstrap_go.py   # Go가 없을 때만; Python 3.12+, curl 필요
make check                       # gofmt, vet, 전체 test, build
```

고정 Scouter 원본은 기본 `../scouter`이며 다른 위치라면 `SCOUTER_SOURCE`를 지정한다. 별도 worktree는 sibling 경로가 달라질 수 있으므로 원본 경로를 먼저 확인한다. 원본 저장소는 읽기 전용이다.

운영 흐름은 [전환 runbook](docs/operations/cutover-runbook.md), [설정 산정](docs/operations/sizing.md), [감시 기준](docs/operations/monitoring.md)을 읽고 익힌다. 실행 예시는 README를 따르며, 임시 DB·loopback listener로 기존 서비스와 분리한다.

## 작업과 검증

- 작업 branch는 `master`에서 만든 `codex/<topic>`이다. 변경 전에 `git status --short --branch`와 `git worktree list`로 진행 중인 작업을 확인한다.
- 요구사항·정본 소스·기존 테스트 확인 → 경계와 계약 설계 → 작은 구현 → 검증 → 개선·운영 영향 검토 순서로 진행한다. 호환성 변경은 Java oracle 또는 재현 테스트를 먼저 마련한다.
- 모든 변경은 `make check`로 확인한다. 동시성은 `make race`, codec·parser는 `make fuzz`·`make fixtures-check`, 저장은 `make storage-check`, Client 명령은 `make java-integration`, Agent 통신 경로는 실제 [Agent 상호운용 harness](tools/java-agent-interop/README.md)를 더한다.
- 도구·원본 부재로 skip된 검사는 통과로 기록하지 않는다. 실행 명령·결과·가정·남은 gate를 함께 기록한다.
- 검증한 단위마다 즉시 작은 commit을 만든다. push·merge·release·배포는 별도 요청 범위에서만 수행한다.
- 공개 저장소다. [AGENTS.md](AGENTS.md)의 "공개 저장소 규칙"을 읽고, commit 전에 `make public-check`를 실행한다. 실제 서버 접근과 부하는 별도 요청 범위에서만 수행한다.

## 역할을 나눌 때

리더가 필요한 독립 작업만 나누고 파일 소유권·계약·검증 결과를 통합한다. 같은 파일을 여러 역할이 동시에 수정하지 않는다.

Claude Code의 역할 구성은 [.claude/TEAM.md](.claude/TEAM.md)와 `.claude/agents/`에 있다. Java 원본 분석, codec·transport, ingest·runtime, storage, service, Pulse, 호환성 검증, 독립 리뷰로 나뉜다. 단순 수정은 메인 세션에서 처리한다.

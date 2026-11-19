# 초기 설정과 첫 codec 검증

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

실행일: 2026-10-13. 작업 branch: `codex/bootstrap-server-port`.
이번 단계의 완료 범위는 개발 기반과 순수 codec이다. 전체 Scouter 서버 대체 완료 보고가 아니다.

## 변경과 근거

- AGENTS/CONTRIBUTING, Go 1.27.2 local bootstrap, Makefile, GitHub Actions workflow를 구성했다.
- Java/Scala·Pulse plugin·참고 Go source SHA와 프로젝트 요구사항을 baseline에 고정했다.
- Java/Scala source에서 229개 고유 handler/34개 handler 파일/16개 Pack ID를 결정적으로 추출했다. command 구현 상태는 모두 초기 `not_implemented`다.
- wire/서버/저장·이관/Pulse 계약, 단계별 계획, 공유 서버의 자원·부하 검증 기준을 작성했다.
- 공통 primitive, Value 17종, Pack 7종(Map/Object/PerfCounter/Text/XLog/Profile V1/V2)을 독립 구현했다.
- Java 원본 common을 임시 compile하여 synthetic fixture 54개를 생성했다. 각 fixture의 byte 길이/SHA-256, 기대 필드, Go 재인코딩 byte 일치를 검사한다.
- XLog append version 0–12와 최신 이후 unknown tail, 잘린 입력, count/depth/size/overflow, 입력 buffer 재사용 후 결과 소유권을 검증했다.

## 환경과 실행 결과

로컬 검증 환경: Apple M1 Pro, Darwin/arm64, Go 1.27.2, JDK 18.0.2(`javac --release 11`), Python 3.14.4. Go 검증은 `GOMAXPROCS=2`다. 이는 목표 서버의 성능 측정 환경이 아니다.

| 실행 명령 | 결과 | 범위 |
| --- | --- | --- |
| `python3 scripts/bootstrap_go.py` | PASS | 공식 SHA-256 확인 후 `.tools/go` 신규 설치, 재실행도 같은 버전 확인 |
| `make check` | PASS | gofmt, `go vet ./...`, `go test -count=1 ./...`, `go build ./...` |
| `make race` | PASS | `go test -race -count=1 ./...` |
| `make fixtures-check` | PASS | 실제 javac/Java oracle 54개 재생성 byte 비교 |
| `make inventory-check` | PASS | 고정 source SHA/version/clean 및 생성 JSON 일치 |
| `make fuzz` | PASS | Pack 10초 716,573회, Value 10초 229,503회; target별 2 worker |
| `make bench` | PASS | 아래 synthetic XLog codec microbenchmark |
| `git diff --check` 및 문서/JSON/Python 검사 | PASS | whitespace, Markdown local path, JSON parse, Python AST, baseline 개수/버전 |
| 없는 GOFMT를 지정한 `make fmt-check` | PASS | 기대한 실패 exit 2. 실패를 성공으로 삼지 않음 |

fuzz 횟수는 실행마다 다르며 성능 지표가 아니다. 사용 가능한 seed corpus와 경계 검사를 제한 시간 동안 실행한 결과이지 모든 입력의 무결함 증명은 아니다.

153-byte synthetic XLog, `-benchtime=1s`, 단일 실행 baseline:

| benchmark | ns/op | B/op | allocs/op |
| --- | ---: | ---: | ---: |
| DecodeXLog | 450.9 | 592 | 10 |
| EncodeXLog | 739.5 | 1024 | 11 |

네트워크·queue·dictionary·인덱스·디스크·조회·Kafka가 없는 codec 비용이다. Java 대비 개선율이나 서버 TPS로 환산하지 않는다. 현재 구현은 owned copy를 사용하며 zero-copy/pool 최적화는 아직 적용하지 않았다. 2026-10-14 `123403f` 이후 수치(EncodeXLog 1 alloc 등)는 [권고 적용 기록](2026-10-14-recommendations.md) §2에 있다.

## 검증 중 수정한 문제

Makefile 실행 오류는 1회 수정으로 해결했다.

| 확인/분류 | 수정 | 재검증 |
| --- | --- | --- |
| `make fmt-check GOFMT=/nonexistent/scouter-review-gofmt`가 exit 0: implementation bug | recipe에 `set -eu`를 적용해 command substitution 실패를 전파 | 기대한 실패 exit 2 확인, 실제 gofmt 및 전체 check PASS |
| Java `writeText(null)` overload ambiguity와 MapValue insertion-order 가정: oracle/test expectation 오류 | null을 String으로 명시하고 Hashtable의 key별 의미 비교와 Go wire-order 비교를 분리 | javac 및 fixture 54개 재생성 PASS |

코드 작성 중 한 차례 `make check`가 아직 format하지 않은 `codec_test.go`에서 중단되었다. gofmt한 뒤 check/race/fixture/inventory/fuzz를 다시 실행하여 위 결과를 확보했다.

## 독립 조사와 검토

wire, 서버/저장, Pulse 계약, 참고 Go 구현의 네 범위를 분리해 조사했다. 조사 결과는 핵심 원본 코드와 대조했고, 그 뒤 최종 통합 검증을 실행했다. tooling 리뷰는 fmt-check 오류를 발견했으며, codec 별도 리뷰에서는 현재 지원 범위의 중요한 추가 오류를 발견하지 못했다.

참고 Go repo `8a869592`에서 `GOMAXPROCS=2 go test ./...`는 **FAIL**이었다. `internal/http`의 `TestCounterRealtimeEndpoint`, `TestCounterRealtimeEndpointFloatValue`가 expected 200/got 404로 실패했다. 같은 실패를 `go test ./internal/http -run TestCounterRealtimeEndpoint -count=1`로 다시 재현했다. 참고 소스는 수정하지 않았고 자세한 평가는 [reference-evaluation.md](../reference-evaluation.md)에 있다.

## 미완료와 남은 gate

- UDP/TCP listener·framing/reassembly·session/권한·실제 Java Agent/Eclipse Client: 미구현이므로 통합 검증 미실행.
- 나머지 9개 Pack, Step 해석, 229개 handler, core/cache/storage/index/retention/recovery: 미구현.
- 기존 DB reader/importer 및 rollback: 미구현. 원본 파일은 변경하지 않았다.
- Pulse 내장 producer/필터/JSON golden, Kafka/Collector 연계: 계약 조사만 완료, 구현·통합 검증 미실행.
- 목표 처리량, 자원 절감, 공유 서버 soak: 서버 파이프라인·실제 corpus·할당 CPU/RAM/I/O 예산이 없어 미실행.
- GitHub Actions: workflow 작성 및 로컬 대응 명령 통과. 원격 push/CI 실행은 이번 작업에 포함하지 않았다.
- malformed UTF-8/비표준 Decimal 폭/int32 overflow는 Java보다 엄격히 거절한다. [codec 계약](../../internal/protocol/CONTRACT.md)에 의도된 차이를 명시했다.
- 구형 XLog에서 미전송된 문자열은 Java null이다. 향후 JSON/조회는 `TailVersion`으로 presence를 유지해야 한다.
- Profile blob은 synthetic opaque byte다. Pack 경계 검증을 Step 또는 Pulse profile 호환성으로 해석하지 않는다.

다음 완료 단위는 미지원 Pack/Step 및 transport의 Java interop fixture와 제한된 수신 파이프라인이다. 상세 순서는 [implementation-plan.md](../implementation-plan.md)를 따른다.

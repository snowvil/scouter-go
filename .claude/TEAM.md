# Scouter Server Java → Go 포팅 팀 구성

리더(메인 세션)가 요구사항 분석·계획·파일 소유권 배정·통합·commit을 맡는다. 작업마다 필요한 역할만 생성하고, 끝나면 종료한다.

| 역할 (agent type) | 담당 경로 | 주 업무 |
|---|---|---|
| `scouter-java-analyst` | `docs/compatibility/*` | 원본 Java/Scala 계약 조사·문서화 |
| `scouter-protocol-dev` | `internal/protocol`, `internal/transport`, `internal/scoutertime` | Pack/Value/Step codec, UDP·TCP framing |
| `scouter-ingest-dev` | `internal/ingest`, `internal/server`, `internal/syncx`, `cmd/` | 수집 파이프라인, 서버 런타임·설정 |
| `scouter-storage-dev` | `internal/storage/**` | 원본 호환 파일 DB, index, TTL |
| `scouter-service-dev` | `internal/service` | Client TCP 조회 명령 handler |
| `scouter-pulse-dev` | `internal/pulse` | Pulse callback/filter/JSON/Kafka 계약 내장 (3단계) |
| `scouter-compat-tester` | 지정 `*_test.go`, `testdata`, `scripts/`, `docs/validation/*` | Java fixture, race·fuzz, 상호운용·부하 검증 |
| `scouter-port-reviewer` | (읽기 전용) | Java→Go 의미 차이·동시성·자원 상한 독립 리뷰 |

Pulse 통합(3단계)은 `scouter-pulse-dev`가 맡고, ingest 연결 지점은 `scouter-ingest-dev`와 리더가 조율한다.

## 진행 흐름
1. analyst가 계약 문서·fixture 후보를 정리한다 (계약이 이미 문서화된 경우 생략).
2. 리더가 파일 소유권을 배정하고 dev들이 병렬 구현한다. 같은 파일 동시 수정 금지. 패키지 경계 API 변경은 리더가 조율한다.
3. tester가 구현 결과로 fixture·회귀·race·fuzz·상호운용 검증을 실행한다.
4. reviewer가 독립 리뷰하고, 리더가 수정 배정 → 영향 검증 → `make check` 등 표준 명령 결과로 최종 판정한다.

## 작업별 권장 구성
- codec/transport 변경: protocol-dev + compat-tester + port-reviewer
- 새 Client 조회 명령군: analyst → service-dev (+ storage-dev) + compat-tester + port-reviewer
- 저장 형식·이관: analyst → storage-dev + compat-tester + port-reviewer
- Pulse 통합: analyst(fixture 계약) → pulse-dev (+ ingest-dev 연결) + compat-tester + port-reviewer
- 관리 명령 45개: analyst → service-dev (+ storage-dev) + compat-tester + port-reviewer
- 이관·운영 gate(DB 복사본, retention, crash 복구, 부하): storage-dev/ingest-dev + compat-tester + port-reviewer
- 단순 수정: 리더 직접 처리

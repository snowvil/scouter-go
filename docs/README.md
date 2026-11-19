# 문서 색인

문서가 서로 다르면 호환성의 정본은 고정 원본 소스([baseline.json](compatibility/baseline.json))다. Go의 현재 동작은 소스·테스트와 계약 문서(`compatibility/*`, `internal/*/CONTRACT.md`)를 함께 확인한다. 검증 기록(`validation/*`)은 실행한 revision·환경·범위의 증거이며, 날짜가 붙은 조사·측정 문서는 본문에 명시한 시점의 기록이다. 이후 절이 추가된 문서도 과거 결과를 현재 검증으로 해석하지 않는다.

최신 문서 점검은 2026-11-07이며, 실행한 검증과 남은 gate는 [2026-11-07 소스·문서 정합성 기록](validation/2026-11-07-source-docs-sync.md)에 정리했다.

## 처음 읽을 문서

| 문서 | 내용 |
|---|---|
| [README](../README.md) | 현재 상태, 빌드·실행·배포, 검증 명령 |
| [AGENTS.md](../AGENTS.md) | 구현·호환성·성능·검증·협업 규칙 |
| [CONTRIBUTING.md](../CONTRIBUTING.md) | branch, toolchain, Java oracle, commit, release gate |
| [ONBOARDING.md](../ONBOARDING.md) | 처음 참여할 때의 순서 |

## 설계와 단계

| 문서 | 내용 |
|---|---|
| [architecture.md](architecture.md) | 요구사항, 접근 방식, 컴포넌트, 공유 자원 모델, 저장 이관 |
| [implementation-plan.md](implementation-plan.md) | 단계별 산출물·완료 조건·현재 상태(정본) |
| [performance.md](performance.md) | TPS 정의, 계수, 비교 실험, 측정 도구 |
| [improvement-candidates-2026-10-21.md](improvement-candidates-2026-10-21.md) | 운영 전환 전 개선 대상과 1~14단계 진행 결과 |
| [performance-structural-review-2026-10-20.md](performance-structural-review-2026-10-20.md) | 구조적 성능 개선 검토와 측정 |
| [reference-evaluation.md](reference-evaluation.md), [2026-10-14 재평가](reference-evaluation-2026-10-14.md) | 참고 Go 구현 평가 |

## 사례 보고서

| 문서 | 내용 |
|---|---|
| [reports/java-to-go-cutover-performance.md](reports/java-to-go-cutover-performance.md) | Java 서버를 Go 구현으로 교체한 한 환경의 자원·처리량·유실 측정(익명) |

## 운영

| 문서 | 내용 |
|---|---|
| [operations/cutover-runbook.md](operations/cutover-runbook.md) | 사전 확인, 이관 리허설, Agent 전환 동작, 전환 방식, rollback, 중단 기준, 종료 절차 |
| [operations/sizing.md](operations/sizing.md) | 연결·수신·CPU·메모리·디스크 설정 산정과 Go 전용 상한 키 |
| [operations/monitoring.md](operations/monitoring.md) | `/health`·stats 줄·종료 로그 항목과 경보 기준 |
| [compatibility/migration.md](compatibility/migration.md) | `scouter-db` 검사·이관·증분 이관·verify·rollback |
| [compatibility/runtime.md](compatibility/runtime.md) | 실행 설정, 종료 상한, 운영자 안내(Java와 달라진 동작) |
| [compatibility/config-behavior.md](compatibility/config-behavior.md) | `scouter.conf` 키별 Java 반영 시점과 Go 상태 |

## 호환성 계약

| 영역 | 문서 |
|---|---|
| 원본 기준 | [baseline.json](compatibility/baseline.json), [source-inventory.json](compatibility/source-inventory.json)(원본 229 handler, Pack ID, 파일 fingerprint. 상태 필드는 정책상 모두 `not_implemented`이며 구현 등록은 `service.SupportedCommands()`와 기능별 계약이 추적한다), [query-scope.json](compatibility/query-scope.json)(조사 시작 시점의 분류) |
| wire·수신 | [wire-contract.md](compatibility/wire-contract.md), [packs.md](compatibility/packs.md), [steps.md](compatibility/steps.md), [transport.md](compatibility/transport.md), codec [CONTRACT](../internal/protocol/CONTRACT.md) |
| 서버·저장 | [server-contract.md](compatibility/server-contract.md), [storage-contract.md](compatibility/storage-contract.md), [pack-storage.md](compatibility/pack-storage.md), [ttl-index.md](compatibility/ttl-index.md), [recovery-retention.md](compatibility/recovery-retention.md) |
| 조회 | [query-contract.md](compatibility/query-contract.md), [counter-query-extended.md](compatibility/counter-query-extended.md), [packet-query.md](compatibility/packet-query.md), [trace-query.md](compatibility/trace-query.md), [aggregation-query.md](compatibility/aggregation-query.md), [service-group.md](compatibility/service-group.md), [server-metadata-query.md](compatibility/server-metadata-query.md), [database-counter-query.md](compatibility/database-counter-query.md), [local-store-query.md](compatibility/local-store-query.md) |
| Agent relay | [agent-read-query.md](compatibility/agent-read-query.md), [agent-side-effects.md](compatibility/agent-side-effects.md)(서버 동작이 계측 대상 애플리케이션에 미치는 영향) |
| 관리·HTTP | [management-commands.md](compatibility/management-commands.md), [http-contract.md](compatibility/http-contract.md), [webapp-api.md](compatibility/webapp-api.md), [console.md](compatibility/console.md) |
| Pulse | [pulse-contract.md](compatibility/pulse-contract.md), pulse [CONTRACT](../internal/pulse/CONTRACT.md) |

패키지 계약: [account](../internal/account/CONTRACT.md), [config](../internal/config/CONTRACT.md), [console](../internal/console/CONTRACT.md), [counters](../internal/counters/CONTRACT.md), [geoip](../internal/geoip/CONTRACT.md), [httpapi](../internal/httpapi/CONTRACT.md), [ingest](../internal/ingest/CONTRACT.md), [logger](../internal/logger/CONTRACT.md), [protocol](../internal/protocol/CONTRACT.md), [pulse](../internal/pulse/CONTRACT.md), [service](../internal/service/CONTRACT.md), [sqltable](../internal/sqltable/CONTRACT.md), [storage](../internal/storage/CONTRACT.md), [legacyindex](../internal/storage/legacyindex/CONTRACT.md), [telegraf](../internal/telegraf/CONTRACT.md), [transport](../internal/transport/CONTRACT.md), [webapp](../internal/webapp/CONTRACT.md).

## 검증 기록

| 기록 | 내용 |
|---|---|
| [2026-11-07 source-docs-sync](validation/2026-11-07-source-docs-sync.md) | 현재 소스와 문서 대조, 로컬 검증 범위, 병합 전 점검 |
| [2026-10-13 foundation](validation/2026-10-13-foundation.md) | 초기 설정과 첫 codec |
| [2026-10-13 codec-transport](validation/2026-10-13-codec-transport.md) | Pack·Step·수신 계층 |
| [2026-10-13 core-server](validation/2026-10-13-core-server.md) | 2C 수집·저장·조회와 실행 서버 |
| [2026-10-13 phase2d](validation/2026-10-13-phase2d.md) | 2D 추가 수집·저장·READ, Java 통신 클래스 56 명령 통합 |
| [2026-10-13 phase2e](validation/2026-10-13-phase2e.md), [2026-10-14 개선](validation/2026-10-14-phase2e-improvements.md) | 이관·복구·retention·성능 baseline |
| [2026-10-14 recommendations](validation/2026-10-14-recommendations.md) | 참고 구현 평가 권고 적용 |
| [2026-10-14 phase3-pulse](validation/2026-10-14-phase3-pulse.md) | Pulse golden·producer·격리 broker |
| [2026-10-14 stats-line](validation/2026-10-14-stats-line.md) | `--stats-interval` 로그 |
| [2026-10-15 remaining-gaps](validation/2026-10-15-remaining-gaps.md) | 3C 잔여 기능 |
| [2026-10-16 management-gaps](validation/2026-10-16-management-gaps.md) | 3B 관리 명령·계정·설정·HTTP Java oracle |
| [2026-10-17 xlog-max-benchmark](validation/2026-10-17-xlog-max-benchmark.md), [bench-profiling](validation/2026-10-17-bench-profiling.md) | 로컬 XLog 최대 지속 처리량, 병목 프로파일링 |
| [2026-10-22 connection-scale](validation/2026-10-22-connection-scale.md) | 대규모 Agent TCP 연결 |
| [2026-10-23 incremental-migration](validation/2026-10-23-incremental-migration.md) | 증분 이관 측정 |
| [2026-10-23 profile-scout](validation/2026-10-23-profile-scout.md) | 개선 후보 실측 |
| [2026-10-24 java-agent-interop](validation/2026-10-24-java-agent-interop.md) | 실제 Java Agent·Host Agent 상호운용 |
| [isolated-benchmark](validation/isolated-benchmark.md), [bench-xlog-max-driver](validation/bench-xlog-max-driver.md), [bench-java-server](../scripts/bench-java-server.md) | 격리 호스트 측정 절차와 driver, Java 기준치 실행기 |

Java oracle·상호운용 harness의 사용법은 `tools/*/README.md`에 있다(예: [java-agent-interop](../tools/java-agent-interop/README.md), [java-integration](../tools/java-integration/README.md)).

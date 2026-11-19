# 현재 소스와 문서 정합성 점검 (2026-11-07)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

## 기준과 범위

- Go 소스 기준: `e370ae9ba90e33037f86b76fe10ce43e1017da78`. 변경은 Markdown 문서뿐이며 Go·Python·shell 소스, fixture, 설정 기본값은 바꾸지 않았다.
- 호환성 정본: [baseline.json](../compatibility/baseline.json)의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`(2.21.3). 원본은 읽기 전용으로 대조했다.
- README·개발 안내·아키텍처·구현 순서·계약·운영·측정 도구 문서를 점검했다. 계약 문서의 모든 wire byte를 새로 감사한 것은 아니며, 현재 구현 여부·기본값·설정 전달·CLI·검증 주장과 최근 변경의 불일치를 정리했다.
- 날짜가 있는 Agent·실환경·성능 기록은 당시 revision의 증거로 유지했다. 이번 실행으로 운영 전환·전체 포팅 완료를 선언하지 않는다.

## 주요 정정과 근거

| 항목 | 정정 | 소스·테스트 근거 |
|---|---|---|
| 시작 안내·현재 상태 | 낡은 서술 대신 개발 시작 순서, 이미 구현된 패키지 경계, 현재/과거 검증 구분 | `AGENTS.md`, `Makefile`, `go.mod`, `docs/implementation-plan.md` |
| handler 등록 | 229개(READ 184 + 관리 45)는 계정 파일 모드 기준. 단일 계정 모드의 계정 관리 명령 제외를 명시 | `internal/service/command_registry_test.go`, `account_commands_test.go`, `cmd/scouter-go/main.go` `accountConfig` |
| 저장·이관 | `ReadAt` 기반 조회와 Java 형식 writer, 검증된 복사 이관·증분 이관을 현재 구현으로 설명 | `internal/storage/store.go`, `legacyindex/index.go`, `cmd/scouter-db/main.go` |
| live 설정·Telegraf | OnChange의 live 전달, 2초 normalize bucket, JAXB oracle 범위, 오류 fallback·재합성 동작 반영 | `cmd/scouter-go/main.go`, `internal/server/settings.go`, `internal/telegraf/{config,convert,oracle_test}.go` |
| HTTP·console | listener 충돌 사전 거절, 실제 UDP queue·visitor·오류 로그 통계와 현재 콘솔 구현 반영 | `cmd/scouter-go/main.go` `listenerConflict`, `internal/server/console_source.go`, `internal/console/*_test.go` |
| Pulse·profile | `sent`의 queue 수락 의미, profile 저장 기본 4MiB 상한을 명시 | `internal/pulse/dispatcher.go`, `internal/storage/{profile,packwriter,store}.go` |
| 종료·감시·CPU | exit 0에서도 clean marker 생략 가능, 누적/현재/구간 계수 구분, visitor write-through 의미, runtime CPU 기본값 복원 | `internal/server/{runtime,http,wiring}.go`, `cmd/scouter-go/shutdown_test.go`, `internal/storage/visitor.go` |
| replay·Java 실행기 | 외부 모드는 TCP 조회만 검증하며 영속성·서버 backlog는 입증하지 않음. 낡은 실행 안내를 실제 도구 안내로 교체 | `cmd/scouter-replay/main.go`, `scripts/bench-java-server.py`, `scripts/bench-xlog-max.py` |

계약과 운영·도구 범위를 나눠 점검한 뒤 변경 문장·핵심 소스·테스트를 재확인했다. 새로운 기능이나 성능 개선 주장은 추가하지 않았다.

## 실행 검증

환경: macOS arm64, Go 1.27.2, Python 3.14.4, JDK 18.0.2. 고정 Scouter 원본을 `SCOUTER_SOURCE`로, Scala용 JDK 8을 `SCOUTER_SCALA_JAVA`로 지정했다. 기존 toolchain을 재사용했으며 새 도구를 설치하지 않았다. `GOMAXPROCS=2`, `GOTOOLCHAIN=local`은 Makefile 기본값이다.

| 명령·검사 | 결과 | 범위 |
|---|---|---|
| `make check` | FAIL(exit 2) | gofmt·vet 통과. service/storage/legacyindex/migrate/transport 테스트 실패. build는 별도 통과 |
| `make inventory-check` | PASS | 고정 원본 inventory와 커밋된 JSON 일치 |
| `make fixtures-check` | PASS | Java oracle fixture 141개 일치 |
| `make java-integration` | PASS | 전체 build, 원본 Java 232개 소스 컴파일, synthetic 수집·조회·Agent V1 relay·SIGTERM·동일 DB 재기동 |
| `JAVA8_HOME=<JDK8> go test -v -count=1 ./internal/telegraf -run '^TestJAXBOracle$'` | PASS | `SCOUTER_SOURCE`도 지정하여 JAXB 선택 검증을 명시 실행 |
| Markdown 상대 링크 파일 존재 검사 | PASS: Markdown 101개·상대 링크 557개, 누락 0 | inline/fenced code와 외부 URL은 제외. heading anchor·외부 URL 응답은 검사하지 않음 |
| `git diff --check` | PASS | 공백·patch 형식 |

2026-11-19 공개 전 재검증: `make check` 통과(25개 패키지), `make fixtures-check` 141개 통과, `make inventory-check` 통과. 위 표의 `make check` FAIL은 2026-11-07 당시 기록으로 보존한다.

`make check`에는 환경에 따라 skip되는 선택 검증이 있다. Kafka Java codec(`SCOUTER_KAFKA_JAVA_CP`)·실제 Kafka broker 테스트는 이번 범위에서 실행하지 않았다. JAXB는 별도 JDK 8 환경을 지정한 명령으로 PASS를 확인했다. skip을 실제 연동 PASS로 해석하지 않는다.

## 제한 재검증

실패 재검증은 최초 실패 후 대표 실패 두 개를 한 차례 재실행하는 범위로 제한했다. 소스·테스트의 oracle·지연 임계값을 변경하지 않았다.

- 최초 `internal/service`: `TestExtendedCounterOriginalScalaTodayWireOracle`, `TestDatabaseOriginalScalaHandlerWireOracle`, `TestLocalStoreActualJavaWriteHandlersAndDB`, `TestLocalStoreActualJavaHandlersAndDB`가 각 120초 제한에서 `signal: killed`; 이어 package 10분 제한으로 `TestMetadataJavaWireOracle` 중 중단됐다.
- 최초 `internal/storage`: `TestBatchArchiveActualJava`의 원본 컴파일이 제한 시간에서 중단됐다. `TestCompressedBlockDecodeIngestWait`는 txid 조회 중 `PutXLog` 대기 240.49ms가 기준 150ms를 넘었다. 이후 package 10분 제한으로 `TestLongKeyChainIngestWait` 중 중단됐다.
- 최초 `internal/storage/legacyindex`: `TestTTLActualScalaBidirectionalFiles`의 후반 `--verify-ttl` oracle이 90초 제한에서 중단됐다. 앞선 generate·verify 일부 통과를 전체 양방향 검증 PASS로 처리하지 않았다.
- 최초 `internal/storage/migrate`: `TestJavaOracleMigration` 실행 중 package 10분 제한으로 중단됐다.
- 최초 `internal/transport`: `TestAgentChunkStreamBoundsOnlyItemsAndReads`가 `write: broken pipe`로 실패했다.
- 분류: 변경하지 않은 소스의 검증 실패(oracle 실행 시간·저장 지연·통신 테스트). 실행 환경 영향은 후보이며 원인을 확정하지 않는다.
- 재검증 명령: `go test -v -count=1 -p=1 -timeout=4m ./internal/service ./internal/storage -run '^(TestExtendedCounterOriginalScalaTodayWireOracle|TestCompressedBlockDecodeIngestWait)$'`. 결과는 exit 1. Scala oracle은 다시 120초 제한에서 실패했고, 저장 지연 테스트는 6.16초에 PASS(최대 PutXLog 대기 47.31ms)했다. 대표 두 개의 결과이며 전체 suite PASS가 아니다. 정한 1회 재시도 범위를 마쳤고 추가 코드 수정이나 재실행은 하지 않았다.

## 미실행과 남은 gate

- `make race`, `make fuzz`, `make bench`, 별도 `make storage-check`는 문서만 변경해 재실행하지 않았다. 저장 코드의 일반 테스트는 `make check` 범위에 포함된다. 이번 작업은 동시성·parser·성능·저장 형식 변경이 아니다.
- Pulse 원본 golden 재생성, 실제 Agent JDK 8/11 상호운용, Linux/Windows 실행, 실제 Agent 환경 확인, 부하·장애 시험은 재실행하지 않았다.
- 운영 DB 복사본 이관·rollback, 전체 GUI/Batch Agent/복수 TCP session, 실제 LDAP·Telegraf·webapp JSON, Pulse Collector·UUID·Kafka 장애 행렬, 공유 자원 한도 지속 부하·전원 손실 내구성 gate는 유지한다.
- 문서 정합성 정리 범위이며 release·배포는 포함하지 않는다.

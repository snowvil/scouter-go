# 단계별 구현과 완료 조건

전체 목표: Agent/Client/조회/기존 데이터 이관 호환을 유지하는 단일 Go Scouter Server와 Pulse 내장 전파 기능. 단계별 완료 근거 없이 전체 완료 비율을 추정하지 않는다. 아래 표의 "현재 상태"가 단계별 상태의 정본이고, 표 아래 절들은 각 단계를 시작할 때의 범위 기록이다.

2026-11-07 소스 대조 기준으로 소스·문서를 다시 대조했다([점검 기록](validation/2026-11-07-source-docs-sync.md)). 아래의 실제 Agent 환경·성능 결과는 각 링크의 과거 실행 증거이며 이번 점검에서 재실행한 결과가 아니다. handler 229개 등록은 계정 파일 모드 기준이고, 등록만으로 모든 GUI·운영 동작이 검증됐다는 뜻은 아니다.

| 단계 | 산출물 | 완료 조건 | 현재 상태 |
| --- | --- | --- | --- |
| 1. 지침·초기 설정 | AGENTS, 고정 source SHA, build/test 도구, 설계, command inventory | 도구 설치·검증, 원본 수정 없음, 문서/branch/commit 확인 | 완료 — 로컬 검증. 원격 CI 실행은 별도 |
| 2A. codec | Decimal/Blob/Value/Pack, Java golden, 오류/상한/fuzz | 지원하는 codec별 Java 대조와 legacy 경계 통과 | codec 완료 — Pack 16종/Value 17종/Step 23종, Java fixture/race/fuzz 검증 |
| 2B. transport | UDP 단일/묶음/분할, TCP Client/Agent V1/V2/request | Java sender↔Go, partial read, timeout, 포화, 종료 검증 | 수신 계층 구현·Java bytes loopback 검증. 실제 Agent 환경에서 Java agent와 Scouter Client의 수집·조회 상호운용과 재기동 반복을 확인했다([요약](validation/2026-10-15-remaining-gaps.md)). 2026-10-24에는 실제 `scouter.agent.java`·Host Agent와 원본 Client 통신 클래스로 수집·relay·실패 경로·재기동·conf 주소 변경·서버 발신 명령을 확인했다(JDK 8·11 각각 56/56, [기록](validation/2026-10-24-java-agent-interop.md)). Batch Agent, `net_collector_tcp_session_count` 2 이상, GUI 전체 기능의 상호운용은 별도 gate |
| 2C. core·저장·기본 조회 | object/text/counter/XLog/profile, 로그인·기본 Client | 수집→저장→재시작→실제 Client 조회 일치 | 기본 흐름 구현. 원본 DB 양방향 및 Java 통신 클래스 통합 검증. 전체 GUI/운영 검증 별도 |
| 2D. 추가 수집·READ 기능 | 나머지 built-in Pack, READ/relay/집계/원본 저장 계약 | 각 기능의 원본 bytes·storage·Agent/Client 통합 evidence | 2D 범위의 READ179 구현·Java56 synthetic 통합 PASS. 당시 제외한 JVM 의존 READ5와 mutation45는 3B에서 구현 등록했다(현재 READ184/184, mutation45/45, unsupported 0). 운영 전환 gate는 별도 |
| 2E. 이관·성능 | legacy reader/importer, recovery, baseline/soak | 기존 DB 비교, rollback, 자원 예산과 지속 부하 gate | 검사·검증된 복사 이관·rollback 도구, 제한된 crash 복구, Java 호환 retention, replay/soak 도구 구현. Java writer synthetic DB 양방향과 로컬 baseline 검증. 운영 DB·목표 호스트·전원 손실·목표 처리량은 미검증([기록](validation/2026-10-13-phase2e.md), [개선 기록](validation/2026-10-14-phase2e-improvements.md), [로컬 XLog 최대 처리량 측정](validation/2026-10-17-xlog-max-benchmark.md)). 이후 증분 이관(`--precopy`/`--finish`, [측정](validation/2026-10-23-incremental-migration.md)), crash checkpoint로 꼬리만 검증하는 재기동, 대규모 Agent 연결 시험([기록](validation/2026-10-22-connection-scale.md))을 더했다([개선 대상](improvement-candidates-2026-10-21.md) §9~§12). 운영 DB 복사본 이관 리허설은 하지 않았다 |
| 3. Pulse 내장 | callback/filter/model/Kafka 및 지표 | plugin golden, broker 장애, Collector 조회, UUID 경계 검증 | 구현 완료. Java golden 124/124, 표준 라이브러리 Kafka producer, 격리 Kafka 4.0.0 broker end-to-end(XLog 3600/profile 955 전송·소비 일치). producer 압축(lz4 기본)과 flexible version은 3C에서 추가했다. Collector 실연동·broker 장애 행렬·운영 부하는 미검증([기록](validation/2026-10-14-phase3-pulse.md)) |
| 3B. 기능 공백 | 관리·변경 명령 45, JVM READ 5, 계정/LDAP, scouter.conf 리로드, HTTP servlet/Telegraf, webapp REST, counters.site.xml | Java oracle(golden 336)·loopback·리뷰·전체 gate | 2026-10-16 구현·통합([계약](compatibility/management-commands.md), [HTTP](compatibility/http-contract.md), [webapp](compatibility/webapp-api.md), [검증 기록](validation/2026-10-16-management-gaps.md)). Java golden 대조(검증 기록 §9) PASS, 표준 gate(§11) PASS, 실제 Agent 환경에서 기동·servlet·webapp 응답 확인([요약](validation/2026-10-15-remaining-gaps.md)). Client GUI의 관리 명령 실동작, 실제 Telegraf·LDAP 서버, Java webapp JSON 대조는 미검증. JVM 스크립트·plugin 실행은 제외 확정 |
| 3C. 잔여 기능 | Pulse Kafka lz4·flexible version, legacy GeoIP reader와 XLog 국가·도시, Kubernetes pod 순번, CPU 초과 자동 thread dump, ServerStat, AgentManager alert·object type 자동 등록, `-console`, `<pid>.scouter` 종료 파일·SIGHUP, SqlTables, Java 형식 server/request log·Pack 출력, scouter.conf live 설정(F0)과 패키지별 live setter | 기능별 Java oracle(geoip golden 27, ingest·sqltable·logger·console·pulse lz4), 실제 Kafka broker, 서버 runtime 연결, 리뷰, 표준 gate, 실제 Agent 환경 확인 | 구현·서버 연결 완료(3C 구현 이력은 검증 기록 참조). 기능별 Java oracle 대조, 독립 리뷰 2회(High 0, Medium 3·Low 15 수정 또는 문서화), 통합 테스트(java-integration·conf reload·Pulse lz4 end-to-end·종료 경로 PASS, replay FAIL은 수정 commit 반영)은 [3C 검증 기록](validation/2026-10-15-remaining-gaps.md). 표준 gate 결과는 같은 기록의 gate 표를 따른다. 연결하지 않은 키는 대응 구조가 없거나 Java도 기동 값인 키뿐이다([config-behavior](compatibility/config-behavior.md) §5.1a). 3C 이후 빌드를 실제 Agent 환경에서 운용했지만([개선 대상](improvement-candidates-2026-10-21.md) §11~§20), 3C 기능별 실환경 확인은 기록하지 않았다 |
| 4. 최종 전환 검증 | 전체 기능+Pulse를 켠 canary/rollback/soak | 합의된 자원/내구성/SLO 안의 목표 처리량 | 미검증: 합의된 SLO·자원 한도·내구성 기준으로 판정한 검증이 없다는 뜻이다. 한 환경에서 Java 서버를 교체해 운용한 사례는 [전환 성능 평가](reports/java-to-go-cutover-performance.md)에 있으나 이 gate의 통과 근거가 아니다. 준비로 전역 자원 상한, 종료 상한, 운영 전환 문서([runbook](operations/cutover-runbook.md), [sizing](operations/sizing.md), [monitoring](operations/monitoring.md))를 마련했다(2026-10-21~25, [개선 대상](improvement-candidates-2026-10-21.md)) |

## 2A 첫 구현 단위

Scouter의 길이 없는 연속 Pack stream을 정확하게 해석하기 위한 기초 codec을 먼저 구현한다. 지원한 Pack만 성공으로 반환하고 나머지는 명시적 오류를 반환한다. 전체 서버 listener나 성공처럼 보이는 빈 handler를 추가하지 않는다.

첫 단위의 정확한 범위와 의도된 malformed 입력 차이는 [codec 계약](../internal/protocol/CONTRACT.md)을 따른다. 완료 근거는 [초기 검증 보고서](validation/2026-10-13-foundation.md)에 기록한다. Pack/Step 확장과 수신 계층 근거는 [추가 검증 기록](validation/2026-10-13-codec-transport.md), 2C 수집·저장·실행 흐름은 [core 검증 기록](validation/2026-10-13-core-server.md), 2D 확장은 [Phase 2D 기록](validation/2026-10-13-phase2d.md), 2E 이관·복구·성능은 [Phase 2E 기록](validation/2026-10-13-phase2e.md)에 기록한다.

- Java `DataInputX/DataOutputX` 및 Pack 객체를 실행하여 synthetic fixture를 만든다.
- signed 정수 폭, Blob 경계, 한글/비BMP UTF-8, null/empty, Map 순서, XLog 구형 선택 필드, Profile V1/V2를 비교한다.
- negative length, 잘린 payload, 너무 큰 count, 중첩 상한, 알 수 없는 type을 검증한다.
- 원본 source SHA와 fixture checksum을 고정하고 `--check` 재생성을 제공한다.

## 2B transport 구현·검증 범위

1. 실제 Java encoder 기반 UDP single/batch/fragment fixture와 ephemeral-port loopback test를 추가한다.
2. 재조립 key에 sender/objHash/pkid를 사용할지 원본 pkid-only와 비교하여 의도된 방어 차이를 기록한다. fragments/count/TTL/bytes 상한을 별도로 적용한다.
3. 고정 worker/queue와 accepted/rejected/malformed/expired 지표를 구현한다. byte buffer 소유권을 넘기는 시점을 검증한다.
4. TCP client handshake/명령별 request, Agent V1/V2, request upload를 독립 테스트한다. short read/coalescing/reconnect/CLOSE/invalid session을 포함한다.
5. 테스트는 `127.0.0.1:0` 등 OS가 배정한 격리 포트를 사용한다. 기본 포트 6100 listener와 충돌하지 않는다.

## 2C–2E 의존 순서

dictionary/object → counter → XLog/profile → session/query → 날짜별 저장/index → legacy reader/importer → 나머지 기능 순으로 진행한다. 실시간 cache만으로 과거 조회를 대체하지 않는다. code가 있는 것과 실제 명령 지원은 따로 추적한다.

counter의 5분 자동 표본, XLog의 loop/index와 history V2 cursor, profile chunk, retention과 timezone을 별도 회귀 케이스로 둔다. 저장 최적화는 write batching/positional read부터 측정하며 저장 형식 변경과 내구성 변경은 각각 기록한다.

## 2D 당시 범위와 후속 단계

2D 단계에서는 원본229 handler 중 READ184개를 분류·인식했고 unique179개를 구현 등록했다. 당시 `GET_ALERT_REAL_COUNTER_DESC`, `GET_ALERT_SCRIPT_LOAD_MESSAGE`, `GET_PLUGIN_HELPER_DESC`, `SERVER_THREAD_DETAIL`, `SERVER_THREAD_LIST`는 JVM 의존 unsupported였고, 상태 변경·관리 명령 45개 중 KV/TTL 쓰기 6종만 2026-10-14에 원본 handler oracle(45 case)로 구현·검증했다. 이후 3B에서 나머지 mutation 39개와 JVM 의존 READ 5개(descriptor 정적 표·load message ring·goroutine 정보)를 구현 등록했다([관리 명령 계약](compatibility/management-commands.md), [검증 기록](validation/2026-10-16-management-gaps.md)).

추가 수집은 DroppedXLog/Status/Interaction/Alert/Stack/Summary/Batch/SpanContainer를 연결한다. MapPack·standalone SpanPack의 extension chain은 미지원이다(JVM 확장 chain 실행은 제외 확정). 조회는 Counter·Special·Export, Packet·Summary, Visitor·Tag, Trace/B3 profile, Maria/Cubrid counter, KV/query-list, Agent READ relay와 metadata READ를 포함한다. 원본 DB와 실제 Java/Scala handler oracle, 실제 Host 송신·Client 통신 클래스의 synthetic loopback은 각각 별도 증거로 기록한다.

PerfCounter의 objHash/time은 원본 `NetDataProcessor`에서 주입된다. 이 값은 저장 row·cache·sampling 예산에도 포함해야 한다. Agent pool은 idle socket checkout/반환, 대기 취소, keepalive 회수, 오류 시 partial Blob 전달을 검증한다. 2D의 Telegraf는 원본 sample 또는 파일 **READ만** 연결했다. metric 수집·설정 적용은 3B에서 구현했다([HTTP 계약](compatibility/http-contract.md)).

2D 당시 `make check`, `make race`, 해당 binary의 Java56 synthetic 통합은 PASS였으며 [검증 기록](validation/2026-10-13-phase2d.md)에 실행 범위와 변경 시점을 남긴다. 운영 DB 복사본·이관/rollback, retention과 열린 index/reader, crash/disk-full 복구, 전체 GUI/instrumentation 및 공유 호스트 예산의 지속 부하는 별도 gate다. 2D의 선언 범위는 READ다. 관리 명령·JVM READ·HTTP/webapp·LDAP는 3B, Pulse는 3, 잔여 기능은 3C에서 다룬다. JVM plugin/script 실행은 제외 확정이고, 단일 credential 모드의 raw login은 지원하지 않는다.

## 3 Pulse 통합 순서

기존 plugin source와 Collector를 기준으로 key/header/partition/JSON/Step fixture를 먼저 확보한다. 필터와 dictionary 보강을 순수 함수로 검증한 다음 producer adapter를 연결한다. 기본 `acks=0` 의미와 reliable mode를 혼동하지 않는다. UUID를 새로 만들지 않는다. 자세한 계약은 [pulse-contract.md](compatibility/pulse-contract.md)를 따른다.

## 3C 잔여 기능 범위와 제외

3C는 3B 이후 남은 서버 기능을 원본 정본대로 메운다. 기능별 계약과 의도된 차이는 패키지 계약이 정본이다: [pulse](../internal/pulse/CONTRACT.md)(Kafka lz4·flexible version), [geoip](../internal/geoip/CONTRACT.md), [ingest](../internal/ingest/CONTRACT.md)(kube 순번·CPU dump·GeoIP 보강·ServerStat·AgentManager alert·live 설정), [console](../internal/console/CONTRACT.md), [sqltable](../internal/sqltable/CONTRACT.md), [logger](../internal/logger/CONTRACT.md), [config](../internal/config/CONTRACT.md)(F0 live 설정), [storage](../internal/storage/CONTRACT.md)·[transport](../internal/transport/CONTRACT.md)·[webapp](../internal/webapp/CONTRACT.md)·[account](../internal/account/CONTRACT.md)(live setter). 키별 Java 반영 시점은 [config-behavior](compatibility/config-behavior.md)를 따른다.

완료 조건은 기능별 Java oracle 대조, 서버 runtime 연결, 독립 리뷰, 표준 gate(`make check/race/fuzz/fixtures-check/inventory-check/storage-check`와 기능별 oracle check), 실제 Agent 환경 확인이다. 현재 상태는 위 표의 3C 행이며 완료로 선언하지 않는다.

제외 확정(프로젝트 결정): JVM 스크립트·plugin 실행(`.alert` 규칙 컴파일·실행, `.plug`, plugin JAR, 확장 handler chain), webapp의 extweb·swagger·WebSocket `/basic`·webroot·`/v0/temp/test/*`. 이 기능은 빈 성공으로 대체하지 않고 명시적으로 미지원으로 둔다(`.alert`/`.conf` 저장·조회·descriptor는 지원).

## 완료 보고

각 단계는 commit, 변경 파일, 실행 명령/pass·fail, 미실행 이유, 가정, 남은 gate를 함께 보고한다. `docs/validation/`에 실행 결과를 남긴다. 원본/참고 저장소/실제 배포 버전을 혼동하지 않는다.

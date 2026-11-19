# Phase 3 Pulse 내장: golden·producer·격리 broker 검증

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

2026-10-14 기록이다. 최종 검증 커밋은 §5의 gate 표에 적는다. 정본은 Pulse plugin 원본(비공개, 당시 git commit으로 고정했고 지금은 [baseline.json](../compatibility/baseline.json)의 `pulse_plugin.source_digest`로 고정한다. golden 생성 도구는 공개 저장소에 없고 커밋된 golden은 고정 자료다. Scouter 2.20.0, kafka-clients 4.0.0, Jackson 2.15.2)와 [pulse-contract](../compatibility/pulse-contract.md)다. 운영 코드에 Java는 없다. Java와 Kafka 배포본은 검증 oracle과 격리 broker로만 실행했다. Pulse 수집 서비스 실연동, 운영 Kafka, 운영 부하는 미검증이다.

## 1. 변경 단위

| 커밋 | 내용 |
|---|---|
| `700522d` | Pulse golden 생성 도구(Java oracle harness, 현재 이 저장소에 없음): plugin 정본을 임시 복사본에서 offline Gradle로 컴파일하고 MockProducer seam으로 124 case golden 생성(`internal/pulse/testdata/`), `--check` 재생성 비교. `scripts/kafka-local.py`: 공식 Kafka 배포본으로 KRaft 단일 노드를 임시 디렉터리·loopback 포트에 기동·정지·topic·produce/consume(JSON lines) |
| `6877522` | `internal/pulse/kafka`: 표준 라이브러리 producer(ApiVersions v0, Metadata v1–8, Produce v3–7, RecordBatch v2 + CRC32C, headers, acks 0/1/-1, batch/linger, broker별 연결, metadata 갱신, bounded buffer·Close) |
| `56870e1` | `internal/pulse`: application 추출, Java `String.hashCode` partition, HashUtil CRC32, 필터·rebase 전부, Trace/Span 모델과 Jackson 호환 JSON, Step singleton Base64, dispatcher 2개(bounded queue/worker, filtered txid TTL map), ingest hook(`XLogCore`/`ProfileCore`의 `PlugInManager` 지점) |
| (이번) | `internal/server/pulse.go`: `--pulse-conf`(Java properties 키) + `--pulse-kafka-acks`, producer sink, 종료 순서(engine → dispatcher → producer), 통계 줄 `pulse=… kafka=…`; replay 도구의 상태 줄 분류 |

## 2. Java golden 대조

당시 golden 재생성 비교(생성 도구의 `--check`)는 plugin 정본을 다시 실행해 124 case가 동일함을 확인했다(2회 실행 PASS). Go `TestJavaGolden*`는 124/124 일치다.

| 분류 | case 수 | 대조 항목 |
|---|---:|---|
| sent | 98 | topic, partition, key 8-byte, header 순서·bytes, JSON bytes, profile step Base64 decode bytes |
| filtered | 20 | 제외 단계(application 추출 실패/exclusion/HTTP status/extension/Spring URI/profile filtered txid) |
| exception | 6 | Java task 예외(objName `a/b` AIOOBE, dictionary miss NPE, unknown step type, truncated step) → 미전송 동일 |

golden으로 확정된 Java 동작: `xtype` key가 마지막, Jackson surrogate pair·대문자 hex escape, rebase 시 `transactionDictId` 항상 재계산, transaction 확장자 검사의 null 제외, SPAN/SPANCALL StepSingle bytes(start_cpu=0), profile text의 현재 날짜 preload 우선. 미생성: HTTP status txid TTL 만료(시계 조작 불가), Pulse 수집 서비스 역직렬화(별도 저장소).

## 3. Kafka producer 검증

- 단위·fake broker: zigzag/CRC32C 벡터, RecordBatch 74바이트 수작업 비교, 2-broker 라우팅, NOT_LEADER 재조회, 재연결, acks=-1 오류 전파, 버퍼 포화 drop, Close drain, 과대/절단 응답 거부, 버전 매트릭스. `-race` PASS, fuzz 3 target(10s×3) PASS.
- **실제 broker**(`scripts/kafka-local.py`, Kafka 4.0.0 KRaft, JDK 18): Go producer가 30 partition topic에 90 record(음수 txid key, 한글·escape JSON, 헤더 3개)를 전송하고 독립 Java consumer가 읽은 결과와 partition·key·value·header bytes가 **완전 일치**, acks=0과 acks=1 모두(`TestRealBrokerProduce`, 환경변수 지정 시 실행).

## 4. 서버 end-to-end (격리 broker)

`scouter-replay`로 Go 서버에 UDP 수집(300 tx/s × 10s + warm-up, object 10, profile 비율 0.3)을 보내고 `--pulse-conf`로 Pulse를 켰다. broker는 `scripts/kafka-local.py start`, topic은 기본 이름 2개(partition 30).

| 항목 | 결과 |
|---|---|
| 기본 설정(Spring URI filter 켜짐) | XLog 3600·profile 957 모두 `FilterSpringURI` 단계에서 제외, Kafka 전송 0. replay 생성기의 transaction 이름(`/api/...`)이 `<`,`>`를 포함하지 않아 Java 의미대로 제외된 것이다 |
| `ext_plugin_pulse_enable_spring_uri_filter=false` | ingest XLog 3600 → pulse XLog Enqueued 3600/Sent 3600, profile 955/955, Dropped 0, Failed 0, ShutdownDiscarded 0; kafka Enqueued 4555 = SentRecords 4555, Failed 0, abandoned 0 |
| Java consumer 소비 | meta topic 3600건(unique txid 3600), profile topic 955건. 모든 record에서 `traceId` = key(int64 big-endian), partition = `floorMod(String.hashCode(app), 30)`, header 순서 `apm_type/xlog_serializer_version/xlog_application`, JSON key 순서가 golden과 동일(`xtype` 마지막), profile step 21개 decode·SQL mainValue 확인 |
| 종료 | 서버 SIGTERM exit 0, 재시작 조회 3600/3600, broker 정지 후 잔여 프로세스 0 |

## 5. 표준 검증

커밋 `a59dea6`(Pulse 서버 연결)의 `git archive` 복사본에서 순서대로 실행했다(`SCOUTER_SOURCE`와 Pulse plugin 원본 경로 지정).

| 명령 | 결과 |
|---|---|
| `make check` | **PASS exit 0** (208s) |
| `make race` | **PASS exit 0** (271s), 전체 package |
| `make fixtures-check` | **PASS**, Java oracle fixtures 141개 |
| `make inventory-check` | **PASS** |
| `make storage-check` | **PASS** (12s), 원본 storage 양방향 oracle 실제 실행 |
| `make fuzz` (`FUZZTIME=10s`, 11 target) | **PASS** (127s) |
| `make build` + `java-integration` | **PASS** (20s), `integration_passed=True` |
| Pulse golden 재생성 비교(당시 make target, 현재 제거) | **PASS**, plugin 정본 재실행 124 case 동일 |

gate 빌드 SHA256: `dist/scouter-go` `cb5a0f03218d6859c4d92b96bcf525280ae33bb2014ae011696cdd09a5e41daf`, `dist/scouter-db` `20338cf07c68d0c06724acce9c5a670604746bd3ca916f2a80bff015a3fcb87f`, `dist/scouter-replay` `583dc2973371db34f7e114eadef9e7cfd4614207505c023ae2bc178bdb8d4989`. 이후 커밋 `ee61387`(UDP flag·HTTP health)은 server package race로 검증했고 전체 gate는 다음 단위에서 재실행한다.

## 6. 의도된 차이와 남은 문제

- producer: compression none, flexible version 미지원, in-flight 1, buffer 64MiB 기본, bounded Close drain(Java shutdownNow). acks는 기본 0이며 1/-1은 옵션이다. 내구성 옵션 비교(drop/duplicate/ordering/latency)는 미측정이다.
  - 이후 변경(branch `codex/remaining-gaps`, 이 기록의 검증 범위 밖): producer는 lz4 압축을 기본으로 하고(`none` 선택 가능) ApiVersions v3·Metadata v1–13·Produce v3–12 flexible version을 지원한다. 근거와 검증은 [pulse CONTRACT](../../internal/pulse/CONTRACT.md) "Kafka producer 압축과 protocol 버전"과 [pulse-contract](../compatibility/pulse-contract.md) 끝의 갱신 주석이다. 위 본문은 2026-10-14 당시 상태로 보존한다.
- dispatcher: filtered txid map 상한(100,000), 종료 drain과 잔여 계수, 통계 단조 증가(Java는 10초 reset), 추출 실패를 filtered로 통일.
- Pulse 수집 서비스 수신·UUID 경계·DB 정합성, broker 장애(재시작·응답 지연) 행렬, 공유 호스트 부하에서 Pulse on/off 처리량 비교는 미검증이다.
- Gradle 캐시 LRU cleanup이 테스트 중 Kafka 3.0.1 jar를 삭제했다. broker는 공식 배포본(`--kafka-home`)으로 대체했으며 배포본은 저장소에 포함하지 않는다.

## 7. 후속 보완

Pulse 서버 연결까지의 변경을 정적 검토해 아래 항목을 보완했다. 핵심 변환·필터·partition·CRC·JSON·Step·RecordBatch 계약은 원본과 일치한다.

| 항목 | 반영 |
|---|---|
| 운영 dictionary 조회에 Java `TextCache`(날짜 무관 최근 수신) 의미가 없음 | 서버 lookup이 event 날짜 miss 시 수신 날짜(오늘)로 한 번 더 조회(`pulseLookup`). `TestPulseLookupFallsBackToToday` |
| Pulse 설정 오류가 서버 기동을 막음 | Java plugin처럼 격리: 로그 + `/health`의 `pulseError`, stream만 꺼짐. `TestRuntimePulseConfigErrorIsIsolated` |
| acks=0 + idle 연결에서 첫 batch가 `Sent`로 집계된 채 유실 가능 | `IdleReconnect`(기본 9분, broker `connections.max.idle.ms` 10분보다 짧게) 초과 연결은 전송 전 재dial. `TestIdleConnectionIsRedialled` |
| `Offer`∥`Close` 동시 호출 시 closed channel send panic | `offerMu`(RW)로 직렬화. `TestOfferCloseRace`(-race, 50회) |
| 지연(child) profile `time`이 Java(release 시각)와 다름 | `releaseProfiles`/`recoverPack`(당시 `recoverProfiles`)이 release 시각을 설정(Java `ProfileCore.add`) |
| lookup마다 store gate 경합(성능) | 미수정. Pulse on/off 처리량 비교는 측정 후 결정 |
| 구분자 없는 key 줄 | Java `Properties`처럼 빈 값(→ `ext_plugin_pulse_enable`만 쓰면 비활성). `TestReadPulsePropertiesBareKey` |
| 종료 예산 공유 | dispatcher·producer 각 5초 |
| 강제 종료 중 취소된 task가 `Failed`로 집계 | `ShutdownDiscarded`로 분류 |
| pprof 바인드 | loopback이 아니면 경고 로그 |
| debug off에서도 filter Event 생성 | filter 단계 Event는 `ext_plugin_pulse_debug=true`일 때만(Java plugin의 filter 로그 gate) |
| 그 밖의 차이 | `internal/pulse/CONTRACT.md`에 차이·영향 기록, 미수정 |

dictionary 날짜 fallback, idle 연결 재dial, Offer∥Close 직렬화, 지연 profile time의 실 환경 재현(실 store cross-day, 실제 idle broker, 운영 부하)은 미실행이다.

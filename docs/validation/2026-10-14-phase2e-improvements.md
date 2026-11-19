# Phase 2E 개선: 성능 병목·복구 정합성·독립 호환 검증

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

2026-10-14 기록이다. branch `codex/phase2e-storage-ops`, 시작 커밋 `2cee3b6`(2E 기록), 최종 검증 커밋 `de26ccd`다. 정본은 Scouter 2.21.3 SHA `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. 우선순위는 **호환성 → 복구 정합성 → 성능**이며, 파일 형식·조회 의미·Java 상호운용을 바꾸는 최적화는 넣지 않았다. 환경은 [Phase 2E 기록](2026-10-13-phase2e.md)과 같다(Apple M1 Pro 8코어 loopback, Go 1.27.2, JDK 18/8). 수치는 로컬 synthetic 측정이며 목표 호스트 검증이 아니다.

## 1. 변경 단위

| 커밋 | 내용 | 호환성 영향 |
|---|---|---|
| `b98062b` | `SyncData`/`Counters.Sync`가 gate 안에서 header snapshot(`legacyindex.SyncPoint`)만 뜨고 fsync는 gate 밖에서 수행. visitor snapshot 저장을 runtime maintenance loop로 이동. append offset 캐시. `--profile-dir` | 없음(파일 형식·순서 동일: data fsync → header) |
| `d4a4317` | tag top100 cache와 VALUES/VALUE_DATA 조회를 스트리밍화 | 없음(출력 bytes가 이전 구현·Java oracle과 동일) |
| `5dfdaa1` | relink 전에 data 존재 검사(`OpenChecked`/`RelinkCheck`) | 없음(거부 시 Java 의미) |
| `ae7273b` | 압축 writer: record마다 GZIP member append, count.dat 제자리 갱신, block cache, torn member 복구 | **의도된 차이**: 파일당 multi-member. Java `GZIPInputStream`이 같은 논리 bytes로 해제함을 원본 reader로 검증. 압축률 0.192(Java block 전체 gzip 0.145) |
| `4893899` | 거부된 relink를 tombstone(deleted flag 2)으로 영구화. `Close`가 `syncRun`을 먼저 잡음. visitor eviction I/O 순서. inspect 미리보기에 data 검사 적용 | Java는 flag≠0을 deleted로 읽으므로 get/getAll/scan 의미 동일(원본 oracle `verify-double-crash`: active 6, deleted 3) |
| `de26ccd` | inspect가 tagcnt `.kfile`을 legacy index로 오인하던 결함 수정. replay DB Java oracle 추가 | 도구 결함 수정 |

## 2. 독립 호환 검증 (원본 Java reader)

구현과 분리한 독립 검증으로 "Go 서버가 실제로 쓴 DB를 원본 Java reader로 전부 읽어 대조"하는 절차를 수행했다(`scripts/replay-java-oracle.py`, `tools/java-storage/ReplayDbOracle.java`). 원본 클래스 245개를 `--release 8`로 컴파일해 실행했고, Go 서버 두 프로세스(plain/gzip, 300 tx/s × 15s, object 10, seed 7)를 종료한 뒤 **복사본**을 읽었다.

| 항목 | plain Go/기대 | plain Java | gzip Go/기대 | gzip Java |
|---|---:|---:|---:|---:|
| XLog txid index 건수 / unique txid | 5100 | 5100 | 5100 | 5100 |
| decode 성공 / `getByTxid` payload 동일 | 5100 | 5100 / 5100 | 5100 | 5100 / 5100 |
| 기대 txid 누락 / 추가 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Profile 조각 / bytes | 1337 / 13043484 | 1337 / 13043484 | 1359 / 13297631 | 1359 / 13297631 |
| Object / Text | 10 / 5515 | 10 / 5515 | 10 / 5481 | 10 / 5481 |

- 기대 txid는 generator의 seed 7 bijection을 Python으로 재계산해 Java 출력 집합과 비교했다.
- gzip `xlog.0`의 Java `CompressUtil.unZip` 해제 길이 13928717 = `count.dat`.
- 원본 `XLogDataWriter`/`XLogIndex`로 Go gzip DB 복사본에 XLog 3건을 이어 쓴 뒤, Java 전체 읽기 5103/5103과 Go 서버 secure LOGIN·`TRANX_LOAD_TIME_GROUP` 조회(추가 txid 3개)가 성공했다. 해제 길이 = count = 13928972.
- 표준 gate 재실행: `make build`/`storage-check`/`fixtures-check`(141)/`java-integration` 모두 exit 0, skip 없음.
- **발견한 결함**: `scouter-db inspect`가 모든 DB의 `tagcnt/*/first.kfile`·`next.kfile`에서 `index.head`/`index.cycle` 오류 2건을 냈다. tagcnt는 legacy index와 확장자만 같고 형식이 다른데 index로 등록돼 파싱된 것이다. 실제 DB에는 tagcnt가 항상 있으므로 migrate가 모든 DB를 거부했을 결함이다. `de26ccd`에서 Unchecked/Unknown 범주를 index 등록과 `.hfile` pair 검사에서 제외했고, `TestInspectDoesNotParseTagCountFilesAsIndexes`로 고정했다.

한계: 정상 종료 DB의 소규모 읽기 증거다. counter/tagcount/visitor 읽기, 손상 주입, 32MiB rollover 이상 다중 block은 범위 밖이다.

## 3. 독립 리뷰와 반영

독립 리뷰에서 나온 항목을 아래와 같이 반영했다.

| 항목 | 반영 |
|---|---|
| relink 거부가 저장되지 않아 data 재성장 뒤 재오픈 시 relink됨 | 거부된 살아 있는 orphan에 deleted flag 2를 기록(tombstone). Go는 tombstone을 relink 후보에서 영구 제외. Java-deleted(flag 1)는 chain 구성원 유지. `TestRefusedRelinkStaysRefusedAfterDataRegrows`(plain/gzip), `TestRefusedRelinkIsTombstoned`, Java `verify-double-crash` |
| `Close`가 진행 중 `SyncData`의 옛 snapshot에 덮임 | `Close`가 `syncRun`→`mu` 순서. `TestCloseDoesNotLoseHeaderToConcurrentSync`(20회) |
| visitor eviction 저장·로드와 gate 밖 flush 경합 | eviction I/O를 `visitorIO`로 직렬화 |
| inspect 미리보기가 runtime 판단과 다름 | `AnalyzeRecoveryChecked`에 같은 data 검사 적용 |
| 쓰기 없는 날짜의 torn member와 Java rollback | [migration.md](../compatibility/migration.md)에 Java 재기동 gate(`zip.torn_member`·`index.unreachable_records`·`index.relink_refused` 금지) 명시 |
| cache 상한 문구 | [storage-contract](../compatibility/storage-contract.md)에 `cap` 여유와 snapshot 일시 할당 기록 |
| eviction이 gate 안에서 fsync 대기 | 미수정. 측정 후 판단할 후속 후보 |
| (기존) First 파일 중복 key의 Java HashMap 중복 제거 | 미수정. 비정상 파일에서만 발생. 후속 후보 |

tombstone 설계는 "거부 시 파일 무변경" 전제를 바꾸므로 migrate는 `index.relink_refused` index의 조회 비교를 건너뛰고(`query.not_compared`), 쓰기 open 뒤 변경은 `post_migration.changes`로 분류된다(`TestDoubleCrashIsWarningAndMigratesUnchanged`).

## 4. 성능 baseline (커밋 `ae7273b` 빌드, `scouter-replay`)

TPS 정의와 payload mix는 [Phase 2E 기록](2026-10-13-phase2e.md) §3과 같다(seed 42, warm-up 3s, steady 20s, 조회 worker 2, `GOMAXPROCS` 2). 서버 binary는 Phase 2E 기록의 것이 아니라 `ae7273b` 빌드다.

| run | 조건 | 재조회/예정 | UDP reject | TPS(drain 포함) | CPU s | max RSS MiB | history p95/p99 ms | 판정 |
|---|---|---|---|---|---|---|---|---|
| t-r200-o20 | 200 tx/s, 20 obj | 4600/4600 | 0 | 189.2 | 2.7 | 136 | 69/70 | PASS |
| t-r1000-o20 | 1000, 20 | 23000/23000 | 0 | 954.4 | 11.4 | 199 | 82/145 | PASS |
| t-r1000-o200 | 1000, 200 | 23000/23000 | 0 | 955.3 | 11.5 | 767 | 86/90 | PASS |
| t-r2000-o200 | 2000, 200 | 46000/46000 | 0 | 1912.1 | 13.3 | 849 | 143/243 | PASS |
| t-r2000-o200-sync0 | 2000, 200, sync off | 46000/46000 | 0 | 1877.1 | 13.4 | 872 | 120/238 | PASS |
| t-r200-o20-gzip | 200, 20, 압축 | 4600/4600 | 0 | 190.1 | 2.6 | 162 | 64/67 | PASS |
| t-r1000-o20-gzip | 1000, 20, 압축 | 23000/23000 | 0 | 956.5 | 12.2 | 372 | 99/166 | PASS |
| k-r1000-o20 | 1000, SIGKILL@15s | 15001/15001 | — | 999.6 | 5.1 | 196 | 82/82 | PASS(손실 0, 재기동 5.1s, `index-relinked` 1) |
| soak-r500-o200 | 500, 200, 600s | 300088/301500 | 1475 | 496.3 | 404.5 | 881 | 285/520 | 99.5% |
| t-r1000-o20-sync0 | 1000, 20, sync off | 22454/23000 | 172 | 886.3 | — | — | 621/1002 | 노이즈(송신 lag p99 277ms) |
| t-r2000-o200-p4 | 2000, 200, P4 | 35389/46000 | 2042 | 1422.1 | — | — | 2349/2435 | 노이즈(송신 lag p99 1107ms) |

마지막 두 run은 독립 검증의 Java 컴파일·replay와 같은 시간에 실행돼 생성기 송신 lag 자체가 수백 ms였다. 호스트가 유휴일 때 최종 커밋 `de26ccd` 빌드로 재측정한 결과는 다음과 같다.

| run (de26ccd) | 조건 | 재조회/예정 | UDP reject | TPS(drain 포함) | CPU s | max RSS MiB | history p95/p99 ms | lag p99 ms |
|---|---|---|---|---|---|---|---|---|
| t-r1000-o20-sync0 | 1000, 20, sync off | 23000/23000 | 0 | 945.9 | 11.1 | 197 | 99/127 | 5.5 |
| t-r2000-o200 | 2000, 200, P2 | 45905/46000 | 44 | 1908.7 | 13.6 | 827 | 143/266 | 4.0 |
| t-r2000-o200-p4 | 2000, 200, P4 | 45989/46000 | 9 | 1909.9 | 13.4 | 832 | 164/226 | 10.7 |

2000 tx/s·200 obj는 이 호스트의 처리 한계 부근이라 run마다 0~44건의 reject가 생긴다(`ae7273b` 행렬에서는 0). `GOMAXPROCS` 2→4는 처리량 차이가 없다(단일 Store gate). soak의 RSS는 60s 747MiB → 600s 817MiB로 증가 추세가 없고, `StorageErrors`·`Rejected`·recovery 0, tag top100 cache 실패 0이다.

[Phase 2E 기록](2026-10-13-phase2e.md) 대비 변화: 2000 tx/s·200 obj 42977→46000/46000(reject 1478→0), 압축 200 tx/s 1023→4600/4600(32.5→190.1 TPS), 압축 1000 tx/s 23000/23000, soak 294312→300088/301500(reject 7610→1475). 

## 5. 표준 검증 (최종 커밋 `de26ccd`, `git archive` 복사본)

| 명령 | 결과 |
|---|---|
| `make check` | **PASS exit 0** (190s) |
| `make race` | **PASS exit 0** (246s). storage 142.6s, migrate 147.5s(Java oracle 포함), legacyindex 37.1s, service 47.5s, 나머지 package 모두 ok |
| `make fixtures-check` | **PASS**. Java oracle fixtures 141개, source `dee47fb5` |
| `make inventory-check` | **PASS** |
| `make storage-check` | **PASS** (13s). `TestJavaStorageInterop`, `TestOriginalJavaStorageInterop` 실제 실행 |
| `make fuzz` (`FUZZTIME=10s`, 11 target) | **PASS** (131s). 11 target 모두 ok |
| `make build` + `java-integration` | **PASS** (21s). `integration_passed=True` |

gate 빌드 SHA256: `dist/scouter-go` `c87a482b8648c36e97ceeca1c23cbbf8339d7e75901af3c13674ec3c25ac6eba`, `dist/scouter-db` `68171584a2a1410de43d09e9e2ddde1d8ccb7f9ae66ab7a4ae13361deec2d4f5`, `dist/scouter-replay` `7922db05fe8563d0f599342e30057a2c60062ef49de4e720f4782e6997f5b0a8`.

## 6. 남은 문제

- 전원 손실에서 압축 `count.dat`이 block보다 먼저 기록되면 그 날짜의 압축 append가 `ErrCorrupt`로 멈춘다(다른 record를 돌려주지는 않음). block fsync 뒤 count 기록 순서는 후속 작업이다.
- 압축률이 Java보다 낮다(0.192 vs 0.145). rollover 시 재압축이 후속 후보다.
- eviction 경로의 gate 안 fsync 대기, First 중복 key(Java HashMap 의미)는 측정·조사 뒤 결정한다.
- 운영 DB 이관 리허설, Linux 목표 호스트, 전원 손실 실제 재현, Java 동일 조건 비교, 목표 처리량은 여전히 미검증이다.

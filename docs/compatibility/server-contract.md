# Scouter 서버 계약

정본은 [baseline.json](baseline.json)의 Scouter commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`, 버전 `2.21.3`이다. 아래 원본 경로와 줄 번호는 이 commit의 root 기준이다. Go의 구현 등록 상태와 실제 원본 실행·통합·운영 검증은 구분한다.

## 범위와 inventory

[source-inventory.json](source-inventory.json)은 운영 Scala 204파일/26,117줄, Java 77파일/11,846줄과 테스트 Java 9파일/991줄을 열거한다. `@ServiceHandler`는 34파일의 고유 명령 229개다. 명령 상수 이름과 literal은 별도 계약이다. 예를 들어 `RequestCmd.OBJECT_SOCKET`의 wire literal은 `SOCKET`이다. 모든 명령 상태는 초기 `not_implemented`이며 codec 구현만으로 서버 명령 지원 상태를 올리지 않는다.

| 기능군 | handler | 명령 수 |
|---|---|---:|
| 인증·계정 | LoginService, AccountService | 12 |
| 객체·thread·host | AgentInfo, ThreadList, HostService | 32 |
| counter·group·export | CounterService, DailyCounterService, SpecialCounterService, InteractionCounterService, GroupService, ExportService | 34 |
| XLog·profile·text | XLogService, HashText | 16 |
| 요약·tag·visitor·status·alert | SummaryService, SummaryEndUserAjax/Error/Nav, TagCountService, VisitorService, StatusService, AlertService | 26 |
| 설정·alert script | ConfigureService, AlertScriptingService | 24 |
| 진단·서버 정보 | DumpService, HeapDumpService, StackAnalyzerService, ServerDebug, ServerInfo | 28 |
| batch | BatchService | 5 |
| DB agent | DatabaseService, CubridService, MariaService | 37 |
| KV | KeyValueStoreService | 10 |
| 서버 관리 | ManageService | 5 |

`scouter.server/src/main/java/scouter/server/netio/service/ServiceHandlingProxy.java:59-104`는 annotation scanning과 reflection으로 명령을 등록한다. `scouter.handler` 사용자 handler는 별도 확장 계약이며 Go에서 자동 지원되지 않는다. `scouter.server/src/main/java/scouter/server/http/HttpServer.java:92-115`에는 `/counter/*`, `/register/*`, `/telegraf/*`와 선택적 webapp/WebSocket 로딩이 있다. JVM plugin JAR, alert script 실행과 Go 내장 기능은 동일하지 않다.

전체 호환 목표는 모든 명령의 request/response, 권한, agent 왕복, 실패 의미를 단계별로 검증하는 것이다. 초기 일반 관측 흐름은 login/session → object/text → realtime·기간 counter → XLog cursor/history/ID 조회 → profile → alert로 연결한다. 후속 관리·DB·batch 명령을 빈 성공 응답으로 대신하지 않는다.

## Phase 2D Go 구현 경계

정본229 handler를 READ184개와 상태 변경·관리45개로 분류한다. Phase 2D 당시 `Service.SupportedCommands()`는 Runtime의 AgentBackend가 제공될 때 179개 unique 구현 등록 명령을 반환했고, 나머지 JVM 의존 READ 5개는 별도 미지원 목록으로 분리했다. 지금은 그 5개도 JVM 실행 대신 descriptor 정적 표·load message ring·goroutine 정보로 응답하도록 등록되어 READ 184개와 상태 변경·관리 45개가 모두 구현 등록이다(registry 테스트가 read 184 / mutation 45를 검사한다). 아래 표는 Phase 2D 시점의 분류 기록이다. 임시 Go overlay 테스트로 registry를 실제 호출하고 [query-scope.json](query-scope.json)의 원본 분류와 집합 차이를 검사했다. static inventory의 초기 `not_implemented`는 그대로 유지한다.

| 구현 등록 군 | unique 수 | 경계 |
|---|---:|---|
| 2C 기본 조회·인증 | 27 | OBJECT_ENV는 아래 relay에 포함 |
| Counter·Special·Export·오늘 Object | 26 | 원본 숫자형·날짜·결함 동작, [counter-query-extended.md](counter-query-extended.md) |
| Status·Alert·Stack·Batch·Interaction·Summary·Group | 21 | PacketCommands23개 중 Batch Agent relay2개는 아래에 포함, [packet-query.md](packet-query.md) |
| Agent READ relay | 44 | 추가43+OBJECT_ENV; 실제 Agent 연결·지원 기능에 따라 결과 달라짐 |
| Maria/Cubrid counter | 18 | Status/counter 저장 조회와 Agent relay, [database-counter-query.md](database-counter-query.md) |
| 서버 metadata READ | 20 | local account/config/log/script-file 목록·내용, JVM 응답 위장 금지 |
| KV·query-list READ | 6 | 원본 TTL index와 기존 저장 데이터 조회; mutation API 제외 |
| Visitor·Tag | 12 | 원본 HLL/tag 파일과 조회, [aggregation-query.md](aggregation-query.md) |
| 추가 XLog·Trace | 5 | GXID/txid/search 및 B3 profile, [trace-query.md](trace-query.md) |

2D 당시 unsupported5개는 `GET_ALERT_REAL_COUNTER_DESC`, `GET_ALERT_SCRIPT_LOAD_MESSAGE`, `GET_PLUGIN_HELPER_DESC`, `SERVER_THREAD_DETAIL`, `SERVER_THREAD_LIST`였고 상태 변경45개는 READ 단계 범위에서 제외했다. 이후 3B에서 둘 다 구현 등록했다(unsupported 0). JVM 5개는 JVM 실행 없이 descriptor 정적 표·load message ring·goroutine 정보로 응답하며 script/plugin 실행은 제외 확정이다([management-commands](management-commands.md), [server-metadata-query](server-metadata-query.md)). 인식·구현 등록 수는 모든 요청의 byte-equivalence나 운영 호환 완료 비율이 아니다.

추가 built-in 수집은 DroppedXLog/Status/Interaction/Alert/Stack/Summary/Batch/SpanContainer를 처리한다. Status·Alert receiver time, receive TTL의 Interaction replacement, driving XLog와 지연 Profile/XLog·Dropped의 sampling 상태를 분리한다. Batch의 implicit Object 등록, 원본 Stack/Batch 저장, Batch archive upload/query도 연결한다. MapPack·standalone SpanPack은 extension chain 경계이며 현재 Go built-in ingest에서 unsupported로 계수한다.

Agent pool은 object별 idle connection checkout, 최대1초 대기와 context 취소, 반환 시 FIFO tail 이동을 사용한다. idle5초 이후 KEEP_ALIVE를 최대3초 기다리고 실패한 slot을 회수한다. 대기 1초와 idle 5초는 Java 기본값이며 live 설정 `net_tcp_get_agent_connection_wait_ms`·`net_tcp_agent_keepalive_interval_ms`를 reload마다 반영한다(`internal/server/agent_checkout.go`). pool waiter 취소가 다른 request owner의 socket을 닫지 않으며 shutdown 시 transport context와 socket 종료 후 handler를 join한다. Blob relay는 완전히 읽은 chunks와 upstream error를 함께 전달하여 이미 작성한 응답을 flush하고 성공 NoNEXT를 보내지 않는다.

`GET_CONFIGURE_TELEGRAF`는 `--telegraf-config`의 현재 파일 또는 정본 `TgConfig.getSampleContents()`로 생성한 embedded sample XML을 READ한다. 존재하는 빈 파일은 빈 text이며, 없는 파일/parent만 sample fallback이다. 이 문단을 쓸 당시(2D) 미구현이던 Telegraf metric 수집·설정 적용, HTTP servlet/webapp, LDAP는 3B, retention scheduler는 2E, Pulse는 3단계에서 구현했다. JVM plugin/script 실행은 제외 확정이고 단일 credential 모드의 raw login은 지원하지 않는다([runtime](runtime.md) "남은 범위"). 파일 DB의 원본 format 검증과 운영 DB 이관·복구 검증은 별도 gate다.

선행 단위와 최종 검증 상태는 [Phase 2D 기록](../validation/2026-10-13-phase2d.md)을 따른다. 전체 check/race와 Java56 명령의 synthetic 통합은 PASS다(위 Phase 2D 기록). 전체 check/race 이후 마지막 Interaction compaction 변경은 ingest 전체 race로 재검증했고, 최종 재빌드 binary의 Java56 통합도 PASS다. counter·Interaction TTL bulk cleanup은 반복 slice 삭제 대신 stable compaction을 사용한다.

## 조회와 집계 의미

| 계약 | 원본 근거 |
|---|---|
| login 응답은 session, 서버 시간/ID/버전, 권장 client 버전, group policy, timezone, menu, timeout, 외부 링크 포함 | `scouter.server/src/main/scala/scouter/server/netio/service/handle/LoginService.scala:42-86` |
| 수신 PerfCounter는 time/timetype 기본값을 적용하고 **data.objHash와 data.time을 주입/덮어쓴 뒤** Core에 전달. 저장 row·cache·표본에도 metadata가 포함됨 | `scouter.server/src/main/scala/scouter/server/netio/data/NetDataProcessor.scala:137-148`, `scouter.common/src/main/java/scouter/lang/counters/CounterConstants.java:21-22` |
| realtime counter는 저장·cache·5분 표본·alert·CPU 점검으로 전달. 비 realtime은 날짜와 hhmm로 저장. PerfCountCore의 metadata 추가 주석은 앞선 수신 단계의 실제 주입을 취소하지 않음 | `scouter.server/src/main/scala/scouter/server/core/PerfCountCore.scala:39-70` |
| 자동 5분 저장은 마지막 numeric/boolean 단일 표본. 평균·최댓값 아님. Auto5MSampling에 FIVE_MIN 제외 조건이 있으나 정본 PerfCountCore의 add 호출은 realtime branch에만 있음 | `scouter.server/src/main/scala/scouter/server/core/Auto5MSampling.scala:40-77`, `PerfCountCore.scala:46-69` |
| realtime XLog는 loop/index cursor, 객체 필터, elapsed 하한을 사용 | `scouter.server/src/main/scala/scouter/server/netio/service/handle/XLogService.scala:166-244` |
| history V1은 date/stime/etime/max/reverse/objHash, elapsed 필터는 strict greater-than. V2는 lastBucketTime+txid/pageCount와 metadata | `scouter.server/src/main/scala/scouter/server/netio/service/handle/XLogService.scala:254-367` |
| COUNTER_PAST_TIME은 stime의 날짜 하나를 선택. 날짜를 가로지르는 결과 확장은 별도 계약 변경 | `scouter.server/src/main/scala/scouter/server/netio/service/handle/CounterService.scala:222-250` |

정렬, NULL/0, hash signedness, timezone, cursor 재개, 빈 목록, profile 미존재·보존 만료를 golden 응답으로 고정한다. 모든 명령의 응답이 MapPack이라고 가정하지 않는다. Pack ID 목록은 inventory의 `pack_types`이며 ID 열거는 Pack body 호환의 증거가 아니다.

## 저장과 이관

외부 DB가 아닌 날짜별 사용자 정의 파일 DB다. wire 호환과 기존 파일 읽기·이관 호환은 독립 gate다. 원본 DB는 읽기 또는 복사본 조사만 허용한다.

| 데이터 | 저장 계약·원본 근거 |
|---|---|
| XLog | `yyyyMMdd/xlog/xlog.service`; time/txid/gxid 인덱스. txid/gxid key 8byte, 위치 5byte, gxid=0 제외. `scouter.server/src/main/scala/scouter/server/db/XLogWR.scala:52-76,109-137`, `scouter.server/src/main/scala/scouter/server/db/xlog/XLogIndex.scala:61-90` |
| profile·압축 | 비압축 XLog 길이 prefix 2byte, profile 4byte. 파일별 `.service.conf/.profile.conf` 압축 여부를 읽고 압축 시 공용 GZipStore 사용. `scouter.server/src/main/scala/scouter/server/db/xlog/XLogDataWriter.scala:49-82`, `scouter.server/src/main/scala/scouter/server/db/xlog/XLogProfileDataWriter.scala:49-84` |
| realtime counter | 날짜별 `counter/real`, header dictionary + objHash/time 인덱스 + data. `scouter.server/src/main/scala/scouter/server/db/counter/RealtimeCounterDBHelper.scala:120-144`, `scouter.server/src/main/scala/scouter/server/db/counter/RealtimeCounterIndex.scala:51-71` |
| 기간 counter | 날짜별 `counter/5m`; CounterKey와 고정 bucket. 1분 1440, 5분 288, 10분 144, 시간 24, 일 1. `scouter.server/src/main/scala/scouter/server/db/DailyCounterWR.scala:43-68,115-140`, `scouter.server/src/main/scala/scouter/server/db/counter/DailyCounterUtils.scala:41-75` |
| text | 날짜별 text와 영구 `00000000/text/text_${div}`; div/hash 참조·중복 제거. `scouter.server/src/main/scala/scouter/server/db/TextWR.scala:92-123,169-173`, `scouter.server/src/main/scala/scouter/server/db/TextPermWR.scala:98-121` |
| KV | 영구 `00000000/kv2/kv_${div}`, UTF-8 value append + IndexKeyFile2 update/TTL. `scouter.server/src/main/scala/scouter/server/db/KeyValueStoreRW.scala:50-83,102-141`, `scouter.server/src/main/scala/scouter/server/db/kv/KeyValueStoreIndex.scala:68-86` |

Go는 object/alert/status/summary/visitor/tagcnt/batch/span과 KV TTL index·Batch archive의 원본 format 구현을 추가했다. synthetic 원본 reader/writer 검증은 각 단위에 기록하지만 운영 DB를 별도로 이관·검증해야 한다. 새 서버는 원본과 분리된 directory에 쓴다. Java와 Go가 같은 파일 DB를 동시에 쓰지 않는다. 복사본 reader 또는 importer → backfill → watermark·누락/중복 검증 → canary → rollback gate 순으로 전환한다.

## 보존 정책

`scouter.server/src/main/java/scouter/server/Configure.java:772-792,337` 기본값: purge 활성, disk 80%, profile 10일, XLog 30일, counter/realtime/tag/visitor 70일, daily text 140일, summary 60일. profile/XLog의 0은 fallback이며 counter의 0은 기간 삭제 비활성화다.

`scouter.server/src/main/java/scouter/server/core/AutoDeleteScheduler.java:27,106-175`는 1분마다 검사하고 디스크 압박 시 오래된 profile → XLog/span → 날짜 전체 순으로 삭제한다. 기본 disk 설정에서 임계값은 72%/76%/80%다. 기간 경계 이하를 삭제하며 당일은 제외한다. `:341-420`은 파일별 삭제 범위와 `0000` 포함 directory 제외를 보여준다. 영구 text/KV는 날짜 retention 대상과 다르다.

현재 삭제 구현은 `File.delete()` 반환값을 검사하지 않는다. 이를 성공 계약으로 재현하지 말고 실패 관측과 의도된 변경을 기록한다. 압축 공용 block·열린 reader/writer·지연 packet과 purge의 상호작용은 추가 검증이 필요하다.

## Lifecycle와 설정

- 기동: Configure → DB lock → alert/counter/account → UDP/service registry/TCP/HTTP → plugins → processor → purge → shutdown hook. 근거: `scouter.server/src/main/java/scouter/server/Main.java:45-65`.
- DB lock은 `lock.dat` mtime을 매초 갱신하고 5초 이상 오래된 lock을 제거한다. 단일 writer 의도를 보존하되 stale 판정과 중복 기동은 재현해야 한다. 근거: `scouter.server/src/main/scala/scouter/server/db/DBCtr.scala:39-61`, `Main.java:88-95`.
- 종료는 등록 callback 호출·clear이다. 검토한 ShutdownManager/CoreRun/DBCtr/XLogWR 경로에는 전체 queue drain와 thread join 완료 보장이 없다. Go 종료는 수신 중지 → queue drain 또는 명시적 실패 집계 → 저장 flush/close → 종료 완료를 검증한다. 근거: `scouter.server/src/main/scala/scouter/server/ShutdownManager.scala:32-43`, `scouter.server/src/main/scala/scouter/server/core/CoreRun.scala:9-17`, `XLogWR.scala:52-83`.
- 설정은 `scouter.config` 또는 `./conf/scouter.conf`, 3초마다 mtime 확인 후 Properties 치환·적용이다. 기본 db_dir/log_dir은 `./database`/`./logs`, XLog queue 10000, profile queue 1000, TCP service pool 100, 검색 최대 500. 객체 초기화 시 queue 크기를 읽는 경로는 파일 reload와 실제 queue 재설정을 구분한다. 근거: `scouter.server/src/main/java/scouter/server/Configure.java:569-599,638-671,716-718,826-828`.

## 검증과 성능 기준

목표 호스트는 **다른 서비스와 공유하는 서버**일 수 있다. CPU quota·RSS 예산·디스크 처리량을 전용 자원으로 가정하지 않는다. 운영 규모와 처리량 목표는 환경별 요구이며 이 프로젝트 실측이 아니다. TPS는 완료 XLog 수로 정의하고 packet/profile/byte rate를 별도 기록한다.

소스상 후보는 bounded queue reject, 단일 writer·3중 인덱스, 단건 synchronized write/flush, 압축·조회 decode 비용이다. `scouter.common/src/main/java/scouter/util/RequestQueue.java:89-97`은 포화 시 false, `XLogWR.scala:86-90`은 로그만 남긴다. `scouter.server/src/main/scala/scouter/server/db/io/RealDataFile.scala:29-32,75-76`의 flush는 fsync 보장을 의미하지 않는다. `scouter.server/src/main/java/scouter/server/db/io/zip/GZipStore.java:67-94`의 dirty idle block은 10초 기준 store된다. 이는 운영 병목/손실을 측정한 결과가 아니다.

추가 gate:

1. 같은 packet corpus로 Java/Go 응답의 필드·순서·cursor·날짜·NULL/0 비교, 실제 Agent/Client 화면 검증.
2. 기존 DB 복사본 읽기, restart, 잘린 tail, index/data 불일치, disk full, 압축/비압축, profile 만료 검증.
3. 자정·timezone·역순 packet·queue 포화/회복·SIGTERM 잔여 queue·중복 기동·active query 중 purge 검증.
4. 동일 payload mix·조회/retention 병행·자원 한도·fsync 정책에서 EPS/MBps/p95/p99/CPU/RSS/alloc/reject와 종료 잔여 수 비교.

기존 테스트 9파일은 설정·counter 정의·KV·Telegraf parser 등이 중심이다. 전체 XLog/retention/종료 호환을 증명하지 않는다. 각 기능은 fixture → 실제 Agent/Client 통합 → 운영 gate의 근거를 별도로 남긴다.

## Inventory 재현

```sh
python3 scripts/source_inventory.py
python3 scripts/source_inventory.py --check
SCOUTER_SOURCE=../scouter python3 scripts/source_inventory.py --check
python3 scripts/source_inventory.py --source ../scouter --check
```

생성기는 baseline SHA/version과 clean 원본을 검사하며 unresolved handler 상수·지원하지 않는 표현식·중복 명령은 오류로 처리한다. JSON에는 원본 상대 경로/줄 번호/파일 SHA-256과 Pack ID를 포함하고 실행 시간·절대 경로는 넣지 않는다. `--check`는 기존 파일의 byte 비교만 수행하고 파일을 생성·수정하지 않는다. 상태 변경은 실제 서버 구현 전수 확인과 검증 근거가 필요하며 이 생성기의 초기 상태를 완료 추정으로 사용하지 않는다.

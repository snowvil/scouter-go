# Maria/Cubrid DB 조회 계약

정본은 `baseline.json`의 Scouter commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. `MariaService.scala`의 10개 local READ와 `CubridService.scala`의 8개 local READ를 대상으로 한다. `CUBRID_GET_ALERT_CONFIGURE`는 Agent read relay 소유이며 `CUBRID_SET_ALERT_CONFIGURE`와 `DB_KILL_PROCESS`는 이 목록에 포함하지 않는다.

`internal/service/database_counters.go`는 기존 `Backend.Counter/CounterDaily`, `CounterBulkRowsBackend`, `StatusBackend`를 사용한다. 선택 기능 backend가 없으면 `ErrUnsupported`이며 빈 성공으로 대체하지 않는다. 인증 검증 후 MapPack 한 개를 읽고 response item을 기록한다. `NoNEXT`는 TCP dispatcher가 소유한다.

## 응답과 계산

| 명령 | 원본 위치 | 응답/계산 |
|---|---|---|
| `DB_REALTIME_CONNECTIONS` | Maria:45 | HasNEXT + **MapValue**. REALTIME THREADS_TOTAL/THREADS_RUNNING의 CastUtil.clong 합, total/active DecimalValue |
| `DB_REALTIME_ACTIVITY` | Maria:74 | HasNEXT + **MapValue**. COM_CALL_PROCEDURE/SELECT/UPDATE/INSERT/DELETE 합, call/select/update/insert/delete DecimalValue |
| `DB_REALTIME_HIT_RATIO` | Maria:160 | HasNEXT + **MapPack**. 각 INNO_BUF_HIT/KEY_RATIO/QCACHE_RATIO/TCACHE_RATIO의 존재하는 객체 수로 float32 평균. 없는 counter의 key 생략 |
| `DB_REALTIME_RESPONSE_TIME` | Maria:124 | 객체별 HasNEXT + MapPack(objHash DecimalValue, value **DoubleValue**). cache digest의 LAST_SEEN > now-300000만 AVG_TIMER_WAIT×COUNT_STAR를 합산하고 int32 COUNT_STAR 합으로 나눔. 양수 count 없으면 0 |
| `DB_DAILY_CONNECTIONS` | Maria:212 | MapPack(time,total,active). sdate/edate 양끝 날짜 포함, FIVE_MIN, 누락 슬롯 0 **DoubleValue**. 객체별 누적 |
| `DB_DAILY_ACTIVITY` | Maria:267 | MapPack(time,call,select,insert,update,delete). 위와 같은 5분 누적; realtime과 insert/update key 삽입 순서가 다름 |
| `DB_DIGEST_TABLE` | Maria:353 | 선택 digest StatusPack마다 percent MapPack → (objHash,digestHash)별 summary MapPack → 객체별 마지막 StatusPack. LAST_SEEN이 stime/etime 양끝 포함. MIN 최소, MAX 최대, **AVG는 long 합**, count는 snapshot 수 |
| `DB_LAST_DIGEST_TABLE` | Maria:436 | time-300000..time의 reverse scan, 객체별 첫 digest StatusPack 한 개. 요청 중복 hash는 set으로 제거 |
| `DB_MAX_TIMER_WAIT_THREAD` | Maria:467 | MapPack(time,thread_id,timer,lock_time,sql_text). statements_current snapshot에서 첫 일치 digest만 검사, 양수 최대 TIMER_WAIT 선택. 후보 없으면 원본과 같이 빈 MapPack |
| `DB_LOAD_DIGEST_COUNTER` | Maria:521 | 객체별 MapPack(objHash,time,value). digest의 첫 일치 행에서 column의 원래 Value 보존. 요청 중복 hash는 map으로 제거. 누락 객체도 빈 time/value lists |
| `CUBRID_ACTIVE_DB_LIST` | Cubrid:106 | 요청 hash 순서대로 StatusCache(objHash,key)의 StatusPack; 누락 생략, 중복 요청 보존 |
| `CUBRID_DB_REALTIME_DML` | Cubrid:55 | HasNEXT + **MapValue**. num_query_selects/updates/inserts/deletes long 합. **dbname은 db_num을 long으로 변환한 문자열 연결**이며 숫자 합이 아님 |
| `CUBRID_DB_REALTIME_MULTI_DATA` | Cubrid:238 | HasNEXT + **MapValue**. 요청 counter key에 REALTIME CastUtil.clong 합 |
| `CUBRID_DB_PERIOD_MULTI_DATA` | Cubrid:192 | readBulk의 embedded objHash/time을 사용한 MapPack(objHash,time,value). 요청 순서/중복 보존. 누락 counter는 제외, 존재하는 NullValue는 보존 |
| `CUBRID_DB_LONG_PERIOD_MULTI_DATA` | Cubrid:264 | sDate/eDate의 날짜 → 요청 hash 순서로 FIVE_MIN MapPack(objHash,time,value). 누락 daily 배열도 빈 lists로 응답 |
| `CUBRID_DB_REALTIME_STATUS` | Cubrid:126 | reverse status read의 첫 selected 객체/key match를 hash 수만큼 반복하는 원본 control-flow 계약을 보존 |
| `CUBRID_DB_SERVER_INFO` | Cubrid:159 | 위와 동일 |
| `CUBRID_DB_LONG_TRANSACTION_DATA` | Cubrid:313 | 위와 동일 |

REALTIME 합계/비율의 hash list가 비면 응답 item이 없다. 비지 않은 hash list에서 counter가 모두 없으면 합계는 0이며 hit ratio는 빈 MapPack이다. StatusCache 조회의 누락 객체는 응답 item이 없다.

MapPack key 삽입 순서와 list 순서는 보존한다. MapValue는 Java Hashtable이므로 insertion order 계약이 없고 key별 의미로 검증한다. Java HashMap 기반 digest summary/last/series 객체 응답 간 순서는 보장되지 않는다. Go는 first-seen 순서를 안정적으로 선택하며 각 그룹 내부와 progress→summary→last 순서는 유지한다.

MapPack/ListValue Number getter는 DecimalValue/FloatValue/DoubleValue만 숫자로 취급한다. int32 Decimal narrowing은 Java와 같이 wrap한다. CounterCache 수치 변환의 CastUtil은 parseable TextValue도 받아들이며 null/변환 불가 값은 0이다. FloatValue 누적은 float32, daily/응답 시간은 float64, 실시간 합과 digest summary는 int64다.

## 예외와 제한

- context 취소, backend 오류, 응답 쓰기 오류는 전파한다. 기존 Scala StatusRD의 광범위한 catch로 오류를 숨기는 동작은 복제하지 않는다.
- 객체/hash/record/series 작업량은 100000 cell, 날짜는 3660일 상한을 적용하고 daily 전체 response의 key×슬롯 수도 제한한다. 원본의 무제한 할당을 제한하는 의도된 차이다.
- 빠진/잘못된 필수 ListValue, 정렬되지 않은 status column 길이, 역전 기간, 0폭 digest 기간은 `ErrMalformed`다. 원본 NPE/0나눗셈/부분 성공을 정상 계약으로 삼지 않는다.
- daily 원본은 고정 86400000ms로 날짜를 증가시킨다. Go도 이를 보존한다. FIVE_MIN 배열이 288보다 긴 잘못된 daily 데이터는 거절한다.
- Cubrid status 세 명령의 lambda `return`은 Scala NonLocalReturnControl이다. `StatusRD.readFromEndTime`의 `catch { case e: Throwable => }`이 이를 삼켜 해당 read를 중단하며 outer object loop는 다음 같은 scan을 실행한다. 이 원본 동작은 actual Maria/Cubrid handler + StatusRD/index/reader/writer oracle에서 hash43 최신 status가 두 번 나오는 결과로 확인했다.
- StatusBackend는 정본 forward index scan 순서의 owned snapshot을 반환해야 한다. 실제 Store는 index time과 StatusPack.Time 불일치를 ErrCorrupt로 거절한다. 정본에서는 손상 index의 시간으로 progress를 계산할 수 있으므로 이 경우는 의도된 실패 차이다.

## 검증 현황

실행한 `.tools/go/bin/go test ./internal/service -run '^TestDatabase' -count=1`, 동일 `-race` 실행, `.tools/go/bin/go vet ./internal/service`는 pass다.

`TestDatabase*`는 실제 TCP loopback에서 18개 명령의 응답 family, 누락/빈 데이터, 중복 hash, daily DoubleValue 및 key 순서, digest progress/합산/최신/첫 행 선택을 확인한다. 인증/취소/관리 명령 미인식도 검사한다. Go round-trip만으로 actual Java/Scala handler 호환을 선언하지 않는다. `TestDatabaseOriginalScalaHandlerWireOracle`는 `python3 scripts/java-storage.py --generate-db-handlers <temporary-dir>`로 고정 source를 매번 컴파일하고 8개 request를 실제 Go TCP에 재생한다. connections/activity/Cubrid DML은 MapValue key 순서만 정규화한 Value byte 비교, hit ratio/response time/Cubrid status/server info는 전체 response byte 비교, digest table은 마지막 StatusPack.Data의 MapValue key 순서만 정규화한 Pack byte 비교를 통과했다. progress 2개 → Decimal AVG 합 50/count 2 summary → 마지막 StatusPack의 순서와 scalar 형식은 원본과 일치한다. 마지막 StatusPack.Data의 AVG/MAX key 순서는 Java Hashtable이 파일을 decode 후 재-encode할 때 바뀌므로 raw 순서 동일성을 요구하지 않는다.

actual oracle에는 CounterCache/StatusCache 입력 공급과 DB root/lifecycle 경로 seam이 있다. 계산 handler와 StatusRD/index/reader/writer/codec는 정본 소스를 실행한다. 이는 위 8개 합성 시나리오의 handler fixture 증거이며 실제 Agent/Client 전체 명령 통합·운영 부하를 증명하지 않는다. 나머지 10개 명령은 source 분석과 Go TCP/race 검증 증거다.

원본 Maria/Cubrid 및 StatusRD/Value 구현의 계약을 참고한 독립 Go 구현이며 원본 파일은 수정하지 않는다. 원본 저작권 및 Apache-2.0 고지는 Scouter 원본 파일과 이 저장소 LICENSE/NOTICE를 따른다.

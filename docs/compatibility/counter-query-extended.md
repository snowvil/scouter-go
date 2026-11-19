# Counter·Special·Export·Object 조회 확장

정본은 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. `query-scope.json`은 작성 시점 Go 구현의 지원 28개를 제외한 201개 조사 목록이다. 현재 지원 상태를 자동 판정하는 파일이 아니다.

이번 단위는 CounterService 7개, DailyCounterService 10개, SpecialCounterService 6개, ExportService 2개, OBJECT_TODAY_FULL_LIST 1개다. `ExtendedCounterCommands()`는 26개를 반환하고 `Service.HandleExtendedCounterCommand(ctx, request, stream)`은 `(recognized, error)`를 반환한다. 메인 dispatcher가 기존 MapPack 읽기 **전**에 호출한다. OBJECT_TODAY_FULL_LIST만 bodyless다. 성공 응답 끝의 NoNEXT는 TCP transport가 작성한다.

## Backend 경계

기존 Backend의 Objects/Object/Counter/CounterHistory/CounterDaily를 사용한다. 다음 optional interface가 없으면 해당 명령은 명시적 unsupported 오류를 반환한다.

```go
type CounterCatalogBackend interface {
    CounterNames(context.Context, int32) ([]string, error)
    CounterNamesForDate(context.Context, string, int32) ([]string, error)
    CounterType(context.Context, string) (CounterTypeMetadata, error)
}
type CounterTypeMetadata struct {
    DisplayName string
    Counters []string
}
type CounterRow struct {
    Time int64
    Values protocol.Value // ValueMap
}
type CounterRowsBackend interface {
    CounterRows(context.Context, string, int32, int64, int64) ([]CounterRow, error)
}
type CounterBulkRowsBackend interface {
    CounterBulkRows(context.Context, string, int64, int64) ([]CounterRow, error)
}
```

Catalog은 현재 counter names, 날짜별 저장 names, Counter XML의 default counters/display name을 구분한다. CounterRows는 Export에 쓰며 원본 row 순서·중복 timestamp·모든 요청 counter가 missing인 row도 보존한다. 이름별 CounterHistory를 합치는 방식으로 대체하지 않는다.

BulkRows는 `RealtimeCounterRD.readBulk`의 1분 구간별 index first/last physical offset 사이 연속 data rows를 반환한다. GROUP 응답은 **row 안의 `objHash`/`time`**을 읽는다. index metadata로 보충하지 않는다. 원본 NetDataProcessor는 PerfCountCore 호출 전에 두 필드를 주입한다. Go 수신 경로도 동일하게 주입한다. PerfCountCore 내부의 주석은 이 바깥 수신 처리를 제거하지 않는다. 저장 RD에 직접 기록한 legacy/raw row에만 metadata가 없을 수 있고, 이 경우 missing metadata는 0으로 처리된다.

Backend 결과는 detached snapshot이며, handler는 cache 객체를 수정하지 않는다. Object tags의 status와 NumberValue sum도 copy에 적용한다.

## 명령별 계약

표에서 `P`는 MapPack, `[]`는 ListValue다. 모든 응답 항목 앞에는 HasNEXT가 붙는다.

| 명령 | request | response와 selector |
|---|---|---|
| COUNTER_REAL_TIME_OBJECT_ALL | P{objHash} | P{counter[],value[]} 1개, empty도 빈 lists; Catalog 필요 |
| COUNTER_REAL_TIME_OBJECT_TYPE_ALL | P{objType} | live object의 objName→counter MapValue를 담은 P; empty object 생략 |
| COUNTER_REAL_TIME_ALL_MULTI | P{counter[],objType 또는 objHash[]} | P{objHash[],counter[],value[]} 1개; explicit hashes 우선, 요청 순서·중복 보존, missing 생략 |
| COUNTER_REAL_TIME_GROUP | P{counter,objHash[]} | P{objHash[],value[]} 1개; 요청 순서·중복 보존 |
| COUNTER_REAL_TIME_TOT | P{counter,objType,mode} | live cache hit 합계/avg DoubleValue 1개; avg 대소문자 무시, missing 전체=0 |
| COUNTER_PAST_TIME_GROUP | P{counter,objHash[],stime,etime} | BulkRows embedded metadata 기반 객체별 P{objHash,time[],value[]}; 요청 순서·중복, missing은 빈 lists |
| COUNTER_PAST_TIME_TOT | P{counter,objType,stime,etime,mode} | empty면 P{time[],value[]} 1개; nonempty는 원본 TimedSeries 예외에 대응하는 명시적 실패 |
| COUNTER_TODAY_ALL | P{counter,objType} | 현재 등록 객체, inactive 포함; series 없는 객체 생략 |
| COUNTER_TODAY_GROUP | P{counter,objHash[]} | 요청 순서, series 없어도 빈 lists |
| COUNTER_PAST_DATE_ALL | P{counter,date,objType} | 날짜별 객체, series 없어도 빈 lists |
| COUNTER_PAST_DATE_GROUP | P{counter,date,objHash[]} | 요청 순서, series 없어도 빈 lists |
| COUNTER_PAST_LONGDATE_ALL | P{counter,sDate,eDate,objType 또는 objHash[]} | 날짜→객체 순서로 series P; explicit hashes 우선 |
| COUNTER_PAST_LONGDATE_GROUP | P{counter,stime,etime,objHash[]} | 각 날짜 전체 포함; missing 하루는 288 NullValue; 객체별 P 1개 |
| COUNTER_TODAY_TOT | P{counter,objType,mode} | 원본 singleton 배열 결과 그대로: 초기 sum=288/count=288, slot0 양수만 추가, 뒤 양수 slot 예외 이후 누적 결과 반환 |
| COUNTER_PAST_DATE_TOT | P{counter,date,objType,mode} | 288 DoubleValue slots; nonnull 전체가 합계/평균에 포함(0·음수 포함) |
| COUNTER_PAST_LONGDATE_TOT | P{counter,sDate,eDate,objType,mode} | 기간 288×일수 slots; 양수만 합계/평균에 포함 |
| GET_COUNTER_EXIST_DAYS | P{counter,objType,duration,etime} | P{date[],exist[]} 1개; duration+1일, series 길이>0이면 true |
| ACTIVESPEED_REAL_TIME | P{objType} | 등록 객체(inactive 포함)별 P{objHash,act1,act2,act3}; missing/짧은 list=0 |
| ACTIVESPEED_GROUP_REAL_TIME | P{objHash[]} | 요청 객체 순서 per-object P, unknown도 0 |
| ACTIVESPEED_REAL_TIME_GROUP | P{objType} | live만 합산한 P{act1,act2,act3,tps:FloatValue} 1개 |
| ACTIVESPEED_GROUP_REAL_TIME_GROUP | P{objHash[]} | 알려진 live hash만 합산; 중복 hash는 중복 합산 |
| SHOW_REAL_TIME_STRING | P{objType,counter} | P{desc,result:NumberValue}; 첫 값의 숫자형 유지. Decimal first는 다음 값을 long으로 변환, Float first는 float32로 변환 |
| COUNTER_MAP_REAL_TIME | P{objHash[],counter} | cache ValueMap을 **Value**로 반환(MapPack 아님); missing cache는 원본 NPE 대응 실패 |
| EXPORT_OBJECT_TIME_COUNTER | P{objHash,date,stime,etime} | 먼저 P{values:[time,정렬 counter명…]}, 이후 row마다 P{values:[hhmmss,값…]}; missing은 TextValue("") |
| EXPORT_OBJECT_REGULAR_COUNTER | P{objHash,date,timeType} | default+stored counter names 정렬 header, 하루 slots의 P{values:[HH:mm,값…]}; missing은 NullValue |
| OBJECT_TODAY_FULL_LIST | body 없음 | 오늘 저장 Object 순서; 현재 객체로 교체 후 active/inactive, 현재 없으면 저장 객체에 dead/alive=false |

날짜는 `scoutertime`의 원본 고정 24시간 calendar를 사용한다. LONGDATE 범위는 마지막 날짜를 포함한다. GROUP의 stime/etime 시각으로 하루 slots를 자르지 않는다. periodic object selector는 동일 hash의 첫 날짜 ObjectPack을 유지한 뒤 objType을 필터한다. hash/cache 열거 순서는 정렬된 응답 계약이 아니며 Backend가 반환하는 snapshot 순서를 유지한다. Export counter column만 원본대로 문자열 정렬한다.

## 실제 원본 실행으로 확정한 결함

`scripts/java-storage.py --generate-counter-handlers DIR`은 복제 handler가 아닌 원본 Scala DailyCounterService/CounterService와 TimedSeries, Java NumberValue를 실행한다.

- TODAY_TOT avg empty/zero: time/value 각 1개, value=1.0.
- TODAY_TOT positive12: 300/289. positive12+24: index1 예외를 내부 catch하고 동일 결과를 반환.
- PAST_TIME_TOT empty: HasNEXT+빈 time/value MapPack. nonempty: `TimedSeries.addEnd` ClassCastException, response body 0 bytes.
- NumberValue Decimal1+Double2.5=Decimal3, Float1.25+Double2.5=Float3.75. Double first의 두 번째 nonnull add는 ClassCastException.
- PAST_TIME_GROUP 원본 RD에 직접 기록한 metadata 없는 cpu row: 요청 hash42의 time/value 빈 lists. 수신 경로 row와 구분한다. embedded objHash42/time1000 row: time1000/value12 한 점. index metadata로 채우지 않는다.

원본 결함을 정상 aggregate로 수정해 호환이라고 주장하지 않는다. Go는 해당 실패를 명시적 error로 반환하며 성공 terminator를 보내지 않는다. cache mutation은 재현하지 않는다. 원본과 transport의 buffered partial-output 처리 차이는 별도 통합 검증 대상이다.

## 상한·취소·검증

명령당 100000 query cells, 최대 3660 dates를 둔다. 실시간 Cartesian selector도 계산 전에 제한하고, long daily aggregate 288×일수도 제한한다. 별도 transport frame byte 상한이 적용된다. 상한 초과는 partial 성공이 아닌 명시적 ErrLimit이다. 유효하지 않은 date/timeType/inverted time range는 malformed로 처리한다. 원본의 timeType=4 zero-step 무한 loop는 재현하지 않는다.

Backend 호출에 request context를 전달하고 긴 조회 루프에서도 취소를 확인한다. Backend는 자신의 lock/I/O 경계에서 context를 처리해야 한다. 동기 disk syscall을 강제 중단한다는 보장은 없다.

loopback tests는 request 순서·중복, live/current/dated 차이, missing/NullValue, bulk embedded metadata, full-date slots, numeric receiver type와 long→float direct rounding, bodyless pipeline, 오류/취소/상한을 확인한다. Java 생성 raw body와 Go TCP body를 TODAY_TOT 4개, PAST_TIME_TOT 2개, PAST_TIME_GROUP 2개 case에서 비교한다. 모든 26개 명령의 실제 Agent/Client 및 운영 부하 검증 완료를 뜻하지 않는다.

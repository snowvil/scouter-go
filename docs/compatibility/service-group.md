# 서비스 그룹 집계 계약

기준 원본 SHA는 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. 구현 범위는 WEB_SERVICE/APP_SERVICE XLog의 group 산정, 객체·group별 meter, REALTIME_SERVICE_GROUP 조회, 매분 5분 counter 저장이다. 그룹 상태는 Engine mutex가 소유하며, 일반 수집 queue·sampling·Client 인증은 상위 계층의 계약을 따른다. source fixture 또는 실제 Client 상호운용 완료 여부는 아래 검증 경계와 구분한다.

## 정본과 처리 순서

| 계약 | 원본 근거 |
| --- | --- |
| WEB_SERVICE/APP_SERVICE에서 group 계산 후 meter queue로 전달 | `../scouter/scouter.server/src/main/scala/scouter/server/core/XLogCore.scala:40` 및 `:56` |
| 전달된 group이 0이 아니면 보존; service 사전을 XLog endTime 날짜로 조회 | `../scouter/scouter.server/src/main/scala/scouter/server/core/app/XLogGroupUtil.scala:38` |
| service→group 50,000개; 0 hash는 캐시하지 않음 | 같은 파일 `:36`, `:43` |
| meter key는 objHash/group; 최대 2,000개 | `../scouter/scouter.server/src/main/scala/scouter/server/core/app/XLogGroupPerf.scala:50`, `:111` |
| count 증가, elapsed 합계, error!=0이면 error 증가 | `../scouter/scouter.server/src/main/scala/scouter/server/core/app/MeterService.scala:47` |
| 1초 단위 301개 ring; 현재 초부터 역순 period개 합산 | `../scouter/scouter.common/src/main/java/scouter/util/MeteringUtil.java:26`, `:55`, `:87` |
| 합계 count/error는 Java int, elapsed는 long; 평균은 float32(elapsed)/count | `../scouter/scouter.server/src/main/scala/scouter/server/core/app/PerfStat.scala:21`, `:32` |
| put은 LAST insertion, overflow는 첫 삽입 항목 제거; get은 FIFO 갱신 없음 | `../scouter/scouter.common/src/main/java/scouter/util/LinkedMap.java:208`, `:254`; `../scouter/scouter.common/src/main/java/scouter/util/IntIntLinkedMap.java:178`, `:237` |

Go는 `processGroup(ctx,p,now)`를 XLog 수집 mutex 안에서 호출한다. p는 상위 수집 계층의 소유 복사본이다. group 산정·집계 후 XLog 저장 실패가 발생해도 group meter를 rollback하지 않는다. 원본도 저장 이전에 별도 집계를 전달하므로 저장과 집계는 하나의 transaction이 아니다.

## URL, hash, dictionary

`XLogGroupUtil.scala:87`을 따른다. URL의 마지막 `.` 위치가 0보다 크면 뒤의 전체 문자열을 소문자로 바꾸어 `jsp`→`*.jsp`, `gif/jpg/png/bmp/ico`→`images`, `html/htm/css/xml/js`→`statics`로 분류한다. query string을 제거하지 않는다. 따라서 `/api/x.png?q=1`은 `images`가 아니라 `/api`이다. 해당 suffix가 없고 URL이 비었거나 `/`이거나 index 1 이후 `/`가 없으면 `/**`; 그 외 첫 slash 앞 substring을 group 이름으로 쓴다. 전체 URL `http://example.invalid/x`는 `http:`이다.

hash는 UTF-8 바이트의 IEEE CRC32를 signed int32로 해석한다. `HashUtil.java:56`의 기본 charset 의존성은 이 프로젝트 기준 Java UTF-8 실행 환경을 가정한다. suffix의 ASCII 대소문자 규칙을 유지하며 locale 특이 동작은 별도 운영 gate이다.

`*.jsp`, `**`, `images`, `statics`, `/**` 기본 group 사전은 최초 group 처리에서 영구 Text 사전에 기록한다(`XLogGroupUtil.scala:70`). `**`는 초기 사전 값이며 URL 분류 결과로 반환되지 않는다. 동적 prefix 이름은 group 사전으로 제출한다. group hash와 service→group 캐시의 최초 값은 바꾸지 않는다. 캐시는 날짜 변경에도 유지되고 FIFO로 밀려날 때만 다시 조회한다. service 사전이 없으면 group 0이며 meter를 만들지 않는다. 뒤늦게 service 사전이 도착한 후 다음 XLog에서 다시 산정한다.

사전의 first-value 근거는 `../scouter/scouter.server/src/main/scala/scouter/server/db/TextPermWR.scala:58`의 기존 hash 기록 제외와 `../scouter/scouter.server/src/main/scala/scouter/server/core/cache/TextCache.scala:31`의 기존 hash cache 제외이다. Go는 기존 Store.Text/PutText를 재사용하며 group 전용의 두 번째 text cache를 만들지 않는다.

원본 `saved` 집합은 선언·contains 검사만 있고 추가 호출이 없다(`XLogGroupUtil.scala:80`, `:114`). 따라서 동적 이름을 반복 제출하는 현재 코드와 문서상의 중복 방지 의도를 분리한다. Go도 해당 제출을 보존하고 기존 Store.PutText의 first-value 계약으로 재기록을 막는다. 수정된 원본이라고 가정하지 않는다.

## REALTIME_SERVICE_GROUP

원본 `../scouter/scouter.server/src/main/scala/scouter/server/netio/service/handle/GroupService.scala:41`을 따른다. request MapPack의 `objHash`는 Decimal List이다. 누락 또는 빈 객체 집합이면 응답 Pack을 쓰지 않는다. 인증은 기존 Client dispatcher가 검증한다.

Engine 조회 API는 `RealtimeServiceGroup(context.Context, []int32) (*protocol.MapPack,error)`이다. nonempty 객체 집합이면 다음 4개 List를 가진 단일 MapPack을 반환한다. 대상 meter가 없어도 4개 빈 List를 반환한다.

| key | List 항목 | 의미 |
| --- | --- | --- |
| name | Text | 영구 group 사전 이름; 사전 누락은 `unknown` |
| count | Decimal | 최근 30개 초 버킷의 선택 객체 transaction 개수 합 |
| elapsed | Float | float32 총 elapsed / count; count 0이면 0 |
| error | Decimal | 최근 30개 초 버킷의 error!=0 개수 합 |

elapsed는 TPS나 error 비율이 아니다. 같은 group의 서로 다른 객체를 합친다. 객체 hash 중복은 집계에 중복 적용하지 않는다. 원본 Hashtable/IntKeyMap의 순서가 명령 계약에 정의되지 않아 Go는 signed group hash 오름차순으로 4개 List를 함께 정렬한다.

snapshot cache는 전체 객체별 meter를 1초에 한 번 복사한다. 시작 시 lastBuild가 초기 시각이고 cache가 비어 있으므로 첫 1초 조회는 빈 Lists이다. cache 작성 이후 새 XLog는 다음 cache 갱신까지 조회에 반영되지 않는다(`XLogGroupPerf.scala:142`). cache에 남은 group은 30초 만료 후 count/error/elapsed 0으로 나타날 수 있다. 반환 Pack/List는 새로 할당하여 caller 수정이 내부 cache를 바꾸지 않는다.

## 매분 5분 저장과 제한

`XLogGroupPerf.scala:69`, `:119`을 따른다. 시작 시 다음 epoch minute 경계를 예약하고, Engine.Tick에서 해당 경계를 지났으면 현재 300개 초 버킷의 합계를 저장한다. 저장 대상 날짜·minute만 현재 시각에서 10초를 뺀 값을 사용한다. 합계의 meter 시각은 현재 시각이며 10초를 빼지 않는다. Scouter 고정 24시간 날짜 계산은 공용 scoutertime을 사용한다.

각 객체·group에 FIVE_MIN(3) Decimal `cnt:<signed group>`, `tm:<signed group>`, `err:<signed group>`를 기록한다. `Counters.PutDailyContext`는 명시적 objHash로 기존 5m 저장 형식에 쓴다. 같은 5분 슬롯은 매분 갱신하고 합계를 reset하지 않는다. count 0의 meter는 삭제 후보로 기록하고 다음 group 처리 직전에 제거한다. 후보 집합은 meter 상한 2,000개 이내이며 중복 후보를 추가하지 않는다.

Go의 queue 소비와 timer는 Engine 수집/Tick로 통합한다. 원본 별도 queue의 추가 drop 경로(`XLogGroupPerf.scala:79`)를 새로 만들지 않는다. Tick이 늦으면 실제 호출 시각의 현재 snapshot을 한 번 저장하고 다음 minute을 예약한다. 과거 실행 시각의 데이터를 재구성하지 않는다. 저장 오류/취소 시 다음 실행의 retry 기한을 유지한다. 3개 counter 쓰기는 원본처럼 원자적 묶음이 아니며 일부 파일 쓰기 완료 후 오류가 반환될 수 있다.

meter 2,000개와 service mapping 50,000개는 FIFO 상한이다. 각 meter의 301개 버킷, mapping, snapshot은 Engine MaxCacheBytes 예산에 보수적 고정 비용으로 계상한다. 전체 예산 초과 시(2026-10-21부터) XLog 저장은 그대로 하고 meter·memo 갱신만 건너뛰며 `XLogCacheSkipped`로 센다([ingest CONTRACT](../../internal/ingest/CONTRACT.md) §12). 조회 object hash는 최대 100,000개로 제한한다. 입력/출력 배열·map·candidate 집합은 상한이 있다. engine 종료, canceled context, dictionary/storage 오류를 반환한다. 고정 비용은 runtime RSS 실측값이 아니다.

## 검증과 남은 gate

`internal/ingest/group_test.go`는 URL suffix/query/전체 URL, 고정 CRC, 최초 dictionary 값, service cache FIFO, cache 시작·1초 갱신·소유권, 객체별 합계, 30초·301초 만료, 매분 반복 5분 저장, deferred empty cleanup, meter 2,000개·service mapping 50,000개 상한, 전체 예산 초과, 취소·종료·저장 오류 retry를 검증한다. 동시 Accept/query/Tick은 race 검증 대상이다. 테스트의 mapping preload는 상한 검증이며 50,000개 실제 dictionary 처리 성능을 측정하지 않는다.

```sh
.tools/go/bin/go test ./internal/ingest -run '^TestGroup' -count=1
.tools/go/bin/go test -race ./internal/ingest -run '^TestGroup' -count=1
.tools/go/bin/go vet ./internal/ingest
```

현재 증거는 정본 source 대조와 범위 단위/race 테스트이다. 실제 원본 Scala group 구현의 byte oracle, 실제 Agent/Client 연계 및 공유 호스트에서 dictionary miss·최대 meter·조회 병행·minute flush 비용은 별도 gate이다. 실제 Client harness의 WEB XLog 3개와 `/**` group count/평균/error 검증 결과는 통합 검증 기록에서 확인한다. 그룹 상태는 재시작 시 복원하지 않으며 영구 dictionary/5m counter만 기존 저장 계약에 따라 남는다.

2026-10-13 범위 검증: `go test ... -run '^TestGroup'` PASS, 동일 범위 `-race` PASS, `go vet ./internal/ingest` PASS. 동시 fixture의 service 0은 원본 sampling에서 drop되므로 정상 완료 fixture를 service 1로 수정했다.

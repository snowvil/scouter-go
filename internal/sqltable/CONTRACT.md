# internal/sqltable 계약

원본(고정 commit `dee47fb5`):

- `scouter.server/src/main/scala/scouter/server/core/SqlTables.scala` (queue·집합·daemon·저장 호출)
- `scouter.common/src/main/java/scouter/util/SQLSimpleParser.java` (`getCrudInfo`, `SqlTables.parseTable`이 호출)
- 호출처 `scouter.server/.../core/TextCore.scala:38-41`: xtype `sql`인 TextPack마다 `SqlTables.add(yyyymmdd, hash, text)`를 `TextWR.add`보다 **먼저** 부른다.
- `core/sqltable/TableFinder.java`(jsqlparser visitor)와 `Test.java`는 SqlTables가 호출하지 않는 미사용 코드다. jsqlparser import도 미사용이다. 포팅하지 않는다.

## 1. CRUD 문자열 (`CrudInfo`, `Parse`)

- `CrudInfo(sql)`은 `new SQLSimpleParser().getCrudInfo(sql)`과 같은 문자열을 돌려준다. 형식은 `table(T)`를 node 순서로 `,` 결합, 같은 `table(T)` 문자열은 첫 번째만 남긴다. `T`: SELECT/JOIN `R`, DELETE `D`, UPDATE `U`, INSERT·MERGE `C`.
- tokenizer·`crudInfo`·`applyNode`·`findNode`의 quirk를 그대로 보존한다. 예: `FROM t1 AS a`는 `t1`만, `"T Q"`는 `"T`, 문장 구분 `;`는 table 이름에 붙고(`t1;(R)`), `MERGE INTO ... UPDATE SET`은 `SET(U)`다.
- Java `Exception`(배열 범위 초과, `last index`, `copyOfRange`)은 Java처럼 `""` 결과다. 오류가 아니며 SqlTables는 이 `""`도 저장 호출에 넘긴다.
- 대소문자: Java `toUpperCase()`/`toLowerCase()`는 default locale을 쓴다. **Turkish가 아닌 locale**(en/ko)을 가정한다. 비ASCII token은 `strings.ToUpper/ToLower`로 비교하며 U+017F(ſ→S), U+0131(ı→I)이 keyword가 되는 것까지 Java와 같다(golden 포함).
- 입력은 protocol이 검증한 유효 UTF-8 문자열이다. tokenizer가 보는 문자는 모두 ASCII라 byte 단위 처리와 Java UTF-16 code unit 처리가 같은 token을 만든다. 무효 UTF-8에 대한 Java 동등성은 주장하지 않는다(fuzz로 panic·비결정성 없음만 검증).

### Go 전용 상한 (의도된 차이)

| 상수 | 값 | Java | Go 동작 |
|---|---|---|---|
| `MaxSQLBytes` | 1 MiB | 상한 없음 | `ErrTooLong` → 파싱 실패 처리 |
| `MaxWorkSteps` | 20,000,000 | 상한 없음 | `ErrBudget` → 파싱 실패 처리 |
| `MaxNodes` | 100,000 | 상한 없음 | `ErrBudget` → 파싱 실패 처리 |

- 근거: 원본 parser는 입력 길이와 작업량을 제한하지 않는다. 일부 중첩 형태의 입력에서는 처리 시간이 입력 길이에 대해 초다항식으로 늘어난다(JDK 8, 개발 장비에서 같은 harness로 실측. `StackOverflowError`는 없었다).
- Go는 `findNode`의 선형 탐색을 depth별 index로 바꿔(결과 동일) 그런 입력에서도 상한 안에서는 Java와 같은 결과를 낸다. 상한을 넘는 입력만 실패로 처리한다.
- 상한 초과는 Java `Throwable` 경로(`failSet` + `S112`)와 같은 방식으로 처리한다. Java에는 이 상한이 없다.

## 2. SqlTables (`Tables`)

| 항목 | Java | Go |
|---|---|---|
| 활성화 | `add`마다 `sql_table_parsing_enabled` 확인, false면 즉시 반환 | `Options.Enabled()`를 `Add`마다 호출, `Stats.Disabled` |
| queue1 | `RequestQueue(500)`, text 포함 | `chan`(500), `Stats.Enqueued1` |
| queue2 | queue1이 가득 차면 text를 버리고 `RequestQueue(10000)` | `chan`(10000), `Stats.Enqueued2` |
| 둘 다 가득 참 | `Logger.println("S111", 10, "queue exceeded!!")` | `Log("S111", 10, "queue exceeded!!")`, `Stats.Rejected` |
| 처리 순서 | queue1 우선, 다음 queue2, 없으면 대기 | 동일 |
| parsed | `StringKeyLinkedMap.setMax(2)`(date별), 각 `IntLinkedSet.setMax(10000)` | 동일 상한, 오래 넣은 date와 hash부터 제거, get은 순서를 바꾸지 않음 |
| failSet | `IntLinkedSet.setMax(10000)`, date와 무관 | 동일 |
| text 없는 항목 | `TextRD.getString(date, "sql", hash)`, null이면 조용히 반환 | `Source.Text(ctx, date, "sql", hash)`; 없음 `TextMissing`, 오류 `SourceErrors`(Java는 Exception을 잡고 null) |
| 저장 | `parsed.put` 후 `TextWR.add(date, "table", hash, result)` | 같은 순서로 `Sink.AddText(ctx, date, "table", hash, result)` |
| 실패 | `catch Throwable` → `failSet.put`, 설정 시 `S112` | Go 상한·panic → `failSet`, `ParseFailures`, `LogFailEnabled()`가 true면 `S112` |
| 종료 | daemon이 `CoreRun.running` false에서 멈추고 남은 queue는 버림 | `Close(ctx)`: 현재 항목 후 멈춤, 남은 항목은 `RemainingAtClose`, 이후 `Add`는 `RejectedClosed` |

- `Add`는 blocking하지 않는다. 수집 경로에서 저장을 기다리지 않는다.
- goroutine 소유: `New`가 worker 1개를 시작하고 `Close`가 종료를 기다린다. `Close(ctx)`의 ctx가 먼저 끝나면 worker context를 취소한다. runtime 배선의 Sink(`Store.PutTextContext`)는 store gate·lock 대기와 기록 시작 전 단계에서 취소되지만, 이미 시작한 레코드 쓰기(`.data` append → index 등록)는 파일 무결성을 위해 끝까지 수행하므로 `Close`는 그 한 건이 끝날 때까지 기다릴 수 있다. `Close`는 여러 번 호출해도 안전하다.
- 집계 불변식: `Add` 결과는 `Disabled | Rejected | RejectedClosed | Enqueued1 | Enqueued2` 중 하나다. 꺼낸 항목은 `Stored | AlreadyParsed | AlreadyFailed | TextMissing | SourceErrors | ParseFailures | StorageErrors` 중 하나이고, 종료 시 남은 항목은 `RemainingAtClose`다. `Empty`는 `Stored` 중 결과가 `""`인 수다.
- 저장 오류: Java와 같이 parsed 표시 뒤에 저장하므로 실패한 hash도 같은 날 다시 시도하지 않는다. Java는 `TextWR.add`가 비동기 enqueue라 이 오류를 볼 수 없다. Go는 `StorageErrors`로 따로 집계하고 failSet에는 넣지 않는다.
- `_trace`는 SqlTables에서 읽기만 하고 사용하지 않는다. 포팅하지 않는다.

### S112 원문 SQL 마스킹 (D-S1 결정)

Java는 `S112`에 `<원문 SQL>\n<Throwable>`을 기록한다. Go는 원문 대신 `sql hash=<hash> length=<byte 길이>\n<오류>`를 기록한다. SQL literal에 비밀값이 섞일 수 있어 원문을 로그에 남기지 않는다. 의도된 차이다.

## 3. 연결 계약 (ingest·storage 쪽 배선)

```go
tb, err := sqltable.New(sqltable.Options{
    Sink:           textSink,   // AddText(ctx, date, "table", hash, text) error
    Source:         textSource, // Text(ctx, date, "sql", hash) (string, bool, error)
    Enabled:        func() bool { /* sql_table_parsing_enabled, 매번 현재 값 */ },
    LogFailEnabled: func() bool { /* log_sql_parsing_fail_enabled */ },
    Log:            func(id string, sec int, msg string) { /* Logger.println(id, sec, msg) */ },
})
// TextCore 위치: xtype == "sql"인 TextPack마다 text 저장 enqueue보다 먼저
tb.Add(yyyymmdd, pack.Hash, pack.Text)
// 종료: 수집 입력을 멈춘 뒤
err = tb.Close(ctx)
st := tb.Stats()
```

- `Sink.AddText`는 `TextWR.add`의 의미를 지켜야 한다. `TextCache.put`, `TextDupCheck`, `TextPermWR.isA("table")`=true(설정과 무관)라 영구 사전으로 보낸다. `TextPermWR.add`는 `StringUtil.isEmpty(text)`(null 또는 길이 0, trim 없음)이면 쓰지 않지만, 그 전에 `TextWR.add`가 `TextCache.put("table", hash, "")`를 한다.
- `Source.Text`는 `TextRD.getString`처럼 `TextCache`를 먼저 본 뒤 저장소를 조회해야 한다. Java에서는 `TextCore.add`가 queue 전에 `TextCache.put`을 하므로, text를 버린 queue2 항목도 대개 cache에서 찾는다. Go 저장 queue가 아직 쓰지 않은 text를 찾지 못하면 Java처럼 조용히 건너뛰고 `TextMissing`으로 집계한다.
- `Log`는 Java `Logger.println(id, sec, msg)`의 per-id 간격 억제(`sec > 0`)와 `mgr_log_ignore_ids`를 적용해야 한다. sqltable은 간격 억제를 하지 않는다.
- `Enabled`/`LogFailEnabled`는 설정 reload를 반영하도록 호출 시점의 값을 읽어야 한다.

## 4. 검증

- Java oracle: `python3 -I scripts/java-sqltable.py [--source <scouter>] [--java-home <JDK8>]`가 원본 HEAD·`SQLSimpleParser.java` blob을 확인하고 JDK 8(`-Duser.language=en -Duser.country=US`)로 `tools/java-sqltable/SqlTableOracle.java`를 실행해 `testdata/java_crud_golden.json`을 만든다. `--check`는 재생성 결과와 비교한다. 입력은 `tools/java-sqltable/cases.json`의 손으로 쓴 합성 SQL, 긴 SQL·병적 SQL 6건, 고정 seed xorshift 생성기의 token soup 1,200건과 구조화 문장 800건이다. 운영 SQL은 넣지 않는다.
- Go 대조: `TestJavaCrudGolden`. `--extra-out <path> --seed N --count M`으로 더 큰 표본을 만들고 `SQLTABLE_GOLDEN=<path>`로 같은 테스트를 돌릴 수 있다.
- Fuzz: `FuzzCrudInfo`(64 KiB 이하)는 panic, 비결정성, 유효 입력에서 무효 UTF-8 결과가 없는지 확인한다. 상한 때문에 무한 루프가 없다.
- 미검증: 실제 Agent 수집 SQL 분포에서의 결과 비교, Turkish locale JVM 동작, Client가 `table` text를 조회하는 화면·명령(config-behavior.md 미확인 항목).

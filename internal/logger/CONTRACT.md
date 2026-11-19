# server log·request log·Pack 출력 계약

정본은 Scouter 2.21.3(commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`)의 `scouter.server/src/main/scala/scouter/server/Logger.scala`, `scouter.server/src/main/java/scouter/server/logs/RequestLogger.java`, `scouter.server/.../netio/data/NetDataProcessor.scala`, `core/ProfilePreCore.scala`, `CounterManager.java`, `scouter.common`의 Pack/Value `toString`이다. 설정 키 의미는 [config-behavior.md](../../docs/compatibility/config-behavior.md) §1.1, §1.8, §2, §4(D-L1~D-L3, D-S1; D-L4는 이 문서 §6), §5.1(L1, I8, T6, S5)을 따른다.

이 패키지는 `internal/config`를 import하지 않는다. 설정은 `Config` 값으로 받는다. `internal/protocol`은 Pack 출력에만 쓴다.

## 1. 공개 API와 배선

| API | Java 대응 | 호출할 곳(이 패키지 밖 배선) |
|---|---|---|
| `New(Config, Options) (*Logger, error)` | `object Logger` 초기화 | `cmd/scouter-go` 기동. `Options.Location`은 `--timezone`의 `*time.Location`(필수) |
| `(*Logger).SetConfig(Config)` | `ConfObserver "Logger"` + 사용 시마다 읽는 `_trace`·`mgr_log_ignore_ids`·`log_keep_days` | `config.Store` OnChange. reload lock 안에서 불러도 된다(원자 교체 + 크기 1 신호만, I/O 없음) |
| `Println(msg)` | `Logger.println(msg)` | ID 없는 로그 |
| `PrintlnID(id, msg)` / `PrintlnIDRate(id, sec, msg)` / `PrintlnIDRateDetail(id, sec, msg, detail)` | `Logger.println(id, msg)` / `(id, sec, msg)` / `(id, sec, msg, t)` | `S150`, `S160`, `S152` 등 ID 로그(T6, S5). `detail`은 Java의 stack trace 자리(접두 없이 다음 줄) |
| `Trace(msg)`, `TraceEnabled()` | `Logger.trace` | `_trace` 상세 로그 |
| `Stdout(line)` | `System.out.println` | `log_udp_*`, `log_udp_some_object` 출력(I8). owner goroutine이 줄 단위로 쓴다 |
| `RequestCommand(cmd, session, lookup) bool` | `RequestLogger.add(cmd, session)` | Client TCP 명령마다(TcpServiceWorker 위치: 세션 검사 뒤, handler 호출 전). `lookup`은 session → (id, ip, ok) |
| `RegisterRequestCommand(cmd)` | `RequestLogger.registerCmd(cmd)` | Agent relay(AgentCall) 전송 직전 |
| `StdLogWriter(stream) io.Writer` | 없음(Go 추가) | `log.SetOutput(lg.StdLogWriter(os.Stderr))` |
| `Sync() bool` | 없음 | 종료 전 flush 확인, 테스트 |
| `Close() error` | 없음(Java daemon thread) | 종료 경로. 큐 잔여분을 쓰고 파일을 닫는다 |
| `Stats() Stats` | 없음 | 운영 통계 |
| `FormatPack(p, loc)`, `FormatValue(v)`, `UDPLog(p)` | Pack/Value `toString`, NetDataProcessor 분기 | ingest 처리 **뒤** `if conf[key] { lg.Stdout(prefix + FormatPack(p, loc)) }`. `log_udp_packet`은 분기 **전** `lg.Stdout(FormatPack(p, loc))` |
| `ProfileDebugString`, `ProfileTestLine`, `ProfileCase*` | `toDebugString`, ProfilePreCore 로그 | `log_test_rate` 표본마다 `lg.Println(ProfileTestLine(tag, p, svc))`. 서비스명이 없으면 `JavaNull` |
| `SomeObjectLine` 외 3개 | CounterManager `DEBUG objType:` | `log_udp_some_object`가 objType과 같을 때 `lg.Stdout(...)` |
| `MaskSQL(text)` | 없음(D-S1) | `log_sql_parsing_fail_enabled`(I6) 등 SQL을 로그에 남기는 모든 곳 |

배선 예(설명용, 이 패키지 밖 코드):

```go
lg, err := logger.New(logger.Config{
	Dir: snap.String("log_dir", "./logs"), Rotation: snap.Bool("log_rotation_enabled", true),
	KeepDays: snap.Int("log_keep_days", 31), Trace: snap.Bool("_trace", false),
	IgnoreIDs: snap.StringSet("mgr_log_ignore_ids"),
}, logger.Options{Location: cfg.Location})
defer lg.Close()
log.SetOutput(lg.StdLogWriter(os.Stderr)) // stderr 출력은 그대로, 파일에 추가 기록
```

`--log-dir` flag 고정 규칙(F0)은 호출자가 `Config.Dir`에 반영한다. `mgr_log_ignore_ids`의 LIST 표시값 `{}`를 ID로 넘기지 않는다(`IgnoreIDs`는 이미 나눈 ID 목록. 앞뒤 공백과 빈 값은 버린다).

## 2. 파일과 줄 형식

날짜·시각은 `DateTimeHelper` 고정 offset 달력이다. 기준은 `Location`의 2000-01-01 00:00이고 하루는 항상 24시간이다. DST 지역에서는 여름철 날짜 전환 시각이 wall clock과 어긋난다(New York은 01:00, Java와 같음, golden `time-America_New_York.json`).

| 대상 | 파일 | 줄 |
|---|---|---|
| server log, `log_rotation_enabled=true` | `log_dir/server-yyyyMMdd.log`(append) | `yyyyMMdd HH:mm:ss msg` / `yyyyMMdd HH:mm:ss [ID] msg` / `yyyyMMdd HH:mm:ss [TRACE] msg` |
| server log, `false` | `log_dir/server.log`(append, 의도된 차이 D-L1) | 같음 |
| request log | `log_dir/request-yyyyMMdd.log`(rotation과 무관) | `yyyyMMdd HH:mm:ss.SSS <id>(<ip>) <cmd>`, 모르는 session은 `unknown<session>() <cmd>`, 등록은 `unknown(register) <cmd>` |

- 줄 끝은 `\n`. 메시지 안의 줄바꿈은 그대로 쓴다(둘째 줄부터 접두 없음).
- 파일 날짜는 **기록 호출 시각**의 날짜다. 날짜가 바뀐 첫 줄에서 새 파일을 연다.
- ID 필터(`mgr_log_ignore_ids`)는 ID 있는 호출에만 적용된다. `Println`·`Trace`·`StdLogWriter` 줄은 막을 수 없다(Java와 같음).
- rate limit: `sec > 0`이면 같은 ID의 마지막 출력 시각 + `sec`초 전까지 버린다. 시각은 최대 1000개 ID를 삽입 순서로 보관하고, 1001번째 새 ID가 오면 가장 먼저 넣은 ID를 잊는다(`StringLongLinkedMap.setMax(1000)`, 기존 ID 갱신은 위치를 바꾸지 않음).
- `_trace=true`이면 server log 줄을 stdout에도 쓴다(호출 시점 값).
- request log 대상 명령: `SERVER_DB_DELETE`, `REMOTE_CONTROL`, `REMOTE_CONTROL_ALL`, `TRANX_LOAD_TIME_GROUP` + `RegisterRequestCommand`로 등록한 명령. `ACTIVE_QUERY_LIST`는 등록하지 않는다. `RequestCommand`의 반환값은 Java `add`와 같다(대상 명령이고 session이 알려진 경우만 true). 등록 집합은 1024개 상한이다(초과 등록은 `Dropped`).

## 3. 오래된 로그 정리

owner goroutine이 `CleanupInterval`(기본 1시간, 첫 실행은 시작 1시간 뒤)마다 실행한다.

1. `log_rotation_enabled=false`이거나 `log_keep_days <= 0`이면 하지 않는다.
2. `log_dir`의 항목 중 디렉터리(심볼릭 링크가 가리키는 디렉터리 포함)는 건너뛴다.
3. 이름이 `server-` 또는 `request-`로 시작하고, 접두와 **마지막 `.`** 사이가 정확히 8글자이며, `Integer.parseInt` 규칙(선행 `+`/`-` 허용)으로 연·월·일을 읽을 수 있어야 한다. 연도 < 2000은 day 0(즉시 삭제 대상), 연도 ≥ 2100은 2099-12-31 다음 날, 불가능한 월·일은 건너뛴다.
4. `오늘 day - 파일 day > log_keep_days`이면 지우고 `[scouter] delete log file : <절대 경로>`를 ID 없이 기록한다. 절대 경로는 Java `File.getAbsolutePath` 규칙(중복·끝 `/` 제거, `.` 미해석, 상대 경로는 작업 디렉터리 기준)이다.

## 4. 실행 구조·상한·오류

- goroutine은 `New`가 만든 owner 하나다. 파일·stdout 쓰기, 날짜 전환, 설정 반영, 정리를 모두 이 goroutine이 한다. `Close`가 종료 경로다(멱등). `Close`는 send lock을 잡아 이후 입력을 막고, 큐에 남은 기록을 모두 쓴 뒤(`Remaining`) 파일을 닫고 owner 종료를 기다린다.
- 호출자는 기다리지 않는다. 큐(`QueueSize`, 기본 8192)가 차면 기록을 버리고 `Dropped`를 센다. Close 뒤 호출은 `DroppedClosed`. `Sync`만 큐 자리를 기다린다.
- 파일은 server·request 각각 최대 1개만 열려 있다. 쓰기는 32 KiB buffer이고 큐가 빌 때마다 flush한다.
- 파일 open 실패: stderr에 sink별로 한 번 경고하고(성공하면 다시 경고 가능) 1초 동안 open을 다시 시도하지 않는다. 그동안 server 줄은 stdout으로 쓴다(Java fallback, `StdoutFallback`). request 줄은 버린다(Java도 stack trace만 찍고 버림, `RequestLost`).
- 정리 목록 조회·삭제 실패는 `DeleteErrors`와 경고 한 번. 다음 주기에 다시 시도한다.
- `Stats` 불변식: `Enqueued = Written + StdoutFallback + RequestLost + 큐 잔여`. `Written`은 buffer에 넘긴 수이고 이후 flush 실패는 `FileErrors`로 센다.
- 권한: 디렉터리 0750, 파일 0640(이 저장소의 HTTP request log와 같음).

## 5. Pack·Value 출력(`FormatPack`, `FormatValue`)

Java `toString()`과 같은 문자열이다(아래 마스킹 제외).

- 시각: Pack 시각은 `DateUtil.timestamp`(고정 offset 달력, base 이전은 `20000101 00:00:00.000`). `ObjectPack` wakeup은 `java.sql.Timestamp.toString`(DST 반영 실제 달력, 소수부는 끝 0 제거, 0이면 `.0`).
- hash: `Hexa32.toString32`(0..9 십진, 그 밖 `x`/`z` + 32진, `Long.MIN_VALUE`는 `z8000000000000`). `DroppedXLogPack`, `SpanPack`, `InteractionPerfCounterPack`의 숫자는 십진 그대로.
- `MapPack`: `MapPack {k=v, k=v}`(LinkedHashMap, wire 순서, `, `). `MapValue`: `{k=v,k=v}`, JDK 8 `Hashtable`에 wire 순서로 넣었을 때의 순회 순서(용량 11, load factor 0.75, 2n+1 rehash, 높은 bucket부터, chain은 최근 삽입부터). `ListValue` `[a,b]`, 배열 `[a, b]`, NullValue와 미설정 값은 `null`, Java에서 `new MapValue()`로 초기화되는 필드(`tags`, `data`, `table`, `customData`)의 미설정 Go 값은 `{}`.
- 숫자: `FloatValue`·`FloatArray`는 JDK 8 `Float.toString`, `DoubleValue`·`DoubleSummary`는 `DecimalFormat("#0.0#################")`. 둘 다 JDK 8 `FloatingDecimal` 알고리즘을 그대로 옮겼다(최단 표현이 아님: `1e23` → `99999999999999990000000.0`, `3.4e10f` → `3.3999999E10`, `Float.MIN_VALUE` → `1.4E-45`). NaN은 `�`, 무한대는 `∞`/`-∞`, 0으로 반올림되는 음수는 `-0.0`.
- `TimeTypeEnum` 이름이 없으면 빈 문자열(그래서 공백 두 개).
- `UDPLog`: 키·접두는 config-behavior.md §1.8 표와 같다. MapPack·단독 SpanPack은 per-type 키가 없다(`ok=false`).
- Pack 자체를 바꾸지 않는다. Java는 출력 전에 PerfCounterPack `data`에 `objHash`·`time`을 넣고, ObjectPack 빈 `address`를 송신 IP로 채운다. 같은 줄을 원하면 호출자가 처리한 **뒤**의 Pack을 넘긴다.

## 6. 의도된 차이

| 항목 | Java | Go | 이유 |
|---|---|---|---|
| D-L1 | `log_rotation_enabled=false`면 open마다 `server.log`를 비움 | append | 재기동·날짜 전환·설정 변경 때 로그 소실 방지(결정) |
| D-L2 | `request-*.log`는 class 초기화 때 `log_dir`에 고정 | live `log_dir` | server log와 같은 디렉터리 유지 |
| D-L3 | 정리 중 예외(목록 null)로 Logger thread 종료 → 날짜 전환 중단 | 경고·집계 후 계속 | 단일 owner 유지 |
| D-L4 | 기동 경합(원본 결함 후보, 아래) 때문에 첫 줄이 파일 대신 stdout으로 갈 수 있음 | 파일 open·close를 owner goroutine 하나만 하므로 경합이 없음 | 단일 owner |
| D-S1 | `log_udp_text`가 원문 SQL 출력 | `sql` Text는 `<sql masked len=<UTF-8 bytes> hash=<Hexa32(CRC32)>>`, `login` Text는 `<login masked len=… hash=…>` | 원문 SQL·계정 정보 로그 금지(AGENTS.md). hash는 Scouter text hash와 같은 계산이라 TextPack hash와 대조 가능 |
| 날짜 전환 | 5초 주기 thread가 닫음(최대 5초 동안 이전 날짜 파일) | 호출 시각 날짜로 즉시 선택 | 줄 날짜와 파일 날짜 일치 |
| 큐 | 호출 thread가 직접 파일에 씀(블록) | bounded 큐, 포화 시 버리고 집계 | 수집 경로가 디스크를 기다리지 않음 |
| open 재시도 | 실패하면 매 줄 재시도 | 1초 backoff | 디스크 오류 시 syscall 폭주 방지 |
| 삭제 로그 | `delete()` 결과와 무관하게 "delete log file" 기록 | 성공했을 때만 기록, 실패는 집계·경고 | 실제 삭제와 로그 일치 |
| Go 표준 `log` | 없음 | stderr 출력 유지 + server log 파일에 추가 기록(표준 timestamp 접두 제거) | 운영 감시는 stderr를 계속 읽음 |
| 파일 권한 | umask 기본(대개 0644) | 0640/0750 | 저장소 규칙 |
| request 명령 등록 | 무제한 | 1024개 | 상한 규칙 |
| 숫자 문자열 파싱 | `parseInt`가 비ASCII 숫자도 허용 | ASCII만 | 실제 로그 파일명에 없음 |

### D-L4 원본 기동 경합(결함 후보)

근거: `Logger.scala:111-152`.

1. 첫 `Logger.println`이 `object Logger`를 초기화하면서 `ThreadScala.startDaemon("scouter.server.Logger")`를 시작한다.
2. daemon의 첫 반복은 `lastDataUnit != DateUtil.getDateUnit()`을 검사한다. 초기값 0이거나 main thread의 `openFile`이 값을 쓰기 전이면 오늘과 다르다고 보고, 동기화 없이 `pw`를 close한 뒤 `null`로 만든다.
3. 같은 순간 main thread는 `printlnInternal`에서 다음 셋 중 하나를 겪는다.
   - 닫힌 `pw`에 쓴다. 이 경우 `PrintWriter`가 오류를 삼켜 그 줄이 사라진다.
   - `pw == null` 분기로 들어가 stdout에 쓴다.
   - NPE가 나서 catch 블록이 stdout에 쓴다.

결과적으로 기동 직후 첫 줄(들)이 server log 대신 stdout으로 갈 수 있다. 재현 결과, busy-loop 8개로 CPU 경합을 건 상태에서 `run_oracle.py --check` 20회 중 3회 `"<datetime> plain message"`가 `formats_lines`에서 빠지고 `formats_stdout`으로 옮겨 갔다.

oracle 대응(`LoggerOracle` section 0): 버리는 `log_dir`에 한 번 기록한 뒤 daemon thread가 첫 `ThreadUtil.sleep`(`TIMED_WAITING`)에 들어갈 때까지 기다린다(상한 10초). 그다음 stdout 캡처를 비우고 `log_dir`를 바꾼다. 이 경합은 정상 계약이 아니므로 golden에 넣지 않는다.

## 7. 검증

- `python3 tools/java-logger/run_oracle.py --generate|--check`: 원본 class를 실행해 golden 생성·재현 확인([README](../../tools/java-logger/README.md)).
- `go test ./internal/logger`: golden 전부(Pack 37·Value 47·toDebugString 8·Float 826·Double 1044·시각 321×2 zone·날짜 파싱 16·Logger/RequestLogger 시나리오), 마스킹 allowlist(`text_sql`, `text_sql_unicode`, `text_login`), 시계 주입 날짜 경계(KST 자정, New York DST), 정리 주기, Close drain·멱등·Close 뒤 거절, 큐 포화, 디스크 실패 fallback·복구, live 설정, `StdLogWriter`, nil logger, 동시 사용(`-race`).

## 8. 미검증·남은 위험

- 실제 Java 서버 프로세스의 로그 파일과 나란히 비교하지 않았다. 원본 `Logger.scala`/`RequestLogger.java` class를 seam(Configure, LoginManager)과 함께 직접 실행한 결과만 비교했다.
- `PrintlnIDRateDetail`의 detail은 호출자가 만든다. Java stack trace(`ThreadUtil.getStackTrace`) 형식 재현은 범위 밖이다.
- golden의 JDK는 8이다. JDK 9+ CLDR에서는 `DecimalFormat` NaN이 `NaN`으로 바뀌는 등 다를 수 있다.
- 배선(main, ingest, service, transport)은 이 패키지 범위 밖이다. 배선 전에는 파일이 생기지 않는다.

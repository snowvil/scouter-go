# `internal/console` 계약

`-console` 모드(Java `scouter.server.term.TermMain`, `term/handler/*`)의 Go 구현이다. 원본 정본은 `docs/compatibility/baseline.json`의 Scouter `dee47fb`이며, 원본 동작 조사는 `docs/compatibility/console.md`를 따른다. 이 문서는 Go 구현이 그 계약을 어떻게 따르고 어디서 의도적으로 다른지를 기록한다.

## 구성과 소유권

| 요소 | 위치 | 소유·종료 |
|---|---|---|
| `Console` | `console.go` | 입력 루프 goroutine(호출자)이 `Execute`/`Run`/`Close`를 부른다. 반복 명령 job goroutine은 최대 1개이며 Console이 소유한다. 다음 입력, `Run`의 ctx 종료, `Close`가 job을 cancel하고 끝날 때까지 기다린다. |
| 명령 handler | `commands.go` | 12개 명령(help, q/quit, objtype, object, counter, dashboard, realtime, xlog, xlist, tagcnt, visitor, debug). |
| `Source` | `source.go` | 서버 상태를 읽는 작은 인터페이스. `internal/server/console_source.go`가 `ingest.Engine`·counters·transport 통계로 구현한다. |
| `PlainReader` | `reader.go` | 터미널이 아닌 입력. 첫 `ReadLine`에 읽기 goroutine 1개를 띄운다. EOF·읽기 오류·`Close`(unblock 호출 후 최대 1초 대기, 못 깨우면 오류 보고)로 끝난다. |
| `editor` / `terminal` | `editor.go`, `terminal_unix.go` | 터미널 입력. 별도 goroutine 없이 `ReadLine` 호출 안에서 읽는다(VMIN=0/VTIME=1, 100ms마다 ctx 확인). `Close`가 저장한 termios를 복구한다. |

출력은 하나의 mutex로 직렬화한 writer(`Console.Writer`)로만 쓴다. 명령 출력은 `Out`(stdout), 한 줄 오류는 `Err`(stderr)이다.

상한: 입력 한 줄 4096 byte(초과 줄은 통째로 버리고 한 줄 오류), 편집 줄 1024 rune, history 500개(jline MemoryHistory 기본값), 반복 job 1개, 조회 결과 크기는 storage의 `MaxQueryRecords`를 따른다.

## 입력

- 터미널(`OpenInput`이 termios 조회 성공): ICANON·ECHO·IEXTEN을 끄고 ISIG는 유지한다. Ctrl-C는 SIGINT로 기존 graceful 종료 경로를 탄다. Ctrl-\는 SIGQUIT이며 JVM thread dump처럼 모든 goroutine stack을 stderr에 쓰고 서버와 콘솔은 계속 실행한다(`cmd/scouter-go` `dumpOnSIGQUIT`; Go 기본 동작인 dump 후 exit 2는 쓰지 않는다). 종료 시 `Close`가 원래 termios를 복구한다. 편집: 문자 삽입, Backspace, Delete(`ESC[3~`), ←/→, Home/End(`ESC[H/F`, `ESC O H/F`, `ESC[1~/4~/7~/8~`, Ctrl-A/E), Ctrl-B/F, Ctrl-K/U/W, ↑/↓·Ctrl-P/N history, Ctrl-L redraw, Ctrl-D(빈 줄이면 EOF, 아니면 커서 문자 삭제). 모든 문자를 1칸 폭으로 가정한다(동아시아 전각 문자는 커서 위치가 어긋날 수 있다).
- Tab 완성: `Words`(Java `Help.words()` 8개: help, quit, objtype, object, counter, realtime, tagcnt, dashboard). jline `StringsCompleter` 규칙대로 **버퍼 전체**가 접두사인 단어가 후보다. 후보 1개면 커서 앞을 `단어 + " "`로 바꾼다(이미 같으면 그대로). 여러 개면 공통 접두사로 바꾸고 후보를 정렬해 다음 줄에 두 칸 간격으로 출력한 뒤 prompt를 다시 그린다(jline은 터미널 폭 기준 열 배치). 후보가 없으면 bell.
- history: 빈 줄과 직전과 같은 줄은 저장하지 않는다(jline 기본값). 파일 history는 없다(Java도 메모리 history).
- 비터미널: stdin을 dup하고 O_NONBLOCK으로 poller에 등록해 `Close`가 대기 중인 read를 깨운다. `Close`는 공유 file description의 blocking 상태를 되돌린다(SIGKILL이면 non-blocking으로 남을 수 있다). 경계 사례: O_NONBLOCK은 fd가 아니라 open file description의 flag이므로, stdin이 stdout/stderr와 같은 description을 공유하면(예: inetd/socat처럼 하나의 socket을 0·1·2에 dup한 경우, `exec 0<&1`, 같은 파일을 `<>`로 연 경우) 콘솔이 실행되는 동안 stdout/stderr도 non-blocking이 된다. Go의 `os.Stdout`/`os.Stderr`는 시작 시 blocking fd로 만들어져 poller에 등록되지 않았으므로, 받는 쪽이 느려 버퍼가 차면 write가 `EAGAIN` 오류로 끝나 명령 출력·로그 줄이 일부 유실될 수 있다. 터미널(pty)은 이 경로를 쓰지 않으므로 해당하지 않으며, 이 구성에서는 stdin을 별도 pipe/파일로 주거나 `-console` 없이 실행한다. prompt는 비터미널에서도 출력한다(Java jline도 출력).
- darwin/linux 외 OS(`input_other.go`)는 단순 line reader만 쓴다.

## 명령 해석

`ProcessMain.process`와 같다. 입력을 `String.trim()`(≤U+0020 제거)하고, 공백으로 자른 첫 단어를 ASCII 소문자로 바꿔 고른다. 인자는 `trim(cmd)[len(word):]`의 trim이다(원래 대소문자 유지). 빈 줄을 포함한 **모든 입력이 실행 중인 반복 job을 먼저 멈춘다**(Java `loopProcess += 1`). 알 수 없는 첫 단어는 help다.

`StringUtil.tokenizer`(빈 입력이면 Java null), `firstWord`, `lastWord`, `leftPad`/`rpad`(UTF-16 길이, 자르지 않음), `CastUtil.cint`(실패 0), String 정렬(UTF-16 순서)을 그대로 옮겼다.

## 출력 형식

- 시각: `DateUtil.getLogTime` = `scoutertime.Millis` 기반 `HH:mm:ss.SSS`, 2000-01-01 이전은 `00:00:00.000`. timezone은 주입한 `Location`(`--timezone`)이고 시계는 `Config.Now`로 주입한다.
- 숫자: `FormatUtil.print(…, "#,##0")`/`"#,##0.0"`. NumberValue·Double은 `doubleValue()`의 정확한 이진값에 HALF_EVEN, int/long은 정수 그대로다. 천 단위 `,`(locale 고정), 음수가 0으로 반올림되면 `-0`이다. DecimalFormat처럼 최단 표현 자릿수를 넘는 정수부는 0으로 채운다(예: `Long.MAX_VALUE`의 DecimalValue → `9,223,372,036,854,776,000`).
- `Double.toString`(tagcnt count), `Integer.toString(v,16)`(TEXT_HASH), `Hexa32.toString32`(txid), `IPUtil.toString`(null이면 `0.0.0.0`).
- 근거: `testdata/ConsoleFormatOracle.java`를 고정 원본 `scouter.common`과 함께 JDK 8(1.8.0_345)·JDK 17(17.0.4)로 실행한 `testdata/format-jdk8.golden`, `format-jdk17.golden`(각 246줄, `-Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8`). `TestJavaFormatOracle`이 Go 결과와 전부 비교한다. 두 JDK의 차이는 NaN 하나(JDK 8 `U+FFFD`, JDK 17 `NaN`)다.
- ANSI(`Config.ANSI`): prompt·objtype·counter·xlog 막대는 green(`ESC[32m…ESC[0m`), realtime objName은 blue(`ESC[34m`).

명령별 출력은 `testdata/golden/{commands,ansi,dashboard-cycle}.txt`에 고정했다(시계·seed 데이터 주입, `go test ./internal/console -run Golden -update`로 갱신).

## Go 데이터 원천 대응

| 명령 | Source 메서드 | server 구현 |
|---|---|---|
| objtype/object/filter/agentName | `Objects` | `ingest.Engine.Objects(ctx, "")`. 순서는 ObjHash 오름차순이다(Java ObjectMap 순서는 재현하지 않음). |
| counter/dashboard | `ObjectType` | `--conf` 디렉터리가 있으면 `counters.Store`(counters.site.xml 포함), 없으면 내장 counters.xml을 1회 parse. family counter 다음 type counter, `all=true`만. family가 없으면 `HasFamily=false`. |
| dashboard/realtime | `RealtimeCounter` | `Engine.Counter(hash, name, REALTIME=1)` |
| xlog | `XLogsSince` | `Engine.XLogsRealtime(Loop, Index, ObjectHashes)`. 대상이 없으면 조회하지 않고 `…`를 출력한다(Java와 같음). 커서는 Console 상태로 명령 사이에 유지한다(Java object 변수). |
| xlist | `XLogsByTime`, `Text` | `Engine.XLogsByTime(오늘, t, t+2000)`, `Engine.Text(EndTime 날짜, "service"/"ua")` |
| tagcnt | `TagValues`, `TagValueSeries`, `Text` | `Engine.TagValues`(top 100, count 내림차순), `Engine.TagValueSeries`, `Text(오늘, "error")` |
| visitor | `VisitorObjType`, `VisitorObject` | `Engine.VisitorRealtime(nil, objType)` / `([]int32{hash}, "")` |
| debug queue | `Stats` | 아래 키 집합 |

`tagcnt group`/`tag`는 `TagCountConfig` 순서(`service, alert`와 고정 StringSet 순서)를 console 안에 둔다(`internal/service/tagcount.go`와 같은 목록).

## `debug queue` 키

Java `ServerStat`의 stage queue(`xlog.core.queue` 등 15개)는 Go에 그대로 있지 않다. Go는 Core 단계 queue 없이 engine이 UDP worker에서 메모리를 갱신하고, 디스크 쓰기는 storage writer(XLog·profile·text)와 ingest kind별 writer(2026-10-21, [ingest CONTRACT](../ingest/CONTRACT.md) §13)가 맡는다. 가짜 값으로 채우지 않고 Go에 실제로 있는 queue·통계를 `key=value`로 키 오름차순(UTF-16) 출력한 뒤 빈 줄 하나를 더 쓴다(Java `println`과 같음). 값은 모두 정수다. 이름이 `.queue`로 끝나는 키와 `udp.queue_capacity`, `tcp.active`, `ingest.objects`, `server.error_log_queue`는 현재 값이고 나머지는 누적 counter다.

| 키 | 의미 |
|---|---|
| `agentcall.kube-seq.queue`, `agentcall.cpu-dump.queue`, `pulse.xlog.queue`, `pulse.profile.queue`, `pulse.kafka.queue` | `Runtime.ServerStat()`과 같은 이름·값(Pulse 키는 Pulse가 켜졌을 때, `pulse.kafka.queue`는 Kafka producer가 있을 때만). console은 Close가 바꾸는 `Runtime.pulse`를 읽지 않도록 ConsoleSource 생성 시 잡은 구성요소로 같은 값을 계산하고, `TestConsoleStatsIncludeServerStat`가 두 결과를 비교한다. |
| `agentcall.{kube-seq,cpu-dump}.{enqueued,done,failed,dropped,abandoned}` | Agent 호출 queue 누적 counter(`ingest.Stats.KubeSeqCalls/CPUDumpCalls`) |
| `udp.queue`, `udp.queue_capacity` | UDP worker queue 현재 길이·용량(`UDPStats.QueueLen/QueueCap`, Serve 밖에서는 0) |
| `ingest.completed`, `ingest.rejected`, `ingest.storage_errors`, `ingest.xlogs`, `ingest.dropped_xlogs`, `ingest.skipped_stacks`, `ingest.geoip_errors`, `ingest.geoip_text_errors` | `ingest.Engine.Stats()` (`geoip_errors`는 실패한 lookup과 city text 저장 실패의 합, `geoip_text_errors`는 그중 city text 저장 실패) |
| `ingest.objects` | 메모리 object 수 |
| `udp.datagrams`, `udp.packets`, `udp.rejected`, `udp.malformed`, `udp.expired`, `udp.handler_errors`, `udp.aborted_assemblies`, `udp.aborted_packets`, `udp.shutdown_discarded`, `udp.assembly_rejected` | `UDPReceiver.Stats()` |
| `udp.socket_drops` | kernel이 이 socket에서 버린 datagram 누적 수. `UDPStats.KernelCounters=true`인 Linux에서만 |
| `tcp.accepted`, `tcp.rejected`, `tcp.errors`, `tcp.client_requests`, `tcp.stack_uploads`, `tcp.active` | `TCPServer.Stats()` |
| `server.error_log_queue`, `server.dropped_error_logs`, `server.maintenance_errors`, `server.purged`, `server.recoveries` | Runtime 오류 로그 queue 길이와 누적 counter |
| `ingest.writer.<kind>.{enqueued,written,write_errors,dropped,abandoned,queue}` | ingest kind별 writer(`counter`, `daily`, `object`, `status`, `alert`, `summary`, `stack`, `batch`, `span`, `visitor`)의 누적 counter와 현재 queue 길이(`ingest.Engine.WriterStats`). Java `*WR` queue에 해당하며 `ServerStat`의 `ingest.writer.<kind>.queue`와 같은 값이다 |
| `pulse.{xlog,profile}.{enqueued,sent,dropped,failed}` | Pulse가 켜졌을 때만 |
| `kafka.failed`, `kafka.dropped` | Pulse Kafka producer가 있을 때만 |

## 의도된 차이와 원본 결함 처리

| 항목 | Java | Go |
|---|---|---|
| 예외 | stack trace(stderr) 후 계속 | stderr 한 줄 오류 후 계속. 반복 명령은 그 주기만 건너뛰고 다음 주기를 계속한다(Java와 같은 주기 단위). |
| 인자 없는 realtime/xlog/xlist/tagcnt(D-C7) | NPE | 사용법 한 줄 오류 |
| `xlog … 0`, 음수·overflow elapsed(D-C7) | 주기마다 ArithmeticException/AIOOBE | 같은 int32 산술로 계산하고 주기마다 한 줄 오류 |
| `realtime … SUM` 중 값 없는 object(D-C8) | 주기마다 NPE | 주기마다 한 줄 오류 |
| counter·dashboard에서 family 없는 type | NPE | 한 줄 오류(dashboard는 1회 후 종료) |
| `dashboard`의 javaee·host 외 family(D-C3) | swallowed non-local return으로 2초마다 반복할 것으로 판단 | `not supported : <type>` 1회 출력 후 종료 |
| `debug` 그 밖의 인자(D-C4) | MatchError | 한 줄 오류. `debug _`는 Java처럼 아무것도 하지 않는다 |
| `xlist` objType·minElapsed 무시(D-C5) | 무시 | **Java와 같게 무시**(출력 호환). count 상한만 적용 |
| `tagcnt top100` TEXT_HASH를 `error` 사전에서 조회(D-C6) | 그대로 | **Java와 같게** `error` 사전 |
| `xlist` 시각 형식 오류 | `getTime`이 0을 돌려 출력 없음 | 한 줄 오류. 형식은 lenient SimpleDateFormat처럼 `HH` 2자리, 나머지는 숫자열, 범위 초과는 이월, 뒤 문자열 무시 |
| `tagcnt data` 분 데이터 없음 | NPE | 한 줄 오류 |
| stdin EOF(D-C1) | NPE stack trace busy loop | 입력 루프만 끝내고 로그 한 줄. 서버는 계속 실행하고, 실행 중이던 반복 명령은 종료 시까지 계속 출력한다(Java도 입력이 없으면 thread가 계속 돈다). |
| 반복 명령 첫 주기 | 새 thread에서 즉시(다음 prompt와 섞일 수 있음) | `Execute` 안에서 동기 실행해 다음 prompt 전에 출력. 이후 주기는 2초마다 job goroutine |
| 반복 명령 정지 | 다음 입력 뒤 최대 2초(sleep 경계) | 다음 입력에서 job ctx를 cancel하고 끝날 때까지 기다린다(진행 중 조회도 취소). 취소된 조회의 오류는 출력하지 않는다. |
| 숫자 locale | JVM 기본 locale | `,` 천 단위 고정. NaN은 JDK 17처럼 `NaN`, 무한대 `∞` |
| `Double.toString` | JDK 8~18 FloatingDecimal | 최단 표현. JDK 8/17과 다른 값은 oracle의 `1.0E23`(JDK `9.999999999999999E22`), `2.0E23`(JDK `1.9999999999999998E23`) 두 건만 확인했다(tag count에서 실질적으로 나오지 않는 크기). |
| object 순서 | ObjectMap 순서 | ObjHash 오름차순 |
| `quit` | `System.exit(0)` | `Config.Quit` 호출 → cmd가 signal ctx를 cancel해 일반 SIGTERM과 같은 graceful shutdown(exit 0) |
| SIGHUP | JVM shutdown hook | 모드와 무관하게 SIGINT/SIGTERM과 같은 graceful shutdown(`cmd/scouter-go` `shutdownSignals`). 기동 시 이미 ignore 상태(`nohup`)면 구독하지 않아 계속 무시한다. |
| SIGQUIT(`kill -3`, Ctrl-\) | JVM thread dump(stdout) 후 계속 실행 | 모든 goroutine stack(`pprof` goroutine debug=2)을 stderr에 쓰고 계속 실행. 모드와 무관하며 logger 생성 직후부터 적용된다(그 전에는 Go 기본 동작). |

## 미검증

- Java 콘솔 서버 실제 출력과의 행 단위 대조(같은 입력을 Java `-console` 서버에 넣는 도구 없음). 명령 handler 출력은 Scala 코드 판독과 원본 helper oracle(숫자·패딩·txid)로만 확인했다.
- jline 1(scala.tools.jline)의 실제 Tab 후보 열 배치, Ctrl-C·Ctrl-D·EOF 동작 실행 확인.
- linux에서의 termios·pty 테스트 실행(`GOOS=linux go vet`만 통과, darwin에서만 실행).
- SIGTSTP(Ctrl-Z) 후 재개 시 raw mode 재설정은 하지 않는다.

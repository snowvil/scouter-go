# `-console` 콘솔 모드 계약 (TermMain)

정본은 `baseline.json`의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. Go 측 대응은 현재 `internal/console`과 `internal/server/console_source.go`를 따르며 구현 계약은 [console CONTRACT](../../internal/console/CONTRACT.md)에 있다. 근거 약어: `J/`=`scouter.server/src/main/java/scouter/server/`, `S/`=`scouter.server/src/main/scala/scouter/server/`, `H/`=`S/term/handler/`, `CM/`=`scouter.common/src/main/java/scouter/`. 원본 동작은 **코드 판독 결과**다. Java 콘솔 서버의 실제 출력과 대조하지는 않았다.

## 1. 진입과 일반 모드와의 차이

- 실행: `startcon.sh`·`startcon.bat`·`sample1.startcon.sh`가 `java -Xmx512m -cp scouter-server-boot.jar scouter.boot.Boot ./lib -console`로 시작한다(`scouter.server.boot/assembly/scripts/`).
- `Main.main`(`J/Main.java:40-97`)은 Logo 출력부터 `TextCacheReset.engine()`까지 일반 모드와 **같은 기동**을 마친 뒤 `ShellArg`로 인자를 본다(`:66`). `-console` 키가 있으면 `TermMain.process(sarg)`를 main thread에서 실행하고 돌아오지 않는다(`:67-68`).
- `ShellArg`(`CM/util/ShellArg.java:31-50`)는 `-`로 시작하는 인자를 키로 삼는다. 다음 인자가 `-`로 시작하지 않으면 값으로 가져간다. 따라서 `-console foo`처럼 쓰면 `foo`가 값이 되지만 콘솔 판정에는 영향이 없다.

| 항목 | 일반 모드(`Main.java:69-95`) | `-console`(`Main.java:67-68`) |
|---|---|---|
| 종료 신호 파일 | `<pid>.scouter`를 cwd에 만들고(실패하면 `$HOME/<pid>.scouter.run`, 그것도 실패하면 `System.exit(1)`) `deleteOnExit`. 파일이 사라지면 `ShutdownManager.shutdown()` 후 `System.exit(0)` | **없음** |
| `lock.dat` heartbeat | main loop가 1초마다 `DBCtr.updateLock()`(`S/db/DBCtr.scala:39-44`) | **없음**. 기동 5초 뒤부터 다른 프로세스가 lock을 stale로 보고 지울 수 있다(`DBCtr.scala:51-53`, recovery-retention.md §6) |
| JRE 버전 출력 | `System JRE version : …`를 stdout에 | 없음 |
| 종료 | 신호 파일 삭제, SIGTERM(shutdown hook) | `quit`/`q` → `System.exit(0)` → shutdown hook. SIGTERM도 같다 |
| 수집·조회 | 동일 | 동일(콘솔은 같은 JVM 안에서 cache·DB를 직접 읽는다) |

## 2. TermMain(`J/term/TermMain.java:31-70`)

- ANSI 사용 여부: 기본은 Windows가 아니면 켠다(`:35`). 인자 `-ansi`면 켜고, 없고 `-noansi`면 끈다(`:37-40`, `-ansi`가 우선).
- 종료 시 `TerminalFactory.get().restore()`를 ShutdownManager에 등록한다(`:42-50`).
- 입력: `scala.tools.jline.console.ConsoleReader`(Scala 2.x에 포함된 jline 1 계열). history를 켜지만 파일 history를 지정하지 않아 **메모리 history**다(`:52`). Tab 완성은 `StringsCompleter(Help.words())`로 8개 단어 `help, quit, objtype, object, counter, realtime, tagcnt, dashboard`다(`H/Help.scala:50-62`). `xlog, xlist, visitor, debug`는 완성되지 않는다.
- prompt: `AnsiPrint.green("scouter> ")`(`:56`).
- loop: `readLine()` → `cmd.trim()` → `ProcessMain.process`(`:57-64`). 모든 `Throwable`은 `printStackTrace`(stderr) 후 계속한다.
- `ScouterHandler`(`J/term/ScouterHandler.java:22-25`)는 RUNTIME annotation 선언뿐이다. `Help.scala`가 import만 하고 어디에도 붙이지 않는다. 동작에 영향이 없다.

`AnsiPrint`(`J/term/AnsiPrint.java:20-64`): `enable`이면 `ESC[3Xm` + 문자열 + `ESC[0m`. 색 코드는 red 31, green 32, yellow 33, blue 34, cyan 36이다(`purple`, `white`, `black` 상수는 함수가 없다). 실제로 쓰는 곳은 prompt·`objtype`·`counter`·`xlog` 막대(green), `realtime` objName(blue)뿐이다.

## 3. 명령 해석(`H/ProcessMain.scala:37-100`)

1. `loopProcess += 1`. **빈 줄을 포함해 모든 입력마다** 증가한다(`:41`).
2. `StringUtil.firstWord(cmd, " ").toLowerCase()`로 첫 단어를 고른다(`:43`). 대소문자를 구분하지 않는다.
3. 인자는 `cut(cmd, word)` = `cmd.trim().substring(word.length).trim()`(`:97-99`)이다. 원래 대소문자를 그대로 둔다.

| 첫 단어 | 처리 |
|---|---|
| `help` | `Help.help` |
| `q`, `quit` | `Help.quit` |
| `objtype` | `objType()` |
| `object` | `objectList(cmd)` |
| `counter` | `counterList(cmd)` |
| `dashboard`, `realtime`, `xlog`, `xlist`, `tagcnt`, `visitor`, `debug` | 각 handler `process(인자)` |
| `""`(빈 줄) | 아무것도 하지 않는다. 그래도 1단계에서 증가했으므로 실행 중인 반복 출력이 멈춘다 |
| 그 밖 | `Help.help` |

반복 출력 명령(`dashboard`, `realtime`, `xlog`)은 daemon thread를 띄운다. thread는 시작할 때의 `loopProcess`를 기억하고 값이 바뀌면 끝난다. 다음 입력이 들어오면(Enter만 쳐도) 멈춘다는 뜻이다. `loopProcess`는 volatile이 아니다(`:38`). 실제로는 sleep 경계에서 보이지만 JMM상 보장은 없다.

`StringUtil` 동작(`CM/util/StringUtil.java`):
- `firstWord`·`lastWord`(`:31-52`)는 `StringTokenizer` 기반이다. 입력이 빈 문자열이면 `""`.
- `tokenizer(target, delim)`(`:54-67`)는 **입력이 빈 문자열이면 null**을 돌려준다(빈 token은 버림). 그래서 인자 없는 `realtime`·`xlog`·`xlist`·`tagcnt`는 NPE가 난다(§7).
- `leftPad`(`:277-288`)는 앞을 공백으로 채우고, 길면 자르지 않는다. `rpad`(`:359-367`)는 뒤를 채운다.

## 4. 공통 출력 형식

- 시각 `DateUtil.getLogTime(ms)`(`CM/util/DateTimeHelper.java:254-270`): JVM 기본 timezone의 `HH:mm:ss.SSS`(12자). 2000년 이전이면 `00:00:00.000`.
- 숫자 `FormatUtil.print(o, "#,##0")`(`CM/util/FormatUtil.java:29-45`): `Number`(모든 `NumberValue` 포함)는 JVM 기본 locale의 `DecimalFormat`이다. 반올림은 HALF_EVEN, 천 단위 `,`. null은 `""`. 그 밖(예: `BooleanValue`, `TextValue`)은 `toString()`.
- 출력은 `println`(Scala `Console.out` = stdout)이며 Logger 파일에 남지 않는다. 예외 stack trace는 stderr로 간다.

## 5. 명령별 동작

### 5.1 `help [아무거나]`(`H/Help.scala:25-43`)

인자와 무관하게 다음 16줄을 앞에 탭을 붙여 그대로 출력한다. 원문 철자를 유지한다(`dashdoard`, `Count=20000` 포함).

```
	help = Help
	quit = Quit
	objtype = ObjType List
	object = Object List
	counter [objType] = counter list for the objType
	visitor [objType] = today visit users for the objType
	dashboard [objType]  = javaee's dashboard
	dashboard [objType]  [Counter] = host's dashdoard
	realtime [objType]  TPS [SUM/AVG] = realtime counter view
	xlog [objType]
	xlist [objType] [Time=10:12:23.123] [Count=20000] [minElapsed=0]
	tagcnt group
	tagcnt tag [group]
	tagcnt top100 [objType] [group] [tag] 
	tagcnt data [objType] [group] [tag] [inx]
	debug queue
```

(마지막 `tagcnt top100` 줄은 끝에 공백 1개가 있다. help의 `Count=20000`은 실제 기본값 10000(§5.9)과 다르다.)

### 5.2 `q` / `quit`(`Help.scala:45-49`)

`bye bye!!` 출력 후 `TermMain.exit()` → `System.exit(0)` → shutdown hook(`ShutdownManager.shutdown`, terminal restore). 서버 프로세스 전체가 끝난다.

### 5.3 `objtype`(`ProcessMain.scala:63-72`)

- 원천: `AgentManager.getObjPacks()`. 메모리 object map 전체이며 **dead object도 포함**한다(`S/core/AgentManager.scala:311-313`).
- 출력: objType을 중복 제거·오름차순(`TreeSet` 후 `Arrays.sort`)으로 정렬해 줄마다 `\t` + green(objType)으로 쓴다.

### 5.4 `object [filter]`(`ProcessMain.scala:73-83`)

- token은 `tokenizer(cmd, " ,")`이며 `cmd`에 `object` 자신이 들어 있다. 두 번째 token이 filter다.
- 원천: `AgentManager.getLiveObjHashList()`. alive인 object만, object map 순서다(`AgentManager.scala:230-245`).
- 줄 형식: `"\t" + rpad(objType,20) + "\t" + rpad(objName,30) + "\t" + "alive"`. filter가 있으면 **이 줄 문자열**에 filter가 부분 문자열로 들어 있을 때만 출력한다.

### 5.5 `counter <objType>`(`ProcessMain.scala:85-95`)

- 인자가 없으면 출력하지 않는다.
- 원천: `CounterManager.getCounterEngine().getAllCounterList(objType)`(`CM/lang/counters/CounterEngine.java:292-305`). family counter와 objType counter 중 `isAll()`인 이름이다.
- 오름차순 정렬 후 `\t` + green(name)을 출력한다. objType을 모르면 빈 출력이다. family가 없는 objType이면 `listCounters()`가 null이라 NPE가 난다(stack trace).

### 5.6 `dashboard <objType> [counter]`(`H/Dashboard.scala:37-134`)

- 인자의 첫 단어로 `CounterEngine.getObjectType`을 찾는다. 없으면 `no object for <인자>`를 출력하고 끝난다.
- daemon thread가 2초마다 반복한다. 대상은 `AgentManager.getLiveObjHashList(objType)`이다.
- family `javaee`(`:68-80`):
  - REALTIME CounterCache에서 `ActiveService`(INST = 값이 있는 object 수, ACT = 합), `TPS`(합), `ElapsedTime`(평균 = 합/개수), `GcCount`(합)를 구한다.
  - 각 값은 `"#,##0"` 형식이며 5칸 왼쪽 공백 채움이다.
  - 줄 형식: `HH:mm:ss.SSS INST ACT TPS RTIME GCCNT`(공백 1칸 구분).
  - header `"            " + "  INST" + "   ACT" + "   TPS" + " RTIME" + " GCCNT"`는 전역 `lines`가 0일 때 출력한다. `lines`는 0..10을 순환하므로 header는 11줄마다 나온다.
- family `host`(`:96-132`):
  - counter 이름은 `StringUtil.lastWord(cmd, " ")`다. 인자가 objType 하나뿐이면 counter 이름이 objType이 되어 대개 값이 없다.
  - object마다 `getAgentName(objHash).substring(1)`(앞 `/` 제거)을 열 이름으로 쓴다. `TreeMap`이라 열은 이름 오름차순이다.
  - 열 폭은 `max(5, 가장 긴 이름)`이다. header는 `"            "` + 열마다 `" " + leftPad(name, 폭)`이며, `lines==0`이거나 열 **개수**가 바뀌면 출력한다.
  - 값 줄은 시각 + 열마다 `" " + leftPad(format(v), 폭)`.
- 그 밖 family: `not supported : <objType>`을 출력한 뒤 `return`. 이 `return`은 Scala non-local return이라 `NonLocalReturnControl`(`Throwable`)을 던지고, 같은 loop의 `case x: Throwable =>`(`:59`)가 삼킨다. 그 결과 loop는 끝나지 않고 **2초마다 같은 메시지를 반복 출력**할 것으로 판단한다(코드 판독, 실행 미확인; §7 D-C3).
- 값이 `Number`가 아니면(`BooleanValue` counter) `ClassCastException` stack trace를 2초마다 출력한다.

### 5.7 `realtime <objType|이름조각|*> <counter> [SUM|AVG|기타]`(`H/REALTIME.scala:33-107`)

- `tokenizer(인자, " ")`의 token이 2개 미만이면 아무것도 하지 않는다. 인자가 비면 NPE다.
- 대상: 2초마다 `AgentManager.filter(token0)`(`AgentManager.scala:292-307`)으로 정한다.
  - `*`면 alive 전체.
  - 알려진 objType이면 그 type의 **모든**(dead 포함) object.
  - 아니면 objName에 token0이 들어 있는 모든 object(dead 포함).
- 대상이 없으면 `HH:mm:ss.SSS ...`.
- mode가 없으면 object마다 `HH:mm:ss.SSS <blue(objName)> <값|null>`. 값은 `FormatUtil.print(Value, "#,##0")`.
- mode가 있으면 합계를 구한다. 값이 없는 object가 하나라도 있으면 NPE stack trace가 난다. 출력은 `SUM`→합, `AVG`→합/개수, 그 밖→합이며 형식은 `HH:mm:ss.SSS <counter> <값>`이다. mode 비교는 대소문자를 무시한다.

### 5.8 `xlog <objType|이름조각|*> [maxSec]`(`H/XLOG.scala:37-101`)

- `maxSec`은 `CastUtil.cint(token1)`, 기본 10이다. 0이면 `/ by zero` stack trace를 2초마다 출력한다.
- 커서 `loop`, `index`는 **object 전역 변수**다(`:57-58`). 명령을 다시 실행해도 이전 커서에서 이어간다. 첫 실행은 `loop=0,index=0`이다.
- 2초마다 `XLogCache.get(objHashSet, loop, index, 0)`(`S/core/cache/XLogCache.scala:24,35-37`, 20480개 loop cache)에서 커서 이후 XLog를 읽고 커서를 갱신한다. 대상 hash 집합은 realtime과 같은 `AgentManager.filter`다.
- 막대: 60칸을 `'_'`로 채운다. XLog마다 칸 `elapsed*60/(maxSec*1000)`(60 이상은 59)에 오류면 `E`, 아니면 `#`를 찍는다. 이미 `E`인 칸은 덮지 않는다. elapsed가 음수면 배열 범위 예외다.
- 줄 형식: `<첫 XLog endTime의 HH:mm:ss.SSS 또는 현재 시각> <green(60자 막대)> <maxSec>sec <건수 #,##0>`.

### 5.9 `xlist <objType> <HH:mm:ss.SSS> [count=10000] [minElapsed=0]`(`H/XLIST.scala:43-93`)

- 한 번만 실행한다(반복 없음).
- 시작 시각: `DateUtil.getTime(오늘yyyyMMdd + token1, "yyyyMMddHH:mm:ss.SSS")`. 오늘 날짜는 JVM 기본 timezone이다.
- 원천: 오늘 날짜 XLog DB `XLogRD.readByTime(date, time, time+2000, handler)`. 2초 구간이다.
- **objType 필터와 minElapsed는 계산만 하고 쓰지 않는다.** 모든 object의 XLog를 출력한다(원본 결함 D-C5).
- 건수가 count에 이르면 handler의 non-local return으로 읽기를 멈춘다. 이 `return`은 `process` 자신의 반환이라 정상 동작한다고 판단한다. 단 `XLogRD.readByTime`이 내부에서 `Throwable`을 잡는지는 확인하지 않았다(§8).
- 줄 형식(공백 구분): `leftPad(순번 #,##0, 5) HH:mm:ss.SSS(endTime) <txid Hexa32> <objName> <IP> <service 이름> <elapsed #,##0>ms`. 그 뒤에 `sqlCount>0`이면 `sql:<n>/<ms>ms `, `apicallCount>0`이면 `api:<n>/<ms>ms `, `userAgent!=0`이면 `<UA 문자열> `을 붙인다.
- service·UA 이름은 `TextRD.getString(XLog 시각 날짜, …)`(config-behavior.md §3.2)로 얻는다.
- token이 2개 미만이면 NPE 또는 배열 범위 예외다. 시각 형식이 틀리면 `getTime` 결과에 따른다(확인하지 않음).

### 5.10 `tagcnt …`(`H/TAGCNT.scala:46-130`)

| 형식 | 원천 | 출력 |
|---|---|---|
| `tagcnt group` | `TagCountConfig.getTagGroups()`(`S/tagcnt/TagCountConfig.scala:56-58`, 등록 순서 `StringKeyLinkedMap`) | 줄마다 `\t<group>` |
| `tagcnt tag <group>` | `getTagNames(group)`(`StringSet` 열거 순서) | 줄마다 `\t<tagName>` |
| `tagcnt top100 <objType> <group> <tag>` | 오늘 날짜 `TagCountProxy.getTagValueCountWithCache(date, objType, group, tag, 100)` | 값마다 `\t<순번> <값> \t= <count>`. 값이 TEXT_HASH면 **`error` text 사전**에서 찾아 `\t<순번> <문자열>(<값>) \t= <count>`(원본은 tag 종류와 무관하게 `TextTypes.ERROR`를 쓴다: D-C6) |
| `tagcnt data <objType> <group> <tag> [inx]` | 위 top100에서 `inx`번째(없거나 음수면 첫 번째) 값의 `getTagValueCountData` → `float[1440]` | 24줄. `leftPad("H"+h,5) + " "` 뒤에 분별 60개 값을 `, `로 연결(`#,##0.0`). inx=0이면 아무것도 하지 않는다 |

- 첫 token이 4개 형식에 맞지 않으면 출력하지 않는다. 인자가 없으면 NPE다.
- `inx`가 숫자가 아니면 `NumberFormatException`이 난다.

### 5.11 `visitor <objType|이름조각|*>`(`H/VISITOR.scala:39-58`)

- 첫 단어가 알려진 objType이면 `VisitorDB.getVisitorObjType(objType)`(오늘 HLL cardinality, `S/db/VisitorDB.scala:143-149`)를 `\t<#,##0>`으로 출력한다.
- 아니면 `AgentManager.filter(word)` 대상 object마다 `getVisitorObject(objHash)`가 0보다 클 때만 `\t<objName> = <#,##0>`을 출력한다.
- 인자가 비면 `filter("")`가 모든 object(objName에 `""` 포함)를 고른다.

### 5.12 `debug queue`(`H/DEBUG.scala:43-56`, `J/core/ServerStat.java:7-35`)

- 인자가 정확히 `queue`면 `ServerStat.toString("", null)`을 출력한다. 키를 `Arrays.sort`로 오름차순 정렬해 줄마다 `<key>=<value>`이며 마지막에 빈 줄이 하나 더 생긴다(`println`이 끝 `\n` 뒤에 줄바꿈 추가).
- 인자가 `_`면 아무것도 하지 않는다. 그 밖(빈 인자 포함)은 `case "_"`가 와일드카드가 아닌 문자열 `"_"`라 `scala.MatchError` stack trace가 난다(D-C4).
- `ServerStat` 값은 각 queue 소비 thread가 **항목 하나를 꺼낸 직후** 남은 크기를 넣은 것이다. queue가 비어 소비가 멈추면 마지막 값이 그대로 남는다. 한 번도 소비하지 않은 queue는 키가 없다. 상한 1000개 `StringKeyLinkedMap`이다.

| 키 | 기록 위치 | queue 용량 |
|---|---|---|
| `alert.core.queue` | `S/core/AlertCore.scala:33` | 10000(`S/core/CoreRun.scala:7`) |
| `profile.core-r.queue` | `S/core/ProfileDelayingsRecoverCore.scala:36` | — |
| `profile.core.queue` | `S/core/ProfileCore.scala:38` | `profile_queue_size` |
| `profile.core0.queue` | `S/core/ProfilePreCore.scala:46` | `profile_queue_size` |
| `profile.db.queue` | `S/db/XLogProfileWR.scala:64` | `profile_queue_size` |
| `span.core.queue` | `S/core/SpanCore.scala:39` | `span_queue_size`(1000) |
| `summary.db.queue` | `S/db/SummaryWR.scala:40` | — |
| `text.core.queue` | `S/core/TextCore.scala:37` | 10000(`S/core/CoreRun.scala:7`) |
| `text.db.queue` | `S/db/TextWR.scala:57` | 100000 |
| `visitor.core.queue` | `S/core/VisitorCore.scala:48` | 10000(`S/core/CoreRun.scala:7`) |
| `xlog.core-r.queue` | `S/core/XLogDelayingsRecoverCore.scala:38` | — |
| `xlog.core.queue` | `S/core/XLogCore.scala:52` | `xlog_queue_size` |
| `xlog.core0.queue` | `S/core/XLogPreCore.scala:38` | `xlog_queue_size` |
| `xlog.db.queue` | `S/db/XLogWR.scala:56` | `xlog_queue_size` |
| `zipkin-span.db.queue` | `S/db/ZipkinSpanWR.scala:57` | `span_queue_size`(1000) |

(`—`는 용량을 이번에 확인하지 않은 queue다.)

**Go 대응(구현, [console CONTRACT](../../internal/console/CONTRACT.md) "`debug queue` 키")**: 위 Java stage queue 15개는 같은 모양으로는 Go에 없다(UDP worker가 engine lock 아래에서 Core 처리를 하고, 저장은 kind별 ingest writer queue와 storage의 XLog·profile·text writer queue가 한다: [ingest CONTRACT](../../internal/ingest/CONTRACT.md) §13). Go writer queue는 `ingest.writer.<kind>.*` 키로 나온다. Java 키 이름을 가짜 값으로 채우지 않고, Go에 실제로 있는 queue·통계를 같은 형식(`key=value`를 키 오름차순으로, 마지막에 빈 줄 하나)으로 출력한다. 값은 모두 정수다.

| Go 키 | 의미 |
|---|---|
| `agentcall.kube-seq.queue`, `agentcall.cpu-dump.queue`, `pulse.xlog.queue`, `pulse.profile.queue`, `pulse.kafka.queue` | `server.Runtime.ServerStat()`과 같은 이름·값(현재 길이). Pulse 키는 Pulse가 켜졌을 때, `pulse.kafka.queue`는 Kafka producer가 있을 때만 |
| `agentcall.{kube-seq,cpu-dump}.{enqueued,done,failed,dropped,abandoned}` | Agent 호출 queue 누적 counter |
| `udp.queue`, `udp.queue_capacity` | UDP worker queue 현재 길이·용량 |
| `ingest.completed`, `ingest.rejected`, `ingest.storage_errors`, `ingest.xlogs`, `ingest.dropped_xlogs`, `ingest.skipped_stacks`, `ingest.geoip_errors`, `ingest.geoip_text_errors`, `ingest.objects` | ingest 누적 counter와 메모리 object 수(현재 값). `geoip_text_errors`는 `geoip_errors` 중 city text 저장 실패 |
| `udp.datagrams`, `udp.packets`, `udp.rejected`, `udp.malformed`, `udp.expired`, `udp.handler_errors`, `udp.aborted_assemblies`, `udp.aborted_packets`, `udp.shutdown_discarded`, `udp.assembly_rejected` | UDP 수신 누적 counter |
| `udp.socket_drops` | kernel이 이 socket에서 버린 datagram 누적 수. `UDPStats.KernelCounters=true`인 Linux에서만 |
| `tcp.accepted`, `tcp.rejected`, `tcp.errors`, `tcp.client_requests`, `tcp.stack_uploads`, `tcp.active` | TCP 누적 counter와 현재 연결 수 |
| `server.error_log_queue`, `server.dropped_error_logs`, `server.maintenance_errors`, `server.purged`, `server.recoveries` | Runtime 오류 로그 queue 길이와 누적 counter |
| `pulse.{xlog,profile}.{enqueued,sent,dropped,failed}`, `kafka.failed`, `kafka.dropped` | Pulse·Kafka가 켜졌을 때만 |

Java는 소비 thread가 마지막으로 꺼낸 시점의 잔량을 남기지만 Go의 `.queue` 값은 호출 시점의 길이다. Java에서 한 번도 소비하지 않은 queue는 키가 없지만 Go는 존재하는 queue를 항상 보인다. `debug _`는 Java처럼 아무것도 하지 않고, 그 밖의 인자는 한 줄 오류다(D-C4).

## 6. 데이터 원천 ↔ Go API

| Java 원천(명령) | 의미 | Go 구현 | 상태 |
|---|---|---|---|
| `AgentManager.getObjPacks()`(objtype) | 메모리 object 전체(dead 포함) | `ingest.Engine.Objects(ctx, "")`(`internal/ingest/engine.go:343-405`, Alive 계산 포함) | 구현. ObjHash 오름차순으로 정렬한다(결정) |
| `getLiveObjHashList()`(object) | alive만, object map 순서 | 위 결과에서 `Alive` 필터 | 구현(순서 차이: Java ObjectMap 순서는 재현하지 않고 ObjHash 오름차순, 결정) |
| `getLiveObjHashList(objType)`(dashboard) | type별 alive | 위 + objType 필터 | 있음(조합) |
| `AgentManager.filter(word)`(realtime·xlog·xlist·visitor) | `*`/type/이름 부분일치 | `Objects` 위에 console 쪽 함수로 구현 | 구현 |
| `isObjType(word)` | 그 type object가 map에 있는가 | `Objects` 결과로 판정 | 조합 |
| `getAgentName(objHash)` | objName | `ingest.Engine.Object(ctx, "", hash)`(`engine.go:407`) | 있음 |
| `CounterManager.getCounterEngine().getObjectType/getAllCounterList`(dashboard·counter) | counter 정의 | `counters.Store.Engine().ObjectType(name)`(`internal/counters/store.go:107`, `internal/counters/engine.go:62`), family는 `ObjectType.Family`, `Counter.All` | 있음. `listCounters`(family + type, `All`만) 조합은 console이 구현 |
| `CounterCache.get(CounterKey(h, name, REALTIME))` | 최신 실시간 값 | `ingest.Engine.Counter(ctx, hash, name, kind)`(`engine.go:428`) | 있음 |
| `XLogCache.get(set, loop, index, 0)`(xlog) | loop cache 커서 조회 | `ingest.Engine.XLogsRealtime(ctx, service.RealtimeQuery{Loop, Index, ObjectHashes})`(`engine.go:478`, `internal/service/backend.go:16-28`) | 있음. 빈 hash 목록은 Java가 조회하지 않는 경우라 console이 먼저 거른다 |
| `XLogRD.readByTime(date, from, to, h)`(xlist) | 날짜 XLog 시간 구간 | `ingest.Engine.XLogsByTime(ctx, date, from, to)`(`internal/ingest/packet_state.go:426`) | 있음. 건수 상한은 결과를 자르거나 storage 상한(`MaxQueryRecords`)을 따른다 |
| `TextRD.getString(date, div, hash)`(xlist·tagcnt) | text 사전 | `ingest.Engine.Text(ctx, date, div, hash)` → `storage.TextContext` | 있음. 일별 div는 해당 날짜에서 miss이면 영구 text로 fallback한다 |
| `TagCountConfig.getTagGroups/getTagNames` | tag 목록 | `internal/service/tagcount.go:10-18`(`tagNames`)와 `TAGCNT_DIV_NAMES` 순서 `service, alert` | 구현(같은 목록을 console 안에 둔다) |
| `TagCountProxy.getTagValueCountWithCache(…,100)` | top100 | `ingest.Engine.TagValues(ctx, date, typ, group, name)`(`internal/ingest/aggregation.go:19-26`) | 구현(top 100, count 내림차순) |
| `TagCountProxy.getTagValueCountData` | 분별 1440값 | `ingest.Engine.TagValueSeries(…)`(`aggregation.go:27-29`) | 있음 |
| `VisitorDB.getVisitorObjType(objType)` / `getVisitorObject(objHash)` | 오늘 HLL 값 | `ingest.Engine.VisitorRealtime(ctx, nil, objType)` / `VisitorRealtime(ctx, []int32{hash}, "")`(`internal/server/console_source.go` `VisitorObjType`/`VisitorObject`) | 있음. 전자는 type HLL, 후자는 지정 object HLL을 조회한다(`storage.VisitorRealtimeContext`) |
| `ServerStat`(debug queue) | stage queue 잔량 | Java와 같은 Core 단계 queue는 없다. `internal/server/console_source.go` `Stats`가 실제 ingest writer·Agent 호출·Pulse queue와 UDP/TCP/ingest 통계를 조합한다. UDP worker queue 길이·용량은 `UDPReceiver.Stats()`의 `QueueLen`/`QueueCap`으로 읽는다 | 구현: Go 실제 queue·통계 키 집합(§5.12 "Go 대응") |
| `DateUtil.getLogTime`, `yyyymmdd()` | JVM 기본 TZ 시각·날짜 | `scoutertime.Date(ms, loc)`(`internal/scoutertime/date.go:24`), `storage.JavaDate`. `HH:mm:ss.SSS`는 console이 `scoutertime.Millis` 기반으로 구현 | 구현 |
| `FormatUtil.print(…, "#,##0")`, `"#,##0.0"` | DecimalFormat HALF_EVEN | console format 함수 | 구현. JDK 8/17 `ConsoleFormatOracle` golden(각 246줄)과 비교 |
| `ProcessMain.loopProcess` | 반복 출력 중단 | 반복 job 1개, 다음 입력에서 cancel·대기 | 구현 |
| `<pid>.scouter` 신호 파일 | 일반 모드 종료 | `cmd/scouter-go/exitfile.go`(`-exit-file`, 기본 켬, 1초 확인). 콘솔 모드에서는 만들지 않는다 | 구현 |
| `DBCtr.updateLock` | lock heartbeat | `Runtime.startHeartbeat`(`internal/server/runtime.go:246`)가 모드와 무관하게 1초마다 실행 | 있음(Go가 더 엄격: 콘솔에서도 유지) |

## 7. 원본 결함·취약 동작 후보

| ID | 내용 | 근거 | 재현 |
|---|---|---|---|
| D-C1 | stdin EOF(파이프 종료, nohup, systemd)에서 `readLine()`이 null을 돌려준다. `cmd.trim()` NPE가 `catch Throwable`에 잡혀 **stack trace를 끝없이 찍는 busy loop**가 된다 | `TermMain.java:57-64` | `startcon.sh < /dev/null` |
| D-C2 | 콘솔 모드에는 lock heartbeat가 없다. 5초 뒤 다른 서버가 같은 DB lock을 지우고 동시에 쓸 수 있다 | `Main.java:67-68,88-95`, `DBCtr.scala:45-53` | 콘솔 서버 기동 6초 뒤 같은 `db_dir`로 두 번째 서버 기동 |
| D-C3 | `dashboard`에서 javaee·host가 아닌 family면 non-local return이 삼켜져 2초마다 같은 메시지를 반복할 것으로 판단한다 | `Dashboard.scala:45-63` | family가 다른 objType으로 `dashboard`(실행 미확인) |
| D-C4 | `debug`에 `queue`·`_` 외 인자를 주면 `MatchError` | `DEBUG.scala:49-52` | `debug` |
| D-C5 | `xlist`가 objType 필터와 minElapsed를 적용하지 않는다. help의 `Count=20000`은 실제 기본값 10000과 다르다 | `XLIST.scala:49-54,58-90`, `Help.scala:36` | 두 type 수집 후 `xlist <type> <time>` |
| D-C6 | `tagcnt top100`이 TEXT_HASH 값을 항상 `error` 사전에서 찾는다 | `TAGCNT.scala:90-92` | service tag 중 TEXT_HASH 값 |
| D-C7 | 인자 없는 `realtime`/`xlog`/`xlist`/`tagcnt`는 `tokenizer("")==null` 때문에 NPE, `xlog … 0`은 0 나누기, 음수 elapsed는 배열 범위 예외 | §5 각 항 | 각 입력 |
| D-C8 | `realtime … SUM`에서 값이 없는 object가 하나라도 있으면 NPE로 그 주기 출력이 없다 | `REALTIME.scala:83-87` | counter가 일부 object에만 있을 때 |

## 8. Go 구현과 검증 범위

입력 처리·출력 형식은 `internal/console`, 데이터 공급은 `Source` 인터페이스를 구현한 `internal/server/console_source.go`가 맡는다. `-console`, `-ansi`, `-noansi`는 `cmd/scouter-go`에서 등록한다.

1. **EOF(D-C1)**: 입력 루프만 끝내고 로그 한 줄을 남긴다. 서버와 실행 중이던 반복 명령은 종료 시까지 계속 실행한다.
2. **lock heartbeat(D-C2)**: 콘솔에서도 유지한다(의도된 차이, [recovery-retention](recovery-retention.md) G8).
3. **종료 파일**: 일반 모드는 `<pid>.scouter`를 만들고 삭제를 감시한다(`-exit-file`, 기본 켬). 콘솔 모드는 만들지 않는다.
4. **입력 편집**: darwin/linux 터미널은 termios 기반 line editor로 Tab 완성(8단어)·메모리 history(500)를 제공한다. 비터미널과 그 밖의 OS는 단순 reader를 쓴다.
5. **`debug queue`**: Java Core 단계 queue 대신 실제 Go queue·통계를 `key=value` 오름차순으로 출력한다(§5.12).
6. **결함 처리**: D-C3·C4·C7·C8은 한 줄 오류로 보고하고, D-C5·D-C6은 Java 조회 의미를 유지한다.
7. **시각·숫자**: 시각·날짜는 `--timezone`, 숫자는 `,` 천 단위 HALF_EVEN이다. `quit`은 graceful 종료이며 SIGHUP도 기동 시 ignore 상태가 아니면 같은 경로를 탄다.

검증 코드는 다음과 같다. 이 목록은 이번 문서 정리에서 테스트를 재실행했다는 뜻이 아니다.

- Go 명령 출력 golden: `commands_test.go`의 `TestCommandsGolden`·`TestCommandsGoldenANSIAndQuit`, `testdata/golden/`의 고정 시각·seed 출력. Java 콘솔 서버 출력과의 대조는 남아 있다.
- 반복 중단·EOF: `TestRepeatStopsOnNextInput`, `TestStopWaitsForBlockedCycle`, `TestEOFKeepsRepeatUntilClose`.
- 실행 모드: `cmd/scouter-go/console_test.go`의 종료 파일·quit·EOF·SIGHUP·ANSI 우선순위 테스트.
- 숫자 형식: `TestJavaFormatOracle`이 커밋된 JDK 8/17 `ConsoleFormatOracle` golden과 비교한다.

## 9. 조사하지 못한 항목

- 콘솔 실제 실행 출력은 확보하지 않았다. jline 1의 EOF·Ctrl-C·Ctrl-D 동작도 실행으로 확인하지 않았다.
- `XLogRD.readByTime`이 handler 예외(non-local return 포함)를 잡는지 확인하지 않았다(xlist count 상한 동작).
- `XLogLoopCache.getList` 커서 의미는 [query-contract.md](query-contract.md)의 realtime 절을 따른다고 가정했다.
- `DateUtil.getTime`이 형식이 틀린 시각을 받았을 때의 반환값.
- `ProfileDelayingsRecoverCore`, `XLogDelayingsRecoverCore`, `SummaryWR` queue 용량.

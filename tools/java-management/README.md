# 관리 명령·계정·설정·HTTP 입력 Java oracle

고정 원본 Scouter 2.21.3(commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`)의 관리 handler와 HTTP 입력 handler를 임시 디렉터리에 **수정 없이** 컴파일하고 직접 실행해 golden을 만든다. 원본 source tree는 읽기만 한다. 데이터는 모두 synthetic이다.

```sh
export SCOUTER_SOURCE=/path/to/pinned/scouter        # 기본값 ../scouter
python3 scripts/java-management.py --generate /tmp/새-빈-디렉터리            # 전체 scenario
python3 scripts/java-management.py --generate /tmp/x --scenario account      # 일부만
python3 scripts/java-management.py --check                                   # internal/service/testdata/management 재생성 비교
python3 scripts/java-management.py --verify-xml /tmp/Go가-쓴-conf-복사본       # 원본 parser로 account*.xml, counters.site.xml 읽기
python3 scripts/java-management.py --classes-dir /tmp/classes                # classpath 출력
```

- `--generate`는 비어 있지 않은 디렉터리를 거절한다. `--check`는 golden의 `oracle-manifest.json`에 적힌 scenario를 임시 디렉터리에 다시 만들고 모든 파일을 byte 단위로 비교한다(JSON은 diff 출력). 차이가 하나라도 있으면 exit 1이다.
- `--verify-xml`은 디렉터리의 `account.xml`, `account_group.xml`, `counters.site.xml` 중 있는 파일을 원본 `AccountFileHandler.parse`, `GroupFileHandler.parse`, `CounterEngine.parse`로 읽어 JSON으로 출력한다. 하나라도 parse에 실패하면 exit 1, 대상 파일이 없으면 exit 2다. 원본 DB/conf가 아닌 **복사본**만 넣는다.
- 생성 후 leak check가 golden 전체에서 home 경로, 사용자 이름, hostname, 임시 디렉터리, `PATH=` 문자열을 찾으면 실패한다.

## 도구와 버전

| 항목 | 사용 | 이유 |
|---|---|---|
| common Java | PATH의 `javac --release 8`(index oracle과 공용) | 기존 oracle과 같은 방식 |
| server Java·seam·oracle | JDK 8 `javac -source 8 -target 8` | `Configure`, `CounterManager`, `TgConfig`가 `javax.xml.bind`를 쓴다 |
| Scala | `scouter.deploy/lib/scala/scala-compiler.jar` 2.10.4를 JDK 8에서 실행. Java source를 함께 넘겨 mixed compile | 원본 deploy 컴파일러 |
| 실행 | JDK 8(`/usr/libexec/java_home -v 1.8`, `SCOUTER_JAVA8_HOME`로 변경 가능) | JDK 8 내장 JAXB와 Xalan으로 실행 |
| descriptor 비교 | JDK 18(`/usr/libexec/java_home -v 18`, `SCOUTER_JAVA18_HOME`) | `descriptors` scenario만 같은 class를 JDK 18에서도 실행 |
| jar | `scouter.deploy/lib`의 json-simple 1.1.1, javax.servlet-api 3.1.0, httpclient 4.3.6, httpcore 4.3.3 | 원본 배포본 |
| javassist | `~/.m2/.../javassist-3.29.2-GA.jar`(원본 pom 버전, `SCOUTER_JAVASSIST_JAR`로 변경 가능) | deploy/lib의 3.18은 `AlertRuleLoader`의 `CtClass.toClass(Class)`가 없어 컴파일 실패 |

oracle JVM은 환경 변수 `SCOUTER_ORACLE_ENV_MARKER=synthetic` **하나만** 가진 상태로 실행한다(`-Duser.timezone=Asia/Seoul -Dfile.encoding=UTF-8 -Duser.language=en`). 이유는 아래 "원본 결함 후보" 1번이다.

## 실행하는 원본 클래스

- Scala: `AccountService`, `ConfigureService`, `AlertScriptingService`, `AgentInfo`, `ThreadList`, `LoginService`, `XLogService`, `XLogCache`, `XLogLoopCache`, `AccountManager`, `AccountFileHandler`, `GroupFileHandler`, `RemoteControl`, `RemoteControlManager`, `LoginUser`, `XmlUtil`, `ThreadScala`, `EnumerScala`, `CacheOut`, `AlertScriptLoadMessageCache`, `AlertScriptLoadMessageLoopCache`, `CounterTimeCache`.
- Java: `ServiceHandlingProxy`(dispatch·예외 삼킴), `ServiceHandler`, `ManageService`, `Configure`, `CounterManager`, `ScouterTgMtConfig`, `TgConfig`, `TgmConfig`, `TgCounterMapping`, `TelegrafInputHandler`, `CounterHandler`, `RegisterHandler`, `InfluxSingleLine`, `CounterProtocol`, `NumberValueWithTime`, `MeterCounter`, `MeterCounterManager`, `AsyncRun`, `PluginHelper`, `Neighbor`, `AlertRuleLoader`, `AlertConf`, `AlertEngine`, `AlertRule`, `AlertUtil`, `RealCounter`, common 전체.
- resource: `scouter.common/src/main/resources`(counters.xml), `scouter.server/src/main/resources`(기본 account.xml, account_group.xml)를 classpath에 그대로 둔다.

## Infrastructure seam(`seams/`)

| seam | 원본 | 대체 이유와 범위 |
|---|---|---|
| `Logger` | 파일 rotation logger | stderr 출력 + `seen` 목록. 각 case의 `logs`로 기록 |
| `ConfObserver`, `ShutdownManager` | 관찰자/hook 등록 | 호출만 받는다 |
| `LoginManager` | `AccountManager` 인증 + `CacheTable` | case가 synthetic session을 직접 등록. `getUser`는 모르는 session에 null(원본과 같음). `login`은 원본 본문(`AccountManager.authorizeAccount`, LoginUser 필드)을 따르되 session은 `KeyGen.next()` 대신 결정적 counter. `__local` 분기와 CacheTable 만료는 재현하지 않는다 |
| `core.CoreRun` | daemon 실행 flag | `false`로 시작해 `AccountManager` 5초 reload와 `RemoteControlManager` 5초 session 정리 daemon을 멈춘다 |
| `core.AgentManager`(Scala) | ObjectMap + alert/plugin 부수효과 | 원본 Scala 시그니처(`java.util.List[Int]`) 유지. `removeAgents`/`clearInactive` 본문은 원본 줄과 같다 |
| `core.AlertCore`, `db.TextRD`, `db.KeyValueStoreRW` | alert/text/KV | `PluginHelper`/`AlertScriptingService`/`XLogService` 심볼용. 호출되는 case 없음(`TextRD`는 SEARCH의 service/login/desc 필터에서만 쓰이며 xlogbounds는 필터를 보내지 않는다) |
| `db.XLogRD`(Scala) | XLogDataReader + IndexTimeFile | 메모리의 synthetic XLog bytes를 시간 순(포함 범위)으로 handler에 넘긴다. scan을 감싼 `catch Exception / Throwable`은 원본 줄과 같아 handler의 Scala non-local return(TRANX_LOAD_TIME_GROUP `max`, SEARCH 상한)을 원본처럼 삼킨다 |
| `db.XLogProfileRD`, `db.ZipkinSpanRD`, `core.app.SpanStepBuilder`(Scala) | profile/span 파일·변환 | `XLogService` 컴파일용. 호출되는 case 없음 |
| `core.cache.CounterCache` | 실시간 counter cache | 빈 결과 |
| `db.ObjectRD` | 일별 object DB reader | 원본처럼 호출마다 새 ObjectPack을 decode한다(handler의 tag 변경이 다음 호출로 새지 않음) |
| `db.ObjectWR` | 비동기 writer queue | 원본도 handler 안에서는 즉시 반영되지 않으므로 `remove/add` 호출만 기록 |
| `db.DBCtr` | DB lifecycle | `getRootPath()`만, 본문은 원본 `Configure.getInstance().db_dir`과 같다 |
| `netio.AgentCall`(Scala) | Agent TCP relay | 이 oracle 범위 밖. 호출되면 예외(호출하는 case 없음) |
| `netio.data.NetDataProcessor` | UDP 처리 queue | Telegraf/Counter HTTP가 넘긴 Pack/CAFE bytes를 기록 |
| `http.HttpServer` | Jetty 내장 | `getRemoteAddr`만 원본 줄 그대로 |
| `access/.../TelegrafOracleAccess` | 없음(bridge) | package-private `TelegrafInputHandler.count(line, remoteAddr)` 호출용. `receivedTime`을 고정하기 위해 `handlerRequest` 대신 사용 |

handler는 원본 `ServiceHandlingProxy.load()`/`process()`로 호출하므로 `RequestCmd` → method 연결과 예외 삼킴(printStackTrace)이 원본과 같다. 응답 끝에는 `TcpServiceWorker`처럼 `NoNEXT`를 붙인다.

## golden 형식(`internal/service/testdata/management/<scenario>/`)

- `<case>.request.bin`: handler가 읽는 body만(`writeText(cmd)+writeLong(session)` 이후). body 없는 명령은 0 byte.
- `<case>.response.bin`: handler가 쓴 `HasNEXT+item...` 뒤에 worker의 `NoNEXT`(0x04). handler 예외는 삼켜지므로 그때까지 쓴 bytes + `NoNEXT`다.
- `cases.json`: case마다 `command`, `item_kind`(pack/value), 원본 decoder로 해석한 `items`, `stderr_first_line`(삼킨 예외나 XML parser 메시지의 첫 줄), `logs`, 그리고 scenario별 `facts`(파일 해시, 메모리 상태, 파싱 결과).
- 그 밖의 파일: 기본 XML 사본, 저장 후 파일 스냅샷, Pack/CAFE bytes, `key-value-table.initial.json`.
- `oracle-manifest.json`: source SHA, 생성/검사 명령, JDK, 컴파일한 원본 파일의 SHA256·git blob, resource·외부 jar·oracle source 해시, 전체 파일 크기·SHA256.

### 추가 scenario

- `descriptors`: 원본 `RealCounter.getRealCounterDescription()`과 `PluginHelper.getPluginHelperDesc()`를 JDK 8과 JDK 18에서 각각 호출한다. 결과는 `descriptors.java8.json`, `descriptors.java18.json`(원소를 `methodName(params)`로 정렬)과 `summary.json`(두 JDK 집합 동일 여부, 개수)이다.
- `telegraf`, `telegraf-legacy`는 Go 재생용 설정 입력 `scouter.conf.input`(과 `scouter-telegraf.input.xml`)을 함께 남긴다.
- `telegraf-golden`: `internal/telegraf/testdata/golden_packs.json`의 입력(name/offsetMs/remote/body)만 뽑아 `input-steps.json`으로 고정한다. `goldenStart`는 `golden_test.go`에서 읽는다. 설정은 Go `sampleConfig`와 같은 `TgConfig.getSampleContents()`다. 실행은 두 JVM으로 나눈다.
  1. warm-up: `SEED_SITE`에서 시작해, 모든 줄이 첫 count에 pack을 낼 때까지 여러 pass 등록한다.
  2. replay: warm-up의 `counters.site.xml`을 들고 새 JVM에서 실행한다. 각 body를 원본 `TelegrafInputHandler.handlerRequest`(HashMap 중복 제거·순회 순서 포함)에 그대로 넘긴다. 이때 `HttpServletRequest`는 `getReader/getHeader/getRemoteAddr`만 있는 Proxy다.

  결과 `java_packs.json`은 Go golden과 같은 필드에 float/double의 IEEE `bits`를 더한 형식이다. `PerfCounterPack.time`은 호출 구간 안에 있음을 확인한 뒤 `goldenStart+offsetMs`로 바꾼다. `MeterCounter` 평균은 실제 시계를 쓰지만, replay는 1초 안에 끝나고 offset은 최대 22초라 30초 창 안에 든다.

- `login`: 원본 `LoginService.LOGIN`(원본 `AccountManager`의 기본 admin 계정)을 `scouter.conf` 4가지(빈 파일, 값 변경, `tagcnt_enabled=yes`·숫자 아님·빈 값·공백 값, 대문자 `TRUE`·음수)로 실행한다. conf는 쓸 때마다 증가하는 mtime을 주고 `Configure.reload(true)`로 적용한다(3초 thread와 같은 경로). 각 case의 `applied`는 그때 Configure field 값이다. `scouter.conf.<이름>`은 입력 conf 원문이다. 응답의 `time`(`System.currentTimeMillis`)은 호출 구간 안인지 확인한 뒤 0으로, host 이름인 `server_id`는 `<hostname>`으로 바꿔 다시 encode한다(`redactions`).
- `xlogbounds`: 원본 `XLogService`를 원본 `XLogCache`(XLogCore와 같은 `put(objHash, elapsed, error, bytes)`)와 `XLogRD` seam 위에서 실행한다. elapsed `-3,0,1,499,500,501` XLog 6개로 `xlog_realtime/pasttime_lower_bound_ms` × request `limit` 조합(0·1·499·500·501, 음수)의 TRANX_REAL_TIME_GROUP/TRANX_LOAD_TIME_GROUP, XLog 502개로 `req_search_xlog_max_count` -1·0·1·2·499·500·501의 XLOG_LOAD_BY_TXIDS/SEARCH_XLOG_LIST를 기록한다. 입력 XLog는 `xlogs-bounds.bin`, `xlogs-search.bin`(Pack 연속)이다. TXIDS/SEARCH case의 `items`는 txid 목록과 응답 SHA-256으로 대신하고, max ≥ 499의 request/response `.bin`(각 수십 KiB)은 남기지 않는다(`request_sha256`, `response_sha256`).

### XML 출력 기준

`oracle-manifest.json`의 `xml_output_basis`에 적었다.
- `counters.site.*.xml`: 원본 `CounterManager`가 JDK 8 내장 Xalan 변환과 JDK 8 내장 JAXB formatted marshal을 거쳐 쓴 파일이다. UTF-8, 4칸 들여쓰기이며 끝 newline은 정확히 1개다.
- `account*.default.xml`: classpath 기본값의 byte 사본이다. tab 들여쓰기이고 끝 newline이 없다.
- 계정 편집의 Xalan 결과 문자열: 원본이 버리므로 캡처하지 않았다.

### 결정성 보정(기록된 rewrite)

1. `LIST_CONFIGURE_SERVER` 응답: `SysJMX.getHostName()`과 같은 Text는 `<hostname>`, `Class@hex` identity hash는 `Class@<identity>`, `${...}` 치환 결과(아래 "원본 결함 후보" 1번)는 고정 placeholder로 바꾼 뒤 다시 encode한다. 바뀐 위치는 case의 `redactions`에 남는다.
2. `GET_ALERT_REAL_COUNTER_DESC`/`GET_PLUGIN_HELPER_DESC`: 원본 순서는 `Class.getDeclaredMethods()` 순서이며 **같은 JDK 8에서 실행마다 달랐다**(이 oracle 개발 중 `--check` 실패로 확인). item bytes는 그대로 두고 순서만 `methodName(parameterTypeNames)` 정렬로 바꾼다(`canonical_keys`).
3. `CounterHandler`의 `PerfCounterPack.time`은 `System.currentTimeMillis()`라 호출 구간 안에 있음을 확인(`time_in_call_window`)한 뒤 0으로 바꿔 다시 encode한다.
4. `SERVER_THREAD_LIST`와 thread가 있는 `SERVER_THREAD_DETAIL`은 JVM 상태에 따라 달라지므로 bytes 없이 key 순서와 value type만 기록한다(`facts.thread_*_shape`).
5. Telegraf `normalizeSeconds>0`과 delta 정규화는 `MeterCounter`(2초 bucket 31개)의 평균이다. 한 scenario는 수 초 안에 끝나므로 결과가 결정적이지만, 30초 넘게 걸리면 값이 바뀐다.

## 원본 결함 후보(oracle 실행으로 확인, 정상 계약과 분리)

1. **`${...}` 치환이 의도한 값 하나로 되지 않는다.** 원본 `ConfigValueUtil.replaceSysProp`의 overload 선택 결함이며, 치환 결과가 설정 조회 응답에 들어간다. 닫는 `}`가 없으면 그 뒤가 잘린다. Go의 동작은 management-commands.md §3.1을 따른다.
2. 계정 4종은 파일에 기록되지 않는다(`account_xml_unchanged`, `account_group_xml_unchanged`가 true). 그래서 런타임에 추가한 계정을 `EDIT_ACCOUNT`하면 파일에서 id를 찾지 못해 false다. `EDIT_GROUP_POLICY`는 true를 반환하고 파일을 다시 읽어 직전 `ADD_ACCOUNT_GROUP`까지 지운다. `id` 없는 `ADD_ACCOUNT`는 true인데 목록에 나타나지 않는다. 일부 field가 없는 계정이 있으면 원본 `LIST_ACCOUNT` 응답이 중간에 끝난다. Go는 없는 field를 빈 문자열로 정규화해 전체 목록을 응답한다(management-commands.md §9-12).
3. 새 설치(`counters.site.xml` 없음)에서 Telegraf 신규 type 등록은 항상 실패한다. `safelyAddFamily`가 빈 `<Types/>`로 `normalizeCountersXml`을 호출해 NPE가 나므로 pack이 나오지 않는다(`telegraf-fresh`). ObjectType이 하나 이상 있는 파일에서는 동작한다(`telegraf`).
4. `/register`(`RegisterHandler`)는 true를 반환하고 `REFETCH_COUNTER_XML`을 알리지만 파일과 engine은 바뀌지 않는다(`XmlUtil` 반환값 버림).
5. `SET_CONFIGURE_TELEGRAF`는 잘못된 XML도 저장하고 `"true"`를 반환한다(JAXB 예외는 reload 중에 삼켜진다).
6. 원본 `SERVER_DB_DELETE`는 `file` 인자의 경로 구성요소를 검증하지 않는다. Go는 검증한다(management-commands.md §9-4). `size`는 요청 수다.
7. `OBJECT_REMOVE` ListValue 원소는 `getLong(i).toInt`라 Text는 0, Float는 버림, 2^32 초과는 wrap된다. golden은 **응답 stream만**이다. 원본 writer는 remove Order에서 NPE가 나 디스크에 delete marker를 남기지 않는다(management-commands.md §6.3, §9-6, 정적 분석). 그래서 `ObjectWR` seam이 기록한 호출 목록(`objectwr_seam_remove_calls_*`)을 디스크 기대값으로 쓰면 안 된다.
8. Telegraf 신규 objType 등록(`registerNewObjType0`)은 그 줄의 counter만 가진 새 Family를 만든다. `safelyAddFamily`가 기존 `<Family>`를 이 Family로 통째로 바꾸므로, 같은 family의 다른 objType이 먼저 등록한 counter가 파일에서 사라진다. 이후 그 counter는 `addCounter` 경로로 하나씩 다시 등록되며, 그 사이 해당 줄은 pack을 내지 않는다(`telegraf-golden/warmup/cases.json` pass 1→2).

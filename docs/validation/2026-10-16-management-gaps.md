# 관리 명령·계정·설정·HTTP 입력 Java oracle 검증 기록 (1단계)

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

결론: 고정 원본 handler를 직접 실행하는 oracle(`scripts/java-management.py`, `tools/java-management/`)과 golden(`internal/service/testdata/management/`, 파일 336개)을 만들었다. 재생성 비교(`--check`)는 통과했다. 1단계 작성 시점에는 Go 구현과의 대조(2단계)를 실행하지 않았다(§1~§8은 1단계 작성 시점 서술이고, 이후 결과는 §9 이하에 있다). 예외로 Telegraf는 Go `golden_packs.json`과 같은 입력을 Java로 재생해 일회성으로 비교만 해 봤다(§7.1).

## 1. 사용법

```sh
export SCOUTER_SOURCE=/path/to/pinned/scouter   # dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5, clean
python3 scripts/java-management.py --check                       # golden 재생성 비교
python3 scripts/java-management.py --generate <빈 디렉터리>        # 새로 생성
python3 scripts/java-management.py --verify-xml <Go가 쓴 conf 복사본>  # 원본 parser 수용성
```

seam 목록, golden 형식, 결정성 보정 규칙은 [tools/java-management/README.md](../../tools/java-management/README.md)에 있다. 1단계 작성 시점에는 Makefile target이 없었고, 이후 `make management-oracle-check`(`python3 scripts/java-management.py --check`)로 추가했다.

## 2. 실행 환경

| 항목 | 값 |
|---|---|
| 원본 | Scouter 2.21.3 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`. script가 HEAD와 대상 파일·resource·deploy/lib의 clean 상태를 확인한다 |
| 실행 JDK | Zulu 8.64.0.19(1.8.0_345, macOS aarch64). `descriptors` scenario만 Zulu 18.0.2에서도 실행 |
| javac | common: JDK 18.0.2 `--release 8`. server·seam·oracle: JDK 8 javac |
| Scala | deploy `scala-compiler.jar` 2.10.4(JDK 8에서 실행) |
| 외부 jar | deploy/lib의 json-simple, servlet-api 3.1.0, httpclient 4.3.6, httpcore 4.3.3. javassist는 `~/.m2`의 3.29.2-GA(원본 pom 버전). 모든 jar의 SHA256을 manifest에 기록 |
| 컴파일 결과 | class 513개(common 포함) |
| JVM 환경 | env는 `SCOUTER_ORACLE_ENV_MARKER=synthetic` 하나뿐. 옵션은 `-Duser.timezone=Asia/Seoul -Dfile.encoding=UTF-8 -Duser.language=en -XX:ActiveProcessorCount=2` |

## 3. 실행 결과

| 명령 | 결과 |
|---|---|
| `python3 scripts/java-management.py --generate internal/service/testdata/management` | 성공. scenario 14개, 전체 약 50초(그중 telegraf-fresh 22초는 등록 실패를 기다리는 시간) |
| `python3 scripts/java-management.py --check` | `OK: regenerated 336 golden files identical` |
| `--verify-xml`(기본 account XML 2개 + 정규화된 counters.site.xml) | exit 0. 잘린 `account.xml`은 `PARSE FAILED`, exit 1 |
| leak check(home·user·hostname·tmp·`PATH=`) | 통과 |

첫 `--check`는 실패했다. `GET_ALERT_REAL_COUNTER_DESC` 응답 순서가 실행마다 달랐기 때문이다(`getDeclaredMethods()` 비결정). descriptor 순서를 정렬하도록 바꾼 뒤 통과했다. scenario를 추가할 때마다 generate와 check 2회의 결과가 동일했다. 더 많은 반복은 하지 않았다.

## 4. scenario와 case 수

handler case 127개(원본 `ServiceHandlingProxy`로 dispatch), HTTP 입력 step 31개, Telegraf golden 재생 step 8개, descriptor JDK 2종을 만들었다.

| scenario | handler case | 그 밖의 golden | 다루는 명령/경로 |
|---|---|---|---|
| account | 25 | `Account.toBytes` 4개(basic/unicode/empty/260 byte wrap), 기본 XML 2개, 원본 parser 결과 | ADD/EDIT_ACCOUNT, ADD_ACCOUNT_GROUP, EDIT_GROUP_POLICY 성공·실패·예외, LIST_ACCOUNT, LIST_ACCOUNT_GROUP, GET_GROUP_POLICY_ALL, CHECK_ACCOUNT_ID |
| configure | 16 | `scouter.conf.after-set`, `key-value-table.initial.json`(LIST_CONFIGURE_SERVER 전체 key/value/default), loadText/saveText | GET/LIST/SET_CONFIGURE_SERVER, CONFIGURE_DESC, CONFIGURE_VALUE_TYPE, CONFIGURE_VALUE_TYPE_DESC, GET/SET_CONFIGURE_TELEGRAF |
| counters | 15 | 저장 후 counters.site.xml 3개, 입력 XML, `CounterEngine.parse` 수용성 | GET_XML_COUNTER, GET/SET_CONFIGURE_COUNTERS_SITE(정상·invalid·`<Types/>` NPE·contents 없음·빈 root), DEFINE/EDIT_OBJECT_TYPE, CHECK_JOB으로 REFETCH 수신 |
| alert | 23 | plugin_dir 파일 해시 | GET_ALERT_SCRIPT_LOAD_MESSAGE ring cursor 12종, descriptor 25/22개, SAVE/GET_ALERT_SCRIPTING(_CONFIG)_CONTETNS(null contents/name, 하위 경로, 덮어쓰기) |
| manage | 34 | DB 트리 해시 | SERVER_DB_DELETE(day, 없는 경로, list 없음/빈 list, 비Text 항목, 검증되지 않은 경로 구성요소), REMOTE_CONTROL(정상, queue 5 초과, 무효 to/from), REMOTE_CONTROL_ALL, CHECK_JOB(있음/없음/초과 후 순서) |
| agentinfo | 8 | 메모리 상태, `ObjectWR` seam 호출 목록(디스크 기대값 아님) | OBJECT_TODAY_FULL_LIST, OBJECT_REMOVE_INACTIVE, OBJECT_REMOVE_IN_MEMORY, OBJECT_REMOVE(혼합 ListValue, 빈 list, MapPack/Text body) |
| threadlist | 4 | thread list/detail key 순서·type | SERVER_THREAD_DETAIL id 0/없음/미존재/Text |
| telegraf-fresh | 0 | step 2 | counters.site.xml이 없을 때 등록 NPE |
| telegraf | 1 | step 13, line key 5, pack 14개 | XML 설정(host mapping, prepend/append tag, `!` tag filter, NONE/DELTA/BOTH, normalize 30s), 비활성·무효 설정, timestamp 없음, `1.5i`, escape, host tag 없음 |
| telegraf-legacy | 0 | step 3, pack 4개 | scouter.conf `input_telegraf_$m$_*` 방식(`&`/`&&` delta, 짧은 mapping, host mapping 오류 항목) |
| counterhttp | 0 | step 8, CAFE record 11개 | `/counter` JSON(array, 단일, 비객체 원소, object 없음, 숫자 root, invalid, address 없음, unicode) |
| register | 1 | step 5 | `/register` JSON(신규, 기존 type, invalid, object 없음, total 비Boolean) |
| descriptors | 0 | JDK 8/18별 `descriptors.java{8,18}.json`, `summary.json` | `RealCounter`/`PluginHelper` descriptor. 두 JDK 모두 25/22개이며 원소 집합이 같다(`*_same_set: true`). 순서는 정렬해서 저장 |
| telegraf-golden | 0 | `input-steps.json`, `java_packs.json`, pack 18개, warm-up 등록 기록과 `counters.site.registered.xml` | Go `golden_packs.json` 8 step을 같은 입력·sample 설정으로 원본 `handlerRequest`에서 재생 |

## 5. 생성하지 못했거나 범위 밖인 항목

- **OBJECT_REMOVE 디스크 결과**: golden은 OBJECT_REMOVE/IN_MEMORY/INACTIVE의 **응답 stream만**이다. 원본 `ObjectWR` writer는 remove Order에서 NPE가 나(`Order.add` 기본 true, management-commands.md §6.3·§9-6) 디스크 delete marker를 남기지 않는다. 그래서 디스크 결과를 기대값으로 쓰지 않는다. oracle의 `ObjectWR`은 호출만 기록하는 seam이다. `facts.objectwr_seam_remove_calls_*`는 "`removeAgents(permanent=true)`가 이 objHash로 `ObjectWR.remove`를 불렀다"는 뜻일 뿐이다. 원본 writer NPE 자체는 이 oracle에서 실행하지 않았다(정적 분석 근거).

- **계정 XML 변환 문자열**: 원본 `XmlUtil.writeXmlFileWithIndent`는 문자열만 반환하고 handler가 그 값을 버린다. 원본을 수정하지 않고는 꺼낼 수 없어 bytes를 만들지 않았다. 대신 "디스크 파일이 그대로다(SHA 동일)"는 사실과 메모리 상태(LIST/GET 응답)를 golden으로 남겼다. Go가 파일을 실제로 쓰기로 결정하면 그 출력은 `--verify-xml`로 원본 parser 수용성을 확인한다.
- Agent relay가 필요한 분기(`objHash≠0`인 CONFIGURE_DESC/VALUE_TYPE, OBJECT_RESET_CACHE 등)는 `AgentCall` seam 범위 밖이라 실행하지 않았다.
- LDAP 인증, `LoginManager` 실제 인증·만료, `__local` 로그인은 seam이어서 다루지 않았다.
- `telegraf*` scenario는 `count()`를 고정 시각으로 호출한다. `handlerRequest` 본문 경로는 `telegraf-golden`에서만 실행했다. 1000줄 초과 조기 반환은 실행하지 않았다.
- `RegisterHandler`에서 같은 이름의 family가 이미 있을 때 `Math.random()` 이름을 만드는 분기는 비결정이라 제외했다.
- AlertRuleLoader의 javassist 컴파일과 5초 감시 루프(규칙 실행)는 실행하지 않았다(Go는 실행 미지원으로 결정됨).
- JDK 8 외의 JDK(Xalan/JAXB 출력 차이)와 실제 Jetty/TCP socket 상호운용은 검증하지 않았다.

## 6. 원본 결함 후보

oracle 실행으로 확인한 원본 결함 후보는 [README의 원본 결함 후보](../../tools/java-management/README.md#원본-결함-후보oracle-실행으로-확인-정상-계약과-분리)에 정리했다. 그중 분석 문서(`management-commands.md`)와 다른 점은 다음과 같다.

- `${X}` 치환: 문서는 "미정의 변수 → `STRING@hash`"라고 적었다. 실제 서버 경로에서는 **정의 여부와 관계없이** `${...}` 치환이 이름별 값으로 되지 않는다(원본 overload 선택 결함).
- ADD_ACCOUNT `id` 없음: 문서의 예측은 NPE(응답 없음)였지만 실제 응답은 `result=true`이고 목록에는 나타나지 않는다.
- EDIT_ACCOUNT: 런타임에 추가한 계정은 파일에 없으므로 false가 반환된다(`Cannot find account id`).
- descriptor 순서: 선언 순서가 아니고, 같은 JDK에서도 실행마다 다를 수 있다.

## 7. Telegraf: Go golden 입력의 Java 재생

### 7.1 일회성 비교 결과(Go 테스트로 고정하지 않음)

`telegraf-golden/java_packs.json`과 `internal/telegraf/testdata/golden_packs.json`(Go 기대값)을 비교했다. 기준: pack 필드와 값의 type, decimal 문자열, float/double IEEE bits. 저장소에 넣지 않은 일회성 script로 비교했다.

| step | 결과 |
|---|---|
| cpu total(0), cpu 정규화 평균(2000), nginx 2개, dropped lines | 순서까지 동일 |
| mem(2000) | 값은 같다. PerfCounterPack data의 순서만 다르다: Java `mem-total, mem-used_percent, mem-free`(MapValue Hashtable 순서), Go `used_percent, free, total` |
| redis 첫 sample(4000) | pack 집합은 같다. 두 줄의 pack 순서가 다르다(Java `uniqueLineString` HashMap 순회 순서) |
| redis delta(14000) | 값은 같다. pack 순서가 다르고(redis_keyspace가 먼저), redis perf data 순서도 다르다(Java `expired_keys_$delta`가 `expired_keys`보다 먼저) |

결론: 변환 값(objHash, objName, objType, address, peer `0.0.0.0` for IPv6, deadTime, decimal/float/double type과 bits, delta, normalize 평균)은 8 step 모두 Java와 같다. 차이는 순서뿐이다. ① 한 body 안 여러 줄의 pack 순서 ② PerfCounterPack data MapValue entry 순서(wire byte 순서). Java는 둘 다 HashMap/Hashtable 순서다. 이 차이를 의도된 차이로 둘지 맞출지는 결정이 필요하다.

### 7.2 Java 재생 방식의 전제

- Java는 type·counter가 counters.site.xml/engine에 등록된 뒤에만 pack을 낸다. Go golden은 registrar 없이 바로 낸다. 그래서 warm-up JVM이 먼저 등록을 마치고, 새 JVM이 등록된 파일로 재생한다. warm-up은 3 pass 만에 수렴했다. pass 2에서 cpu 줄이 다시 등록된 것은 README 결함 후보 8(Family 통째 교체)의 증거다.
- `PerfCounterPack.time`은 실제 시계 값을 확인한 뒤 Go 시계(`goldenStart+offsetMs`)로 바꾼다. normalize 평균은 실제 시계의 30초 창 안에서 계산된다. Go golden의 최대 offset이 22초여서 같은 결과지만, 30초를 넘는 step을 추가하면 Java 재생이 달라진다.
- `input-steps.json`은 Go golden의 입력만 담는다. Go 기대값이 바뀌어도 oracle `--check`는 영향받지 않는다. 입력이 바뀌면 `--check`가 실패하므로 재생성해야 한다.

## 8. 2단계 Go 테스트 제안(어떤 테스트가 어떤 golden을 읽을지)

| Go 테스트(제안 파일) | golden | 비교 방법 |
|---|---|---|
| `internal/service/account_commands_oracle_test.go` | `account/*.request.bin`, `*.response.bin`, `account-tobytes-*.bin`, `account*.default.xml` | 순서대로 dispatch해 response bytes를 비교한다. 단, 의도된 차이(파일 실제 기록, 그에 따른 EDIT 성공, null id·email 거절, 260 byte 거절)는 case 이름 allowlist로 분리하고 사유를 남긴다. 기본 XML bootstrap 바이트도 비교 |
| `internal/service/configure_commands_oracle_test.go` | `configure/*`, `key-value-table.initial.json` | CONFIGURE_DESC/VALUE_TYPE/VALUE_TYPE_DESC 바이트 일치. LIST_CONFIGURE_SERVER는 key 순서·type과 default 표를 비교하고 `redactions` 위치는 제외. `${}` 치환은 의도된 차이(env 노출 금지) |
| `internal/config` 테스트 | `configure/scouter.conf.after-set`, `facts.applied_after_set` | ISO-8859-1 로드, trim, 잘못된 숫자의 기본값, 연속 줄 처리 |
| `internal/counters`(counters.site) 테스트 | `counters/*.xml`, `counters/*.bin` | 정규화 출력, DEFINE/EDIT/SET 응답 bytes, Go 출력을 `--verify-xml`로 확인 |
| `internal/service/alert_*_test.go` | `alert/*` | load message ring cursor 12종 바이트 일치. descriptor는 정렬된 `canonical_keys`와 item bytes를 비교하며 Go 응답 순서를 확정한 뒤 적용. 저장 파일 경로·내용 |
| `internal/service/manage_commands_test.go`, `remote_control_test.go` | `manage/*` | REMOTE_CONTROL·CHECK_JOB·queue 5 초과(가장 오래된 항목 버림 + false) 바이트 일치. DB delete는 `size` 바이트를 비교하고 경로 구성요소 검증은 의도된 차이(거절) |
| `internal/service/object_mutations_test.go` | `agentinfo/*` | ListValue body, 0/Float/wrap 변환, `dead/active/inactive` tag, 응답 없음 case |
| `internal/telegraf/java_oracle_test.go`(신규, `oracle_test.go` 옆) | `../service/testdata/management/telegraf-golden/{input-steps.json,java_packs.json}` | `input-steps.json`이 `golden_packs.json` 입력과 같은지 먼저 확인한다. `runGolden` 결과를 step별 pack multiset(필드 + 값 type + decimal 문자열 + float/double bits)으로 Java와 비교한다. 순서 차이(§7.1)는 결정에 따라 별도 assertion 또는 known difference로 둔다 |
| `internal/telegraf` 추가 테스트 | `telegraf*/cases.json`, `*.pack.bin` | `InfluxSingleLine` 파싱 JSON, line key, 설정 경로(XML/legacy)별 pack. fresh 설치 NPE는 의도된 차이(등록 성공) |
| alert descriptor 테스트 | `descriptors/descriptors.java{8,18}.json` | Go descriptor 집합이 두 JDK 집합과 같은지 확인한다(순서는 Go 고정 순서) |
| `internal/httpapi` 테스트 | `counterhttp/*.cafe.bin`, `register/cases.json` | CAFE+Pack bytes(time 0 정규화), 로그 id. register no-op는 의도된 차이 여부를 결정 |

## 9. 2단계 대조 결과

모든 Go 대조 테스트는 새 `*_oracle_test.go` 파일이다. 커밋된 golden(Java 재컴파일 없음)만 읽고, golden이 없으면 skip하지 않고 FAIL한다. 의도된 차이는 case별 allowlist에 사유와 Go 쪽 assertion을 둔다. 모든 golden case가 분류되지 않으면 테스트가 실패한다.

| Go 테스트 | golden | 결과(HEAD a8322df) |
|---|---|---|
| `internal/service/manage_oracle_test.go` `TestManageJavaOracle` | `manage/*` 34 case | **PASS**. exact 27, 의도된 차이 7. negative check 1회(기대값을 exact로 바꾸면 실패) |
| 〃 `TestAgentInfoRemoveJavaOracle` | `agentinfo/*` 8 case | **PASS**. exact 1, MapValue 순서만 다름 4, 의도된 차이 3 |
| `internal/telegraf/java_oracle_test.go` `TestJavaOracleGoldenPacks` | `telegraf-golden/*` 8 step | **PASS**. pack multiset(필드·type·IEEE bits). 순서는 비교하지 않음(결정 사항) |
| 〃 `TestJavaOracleLineConversion` | `telegraf/`, `telegraf-legacy/` 16 step | **PASS**(Alive 수정 반영 뒤). LineKey, Resolve 결과, 줄별 pack multiset(MapValue entry 정렬) |
| `internal/httpapi/java_oracle_test.go` `TestJavaOracleCounterServlet` | `counterhttp/*` 8 step, CAFE 11 | **PASS**(Alive 수정 반영 뒤). CAFE+Pack bytes, peer |
| 〃 `TestJavaOracleRegisterServlet` | `register/*` 5 step | **PASS**. 결과 true/false ↔ 201/500. Go는 registrar에 실제 반영(의도된 차이) |
| `internal/service/account_golden_test.go`(기존 테스트) | `account/*` 25 case, toBytes 4, 기본 XML | **PASS**. 중복 작성하지 않음 |
| `internal/account/defaults_oracle_test.go` `TestDefaultFilesJavaOracleParse` | `account/*.default.xml`, `facts.parsed_default_*` | **PASS**. 원본 parser 결과와 계정(파일 순서)·그룹 policy 집합 일치(policy 순서는 Hashtable vs 파일 순서로 집합 비교, 결정 5) |
| `internal/server/configure_oracle_test.go` `TestConfigureJavaOracle` | `configure/*` 16 case | **PASS**. Go Runtime 전체를 Java 배치(conf/scouter.conf store, ConfDir, plugin, telegraf 경로)로 열어 Client TCP로 재생한다. exact 8, 의도된 차이 1, 구조 비교 7(LIST 3, GET 2, DESC, VALUE_TYPE). LIST 셀은 값 타입까지 비교한다. 통합 시 수정 d93d090(LIST Decimal/Boolean/Null, CONFIGURE_DESC 미문서 key 제외)으로 해당 차이가 사라졌다. 추가 확인: 저장된 scouter-telegraf.xml을 `telegraf.Load`로 읽은 Go Telegraf 설정의 ObjectDeadtimeMs·Enabled·Debug가 Java 적용값(45000/true/false)과 같다 |
| `internal/server/counters_oracle_test.go` `TestCountersSiteJavaOracle` | `counters/*` 15 case, 저장 파일 3 | **PASS**. exact 11(DEFINE/EDIT 응답, SET 결과, REFETCH_COUNTER_XML의 CHECK_JOB 2건 포함), 의도된 차이 4. counters.site.xml은 CounterEngine 의미(타입·family·counter 속성)로 비교 |
| `internal/service/alert_oracle_test.go` `TestAlertScriptingJavaOracle` | `alert/*` 23 case, plugin_dir | **PASS**. load-message ring 11 cursor exact, save/get exact 8, descriptor 2(byte 동일 item 집합), 의도된 차이 1(subdir), 소켓으로 재현 불가 1(no-body) |
| 〃 `TestAlertDescriptorsJavaOracle` | `descriptors/descriptors.java{8,18}.json` | **PASS**. Go 표 25/22개가 두 JDK 집합과 일치 |
| `internal/service/server_threads_oracle_test.go` `TestServerThreadsJavaOracle` | `threadlist/*` 4 case + shape 2 | **PASS**. id 0/없음/미존재/Text 응답 exact. SERVER_THREAD_LIST·DETAIL key 순서와 value type이 Java shape와 일치(goroutine 기반 값은 비교 대상 아님) |

**의도된 차이(allowlist, 테스트에 사유 기록)**

| 영역 | case | Java | Go | 근거 |
|---|---|---|---|---|
| manage | db-delete-non-text-item, db-delete-traversal | 경로 구성요소를 검증하지 않는다 | 거절, deleter 미호출 | §9-4 |
| manage | remote-control-invalid-to/from | enqueue 후 NPE, 응답 없음 | 아무것도 넣지 않고 `success=false` | §9-5 |
| manage | check-job-invalid-to, check-job-after-invalid-from, check-job-all-invalid-101 | 다른 session queue 또는 앞 단계 enqueue 결과 반환 | item 없음 | §9-13 소유 검사 |
| agentinfo | remove-permanent-mixed | Text→0, Float 절삭, long wrap | 거절 | strict 입력 검증 |
| agentinfo | remove-mappack-body, remove-text-body | 예외 삼킴, 연결 유지 | 거절, 연결 종료 | transport 오류 정책 |
| 공통 | MapValue entry 순서 | Hashtable 순서 | 삽입 순서 | `internal/protocol/value.go` |
| telegraf | 한 body의 pack 순서·PerfCounterPack data 순서 | HashMap/Hashtable 순서 | 줄·field 순서 | 프로젝트 결정, telegraf CONTRACT |
| register | 성공 시 효과 | 무반영(XmlUtil 결함) | registrar 반영 | 프로젝트 결정 |
| configure | Configure 내부 field(telegrafInputConfigMap*, telegrafOriginalConfig, property, allowIp*) | LIST/GET/VALUE_TYPE에 노출 | 노출 안 함 | §9-8 |
| configure | 시작 전용 key 값(db/log/plugin dir, listen ip/port, mgr_purge_enabled, net_http_*) | 파일 값 | running 값 overlay | `internal/server/metadata.go` |
| configure | reload 뒤 restart 전용 key(server_id, object_deadtime_ms, net_http_port, mgr_purge_keep_days와 파생 days) | 즉시 적용 | running 값 유지 | `internal/config` Key.Restart |
| configure | redact 셀(hostname, identity, `${}` env dump) | — | 비교 제외, Go 값에 env dump 없음만 확인 | oracle redaction |
| configure | Telegraf XML 전역(enabled, debug, delta normalize(seconds), objectDeadtimeMs) 이후 LIST 값 | XML 값으로 Configure field 덮어씀(`telegraf_object_deadtime_ms` 45000) | scouter.conf 값(35000). XML 값은 Telegraf 입력에만 적용 | 프로젝트 결정 |
| configure | set-configure-telegraf-invalid-xml | 저장 후 "true" | 사전 검증, "false" | `runtimeMetadataWith` |
| counters | counters.site.xml 바이트 | JAXB 재배열·기본 속성 추가 | 입력 순서·속성 보존(의미 비교) | counters CONTRACT |
| counters | get-xml-counter-after-define, get-counters-site-after-define | EDIT 결과 버림 | EDIT 반영 | §9-2 |
| counters | set-counters-site-types-empty | NPE, 응답 없음 | "true" | §9-3 |
| counters | set-counters-site-no-contents | NPE 삼킴, 연결 유지 | 거절, 연결 종료 | transport 오류 정책 |
| alert | save-alert-script-subdir | plugin_dir/sub/Elapsed.alert 생성 | 거절 | §9-9 |
| alert | null contents 저장 | 빈 ErrorCount.alert 생성 후 false | 파일 없이 false | Java 부작용 미재현 |
| alert | descriptor stream 순서 | getDeclaredMethods 순서(실행마다 다름) | 선언 순서, 집합 비교 | §5 |
| alert | load-message-no-body | 유한 입력의 EOF | 미전송(소켓에서는 양쪽 모두 body 대기) | 재현 불가 |

**재생 방법**
- session id: Java 101/102·5001/5002를 Go 로그인 session으로 바꿔 request와 기대 응답에 넣는다.
- configure: oracle이 중간에 scouter.conf를 직접 저장한 단계는 `config.Store.Reload(true)`로 재현했다(runtime의 Store.Watch 역할).
- counters: Java 기본 counters.xml은 golden의 GET_XML_COUNTER `default` blob에서 꺼내 Go engine에 넣었다.

**대조하지 않은 항목**
- OBJECT_REMOVE 디스크 결과(§5)
- servlet 수준 400/413/415(Java golden 없음)
- telegraf-fresh 등록 NPE(Go registrar 경로 별도)
- counters `--verify-xml`(이 단계에서는 Java를 재컴파일하지 않았다. counters CONTRACT에 2026-10-15 실행 기록 있음)

실행: `go test ./internal/service ./internal/server ./internal/account ./internal/telegraf ./internal/httpapi -run 'JavaOracle|Golden'`, race 1회, gofmt·vet 통과. 모두 PASS(HEAD a8322df).

## 10. 남은 위험

- golden은 JDK 8 + macOS aarch64 한 환경에서만 생성했다. HashMap/Hashtable 순서(MapValue, Telegraf tags)는 같은 JDK 계열을 전제로 한다.
- descriptor 이외에도 비결정 요소가 숨어 있을 수 있다. `--check`는 2회만 반복했다.
- Telegraf normalize 값은 scenario 실행 시간이 30초를 넘거나 golden offset이 30초를 넘으면 달라질 수 있다.
- `descriptors`와 `telegraf-golden`은 각각 JDK 18과 `internal/telegraf` 파일(`golden_packs.json`, `golden_test.go`의 `goldenStart`)에 의존한다.
- javassist는 deploy가 아닌 로컬 Maven cache에서 가져온다. 해당 jar가 없는 환경에서는 실패한다(skip하지 않음).

## 11. 통합 결과와 표준 gate (2026-10-16)

branch `codex/management-gaps`, scaffold `9424acc` 이후 64 commit, HEAD `8d5d6a1`. 범위·결정은 [management-commands](../compatibility/management-commands.md) §9·§10, [http-contract](../compatibility/http-contract.md), [webapp-api](../compatibility/webapp-api.md) §4.1·§5, 각 패키지 `CONTRACT.md`(`internal/{account,config,counters,httpapi,telegraf,webapp}`)에 있다.

| 영역 | 구현 | 근거 |
|---|---|---|
| 관리·변경 명령 45 | 전부 등록(`command_registry_test`: mutation 45/45, unsupported 0) | KV 6·계정 4·counters 3은 Java oracle 일치, 나머지 32는 loopback + manage/agentinfo golden 대조(§9) |
| JVM READ 5 | descriptor 정적 표(25/22, JDK 8/18 집합 일치), load message ring, goroutine thread 목록 | `alert_oracle_test`, `server_threads_oracle_test` |
| 계정/LDAP | `internal/account`(JDK 8 Xalan byte 일치, LDAP v3 stdlib), `--account-dir`, `__local` loopback | `account_golden_test` 25 case(17 byte 일치 + allowlist 8), `--verify-xml` |
| scouter.conf | `internal/config`(키 136, 3초 리로드, `--conf`, `${X}` A안) | configure golden 16/16(allowlist 1), Properties oracle 8종 |
| HTTP servlet/Telegraf | `internal/httpapi`·`internal/telegraf`, `--scouter-http`, MaxInFlight 64, counters.site.xml 등록 | JAXB oracle, Telegraf golden 8 step multiset, /counter·/register golden |
| webapp REST | `internal/webapp` 99 endpoint, `--webapp`, 인증 3종, 501 변환 | fake collector + 실제 runtime e2e; Java webapp JSON golden은 미대조 |
| 리뷰 | 독립 리뷰 2건(관리·storage·config·httpapi, 계정·webapp)과 재검토. 차단 항목 0, 나온 항목은 전부 반영 또는 기록 | 본 문서 §9, CONTRACT |

> 정정 주석: 위 표의 webapp "99 endpoint"는 합산 오류다. 등록 route는 controller A 38 + B 55(`bRoutes` 길이 테스트) = 93이다(원본 101 − 제외 TempTest 8, [webapp-api](../compatibility/webapp-api.md) §4.1). 표 본문은 당시 기록으로 보존한다.

표준 gate(통합 시 실행, 유휴 호스트, `SCOUTER_SOURCE` 지정, JDK 8 Zulu):

| 명령 | 결과 |
|---|---|
| `make check` (fmt-check/vet/test/build) | **PASS** exit 0. 21개 package ok(storage 220s, service 115s, migrate 123s) |
| `go test -race` 전체 package | **PASS**(server는 별도 실행 ok 7.5s, 나머지 21개 package ok). 1회차는 `alert_oracle_test.go`가 편집 중인 상태에서 실행되어 2건이 실패했고, 최종 파일로 재실행해 PASS |
| `make fuzz` (15 target × 10s, 신규 FuzzAccountXML/FuzzBER/FuzzParseProperties/FuzzLineProtocol 포함) | **PASS** exit 0 |
| `make fixtures-check` | **PASS** 141 Java fixtures |
| `make inventory-check` | **PASS** |
| `make storage-check` | **PASS**(`TestJavaStorageInteropObjectRemove` 포함) |
| `make management-oracle-check` | **PASS** `OK: regenerated 339 golden files identical` |
| `make build` | `dist/scouter-go` SHA256 `ffe199fd9f268547…`, `scouter-db` `a0dc81bbd7613e1c…`, `scouter-replay` `e43ea40950f7d7de…` |

미실행·남은 위험:
- 실제 Java Client GUI·실제 Java Agent·실제 Telegraf·실제 LDAP 서버·Java webapp JSON golden 대조는 미실행(실제 Agent 환경 검증 gate).
- JVM 스크립트(`.alert`/`*.plug`)·plugin JAR **실행**은 미지원(관리 기능만 지원).
- 의도된 차이 allowlist(§9, 22항목)와 원본 결함 비재현(계정 파일 미기록 cc9bda37, ObjectWR NPE, `${X}` env 유출, SafelyAddFamily counter 소실, counters.site.xml 부재 NPE, Client 빈 MapPack desync)은 운영 전환 시 Client 관측 결과가 Java와 달라질 수 있는 지점이다.
- `net_http_server_enabled`/Telegraf XML enabled는 Java처럼 0.0.0.0 포트를 열므로(기동 로그) 공유 호스트에서는 conf의 포트를 확인해야 한다. webapp은 원본처럼 기본 무인증이며 비 loopback에서는 기동 경고를 낸다.
- 32-bit 빌드의 LDAP BER 경로는 vet/빌드만 확인했다.

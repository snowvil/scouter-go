# internal/telegraf 계약

정본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `scouter.server/src/main/java/scouter/server/`
`support/telegraf/{TgConfig,TgmConfig,TgCounterMapping}.java`, `ScouterTgMtConfig.java`,
`http/model/{InfluxSingleLine,CounterProtocol,NumberValueWithTime}.java`, `http/handler/TelegrafInputHandler.java`,
`Configure.java`(`applyTelegrafInputConfig`, `applyTelegrafInputConfigNew`, `reloadTgConfig`),
`core/app/MeterCounter*.java`, `core/cache/CounterTimeCache.scala`, `scouter.common` `MeteringUtil`/`LinkedMap`/`CacheTable`/`StringUtil`/`IPUtil`/`HashUtil`.
분석 문서: `docs/compatibility/http-contract.md` §4–§6(D1–D11, Q1–Q5).

이 패키지는 네트워크 의존성이 없다. HTTP 마운트는 `internal/httpapi`가 하고, ingest 투입은 `Acceptor.AcceptFrom(ctx, pack, peerHost)`만 쓴다.

## 구성 요소

| Go | Java | 계약 |
|---|---|---|
| `ParseFile`, `File.Marshal` | JAXB `TgConfig` | 요소 부재는 Java 필드 기본값 유지, wrapper가 나올 때마다 list를 새로 시작(빈 wrapper `<objTypePrependTags/>`는 빈 list, 반복 wrapper는 마지막 것이 남음). 반복 scalar 요소는 마지막 값. scalar 변환은 JDK 8 JAXB RI와 같다: boolean은 앞 공백 뒤 `1…`/`true…`만 true(`TRUE`·`yes`·빈 값은 false), int는 공백·`+` 무시, `-`가 있으면 음수, 숫자 누적(int32 overflow), 그 밖의 문자는 0, enum은 정확 일치(공백·소문자는 null). root가 `TelegrafConfig`가 아니거나 well-formed가 아니면 error. `Marshal`은 JAXB formatted 출력(4칸, standalone header, 빈 String `<x></x>`, 빈 list `<x/>`, null 생략)과 같고 pinned sample을 byte 단위로 재현한다(`TestSampleRoundTrip`). 입력 상한 4 MiB. |
| `FromProperties` | `applyConfig` + `applyTelegrafInputConfig` | 전역 키는 trim 후 해석(`getBoolean`/`getInt`), measurement 키 값은 그대로. `input_telegraf_$<m>$_<postfix>` 13종 전부. counter mapping `{field}:{counter}:{display?}:{unit?}:{total?}:{norm?}`, `&`=DELTA, `&&`=BOTH, 기본 unit `""`·total true·normalize 0. |
| `Config.ApplyFile` | `applyTelegrafInputConfigNew` | XML 전역이 property 전역을 덮어씀, XML measurement가 같은 이름의 deprecated를 대체, XML에 없는 deprecated는 유지. `FromFile && Enabled`면 `ForcesHTTPServer()`(Java `net_http_server_enabled=true`). |
| `Compose`, `Load` | `reload` → `reloadTgConfig` | `Compose(props, xml, exists, now)`는 두 소스 전체로 매번 다시 합성한다(D13 해소, config store 연동용). `Load`는 경로를 열어 stat 크기와 `io.LimitReader`로 4 MiB(`MaxFileBytes`)를 넘지 않게 읽은 뒤 `Compose`를 부른다(초과는 err + fallback). |
| `Load`(세부) | 〃 | 파일 없음: deprecated 설정(`FromFile=false`). 파일 읽기/파싱/적용 실패: err와 함께 Java fallback(기본 `TgConfig` + deprecated, 즉 `Enabled=true`, `FromFile=true`). 호출자가 err를 로그로 남긴다. |
| `Measurement` | `ScouterTgMtConfig` | `X$` 접두사는 family·objName base에만 붙고 objType에는 없다(원본 문서의 `X$ORDER_SYSTEM_redis` 예와 코드가 다르며 코드를 따른다). 없는 tag는 문자열 `null`, host tag 없음은 `unknown`, prepend tag는 빈 값이면 생략, tag filter는 OR이며 `!v`는 tag 부재도 일치. `Valid()`는 family base가 `""`/`X$`가 아님. |
| `CounterProtocol` | `CounterProtocol` | `$tag$` 치환, delta 이름 `_$delta`, delta unit `unit + "/s"`, normalize `<4→0`, `>60→60`. fresh 15초 창: 생성 후 15초를 넘긴 **첫 검사까지** 깊은 비교 후 stale(Java `setFreshFalseIfIntervalExpired` 순서). deltaType null(알 수 없는 enum)은 NONE처럼 동작. |
| `ParseLine`, `LineKey` | `InfluxSingleLine.of` scan, `toLineStringKey` | byte 단위 상태기계(구분자가 모두 ASCII라 UTF-8 결과가 Java char 처리와 같다). 반복 key는 첫 위치·마지막 값. 따옴표 안 문자와 따옴표 안 `\`는 버린다. 한 줄 상한 `MaxLineBytes`(64 KiB, Go 추가). key가 16개를 넘으면 map index로 전환해 중복 검사가 O(n)이다. |
| `Resolve` | `of` 후반 + 생성자 | 검사 순서: 설정 없음 → disabled → invalid → tag filter → timestamp(`Long.parseLong/1e6`, 0 방향 절삭) → mapped field. mapped field가 빈 값·파싱 실패면 줄 전체 drop. `...i`는 `Long.parseLong`→Decimal, 끝이 숫자면 `Float.parseFloat`→Float(공백 trim, hex `p` 지수, 범위 초과 ±Inf; `_`는 거부), 그 외(bool·`u`·문자열)는 skip. |
| `ObjectPack` | `toObjectPack` | `{objHash=HashUtil.hash(objName), objName, objType, address=remoteAddr, alive=true, wakeup=0, tags={deadTime: Decimal(objectDeadtimeMs)}}`, version 없음. `alive=true`는 Java 필드 기본값(`ObjectPack.java:58`)이 wire에 그대로 실리는 것과 같게 맞췄다(Java oracle `TestJavaOracleLineConversion`이 대조). |
| `Converter.PerfCounterPack` | `toPerfCounterPack` | `time=receivedTime`(서버 수신 ms), `timetype=REALTIME(1)`, objHash 미설정(ingest가 objName으로 계산). 아래 delta/normalize 참조. |
| `Input` | `TelegrafInputHandler` | 줄 분리(`\n`,`\r`,`\r\n`), 1000줄 초과 payload 전체 drop(D7, `TG010` 로그, 응답은 호출자가 200), `LineKey` dedupe(마지막 줄 승), 등록 gating, `count()`. |

## delta·normalize 상태

- previous value: `(objHash, delta counter 이름)` → `(value, line timestamp ms)`, 수명 120초(put 때 갱신), 기본 상한 100,000(Java 1,000,000), FIFO eviction(`LinkedMap`/`CacheTable` LAST mode와 같이 기존 key 갱신은 위치 유지). eviction 수는 `ConverterStats.PrevEvicted`.
- rate: DECIMAL이면 long 차, 아니면 float 차; `(float)delta * 1000f / (float)(ts - prevTs)`. 같은 timestamp 재전송은 ±Inf/NaN(D5, 원본 그대로).
- meter: `MeterCounter`(`core/app/MeterCounter.java:32-60`) = `MeteringUtil(2000, 31)`, 2초 bucket 31개. `add(v)`는 현재 bucket `value += v; count++`. `getAvg(N)` = 현재 bucket부터 거슬러 `P = min(N, 30)`개(`MeteringUtil.check`)의 **Σvalue / Σcount**(sample 수 가중, 0건이면 0). bucket 위치는 `t = now_ms / 2000`, `pos = t % 31`이며 경과한 bucket 수(최대 31)만큼 앞 bucket을 비운다. 따라서 실효 창은 `2·P`초(normalizeSeconds=30 → 60초, Q1). **결정: 원본 호환(2N초)만 제공하고 명목 N초 옵션은 두지 않는다.** 상한 10,000(Java와 같음), FIFO eviction(`get`은 순서를 바꾸지 않으므로 계속 쓰이는 meter도 밀려나 평균이 초기화될 수 있다 — 원본과 같음).
- normal counter: `NONE && normalizeSec>0`만 meter 평균을 Double로, BOTH의 normal 값과 normalize 0은 원 값.
- delta counter: mapping normalizeSec>0이면 그 값, 아니면 `DeltaNormalizeDefault`일 때 default seconds로 meter 평균 후 Float.
- 모든 상태는 `Converter.mu` 하나로 보호한다. 요청 동시성은 HTTP 서버가 정한다.

## 등록(TypeRegistrar)과 ingest

- `TypeRegistrar`는 counters.xml/counters.site.xml 소유 패키지가 구현한다(`counters.Registrar`, runtime은 counters.site.xml store가 있으면 연결한다). 메서드: `ObjectType`, `HasFamily`, `AddFamilyAndObjectType`, `EditObjectType`, `SafelyAddFamily`, `SafelyAddObjectType`, `AddFamilyCounter`(Java `family.addCounter` + `safelyAddFamily`), `NotifyClients`(REFETCH_COUNTER_XML, non-blocking).
- registrar 있음: objType 미등록 → 등록 queue(1024, 포화 시 drop·`RegisterQueueFull`)에 넣고 **그 줄은 저장하지 않음**(Q3). 새/변경 counter → counter queue(1024)에 normal·delta counter를 넣고 그 줄은 저장하지 않음. 단일 goroutine이 등록: 새 type은 그 줄의 counter만 담은 family로 `SafelyAddFamily`(같은 이름 family 통째 교체, Q2) + `SafelyAddObjectType` 후 notify; counter 추가는 `prevAddedCounter`(10,000개, 5초) semantic equal이면 무시, 한 번에 비운 counter 묶음마다 notify 1회.
- registrar nil: 모든 resolved 줄을 ingest하고 objType마다 1회(최대 1,024종) 로그. counters.xml에 없는 counter는 Client가 표시하지 못할 수 있다(Java와 같은 결과가 아니다. runtime은 `ConfDir`이 없을 때만 nil이다).
- ingest: 줄마다 `ObjectPack` → `PerfCounterPack` 순서로 `AcceptFrom`. `peerHost`는 Java `IPUtil.toBytes` 의미(점 4개 IPv4만 통과, 그 외 `0.0.0.0`). AcceptFrom 오류는 `AcceptErrors`로 계수하고 다음 줄을 계속 처리한다(Java `NetDataProcessor` queue는 비동기라 응답에 반영되지 않음).
- **deadTime과 ingest inactive 판정**: ingest(`Engine` object 조회)는 ObjectPack tag `deadTime`(Decimal>0)을 기본 8초 대신 사용하므로 Telegraf object는 마지막 수신 후 `objectDeadtimeMs`(기본 35,000ms) 동안 alive다(Java `AgentManager`와 같은 의미). Telegraf flush 간격이 이 값보다 길면 object가 inactive로 보인다. 단순 heartbeat 경로에서 ingest는 tags를 최신 pack 값으로 교체하므로 설정 reload로 바뀐 deadTime도 다음 수신부터 반영된다.

## 의도된 차이·미검증

| 항목 | 원본 | Go |
|---|---|---|
| 처리 순서 | `HashMap` 순서(비결정) | dedupe 후 key 첫 등장 순서, field는 줄 순서. family master도 줄의 첫 mapped field |
| 등록 thread | registerQ 전부 → counterQ 전부 → notify, 항목 사이 100ms sleep | 두 queue를 `select`로 처리, sleep 없음, counter 묶음 notify 1회 |
| D4 빈 tag filter match | 요청 시 `charAt(0)` 예외 → 500, 나머지 줄 미처리 | 설정 적용 시 error(`ApplyFile`/`Load`), fallback 설정 사용 |
| D6 중복 tgFieldName/hostMapping, null 값 | `Collectors.toMap` 예외가 reload thread로 전파 | `ApplyFile` error, 수신기는 이전/ fallback 설정 유지 |
| D14 deprecated `counter_mappings` 토큰에 `:` 없음, field가 빈 문자열/`&` (`Configure.java:925-949`) | AIOOBE/SIOOBE → 설정 적용 전체 중단, Configure thread 종료 | 그 항목만 건너뛰고 warning, 나머지 적용 |
| D15 `input_telegraf_$` 시작·`$_` 없는 키 (`Configure.java:870-874`) | `substring(0,-1)` 예외 → 설정 적용 전체 중단 | 그 키만 무시하고 warning |
| D13 갱신 순서 (`Configure.java:602-619`, `:830-836`) | scouter.conf만 바뀌면 전역 telegraf 값이 conf 값으로 되돌아가고 XML·deprecated measurement 변경은 XML mtime이 바뀔 때까지 미반영 | `Compose`가 매번 두 소스 전체로 다시 합성(호출자가 어느 쪽이 바뀌든 호출). 결과는 "XML 전역 > conf 전역, XML measurement > deprecated measurement"로 항상 같다 |
| D12 quoted field 안 `\"` (`InfluxSingleLine.java:260-272`) | quote 안 `\`는 무시되어 `\"`가 quote를 닫고, 이어지는 공백에서 timestamp 구간으로 넘어가 줄 drop | **원본 그대로 재현**(파서 byte 동등, `TestParseLine`의 D12 사례). 결함이지만 같은 줄을 Java와 다르게 저장하지 않기 위해 고치지 않는다 |
| Q6 등록 queue 포화 (`TelegrafInputHandler.java:53-55`, `RequestQueue.java:89-98`) | 로그·계수 없이 drop, 항목마다 100ms sleep(초당 ~10건) | drop은 같지만 `RegisterQueueFull`/`CounterQueueFull`로 계수, sleep 없음 |
| remote address 옥텟 범위 초과 (`X-FORWARDED-FOR: 300.1.1.1`) | `getByAddress(null)` `UnknownHostException` → 500, 남은 줄 미처리(D9 연장) | peerHost `0.0.0.0`으로 계속 처리, ObjectPack.address는 원문 유지 |
| `$tag$` 개수보다 `*`가 많은 이름 | `IndexOutOfBounds` → 500 | `null` 치환 |
| 줄 길이 | 무제한 | 64 KiB 초과 줄은 `LongLines`로 계수 후 무시 |
| prev value 상한 | 1,000,000 | 기본 100,000(`Limits.PrevValues`) |
| debug 로그 | 무제한 | 초당 20건, 초과는 `SuppressedDebugLogs` |
| `Float.parseFloat`/`Long.parseLong` 세부 | Unicode 숫자 등 허용 | ASCII 범위만 허용(미검증) |
| `HashUtil.hash` charset | JVM 기본 charset | UTF-8 가정 |
| 설정 reload | 3초 polling | config `Store`가 3초마다 scouter.conf·XML을 확인하고, telegraf 관련 변경이면 `Runtime.ReloadTelegraf`가 처음부터 다시 합성해 `Input.SetConfig`로 교체(config CONTRACT §5.1) |

## 테스트와 fixture

- `file_test.go`: sample byte round-trip, 기본값/빈 wrapper, 잘못된 root, escape.
- `oracle_test.go` `TestJAXBOracle`: `JAVA8_HOME`(JAXB 내장 JDK 8)과 `SCOUTER_SOURCE`(pinned commit)가 있으면 pinned `TgConfig`로 `testdata/jaxb/*.xml`·sample·잘못된 root를 unmarshal해 Go `ParseFile` 결과와 필드 단위로 비교한다. 환경이 없으면 skip(통과 아님).
- `config_test.go`: sample 매핑 table(cpu/mem/redis/redis_keyspace/mysql, prefix tag, filter, D1–D3), deprecated properties, `ApplyFile` 오류(D4/D6), `Load` fallback, fresh 창.
- `line_test.go`: scan table(escape·따옴표·`i`·bool·timestamp·여러 줄), `LineKey`, `splitLines`, number parsing, `FuzzLineProtocol`.
- `convert_test.go`: delta(정수/실수/0 경과/만료), 기본 normalize 창, NONE normalize, BOTH, float 산술, 상한 eviction, ObjectPack.
- `input_test.go`: 등록 gating 흐름(fake registrar), 1000줄 규칙, dedupe, 통계, queue 포화와 `Close` 잔여.
- `testdata/golden_packs.json`: sample 설정·registrar 없음·고정 시계로 요청 body를 순서대로 적용한 입력과 기대 pack(JSON, float는 `strconv 'g'` 문자열). `go test ./internal/telegraf -run TestGoldenPacks -update`로 재생성한다. `java_oracle_test.go` `TestJavaOracleGoldenPacks`가 같은 입력의 Java replay(telegraf-golden)와 요청별 pack multiset으로 대조한다(pack·MapValue 순서는 의도된 차이로 비교하지 않음).

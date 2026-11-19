# internal/ingest 부수 경로 계약

고정 원본 Scouter 2.21.3(commit `dee47fb`)의 수집 경로 부수 기능 중 이 package가 구현한 것과 원본과의 차이를 기록한다. Java oracle 대조는 `tools/java-ingest`(golden `testdata/java-oracle`, 재생 `java_oracle_test.go`).

## 1. Kubernetes pod 순번 (`kube.go`)

원본: `kube/PodSeqManager.java`, `core/AgentManager.scala` `active`(82-99행)·`inactive`(205-217행).

- ObjectPack의 kube 필드(`podName`, `hostName`, `podSeq`, `initialTime`, `allowProceed`, `pushSeq`)는 모두 `transient`다. `addKubeProp`/`copyKubePropFrom`은 encode되는 필드(tags 포함)를 바꾸지 않으므로 저장 bytes와 Client 응답은 kube 처리와 무관하다. Go는 이 값을 별도 구조(`kubeResult`, `kubeObject`)로 둔다.
- `active`: `useKubeSeq`(BooleanValue만 true)와 비어 있지 않은 `podName`(TextValue만)일 때 manager의 `applyPodSeq` → push 순서다. `allowProceed=false`이면 객체를 등록하지 않고 끝낸다(`Stats.KubeHeld`). 이 판단은 MaxObjects 검사보다 앞이다(Java에는 객체 수 상한이 없다).
- `kubeSeq`는 `MapValue.getInt`: 없거나 Number가 아니면 0(초기화가 아니라 seq 0 복구), Decimal은 `(int) long`, Float/Double은 Java 축소 변환(-1.5 → -1). `hostName`이 null이거나 빈 문자열이면 kube 속성만 붙이고 진행한다.
- 15초 규칙: `initialTime > now - 15000`이면 보류(push 포함), 정확히 15000ms 이후에는 push 없이 진행하고 현재 manager에서 그 host의 seq를 지우며 initial 기록은 유지한다. initial 기록은 전역(hostName 키)이라 다른 podName의 기록을 복사할 수 있고(`copyKubePropFrom`), 이후 inactive 해제는 복사된 podName manager에서 일어난다.
- `getNewPodSeq`: host의 이전 seq를 재사용하고, 없으면 0..9999에서 처음 비어 있는 seq, 모두 차면 -1(그대로 push).
- push: `OBJECT_SET_KUBE_SEQ`, MapPack `{"seq": DecimalValue(podSeq)}`, 대상 objHash는 ObjectPack의 objHash(0이면 objName hash). Java는 AgentManager thread에서 동기 호출한다. Go는 engine lock을 잡은 수집 경로에서 기다리지 않도록 bounded queue에 넣고 `RunAgentCalls` worker가 호출한다(§4).
- inactive: Java AgentManager daemon(1초)이 dead time을 넘긴 alive 객체를 한 번 inactive로 바꾸며 저장 객체의 podName/hostName으로 `objectInactivated`를 부른다. Go는 `Tick`(runtime 1초)의 객체 판정(§6)에서 같은 전이 때 kube 속성이 있으면 수행한다. 저장 객체의 kube 속성은 새 객체·저장(save) 갱신 때만 바뀌고 변경 없는 heartbeat는 alive만 되살린다(Java `objPack.wakeup()`과 같음).
- `RemoveObjects`/`RemoveInactiveObjects`(ObjectMap.remove)는 inactive를 부르지 않으므로 seq를 해제하지 않는다(원본 동작 유지).
- 락: Java는 manager별 lock + ConcurrentHashMap, Go는 모든 상태를 engine lock 아래에서 바꾼다.

### 상한(원본에는 없음)

| 상태 | Go 상한 | 포화 동작 |
|---|---|---|
| manager(podName) | `MaxObjects` | key를 늘리는 새 podName의 ObjectPack을 `ErrLimit`로 거절(`KubeLimited`). 빈 manager는 즉시 지우고, key를 늘리지 않는 분기는 manager를 만들지 않는다(빈 manager와 없는 manager는 동작이 같다) |
| hostSeq+seqHost key 합계 | `MaxCounters` | 실제로 택할 분기가 늘리는 key 수(복구 1~2, 새 seq 2, 이전 seq 재사용 0~1)만 미리 확인해 `ErrLimit`(`KubeLimited`). key를 늘리지 않는 분기 — seq가 확정된 heartbeat(`seqHost[kubeSeq]==host`, 원본 84-86행), 15초 보류 중인 재초기화, 15초 뒤 진행 — 는 포화 중에도 거절하지 않는다(거절하면 확정 객체가 INACTIVE/ACTIVATED를 반복한다). 0..9999가 모두 찬 새 seq 탐색도 2개로 보수적으로 센다. 0..9999 탐색 자체가 manager당 seq를 10000으로 묶지만 복구 경로는 임의 seq를 넣을 수 있다 |
| initial 기록(전역) | 10000, 삽입 순서 FIFO | 가장 오래된 기록을 버림(`KubeInitialEvicted`). 그 host는 다음 init에서 다시 15초 보류될 뿐이다 |
| 바이트 | `MaxCacheBytes`(object와 kube 자신의 bytes만, §12) | 택할 분기의 증가분(새 key·새 initial 기록·새 manager, initial FIFO 축출분은 차감하지 않음)을 미리 `coreDelta`로 확인, 실패 시 `ErrLimit`(`KubeLimited`). 증가분 0인 분기는 거절하지 않는다. 다른 cache(지연 bucket·gxid·counter·status 등)가 예산을 채워도 거절하지 않는다(2026-10-24) |
| 객체별 kube 속성 | live 객체 수 | 객체 제거와 함께 삭제 |

포화 거절은 모든 변경 전에 일어나며 상태를 바꾸지 않는다. 단, kube 처리 후 MaxObjects로 객체 등록이 거절되면 kube 상태 변경(복구된 seq 등)은 남는다(Agent의 다음 heartbeat가 같은 결과를 확인한다).

## 2. CPU 초과 자동 thread dump (`objcpu.go`)

원본: `core/app/ObjectCpuChecker.scala`, 호출 위치 `core/PerfCountCore.scala` 60행(REALTIME 저장·cache 갱신 뒤).

- REALTIME(timetype 1) counter만, `AgentManager.getAgent(objName)`(이름 table: 마지막 put이 이김, 제거된 객체 이름은 다른 객체를 가리켜도 지운다, objType이 바뀐 갱신은 옛 이름을 지운다), CounterEngine family `javaee`(`Config.ObjectType`), tag `ADC`(BooleanValue), `ADC_D`/`ADC_TH`(`getInt`), `ProcCpu`(`containsKey` 후 `getFloat`; Text·NaN은 0/비교 거짓).
- 비교: `threshold(int→float) < cpu`(같으면 clear), streak 시작 시각이 `time - duration*2`보다 이르면 clear 후 종료(`duration*2`는 Scala Int로 wrap), `first < time - duration`이면 clear 후 dump. 음수 duration은 첫 초과 pack에서 dump한다.
- 상태: Java는 objName마다 `ObjectCpuStatus`를 영원히 보관한다. `firstOccurenceTime <= 0`(새 상태 -1, clear 0)은 모든 분기에서 같게 동작하므로 Go는 진행 중인 streak(>0)만 보관하고 clear와 객체 제거 때 지운다. 상한 `MaxObjects`(이름이 live 객체를 가리키는 한 도달 불가, 도달 시 streak를 기억하지 않고 `CPUStreakLimited`). `overCount`는 원본에서 읽지 않으므로 두지 않는다.
- dump: `TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`, MapPack `{"objHash": DecimalValue(agent.objHash), "TRIGGER_DUMP_REASON": "TRIGGER_DUMP_REASON_TYPE_CPU_EXCEEDED"}`. Java `RequestQueue(100).put`은 가득 차면 새 항목을 버린다(반환값 무시) → Go queue 100, 초과는 `Dropped`. daemon이 1건을 처리 중이면 Java·Go 모두 101건까지 받는다(oracle `saturate`).

## 3. GeoIP (`geoip.go`)

원본: `geoip/GeoIpUtil.scala`, 호출 `core/XLogCore.scala` `calc`(WEB_SERVICE·APP_SERVICE만, `XLogGroupUtil.process` 다음·`XLogGroupPerf.add` 앞, group이 없어도 실행). 이후 plugin hook(Pulse)·XLog 저장이 이어진다.

- 소비자 인터페이스 `GeoIPLookup.Lookup(ip [4]byte) (geoip.Location, bool, error)`(`*geoip.DB`가 구현). `found=false, err=nil`이 Java null Location이다. `Config.GeoIP == nil`은 `GISDataUtil.isOk() == false`(DB 없음)로 아무것도 하지 않는다. `geoip_enabled`는 live 설정(`Settings.GeoIP`, §7)으로 pack마다 읽는다. runtime은 기동 때 `geoip_data_city_file`(기본 `./conf/GeoLiteCity.dat`)을 열고, 없으면 `S145` 로그 후 GeoIP를 끈다(성공 `S146`).
- reload(`server.Runtime.ReloadGeoIP(path)` → `Engine.SetGeoIP`): `geoip_data_city_file` live 값이 바뀌면 runtime settings owner goroutine이 호출한다(Java ConfObserver는 reload lock 안에서 로드). Java처럼 값이 바뀐 경우에만 다시 열고, 파일이 없거나 열기에 실패하면 이전 DB를 유지한다(경로 값은 실패해도 기억). **의도된 차이**: 새 DB가 열리면 IP→Location cache를 비운다(Java는 이전 파일 결과를 계속 씀). citySet은 중복 text 기록 억제용이라 유지한다.
- **의도된 차이(lookup 오류)**: Java는 `getLocation`의 RuntimeException이 `XLogCore` loop까지 올라가 그 XLog 전체(group perf, plugin, cache, 저장)가 빠진다. Go는 XLog를 그대로 처리하고 GeoIP 보강만 건너뛴다. `Stats.GeoIPErrors`로 세고 오류 로그는 10초에 한 줄(Java `Logger.println(id, 10, …)` 간격)이며 IP는 로그에 남기지 않는다. 오류는 캐시하지 않는다.
- 건너뜀: ipaddr 길이 ≠ 4, DB 없음, countryCode가 비어 있지 않음, 사설 IP, ipInt == 0. lookup이 null(found=false)이면 아무것도 바꾸지 않고 캐시하지 않는다(다음 pack이 다시 조회).
- 사설 IP: 첫 octet은 `& 0xff`, 두 번째는 Java signed byte 비교. 172.16-31만 사설이고 172.128-255(음수)는 공인, `ip(1) == 168`은 byte에서 성립할 수 없어 **192.168.x.x는 사설로 보지 않고 조회한다**(원본 결함 후보, 의도적으로 재현).
- city가 빈 값(Java null 포함)이면 "unknown". country 문자열은 reader 값 그대로(index 0은 reader가 "--"로 준다). `p.city = HashUtil.hash(city)`로, Go는 UTF-8 byte의 CRC32다(원본 JVM `file.encoding=UTF-8`과 같음, 비ASCII city는 다른 기본 charset의 원본과 다를 수 있다).
- cache: `locationCache` IntKeyLinkedMap·`citySet` IntLinkedSet, 각각 setMax(10000). 없는 key만 끝에 넣고, get은 순서를 바꾸지 않으며, 넘치면 맨 앞(가장 먼저 넣은 것)을 버린다. 새 city(citySet에 없음)는 `TextPermWR.add("city", hash, city)` → Go `Store.PutText(date, city)`(영구 text).
- 차이: Java는 캐시에 넣은 뒤 text를 queue에 넣는다(`TextPermWR` queue 실패는 XLog와 무관). Go는 text를 먼저 queue에 넣고(검증 통과) 그 뒤 두 캐시에 넣는다. **city text 쓰기 거절**(검증 오류, 닫힌 store)은 lookup 오류와 같은 정책이다: XLog는 실패시키지 않고 **countryCode/city를 채우지 않은 채** 처리·저장하며(거절된 text의 city hash를 가진 XLog를 남기지 않음) 두 캐시에도 넣지 않아 다음 miss가 다시 쓴다. `Stats.GeoIPErrors`(전체)와 `Stats.GeoIPTextErrors`(그중 text 쓰기 실패)로 세고, 로그는 lookup 오류와 같은 10초 간격 한 줄이다. text queue가 가득 차 lock 밖에서 기다린 끝에 버려지면(§11) 이전과 같이 오류가 아니다(storage가 `TextDropped`로 센다). lock 밖 제출이 닫힌 store로 실패하면 XLog는 이미 보강되었으므로 `GeoIPErrors`·`GeoIPTextErrors`만 세고 로그는 남기지 않는다(종료 중에만 생긴다).
- 차이: Java는 XLogPack의 모든 필드를 항상 쓴다. Go codec은 받은 append group(TailVersion)을 보존하므로 TailVersion 0인 구형 pack에 GeoIP 값을 넣을 때 TailVersion을 1로 올린다(countryCode/city가 group 1에 있음).

## 4. Agent 호출 queue (`agent_calls.go`)

| queue | 크기 | 원본 |
|---|---|---|
| kube-seq (`OBJECT_SET_KUBE_SEQ`) | 1024 | Java는 동기 호출(queue 없음) |
| cpu-dump (`TRIGGER_THREAD_DUMPS_FROM_CONDITIONS`) | 100 | `RequestQueue(100)` |

- `offer`는 막지 않는다. 가득 차면 `Dropped`, worker 종료 후에는 `Abandoned`.
- `RunAgentCalls(ctx)`가 queue마다 worker 1개를 띄우고 ctx 종료 또는 `Engine.Close`에서 끝난다. 둘 다 진행 중 호출의 ctx도 취소한다. ctx 종료와 queue 수신이 동시에 준비되어 select가 수신을 고르더라도 ctx가 이미 끝났으면 그 호출은 실행하지 않고 `Abandoned`로 센다(취소된 ctx로 호출해 `Failed`로 세지 않음). 호출 1건 timeout은 60초(Java `net_tcp_agent_so_timeout_ms` 기본값).
- 집계: `Enqueued = Done + Failed + Abandoned + Pending`(진행 중 1건 제외). `Stats`에는 log line(`ingest=%+v`) 파서가 scalar만 받도록 `KubeSeq*`/`CPUDump*` 평면 필드로 두고 `Stats.KubeSeqCalls()`/`CPUDumpCalls()`로 묶어 읽는다. `Config.AgentCall == nil`이면 `Failed`.
- 응답(MapPack 또는 없음)은 Java처럼 버린다. runtime의 agent pool은 TCP 세션이 없는 Agent에 `(nil, nil)`을 돌려주므로 Go에서는 "세션 없음"(Java 로그 S501)도 `Done`으로 집계된다.

## 5. ServerStat 대응

`server.Runtime.ServerStat()`가 이름순 snapshot을, `server.FormatServerStat(entries, key, prefix)`가 `ServerStat.toString`(`prefix + name=value\n`, key 부분 문자열 filter) 형식을 제공한다. Java는 core thread가 마지막으로 꺼낼 때의 크기를, Go는 호출 시점의 길이를 보고한다. Java에서 한 번도 꺼내지 않은 queue는 key가 없지만 Go는 존재하는 queue를 항상 보고한다.

| Java 이름 | Go |
|---|---|
| `xlog.core0.queue`(XLogPreCore), `xlog.core.queue`, `xlog.core-r.queue`, `profile.core0.queue`, `profile.core.queue`, `profile.core-r.queue`, `text.core.queue`, `alert.core.queue`, `visitor.core.queue`, `span.core.queue` | 대응 queue 없음: UDP worker가 engine lock 아래에서 Core 처리(메모리 갱신)를 하고 저장은 writer queue로 넘긴다(§13). 앞단 대기열은 transport UDP datagram queue(`UDPConfig.QueueCapacity`)다. 길이·용량은 `UDPStats.QueueLen/QueueCap`으로 노출되며 콘솔 `debug queue`의 `udp.queue`/`udp.queue_capacity`와 stats 줄에 보인다(`Runtime.ServerStat()` 항목에는 넣지 않는다) |
| `xlog.db.queue`, `text.db.queue`, `profile.db.queue`, `summary.db.queue`, `zipkin-span.db.queue` | 같은 이름의 queue 없음: XLog·profile·text는 storage writer queue(storage CONTRACT §8, `/health`의 `xlogQueue`·`profileQueue`), summary·span을 포함한 나머지 kind는 ingest writer queue(§13) |
| (없음) | `agentcall.kube-seq.queue`, `agentcall.cpu-dump.queue` |
| (없음, Java는 kind마다 core queue + db queue 2단) | `ingest.writer.<kind>.queue`(§13의 kind 10개, 대기 record 수). 콘솔 `debug queue`에는 `ingest.writer.<kind>.{enqueued,written,write_errors,dropped,abandoned}`도 나온다 |
| (없음, Pulse plugin 내부) | `pulse.xlog.queue`, `pulse.profile.queue`, `pulse.kafka.queue`(Pulse 활성 시) |

## 6. AgentManager 객체 생명주기와 alert (`objects.go`)

원본: `core/AgentManager.scala`(`active` 82-147, `alertTooManyChange`/`alertInactiveObject`/`alertReactiveObject` 151-182, daemon 46-72, `inactive` 205-217), `core/ObjectMap.scala`, `CounterManager.addObjectTypeIfNotExist`. Java oracle `alerts` scenario(원본 AgentManager·ObjectMap·daemon을 JDK 8에서 실행)와 `TestJavaOracleAgentManagerAlerts`로 대조한다.

- `CounterManager.addObjectTypeIfNotExist`는 active의 첫 단계다. 엔진 lock 밖(`AcceptFrom`)에서 `Config.AddObjectTypeIfNotExist(objType, detected)`를 부른다(알려진 type이거나 `detected`가 TextValue가 아니면 호출하지 않음). runtime은 `counters.Store.DefineObjectType`로 `{name=objType, disp=objType, family=detected의 family, icon=detected, sub-object=detected의 값}`을 counters.site.xml에 append한다. ConfDir이 없으면 기록할 곳이 없어 1회 로그 후 건너뛴다(의도된 차이).
- alive 상태(`objState`)는 이 프로세스에서 active로 등록한 객체만 가진다. 기동 때 오늘 object 파일에서 읽은 객체는 OBJECT_LIST에는 보이지만 Java objMap에는 없으므로(`AgentManager.read`는 호출되지 않음) alert가 없고 첫 heartbeat는 새 등록이다: 주소·type·version·날짜가 같아도 Java `objPack == null` 분기처럼 `ObjectWR.add`(Go object writer의 `PutObject` — 같은 objHash의 기존 위치 갱신 — 와 object text `PutText`, §13)를 하고 받은 tags·wakeup으로 바꾼 뒤 objMap에 넣는다(`updated` 0). 이후 heartbeat부터 변경 없는 heartbeat는 저장하지 않는다.
- 1초 판정(`Tick`): objMap 순서(`IntKeyMap`: 용량 101, load factor 0.75f, mixed hash, 2n+1 rehash, 위 bucket부터 열거 — `intKeyOrder`)로 alive 객체를 돌며 `now > wakeup + adjustDeadTime`이면 한 번 inactive로 바꾼다. `adjustDeadTime`: `deadTime` tag(`getInt`, Decimal/Float/Double, 음수 포함)가 0이 아니면 그 값, 아니면 type이 `zipkin`이거나 `z$`로 시작하면 `object_zipkin_deadtime_ms`, 그 밖에는 `object_deadtime_ms`. `batch`는 제외. 같은 규칙을 `Objects()` alive와 `RemoveInactiveObjects`도 쓴다.
- `INACTIVE_OBJECT`: CounterEngine에 있고 sub-object가 아닌 type만. `ACTIVATED_OBJECT`: 등록된 객체가 inactive 상태에서 heartbeat를 받고 type이 primary(같은 조건)일 때, 갱신 전 저장 pack 기준. **Go는 그 heartbeat가 object byte 예산(`coreDelta`, §12)을 통과한 뒤에** 만든다(Java에는 거절이 없어 순서 차이가 관찰되지 않음). 거절된 heartbeat는 alert를 만들지 않고 객체를 inactive로 둔다 — 포화 중 heartbeat마다 ACTIVATED_OBJECT가 반복 저장되지 않는다. 같은 heartbeat의 `MAYBE_OBJ_DUP`도 통과 뒤 ACTIVATED_OBJECT 다음에 넣는다(Java 순서). 둘 다 level=`object_inactive_alert_level`의 byte 축소, objType `scouter`, AlertCore 경로(summary·realtime·저장 queue·tag count). cache 예산이 차도 alert는 저장하고 summary·ring만 건너뛴다(`Stats.AlertCacheSkipped`, §12). encode 실패나 tag count 쓰기 실패는 heartbeat를 실패시키지 않고 `Stats.ObjectAlertErrors`로 센다. 저장 자체는 alert writer가 하고 실패는 그 writer의 `WriteErrors`로 센다(§13).
- `MAYBE_OBJ_DUP`: 등록된 객체가 save(주소·type·version·날짜 변경)될 때 `updated`를 올리고(날짜가 바뀌면 0부터) 20의 배수면 만든다. objHash 0, level 0, **AlertCache.put만**(저장·summary·tag count 없음) — Go realtime ring만, cache 예산이 차면 넣지 않고 `Stats.AlertCacheSkipped`로 센다.
- message는 `objName + " is not running. "`/`" is running now. "`/`"Maybe duplicated objNames!! Please check "` + `ObjectPack.toString`: `"OBJECT "` + `" objType="`(공백 2개), Hexa32 objHash, objName, 있으면 `addr=`·version, Java alive flag일 때 `alive`, `java.sql.Timestamp(wakeup)`(서버 timezone, nanos 끝 0 제거), tags `MapValue.toString`(Hashtable 열거 순서: 용량 11·0.75·head 삽입·2n+1 rehash, 값 toString — Decimal/Text/Boolean/Null/TextHash(16진)/Blob/IP4/List/배열/Summary, Float는 `Float.toString`, Double은 `DecimalFormat("#0.0#################")`, JDK 8 기호 NaN U+FFFD·∞). JDK 8 `Float.toString`이 최단 표현보다 한 자리 더 찍는 드문 값은 다를 수 있다.
- kube: inactive 전이 때 kube 속성이 있으면 `objectInactivated`(§1).

## 7. live 설정 (`settings.go`, F0)

`Engine.SetSettings(Settings)`(원자 pointer 저장)으로 runtime이 `config.Live`를 넘긴다. 각 값은 Java가 Configure field를 읽는 지점에서 읽는다.

| 설정 | 읽는 지점 |
|---|---|
| `object_deadtime_ms`, `object_zipkin_deadtime_ms` | 1초 판정·`Objects()`·`RemoveInactiveObjects` |
| `object_inactive_alert_level` | alert 생성 시 |
| `tagcnt_enabled` | XLog 저장(`XLogCore`: `XLogTagCount.add`)과 alert(`AlertCore`: `AlertTagCount.add`)마다 |
| `_auto_5m_sampling` | 5분 tick(`Sample5M`): false면 쓰지도 비우지도 않는다(누적 계속) |
| `xlog_sampling_matcher_gxid_keep_memory_count` | `New` 한 번(`Config.SamplingGxidKeepCount`, XLogDelayingCache 생성자) |
| `..._xlog_keep_memory_count`, `..._profile_keep_memory_count` | 새 bucket을 만들 때(bucket이 상한을 가진다) |
| `..._xlog_keep_memory_millis` | 지연 XLog마다(`now / millis` 단위가 바뀌면 회전) |
| `..._profile_keep_memory_secs` | profile bucket 회전마다(`secs*1000/500`, Java int 연산. 0 이하 결과는 10) |
| `geoip_enabled` | XLog GeoIP 단계마다 |

0 이하 sampling 값은 기본값으로 바꾼다(config와 `SetSettings` 양쪽). Java는 무제한 map 또는 예외 경로다(원본 취약 동작). 메모리는 계속 `MaxCacheBytes`로 묶인다.

## 8. 이 package에 없는 관련 원본 동작

- `PlugInManager.active`(JVM plugin, 제외 범위), `AgentManager.primaryObjCount`(조회 소비자 없음), `ObjectWR.add`의 매 save 재기록은 storage 경로가 맡는다.
- Java objMap은 ObjectMap의 type별 table도 갖지만 Go 조회는 `e.objects`를 직접 걸러 쓴다.

## 9. 배선 hook(`Config`)

runtime(`internal/server/wiring.go`)이 SqlTables·Logger를 연결하는 지점이다. ingest는 logger·sqltable을 import하지 않는다. 모두 nil이면 아무것도 하지 않는다.

| hook | Java 위치 | 호출 시점·제약 |
|---|---|---|
| `OnSQLText(date, hash, text)` | `TextCore`: `SqlTables.add`가 `TextWR.add`보다 먼저 | xtype `sql` TextPack마다 engine lock 안, `PutText` 전. 막지 않아야 한다(`sqltable.Tables.Add`) |
| `PackLog(p, processed)` | `NetDataProcessor.process`: `log_udp_packet`(dispatch 전), `log_udp_<type>`(Core.add 뒤) | `AcceptFrom`마다 detach한 pack으로 처리 전 1회·처리 뒤 1회(결과와 무관, Java도 enqueue 뒤 출력). 처리 뒤 호출은 engine lock 안이고 engine 소유 pack이므로 보관·engine 호출 금지 |
| `ObjectTypeLog(objType, known, detected, ok)` | `CounterManager.addObjectTypeIfNotExist`의 `log_udp_some_object` 줄 | ObjectPack마다 engine lock 밖 |
| `ProfileTestLog(case, cnt, total, p)` | `ProfilePreCore` `log_test_rate` 분기(Case1/2/3, processDelaying) | engine lock 안. 지연 자식은 해제 시각 stamp 전(수신 시각)의 pack. 반환값 true면 processDelaying `cnt` 증가 |

- `Engine.ObjectNameCached(hash)`: `TextCache.get("object", objHash)` 대응. live 객체의 이름을 engine lock 없이 읽는 cache(`sync.Map`, 등록·기동 적재 때 저장, 객체 제거 때 삭제, 크기는 live 객체 수). transport S150/S160 줄이 쓴다.

## 10. Client 범위 조회 streaming

- `XLogsHistory(ctx, q, visit)`: Java `XLogService.getHistoryPerfGroup(V2)`처럼 레코드를 읽는 대로
  `visit`(Client write)에 넘긴다. 객체 filter(빈 목록은 전체)와 V1 `elapsed > limit`은
  `storage.XLogScan.Match`로 scan 안에서 적용한다. V1은 `max>0`이면 match `max`건에서 멈춘다.
  V2는 `txid` 뒤부터 센 match가 `pageCount+1`건이면 멈추고 마지막 1건은 보내지 않고
  `hasMore=true`만 세운다(Java `readByLimitCount`의 `counted > limitCount`). 결과를 모으지 않으므로
  총 건수 상한이 없다.
- `ScanXLogsByTime`(SEARCH_XLOG_LIST), `ScanSummaries`, `ScanAlertHistory`: storage scan을 그대로
  넘긴다. `visit`은 storage gate 밖에서 실행되므로 Backend를 다시 불러도 된다(Text 조회 등).
- slice를 반환하는 `XLogsByTime`(console), `Summaries`, `AlertHistory`, `StatusHistory`는 storage
  수집 결과 상한(`MaxQueryRecords`/`MaxQueryBytes`)을 따른다.

## 11. engine lock 밖에서 하는 작업

engine lock(`Engine.mu`)은 모든 UDP pack 수집을 직렬화한다. 다른 기능(Client 조회, 주기 저장)이 그 lock을 잡은 채 decode나 디스크 쓰기를 하면 수집 전체가 멈추므로, 상태 snapshot만 lock 안에서 하고 나머지는 lock을 놓은 뒤 같은 goroutine에서 한다.

| 경로 | lock 안 | lock 밖 |
|---|---|---|
| `XLogsRealtime`(Client realtime XLog) | cursor(Loop/Index)·인자 검증, 방문 순서대로 ring slot의 `[]byte` 참조 복사 | `DecodePack`, 객체·elapsed filter |
| `Sample5M`(5분) | live 설정 확인, PerfCounterPack(timetype 3) 생성, samples·예산 비움, `pruneCounterNames` | daily writer queue에 한 item으로 넣기(쓰기는 writer, §13) |
| `Tick`의 alert summary(5분 경계) | SummaryPack 생성, summary 비움·예산 반환, `nextSummary` 갱신 | summary writer queue에 한 item으로 넣기 |
| `Tick`의 group 저장(매분) | 300초 합계 계산, 0인 meter의 empty 표시, row 생성, `nextSave` 갱신 | daily writer queue에 한 item으로 넣기(meter 순서, cnt·tm·err) |

- ring slot은 제자리에서 바뀌지 않는다: `storeXLog`가 매번 새 `EncodePack` 결과를 넣고 slot을 교체만 한다. 따라서 lock을 놓은 뒤 교체되어도 조회는 가져간 bytes를 그대로 decode한다. 결과(record·순서·Loop/Index·오류)는 이전과 같다(`TestXLogsRealtimeMatchesLockedQuery`가 이전 구현과 대조). 교체된 slot의 옛 bytes는 조회가 끝날 때까지 살아 있으므로 동시 조회 수 × ring 크기만큼 `MaxCacheBytes` 밖 일시 메모리가 생긴다(이전에도 decode 결과는 예산 밖이었다).
- 쓰기 순서: summary는 summary writer, group row와 5분 sample은 daily writer가 각자 FIFO로 쓴다(§13). Tick·Sample5M은 engine 상태 작업의 오류만 돌려주고, 쓰기 오류는 writer가 kind별로 세고 10초에 한 줄 로그한다.
- **의도된 차이(쓰기 실패)**: Java `Auto5MSampling`·`AlertSummary.doFlush`는 map을 새 것으로 바꾼 뒤 queue(`DailyCounterWR`/`SummaryCore`)에 넣으므로 실패해도 그 회차를 다시 쓰지 않는다. Go도 상태를 먼저 비우고 회차 전체를 writer queue의 한 item으로 넣는다. 첫 쓰기 오류에서 그 회차를 끝내고 쓰지 못한 나머지(실패한 것 포함)를 `Stats.Sample5MDropped`/`Stats.AlertSummaryDropped`와 writer의 `WriteErrors`로 센다. queue가 가득 차 회차를 넣지 못하면 회차 전체를 같은 counter로 센다(writer의 `Dropped`도). lock을 놓은 사이 들어온 새 sample·alert와 섞여 중복·유실이 생기지 않게 하려는 것이다.
- group 저장은 같은 분 slot을 덮어쓰므로 이전처럼 재시도한다: daily writer의 쓰기 실패(또는 queue 포화로 넣지 못함) 시 lock을 다시 잡아 `nextSave`를 이전 값으로 되돌리고(그 사이 다음 저장이 `nextSave`를 옮겼으면 그대로 둔다), 실패한 meter부터 이 저장이 붙인 empty 표시를 지운다. 그 사이 `processGroup`이 이미 지운 0 meter는 재시도에서 빠진다(합계 0인 행이 써지지 않을 수 있음, 이전에도 실패 지점까지 표시된 meter는 같았다). 이전에는 같은 tick의 summary 쓰기 실패가 group 저장을 미뤘지만 이제 둘은 다른 writer라 서로 영향이 없다(원본도 `SummaryWR`와 `DailyCounterWR`는 다른 thread).
- visitor roll(`VisitorRollContext`)은 lock 안에서 visitor writer queue에 넣고, tag tick(`TagTickContext`)은 engine lock을 놓은 뒤 부른다(§13, storage CONTRACT §8.4).
- Close: `closed` 검사는 lock 안이므로 Close 전에 시작한 회차가 Close 뒤 enqueue를 시도할 수 있다. writer가 이미 닫혔으면 그 회차는 `Dropped`로 센다.

### lock 안에 남은 I/O

| 위치 | 작업 | 남긴 이유 |
|---|---|---|
| `AlertsRealtime`, `InteractionCounters`, `Status`, `Objects("")` | lock 안 decode/clone(alert ring 1024칸, 객체 수) | 크기가 작아 이번에는 옮기지 않음 |

이전에 이 표에 있던 INACTIVE_OBJECT 저장, visitor roll, `PutVisitorContext`(HLL load/save)와 counter·object·status·alert·summary·stack·batch·span 저장, permanent `RemoveObjects`의 `RemoveObject`는 §13의 writer로 옮겼다. tag count의 flush·rebuild 표시(`TagTickContext`, Store gate)는 `Tick`이 engine lock을 놓은 뒤 한다.

`RealtimeServiceGroup`은 lock 안에서 group 합계만 만들고, group 이름 조회(`store.TextContext`)는 lock 밖에서 한다.

이번 범위 밖으로 둔 것: 위 표 첫 행(`AlertsRealtime` 등 lock 안 decode/clone)은 디스크 I/O나 대기가 아니라 상한이 있는 메모리 작업(alert ring 1024칸, live object 수)이다. 옮기려면 ring slot 소유권 계약(`XLogsRealtime`과 같은 방식)을 새로 세워야 해서 효과에 비해 변경이 크다. 대량 Agent 접속에서 측정으로 문제가 확인되면 따로 한다.

### storage queue 대기를 lock 밖으로 (2026-10-22, `storewrite.go`)

수집 경로의 storage queue 쓰기는 text(`TextPack`, object 이름, `processGroup` seed·group 이름, GeoIP city), XLog, profile, tag count(XLog·alert)다. 이전에는 engine lock을 쥔 채 `PutText`/`EnqueueXLog*`/`EnqueueProfile`/`Put*TagsContext`를 불렀다. 그래서 queue가 가득 차면(포화 전) 최대 100ms(text·XLog·profile) 또는 `tagInboxWait`(100ms)를 lock 안에서 기다렸고, 그동안 다른 worker의 수집(counter·object 등 그 queue와 무관한 pack 포함)이 모두 멈췄다(`TestIngestFullStorageQueueWaitsOutsideEngineLock`·`TestFullTagInboxWaitsOutsideEngineLock`: 수정 전 다른 worker의 counter 수집이 약 80~100ms 걸림).

- **lock 안**: storage `Prepare*`(`PrepareText`, `PrepareXLogEncoded`, `PrepareProfile`, `PrepareXLogTags`, `PrepareAlertTags`)로 검증하고, `Store.Offer`로 기다리지 않고 넣는다. Offer는 queue의 leaf lock만 잡는다. 결과는 셋이다.
  - 들어감 또는 같은 key 중복(text): 이전과 같다.
  - queue가 포화(saturated) 상태: 바로 버리고 센다. 이전에도 포화 중에는 기다리지 않았다.
  - 가득 찼지만 포화 전(이전이면 기다렸을 경우): queue에는 넣지 않고 `deferredWrite`로 engine의 `held`에 남긴다(`Stats.StoreWritesDeferred`). text는 이때 storage pending에 남겨 둔다(queue bytes 없이, storage CONTRACT §8.5). 그래서 Submit 전에도 다른 worker의 group 조회와 Client text 조회가 그 text를 본다(이전에 lock 안에서 기다리던 text가 보이던 것과 같다. `TestIngestDeferredTextVisibleToOtherCollectors`: queue가 bytes로 가득 찬 상태에서 수정 전에는 다른 worker XLog의 group이 0으로 저장됨).
- **같은 queue 순서**: engine은 hold마다 deferred가 생긴 queue를 bit로 기억한다(`heldQueues`). 그 queue의 뒤 쓰기는 Offer하지 않고 뒤에 붙인다. 판정은 O(1)이다(처음 구현은 쓰기마다 `held`를 처음부터 훑어, XLog queue가 가득 찬 채 child가 많이 풀리는 hold에서 O(n²)였다. `TestHeldWritesBoundedAndLinear`: 한 hold에 쓰기 60,000건, 이전 방식은 lock 안 약 1.0초(측정 1회, 500ms 상한 실패), 현재는 상한 안에서 통과).
- **hold당 상한**: `heldMaxWrites`(1024건)·`heldMaxBytes`(4 MiB, `storage.Write.Size` 추정, 첫 held 쓰기는 크기와 무관하게 받는다). 상한에 닿으면 그때까지의 held 쓰기를 **lock 안에서** 순서대로 Submit하고(쓰기마다 최대 100ms, 이전 lock 안 대기와 같음) held와 queue bit를 비운 뒤 Offer를 이어 간다(`Stats.StoreWritesHeldInline`). 그 hold에서만 lock 안 대기가 되돌아오고, 순서와 손실은 이전과 같다. 처음 구현은 상한을 넘는 쓰기를 바로 버려, XLog queue가 잠깐만 찼다가 writer가 곧 비우는 경우에도 손실이 생겼다(`TestIngestManyReleasedXLogsWithBriefFullQueue`: child 1300건, queue 64, writer 20ms 정지 → 수정 전 212건 버림, 이제 0건). 4 MiB에 가까운 profile 하나도 바로 버려졌다(`TestIngestLargeProfileWithFullQueueWaits`). lock 안 Submit의 오류(닫힌 store, ctx)는 그 hold의 결과로 남겨 lock을 놓은 뒤 돌려준다(`heldErr`). driving XLog나 bucket 회전이 풀어 준 child는 이미 cache 예산(`MaxCacheBytes`)에서 빠졌으므로, 이 상한이 예산 밖에 남는 양을 묶는다(hold당 최대 4 MiB + 첫 쓰기 하나). 동시에 Submit 중일 수 있는 hold는 engine에 pack을 넣는 goroutine마다 하나다: UDP worker, HTTP counter API(`httpapi`의 `AcceptFrom`), Telegraf 입력(`telegraf`의 `AcceptFrom`) 요청 goroutine, `Tick`.
- **lock 밖**: lock을 쥔 goroutine(수집 worker 또는 `Tick`)이 lock을 놓은 직후 `held`의 쓰기를 넣은 순서대로 `Store.Submit`한다. Submit은 이전 동기 호출과 같은 대기·버림·계수다. 각 쓰기가 최대 100ms를 기다리고, 시간이 초과되면 queue에 포화가 표시되어 같은 queue의 뒤 쓰기는 바로 버린다(포화 해제는 queue 절반). 빈자리가 매번 생기면 한 hold에서 여러 번 기다릴 수 있다(이전 lock 안 대기와 같은 성격). ctx는 이전 호출과 같다(XLog·tag는 수집 ctx, text·profile은 `context.Background()`).
- **검증 오류는 이전처럼 lock 안에서 동기로** 돌려준다(`ErrInvalid`/`ErrLimit`/날짜 오류). 그래서 object 이름을 저장할 수 없는 object를 상태 변경 전에 거절하는 것(§13), group text 오류의 XLog 거절, GeoIP city text 거절 시 보강 생략은 그대로다. Offer의 `ErrClosed`도 lock 안에서 돌려준다.
- **Submit 오류**: 대기 중 queue 포화로 버린 경우는 오류가 아니다(이전과 같음). Submit이 오류를 내는 경우는 닫힌 store(`ErrClosed`)와 ctx 취소뿐이며 종료 중에만 생긴다. 수집 pack이면 그 pack의 결과(`Rejected`/`StorageErrors`)가 된다(`TestIngestCloseDuringDeferredSubmit`). 이전과 달리 메모리 갱신(ring·계수 등)은 이미 끝난 뒤다. object alert(INACTIVE/ACTIVATED_OBJECT)의 tag는 `ObjectAlertErrors`, GeoIP city text는 `GeoIPErrors`·`GeoIPTextErrors`로 센다(그 pack을 실패시키지 않던 이전 정책과 같다). 첫 오류 뒤의 쓰기도 Submit한다(같은 원인으로 바로 실패한다).
- **`Completed`/`Rejected`**: lock 밖 Submit이 끝난 뒤 센다. 의미(받아서 queue에 넘김 또는 queue 포화로 버림 = 완료)는 같다. `PackLog(p, true)`는 이전처럼 lock 안, 처리 직후다.
- **Flush·Close**(`TestFlushAndCloseWaitForDeferredWrites`): hold가 넘겨받은 쓰기 묶음은 Submit이 끝날 때까지 in-flight로 센다(`heldFlights`, id는 hold 순서). `Engine.Flush`는 자기 lock 이전 hold의 묶음이 모두 Submit될 때까지 기다린 뒤(ctx 상한, runtime은 sync·purge 전 최대 5초) writer barrier를 한다. 그래서 sync·purge 전에 deferred text·XLog·profile·tag가 queue에 들어가 있거나 버려져 있다. `Engine.Close`는 `closed` 표시 전의 묶음을 최대 5초(`writerCloseDeadline`) 기다린다. runtime이 그 뒤 store를 닫으므로 Submit 중 store가 닫히는 일은 Close 상한을 넘긴 경우뿐이다.
- **panic**: 수집이나 `Tick` 처리 중 panic이면 `held`의 쓰기를 그 queue의 drop으로 버리고(`Store.Drop`, `StoreWritesHeldDropped`, 보류된 text는 pending에서 버린 text cache로) lock을 푼다. in-flight로 등록하지 않으므로 이후 Flush가 기다리지 않고, 다음 lock holder가 남의 쓰기를 넣지 않는다(`TestPanicDiscardsHeldWrites`, `TestTickPanicDiscardsHeldWrites`: 수정 전 Tick panic 뒤 묶음이 in-flight로 남아 Close가 5초 기다림).

#### 순서 계약

| 대상 | 결과 | 근거 |
|---|---|---|
| 같은 hold 안 같은 queue의 쓰기 | 유지. 한 queue에 deferred가 생기면 그 hold의 그 queue 뒤 쓰기는 Offer하지 않고 모두 deferred로 이어 붙여 같은 순서로 Submit한다(hold당 상한에서는 그때까지의 것을 lock 안에서 Submit한 뒤 이어 감) | `offerLocked`·`heldQueues`. 예: driving XLog가 풀어 주는 child XLog들 → driving XLog 순서가 XLog 파일에서 그대로다(`TestIngestDeferredXLogsKeepTheirOrder`: 여유 있을 때와 가득 찼을 때 gxid 조회 순서 동일) |
| 같은 worker(같은 datagram의 여러 pack 포함)의 연속 pack | 유지. worker는 앞 pack의 Submit을 마친 뒤 다음 pack의 lock을 잡는다 | UDP worker는 handler를 순서대로 부른다 |
| 서로 다른 queue(text와 XLog, XLog와 tag 등) | 한 queue만 deferred이면 다른 queue의 뒤 쓰기가 먼저 들어갈 수 있다 | queue마다 writer가 따로라 원래 서로 순서가 없다. deferred text는 pending으로 보이므로 조회 결과는 달라지지 않는다 |
| 서로 다른 worker | queue에 들어가는 순서가 engine lock 순서와 다를 수 있다(가득 찬 queue에서만) | UDP datagram은 공유 queue에서 아무 worker나 가져가므로(`transport/udp.go`) worker 사이의 lock 순서는 원래 도착 순서를 보장하지 않는다 |
| realtime ring(XLogsRealtime)·Pulse `OnXLog`/`OnProfile` | 바뀌지 않음. ring 갱신과 콜백은 lock 안, Offer 직전·직후 그대로다 | ring·콜백 순서는 engine lock 순서. 저장 queue 순서와의 일치는 같은 worker 안에서만 보장(위 행) |
| text 첫 값 | 바뀌지 않음. 보류된 text가 pending을 차지하므로 같은 key의 뒤 text는 중복으로 끝난다 | 이전에 lock 안에서 기다리던 text와 같다 |
| tag count First 100 | 도착(inbox 진입) 순서로 정해진다. deferred 된 XLog의 tag는 다른 worker의 뒤 XLog보다 늦게 들어갈 수 있다(포화 때만) | 원본 `XLogTagCount`도 queue 진입 순서 |

- **기록만 하는 차이**: lock 안 tag Offer는 이전 `Put*TagsContext`처럼 ctx 취소를 먼저 확인하지 않는다(취소는 Submit의 대기나 다음 단계에서 드러난다). queue가 가득 찼을 때 어느 worker의 쓰기가 들어가고 어느 쪽이 버려지는지(Submit끼리의 빈자리 경쟁)는 이전의 "lock 순서대로 한 번에 하나"와 다르다. 버려지는 총량과 상한은 같다. 자정 경계에서 `lockForAccept`가 정한 날짜와 처리 시각(`now`)의 날짜가 다르면(EndTime 0인 XLog) 미리 찾은 service text 답은 쓰지 않고 그 날짜로 다시 묻는다.
- **purge 순서**: XLog·profile record의 purge sequence는 lock 안 Prepare 때 찍는다(`newPackItem`). 보류된 text는 purge·삭제가 pending에서 지우면 Submit이 버린다. tag 기여는 Prepare 때 tag purge 세대를 찍고, 그 뒤 자기 경로가 purge되었으면 inbox에 넣을 때 버린다(storage CONTRACT §8.5). 그래서 Prepare 뒤 Submit 전에 일어난 purge·삭제가 지운 디렉터리를 다시 만들지 않는다. 이전에 lock 안에서 기다리다 들어간 text·tag는 이 보장이 없었다.

#### 버퍼 소유권

lock 밖으로 넘기는 것은 Prepare가 만든 storage item뿐이다. XLog는 ring에 넣은 것과 같은 `EncodePack` 결과 bytes(새로 할당, ring과 읽기 전용 공유, 이전과 같음), profile은 pack의 `Profile` bytes(이전에도 `EnqueueProfileContext`가 소유권을 가져감, engine은 이후 쓰지 않음), text는 `TextPack` 값 복사(string), tag는 lock 안에서 만든 기여 slice(새 할당)다. 재사용 buffer나 engine 상태를 빌린 slice는 넘기지 않는다. `held`는 engine lock 아래에서만 읽고 쓰며, 넘길 때 통째로 떼어 내 lock holder goroutine만 쓴다.

### service text 미리 읽기 (2026-10-22, `storewrite.go`)

`processGroup`(XLogGroupUtil)은 service→group memo에 없는 service의 text를 읽어 group 이름을 정하고, 그 group hash를 그 XLog의 저장 값에 넣는다. storage cache(pending·읽기·버린 text·"없음" cache, storage CONTRACT §8.2)가 답하지 못하면 Store gate 아래 파일을 읽는다. 원본 Java는 XLogCore 전용 thread에서 동기로 읽어 그 thread만 멈추지만, Go는 engine lock을 쥔 채 읽어 gate가 오래 잡히면(writer hold, 느린 디스크) 모든 수집이 멈췄다(`TestIngestColdServiceTextReadOutsideEngineLock`: 수정 전 gate를 잡은 동안 다른 worker의 counter 수집이 1초 timeout). 기동 직후나 대량 Agent 접속 때처럼 cache가 차기 전에 몰린다.

- **수집 XLog**(`lockForAccept`): lock을 잡은 뒤, 아무것도 바꾸기 전에 그 XLog의 group 단계가 service text를 찾을지 본다(`serviceLookupLocked`: service≠0, type 0/1, group 0, memo 없음). 찾을 거면 답을 여기서 구해 그 hold의 group 단계에 넘긴다(`groupHint`).
  - `Store.TextCached`(gate 없이)가 답하면 그 답을 쓴다.
  - 모르면 lock을 놓고 `Store.TextWithin`으로 파일 읽기를 맡기고 최대 `serviceTextWait`(50ms) 기다린 뒤(답은 읽기 cache 또는 "없음" cache에 남는다) lock을 다시 잡는다. 다시 잡은 뒤 `TextCached`에 한 번 더 묻는다. 그 사이 queue에 들어온 text가 있으면 그 답을 쓰고(`TestServiceTextArrivingDuringReadAhead`), 없으면 읽은 답(또는 읽기 오류)을 쓴다. 재시도는 없다.
  - 상태를 바꾸기 전에 lock을 놓으므로 다시 잡아도 처리 결과는 처음 잡은 것과 같다. `now`는 다시 잡은 때의 값이다(이전에도 lock을 잡은 때의 값).
  - 판정은 실제 group 단계보다 넓다(지연되거나 버려질 XLog도 읽는다): 결과에 영향 없는 읽기 한 번이다.
- **읽기 오류**: 미리 읽기가 오류면 group 단계가 그 오류로 XLog를 거절한다. lock 안에서 읽던 이전과 같은 결과이고, 파일은 한 번만 읽는다(`TestIngestServiceTextReadErrorRejectsAsBefore`). 오류는 cache하지 않으므로 오류가 이어지면 그 service의 memo 없는 XLog마다 lock 밖에서 한 번씩 읽는다.
- **ctx**: 미리 읽기는 수집 ctx를 따른다. 이전의 lock 안 읽기는 `context.Background()`라 gate를 끝까지 기다렸다. 이제 취소된 수집은 gate를 기다리지 않고 그 pack을 거절한다(`TestIngestServiceTextReadAheadCanceled`, 종료 때만 생긴다).
- **지연된 child XLog**: 판정이 저장 여부를 묻지 않으므로 XLog sampling으로 지연될 child도 자기 accept 때 위처럼 미리 읽힌다. driving XLog가 풀어 줄 때나 bucket 회전으로 복구될 때(다른 hold)의 group 단계는 `TextCached`로 답을 얻는다(`TestIngestDelayedServiceTextReadAhead`: gate를 잡은 상태에서 driving XLog가 child를 풀어도 기다리지 않고, child의 group이 맞게 저장됨). 지연 동안(최대 `xlog_sampling_matcher_xlog_keep_memory_millis`의 2배) storage cache에서 밀려났거나 purge·삭제로 지워졌으면 lock 안에서 파일을 읽지 않는다. 읽기만 맡기고(`TextWithin`, 대기 0) 그 XLog는 group 없이 저장하며 `Stats.ServiceTextSkipped`로 센다(`TestEvictedServiceTextNotReadUnderLock`). engine lock 안에 남은 파일 읽기는 없다.
- **기다림의 상한(2026-11-13)**: 이전에는 lock 밖에서 gate를 끝까지 기다렸다. gate hold가 길어지면(page cache가 비워진 직후의 디스크 읽기, 느린 디스크) UDP worker가 모두 그 뒤에 서서 수신 queue가 차고 datagram이 거절될 수 있었다. 이제 `serviceTextWait` 안에 답이 없으면 그 XLog는 group 없이(0) 저장하고 `Stats.ServiceTextSkipped`로 센다. 읽기는 Store의 fetcher가 이어서 하므로 그 service의 다음 XLog는 cache에서 group을 얻는다. 한 번 시간이 넘으면 1초 동안은 기다리지 않는다(storage CONTRACT §8.2 `TextWithin`). XLog 자체와 profile, Pulse 전송은 그대로이고 빠지는 것은 그 XLog의 group 값과 group 합계 한 건이다(`TestIngestColdServiceTextDoesNotWaitForHeldGate`). 원본은 전용 thread에서 끝까지 기다린다(의도한 차이).
- **group 값 의미**: 답은 같은 순서(daily 다음 permanent, TextRD)의 같은 데이터(파일 + pending(보류 text 포함) + 버린 text)에서 나온다. 저장된 text는 바뀌지 않고, "없음" cache는 그 ref의 text가 pending이 되는 순간 지워지므로 lock 안에서 파일을 읽은 것과 같은 답이다. memo 상한(50000)·memo 예산 초과 시 skip·"없음"일 때 memo를 두지 않는 것은 그대로다.
- **비용**: memo hit(대부분의 XLog)는 lock 판정에서 memo map 조회가 한 번 늘었다. memo miss인 XLog는 lock 판정에서 `TextCached`를 한 번 하고(날짜 계산 포함), group 단계는 그 답을 쓴다(같은 service·EndTime이면 다시 묻지 않음). 파일을 읽은 경우만 lock을 다시 잡은 뒤 `TextCached`를 한 번 더 한다.
- `Stats.ServiceTextsAhead`는 lock 밖에서 읽기를 맡긴 수, `Stats.ServiceTextSkipped`는 답을 기다리지 못해 group 없이 저장한 XLog 수다.

## 12. 메모리 상한, object 축출, cache 예산과 영속 저장 분리

원본: `core/AgentManager.scala`(`objMap`, `clearInactive` 219-228), `core/ObjectMap.scala`, `core/PerfCountCore.scala`(`RealtimeCounterWR.add` → `Auto5MSampling.add`·`CounterCache.put` → `ObjectCpuChecker.checkCpu`), `core/XLogCore.scala`(`XLogWR.add`·`XLogCache.put`). Java는 이 상태들에 개수·byte 상한이 없다(Java heap이 상한). Go는 아래 상한을 두고 포화 동작을 정한다.

### 상한(`Config`, 0이면 기본값)

| 필드 | 기본값 | 대상 |
|---|---|---|
| `MaxObjects` | `DefaultMaxObjects` 16384 | live object map, 그리고 object 수로 크기를 묶는 table(counter 이름, alert summary, kube manager, CPU streak) |
| `MaxCounters` | `DefaultMaxCounters` 262144 | realtime counter cache key, 5분 sample key(각각), status+interaction 행, kube seq key |
| `MaxCacheBytes` | `DefaultMaxCacheBytes` 64MiB | object·kube·counter cache/sample·XLog ring·alert ring·alert summary·group meter·sampling gxid/지연 bucket·status/interaction의 **계상 bytes** 합. object와 kube는 자기 bytes(`coreBytes`)만으로 따로 검사한다(아래 S2) |

runtime은 `--max-objects`, `--max-counters`, `--engine-cache-mb`(MiB)로 이 값을 넘긴다(0 또는 flag 없음이면 기본값).

### 계상 bytes와 실제 heap

`MaxCacheBytes`는 encode된 크기와 작은 고정 overhead를 센다. Go map·struct·allocator overhead는 세지 않으므로 실제 heap은 더 크다. 측정(go1.27.2 darwin/arm64, `runtime.GC` 뒤 해당 상태를 버렸을 때 회수된 `HeapAlloc`, 일회성 측정 1회, 재현 테스트는 저장소에 없음):

| 상태 | 조건 | 계상 | 회수된 heap | 비율 |
|---|---|---|---|---|
| object | 16384개, tag 4개 | 1.86MB | 31.3MB | 약 17배 |
| counter cache + sample | 270336 key(object 4096 × 64 + metadata 2), Float 값 | 11.5MB | 85.9MB(이전 구조 157.3MB) | 약 7.5배 |
| sampling gxid | 200000개 | 8.0MB | 6.3MB(이전 구조 17.5MB) | 약 0.8배 |
| XLog ring | 20480칸, 55B XLog | 1.1MB | 5.7MB | 약 5배(encoder 최소 용량 256B) |

- 이번 변경에서 counter cache 값은 `protocol.Value`(약 256B struct) 대신 encode된 정확한 크기 `[]byte`로 보관한다(current와 sample이 같은 slice를 공유, 읽을 때 decode). 같은 조건에서 278MB → 158MB. 조회 결과(`Counter`, `Objects`의 counter tag, 5분 sample 값)는 같다(이전에도 encode→decode 복제본을 돌려줬다).
- 2026-10-23 counter cache는 map 5개(current·samples·counterSizes·sampleSizes·counterExpiry)를 2개(`current`의 `cachedCounter{data, expiry}`, `samples`)로 줄였다. 계상 크기는 늘 `len(value)+len(name)`(sample은 +16)이었으므로 따로 들지 않고 계산한다. entry당 map 연산은 11회에서 4회(새 key는 5회)가 되고, encode는 engine 소유 `Encoder`를 재사용한다(entry당 256B 임시 buffer 없음). 상태·계상 bytes·skip 수·순서·만료·5분 sample은 이전 구현과 같다(`TestCounterCacheMatchesMapOracle`: 이전 구현을 oracle로 두고 개수·object·byte 상한, 모든 값 종류, 만료·단건 만료·5분 clear를 섞은 무작위 순서). `BenchmarkCacheCounter`(replay generator의 counter pack, object 4000): 4.48µs → 1.56µs/pack, 2013B·15 alloc → 45B·7 alloc.
- 같은 날 처리된 gxid 집합(`addProcessed`, 원본 `XLogDelayingCache.processedGxidMap` = `LongIntLinkedMap.setMax`, `add` LAST mode)은 `container/list` 대신 pointer 없는 `map[int64]int32`와 삽입 순서 ring `[]int64`(`processedSet`)다. 원본도 축출 외에 entry를 지우지 않으므로 FIFO가 ring과 같다. 의미는 그대로다: 다시 온 txid는 자리를 유지한 채 discard 값을 더하고(int wrap), 상한에서 새 txid는 가장 오래된 것을 밀어내며, 상한은 기동 때 값(conf reload 무관)이고 재기동하면 비어 있다. 계상은 entry당 40B 그대로(상한에서의 교체는 0B). 검증: `TestProcessedSetMatchesListOracle`(이전 list 구현 oracle, 상한 1~5000, 중복·음수·0 txid), `TestProcessedGxidSemantics`, `TestProcessedGxidBudget`. `BenchmarkAddProcessed`(기본 상한 500,000, 가득 찬 상태): live heap object 약 1,002,000 → 약 2,200(engine 기본분 포함), live heap 51.9MB → 23.9MB, add 약 195ns·2 alloc → 약 128ns·0 alloc.
- 따라서 실제 메모리 상한을 정하는 것은 byte 예산이 아니라 개수 상한이다. 기본값에서 최악 추정: object 약 31MB + counter 약 85MB + gxid 50만 개 약 23MB + ring·기타. 64MiB 예산은 계상 기준으로 기본 개수 상한을 대략 담는 크기다(object 약 2MiB, counter+sample 약 12MiB, gxid 최대 약 20MiB, ring·alert ring 수 MiB, 나머지는 지연 bucket). `GOMEMLIMIT`는 이 위의 soft limit일 뿐이다.

### object 상한과 축출(`acceptObjectLocked`, `pickEvictionLocked`)

- 새 object(live map에 없는 objHash)가 `MaxObjects`에 닿으면 **가장 오래 inactive인 object 하나**를 live map에서 뺀 뒤 등록한다. 후보는 `RemoveInactiveObjects`와 같은 규칙(`now > wakeup + adjustDeadTime`, `batch` 제외)으로 dead인 object이고, `wakeup + adjustDeadTime`(inactive가 된 시각)이 가장 이른 것, 같으면 작은 objHash다. 정리 범위는 `dropObjectLocked`(= `RemoveInactiveObjects`, Java `objMap.remove`)와 같다: objState·이름 table·kube 객체 속성·CPU streak·object 이름 cache·계상 bytes. 저장된 object 레코드(`ObjectWR`), 이미 낸 alert, kube pod seq는 건드리지 않는다. counter cache·sample은 objHash 별로 지우지 않고 10초 만료·5분 sample로 사라진다(`RemoveInactiveObjects`와 같음).
- 축출된 object의 다음 heartbeat는 새 등록이다(Java `objMap`에 없던 object와 같음: `ObjectWR.add`, ACTIVATED_OBJECT 없음). `Stats.ObjectsEvicted`.
- dead object가 없고 Tick을 기다리는 dead object(dead 시각은 지났지만 아직 alive, 다음 1초 Tick에 후보가 된다)도 없으면 **조용한 면제 object** 하나를 고른다(R7, 2026-10-23). 면제 object는 Java 규칙으로 한 시간 안에 죽지 않는 object로 좁혔다. `batch`(`AgentManager.scala:58`, dead 판정 제외)이거나, deadTime tag(`AgentManager.scala:60-64`)로 정한 dead 시각이 `exemptEvictAt`보다 늦은 object다. `exemptEvictAt` = 마지막 heartbeat + max(그 type의 tag 없는 dead time(`object_deadtime_ms`/`object_zipkin_deadtime_ms`), batch는 `exemptObjectSilenceMs` 60초, 그 밖은 `exemptDeadTimeMs` 1시간)다. 따라서 deadTime이 1시간 이하인 object(Telegraf 기본 35초 `telegraf_object_deadtime_ms`, 120초·300초를 쓰는 제3자 Agent 등)는 면제가 아니다. Java처럼 dead 시각 뒤 Tick에서 inactive가 되어 INACTIVE_OBJECT를 내고 kube seq를 반납한 뒤, 위의 dead 축출 대상이 된다(`TestObjectLimitLongDeadTimeFollowsJava`). batch 60초의 근거: batch Agent는 ObjectPack을 10초마다 보낸다(`scouter.agent.batch/.../Main.java:66-70`). 1시간 경계의 근거: Java 기본 dead time(8초)·Telegraf(35초)보다 훨씬 길고, 이보다 긴 deadTime은 "사실상 죽지 않음"을 뜻하는 설정으로 본다. 그런 object가 1시간 넘게 heartbeat가 없으면 상한에서만 뺀다. 고른 것 중 `exemptEvictAt`이 가장 이른 것(같으면 작은 objHash)을 위와 같은 범위(`dropObjectLocked`)로 live map에서만 뺀다. `Stats.ObjectsEvictedSilent`, 로그는 10초에 한 줄이다. 이유: 면제 object만으로 상한이 차도 새 Agent 등록이 계속 거절되지 않게 한다. 정상 Agent 영향: 상한 아래에서는 바뀌는 것이 없다. 상한에서도 위 시간 안에 heartbeat가 온 object는 빼지 않는다(`TestObjectLimitSilentExemptObjects`, `TestObjectLimitDeadBeforeTickPreferred`, `TestObjectLimitSilenceFollowsObjectDeadTime`).
- 후보가 없으면 이전처럼 `ErrLimit`로 거절한다(`Stats.Rejected`와 `Stats.ObjectsLimited`, 로그는 10초에 한 줄, objName과 live 수). 거절된 heartbeat는 저장하지 않는다(Agent가 2초마다 다시 보낸다).
- 축출 판단은 live object 수에 비례하는 scan이다. 후보가 없을 때 가장 이른 dead 시각(면제 object는 `exemptEvictAt`)을 기억해(`evictHint`, 그 뒤 heartbeat가 deadTime을 줄이면 낮추고 live 설정이 바뀌면 버린다) 그 시각 전에는 scan하지 않는다. 상한에서 새 object heartbeat가 몰려도 매번 전체 scan하지 않는다.
- 순서: kube `applyPodSeq`(§1) → 상한에서 축출 대상 선택(`pickEvictionLocked`) → object byte 예산(`coreDelta`, 빠질 object의 bytes를 빼고 계산) → object text 저장 queue → 축출 확정(`evictObjectLocked`) → live map 갱신. 새 object가 object byte 예산이나 text 저장에서 거절되면 아무것도 빼지 않는다(`TestObjectLimitRefusedRegistrationEvictsNothing`). 이전에는 선택과 동시에 뺐다.
- **원본과 차이**: Java는 상한이 없어 거절·축출이 없다. Client `AgentInfo`의 `clearInactive`(Go `RemoveInactiveObjects`)는 그대로다. 조용한 면제 object 축출은 Java에 없는 동작이다. ① 알림: Java에서 그 object는 inactive가 되지 않으므로 INACTIVE_OBJECT를 내지 않고, Go도 내지 않는다. 빠진 뒤 다음 heartbeat는 새 등록이라 ACTIVATED_OBJECT도 없다(dead 축출과 같음). ② kube: `inactive()`를 부르지 않으므로 pod seq를 반납하지 않는다(Java `objMap.remove`와 같음, `TestObjectLimitSilentEvictionKeepsKubeSeq`). ③ `updated`(20번마다 MAYBE_OBJ_DUP)는 objState와 함께 지워져 재등록 뒤 0부터 다시 센다. Java에서는 날짜가 바뀔 때만 0이 된다. ④ 저장된 object 레코드는 남는다.

### 기동 시 적재(`loadObjects`)

- 오늘 object 파일의 레코드가 `MaxObjects`(또는 object bytes가 `MaxCacheBytes`)를 넘어도 기동은 실패하지 않는다(이전: `New`가 `ErrLimit` → 서버 기동 실패). **wakeup이 최근인 순서**(같으면 작은 objHash)로 상한까지 적재하고 나머지는 적재하지 않는다. 최근 활동한 object일수록 곧 heartbeat를 보낼 가능성이 크고, 빠지는 쪽은 축출 규칙이 먼저 내보낼 object와 같다. 적재하지 않은 수는 `Stats.ObjectsNotLoaded`와 경고 로그 1회.
- 적재하지 않은 object는 다음 heartbeat 전까지 실시간 OBJECT_LIST에 보이지 않는다(날짜 지정 조회는 파일을 읽으므로 보인다). 원본 Java는 기동 때 오늘 파일을 objMap에 넣지 않으므로(§6) 이 경우의 실시간 목록은 원본과 다르지 않다.
- storage `Objects`의 `MaxQueryRecords`(기본 100000)·`MaxQueryBytes`를 넘는 파일은 여전히 `New` 실패다(storage 범위).

### cache 예산 초과와 영속 저장(S2)

cache는 실시간 조회용 보조 상태다. 예산·개수 상한에 걸려도 **영속 저장은 그대로 하고 cache 갱신만 건너뛴다**. 수집 결과는 성공(`Completed`)이며 `Rejected`가 아니다.

| 경로 | 저장 | 건너뛰는 cache | 영향 | 계수 |
|---|---|---|---|---|
| XLog `storeXLog` | `OnXLog` → XLog writer enqueue(이전과 같음) | realtime ring 칸 | 그 XLog는 Client realtime XLog(`XLogsRealtime`, loop/index cursor 불변)에 나오지 않는다. 기간 조회·txid 조회·profile은 그대로 | `XLogCacheSkipped` |
| XLog `processGroup` | 위와 같음, group 값도 그대로 채운다 | service→group memo, 새 group meter | memo가 없으면 다음 XLog가 service text를 다시 읽는다. meter가 없으면 그 XLog가 realtime service group·매분 group 저장 합계에서 빠진다 | `XLogCacheSkipped` |
| XLog `addProcessed`(driving XLog) | 위와 같음 | processed gxid | 그 gxid의 뒤늦은 child XLog/profile이 matching되지 않고 지연 bucket으로 간다. 회전 때(또는 아래 `delayPack` 예산 처리로 일찍) 복구 규칙(XLog는 discardAll이 아니면 저장, profile은 discardNone만 저장)을 따른다. 원본에서 gxid map이 `xlog_sampling_matcher_gxid_keep_memory_count`로 밀려난 경우와 같다 | `XLogCacheSkipped` |
| 지연 child XLog/profile `delayPack` | 거절하지 않는다. 예산이 모자라면 **같은 종류(XLog 또는 profile)의 가장 오래된 지연 group**부터 복구 규칙을 일찍 적용해(`recoverGroup`, 회전 때와 같은 규칙·순서·release time) 자리를 만든다. 같은 종류의 group을 모두 복구해도 자리가 생기지 않으면(남은 예산 + 그 종류의 지연 bytes(`xlogBytes`/`profileBytes`)보다 크면, 예산을 다른 상태가 채움) 하나도 복구하지 않고 들어온 pack에 그 자리에서 복구 규칙을 적용한다. 기다리던 group은 그대로 기다린다 | 지연 대기(그 group만) | 일찍 복구된 child는 driving XLog를 기다리지 못한다. 그 driving이 그 child의 회전 전에 왔다면 원본에서는 driving의 discard로 정해졌을 것(child XLog는 driving이 discardNone일 때만 저장, child profile은 driving이 discardAll이 아니면 저장)이 복구 규칙으로 정해진다. 결과는 원본에서 driving이 회전 뒤에 온 경우와 같다. 일찍 복구된 profile의 time은 그 시점(release time)이다 | `DelayedRecoveredEarly`(일찍 적용해 저장 queue에 넘김. 그 뒤 queue 포화로 버려진 것은 storage `xlogQueue`/`profileQueue`의 `Dropped`에 들어가며 이 값에서 빠지지 않는다), `DelayedDiscardedEarly`(일찍 적용해 규칙대로 버림), `DelayedAbandoned`(꺼낸 group의 복구가 오류(ctx 취소·decode·저장)로 멈춰 처리하지 못한 child, 회전 때 포함). 종료 때 bucket에 남아 있던 지연 group은 꺼내지 않으므로 어느 계수에도 들어가지 않는다(원본도 메모리 cache와 함께 잃는다) |
| AlertPack `acceptAlertLocked` | `AlertWR.add`(queue)와 tag count는 항상 | alert summary(object 수 `MaxObjects`, object당 title 수 `MaxCounters`, 예산), realtime alert ring(예산). 둘을 따로 판단한다. ring은 예산이 모자라면 먼저 **가장 오래된 지연 profile group**부터 복구 규칙을 일찍 적용해 자리를 만들고(실시간 alert 팝업이 driving을 기다리는 profile보다 우선), profile group을 모두 복구해도 자리가 생기지 않으면(남은 예산 + `profileBytes`보다 크면) 하나도 복구하지 않고 바로 건너뛴다(alert 하나가 lock 안에서 지연 profile 전부를 복구하지 않게). 지연 XLog group은 쓰지 않는다(XLog 저장 경로를 Tick의 object alert 안에서 돌리지 않기 위해서) | summary를 건너뛰면 다음 5분 alert summary pack(SummaryPack 저장)에서 그 alert가 빠진다(group meter와 같은 처리). ring을 건너뛰면 Client realtime alert(cursor 불변)에 나오지 않는다. 기간 조회는 그대로 | `AlertCacheSkipped`(summary·ring 각각, alert 하나에 최대 2) |
| StatusPack | `StatusWR.add`(queue) 항상 | status cache(status+interaction 행 `MaxCounters`, 예산) | 새 key는 cache되지 않고, 있던 key는 **지운다**(받은 값보다 오래된 값이 최대 15초 현재 값처럼 보이지 않게. Java는 늘 갱신하므로 stale 값보다 "없음"이 가깝다. 지우기는 bytes를 줄이기만 한다). 기간 조회는 그대로 | `StatusCacheSkipped` |
| realtime counter(timetype 1) | counter writer queue에 넣기(RealtimeCounterWR.add) 먼저 — 원본 PerfCountCore 순서, 쓰기 결과와 무관(§13) | counter cache·5분 sample·counter 이름(pack 단위로 전부 또는 전무) | 그 pack의 값은 `Counter`/OBJECT_LIST `counter` tag에 반영되지 않고 기존 값이 10초 만료까지 남는다. 5분 daily 값은 그 구간에 cache된 마지막 값이 쓰이고, 그 구간에 한 번도 cache되지 못한 key는 그 칸이 비어 있다(realtime 기록은 있음). `ObjectCpuChecker`는 pack을 읽으므로 그대로 수행 | `CounterCacheSkipped` |

- 한 XLog가 ring·meter·gxid에서 각각 셀 수 있다.
- 이전 Go는 위 경우 모두 `ErrLimit`로 저장 전에 거절했다(XLog·counter 유실). 원본 Java는 상한이 없어 이 상황이 없다.
- 2026-10-24(S2 나머지): 지연 child·alert·status도 위 원칙으로 바꿨다. 이전에는 예산이 없으면 저장 전에 `ErrLimit`로 거절했다(`DelayedRejected`, `Rejected`). `DelayedRejected`는 `DelayedRecoveredEarly`·`DelayedDiscardedEarly`·`DelayedAbandoned`로 바꿨다(`/health` key도 바뀐다). 수정 전 재현과 회귀: `TestPersistedPastCacheBudget`(지연 bucket으로 예산을 채운 뒤 AlertPack·StatusPack·지연 child XLog/profile을 넣고 store를 다시 열어 디스크에서 찾는다. 수정 전에는 4건 모두 `ErrLimit`, 디스크에 없음), `TestDelayPackRecoversOldestPastBudget`, `TestStatusAndAlertCacheSkippedStillStored`, `TestAlertRingRecoversDelayedProfiles`, `TestEarlyRecoveredChildStoredOnce`(일찍 저장된 child는 뒤에 온 driving(discardNone/discardAll)이 다시 저장하지 않는다: txid당 1건, `XLogs`는 driving만 +1), `TestRecoverGroupCanceledMidway`, `TestRollCanceledKeepsRemainingGroups`(회전 중 취소: 처리 중 group의 남은 child만 Abandoned, 닿지 못한 group은 old bucket에 남아 다음 회전에서 복구), `TestDelayPackNegativeRoom`, `TestAlertRingNoRoomRecoversNothing`, `TestObjectAlertsRecoverDelayedProfiles`(Tick의 INACTIVE_OBJECT, heartbeat의 ACTIVATED_OBJECT, 20번째 save의 MAYBE_OBJ_DUP가 지연 profile을 복구해 ring에 들어간다. profile queue 1(포화) 변형 포함, Tick 반환·계상 일치), `TestCacheBytesSymmetric`(지연 child·profile·driving·dropped·status·alert·Tick·예산 변경을 무작위로 섞은 4000 단계마다 `cacheBytes`·`ringBytes`를 상태에서 다시 계산해 비교).
- 회전 경로(`recoverBucket` → `recoverGroup` → `recoverPack`): 앞 group부터 꺼내 child마다 ctx 확인 → decode → XLog는 discardAll이 아니면 `storeXLog`, profile은 time=now 뒤 discardNone이면 `putProfile`. 오류 때 그 child와 남은 child는 `DelayedAbandoned`로 센다. driving이 꺼내는 경로(`acceptXLog`의 child 저장, `releaseProfiles`)의 같은 오류는 아직 세지 않는다.
- **크기가 늘지 않는 갱신**(2026-10-24): `cacheDelta`는 bytes를 늘리지 않는 갱신(delta ≤ 0, 같은 크기의 status·counter 값, 이미 있는 alert title의 summary 증가, 더 작은 ring 칸)을 늘 통과시킨다. object·kube 때문에 남은 예산이 음수여도 그렇다. 이전에는 남은 예산이 음수가 될 수 없었다.
- **공유 cache 예산과 object·kube**: 공유 cache 예산은 object·kube(`coreBytes`)도 포함해 계산한다. 이 몫이 커지면 bytes가 늘어나는 cache 갱신을 건너뛰고 Skipped 계수로 센다. 저장은 계속된다. `TestDelayPackNegativeRoom`, `TestAlertRingNoRoomRecoversNothing`.
- **`delayPack`을 "오래된 group부터 일찍 복구"로 정한 근거**: 대안은 "들어온 pack에 즉시 복구 규칙 적용"이다. 원본에서 지연 child는 ① driving XLog가 오면 driving의 discard로 정해지고 ② 오지 않으면 bucket 회전 때 복구 규칙으로 정해진다(`XLogDelayingsRecoverCore`, `ProfileDelayingsRecoverCore`). 가장 오래된 group은 회전 때 가장 먼저 복구될 group이고 원래 ②로 정해질 시점에 가장 가깝다. child는 대개 driving보다 먼저 끝나 먼저 오므로, 막 들어온 child일수록 driving을 만나 ①로 정해질 가능성이 크다. 그래서 오래된 group을 일찍 복구하는 쪽이 원본과 다르게 정해지는 child가 적다. 개수 상한(`LongKeyLinkedMap.setMax`)에서 가장 오래된 group을 **복구 없이 버리는** 원본 동작은 그대로 둔다(원본 의미). 예산은 원본에 없는 Go 상한이므로 그 때문에 데이터를 버리지 않는다. 종류를 넘어 꺼내지 않는 것은(XLog가 profile group을 복구하지 않음) 종류마다 회전 주기가 달라 "가장 오래된"을 비교할 기준이 없기 때문이다. 다른 cache가 예산을 채운 경우에는 들어온 pack이 즉시 복구 규칙을 따른다.
- **object·kube는 공유 cache 예산에서 뺐다**(2026-10-24): object live map과 kube pod seq 상태는 cache가 아니라 AgentManager 상태다. 거절하면 heartbeat가 저장되지 않고 object가 INACTIVE가 되며 kube seq를 받지 못한다. 반면 개수 상한(`MaxObjects`, `MaxCounters`, initial 10000)만으로는 메모리 상한이 되지 않는다(ObjectPack 하나의 크기는 개수 상한과 별개다). 그래서 byte 검사는 남기되 **object와 kube 자신의 bytes(`coreBytes`)만** `MaxCacheBytes`와 비교한다(`coreDelta`). 이 bytes는 `cacheBytes`에도 들어가므로 다른 cache는 그만큼 갱신을 건너뛴다. 계상 합계의 최악은 `MaxCacheBytes`의 2배다(object·kube가 예산을 채우고 cache도 예산까지 찬 경우, 기본 64MiB면 128MiB 계상). `coreBytes`는 object 등록·heartbeat 갱신·save·축출·제거·Tick inactive(kube seq 반납)·날짜 변경 모든 경로에서 대칭이다(`TestObjectsAndKubeIgnoreFullCache`, `TestCoreBytesReturnAfterRemoval`, `TestCoreBytesSymmetric`: 무작위 3000 단계마다 `coreBytes` = object 크기 합 + kube 상태 재계산값, 삭제 경로 2곳을 일부러 틀리면 실패함을 확인). `/health`에는 노출하지 않는다(`Stats`는 engine lock 없이 atomic만 읽으므로 별도 mirror가 필요하다). 기동 적재(`loadObjects`)도 object bytes만 본다.
- interaction counter(`InteractionPerfCounterPack`)는 원본에서도 cache(`InteractionCounterCache`)만 있고 저장이 없다. 그래서 상한에 걸리면 이전처럼 `ErrLimit`(`Rejected`)로 센다.
- **object 축출 후보**: dead이면서 1초 Tick이 이미 inactive로 바꾼(`objState.alive == false`) object만 후보다. Java `clearInactive`가 `alive == false`만 지우는 것과 같고, INACTIVE_OBJECT alert와 kube seq 반납(`inactive()`)이 먼저 일어난다. dead지만 아직 alive인 object만 있으면 새 object를 거절하고(`ObjectsLimited`), 다음 Tick 뒤에는 축출할 수 있다.
- **운영 영향**: 축출된 object가 돌아오면 새 등록으로 처리되어 ACTIVATED_OBJECT가 나가지 않는다. 운영자는 해제 알림 없는 INACTIVE_OBJECT만 보게 된다.

### 인증 없는 UDP 입력의 자원 상한(R7, 2026-10-23)

UDP 수집에는 인증이 없다(원본과 같음). 원본은 아래 자원에 상한을 두지 않는다. Go는 상한과 계수를 둔다.

| 자원 | 원본 | Go |
|---|---|---|
| 영구 text div | 상한 없음 | 원본 17개 div는 그대로. 그 밖의 div는 64개까지(storage `admitTextDiv`), 넘으면 거절·`Stats.TextDivsLimited`·로그. 세부는 [storage-contract](../../docs/compatibility/storage-contract.md) "영구 Text div 상한" |
| live object | 상한 없음 | `MaxObjects`(16384). 상한에서는 위 축출 규칙을 따르고, 후보가 없으면 새 등록을 거절한다(`ObjectsLimited`). 이미 등록된 Agent의 수집은 영향이 없다 |
| object 이름 text | 상한 없음 | 원본과 같음 |
| 날짜별 realtime counter 이름 | 상한 없음 | 날짜 사전 65,536개(body 16MiB)까지. 넘으면 새 이름의 entry만 빼고 나머지는 저장한다(`Counters.NamesLimited()`, `S133`). 세부는 [storage-contract](../../docs/compatibility/storage-contract.md) "Realtime counter 이름 사전" |

counter 이름 사전의 읽기 한도(Java가 쓴 사전은 이름 1,048,576개·body 64MiB까지)와 계수는 [storage-contract](../../docs/compatibility/storage-contract.md) "Realtime counter 이름 사전"에, 운영 지표 `/health` `maintenance.counterNamesLimited`의 해석은 [monitoring](../../docs/operations/monitoring.md)에 있다. 같은 값이 종료 stats `counterNamesLimited=`로도 남으며 `WriteInvalid`에는 들어가지 않는다. 원본이 상한 없이 이름을 더하는 근거는 `RealtimeCounterDBHeader.scala:105-146`이다.

수신 내용만으로는 송신자를 구별할 수 없다. 수집 port는 망 분리·방화벽으로 신뢰하는 출처에만 연다. UDP 출처 allowlist(Go 전용, 기본 off)는 이번 범위 밖이다.

- **counter 이름 상한의 부분 저장**: 2026-10-23 storage에 반영했다(위 표). 상한은 65,536개로 올렸고, 상한에 닿으면 새 이름 entry만 빼며 전용 계수 `NamesLimited`를 둔다. 이름이 4096개를 넘는 Java DB 날짜도 읽는다(`TestJavaStorageInteropRealtimeCounterNames`).

## 13. kind별 비동기 record writer (`writer.go`, S1·S3)

원본: `core/CoreRun.scala`·`db/DBCtr.scala`(`MAX_QUE_SIZE` 10000), `util/RequestQueue.java`(`put`은 가득 차면 새 항목을 버리고 false), `core/PerfCountCore.scala`, `db/RealtimeCounterWR.scala`, `db/DailyCounterWR.scala`, `core/Auto5MSampling.scala`, `core/app/XLogGroupPerf.scala`, `db/ObjectWR.scala`, `core/StatusCore.scala`·`db/StatusWR.scala`, `core/AlertCore.scala`·`db/AlertWR.scala`·`core/AlertSummary.scala`, `core/SummaryCore.scala`·`db/SummaryWR.scala`, `core/StackAnalyzerCore.scala`·`db/StackAnalyzerDB.scala`, `core/BatchCore.scala`·`db/BatchDB.scala`, `core/SpanCore.scala`·`db/ZipkinSpanWR.scala`(`span_queue_size` 1000), `core/VisitorCore.scala`.

원본 UDP thread는 디스크에 쓰지 않는다. kind마다 Core queue → WR/DB writer queue를 각각 단일 thread가 소비한다. 이전 Go는 UDP worker가 engine lock을 잡은 채 counter·object·status·alert·summary·stack·batch·span·visitor를 **동기 디스크 I/O**로 썼다. 그래서 느린 디스크나 오래 잡힌 Store/Counters gate가 XLog를 포함한 수집 전체를 멈췄다(S1). counter·alert는 저장이 실패하면 cache·ring도 갱신하지 않았다(S3).

이제 engine lock 안에서는 **메모리 갱신과 enqueue만** 한다. 디스크 쓰기는 kind별 writer goroutine 하나가 FIFO로 한다. XLog·profile·text는 이미 storage의 writer queue로 쓰므로(storage CONTRACT §8) 여기서 다시 감싸지 않는다.

### queue 표

| kind | 원본 queue | 개수 상한 | byte 상한(추정 heap) | 생산자(Go) | 포화 로그 id | 쓰기 오류 로그 id |
|---|---|---|---|---|---|---|
| `counter` | PerfCountCore → RealtimeCounterWR | 10000 | 32 MiB | realtime PerfCounterPack(timetype 1) | S109/S134 | S133 |
| `daily` | DailyCounterWR(PerfCountCore timetype≠1, Auto5MSampling, XLogGroupPerf) | 10000 | 32 MiB | Agent의 non-realtime pack, `Sample5M` 회차(1 item), group 저장(1 item) | S123 | S122 |
| `object` | ObjectWR | 10000 | 8 MiB | object 등록·변경(`acceptObjectLocked`), 쓰지 못한 object의 다음 heartbeat, permanent `RemoveObjects`(`ObjectWR.remove`) | S131 | - |
| `status` | StatusCore → StatusWR | 10000 | 8 MiB | StatusPack | S113/S136 | S135 |
| `alert` | AlertCore → AlertWR | 10000 | 8 MiB | AlertPack, INACTIVE/ACTIVATED_OBJECT | S106/S121 | S120 |
| `summary` | SummaryCore → SummaryWR | 10000 | 256 MiB(모든 Agent가 같은 5분 경계에 보내는 burst를 담는 크기) | Agent SummaryPack, 5분 alert summary(1 item) | SummaryCore/S211 | S210 |
| `stack` | StackAnalyzerCore → StackAnalyzerDB | 10000 | 16 MiB | StackPack(live object만) | S200/S000 | - |
| `batch` | BatchCore → BatchDB | 10000 | 8 MiB | BatchPack | S200/S000 | - |
| `span` | SpanCore → ZipkinSpanWR | 1000(`span_queue_size` 기본값, 설정은 읽지 않음) | 16 MiB | SpanContainerPack | SZ116/SZ144 | SZ143 |
| `visitor` | VisitorCore | 10000 | 2 MiB | 방문(XLog xtype 0, userid≠0, live object), Tick의 visitor roll | S208 | S199 |

- **counter와 daily를 나눈 이유**: 원본도 RealtimeCounterWR와 DailyCounterWR는 다른 thread다. 한 queue로 합치면 5분 sample 회차(최대 `MaxCounters`개 record)를 쓰는 동안 realtime counter가 그 뒤에서 기다리다 포화될 수 있다. 같은 daily key·bucket의 순서는 daily queue 하나가 지킨다. 세 생산자(Agent non-realtime pack, `Sample5M`, group 저장)가 같은 queue에 넣으므로 **마지막에 enqueue된 값이 남는다**(원본과 같다). realtime과 daily는 다른 파일이라 서로 순서가 필요 없다.
- **byte 상한**: 원본은 개수만 제한한다(Java heap). 큐에 든 Go pack은 encode 크기의 최대 약 20배다(counter entry 하나가 272B `Entry`). 그래서 문자열·byte 길이와 Value/Entry 구조체 크기(go1.27 64bit: `Value` 256B, `Entry` 272B)로 heap을 추정해(`packHeapBytes`) 제한한다. 예: key 50개 Float counter pack은 encode 777B, 추정 heap 약 14KB라 counter queue에는 약 2300개가 들어간다(개수 상한 10000보다 byte 상한이 먼저 걸린다). 초당 500 pack이면 약 4.6초의 쓰기 정체를 버리지 않고 견딘다. 모든 kind의 합은 138 MiB(추정 heap)로, 디스크가 멈춘 최악에서도 queue가 이보다 커지지 않는다. 값은 운영 측정 전 추정이며 정체 시간과 메모리의 교환이다. queue가 비어 있으면 상한보다 큰 record 하나도 받는다.
- **회차 item**(`passKind`): `Sample5M`·5분 alert summary·group 저장은 회차 전체를 한 item으로 넣는다. 원본은 record마다 queue에 넣어 queue를 넘는 부분만 버린다. Go 회차 item은
  - **byte 상한을 면제**한다. 같은 tick에 group 저장과 5분 sample이 daily queue에서 만나거나 queue가 Agent record로 차 있어도 회차 전체가 버려지지 않는다(`TestPassesShareTickWithoutByteDrop`: 면제를 빼면 실패함을 확인). 개수 상한(channel 10000 item)은 그대로다.
  - **생산자마다 하나만** queue에 있을 수 있다. 앞 회차가 아직 queue에 있거나 쓰는 중이면 새 회차는 버리고 센다(`Dropped`, `Sample5MDropped`/`AlertSummaryDropped`, group 저장은 다음 tick 재시도). writer가 멈춘 동안 회차가 쌓이지 않게 하는 상한이다(원본도 멈춘 writer 뒤 queue가 차면 다음 회차를 버린다).
  - 그래서 회차가 queue에서 쓰는 최악 메모리는 생산자당 회차 1개이며 engine 상태 상한으로 묶인다(추정 heap, 기본 상한): 5분 sample은 sample 수 ≤ `MaxCounters`(262144) × (`sample5M` 56B + encode 값 ≤ 10B) ≈ **16.5 MiB**(값은 encode 상태로 두고 writer가 record마다 pack을 만든다. 예전처럼 decode한 pack이면 같은 수에서 약 160 MiB였다), group 저장은 meter ≤ 2000 × 3 row × (`groupRow` 40B + 이름 ≤ 15B) ≈ **0.3 MiB**, alert summary는 object ≤ `MaxObjects`(16384)개의 SummaryPack이고 title 하나가 Value 3개(약 0.8KB)다. title 수는 cache 예산(`MaxCacheBytes`, title당 `len+16` 계상)으로 묶이며, 이 회차는 이전에도 Tick이 한 번에 만들던 크기다(writer를 둔 뒤 늘어난 것은 회차 1개를 queue에서 기다리는 시간뿐).
  - 집계는 record 단위다(`Enqueued`/`Written`/`WriteErrors`/`Dropped`가 회차의 record 수만큼 늘어난다).

### lock 안 순서와 소유권

- engine lock 안에서 메모리 상태를 먼저 갱신하고, 그 결과와 무관하게 enqueue한다. 디스크 결과·queue 포화는 메모리 상태를 되돌리지 않는다(S3, 원본 순서).
  - counter: queue → counter cache·5분 sample → `ObjectCpuChecker`(`RealtimeCounterWR.add` 다음 `Auto5MSampling`·`CounterCache`, 그다음 `checkCpu`).
  - alert: time = 서버 now → `AlertSummary.add` → `AlertCache.put`(ring) → `AlertWR.add`(queue) → tag count.
  - status: time = 서버 now → `StatusCache` → `StatusWR`(queue).
  - object: text 확인·queue(`TextCore`) → live map(`objMap.put`) → `ObjectWR.add`(queue). text를 먼저 확인하는 것은 이름을 저장할 수 없는 object를 상태 변경 전에 거절하던 이전 Go 동작을 지키기 위해서다(text queue는 별도라 원본 순서와 관찰 차이가 없다).
- cache 예산·개수 상한에 걸린 status·alert는 cache만 건너뛰고 enqueue한다(2026-10-24, §12). object는 자기 byte 예산(`coreDelta`)과 `MaxObjects`에 걸릴 때만 enqueue 전에 거절한다.
- 시간·날짜는 accept 때 정한다. alert·status time은 서버 now, counter time 0이면 now, span timestamp 0이면 now, object 파일 날짜는 accept 시점 날짜다. 그래서 자정 직전에 받고 자정 뒤에 쓴 record도 자기 날짜 파일로 간다. **차이**: 원본 `ObjectWR`은 writer thread의 현재 날짜 파일을 연다. Go는 accept 시점 날짜를 쓴다.
- writer에 넘기는 값은 engine이 더 바꾸지 않는 것이다. 수집 경로가 넘겨받은 pack(이후 engine은 읽기만 한다: `PackLog`, tag count, `ObjectCpuChecker`), object는 encode한 bytes를 decode한 **복사본**(engine은 live object를 heartbeat마다 바꾼다), stack·batch는 accept 때 읽은 objName, visitor는 scalar(now, objType, userid, objHash), 5분 sample은 sample map에서 뗀 encode 값(읽기 전용, cache와 공유해도 제자리 수정이 없다)이다.
- **datagram 별칭**: UDP 수신기가 넘긴 pack의 byte field는 datagram 전체 buffer를 가리킬 수 있다(protocol `AdoptInput`, capacity를 잘라 두므로 밑 buffer 크기는 알 수 없다). 그래서 queue에 넣는 Stack `Data`, Span `Spans`, Batch `Extra`는 enqueue 전에 복사한다(`bytes.Clone`). cap 기준 계상은 밑 buffer를 볼 수 없고, 한 datagram의 여러 pack이 같은 buffer를 각자 세게 되어 쓸 수 없다. 복사는 어차피 디스크에 쓸 payload를 한 번 더 복사하는 비용이고, 이 pack들은 XLog·counter보다 드물다. Value 안 Blob(status·alert tag 등의 드문 경우)은 복사하지 않아 datagram을 붙잡을 수 있다(남은 위험).
- Stack·Visitor는 처리 시점(Go는 lock 안 accept)에 live object가 있어야 한다. 원본은 없으면 조용히 버린다. Go는 `Stats.SkippedStacks`와 `Stats.VisitorsSkipped`로 센다. Batch는 모르는 object를 accept 경로에서 등록한다(이전과 같다).
- visitor: HLL offer와 그에 필요한 HLL load/save(`PutVisitorContext`), Tick의 visitor roll(`storage.VisitorRollContext`, visitor lock만)이 모두 visitor queue 하나로 lock 안 accept 순서대로 들어간다. 그래서 roll이 그 tick의 now로 앞선 방문 뒤, 뒤따르는 방문 앞에서 실행된다(늦은 now가 더 새 방문의 시간대를 되돌리지 않는다). 이전 `VisitorSyncContext`의 다른 절반인 tag count 표시(`storage.TagTickContext`)는 Store gate가 필요하므로 visitor writer에 두지 않고(gate를 오래 기다리면 뒤의 방문이 밀린다) `Tick`이 engine lock을 놓은 뒤 부른다. 시각만 비교해 표시하므로 순서와 무관하다. 그 오류(store 종료 등)는 `Tick`이 이전처럼 돌려준다. visitor 조회(`VisitorRealtime` 등)는 storage `visitorMu` 아래 메모리를 읽으므로 writer와 동시에 안전하다(`TestVisitorQueueOrderAndConcurrentQueries`, -race). 10초 snapshot 저장은 이전처럼 runtime `FlushVisitorsIfDue`가 한다. 방문은 하나씩 queue에 넣지 않고 engine 안에서 256개(`visitBatch`)씩 모아 한 item으로 넣는다. 하나씩 넣으면 XLog마다 channel send와 writer 깨우기가 생겨 XLog 전용 30,000/s에서 서버 CPU가 4.8% 늘었다(VM 8쌍 전부 증가). batch는 가득 찼을 때, 각 Tick의 roll 직전(그래서 roll의 순서 위치가 유지된다), `Flush`와 `Close`에서 넣는다. 그래서 실시간 visitor 수는 최대 한 tick(1초) 늦게 반영된다. 원본 VisitorCore도 queue를 거친다.
- visitor batch 집계: batch의 방문을 모두 시도한다. 방문 하나는 objType·object·hourly offer가 모두 성공해야 `Written`이고, 하나라도 거절되면 입력 오류가 있으면 `WriteInvalid`, 없으면 `WriteErrors`로 센다(성공한 offer는 HLL에 이미 반영됨). write-through 실패는 방문 오류가 아니라 다음 flush pass의 오류로 보고한다([storage CONTRACT](../storage/CONTRACT.md) §8.1).

### 가시성

원본과 같다. 실시간 조회는 메모리를 읽으므로 accept 직후 보인다(counter cache, alert ring, status cache, live object, XLog ring). 과거·daily·summary·status·alert 이력·stack·batch·span·object 날짜 조회와 visitor는 writer가 처리한 뒤 보인다. 테스트와 내부 경로는 `Engine.Flush(ctx)`(barrier: 호출 전에 enqueue된 모든 kind의 record가 쓰이거나 실패로 집계될 때까지 기다림)를 쓴다.

- runtime: 주기 `SyncData`/`counters.Sync` 전, 보존 기간 purge 전에 최대 5초(`ingestFlushWait`) `Flush`한다. 시간이 지나면 기다리지 않고 진행한다(record는 queue에 남아 다음 sync에 반영).
- `DeleteDatabasePaths`(SERVER_DB_DELETE)는 삭제 전에 `Flush`한다. 삭제 전에 받은 record가 삭제 뒤 파일을 다시 만들지 않게 하려는 것이다(동기 쓰기 때와 같은 의미, 삭제 뒤 받은 record는 이전처럼 파일을 새로 만든다).
- `RemoveObjects(permanent)`는 engine lock 안에서 `RemoveObject` 묶음을 object queue에 넣고 live map에서 지운 뒤 바로 반환한다(원본도 `objMap.remove` 뒤 `ObjectWR.remove`를 queue에 넣는다). FIFO라 앞서 queue에 든 object 쓰기 뒤에 실행되어 지운 record가 다시 써지지 않는다. enqueue가 거절되면(포화·종료) 오류를 돌려주고 아무것도 바꾸지 않는다. 삭제 실패는 object writer의 `WriteErrors`로 센다(이전에는 명령 오류로 돌려줬다: 의도된 차이, 원본은 로그만).
- `Flush`와 `Close`는 engine lock 밖에서만 부른다. daily writer가 group 저장 실패를 되돌릴 때 engine lock을 잡기 때문이다.

### 포화와 오류

- enqueue는 기다리지 않는다. 개수나 byte 상한에 걸리면 새 record를 버리고 kind별 `Dropped`로 센다. 10초에 한 줄 `ingest: <kind> writer queue exceeded!! (<원본 id>; N records dropped so far)`를 남긴다(원본 `RequestQueue.put` + `Logger.println(id, 10, ...)`). 수집 결과는 성공이다(`Completed`, `Rejected` 아님). XLog 수집과 메모리 상태는 영향받지 않는다.
- writer는 record마다 오류를 `WriteErrors`로 세고 10초에 한 줄 로그한 뒤 다음 record로 넘어간다. **의도된 차이(원본 결함 D1)**: 원본 StackAnalyzerDB·BatchDB writer는 IOException 하나에 thread가 끝나 그 뒤 record를 영영 쓰지 않는다. Go는 계속 돈다.
- **입력 오류와 I/O 오류 구분**: storage가 record 내용 때문에 거절한 쓰기(`ErrInvalid`·`ErrLimit`·`ErrUnsupported`: daily counter의 지원하지 않는 값 type, 4KiB를 넘는 object slot, record 크기 상한 등)는 `WriteInvalid`로 따로 세고 로그도 따로 rate-limit한다(`refused a record`). 회차 item에서 첫 실패가 입력 오류면 그 1건이 `WriteInvalid`, 시도하지 않은 나머지는 `WriteErrors`다. 그 밖(디스크 I/O, 닫힌 storage, 취소)은 `WriteErrors`. realtime counter 이름 상한은 오류가 아니라 부분 저장이라 여기에 들지 않는다(storage `NamesLimited`).
- accept에서 미리 거절하는 입력(이전 동기 쓰기와 같은 결과): counter data가 MapValue가 아니면 `ErrInvalid`, timetype이 1~6이 아니면(daily 파일 layout 없음) `ErrUnsupported`. 값 type은 entry마다 달라 원본처럼 writer에서 판정한다.
- 이전 Go와 달라진 점: 위 쓰기 실패가 더는 `Stats.Rejected`/`StorageErrors`에 들어가지 않는다(수집은 성공했고 쓰기는 writer가 센다).
- **쓰지 못한 object 재기록(원본보다 안전한 차이)**: object 등록의 `ObjectWR.add`가 I/O로 실패하거나 queue에 들어가지 못하면 그 등록에 `unsaved`를 표시한다. 같은 날 변경 없는 다음 heartbeat는 원래 쓰지 않는 fast path지만, 표시가 있으면 현재 object를 한 번 다시 queue에 넣는다(변경 횟수 `updated`·`MAYBE_OBJ_DUP`에는 영향 없음). 원본은 재시도하지 않아 그날 object 파일에서 빠진다. 입력 오류(`WriteInvalid`)는 매 heartbeat 같은 실패가 반복되므로 재기록하지 않는다(`TestUnsavedObjectRewrittenByNextHeartbeat`).
- enqueue 계수: `Enqueued`는 channel send 전에(queue lock 안) 올리고 실패하면 되돌린다. 그래서 snapshot에서 `Written`이 `Enqueued`보다 커 보이는 순간이 없다(`WriterStats`는 결과 counter를 먼저, `Enqueued`를 나중에 읽는다). Close 뒤 enqueue는 `Dropped`로 세되 "queue exceeded" 로그는 남기지 않는다(종료 로그가 writer 집계를 보고한다).

### 종료

- `Engine.Close`: 새 enqueue를 막고(`closed`) writer에 종료를 알린 뒤 **최대 5초**(`writerCloseDeadline`) 동안 남은 queue를 쓴다. 5초를 넘기면 진행 중 쓰기의 context를 취소하고(그 record는 `WriteErrors`) 나머지를 쓰지 않고 `Abandoned`로 센 뒤 로그한다. 그 뒤 writer goroutine이 끝나기를 최대 1초(`writerStuckGrace`) 더 기다린다. context를 보지 않는 호출(멈춘 디스크)에 갇힌 writer는 남겨 두고 `ingest: <kind> writer still in a write ... N records left pending` 줄을 남긴다. 그 kind의 남은 record는 `Pending`으로 남는다. 호출이 나중에 돌아오면 진행 중 record는 취소된 context로 실패(`WriteErrors`)하고 나머지는 `Abandoned`로 센다(`TestCloseDeadlineLeavesAStuckWriter`, 수정 전에는 무한 대기). runtime은 `Close` 뒤 `Pending`이 남아 있으면 `Store.SkipCleanMarker()`로 `.go-clean`을 쓰지 않는다. `--shutdown-timeout 0`이어도 같다(`TestStuckWriterStillClosesStore`). 불변식은 "writer의 파일 I/O는 모두 counters·store가 `Close`에서 잡는 gate 아래에서만 한다"이다. counter는 `Counters.mu`, 나머지는 Store gate와 `visitorMu`다. 그래서 gate를 쥔 채 갇힌 쓰기는 `Close`를 멈추게 하고(프로세스 상한이 처리), gate 밖에서 갇혔다 돌아온 쓰기는 닫힌 storage에서 `ErrClosed`로 실패한다. 이 불변식을 깨는 writer(gate 없이 파일을 쓰는 것)를 추가하지 않는다. 프로세스 전체 상한은 main `--shutdown-timeout`이 맡는다(S6, [runtime](../../docs/compatibility/runtime.md) "종료 상한"). 정상적으로는 모든 writer goroutine이 끝난 뒤 반환하므로 runtime은 그다음에 counters·store를 닫는다(`Runtime.Close` 순서: listener → engine → sqltable → pulse → counters → store). 5초는 runtime이 SQL table worker에 주는 상한과 같다. 원본은 종료 때 queue를 비우지 않고 버린다(visitor 최대 10초분, AlertSummary 최대 5분분 미flush). Go는 상한 안에서 비우는 쪽이 더 많이 남긴다(의도된 차이). 단, 5분 alert summary와 5분 sample은 아직 회차가 오지 않았으면 이전처럼 쓰지 않는다.
- 모든 writer 쓰기는 writer context를 storage에 넘긴다. object도 `storage.PutObjectContext`(Store gate를 context로 기다림, 이번에 추가)를 쓰므로 deadline 뒤 gate를 기다리던 쓰기는 끝난다. gate를 이미 잡고 디스크에서 멈춘 syscall은 끊을 수 없어 Close는 그 한 건이 끝날 때까지 기다린다. writer가 storage 안에 있는 채로 store를 닫지 않기 위해서다.
- 종료 로그: runtime이 `ingest writers closed: [...]`(kind별 집계)를 남긴다.

### 집계(`Stats`, `WriterStats`, `/health`, 콘솔)

- kind별 `WriterStats{Kind, Enqueued, Written, WriteErrors, WriteInvalid, Dropped, Abandoned, Pending}`. `Enqueued = Written + WriteErrors + WriteInvalid + Abandoned + Pending`이고 `Dropped`는 enqueue되지 않은 record다. 회차 item은 record 수로 센다.
- `Stats`(로그 `ingest=` 줄과 `/health`의 `ingest`)에는 kind 합계를 평면 scalar로 둔다: `WriteEnqueued`, `WriteWritten`, `WriteErrors`, `WriteInvalid`, `WriteDropped`, `WriteAbandoned`, `WritePending`. `ingest=` 줄 파서(`scripts/java-integration.py`)가 중괄호 없는 scalar를 기대하기 때문이다. 그리고 `VisitorsSkipped`. `scripts/java-integration.py`는 최종 줄의 `WriteErrors`·`WriteInvalid`·`WriteDropped`·`WriteAbandoned`·`VisitorsSkipped`·`Sample5MDropped`가 0인지, `scripts/bench-xlog-max.py`는 `/health`의 `Write*` 오류가 0인지 본다. `WritePending`은 최종 줄이 Close의 drain 전에 찍히므로 검사하지 않는다.
- kind별 값: `/health`의 `ingestWriters`, 종료 stats 줄 끝의 `ingestWriters=[...]`, `Runtime.ServerStat()`의 `ingest.writer.<kind>.queue`, 콘솔 `debug queue`의 `ingest.writer.<kind>.*`.

### 검증

`writer_test.go`: `TestStalledWritersDoNotStallIngestOrMemory`(writer를 막아도 XLog 수집이 계속되고 counter cache·alert ring·status·live object가 즉시 보임, 풀리면 파일에 저장), `TestFailedWritesKeepMemoryAndAreCounted`(저장소를 닫아 쓰기 실패 → 메모리 갱신·kind별 오류 집계, `Rejected` 0), `TestFullWriterQueueDropsAndCounts`(개수·byte 포화 drop 집계, XLog 무영향), `TestDailyCounterFIFOLastQueuedValueWins`(FIFO, 같은 daily bucket은 마지막 enqueue 값), `TestStackAndVisitWithoutObjectAreCounted`, `TestQueuedRecordsKeepTheirDayAcrossMidnight`, `TestCloseDrainsQueuedRecords`, `TestCloseDeadlineAbandonsTheRest`, `TestVisitorQueueOrderAndConcurrentQueries`, `TestWritersConcurrentWithIngest`(-race), `TestPermanentRemovalFollowsQueuedObjectWrite`(writer가 막힌 상태를 hook으로 확인, 명령이 기다리지 않고 수집도 계속됨), `TestPassesShareTickWithoutByteDrop`(같은 tick 회차 충돌, 생산자당 회차 1개, `WriteInvalid`, 미지원 timetype 거절), `TestUnsavedObjectRewrittenByNextHeartbeat`. storage: `TestPutObjectContextWaitsForGateWithContext`. 파일 bytes 의미는 storage 함수가 그대로라 바뀌지 않는다(Java oracle·storage interop 테스트 그대로 통과).

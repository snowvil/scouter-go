# internal/counters 계약

정본은 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `scouter.common/src/main/java/scouter/lang/counters/CounterEngine.java`, `scouter/lang/{Counter,Family,ObjectType}.java`, `scouter.server/src/main/java/scouter/server/CounterManager.java`, `scouter.server/src/main/scala/scouter/server/util/XmlUtil.scala`다. 분석 근거는 `docs/compatibility/management-commands.md` §4다.

## 범위

- `Engine`: `CounterEngine` 모델. `Parse`는 Java `parse(byte[])`가 `false`를 반환하는 입력(= DOM parse 예외)에 `ErrMalformed`를 반환한다. 성공 시 `StringKeyLinkedMap.put(MODE.LAST)`처럼 같은 이름 Family는 **위치 유지·값 교체**, 같은 이름 ObjectType은 **첫 객체 유지 + Counter 추가**다. `Family`/`ObjectType`/`Counter` element는 문서 전체 preorder(`getElementsByTagName`), Counter는 해당 element의 모든 하위(직계 아님)에서 찾는다. ObjectType의 `family`는 그 시점 familyMap 조회 결과이며 없으면 nil이다. `all`/`total`/`sub-object`는 `Boolean.valueOf`(대소문자 무시 `true`만 true), Counter `all`/`total` 기본값은 true다.
- `Store`: `CounterManager`의 custom 파일 소유자. 파일명은 원본 상수 `counters.site.xml`(`CUSTOM_FILENAME`)이며 `StoreConfig.Dir`(Java `Configure.CONF_DIR`) 아래에 있다. 
  - `Save`(SET_CONFIGURE_COUNTERS_SITE): 유효성(`ErrMalformed` → `false`) → Go 정규화(아래) → 저장 → default+custom engine 재구성. Java JAXB가 `JAXBException`을 던지는 입력(root가 `{}Counters`가 아님, 미선언 prefix, root 기본 namespace, 선행 U+FEFF)은 Java처럼 **원문 그대로** 저장한다. 빈 `<Types/>`는 허용한다(Java NPE·응답 없음, 결정 §9-3).
  - `DefineObjectType`(DEFINE_OBJECT_TYPE): engine에 이름이 있으면 `false`, family 미존재 → `false`(Java `setObjectTypeAttribute` NPE를 `appendObjectType`이 잡음), custom 파일 파싱 실패 → `false`(`getCustomDocument` null), 다른 document element만 있는 문서 → `false`(DOMException). 그 외 첫 `Counters`(문서 전체) 아래 첫 `Types`(문서 전체, 없으면 생성)에 `ObjectType`을 append하고 attribute를 `name, disp, family, icon, sub-object` 순으로 설정한 뒤 정규화·저장·재구성한다.
  - `EditObjectType`(EDIT_OBJECT_TYPE): 같은 `name` attribute를 가진 첫 ObjectType의 attribute를 교체하고(기존 하위 Counter와 unknown attribute 유지) 없으면 append한다. **원본은 편집 결과를 버리고 파일을 그대로 다시 읽어 true만 반환한다(결함 §9-2). Go는 DEFINE과 같은 저장 경로로 실제 기록한다(결정 §9-2).** family 미존재는 Java와 같이 `false`.
  - 단일 소유자: DEFINE/EDIT/SET 모두 `Store` mutex 아래에서 **매번 디스크의 최신 custom 문서**를 기준으로 갱신한다(Java Telegraf `safely*`의 기동 시 캐시 `unsafeDoc` 덮어쓰기 결함 없음, §9-3).
  - `CustomXML()`: `xmlCustomContent` — Open 시 읽은 bytes 또는 마지막 성공 저장 bytes, 파일이 없으면 nil. `GET_XML_COUNTER`의 `custom` BlobValue용(service.go wiring은 이 패키지 범위 밖). 외부 편집은 Java처럼 재시작 전까지 반영하지 않는다.
  - `Text()`: `readCountersSiteXml` — 매 호출 파일을 읽고 없으면 `""`, 잘못된 UTF-8 byte는 U+FFFD.
  - engine은 새로 만든 뒤 통째로 교체한다(Java `clear()` 후 parse의 빈 engine 노출 창 없음). 재구성 실패 시 이전 engine을 유지하고 `false`를 반환한다(파일은 이미 저장됨, Java와 동일 순서).
- `WriteFile`/`ReadFile`: 같은 디렉터리의 임시 파일(`O_EXCL|O_NOFOLLOW`) 작성 → `fsync` → `os.Root.Rename` → 디렉터리 `fsync`. 대상 이름은 단일 path element만 허용하고 기존 symlink/비정규 파일은 `ErrUnsafeTarget`로 거절한다. 기존 파일 권한을 유지하며 새 파일은 호출자 권한(서버 설정 0600, 그 외 0644)이다. 디렉터리가 없으면 Java `mkdirs`처럼 만든다. 읽기는 `os.Root` + `Lstat` + `O_NOFOLLOW`, 크기 상한 초과는 `ErrLimit`.

## HTTP 입력용 등록 연산 (telegraf.TypeRegistrar 의미)

`Store`는 counters 자체 타입(`FamilySpec`, `ObjectTypeSpec{…, Counters}`, `CounterSpec`)으로 등록 연산을 제공하고, `registrar.go`의 `NewRegistrar(store *Store, notify func()) *Registrar`가 이를 `telegraf.TypeRegistrar`로 감싼다(counters → telegraf 단방향 import). 모든 연산은 `Store` mutex 아래에서 디스크의 최신 custom 문서를 읽어 수정 → 정규화 → 저장 → engine 재구성한다.

`NotifyClients`는 `RegisterHandler.notifyAllClients`(`AsyncRun`)처럼 호출자를 막지 않는다. `notify`가 nil이면 no-op이고, 아니면 최대 1개의 background goroutine이 요청을 합쳐(coalesce) 호출한다. 호출 중 들어온 요청은 끝난 뒤 한 번 더 호출되며 대기 요청이 없으면 goroutine은 종료한다. Java는 호출마다 별도 알림을 queue(128, 포화 시 drop)에 넣지만 Client는 어차피 전체 XML을 다시 받으므로 합쳐도 결과는 같다.

| 메서드 | 원본 | Go 동작 / 의도된 차이 |
|---|---|---|
| `ObjectTypeSpec(name)` | `CounterEngine.getObjectType` | 자체 counter 먼저, 그다음 가려지지 않은 family counter(`ObjectType.getCounter` 탐색 순서) |
| `HasFamily(name)` | `getFamily != null` | 동일 |
| `AddFamilyAndObjectType(f, t)` | `addFamilyAndObjectType`: DOM 결과를 버려 **디스크·engine 무변경인데 true**(§9-2) | 실제 기록. `<Family>`/`<ObjectType>` append 후 정규화가 같은 name을 마지막 항목으로 정리 |
| `EditObjectTypeCounters(t)` | `editObjectType(ObjectType)`: 결과 버림(§9-2) | 실제 기록. attribute 교체, `t.Counters`를 자체 `<Counter>`로 추가하되 같은 name은 제자리 교체(Java는 중복 append). family 미존재는 오류(Java NPE) |
| `SafelyAddFamily(f)` | `safelyAddFamily`: 기동 후 처음 읽은 캐시 문서(`unsafeDoc`) 기준 같은 name `<Family>`를 **통째로 교체** → 같은 family를 쓰는 다른 objType이 등록한 counter가 사라짐(http-contract Q2). **custom 파일이 없는 새 설치에서는 NPE로 항상 실패**(원본 실행으로 확인) | **병합**: engine의 현재 family(default 또는 custom)의 counter와 master를 유지하고 `f.Counters`를 추가/제자리 교체, master는 기존 값이 없을 때만 `f.Master`. 최신 파일 기준, 파일이 없으면 빈 문서를 만들어 정상 등록 |
| `SafelyAddObjectType(t)` | `safelyAddObjectType`: 같은 name `<ObjectType>` 교체(자체 counter 소실) | attribute만 제자리 갱신하고 기존 자체 `<Counter>` 유지, `t.Counters` 추가/교체. 없으면 append |
| `AddFamilyCounter(family, c)` | `objectType.getFamily().addCounter(c)` + `safelyAddFamily` | engine의 family(default 또는 custom) 복사본에 counter를 추가/제자리 교체해 family 전체를 custom에 기록. Java처럼 unknown family attribute는 옮기지 않음. family 미존재는 오류 |

`<Counter>` attribute는 원본 `setAttribute` 순서 `name, disp, unit, icon, all, total`("true"/"false"), Family는 `name, master`, ObjectType은 `name, disp, family, icon, sub-object`다. 결과 파일은 `scripts/java-management.py --verify-xml`(원본 `CounterEngine.parse`)로 parse=true와 objectType/family 연결을 확인했다(2026-10-15, `TestRegistrarOperations`와 같은 시나리오의 출력).

## 정규화 직렬화 (Go 레이아웃, 결정 §9-3)

원본 `normalizeCountersXml`(JAXB)의 골격 — 첫 줄 `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`, 4-space indent, `<Types>` 다음 `<Familys>`, 빈 목록은 `<Types/>`/`<Familys/>`, root 직계 `Types`/`Familys`의 직계 `ObjectType`/`Family`와 그 직계 `Counter`만 읽음(namespace가 있는 element 무시), `zws-metric.`/`zps-metric.` Family 제거, 같은 name은 마지막 항목의 내용 — 은 유지한다. 다음은 Java JAXB 출력과의 **의도된 바이트 차이**다.

| 항목 | Java JAXB | Go |
|---|---|---|
| 순서 | `HashMap.values()`(name hash 순) | 입력 순서. 중복 name은 처음 나온 위치에 마지막 내용 |
| ObjectType 하위 `<Counter>` | 삭제 | 보존 |
| unknown attribute(Family/ObjectType/Counter) | 삭제 | 보존(입력 순서) |
| attribute 순서·기본값 | 선언 순서 + 없던 attribute를 `""`(Counter `all="true"`)로 출력 | 입력에 있는 attribute만 입력 순서로 출력. 따라서 Counter `total` 미지정이 CounterEngine 기본값 true로 유지된다(JAXB의 `total=""`은 false로 바뀜) |
| `<Types/>`(ObjectType 없음) | NullPointerException → 응답 없음 | 허용 |
| `<Types>`/`<Familys>` 복수 | 마지막 하나만 | 모두 합침 |
| TAB(문자참조) | literal TAB 출력 → 재파싱 시 공백 | `&#9;` |
| namespace 선언·prefix attribute | (namespace-aware 무시) | 출력하지 않음 |

attribute escape: `&`→`&amp;`, `<`→`&lt;`, `>`→`&gt;`, `"`→`&quot;`, LF→`&#10;`, CR→`&#13;`(JAXB 관찰 결과와 동일), TAB→`&#9;`(Go 추가). 입력 attribute 값은 XML 1.0 §3.3.3대로 literal TAB/LF/CR(CRLF는 하나)을 공백으로 바꾸고 `&#9;`/`&#10;`/`&#13;`(16진 포함) 문자참조는 보존한다. Go decoder가 참조를 먼저 풀기 때문에 U+FDD0–U+FDD2 비문자 placeholder로 치환했다 복원하며, 이 문자가 원문에 있으면 `ErrUnsupportedDocument`로 거절한다. 정규화는 멱등이다.

## 유효성 판정 (Java DocumentBuilder와 동일하게 맞춘 것 / 다른 것)

동일: 짝이 맞지 않는 태그, 미종결, root 없음/복수, root 밖 비공백 문자, 중복 attribute, 문서 중간 `<?xml`, 미정의 entity, attribute 값의 `<`/`&` 단독, 금지 제어 문자, 빈 입력은 모두 `ErrMalformed`(Java `false`). 주석·PI·CDATA·UTF-8 BOM 허용. namespace prefix는 DOM에서 검사하지 않는다(미선언 prefix도 parse 성공).

의도된/알려진 차이:
1. `<!DOCTYPE ...>`가 있는 문서는 거절한다. Java 기본 DocumentBuilder는 DTD를 처리한다. oracle `doctype`은 Java true / Go `ErrMalformed`로 고정했다.
2. `a:b:c`처럼 콜론이 둘 이상인 이름은 Go decoder가 거절한다(Java non-namespace DOM은 허용). oracle `bad_colon_name`.
3. XML 선언 encoding은 UTF-8, ISO-8859-1/latin1, US-ASCII만 지원한다. 다른 encoding(예: EUC-KR) 선언은 `ErrMalformed`로 Java보다 좁다.
4. 문서 상한: 기본 4 MiB, depth 256, element 200,000(`ErrLimit`). Java에는 상한이 없다.
5. DEFINE/EDIT에서 custom 문서의 document element가 `Counters`가 아니거나(`<Other><Counters/></Other>`) namespace 오류가 있으면 Java는 Xalan 출력 원문을 저장하지만 Go는 Xalan 직렬화를 재현하지 않으므로 `ErrUnsupportedDocument`(명시적 실패)다.
5a. Java `addObjectType`의 JAXB 정규화가 custom 문서의 ObjectType 하위 Counter를 지우는 것과 달리 Go DEFINE/EDIT는 기존 내용을 보존한다(§9-3).
6. DEFINE/EDIT에서 `name`이 null(비Text)이면 service는 `false`를 반환한다(Java는 Xerces `setAttribute(…, null)` 경로로 비결정적). null `disp/family/icon`은 빈 문자열로 취급한다.
7. 쓰기 대상이 symlink/비정규 파일이면 거절한다. `Store`는 이를 오류로 반환하고, service의 SET_CONFIGURE_COUNTERS_SITE는 Java `saveText` 실패처럼 `"false"`로 응답한다(로그 기록). DEFINE/EDIT는 오류(응답 없음)다.
8. 동시성: 모든 변경은 `Store` mutex로 직렬화된다. Java `addObjectType(MapPack)`의 존재 검사는 synchronized 밖에서 일어나 경쟁 시 중복 append가 가능하다.

## 검증 근거와 재생성

- `go test -race ./internal/counters/...`:
  - parse 판정: oracle 45개 입력(+`cmd/scouter-go/counters.xml`)의 Java `CounterEngine.parse` 결과와 정확히 일치(의도된 차이 2개: `doctype`, `bad_colon_name`).
  - 정규화 의미 동등성: Java JAXB 출력의 모든 ObjectType/Family/Counter와 attribute 값(JAXB 기본값 포함)이 Go 출력에 있다(`compareWithJAXB`). JAXBException 입력은 원문 저장, NPE 입력은 Go 허용.
  - Go 바이트 golden(`TestNormalizeGoLayout`)과 멱등성, Counter `total` 기본값 보존.
  - DEFINE/EDIT 5개 체인(Java DOM append → Xalan → JAXB, `testdata/define`)과 의미 동등 + site.xml의 ObjectType 하위 Counter·unknown attribute 보존.
  - store/파일 안전성/동시성.
- Go 출력의 Java 판독성: oracle 입력 중 parse 가능한 29개의 Go 정규화 출력을 Java `CounterEngine.parse`에 넣어 29개 모두 `true`(일회성 실행, 2026-10-15). 재현은 Go 출력을 파일로 저장한 뒤 아래 `CountersOracle`에 전달한다.
- oracle 생성(읽기 전용 원본 사용, 임시 디렉터리): `testdata/oracle-src/CountersOracle.java`(CounterEngine는 `-sourcepath <scouter>/scouter.common/src/main/java`로 원본 컴파일, normalize와 JAXB 클래스는 CounterManager에서 verbatim 복사)와 `DefineOracle.java`(DOM append/edit + Xalan `indent-amount=2` + normalize).
  ```
  CP=jaxb-api-2.3.1.jar:jaxb-runtime-2.3.2.jar:txw2-2.3.2.jar:istack-commons-runtime-3.0.8.jar:jakarta.activation-api-1.2.2.jar
  javac -d classes -cp $CP -sourcepath $SCOUTER_SOURCE/scouter.common/src/main/java CountersOracle.java DefineOracle.java
  java -cp classes:$CP CountersOracle case.xml ...           # case.xml.parse, case.xml.norm
  java -cp classes:$CP DefineOracle <custom|-> name disp family icon sub edit out
  ```
  원본 pom의 `jaxb-impl 2.3.0.1`은 JDK 18에서 실행되지 않아 `jaxb-runtime 2.3.2`를 썼다(분석 문서 §4.3: JDK 8 + 원본 jaxb-impl 출력과 byte 동일 확인).
- 미검증: Scala `ConfigureService` 실제 TCP 실행, Java Client GUI 상호운용, `REFETCH_COUNTER_XML` 통지(아래).

## 남은 연결 지점

- `SET_CONFIGURE_COUNTERS_SITE` 성공 후 Java `RegisterHandler.notifyAllClients()`는 모든 로그인 session에 `REFETCH_COUNTER_XML` remote control을 넣는다. 이 queue는 manage 명령 소유이므로 service는 `FileMetadataConfig.Reload(ctx, "counters-site")` callback만 호출한다. 통지 연결은 runtime 배선(`internal/server`)이 맡는다.
- `GET_XML_COUNTER`가 `Store.CustomXML()`을 `custom` BlobValue로 보내도록 하는 `service.go` 연결과, runtime의 object type 조회가 merged engine을 쓰도록 하는 연결은 이 패키지 범위 밖(`internal/service`와 runtime 배선)이다.

# KV 및 Query list 조회 계약

정본은 `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `KeyValueStoreService.scala`, `DatabaseService.scala`, `KeyValueStoreRW.scala`, `kv/KeyValueStoreIndex.scala`, `io/RealKeyFile2.scala`, `io/IndexKeyFile2.scala`, `StatusRD.scala`, `StatusPack.java`다.

| 명령 | 요청 | 응답 |
| --- | --- | --- |
| `GET_GLOBAL_KV` | Pack이 아닌 `Value` 하나. 정상 Client는 `TextValue(key)` | `HasNEXT + TextValue(value)` 한 개. missing은 empty Text |
| `GET_CUSTOM_KV` | `MapPack(keySpace:Text, key:Text)` | 동일. key가 Null/없으면 응답 항목 없음 |
| `GET_GLOBAL_KV_BULK` | `MapPack(key:ListValue)` | 존재하는 key만 `MapPack` 한 개. key List가 없거나 비면 항목 없음 |
| `GET_CUSTOM_KV_BULK` | `MapPack(keySpace:Text, key:ListValue)` | 동일 |
| `EXIST_QUERY_LIST` | `MapPack(date:Text, objHash:Decimal, stime:Decimal, etime:Decimal)` | `MapPack(time:ListValue<Decimal>)` 한 개. 결과가 없어도 empty List 반환 |
| `LOAD_QUERY_LIST` | 동일 | matching `StatusPack.data`만 복사한 `MapPack`을 각각 반환. metadata는 추가하지 않음 |

Go dispatcher는 인증된 Client만 처리한다. optional `LocalStoreBackend`가 없으면 `ErrUnsupported`로 실패한다. Store 결과는 owned이며 context와 조회 record/byte/time bound를 유지한다. bulk 응답은 key 순서대로 조회하고 missing을 생략하며 duplicate는 최초 삽입 위치에 최종값을 교체한다. 원본 `LinkedHashMap` 순서다. Go key 변환은 모든 wire Value 타입의 원본 `toString`을 처리한다. Float는 Java 8~18 digit/rounding/32·64 bit overflow 동작을 유지한다. Double과 DoubleSummary는 `DecimalFormat("#0.0#################")`의 고정 표기, 18개 이하 소수, HALF_EVEN, signed zero와 infinity symbol을 사용한다. List는 comma, typed array는 comma+space를 사용하며 Blob은 `byte[length]`, TextHash는 signed hexadecimal, IP4는 dotted decimal이다. Map은 default Hashtable(초기 capacity 11, load factor .75)의 wire insertion/rehash/bucket-chain 순서를 재구성한다.

## KV 저장

`00000000/kv2/kv_<keySpace>`를 base로 `.hfile`, `.k2file`, `.kvdata`를 사용한다. global keySpace는 `__SCOUTER_KV_GLOBAL__`다. `.kvdata`는 signed big-endian int32 byte length와 UTF-8 원문이다. index payload는 signed long5 data offset이다. 일반 `.kfile`은 TTL을 담지 못하므로 공유 `legacyindex.TTLIndex`의 `RealKeyFile2` codec을 사용한다. Store는 TTL index를 최대 16개 보관하고 eviction 및 Sync/Close에서 flush/close한다.

`RealKeyFile2` record는 `deleted:boolean + expire:long5 + previous:long5 + key:ShortBytes + dataOffset:Blob`다. expiry는 epoch 초다. `expire < nowSeconds`에서만 만료하므로 동일 초는 live다. TTL은 초 단위이고 음수면 원본 `DataInputX.LONG5_MAX_VALUE`(`549755813632`) sentinel을 쓴다. deleted/expired latest matching record가 이전값을 가리더라도 과거값으로 fallback하지 않는다. delete와 TTL 갱신은 latest matching record만 대상으로 한다.

write API는 `PutKeyValueContext`, `SetKeyValueTTLContext`, `DeleteKeyValueContext`다. network SET/TTL 6종은 아래 "KV 쓰기 명령" 절의 `LocalStoreWriter`로 연결하며 DELETE network command는 원본에도 없다. `Put`은 원본 `updateOrPut`처럼 기존 matching record offset을 갱신하고 새 record도 append한다. 원본 `KeyValueStoreRW.setTTL`은 index에서 key를 갱신하지 못해도 namespace open에 성공하면 true를 반환하며 이 wrapper 동작도 유지한다. empty space/key/value write는 false, empty space/key read는 missing이다. UTC 초 now를 명시적으로 받아 경계 검증을 결정적으로 수행한다.

파일명 traversal, 기존 symlink/nonregular file, invalid UTF-8, signed short로 읽을 수 없는 32767 byte 초과 key와 길이/offset/TTL overflow를 거부한다. KV namespace는 최대 245 UTF-8 byte다. 조회가 없던 namespace 파일을 새로 만들지 않는 차이는 명시적이다. 원본처럼 malformed 파일을 묵시적 missing으로 감추지 않고 오류를 반환한다. 기존 DB는 복사본에서 조회/이관하며 기존 Java 원본에 동시 writer를 두지 않는다. 파일 경로 검증 후 외부 프로세스가 경로를 교체하는 경쟁은 지원하지 않는다.

## KV 쓰기 명령 (SET/TTL 6종)

정본은 `scouter.server/src/main/scala/scouter/server/netio/service/handle/KeyValueStoreService.scala`(`dee47fb5`)의 `setText:80`, `setTTL:99`, `setTextBulk:117`, `setTextToCustomStore:158`, `setTtlToCustomStore:178`, `setTextBulkToCustomStore:221`과 `scouter.server/src/main/scala/scouter/server/db/KeyValueStoreRW.scala`의 `set:54`, `setTTL:71`, `open:102`다. 명령 상수는 `scouter.common/src/main/java/scouter/net/RequestCmd.java:177-185`의 `SET_GLOBAL_KV`, `SET_GLOBAL_TTL`, `SET_GLOBAL_KV_BULK`, `SET_CUSTOM_KV`, `SET_CUSTOM_TTL`, `SET_CUSTOM_KV_BULK`이고 parameter 이름은 `ParamConstant.java:31-37`의 `key`, `value`, `keySpace`, `kv`, `ttl`, `TTL_PERMANENT=-1`이다.

| 명령 | 요청 MapPack | 응답 | key/kv 부재 |
| --- | --- | --- | --- |
| `SET_GLOBAL_KV` | `key:Text, value:Text, ttl:Number` | `HasNEXT + BooleanValue(RW.set)` | `key`가 null이면 응답 항목 없음 (`:85`) |
| `SET_GLOBAL_TTL` | `key:Text, ttl:Number` | `HasNEXT + BooleanValue(RW.setTTL)` | 동일 (`:103`) |
| `SET_GLOBAL_KV_BULK` | `ttl:Number, kv:MapValue<Text,Text>` | `HasNEXT + MapPack{key: Boolean}` | `kv`가 null이거나 비면 응답 항목 없음 (`:121`) |
| `SET_CUSTOM_KV` | `keySpace:Text, key:Text, value:Text, ttl:Number` | `BooleanValue` | `key` null이면 없음 (`:164`) |
| `SET_CUSTOM_TTL` | `keySpace:Text, key:Text, ttl:Number` | `BooleanValue` | `key` null이면 없음 (`:183`) |
| `SET_CUSTOM_KV_BULK` | `keySpace:Text, ttl:Number, kv:MapValue` | `MapPack{key: Boolean}` | MapPack 자체가 비면 없음 (`:223`). `kv`가 빈 MapValue면 **빈 MapPack을 응답**한다 |

- 인증: `TcpServiceWorker.scala:117-122`는 free command가 아닌 모든 명령에 유효한 session을 요구하며 `INVALID_SESSION` 후 연결을 끊는다. handler의 `login` 인자는 사용되지 않고 별도 policy/권한 검사는 없다. Go도 인증된 Client만 처리하고 추가 권한을 만들지 않는다.
- 값 해석: `MapPack.getText`(`MapPack.java:120`)는 Text가 아닌 Value를 null로 보므로 Text가 아닌 `key`는 부재와 같고, Text가 아닌 `value`는 `RW.set`의 `StringUtil.isEmpty` 검사로 false다. `getLongDefault`(`:104`)는 `NumberValue`(Decimal/Float/Double)만 읽고 그 외 타입은 `-1`이다. Float/Double은 `(long)` 변환(`FloatValue.java:87`, `DoubleValue.java:88`)이라 `90.9→90`, `-0.5→0`, NaN→0, 범위 밖은 saturate다.
- TTL 의미: 초 단위 relative TTL. `RealKeyFile2.write:165`와 `setTTL:133`은 `ttl < 0`이면 `LONG5_MAX_VALUE`(`549755813632`) sentinel, 아니면 `currentTimeMillis/1000 + ttl`을 기록한다. `ttl=0`은 같은 초까지만 live이고 `-1`이 아닌 음수도 permanent다. `ttl` 부재는 `-1`이다.
- `RW.set:55`는 `text`/`key`/`div`가 null 또는 빈 문자열이면 false, 그 외에는 `.kvdata`에 `int32 length + UTF-8`을 append한 뒤 `IndexKeyFile2.updateOrPut`(`IndexKeyFile2.scala:63`)으로 기존 matching record 전부의 flag/expire/offset을 갱신하고 새 record를 append한 후 true다. `RW.setTTL:71`은 key/div가 비면 false이고, namespace를 열 수 있으면 index가 key를 못 찾거나 만료·삭제 record여도 true다. 존재하지 않는 keySpace에 대한 `SET_*_TTL`도 `open:114`가 `.hfile/.k2file/.kvdata`를 새로 만들고 true를 돌려준다.
- bulk: `EnumerScala.foreach(kv.keys())`(`KeyValueStoreService.scala:125`, `:231`)는 wire로 받은 `MapValue`의 **Hashtable enumeration 순서**로 key마다 `RW.set`을 호출하고 결과 `MapPack`(LinkedHashMap)에 같은 순서로 `Boolean`을 넣는다. 각 key는 독립적으로 false를 받을 수 있고 한 key의 실패가 다음 key를 막지 않는다. wire 중복 key는 Hashtable `put`처럼 최초 위치에 마지막 값이 남는다. `keySpace`가 없는 custom bulk는 모든 key가 false인 MapPack을 응답한다.
- 원본 결함 후보: `setTextBulkToCustomStore:231`은 `kv` null 검사 없이 `kv.keys()`를 호출해 `NullPointerException`을 던지고, 두 bulk 모두 `kv`가 MapValue가 아니면 `asInstanceOf` `ClassCastException`을 던진다. `ServiceHandlingProxy.java:40-52`가 예외를 삼키고 worker가 `NoNEXT`를 쓰므로 Client는 응답 항목 없음으로 본다. Go는 이 세 경우를 `ErrMalformed`로 명시적으로 실패시킨다(쓰기 전 실패, 성공 terminator 없음). 이것이 의도된 차이다.
- Go 구현: `internal/service/local_store_mutations.go`는 optional `LocalStoreWriter`(`PutKeyValue`, `SetKeyValueTTL`)를 통해 `storage.PutKeyValueContext`/`SetKeyValueTTLContext`를 호출하며 backend에 writer가 없으면 `ErrUnsupported`다. `now`는 서비스 Clock의 epoch 초이고 한 명령 안의 모든 key에 같은 now를 쓴다(원본은 record마다 `currentTimeMillis`를 읽는다). key 32767 byte 초과, value `MaxRecordBytes` 초과, 40-bit expire overflow, namespace 경로 검증 실패는 원본처럼 묵시적으로 처리하지 않고 오류다. bulk `kv` entry 수는 `extendedQueryCells`로 제한한다.

### 쓰기 검증

`scripts/java-storage.py --generate-local-store-writes <빈DIR>`는 고정 원본 `KeyValueStoreService`/`KeyValueStoreRW`/`KeyValueStoreIndex`/`KeyValueStoreWriter`/`RealKeyFile2`를 compile해 `LocalStoreOracle writes`를 실행한다. 기존 `--generate-local-store` case는 변경하지 않는다. 45개 case(SET/TTL 36개, READ 재조회 6개, 원본 예외 3개)를 실제 handler로 실행해 request/response bytes(`kv-write-cases.bin`, `<name>-request/response.bin`), 쓰기 전후 epoch 초, 결과 `00000000/kv2` 파일을 남긴다. case는 permanent/relative/0/음수 TTL, Float/Double/Text ttl, 덮어쓰기(updateOrPut), 빈/비Text value, null/비Text/빈 key, keySpace 부재/빈 문자열, Hashtable rehash를 일으키는 10-key bulk, 빈 kv, 새 keySpace에 대한 TTL, 한글 key/value를 포함한다.

`TestLocalStoreActualJavaWriteHandlersAndDB`는 이 oracle을 실행한 뒤 Go Storage-backed 인증 TCP에 42개 request bytes를 같은 순서로 보내 response bytes와 `NoNext` 경계를 byte-exact 비교하고, 원본 예외 3건은 Go가 응답 없이 명시적으로 실패하는지 확인한다. 이어 세 namespace의 `.hfile`/`.kvdata` 전체 bytes와 `.k2file`의 record position/previous link/deleted/key/value를 비교한다. expire 필드만 clock에 따라 달라지므로 sentinel은 동일성을, relative expire는 `Go expire - Go now = ttl`일 때 `Java expire - ttl`이 oracle의 쓰기 전후 초 창 안에 있는지 검증한다(20 record). 마지막으로 Go가 쓴 DB를 다시 열어 READ 재조회 결과와 TTL 만료(bulk 60초, `SET_GLOBAL_TTL` 7200초)를 확인한다. 단위 테스트 `TestLocalStoreMutation*`는 fake writer로 body 유무, Boolean 결과, TTL 기본값/Number 변환, Hashtable 순서, 비Map `kv`·storage 오류·writer 부재의 명시적 실패를 확인한다.

## Query list

기존 Status `.pstat` + timed legacy index를 재사용한다. `StatusRD.readByTime`의 forward/index 순서, inclusive time bounds, 같은 timestamp의 duplicate를 유지한다. `objHash`만 필터링하며 `key`, `objType` 필터는 없다. `StatusPack.toMapPack()`는 `data`의 Hashtable enumeration만 옮긴다. wire insertion에서 Java `MapValue.read`의 Hashtable을 재구성한 enumeration 순서를 사용한다. 충돌하는 key의 순서도 검증한다. Text/List/Decimal/Blob 등의 typed schema를 문자열로 변환하지 않는다. 조회 상한은 필터 결과만 아니라 읽은 모든 Status record에 적용한다.

## 검증 범위

`go test ./internal/service ./internal/storage -run 'TestLocalStore|TestQueryList|TestKeyValue' -count=1`로 Value/Pack body 구분, TCP pipeline, missing/empty/duplicate/order, typed query schema, object filter, expiry 동등초/초과초, delete와 revive, restart, context, byte/count/time/path bound를 확인한다. `go test -race`로 동일 scope를 확인한다. `TestLocalKVJavaFloatingKeys`는 정본 SHA/원본 common 무변경을 확인하고 actual `FloatValue`/`DoubleValue`/other Value를 임시 경로에서 컴파일한다. Java 18.0.2 실행으로 edge + 고정 seed 10,000 Float/10,000 Double bit pattern, compound/Unicode/Hashtable resize key를 비교해 pass를 확인했다. 추가 Java 8 실행은 finite/Infinity/compound 순서를 검증하고 Double NaN symbol의 실제 차이를 별도 확인한다. `TestLocalStoreActualJavaHandlersAndDB`는 actual pinned 4 KV/2 Database handler, 26 Value key 및 empty bulk/custom absent-key fixture의 raw response를 Go Storage-backed 인증 TCP에서 byte-exact 비교하고 NoNext 경계를 확인했다. original permanent KV seed를 Go write API로 replay하여 global/custom `.hfile`/`.k2file`/`.kvdata` 전체 byte-exact 비교도 pass했다. race를 포함한 scoped 명령은 pass이며 전체 저장소 gate는 통합 검증 결과를 따른다. 실제 Scouter Client 상호운용, 기존 DB의 대량 이관/rollback 및 운영 검증은 이 단위 테스트만으로 확인되지 않는다. TPS나 운영 성능을 주장하지 않는다.

## JVM 표현 환경의 경계

원본 Scouter SHA만으로 JVM의 `Float.toString` 구현 버전과 `DecimalFormat` default locale는 고정되지 않는다. 다른 JVM의 Float decimal algorithm이나 comma decimal locale는 같은 Value wire bytes에도 다른 KV key를 만들 수 있다. actual Java 8/18 oracle에서 Double NaN symbol은 각각 `�`와 `NaN`으로 달랐다. 현재 원본 oracle의 Java 18 및 Korean/English처럼 decimal point `.`인 환경을 baseline으로 사용한다. Java 8 NaN을 동일한 key로 취급한다고 주장하지 않는다. Float 및 finite Double/Infinity는 Java 8/18 비교를 통과했다. 이 설정은 Value wire에 없으므로 서버 JVM 환경을 확인하지 않고 모든 JVM/locale 조합에 대한 문자열 key 동일성을 선언할 수 없다. 원본 KV를 이관할 때 원본 JVM 버전/locale를 함께 기록해야 한다.

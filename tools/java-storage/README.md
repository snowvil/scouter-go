# 실제 Java/Scala 저장 oracle

`StorageOracle.java`는 synthetic 입력을 **원본 저장 writer에 전달하고 원본 reader로 조회**한다. 저장 형식의 수작업 Java 복제품을 oracle로 사용하지 않는다. 정본은 Scouter2.21.3 commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`; [고정 source tree](https://github.com/snowvil/scouter/tree/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main)와 [common codec](https://github.com/snowvil/scouter/tree/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.common/src/main/java) 모두 Apache-2.0 원본이다. source HEAD 및 common/server DB/Scala compiler jar의 clean 상태를 확인하며 원본은 수정하지 않는다.

```sh
python3 scripts/java-storage.py --generate /tmp/새로운-빈-fixture-디렉터리
python3 scripts/java-storage.py --verify /tmp/Go-writer가-생성한-DB-디렉터리
python3 scripts/java-storage.py --classes-dir /tmp/원본-컴파일-출력
```

`--generate`는 비어 있지 않은 출력 디렉터리를 거절한다. `--verify`는 원본 reader가 일부 파일을 `rw`로 열고 hash header를 flush할 수 있으므로 **원본 운영 DB가 아닌 복사본/fixture만 입력**한다. 읽지 못한 필수 항목을 빈 성공으로 처리하지 않는다. generated manifest는 SHA/version/timezone/synthetic 표시 및 파일별 크기/SHA256을 기록한다. Java Properties comment timestamp와 gzip compressor 차이 때문에 전체 DB bytes 동일성을 요구하지 않으며 원본 인덱스 bytes 동일성은 별도 legacyindex 테스트에서 검증한다.

Python3, javac JDK11+, Java runtime, 원본 deploy의 Scala2.10.4 compiler가 필요하다. compiler는 macOS의 설치된 JDK8을 사용한다. 다른 환경은 `SCOUTER_SCALA_JAVA=/path/to/jdk8/bin/java`, 다른 source 위치는 `SCOUTER_SOURCE=/path/to/pinned/scouter`로 지정한다. 새 JDK의 compiler mirror 실패를 호환 통과로 처리하지 않는다. common과 oracle Java bytecode는 `--release8`로 컴파일한다.

## 원본 클래스와 격리 경계

실제로 컴파일/실행하는 저장 클래스:

- `RealKeyFile`, `IndexKeyFile`, `IndexTimeFile`, `MemHashBlock`, `MemTimeBlock`, `RealDataFile`.
- `ObjectData`, `ObjectIndex`; `TextPermData`, `TextPermIndex`, `TextTable`.
- `XLogDataWriter`, `XLogDataReader`, `XLogProfileDataWriter`, `XLogProfileDataReader`, `XLogIndex`, `XLogProfileIndex`.
- `DailyCounterData`, `DailyCounterUtils`, `DailyCounterIndex`, `RealtimeCounterData`, `RealtimeCounterIndex`, `RealtimeCounterKeyFile`, `RealtimeCounterDBHeader`, `RealtimeCounterDBHelper`.
- Java `GZipStore`, `IOChannel`, `Block`, `CountBoard`, `GZipCtr`, `BKey` 등 원본 zip package 전체; common `DataInputX`, `DataOutputX`, `HashUtil`, `DateUtil`, `CompressUtil`, Value/Pack 및 `EnumerScala`.

`Configure`의 index/cache/compression 설정, `Logger`, `ConfObserver`, `ShutdownManager`, `DBCtr.getRootPath`, `XLogWR.dir`만 fixture 실행을 위한 infrastructure stub이다. 마지막 두 항목은 root/`/xlog` path routing만 제공한다. 실제 서버 queue thread, DB root lifecycle, WR/RD facade는 oracle 범위가 아니다. Object는 원본 `ObjectData.write/update`에 4096byte Pack padding을 전달하고 Text first-write 규칙은 fixture에 각 key를 한 번 넣는다. 이는 전체 서버 ObjectWR/TextWR 실행의 증거와 구별한다.

GZipStore의 실제 flush는 IOChannel의 비동기 executor에 전달된다. oracle는 flush/shutdown 후 executor를 reflection으로 찾아 `shutdown/awaitTermination(30s)`를 수행하여 파일 쓰기 완료를 확인한다. DB 코드를 대체하거나 수정하지 않는다.

## 양방향 dataset 계약

출력은 `<DIR>/plain`, `<DIR>/gzip` 두 root며 데이터는 같다. date=`20260101`, timezone=`Asia/Seoul`, `TIME=1767193200125`(00:00:00.125), hash index size=1MiB다. 압축 설정은 저장된 `.service.conf`/`.profile.conf`가 우선하며 verify 과정의 process default는 반대로 지정해 이 의미를 확인한다.

| 대상 | 고정 입력/조회 기대값 |
|---|---|
| Object | hash42, type`java`, 최종 name`oracle-last`(초기`oracle-first`후 같은slot update), address`127.0.0.1`, version`oracle`, wakeupTIME, alive=true, tags`region=synthetic`; obj.data4100byte |
| permanent Text | service/hash7=`첫 서비스`, root`00000000/text/text_service` |
| daily Text | error/hash8=`합성 오류`, error/hash9=empty, root`20260101/text/text` |
| XLog | tx101/endTIME+200/gx9001; tx102/endTIME+100/gx9001; tx103/endTIME+550/gx0; 공통objHash42/service7/elapsed10; V1 time order102,101,103; gxid 최신102,101 |
| Profile | tx101, chunks `{1,2}`, `{3,4}`; physical offset 오름차순으로 조회 |
| Daily counter | objName`/oracle/app`의 HashUtil hash; kind2..6마다 b{kind}=Booleantrue, f{kind}=Float1.25, d{kind}=Double2.5, n{kind}=Decimal-9223372036854775807, HHMM0; 각 count1440/288/144/24/1 |
| Realtime counter | TIME/TIME+500, 동일 objHash; cpu Double12.5/13.5, errors Decimal7/8; 실제 `.head` dictionary와 indexed data 읽기 |

Java writer→Go reader와 Go writer→Java reader를 별도로 실행해야 양방향 검증이다. `--generate` 후 같은 Java로 `--verify`한 결과만으로 Go 호환을 선언하지 않는다. 이 oracle는 synthetic 소규모 저장/조회 검증이며 운영 DB 이관/rollback, crash recovery, 쓰기 실패 복구, 실제 Agent/Client 및 지속 목표 처리량의 증거가 아니다.

## 2D 추가 DB와 실제 handler oracle

```sh
python3 scripts/java-storage.py --generate-extra /tmp/새-빈-extra-root
python3 scripts/java-storage.py --verify-extra /tmp/Go가-쓴-extra-root-복사본
python3 scripts/java-storage.py --generate-spans /tmp/새-빈-span-golden
python3 scripts/java-storage.py --generate-counter-handlers /tmp/새-빈-handler-golden
```

추가 root는 `plain/gzip` 하위 구분 없이 단일 root이며 DATE/TIME은 기본 dataset과 같다. Status/Summary/Alert/Span 각각 원본 writer/reader/index 3종(총12개), 원본 `StackAnalyzerDB`와 `BatchDB`, 원본 `SpanStepBuilder`를 임시 컴파일한다. `DBCtr.running=false`로 background queue를 멈추고 protected append/index append에 reflection으로 접근한다. 저장 prefix/codec/index 형식을 구현한 substitute는 없다. 추가 manifest에 baseline 및 guarded 원본 source의 길이/SHA256을 기록한다.

| 파일/입력 | 의미 |
|---|---|
| `20260101/status/status.pstat`, `status_tim.*` | Status timeTIME/hash42/key`synthetic-status`/data.state`synthetic` |
| `20260101/sum/sum.sum`, `sum_app_tim.*` | APP1 Summary parallel lists id7/count3/error1/elapsed90/cpu12/mem1024 |
| `20260101/alert/alert.alert`, `alert_tim.*` | Alert timeTIME/hash42/level2/title`synthetic alert`/message`synthetic message`/tag region |
| `20260101/span/span.data`, `span_gid.*`, `span_tim.*` | SpanContainer gxid9001, 5개 Span의 wire order 보존 |
| `20260101/oracle/app/stack.dat`, `stack.idx` | timeTIME/TIME+100의 원본 `StackPack.setStack` 압축 record2개. verify는 decompressed 문자열을 비교하여 gzip header 차이를 허용 |
| `20260101/oracle/app/batch.dat`, `batch.idx` | Batch2개 startTIME/TIME+100, elapsed25/50, PID7/8, SQL stats1개/unique SQL1개. list는 details nil, physical position0 및 `4+firstPackLength`; detail은 전체 Pack |
| `oracle-extra/*.bin` | 위 입력의 원본 serialized Pack. `batch-simple-{1,2}.bin`은 실제 `BatchPack.writeSimple`의 Blob body이며 Pack type/HasNEXT를 포함하지 않음 |

`--generate-spans`는 원본 Scala `SpanStepBuilder.toSteps`의 input Container와 output Step bytes를 보관한다. `span-simple-steps.bin`은203byte/4Step(parent -1,0,0,2; start0,5,10,11); CLIENT 경계의 child를 제외한다. `span-shared`는 SERVER/CLIENT shared-ID와 동일 timestamp sibling을 포함하며3Step(parent -1,0,0; start0,10,20)이다. timestamp만 비교하는 TreeSet의 sibling 제거와 CLIENT 경계, optional URL/endpoint/tag/annotation을 원본 실행으로 확인한다. `TextRD` seam은 wire에 쓰지 않는 `nameDebug`만 제공한다.

`--generate-counter-handlers`는 실제 원본 `DailyCounterService`, `CounterService`, `TimedSeries`, `SummaryService`, `SummaryEndUserNav/Ajax/Error`와 common `NumberValue`를 실행한다. Agent selector/record 공급만 임시 seam이고 handler/aggregation/serialization은 정본 그대로다. 요청 Pack와 응답 HasNEXT+Pack bytes, Summary 입력 Pack와 관측 JSON을 보관한다.

- TODAY_TOT avg는 원본 `Array[Double](288)`와 `Array[Int](288)` 때문에 singleton이다. empty/zero는 value1.0, 첫 positive12는300/289; 두 번째 positive24는 내부 index1 예외를 catch한 뒤 같은 partial 결과를 전송한다.
- PAST_TIME_TOT empty는 empty time/value Map; 두 sample은 actual `TimedSeries.addEnd`의 sort Unit→Array cast `ClassCastException`으로 응답 body0byte다.
- `NumberValue.add`는 Double first에서 ClassCastException, Decimal1+Double2.5는3, Float1.25+Double2.5는3.75다.
- EndUser longID4294967303을 두 record(count2,3)에 반복하면 `getInt` lookup과 long key put 차이로 마지막 record count3/duration10이 남는다. Error의 host는 inputhost99 대신 stacktrace44며 count field를 전송하지 않는다.

이는 정본 결함 후보의 실제 실행 결과이며 의도대로 정규화한 알고리즘의 성공을 뜻하지 않는다. 정본 backend seam 실행과 Go query byte 비교, 실제 socket 상호운용은 각각 별도의 검증이다.

동일 handler oracle의 `past-group-{plain,metadata}-{request,response,row}.bin`은 실제 `CounterService.getPastTimeGroupAll`을 실행한다. `{cpu:12}`만 있는 bulk row는 요청 hash42에 대해 빈 배열이며 `{objHash:42,time:1000,cpu:12}`는 sample1개다. 원본 `PerfCountCore.scala:47-48` metadata 자동 주입은 주석이므로 일반 counter의 objName/time을 임의로 group row에 합성하지 않는다. `.row.bin`은 Pack이 아닌 원본 `writeValue(MapValue)` bytes다.

이는 direct RD 공급 두 경계의 결과다. 실제 UDP 수신은 **`NetDataProcessor.scala:137-148`에서 objHash/time을 data에 먼저 넣은 뒤 PerfCountCore에 전달**한다. 따라서 실제 Agent 수신 row의 group query에는 이 metadata가 존재한다. PerfCountCore의 주석만으로 전체 수신 경로의 metadata가 없다고 해석하지 않는다.

## DB handler, TTL index, local KV oracle

```sh
python3 scripts/java-storage.py --generate-db-handlers /tmp/새-빈-db-handler-root
python3 scripts/java-storage.py --generate-ttl /tmp/새-빈-ttl-root
python3 scripts/java-storage.py --verify-ttl /tmp/Go가-쓴-ttl-root-복사본
python3 scripts/java-storage.py --generate-local-store /tmp/새-빈-local-store-root
```

모든 generate mode는 빈 디렉터리를 요구한다. manifest의 `guarded_source_checksums`는 정본 guard 대상이며 모두가 컴파일됐다는 목록은 아니다. 실제 class 수, javac 버전과 oracle helper SHA256도 기록한다. Scala compiler는 같은 pinned2.10.4/JDK8 경계를 사용한다. 기본/extra mode는 유지한다.

`--generate-db-handlers`는 실제 `MariaService`, `CubridService`, `StatusRD`와 실제 Status writer/reader/index를 실행한다. `CounterCache`/`StatusCache` 입력 공급, root path, 미실행 AgentCall 분기만 seam이다. 다음 request/response 파일은 HasNEXT를 포함한 실제 handler bytes다.

| `db-{name}-{request,response}.bin`의 name | 확인 결과 |
|---|---|
| connections / activity / cubrid-dml | `MapValue` 합계, Cubrid db_num decimal 문자열을 `24`로 연결 |
| hit-ratio / response-time | `MapPack` 안의 FloatValue / weighted DoubleValue. 각각 standalone Value 응답이라고 가정하지 않음 |
| digest-table | progress2개 → Decimal summary → 최신 Status. AVG_TIMER_WAIT는 snapshot long 합50이며 count 평균이 아님 |
| cubrid-status / cubrid-server-info | actual StatusRD가 Scala nonlocal return을 catch. 요청 hash42,43의 각 loop가 같은 reverse 첫 Status hash43을 반복해2개 전송 |

`db-counter-{42,43}-input.bin`, `db-status-{0..3}-input.bin`, 동적 `db-response-time-input-status.bin`과 관측 JSON도 보관한다. Response-time 입력의 LAST_SEEN은 실행 시각이므로 해당 fixture를 자체 소비해야 한다.

`--generate-ttl`/`--verify-ttl`은 실제 `RealKeyFile2`, `IndexKeyFile2`, `MemHashBlock`을 호출한다. 출력 `ttl.k2file`/`ttl.hfile`(hash1MiB)과 `ttl-metadata.json`을 사용한다. 원본 writer가 collision/update/delete/TTL bytes를 직접 쓰며 원본 get/getAll/record walk/reopen으로 검증한다. `a` key의 expired/deleted 기존 match 모두를 updateOrPut44로 다시 쓰고 새 record까지 append하여3개가 남는다. `collision-359220` offset22, expired offset55/expire100, deleted offset66, empty zero bytes, future offset77/expire4294967295도 포함한다. 영구 만료 sentinel은 **549755813632 =2^39-256**이며 일반 signed long5 최대값과 다르다. 절대 만료 값은 원본 `keyFile.setExpire`로 고정해 clock 변동을 제거한다. getAll은 삭제되지 않은 expired row도 반환하는 원본 의미를 확인한다.

`--generate-local-store`는 실제 `KeyValueStoreRW`, `KeyValueStoreWriter/Reader/Index`, `KeyValueStoreService`, `DatabaseService`를 실행한다. KV 저장 root는 `00000000/kv2/kv_<keySpace>.{kvdata,k2file,hfile}`이며 global=`__SCOUTER_KV_GLOBAL__`, custom=`synthetic-space`다. 네트워크 forwarding AgentCall은 실행 시 예외를 내는 seam이므로 Agent 전달 지원 증거가 아니다.

- `kv-{global,global-missing,custom,custom-missing,global-bulk,custom-bulk}-{request,response}.bin`: 원본 READ4 handler의 단건 hit/miss 및 bulk b,missing,a,b 입력. missing 단건은 empty TextValue, bulk는 missing 생략/duplicate replacement/첫 b,a 순서 보존.
- `kv-global-bulk-empty`와 `kv-custom-no-key`의 response는0byte다. 요청을 빈 성공 Map으로 바꾸지 않는다.
- `kv-key-cases.bin`: 원본 writeValue(ListValue), 각 MapValue의 `request`/`response`는 원본 Value 요청/HasNEXT 응답 Blob이며 `key`는 실제 decoded Value.toString 결과 TextValue 또는 NullValue다. Float/Double ±0, 1e-3, 1e7, NaN/Infinity, min/max, Double18자리 경계 및 Decimal/Boolean/Null/Text/Blob/List/Hashtable Map 총26종을 실제 handler에 전달한다. locale는 관측 JSON에 기록한다.
- `kv-seeds.bin`: 원본 writeValue(ListValue)의 각 MapValue `{keySpace,key,value}`. 실제 RW.set 순서를 기록해 formatter key alias도 재현할 수 있다. a/b global 값은 synthetic-alpha/beta, custom은 synthetic-custom-alpha/beta다.
- `query-list-{exist,load}-{request,response}.bin`, `query-list-map-{0..2}.bin`: actual StatusRD의 hash42 row3개(키 종류 제한 없음); EXIST time3개와 LOAD original StatusPack.toMapPack의 data-only/Hashtable entry 순서. Status metadata를 새 field로 넣지 않는다.

원본 `KeyValueStoreReader.close`가 Writer table entry를 제거하므로 harness는 두 실제 handle을 먼저 보관한 뒤 닫는다. RW/server 전체 lifecycle을 대체하거나 정상화하지 않는다. 이 실행 결과와 Go의 실제 DB 읽기/TCP raw 비교는 별도의 gate이며 전체 KV 관리/쓰기 명령을 검증했다는 의미가 아니다.

## Go 서버가 실제로 쓴 DB의 원본 Java 전체 읽기 (`ReplayDbOracle`)

`scripts/replay-java-oracle.py <빈 출력 DIR>`는 `dist/scouter-go`/`dist/scouter-replay`로 plain·gzip DB를 실제로 생성하고, `scouter-db inspect` 뒤 **복사본**을 원본 Java reader로 전부 읽는다. `tools/java-storage/ReplayDbOracle.java`는 기존 `compile_storage`가 컴파일한 원본 클래스(`IndexTimeFile`, `XLogIndex`, `XLogDataReader`, `XLogProfileIndex`/`XLogProfileDataReader`, `ObjectData`/`ObjectIndex`, `TextPermData`/`TextPermIndex`, `TextTable`, `GZipStore`/`IOChannel`/`Block`, `CompressUtil`)로 날짜 전체 time bucket을 순회해 XLog txid 집합·txid keyed lookup 동일성, Profile 조각 수와 bytes, Object·Text 건수를 출력한다. 압축 DB는 `CompressUtil.unZip` 해제 길이와 `count.dat`의 관계도 출력한다. 인자를 하나 더 주면(`<root> <date>`) 원본 `XLogDataWriter`/`XLogIndex`로 XLog 3건을 이어 쓰고, `query <addr> <date>` 모드는 secure LOGIN 후 `TRANX_LOAD_TIME_GROUP`으로 Go 서버를 조회한다.

이 oracle은 정상 종료·재시작된 소규모 replay DB의 읽기 호환 증거다. 운영 DB, 손상 주입, 32MiB rollover 이상의 다중 block, counter/tagcount/visitor 읽기는 범위 밖이다. 2026-10-14 실행 결과는 [Phase 2E 개선 기록](../../docs/validation/2026-10-14-phase2e-improvements.md) §2에 있다.

## KV/TTL 쓰기 handler oracle (`--generate-local-store-writes`)

`python3 scripts/java-storage.py --generate-local-store-writes <빈DIR>`는 실제 `KeyValueStoreService`의 `SET_GLOBAL_KV`, `SET_GLOBAL_TTL`, `SET_GLOBAL_KV_BULK`, `SET_CUSTOM_KV`, `SET_CUSTOM_TTL`, `SET_CUSTOM_KV_BULK`를 원본 `KeyValueStoreRW`/writer/index와 함께 실행해 45 case의 request/response bytes, 쓰기 뒤 `kv2` 파일(`kvdata`/`k2file`/`hfile`)과 READ 재조회 결과를 남긴다. 원본 handler가 예외를 내는 3 case(custom bulk `kv` 부재, 비Map `kv`)는 예외 문자열을 기록한다. Go `TestLocalStoreActualJavaWriteHandlersAndDB`가 이를 재생해 response bytes와 파일을 대조한다([local-store-query](../../docs/compatibility/local-store-query.md)).

## Object 영구 삭제 oracle (`ObjectRemoveOracle`)

`ObjectRemoveOracle.java`는 `compile_storage`(`--classes-dir`)가 `StorageOracle`과 함께 컴파일하며 원본 `ObjectData`/`ObjectIndex`(그 아래 `IndexKeyFile.delete`/`RealKeyFile.setDelete`)만 호출한다. 모드는 `generate <root>`(date `20260101`, hash1..5를 `ObjectWR` add 순서 `index.get<0 → write → set`으로 기록한 뒤 hash3을 `index.get>=0 → delete`로 삭제), `remove <root> <hash>`, `verify <root> <live csv> <removed csv>`(원본 `ObjectIndex.read`로 목록을 읽고 Pack hash/name과 `get` 결과를 확인)다. Go `TestJavaStorageInteropObjectRemove`(`make storage-check` 대상)가 Java→Go→Java, Go→Java→Go 양방향과 같은 Java DB 복사본에 대한 Java/Go 삭제 결과의 `obj.kfile`/`obj.hfile`/`obj.data` bytes 동일성을 검증한다. `ObjectWR` writer thread·queue는 실행하지 않는다. 고정 source의 writer는 remove order에서 add 분기 NPE로 delete에 도달하지 않으며, 이 차이는 [storage-contract](../../docs/compatibility/storage-contract.md) "Object 영구 삭제"에 source 근거로 기록한다.

## Realtime counter 이름 사전 oracle (`RealtimeCounterNamesOracle`)

`RealtimeCounterNamesOracle.java`는 Go `TestJavaStorageInteropRealtimeCounterNames`(`make storage-check` 대상)가 `compile_storage` classpath로 따로 `javac --release 8` 컴파일한다(script 변경 없음). 원본 `RealtimeCounterDBHeader`/`RealtimeCounterData`/`RealtimeCounterIndex`/`RealtimeCounterDBHelper.getTagBytes`를 `RealtimeCounterWR` 순서(`intern(keySet)` → data write → index write)로만 호출한다.

- `generate <root> <names>`: 이름 `names`개(테스트는 5000)를 100개씩 record로 쓴다.
- `verify <root> <names>`: Go가 더한 이름 하나와 record를 원본 reader로 확인한 뒤, Java가 이름을 하나 더 쓴다.
- `verify-go <root>`: Go가 만든 날짜를 Java가 읽는다. 이 날짜에는 createNames로 만든 `.head`, 실패한 append가 남긴 꼬리, 이름 상한으로 entry 하나가 빠진 record가 있다. 읽기는 `RealtimeCounterDBHelper.open(date, true)`와 `RealtimeCounterRD.read`/`readBulk`가 부르는 원본 호출(`counterIndex.read`, `getStartEndDataPos`, `counterData.readBulk`, `setTagBytesMulti`)로 한다. `RealtimeCounterRD` 객체는 compile 목록에 없어 직접 부르지 않는다.

Go는 그 결과를 다시 읽는다. writer thread·queue는 실행하지 않는다.

## 설정 키 동작 oracle (`ConfigStorageOracle`)

`python3 scripts/java-storage.py --config-classes-dir <DIR>`는 기본 storage class에 원본 `TextWR`/`TextRD`/`TextPermWR`/`TextPermRD`/`TextCache`/`TextDupCheck`/`ThreadScala`/`ServerStat`, `RealKeyFile2`/`IndexKeyFile2`/`KeyValueStoreIndex`, `AutoDeleteScheduler`를 더 컴파일하고 classpath를 출력한다. seam은 `Configure`(설정 필드: `mgr_text_db_daily_*_enabled`, `mgr_purge_*`, `_mgr_kv_store_index_default_mb=8` 추가), `ConfObserver`(Runnable 보관·`exec()`), `DBCtr`(root, `running=false`), `CoreRun.running()=false`, `ZipkinSpanWR.SPAN_DIR()`뿐이다. 모드는 `text <root> <script>`(`daily S A U`/`put date div hash text`/`get date div hash` 줄을 원본 `TextWR.process`·`TextRD.getString`으로 실행), `sizes <root> <mb 10개>`·`read-sizes <root>`(원본 index class로 `*_mb` 크기 파일 생성·판독), `purge <root> <phase>...`(원본 `AutoDeleteScheduler` 한 회차씩, 둘째 phase부터 ConfObserver 실행)다. writer thread는 `running=false`로 바로 끝나므로 harness가 `TextPermWR` writer loop 본문과 `FlushCtr` flush 1회를 대신 실행한다. Go `TestJavaStorageInteropConfig*`(`make storage-check` 대상)가 사용하며 계약은 [internal/storage/CONTRACT.md](../../internal/storage/CONTRACT.md)에 있다.

`--generate-local-store*` stub의 `_mgr_kv_store_index_default_mb`는 Java 기본값 8이다(Go 새 KV index도 8MiB).

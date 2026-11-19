# 서버 metadata READ 계약

정본은 `baseline.json`의 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이다. 아래 원본 경로의 Scala/Java service handler를 조사했다. source line은 annotation 위치다. 이 문서는 `query-scope.json`의 해당 25개 `local_read`만 다룬다. account/configuration 변경, DB 삭제, thread control을 포함한 mutation은 [management-commands](management-commands.md)가 다룬다(모두 구현 등록). `CHECK_JOB`은 `REMOTE_CONTROL*`·`REFETCH_COUNTER_XML`이 넣은 job을 꺼내며 없으면 Java의 "job 없음" 빈 응답이다([agent-read-query](agent-read-query.md)).

## 요청·응답 계약

모든 지원 명령은 인증 후 실행한다. 각 응답 item 앞은 `HasNEXT`, service 완료는 transport의 `NoNEXT`다. `body 없음` 명령에서 불필요한 MapPack을 읽지 않아 persistent TCP에서 다음 명령을 소비하지 않는다. 없는 필드의 Scouter 기본값은 text=null, decimal=0이며, Go handler는 text 누락을 빈 문자열로 처리한다. 문자열 비교 account id 조회의 결과는 존재 여부의 반대인 **사용 가능 여부**다.

공통 원본 prefix: `scouter.server/src/main/scala/scouter/server/netio/service/handle/`.

| 명령 | 원본 / line | request | response 순서 / 예외 | Go 상태 |
|---|---|---|---|---|
| CHECK_ACCOUNT_ID | AccountService.scala:75 | MapPack.id | MapPack.result Boolean | 계정 파일 모드는 `account.xml` 계정, 단일 credential 모드는 그 계정 하나(아래 account source 항목) |
| LIST_ACCOUNT | AccountService.scala:86 | body 없음 | account별 BlobValue(Account.toBytes) | 계정 파일의 계정(단일 credential 모드는 그 계정 하나), id/password/email/group 순서 |
| LIST_ACCOUNT_GROUP | AccountService.scala:95 | body 없음 | MapPack.group_list List<Text> | `account_group.xml`의 group(단일 credential 모드는 admin) |
| GET_GROUP_POLICY_ALL | AccountService.scala:107 | body 없음 | MapPack[group] MapValue(policy) | `account_group.xml`의 group별 policy(단일 credential 모드는 LOGIN과 같은 admin 정책) |
| CHECK_LOGIN | LoginService.scala:130 | MapPack.id/pass | BooleanValue (Pack 아님) | LOGIN과 같은 salted SHA256 입력 비교, session 생성 없음 |
| GET_LOGIN_LIST | LoginService.scala:89 | body 없음 | MapPack.session/user/ip/logintime/ver/host List 순서; user 없음은 item 없음 | 실제 유효 session snapshot; logintime은 경과 초 |
| GET_CONFIGURE_SERVER | ConfigureService.scala:46 | body 없음 | MapPack.configKey/serverConfig | `--conf`가 있으면 Configure 키 136개와 `scouter.conf` 원문(`Store.LoadText`), 없으면 flag 행으로 만든 properties text([config CONTRACT](../../internal/config/CONTRACT.md) §7) |
| LIST_CONFIGURE_SERVER | ConfigureService.scala:111 | body 없음 | MapPack.key/value/default List 순서 | `--conf`가 있으면 현재 Snapshot(기동 소비 키는 실행 값, live 키는 적용 값), 없으면 flag 행 |
| CONFIGURE_DESC | ConfigureService.scala:164 | MapPack.objHash=0 기본 | server: key→Text; objHash!=0: Agent 최종 MapPack만 | local descriptor 또는 Agent relay |
| CONFIGURE_VALUE_TYPE | ConfigureService.scala:186 | MapPack.objHash=0 기본 | key→Decimal; objHash!=0: Agent 최종 MapPack만 | Scouter ValueType 1..5 |
| CONFIGURE_VALUE_TYPE_DESC | ConfigureService.scala:208 | MapPack.objHash=0 기본 | server: key→MapValue; objHash!=0: 빈 MapPack (원본 계약) | 공급된 TypeDescription만; 옵션 없는 Go 설정은 entry 없음 |
| GET_CONFIGURE_TELEGRAF | ConfigureService.scala:224 | body 없음 | MapPack.tgConfigContents Text | 설정된 현재 파일 또는 원본 TgConfig sample fallback |
| GET_CONFIGURE_COUNTERS_SITE | ConfigureService.scala:247 | body 없음 | MapPack.contents Text | 명시적 구성 owner가 text 공급한 경우 지원 |
| DEBUG_SERVER | ServerDebug.scala:52 | MapPack (현재 무사용) | MapPack.msg=`processed N`, 오늘 AlertRD 전체 읽기 | 해당 날짜 실제 AlertHistory record 수 |
| SERVER_DB_LIST | ManageService.java:37 | body 없음 | MapPack.name/size/lastModified List/free/total; depth-first 하위 directory 행, root 파일도 total에 합산 | os.Root read-only, 실제 filesystem free bytes |
| SERVER_ENV | ServerInfo.scala:64 | body 없음 | System.getProperties key→Text MapPack | 실제 os.name/os.arch/go.version, java.* 없음 |
| SERVER_LOG_LIST | ServerInfo.scala:84 | body 없음 | MapPack.name/size/lastModified; `.log` regular files; directory 부재는 item 없음 | symlink 제외, 실제 file metadata |
| SERVER_LOG_DETAIL | ServerInfo.scala:105 | MapPack.name | TextValue (Pack 아님); 이름/파일 없음은 item 없음 | root의 regular `.log`만 읽기 |
| SERVER_THREAD_DETAIL | ThreadList.scala:39 | MapPack.id Long | JVM ThreadUtil.getThreadDetail MapPack 또는 item 없음 | goroutine 정보를 같은 key 순서로(2026-10-16, `HandleConfigureCommand`; cpu 0, name "goroutine N …") |
| SERVER_THREAD_LIST | ThreadList.scala:50 | body 없음 | JVM ThreadUtil.getThreadList MapPack 또는 item 없음 | goroutine 목록(id/name/stat/cpu, 상한 10,000) |
| GET_ALERT_SCRIPTING_CONTETNS | AlertScriptingService.scala:38 | MapPack.counterName | MapPack.contents Text; 파일 없음은 빈 contents | plugin root의 `.alert` read-only |
| GET_ALERT_SCRIPTING_CONFIG_CONTETNS | AlertScriptingService.scala:49 | MapPack.counterName | MapPack.contents Text; 파일 없음은 빈 contents | plugin root의 `.conf` read-only |
| GET_ALERT_SCRIPT_LOAD_MESSAGE | AlertScriptingService.scala:82 | MapPack.loop/index=0 기본 | MapPack.loop/index/messages; cache 없음은 item 없음 | 50칸 ring; Go는 컴파일하지 않으므로 "compile/execute unsupported; <file> saved/detected" 사실 메시지 |
| GET_ALERT_REAL_COUNTER_DESC | AlertScriptingService.scala:105 | body 없음 | descriptor별 MapPack.desc/methodName/returnTypeName/parameterTypeNames | 고정 원본 소스의 25개 descriptor 정적 표(소스 선언 순서) |
| GET_PLUGIN_HELPER_DESC | AlertScriptingService.scala:124 | body 없음 | 위와 같은 Java reflection descriptor stream | 22개 descriptor 정적 표 |

`ManageService.java`는 위 Scala prefix와 다른 `scouter.server/src/main/java/scouter/server/netio/service/handle/`에 있다. `Account.java` 정본은 `scouter.common/src/main/java/scouter/lang/Account.java:33`: 각 field가 UTF-8 length **1 byte** + bytes이며 바깥은 **BlobValue**다. password field는 원본과 같이 encrypted/salted digest다. 응답은 인증된 READ wire에만 포함하며 테스트 출력/fixture/log 문서에 실제 credential을 기록하지 않는다. 255 bytes 초과 field는 Java 원본의 length wrap defect를 재현하지 않고 `ErrLimit`으로 거절한다.

## 구현 경계와 의도된 차이

- `metadata_backend.go`의 `MetadataBackend`는 설정 owner와 filesystem의 실제 snapshot을 제공한다. `Config.Metadata=nil`, properties 미제공, telegraf file 및 fallback 미제공, counters-site text 미제공은 `ErrUnsupported`이며 빈 성공 응답을 만들지 않는다.
- Telegraf 내용 조회는 `Configure.java:621-631` 계약을 따른다. `FileMetadataConfig.TelegrafFile` 경로의 현재 regular file을 매 조회 read-only로 읽고, file 또는 parent directory가 없으면 configuration owner가 제공한 `TgConfig.getSampleContents()` text를 반환한다. 존재하는 빈 file은 빈 text를 그대로 반환하고 sample로 바꾸지 않는다. 파일과 fallback 모두 미제공이면 unsupported다. 원본은 exists-but-unreadable에서 빈 text를 반환하지만 Go는 filesystem 오류를 명시적으로 반환한다. symlink/nonregular/byte-limit 오류도 fallback으로 숨기지 않는다. Telegraf 수집·설정 적용은 이 READ와 별개로 `internal/telegraf`가 맡는다([http-contract](http-contract.md)).
- `FileMetadata` 설정은 생성 시 복사하고 descriptor까지 복사한다. 조회 결과도 별도 snapshot이다. `--conf`가 없을 때의 `serverConfig`는 Go properties의 정규화 text이므로 Java `Configure.loadText()`의 원문 comments/unknown keys와 동일하지 않다. `--conf`가 있으면 파일 원문 그대로다. Java-only 설정을 실제 지원하는 것처럼 만들지 않는다.
- `ValueTypeDesc`의 `strings/strings1/strings2/strings3/booleans/booleans1/ints/ints1` list schema는 `scouter.lang.conf.ValueTypeDesc.toMapValue()` 정본이다. 실제 enumeration descriptor가 필요하면 configuration owner가 그 MapValue entries를 공급한다. 옵션 없는 property는 annotation 없는 Java 설정처럼 descriptor entry를 추가하지 않는다.
- JVM thread id/state/CPU time/stack의 의미는 Go goroutine과 같지 않다. `SERVER_THREAD_*`는 2026-10-16부터 goroutine 정보를 같은 스키마로 돌려주되 name에 "goroutine" 접두어, cpu 0, Blocked/Waited -1로 차이를 드러낸다(`internal/service/server_threads.go`).
- Java AlertRuleLoader/compiler는 내장 실행 구현이 없다(JVM 바이트코드 실행 불가). 2026-10-16부터 descriptor 2개는 고정 소스의 정적 표, load-message는 저장·변경 사실만 담은 메시지 ring으로 응답하며 script가 컴파일/실행된 것처럼 응답하지 않는다. `.alert`/`.conf` 저장(SAVE_ALERT_SCRIPTING_*)은 지원한다. JVM 스크립트·plugin 실행은 제외 확정(명시적 미지원)이다.
- `AlertRuleLoader.java:86-109`의 script/config 내용 조회는 compiler와 별개인 파일 읽기다. `MetadataAlertScriptBackend`와 `FileMetadataConfig.PluginDir`로 `.alert`/`.conf`를 read-only 반환한다. plugin root 미설정은 unsupported지만 설정된 root에서 파일/디렉터리가 없으면 원본처럼 빈 contents를 반환한다. counterName 누락은 Java null 문자열 결합대로 `null.alert`/`null.conf`이며 path separator/symlink/nonregular 파일은 거절한다. 실제 script 실행 지원을 뜻하지 않는다.
- `SERVER_ENV`는 같은 MapPack dictionary지만 실제 Go runtime을 나타낸다. Java system properties 전체 호환이 아니다. `os.arch`는 Go runtime 식별자이고 JVM architecture 표기와 다를 수 있다.
- account source는 `--account-dir`이 있으면 Java `account.xml`/`account_group.xml`(복수 계정·group policy·LDAP, `HandleAccountCommand`가 CHECK_ACCOUNT_ID/LIST_ACCOUNT/LIST_ACCOUNT_GROUP/GET_GROUP_POLICY_ALL과 변경 4종을 처리, 2026-10-16)이고, `--user/--password` 단일 credential 모드에서는 이 문단의 종전 동작(email 빈 값, group admin, 변경 미지원)이 유지된다.
- file 목록 sibling 순서는 Java `File.listFiles()`가 보장하지 않는다. Go는 이름 정렬 후 traversal하며 DB parent directory 행은 하위 directory 뒤에 나온다. symlink는 목록/recursive 합산에서 제외한다.
- DB/log access는 `os.Root`로 root를 고정하고 log detail은 `O_NOFOLLOW`를 사용한다. `../`, absolute path, separator, non-`.log`, nonregular 파일은 거절한다. Java 원본은 `name`의 경로를 검증하지 않는다. Go는 root의 regular `.log`만 읽는다. 공유 호스트의 symlink/path 노출을 방지하는 의도된 차이다.
- 기본 전체 file entry 4096, recursion depth 64, text 4 MiB 상한이다. context 취소, read 실패, limit 초과는 error이며 부분 스냅샷을 성공으로 반환하지 않는다. file 읽기는 read-only이며 DB 원본을 변경하지 않는다. syscall filesystem query는 Linux/macOS 대상이며 Windows build 호환은 이 구현에서 검증하지 않았다.

## 검증

`server_metadata_test.go`는 scope inventory의 25개를 비교하고 모든 mutation을 부정한다. 인증/context 취소는 body 읽기 전 거절한다. 실제 loopback TCP에서 no-body/MapPack/BooleanValue/TextValue/BlobValue 응답 종류, field 순서와 다음 명령 경계를 확인한다. isolated temporary DB/log 파일로 recursive 합산/postorder/root 파일/`.log` filtering/path traversal/symlink/limits를 검사한다.

`TestMetadataJavaWireOracle`은 고정 원본 SHA를 확인하고 임시 경로에서 원본 `scouter.common`과 synthetic oracle을 javac로 컴파일하여 Java `DataOutputX/MapPack/BooleanValue/Account.toBytes/BlobValue` 바이트를 Go codec 결과와 비교한다. 이는 wire 바이트 근거이며 전체 Scala server business handler 또는 운영 상호운용 완료 근거가 아니다. JDK 부재 시 skip을 pass로 보고하지 않는다. 실제 Agent/Client GUI, JVM plugin, 운영 계정/로그, 공유 서버 부하 gate는 미실행이다.

이번 범위의 실행 결과:

- `.tools/go/bin/go test ./internal/service -run Metadata -count=1`: PASS. Java oracle 실제 실행, skip 없음.
- `.tools/go/bin/go test -race ./internal/service -run Metadata -count=1`: PASS. TCP/metadata/config snapshot/file 경계 테스트 포함.
- 전체 `make check/race/fuzz/bench/fixtures-check/inventory-check`는 통합 검증 단위다. 이 범위 결과에서 전체 저장소 검증 완료를 선언하지 않는다.

추가로 rule 파일 READ 2개의 actual TCP contents/missing/null-default/path/symlink/read-only 검증을 포함한다.

원본 Java의 `new String(content)`는 platform charset로 malformed text를 replacement할 수 있다. Go log/rule detail은 protocol의 엄격한 UTF-8 Text 규칙을 따르며 비UTF-8 로그는 encoding error로 거절한다. 이 차이는 binary log를 성공적인 text로 반환하는 동작을 지원하지 않는다는 의미다.

`TestMetadataTelegrafConfiguredFileAndFallback`은 현재 file 변경/빈 file/없는 parent 및 file/sample fallback/실제 TCP response/symlink/byte-limit/context 취소를 검증한다. sample 정본 생성과 runtime CLI 연결은 통합 검증 단위다.

Telegraf file/fallback 추가 후 `.tools/go/bin/go test ./internal/service -run MetadataTelegraf -count=1`, `.tools/go/bin/go test -race ./internal/service -run Metadata -count=1`, `.tools/go/bin/go vet ./internal/service` 모두 PASS다. metadata Java oracle도 race 실행에 포함했으며 skip하지 않았다.

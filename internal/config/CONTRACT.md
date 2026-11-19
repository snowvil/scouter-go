# scouter.conf 계약 (`internal/config`)

정본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `scouter.server/.../Configure.java`(이하 `C:`), `scouter.common/.../lang/conf/ConfigValueUtil.java`, `scouter.common/.../util/ParamText.java`, `scouter.server/.../netio/service/handle/ConfigureService.scala`. 상세 원본 분석은 `docs/compatibility/management-commands.md` §3.

## 1. 파일과 리로드

| 항목 | Java | Go |
|---|---|---|
| 경로 | `System.getProperty("scouter.config", "./conf/scouter.conf")`(`C:578-585`) | `--conf`(기본 `./conf/scouter.conf`). 기본 경로가 없으면 flag·기본값만 사용(로그 1줄), **명시한 `--conf`가 없으면 기동 실패**, `--conf ""`는 비활성 |
| 주기 | Configure thread가 `reload(false)` 후 3초 sleep(`C:569-574`) | `Store.Watch(ctx)`: 3초 timer → `Reload(false)` → timer 재설정. ctx 종료 시 반환. 소유자는 `cmd/scouter-go/main.go`(Run 종료 후 cancel·대기) |
| gate | `reload(force)` synchronized, force=false면 `last_check+3000ms` 이내 무시(`C:593-600`) | 동일(`Store.mu`, `Options.Interval`) |
| 변경 감지 | `file.lastModified()` 비교(`C:638-643`) | mtime + size + 파일 identity(`os.SameFile`). rename 교체가 같은 mtime 해상도 안에서 일어나도 감지(의도된 확장) |
| 파일 없음 | `lastModified()==0`, `canRead()` false → 이전 값 유지 | 동일: 이전 snapshot 유지, 로그 |
| 읽기 실패 | 이전 값 유지 | 동일, 오류 반환·로그. 단 `Open`(기동)에서는 실패로 처리 |
| 상한 | 없음 | `MaxFileBytes` 4 MiB 초과는 `ErrTooLarge`(읽기·쓰기 모두) |
| telegraf XML | `reload()` 끝의 `reloadTgConfig`가 XML mtime이 바뀐 경우만 재적용(`C:602-619`) | `Options.TelegrafPath`(main은 `--telegraf-config`)의 stat도 같은 `Reload`에서 확인하고, conf와 XML 중 하나라도 바뀌면 둘을 함께 담은 새 Snapshot을 만든다(§5.1) |

`Reload`는 직전 **파일 항목**과 비교해 바뀐 키 집합을 `Change{Applied, Ignored, Unknown}`으로 돌려준다. 초기 로드 이후의 변경만 `Options.OnChange`(reload lock 안에서 호출, Reload/SaveText 재호출 금지)로 전달한다.

## 2. Properties 파싱(`ParseProperties`)

JDK `Properties.load(InputStream)`의 LineReader/load0/loadConvert와 같다: 선행 공백 제거, `#`/`!` 주석(주석 끝의 `\`는 연속행 아님), 키는 첫 비escape `=`·`:`·공백에서 끝남, 홀수 개 후행 `\`는 다음 물리행과 연결(다음 행 선행 공백 제거), `\t \n \r \f \uXXXX` 해석, 그 밖의 `\x`는 `x`, 중복 키는 마지막 값, `\r`·`\n`·`\r\n` 모두 행 끝, EOF 직전 `\`는 버림.

- **Java oracle fixture**: `testdata/properties/*.properties`를 `testdata/oracle/PropertiesOracle.java`로 JDK 8(Zulu 8)과 JDK 17(Zulu 17)에서 실행한 결과가 동일했고 `testdata/properties/expected.json`에 저장했다. `TestParsePropertiesJavaOracle`이 비교한다. 재생성:
  `javac -d <out> testdata/oracle/PropertiesOracle.java && (cd testdata/properties && java -cp <out> PropertiesOracle *.properties > expected.json)`
- 잘못된 `\uXXXX`: Java는 `IllegalArgumentException`으로 load를 중단하고 Configure는 그때까지 읽은 항목을 적용한다(`C:651-657`). Go는 그 앞 항목과 `ErrMalformedUnicode`를 함께 반환하고 Store는 로그 후 적용한다(기동도 계속).
- **의도된 차이 — 문자 인코딩**: Java는 바이트를 ISO-8859-1로 읽어 UTF-8 한글이 mojibake가 된다. Go는 바이트를 그대로 두어 UTF-8 값이 UTF-8로 남고, `\uXXXX`는 UTF-8로 인코딩한다(짝 없는 surrogate는 U+FFFD).

## 3. `${...}` 치환과 환경변수

`ConfigValueUtil.replaceSysProp`(`ConfigValueUtil.java:39-55`)는 결과 Properties에 **환경변수 전체와 System properties 전체를 먼저 넣고** 파일 항목을 덮은 뒤, 값마다 `StringUtil.trim` 후 `new ParamText(value).getText(args)`를 호출한다.

- **원본 결함(oracle 확인)**: 원본은 overload 선택 결함으로 `${이름}`이 이름별 값으로 치환되지 않으며, 정의된 이름과 정의되지 않은 이름의 결과가 같다. 닫는 `}`가 없으면 그 지점부터 버리고 값 앞뒤 공백은 제거한다. 실행 근거: 고정 commit(작업 트리 clean)의 `scouter.common/src/main/java`를 `-sourcepath`로 **수정 없이** 컴파일하고 `ConfigValueUtil.replaceSysProp`를 직접 호출(Zulu 8). 이름별 치환이라는 의도와 다르므로 재현하지 않고 원본 결함으로 분리했다. (`docs/compatibility/management-commands.md` §3.1의 "미정의 변수 → `STRING@hash`"는 `getText(Map)`을 직접 호출한 경우이며 Configure 경로와 다르다.)
- **Go 결정**: 값은 Java `String.trim`(≤ `' '` 바이트 제거) 후 `${name}`을 `Options.Lookup`(기본 `os.LookupEnv`)으로 개별 치환한다. 이름은 trim, 닫는 `}`가 없으면 그 지점부터 버림(ParamText 토큰화와 동일), **정의되지 않은 이름은 `${name}` 문자 그대로 유지**. System properties는 Go에 없다.
- **환경변수 fallback(원본 유지)**: 파일에 없는 키는 같은 이름의 환경변수 값을 trim해 사용한다(Java가 env를 Properties에 섞기 때문). 예: 파일에 `server_id`가 없고 env `server_id=x`면 `x`. 환경변수 변경은 감시하지 않는다.

## 4. 키 표와 값 읽기

`Keys`는 `Configure`의 public non-static field 142개 중 설정이 아닌 6개(`property`, `telegrafInputConfigMap`, `telegrafInputConfigMapDeprecated`, `telegrafOriginalConfig`, `allowIpExact`, `allowIpMatch`)를 뺀 **136개**를 선언 순서(HotSpot `getFields()` 순서, 테스트로 소스 순서와 대조)로 담는다: 이름, Java 타입(`Kind`), field 초기값(`Default`, LIST의 `default`), `@ConfigDesc`, `ValueType`(int/long→NUM 2, boolean→BOOL 3, 그 외 VALUE 1, `@ConfigValueType` 우선), COMMA_COLON 키의 `strings/booleans`.

- 값(`Snapshot.Value`, LIST의 `value`)은 `applyConfig` 규칙: `getValue`=trim, `getInt`=`Integer.parseInt` 실패 시 기본값(Go `strconv.ParseInt(.,10,32)`; Java가 허용하는 비ASCII 숫자는 거절), `getLong`, `getBoolean`=대소문자 무시 `"true"`만 true.
- `applyConfig` 기본값이 field 초기값과 다른 키: `object_zipkin_deadtime_ms` 300000(초기값 180000), `net_webapp_tcp_client_pool_size` 12(30), `net_webapp_tcp_client_pool_timeout` 현재 `net_tcp_client_so_timeout_ms`(60000), `net_http_api_auth_ip_header_key` `""`(null). `mgr_purge_*`는 `C:772-792` 파생식(`storage.JavaRetentionConfig`와 같은 결과를 테스트로 확인).
- `applyConfig`가 읽지 않는 키(`net_webapp_tcp_client_so_timeout`, `span_queue_size`, `input_telegraf_config_file`)는 파일 값과 무관하게 초기값을 보인다.
- `server_id` 기본값은 host 이름(`Options.HostName`, 기본 `os.Hostname`).
- `mgr_log_ignore_ids`(StringSet): `{a, b}` 형식. Java `StringSet` 해시 순서 대신 **첫 등장 순서**(의도된 차이).
- null 초기값(`net_http_api_auth_ip_header_key` default)은 Go `MetadataProperty`가 문자열뿐이라 `""`로 보낸다(Java `NullValue`). 모든 value/default가 Text로 나가는 것(Java는 Decimal/Boolean)은 `internal/service` 응답 계약 소관이다.
- 알려진 비-field 키: LDAP(`account_use_ldap`, `account_ldap_provider_url`, `account_ldap_auth=`(원본 오타 키 그대로, `account_ldap_auth\==...`로만 설정 가능), `account_ldap_principal_domain`, `account_ldap_debug`, `account_ldap_basedn`, `account_ldap_email_id`, `account_ldap_group_id`), `Options.ExtraKeys`/`RestartKeys`(main은 Pulse `pulse.JavaKeys()`), `input_telegraf_$<m>$_<opt>` 패턴. 그 밖의 키는 **보존**(Snapshot·파일 원문)하고 처음 로드할 때와 그 키가 바뀔 때 `unknown key` 로그를 남긴다.

## 5. 키 반영 분류(Go)

Java 반영 시점은 `docs/compatibility/config-behavior.md` §0·§1. Go 분류는 **Go 소비자 기준**이며 `Key.Restart`/`Key.Live`/`Key.GoUse`로 표에 남긴다(`GoUse`는 재기동 키와 live 키의 소비자를 적는다. 그 밖의 키도 `cmd/scouter-go`가 기동 때 한 번 읽을 수 있다 — §6).

| 분류 | 의미 | 키 |
|---|---|---|
| 재기동 필요(`Key.Restart`, `Options.RestartKeys`) | 바뀌면 `config: <key> changed ...; restart required` 로그. `Current()`는 기동 시 값을 유지(파일 원문에는 새 값). 원래 값으로 되돌리면 보고하지 않음 | `db_dir`, `net_udp_listen_ip/port`, `net_tcp_listen_ip/port`, `net_udp_so_rcvbuf_size`, `net_udp_packet_buffer_size`, `_net_udp_worker_thread_count`, `net_http_server_enabled`, `net_http_port`, `plugin_dir`, `_mgr_text_db_index_*_mb` 7개, `_mgr_text_db_daily_index_mb`, `_mgr_kv_store_index_default_mb`, `xlog_queue_size`·`profile_queue_size`(storage XLog·profile writer queue 용량, 원본 `XLogWR`·`XLogProfileWR`의 `RequestQueue`도 기동 때 만든다), Pulse 키(`ext_plugin_pulse_*` 등) |
| live(`Key.Live`, `LiveKeys`, F0) | reload마다 `Snapshot.Live()`로 typed 값이 만들어지고 `server.Runtime.ApplySettings`가 적용(§5.2, 소비자 표 §5.3) | AgentManager·sampling·GeoIP·agent pool: `object_deadtime_ms`, `object_zipkin_deadtime_ms`, `object_inactive_alert_level`, `tagcnt_enabled`, `_auto_5m_sampling`, `xlog_sampling_matcher_*` 5개, `geoip_enabled`, `geoip_data_city_file`, `net_tcp_agent_keepalive_interval_ms`, `net_tcp_get_agent_connection_wait_ms`. service: `server_id`, `ext_link_name`, `ext_link_url_pattern`, `xlog_realtime_lower_bound_ms`, `xlog_pasttime_lower_bound_ms`, `req_search_xlog_max_count`, `log_dir`. transport: `net_tcp_client_so_timeout_ms`, `net_tcp_agent_so_timeout_ms`, `net_tcp_service_pool_size`, `log_tcp_action_enabled`, `log_udp_multipacket`, `log_expired_multipacket`. webapp: `net_http_api_cors_allow_origin/_credentials`, `net_http_api_auth_ip_enabled`, `net_http_api_allow_ips`, `net_http_api_auth_ip_header_key`, `net_http_api_auth_session_enabled`, `net_http_api_auth_bearer_token_enabled`, `net_webapp_tcp_client_pool_timeout`. storage: `mgr_text_db_daily_*_enabled` 3개, `compress_xlog_enabled`, `compress_profile_enabled`, `visitor_hourly_count_enabled`, `_mgr_xlog_id_index_mb`, `log_index_traversal_warning_count`, `_compress_read_cache_block_count`, `_compress_dailycount_header_cache_size`, `mgr_purge_enabled`, `mgr_purge_*` 11개. logger: `log_rotation_enabled`, `log_keep_days`, `_trace`, `mgr_log_ignore_ids`. SqlTables: `sql_table_parsing_enabled`, `log_sql_parsing_fail_enabled`. 디버그 출력: `log_udp_*` 13개, `log_udp_some_object`, `log_test_rate` |
| live(Configure field 아님) | `Live.LDAP`. LIST에 없으므로 `LiveKeys`·`Effective`에 넣지 않는다 | `account_use_ldap`, `account_ldap_*` |
| 즉시 반영(별도 경로) | telegraf: `Change.Telegraf` 또는 telegraf 키 변경 시 `Runtime.ReloadTelegraf`가 다시 합성(§5.1) | `input_telegraf_*`, `telegraf_object_deadtime_ms`, scouter-telegraf.xml |
| 기동 시 1회(재기동 표시 없음) | `cmd/scouter-go`가 기동 때만 읽는다. reload는 Snapshot·LIST 값만 바꾼다 | `net_http_api_enabled`, `net_http_api_gzip_enabled`, `net_http_api_session_timeout`, `net_webapp_tcp_client_pool_size`. servlet listener(`internal/httpapi`)는 `net_http_api_allow_ips`·`net_http_api_auth_ip_header_key`·`log_keep_days`도 기동 때 한 번 읽는다(같은 키의 webapp·logger 소비는 live) |
| 구조 차이·미사용(표시 전용) | Go 동작에 쓰이지 않음. Snapshot·LIST 값만 갱신 | `span_queue_size`, `_compress_write_buffer_block_count`, `_compress_read_cache_expired_ms`, `_compress_write_thread`, `log_service_handler_list`, `plugin_enabled`, `client_dir`, `temp_dir`, `net_http_extweb_dir`, `net_http_api_swagger_*`, notApplied 키(config-behavior.md §5.2) |

### 5.1 scouter-telegraf.xml 재합성

Java 결함 D13(`docs/compatibility/http-contract.md` §5.1): XML이 적용된 뒤 scouter.conf만 바뀌면 `applyConfig`(`C:830-836`)가 XML이 덮었던 전역 값(`input_telegraf_enabled/_debug_enabled/_delta_counter_normalize_default/_seconds`, `telegraf_object_deadtime_ms`, 강제된 `net_http_server_enabled`)을 conf 값으로 되돌린다. XML mtime은 그대로라 `applyTelegrafInputConfigNew`가 다시 돌지 않는다. deprecated measurement 키 변경도 XML이 바뀔 때까지 사용 map에 반영되지 않는다.

**Go 결정(의도된 차이)**: 모든 Snapshot은 같은 reload에서 읽은 XML 원문과 conf deprecated 키를 함께 가진다. `Change.Telegraf`(XML 변경) 또는 `Applied`의 telegraf 키 변경이 있으면 `cmd/scouter-go`의 OnChange가 `Runtime.ReloadTelegraf`를 불러 소비자(`internal/telegraf`)가 `Snapshot.Telegraf()`와 `Snapshot.TelegrafDeprecated()`로 **매번 처음부터** 합성한다(XML measurement > deprecated measurement, XML 전역 > conf 전역). config는 합성하지 않고 다음만 제공한다.

- `Snapshot.Telegraf() TelegrafFile{Path, Exists, Text, ModTime}`: 원문(4 MiB 상한)과 mtime. 파일 없음은 `Exists=false`이며, Java는 이때 GET에 sample을 돌려준다(sample은 소비자 몫). 읽기 실패는 이전 원문을 유지하고 로그를 남긴다. XML 문법 검증은 하지 않는다.
- `Snapshot.TelegrafDeprecated() map[string]string`: 파일에 있는 `input_telegraf_*`와 `telegraf_object_deadtime_ms`(trim·`${}` 적용 후).
- `Store.SaveTelegrafText(text)`: `SaveText`와 같은 원자적 교체. Java `saveTgConfigContents`처럼 내용 검증을 하지 않는다. 적용은 `Reload(true)`.
- telegraf 키는 재기동 필요 키가 아니다. 초기 로드 이후 바뀌면 `Applied`로 보고된다.

### 5.2 live 설정 전달(F0)

- `Snapshot.Live() Live`: live 키의 typed 묶음. `getInt`/`getBoolean`/`getValue`와 applyConfig 기본값(`object_zipkin_deadtime_ms` 300000)을 따른다. `Live.Effective()`는 실제 적용 값 문자열(LIST overlay용). `DefaultLive()`는 빈 설정의 값.
- Go 정규화(의도된 차이): `xlog_sampling_matcher_*` 5개가 0 이하이면 기본값을 쓴다. Java는 count가 무제한 map, millis·secs는 0 나눗셈·`NoSuchElementException`이 되는 원본 취약 경로다. dead time·alert level·keepalive·wait는 Java처럼 그대로(음수 dead time은 즉시 inactive, wait ≤ 0은 대기 없음).
- 전달: `cmd/scouter-go`의 `OnChange`가 `Runtime.ApplySettings(store.Current().Live())`를 호출하고, `Runtime.Open`이 기동 Snapshot으로 한 번 호출한다. `ApplySettings`는 원자 저장, 등록된 setter(원자 저장만), 크기 1 channel 신호만 한다. reload lock 안에서 I/O를 하지 않는다. 파일 I/O가 있는 작업(GeoIP 파일 재로드)은 `Run`이 띄우는 owner goroutine이 한다(Java ConfObserver는 reload lock 안에서 로드 — 의도된 차이). 여러 변경은 owner의 한 번 실행으로 합쳐진다.
- flag 고정: `Options.Pinned`(main은 명시 flag → 키 map, `flagKeys`·`--mgr-purge-*`)의 키는 `Live()`가 flag 값을 쓴다. 파일에서 그 키가 처음 바뀔 때 한 번 `config: <key> changed ...; it is fixed by a command-line flag` 로그를 남긴다. `Snapshot.Pinned()`로 조회.
- LIST_CONFIGURE_SERVER: live 키는 현재 Snapshot의 `Live().Effective()`로 덮어 보인다(정규화·flag 고정 반영). `mgr_purge_enabled`는 Go 결정(키가 없으면 off)을 반영한 값이다.
- 기동 순서: `Runtime.Open`이 모든 소비자를 만든 뒤 `wireSettings` → `ApplySettings(기동 Live)`를 한 번 부르고 그 뒤에 Run이 listener를 연다. 첫 적용은 모든 값을 검사하고, 이후 적용은 바뀐 값만 다시 검사·로그한다.
- scouter.conf가 없을 때(`Config.ConfigFile == nil`): reload가 없으므로 retention·logger·webapp·LDAP setter는 등록하지 않고 `Config` 값(`Retention`, main의 logger 설정, `ScouterHTTP.Webapp`, `LDAP`)을 그대로 쓴다. 나머지 소비자는 `DefaultLive()`에 `Config`의 `ServerID`·`LogDirectory`·`Compress*`·`ObjectDeadTime`을 덮은 값을 받는다(`initialSettings`).

### 5.3 live 키 소비자(`internal/server/wiring.go`, `settings.go`)

| 키 | 소비자 API | 반영 시점 |
|---|---|---|
| `server_id`, `ext_link_*`, `net_tcp_client_so_timeout_ms`(so_time_out), `tagcnt_enabled`(menu), `xlog_*_lower_bound_ms`, `req_search_xlog_max_count` | `service.Service.SetSettings` | 다음 Client 요청(LOGIN 등) |
| `log_dir` | `service.FileMetadata.SetLogDir`, `logger.Logger.SetConfig` | 다음 SERVER_LOG_* 요청, 다음 로그 줄(이전 파일은 owner가 닫음) |
| `net_tcp_client_so_timeout_ms`, `net_tcp_agent_so_timeout_ms` | `transport.TCPServer.SetClientSoTimeout`/`SetAgentSoTimeout` | 이후 accept한 Client 연결 / 이후 등록한 Agent 연결. 음수는 거절·로그·이전 값 유지 |
| `net_tcp_service_pool_size` | `TCPServer.SetServicePoolSize` | 즉시. 1 미만은 거절·로그. `--max-connections`보다 크면 경고 로그 |
| `log_tcp_action_enabled`, `log_udp_multipacket`, `log_expired_multipacket` | `TCPServer.SetActionLog`, `UDPReceiver.SetLogUDPMultipacket`/`SetLogExpiredMultipacket` | 로그 지점마다. 줄은 `SetLogFunc`→logger(`Println`/`PrintlnID`), objName은 `UDPReceiver.SetObjectNameFunc(ingest.Engine.ObjectNameCached)`(engine lock 없이 읽는 cache) |
| `net_http_api_cors_*`, `net_http_api_auth_*`, `net_http_api_allow_ips`, `net_webapp_tcp_client_pool_timeout` | `webapp.Server.SetSettings` | 다음 webapp 요청. pool timeout 키가 없으면 `net_tcp_client_so_timeout_ms`(`Snapshot.Value` 파생) |
| `account_use_ldap`, `account_ldap_*` | `account.Manager.SetLDAP` | 다음 LOGIN/CHECK_LOGIN. 값이 같으면 호출하지 않음 |
| `mgr_text_db_daily_*`, `compress_*_enabled`, `visitor_hourly_count_enabled`, `_mgr_xlog_id_index_mb`, `log_index_traversal_warning_count`, `_compress_read_cache_block_count`, `_compress_dailycount_header_cache_size` | `storage.Store.SetTextDaily`/`SetCompression`/`SetVisitorHourlyCount`/`SetXLogIDIndexMB`/`SetIndexTraversalWarningCount`(+`Counters`)/`SetCompressReadCacheBlocks`/`SetCompressDailyCountCacheSize` | storage CONTRACT §1. S152/S161 줄은 `Config.Log`/`Counters.SetLog`→logger `PrintlnID`(간격은 storage가 적용) |
| `mgr_purge_enabled`, `mgr_purge_*` | `storage.Retention.Apply(enabled, JavaRetentionConfig(Live.Purge))` | 다음 retention cycle. runtime은 Retention과 ticker를 항상 두고 꺼져 있으면 `RunOnce`가 아무것도 하지 않는다. 값이 바뀌면 로그 1줄 |
| `log_rotation_enabled`, `log_keep_days`, `_trace`, `mgr_log_ignore_ids` | `logger.Logger.SetConfig` | 다음 로그 줄·다음 정리 주기 |
| `sql_table_parsing_enabled`, `log_sql_parsing_fail_enabled` | `sqltable.Options.Enabled`/`LogFailEnabled`(runtime의 현재 Live를 읽는 함수) | sql TextPack마다 / 파싱 실패마다 |
| `log_udp_*` 13개 | `ingest.Config.PackLog` → `logger.UDPLog`/`FormatPack` → `Logger.Stdout` | pack마다. `log_udp_packet`은 dispatch 전, 나머지는 처리 뒤(주소·counter objHash/time이 채워진 pack). sql·login Text는 마스킹(logger D-S1) |
| `log_udp_some_object` | `ingest.Config.ObjectTypeLog`, runtime `addObjectTypeIfNotExist` | ObjectPack마다 |
| `log_test_rate` | `ingest.Config.ProfileTestLog` → `Logger.Println(ProfileTestLine)` | ProfilePreCore 분기마다 `rand.IntN(100) < rate` |
| (기동) `net_udp_packet_buffer_size`, `_net_udp_worker_thread_count`, `_mgr_*_index_*_mb` | `server.Config.UDPMaxDatagram`/`UDPWorkers`(`--udp-workers` 우선)/`IndexSizes` | 파일에 키가 있을 때만. 조정되면(`UDPPacketBufferSize`/`UDPWorkerCount` adjusted, `*_mb` 범위 밖) 기동 로그 |

- 원본 TextCore 순서: ingest가 xtype `sql` TextPack을 `PutText`보다 먼저 `OnSQLText`(`sqltable.Tables.Add`, 막지 않음)로 넘긴다. Sink는 `Store.PutText(date, {XType:"table"})`(영구 `text_table`), Source는 `Store.TextContext`다. Go의 text 저장은 원본 TextWR처럼 비동기다(writer goroutine, 대기 중인 text는 TextCache처럼 바로 조회된다. `internal/storage/CONTRACT.md` §8.2). 그래서 queue2 항목도 대개 저장소에서 찾는다.
- RequestLogger: runtime의 Client handler wrapper(transport가 session을 검사한 뒤, service handler 전)에서 `Logger.RequestCommand(cmd, session, lookup)`, lookup은 `service.Service.LoginUser`(LOGIN의 id·ip). AgentCall 등록은 agent pool이 세션을 얻은 뒤 명령을 쓰기 직전 `Logger.RegisterRequestCommand`.
- logger 수명: `cmd/scouter-go`가 기동 초기(`--timezone` 해석 직후)에 `logger.New`, `log.SetOutput(lg.StdLogWriter(os.Stderr))`, `server.Config.Logger`로 전달하고, Run·reload 감시가 끝난 뒤 마지막에 `Close`한다. runtime은 logger를 닫지 않는다. SqlTables는 runtime이 engine을 닫은 뒤 닫는다(`sqltable=` 종료 로그).

## 6. flag ↔ scouter.conf (우선순위 flag > conf > Go 기본값)

flag를 명시(`flag.Visit`)하지 않았고 conf(파일 또는 환경변수 fallback)에 키가 있을 때만 conf 값을 쓴다. conf에 없는 키는 **Go flag 기본값**(Java 기본값과 다를 수 있음)이다. 명시 flag는 §5.2의 `Options.Pinned`로 고정된다.

| conf 키 | flag | 비고 |
|---|---|---|
| `server_id` | `--server-id` | 둘 다 없으면 host 이름. live(flag 고정) |
| `db_dir` | `--db` | |
| `net_udp_listen_ip` + `net_udp_listen_port` | `--udp` | 한쪽만 있으면 나머지는 flag 기본값(127.0.0.1 / 6100). Java 기본 0.0.0.0과 다름 |
| `net_tcp_listen_ip` + `net_tcp_listen_port` | `--tcp` | 동일 |
| `net_udp_so_rcvbuf_size` | `--udp-read-buffer` | 파일에 없으면 Java 기본과 같은 4 MiB(`DataUdpServer`는 키가 없어도 4 MiB를 건다). 0은 OS 기본. OS 상한(Linux `net.core.rmem_max`)이 더 작으면 Java처럼 그 값으로 줄어든다 |
| `log_dir`, `plugin_dir` | `--log-dir`, `--plugin-dir` | `log_dir`은 live(flag 고정), `plugin_dir`은 재기동 |
| `_net_udp_worker_thread_count` | `--udp-workers` | 파일에 있을 때만. 없으면 Go 기본 2(Java 3과 다름, 의도: Go worker는 engine lock 하나로 직렬화되어 3개째는 lock 전달만 늘린다. 격리 replay 측정(docs/validation/2026-10-17-xlog-max-benchmark.md): 16,000 mix XLog/s에서 UDP queue 최대 2개 223~262, 3개 425~687). 1 미만은 3, 256 초과는 256(로그) |
| `net_udp_packet_buffer_size` | 없음 | 파일에 있을 때만. 65507 초과·1 미만은 65507(로그) |
| `compress_xlog_enabled`, `compress_profile_enabled` | `--compress-xlog`, `--compress-profile` | live(flag 고정) |
| `net_http_server_enabled=true` + `net_http_port` | `--scouter-http` | Java처럼 **0.0.0.0:<port>**에 servlet listener(결정 2026-10-16). Telegraf XML `enabled=true`도 이 listener를 켠다. 허용 IP 검사는 `--scouter-http-check-allow-ips`(Go 전용 opt-in) |
| `net_http_api_gzip_enabled` | `--scouter-http-gzip` | servlet 응답 gzip |
| `net_http_api_allow_ips` | `--scouter-http-allow-ips` | servlet 허용 목록(검사 opt-in) |
| `net_http_api_auth_ip_header_key` | `--scouter-http-auth-ip-header` | |
| `log_keep_days` | `--log-keep-days` | server/request log 정리(live, flag 고정)와 HTTP request log 보관(기동) |
| `net_http_api_enabled` | `--webapp` | webapp REST API(servlet listener 필요) |
| `net_http_api_cors_*`, `net_http_api_auth_*`, `net_http_api_session_timeout`, `net_webapp_tcp_client_pool_size/timeout` | 없음 | webapp 설정. `net_http_api_session_timeout`·`net_webapp_tcp_client_pool_size`는 기동 시, 나머지는 live(§5.3) |
| `mgr_purge_enabled` | `--purge` | conf에 키가 있을 때만 사용. **키가 없으면 Go 기본 off**(Java 기본 true와 다름: 삭제 동작은 opt-in). live(flag 고정) |
| `mgr_purge_*` | `--mgr-purge-*` | conf 값 위에 flag 값을 덮어 `storage.JavaRetentionConfig`. `--mgr-purge-*`/`--purge-interval`은 `--purge` 또는 `mgr_purge_enabled=true` 필요 |
| `ext_plugin_pulse_*` | `--pulse-conf` | `--pulse-conf`가 없고 conf에 Pulse 키가 하나라도 있으면 같은 파일로 Pulse 구성. `--pulse-kafka-acks`는 공통 |
| `account_use_ldap`, `account_ldap_*` | 없음 | `account_use_ldap=true`면 LOGIN이 LDAP bind(계정 파일은 목록·정책 유지). 기동 값은 `account.Config.LDAP`, 이후 live(`SetLDAP`) |
| `object_deadtime_ms` | 없음 | `server.Config.ObjectDeadTime`(Snapshot 없을 때)과 live 값 |
| live 키(§5) | 없음 | `Snapshot.Live()` |
| `go_maxprocs` (Go 전용) | 없음 (환경 변수 `GOMAXPROCS`, 배포 스크립트 `SCOUTER_GOMAXPROCS`) | Go runtime이 동시에 Go 코드를 실행하는 CPU 수, live. 키가 없거나 0 이하면 기동 값(환경 변수 `GOMAXPROCS`, 없으면 runtime 기본값: 논리 CPU 수, cgroup CPU 제한이 있으면 그 값)이다. 키를 지우면 기동 값으로 돌아간다. Configure field가 아니라 LIST_CONFIGURE_SERVER에는 나오지 않는다. 근거와 trade-off는 `docs/performance-structural-review-2026-10-20.md` §10 |
| `go_*` 자원 상한 (Go 전용, 아래 표) | 없음 | `config.GoLimits`(`Snapshot.GoLimits()`, `Live.Limits`). 알려진 키라 unknown 경고 대상이 아니고 LIST_CONFIGURE_SERVER에 나오지 않는다. 우선순위는 키 > Go 기본값(같은 값을 정하는 flag는 없다). 값 0은 `go_maxprocs`처럼 조용히 기본값이다. 그 밖에 정수가 아니거나 범위 밖이면 파일을 읽거나 그 키가 바뀔 때 `config: <key>="v" is not an integer in a..b; the Go default applies`를 한 번 남기고 기본값을 쓴다(기동 실패 아님). 실행 중 재기동 키를 잘못된 값으로 바꾸면 `...; restart required, the Go default applies after a restart`이며 실행 값은 그대로다. 환경 변수 fallback(Java `System.getenv` 병합)도 적용된다. scouter.conf가 없을 때도 Go는 환경 변수를 읽는다(Java는 파일이 없으면 env를 합치지 않는다, 기존 차이). 이때 잘못된 값도 기동 때 경고한다. 재기동 키는 Store가 기동 값을 유지하고 변경 시 "restart required"를 남긴다 |

| 키 | 단위 | 기본 | 범위 | 반영 | 적용 위치 |
|---|---|---|---|---|---|
| `go_http_max_connections` | 연결(listener별) | 1024 | 16..65536 | 재기동 | `--http`·servlet listener 연결 상한. open file 한도 맞춤(`DB 예비분/4 − 64`)이 더 작으면 그 값으로 낮추고 로그. over-limit 연결 64는 고정 |
| `go_http_servlet_max_inflight` | 요청 | 64 | 1..1024 | 재기동 | `/counter`·`/register`·`/telegraf` 동시 처리(`httpapi.Config.MaxInFlight`) |
| `go_http_servlet_queue_wait_ms` | ms | 3000 | 1..60000 | 재기동 | servlet 빈자리 대기 뒤 503(`httpapi.Config.QueueWait`) |
| `go_http_webapp_max_inflight` | 요청 | 64 | 1..256 | 재기동 | `/scouter/*` 동시 처리 |
| `go_http_webapp_queue_wait_ms` | ms | 3000 | 1..60000 | 재기동 | webapp 빈자리 대기 뒤 503 |
| `go_http_ops_max_inflight` | 요청 | 16 | 1..1024 | 재기동 | `/health`·`/debug/pprof` 동시 처리(대기 없음) |
| `go_agent_relay_budget_mb` | MiB | GOMEMLIMIT/4(최소 768), 없으면 1024 | 768..1048576 | live | Agent relay 전역 byte 예산. 줄이면 들고 있는 permit은 유지하고 새 relay만 거절. 큰 relay는 예산보다 크면 예산 크기로 잡아 혼자 돈다 |
| `go_agent_relay_max_large` | relay | 2 | 1..64 | live | 256MiB relay(`OBJECT_THREAD_DUMP`·`OBJECT_HEAPHISTO`·`OBJECT_DOWNLOAD_JAR`) 동시 수 |
| `go_agent_relay_server_wait_ms` | ms | 10000 | 1..60000 | live | 대기열의 서버 발신 호출(kube 순번 push, CPU dump trigger)의 relay 예산 대기 |
| `go_client_response_budget_mb` | MiB | GOMEMLIMIT/4(최소 257), 없으면 1024 | 257..1048576 | 재기동 | Client 응답 전역 byte 예산(`transport.TCPConfig.MaxClientResponseBytes`) |
| `go_pulse_xlog_queue_mb`, `go_pulse_profile_queue_mb` | MiB | 16, 64 | 1..4096 | 재기동 | Pulse queue byte 상한(`pulse.Options.MaxXLogQueueBytes`/`MaxProfileQueueBytes`). Pulse 키와 같이 재기동 |
| `go_text_cache_reset_parallel` | 호출 | 16 | 1..64 | live | `OBJECT_RESET_CACHE` 동시 호출 수, 다음 pass·재시도 round부터 |
| `go_text_max_unknown_divs` | div | 64 | 1..4096 | live | 원본 17개(`TextTypes`) 밖 div의 영구 text 사전 수(`storage.Store.SetMaxUnknownTextDivs`, R7). 다음 text부터. 줄여도 이미 있는 div는 계속 쓴다 |
| `go_text_cache_mb` | MiB | 128 | 16..8192 | 재기동 | storage text 읽기 cache byte 상한(`storage.Config.TextCacheBytes`). XLog마다 하는 service·object·thread 이름 조회가 파일(Store gate, page cache)이 아니라 이 cache에서 답을 얻는다. `/health` `textCache.Evicted`가 계속 늘고 `FileReads`가 많으면 늘린다 |
| `go_visitor_resident_mb` | MiB | 256 | 1..16384 | live | 통째로 메모리에 두는 visitor HLL(resident) 예산(`storage.Store.SetVisitorBudgets`). 줄이면 다음 적용 시점(방문, roll tick, `FlushVisitorsIfDue`. 보통 1초 이내)에 오래 안 쓴 entry부터 partial로 강등한다. 강등하면 delta 예산을 넘게 되는 dirty entry는 다음 pass가 쓴 뒤 강등한다. 늘리면 다음 flush pass부터 승격한다. 파일 bytes는 같고 메모리·flush IO·realtime 조회 IO만 바뀐다 |
| `go_visitor_delta_mb` | MiB | 64 | 16..1024 | live | flush를 기다리는 visitor register(delta) 예산. 넘으면 방문을 수집 경로에서 바로 쓴다(write-through). 지금 delta보다 작게 바꾸면 flush를 바로 due로 표시한다 |

0은 모든 키에서 "기본값" 표식이라 유효 값은 1 이상이다(대기 없이 거절하려면 1ms). live 키를 지우면 기본값으로 돌아간다.

범위 근거:
- `go_agent_relay_budget_mb` 하한 768은 `agentRelayBudgetMin`(큰 relay 2개 512MiB + 일반 relay 32개 256MiB)과 같다. 기본 크기 규칙이 보장하는 값보다 작게 설정할 수 없다.
- `go_client_response_budget_mb` 하한 257은 transport의 Client 응답 item 하나 상한(`clientPendingLimit`, `DefaultClientResponseBytes`의 하한)과 같다.
- `go_text_cache_reset_parallel` 상한 64: reset 호출 하나는 `OBJECT_RESET_CACHE`의 `agentPackLimits` TotalBytes(일반 relay 8MiB)를 permit으로 잡는다. 64 × 8MiB = 512MiB는 relay 예산 하한 768MiB의 2/3이라 reset pass 중에도 Client relay 몫(256MiB 이상)이 남는다. 기본 16은 128MiB(1GiB 예산의 1/8)다.
- `go_http_servlet_max_inflight` 상한 1024: servlet 요청 메모리는 대략 MaxInFlight × 3 × MaxBodyBytes(1MiB)라(`httpapi` `DefaultMaxInFlight` 주석) 1024면 최악 약 3GiB다. 기본 64는 약 192MiB다.
- `go_http_webapp_max_inflight` 상한 256: webapp 요청 body는 최대 4MiB이고(256 × 4MiB = 1GiB) collector 호출은 webapp pool 12개를 나눠 써서 동시 요청을 더 늘려도 pool 대기만 길어진다.
- `go_text_max_unknown_divs` 상한 4096: 미지 div 하나는 `00000000/text/text_<div>` 영구 사전(hash `_mgr_text_db_index_default_mb` 기본 1MiB + key·data)이라 4096이면 최악 약 4GiB 이상이고 retention 대상도 아니다. Java처럼 무제한은 둘 수 없다(go_* 키는 1 이상). 기본 64의 근거는 [storage-contract](../../docs/compatibility/storage-contract.md) "영구 Text div 상한".
- `go_text_cache_mb`: 항목 하나는 text 길이 + key + 96B로 계상한다(실제 heap은 map·문자열 header로 약 1.5~2배). 하한 16은 이전 고정값(16MiB)이고, 상한 8192는 text가 수천만 개인 날짜 사전 전체에 해당한다. 기본 128은 service 이름 길이 수십 byte 기준 약 100만 개다.
- `go_visitor_resident_mb`: 하한 1MiB에는 dense HLL(약 683KiB)이 하나만 들어간다. 0은 기본값 표식이라 resident를 완전히 끌 수는 없다. 상한 16384MiB는 Java 최대치(dense 1100개, 약 733MiB)의 20배가 넘고, object 4000개의 daily·hourly HLL이 모두 dense여도(약 5.3GiB) 담을 수 있다. 이론상 최대(objType 100 + object 32768 × 2가 모두 dense, 약 44GiB)까지 열어 두면 공유 호스트에서 잘못 설정할 위험이 커서 막았다. 크게 줄일 때는 단계적으로 줄인다. 강등한 dirty entry의 delta가 `go_visitor_delta_mb`를 넘게 되면 그 entry는 강등하지 않는다. flush pass(due로 표시)가 그 entry를 쓴 뒤 강등하므로, 그 entry가 쓰이고 강등될 때까지 resident가 예산을 넘는다. 상한은 resident entry 수 × 약 683KiB다. 쓰기가 실패하면 10초 tick마다 다시 시도한다([storage CONTRACT](../storage/CONTRACT.md) §8 visitor). 값은 reload 때 atomic으로 저장만 하고, visitor state가 다음 방문이나 tick에 적용한다.
- `go_visitor_delta_mb`: 평소 delta는 10초치 방문으로 수 MiB이다. 하한 16은 그보다 작으면 상시 write-through(방문마다 page read-modify-write 동기 IO)가 되기 때문이다. 상한 1024는 flush가 멈췄을 때 쌓일 수 있는 메모리 상한이다. 그 위로 올려도 backpressure(write-through) 시작만 늦어진다.
- object entry 상한(`visitorObjectsMax`, 32768)은 키로 노출하지 않는다. 실행 중에 줄이면 다음 새 entry 방문 하나가 초과분 전부를 수집 경로에서 동기로 쓴다. partial entry가 약 200B라 메모리 효과도 작다.
- `go_agent_relay_server_wait_ms` 상한 60000: 서버 발신 호출(kube 순번 push, CPU dump trigger)은 대기열(kube 1024, CPU 100)마다 worker 하나가 차례로 보내므로, 대기 하나가 그동안 같은 대기열의 뒤 호출을 모두 막는다. 1분을 넘는 대기는 대기열 포화(`Dropped`)를 부른다.

`--timezone`, `--user/--password`, `--max-connections`, `--udp-workers/queue`, `--sync-interval`, `--telegraf-config`, `--stats-interval`, `--account-dir`은 Java conf 키가 없어 flag 전용이다(`input_telegraf_config_file`은 Java도 적용하지 않는다).

## 7. Client 명령 연결(`internal/server`)

- `server.Config.ConfigFile *config.Store`가 있으면 `runtimeMetadataWith(cfg, store, ...)`의 `confMetadata`가 서버 설정 명령을 맡는다. 없으면 flag 행 목록만 보인다.
- `GET_CONFIGURE_SERVER`: `configKey`=Keys 이름 목록, `serverConfig`=`Store.LoadText()` 원문(없으면 `""`, Java `loadText` null→`""`).
- `LIST_CONFIGURE_SERVER`/`CONFIGURE_DESC`/`CONFIGURE_VALUE_TYPE`/`CONFIGURE_VALUE_TYPE_DESC`: `Snapshot.Properties()`. 기동 때 소비한 키는 실제 실행 값(flag 우선·재기동 대기 반영), live 키는 `Live().Effective()`로 덮어 보인다. `ValueTypeDesc` MapValue entry 순서는 Java `Hashtable(11)` 열거 순서 `strings, ints1, strings3, booleans, strings2, strings1, ints, booleans1`(JDK 8·17 oracle 동일).
- `SET_CONFIGURE_SERVER`: `ServerConfigFile.SaveConfigurationText(ctx, "server", text) error` → `SaveText` → `Reload(true)`(→ OnChange → live 적용). nil=Java `"true"`. 저장 후 reload 문제(잘못된 `\u`, 재기동 키)는 로그만 남기고 성공.
- `SaveText`: 같은 디렉터리 임시파일 → write → chmod(기존 권한 유지, 신규 0644) → fsync → rename(symlink면 최종 대상 교체) → 디렉터리 fsync(best effort). 부모 디렉터리 생성(Java mkdirs). 4 MiB 초과 거절. Java `saveText`는 비원자 덮어쓰기(의도된 차이).

## 8. 미구현·남은 차이

- Java ConfObserver 효과 중 retention 이력 clear는 Go에 대응이 없다(Go는 이력을 두지 않고 매 cycle 디스크를 다시 나열한다, storage CONTRACT §4). compress thread 구조는 없다(구조 차이).
- servlet listener(`internal/httpapi`)의 `net_http_api_allow_ips`·`net_http_api_auth_ip_header_key`·`net_http_api_gzip_enabled`·HTTP request log 보관은 기동 값이다. 같은 키의 webapp 쪽은 live다.
- `ingest`의 GeoIP 오류 로그 등 Go 표준 `log` 줄은 `StdLogWriter`를 거쳐 ID 없이 server log에 남는다(`mgr_log_ignore_ids`로 거를 수 없다).
- `log_udp_some_object`의 `new XML =` 줄은 Java `XmlUtil.writeXmlFileWithIndent` 결과 대신 Go가 저장한 counters.site.xml 원문이다.
- `log_test_rate`의 서비스명은 TextCache 없이 저장소에서 읽는다(engine lock 안 디스크 읽기, 디버그 키라 허용).
- 표시 전용 키 목록은 §5 표, 원본 근거는 config-behavior.md §5.2.
- telegraf 매핑·합성(`applyTelegrafInputConfig*`)과 `SET_CONFIGURE_TELEGRAF` 연결은 `internal/telegraf` 담당이다. config는 §5.1의 원문·mtime·deprecated map·저장만 제공한다.
- `getKeyValueInfo`의 비설정 field(`telegrafOriginalConfig` 등) 노출은 재현하지 않는다.

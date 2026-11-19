# account 계약

정본은 Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 다음 소스다.

- `scouter.server/src/main/scala/scouter/server/account/{AccountManager,AccountFileHandler,GroupFileHandler}.scala`
- `scouter.server/src/main/scala/scouter/server/util/XmlUtil.scala`
- `scouter.server/src/main/resources/scouter/server/account/{account,account_group}.xml` (`defaults/`에 byte 동일 복사)
- `scouter.server/src/main/scala/scouter/server/netio/service/handle/{AccountService,LoginService}.scala`, `server/LoginManager.scala`
- `scouter.common/src/main/java/scouter/lang/Account.java`, `scouter/util/CipherUtil.java`, `scouter/net/NetConstants.java`
- `scouter.client/src/scouter/client/net/LoginMgr.java`, `client/popup/AccountDialog.java`, `client/configuration/views/{AccountGroupPolicyView,AccountListView}.java`

Wire 처리(`internal/service/account_commands.go`)는 [service 절](#client-명령)에 정리한다.

## 파일과 로드

| 항목 | Java | Go |
|---|---|---|
| 위치 | `Configure.CONF_DIR`(`./conf/`) + `account.xml`, `account_group.xml` | `Config.Dir` + 같은 이름 |
| 없을 때 | `canRead()==false`이면 classpath resource를 4회까지 복사 | 파일이 **없을 때만** embed 기본 파일을 임시 파일+rename으로 생성(mode 0600) |
| 존재하지만 읽기 불가 | `FileOutputStream`으로 기본 파일을 덮어씀 | 덮어쓰지 않고 오류 (의도된 차이: 사용자 계정 파일 보존) |
| 시작 시 parse 실패 | 예외 출력 후 빈 map으로 계속 | `Open` 오류 (의도된 차이: 아무도 로그인 못 하는 상태로 기동하지 않음) |
| 순서 | group 파일 → account 파일 | 동일 |
| reload | 5초 daemon, `lastModified` 변화 시 재파싱. 실패하면 이전 map 유지, stamp 미갱신이라 5초마다 재시도·재로그 | `Run(ctx)`가 `ReloadInterval`(기본 5s)마다 (mtime, size) stamp 비교. 실패 시 이전 snapshot 유지, 같은 stamp의 실패는 한 번만 로그 |
| 삭제된 파일 | 다음 tick에 기본 파일 재생성 | 동일. account.xml이면 공개된 기본 admin/guest 비밀번호가 다시 유효해진다는 `WARNING` 로그를 남긴다 |
| 크기 | 무제한 | `MaxFileBytes`(기본 4 MiB), regular file만, XML depth 64. 크기·파일 종류 거절도 parse 실패와 같이 stamp를 기록해 같은 stamp는 1회만 로그 |

parse 규칙:

- `Account`: 문서 전체의 `<Account>`(document order, 중첩 포함). `id/pass/group` attribute(없으면 `""`), 첫 하위 `<Email>`의 textContent. `<Email>`이 없으면 Java는 `null`이라 `LIST_ACCOUNT`가 `Account.toBytes` NPE로 끊긴다. Go는 모든 null field를 `""`로 정규화해 목록이 끊기지 않는다(의도된 차이, management-commands §9-12). ADD/EDIT 요청의 null `pass/email/group`도 `""`(Java DOM 출력도 `pass=""`, `<Email/>`로 같다).
- 같은 id/group name이 반복되면 `StringKeyLinkedMap.put`처럼 **첫 위치, 마지막 값**.
- `Group`: `<Group name>`의 첫 하위 `<Policy>`의 child element. `<Policy>`가 없으면 Java parse 전체가 NPE로 실패하므로 Go도 파일 전체를 거절한다. 값은 Scala `String.toBoolean`(대소문자 무시 `true`/`false`, 공백 불허), 그 외는 로그 후 건너뜀. 같은 policy 이름은 첫 위치, 마지막 값(Hashtable.put).
- XML: Xerces 비 namespace DOM과 같이 prefix를 이름 그대로 보존(`xs:attribute`), 인접 text는 `normalize()`처럼 병합, root 밖 공백은 버리고 comment/PI는 보존. UTF-8 BOM 허용. DOCTYPE/entity 선언과 UTF-8 이외 encoding 선언은 명시적으로 거절한다(원본 파일에는 없다).

## 쓰기와 직렬화

**원본 결함**: `XmlUtil.writeXmlFileWithIndent`는 commit `cc9bda37`(2022-11-02) 이후 `StreamResult(StringWriter)`로 문자열만 반환하고 `file`에 쓰지 않는다. 고정 원본에서 `ADD_ACCOUNT`/`EDIT_ACCOUNT`/`ADD_ACCOUNT_GROUP`은 메모리 map만 바뀌어 재시작 시 사라지고, `EDIT_GROUP_POLICY`는 직후 `loadGroupFile()`이 변경 전 파일을 다시 읽어 `true`를 응답하면서 효과가 없다. Go는 cc9bda37 이전 동작(파일 기록)을 따른다. 이것이 의도된 차이다.

- 모든 load/write는 `Manager.mu`로 직렬화하고, 조회는 `atomic.Pointer` snapshot을 읽는다. 쓰기 순서: 현재 파일 parse → DOM 변경 → 직렬화 → 직렬화 결과 재parse(실패 시 미기록) → 같은 디렉터리 임시 파일 write+fsync+chmod → rename → 디렉터리 fsync → stamp 갱신 → 재parse 결과로 snapshot 교체. 따라서 자신의 쓰기는 reload를 일으키지 않고, 메모리 상태는 재시작 시 load할 내용과 같다.
- 기존 파일의 permission bit를 유지한다. symlink/비정규 파일은 읽기는 허용하되 교체를 거절한다(`os.Rename`이 link를 일반 파일로 바꾸는 것을 방지).
- 중복 확인(`Add`의 id 존재, `Edit`의 id 부재)은 Java처럼 메모리 map이 아니라 **방금 읽은 파일** 기준이다. 외부 편집 직후에도 파일과 어긋나지 않는다.
- 같은 id가 파일에 반복된 경우 Java `editAccount`는 첫 element를 고치고 메모리 map은 수정값을 보이지만, Go snapshot은 재parse 결과(마지막 element 값)를 보인다. 재시작 후 Java와 같은 값이다.

직렬화 목표는 **JDK 8** `com.sun.org.apache.xalan.internal.xsltc.trax.TransformerFactoryImpl`, `indent=yes`, `indent-amount=2` 출력의 UTF-8 byte다(`xmldoc.go`).

- 선언 `<?xml version="1.0" encoding="UTF-8" standalone="no"?>` + newline. 원본 선언이 `standalone="yes"`이면 standalone 없이 newline 없이 쓴다.
- attribute는 Xerces `NamedNodeMap` 순서인 이름의 UTF-16 사전순(`group, id, pass`).
- 기존 whitespace text node는 그대로 쓰고, text 바로 뒤의 element/comment/PI에는 indent를 넣지 않으며, text가 앞서지 않은 위치에만 `\n` + depth×2 공백을 넣는다. 그래서 tab 들여쓰기 원본 끝에 추가한 element는 `<Account …>`가 줄 맨 앞에 오고 내부는 2-space 들여쓰기가 된다. 반복 저장해도 빈 줄이 늘지 않는다.
- escape: text는 `& < >`, CR, U+007F–U+009F, BMP 밖 문자를 `&…;`/`&#n;`으로, attribute는 `& < > "`, TAB/LF/CR, BMP 밖 문자를 escape한다. 마지막에 newline 하나.

JDK 9+ Xalan은 기존 whitespace text 앞뒤에도 indent를 추가해 저장할 때마다 공백 줄이 누적된다(JDK 17 실행으로 확인). 두 출력을 동시에 만족할 수 없어 JDK 8을 고정 목표로 삼았다. JDK 9+에서 실행하던 Java 서버의 출력과 byte 동일하지 않다(어차피 고정 원본은 파일을 쓰지 않는다). 두 형식 모두 Java `DocumentBuilder`로 같은 계정/정책을 읽는다.

알려진 직렬화 차이(파일을 손으로 편집했을 때만 발생):

- CDATA section은 escape된 text로 다시 쓴다(의미 동일).
- attribute 값 안의 literal TAB/LF/CR은 XML 1.0 attribute-value normalization(공백 치환)을 하지 않고 그대로 유지해 `&#9;` 등으로 쓴다. Xerces는 공백으로 읽는다.
- element 이름 검증은 XML 1.0 5판 Name 규칙이다. Xerces는 4판 문자표를 쓰므로 희귀 Unicode 이름에서 판정이 다를 수 있다.

값 검증(의도된 차이, 모두 `false` 응답 + 로그, 파일 미변경):

- `Account` 필드 255 byte 초과: `Account.toBytes`는 Java 플랫폼 기본 charset(`String.getBytes()`)으로 encode하고 Go는 UTF-8 고정이다(UTF-8 기본 charset JVM에서 동일, 그 외 charset의 비 ASCII 값은 byte가 다르다). Go 기준 UTF-8 255 byte 초과일 때 Java는 저장하고 `LIST_ACCOUNT`에서 길이 byte가 wrap된 손상 record를 보낸다. Go는 추가/수정을 거절하고, 손 편집 파일에 이미 있으면 `LIST_ACCOUNT` 전체를 `ErrLimit`로 실패시킨다(부분 목록 없음).
- XML 1.0에 없는 문자(제어 문자, 잘못된 UTF-8): Java는 DOM에 넣어 다시 읽을 수 없는 문자열을 만든다. Go는 거절한다.
- policy 이름이 XML Name이 아니면 Java도 `createElement`의 `DOMException`으로 `false`다(oracle 확인).

## 그룹 변경 의미

- `AddGroup(name, policy)`: `policy`를 **받은 순서**(service는 wire 순서)로 `<Group name><Policy><X>true|false</X>…`를 첫 `<Groups>` 끝에 붙인다. Java는 MapValue(Hashtable)의 열거 순서로 쓰므로 원소 순서가 다를 수 있다(의도된 차이, 의미 동일). 값은 `MapValue.getBoolean`(Boolean 아니면 false). Java처럼 같은 이름을 거절하지 않는다: `<Group>`이 하나 더 생기고 map은 첫 위치·마지막 값이다. `<Groups>`가 없으면 `false`.
- `EditGroupPolicy(edits)`: MapPack key 순서로(group 안의 policy는 받은 순서, Java는 Hashtable 순서), group마다 이름이 같은 첫 `<Group>`(live NodeList처럼 매번 다시 조회)의 첫 `<Policy>` 아래 첫 하위 element의 text를 바꾸거나 끝에 새 element를 만든다. 없는 group은 무시하고 `true`. text는 `CastUtil.cString`(아래 service 절). 빈 text는 `<X/>`이며 다음 load에서 boolean이 아니므로 건너뛴다. 쓰기 후 재parse가 Java의 `loadGroupFile()`에 해당한다.

## 인증

- 파일 모드: `account.password.equals(pass)` 문자열 비교(Go는 constant-time). Client가 보내는 값은 보통 `CipherUtil.sha256(raw)`(salt `qwertyuiop!@#$%^&*()zxcvbnm,.` + raw의 SHA-256 소문자 hex)이며 raw 비밀번호 로그인(`secureLogin=false`)도 같은 비교다.
- LDAP 모드(`Config.LDAP != nil`, Java `account_use_ldap=true`): 인증만 LDAP이 하고, 목록/추가/수정과 LOGIN 응답의 email은 계속 로컬 파일을 쓴다(Java 동일).
- **live 설정**: Java `authorizeAccount`는 LOGIN/CHECK_LOGIN마다 `account_use_ldap`과 `account_ldap_*`를 Configure에서 읽는다(`AccountManager.scala:145-180`). Go는 `Manager.SetLDAP(*LDAPConfig)`(원자 교체, 사용하는 쪽 정의)로 다음 `Authorize`부터 반영한다. `Config.LDAP`은 초기값이다. `nil`은 `account_use_ldap=false`(파일 인증)다. 같은 설정을 다시 넣으면 기존 authenticator를 유지해 설정 오류 로그가 reload마다 반복되지 않는다. 잘못된 URL·인증 방식은 `Open`과 같이 1회 로그 후 모든 LDAP 로그인을 실패시킨다. 음수 `Timeout`/`MaxMessageBytes`만 오류이며 이때 이전 모드를 유지한다. 진행 중인 `Authorize`는 시작할 때의 설정으로 끝난다. 키 대응: `URL`=`account_ldap_provider_url`, `Auth`=`account_ldap_auth=`(기본 `simple`), `PrincipalDomain`=`account_ldap_principal_domain`, `BaseDN`=`account_ldap_basedn`, `EmailAttr`=`account_ldap_email_id`, `GroupAttr`=`account_ldap_group_id`, `Debug`=`account_ldap_debug`. `Timeout`·`TLSConfig`·`MaxMessageBytes`는 Go 전용이다.

### LDAP (`ldap.go`)

표준 라이브러리만 사용한다. 요청 encode는 `encoding/asn1`(DER은 유효한 BER), 응답 decode는 자체 BER reader다. `encoding/asn1.Unmarshal`은 비최소 길이(Active Directory의 `0x84` 4-byte 길이 등)를 거절하므로 쓰지 않는다.

| 항목 | Java(JNDI `InitialDirContext`) | Go |
|---|---|---|
| URL | `account_ldap_provider_url`, 공백 구분 다중 URL, URL의 DN은 context | 동일. `ldap://`(기본 389), `ldaps://`(기본 636, `crypto/tls`, TLS1.2+, `TLSConfig` 복제). userinfo/query/fragment 거절 |
| 인증 | `account_ldap_auth=`(키 끝 `=` 오타), 기본 `simple` | `LDAPConfig.Auth`. 설정 계층이 오타 키를 그대로 읽는다(읽기 호환). `simple`만 지원, 그 외는 아래 설정 오류 |
| bind | simple bind, name=`id + account_ldap_principal_domain`(미설정이면 `"idnull"`) | LDAPv3 BindRequest, name=`id + PrincipalDomain`(미설정은 `""`). resultCode 49는 단순 실패, 그 외 비 0은 오류 |
| 빈 비밀번호 | 검사하지 않고 그대로 bind | 연결하지 않고 실패(RFC 4513 §5.1.2, 의도된 차이) |
| 빈 id | bind name=domain만, filter `(cn=)` | 연결하지 않고 실패(사용자를 가리킬 수 없음) |
| search | base=`account_ldap_basedn`(+URL DN), SUBTREE, filter 문자열 `"(cn="+id+")"` 미escape | SearchRequest wholeSubtree, derefAliases always, size/time limit 0, typesOnly false, 모든 user attribute, filter는 `equalityMatch(cn, id)`: 문자열 filter `"(cn=" + RFC 4515 escape(id) + ")"`의 BER 형태와 같다. id의 `* ( ) \ NUL`이 filter 문법으로 해석되지 않는다(의도된 차이: LDAP injection 차단) |
| 결과 | 첫 entry의 attribute 중 **첫 값이 id와 같은 것**이 있으면 `Account{id}` + `email=attr(account_ldap_email_id)`, `group=attr(account_ldap_group_id)`를 한 try에서 읽음 | 동일. attribute 이름은 대소문자 무시, 값 비교는 byte 동일. email attribute가 없으면 group도 빈 값(같은 try의 NPE) |
| referral | 기본 `ignore` | SearchResultReference는 건너뜀(첫 entry 전 최대 1024개) |
| 실패 | 모든 예외 → `null` | (false, err): 호출자는 로그인 실패로 응답하고 원인을 로그 |
| 설정 오류 | `account_ldap_provider_url` null 등은 `Properties.setProperty` NPE가 try 밖에서 발생해 LOGIN 응답 없음 | `Open`은 성공하고 설정 오류를 1회 로그, 모든 LDAP 로그인을 `login fail`로 응답(의도된 차이). URL scheme/형식 오류, 빈 URL, `simple` 외 인증이 해당. 음수 `Timeout`/`MaxMessageBytes`는 프로그램 설정 오류로 `Open` 실패 |
| bind 비밀번호 | Client가 보낸 `pass`(보통 SHA-256 hex) 그대로 | 동일. LDAP 디렉터리에도 그 hash 문자열이 비밀번호여야 한다(raw 로그인 `secureLogin=false`면 평문) |
| 시간 | 제한 없음 | dial+TLS+bind+search 전체 `Timeout`(기본 10s), 호출 ctx 취소 반영 |
| 크기 | 제한 없음 | 수신 LDAPMessage `MaxMessageBytes`(기본 1 MiB)를 할당 전 검사, indefinite length·5 byte 이상 길이·중첩 32 초과 거절. 길이는 int64로 누적해 32-bit에서도 음수 wrap 없음 |
| TLS | `ldaps://`는 JVM 신뢰 저장소. 원본 코드는 StartTLS 확장 요청을 쓰지 않음 | `ldaps://`만. `LDAPConfig.TLSConfig`가 없으면 운영체제 시스템 신뢰 저장소만 사용(JVM truststore/설정 파일 지정 없음). StartTLS(Extended Request 1.3.6.1.4.1.1466.20037) 미지원 |
| debug | `account_ldap_debug`이면 bind 정보를 비밀번호까지 가리지 않고 출력 | id, bind name, 주소, base만 로그. 비밀번호는 쓰지 않음(의도된 차이) |
| 종료 | `ctx.close()` | UnbindRequest 후 close |

## Client 명령

`internal/service`에서 `Config.Accounts`(`service.AccountStore`, `*account.Manager`가 구현)가 설정되면 아래가 활성화된다. 설정되지 않으면 READ 5개는 기존 단일 credential metadata handler가 응답하고 mutation 4개는 등록되지 않아 body를 읽기 전에 `ErrUnsupported`로 실패한다. 서버는 Java처럼 group policy를 검사하지 않는다(Client UI만 적용).

| 명령 | request | response |
|---|---|---|
| LOGIN | MapPack `id, pass`, 선택 `ip, hostname, version, internal` | MapPack. 실패 `id, session=0, error`. 성공 `id, session, time, server_id, type(=group), version, client_version, [email: 로컬 파일에 id가 있을 때], timezone, [policy: group이 있을 때, 파일 순서 MapValue], menu, so_time_out, ext_link_name, ext_link_url_pattern` |
| CHECK_LOGIN | MapPack `id, pass` | BooleanValue(`authorizeAccount` 결과, `__local` 예외 없음, 오류는 false) |
| CHECK_ACCOUNT_ID | MapPack `id` | MapPack `result` Boolean(사용 가능 여부) |
| LIST_ACCOUNT | body 없음 | 계정마다 BlobValue(`Account.toBytes`), 파일 순서, 없으면 item 없음 |
| LIST_ACCOUNT_GROUP | body 없음 | MapPack `group_list` List<Text> |
| GET_GROUP_POLICY_ALL | body 없음 | MapPack `{group: MapValue{policy: Boolean}}`, group과 policy 모두 파일 순서 |
| ADD_ACCOUNT / EDIT_ACCOUNT | MapPack `id, pass, email, group` Text | MapPack `result` Boolean. 없거나 Text가 아닌 field는 `MapPack.getText`의 null이며 `""`로 정규화한다. id가 null이면 Java는 `StringKeyLinkedMap.put(null)`이 무시되어 저장 없이 `true`를 응답하고(oracle 관찰), Go는 id `""` 계정으로 저장하고 `true`를 응답한다(의도된 차이: 묵시적 폐기 금지) |
| ADD_ACCOUNT_GROUP | MapPack `name` Text, `policy` MapValue | BooleanValue. name/policy 없음은 false, policy가 MapValue가 아니면(Java `ClassCastException`이 handler 밖으로) malformed로 연결 종료 |
| EDIT_GROUP_POLICY | MapPack `{group: MapValue{policy: Value}}` | 단독 BooleanValue. 존재하는 group의 값이 MapValue가 아니면 false |

- LOGIN 응답은 원본처럼 요청 MapPack을 수정해 `pass`를 되돌려주지 않고 새 MapPack을 만든다(기존 [query-contract](../../docs/compatibility/query-contract.md)의 의도된 차이 유지). `timezone`은 Go `Location.String()`이다.
- `__local`(`NetConstants.LOCAL_ID`): 원본처럼 loopback peer의 로그인만 비밀번호 검사 없이 허용하며 group은 `""`이다(Java 원본 동작 유지). 공유 호스트에서는 운영 위험으로 남는다.
- `internal=true`(Text, 대소문자 무시) 세션은 7일 TTL. 연결 인증(`okSession`)은 7일로 연장하고, `CHECK_SESSION`(`validSession`→`getKeepAlive`)은 Java처럼 기본 TTL(3h)로 연장한다.
- `EDIT_GROUP_POLICY` 값의 `CastUtil.cString`: Boolean→`true/false`, Text→그대로, Decimal→10진수, Null→빈 element. Float/Double/List/Map/Blob 등은 Java가 `toString` 문자열을 쓰지만 Go는 같은 표기를 보장할 수 없어 해당 group이 존재하면 `false`(미기록)로 응답한다. Java Client는 BooleanValue만 보낸다.
- `ADD_ACCOUNT_GROUP` 직후 Java 메모리는 받은 MapValue 자체라 `GET_GROUP_POLICY_ALL`/LOGIN이 Boolean이 아닌 값(예: Text `"yes"`)도 그대로 돌려준다(oracle 관찰). 파일에는 `getBoolean` 결과(`false`)가 기록되므로 Go는 재parse한 Boolean을 응답한다(의도된 차이: 응답이 재시작 후 상태와 같다).
- policy MapValue 순서: Java는 `MapValue`(Hashtable)의 열거 순서로 보낸다. 이 순서는 key의 `String.hashCode`와 삽입 순서로 정해지는 결정적 값이지만(`localJavaMapEntries`가 KV bulk에서 같은 규칙을 Java oracle로 검증), Go는 파일 순서로 보낸다(의도된 차이). Client는 key로만 조회하므로 의미 차이는 없고 byte는 다르다. Hashtable 순서가 필요해지면 `policyValue`와 ADD/EDIT 경로에 `localJavaMapEntries`를 적용하면 된다.

## 검증

- `TestXalanJDK8Oracle`: `testdata/oracle/XmlOracle.java`(원본 DOM 조작 코드 복사 + 원본 `XmlUtil` 변환)를 JDK 8로 컴파일·실행해 기본 파일 identity, add/edit account(특수문자·non-BMP·C1·null 필드), add group, edit policy(기존/신규/빈 text/없는 group), prolog/epilog/standalone, compact, CRLF·문자 참조·mixed text, 중첩·Email 없음, 컨테이너 없음, `<Policy>` 없음 오류 경로를 **연속 적용하며 각 단계 byte 비교**한다. JDK 8 위치는 `SCOUTER_JDK8_HOME` 또는 `/usr/libexec/java_home -v 1.8`; 없으면 skip(미실행으로 보고).
- `manager_test.go`: 기본 파일 생성/권한, 파일 순서, 쓰기 byte(oracle과 같은 DOM 경로), 재시작 load, 중복·누락 parse 규칙, 255 byte·잘못된 문자·잘못된 policy 이름 거절과 파일 불변, reload(외부 변경/깨진 파일 1회 로그/삭제 재생성/자기 쓰기 비재로드), `Run` 취소, 경로 오류(디렉터리·symlink·크기·DOCTYPE·encoding·depth), 동시 add/edit/addGroup + 조회·reload(`-race`).
- `settings_test.go`: `SetLDAP`으로 파일→LDAP→다른 URL·domain→파일 전환이 다음 `Authorize`에 반영됨, 같은 잘못된 설정 반복 시 로그 1회, 음수 limit 거절 시 이전 모드 유지, 동시 교체·인증(`-race`).
- `ldap_test.go`: loopback 가짜 LDAP 서버로 bind 성공/49/기타 오류, search 결과 없음/일치 없음/첫 entry만/email·group 누락, filter metacharacter literal, 빈 비밀번호 무연결, debug 로그 비밀번호 부재, URL DN base 결합, 다중 URL failover, ldaps(self-signed RootCAs 성공/미신뢰 실패), indefinite length, 크기 초과, truncated, message id 불일치, notice of disconnection, timeout/ctx 취소, AD식 비최소 길이 수용. `FuzzBER`, `FuzzAccountXML`.
- `internal/service/account_commands_test.go`: 실제 loopback TCP로 LOGIN 성공/실패 entry 순서, policy 파일 순서, `__local`, group 없는 계정, CHECK_LOGIN, internal 7일/CHECK_SESSION 3h, body 없는 명령 연속, LIST_ACCOUNT blob, CHECK_ACCOUNT_ID, ADD/EDIT 결과·파일 내용, ADD_ACCOUNT_GROUP 원소 순서, EDIT_GROUP_POLICY 값 변환·없는 group·비 Map, malformed 연결 종료, store 없을 때 fallback과 mutation 미등록.

- `internal/service/account_golden_test.go`: `testdata/management/account/`(원본 AccountService handler 실행 golden, 25 case)를 같은 순서로 Go 서버에 보내 응답 byte를 비교한다. 17개는 byte 동일이고, 8개는 allowlist에 이유를 두고 Go 결과를 따로 단언한다(policy 순서, 예외 시 NoNEXT 대신 연결 종료, 파일 미기록 결함 2건과 그 후속 목록, null email NPE, ADD_ACCOUNT_GROUP 직후 비 Boolean 값). 기본 XML 2개와 `Account.toBytes` golden 4건(260 byte wrap은 Go 거절)도 비교한다. `SCOUTER_ACCOUNT_CONF_OUT`로 남긴 Go 기록 파일을 `scripts/java-management.py --verify-xml`로 원본 parser가 읽었다(계정 6, group 3, 값 일치).

미실행: 실제 Java Client GUI 상호운용, 실제 OpenLDAP/Active Directory, JDK 9+ 출력 byte 비교(차이만 확인), 운영 계정 파일 복사본 load.

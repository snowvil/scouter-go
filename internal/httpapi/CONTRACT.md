# internal/httpapi 계약

정본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`의 `scouter.server/src/main/java/scouter/server/http/`
`HttpServer.java`, `servlet/{Counter,Register,TelegrafInput}Servlet.java`, `handler/{Counter,Register}Handler.java`,
`Configure.java`의 `net_http_*`·`log_dir`·`log_keep_days`, webapp `framework/filter/AuthFilter.java`·`framework/util/ZZ.java`,
`scouter.common` `StrMatch`/`IPUtil`/`StringUtil`, Jetty 9.4.6(`NCSARequestLog`, `RolloverFileOutputStream`, `GzipHandler`, `Request.getReader`).
분석 문서: `docs/compatibility/http-contract.md` §1–§3, §6.

`New(engine, Config) (*API, error)`가 돌려주는 `*API`는 `http.Handler`이고 `Close()`로 Telegraf 등록 goroutine과 요청 로그 파일을 닫는다. ingest 투입은 `AcceptFrom(ctx, pack, peerHost)`만 쓴다. webapp(`/scouter/v1/*`)·WebSocket·extweb은 이 패키지 범위가 아니며 404다.

## 요청 처리 순서

1. 요청 로그(전 요청, 아래 형식) 래퍼.
2. `CheckAllowIPs=true`일 때만 허용 IP 검사 → 실패 403(빈 본문). 기본은 검사 없음(원본). `Close()` 이후 503.
3. (선택) gzip 래퍼 → servlet 라우팅: `/counter`, `/register`, `/telegraf`와 그 하위 경로(Jetty `/name/*`). 그 외 404.
4. method: `POST`만 처리, `OPTIONS` 200 + `Allow: POST, OPTIONS`, 그 외 405 + 같은 `Allow`(Java `HttpServlet` 기본 405; TRACE echo는 지원하지 않음).
5. body: `Content-Encoding`이 비었거나 `identity`가 아니면 415(Java는 gzip 요청 body를 풀지 않고 깨진 텍스트로 처리). `MaxBodyBytes`(기본 1 MiB) 초과 413. 읽기 오류 400.
6. charset(`Request.getReader`): `Content-Type`의 charset → 없고 값이 정확히 `application/json`이면 UTF-8(Jetty MimeTypes 가정 charset, Jetty 9.4.6 미실행 확인) → 그 외 ISO-8859-1(D11: charset 없는 JSON 비ASCII가 깨지는 원본 동작을 그대로 따른다). UTF-8/ISO-8859-1/US-ASCII 외 charset은 415(Java는 `UnsupportedEncodingException`으로 500). 잘못된 UTF-8은 U+FFFD.

## servlet 응답

| 경로 | 원본 | Go |
|---|---|---|
| `/counter/*` | 항상 200·빈 본문. JSON 배열은 요소 순서대로, 객체는 단일 처리, 그 외 `SC-8001`. 첫 예외(형 변환, null name, 숫자가 아닌 value, address 옥텟 범위 초과 시 `getByAddress(null)`의 `UnknownHostException` — ObjectPack enqueue 전, D8)에서 남은 요소 중단, 이미 넘긴 pack 유지 | 같음. `ObjectPack{objHash=hash(objName), objName="/"+host(+"/"+name), objType, address, alive=true(Java 필드 기본값, ObjectPack.java:58), wakeup=0, tags={}}`, `PerfCounterPack{time=now, REALTIME, objName, data: name→Float(value.floatValue())}`. peerHost는 `IPUtil.toBytes` 의미(IPv4 아니면 `0.0.0.0`, address 없으면 ingest가 `0.0.0.0`으로 채움). json-simple처럼 정수 literal은 int64(`Long`) 범위여야 하며 넘치면 body 파싱 오류. JSON 깊이 64 상한 |
| `/register/*` | 201 성공, 500 그 외. 새 objType: family=type(같은 이름 family가 있으면 `type + "." + Math.random()*100`), 첫 counter가 master, `addFamilyAndObjectType`; 기존 objType: 이미 해석되는 counter 제외 후 `editObjectType`; 성공 시 `notifyAllClients` | 같음(`TypeRegistrar`). registrar 미설정은 500(빈 성공 금지). `object.type`이 없으면 500(Java는 null 이름으로 진행할 수 있음). 원본은 `containsKey` 판정이라 `"display": null`·`unit` 누락이 null 값으로 저장되지만 `CounterDef`/`ObjectTypeDef`는 string이라 빈 문자열로 전달한다(의도된 차이; null을 구분해야 하면 registrar 계약 확장 필요) |
| `/telegraf/*` | 항상 200·빈 본문, 비활성 시 body를 읽지 않음 | 같음. 처리 의미는 `internal/telegraf/CONTRACT.md`. ObjectPack.address는 `HttpServer.getRemoteAddr`(첫 X-FORWARDED-FOR 항목, trim 없음, 없으면 peer). 원본은 그 값이 옥텟 범위를 벗어난 dotted 문자열(`X-FORWARDED-FOR: 300.1.1.1`)이면 `UnknownHostException`으로 500·남은 줄 미처리(D9 연장)지만 Go는 peerHost `0.0.0.0`으로 계속 처리하고 200(의도된 차이: header 하나로 payload 전체를 버리게 하지 않음) |

ingest `AcceptFrom` 오류는 응답 status를 바꾸지 않고 `CounterAcceptErrors`/`Telegraf.AcceptErrors`로 계수한다(Java `NetDataProcessor` 비동기 queue와 같은 외부 의미).

## 허용 IP (옵션, 기본은 원본 동작)

- 원본 근거: `HttpServer.java:96-98`이 세 servlet을 webapp context에 직접 등록한다. `net_http_api_allow_ips`(`Configure.java:708-714`의 `allowIpExact` + `*` 포함 항목 `StrMatch`)는 webapp `AuthFilter.java:56-65`에서 `net_http_api_auth_ip_enabled`일 때만 평가된다. 이 filter는 Jersey resource(`AddAuthFilterFeature.java:35-37`, `scouterx.` class)에만 등록되고 Jersey servlet은 `/scouter/*`(`WebAppMain.java:144`)이므로 **세 servlet에는 IP 검사·인증이 없다**.
- Go 기본값 `CheckAllowIPs=false`는 원본처럼 검사하지 않는다(무인증·무제한). `CheckAllowIPs=true`는 공유 호스트 보호용 **의도된 차이**로 같은 목록을 세 servlet에 적용하고 불일치는 403(빈 본문)이다.
- 판단: 옵션으로 둔다. 원본 동작이 기본이어야 기존 Telegraf/script 전송자가 바뀌지 않고, 공유 호스트에서는 운영자가 명시적으로 제한을 켤 수 있다. 원본에는 servlet용 키가 없으므로 Go 전용 flag `--scouter-http-check-allow-ips`로 켜며, `--scouter-http-allow-ips`를 주어도 켜진다(`cmd/scouter-go`).
- 판정 IP는 webapp `ZZ.getRequestIp`(`ZZ.java:40-55`)와 같다: `AuthIPHeader`(`net_http_api_auth_ip_header_key`)가 설정되고 header가 있으면 그 값(`,`가 있으면 첫 항목 trim), 아니면 TCP peer. **X-FORWARDED-FOR는 허용 판정에 쓰지 않는다**(header 하나로 우회 가능). X-FORWARDED-FOR 첫 항목은 원본 `HttpServer.getRemoteAddr`(`:133-145`)처럼 Telegraf ObjectPack.address에만 쓴다. peer는 Java `getRemoteAddr` 형식(IPv6 비압축 `0:0:0:0:0:0:0:1`)과 Go 압축형(`::1`)을 모두 대조한다.
- `ParseAllowIPs`는 Java `String.split(",")`처럼 trim하지 않고 끝의 빈 항목을 버린다. `AllowIPs` nil은 Java 기본 목록.

## 요청 로그

- `LogDir/http-request-yyyy_MM_dd.log`, append, Jetty `NCSARequestLog` extended 형식:
  `addr - - [dd/MMM/yyyy:HH:mm:ss Z] "METHOD URI PROTOCOL" status bytes "referer" "agent"`. addr는 peer(Java 형식, `preferProxiedForAddress=false`), user는 항상 `-`, header 부재는 `"-"`, 빈 header는 `""`, bytes는 응답 본문 byte(gzip 후). 시간대는 `Location`(Java JVM 기본).
- 첫 기록 시 파일을 열고, `Location` 기준 날짜가 바뀐 첫 요청에서 회전한다(Jetty는 자정 timer로 회전; 요청이 없으면 파일도 생기지 않는 차이). 회전 때 `log_keep_days`(Jetty `retainDays`)보다 mtime이 오래된 같은 prefix/suffix 파일을 지운다. `LogKeepDays<=0`이면 삭제하지 않는다.
- 줄마다 flush한다(fsync 없음). 열기/쓰기/삭제 실패는 `LogFailures`로 계수하고 분당 1회 로그, 요청 처리는 계속한다.

## gzip

`Gzip=true`(`net_http_api_gzip_enabled`, Java 기본 true)이면 GET/POST/PUT/DELETE 응답이 1024 byte 이상이고 `Accept-Encoding`이 gzip(q=0 제외)을 허용하며 handler가 `Content-Encoding`을 정하지 않았을 때 압축한다. 세 servlet 응답은 본문이 없어 실제로 압축되지 않는다. Jetty의 MIME 제외 목록·`Vary` 세부는 재현하지 않았다.

## 상한·계수

| 항목 | 값 | 포화 동작 |
|---|---|---|
| 동시 요청 | 64(`MaxInFlight`) | 빈자리를 `QueueWait`(기본 3초, Telegraf `outputs.http` 기본 timeout 5초보다 짧게: 대기 중엔 body를 읽기 전이라 client 이탈을 알 수 없어 재전송과 중복 수집될 수 있다)까지 기다린 뒤 body를 읽지 않고 503 + `Retry-After: 1`, `Busy`(분당 1줄 로그). 기다린 요청은 `Waited`, 대기 중 client가 끊으면 `Abandoned`. 요청 로그에는 남는다. Jetty는 thread 200 + 무제한 대기 queue라 503이 없다(의도된 차이) |
| body | 1 MiB(`MaxBodyBytes`) | 413, `TooLarge`. Content-Length 크기로 한 번 할당하고, ISO-8859-1/US-ASCII 변환은 정확한 크기의 문자열 하나(최대 3배)로 만든다 |
| `/counter` 요소당 counter 이름 | 4,094(`MaxCounterNames` = protocol MapValue 기본 4096 − ingest가 넣는 objHash·time) | 그 요소의 PerfCounterPack은 보내지 않고 `CounterTooMany` + 로그, 다음 요소 계속(Java는 상한 없음; 넘는 pack은 Go ingest가 encode할 수 없다). 이름 중복은 map index로 O(n) |
| JSON 깊이 | 64 | 파싱 오류(/counter 200, /register 500) |
| Telegraf 줄 수 | 1000 | payload 전체 drop, 200, `Telegraf.OversizedPayloads` |
| Telegraf 등록 queue | 1024 ×2 | drop, `RegisterQueueFull`/`CounterQueueFull` |
| 연결 수·header | `internal/server` `http.Server`(listener당 연결 1024, header 16 KiB, ReadHeaderTimeout 5s, ReadTimeout 30s, `http_admission.go`) | 연결 상한 초과는 503 + `Connection: close` 또는 accept 즉시 닫음 |

`Stats()`는 요청/거부/404/405/413/415/body 오류, counter·register·telegraf 처리 수, 로그 줄·실패·삭제, Telegraf 통계와 `Close` 시 등록 queue 잔여(`TelegrafResidualAtClose`)를 따로 센다.

## runtime 연결

- `internal/server`: `Config.ScouterHTTP`(`ScouterHTTPConfig`)가 nil이면 servlet을 열지 않는다(Java `net_http_server_enabled=false`). `Listen`이 있으면 별도 listener, 비어 있으면 `--http` listener(`newHTTPServerWithAPI`)에 함께 mount하며 둘 다 없으면 기동 오류다. `openScouterHTTP`가 Telegraf 설정을 읽고 API(와 webapp)를 만든다. `API.LogDir`·`API.Location`이 비어 있으면 runtime 값을 쓴다. flag·scouter.conf에서 `ScouterHTTPConfig`를 채우는 것은 `cmd/scouter-go` `servletConfig`다.
- registrar: counters.site.xml store(`Config.ConfDir`)가 있으면 runtime이 `counters.NewRegistrar`를 연결하고, 등록 뒤 REFETCH_COUNTER_XML을 Client session에 보낸다(`RegisterHandler.notifyAllClients`). store가 없으면 `/register`는 500, Telegraf는 등록 없이 수집한다.
- 종료: `Runtime.Close`는 `--http` listener 다음, UDP/TCP·engine보다 먼저 servlet listener를 닫는다(API에 닿는 listener가 모두 닫힌 뒤 engine).
- `/health`: `scouterHttp`(API `Stats()`)와 `endpoints.scouterHttp`.
- Telegraf 설정 교체: config `OnChange`의 `Change.Telegraf`(XML 변경) 또는 telegraf 키(`input_telegraf_*`, `telegraf_object_deadtime_ms`) 변경, `SET_CONFIGURE_TELEGRAF` 저장 뒤 reload hook에서 `Runtime.ReloadTelegraf`가 deprecated 키와 XML을 **매번 처음부터** 다시 읽어 합성하고 `API.SetTelegrafConfig`로 교체한다(atomic pointer, 진행 중 요청은 이전 설정으로 끝난다). 부분 갱신이 없으므로 원본 D13(scouter.conf만 바뀌면 XML 전역이 되돌아가고 XML measurement가 stale)이 생기지 않는다. XML을 읽지 못하면 이전 설정을 유지하고 로그를 남긴다.
- `net_http_server_enabled`/`net_http_port`/allow 목록/auth IP header/gzip/request log 보관 변경은 Java처럼 실행 중 listener에 반영하지 않는다(재기동 필요).
- `ForcesHTTPServer()`(XML `enabled=true`이면 Java가 HTTP 서버를 강제 기동): **결정(2026-10-16)**: Java 의미를 유지해 `net_http_port`에 listener를 열되 기동 로그에 포트를 남긴다(`cmd/scouter-go/main.go servletConfig`). Go 전용 보호는 `CheckAllowIPs` opt-in이다.

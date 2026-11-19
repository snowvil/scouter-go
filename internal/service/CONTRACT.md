# service 계약

기준 원본: Scouter `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`. Client 명령별 request/response 정본은
[query-contract.md](../../docs/compatibility/query-contract.md)와
[management-commands.md](../../docs/compatibility/management-commands.md)다. 이 문서는 패키지가
정한 Java와의 의도된 차이 중 명령 handler 밖의 정책만 적는다.

## 1. Login session 유지 규칙(의도된 차이)

Java `LoginManager`(`scouter.server/.../LoginManager.scala:26-82`)는 session을 마지막 keepalive
(`okSession`: 연결의 첫 인증 명령, `validSession`: `CHECK_SESSION`) 뒤 3시간(`CacheTable` 기본
keep) 동안 유지하고, 로그아웃·연결 종료 때 지우지 않는다. 그래서 연결이 끊길 때마다 새로
LOGIN하는 소비자는 재연결마다 사용하지 않는 session을 `GET_LOGIN_LIST`에 남긴다.

Go 규칙(`sessions.go`, 2026-10-16 개정: internal 포함·연결 보유 중 TTL 없음):

- session(internal 여부 무관)은 둘 중 하나를 만족하는 동안 유효하다.
  - 그 session을 붙잡은 **살아 있는 Client 연결**이 있다. 이때는 TTL을 적용하지 않는다. Client API 소비자는
    polling 연결을 수명 상한 없이 며칠씩 유지하며 몇 초마다 명령을 보낼 수 있다(연결 인증 latch 안).
    그런 session이 3h/7d가 지났다고 목록에서 빠지거나 `CHECK_SESSION`에서 무효가 되면 안 된다.
  - 붙잡은 연결이 없다: 마지막 keepalive 뒤 TTL(`Config.SessionTTL` 기본 3h, internal 7일)이 지나지
    않았고, 마지막 연결이 끝난 시점(연결이 없었으면 LOGIN이나 마지막 요청 시점)부터 유예 시간 G가
    지나지 않았다. G가 TTL보다 짧으므로 실제로는 G가 정한다.
- 연결이 session을 붙잡는 경우: 그 연결의 명령 header가 그 id를 보냈거나, 그 연결에서 LOGIN이
  그 session을 만들었다(LOGIN 응답 전에 묶는다, transport 계약 §3.1). 연결은 **마지막으로 쓴
  session 하나만** 붙잡는다. Scouter Client는 서버당 session 하나를 쓰고, 기준으로 삼은 Client API 소비자는 연결마다
  LOGIN 한 번·그 연결에서만 사용하므로 두 경우 모두 이 규칙과 맞는다.
- G = 2 × live `net_tcp_client_so_timeout_ms`(`Settings.ClientSoTimeoutMillis`), 최소 30초·최대
  10분. 경계값(정확히 G)은 유효하다. 설정이 바뀌면 다음 판정부터 새 G를 쓴다.
- `internal=true`도 같은 규칙이다. Client API 소비자가 모든 pooled 연결에서 `internal="true"`로
  LOGIN하고 닫힌 연결의 session을 다른 소켓에서
  다시 보내지 않으면, Java 7일 규칙만으로는 재연결마다 사용하지 않는 session이 7일 동안 목록에
  남는다. Go 내장 webapp의 `__local` session도 pool 연결이 모두
  닫혀 G가 지나면 제거되고, webapp은 `INVALID_SESSION`에서 즉시 재로그인한다(webapp 계약).
- 서버는 session 만료를 이유로 연결을 먼저 닫지 않는다. 만료된 session id를 보낸 새 연결만
  `INVALID_SESSION`을 받는다(Java와 같은 응답).
- 이미 만료된 session은 연결이 다시 보내도 살아나지 않는다(제거 후 `INVALID_SESSION`).
- 정리 시점: 주기 goroutine 없이 접근할 때 지운다. LOGIN(`MaxSessions` 계산 전 전체 정리),
  연결 인증(`Authorize`), `CHECK_SESSION`, `GET_LOGIN_LIST`·REMOTE_CONTROL session 목록이 같은
  판정 함수를 쓴다. 표에 남아 있어도 만료 판정이면 어느 경로에서도 유효하지 않다.
  RequestLogger용 `LoginUser`는 판정·연장 없이 표만 읽는다(기존과 같음).
- 바뀌지 않은 것: `MaxSessions` 상한, 무작위 64bit session key(세션 고정 방지), 인증된 연결의
  latch(Java `sessionOk`), Agent 연결(session 무관).

근거(정상 Client의 자동 복구): Java Client `TcpProxy.process`는 `INVALID_SESSION`을 받으면
`server.setSession(0)`으로 session을 지우고 연결을 닫는다(`scouter.client/src/scouter/client/net/TcpProxy.java:166-168`).
`SessionObserver`는 5초마다 session 0인 서버에 저장된 계정으로 `LoginMgr.silentLogin`을 한다
(`scouter.client/src/scouter/client/threads/SessionObserver.java:42-68`). 그래서 서버가 session을
일찍 지워도 Client는 다음 주기에 다시 로그인한다. so-timeout(기본 8초)보다 오래 요청이 없는
Client는 어차피 서버가 연결을 닫으므로(transport 계약 §2) G는 그 두 배 이상이다.

검증: `session_liveness_test.go`(실제 transport 위의 재연결 시나리오, 같은 연결 재로그인, 다른 연결 유지,
유예 안 재연결, internal 연결 규칙, 연결 보유 중 8일 경과 유지, so-timeout live 변경, G 경계, 만료 session 비부활, 동시 race).
이 정책은 연결마다 `internal=true`로 LOGIN하고, 30초 넘게 쉰 연결은 재사용하지 않으며, 닫힌 연결의 session을 다른 소켓에서 쓰지 않는(`CHECK_SESSION`도 쓰지 않는) Client API 소비자를 기준으로 정했다.
실제 Java Client의 재로그인 동작은 원본 코드 판독이며 실행 확인은 하지 않았다.

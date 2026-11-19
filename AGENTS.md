# Scouter Go 개발 지침

## 목적과 정본

- 단일 Go 서버로 Scouter Server를 대체한다. 기존 Java/Host Agent, Scouter Client, 조회 의미, 기존 저장 데이터 이관의 전체 호환성을 단계별로 확보한다.
- 성능 목표는 "같은 호환성에서 더 적은 자원으로 더 높은 완료 transaction 처리량"이다. 특정 운영 환경의 규모·현재 처리량·목표 TPS·호스트 사양은 공개 문서에 적지 않으며, 측정 결과만 조건과 함께 기록한다. 예외는 아래 "공개 저장소 규칙"의 익명 사례 보고서뿐이다.
- 목표 호스트는 다른 서비스와 자원을 공유하는 서버일 수 있다. 전체 자원을 전용 예산으로 가정하지 않는다.
- 계약의 정본은 `docs/compatibility/baseline.json`에 고정한 Java/Scala Scouter와 Pulse plugin 소스다. 원본 문서와 현재 코드가 다르면 차이를 기록한다.
- `scouter-project/scouter-server-go`는 설계 참고자료다. README 성능 수치를 이 프로젝트의 실측으로 사용하거나 구현을 통째로 복사하지 않는다.

## 작업 순서

1. 요구사항, 해당 소스, 기존 테스트와 호환성 목록을 확인한다.
2. 바이트 형식/상태 전이/저장/예외/순서 계약과 설계를 기록한다.
3. Java oracle fixture 또는 재현 테스트를 먼저 마련한 뒤 작은 단위로 구현한다.
4. 경계 테스트, race, fuzz, 실제 Agent/Client 상호운용을 변경 범위에 맞춰 검증한다.
5. 프로파일과 동일 조건의 부하 결과로 개선 효과를 판단하고 검증 결과를 남긴다.

구현 단계는 `docs/implementation-plan.md`를 따른다. 서버 호환 구현 다음에 Pulse 기능을 내장한다. 미구현 기능을 빈 성공 응답이나 묵시적 데이터 폐기로 대체하지 않는다.

## 경계와 코드 원칙

- Go 표준 라이브러리를 우선한다. 필요성이 확인되기 전 DB, DI framework, 외부 queue, generic repository를 추가하지 않는다.
- KISS/YAGNI, Rule of Three/AHA, SRP를 따른다. 인터페이스는 사용하는 쪽에 두고 실제 교체/검증 경계에만 도입한다.
- `internal/protocol`: 순수 wire codec. 네트워크/디스크/Kafka 의존성 금지.
- `internal/transport`: UDP/TCP framing·deadline·connection lifecycle.
- `internal/ingest`, `internal/storage`, `internal/service`: 수집/저장/조회 경계를 분리한다.
- `internal/pulse`: 기존 callback/filter/JSON/Kafka 계약. 수집 경로에서 Kafka 응답을 기다리지 않는다.
- 파일이나 goroutine은 소유자와 종료 경로가 분명해야 한다. 모든 queue/cache/reassembly는 크기·시간 상한과 포화 동작이 있어야 한다.
- 버퍼 재사용은 소유권을 문서화하고 race 검증 후 적용한다. 비동기 소비자에 빌린 slice를 넘기지 않는다.
- protocol length/count를 할당 전에 검증한다. 무제한 할당, 무제한 goroutine, silent parse error를 금지한다.

## 호환성 규칙

- Scouter Decimal은 varint가 아니다. signed 1/2/3/4/5/8 byte 폭, big-endian, UTF-8, null/empty, 수신한 Map entry 순서를 보존한다. MapPack의 LinkedHashMap과 MapValue의 Hashtable을 혼동하지 않는다.
- UDP 단일/묶음/분할, TCP Client/Agent V1/Agent V2/Agent request를 서로 다른 framing으로 취급한다.
- Client 명령별 request/response body를 조사한다. 모든 응답이 MapPack이라는 가정을 금지한다.
- XLog 구형 선택 필드, counter timetype, 날짜/timezone, text dictionary, realtime cursor/loop 의미를 보존한다.
- 원본 취약 동작이나 결함 후보는 정상 계약과 분리한다. 재현 근거와 의도된 차이를 기록한다.
- 기존 DB 파일은 읽기 검증 또는 복사본에서만 조사한다. 이관/rollback 검증 전 원본 파일에 쓰지 않는다.
- Pulse topic/key/partition/header/JSON/Base64/Step/filter 의미를 고정한다. application header는 UUID가 아니다.
- JVM plugin JAR/임의 plugin 확장과 Go 내장 기능은 동일하지 않다. 확장 미지원은 명시한다.

## 성능과 운영

- TPS는 **완료된 transaction/XLog 수**로 정의하고 packet/s, profile/s, byte/s와 구분한다.
- microbenchmark의 ns/op를 서버 TPS로 환산해 보고하지 않는다.
- payload mix, profile 크기, dictionary hit, 조회 병행, retention, CPU quota, 메모리 한도, disk/fsync 정책을 기록한다.
- enqueue 수, 처리 완료 수, drop/reject/parse/storage/Pulse 오류와 종료 시 잔여 queue를 별도 집계한다.
- `GOMEMLIMIT`는 Go runtime의 soft limit이다. RSS hard cap이나 OOM 방지를 보장하지 않는다.
- 운영망/실서비스 대상으로 부하를 생성하지 않는다. 격리된 replay 환경에서 먼저 검증한다.
- 인증/권한/관리 명령을 우회하지 않는다. 비밀값, 원문 SQL/프로파일/계정 정보를 fixture나 로그에 넣지 않는다.

## 공개 저장소 규칙

이 저장소는 공개다. commit한 내용은 지워도 이력과 복제본에 남으므로, 넣기 전에 막는다.

- 넣지 않는 것:
  - 조직·서비스·application·호스트 이름, 내부 주소와 주소 대역, 특정 환경의 포트·계정·설정 값
  - 비밀값(비밀번호, key, token, 인증서)과 특정 환경의 인증 상태 서술
  - 특정 환경에서 관찰한 Agent·object·연결·세션 수, 처리량, 날짜·시각이 붙은 사건 경위
  - 실제 트래픽에서 캡처한 데이터(SQL, URL, object 이름, profile, 계정). fixture는 합성 값이나 원본 Scouter 기본값만 쓴다
  - 로컬 절대 경로, 개인 계정 이름, 회사 도메인 email
  - 비공개 저장소의 이름·commit SHA·파일 경로·줄 번호·내부 구조. 비공개 구성요소는 역할로만 부른다(예: "Client API 소비자")
  - 내부 도구·절차·작업 방식의 흔적과, 저장소에 없는 로컬 산출물 참조
- 측정값은 격리 환경(개발 장비, VM, loopback replay, 합성 데이터)의 결과를 조건과 함께 적는다. 실환경에서 확인한 사실은 수치와 식별자 없이 "실제 Agent 환경에서 확인했다"로 적는다.
- 익명 사례 보고서(`docs/reports/`)는 저장소 소유자가 공개를 승인한 것만 둔다. 실환경의 측정 수치를 적을 수 있지만 위 식별 정보는 넣지 않고, 유리한 값만 고르지 않으며 유실·결함·한계를 함께 적는다.
- 원본 Scouter의 보안 결함은 "원본은 X를 검증하지 않는다. Go는 검증한다" 수준의 계약 차이로만 적는다. 악용 절차는 적지 않는다.
- commit 전에 `make public-check`를 실행한다. 조직 고유 식별자는 저장소에 적을 수 없으므로 git이 무시하는 `.public-denylist`에 정규식으로 두고 검사한다([CONTRIBUTING.md](CONTRIBUTING.md)).
- commit 작성자 email은 회사 도메인이 아닌 주소(예: GitHub noreply)를 쓴다.
- 실수로 넣었으면 덮어 쓰는 commit으로 끝내지 않는다. 저장소 소유자에게 알리고, 비밀값이면 먼저 폐기·교체한다.

## 검증과 완료 판정

- 표준 명령: `make check`, `make race`, `make fuzz`, `make bench`, `make fixtures-check`, `make inventory-check`, `make public-check`.
- 각 명령의 실제 pass/fail, 미실행 이유, 가정, 남은 위험을 기록한다. 도구 부재나 테스트 skip을 pass로 쓰지 않는다.
- Java fixture는 고정 source commit에서 생성한다. Go round-trip만으로 Java 호환을 선언하지 않는다.
- 호환성 표는 `미구현 → fixture 검증 → 통합 검증 → 운영 검증`으로 증거에 따라 갱신한다.
- 전체 포팅 완료는 Agent/Client/저장/운영/Pulse/부하 gate 모두의 근거가 있을 때만 선언한다.

## 저장소와 협업

- 이 저장소는 독립 Git root다. 기본 branch는 `master`, 작업 branch는 `codex/<topic>`이다. Scouter 원본의 develop 정책을 여기로 복사하지 않는다.
- 기여/버전/릴리즈 규칙은 `CONTRIBUTING.md`를 따른다. 단계별 검증이 끝나면 즉시 작은 commit을 만든다. 요청 없는 push/merge/release는 하지 않는다.
- Scouter/Pulse 원본 저장소는 읽기 전용 참고자료다. 변경이 필요하면 별도 범위와 해당 저장소 지침을 적용한다.
- 독립 작업만 subagent에 위임한다. subagent의 model·effort 선택은 각 작업자의 로컬 도구 설정을 따르며, 이 저장소에 특정 도구를 요구하지 않는다.
- subagent는 지정 파일만 수정하고 재위임/사용자 질문/다른 작업자 수정 취소를 하지 않는다. 메인 에이전트가 통합·검증·commit을 담당한다.
- 답변과 프로젝트 문서는 한국어, 기술 식별자는 원문을 유지한다.

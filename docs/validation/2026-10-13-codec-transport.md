# Pack·Step·수신 계층 검증 기록

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

기준일: 2026-10-13. Java 정본은 Scouter 2.21.3
`dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`다. 원본 저장소는 수정하지 않았다.
macOS/Apple M1 Pro, Go 1.27.2, JDK 18의 `javac --release 11`, `GOMAXPROCS=2`에서
synthetic 데이터와 loopback socket만 사용했다.

## 완료한 구현 단위

- `918b146`: 길이 헤더 없는 `ReadPack/ReadValue`. 다음 객체를 미리 읽지 않는다.
- `b0aed5a`: Java Pack/Step oracle와 fixture 확장. 54 + 12 + 57 = 123개.
- `f08eb54`: stream/slice 결과와 소비 경계를 비교하는 fuzz.
- `4ab7fe3`: collection의 stream byte 예산 초과를 `ErrLimit`으로 분류.
- `54eebd6`: 미지원 Pack 9종. 기본 PackEnum 16종을 모두 지원.
- `f8f9927`: StepEnum 23종과 EOF sequence.
- `69d5e65`: UDP/TCP 수신, Agent request, stack upload, 제한·취소·종료 관리.

Java writer/read를 실제 실행하고 Go에서 field 의미와 exact 재인코딩을 대조했다.
Profile payload 해석은 `DecodeSteps`를 명시적으로 호출하며, 압축 stack과 SpanContainer
내부 span 검증·저장·조회는 소비자 계층의 후속 작업이다. Batch simple query projection은
표준 Pack과 별도 계약이다. 엄격한 malformed 처리 차이는 각 계약 문서에 기록했다.

## codec 검증 근거

- `make fixtures-check`: 최종 141개 fixture 재생성 및 SHA/size 대조 PASS.
  초기 54개 + 추가 Pack 12개 + Step 57개 + transport 18개다.
- `GOMAXPROCS=2 .tools/go/bin/go test -race ./internal/protocol`: PASS.
- `GOMAXPROCS=2 .tools/go/bin/go vet ./internal/protocol`: PASS.
- `make fuzz`: protocol 5개 target, 각 10초·2 workers, 모두 PASS.
  DecodePack 591,682회, DecodeValue 62,010회, ReadPack 524,829회,
  ExtraPack 659,488회, DecodeSteps 307,345회. 실행 횟수는 성능 지표가 아니다.
- `make bench`: PASS. 153-byte XLog decode 474.8 ns/op, 624 B/op, 10 allocs/op;
  encode 713.9 ns/op, 1024 B/op, 11 allocs/op. 단일 host의 codec microbenchmark이며
  서버 transaction/s나 공유 서버 처리량으로 환산하지 않는다.

ReadPack 구현 전 undefined API로 RED를 확인했다. 최초 테스트의 fixture 이름 오타를
수정했고, 병렬 작업 중 아직 구현되지 않은 Step/TCP 심볼에 의한 build 실패는 통합 후
별도 재검증했다. 완전한 5-byte Map에 MaxBytes=4를 주었을 때 `ErrTruncated`로 분류하던
문제는 독립 리뷰와 RED 테스트로 재현한 뒤 `ErrLimit`으로 수정하고 race로 확인했다.

## 수신 계층과 최종 통합 검증

- `make check race fixtures-check`: 전체 protocol/transport gofmt·vet·test·build·race 및
  Java fixture 141개 대조 PASS.
- `make inventory-check`: PASS. Java/Pulse source SHA와 clean 상태도 각각 확인했다.
- `GOMAXPROCS=2 .tools/go/bin/go test ./internal/transport -run '^$' -fuzz '^FuzzUDPDatagram$' -fuzztime=10s -parallel=2`:
  PASS, 25,381회 실행.
- `GOMAXPROCS=2 .tools/go/bin/go test ./internal/transport -run '^$' -fuzz '^FuzzTCPHeader$' -fuzztime=10s -parallel=2`:
  PASS, 735,738회 실행. 최종 Makefile은 protocol 5개와 이 2개 target을 모두 포함한다.
- `GOMAXPROCS=2 .tools/go/bin/go test -race ./internal/transport -run '^TestTCPStackPipeliningAndTruncatedBody$' -count=1`:
  PASS. 이어지는 두 upload를 한 번에 보내도 각 body를 정확히 분리하며,
  선언 길이보다 짧은 upload에는 OK를 보내지 않는다.

UDP는 6개 magic, batch 원자적 검증, 역순/동일 중복 fragment, sender 분리,
상한·만료·충돌, idle 만료, oversized datagram, queue 포화·취소·종료를 확인했다.
재조립은 sender/address/port/objHash/pkid로 분리하며 payload·항목·TTL 상한을 둔다.
통계는 datagram과 Pack, fragment assembly 단위를 구분한다. UDP 종료 시 미처리 queue와
불완전 assembly는 계수하고 종료하며, 영속 저장까지 drain했다고 주장하지 않는다.

TCP는 Client/Agent V1/V2/Agent request handshake, byte 단위 short read,
연속 Client command, session 검사/latch/CLOSE, missing handler의 실패,
V2 flag 바깥 length와 NoNEXT 단독 byte, stack raw body·OK 순서를 검증했다.
각 callback은 context 취소에 협조해야 한다. 구현은 callback을 임의로 중단시키는
별도 goroutine을 만들지 않는다. Agent connection pool/keepalive 정책은 다음 service 계층의 책임이다.

리뷰에서 발견한 Client/Auth/Stack callback timeout 미전달, 잘린 command의 clean EOF
오분류, Agent 요청의 mutex 대기 중 취소 지연, Agent receive callback timeout 미전달을
회귀 테스트로 보강했다. command context deadline과 취소 가능한 직렬화 gate를 사용하며,
대기 요청 취소는 진행 중인 첫 요청의 연결을 닫지 않는 것을 확인했다.

## 검증 경계

고정 Java source가 생성한 바이트와 Go socket framing 검증을 실제 운영 Agent/Client
전체 접속 검증과 구분한다. 이 작업은 수신 callback 경계까지이며 인증/session 저장소,
229개 command handler, ingest/dictionary/cache, 영속 저장·이관, Pulse Kafka 전송을
완료한 것이 아니다. 운영망 부하, 목표 처리량, 공유 호스트 자원 예산, remote CI는
이 로컬 검증의 근거에 포함하지 않는다.

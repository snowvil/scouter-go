# 보안 정책

## 취약점 신고

보안 취약점은 공개 issue나 pull request로 올리지 않는다. GitHub의 비공개 신고 기능(저장소의 Security 탭 → "Report a vulnerability")으로 알려 준다.

신고에는 다음을 적는다.

- 영향받는 구성요소(UDP·TCP 수신, Client 명령, HTTP·webapp API, 저장, Pulse 전송, 도구)와 revision
- 재현 절차 또는 입력, 예상 영향
- 알고 있다면 원본 Java Scouter Server에도 같은 문제가 있는지

같은 문제가 원본 Scouter에도 있으면 원본 프로젝트(<https://github.com/scouter-project/scouter>)에도 알려야 한다. 이 저장소는 원본의 결함을 고친 곳을 각 `CONTRACT.md`의 "의도된 차이"에 계약 차이로 적으며, 악용 절차는 싣지 않는다.

## 지원 범위

릴리즈 버전을 아직 발행하지 않았다. 수정은 `master`에만 반영한다.

## 배포할 때

이 서버는 Agent의 UDP·TCP, Scouter Client 명령, 선택 사항인 HTTP API를 받는다. 신뢰하지 않는 네트워크에 직접 노출하지 않는다. 계정 파일과 HTTP 인증 설정은 [실행 설정](docs/compatibility/runtime.md)과 [전환 runbook](docs/operations/cutover-runbook.md)을 따른다. 기본 계정의 비밀번호는 원본 Scouter와 같은 공개된 기본값이므로 반드시 바꾼다.

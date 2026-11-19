# server log·request log·Pack toString Java oracle

고정 원본 Scouter 2.21.3(commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`)의 `Logger.scala`, `RequestLogger.java`, `scouter.common`(Pack/Value `toString`, `DateUtil`, `java.sql.Timestamp`, JDK 8 `Float.toString`·`DecimalFormat`)을 임시 디렉터리에 **수정 없이** 컴파일·실행해 `internal/logger/testdata/java` golden을 만든다. 원본 source tree는 읽기만 한다. 데이터는 모두 synthetic이다(SQL 문자열은 마스킹 검증용 가짜 문장).

```sh
export SCOUTER_SOURCE=/path/to/pinned/scouter          # 기본값 ../scouter
export SCOUTER_JAVA8_HOME=/path/to/jdk8                # 기본값 /usr/libexec/java_home -v 1.8
python3 tools/java-logger/run_oracle.py --generate     # golden 재생성
python3 tools/java-logger/run_oracle.py --check        # 임시 디렉터리에 다시 만들어 byte 비교(차이가 있으면 exit 1)
```

## 컴파일·실행

| 단계 | 도구 | 대상 |
|---|---|---|
| common | JDK 8 `javac -source 8 -target 8` | `scouter.common/src/main/java` 전체 |
| server | `scouter.deploy/lib/scala/scala-compiler.jar` 2.10.4를 JDK 8에서 실행(mixed compile) | 원본 `Logger.scala`, `ConfObserver.scala`, `LoginUser.scala`, `util/ThreadScala.scala`, `util/EnumerScala.scala` + `RequestLogger.java` + seam |
| oracle | JDK 8 `javac` | `PackStringOracle.java`, `LoggerOracle.java` |
| 실행 | JDK 8, `-Duser.timezone=Asia/Seoul`(time 파일은 `America/New_York`도), `-Duser.language=en -Duser.country=US -Dfile.encoding=UTF-8`, 환경 변수는 `SCOUTER_ORACLE_ENV_MARKER=synthetic` 하나 | |

## Seam(`seams/`)

| seam | 원본 | 대체 이유 |
|---|---|---|
| `Configure` | 서버 설정 전체(JAXB·파일 reload) | Logger·RequestLogger가 읽는 5개 field(`log_dir`, `log_rotation_enabled`, `log_keep_days`, `_trace`, `mgr_log_ignore_ids`)만, 원본 기본값. oracle이 field를 바꾸고 `ConfObserver.exec()`를 호출한다 |
| `LoginManager` | 계정 인증·session cache | `getUser(session)`만. 모르는 session은 null(원본과 같음) |

## golden(`internal/logger/testdata/java/`)

- `packstring.json`: Pack 37개·Value 47개·`toDebugString` 8개. 각 항목은 `DataOutputX.writePack/writeValue` bytes(hex)와, 그 bytes를 **원본 `DataInputX`로 다시 읽은** 객체의 `toString()`이다. NetDataProcessor가 출력하는 것은 decode된 Pack이므로 MapValue(Hashtable) 순서는 wire 순서로 다시 채운 결과다.
- `numbers.json`: `Float.toString` 826개, `DecimalFormat("#0.0#################")` 1044개(경계값 + 고정 seed 무작위 bit/크기).
- `time-<tz>.json`: `DateUtil.datetime/timestamp/yyyymmdd/getDateUnit`, `java.sql.Timestamp.toString`(321개 시각), `DateUtil.yyyymmdd(String)` 파싱 16개(오류 포함).
- `logger.json`: Logger 줄 형식·ID 필터·rate limit(1000개 상한 eviction)·`_trace` stdout·rotation=false 재open 시 비움(D-L1)·`log_dir` 변경·`clearOldLog` 삭제 결정(19개 이름 template, keep=3/0, rotation off)·RequestLogger 줄·`add` 반환값·`registerCmd`·`log_dir` 변경 후 이전 dir 유지(D-L2). 실행 시각 의존 부분은 `<datetime>`, `<timestamp>`, `<today-N>`으로 정규화한다. 예외 stack frame 줄(`\t`로 시작)은 oracle 줄 번호에 의존하므로 뺀다. 원본 기동 경합(daemon 첫 반복이 `pw`를 닫아 첫 줄이 stdout으로 가는 문제, `internal/logger/CONTRACT.md` D-L4)을 피하려고, 버리는 디렉터리에 한 번 기록한 뒤 daemon이 첫 sleep에 들어갈 때까지 기다리고 나서 기록을 시작한다.
- `manifest.json`: source SHA, JDK, 컴파일한 원본 파일과 oracle source의 SHA256, golden 파일 크기·SHA256.

생성 후 golden 전체에서 home 경로·임시 디렉터리·사용자 이름을 찾으면 실패한다.

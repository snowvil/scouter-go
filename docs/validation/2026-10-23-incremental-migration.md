# 2026-10-23 증분 이관(precopy/finish) 측정

계약과 설계는 [migration](../compatibility/migration.md) "증분 이관"을 따른다. 이 문서는 synthetic DB에서 전체 migrate와 `--precopy` + `--finish`의 시간을 비교한 결과다. 운영 DB·운영 규모로는 측정하지 않았다.

## 조건

- 도구: `internal/storage/migrate` `TestIncrementalMeasure`(환경변수가 있을 때만 실행). 작업 디렉터리는 비어 있는 로컬 디렉터리다.

  ```sh
  SCOUTER_MIGRATE_MEASURE_DIR=<빈 디렉터리> go test -count=1 -timeout 60m -run TestIncrementalMeasure -v ./internal/storage/migrate/
  # 기본값: _DATES=8, _XLOGS=200000(날짜당), _PROFILE_MB=480(날짜당)
  ```

- DB: Go storage writer로 만든 8개 날짜, 4,273,469,551 byte(99개 파일, 비압축 XLog)다. 날짜마다 XLog 200,000건과 64KiB profile chunk 480MiB를 쓰고, `00000000/text/text_sql`에 영구 text를 쓴다. Java writer로 만든 DB가 아니다.
- "Java 실행 중"은 fresh `lock.dat`로 흉내 냈다. precopy 사이에는 마지막 날짜("오늘")에 XLog 20,000건과 48MiB profile을, precopy 2 뒤에는 XLog 2,000건과 약 4.8MiB profile을 더 썼다. 각 쓰기 뒤에는 바뀐 파일의 mtime을 1시간 전으로 옮겼다. writer가 멈춘 지 오래된 상태라서 racy 재복사는 없다.
- 호스트: macOS(Apple M1 Pro 8코어, 16GiB, APFS 내장 SSD), 같은 디스크에 src·staging·dst를 두었다. 다른 작업이 함께 돌아 load average가 약 7이었다. page cache는 비우지 않았고, 일부 파일은 warm 상태였을 수 있다. 각 단계는 1회씩 측정했다.

## 결과

| 단계 | 시간 | 복사 | 재사용 | inspect group | src 읽기 |
|---|---:|---:|---:|---:|---:|
| 전체 migrate(초기 src) | 94.7 s | 99 파일, 4.27 GB | – | 전체 | src 3회 이상 전체 |
| precopy 1 (Java 실행 중) | 60.6 s | 99 파일, 4.27 GB | 0 | 9 | – |
| precopy 2 (Java 실행 중) | 8.8 s | 13 파일, 588 MB | 86 파일, 3.74 GB | 2 (7 재사용) | – |
| **finish (Java 정지 뒤)** | **9.7 s** | 13 파일, 594 MB | 86 파일, 3.74 GB | 2 (7 재사용) | 1.19 GB = 바뀐 파일 × 2 |
| 전체 migrate(최종 src, 비교 기준) | 98.3 s | 99 파일, 4.33 GB | – | 전체 | src 3회 이상 전체 |

finish 내부는 복사 1.6 s, inspect 3.7 s, 조회 비교 4.0 s, src 재확인(stat 전체 + 복사 파일 재hash) 0.4 s였다. finish manifest는 최종 src를 전체 migrate한 manifest와 파일 목록·summary·조회 비교 수가 같았다(테스트 안의 `sameManifest`).

## 해석

- 이 조건에서 손실 구간(Java 정지 → Go 기동 가능)은 전체 migrate 98.3 s에서 finish 9.7 s로 줄었다. 바뀐 파일은 DB의 13.7%(594MB/4.33GB)였다.
- finish 비용은 그 사이 Java가 쓴 byte가 아니라 **바뀐 파일과 그 group의 크기**에 비례한다. 오늘 XLog group 전체(`xlog.service`, `xlog.profile`, index)를 다시 복사하고 decode 검사하기 때문이다. 보존 기간이 길수록 비율이 줄고, 날짜를 넘긴 직후에는 오늘 group이 작다. 늦은 XLog가 과거 날짜를 건드리면 그 날짜 group도 다시 검사한다.
- precopy 2와 finish의 차이는 작다. 여러 번 반복하면 마지막 precopy와 finish 사이의 변경만 남지만, Java가 계속 쓰는 오늘 group은 매번 다시 복사된다.
- microbenchmark가 아니며 운영 디스크·page cache·동시 부하에 따라 달라진다. 운영 전환 전에는 운영 DB **복사본**과 같은 디스크에서 `--precopy`/`--finish --dry-run`으로 바뀌는 byte를 확인하고 다시 측정해야 한다.

## 미검증

- 실제로 실행 중인 Java 서버 옆에서의 precopy(복사 중 변경, racy 경로). 이 경로는 단위 테스트의 fresh 파일 시나리오(`TestPrecopyRacyFilesAreCopiedAgain`)만 거쳤다.
- Linux에서의 시간 측정(기능 테스트는 Linux arm64 VM에서 `go test -race -short`로 통과), 원격 파일시스템, cold cache, gzip(XLog 압축) 대용량 DB.
- 측정 뒤 리뷰 반영으로 racy window를 60초로 늘리고, 동시 실행 lock과 Java 종료 확인을 추가했다. 측정 시나리오의 파일은 1시간 전 mtime이라 결과에 영향은 없다. 재측정은 하지 않았다.

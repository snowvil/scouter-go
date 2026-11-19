# legacy GeoIP Java oracle

원본 Scouter 서버가 쓰는 `com.maxmind.geoip:geoip-api:1.3.1`의 `LookupService`로 합성 legacy GeoIP `.dat`를 조회한다. 그 결과를 `internal/geoip/testdata/oracle/golden.json`에 고정한다. 계약과 의도된 차이는 `internal/geoip/CONTRACT.md`에 있다.

```sh
python3 -I scripts/java-geoip.py --check                 # 임시 디렉터리에 재생성한 뒤 testdata와 byte 비교
python3 -I scripts/java-geoip.py --generate /tmp/새-빈-디렉터리
```

- 입력 `.dat`는 `scripts/java-geoip.py`가 형식 지식으로 직접 만든다. 실제 GeoLite 데이터는 쓰지 않는다. 정상·경계·손상 DB와 조회 IP 목록(`spec.txt`)도 이 script가 정한다.
- `GeoipOracle.java`는 `GISDataUtil`과 같은 생성자 `new LookupService(file, GEOIP_MEMORY_CACHE)`를 쓴다. 조회는 `getLocation(InetAddress.getByAddress(4 byte))`이다. 열기 결과에는 private field `databaseType`, `databaseSegments`, `recordLength`, `DatabaseInfo.info`를 reflection으로 남긴다. 조회 결과는 `null`, Location(float는 `floatToIntBits`), 예외 class 중 하나다. `InvalidDatabaseException`이면 message와 cause class도 남긴다.
- jar: `SCOUTER_GEOIP_JAR`를 쓰고, 없으면 `~/.m2/repository/com/maxmind/geoip/geoip-api/1.3.1/geoip-api-1.3.1.jar`를 쓴다. SHA-256 `d4e162e4da56504dce2b5566c304be535298859be7c6235852dbc30ecf56e7fc`가 아니면 실패한다. 원본 배포본 `server/lib/geoip-api-1.3.1.jar`와 같은 파일이다.
- JDK: `SCOUTER_JAVA8_HOME`을 쓰고, 없으면 `/usr/libexec/java_home -v 1.8`을 쓴다. JDK 17로 만든 `golden.json`도 byte 단위로 같았다(`oracle-manifest.json`의 `runtime_jdk`만 다르다).
- `--generate`는 비어 있지 않은 디렉터리를 거절한다. 생성 후 leak check가 JSON/text 출력에서 home 경로, 사용자 이름, hostname, 임시 디렉터리를 찾으면 실패한다.

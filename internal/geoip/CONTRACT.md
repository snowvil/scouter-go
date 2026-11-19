# legacy GeoIP(.dat) reader 계약

`internal/geoip`는 원본 Scouter 서버가 XLog 수집 때 쓰는 legacy MaxMind GeoIP 파일(기본 `conf/GeoLiteCity.dat`) 조회를 Go 표준 라이브러리로 새로 구현한다. 수집 연결(캐시, 사설 IP 판정, `TextTypes.CITY` 기록, `geoip_enabled`)과 설정 reload는 이 패키지 범위가 아니다(아래 "호출 측 계약" 참고).

## 원본 대응

| 원본 | 위치 | Go |
|---|---|---|
| `GISDataUtil.load()` | `scouter.server/.../geoip/GISDataUtil.scala` | `Open(path)` |
| `new LookupService(file, GEOIP_MEMORY_CACHE)` | geoip-api 1.3.1 | `Open`/`FromBytes`(파일 전체를 메모리에 올림) |
| `LookupService.getLocation(InetAddress)` | geoip-api 1.3.1 | `(*DB).Lookup([4]byte)` |
| `Location.countryCode/region/city/postalCode/latitude/longitude/metro_code(dma_code)/area_code` | geoip-api 1.3.1 | `Location` 같은 이름 필드(`countryName` 제외) |
| `DatabaseInfo` 문자열, `databaseType` | geoip-api 1.3.1 | `Info()`, `Type()` |

- 기준 jar는 `com.maxmind.geoip:geoip-api:1.3.1`(SHA-256 `d4e162e4…7fc`)이다. 원본 `scouter.server/pom.xml` 의존성이고 배포본 `server/lib/geoip-api-1.3.1.jar`와 같다. 원본 저장소의 `scouter.deploy/lib/maxmind.geoip.jar`(2015, 이전 API)는 boot test build에서만 쓰여 기준이 아니다.
- geoip-api는 LGPL이다. 코드는 복사하지 않았다. 형식 지식으로 작성했고, 동작은 jar의 `javap` disassembly 관찰과 oracle 실행 결과로 맞췄다. `scouter-project/scouter-server-go`의 구현도 쓰지 않았다.

## 파일 형식

```
[trie node × segments][record 영역][00 00 00 + database info][FF FF FF + type byte (+ segments 3 byte LE)]
```

정수는 모두 little-endian이고 Java `int`(32bit, overflow wrap)로 계산한다. Go도 `int32` wrap으로 같은 값을 만든다.

### 구조 정보 탐색(생성자 `init()`)

1. 파일 끝 3 byte 위치 `p = len-3`에서 시작한다. `p < 0`이면 `IOException`(음수 seek)이다.
2. 최대 20번 `b[p..p+3] == FF FF FF`를 확인한다. 불일치하면 `p-1`로 seek하고, 이 seek가 음수면 마지막 시도여도 `IOException`이다(23 byte 미만이고 delimiter가 없으면 실패).
3. 찾으면 다음 byte가 type이다. 없으면 `EOFException`이다. type은 **signed byte**로 다루고 106 이상(106..127)이면 105를 뺀다. 0x80 이상 byte는 음수 type이 되어 보정하지 않는다.
4. type별 segments와 record 길이:

| type(보정 후) | segments | recordLength |
|---|---|---|
| 7 REGION_REV0 | 16700000 | 3 |
| 3 REGION_REV1 | 16000000 | 3 |
| 6 CITY_REV0, 2 CITY_REV1, 9 ASNUM, 21 ASNUM_V6, 32/33 NETSPEED_REV1(_V6), 31/30 CITY_REV0/REV1_V6 | type 뒤 3 byte LE | 3 |
| 5 ORG, 23 ORG_V6, 4 ISP, 22 ISP_V6, 11 DOMAIN, 24 DOMAIN_V6 | type 뒤 3 byte LE | 4 |
| 1 COUNTRY, 12 COUNTRY_V6, 8 PROXY, 10 NETSPEED | 16776960(COUNTRY_BEGIN) | 3 |
| 그 밖(0, 13 등, 음수) | 없음(`null`) | 0 |

   segments 3 byte가 파일 밖이면 `EOFException`이다. delimiter를 못 찾으면 type은 기본값 1(COUNTRY)이다. 잘린 City 파일이 Country로 열리는 이유다.
5. segments 값이 `FF FF FF`(0xFFFFFF)이면 trailer 끝 3 byte가 먼저 delimiter로 잡혀 type byte가 없어 `EOFException`이다(oracle `city_rev1_segffffff.dat`).

### database info 탐색(생성자에서 호출하는 `getDatabaseInfo()`)

delimiter를 찾았으면 `p-3`, 못 찾았으면 `len-3`에서 시작해 최대 100번 1 byte씩 뒤로 가며 `00 00 00`을 찾는다. i번째에서 찾으면 그 뒤 `i` byte가 info다(ISO-8859-1). 시작 위치나 다음 seek가 음수면 `InvalidDatabaseException("Error reading database info", cause IOException)`이다. 100번 안에 못 찾으면 info는 `""`이고 열기는 성공한다. 그래서 delimiter가 파일 앞 3 byte 안에 있거나, delimiter가 없고 끝 쪽 100 byte 안에 `00 00 00`도 없는 103 byte 미만 파일은 열기에 실패한다.

### trie 탐색(`seekCountry`)

`off = 0`에서 시작해 depth 31..0마다 node `[2*rl*off, 2*rl*off + 2*rl)`을 읽는다. IP bit가 1이면 오른쪽, 0이면 왼쪽 record를 쓴다. 값 `x >= segments`(signed 비교)이면 종료하고, 아니면 `off = x`다.

- node 범위가 buffer 밖이거나 음수(rl=4에서 상위 bit가 서면 `x`가 음수)이면 `ArrayIndexOutOfBoundsException`이다.
- segments가 없는 type은 node를 읽은 뒤 `NullPointerException`이다.
- 32번 안에 끝나지 않으면 `InvalidDatabaseException("Error seeking country while searching for <ipnum>")`이다.

### City record(`readCityRecord`)

- `x == segments`이면 **미등록**이다(Java `null`, Go `found=false`).
- record 위치 `ptr = x + (2*rl-1)*segments`(int wrap)다. `ptr < 0` 또는 `ptr > len`이면 `IndexOutOfBoundsException`(ByteBuffer.wrap)이다. 볼 수 있는 길이는 `min(len-ptr, 60)`이다. `ptr == len`이면 첫 byte에서 `BufferUnderflowException`이다.
- 순서: country index 1 byte → region\0 → city\0 → postal\0 → latitude 3 → longitude 3 → (type 2 CITY_REV1이고 국가가 `"US"`일 때만) metro/area 3.
- 문자열은 NUL(소비)이나 60 byte window 끝까지 읽는다. window 끝까지 NUL이 없으면 **마지막 byte를 버린다**(원본 `position-1` 계산). 길이 0이면 `null`이다. 원본은 빈 문자열을 만들지 않으므로 Go `""`는 Java `null`과 1:1 대응한다.
- 문자열 decode는 **ISO-8859-1 고정**이다(`charset` static field, 옵션 없음). UTF-8 byte가 들어 있으면 byte마다 U+0080..U+00FF 한 문자가 되는 mojibake를 그대로 돌려준다. 예: `"São Paulo"`의 UTF-8 byte는 `"SÃ£o Paulo"`가 된다. Go 문자열은 이 Java `String`과 같은 문자열의 UTF-8 표현이다.
- 위도/경도: 3 byte 값 `v`에 대해 `(float)v / 10000f - 180f`(단정도). 남은 byte가 3 미만이면 `InvalidDatabaseException("Unexpected end of data record when reading angle")`이다.
- metro: `combo/1000`이 metro_code(=dma_code), `combo%1000`이 area_code다. 남은 byte가 3 미만이면 `InvalidDatabaseException("... reading metro area")`이다. rev0에서는 읽지 않는다.
- 국가 코드 표는 256개이고(`country.go`), index 246과 255가 모두 `"O1"`이다. 표 전체를 Java `getAllCountryCodes()`와 비교한다.
- `getLocation`은 database type을 확인하지 않는다. Country/Region DB에서 `getLocation`을 부르면 대부분 record 위치가 파일 밖이라 `IndexOutOfBoundsException`이고, 국가 index 0(=segments)만 `null`이다. Go도 같다.

## Go API와 결과

```go
func Open(path string) (*DB, error)
func FromBytes(b []byte) (*DB, error)          // b 소유권을 DB로 넘김
func (*DB) Lookup(ip [4]byte) (Location, bool, error)
func (*DB) Type() int; func (*DB) Info() string; func (*DB) Size() int
type Error struct{ Op, Java, Msg string; Err error }
```

`Lookup` 결과는 원본의 세 경우와 1:1이다.

| 원본 | Go |
|---|---|
| `Location` 반환 | `loc, true, nil` |
| `null` 반환(미등록) | `Location{}, false, nil` |
| RuntimeException | `Location{}, false, *Error{Op:"lookup", Java:<class>}` |

처음 제안된 API `(Location, bool)`에 `error`를 더했다. 원본에서 예외는 `null`과 다른 결과를 만든다(아래 XLog 폐기). 둘을 `false`로 합치면 손상 DB가 조용히 미등록처럼 보이게 된다.

`Open` 실패도 원본 생성자 예외와 같은 조건에서 `*Error{Op:"open", Java:<class>}`를 돌려준다. Go에서만 생기는 실패(파일 없음·정규 파일 아님·크기 상한·읽기 오류)는 `Java == ""`이고 `Err`를 감싼다(`errors.Is(err, fs.ErrNotExist)` 가능).

## 호출 측 계약(ingest/runtime 참고)

- 원본 호출 경로: `XLogCore.calc`와 plugin `IXLog.setLocation`이 `conf.geoip_enabled`일 때 `GeoIpUtil.setNationAndCity(p)`를 부른다. `ipaddr` 길이 4 아님, `GISDataUtil.isOk()==false`, 이미 `countryCode` 있음, 사설 IP(127/8, 10/8, 172.16-31), `ipInt==0`이면 조회하지 않는다. 판정은 signed byte 비교다. `172`는 두 번째 octet 16..31만 사설이고, `192`는 `ip(1) == 168`이 signed byte(-88)와 168의 비교라 항상 거짓이어서 **192.168.x.x는 사설로 보지 않고 조회한다**(원본 결함, `tools/java-ingest` oracle로 확인, Go도 재현).
- `null`이면 XLog 필드를 바꾸지 않고 캐시하지 않는다. 다음 같은 IP가 오면 다시 조회한다.
- `Location`이면 `city`가 비었을 때 `"unknown"`으로 바꾼 뒤 `IntKeyLinkedMap(max 10000)`에 캐시한다. `HashUtil.hash(city)`가 처음 보는 값이면 `IntLinkedSet(max 10000)`에 넣고 `TextPermWR.add(CITY, hash, city)`를 한다. `p.countryCode = loc.countryCode`(index 0이면 `"--"`), `p.city = hash`다.
- `HashUtil.hash(String)`은 `str.getBytes()`(JVM 기본 charset)의 CRC32다. 비ASCII city(ISO-8859-1 mojibake 포함)의 hash는 원본 JVM `file.encoding`에 따라 달라진다. Go `Location.City`를 UTF-8 byte로 hash하면 `file.encoding=UTF-8` 원본과 같다.
- **예외**: `getLocation`의 RuntimeException은 `setNationAndCity`와 `calc`에서 잡히지 않는다. `XLogCore` daemon loop의 `ThreadScala.startDaemon` catch(`S189`/`S190`, `Logger.println(id, 10, …)`의 id별 10초 간격 로그)까지 올라간다. 그 결과 그 XLog는 `XLogGroupPerf`, plugin, `XLogCache`, tag count, `XLogWR` 저장에서 모두 빠진다. WEB_SERVICE는 그 전에 `VisitorCore.add`가 이미 실행됐다. 원본 결함 후보이며 Go에서 그대로 재현할지는 ingest 패키지가 정한다([ingest CONTRACT](../ingest/CONTRACT.md) §3). 이 패키지는 `error`로만 알린다.
- 원본 `getLocation`은 `synchronized`라 조회가 직렬화된다. Go `Lookup`은 불변 buffer만 읽으므로 lock이 없다.
- reload: 원본은 `geoip_data_city_file` 값이 바뀔 때만(`ConfObserver`) 다시 연다. 파일이 없으면 `S145` 로그 후 기존 `lookupService`를 유지하고, 생성자 예외는 stack trace 출력 후 기존 값을 유지한다. 처음부터 실패하면 `isOk()==false`로 GeoIP를 건너뛴다. 같은 경로의 파일 내용 변경은 반영하지 않는다(GEOIP_CHECK_CACHE 미사용).

## 의도된 차이

| 항목 | 원본 | Go | 이유 |
|---|---|---|---|
| 파일 크기 | `(int)file.length()` byte[] 할당(2 GiB 미만 사실상 무제한) | `MaxFileSize` 256 MiB 초과 거절 | 무제한 할당 금지. legacy City DB는 수십 MiB |
| 정규 파일 아님 | 디렉터리는 FileNotFoundException, FIFO 등은 열기/읽기에서 대기 가능 | `Java==""` 오류로 거절 | 대기·무제한 읽기 방지 |
| 열기 중 파일 길이 변경 | `length()`만큼 `readFully`(줄면 EOF) | stat 크기만큼 `io.ReadFull`(줄면 `Java=EOFException` 오류, 늘어난 부분 무시) | 같은 의미 |
| countryName | 제공 | 제공 안 함 | Scouter가 쓰지 않음. 256개 이름 표 불필요 |
| 오류 message | JDK별 message | Go message. class 이름(`Error.Java`)과 InvalidDatabaseException message 앞부분만 일치 | message는 계약이 아님 |
| 다른 API/모드 | getCountry/getOrg/getRegion/IPv6, GEOIP_STANDARD/CHECK_CACHE/INDEX_CACHE | 없음 | Scouter는 MEMORY_CACHE + getLocation(IPv4)만 사용 |
| 동시성 | `synchronized` | lock 없음 | 불변 데이터 |

## 상한과 자원

- 메모리: 파일 크기만큼 1회 할당(최대 256 MiB). `DB`는 불변이고 goroutine을 만들지 않는다. 닫을 자원이 없다(파일은 `Open` 안에서 닫는다).
- `Lookup`: trie 최대 32 node, record 최대 60 byte. 할당은 비ASCII 문자열 변환과 region/city/postal 문자열 최대 3개다. 할당 전에 모든 offset을 buffer 길이로 검증하고 panic 경로가 없다(fuzz로 확인).

## 검증

- Java oracle: `python3 -I scripts/java-geoip.py --check`. 이 명령은 합성 `.dat` 25개(정상 City rev1/rev0/구형 type 번호, segments 0/과대/0xFFFFFE/0xFFFFFF, record·node 영역 절단, trailer 절단, 절반 절단, Country/type 106, Region rev1, Org(rl=4, 음수 record), 알 수 없는/음수 type, 열기 실패 9종)를 다시 만든다. 그다음 geoip-api 1.3.1 `LookupService`로 조회해 `testdata/oracle/golden.json`과 byte 비교한다. 입력은 모두 합성이며 실제 GeoLite 데이터는 없다.
- golden은 JDK 8(Zulu 8.64)로 만든다. JDK 17(Zulu)로 다시 만든 `golden.json`도 byte 단위로 같았다.
- Go: `TestOracleLookups`(열기 결과, type/segments/recordLength/info, 모든 조회의 결과 종류·필드·float bit·예외 class·InvalidDatabaseException message), `TestOracleCountryCodes`, `TestOracleCoverage`(필수 경계가 golden에 있는지), `TestConcurrentLookup`(-race), `FuzzFromBytes`.

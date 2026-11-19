# 참고 구현 평가 권고 5개 적용 기록

> 이 기록의 commit SHA와 branch 이름은 공개 이전의 개발 이력을 가리키며, 이 저장소에서는 조회되지 않는다.

2026-10-14, branch `codex/phase2e-storage-ops`. [참고 구현 평가](../reference-evaluation-2026-10-14.md) §4의 권고를 호환성 우선 원칙으로 적용했다. 측정은 로컬 M1 Pro loopback(생성기·조회·서버 공유, 같은 장비에서 다른 테스트가 동시에 실행됨)이며 목표 호스트 결과가 아니다.

## 1. Store gate stall 제거 (`9886a0a`)

- eviction·archive 교체·retention이 진행 중인 `SyncData` fsync pass를 gate를 잡은 채 기다리던 stall을 제거했다. pass 중에는 handle을 cache에서 제거만 하고 close를 pass 뒤 gate 밖에서 순서대로 실행한다(`closeQueue`). 같은 경로를 다시 열면 큐의 handle을 재채택해 새 handle 위에 옛 header가 쓰이지 않는다. index eviction의 사전 fsync는 그 index 디렉터리 subtree의 data file로 한정했다.
- 불변식 유지: data fsync → header, header는 snapshot 이전 record만 참조, `Close`는 `syncRun` → `mu`.
- 검증: seam 테스트(16 file × 40ms 느린 fsync)에서 옛 코드 eviction Put 1.056s → 9ms. `go test -race ./internal/storage/...`(Java oracle 포함) PASS, `FuzzIndexFiles`/`FuzzCompressedRecords` 15s PASS, `make storage-check` PASS.
- 한계: 2000 tx/s·200 obj·1일 replay에서는 index 15개·data 11개로 32/16 상한에 못 미쳐 eviction이 일어나지 않아 전후 처리량 차이는 소음 범위다. 다중 날짜 조회 부하 시나리오는 미측정이다. gate 분할은 불필요하다고 판단했다.

## 2. protocol allocs/op 감소 (`123403f`)

같은 시점 교대 5회 median, `GOMAXPROCS=2`, 공유 호스트(ns/op ±15% 잡음, allocs/B만 결정적). 서버 TPS로 환산하지 않는다.

| benchmark | ns/op 전→후 | B/op 전→후 | allocs 전→후 |
|---|---|---|---|
| DecodePack/xlog (153B) | 583 → 538 | 624 → 368 | 10 → 8 |
| DecodePack/counter | 2907 → 3000 | 7592 → 6512 | 35 → 16 |
| DecodePack/textpack | 128 → 105 | 208 → 80 | 4 → 3 |
| EncodePack/xlog | 880 → 307 | 1024 → 256 | 11 → 1 |
| EncodePack/counter | 2110 → 1197 | 2432 → 768 | 12 → 2 |
| EncodeSteps | 4433 → 2248 | 4696 → 2176 | 11 → 2 |

최종 gate(`make bench`, 아래 §6): EncodePack/xlog 300.4ns/1 alloc, DecodePack/xlog 531ns/8 allocs. 출력 bytes 불변(Java fixture 141개 PASS, fuzz 6 target PASS). 빌린 view는 codec 내부에서만 쓰고 반환 slice는 모두 소유 복사본이다. `Value` struct(272B)가 map decode 비용을 지배하며 공개 API라 보류했다.

## 3·4. GOMEMLIMIT와 UDP queue/worker (측정 행렬)

구현과 분리한 독립 측정으로 고정 binary(HEAD `bffed9e` 빌드)의 27개 유효 run을 실행했다(공통: 20s steady + 3s warm-up, 조회 worker 2, seed 42, `GOMAXPROCS` 2; 각 설정 2회; load average 기록). 원시 결과는 저장소에 넣지 않았다.

| 설정 (2000 tx/s, 200 obj) | 재조회/예정 (2회) | UDP reject | max RSS MiB | history p99 ms |
|---|---|---|---|---|
| 기본(queue 256, worker 2, GOMEMLIMIT 없음) | 45553·45721/46000 | 227·149 | 851·834 | 323·318 |
| GOMEMLIMIT 1GiB | 45751·45592 | 142·190 | 847·837 | 355·371 |
| GOMEMLIMIT 2GiB | 45755·45635 | 129·200 | 834·828 | 337·347 |
| GOMEMLIMIT 512MiB | 45661·45368 | 181·324 | 612·592 | 172·519 |
| `--udp-queue=1024` | **46000·46000** | **0·0** | 829·865 | 265·227 |
| `--udp-workers=4` | 45613·41031 | 180·2560 | 859·804 | 327·1397 |
| queue 1024 + worker 4 | 46000·46000 | 0·0 | 850·846 | 246·390 |
| queue 1024 + worker 4 + read buffer 4MiB | 46000·46000 | 0·0 | 855·849 | 355·304 |

| 상한 탐색 | queue 1024 | 기본 |
|---|---|---|
| 3000 tx/s | 69000/69000 ×2, reject 0, TPS 2870 | 66399·66626, reject 973·854 |
| 4000 tx/s | 92000/92000 ×2, reject 0, TPS 3770~3789, p99 465~507ms | 83937·80954, reject 2500·3529 |
| 5분 soak 500 tx/s·200 obj (queue 1024) | 151500/151500, reject 0, RSS 676→705→785MiB(증가, plateau 미확인) | — |

판단과 적용:
- **GOMEMLIMIT: 기본값 미설정 유지.** 1·2GiB는 RSS·정합성·tail에 차이가 없고, 512MiB는 RSS를 약 29% 줄이지만 reject·p99 편차가 커 운영 기본값으로 제안하지 않는다. soft limit이라 RSS cap이 아니며(512MiB 설정에서 RSS 592~612MiB) live heap(visitor HLL·index header)은 GC로 줄지 않는다.
- **UDP queue 기본값 256 → 1024 적용**(이 기록의 커밋). worker 4 단독은 악화, queue 1024 위에 worker·read buffer를 더해도 추가 개선이 없어 worker 2·read buffer OS 기본을 유지한다. queue는 여전히 유한하며 포화는 `Rejected`로 센다. 최악 payload 예산은 256에서 약 16MiB, 1024에서 약 64MiB(65507B datagram 기준)이며 `MaxBufferedBytes` 상한이 함께 적용된다.
- 4000 tx/s 무손실은 이 호스트·20s 조건의 증거이며 최대 지속 처리량이나 운영 SLO가 아니다. PASS run 2건에 `tcp.connection broken pipe` 로그 1줄씩이 있었고 원인은 미확정이다.

## 5. HTTP health/pprof와 KV/TTL 관리 명령

- `--http`(`ee61387`): `GET /health` JSON 계수 + `/debug/pprof`, 기본 off, loopback 외 바인드 경고. 원본 webapp API가 아니다.
- KV/TTL 쓰기 6종(`8e9acf5`): `SET_GLOBAL_KV`, `SET_GLOBAL_TTL`, `SET_GLOBAL_KV_BULK`, `SET_CUSTOM_KV`, `SET_CUSTOM_TTL`, `SET_CUSTOM_KV_BULK`. 원본 `KeyValueStoreService` handler를 실행한 oracle 45 case(예외 3 포함)와 Go 응답 bytes·`kv2` 파일(`hfile`/`kvdata` 전체, `k2file` record)이 동일하고 READ 재조회가 일치한다. 원본의 NPE/ClassCastException 3경로는 명시 실패로 둔다. mutation 45개 중 6개 구현, 39개 미구현.

## 6. 표준 검증 (커밋 `123403f`, `git archive` 복사본)

| 명령 | 결과 |
|---|---|
| `make check` | **PASS** (253s) |
| `make race` | **PASS** (333s) |
| `make fixtures-check` | **PASS**, Java fixtures 141 |
| `make inventory-check` | **PASS** |
| `make storage-check` | **PASS** (16s) |
| `make fuzz` (11 target, 10s) | **PASS** (135s) |
| `make build` + `java-integration` | **PASS**, `integration_passed=True` |
| `make pulse-fixtures-check` | **PASS**, 124 case |
| `make bench` | 완료(§2 수치) |

gate 빌드 SHA256: `dist/scouter-go` `fdf365a9cf2d95c41b12e250f16263cd8d4a25e1fe977285dc9cef2161399f48`.

UDP queue 기본값 커밋(`c1bbab8`)의 `git archive` 복사본에서 전체 gate를 다시 실행했다: check 252s, fixtures-check(141), inventory-check, storage-check 15s, fuzz 130s, build + java-integration 20s(`integration_passed=True`), pulse-fixtures-check(124) 모두 **PASS**. `make race`(318s)는 `internal/pulse/kafka` `TestProducerRoutesToPartitionLeaders` 1건만 실패했다(`MetadataRefreshes` 2, 기대 1). 원인은 테스트 assertion의 타이밍 의존이다: coordinator가 세 Enqueue 사이에 깨면 `meta` topic만으로 metadata를 받고 뒤에 나타난 `profile`에 대해 다시 refresh하는데, 이는 의도된 missing-topic 경로이고 전송 결과·record bytes는 동일하다(`-race -count=30`에서 9회 재현). assertion을 1~2회·실패 0으로 바꾸고 `-race -count=50` PASS, `internal/pulse/...` race PASS로 확인했다. 생산 코드 변경은 없다. 이 빌드 SHA256: `dist/scouter-go` `bce24d882717b4d862f08167adf2d249b9e87adb9cc636e4b3df314c94d01418`, `dist/scouter-db` `ed230460…`, `dist/scouter-replay` `1aa484d4…`.

같은 gate 빌드로 flag 없이(기본값 queue 1024) 2000 tx/s·200 obj·20s replay를 1회 실행했다: scheduled 46000 = sent 46000 = verified 46000, UDP rejected 0, unreceived 0, shutdown residue 0, TPS(drain 포함) 1911, history p99 302ms, server-1 SIGTERM exit 0, PASS. 측정 시작 load average 2.95.

테스트 수정 커밋 `5d862de`의 archive 복사본에서 전체 gate를 한 번 더 실행했다: check 243s, **race 299s**, fixtures-check(141), inventory-check, storage-check 13s, fuzz 131s, build + java-integration 20s(`integration_passed=True`), pulse-fixtures-check(124) **모두 PASS**. 빌드 SHA256: `dist/scouter-go` `d1774922404e914e59ab4041315c9e8466f0a725585176897775d1cd5daeaa18`, `dist/scouter-db` `2ff50479…`, `dist/scouter-replay` `cd347fad…`. 이 빌드로 같은 기본값 replay를 다시 실행했다: 46000/46000, rejected 0, unreceived 0, residue 0, TPS 1904, history p99 209ms, PASS(시작 load average 4.83).

## 남은 문제

- gate stall 제거의 운영 효과(다중 날짜 조회·eviction 부하), 장기 soak의 RSS plateau, Linux 목표 호스트, Java 동일 조건 비교는 미검증이다.
- `Value` struct 축소, 관리 명령 39개, 설정 리로드, 참고 구현의 GeoIP는 후속 후보다.

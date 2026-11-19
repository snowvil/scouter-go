# Go 참고 구현 평가

## 범위와 정본

2026-10-13 기준 읽기 전용으로 조사했다. 참고 구현의 코드를 복제하지 않고, 성능 및 모듈 경계 설계만 선택적으로 참고한다. 이 문서의 수치는 이 프로젝트의 실측 성능이 아니다.

- Go 참고 구현: [`scouter-project/scouter-server-go@8a869592`](https://github.com/scouter-project/scouter-server-go/tree/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0).
- 계약 정본 Java/Scala: [`snowvil/scouter@dee47fb5`](https://github.com/snowvil/scouter/tree/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5).
- 전체 호환은 Agent wire format, Client 명령/응답, 조회 의미, 기존 저장 데이터 읽기/이관을 포함한다. 처리량 목표는 실제 완료한 XLog 수로 검증한다.

## 채택할 설계 5개

| 설계 | 고정 소스 근거 | 적용 조건 |
|---|---|---|
| 버퍼 재사용과 primitive 직렬화 | [DataOutputX:16–44,93–142](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/protocol/dataoutx.go#L16-L142) | `append`/scratch 재사용으로 할당 감소. Java byte fixture 검증 후 적용하고 stream write 오류는 전달한다. |
| positional read | [XLogData:60–114](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_data.go#L60-L114), [RealKeyFile:115–145](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/io/real_key_file.go#L115-L145) | `ReadAt`로 shared seek offset의 직렬화를 줄인다. purge/close와 조회의 수명을 제어한다. |
| bounded batch와 append buffer | [XLogWR:23,49–87](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_wr.go#L23-L87), [RealKeyFile:13–15,78–93](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/io/real_key_file.go#L13-L93) | 최대 512건 batch와 16KiB buffer는 syscall 감소 아이디어다. 큐 수락/저장 완료/동기화의 의미와 종료 drain을 먼저 정의한다. |
| bounded realtime ring과 object index | [XLogCache:20–137](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/core/cache/xlog_cache.go#L20-L137) | 선택한 object만 조회해 scan 비용을 줄인다. multi-object 반환 순서, loop/index cursor, borrowed slice 소유권을 검증한다. |
| protocol/core/db/netio 경계 | [Dispatcher:11–48](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/core/dispatcher.go#L11-L48), [Registry:7–30](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/netio/service/handler.go#L7-L30) | codec와 transport/ingest/storage/service를 구분하고 실제 command 지원 상태를 inventory로 추적한다. |

## 그대로 채택하지 않을 사항 5개

| 보류 사항 | 고정 소스 근거 | 이유 |
|---|---|---|
| README 성능을 서버 처리량으로 사용 | [README:7–15](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/README.md#L7-L15), [XLog benchmark:262–338](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_bench_test.go#L262-L338) | ~27ns/~11ns는 async `Add` 호출 시간이다. 큐가 찼을 때 버리는 비용도 포함되며 저장 완료를 기다리지 않는다. |
| silent drop/error 및 종료 방식 | [XLogWR:53–98,138–175,301–315](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_wr.go#L53-L315) | full queue는 무통보 drop, process/flush 오류는 집계하지 않는다. cancel 시 queue drain, writer join, 오류 결과 전달을 보강해야 한다. |
| 새 파일/압축 포맷을 기존 DB 호환으로 취급 | [Go XLogData:25–47](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_data.go#L25-L47), [Go zstd:19–23,49–87](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/compress/zstd.go#L19-L87) | `xlog.data`와 자체 `[00][01][zstd]` body는 원본 저장 포맷과 다르다. legacy reader 또는 이관/rollback 검증이 필요하다. |
| Pack/command 상수로 전체 지원 판정 | [Go main:216–233](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/cmd/scouter-server/main.go#L216-L233), [RequestCmd:209–301](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/protocol/requestcmd.go#L209-L301) | DroppedXLog/Status/Stack/Batch/InteractionCounter는 decode 정의가 있지만 core handler 등록이 없다. StackAnalyzer/Batch history/TagCount 조회는 상수만 있고 해당 service handler가 없다. |
| README 한 줄로 라이선스/릴리스 준비 완료 판정 | [Go README:189–191](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/README.md#L189-L191), [Go Makefile](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/Makefile) | 참고 repo에는 독립 LICENSE/NOTICE가 없다. 원본에서 가져온 코드/자산은 source attribution, copyright, license 고지를 추적하고 배포물에 포함한다. |

## 성능 근거의 한계

- [AddAndWait:308–310](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_bench_test.go#L308-L310)는 `StopTimer` 뒤 `Close`를 호출하며, `Close`는 queue drain을 하지 않는다.
- [10K write/read:584–616](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/xlog/xlog_bench_test.go#L584-L616)는 fixed sleep 후 read count를 증가시키지만 10,000건 일치를 assert하지 않는다.
- README 768MB/s는 [RealDataFile.Write:83–105](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/io/io_bench_test.go#L83-L105) 단독 benchmark다. 네트워크/decode/enrichment/삼중 인덱스/동시 조회/`fsync`를 포함하지 않는다. [Flush:87–90](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/io/real_data_file.go#L87-L90)는 `bufio.Writer.Flush`다.
- 14ns는 [MemHashBlock.Get:42–58](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/db/io/io_bench_test.go#L42-L58)의 미리 채운 100K key 조회다. Java 대비 메모리/시작/GC 주장에 대응하는 동일 조건 비교 harness는 조사 범위에서 확인하지 못했다.
- `GOMEMLIMIT`는 Go runtime soft limit이다. RSS hard cap 또는 OOM 방지 보장으로 해석하지 않는다.

## 원본과의 차이 및 출처

원본 [XLogDataWriter:55–80](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/xlog/XLogDataWriter.scala#L55-L80)는 `xlog.service.conf`, `xlog.service` 또는 `GZipStore`를 쓴다. 원본 [ProfileDataWriter:56–82](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/xlog/XLogProfileDataWriter.scala#L56-L82)는 `xlog.profile.conf`, `xlog.profile` 또는 `GZipStore`를 쓴다. Go 자체 reader/writer roundtrip이 이 파일들을 읽는다는 증거는 아니다.

원본 [NetDataProcessor:153–220](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/netio/data/NetDataProcessor.scala#L153-L220)는 InteractionCounter/DroppedXLog/Status/Stack/Batch를 실제 처리한다. 원본 [BatchService:44](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/netio/service/handle/BatchService.scala#L44), [StackAnalyzerService:32](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/netio/service/handle/StackAnalyzerService.scala#L32), [TagCountService:45](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/netio/service/handle/TagCountService.scala#L45)는 해당 조회 command를 처리한다.

Java/Go RequestCmd 상수는 정적 비교에서 각각 247개지만 **command constant != support**다. Go의 단순 agent proxy는 동적 등록하므로 direct registration count를 전체 지원률로 사용하지 않는다. Go [Pack test:10–39](https://github.com/scouter-project/scouter-server-go/blob/8a869592f51ee1f8d5e98a61dc3f1486e00b78b0/internal/protocol/pack/pack_test.go#L10-L39)는 Go serializer/parser roundtrip이다. Java golden 및 실제 Agent/Eclipse Client gate가 별도로 필요하다.

라이선스 출처는 원본 [LICENSE.md](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/LICENSE.md)와 [XLogWR copyright/license header:1–17](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/XLogWR.scala#L1-L17)다. 이 평가 단계에서는 참고 Go 소스 코드를 복사하지 않았다.

## 실행 검증

환경: Go 1.27.2, Darwin/arm64, `GOMAXPROCS=2`. 참고 clone HEAD가 위 SHA와 일치하고 실행 전 clean임을 확인했다.

```sh
GOMAXPROCS=2 .tools/go/bin/go test ./...
```

결과: **FAIL, 종료코드 1**. `internal/http`의 다음 2개 테스트가 실패했다.

```text
--- FAIL: TestCounterRealtimeEndpoint (0.00s)
    server_test.go:201: expected status 200, got 404
--- FAIL: TestCounterRealtimeEndpointFloatValue (0.00s)
    server_test.go:235: expected status 200, got 404
```

나머지 테스트가 있는 패키지는 PASS다. 테스트가 없는 패키지는 검증 완료로 취급하지 않는다. 실패 원인은 추가 분석하지 않았으며 참고 소스는 수정하지 않았다. 실행 후 clone HEAD 일치 및 clean을 재확인했다. `-race`, benchmark, Java golden, 실제 Agent/Client 및 부하 테스트는 이번 평가에서 실행하지 않았다. 테스트 결과만으로 전체 호환 또는 목표 처리량을 선언하지 않는다.

남은 gate: unique txid 기준 수신/수락/저장/조회 count 일치, Java Agent replay/Client 조회, 동시 과거 조회, queue saturation, disk full, 날짜 전환, SIGTERM drain, restart 및 기존 DB 이관/rollback. 부하 결과에는 payload/profile mix, 공유 호스트 CPU quota, RSS, disk/fsync 정책, p95/p99 지연, drop/error/queue depth를 함께 기록한다.

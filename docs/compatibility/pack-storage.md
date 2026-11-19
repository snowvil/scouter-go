# Status/Summary/Alert/Stack/Batch 저장 계약

정본은 Scouter commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`이며 원본 Apache-2.0 저작권/출처를 구현 파일에 유지한다.

| Pack | 원본 파일과 레코드 | 인덱스 |
|---|---|---|
| Status | `<date>/status/status.pstat`: int32 길이 + 전체 Pack | `status_tim.{kfile,hfile}`, 시간키8byte + offsetLong5 |
| Summary | `<date>/sum/sum.sum`: int32 길이 + 전체 Pack | APP1→`sum_app_tim`, SQL2→`sum_sql_tim`, end-user10/11/12→`sum_enduser_tim`, 나머지→`sum_other_tim` |
| Alert | `<date>/alert/alert.alert`: **signed short** 길이 + 전체 Pack, 최대32767byte | `alert_tim.{kfile,hfile}` |
| Stack | `<date><objName>/stack.dat`: int32 길이 + 전체 Pack | `stack.idx`: 반복 Long8 time + signed Long5 offset, **13byte** |
| Batch | `<완료일><objName>/batch.dat`: int32 길이 + 전체 Pack | `batch.idx`: 반복 Long8 `(startTime+elapsedTime)` + signed Long5 offset, **13byte** |

[StatusWriter](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/status/StatusWriter.scala#L45),
[SummaryWR](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/SummaryWR.scala#L54),
[SummaryWriter](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/summary/SummaryWriter.scala#L45),
[AlertWriter](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/alert/AlertWriter.scala#L45),
[StackAnalyzerDB](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/StackAnalyzerDB.scala#L137),
[BatchDB](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/db/BatchDB.scala#L143).

저장 계층은 전달된 time을 보존한다. 원본 StatusCore/AlertCore의 서버 시각 덮어쓰기는 수집 계층의 책임이다. Summary는 stype/table/time을 바꾸지 않는다. Stack/Batch의 objName은 수집 계층이 AgentManager 동작에 따라 결정한다. 원본 Stack는 Agent가 없으면 저장하지 않고 Batch는 Pack 정보로 Agent를 생성한다. 파일 저장 API는 Agent registry를 직접 변경하지 않는다.

## 조회와 Batch simple 응답

Status/Summary/Alert는 legacyindex의 500ms 시간 bucket과 정상 원본 시간/offset 순서를 사용한다. Summary 조회는 선택한 index group 안에서 요청 stype만 반환한다. Stack/Batch는 [BinSearch.searchBE](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.server/src/main/scala/scouter/server/util/BinSearch.scala#L39)의 midpoint equality와 `(target-recordTime).toInt`를 그대로 재현하고 그 위치부터 **물리 idx 순서**로 읽는다. 첫 `time>to`에서 종료한다. 동일 timestamp의 앞쪽 레코드가 누락되거나 out-of-order 입력에서 조회가 누락되는 원본 동작을 임의로 정렬/보정하지 않는다.

Batch history의 Position은 physical data offset을 주입한다. SQLStats/UniqueSQLs/Extra는 원본 readSimplePack처럼 history 출력에서 제외하고, detail은 저장된 전체 Pack을 새로 읽는다. detail time은 파일 날짜 선택에 사용하므로 같은 날짜의 다른 시각이어도 원본처럼 해당 physical position을 읽는다.

[BatchPack.writeSimple/readSimple](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.common/src/main/java/scouter/lang/pack/BatchPack.java#L85)의 별도 codec `protocol.EncodeBatchSimple/DecodeBatchSimple`은 **Blob body만** 처리한다. Pack type/HasNEXT byte를 포함하지 않는다. SQLTotalCount는 metadata이므로 양수여도 detail 배열이 없어야 정상 simple body다. Client LIST는 HasNEXT+simple Blob, DETAIL은 HasNEXT+전체 BatchPack이다.

## API와 경계

- `PutStatusContext`, `PutSummaryContext`, `PutAlertContext`; `StatusesContext`, `SummariesContext(...,stype)`, `AlertsContext`.
- `PutStackContext(ctx,objName,p)`, `StacksContext`; `PutBatchContext`, `BatchesContext`, `BatchAtContext`.
- 모든 query는 동일 Java 날짜 안의 inclusive 범위이며 config보다도 넓지 않은 **최대24시간/100000record/64MiB** 상한을 적용한다. Summary group의 scan count/bytes도 집계하여 filter로 자원 상한을 우회하지 못한다. budget 초과 시 partial 성공 없이 error를 반환한다.
- record 길이·물리 파일 extent·남은 query byte budget을 **할당 전** 확인한다. malformed codec/type/time-index mismatch,13byte idx partial tail, 음수 offset/길이, Batch 완료시각 overflow는 명시적 error다. 파일 content가 임의로 조작된 경우의 완전한 record 무결성 검사는 보장하지 않는다.
- objName은 `/host/app` 같은 canonical slash path이며 빈/`.`/`..`/빈 segment/역슬래시/NUL/잘못된UTF8 및 기존 directory symlink를 거절한다. date는 기존 Store의 날짜 검증을 사용한다.
- Mutex 획득 대기와 query/file 읽기 경계에서 context를 확인한다. 취소된 write는 첫 append 전에 중단하며, append가 시작되면 index를 완성한 뒤 취소를 보고할 수 있다. syscall과 기존 legacyindex의 Open/Scan 내부 작업을 중간에 강제 종료하지 않는다.
- decoded Pack/slice는 호출자가 소유한다. 기존 Store의 bounded file/index cache를 사용하여 Sync/Close가 새 파일을 flush/close한다. cache에서 evicted된 idx handle은 후속 IO 전에 재획득한다. 다중 writer/process coordination과 두 파일의 원자적 commit은 제공하지 않는다.

## 검증 수준

단위 테스트는 prefix/index routing, 재시작, 정상/동일timestamp/out-of-order 조회, Batch simple metadata, ownership, typed nil, malformed/truncated 자료, path/date 경계, 취소, record/byte 상한 및 file cache eviction을 확인한다.

`TestJavaExtraStorageInterop`는 `scripts/java-storage.py --generate-extra`로 **수정하지 않은 원본 writer/reader**가 생성한 Pack/DB를 읽고, Go writer 출력 파일을 원본 파일 bytes와 비교하고 `--verify-extra`의 원본 reader로 다시 조회한다. 실제 `Batch.writeSimple` Blob golden도 양방향 codec에 사용한다. Source/도구가 없는 환경의 skip은 Java 호환 통과가 아니다. synthetic 소규모 fixture 근거이며 운영 DB 이관, crash recovery, 실제 Agent/Client 및 지속 TPS 검증을 대체하지 않는다.

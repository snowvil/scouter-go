# 추가 Pack 9종의 byte 계약

정본은 baseline에 고정한 [Scouter `dee47fb5`](https://github.com/snowvil/scouter/tree/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5)다. 참고 Go 구현을 복제하지 않고 원본 Java `read/write`를 기준으로 독립 구현했다. 기존 7종에 다음 9종을 추가하며 현재 `PackEnum`의 기본 Pack 16종 byte codec을 구성한다. 서버 처리·저장·조회 호환이나 처리량을 이 결과로 선언하지 않는다.

아래 `D`는 Scouter signed Decimal, `I`는 int32, `L`은 int64, `S`는 int16, `B`는 byte, `T`는 UTF-8 Text, `V`는 tagged Value다. 모두 big-endian이며 각 body 앞에 Pack type byte가 붙는다.

| type | Java/Go Pack | wire body 순서 |
|---|---|---|
| 22 | DroppedXLogPack | gxid L, txid L |
| 31 | SpanPack | gxid/txid/caller/timestamp L; elapsed D; spanType B; name/objHash/error D; local service D/IP Blob/port S; remote service D/IP Blob/port S; debug/shared Boolean; annotationTimestamps/annotationValues ListValue; tags MapValue |
| 32 | SpanContainerPack | gxid L, spanCount D, timestamp L, spans Blob |
| 61 | StatusPack | time D, objType T, objHash D, key T, data MapValue |
| 62 | StackPack | time D, objHash D, data Blob |
| 63 | SummaryPack | time D, objHash **I**, objType T, stype B, table MapValue |
| 64 | BatchPack | 아래 내부 body를 **Blob**으로 감싼다 |
| 65 | InteractionPerfCounterPack | time **L**, objName/interactionType T, fromHash/toHash/period/count/errorCount **I**, totalElapsed L, customData MapValue |
| 70 | AlertPack | time **L**, level B, objType T, objHash **I**, title/message T, tags MapValue |

`SpanPack` endpoint port는 Java short와 같은 signed int16으로 유지한다. 예를 들어 port 65535의 wire `ffff`는 Go에서도 `-1`로 읽으며 재송신 시 같은 bit열이 된다. IP는 Blob이므로 IPv4/IPv6를 고정 길이로 변환하지 않는다. annotation List의 item이나 Map 내용에 codec 자체의 추가 스키마를 적용하지 않는다.

`SpanContainerPack.Spans`는 연속 Span wire packet의 owned opaque bytes, `StackPack.Data`는 owned opaque 압축 payload다. 여기서는 span 수와 nested packet 수의 대조 및 stack 압축 해제를 수행하지 않는다. Scalar인 spanCount를 보존하고 Blob의 byte 상한을 적용한다.

## Batch body

내부 body 순서는 startTime L, objHash I, batchJobId/args T, pID I, elapsedTime L, threadCnt I, cpuTime/gcTime/gcCount L, sqlTotalCnt I, sqlTotalTime/sqlTotalRows/sqlTotalRuns L, isStack Boolean, objName/objType T, position L이다.

`sqlTotalCnt > 0`이면 다음 두 배열이 이어진다. 두 배열 모두 별도 count header 없이 `sqlTotalCnt`를 따른다.

1. SQLStats: hashValue/runs I, startTime/endTime/totalTime/minTime/maxTime/processedRows L, rowed Boolean. 각 record는 57 byte다.
2. UniqueSQLs: hash I, SQL text T. 각 record는 최소 5 byte다.

Go는 배열 할당 전에 count의 비음수, collection 상한, aggregate 항목 예산, `count × 62` 이상의 남은 byte를 확인한다. SQLStats와 UniqueSQLs를 합산하여 `MaxTotalValues`에 반영한다. encode는 SQLTotalCount와 두 배열 길이의 불일치를 거절한다. Java writer의 불일치 count로 만든 불완전 body를 정상 packet으로 취급하지 않는다.

Java UniqueSQLs는 HashMap으로 insertion 순서를 보장하지 않는다. Go의 `[]BatchSQLText`는 수신한 wire 순서를 유지한다. 중복 hash를 받으면 wire entry를 보존하며 Java HashMap의 overwrite/재열거에 따른 byte 변경을 수행하지 않는다. 사전으로 이용할 때는 마지막 entry가 이기는 Java 의미를 적용해야 한다. 알려진 body 이후의 잔여 byte는 Java reader가 무시하지만 Go는 owned `Extra`로 저장하여 재송신한다.

Java `BatchPack.writeSimple/readSimple`는 SQLStats/UniqueSQLs를 생략하는 별도 query projection이다. 양수 sqlTotalCnt가 있는 simple body는 표준 Java `readPack`에서도 읽을 수 없으므로 이 표준 Pack codec에 혼합하지 않는다. query projection 구현은 별도 계약 단위다.

## 검증과 의도된 차이

`PackOracle.java`는 고정 Java 구현의 writer로 synthetic fixture 12개를 만들며 Java의 실제 reader로 모든 public field의 의미 일치를 확인한다. MapValue/HashMap은 key별로, ListValue는 item 순서로 비교한다. Go 테스트는 각 field의 독립된 literal 기대값과 Java 생성 byte로의 exact reencode를 검증한다.

9 Pack의 정상 fixture에 Batch SQL count 0, Stack empty, SpanContainer empty를 추가한다. 모든 prefix 절단 거절, typed nil, Map/List field 타입, Blob 소유권, Batch의 거대/음수 count와 배열 길이 불일치, aggregate 상한, 알려지지 않은 Batch tail, stream reader가 다음 frame sentinel을 소비하지 않는 동작을 검증한다. `FuzzExtraPack`은 이 12개 fixture를 seed로 buffer/stream의 decode→encode 경계를 검증한다.

malformed Decimal/UTF-8/int32 overflow의 의도적 거절은 `internal/protocol/CONTRACT.md`를 따른다. Java에서 cast error가 되는 Map/List 타입의 불일치는 panic 대신 `ErrInvalid`로 처리한다. Batch의 음수 count도 거절한다. JVM extension Pack은 이번 범위에 포함하지 않으며 `ErrUnsupported`를 유지한다.

## Source attribution

Copyright 2015 the original author or authors, https://github.com/scouter-project/scouter. Apache License 2.0. 정본의 [`PackEnum.java`](https://github.com/snowvil/scouter/blob/dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5/scouter.common/src/main/java/scouter/lang/pack/PackEnum.java)와 같은 directory의 `DroppedXLogPack.java`, `SpanPack.java`, `SpanContainerPack.java`, `StatusPack.java`, `StackPack.java`, `SummaryPack.java`, `BatchPack.java`, `InteractionPerfCounterPack.java`, `AlertPack.java`의 field/read/write를 참조했다. Java source는 변경하지 않는다.

# Pulse 내장 기능 호환 계약

Pulse plugin(Scouter Server용 Java plugin)과 Pulse 수집 서비스(Kafka 수신 측)의 원본은 비공개다. 이 문서는 그 원본을 조사해 확정한 계약의 **의미**만 기록하며 원본의 파일 경로·줄 번호·내부 class 이름은 싣지 않는다. `scouter:` 접두 경로는 공개 Scouter 원본([baseline.json](baseline.json)의 `scouter`) root 기준이다.

## 상태와 기준

Go Pulse 기능은 **구현·golden 검증·격리 broker 통합 검증 완료**(2026-10-14, [Phase 3 기록](../validation/2026-10-14-phase3-pulse.md))이며 Pulse 수집 서비스 실연동과 운영 부하는 미검증이다. 구현은 `internal/pulse`(callback → bounded queue/worker → 변환/필터 → Sink)와 `internal/pulse/kafka`(표준 라이브러리 producer)이고, 서버는 `--pulse-conf <scouter.conf>`로 켠다. Kafka 응답 대기는 서버 수집 경로에 없다. 운영 코드에 Java는 없으며 Java 정본은 golden을 만들 때 검증 oracle로만 실행했다. 구현 범위와 의도된 차이는 [internal/pulse/CONTRACT.md](../../internal/pulse/CONTRACT.md)에 있다.

| 원본 | 기준 / 버전 | 용도 |
|---|---|---|
| Pulse plugin (비공개) | [baseline.json](baseline.json)의 `pulse_plugin.source_digest` / 제품 `1.0.0` | callback, 설정, 필터, Kafka 출력 정본 |
| Pulse 수집 서비스 (비공개) | 조사 시점(2026-10-13) 원본 | header 수신, JSON/Step decode, application 등록 경계 |
| Scouter (공개) | `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5` / `2.21.3` | 서버 및 wire 원본; plugin의 dependency와 버전이 다름 |
| plugin dependency | Scouter `2.20.0`, Kafka clients `4.0.0`, Jackson `2.15.2`, Java 11 | golden을 생성한 Java oracle 실행 환경 |

Pulse plugin 원본은 commit이 아니라 **원본 내용 digest**로 고정한다. `source_digest`는 plugin `src/main` 아래 모든 파일의 (상대 경로, sha256)을 경로순으로 정렬해 계산한 sha256이며 [baseline.json](baseline.json)에 기록한다. 커밋된 golden(`internal/pulse/testdata/pulse-golden.json`, `pulse-metadata.json`)은 **그 비공개 원본에서 생성한 고정 자료**다. 생성 도구(Java oracle harness)는 원본의 내부 class를 직접 호출하므로 이 저장소에 없고, 재생성은 비공개 저장소에서만 가능하다. 이 저장소에서는 `go test ./internal/pulse/`로 Go 구현을 그 golden과 대조하며, 이 테스트는 원본 없이 실행된다. Pulse 수집 서비스는 이 문서의 downstream 조사 대상일 뿐 baseline에 고정하지 않았다. 확인일은 2026-10-13이다. 원본 저장소는 읽기 전용이다. 원본 문서와 소스가 다르면 현재 코드의 관찰 결과와 문서 설명을 구분한다.

## Callback, queue와 순서

`PLUGIN_SERVER_XLOG` callback은 `XLogPack`, `PLUGIN_SERVER_PROFILE` callback은 `XLogProfilePack`을 받는다. enable이 false이거나 dispatcher가 없으면 즉시 반환한다. callback은 원본 pack 참조를 task 객체에 담아 각각 별도 dispatcher에 enqueue한다. dictionary 조회·필터·JSON 생성·Kafka send는 worker에서 수행한다. pack deep copy는 없다.

각 dispatcher는 `ArrayBlockingQueue`와 고정 worker pool을 사용한다. queue/worker 설정은 최소 1로 보정한다. `queue.offer`는 대기 없이 신규 task를 거절하며 기존 task를 밀어내지 않는다. 포화 시 dropped counter와 로그가 발생한다. running=false/null task 거절은 dropped counter를 증가시키지 않는다.

기본 worker는 dispatcher별 2개이므로 FIFO dequeue만으로 실제 send 순서를 보장할 수 없다. xlog/profile의 queue·topic도 별도이므로 trace 선행 처리를 가정하지 않는다. HTTP status 제외 txid 공유 map은 10분 TTL이며 txid=0은 기록하지 않는다. profile이 먼저 처리되면 이후 trace status 제외를 소급 적용하지 못한다.

Go에서 비동기 작업에 넘기는 pack/slice는 소유권을 확보한다. Java의 참조 공유를 그대로 모사해 재사용 buffer의 race를 만들지 않는다. 순서 보장 강화나 status 제외 race 해결은 의도된 차이로 기록하고 fixture 및 Pulse 수집 서비스 통합 검증 후 도입한다.

## Kafka record와 bytes

| 항목 | 현재 Java 계약 |
|---|---|
| Topic | meta/profile별 설정 이름 `ext_plugin_pulse_kafka_xlog_meta_topic`, `ext_plugin_pulse_kafka_xlog_profile_topic` |
| Key | `pack.txid` signed int64; Kafka `LongSerializer`의 8-byte big-endian |
| Partition | `Math.floorMod(application.hashCode(), kafkaTopicPartitionCount)` |
| Value | Jackson UTF-8 JSON bytes를 `ByteArraySerializer`로 전송 |
| Header 순서/값 | `apm_type=SCOUTER`, `xlog_serializer_version=V1`, `xlog_application=<application>`; 모두 UTF-8 bytes |
| Timestamp | explicit timestamp를 지정하지 않는 `ProducerRecord(topic, partition, key, payload)` |

application은 event 날짜의 OBJECT dictionary 문자열을 `/`로 split한 세 번째 요소에서 `@` 앞부분을 취한다. 추출 후 lowercase/trim을 적용하지 않는다. Kafka partition hash는 **Java String.hashCode**이며 UTF-16 code unit, int32 overflow, 음수 floorMod를 보존한다. Go의 rune hash, CRC32, Kafka 기본 partitioner로 대체하지 않는다.

Java `LongSerializer`의 big-endian 구현은 조사 시 cached Kafka 4.0.0 jar의 `javap -c`로 확인했다. jar bytecode 확인은 원본을 고정한 Java oracle fixture를 대신하지 않는다.

## JSON과 profile Step

Trace 변환은 `txid→traceId`, `endTime→traceEndTime`, `objHash→objectDictId`, `service→transactionDictId`, `threadNameHash→threadDictId`, `caller→parentTraceId`, `gxid→globalTraceId`를 수행한다. elapsed/error/cpu/sql/ip/memory/apicall/profile/sampling 필드와 object/transaction/thread/error dictionary 문자열이 동반된다. `XLogPack.status`는 필터에 사용하지만 payload로 복사하지 않는다. 전체 필드·타입은 plugin 원본 기준이며 Java golden 출력으로 확정했다.

Span 변환 필드는 `traceId,time,objectDictId,transactionDictId,elapsed,profile,objectDictContent,transactionDictContent`다. event 날짜로 object/transaction dictionary를 조회한다.

`ipaddr`는 Java `byte[]`의 Base64 JSON 표현이다. JSON number는 signed long/int/byte를 손실 없이 표현해야 한다. null 문자열·IP·profile 및 primitive zero/false를 임의로 생략하지 않는다. Jackson getter에서 정해지는 실제 property 이름과 객체 key 순서는 Java golden 출력으로 확정한다. 모델의 private field 이름만 보고 JSON key를 추정하지 않는다.

profile은 단일 Base64 blob이 아니라 다음 구조의 배열이다.

```json
[
  {
    "mainValue": "synthetic operation",
    "additionalValueList": [],
    "step": "<standard Base64 of Step.toBytes(List.of(singleStep))>"
  }
]
```

위 예시는 구조 설명용이며 golden 또는 유효한 binary fixture가 아니다. `pack.profile=null`이면 profile=null, 빈 bytes이면 빈 list다. 순서는 raw profile에서 읽은 Step 순서다. 각 step은 type byte와 Scouter Step binary를 포함하는 singleton encoding이다. 전체 chunk의 Base64로 대체하지 않는다. Pulse 수집 서비스는 Base64를 decode하고 `Step.toObjectList(bytes).getFirst()`를 사용한다.

`mainValue`는 METHOD/SQL/APICALL/THREAD_SUBMIT/HASHED_MESSAGE/PARAMETERIZED_MESSAGE/DISPATCH/THREAD_CALL_POSSIBLE/SPAN별 dictionary에서 얻는다. MESSAGE는 message 문자열, SOCKET은 IP 문자열이다. DUMP의 additionalValueList는 stack 문자열 목록이고 다른 일반 type은 빈 list다. dictionary hash=0은 빈 문자열이며 miss/blank는 `**unlabeled**:<type>:<hash>`다.

profile dictionary preloading은 현재 날짜를 사용하고, 후속 개별 lookup은 event 날짜를 사용한다. cross-day/cache-hit fixture에서 이 차이를 관찰해야 한다. timezone을 UTC로 임의 고정하지 않는다.

## 필터와 transaction 변경

현재 trace worker 처리 순서는 다음과 같다.

1. application 추출 실패 제외.
2. application exclusion.
3. HTTP status rule 제외와 txid TTL 기록.
4. Trace payload 변환/dictionary 조회.
5. transaction 확장자 exclusion.
6. Spring URI filter.
7. application별 rebase.
8. global suffix 제거.
9. JSON 생성 및 Kafka send.

profile은 가장 먼저 HTTP-status-filtered txid map을 검사하고, application 추출/exclusion → profile 변환 → transaction 확장자 → Spring URI → rebase → global suffix → JSON/send 순서다. profile 자체에 HTTP status 필드는 없다.

Spring URI filter는 transaction이 `/`로 시작하면서 `<`와 `>`를 모두 포함하지 않으면 제외한다. configured non-Spring application과 filter disable은 이를 우회한다. extension exclusion은 마지막 `.` 뒤 문자열을 lowercase 비교한다.

rebase는 delimiter의 n번째 위치 앞까지 남기며 n<2, 조건 null 또는 n번째 delimiter가 없으면 원문을 유지한다. global filter는 여러 조건 중 가장 앞에 나타난 문자열부터 suffix를 제거한다. 내용 변경 후 `transactionDictId`를 **Scouter HashUtil CRC32**로 다시 계산한다. 이것은 partition용 Java String.hashCode와 다르다. HashUtil String 입력은 JVM default charset의 `getBytes()`를 사용한다. 근거: `scouter: scouter.common/src/main/java/scouter/util/HashUtil.java`. 비ASCII와 charset을 oracle 생성 메타데이터에 기록한다.

## 설정과 producer 운영 정책

설정은 Scouter `Configure`에서 static 초기화 시 한 번 읽는다. Java code의 직접 환경변수 lookup이나 runtime reload 계약은 없다. 서버 주소는 아래 설정 이름만 기록하며 endpoint 값은 포함하지 않는다.

| 설정 이름 | 현재 기본값/문법 |
|---|---|
| `ext_plugin_pulse_kafka_servers` | 서버 주소 설정; 값 비공개 |
| `ext_plugin_pulse_kafka_xlog_meta_topic` | `dev-pulse-scouter-xlog-meta` |
| `ext_plugin_pulse_kafka_xlog_profile_topic` | `dev-pulse-scouter-xlog-profile` |
| `plugin_pulse_kafka_topic_partition_count` | 30; 이 key에는 `ext_` prefix가 없음 |
| `ext_plugin_pulse_kafka_produce_timeout` | 5000ms |
| `ext_plugin_pulse_kafka_dispatch_queue_capacity` | dispatcher별 4096 (Go는 개수 상한에 더해 queue별 byte 상한 XLog 16MiB·Profile 64MiB를 둔다. 설정 키 없음, [CONTRACT](../../internal/pulse/CONTRACT.md) "queue byte 상한") |
| `ext_plugin_pulse_kafka_dispatch_worker_count` | dispatcher별 2 |
| `ext_plugin_pulse_enable`, `ext_plugin_pulse_debug` | true |
| `ext_plugin_pulse_enable_spring_uri_filter` | true |
| `ext_plugin_pulse_exclude_transaction_extensions` | `woff,woff2,eot,svg,ttf` |
| `ext_plugin_pulse_exclude_applications` | 원본: 특정 배포의 서비스 이름 1개. Go: 빈 값(의도된 차이) |
| `ext_plugin_pulse_non_spring_based_applications` | 원본: 특정 배포의 서비스 이름 4개. Go: 빈 값(의도된 차이) |
| `ext_plugin_pulse_rebase_transaction_applications` | 원본: 특정 배포의 서비스 이름 1개 `\|/\|3`. Go: 빈 값(의도된 차이); comma-separated `application\|delimiter\|n` |
| `ext_plugin_pulse_rebase_transaction_global_filter` | `$$,@` |
| `ext_plugin_pulse_drop_http_status_rules` | empty; `application\|exactStatus`, `application\|Nxx`, `*\|...` |

**애플리케이션 이름 필터 기본값(의도된 차이):** 원본 plugin 기본값은 특정 배포의 서비스 이름이라 Go는 위 세 키를 빈 값으로 제공한다. 키가 없으면 제외·rebase 대상이 없고 모든 application을 Spring 기반으로 보아 URI filter를 적용한다. 운영은 세 키를 `scouter.conf`(또는 `--pulse-conf` 파일)에 명시한다. `--pulse-conf`를 쓰면 그 파일만 읽고 `scouter.conf`의 Pulse 키와 병합하지 않는다. Pulse가 켜졌는데 세 키 중 없는 키가 있으면 기동 시 키별 `WARNING: pulse: <key> is not set` 줄을 남긴다. `ext_plugin_pulse_non_spring_based_applications`는 Spring URI filter(`ext_plugin_pulse_enable_spring_uri_filter`, 기본 true)가 켜졌을 때만 의미가 있어 filter가 꺼져 있으면 경고하지 않는다. 의도적으로 비우려면 `key=`로 명시한다(결과는 키 생략과 같고 경고가 없다). 키가 있을 때의 파싱·매칭 의미는 원본과 같다. golden을 생성한 Java oracle은 plugin 기본값에 기대지 않도록 모든 profile에 중립 이름을 명시했다(`pulse-metadata.json`의 `profiles.*.conf`).

configured exclusion/rebase/non-Spring sets는 trim/lowercase 처리하지만 extracted application은 lowercase하지 않는다. HTTP status rule matching만 application을 trim/lowercase한다. 따라서 casing 차이를 제거하는 것은 동작 변경이다.

producer 설정은 `acks=0`, `retries=0`, `batch.size=128KiB`, `linger.ms=20`, `compression.type=lz4`, `buffer.memory=512MiB`, `max.in.flight.requests.per.connection=5`다. timeout을 T라 하면 request/max.block=T, delivery=T+100ms다.

queue 포화는 drop-new이며 Kafka buffer/metadata 대기는 worker 안에서 발생한다. async send failure는 failed counter와 로그만 남기고 재enqueue하지 않는다. synchronous lookup/serialization/send exception은 worker 로그 후 다음 task로 넘어가며 failed send counter로 모두 집계되는 것은 아니다. `sent`는 callback 성공이므로 브로커 영속 저장 증거가 아니다. queue counters는 10초 status logging 후 reset한다.

종료는 dispatcher의 `shutdownNow`이며 queue drain이 없다. dispatcher별 close가 공유 producer를 각각 flush/close하고 plugin이 다시 close한다. 종료 시 잔여 task 처리 보장은 없다. Go에서는 명확한 단일 producer owner와 bounded shutdown을 설계하고 drain/drop의 의도된 차이를 검증한다.

**도입 결정:** 원본의 `acks=0/retries=0`를 호환 기준으로 기록한다. 조사만으로 `acks=all`, retry, idempotence를 자동 적용하지 않는다. 내구성 옵션 변경은 독립 결정이며, 옵션별 throughput/latency/drop/duplicate/ordering/flush/recovery를 같은 입력과 자원 한도에서 측정한 뒤 선택한다. 호스트 전체 사양은 Pulse queue/buffer/worker 예산이 아니다. 실제 CPU quota, 메모리·디스크 예산을 합의하고, Java 512MiB buffer를 Go에 무조건 복제하지 않는다.

## Pulse 수집 서비스와 UUID 경계

header `xlog_application`은 application **문자열 ID**이며 UUID가 아니다. Pulse 수집 서비스는 UTF-8 header bytes/String을 trim하여 ID로 조회하고 없으면 application을 생성한다. active=false이면 처리를 반환한다.

Pulse 수집 서비스가 등록 시 자체 UUID를 발급한다. Go plugin은 application 이름 대신 UUID를 header에 넣거나 다른 backend 구성요소의 UUID를 생성하지 않는다. 다른 backend UUID가 서로 동일하다는 가정은 이 경계에서 금지한다. 이번 조사는 Pulse 수집 서비스 수신까지만 확인했으며 수집 서비스 뒤 구성요소와의 UUID 연계 실행은 미검증이다.

version header는 enum `V1`이다. Scouter 제품 버전 `2.20.0`/`2.21.3`을 넣지 않는다. Trace는 object/transaction/thread dictionary ID가 모두 nonzero, Span은 object/transaction ID가 nonzero여야 한다. invalid payload가 method return으로 제외되는 경우와 exception을 구분한다.

Pulse 수집 서비스 consumer는 exception을 catch하여 로그/내부 failure observation을 발행하고 함수 밖으로 다시 던지지 않는다. 따라서 이 code만으로 binder retry/DLQ가 실행된다고 가정할 수 없다. 실제 offset/재전달 결과는 통합 환경에서 측정한다.

## 원본 문서 차이와 결함 후보

plugin 원본에 딸린 설명 문서는 batch 64KB, shutdown hook 없음, tests 없음이라고 설명한다. 고정한 원본 코드에는 batch 128KiB, shutdown hook, producer contract 테스트가 있다. Go 계약은 현재 코드 기준이며 이 오래된 설명을 완료 증거로 사용하지 않는다.

아래는 **정적 결함 후보이며 실행 재현 전**이다. 실패 입력/기존 Java 출력/Go의 의도된 차이를 별도 fixture로 관리한다.

- application parsing guard는 split 배열 길이<=1만 확인하고 index 2를 읽는다. 길이 2 또는 dictionary null 입력은 exception 후보다.
- SPAN/SPANCALL을 plugin 자체의 span step 모델로 바꾸지만 그 모델은 StepSingle을 상속하며 write/read override가 없다. StepSingle.write는 parent/index/start_time/start_cpu만 기록하며 표준 CommonSpanStep decoder는 뒤의 hash/endpoint/tags까지 읽는다(`scouter: scouter.common/src/main/java/scouter/lang/step/StepSingle.java`, `CommonSpanStep.java`). cached Scouter 2.20.0 StepSingle bytecode도 같은 구조였다. Pulse 수집 서비스 Step decoder 실패 또는 누락 가능성을 재현해야 하며 정상 호환 fixture로 승인하지 않는다.
- HTTP status 제외와 profile queue 사이 경쟁, queue 미배출 종료, 공유 producer 반복 close는 현재 동작에서 확인된다. 이를 수정하면 intentional difference와 검증 증거를 남긴다.

## Synthetic golden와 단계 3 검증 gate

실서비스 SQL/계정/profile/endpoint를 fixture에 복사하지 않는다. synthetic dictionary와 pack/Step만 사용한다. golden을 생성한 Java oracle은 digest로 고정한 plugin 원본 및 정확한 Scouter 2.20.0 dependency를 사용했고 timezone, default charset, JDK, effective 필터 설정을 metadata에 기록했다. plugin 원본 테스트의 TextCache seeding/private processor seam과 MockProducer.history()가 golden 생성 출발점이다. 원본의 profile 테스트는 profile=null이라 Step encoding coverage가 없어 oracle이 Step case를 따로 만든다.

golden에는 plugin source digest, case별 plugin 로그 줄 수(`log_message_count`; 로그 본문은 싣지 않는다), synthetic input pack/Step/dictionary/config, topic, partition, signed key와 8-byte key hex, header 순서와 bytes, JSON bytes, 각 decoded singleton Step bytes를 저장한다. JSON key 순서 차이는 byte 비교와 semantic 비교에서 따로 보고하며 이를 묵시적으로 normalize해서 mismatch를 숨기지 않는다. fixture는 plugin 정본을 실행해 만든 `internal/pulse/testdata/pulse-golden.json` 124 case(sent 98/filtered 20/exception 6)이며 Go 구현이 124/124 일치한다. golden으로 확정된 사실: Trace JSON key 순서는 `traceId … errorDictContent, xtype`(`xtype`이 마지막), Jackson 2.15.2는 보조평면 문자를 surrogate pair로 escape하고 제어문자를 대문자 hex로 escape하며 `/ < >`는 그대로 둔다. application별 rebase는 내용이 같아도 `transactionDictId`를 재계산하고 global suffix는 매치될 때만 재계산한다. transaction 확장자 검사는 null을 제외 대상으로 보므로 SERVICE miss는 extension 단계에서 제외된다. SPAN/SPANCALL step bytes는 `StepSingle` 필드만 있고 start_cpu는 0이다. profile text는 현재 날짜 preload 결과를 우선 캐시한다(cross-day miss → `**unlabeled**`).

| 검증 묶음 | 최소 시나리오 / 판정 |
|---|---|
| Record bytes | 음수/0/int64 경계 txid, Java hash 음수/Unicode/보조평면 application, partition count 변경; key/header/partition 일치 |
| JSON | primitive 0/false, null/empty 문자열·IP·profile, >2^53 long, UTF-8·escape·getter 이름; Java bytes 및 Pulse 수집 서비스 decode 검증 |
| Step | MESSAGE/METHOD/SQL/APICALL/DUMP/SOCKET/THREAD 계열, SPAN/SPANCALL; binary singleton 및 main/additional text 검증 |
| Dictionary | hit/miss/hash=0, 현재 날짜와 event 날짜가 다른 lookup, timezone 자정; 이름/ID 일치 |
| Filter | extension/case/Spring URI/rebase/global suffix/status class/wildcard, profile-before-trace, TTL expiry; 전송/제외 결과 일치 |
| Queue/lifecycle | 포화, worker exception, producer buffer 포화/metadata timeout, enqueue-after-close, shutdown 잔여 queue; bounded latency와 loss 계수 |
| Pulse 수집 서비스 정상 | trace/profile 및 profile-before-trace, 동일 application 등록 경쟁, 저장/조회 및 UUID 유지 |
| Pulse 수집 서비스 실패 | 누락/잘못된 apm/version/application header, malformed JSON/Base64/Step, zero dictionary ID, inactive application, DB unavailable, topic/partition mismatch |
| 내구성 옵션 | acks/retries/idempotence 별 broker 재시작/응답 지연/회복; drop/duplicate/offset/ordering/latency 비교 |
| 공유 호스트 부하 | payload mix/profile 크기/dictionary hit/조회 병행/retention 및 할당 CPU·메모리·SSD I/O 기록; 완료 XLog TPS와 packet/profile/byte rates 구분 |

plugin 원본 자체의 테스트 실행은 oracle 생성(원본 컴파일 + oracle 실행)으로 갈음했고 원본의 Gradle 테스트와 Pulse 수집 서비스의 header/identity 테스트는 **미실행**이다. 실행 시 임시 출력 경로 사용과 원본 read-only 방식을 먼저 마련한다. 실제 Kafka/Pulse 수집 서비스 integration은 격리 환경에서 수행한다. 실제 설정의 topic/partition/header mapping과 offset 처리까지 확인한다. source/test 또는 MockProducer 성공만으로 브로커 접속·저장 내구성·DB 정합성·운영 목표 처리량을 입증하지 않는다.

Go 표준 검증 `make check`, `make race`, `make fuzz`, `make bench`, `make fixtures-check`, `make inventory-check`와 격리 broker(`make kafka-local-selftest`, `scripts/kafka-local.py`)를 함께 수행한다. Record bytes·JSON·Step·Dictionary·Filter·Queue/lifecycle은 golden·단위·race·fake broker·실제 Kafka 4.0.0 broker 왕복으로 검증했다. Pulse 수집 서비스 정상/실패, 내구성 옵션 비교, 공유 호스트 부하는 미검증이다.

**Go producer의 의도된 차이**(`internal/pulse/kafka/doc.go`, [pulse CONTRACT](../../internal/pulse/CONTRACT.md)): compression은 Java plugin과 같은 `lz4`가 기본이고 `none`을 고를 수 있다(LZ4 frame 형식은 같고 압축 bytes는 lz4-java와 다를 수 있다. 줄지 않는 batch는 codec none으로 보낸다). ApiVersions v3(미지원 broker는 v0 재시도), Metadata v1–13, Produce v3–12 중 최고 공통 버전을 쓰며 flexible version의 tagged field는 모두 건너뛴다. 연결당 in-flight 1, buffer 기본 64MiB(Java 512MiB 아님, 초과 시 대기 없이 drop하고 `DroppedBufferFull`로 구분), bounded Close drain, acks 기본 0에 `--pulse-kafka-acks`로 1/-1 선택 가능(내구성 옵션 비교는 별도 결정).

> 갱신: 위 문단은 lz4 압축과 flexible version 구현 뒤의 상태다. 이전 판은 "compression none, flexible version 미지원"이었다. 검증은 fake broker 버전 행렬, kafka-clients 4.0.0 `TestJavaLZ4Oracle`, 격리 Kafka 4.0.0 KRaft broker(협상 ApiVersions v3/Metadata v13/Produce v12, segment의 batch codec lz4 확인)이며 상세는 pulse CONTRACT "Kafka producer 압축과 protocol 버전"을 따른다. Kafka 4.1+·2.4 미만 실제 broker, LZ4 CPU 비용과 Pulse on/off 처리량은 미검증이다.

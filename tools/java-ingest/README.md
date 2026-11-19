# ingest 부수 경로 Java oracle

고정 원본 Scouter 2.21.3(commit `dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5`)의 `PodSeqManager`, `ObjectCpuChecker`, `GeoIpUtil`, `AgentManager`(+`ObjectMap`)를 **수정 없이** 임시 디렉터리에 컴파일·실행해 `internal/ingest/testdata/java-oracle/*.json` golden을 만든다. 원본 source tree는 읽기만 한다. 입력은 모두 synthetic이다.

```sh
export SCOUTER_SOURCE=/path/to/pinned/scouter        # 기본값 ../scouter
python3 tools/java-ingest/run.py --generate /tmp/새-디렉터리             # 전체 scenario
python3 tools/java-ingest/run.py --generate /tmp/x --scenario kube     # 일부만
python3 tools/java-ingest/run.py --check                               # golden 재생성 후 byte 비교(manifest 제외)
```

Go 쪽 대조는 `internal/ingest/java_oracle_test.go`(`go test ./internal/ingest -run JavaOracle`)가 golden의 각 step 입력을 engine에 재생하고 출력과 비교한다.

## 도구

| 항목 | 사용 |
|---|---|
| common Java | PATH의 `javac --release 8`(index oracle `compile_classes` 공용) |
| server Java·Scala·seam·oracle | JDK 8(`/usr/libexec/java_home -v 1.8`, `SCOUTER_JAVA8_HOME`) 위에서 `scouter.deploy/lib/scala` 2.10 scalac mixed compile 후 JDK 8 `javac -source 8` |
| jar | `scouter.deploy/lib/maxmind.geoip.jar`(`com.maxmind.geoip.Location`) |
| 실행 | scenario마다 별도 JDK 8 JVM(`-Duser.timezone=Asia/Seoul -Dfile.encoding=UTF-8`). 원본 상태가 static이므로 scenario 사이에 섞이지 않는다 |

## 실행하는 원본

- `kube/PodSeqManager.java` 전체. oracle은 `AgentManager.active`의 kube 분기(82-99행: `allowProceed=true`, `useKubeSeq && podName` 확인, `applyPodSeq`, `pushSeqToAgent`)와 `inactive`의 `objectInactivated(hostName)` 호출(214-216행)만 같은 순서로 호출한다. `AgentManager`의 나머지(ObjectMap, alert, ObjectWR)는 PodSeqManager 계약 밖이다.
- `core/app/ObjectCpuChecker.scala` 전체와 dump daemon(`ThreadScala.startDaemon`, 원본 `util/ThreadScala.scala`).
- `core/AgentManager.scala`·`core/ObjectMap.scala` 전체: `active`, `inactive`, 1초 daemon(dead time·zipkin·`deadTime` tag), `INACTIVE_OBJECT`/`ACTIVATED_OBJECT`/`MAYBE_OBJ_DUP` 생성. `cpu` scenario의 agent 등록도 `AgentManager.active`를 쓴다.
- `geoip/GeoIpUtil.scala` 전체(`IntKeyLinkedMap`/`IntLinkedSet` setMax(10000), `HashUtil`, `DataInputX`는 common 원본).

## Seam(`seams/`)

| seam | 원본 | 대체 이유와 범위 |
|---|---|---|
| `Logger` | 파일 rotation logger | stderr 출력(java-management seam과 같음) |
| `core.CoreRun` | daemon flag + ShutdownManager 등록 | `running=true` static field |
| `CounterManager` | counters.site.xml 읽기/쓰기 포함 | 원본 `CounterEngine`이 원본 `counters.xml` resource를 parse. `getInstance().getCounterEngine()`만 제공 |
| `Configure` | 파일 reload thread | AgentManager가 읽는 `object_deadtime_ms`/`object_zipkin_deadtime_ms`/`object_inactive_alert_level` field. oracle이 reflection으로 바꾼다(`conf` step). index stub의 `log_index_traversal_warning_count` 유지 |
| `core.AlertCore`, `core.cache.AlertCache` | alert queue / realtime ring | `add`/`put` 호출 기록(`via` core/cache) |
| `core.TextCore`, `core.cache.CommonCache`, `db.ObjectWR`/`ObjectRD`/`AlertWR`, `plugin.PlugInManager` | 사전·cache·object DB·plugin | 호출만 받거나 빈 결과. AgentManager의 저장 부수효과는 이 oracle 범위 밖 |
| `netio.AgentCall`, `netio.AgentCallLog` | Agent TCP 호출 | (objHash, 명령, 원본 codec으로 encode한 MapPack)을 기록하고 null 응답. 포화 scenario에서는 gate로 호출을 멈춰 daemon이 1건을 잡은 상태를 만든다 |
| `geoip.GISDataUtil` | MaxMind LookupService 로드 | `isOk`와 IP별 table/fallback을 oracle이 설정. 호출마다 새 `Location`을 돌려준다(원본 LookupService와 같고, GeoIpUtil이 `loc.city`를 바꾸므로 필요) |
| `db.TextPermWR` | 영구 text queue writer | `add(div, hash, text)` 호출 기록 |

## 시간과 비동기 처리

- `alerts`: AgentManager daemon은 실시각으로 판정한다. step마다 논리 시각 T를 두고, 저장된 ObjectPack의 `wakeup`을 "실시각 − 논리 경과"로 다시 맞춘 뒤 daemon 1회 이상(1.6초 대기)을 기다린다. tick 경과는 dead-time 경계에서 2초 이상 떨어져 있어 daemon 일정이 결과를 바꾸지 않는다. message의 `Timestamp`는 다시 맞춘 실시각 값을 `1760000000000 + 논리 시각`의 문자열로 바꿔 기록한다(JVM `user.timezone=Asia/Seoul`). `MAYBE_OBJ_DUP` message의 시각은 원본 `active`가 저장 pack과 따로 읽은 `currentTimeMillis`라 ms 경계에서 1ms 다를 수 있다. 그래서 `active` 호출 구간의 모든 ms 값을 그 step의 논리 시각으로 바꾼다.

- `PodSeqManager`의 15초 규칙은 `System.currentTimeMillis()`를 쓴다. oracle은 step마다 논리 시각을 두고, 실행 직전 `initialHostObjMap`의 각 `initialTime`을 "현재 실시각 − 논리 경과 시간"으로 다시 맞춘다. 경과 시간은 14000, 정확히 15000, 그 이상만 쓴다(재조정과 비교 사이의 ms drift는 `initialTime > now - 15000`을 더 거짓으로만 만들므로 결과가 바뀌지 않는다). golden의 `t`는 논리 시각이다.
- `ObjectCpuChecker`의 dump는 daemon이 queue에서 꺼내 호출한다. oracle은 queue가 비고 시작된 호출이 모두 기록될 때까지 기다린 뒤 step 결과를 읽는다.

## golden 형식

`{"scenario": ..., "steps": [{"op": ..., 입력..., "out": {...}}]}`. 값 tag는 `{"t": text|bool|decimal|float|double, "v": ...}`(NaN은 `"NaN"`), MapPack 인자는 원본 codec bytes의 hex다.

- `kube`: `active`(tags) → `allowProceed`, `pushSeq`, `podSeq`, `podName`, `hostName`, Agent 호출, 비어 있지 않은 manager의 `hostSeq`/`seqHost`, `initial`(host, podName, podSeq, 논리 t). `inactive`(podName, hostName).
- `cpu`: `agent`(objName, objType, tags), `counter`(time, timetype, data) → `first`(ObjectCpuStatus.firstOccurenceTime, 상태 없음은 null), Agent 호출. `saturate` → daemon이 1건을 잡은 동안 queue에 남은 수와 전체 호출 수.
- `alerts`: `timestamps`(java.sql.Timestamp 문자열), `intkeymap`(IntKeyMap 300 put/remove 후 열거 순서), `active`(objName·objType·address·version·tags), `tick`(논리 T), `conf`(Configure field) → 그 step에 생긴 alert(via, title, level, objHash, objType, message)와 objMap의 alive 표.
- `geoip`: `db`(isOk), `table`(entries, fallback), `xlog`(ip, countryCode) → countryCode, city, lookup 횟수, TextPermWR 기록. `sweep`(10/127/172/192/8의 두 번째 octet 0-255 중 lookup된 값), `bulk`(FIFO cache 경계).

Go 비교 시 정규화: Java의 빈 manager(getInstance가 만든 것)는 출력하지 않는다(Go는 빈 manager를 지우며 동작은 같다). Java `first`가 null·-1·0이면 Go에는 streak가 없어야 하고, 양수면 같은 값이어야 한다.

#!/bin/sh
# Regenerates ../b_java_oracle.json from the pinned Scouter sources.
#   SCOUTER_SRC: checkout of scouter at baseline.json commit dee47fb5f4003cecbb146c5acfff8d2ebeaf07f5
#   M2:          Maven repository with jackson-databind 2.12.6.1 / core+annotations 2.12.6
#                (versions pinned by scouter.webapp/pom.xml) and lombok 1.18.20
#                (the pom pins 1.16.18; only getter generation is used).
# The webapp model classes (SXLog, XLogData, SCommonSpanStep, SSpanStep,
# SSpanCallStep) are compiled from SCOUTER_SRC unchanged; src/ only holds
# stubs for their dictionary/util dependencies (shadowing via -sourcepath).
set -eu
HERE=$(cd "$(dirname "$0")" && pwd)
: "${SCOUTER_SRC:?set SCOUTER_SRC}"
M2=${M2:-$HOME/.m2/repository}
J=$M2/com/fasterxml/jackson/core
CP=$J/jackson-databind/2.12.6.1/jackson-databind-2.12.6.1.jar:$J/jackson-core/2.12.6/jackson-core-2.12.6.jar:$J/jackson-annotations/2.12.6/jackson-annotations-2.12.6.jar
LOMBOK=$M2/org/projectlombok/lombok/1.18.20/lombok-1.18.20.jar
WEB=$SCOUTER_SRC/scouter.webapp/src/main/java/scouterx/webapp/model
OUT=$(mktemp -d)
trap 'rm -rf "$OUT"' EXIT
javac -nowarn -d "$OUT" -cp "$CP:$LOMBOK" -processorpath "$LOMBOK" \
  -sourcepath "$HERE/src:$SCOUTER_SRC/scouter.common/src/main/java" \
  "$HERE/src/Oracle.java" "$WEB/XLogData.java" "$WEB/scouter/SXLog.java" "$WEB"/scouter/step/*.java
java -Duser.timezone=Asia/Seoul -cp "$OUT:$CP" Oracle > "$HERE/../b_java_oracle.json"

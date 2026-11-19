GO ?= $(if $(wildcard .tools/go/bin/go),$(CURDIR)/.tools/go/bin/go,go)
GOFMT ?= $(if $(wildcard .tools/go/bin/gofmt),$(CURDIR)/.tools/go/bin/gofmt,gofmt)
PYTHON ?= python3
FUZZTIME ?= 10s
BENCHTIME ?= 1s
# internal/storage alone runs past go test's default 10m under -race on a
# loaded laptop (measured 11m12s; PASS with no race report).
RACE_TIMEOUT ?= 30m
GOMAXPROCS ?= 2
export GOMAXPROCS
export GOTOOLCHAIN := local

.PHONY: help bootstrap fmt fmt-check vet test build build-linux bench-bundle check race fuzz bench fixtures fixtures-check inventory inventory-check public-check storage-check java-integration replay kafka-local-selftest management-oracle-check geoip-oracle-check ingest-oracle-check sqltable-oracle-check logger-oracle-check

help:
	@echo 'bootstrap / check / race / fuzz / bench / fixtures-check / inventory-check'
	@echo 'build는 dist/scouter-go, dist/scouter-db, dist/scouter-replay를 생성합니다.'
	@echo 'replay REPLAY_ARGS="--rate 200 --objects 20 ..."는 loopback 측정 report를 dist/replay-report.json에 씁니다.'

bootstrap:
	$(PYTHON) scripts/bootstrap_go.py

fmt:
	"$(GOFMT)" -w internal cmd

fmt-check:
	@set -eu; files=$$("$(GOFMT)" -l internal cmd); if [ -n "$$files" ]; then echo "$$files"; exit 1; fi

vet:
	"$(GO)" vet ./...

test:
	"$(GO)" test -count=1 ./...

build:
	"$(GO)" build ./...
	@mkdir -p dist
	"$(GO)" build -o dist/scouter-go ./cmd/scouter-go
	"$(GO)" build -o dist/scouter-db ./cmd/scouter-db
	"$(GO)" build -o dist/scouter-replay ./cmd/scouter-replay

# Cross-compile for a Linux host (e.g. a server without a Go toolchain) from any
# platform: the binaries are static (no cgo) and need no runtime on the target.
# Override GOARCH=arm64 for ARM hosts. Checksums are written next to the files.
LINUX_ARCH ?= amd64
build-linux:
	@mkdir -p dist/linux-$(LINUX_ARCH)
	GOOS=linux GOARCH=$(LINUX_ARCH) CGO_ENABLED=0 "$(GO)" build -trimpath -o dist/linux-$(LINUX_ARCH)/scouter-go ./cmd/scouter-go
	GOOS=linux GOARCH=$(LINUX_ARCH) CGO_ENABLED=0 "$(GO)" build -trimpath -o dist/linux-$(LINUX_ARCH)/scouter-db ./cmd/scouter-db
	GOOS=linux GOARCH=$(LINUX_ARCH) CGO_ENABLED=0 "$(GO)" build -trimpath -o dist/linux-$(LINUX_ARCH)/scouter-replay ./cmd/scouter-replay
	GOOS=linux GOARCH=$(LINUX_ARCH) CGO_ENABLED=0 "$(GO)" vet ./...
	cd dist/linux-$(LINUX_ARCH) && shasum -a 256 scouter-go scouter-db scouter-replay > SHA256SUMS

# bench-bundle packs the Linux binaries and the isolated-host benchmark
# (docs/validation/isolated-benchmark.md) into one archive.
bench-bundle: build-linux
	@rm -rf dist/bench-bundle-linux-$(LINUX_ARCH) && mkdir -p dist/bench-bundle-linux-$(LINUX_ARCH)
	cp dist/linux-$(LINUX_ARCH)/scouter-go dist/linux-$(LINUX_ARCH)/scouter-replay scripts/bench-xlog-max.py scripts/bench-isolated.sh dist/bench-bundle-linux-$(LINUX_ARCH)/
	tar -C dist -czf dist/bench-bundle-linux-$(LINUX_ARCH).tar.gz bench-bundle-linux-$(LINUX_ARCH)

check: fmt-check vet test build

# Refuse content that must not reach a public repository (see CONTRIBUTING.md).
public-check:
	sh scripts/public-check.sh

race:
	"$(GO)" test -race -count=1 -timeout=$(RACE_TIMEOUT) ./...

fuzz:
	"$(GO)" test ./internal/protocol -run='^$$' -fuzz='^FuzzDecodeBatchSimple$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/storage/legacyindex -run='^$$' -fuzz='^FuzzTTLIndexFiles$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/protocol -run='^$$' -fuzz='^FuzzDecodePack$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/protocol -run='^$$' -fuzz='^FuzzDecodeValue$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/protocol -run='^$$' -fuzz='^FuzzReadPack$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/protocol -run='^$$' -fuzz='^FuzzExtraPack$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/protocol -run='^$$' -fuzz='^FuzzDecodeSteps$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/transport -run='^$$' -fuzz='^FuzzUDPDatagram$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/transport -run='^$$' -fuzz='^FuzzTCPHeader$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/storage/legacyindex -run='^$$' -fuzz='^FuzzIndexFiles$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/storage -run='^$$' -fuzz='^FuzzCompressedRecords$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/storage -run='^$$' -fuzz='^FuzzCRC32String$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/account -run='^$$' -fuzz='^FuzzAccountXML$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/account -run='^$$' -fuzz='^FuzzBER$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/config -run='^$$' -fuzz='^FuzzParseProperties$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/telegraf -run='^$$' -fuzz='^FuzzLineProtocol$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/geoip -run='^$$' -fuzz='^FuzzFromBytes$$' -fuzztime=$(FUZZTIME) -fuzzminimizetime=5s -parallel=2
	"$(GO)" test ./internal/pulse/kafka -run='^$$' -fuzz='^FuzzLZ4RoundTrip$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/pulse/kafka -run='^$$' -fuzz='^FuzzSkipTags$$' -fuzztime=$(FUZZTIME) -parallel=2
	"$(GO)" test ./internal/sqltable -run='^$$' -fuzz='^FuzzCrudInfo$$' -fuzztime=$(FUZZTIME) -parallel=2

bench:
	"$(GO)" test ./internal/protocol -run='^$$' -bench=. -benchmem -benchtime=$(BENCHTIME)

fixtures:
	sh scripts/generate-fixtures.sh

fixtures-check:
	sh scripts/generate-fixtures.sh --check

inventory:
	$(PYTHON) scripts/source_inventory.py

inventory-check:
	$(PYTHON) scripts/source_inventory.py --check

storage-check:
	"$(GO)" test -v -count=1 ./internal/storage/... -run='TestOriginalJavaStorageInterop|TestJavaStorageInterop'

java-integration: build
	$(PYTHON) scripts/java-integration.py --server dist/scouter-go

REPLAY_ARGS ?= --duration 10s --warmup 2s --rate 200 --objects 20 --query-concurrency 2

replay: build
	dist/scouter-replay --server dist/scouter-go --report dist/replay-report.json $(REPLAY_ARGS)

management-oracle-check:
	python3 scripts/java-management.py --check

geoip-oracle-check:
	python3 -I scripts/java-geoip.py --check

ingest-oracle-check:
	python3 tools/java-ingest/run.py --check

sqltable-oracle-check:
	python3 -I scripts/java-sqltable.py --check

logger-oracle-check:
	python3 tools/java-logger/run_oracle.py --check


# Isolated single-node KRaft broker from an official Kafka distribution (test only).
KAFKA_LOCAL_HOME ?=
KAFKA_LOCAL_DIR ?= /tmp/scouter-kafka-local

kafka-local-selftest:
	KAFKA_LOCAL_HOME="$(KAFKA_LOCAL_HOME)" $(PYTHON) -I scripts/kafka-local.py selftest --dir "$(KAFKA_LOCAL_DIR)" --partitions 30

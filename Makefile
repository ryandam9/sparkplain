BINARY  := bin/sparkplain
GO      := go
PKG     := ./...

VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

# run feeds the committed fixture to the CLI; override with ARGS="...".
ARGS    ?= -app-id application_1790380000000_0042 \
           -eventlog testdata/eventlog/application_1790380000000_0042 -out out

.PHONY: all check fmt fmt-check vet test build install clean run tidy lint vuln bench-log fixtures help

all: check install

# check runs the four commands CLAUDE.md requires before a task is done.
check: fmt-check vet test build vuln

fmt:
	gofmt -w .

fmt-check:
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then \
		echo "gofmt needed on:"; echo "$$out"; exit 1; fi

vet:
	$(GO) vet $(PKG)

test:
	$(GO) test -race -count=1 $(PKG)

build:
	$(GO) build -trimpath -ldflags "$(LDFLAGS)" -o $(BINARY) ./cmd/sparkplain

# install copies the binary to a bin directory (override with PREFIX=...):
#   1) ~/.local/bin, if it is on $PATH
#   2) /usr/local/bin, if it exists
install: build
	@dest="$(PREFIX)"; \
	if [ -z "$$dest" ]; then \
		case ":$$PATH:" in *":$$HOME/.local/bin:"*) dest="$$HOME/.local/bin" ;; esac; \
	fi; \
	if [ -z "$$dest" ] && [ -d /usr/local/bin ]; then dest="/usr/local/bin"; fi; \
	if [ -z "$$dest" ]; then \
		echo "install: no suitable bin dir found; rerun with PREFIX=/path/to/bin"; exit 1; \
	fi; \
	mkdir -p "$$dest" && install -m 0755 $(BINARY) "$$dest/$(notdir $(BINARY))" && \
		echo "installed -> $$dest/$(notdir $(BINARY))"

clean:
	rm -rf bin out
	rm -f sparkplain

# run exits 3 when a source is missing, which is expected for the event-log-only fixture.
run: build
	./$(BINARY) $(ARGS) || [ $$? -eq 3 ]

tidy:
	$(GO) mod tidy

lint:
ifeq (, $(shell which staticcheck))
	@echo "staticcheck not installed, skipping"
else
	staticcheck $(PKG)
endif

# vuln runs a pinned govulncheck (fetched into the module cache once), so a
# commit gives the same result whatever is installed; CI uses this target.
# GOVULNDB points it at another copy of the Go vulnerability database.
# `go run pkg@version` would otherwise pick its toolchain from govulncheck's
# own go.mod; it must be built with this module's Go to load its packages.
GOVULNCHECK_VERSION ?= v1.8.0
vuln:
	GOTOOLCHAIN=$$($(GO) env GOVERSION) $(GO) run golang.org/x/vuln/cmd/govulncheck@$(GOVULNCHECK_VERSION) $(if $(GOVULNDB),-db $(GOVULNDB)) $(PKG)

# bench-log writes a large synthetic event log for the SPEC §8 performance budget.
bench-log:
	$(GO) run ./scripts/benchlog -out out/big.log -tasks 245000

# fixtures regenerates testdata/eventlog; needs Java 17+, pyspark==3.5.1 and SP_SCRATCH.
fixtures:
	scripts/fixtures/generate.sh

help:
	@echo "Targets:"
	@echo "  check      - fmt-check + vet + test + build + vuln (run before calling a task done)"
	@echo "  fmt        - Format source code in place"
	@echo "  fmt-check  - Fail if any file needs gofmt"
	@echo "  vet        - Run go vet"
	@echo "  test       - Run tests with the race detector"
	@echo "  build      - Build $(BINARY) with the version stamped in"
	@echo "  install    - Build and install the binary to a bin dir (PREFIX= to override)"
	@echo "  clean      - Remove bin/, out/ and a stray ./sparkplain"
	@echo "  run        - Build and run on the committed fixture (ARGS= to override)"
	@echo "  tidy       - Tidy go modules"
	@echo "  lint       - Run staticcheck (if installed)"
	@echo "  vuln       - Run govulncheck"
	@echo "  bench-log  - Write a synthetic 1 GB event log to out/big.log"
	@echo "  fixtures   - Regenerate testdata/eventlog (needs PySpark; see the script)"
	@echo "  all        - check + install"

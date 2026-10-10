# sparkplain

A Go CLI that turns one Spark application's logs into a single plain-language HTML report (plus JSON), showing what ran, on which nodes, with how much CPU, memory and storage, and under which identity. The first target is Amazon EMR on EC2 7.3.0+ (Spark 3.5.1+).

- **Source of truth:** `docs/SPEC.md`, the current design. Read it before starting any task. If a decision changes, update the spec in the same change.
- **History:** `docs/HISTORY.md` holds each phase's plan, step notes, live-check results and dated decisions. New plans and status notes go there, not in the spec.
- **Report design reference:** `docs/sample-report.html` (fictional data). Match its structure, tone and the way it explains every metric.

## How we work

- Build in the phases listed in SPEC §8 (plans in `docs/HISTORY.md`). Finish and test one phase before starting the next, and don't start a new phase unless asked.
- For anything bigger than a small fix, propose a plan first and wait for approval.
- Keep commits small, one concern each, and write messages that say why.
- If the spec is ambiguous, ask. Don't guess about Spark or EMR behaviour; check it against a real fixture.

## Stack and layout

- Go, latest stable release. Module path: set in `go.mod`.
- Package layout follows SPEC §4: `cmd/sparkplain`, `internal/sparkplain/{source,eventlog,yarnlog,awsmeta,model,analyze,report}`.
- Prefer the standard library. Approved dependencies:
  - `github.com/aws/aws-sdk-go-v2/...`
  - `github.com/klauspost/compress` (zstd, snappy)
  - `github.com/pierrec/lz4/v4`
  - `gopkg.in/yaml.v3`
  - `github.com/spf13/cobra` and `github.com/charmbracelet/fang` (the CLI and its help)
  - D3 v7 (JavaScript), embedded in the explorer page only, pinned by hash (`internal/sparkplain/report/assets/vendor`)

  Ask before adding anything else.
- The HTML report (`<app-id>-report.html`, with `<app-id>-report.json` beside it) is one self-contained file. Templates and any chart JS are embedded with `go:embed`, and the report makes no network calls when opened.
- The explorer page (`<app-id>-explorer.html`, one per application, like a richer History Server) is also a single self-contained file with its data embedded. Its charts are drawn by the embedded D3 (a small chart kit in `explorer.js`), so it makes no network calls when opened and works offline. Never send report data anywhere, and keep the page readable (tables and text) if a chart can't be drawn.

## Commands

```sh
gofmt -l . && go vet ./...
make test    # go test ./..., then go test -race on the packages that start goroutines (RACE_PKG)
go build ./cmd/sparkplain
govulncheck ./...
```

Run all four before calling a task done. Code that starts a goroutine belongs in a package listed in the Makefile's `RACE_PKG`; a test enforces it.

## Hard rules

- **Production safety: AWS access is read-only.** Use only List, Get, Describe, Head and Lookup calls. Never write to S3, never modify clusters or steps, and never call any mutating API, even in tests or scripts.
- **No secrets in output.** Redact values whose keys match password, secret, token, key or credential, in config and log lines, before anything is rendered or logged. Tests plant fake secrets and assert they never appear.
- **Stream everything.** Never read a whole log into memory. Fold task events into per-stage aggregates. Target under 1 GB RAM for a 1 GB event log.
- **Provenance.** Every value in the model records its source file and line, and every finding links to evidence.
- **Degrade, don't fail.** A missing or unreadable source removes or marks its sections, gets listed in the Sources panel, and makes the run exit 3. The event log is optional (SPEC §3).
- **Tests never call real AWS.** Use interfaces plus stubs.
- **Fixtures in `testdata/` must be synthetic or scrubbed.** Never commit real production logs, account IDs, hostnames or bucket names.

## Spark event log facts (verify against fixtures)

- The format is one JSON object per line, dispatched on the `"Event"` field (for example `SparkListenerApplicationStart`, `SparkListenerTaskEnd`). Skip unknown events and fields, and count them.
- Single-file logs are named `<appId>[_<attemptId>][.lz4|.zstd|.snappy][.inprogress]`.
- Rolling logs are a folder `eventlog_v2_<appId>[_<attemptId>]/` holding `events_<n>_<appId>[.codec]` parts plus an `appstatus_*` marker. Read the parts in order of `n`.
- Spark compresses with Java libraries whose stream formats are not the standard frame formats:
  - `lz4` uses lz4-java's `LZ4BlockOutputStream` (magic `LZ4Block`).
  - `snappy` uses xerial snappy-java's `SnappyOutputStream` (its own header and chunking).
  - `zstd` uses standard zstd frames.

  Detect the codec from the extension and confirm it with the magic bytes. Expect to write small custom readers for lz4 and snappy.
- History Server "Download" gives a zip containing the event log (or its rolling folder).

## Generating test fixtures locally

Use PySpark 3.5 on a laptop:

```sh
mkdir -p /tmp/spark-events
pyspark --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///tmp/spark-events
# compressed variants
pyspark --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///tmp/spark-events \
        --conf spark.eventLog.compress=true --conf spark.eventLog.compression.codec=zstd   # also lz4, snappy
# rolling variant
pyspark --conf spark.eventLog.enabled=true --conf spark.eventLog.dir=file:///tmp/spark-events \
        --conf spark.eventLog.rolling.enabled=true --conf spark.eventLog.rolling.maxFileSize=10m
```

Run small jobs that exercise shuffle, caching, a skewed join and a deliberate failure. Keep fixtures small (under 5 MB each), and scrub hostnames and paths before committing.

## Report writing style

- Write the report and explorer text in ASD-STE100 Simplified Technical English, as `docs/STYLE.md` applies it. `TestPlainLanguage` checks sentence length, contractions and words that STE does not approve.
- Open with a "What happened" summary of 2–4 plain sentences.
- Give every metric a one-line explanation, and show "used of available" side by side.
- Each section shows its coverage status (complete, partial, needs event log) and what's missing.
- Findings carry a severity, a plain explanation, evidence (file:line or API call), and a suggested fix.
- Write short sentences with no jargon left unexplained. Times are shown in the viewer's configured time zone, labelled.

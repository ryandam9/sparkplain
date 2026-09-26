# sparkplain

Turns one Spark application's logs into a single plain-language HTML report (plus JSON): what ran, on which nodes, with how much CPU, memory and storage, and what went wrong, with every finding pointing at the log line behind it. The first target is Amazon EMR on EC2 7.3.0+ (Spark 3.5.1+).

The design is in [docs/SPEC.md](docs/SPEC.md). This version is **phase 1**: it reads a Spark event log you already have and needs no AWS access. Fetching logs from S3 and AWS APIs come in phases 2 and 3.

## Build

```sh
go build ./cmd/sparkplain
```

## Use

```sh
# A single event log (plain, .lz4, .zstd, .snappy or .inprogress)
sparkplain -app-id application_1700000000000_0042 -eventlog ./application_1700000000000_0042.lz4

# A rolling event log folder, a folder holding many logs, or the History Server's Download zip
sparkplain -app-id application_1700000000000_0042 -eventlog ./eventlog_v2_application_1700000000000_0042/
sparkplain -app-id application_1700000000000_0042 -eventlog /var/log/spark/apps/
sparkplain -app-id application_1700000000000_0042 -eventlog ./application_1700000000000_0042.zip
```

It writes `report.html` and `report.json` to `~/sparkplain/<yyyy-mm-dd>/<app-id>/` (or `-out`). The HTML is one self-contained file that makes no network calls when opened. Run `sparkplain -h` for all flags; SPEC §6 describes them and the config file.

Exit codes: 0 complete, 2 fatal, 3 partial (something missing or unreadable, which the report's Sources panel explains), 130 interrupted.

## Develop

```sh
gofmt -l . && go vet ./...
go test -race ./...
go build ./cmd/sparkplain
govulncheck ./...
```

- Fixtures in `testdata/eventlog` come from real PySpark 3.5.1 runs, scrubbed. Regenerate them with `scripts/fixtures/generate.sh` (needs Java 17+ and `pyspark==3.5.1`; see the script header).
- `go run ./scripts/benchlog -out /tmp/big.log -tasks 245000` writes a synthetic 1 GB log for the performance budget in SPEC §8.

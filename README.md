# sparkplain

Turns one Spark application's logs into a single plain-language HTML report (plus JSON): what ran, on which nodes, with how much CPU, memory and storage, and what went wrong, with every finding pointing at the log line behind it. The first target is Amazon EMR on EC2 7.3.0+ (Spark 3.5.1+).

The design is in [docs/SPEC.md](docs/SPEC.md). It reads a Spark event log you already have, or finds it on S3 from the EMR cluster, along with the cluster's container, step and node logs, the EMR and EC2 APIs, CloudWatch and CloudTrail (SPEC §8 records what each phase built).

## Build

```sh
make build     # writes bin/sparkplain, stamped with the git version
make install   # also copies it to ~/.local/bin or /usr/local/bin (PREFIX=... to override)
```

Without make, `go build ./cmd/sparkplain` works too.

## Use

```sh
# A single event log (plain, .lz4, .zstd, .snappy or .inprogress)
sparkplain -app-id application_1700000000000_0042 -eventlog ./application_1700000000000_0042.lz4

# A rolling event log folder, a folder holding many logs, or the History Server's Download zip
sparkplain -app-id application_1700000000000_0042 -eventlog ./eventlog_v2_application_1700000000000_0042/
sparkplain -app-id application_1700000000000_0042 -eventlog /var/log/spark/apps/
sparkplain -app-id application_1700000000000_0042 -eventlog ./application_1700000000000_0042.zip
```

Online, from the cluster (read-only AWS calls; `-profile default` uses the default credential chain):

```sh
# The event log is found from the cluster's spark.eventLog.dir when that is on S3
sparkplain -profile default -cluster-id j-1ABCDEF -app-id application_1700000000000_0042
# Otherwise say where it is
sparkplain -profile default -cluster-id j-1ABCDEF -app-id application_1700000000000_0042 -eventlog s3://my-logs/spark-events/
```

It writes three files to `~/sparkplain/<yyyy-mm-dd>/<app-id>/` (or `-out`), each named after the application so reports of different applications can share a folder, for example `application_1700000000000_0042-report.html`:

- `<app-id>-report.html`: the plain-language report. One self-contained file that makes no network calls when opened.
- `<app-id>-report.json`: the same content for other tools.
- `<app-id>-explorer.html`: an interactive, History Server–style view of the run (jobs, stages with task summaries and samples, executors, SQL plans, storage, environment). Like the report, it is one self-contained file that works offline: its charts are drawn with an embedded copy of D3.

`-format` picks which to write, e.g. `-format html,json` (or `both`) to skip the explorer.

To see your code beside the jobs and stages that ran it, point `-source` at the file or folder (repeatable). The files are matched to the ones the log names and redacted; PySpark records a code location for some actions only, so the explorer says where none was recorded.

```sh
sparkplain -app-id application_1700000000000_0042 -eventlog ./application_1700000000000_0042.lz4 -source ./jobs
```

Every value on the pages cites the event-log line it came from. To read that event, redacted:

```sh
sparkplain -app-id application_1700000000000_0042 -eventlog ./application_1700000000000_0042.lz4 -show application_1700000000000_0042.lz4:1234
```

Run `sparkplain -h` for all flags; SPEC §6 describes them and the config file.

The files are private to you (mode 0600, in folders sparkplain creates with mode 0700): they are redacted, but still hold user names, hosts, cluster IDs and log lines. `chmod` them to share.

Exit codes: 0 complete, 2 fatal, 3 partial (something missing or unreadable, which the report's Sources panel explains), 130 interrupted.

## Supported

- **Platforms:** Linux and macOS. Windows is not supported (output files are replaced with a rename that assumes POSIX semantics).
- **Building:** the Go release in `go.mod` (currently 1.27.1).
- **Spark and EMR:** Spark 3.5 event logs, tested on PySpark 3.5.1 fixtures and on EMR 7.3.0 cluster logs (`testdata/`); EMR on EC2 only.
- **Event logs:** plain, `.lz4`, `.zstd`, `.snappy` and `.inprogress` single files; rolling `eventlog_v2_*` folders (including compacted ones); History Server zips (local only). `.lzf` is not supported.
- **Running applications:** an `.inprogress` log gives a report up to its last event, marked incomplete, and the run exits 3.
- **AWS permissions:** read-only calls only; SPEC §6 lists them, split into required and optional (each optional one only removes its section when missing).
- **Limits:** `-max-size` (stored size per file, 10 GiB), `-max-unpacked` (unpacked size per compressed file, 50 GiB), `-workers` (1 to 256). A file cut by a limit is marked partial, never passed off as complete.

## Develop

```sh
make check     # gofmt check, go vet, go test -race, build and govulncheck: run before calling a task done
make run       # builds, then writes a report for the committed fixture to out/ (ARGS="..." to override)
make help      # lists every target
```

`make check` runs the same four commands as:

```sh
gofmt -l . && go vet ./...
go test -race ./...
go build ./cmd/sparkplain
govulncheck ./...
```

- Fixtures in `testdata/eventlog` come from real PySpark 3.5.1 runs, scrubbed. Regenerate them with `make fixtures` (runs `scripts/fixtures/generate.sh`; needs Java 17+, `pyspark==3.5.1` and `SP_SCRATCH`; see the script header).
- `make bench-log` (`go run ./scripts/benchlog -out out/big.log -tasks 245000`) writes a synthetic 1 GB log for the performance budget in SPEC §8.

## License

MIT, see [LICENSE](LICENSE). The explorer page embeds [D3](https://d3js.org) 7.9.0, which is under the ISC licence ([its notice](internal/sparkplain/report/assets/vendor/d3-LICENSE)).

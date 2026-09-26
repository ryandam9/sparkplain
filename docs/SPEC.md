# sparkplain — Design Specification

2026-09-26

## 1. Overview

**sparkplain** is a Go CLI that turns one Spark application's logs (on EMR on EC2 first) into a single report explaining what ran, on which nodes, with how much CPU, memory and storage, and under which identity.

**Problem.** The Spark History Server shows raw lists per tab (jobs, stages, executors, storage, environment). It gives no cross-cutting view: which node hosted which executors, how memory moved over time, what identity touched Hive, HBase or S3, or why the run failed. Answering those today means reading container logs by hand.

**Goals**

- One command, one application ID, one self-contained HTML report plus a JSON export.
- Cover nodes, executors, memory, storage and I/O, CPU, jobs and stages, configuration, identity and access, and failures.
- Support EMR on EC2 7.3.0 and later (Spark 3.5.1 and newer), for running or terminated clusters, from logs in S3 or on local disk.
- Fetch logs itself from S3 and AWS APIs as a standalone module, with no external tool dependencies.
- Flag problems (skew, spill, OOM, lost nodes, access denials) with evidence pointing to the exact log file and line.

**Non-goals for v1**

- Live monitoring or alerting on running applications.
- EMR Serverless and EMR on EKS (their log layouts differ; revisit after v1).
- Changing clusters or jobs automatically.
- Cost reporting in dollars.

## 2. Log fetching

sparkplain fetches logs itself, scoped tightly to one application, with bounded resources and explicit error reporting.

**Fetching rules**

| Pattern | sparkplain implementation |
| --- | --- |
| Explicit AWS profile | `-profile` required; region from the profile unless `-region`; bucket region auto-detected with `HeadBucket` |
| Cluster to log root | `ListClusters` for name lookup, `DescribeCluster` for `LogUri`; log root is `<LogUri>/<cluster-id>/`; works for terminated clusters |
| Server-side scoping by prefix | List only `containers/<app-id>/`, the app's `steps/<step-id>/`, and `node/<instance-id>/` for nodes that hosted executors; never the whole cluster |
| Streaming decompression | `.gz`, `.bz2`, plain and size-bounded `.zip` for YARN logs; adds `.zstd`, `.lz4` and `.snappy` for event logs |
| Bounded concurrency | Worker pool (default 16), per-object size cap, per-object and overall timeouts |
| LIST/GET consistency | `GetObject` with `If-Match` on the listed ETag; changed objects counted and skipped |
| Error classes | `accessDenied`, `notFound`, `throttled`, `timeout`, `archivedUnavailable`, `corrupt`, shown in the report's Sources panel |
| Archive awareness | Unrestored Glacier objects skipped at listing time and reported |
| Step-to-app attribution | Scan step `stderr` for the first `application_<ts>_<n>` ID |
| Output sanitisation | Strip control and deceptive Unicode characters before log lines reach the HTML |

Only the files the parsers need are downloaded, and each one streams straight into its parser without temporary files.

**Input modes**

1. Online: `-cluster-id` + `-app-id`, everything fetched from S3 and AWS APIs.
2. Offline: `-from <dir>`, any local folder mirroring the S3 layout below the application prefix (for example `container_*/stderr.gz`), plus an event log file. No AWS calls except optional enrichment.

## 3. Data sources

No single source answers every question, so sparkplain joins seven sources on cluster ID, application ID, host name and time window. The Spark event log carries most of the detail, but it is an optional input: on EMR on EC2 it defaults to HDFS on the cluster, so sparkplain never requires cluster changes to get it.

| Source | Location | Provides | Does not provide |
| --- | --- | --- | --- |
| Spark event log | `spark.eventLog.dir` (HDFS default; S3 if configured) | App start/end and user; executors added/removed with host, cores and reason; jobs, stages, tasks with CPU time, GC, spill, shuffle and I/O metrics; peak executor memory metrics; block manager and cached RDD storage; SQL plans; full effective configuration | Host OS CPU, AWS API calls, Kerberos or HBase auth outcomes |
| YARN container logs | `<LogUri>/<cluster>/containers/<app>/` | Driver and executor stdout/stderr: exceptions, OOM, container exit codes (137, 143), Hive metastore, HBase and Kerberos connection lines | Aggregate metrics |
| Step logs | `<LogUri>/<cluster>/steps/<step-id>/` | The `spark-submit` command, arguments and final step status | Anything inside Spark |
| Node logs | `<LogUri>/<cluster>/node/<instance-id>/` | NodeManager daemons, bootstrap actions, instance-state snapshots (processes, memory, disk) | Per-application attribution on shared nodes |
| EMR API | `DescribeCluster`, `ListInstances`, `ListInstanceGroups`/`Fleets`, `ListSteps` | Release label, applications, instance types, market (spot/on-demand), group, launch and end times, roles, security configuration, Kerberos attributes | Anything after cluster termination beyond retained metadata |
| CloudWatch | `AWS/ElasticMapReduce` namespace; CloudWatch agent metrics on EMR 7.0+ | YARN memory and container counts over time; host CPU, memory and disk if the agent is configured | Per-executor metrics |
| CloudTrail | `LookupEvents` for the instance profile or runtime role, in the app's time window | AWS API calls (Glue, STS, KMS, S3 management) and `AccessDenied` errors | S3 object reads/writes unless a trail records data events |

**Event log input (optional).** One flag, `-eventlog`, accepts an S3 location or a manually downloaded file; sparkplain detects which from the value.

| Input | Example | Handling |
| --- | --- | --- |
| S3 prefix | `s3://bucket/sparklogs/` | Lists `<prefix>/<app-id>*` and fetches the matching file or rolling folder |
| S3 object | `s3://bucket/sparklogs/application_…_0042.lz4` | Reads that one object |
| Local file | `./application_…_0042.zstd` | Plain, `.lz4`, `.zstd` or `.snappy` |
| Local folder | `./eventlog_v2_application_…_0042/` | Rolling event log; parts read in sequence order |
| History Server zip | `./application_…_0042.zip` | The zip from the History Server's Download button; unpacked in memory within size limits |

Resolution order:

1. `-eventlog` on the command line.
2. `eventlog-prefix` in the config file (an S3 location, once found), combined with `-app-id`.
3. Neither: the run continues on container logs and AWS APIs. Sections that need the event log show "No event log supplied", and the run exits 3 (partial).

Optional later: Lake Formation and Ranger audit logs, HiveServer2 and HBase server logs, for data-level permission decisions.

## 4. Architecture

sparkplain runs a six-stage pipeline: resolve, collect, parse, correlate, analyse, render. Each stage has one package and a narrow interface, so sources and analysers can be added without touching the rest.

```mermaid
flowchart LR
  R[Resolve<br/>cluster, app, time window] --> C[Collect<br/>S3, local dir, AWS APIs]
  C --> P[Parse<br/>event log, container, step, node logs]
  P --> X[Correlate<br/>host to instance, step to app]
  X --> A[Analyse<br/>modules + rules]
  A --> O[Render<br/>HTML + JSON]
```

**Packages** (standalone Go module)

| Package | Responsibility |
| --- | --- |
| `cmd/sparkplain` | CLI flags, config file, exit codes |
| `internal/sparkplain/source` | `Source` interface with S3 and local-directory implementations; owns listing, filtering, concurrency and streaming decompression |
| `internal/sparkplain/eventlog` | Streaming decoder: plain, `.zstd`, `.lz4`, `.snappy`, `.inprogress` and rolling `eventlog_v2_*` directories; dispatch on the `Event` field; unknown events skipped and counted |
| `internal/sparkplain/yarnlog` | Line classifiers for container, step and node logs (OOM, exit codes, Kerberos, metastore, HBase, exceptions) |
| `internal/sparkplain/awsmeta` | EMR, CloudWatch and CloudTrail clients on AWS SDK for Go v2 |
| `internal/sparkplain/model` | Canonical types: `Application`, `Node`, `Executor`, `Job`, `Stage`, `TaskStats`, `MemorySample`, `ConfigEntry`, `Identity`, `AccessEvent`, `Finding` |
| `internal/sparkplain/analyze` | One `Analyzer` per report section, each returning section data plus findings |
| `internal/sparkplain/report` | `html/template` with embedded CSS and script (`go:embed`); report charts are inline SVG built in Go, so `report.html` needs no chart library; the explorer page uses Google Charts (see §6); JSON writer |
| `internal/sparkplain/redact` | Secret redaction and text sanitising, applied by parsers before values enter the model |

**Key design rules**

- **Stream, never load whole logs.** Event logs reach several GB. Task events are folded into per-stage aggregates (count, sum, min, max, p50, p95 via a fixed histogram) and then discarded.
- **Bounded memory.** Target under 1 GB RAM for a 1 GB event log with one million tasks.
- **Correlation keys.** Executor host name (for example `ip-10-0-1-23.ec2.internal`) joins to `ListInstances` private DNS, giving instance ID, type, market and group. Step logs join to the app through the application ID found in step stderr. AWS queries use the app window padded by 5 minutes.
- **Provenance on everything.** Every model value records its source file and line, so findings link to evidence.
- **Graceful degradation.** A missing source removes its sections and is listed in the report's Sources panel; the run exits 3 (partial).

## 5. Features

The report has ten analysis modules. Each answers a fixed set of questions and emits findings with severity (info, warning, critical) and evidence links.

| # | Module | Questions answered | Main sources | Phase |
| --- | --- | --- | --- | --- |
| 1 | Application summary | Name, ID, Spark and EMR versions, submitting user, queue, step, start, end, duration, final status | Event log, step logs, EMR API | 1 |
| 2 | Cluster and nodes | How many nodes ran executors, of how many in the cluster; instance type, vCPU, memory, spot or on-demand, core or task group; nodes lost or decommissioned mid-run | Event log hosts, EMR API, node logs | 1 (hosts), 3 (instances) |
| 3 | Executors | Executor count over time; cores and memory each; executors per host; lifetime; removal reason (idle, lost, killed); failed tasks per executor | Event log | 1 |
| 4 | Memory | Configured memory plus overhead versus peak JVM heap, off-heap and process RSS per executor; storage memory used; spill to memory and disk; GC time share; OOM and exit 137/143 events | Event log, container logs | 1, 2 |
| 5 | Storage and I/O | Input and output bytes and records per stage; shuffle read and write; disk spill; cached RDDs and their size; S3 paths and tables read or written (from SQL plans) | Event log | 1 |
| 6 | CPU | Executor CPU time versus run time (utilisation %) per stage and executor; host CPU over time where available | Event log, CloudWatch, node logs | 1, 3 |
| 7 | Jobs, stages, tasks | Timeline of jobs and stages; critical path; skew (max versus median task time, confirmed by rows or bytes read); retries, speculation, failed stages and reasons | Event log | 1 |
| 8 | Configuration | Runtime environment table (versions of Spark, Scala, Java, Hadoop and notable libraries; master, deploy mode, OS, time zone, default filesystem; Java and Spark home, working, event log, warehouse and scratch directories); full effective config grouped (Spark, Hadoop, Hive, HBase, JVM); dynamic allocation settings; values that differ from EMR defaults; risky settings flagged | Event log environment, EMR API | 1 |
| 9 | Identity and access | See below | Event log, container logs, EMR API, CloudTrail | 2, 3 |
| 10 | Failures and findings | Root failure, first exception, and rule-based findings ranked by severity | All | 2, 4 |

**Identity and access (module 9) in detail**

- **Who ran it.** OS user, Hadoop user, YARN queue, Kerberos principal and realm if Kerberos is enabled.
- **AWS identity.** EC2 instance profile, EMR service role, and runtime role if the step used one.
- **Cluster security posture.** EMR security configuration: encryption at rest and in transit, Kerberos, Lake Formation integration.
- **Hive.** Catalog type (AWS Glue Data Catalog or Hive metastore URI) and whether connections succeeded.
- **HBase.** ZooKeeper quorum, authentication mode, connection errors.
- **AWS calls.** Services and actions called by the job's role in its time window, plus every `AccessDenied`, from CloudTrail.

**Secret handling.** Spark redacts matching config values in the event log, but sparkplain re-applies its own redaction regex (password, secret, token, key, credential) to config and log lines. The report shows that a credential is configured and where, never its value. In free text (exception messages, plans, JVM options) it hides `key=value` pairs whose key matches, URL passwords and AWS access key IDs; plan column lists such as `keys=[region#12]` are left alone. Because "key" is in the regex, some harmless Hadoop settings (for example key-provider cache sizes) are hidden too; that is deliberate.

**Findings rules (initial set)**

- Task skew: slowest task over 5× the stage median (`skew-ratio`), only for stages with at least 5 successful tasks (`skew-min-tasks`) whose slowest task took at least 1 s (`skew-min-task`), so tiny stages do not raise noise. The slowest task must also have read more than `skew-ratio` × the median task's rows (input plus shuffle), shuffle bytes or input bytes: a slow task that read the usual amount, such as the first task on a warming-up executor, is not skew and is not reported. The explanation leads with rows read against the median.
- Spill: disk spill over 10% of shuffle write in any stage (`spill-share`). Stages that spill but write no shuffle data (a sort before a file write) are flagged too. All spilling stages are grouped into one finding.
- GC pressure: GC time over 10% of executor run time (`gc-share`).
- Low CPU use: executor CPU time under 30% of run time (`low-cpu-share`). Spark counts only JVM CPU time, so for PySpark jobs the finding says that Python UDF work shows up as waiting. A companion info rule flags executor cores busy under the same share of the core time they held.
- Over-provisioned memory: peak heap under 40% of configured executor memory (`memory-used-share`); a warning when peak heap passes 90%.
- Lost executors or nodes, spot interruptions, container OOM kills. From the event log alone this means the removal reason: exit 137 (SIGKILL, usually a memory kill), heartbeat loss or lost node, decommissioning.
- `AccessDenied` in CloudTrail or permission errors in container logs.
- Also from the event log: failed jobs (critical when the last job failed and the app ended), stage retries, task attempts that failed and were retried, static AWS keys in the Spark configuration, and risky settings (unlimited `spark.driver.maxResultSize`, dynamic allocation without shuffle service or tracking, AQE off).

The CPU, GC and over-provisioning rules skip runs with less than 1 minute of task time (`min-run-time`). Thresholds live in the config file so teams can tune them.

## 6. Report output and CLI

Each run writes `report.html` and `report.json` to `~/sparkplain/<yyyy-mm-dd>/<app-id>/` unless `-out` is given.

**Usage**

```sh
# Event log from an S3 prefix
sparkplain -profile prod-emr -cluster-id j-1ABC2DEF3GHI4 -app-id application_1700000000000_0042 \
        -eventlog s3://bucket/sparklogs/

# Event log downloaded from the History Server
sparkplain -profile prod-emr -cluster-id j-1ABC2DEF3GHI4 -app-id application_1700000000000_0042 \
        -eventlog ./application_1700000000000_0042.zip

# Fully offline: local log folder plus downloaded event log
sparkplain -from ./logs/application_1700000000000_0042 \
        -eventlog ./application_1700000000000_0042.zip
```

**Flags**

| Flag | Purpose |
| --- | --- |
| `-profile`, `-region`, `-config` | Named AWS profile (required online), region override, YAML defaults file at `~/.config/sparkplain/config.yaml` |
| `-cluster-id`, `-cluster-name`, `-app-id` | Scope the run; `-app-id` required |
| `-eventlog` | Optional. S3 prefix or object, local file, rolling folder, or History Server zip; falls back to `eventlog-prefix` in the config file |
| `-from` | Offline input directory |
| `-out` | Output directory override |
| `-format` | Comma-separated outputs: `html`, `json`, `explorer` (default all three; `both` still means `html,json`) |
| `-workers`, `-max-size`, `-overall-timeout` | Fetch budgets: concurrency, per-object size cap, run deadline |
| `-no-cloudwatch`, `-no-cloudtrail` | Skip enrichment (fewer permissions needed) |
| `-window-pad` | Padding on the AWS query window (default 5m) |
| `-show` | Print the event at a `file:line` the pages cite, redacted, and exit |
| `-source` | The application's source file or folder (repeatable); the explorer shows it beside the jobs and stages that ran each line, redacted |

**HTML report layout** (single self-contained file, works offline, light and dark themes)

1. Header cards: status, duration, nodes, peak executors, total CPU hours, peak memory, data read and written, finding counts.
2. Timeline: jobs and stages as a Gantt chart, executor count overlaid.
3. Nodes: table of instances with executors hosted and CPU and memory charts per node.
4. Executors: table plus peak-memory-versus-configured chart.
5. Memory, CPU, I/O: charts per stage.
6. Jobs and stages: sortable table with skew and spill columns; stage drill-down.
7. SQL: queries with their physical plans and tables touched.
8. Configuration: a runtime environment table first (versions, runtime settings, locations, each with the property it was read from and its line), then key settings explained, then every setting grouped, searchable, non-defaults highlighted. The header line also shows the Java and Hadoop versions.
9. Identity and access.
10. Findings: ranked, each with evidence links.
11. Sources: every file and API read, what was missing, and why.

**Explorer page** (`explorer.html`, phase 1b): a per-application take on the Spark History Server's tabs, with interactive charts. It is built from the same `-app-id` and `-eventlog` inputs in the same run, written next to `report.html`, and the two pages link to each other (findings link to the stage, executor or query they concern). It is one file with its data embedded as JSON; it loads Google Charts from `www.gstatic.com`, pinned to a frozen release rather than `current`, so it needs internet access when opened. Rules:

- Only chart types that render in the browser (`corechart`, `timeline`); never GeoChart or Map. No report data leaves the machine.
- Tables, text and navigation are drawn by the page's own embedded script, so if Google Charts can't load, a banner says so and everything except the charts still works.
- Stage DAGs and SQL plan graphs are SVG laid out in Go, as in `report.html`, because Google Charts has no directed-graph chart.
- Same redaction, time-zone labelling and light and dark themes as the report. Embedded JSON is escaped so no log value can close the script tag.

| Tab | Shows |
| --- | --- |
| Overview | Status, duration, headline numbers, findings with links into the other tabs, link to `report.html` |
| Jobs | Zoomable timeline of jobs (lanes by job group), executor count overlaid, failures marked; sortable table; job detail lists its stages |
| Stages | Sortable, filterable table; stage DAG. Stage detail: summary metrics (min, p25, median, p75, max) for duration, GC, rows and bytes read, shuffle read and write, spill; task-duration histogram; task scatter (launch time against duration, one colour per executor); the slowest tasks; totals per executor |
| Executors | Lifetimes on a timeline with removal reasons; running tasks against available task slots over time; table of tasks, failures, time, GC, input, shuffle and peak memory; peak memory per executor per stage (only when `spark.eventLog.logStageExecutorMetrics` was on) |
| SQL / DataFrame | Queries on a timeline with duration and status; query detail: plan graph with each operator's metrics (rows, time, size, spill; totals across tasks), the plan text, the jobs it ran, tables read and written. The final AQE plan is shown |
| Storage | Cached RDDs and DataFrames (sizes only when `spark.eventLog.logBlockUpdates.enabled` was on) |
| Environment | The runtime table and all settings, as in the report |

**Explorer data, collected while streaming** (the no-whole-log rule and the 1 GB budget still hold):

- Per stage: exact quartiles and a log-scale histogram per metric, from the existing `distAcc`.
- Task sample per stage: the 100 slowest tasks (a bounded heap) plus a uniform reservoir of 1,000 (fixed seed, so reruns give the same page). An app-wide budget of 100,000 sampled tasks halves every reservoir by random subsampling when it is exceeded, which keeps each sample uniform. The page labels charts drawn from the sample.
- Totals per stage and executor, capped at 1,000,000 cells app-wide; past the cap, the per-executor table says it is partial.
- Running tasks over time: task start and end folded into time buckets that double in width when they run out, capped at 2,000 buckets.
- SQL operator metrics: the plan tree's metric IDs, resolved from `StageCompleted` accumulables and `SparkListenerDriverAccumUpdates` (checked against a real EMR 7.3.0 log: operator row counts, time, spill and peak memory all resolve this way).
- Limits live in the config file under `explorer:` (`slowest-per-stage`, `sample-per-stage`, `max-sampled-tasks`, `max-stage-executor-cells`). At the defaults, `explorer.html` stays under 25 MB.
- The explorer data lives only in `explorer.html`; `report.json` keeps its current shape, so the sample and buckets don't bloat it.

**JSON export** mirrors the model package, versioned with a `schemaVersion` field, so other tools or dashboards can consume it.

**Times.** The HTML renders times in the config file's `timezone` (default: the local zone of the machine running sparkplain). Each time also carries its UTC instant, and the page's script relabels it in the viewer's browser zone, naming the zone. JSON times are UTC.

**Config file** (`-config`, default `~/.config/sparkplain/config.yaml`; unknown keys are rejected):

```yaml
eventlog-prefix: ./spark-events/   # used when -eventlog is not given
timezone: Australia/Sydney
out: ~/reports
format: both
max-size: 10GiB
overall-timeout: 30m
thresholds:
  skew-ratio: 5
  skew-min-task: 1s
  skew-min-tasks: 5
  spill-share: 0.10
  gc-share: 0.10
  low-cpu-share: 0.30
  memory-used-share: 0.40
  min-run-time: 1m
  sched-delay-share: 0.20   # scheduler delay over this share of task time
  locality-any-share: 0.30  # input tasks away from their data over this share
  result-share: 0.50        # a stage's results over this share of spark.driver.maxResultSize
  slow-startup: 1m          # executors taking longer than this to register
explorer:
  slowest-per-stage: 100
  sample-per-stage: 1000
  max-sampled-tasks: 100000
  max-stage-executor-cells: 1000000
```

**Exit codes:** 0 complete, 2 fatal (usage, credentials, listing), 3 partial (a source missing or unreadable), 130 interrupted.

`-app-id` is required and must match the application ID inside the event log; a mismatch exits 2. A path that does not exist exits 2. A log that exists but is corrupt, truncated or still `.inprogress` still produces a report that marks what is missing, and exits 3.

**IAM permissions:** `s3:ListBucket` and `s3:GetObject` on the log and event-log prefixes, `kms:Decrypt` for SSE-KMS buckets, `elasticmapreduce:ListClusters`, `DescribeCluster`, `ListInstances`, `ListInstanceGroups`, `ListInstanceFleets`, `ListSteps`, plus optional `cloudwatch:GetMetricData` and `cloudtrail:LookupEvents`.

## 7. Limitations and risks

The biggest risk is event log availability; the rest are accuracy limits the report must state plainly rather than hide.

| Risk | Impact | Mitigation |
| --- | --- | --- |
| Event log sits in HDFS and dies with the cluster | Most modules empty | Optional input: read from S3 when a copy is found, or from a manually downloaded file; otherwise run without it and mark affected sections. No cluster changes required |
| Event logs of several GB | Slow runs, high memory | Streaming decode, per-stage aggregation, benchmark in CI |
| Executor memory is sampled (heartbeats and stage peaks), not continuous | Short spikes missed | Label memory charts as peaks; enable `spark.executor.processTreeMetrics.enabled` for RSS |
| Host CPU needs the CloudWatch agent (EMR 7.0+) or instance-state snapshots | CPU module partial | Use Spark task CPU time as the primary measure; show host CPU only when present |
| Shared clusters run several apps at once | Node metrics not attributable to one app | Show concurrent YARN apps in the window; mark node-level figures as cluster-wide |
| CloudTrail `LookupEvents` covers 90 days of management events only | No S3 object-level access history | State the gap; read a data-events trail from S3 if one exists (later phase) |
| Event schema drifts in newer Spark releases (baseline: Spark 3.5.1 on EMR 7.3.0) | Parse gaps | Target the Spark 3.5 schema; ignore unknown fields and events; fixture logs per supported EMR release in tests |
| Secrets in logs or config | Leak through the report | Redaction pass before rendering; tests with planted secrets |
| Clock skew between nodes and AWS services | Misaligned timelines | Use event log timestamps as the reference; pad AWS windows |

## 8. Delivery plan

Four phases, each shippable on its own. Phase 1 delivers most of the value from the event log alone and needs no AWS calls.

| Phase | Scope | Done when |
| --- | --- | --- |
| 1. Event log core | `eventlog` decoder, model, modules 1–8 from the event log, offline `-eventlog` input, HTML and JSON output | A real EMR event log renders a full report; a 1 GB log parses in under 60 s using under 1 GB RAM |
| 1c. Full event coverage | Every field of every Spark 3.5 event is used or set aside with a reason, enforced by a field-inventory test; the new data reaches the explorer, the report and new findings; code shown beside jobs and stages with `-source` (plan below) | The inventory test passes on every fixture and the real EMR log; each new view renders with no script errors; the 1 GB budget and the 25 MB page budget still hold; planted secrets never appear |
| 2. S3 fetching and online mode | S3 fetch layer and online mode; optional event log fetch from an S3 prefix; container and step log classifiers; memory OOM evidence; identity from logs; first failure analysis | `-cluster-id` + `-app-id` produces the report with no manual downloads |
| 3. AWS enrichment | EMR instance mapping, CloudWatch host metrics, CloudTrail access events, security configuration | Nodes table shows instance type and market; Identity section lists roles and `AccessDenied` events |
| 1b. Explorer | Explorer data collection in the parser (task sample, per-stage quartiles and histograms, stage × executor totals, running-task buckets, SQL operator metrics); `explorer.html` with the tabs in §6; `-format explorer` | The fixtures and a real EMR event log render every tab; with Google Charts blocked, the tables still work; a 1 GB log still parses in under 60 s using under 1 GB RAM; planted secrets never appear in `explorer.html` |
| 4. Findings and polish | Full rules engine with tunable thresholds, Sources panel, redaction tests, fixture logs from EMR 7.3.0 onward | All findings rules covered by tests; CI benchmark and `govulncheck` pass |

Testing: stub S3 and AWS clients, race detector on, fuzz targets for the event decoder and log classifiers.

**Phase 1 status (built).** Offline mode only: `-eventlog` accepts a local file, rolling folder, folder holding logs (the highest attempt wins, preferring finished logs) or History Server zip. The online flags (`-profile`, `-cluster-id`, `-cluster-name`, `-from`) and `s3://` locations exit 2 and name the phase that adds them. Notes from building it:

- Fixtures come from real PySpark 3.5.1 runs in `local-cluster` mode (`scripts/fixtures/`), scrubbed to EMR-like hosts, paths and IDs. Compressed variants are written with Spark's own `CompressionCodec`, so the lz4 and snappy readers are tested against the Java stream formats. The History Server zip is laid out as Spark's `zipEventLogFiles` writes it, but built by the script rather than downloaded.
- Spark 3.5 does not log the driver's exit code (`ApplicationEnd` has only a timestamp), so the final status comes from the end event and the last job, and the report says so. A log without an end event is "incomplete".
- Cached data sizes are logged only when `spark.eventLog.logBlockUpdates.enabled=true`; process RSS only when `spark.executor.processTreeMetrics.enabled=true`. The report says which is missing.
- The runtime environment table is the driver's view only: the event log records no executor JVM or OS details. Library versions (Hadoop, Hive, EMRFS, AWS SDKs, Iceberg, Hudi, Delta, HBase, Py4J) come from jar names on the driver's classpath, since the log does not state them; Spark home is the folder holding `jars/spark-core_*.jar`. The EMR release and the Python version are not in the event log and show as "not recorded".
- Key settings are compared with Spark 3.5 defaults. Comparing with EMR's own defaults needs the EMR API (phase 3).
- The report uses system fonts. The sample's Google Fonts link would break the no-network rule.
- Decision (2026-09-26): a per-application explorer page, a richer take on the History Server's tabs, will use Google Charts. Google Charts cannot be self-hosted, so `explorer.html` loads it from `www.gstatic.com` and needs internet access when opened; `report.html` stays fully offline. Only browser-rendered chart types are used, so report data never leaves the machine. Its design is in §6 (Explorer page) and it is built as phase 1b, before phases 2 and 3, in four steps, each tested and shippable: (1) data collection and its tests and benchmark; (2) the page shell: embedded data, tabs, tables, offline banner, themes, links with `report.html`; (3) the charts; (4) the stage DAG and SQL plan graphs.
- Phase 1b step 1 (built): `eventlog.Options.Explorer` turns on collection into `EventLog.Explorer` (never serialised into `report.json`). On the 1 GB benchmark log (245K tasks, 490 stages, 200 executors) parsing takes 4.5 s either way; live heap is 76 MiB with explorer data against 1 MiB without, and peak RSS 218 MB. The sample budget halved twice, leaving 134,750 sampled tasks and 98,000 stage × executor cells. As plain JSON those are 42 MiB and 78 MiB, mostly repeated field names, so step 2 embeds them in a compact column format to stay under 25 MB.
- Phase 1b step 2: a worst-case test page (490 stages × 200 executors, every value large) came to 30.7 MB with those limits, so the default sample budget is now 100,000 tasks and the stage × executor rows carry only the 13 columns the page shows. Apps under about 100,000 tasks still keep every stage's full sample.
- Phase 1b step 3: charts use Google Charts release 52 (a frozen version; `current` changes under the page), loaded only when a view has a chart. Charts: running tasks against task slots, and job, executor and query timelines; per stage a duration histogram (every task) and a task scatter (the sample plus the slowest); per executor peak heap by stage. Colour follows the dataviz method and was run through its validator for both themes: one series colour (categorical slot 1), the status red for failures, and neutral grey for everything else. Red, amber and green together failed the colour-vision checks, so timelines use blue, red and grey, and every colour is also named in a caption or tooltip. With the network blocked, a banner says the charts need www.gstatic.com and the rest of the page works.
- Phase 1b step 4: graphs are laid out in Go (`report/layout.go`: layers by longest path, barycentre ordering to cut crossings, every edge pointing down) and drawn by the page as SVG. Job and stage pages show the job's stage DAG, with the current stage, failed and skipped stages marked; query pages show the final plan as a graph with rows and time per operator, above the full operator table. Graphs over 300 nodes are left to the tables.
- Phase 1b status (built): the fixtures and a real EMR 7.3.0 log render every tab with no script errors (checked in headless Chrome, 64 route renders across four logs, including a failed and an in-progress one); tables work with Google Charts blocked; the 1 GB log still runs in 4.6 s at 275 MB peak RSS and gives an 11 MB page; planted secrets never reach `explorer.html`.
- Checked against the real EMR log: task launch and finish times are stamped by the driver, and a slot is reused before the driver records the previous task's finish, so a 2-core executor showed up to 4 overlapping tasks. The Spark UI's timeline has the same overlap. The running-tasks chart will say its values are driver-side times and can briefly exceed the task slots.
- Performance, measured with `scripts/benchlog` on 4 cores: a 1 GB log (245K tasks, real task-event layout) in 5.6 s at 30 MB peak RSS for the whole CLI; 1M tasks (2.4 GB) in 14.7 s at 68 MB.
- Still open for the "done when" check: a real EMR 7.3.0+ event log, which cannot be generated here.

**Phase 1c plan (proposed).** Goal: nothing in the event log is dropped silently, because the detail that explains a problem is often one nobody thought to show. Each step is tested before the next.

1. **Field inventory.** A table in `eventlog` lists every event and nested field Spark 3.5 writes (and EMR's additions), each marked *used* (with where it is shown) or *set aside* with a reason (for example, `Task Info.Accumulables` entries that repeat task metrics already read, or internal properties such as `__fetch_continuous_blocks_in_batch_enabled`). A test walks every line of every fixture and the real EMR log and fails on any field not in the table. At run time, unlisted fields and events are counted and shown in the Sources panel and the explorer, so a newer Spark's additions are visible rather than lost.
2. **Tasks.** Keep locality, task type, partition ID, result size, result serialization time, getting-result time and the scheduler delay Spark's UI derives from them, deserialize CPU time, shuffle write time, local and remote blocks fetched, remote bytes read to disk, push-based shuffle metrics, GC counts and times (minor and major), unified and virtual memory, cache writes (`Updated Blocks`), and the full stack trace of each distinct failure (redacted, capped per reason). Stage summaries gain these as quartiles; the task sample gains the columns the page shows. Per-task SQL metric updates become per-operator distributions (min, median, max and the task that hit the max, as Spark's SQL tab shows).
3. **Executors and nodes.** Read executor and node exclusions at stage and application level, and exclusions being lifted (Spark 3.5 writes each twice, under the new *Excluded* and the old *Blacklisted* names, so they are merged); block manager removals; `TaskStart` for tasks still running when an in-progress log ends; executor log URLs, request and registration time (so startup delay), attributes and resources (such as GPUs); driver log URLs and attributes; resource profiles. Memory: each executor's per-stage peaks (`StageExecutorMetrics`), and the driver's heartbeat samples (`ExecutorMetricsUpdate`), which feed only the driver's overall peak because they always carry stage -1. Checked against Spark 3.5.1's `EventLoggingListener`: it has no handler for `SpeculativeTaskSubmitted`, `UnschedulableTaskSetAdded`/`Removed` or `MiscellaneousProcessAdded`, so they never reach an event log, and it writes heartbeat updates only for the driver and only when `spark.eventLog.logStageExecutorMetrics` is on. Speculation therefore shows through each task's `Speculative` flag and the reason its twin was killed.
4. **Stages and jobs.** Each stage's `RDD Info` (operation, scope, call site, parent RDDs, partitions, storage level) becomes a per-stage operation graph like the Spark UI's, drawn with the existing layout. Keep the long call site (`Details`), resource profile and shuffle-push settings. Job and stage properties are compared with the application's settings, and only local overrides (scheduler pool, job group, per-job settings) are shown, redacted like all configuration.
5. **SQL.** Keep `details`, `modifiedConfigs` (redacted), `rootExecutionId` (sub-queries shown under their parent), `jobTags`, AQE metric updates, and EMR's `SparkListenerQueryExecutionMetrics` (optimizer time per rule). Catalog events (databases and tables created, dropped or altered) join the explorer.
6. **Explorer, report and findings.** Show all of the above where it belongs: new stage-summary rows, task columns, executor facts and timeline marks, a memory-over-time chart, per-stage operation graphs, operator metric distributions, query settings and sub-queries, and a "Log events" view listing counts per event type, including unknown ones. Items only in `report.html` so far (CPU time, catalog events, resource profiles, critical path, driver memory) move into the explorer too. New findings: executors or nodes excluded, high scheduler delay, poor data locality, large task results (driver memory risk), slow executor startup, speculation, tasks waiting for resources, and tasks still running at the end of an in-progress log. For investigation, `sparkplain -show <file:line>` prints the redacted event behind any value the pages cite.

7. **Code beside jobs and stages.** The event log records where in the code work ran, not the code. Call sites come from a job's `callSite.short`, each stage's name and `Details` stack, each RDD's `Callsite` and each query's `details`; for Scala and Java jobs, user frames are the stack frames outside Spark, Scala, Java and py4j packages. Checked on the real EMR PySpark log: only 5 of 19 jobs carry a Python file and line (`collect()` does; `count()` and `write` show only `NativeMethodAccessorImpl.java:0`), because PySpark passes its Python call site for some actions only. So:
   - Every job, stage and query shows the code location Spark recorded, or says none was recorded and how to add one (`sc.setJobDescription(...)` or setting `callSite.short` with `sc.setLocalProperty`). Jobs without a location also show the nearest recorded locations before and after them in time, as context rather than a guess.
   - `-source <file or folder>` (opt-in, repeatable) embeds the matching source files in the explorer, matched by path suffix and then file name, at most 5,000 lines per file and 4 MB in all. A Code tab shows each file with every recorded line marked by the jobs, stages and queries that ran there (count, time, status, findings); selecting a line lists them beside the code. Job, stage and query pages show the code around their line, side by side on wide screens.
   - Code is redacted before it is embedded, with a code-aware pass on top of the usual one: on any line that names a sensitive key, every string literal except the key itself is hidden, so `.config("spark.db.password", "…")` loses its value. Tests plant secrets in a source fixture (the fixture workload script already has them) and assert none reach the page.
   - Phase 2 adds fetching the script automatically when it lives on S3 (a read-only `GetObject`, found from the step's `spark-submit` arguments).

Fixtures (added, not regenerated, so existing expectations hold): `0046` exercises exclusions and their lifting, cache block updates, per-query settings with a planted secret, job groups and scheduler pools, a call-site override, large task results, catalog create, alter, rename and drop, and a sub-query; `0047` is an in-progress copy taken with three tasks still running; `0048` is a job aborted because its only node was excluded; `0049` is the real EMR 7.3.0 log, scrubbed (`scripts/fixtures/scrub_emr.py`). A local cluster cannot show a speculative copy (Spark never runs one on the host of the original, and all local executors share one host), nor push-based shuffle (YARN only) or GPUs, so a second EMR run checks speculation and push-based shuffle; GPU fields stay "not seen in a fixture". Budgets stay as they are, and the tests that enforce them keep running: if the extra task columns push the page past 25 MB, the sample budget comes down again.

- Phase 1c step 1 (built): `eventlog/inventory.txt` lists 537 fields of 48 event types (321 used, 33 set aside with a reason, the rest planned for steps 2 to 5). `TestFieldInventory` walks every distinct fixture and fails on an unlisted field, or on a listed one no fixture holds unless it says "not seen in a fixture"; every set-aside reason that claims something about Spark (empty at task start, duplicated elsewhere) was checked against the fixtures. At run time, top-level fields are checked on every event and nested fields on the first 100 events of each type, and unlisted ones are counted as unknown. The inventory replaced the hand-kept lists of known fields and ignored events, which had missed that Spark writes exclusion events under their full class names (`org.apache.spark.scheduler.SparkListenerExecutorExcluded`), so those had been counted as unknown.
- Second EMR 7.3.0 run (1 primary, 2 core nodes; fixture `0050`, scrubbed): a speculative copy ran on another host, and the other attempt ended `TaskKilled` with "Stage finished". Push-based shuffle did not switch on with `spark.shuffle.push.enabled=true`, the YARN `RemoteBlockPushResolver` and a one-merger minimum (every stage logged `Shuffle Push Enabled: false`); why was not investigated further, so its metrics are read but only ever seen as zero. The first attempt at this run failed because Spark refuses an S3 `spark.eventLog.dir` with no objects under it; an empty `spark-events/` marker object fixed it.
- Phase 1c step 7 (built): jobs, stages and queries carry their code locations. A new Java fixture (`0051`, `scripts/fixtures/java/ClaimsJob.java`, built with `javac --release 11` against Spark's jars) confirmed that a JVM application's stage names and call stacks hold its own frames (`writeTotals` at line 29 called from `main` at line 20); its six jobs all have a location, against 5 of 19 in the real EMR PySpark log, where only `collect()` records one. `NativeMethodAccessorImpl.java:0` and `<stdin>` count as no location, and a job without its own takes its last stage's. `-source` matches each logged file to a local one by the longest shared path tail, embeds the matched files (up to 5,000 lines each, 4 MB in all) redacted with `redact.Code`, which keeps a config call's key and hides every other string on a line that names a secret, even when the secret itself contains a word like PASSWORD. The explorer's Code tab lists every location with what ran there and shows each source file beside that activity; job, stage and query pages show their frames, the code around them, or why none was recorded with the nearest recorded jobs before and after.
- Phase 1c status (built): every one of the 552 fields in the inventory is used or set aside with a reason, and the inventory test fails if one is left planned. `make check` passes; the fixtures and the real EMR logs render every tab with no script errors.
- Phase 1c step 6 (built): new findings `executors-excluded` (warning for application or node exclusions, info for stage-only), `scheduler-delay` (warning over `sched-delay-share` of task time), `poor-locality` (info when a stage's input tasks ran away from their data over `locality-any-share`), `large-results` (warning when a result stage returned over `result-share` of `spark.driver.maxResultSize`), `slow-executor-startup` (info over `slow-startup`), `speculation` (info, with how many copies finished first) and `tasks-running-at-end` (warning; worded for an in-progress log or, when the log did end, for tasks a job abort cut off, as in `0048`). Every finding's evidence links to its stage or executor. The explorer gains CPU share for stages and executors, tables and paths, resource profiles (including extra resources such as GPUs, which the parser had dropped), the critical path, and an Event log tab with files, events by type and anything not recognised. `sparkplain -show <file:line>` prints the redacted event behind any value the pages cite; a miss lists the log's file names.
- Phase 1c step 5 (built): queries keep their parent (`rootExecutionId`; commands such as CTAS run inner queries), job tags, long call site and the session settings in force (`modifiedConfigs`, redacted; the fixture's session token appears in every later query's), the metrics adaptive execution adds after planning (the log names no operator for them, so they are listed per query and resolved like plan metrics), and EMR's optimizer report (`SparkListenerQueryExecutionMetrics`: about 230 rules per query with time and runs, and which changed the plan; the model keeps up to 300 rules, the page the slowest 30).
- Phase 1c step 4 (built): each stage keeps the RDDs it computes (operation scope, call site, parents, partitions, cached partitions, storage level, barrier, determinism; capped at 200), its long call site, resource profile and push-based shuffle settings. Jobs keep the local properties that differ from the application's settings (scheduler pool, job group, session settings such as a changed `spark.sql.shuffle.partitions`), redacted by key; the fixture's session token set with `spark.conf.set` reached every later job's properties, so redaction matters here. Stages keep properties only where they differ from their job's (the EMR log shows `resource.executor.cores` set per stage). The explorer draws each stage's operation graph like the Spark UI's stage graph, shows the call stack, and lists each job's own settings.
- Phase 1c step 3 (built): exclusions (executor and node, stage and application, with when each lifted; Spark's two names for each merged), tasks still running at the end of a log (capped at 100,000), executor log links, container attributes (redacted by key), resources and startup delay (registration less request time), when each block manager left, the driver's log links and attributes, where each cached RDD's blocks were per executor with their storage level, and block-manager activity per kind of block. The explorer shows them on the overview, executors, executor, stage and storage pages, with excluded periods on the executor timeline. The driver's heartbeat samples carry stage -1 in every fixture, so the planned per-stage driver memory was dropped.
- Phase 1c step 2 (built): tasks now yield locality, task type, partition, result size, result serialization and fetch time, and scheduler delay (as the Spark UI computes it: duration less run, deserialize, serialize and fetch time, where "Getting Result Time" is the timestamp the fetch began), deserialize CPU, shuffle write time, local and remote blocks, remote bytes fetched to disk, fetch request time, push-based shuffle merges, cache writes, GC counts and times, unified and virtual memory, the first full stack trace of each distinct failure and whether Spark blamed the app for a lost executor, and the stack trace of a failed job. Per-task SQL metric updates give each operator's min, median and max and the task that hit the max; Spark stores "average" metrics per task as ten times the value (a real EMR log showed 320 over 32 tasks for an average of 1.0). A failed attempt's `Accumulator Updates` repeat its Task Metrics, so they are set aside. The explorer shows all of it on stage, job, executor and query pages; the worst-case page is 18.3 MB. `0046` now returns incompressible 3 MB results so the driver fetches them separately (Spark compresses results, so a repeated string never crossed the 1 MB direct-result limit).

**Phase 2 plan (proposed).** Goal (from the table above): `-cluster-id` + `-app-id` produces the report with no manual downloads, and container, step and node logs join the event log as evidence. Every AWS call stays read-only (List, Get, Describe, Head), and tests use stubs behind interfaces, never real AWS. Each step is tested before the next.

1. **Fetching layer (`source`).** One interface for listing and opening objects, with an S3 implementation (`aws-sdk-go-v2`, an approved dependency: explicit `-profile`, region from the profile or `-region`, bucket region found with `HeadBucket`) and a local-folder one for `-from` and for tests. It carries the §2 rules: listing scoped to the application's prefixes, a worker pool (`-workers`), per-object size cap and timeouts, `If-Match` on the listed ETag, unrestored Glacier objects skipped and reported, streaming `.gz`, `.bz2` and bounded `.zip`, and the error classes shown in the Sources panel.
2. **Cluster and application.** `-cluster-id` (or `-cluster-name` through `ListClusters`) → `DescribeCluster` for `LogUri`, release, applications and the Spark configuration; `ListSteps` and each step's `stderr` to find the step that ran the application (its first `application_<ts>_<n>`). Works for terminated clusters while EMR keeps their metadata.
3. **Event log from S3.** `-eventlog s3://…` as an object or prefix, `eventlog-prefix` in the config file, and, when neither is given, the cluster's own `spark.eventLog.dir` if it is on S3. Otherwise the run continues without it and says so (exit 3).
4. **Log classifiers (`yarnlog`).** Container `stderr`/`stdout`: Java exceptions and Python tracebacks (first line, cause chain, redacted), `OutOfMemoryError`, YARN memory kills and exit codes 137 and 143, lost-executor causes, S3 and IAM access errors, Kerberos, Hive metastore and HBase connection lines. Step logs: the `spark-submit` command, its arguments (redacted) and final status. Node logs: NodeManager container kills and bootstrap action failures. Each classified line keeps its file and line.
5. **Joining and findings.** Container logs map to executors through the `CONTAINER_ID` attribute the event log records, and to the driver. New and sharper findings: the first failure across all sources (with the log line that shows it), memory kills with the container's own message, errors hidden in executor logs that the event log only shows as a lost executor, and access errors. The Identity section fills from logs (OS and Hadoop user, queue, Kerberos principal). The report's Sources panel lists every object read or skipped, and why.
6. **Explorer and code.** Executor and driver pages show their classified log lines, with links to the S3 objects; a Logs tab lists every source file with what was found in it. The application's script is fetched from S3 automatically (found in the step's `spark-submit` arguments) and shown as with `-source`.
7. **Fixtures and a live check.** The step, container and node logs of the four test clusters in the test bucket (including a failed run whose driver `stdout` holds a Python traceback), scrubbed like the event logs into `testdata/emrlogs/`, drive the tests through the local-folder implementation. A final read-only check runs `-cluster-id` against the live testbed cluster and one terminated cluster.

## 9. Open questions

- [ ] Where is the S3 copy of the event logs that the History Server reads (`s3a://…/sparklogs`), and can your AWS profile read it?
- [ ] Is EMR on EC2 the only target? (Minimum release settled: EMR 7.3.0.)
- [ ] Should the offline `-from` mode accept any folder layout, or only one mirroring the S3 structure?
- [ ] Are Kerberos, Lake Formation or Ranger enabled, and is HBase on the same cluster or external?
- [ ] Does a CloudTrail trail record S3 data events for the log and data buckets?
- [ ] Is the CloudWatch agent configured on your clusters?
- [ ] Who reads the reports: just you, or shared with a team?

## Sources

- [Amazon EMR 7.0.0 release notes](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-700-release.html) (CloudWatch agent added, Ganglia removed)

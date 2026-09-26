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

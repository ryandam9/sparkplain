# sparkplain — Design Specification

The current design. How it got here (phase plans, live checks, dated decisions) is in [HISTORY.md](HISTORY.md).

## 1. Overview

**sparkplain** is a Go CLI that turns one Spark application's logs into a plain-language HTML report, a JSON export and an interactive explorer page: what ran, on which nodes, with how much CPU, memory and storage, under which identity, and what went wrong. The target is Amazon EMR on EC2 7.3.0 and later (Spark 3.5.1 and newer), running or terminated clusters, with logs in S3 or on local disk.

**Problem.** The Spark History Server shows raw lists per tab. It does not say which node hosted which executors, how memory moved over time, what identity touched Hive, HBase or S3, or why a run failed; answering that means reading container logs by hand.

**Goals**

- One command, one application ID, one self-contained report.
- Every metric explained in plain words; every finding with a severity, evidence (file and line, or an API call) and a fix.
- Fetch everything itself from S3 and read-only AWS APIs, with no other tools and no cluster changes.
- Degrade, don't fail: a missing source is named and its sections marked, and the run still produces a report.

**Non-goals:** live monitoring or alerting; EMR Serverless and EMR on EKS (different log layouts); changing clusters or jobs; cost in dollars; Kerberos and authorization failures in HBase.

## 2. Inputs and fetching

**Input modes**

| Mode | Command | Reads |
| --- | --- | --- |
| Event log only | `-app-id … -eventlog <path>` | The event log: local file, rolling `eventlog_v2_*` folder, folder of logs, History Server zip, or `s3://` object or prefix |
| Online | `-profile … -cluster-id … -app-id …` | The EMR, EC2, CloudWatch, CloudTrail and STS APIs, and the cluster's logs under `<LogUri>/<cluster-id>/` in S3 |
| Offline | `-from <dir> -app-id …` | A local copy of the cluster's log folder (`containers/`, `steps/`, `node/`), a folder of such copies (the one holding the application wins), or one application's `container_*` folders; no AWS calls |

**Finding the event log.** The event log is optional (on EMR it defaults to HDFS and dies with the cluster). In order: `-eventlog`; `eventlog-prefix` in the config file; the cluster's `spark.eventLog.dir` when it is on S3; any S3 `spark.eventLog.dir` the cluster's steps set in their own `spark-submit` arguments, newest first. Without one, the run continues on the logs and APIs, marks the sections that need it, and exits 3.

| Event log input | Handling |
| --- | --- |
| S3 prefix | Lists only `<prefix>/<app-id>*` and `<prefix>/eventlog_v2_<app-id>*` |
| S3 object | Reads that object; a location without a trailing `/` is checked as an exact key first, then as a prefix |
| Local file | Plain, `.lz4` (lz4-java block stream), `.zstd`, `.snappy` (snappy-java stream) or `.inprogress` |
| Folder | A rolling log (parts in order), or a folder of logs, where the highest attempt wins, preferring finished logs |
| History Server zip | The Download zip, unpacked in memory within limits. Given as a file, or as the folder it was saved in (`eventLogs-<app>.zip`, `eventLogs-<app>-<attempt>.zip`, or a browser's ` (1)` copy; the newest when there are several), by `-eventlog` or a local `eventlog-prefix`. A leading `~/` in paths from flags or the config file is the home folder |

`-app-id` must match the ID inside the event log (a mismatch exits 2). A zip with several logs, none named after the application, is "not found"; a log whose name does not match and that has no application start event is refused.

**Fetching rules**

| Rule | How |
| --- | --- |
| Explicit identity | `-profile` required online (`default` for the default chain); region from the profile unless `-region`; each bucket's region found with `HeadBucket` |
| Scoped listing | Only `containers/<app-id>/`, the steps that may have submitted the application, and `node/<instance-id>/` for the nodes it ran on plus the primary node; never the whole cluster. The exception is `node/*/applications/hbase/` on every node that was up during the run, since a region server can serve the application from a node that ran none of its executors; nodes that ended before the run or joined after it are not listed (`ListInstances` names every instance a cluster ever had) |
| Time window | HBase logs hold every application's hours: hourly files outside the application's time are skipped and other lines dropped. The window comes from the event log, else from the application's container logs. AWS queries use the window padded by `-window-pad` (5 min) |
| Streaming | `.gz`, `.bz2`, plain and bounded `.zip`; every file streams straight into its parser, no temporary files |
| Bounded | Worker pool (`-workers`, 16); stored size per object (`-max-size`, 10 GiB); unpacked size per object, part or zip entry (`-max-unpacked`, 50 GiB); zips in memory (256 MiB each, 1 GiB at once); 1,000 entries per zip; event log lines up to 256 MiB; log lines cut at 64 KiB; `-overall-timeout` (30 min). A file cut short is marked partial, never passed off as complete |
| Consistency | `GetObject` with `If-Match` on the listed ETag; unrestored Glacier and Deep Archive objects are reported, not read |
| Error classes | `accessDenied`, `notFound`, `throttled`, `timeout`, `archivedUnavailable`, `corrupt`, `tooLarge`, shown in the Sources panel |
| Read-only | Only List, Get, Describe, Head and Lookup calls, and `sts:GetCallerIdentity` |

**Access check.** Every run starts by checking every access it will need, before reading anything, with small read-only calls made in parallel (15 s each), and prints one line per source. Online: who the credentials are (`GetCallerIdentity`); the EMR API (`DescribeCluster`, `ListSteps`, `ListInstances`, `ListInstanceGroups` or `ListInstanceFleets`, `DescribeStep` on the newest step, and `DescribeSecurityConfiguration` when the cluster has one); each log folder (container, step, node, and HBase when the cluster runs it, checked on the primary node) by listing one object and reading its first byte (a ranged `GetObject`), since a bucket policy can allow `s3:ListBucket` and refuse `s3:GetObject`, and a KMS-encrypted object fails only when read; where the event log will come from (`-eventlog`, `eventlog-prefix`, the cluster's `spark.eventLog.dir`, or a step's; an HDFS one is marked unreadable, with how to supply the log), listed and read the same way; the newest step's script on S3, read as the run reads it (`GetObject` alone); CloudWatch `ListMetrics` and `GetMetricData` (separate permissions), EC2 `DescribeInstanceTypes` and CloudTrail `LookupEvents`, unless turned off. Offline it checks the `-from` folders. Both check that `-source` paths exist and that the output folder can be written (without writing to it), which would otherwise fail only at the end. A readable folder with nothing in it yet is told apart from a refused one. A line that fails gives the error class, what it needs and an `aws` command that repeats the call. The run then reads every source as usual; the rows go into the JSON as `accessCheck`. `-check` stops after the check (exit 0 all readable, 3 not).

## 3. Data sources

No single source answers every question, so sparkplain joins them on cluster ID, application ID, container ID, host name and time.

| Source | Where | Provides |
| --- | --- | --- |
| Spark event log | `spark.eventLog.dir` | Application, executors (host, cores, removal reason), jobs, stages and tasks (time, CPU, GC, spill, shuffle, I/O, locality), per-stage executor memory peaks, cached data, SQL plans and operator metrics, the full configuration and classpath |
| Container logs | `containers/<app>/<container>/std{out,err}` | Exceptions and tracebacks, out-of-memory, memory kills and exit codes, lost executors, access errors, Kerberos, metastore and HBase lines, HBase table use and retries, missing classes, the jars YARN localized |
| Step logs | `steps/<step>/{controller,stderr}` | The `spark-submit` command (redacted), the application it submitted, the step's status, YARN's final report |
| Node logs | `node/<instance>/…` | NodeManager and ResourceManager lines for the application's containers, each node's YARN capacity (from its registration line, else from the lines placing the application's containers on it: memory in use plus memory left), bootstrap actions. A node that ran part of the application with neither line in the logs read is named under the node chart, never left out silently |
| HBase server logs | `node/<instance>/applications/hbase/hbase-hbase-{master,regionserver}-<host>.log[.<yyyy-mm-dd-hh>].gz`, on the nodes up during the run, only for the application's hours: its time is from the event log, or else from its container logs (for an HBase cluster of its own too). With neither, none is read, since every hour the cluster ever logged would qualify. Hours outside the run are counted in the Sources row, not listed one by one | Region moves and splits, a lost or stopped region server, writes refused, scanner leases dropped, flushes, compactions, pauses, slow calls |
| EMR API | `DescribeCluster`, `ListSteps`, `ListInstances`, `ListInstanceGroups`/`Fleets`, `DescribeStep`, `DescribeSecurityConfiguration` | Release, applications, log URI, configuration, nodes with type, group and market, roles, security posture |
| EC2 API | `DescribeInstanceTypes` | vCPU and memory per instance type |
| CloudWatch | `AWS/ElasticMapReduce`, `AWS/EC2`, `CWAgent` | Containers pending and allocated, YARN memory free, other applications running, node CPU; node memory and disk only with the CloudWatch agent |
| CloudTrail | `LookupEvents` by each node's instance ID | AWS calls made from the application's nodes and every refusal |

**Joins.** Container logs join executors (and the driver) through the `CONTAINER_ID` attribute YARN gives Spark; hosts join EMR instances by short host name (`ip-10-0-2-10.ec2.internal` matches `ip-10-0-2-10.us-east-1.compute.internal`); steps join the application through the ID in the step's `stderr`; HBase server events join the HBase stage running at the time.

**Verified behaviour.** Checked on real EMR 7.3.0 clusters and fixtures, and relied on by the rules.

- *Spark event log.* Spark 3.5 does not log the driver's exit code, so status comes from the end event, the last job, the driver's `Final app status` line and YARN's summary; Spark closes the log normally even when user code failed outside any job. Cached data sizes need `spark.eventLog.logBlockUpdates.enabled`, process RSS `spark.executor.processTreeMetrics.enabled`, per-stage executor peaks `spark.eventLog.logStageExecutorMetrics`. The driver's heartbeat samples carry stage -1. Speculative, unschedulable and miscellaneous-process events never reach the log. Task times are stamped by the driver, so running tasks can briefly exceed task slots. PySpark records a code location for some actions only (`collect()`, not `count()` or `write`). Spark stores "average" SQL metrics per task as ten times the value. Library versions come only from jar names (the classpath, and jars shipped with `--jars` in `spark.jars`/`spark.yarn.dist.jars`).
- *Event log on S3.* Spark refuses an S3 `spark.eventLog.dir` with no objects under it (SparkContext never starts, exit 13). A log appears only when Spark closes it: `.inprogress` when the application died, nothing when it was killed with its cluster.
- *EMR and YARN.* `LogUri` uses `s3n://`, and terminated clusters still describe. EMR copies logs to S3 every few minutes while a cluster runs (the ResourceManager's current log about 15 minutes late) and all of them when it ends. Log times carry no zone and are UTC. EMR sets the executor overhead factor to 0.1875 without it reaching the event log; the driver's launch line gives the real size. The capacity scheduler places containers by memory alone. Exit codes: the application master's 10 (uncaught exception), 11 (too many executor failures), 13 (SparkContext never started), 15 (user class threw); YARN's negative codes (-104 physical memory, -102 preempted); Spark's executor codes 50–56 (52 out of memory); 143 at every normal executor finish. A heap out of memory also ends in 137: Spark's `-XX:OnOutOfMemoryError="kill -9 %p"` runs after HotSpot prints `# java.lang.OutOfMemoryError` to stdout. The NodeManager checks memory every 3 s, so short spikes pass. The primary node's `master.log` says `bootstrap action 1 failed` on every cluster; only `BOOTSTRAP_FAILURE` in the cluster's state means it. `spark.task.maxFailures=2` clashes with EMR's `excludeOnFailure` default and stops SparkContext. EMR names a spot reclaim in the instance's state reason.
- *CloudWatch and CloudTrail.* EMR publishes cluster metrics every minute; EC2 CPU is every 5 minutes without detailed monitoring, each point stamped with the start of its period; retention is 15 days for 1-minute data and 63 days for 5-minute data. Test clusters had no CloudWatch agent. CloudTrail names an instance profile's session by instance ID, never returns S3 object calls (data events), keeps 90 days, and did not show a refused `AssumeRole` of a role that does not exist.
- *HBase 2.4 on EMR 7.3.* EMR 7 does not ship the hbase-spark connector; connector 1.0.1 needs an SLF4J 1.7 binding and HBase's client needs protobuf 2.5 (`/usr/lib/hadoop/lib/`), which Spark 3.5 lacks. The quorum usually comes from an `hbase-site.xml` shipped with `--files`, so only the ZooKeeper client's `connectString` line shows it. The connector opens about one ZooKeeper connection per task. The client retries expired scanner leases, busy regions and moved regions itself, logging busy retries at INFO (`AsyncRequestFutureImpl … attempt=n/m`); it logged nothing when a region server stopped under a running job, which only the Master's `ServerCrashProcedure` and the region server's `STOPPED:` lines show. The Master writes an empty `ERROR master.ServerManager:` line with every region move. `TableInputFormat` gives no splits for a table whose rows are all unflushed. The event log names only the connector's tables; `TableInputFormat` and `TableOutputFormat` tables and regions are only in the executors' logs.

## 4. Architecture

A pipeline: resolve (cluster, application, window) → access check → collect (S3, local folder, AWS APIs) → parse (event log, logs) → join → analyse → render.

| Package | Responsibility |
| --- | --- |
| `cmd/sparkplain` | Flags, config file, access check, console output, exit codes |
| `internal/sparkplain/source` | `Store` interface with S3 and local-folder implementations: listing, sampling, bounded concurrent fetch, decompression, error classes |
| `internal/sparkplain/eventlog` | Streaming event log decoder (codecs, rolling folders, zips); dispatch on `Event`; per-stage aggregates; the explorer's samples; SQL plans; a field inventory of every Spark 3.5 event field, used or set aside with a reason |
| `internal/sparkplain/yarnlog` | Classifies container, step, node and HBase server logs in one streaming pass per file, folding repeats (up to 500 distinct lines per file) |
| `internal/sparkplain/awsmeta` | EMR, EC2, CloudWatch and CloudTrail clients and their readers; `awsfake` replays recorded API answers in tests |
| `internal/sparkplain/model` | The report's types, serialised as the JSON |
| `internal/sparkplain/analyze` | One analyser per section; the findings rules |
| `internal/sparkplain/report` | `report.html` (templates and inline SVG built in Go), `explorer.html` (data embedded, charts drawn by the embedded D3), the JSON writer |
| `internal/sparkplain/redact` | Secret redaction and text sanitising, applied before values enter the model |

**Design rules**

- **Stream, never load whole logs.** Task events fold into per-stage aggregates and fixed-size samples. Budget: 60 s and 1 GB of memory per GB of event log, checked in CI (`make bench`).
- **Provenance.** Every value records its source file and line (or API call); every finding cites its evidence and, where it applies, the stage, job, executor or node it concerns.
- **Degrade, don't fail.** A missing, refused or unreadable source is a Sources row with its reason; its sections say what they lack; the run exits 3.
- **Secrets never reach the output.** Keys matching password, secret, token, key or credential are redacted in configuration and log lines; free text loses matching `key=value` pairs, URL passwords and AWS access key IDs; source code loses every string on a line naming a secret, except the key. Tests plant fake secrets and assert they never appear. Some harmless Hadoop settings with "key" in their name are hidden too, deliberately.
- **Tests never call real AWS.** Clients sit behind interfaces; the test fakes fail if any client is left unstubbed.

## 5. What the report shows

| Section | Answers | Main sources |
| --- | --- | --- |
| What happened | Two to four plain sentences: outcome, scale, data moved, what needs attention | All |
| The run at a glance | A diagram of the run at its busiest: the cluster, what YARN offered and held, a key naming each colour and mark it uses, each node with its containers to scale and labelled with their size (unused space hatched), each executor's cores, container size and peak heap, one executor and the driver region by region; findings pinned as numbered badges on the part they concern | Event log, logs, EMR, CloudWatch |
| Findings | Every finding, most severe first, each with explanation, evidence and fix | All |
| Timeline | Jobs and stages over time, executors alive, periods with no job running | Event log |
| Cluster and nodes | Every node up during the run: type, size, market, group, lifetime, what it offered YARN, its executors, CPU; nodes that ran nothing | Event log, EMR, EC2, CloudWatch, logs |
| Executors | Count over time, size, host, lifetime, removal reason, failed tasks, peak memory | Event log, container logs |
| Memory | Configured against peak heap, off-heap and RSS; storage memory; spill; GC; out-of-memory and memory kills | Event log, logs |
| CPU | CPU time against run time per stage and executor; where task time went; node CPU | Event log, CloudWatch |
| Storage and I/O | Data read, written and shuffled per stage and over time; tables and paths from SQL plans; cached data; the HBase block (below) | Event log, logs |
| Jobs, stages, SQL | Stages with skew and spill; the chain that held up completion; queries with plans and tables | Event log |
| Configuration | Runtime versions and locations; key settings explained; every setting grouped, marked when the cluster's configuration or `spark-submit` set it | Event log, EMR, step logs |
| Identity and access | Spark, YARN and Kerberos users, queue; instance profile, service and runtime roles; security posture; Hive and HBase connections; AWS calls and refusals | Event log, logs, EMR, CloudTrail |
| Sources | Every source and file read or skipped, and why; what could not be read for lack of access, with the permission it needs | All |

**HBase block** (Storage and I/O, when the run used HBase): tables read and written and how (hbase-spark connector from SQL plans; `TableInputFormat`/`TableOutputFormat` from executors' logs); regions read per region server; the ZooKeeper quorum and connections opened (in all, and the most by one process); client and connector versions; the stages that used HBase with their time, CPU share, the client retries logged while they ran and the server events that hit them; regions read on their own node; and what the HBase servers logged during the run, marked when it concerns this run (one of its tables, a scan from one of its executors, or a whole region server), since those logs are the whole cluster's.

**Findings.** Each has a severity (critical, warning, info), a title, a plain explanation, evidence and a fix. Thresholds are in the config file (§6).

| Area | Rules |
| --- | --- |
| Failures | `log-first-failure` (the earliest error across all logs, with the user's code line), `job-failed`, `step-failed`, `app-retried`, `stage-retried`, `task-retries`, `tasks-running-at-end`, `bootstrap-failed`, `classpath-clash` (the missing class, the jar that needed it, where EMR keeps the usual provider) |
| Memory | `out-of-memory`, `executor-memory-kill`, `memory-spill`, `memory-gc-pressure`, `memory-heap-near-limit`, `memory-over-provisioned`, `host-memory-pressure` |
| Executors and nodes | `executor-lost`, `executor-decommissioned`, `executors-excluded`, `spot-interrupted`, `executor-fit` (executors that never fit, and a size that would), `idle-nodes`, `slow-executor-startup`, `waited-for-capacity`, `shared-cluster` |
| Time and CPU | `stage-skew` (confirmed by rows or bytes read), `cpu-low`, `cpu-idle-executors`, `driver-gaps`, `scheduler-delay`, `poor-locality`, `speculation`, `host-cpu-saturated`, `large-results` |
| Settings and access | `access-denied` (logs and CloudTrail), `access-static-keys`, `config-unlimited-result`, `config-dynalloc-no-shuffle`, `config-aqe-off`, `kerberos-failure`, `metastore-failure` |
| HBase failures | `hbase-table-missing`, `hbase-zookeeper` (the address and port tried), `hbase-server`, `hbase-retries`, `hbase-error`, `hbase-access-denied` |
| HBase slowness | `hbase-time` (HBase stages took most of the run; warning when their tasks mostly waited), `hbase-busy` (writes refused, with the server's own counts), `hbase-scanner-expired`, `hbase-region-moved`, `hbase-regions-changed`, `hbase-server-lost`, `hbase-server-pause`, `hbase-slow-calls`, `hbase-zk-connections`, `hbase-hotspot`, `hbase-remote-regions` |

Rules that judge CPU, GC and memory size skip runs with less than `min-run-time` of task time. Every rule has a test (`TestEveryRuleHasATest`).

## 6. Outputs and CLI

**Outputs.** `<app-id>-report.html`, `<app-id>-report.json` and `<app-id>-explorer.html` in `~/sparkplain/<yyyy-mm-dd>/<app-id>/` unless `-out` is given. Files are 0600 and new folders 0700, because reports carry user names, hosts and log lines. Both pages are single self-contained files that make no network requests, in light and dark themes; `report.html` uses system fonts and inline SVG. Linux and macOS only (outputs are replaced with a POSIX rename).

**Flags**

| Flag | Purpose |
| --- | --- |
| `-app-id` | The application (required) |
| `-profile`, `-region`, `-cluster-id`, `-cluster-name` | Online runs. `-cluster-name` (or `cluster-name` in the config file): of the clusters with that name, the one up when the application's YARN started, the time in its ID (`application_<ms>_<n>`), whether running or ended since; of several up then, the last created before it, unless another was created within ten minutes of it; with no time in the ID, the one running now, else the newest. Anything else stops with the clusters of that name listed (ID, state, created, ended). The access check prints which cluster the name found and why |
| `-init-config` | Write a starter config file, every key explained (`cmd/sparkplain/config.example.yaml`, embedded), to the default path or `-config`, never over an existing file, and exit. A test reads it with the strict reader, as written and with every setting uncommented, and checks the thresholds and explorer limits it lists are the defaults |
| `-env` | An environment in the config file, such as `prod` or `nonprod`: its keys (`cluster-name`, `hbase-cluster-name`, `profile`, `region`, `eventlog-prefix`, `timezone`, `out`) override the top-level ones, and flags override both, so `-app-id` and `-env` are enough. An unknown name stops the run and names the environments there are. With `-env`, the environment's cluster is read even when `-eventlog` is given |
| `-hbase-cluster-id`, `-hbase-cluster-name` | HBase on a separate EMR cluster (by name also `hbase-cluster-name` in the config file): the cluster of that name up when the application's YARN started, else the one running now. When none of its nodes has a Master or region server log, the Sources row names the cluster and how it was chosen, how many nodes' `applications/hbase/` were listed, and what files were there instead |
| `-eventlog` | The event log (§2) |
| `-from` | An offline copy of the cluster's logs; not with `-cluster-id` |
| `-out`, `-format`, `-config` | Output folder; `html`, `json`, `explorer` (default all; `both` means `html,json`); config file |
| `-no-cloudwatch`, `-no-cloudtrail`, `-window-pad` | Skip enrichment; padding on AWS queries (5 min) |
| `-workers`, `-max-size`, `-max-unpacked`, `-overall-timeout` | Fetch budgets (§2) |
| `-check` | Run only the access check and exit |
| `-show file:line` | Print, redacted, the event behind any value the pages cite, and exit |
| `-source` | The application's code (repeatable, redacted), shown beside the jobs and stages that ran each line |

**Exit codes:** 0 complete; 2 fatal (usage mistakes, a cluster or file that does not exist, an `-app-id` that does not match, output that cannot be written); 3 partial (a source missing, unreadable or refused); 130 interrupted.

**No access is not fatal.** A refused permission or bad credentials leave the rest of the run intact: the source is printed on stderr with the permission it needs, listed at the top of both pages, recorded in `report.json` (`accessGaps`) and marked `accessDenied` in the Sources panel; the run exits 3. Without `DescribeCluster`, an online run still reads an event log given to it. A missing `-profile`, a malformed application ID and a cluster that does not exist still exit 2.

**IAM permissions:** `s3:ListBucket` and `s3:GetObject` on the log and event log buckets (`kms:Decrypt` for SSE-KMS); `elasticmapreduce:ListClusters`, `DescribeCluster`, `ListInstances`, `ListInstanceGroups`, `ListInstanceFleets`, `ListSteps`; optional `elasticmapreduce:DescribeStep`, `DescribeSecurityConfiguration`, `ec2:DescribeInstanceTypes`, `cloudwatch:ListMetrics`, `cloudwatch:GetMetricData`, `cloudtrail:LookupEvents`.

**Console.** On stdout: a title line (`◆ sparkplain <version> · <app-id>`); the cluster, the credentials' principal, profile and region, and the log folder, once; `▸ Access check`, one line per source with its location relative to the log folder, and for a failure its reason and a `try:` command on one line; `▸ Read` (on a terminal, on stderr as each source is read), one line per source with what was read in a few words (`SourceStatus.Brief`), and under the event log the file actually read (`from <file>`, `from <zip> › <log inside>`, or a rolling folder with its part count), since a folder, a prefix or a zip names only where to look; the notes on which of several candidates was taken come before it; leaving out the EMR and EC2 APIs when the check already showed them readable; `▸ What happened`; `▸ Findings` with counts, one line each; `▸ Written`, the folder and the files; a command to open the report (`xdg-open`, or `open` on macOS); and `Done in … · complete` or `partial` with the exit code. Source names share one column in both lists. Marks are dots: ● read (green), ● refused or failed (red), ◐ partial or empty (amber), ● needed but not given to this run (pink: AWS on an offline run, no cluster logs, no event log or one on HDFS; `AccessCheck.NotGiven`, and a `not-supplied` source), ○ turned off or not needed (grey); findings are dots coloured by severity (red, amber, blue). In colour, numbers and their units (`527`, `1.7 MiB`, `39 s`, `17×`, `86%`) are cyan, while digits inside names and paths (`application_…_0042`, `ip-10-0-2-13`, `/out/001`) are left alone. On a colour terminal the console is animated, saying the same text: the status line spins with a running clock, each source's mark settles into its dot, findings' dots grow in, the title's diamond fills in, sections come a second apart and a rule sweeps across before the last line (about 5 s in all). A CI environment or `SPARKPLAIN_NO_ANIMATION=1` keeps it still, as does anything that turns colour off. Piped, or with `NO_COLOR` or `TERM=dumb`, there is no colour and the marks are Y, N, !, ? and - (findings !!, ! and -), so logs and scripts stay plain; notes keep their `sparkplain:` prefix on stderr. Symbols are single-width, so columns line up. Lines wrap at `COLUMNS` (80, at most 120); a path too long to share its line gets one of its own, and `try:` commands are never wrapped.

**Report page.** Sections as in §5. Every chart and graph has a title and, under it, the same guide in the report and the explorer: what each axis or mark stands for, one labelled line each (Across, Up, or Rows, Bar length, Colour, Boxes for charts that are not x against y); how to read it, as bullets when there is more than one point; "In this run", worked out from the analysis (`runNotes`): what this run's chart shows (the longest stage, the worst straggler that cost at least a second, the stage that lost most time to anything but computing, a node that ran only the driver and why, idle nodes, slot use, driver gaps, spill, peak heap), each finding the chart is evidence for linked by number, or "Nothing unusual here"; and fine print such as sampling or what was left out (a test enforces it); every metric has a one-line explanation and shows used against available; every section shows its coverage (complete, partial, needs the event log) and what is missing. Findings read in three bands: what went wrong, the evidence, what to try. Tables scroll in their own box with pinned headers. Times show in the config file's `timezone` (default the local zone), labelled, and the page relabels them in the viewer's zone; JSON times are UTC.

**Explorer page.** A per-application take on the History Server's tabs, linked both ways with the report. Tabs: Overview (what happened, findings, the chain that held up completion, what the run used, stages worth a look, charts over time), At a glance, Timeline (queries, jobs, stages, executors, tasks against slots and node CPU on one zoomable axis), Jobs, Stages (health map, duration, data, time, spill and skew views; per stage: quartiles, histogram, task scatter with selectable axes and colour, slowest tasks, per executor, operation graph, code), Executors (stage × executor heatmap first), SQL / DataFrame (plan graph with operator metrics), Cluster (online runs), and under More: Storage, Code, Environment, Event log, Logs. Charts are drawn by the embedded D3 7.9.0 (pinned by hash) through a small kit in `explorer.js`; a chart that cannot be drawn says why and the rest works. Graphs are laid out in Go. Code is highlighted by a small built-in highlighter that builds text, never HTML. Embedded JSON is escaped so no value can close the script tag.

Explorer data is collected while streaming, within the memory budget: per-stage quartiles and log-scale histograms; per stage the 100 slowest tasks and a uniform sample of 1,000 (fixed seed), 100,000 sampled tasks at most in the app (halved by subsampling past it; charts drawn from samples say so); stage × executor totals (1,000,000 cells); running tasks in time buckets (2,000); SQL operator metrics; log lines (200 per file, 5,000 in all). The page stays under 25 MB at the defaults. This data is only in `explorer.html`; `report.json` keeps everything else.

**JSON.** Mirrors the model package, with a `schemaVersion`.

**Config file** (`-config`, default `~/.config/sparkplain/config.yaml` on macOS and Linux, or under `$XDG_CONFIG_HOME` when set, `%AppData%` on Windows; flags win; unknown keys are rejected):

```yaml
cluster-name: nightly-etl        # as -cluster-name; not for a run given -eventlog or -from and no cluster
hbase-cluster-name: hbase-prod   # as -hbase-cluster-name
profile: prod-emr                # as -profile and -region
region: us-east-1
environments:                    # picked with -env; each may set cluster-name, hbase-cluster-name, profile, region, eventlog-prefix, timezone and out
  prod: {cluster-name: nightly-etl, profile: prod-emr}
  nonprod: {cluster-name: nightly-etl-dev, profile: dev-emr}
eventlog-prefix: s3://my-logs/spark-events/   # used when -eventlog is not given
timezone: Australia/Sydney
out: ~/reports
format: html,json,explorer
max-size: 10GiB
max-unpacked: 50GiB
overall-timeout: 30m
thresholds:
  skew-ratio: 5             # slowest task over this × the stage median ...
  skew-min-task: 1s         # ... when it took at least this ...
  skew-min-tasks: 5         # ... in a stage with at least this many tasks
  spill-share: 0.10         # disk spill over this share of shuffle write
  gc-share: 0.10            # GC over this share of executor run time
  low-cpu-share: 0.30       # CPU time under this share of run time
  memory-used-share: 0.40   # peak heap under this share of the heap given
  min-run-time: 1m          # CPU, GC and memory rules skip less task time
  sched-delay-share: 0.20   # scheduler delay over this share of task time
  locality-any-share: 0.30  # input tasks (or HBase regions) away from their data over this share
  result-share: 0.50        # a stage's results over this share of spark.driver.maxResultSize
  slow-startup: 1m          # executors taking longer than this to register
  driver-gap-share: 0.25    # no job running for over this share of the run ...
  driver-gap-min: 1m        # ... and at least this long in all
  hbase-time-share: 0.50    # stages using HBase over this share of the run
  hbase-connections: 50     # ZooKeeper connections one process opened over this
  hbase-hotspot-share: 0.75 # one region server holding over this share of a table's regions read
explorer:
  slowest-per-stage: 100
  sample-per-stage: 1000
  max-sampled-tasks: 100000
  max-stage-executor-cells: 1000000
```

## 7. Limitations

The report states these plainly rather than hiding them.

| Limit | Effect |
| --- | --- |
| The event log is on HDFS by default and dies with the cluster | Most sections need it; the run explains how to supply it and works from the logs alone |
| EMR copies logs to S3 only every few minutes while a cluster runs | A recent run can lack logs; the access check and Sources say so |
| Executor memory is sampled (heartbeats and stage peaks) | Short spikes are missed; charts say "peak" |
| Host memory and disk need the CloudWatch agent | Shown only when present |
| Shared clusters run several applications at once | Node and HBase server figures are cluster-wide and marked as such |
| CloudTrail covers management events for 90 days | No S3 object calls; no calls under a step's runtime role |
| Newer Spark releases may add event fields | Unknown fields and events are counted and shown, not lost |
| Clocks differ between nodes and services; log times have whole seconds | The event log is the reference; log lines go to the stage that started last before them |
| The HBase client logs nothing for some problems | HBase's server logs fill the gap; region moves under a running job, region server timeouts, pauses, slow calls, aborts and HBase access refusals were never seen on a real run and are tested with HBase 2.4's own messages |

## 8. Delivery

All five phases are built. Plans, step notes and live checks are in [HISTORY.md](HISTORY.md), which is where new plans go before they are built; this spec is then updated to match.

| Phase | Delivered |
| --- | --- |
| 1. Event log core | The report from the event log alone; the 1 GB budget |
| 1b, 1c, 1d, 1e, 1f | The explorer; every event field used or set aside; offline charts; diagnostic views; curation |
| 2. Online mode | Fetching from S3 and the EMR API; container, step and node logs; the first failure |
| 3. AWS enrichment | Instances, CloudWatch, CloudTrail, security configuration |
| 4. Findings and polish | Every rule tested; the CI benchmark; recorded AWS fixtures |
| 5. HBase | HBase failures told apart; HBase use and slowness; HBase server logs |
| Access check | What a run can read, before it reads it; `-check` |

Testing: stubbed S3 and AWS clients, recorded AWS answers replayed from `testdata/aws/`, scrubbed fixtures from real EMR runs, planted secrets, the race detector on the packages that start goroutines (`RACE_PKG`), fuzz targets for the decoder and classifiers.

## 9. Open questions

- [ ] Where is the S3 copy of the event logs the History Server reads, and can your profile read it?
- [ ] Is EMR on EC2 the only target?
- [ ] Are Lake Formation or Ranger enabled? (Known: production HBase runs on the same, Kerberized, EMR cluster as Spark.)
- [ ] Does a CloudTrail trail record S3 data events for the log and data buckets?
- [ ] Is the CloudWatch agent configured on your clusters?
- [ ] Who reads the reports: just you, or shared with a team?

## Sources

- [Amazon EMR 7.0.0 release notes](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-700-release.html) (CloudWatch agent added, Ganglia removed)

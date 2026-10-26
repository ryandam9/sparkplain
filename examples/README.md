# Examples: run Spark on your own machine, then read it with sparkplain

These pages show, step by step, how to run a small PySpark job on a laptop or a VM and then turn its logs into a sparkplain report. You do not need AWS or EMR for any of them.

| Page | How Spark runs | What you install | What the report shows |
|---|---|---|---|
| [PySpark from PyPI](pyspark-pypi.md) | `local[N]`: one process; the driver runs every task. Also `local-cluster`, which starts real executors. | `pip install pyspark` | Jobs, stages, tasks, shuffles, memory, findings. With `local-cluster`, also the executors and their logs. |
| [A full Standalone cluster](standalone-cluster.md) | A master and two workers, started with Spark's `sbin` scripts, and executors in their own processes. Client and cluster deploy mode. | The Spark distribution from spark.apache.org | Everything above, plus each worker's cores and memory, how each executor ended, and the At a glance diagram. |

Both pages use the same job, [`jobs/orders.py`](jobs/orders.py). It makes 1 million fake orders, then groups, caches, joins and sorts them, so the report has shuffles, a cached table and a skewed join to show. It reads and writes no files, and it runs in about 15 seconds.

## What you need

- **Java 17.** Spark 3.5 supports Java 8, 11 and 17 (21 also works), and does not start on Java 23 or later. Check with `java -version`. To install Java 17 and make Spark use it, see [step 2 of the Standalone page](standalone-cluster.md#2-install-java-17-and-point-spark-at-it). For PySpark from PyPI, set `JAVA_HOME` to that folder before you run `spark-submit`.
- **Python 3.9 or later**, for PySpark.
- **sparkplain**, built or installed as the [main README](../README.md#install) says. On this page, `sparkplain` means that binary (for example `./bin/sparkplain` after `make build`).

## The three things sparkplain reads from a local run

| Input | Flag | Where Spark writes it | Without it |
|---|---|---|---|
| The event log | `-eventlog` | The folder in `spark.eventLog.dir`. Spark writes it only when `spark.eventLog.enabled=true`. | The report has fewer sections, marked "needs event log". |
| The driver's console | `-driver-log` | Your terminal. Save it with `> driver.txt 2>&1`. | The driver's own log lines are not in the report. |
| The executor logs and worker logs | `-from` | `$SPARK_HOME/work` and `$SPARK_HOME/logs` (Standalone and `local-cluster` only). | Each executor's own log lines, and how each executor ended, are not in the report. |

Every run also needs `-app-id`. The event log's file name is the application ID, for example `local-1792970039387` or `app-20261025231453-0000`.

## Tips

- sparkplain never sends your data anywhere. It reads only the files you give it, and the pages it writes make no network calls when you open them.
- To keep the event log on for every run, put the two settings in `$SPARK_HOME/conf/spark-defaults.conf`. Each page shows how.
- The exit code tells you how complete the report is: `0` complete, `3` partial (a source is missing or unreadable), `2` fatal.

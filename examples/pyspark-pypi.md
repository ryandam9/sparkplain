# Run a job with PySpark from PyPI, then read it with sparkplain

`pip install pyspark` gives you Spark in a Python package: `spark-submit`, `pyspark` and the Spark JARs. It runs Spark in **local mode** (`local[N]`) by default. Local mode is one Java process: the driver runs every task, in N task slots, and there are no executors. sparkplain reads these runs, and its report says so ("in local mode. The driver ran every task, in 4 task slots.").

The same package can also run **`local-cluster` mode**, which starts real executors in their own processes. See [step 6](#6-optional-real-executors-with-local-cluster).

The commands are for macOS or Linux. They were checked with PySpark 3.5.3, Java 21 and Python 3.11.

## 1. Install PySpark in a virtual environment

```sh
python3 -m venv ~/venvs/spark
source ~/venvs/spark/bin/activate
pip install pyspark==3.5.3
spark-submit --version          # prints "version 3.5.3"
```

Keep the environment active (`source …/activate`) for the steps below. `spark-submit` and `pyspark` are on your `PATH` only while it is active.

## 2. Make a folder for the event logs, and name the machine

Spark writes the event log only when you ask for it. sparkplain needs the event log for most of the report.

```sh
mkdir -p /tmp/spark-events
export SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
```

The two variables make Spark use one name for your machine. Without them, the driver can use your host name while the executors use an IP address, and the report then shows two hosts for one machine. They also stop Spark's warning that your host name resolves to a loopback address.

## 3. Run the job with spark-submit

From the root of this repository:

```sh
spark-submit --master "local[4]" \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  examples/jobs/orders.py > /tmp/driver.txt 2>&1
```

- `--master "local[4]"` gives the driver 4 task slots. `local[*]` uses one slot per CPU core.
- `> /tmp/driver.txt 2>&1` saves the driver's console. In local mode the console is the driver's only log, and sparkplain reads it with `-driver-log`.
- The job's own output is in the same file: `grep -v INFO /tmp/driver.txt`.

When it ends, the event log is in `/tmp/spark-events`. Its file name is the application ID (yours is different):

```console
$ ls /tmp/spark-events
local-1792970039387
```

## 4. Run sparkplain

```sh
sparkplain -app-id local-1792970039387 \
  -eventlog /tmp/spark-events/local-1792970039387 \
  -driver-log /tmp/driver.txt \
  -out /tmp/sparkplain-local
```

`-eventlog` can also be the whole folder (`-eventlog /tmp/spark-events`). sparkplain then picks the log that `-app-id` names.

The console ends like this:

```text
▸ What happened
  sparkplain_orders_example ran for 11 s as sparkuser and finished.
  It ran 13 jobs (21 stages, 34 tasks) in local mode. The driver ran every task,
  in 4 task slots. Tasks used 4.6 s of CPU time.
  It read 10.6 MiB, wrote 0 B and moved 27.6 KiB between stages in shuffles.

▸ Findings  1 warning · listed in the report

▸ Written  /tmp/sparkplain-local/
  local-1792970039387-report.html · local-1792970039387-explorer.html ·
  local-1792970039387-report.json

Done in 0.1 s · complete (exit 0)
```

## 5. Open the report

```sh
open /tmp/sparkplain-local/local-1792970039387-report.html       # macOS
xdg-open /tmp/sparkplain-local/local-1792970039387-report.html   # Linux
```

Things to look at for a local-mode run:

- **What happened:** the run in 2 to 4 sentences, as in the console.
- **Headline numbers:** Hosts shows "1, ran the driver", Executors shows "0, local mode", and Task slots shows 4 and the driver's heap (`spark.driver.memory`, 1 GiB by default).
- **Findings:** each finding has a severity, the evidence (an event log line) and a suggested fix. Which findings you get depends on the run. In the run above, the one warning was "Static AWS credentials are set in the Spark configuration": Spark copies `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` from the shell into its configuration. sparkplain never shows their values.
- **The explorer** (`…-explorer.html`): jobs, stages, the task timeline and the stage DAG, with charts. There is no At a glance diagram, as there is no cluster to draw.

## 6. Optional: real executors with local-cluster

`local-cluster[W,C,M]` starts W workers inside the driver's process. Each offers C cores and M MiB of memory and starts executors as their own Java processes. This mode is meant for Spark's own tests, but it is the easiest way to see executors without installing anything more.

```sh
spark-submit --master "local-cluster[2,2,2048]" --executor-memory 1g \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  examples/jobs/orders.py > /tmp/driver-lc.txt 2>&1
```

Keep `SPARK_LOCAL_IP` and `SPARK_LOCAL_HOSTNAME` set from step 2. The application ID now starts with `app-`. The executors write their logs to `work/<app-id>/` under the PySpark package folder. Give that folder to `-from`:

```sh
PYSPARK_HOME=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
ls "$PYSPARK_HOME/work"          # app-20261025231620-0000

sparkplain -app-id app-20261025231620-0000 \
  -eventlog /tmp/spark-events \
  -from "$PYSPARK_HOME" \
  -driver-log /tmp/driver-lc.txt \
  -out /tmp/sparkplain-lc
```

```text
  Y Executor logs          4 files from 2 executors
  ...
▸ What happened
  It ran 13 jobs (21 stages, 22 tasks) on 2 executors across 1 host. Tasks used
  4.0 s of CPU time, and executors held 33 s of core time.
```

The report now has the executors, their logs, and the At a glance diagram with the two workers. Delete `$PYSPARK_HOME/work` when you are done, because Spark does not clean it up.

## Other ways to turn on the event log

**Every run, with a config folder.** The PyPI package has no `conf/` folder, but Spark reads one from `SPARK_CONF_DIR`:

```sh
mkdir -p ~/spark-conf
cat > ~/spark-conf/spark-defaults.conf <<'EOF'
spark.eventLog.enabled  true
spark.eventLog.dir      file:///tmp/spark-events
EOF
export SPARK_CONF_DIR=~/spark-conf      # add this to your shell profile
```

**Inside the script**, when you run it with `python` or in a notebook:

```python
spark = (SparkSession.builder
         .master("local[4]")
         .config("spark.eventLog.enabled", "true")
         .config("spark.eventLog.dir", "file:///tmp/spark-events")
         .getOrCreate())
```

The settings take effect only when they create the session. If a session already runs in the notebook, call `spark.stop()` first.

## spark-submit or python?

Both work, and both write the same event log. The difference is the driver's console:

| You run | Console log level | `-driver-log` gives |
|---|---|---|
| `spark-submit job.py` | INFO | The driver's full log: executors added, tasks lost, warnings. |
| `python job.py`, `pyspark`, or a notebook | WARN (PySpark sets it when it starts the session) | Only warnings and errors. |

When the console is only WARN, leave out `-driver-log`. The report is the same without it, except for the driver's log lines.

## Troubleshooting

- **`spark-submit: command not found`**, or `find_spark_home.py` fails: the virtual environment is not active. Run `source ~/venvs/spark/bin/activate`.
- **`/tmp/spark-events` is empty:** the event log was not turned on, or the folder did not exist when the job started. Spark does not make the folder.
- **A file ends in `.inprogress`:** the application is still running, or it stopped without calling `spark.stop()`. sparkplain reads it, and the report marks the work that was still running when the log ended.
- **The report shows two hosts for one machine:** set `SPARK_LOCAL_IP` and `SPARK_LOCAL_HOSTNAME` as in step 2, then run the job again.
- **Exit code 3:** a source is missing or unreadable. The Sources panel in the report and the console's Read list say which one.

# Run a full Spark Standalone cluster on one machine, then read it with sparkplain

A Spark Standalone cluster is Spark's own cluster manager. It has a **master**, which takes the applications, and one or more **workers**, which start the executors. Each executor is its own Java process, with its own logs. This is the nearest a laptop gets to a real cluster such as EMR: sparkplain reads the executors' logs and the workers' logs, and draws the cluster in the explorer's At a glance tab.

The PyPI package (`pip install pyspark`) does not include the scripts that start a master and a worker. Use the Spark distribution from spark.apache.org.

The commands are for macOS or Linux. They were checked with Spark 3.5.3, Java 21 and Python 3.11.

## 1. Download the Spark distribution

```sh
cd ~
curl -LO https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
curl -LO https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz.sha512
shasum -a 512 spark-3.5.3-bin-hadoop3.tgz    # must be the same as the value in the .sha512 file
cat spark-3.5.3-bin-hadoop3.tgz.sha512
tar -xzf spark-3.5.3-bin-hadoop3.tgz
```

Then set these in each terminal you use (or in your shell profile):

```sh
export SPARK_HOME=~/spark-3.5.3-bin-hadoop3
export PYSPARK_PYTHON=python3
export SPARK_LOCAL_IP=127.0.0.1 SPARK_LOCAL_HOSTNAME=localhost
mkdir -p /tmp/spark-events
```

- `PYSPARK_PYTHON` is the Python that the executors run.
- `SPARK_LOCAL_IP` and `SPARK_LOCAL_HOSTNAME` keep every process on your machine's loopback address, so that the cluster does not listen on your network. They are optional on one machine. Spark's processes then name the machine `localhost` or `127.0.0.1`, and sparkplain shows both names as one host, `localhost`.

## 2. Install Java 17 and point Spark at it

Spark 3.5 runs on Java 8, 11 and 17 (Java 21 also works). It does not start on **Java 23 or later**: Hadoop's code in Spark calls `Subject.getSubject`, which newer Java versions refuse, and `start-master.sh` fails with "failed to launch" and a stack trace that names `Subject.getSubject`. Check your version first:

```sh
java -version
```

If it says 17 (or 8, 11 or 21), go to [step 3](#3-start-the-master-and-a-worker). If not, install Java 17 beside the Java you have. You do not need to remove the other version, because Spark uses the Java that `spark-env.sh` names.

**Install Java 17.** Use one of these:

```sh
# Ubuntu or Debian
sudo apt install openjdk-17-jdk          # installs to /usr/lib/jvm/java-17-openjdk-amd64 (arm64 on ARM)

# Fedora, RHEL or Amazon Linux
sudo dnf install java-17-openjdk-devel   # installs to /usr/lib/jvm/java-17-openjdk

# macOS, with Homebrew
brew install --cask temurin@17           # then: /usr/libexec/java_home -v 17

# Any Linux on x64, without root: Eclipse Temurin from adoptium.net
mkdir -p ~/software && cd ~/software
curl -L -o jdk17.tar.gz "https://api.adoptium.net/v3/binary/latest/17/ga/linux/x64/jdk/hotspot/normal/eclipse"
tar -xzf jdk17.tar.gz                    # makes a folder such as jdk-17.0.13+11
ls -d ~/software/jdk-17*
```

Find the folder that holds `bin/java`, and check it:

```sh
ls /usr/lib/jvm                          # Linux packages
/usr/lib/jvm/java-17-openjdk-amd64/bin/java -version   # must say "17"
```

**Point Spark at it.** Every script in `$SPARK_HOME/sbin` and `$SPARK_HOME/bin` reads `conf/spark-env.sh` when it starts, so the master, the worker, the executors and `spark-submit` all use the Java that it names:

```sh
cp $SPARK_HOME/conf/spark-env.sh.template $SPARK_HOME/conf/spark-env.sh
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> $SPARK_HOME/conf/spark-env.sh
```

Put your own folder in place of `/usr/lib/jvm/java-17-openjdk-amd64`: for example `$(/usr/libexec/java_home -v 17)` on macOS, or `$HOME/software/jdk-17.0.13+11` for the download. Do not include `/bin/java`.

After step 3, the first line of the master's log shows the Java that Spark used:

```sh
head -1 $SPARK_HOME/logs/spark-*-org.apache.spark.deploy.master.Master-1-*.out
# Spark Command: /usr/lib/jvm/java-17-openjdk-amd64/bin/java -cp ...
```

If an earlier start failed, run `$SPARK_HOME/sbin/stop-master.sh` once before you start the master again.

## 3. Start the master and a worker

```sh
$SPARK_HOME/sbin/start-master.sh --host localhost
$SPARK_HOME/sbin/start-worker.sh spark://localhost:7077 --cores 4 --memory 4g
```

The worker offers Spark 4 cores and 4 GiB. Open http://localhost:8080 to see the master's page. It lists the worker as ALIVE.

The two daemons write their logs to `$SPARK_HOME/logs`. The worker's log records what it offers Spark and how each executor ended. sparkplain reads both.

## 4. Submit the job (client deploy mode)

From the root of this repository:

```sh
$SPARK_HOME/bin/spark-submit --master spark://localhost:7077 \
  --executor-cores 1 --executor-memory 1g --total-executor-cores 4 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  examples/jobs/orders.py > /tmp/driver-sa.txt 2>&1
```

- This starts 4 executors of 1 core and 1 GiB heap each.
- In client deploy mode, the driver runs in your terminal, so its log is the console. `> /tmp/driver-sa.txt 2>&1` saves it for `-driver-log`.
- The executors write their logs to `$SPARK_HOME/work/<app-id>/<executor-id>/stderr` and `stdout`.

Find the application ID in the event log folder or the driver's console:

```console
$ ls /tmp/spark-events
app-20261025231453-0000
$ grep "Connected to Spark cluster" /tmp/driver-sa.txt
... StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20261025231453-0000
```

## 5. Run sparkplain

Give `-from` the Spark folder. sparkplain finds the executors' logs in `work/` and the worker and master logs in `logs/`:

```sh
sparkplain -app-id app-20261025231453-0000 \
  -eventlog /tmp/spark-events \
  -from $SPARK_HOME \
  -driver-log /tmp/driver-sa.txt \
  -out /tmp/sparkplain-sa
```

```text
▸ Access check
  Y Executor logs          work/app-20261025231453-0000/
                           Found: a Spark Standalone run's work folder
  ? AWS                    EMR API, CloudWatch and CloudTrail: not used, as a
                           Spark Standalone run has no EMR cluster.
  Y Spark event log        /tmp/spark-events
  Y Output folder          /tmp/sparkplain-sa  Will be created

▸ Read
  Y Spark event log        142 events, 619 KiB
  Y Executor logs          8 files from 4 executors
  Y Worker and master logs 2 daemon logs
  Y Driver log             the driver's console
  ...

▸ What happened
  sparkplain_orders_example ran for 26 s as sparkuser and finished.
  It ran 13 jobs (21 stages, 22 tasks) on 4 executors across 1 host. Tasks used
  6.2 s of CPU time, and executors held 1 min 2 s of core time.
  It read 10.5 MiB, wrote 0 B and moved 13.9 KiB between executors in shuffles.

▸ Written  /tmp/sparkplain-sa/
  app-20261025231453-0000-report.html · app-20261025231453-0000-explorer.html ·
  app-20261025231453-0000-report.json

Done in 0.6 s · complete (exit 0)
```

## 6. Open the report and the explorer

```sh
open /tmp/sparkplain-sa/app-20261025231453-0000-report.html      # macOS (xdg-open on Linux)
open /tmp/sparkplain-sa/app-20261025231453-0000-explorer.html
```

Things to look at for a Standalone run:

- **Headline numbers:** 4 executors on 1 host. "Executors held … of core time" against "Tasks used … of CPU time" shows how busy the executors were.
- **Memory:** each executor's memory is its heap (`spark.executor.memory`). Standalone adds no overhead to it, as YARN does.
- **Explorer, At a glance:** the master, a "Worker · localhost" box with what the worker offered (4 cores and 4.0 GiB), the 4 executors in it, and the driver. It also says if there was room for more executors at the busiest time.
- **Explorer, Executors and Logs:** each executor's log lines, and how each one ended, from the worker's log.
- **Explorer, Replay:** the run, step by step, from the logs.

## 7. Optional: cluster deploy mode

In cluster deploy mode, the worker starts the driver too, in `$SPARK_HOME/work/driver-<id>/`. PySpark cannot run in cluster deploy mode on a Standalone cluster, so this example uses SparkPi, a Scala example that comes with Spark:

```sh
OPENS="--add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED \
--add-opens=java.base/java.lang.invoke=ALL-UNNAMED"

$SPARK_HOME/bin/spark-submit --master spark://localhost:7077 --deploy-mode cluster \
  --class org.apache.spark.examples.SparkPi \
  --conf "spark.driver.extraJavaOptions=$OPENS" \
  --executor-cores 1 --executor-memory 1g --total-executor-cores 3 \
  --conf spark.eventLog.enabled=true \
  --conf spark.eventLog.dir=file:///tmp/spark-events \
  $SPARK_HOME/examples/jars/spark-examples_2.12-3.5.3.jar 100
```

- On Java 17 or later, the `--add-opens` options are necessary. spark-submit gives them to a client-mode driver, but not to a driver that a worker starts.
- `spark-submit` returns when the driver starts. Wait until the master's page (http://localhost:8080) shows the driver as FINISHED.
- The driver's log is in the worker's folder, so you do not need `-driver-log`:

```sh
ls $SPARK_HOME/work               # app-20261025231917-0000  driver-20261025231914-0000
sparkplain -app-id app-20261025231917-0000 -eventlog /tmp/spark-events \
  -from $SPARK_HOME -out /tmp/sparkplain-cm
```

```text
  Y Executor logs          8 files from 3 executors and the driver
  Y Worker and master logs 2 daemon logs
  ...
  It ran 1 job (1 stage, 100 tasks) on 3 executors across 1 host.
```

At a glance now draws the driver inside the worker, with the core and heap it took from the worker.

## 8. Stop the cluster and clean up

```sh
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh
rm -rf $SPARK_HOME/work           # executor and driver logs; Spark does not delete them
```

Keep `$SPARK_HOME/logs` if you want to read older runs again. sparkplain reads only the lines that name the application you ask for.

## Keep the event log on for every run

```sh
cat >> $SPARK_HOME/conf/spark-defaults.conf <<'EOF'
spark.eventLog.enabled  true
spark.eventLog.dir      file:///tmp/spark-events
EOF
```

You can then leave out the two `--conf spark.eventLog…` lines. To browse the same logs in Spark's own History Server, add `spark.history.fs.logDirectory file:///tmp/spark-events` to the file, run `$SPARK_HOME/sbin/start-history-server.sh` and open http://localhost:18080.

## More than one worker, or more than one machine

- **Two workers on one machine:** set `export SPARK_WORKER_INSTANCES=2`, then run `start-worker.sh` once. It starts both workers, and `--cores` and `--memory` are for each worker. The report adds up what the workers on a host offered.
- **Workers on other machines:** start each one with `start-worker.sh spark://<master-host>:7077`, and leave out `SPARK_LOCAL_HOSTNAME`. Copy each machine's `work/` and `logs/` folders into one folder per machine (for example `logs-from/node1/`, `logs-from/node2/`), and give the parent folder to `-from`.

## Troubleshooting

- **The application waits with "Initial job has not accepted any resources":** the worker is not ALIVE on http://localhost:8080, or it has fewer free cores or less memory than the executors ask for.
- **Port 8080 or 7077 is in use:** start the master with `--webui-port 8090` or `--port 7078`, and use the same port in `spark://localhost:<port>`.
- **`start-master.sh` says "failed to launch", and the log names `Subject.getSubject`:** Spark runs on Java 23 or later. Set `JAVA_HOME` to Java 17 in `spark-env.sh`, as in [step 2](#2-install-java-17-and-point-spark-at-it).
- **The cluster-mode driver fails with `IllegalAccessError`:** the `--add-opens` options are missing (Java 17 or later).
- **"Capacity not known (needs the worker's log)" in At a glance:** sparkplain did not find the worker's log. Check that `-from` is the folder that holds `logs/`, or that `SPARK_LOG_DIR` was not changed.
- **Exit code 3:** a source is missing or unreadable. The console's Read list and the report's Sources panel say which one.

## Scripts that do all of this

The scripts that made sparkplain's own test fixtures run the same steps, with every log in one output folder: [`run-standalone.sh`](../testdata/standalonescripts/run-standalone.sh) (client mode), [`run-cluster-mode.sh`](../testdata/standalonescripts/run-cluster-mode.sh), [`run-local-cluster.sh`](../testdata/standalonescripts/run-local-cluster.sh) and [`run-lost-executors.sh`](../testdata/standalonescripts/run-lost-executors.sh) (executors killed during the run).

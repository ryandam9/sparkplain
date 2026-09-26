"""Scrub a raw PySpark event log and write the fixture variants.

Usage: scrub.py <raw log file or eventlog_v2 dir> <new app id> <out dir> <kind>
  kind: single   -> <out>/<appId>, .lz4, .zstd, .snappy, and a History Server style .zip
        plainonly-> <out>/<appId> only
        rolling  -> <out>/eventlog_v2_<appId>/events_<n>_<appId>.zstd + appstatus marker
        inprogress -> <out>/<appId>.inprogress, cut in the middle of a line, no ApplicationEnd

Scrubbing rewrites every string in every event:
  * local paths (scratch dirs, the repo checkout, the venv, /root) become neutral EMR-like paths
  * the loopback test IP becomes EMR-style private DNS names, one per local-cluster worker
  * the user becomes "hadoop" and the app ID becomes a YARN-style application ID
It then fails if any known local marker is left. Compressed variants are written
with Spark's own CompressionCodec through py4j, so lz4 and snappy use the Java
stream formats (LZ4BlockOutputStream, SnappyOutputStream) exactly as Spark does.
"""
import json
import os
import re
import sys
import zipfile

src, new_app, out, kind = sys.argv[1:5]
SCRATCH = os.environ["SP_SCRATCH"]
REPO = os.environ["SP_REPO"]
IP = "192.0.2.2"
DRIVER_HOST = "ip-10-0-1-10.ec2.internal"
WORKER_HOSTS = ["ip-10-0-1-23.ec2.internal", "ip-10-0-1-37.ec2.internal"]
FORBIDDEN = ["claude", "scratchpad", "/root", "192.0.2.", "/home/user", SCRATCH]

if os.path.isdir(src):
    parts = sorted((f for f in os.listdir(src) if f.startswith("events_")), key=lambda f: int(f.split("_")[1]))
    files = [os.path.join(src, f) for f in parts]
else:
    files = [src]

lines = []  # (part index, line)
for i, f in enumerate(files):
    with open(f, encoding="utf-8") as fh:
        for l in fh:
            lines.append((i, l.rstrip("\n")))

old_app = None
exec_port = {}
for _, l in lines:
    e = json.loads(l)
    if e["Event"] == "SparkListenerApplicationStart":
        old_app = e["App ID"]
    if e["Event"] == "SparkListenerExecutorAdded":
        url = e["Executor Info"]["Log Urls"]["stderr"]
        exec_port[e["Executor ID"]] = int(re.search(r":(\d+)/", url).group(1))
ports = sorted(set(exec_port.values()))
port_host = {p: WORKER_HOSTS[i % len(WORKER_HOSTS)] for i, p in enumerate(ports)}
exec_host = {x: port_host[p] for x, p in exec_port.items()}

venv_pyspark = [os.path.join(SCRATCH, "venv", "lib", d, "site-packages", "pyspark")
                for d in os.listdir(os.path.join(SCRATCH, "venv", "lib"))]
PATHS = [(p, "/usr/lib/spark") for p in venv_pyspark] + [
    (os.path.join(SCRATCH, "venv"), "/usr/lib/spark/venv"),
    (os.path.join(SCRATCH, "run", "work"), "/mnt/fixture"),
    (os.path.join(SCRATCH, "run", "ev"), "/var/log/spark/apps"),
    (SCRATCH, "/mnt/tmp"),
    (os.path.join(REPO, "scripts", "fixtures"), "/home/hadoop/jobs"),
    (REPO, "/home/hadoop/src"),
    ("/tmp/claude-0", "/mnt/tmp"),
    ("/root", "/home/hadoop"),
]


def fix_str(s):
    for a, b in PATHS:
        s = s.replace(a, b)
    s = s.replace(old_app, new_app)
    s = re.sub(r"\(192\.0\.2\.2 executor (\w+)\)", lambda m: "(%s executor %s)" % (exec_host.get(m.group(1), DRIVER_HOST), m.group(1)), s)
    s = re.sub(r"192\.0\.2\.2:(\d+)", lambda m: "%s:%s" % (port_host.get(int(m.group(1)), DRIVER_HOST), m.group(1)), s)
    return s.replace(IP, DRIVER_HOST)


def walk(v, execid=None):
    if isinstance(v, dict):
        eid = v.get("Executor ID", execid)
        out = {}
        for k, x in v.items():
            if k == "Host" and isinstance(x, str) and eid is not None:
                out[k] = exec_host.get(eid, DRIVER_HOST)
            elif k == "User" or (k == "user.name" and x == "root"):
                out[k] = "hadoop"
            else:
                out[fix_str(k)] = walk(x, eid)
        return out
    if isinstance(v, list):
        return [walk(x, execid) for x in v]
    if isinstance(v, str):
        return fix_str(v)
    return v


scrubbed = []
for part, l in lines:
    e = json.loads(l)
    e = walk(e)
    s = json.dumps(e, separators=(",", ":"), ensure_ascii=False)
    for bad in FORBIDDEN:
        if bad in s:
            sys.exit("scrub left %r in line: %s" % (bad, s[:300]))
    scrubbed.append((part, s))

os.makedirs(out, exist_ok=True)


def text(ls):
    return ("".join(s + "\n" for _, s in ls)).encode("utf-8")


spark = None


def compress(data, codec, path):
    global spark
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.master("local[1]").appName("fixture-compress").getOrCreate()
    jvm = spark._jvm
    c = jvm.org.apache.spark.io.CompressionCodec.createCodec(spark.sparkContext._jsc.sc().conf(), codec)
    stream = c.compressedOutputStream(jvm.java.io.FileOutputStream(path))
    step = 1 << 16  # write in pieces, like Spark's buffered writer
    for i in range(0, len(data), step):
        stream.write(bytearray(data[i:i + step]))
    stream.close()


if kind in ("single", "plainonly"):
    base = os.path.join(out, new_app)
    with open(base, "wb") as fh:
        fh.write(text(scrubbed))
    if kind == "single":
        for codec in ("lz4", "zstd", "snappy"):
            compress(text(scrubbed), codec, base + "." + codec)
        # History Server "Download" zip: one entry named after the log file.
        with zipfile.ZipFile(base + ".zip", "w", zipfile.ZIP_DEFLATED) as z:
            z.write(base + ".lz4", new_app + ".lz4")
elif kind == "inprogress":
    body = [x for x in scrubbed if '"Event":"SparkListenerApplicationEnd"' not in x[1]]
    data = text(body)
    cut = data.rfind(b"\n", 0, len(data) - 1) + 1 + 40  # 40 bytes into the last line
    with open(os.path.join(out, new_app + ".inprogress"), "wb") as fh:
        fh.write(data[:cut])
elif kind == "rolling":
    d = os.path.join(out, "eventlog_v2_" + new_app)
    os.makedirs(d, exist_ok=True)
    for i in range(len(files)):
        n = int(os.path.basename(files[i]).split("_")[1])
        compress(text([x for x in scrubbed if x[0] == i]), "zstd", os.path.join(d, "events_%d_%s.zstd" % (n, new_app)))
    open(os.path.join(d, "appstatus_" + new_app), "w").close()
    with zipfile.ZipFile(os.path.join(out, new_app + "_rolling.zip"), "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(os.listdir(d)):
            z.write(os.path.join(d, f), "eventlog_v2_%s/%s" % (new_app, f))
else:
    sys.exit("unknown kind " + kind)

if spark is not None:
    spark.stop()
print("wrote", kind, new_app, "executors->hosts", exec_host)

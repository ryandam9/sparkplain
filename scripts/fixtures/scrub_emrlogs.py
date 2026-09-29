"""Scrub one EMR cluster's container, step and node logs into testdata.

Usage: scrub_emrlogs.py <cluster log dir> <out root> <old app id> <new app id> [<original event log>]

<cluster log dir> is <LogUri>/<cluster-id>/ as downloaded from S3. Only the
files sparkplain classifies are copied (container stdout and stderr, step
controller and stderr, NodeManager and ResourceManager logs, bootstrap
master.log, HBase Master and region server logs), rewritten line by line with scrub_emr.Scrubber, and written
gzipped under <out root>/<new cluster id>/ in the same layout. Given the
original event log, hosts are named exactly as in its fixture, so the logs
join with it. Cluster, step and instance IDs become fixture IDs. The script
fails if any original marker is left, in content or in a path.
"""
import gzip
import json
import os
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from scrub_emr import Scrubber, app_of, read_eventlog  # noqa: E402

KEEP = [
    re.compile(r"^containers/application_[^/]+/container_[^/]+/std(out|err)\.gz$"),
    re.compile(r"^steps/s-[^/]+/(controller|stderr)\.gz$"),
    re.compile(r"^node/i-[^/]+/applications/hadoop-yarn/[^/]*(nodemanager|resourcemanager)[^/]*\.log\.gz$"),
    re.compile(r"^node/i-[^/]+/bootstrap-actions/master\.log\.gz$"),
    # HBase's Master and region server logs, current and rolled each hour.
    re.compile(r"^node/i-[^/]+/applications/hbase/hbase-hbase-(master|regionserver)-[^/]*\.log(\.\d{4}-\d{2}-\d{2}-\d{2})?\.gz$"),
]


ACCOUNT_RE = re.compile(r"arn:aws[\w-]*:[\w-]*:[\w-]*:(\d{12}):|\b(\d{12}):(?:role|user|root)")


def main():
    args = sys.argv[1:]
    recording = eventlogs_out = None
    extra_logs = []
    app_map = []  # (old app id, new app id) for the cluster's other applications
    while args and args[0].startswith("--"):
        flag = args.pop(0)
        if flag == "--recording":  # an AWS API recording (scripts/recordaws) to scrub alongside
            recording = args.pop(0)
        elif flag == "--eventlogs-out":  # where scrubbed event logs go
            eventlogs_out = args.pop(0)
        elif flag == "--eventlog":  # more event logs of the same cluster: <file>=<new app id>
            extra_logs.append(args.pop(0))
        elif flag == "--app":  # another application of the cluster: <old app id>=<new app id>
            app_map.append(tuple(args.pop(0).split("=")))
    src, out_root, old_app, new_app = args[0:4]
    sc = Scrubber(old_app, new_app)
    if len(args) > 4:
        raw = read_eventlog(args[4])
        if app_of(raw) != old_app:
            sys.exit("the event log is for %s, not %s" % (app_of(raw), old_app))
        sc.eventlog(raw)  # names the hosts as the event log fixture does
    # Learn the account numbers the ARNs in these files carry, so they are
    # replaced wherever they appear.
    accounts = set()
    texts = []
    for dirpath, _, names in os.walk(src):
        for n in names:
            if n.endswith(".gz"):
                try:
                    with gzip.open(os.path.join(dirpath, n), "rt", encoding="utf-8", errors="replace") as fh:
                        texts.append(fh.read())
                except OSError:
                    pass
    if recording:
        texts.append(open(recording, encoding="utf-8").read())
    for spec in extra_logs:
        texts.append(read_eventlog(spec.split("=")[0]))
    for t in texts:
        for m in ACCOUNT_RE.finditer(t):
            accounts.add(m.group(1) or m.group(2))
    sc.add_aws_rules(sorted(accounts))
    # Each other application keeps one new ID everywhere (its containers,
    # logs, event log and the recording): its "<ts>_<n>" is rewritten
    # before the plain timestamp rule would give it the wrong number.
    for spec in extra_logs:
        app_map.append((app_of(read_eventlog(spec.split("=")[0])), spec.split("=")[1]))
    for old, new in app_map:
        ok, nk = old.split("_", 1)[1], new.split("_", 1)[1]
        sc.replace.insert(0, (re.compile(re.escape(ok)), lambda m, nk=nk: nk))
    src = src.rstrip("/")
    old_cluster = os.path.basename(src)
    # Fixture IDs keep EMR's shapes: j- and s- with 13 or more upper-case
    # characters, i- with 17 hex digits.
    new_cluster = "j-FIXTURE" + new_app[-4:] + "CLUSTER"
    ids = {old_cluster: new_cluster}

    def rename(prefix, fmt):
        def fn(m):
            if m.group(0) not in ids:
                ids[m.group(0)] = fmt % (sum(1 for k in ids if k.startswith(prefix)) + 1)
            return ids[m.group(0)]
        return fn

    sc.replace += [
        (re.compile(re.escape(old_cluster)), lambda m: new_cluster),
        (re.compile(r"\bs-[0-9A-Z]{12,}\b"), rename("s-", "s-FIXTURESTEP%04d")),
        (re.compile(r"\bi-[0-9a-f]{17}\b"), rename("i-", "i-0fee000000%07d")),
    ]
    files = []
    for dirpath, _, names in os.walk(src):
        for n in names:
            rel = os.path.relpath(os.path.join(dirpath, n), src).replace(os.sep, "/")
            if any(k.match(rel) for k in KEEP):
                files.append(rel)
    files.sort()
    total = 0
    for rel in files:
        with gzip.open(os.path.join(src, rel), "rt", encoding="utf-8", errors="replace") as fh:
            text = fh.read()
        clean = "".join(sc.fix(line) for line in text.splitlines(keepends=True))
        new_rel = sc.fix(rel)
        for original in [old_cluster] + [k for k in ids if k != old_cluster]:
            if original in clean or original in new_rel:
                sys.exit("scrub left %r in %s" % (original, rel))
        sc.check(clean, rel)
        sc.check(new_rel, "path " + rel)
        dst = os.path.join(out_root, new_cluster, new_rel)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        data = clean.encode("utf-8")
        with open(dst, "wb") as fh:
            # mtime 0 so rerunning gives the same bytes
            with gzip.GzipFile(fileobj=fh, mode="wb", mtime=0, filename="") as gz:
                gz.write(data)
        total += os.path.getsize(dst)
    # More applications of the same cluster: their event logs, scrubbed with
    # the same names, each under its own new application ID.
    for spec in extra_logs:
        path, app = spec.split("=")
        raw = read_eventlog(path)
        lines = []
        for l in raw.splitlines():
            if not l.strip():
                continue
            s = json.dumps(sc.walk(json.loads(l)), separators=(",", ":"), ensure_ascii=False)
            sc.check(s, path)
            lines.append(s)
        dst = os.path.join(eventlogs_out or ".", app)
        with open(dst, "w", encoding="utf-8") as fh:
            fh.write("".join(x + "\n" for x in lines))
        print("wrote", dst, len(lines), "events")
    if recording:
        clean = sc.fix(open(recording, encoding="utf-8").read())
        for original in [old_cluster] + [k for k in ids if k != old_cluster] + sorted(accounts):
            if original in clean:
                sys.exit("scrub left %r in the recording" % original)
        sc.check(clean, "recording")
        dst = os.path.join(out_root, "..", "aws", new_cluster + ".json")
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, "w", encoding="utf-8") as fh:
            fh.write(clean)
        print("wrote", os.path.normpath(dst))
    print("wrote %d files, %d bytes, under %s; hosts %s; ids %s; accounts %d" % (len(files), total, os.path.join(out_root, new_cluster), sc.hosts, ids, len(accounts)))


if __name__ == "__main__":
    main()

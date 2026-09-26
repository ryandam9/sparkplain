"""Scrub one EMR cluster's container, step and node logs into testdata.

Usage: scrub_emrlogs.py <cluster log dir> <out root> <old app id> <new app id> [<original event log>]

<cluster log dir> is <LogUri>/<cluster-id>/ as downloaded from S3. Only the
files sparkplain classifies are copied (container stdout and stderr, step
controller and stderr, NodeManager and ResourceManager logs, bootstrap
master.log), rewritten line by line with scrub_emr.Scrubber, and written
gzipped under <out root>/<new cluster id>/ in the same layout. Given the
original event log, hosts are named exactly as in its fixture, so the logs
join with it. Cluster, step and instance IDs become fixture IDs. The script
fails if any original marker is left, in content or in a path.
"""
import gzip
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
]


def main():
    src, out_root, old_app, new_app = sys.argv[1:5]
    sc = Scrubber(old_app, new_app)
    if len(sys.argv) > 5:
        raw = read_eventlog(sys.argv[5])
        if app_of(raw) != old_app:
            sys.exit("the event log is for %s, not %s" % (app_of(raw), old_app))
        sc.eventlog(raw)  # names the hosts as the event log fixture does
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
    print("wrote %d files, %d bytes, under %s; hosts %s; ids %s" % (len(files), total, os.path.join(out_root, new_cluster), sc.hosts, ids))


if __name__ == "__main__":
    main()

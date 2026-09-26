"""Scrub a real EMR event log into a committable fixture.

Usage: scrub_emr.py <event log or History Server zip> <out file>

The log comes from a throwaway test cluster, but CLAUDE.md still forbids
real hostnames, bucket names and IDs in testdata. Every string is rewritten:
EC2 host names and addresses become fixture-style names, the application
and cluster timestamps become the fixture ones, the test bucket and region
become neutral values, and the planted password becomes the FAKE- form the
secret tests look for. The script fails if any original marker is left.
"""
import json
import re
import sys
import zipfile

src, out = sys.argv[1:3]
if src.endswith(".zip"):
    z = zipfile.ZipFile(src)
    raw = z.read([n for n in z.namelist() if not n.endswith("/")][0]).decode("utf-8")
else:
    raw = open(src, encoding="utf-8").read()

hosts = {}


def host(m):
    h = m.group(0)
    if h not in hosts:
        hosts[h] = "ip-10-0-2-%d" % (10 + len(hosts))
    return hosts[h]


REPLACE = [
    (re.compile(r"ip-172-31-\d+-\d+"), host),
    (re.compile(r"\b172\.31\.\d+\.\d+\b"), lambda m: "10.0.2.99"),
    (re.compile(r"application_1790408460617_0001"), lambda m: "application_1790380000000_0049"),
    (re.compile(r"1790408460617"), lambda m: "1790380000000"),
    (re.compile(r"sparkplain-test-a5165fcd"), lambda m: "sparkplain-fixtures"),
    (re.compile(r"ap-southeast-2"), lambda m: "us-east-1"),
    (re.compile(r"FAKE-hunter2-secret"), lambda m: "FAKE-EMR-PASSWORD-0010"),
]
FORBIDDEN = ["172-31", "172.31.", "a5165fcd", "ap-southeast-2", "1790408460617", "hunter2"]


def fix(s):
    for rx, fn in REPLACE:
        s = rx.sub(fn, s)
    return s


def walk(v):
    if isinstance(v, dict):
        return {fix(k): walk(x) for k, x in v.items()}
    if isinstance(v, list):
        return [walk(x) for x in v]
    return fix(v) if isinstance(v, str) else v


lines = []
for l in raw.splitlines():
    if not l.strip():
        continue
    s = json.dumps(walk(json.loads(l)), separators=(",", ":"), ensure_ascii=False)
    for bad in FORBIDDEN:
        if bad in s:
            sys.exit("scrub left %r in: %s" % (bad, s[:200]))
    lines.append(s)
with open(out, "w", encoding="utf-8") as fh:
    fh.write("".join(l + "\n" for l in lines))
print("wrote", out, len(lines), "events; hosts", hosts)

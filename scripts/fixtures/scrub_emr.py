"""Scrub a real EMR event log into a committable fixture.

Usage: scrub_emr.py <event log or History Server zip> <out file> [<new app id>]

The log comes from a throwaway test cluster, but CLAUDE.md still forbids
real hostnames, bucket names and IDs in testdata. Every string is rewritten:
EC2 host names and addresses become fixture-style names, the application
and cluster timestamps become the fixture ones, the test bucket and region
become neutral values, and the planted password becomes the FAKE- form the
secret tests look for. The script fails if any original marker is left.

scrub_emrlogs.py imports Scrubber so a cluster's text logs get the same
host names as its event log.
"""
import json
import re
import sys
import zipfile


class Scrubber:
    """Rewrites strings for one application, naming hosts in the order it
    first sees them."""

    def __init__(self, old_app, new_app):
        self.hosts = {}
        old_ts, new_ts = old_app.split("_")[1], new_app.split("_")[1]
        # "<ts>_<n>" is in every attempt and container ID too
        # (appattempt_<ts>_<n>_…, container_<ts>_<n>_…), so both parts change
        # together and the IDs stay those of one application.
        old_key, new_key = old_app.split("_", 1)[1], new_app.split("_", 1)[1]
        self.replace = [
            (re.compile(r"ip-172-31-\d+-\d+"), self._host),
            (re.compile(r"\b172\.31\.\d+\.\d+\b"), lambda m: "10.0.2.99"),
            (re.compile(re.escape(old_key)), lambda m: new_key),
            (re.compile(old_ts), lambda m: new_ts),
            (re.compile(r"sparkplain-test-[0-9a-f]{8}"), lambda m: "sparkplain-fixtures"),
            (re.compile(r"ap-southeast-2"), lambda m: "us-east-1"),
            (re.compile(r"FAKE-hunter2-secret"), lambda m: "FAKE-EMR-PASSWORD-0010"),
        ]
        self.forbidden = ["172-31", "172.31.", "sparkplain-test-", "ap-southeast-2", old_ts, "hunter2"]

    def add_aws_rules(self, accounts):
        """Rules for AWS API recordings and logs that quote ARNs: account
        numbers, public addresses, network and resource IDs, temporary key
        and role IDs, and the SSH key pair's name."""
        ids = {}

        def rename(prefix):
            def fn(m):
                if m.group(0) not in ids:
                    n = sum(1 for k in ids if k.startswith(prefix)) + 1
                    width = len(m.group(0)) - len(prefix) - 1
                    ids[m.group(0)] = "%s-%s" % (prefix, ("0f1e%0*x" % (max(width - 4, 1), n))[:width])
                return ids[m.group(0)]
            return fn

        public = {}

        def pub(ip):
            if ip not in public:
                public[ip] = "192.0.2.%d" % (10 + len(public))
            return public[ip]

        def private_ip(m):
            # The same number as the host name, so a node's name and address
            # still match: 172.31.13.227 goes with ip-172-31-13-227.
            name = self._host(re.match(r".*", "ip-" + m.group(0).replace(".", "-")))
            return name[3:].replace("-", ".")

        # Before the base rule, which gives every private address 10.0.2.99.
        self.replace.insert(0, (re.compile(r"\b172\.31\.\d+\.\d+\b"), private_ip))
        for acct in accounts:
            self.replace.append((re.compile(re.escape(acct)), lambda m: "000000000000"))
            self.forbidden.append(acct)
        for prefix in ["subnet", "sg", "vol", "ami", "eni", "vpc", "vpce", "igw", "rtb"]:
            self.replace.append((re.compile(r"\b%s-[0-9a-f]{8,17}\b" % prefix), rename(prefix)))
        self.replace += [
            (re.compile(r"ec2-(\d+-\d+-\d+-\d+)\.([a-z0-9-]+\.)?compute\.amazonaws\.com"), lambda m: "ec2-%s.compute.amazonaws.com" % pub(m.group(1).replace("-", ".")).replace(".", "-")),
            (re.compile(r'((?:sourceIPAddress|PublicIpAddress|publicIp)\\?"\s*:\s*\\?")(\d+\.\d+\.\d+\.\d+)'), lambda m: m.group(1) + pub(m.group(2))),
            (re.compile(r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b"), lambda m: "ASIAFIXTUREFIXTURE01"),
            (re.compile(r"\b(?:AROA|AIDA|ANPA|AGPA)[A-Z0-9]{16,}\b"), lambda m: "AROAFIXTUREFIXTURE01"),
            (re.compile(r'("Ec2KeyName"\s*:\s*")[^"]*'), lambda m: m.group(1) + "fixture-key"),
            # The role the phase 3 access job was refused (it does not exist).
            (re.compile(r"sparkplain-test-no-such-role"), lambda m: "fixture-no-such-role"),
        ]

    def _host(self, m):
        h = m.group(0)
        if h not in self.hosts:
            self.hosts[h] = "ip-10-0-2-%d" % (10 + len(self.hosts))
        return self.hosts[h]

    def fix(self, s):
        for rx, fn in self.replace:
            s = rx.sub(fn, s)
        return s

    def check(self, s, where):
        for bad in self.forbidden:
            if bad in s:
                sys.exit("scrub left %r in %s: %s" % (bad, where, s[:200]))

    def walk(self, v):
        if isinstance(v, dict):
            return {self.fix(k): self.walk(x) for k, x in v.items()}
        if isinstance(v, list):
            return [self.walk(x) for x in v]
        return self.fix(v) if isinstance(v, str) else v

    def eventlog(self, raw):
        """Returns the scrubbed event log's lines."""
        lines = []
        for l in raw.splitlines():
            if not l.strip():
                continue
            s = json.dumps(self.walk(json.loads(l)), separators=(",", ":"), ensure_ascii=False)
            self.check(s, "event log")
            lines.append(s)
        return lines


def read_eventlog(src):
    if src.endswith(".zip"):
        z = zipfile.ZipFile(src)
        return z.read([n for n in z.namelist() if not n.endswith("/")][0]).decode("utf-8")
    return open(src, encoding="utf-8").read()


def app_of(raw):
    return next(json.loads(l)["App ID"] for l in raw.splitlines() if '"SparkListenerApplicationStart"' in l)


def main():
    src, out = sys.argv[1:3]
    new_app = sys.argv[3] if len(sys.argv) > 3 else "application_1790380000000_0049"
    raw = read_eventlog(src)
    sc = Scrubber(app_of(raw), new_app)
    lines = sc.eventlog(raw)
    with open(out, "w", encoding="utf-8") as fh:
        fh.write("".join(l + "\n" for l in lines))
    print("wrote", out, len(lines), "events; hosts", sc.hosts)


if __name__ == "__main__":
    main()

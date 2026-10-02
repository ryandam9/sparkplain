package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	iofs "io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/awsmeta"
	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/report"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// version is overridden at build time with -ldflags "-X main.version=…".
var version = "0.1.0-dev"

// Exit codes (SPEC §6).
const (
	exitOK          = 0
	exitFatal       = 2
	exitPartial     = 3
	exitInterrupted = 130
)

// maxWorkers bounds -workers.
const maxWorkers = 256

// Output permissions: private by default (review SP-005).
const (
	outDirMode  = 0o700
	outFileMode = 0o600
)

var appIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$`)

type options struct {
	profile, region, configPath, env, clusterID, clusterName, hbaseClusterID, hbaseClusterName, appID string
	eventLog, from, out, format, maxSize, maxUnpacked, show, decodeScan                               string
	workers                                                                                           int
	timeout, windowPad                                                                                time.Duration
	noCloudWatch, noCloudTrail, showVersion, check, initConfig                                        bool
	noStepLogs, noNodeLogs, noHBaseLogs                                                               bool
	// off says why each source turned off is not read, by its name in the
	// Sources list (see applyReads).
	off     map[string]string
	sources []string
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sparkplain", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.StringVar(&o.profile, "profile", "", "named AWS profile for online runs (default for the default chain)")
	fs.StringVar(&o.region, "region", "", "AWS region override (online runs)")
	fs.StringVar(&o.configPath, "config", "", "YAML defaults file (default ~/.config/sparkplain/config.yaml)")
	fs.StringVar(&o.env, "env", "", "environment in the config file, such as prod or nonprod: its clusters, profile, region and event log location")
	fs.StringVar(&o.clusterID, "cluster-id", "", "EMR cluster ID: read its metadata and logs from AWS (needs -profile)")
	fs.StringVar(&o.clusterName, "cluster-name", "", "EMR cluster name, instead of -cluster-id: the cluster of that name that ran the application (also cluster-name in the config file)")
	fs.StringVar(&o.hbaseClusterID, "hbase-cluster-id", "", "EMR cluster ID that runs HBase when it is separate from the Spark cluster")
	fs.StringVar(&o.hbaseClusterName, "hbase-cluster-name", "", "EMR cluster name, instead of -hbase-cluster-id: the cluster of that name up when the application ran (also hbase-cluster-name in the config file)")
	fs.StringVar(&o.appID, "app-id", "", "Spark application ID, e.g. application_1700000000000_0042 (required)")
	fs.StringVar(&o.eventLog, "eventlog", "", "event log: local file, rolling eventlog_v2_* folder, folder of logs, or History Server zip")
	fs.StringVar(&o.from, "from", "", "local copy of the cluster's logs (containers/, steps/, node/) or of one application's container folders")
	fs.StringVar(&o.out, "out", "", "output folder (default ~/sparkplain/<yyyy-mm-dd>/<app-id>/)")
	fs.StringVar(&o.format, "format", "", "outputs, comma-separated: html, json, explorer (default all three; both = html,json)")
	fs.IntVar(&o.workers, "workers", 16, "how many log files to read at once, 1 to 256")
	fs.StringVar(&o.maxSize, "max-size", "", "largest file to read, as stored (compressed), e.g. 10GiB (default 10GiB)")
	fs.StringVar(&o.maxUnpacked, "max-unpacked", "", "most bytes one compressed file may unpack to, e.g. 50GiB (default 50GiB)")
	fs.DurationVar(&o.timeout, "overall-timeout", 0, "deadline for the whole run (default 30m)")
	fs.BoolVar(&o.noCloudWatch, "no-cloudwatch", false, "skip CloudWatch metrics (fewer permissions needed)")
	fs.BoolVar(&o.noCloudTrail, "no-cloudtrail", false, "skip CloudTrail lookups (fewer permissions needed)")
	fs.BoolVar(&o.noStepLogs, "no-step-logs", false, "skip the EMR step logs (also read: step-logs: no in the config file)")
	fs.BoolVar(&o.noNodeLogs, "no-node-logs", false, "skip the node logs: YARN's NodeManager and ResourceManager, bootstrap actions (also read: node-logs: no)")
	fs.BoolVar(&o.noHBaseLogs, "no-hbase-logs", false, "skip HBase's Master and region server logs (also read: hbase-logs: no)")
	fs.DurationVar(&o.windowPad, "window-pad", 5*time.Minute, "padding around the run's time window for CloudWatch and CloudTrail queries")
	fs.BoolVar(&o.showVersion, "version", false, "print the version and exit")
	fs.BoolVar(&o.initConfig, "init-config", false, "write a starter config file, every key explained, to ~/.config/sparkplain/config.yaml (or -config) and exit")
	fs.BoolVar(&o.check, "check", false, "check what the run can read, print it, and exit (0 all readable, 3 not)")
	fs.StringVar(&o.show, "show", "", "print the event at file:line (as the pages cite it), redacted, and exit")
	fs.StringVar(&o.decodeScan, "decode-scan", "", "print an HBase scan string (hbase.mapreduce.scan, base64) decoded: key range, columns, filters; - reads it from stdin; reads nothing else and exits")
	fs.Func("source", "the application's source file or folder, shown beside jobs and stages in the explorer (repeatable; redacted)", func(v string) error {
		o.sources = append(o.sources, v)
		return nil
	})
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: sparkplain -app-id <application id> [-eventlog <path>] [-profile <p> -cluster-id <id> | -from <folder>] [flags]\n\n")
		fmt.Fprintf(stderr, "Turns one Spark application's event log and YARN, step and node logs into <app-id>-report.html, <app-id>-report.json and <app-id>-explorer.html.\n\nFlags:\n")
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nExit codes: 0 complete, 2 fatal, 3 partial (a source missing or unreadable), 130 interrupted.\n")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitFatal
	}
	if o.showVersion {
		fmt.Fprintln(stdout, "sparkplain", version)
		return exitOK
	}
	if o.initConfig {
		return writeStarterConfig(firstNonEmpty(o.configPath, defaultConfigPath()), stdout, stderr)
	}
	if o.decodeScan != "" {
		return decodeScan(o.decodeScan, os.Stdin, stdout, stderr)
	}
	con := newConsole(stdout, stderr)
	fail := func(format string, a ...any) int {
		con.clear()
		fmt.Fprintf(stderr, "sparkplain: "+format+"\n", a...)
		return exitFatal
	}
	if fs.NArg() > 0 {
		return fail("unexpected argument %q (flags go before values, e.g. -eventlog <path>)", fs.Arg(0))
	}

	if o.appID == "" {
		fs.Usage()
		return fail("-app-id is required")
	}
	if !appIDRE.MatchString(o.appID) {
		return fail("-app-id %q does not look like a Spark application ID", o.appID)
	}
	// A leading ~/ means the home folder, in the config file and in
	// quoted flags alike, which no shell expands.
	o.configPath, o.eventLog, o.from, o.out = expandHome(o.configPath), expandHome(o.eventLog), expandHome(o.from), expandHome(o.out)
	cfgPath, explicit := o.configPath, o.configPath != ""
	if !explicit {
		cfgPath = defaultConfigPath()
	}
	cfg, err := loadConfig(cfgPath, explicit)
	if err != nil {
		return fail("%v", err)
	}
	if cfg, err = cfg.withEnv(o.env); err != nil {
		return fail("%v", err)
	}
	o.applyReads(cfg.Read)
	cfg.Out, cfg.EventLogPrefix = expandHome(cfg.Out), expandHome(cfg.EventLogPrefix)
	// The config file names the clusters, the profile and the region when
	// the flags do not. Its cluster names stand aside for a -from run, and
	// for an -eventlog run that names no cluster and no -env: those are
	// asked to read local files only.
	o.profile, o.region = firstNonEmpty(o.profile, cfg.Profile), firstNonEmpty(o.region, cfg.Region)
	if o.clusterID == "" && o.clusterName == "" && o.from == "" && (o.eventLog == "" || o.env != "") {
		o.clusterName = cfg.ClusterName
	}
	if o.hbaseClusterID == "" && o.hbaseClusterName == "" && (o.clusterID != "" || o.clusterName != "") {
		o.hbaseClusterName = cfg.HBaseClusterName
	}
	online := o.clusterID != "" || o.clusterName != ""
	if (o.hbaseClusterID != "" || o.hbaseClusterName != "") && !online {
		return fail("-hbase-cluster-id and -hbase-cluster-name need -cluster-id or -cluster-name for the Spark cluster")
	}
	if online && o.from != "" {
		return fail("-from reads a local copy of the cluster's logs, and -cluster-id reads them from S3: pass one or the other")
	}
	outputs, err := parseFormats(firstNonEmpty(o.format, cfg.Format, "html,json,explorer"))
	if err != nil {
		return fail("%v", err)
	}
	// -workers is a safety limit, not a way to open thousands of streams
	// (each holds a file descriptor and buffers, zips hold memory).
	if o.workers < 1 || o.workers > maxWorkers {
		return fail("-workers must be between 1 and %d, not %d", maxWorkers, o.workers)
	}
	maxSize := int64(10 << 30)
	if s := firstNonEmpty(o.maxSize, cfg.MaxSize); s != "" {
		if maxSize = parseSize(s); maxSize <= 0 {
			return fail("-max-size %q is not a size (try 10GiB or 500MiB)", s)
		}
	}
	maxUnpacked := int64(50 << 30)
	if s := firstNonEmpty(o.maxUnpacked, cfg.MaxUnpacked); s != "" {
		if maxUnpacked = parseSize(s); maxUnpacked <= 0 {
			return fail("-max-unpacked %q is not a size (try 50GiB)", s)
		}
	}
	timeout := 30 * time.Minute
	if cfg.OverallTimeout > 0 {
		timeout = cfg.OverallTimeout
	}
	if o.timeout > 0 {
		timeout = o.timeout
	}
	loc := time.Local
	if cfg.TimeZone != "" {
		if loc, err = time.LoadLocation(cfg.TimeZone); err != nil {
			return fail("config timezone %q: %v", cfg.TimeZone, err)
		}
	}
	con.begin(o.appID)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cloud := &awsSession{profile: o.profile, region: o.region, at: appClusterStart(o.appID)}
	if online && o.profile == "" {
		return fail("%v", errNoProfile)
	}
	outDir := firstNonEmpty(o.out, cfg.Out)
	if outDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fail("no home folder for the default output path; pass -out: %v", err)
		}
		outDir = filepath.Join(home, "sparkplain", time.Now().Format("2006-01-02"), o.appID)
	}
	con.status("checking what the run can read")
	chk := accessCheck(ctx, cloud, checkInput{o: o, eventLogPrefix: cfg.EventLogPrefix, outDir: outDir})
	checks := chk.rows
	con.accessCheck(chk, o.profile, cloud.regionName(), online)
	if o.check {
		return checkExit(checks)
	}
	var cluster *model.Cluster
	var logs clusterLogs
	if online {
		con.status("reading the cluster from the EMR API")
		c, err := cloud.cluster(ctx, o.clusterID, o.clusterName)
		switch {
		case errors.Is(err, errNoProfile), errors.Is(err, awsmeta.ErrNotFound):
			return fail("%v", err) // a usage mistake: nothing to report on
		case err != nil:
			// No access to the EMR API (or no working credentials): carry
			// on with what can be read without it.
			con.note("could not describe the cluster (%s): %v", awsmeta.ErrorClass(err), err)
			logs = noCluster(firstNonEmpty(o.clusterID, o.clusterName), err)
		default:
			cluster = &c
			con.clusterFound(c)
			con.status("reading the cluster's steps and instances")
			logs = emrMetadata(ctx, cloud, cluster)
		}
		for _, row := range []*model.SourceStatus{logs.emr, logs.ec2} {
			if row != nil {
				con.sources(*row)
			}
		}
	}
	evPath := o.eventLog
	if evPath == "" && cfg.EventLogPrefix != "" {
		evPath = cfg.EventLogPrefix
		con.note("using eventlog-prefix %s from %s", evPath, cfgPath)
	}
	if evPath == "" && cluster != nil {
		if dir := cluster.Configurations["spark-defaults/spark.eventLog.dir"]; isS3(dir) {
			evPath = dir
			con.note("using the cluster's spark.eventLog.dir %s", evPath)
		}
	}
	if evPath == "" && !online && o.from == "" {
		return fail("pass -eventlog <path> (a file, rolling folder, folder of logs, History Server zip or s3:// location), -cluster-id to read from the cluster, or -from with a copy of its logs")
	}
	if o.show != "" {
		file, line, err := eventlog.ParseLocation(o.show)
		if err != nil {
			return fail("-show: %v", err)
		}
		if evPath == "" {
			return fail("-show needs the event log: pass -eventlog")
		}
		in, err := cloud.resolve(ctx, evPath, o.appID, eventlog.Limits{MaxObjectBytes: maxSize, MaxUnpackedBytes: maxUnpacked})
		if err != nil {
			return fail("%v", err)
		}
		defer in.Close()
		b, err := eventlog.ShowEvent(ctx, in, file, line)
		if err != nil {
			return fail("-show: %v", err)
		}
		fmt.Fprintf(stdout, "%s\n", b)
		return exitOK
	}

	src := model.SourceStatus{Name: eventLogSource, Location: evPath}
	var log *model.EventLog
	var in *eventlog.Input
	var stepDirs []string
	con.status("reading the event log")
	switch {
	case evPath != "":
		in, err = cloud.resolve(ctx, evPath, o.appID, eventlog.Limits{MaxObjectBytes: maxSize, MaxUnpackedBytes: maxUnpacked})
	case online:
		// Jobs often set spark.eventLog.dir in their own spark-submit
		// arguments rather than in the cluster's configuration.
		err = errNoEventLog
		stepDirs = stepEventLogDirs(logs.steps)
		for _, dir := range stepDirs {
			in, err = cloud.resolve(ctx, dir, o.appID, eventlog.Limits{MaxObjectBytes: maxSize, MaxUnpackedBytes: maxUnpacked})
			if err == nil || eventlog.ErrorClass(err) != eventlog.ClassNotFound {
				evPath, src.Location = dir, dir
				con.note("using the spark.eventLog.dir a step set, %s", dir)
				break
			}
			err = errNoEventLog
		}
	default:
		err = errNoEventLog
	}
	switch {
	case errors.Is(err, errNoEventLog) && !online:
		src.Status, src.Detail = "not-supplied", "No event log was given. Pass -eventlog with an S3 copy or a History Server download; until then only the container, step and node logs are shown."
	case errors.Is(err, errNoEventLog):
		src.Status, src.Detail = "not-supplied", "The cluster keeps Spark event logs in HDFS (the default spark.eventLog.dir), which is gone once it ends. Pass -eventlog with an S3 copy or a History Server download, or set spark.eventLog.dir to S3 on the cluster."
		if len(stepDirs) > 0 {
			src.Detail = fmt.Sprintf("No event log for this application under the spark.eventLog.dir its cluster's steps set (%s). If the application never started SparkContext it wrote none; otherwise pass -eventlog with where it went.", strings.Join(stepDirs, ", "))
		}
	case err != nil && eventlog.ErrorClass(err) == eventlog.ClassNotFound && !online:
		return fail("%v", err)
	case err != nil && eventlog.ErrorClass(err) == eventlog.ClassNotFound:
		// Online, the folder was right but this application's log is not in
		// it. On S3 a log appears when Spark closes the file (as .inprogress
		// if the application then died before finishing it); checked on
		// the test clusters, where applications killed with their cluster
		// left none.
		src.Status, src.Class = "not-supplied", eventlog.ClassNotFound
		src.Detail = err.Error() + ". On S3 an event log appears only once Spark closes it, so an application that is still running, was killed with its cluster, or never started SparkContext has none."
	case err != nil:
		src.Status, src.Class, src.Detail = "error", eventlog.ErrorClass(err), err.Error()
		con.note("could not read the event log (%s): %v", src.Class, err)
	default:
		src.Location = in.Where()
		for _, n := range in.Notes { // which of several logs was read, and why
			con.note("%s", n)
		}
		start := time.Now()
		opt := eventlog.Options{}
		if outputs["explorer"] {
			lim := cfg.Explorer.WithDefaults()
			opt.Explorer = &lim
		}
		log, err = eventlog.Parse(ctx, in, opt)
		in.Close()
		if err != nil {
			if ctx.Err() == context.Canceled {
				con.clear()
				fmt.Fprintln(stderr, "sparkplain: interrupted")
				return exitInterrupted
			}
			log.Stats.Notes = append(log.Stats.Notes, fmt.Sprintf("Reading stopped at the %s deadline (-overall-timeout); later events are missing.", timeout))
			log.Stats.Truncated = true
			src.Class = eventlog.ClassTimeout
		}
		switch id := log.Application.ID; {
		case id != "" && id != o.appID:
			return fail("the event log at %s belongs to %s, not %s", evPath, id, o.appID)
		case id == "" && !in.NameMatches && log.Stats.Events > 0:
			// Neither the name nor the log itself says this is the
			// application asked for: refuse rather than report on another.
			return fail("cannot confirm the event log at %s is for %s: it has no application start event and its name does not match", evPath, o.appID)
		case id == "":
			log.Stats.Notes = append(log.Stats.Notes, "The log has no application start event; it was matched to "+o.appID+" by its file name.")
		}
		src.Status, src.Detail = eventSourceDetail(log, time.Since(start))
		src.Brief = eventSourceBrief(log)
		if log.Stats.Events == 0 {
			// Nothing usable: say why, and let every section show that it
			// needs the event log instead of an empty application.
			src.Status, src.Class = "error", eventlog.ClassCorrupt
			con.note("no events could be read from %s", evPath)
			log = nil
		}
	}
	con.sources(src)

	lim := source.Limits{Workers: o.workers, MaxObject: maxSize, MaxUnpacked: maxUnpacked}
	var fetched []report.FetchedSource // the application's scripts, from S3
	mode := "offline-eventlog"
	switch {
	case online && cluster == nil:
		mode = "online" // no cluster details: its logs and metrics cannot be found
	case online:
		mode = "online"
		con.status("reading container, step and node logs")
		separate := o.hbaseClusterID != "" || o.hbaseClusterName != ""
		logs.readLogs(ctx, cloud, log, o.appID, lim, !separate, o.off)
		if why, off := o.off["HBase server logs"]; separate && off {
			logs.sources = append(logs.sources, model.SourceStatus{Name: "HBase server logs", Status: "not-requested", Location: firstNonEmpty(o.hbaseClusterID, o.hbaseClusterName),
				Detail: "Not read: " + why + "."})
		} else if separate {
			con.status("reading HBase server logs from " + firstNonEmpty(o.hbaseClusterID, o.hbaseClusterName))
			logs.readHBaseCluster(ctx, cloud, o.hbaseClusterID, o.hbaseClusterName, log, lim)
		}
		con.sources(logs.sources...)
		con.status("reading CloudWatch metrics")
		logs.readMetrics(ctx, cloud, log, o.off["CloudWatch"], o.windowPad)
		con.sources(logs.sources...)
		con.status("reading CloudTrail")
		logs.readCalls(ctx, cloud, log, o.off["CloudTrail"], o.windowPad)
		con.sources(logs.sources...)
		if outputs["explorer"] {
			var row *model.SourceStatus
			con.status("reading the application's code")
			if fetched, row = logs.fetchScripts(ctx, cloud, o.appID); row != nil {
				logs.sources = append(logs.sources, *row)
			}
			con.sources(logs.sources...)
		}
	case o.from != "":
		mode = "offline-logs"
		con.status("reading the logs in " + o.from)
		if logs, err = offlineLogs(ctx, o.from, o.appID, log, lim, o.off); err != nil {
			if !errors.Is(err, iofs.ErrPermission) {
				return fail("%v", err)
			}
			for _, name := range []string{"Container logs", "Step logs", "Node logs"} {
				logs.sources = append(logs.sources, model.SourceStatus{Name: name, Status: "error", Class: source.ClassAccessDenied, Location: o.from, Detail: err.Error()})
			}
		}
	}
	con.sources(logs.sources...)
	if ctx.Err() == context.Canceled {
		con.clear()
		fmt.Fprintln(stderr, "sparkplain: interrupted")
		return exitInterrupted
	}
	con.status("writing the report")
	ain := analyze.Input{
		AppID:       o.appID,
		Tool:        "sparkplain " + version,
		Mode:        mode,
		GeneratedAt: time.Now(),
		TimeZone:    loc.String(),
		EventLog:    log,
		EventSource: src,
		Thresholds:  cfg.Thresholds.apply(analyze.DefaultThresholds()),
		Steps:       logs.steps,
		Logs:        logs.files,
		Metrics:     logs.metrics,
		AWSCalls:    logs.calls,
		LogsRead:    online || o.from != "",
	}
	if log == nil {
		ain.RunStart, ain.RunEnd, _, _ = runWindow(nil, logs.files, logs.steps, logs.cluster)
	}
	if logs.cluster != nil {
		cl := *logs.cluster
		cl.Instances = logs.instances
		ain.Cluster = &cl
	}
	if logs.emr != nil {
		ain.LogSources = append(ain.LogSources, *logs.emr)
		if logs.ec2 != nil {
			ain.LogSources = append(ain.LogSources, *logs.ec2)
		}
	} else if ain.LogsRead {
		ain.LogSources = append(ain.LogSources, model.SourceStatus{Name: "EMR API", Status: "not-requested", Detail: "Not called: -from reads local files only."})
	}
	ain.LogSources = append(ain.LogSources, logs.sources...)
	r := analyze.Run(ain)
	r.AccessCheck = checks
	for _, g := range r.AccessGaps {
		con.note("no access to %s, so the report does not show %s (needs %s).", g.Source, g.Missing, g.Needs)
	}
	if r.Application.ID == "" {
		r.Application.ID = o.appID
	}
	// Reports hold user, host, cluster and log details: keep them private
	// to the user who ran sparkplain (SPEC §6). Share them with chmod.
	if err := os.MkdirAll(outDir, outDirMode); err != nil {
		return fail("creating %s: %v", outDir, err)
	}
	written := map[string]string{}
	var order []string
	wrote := func(kind, p string) {
		written[kind] = p
		order = append(order, kind)
	}
	ropt := report.Options{Location: loc}
	if outputs["explorer"] {
		ropt.ExplorerHref = outputName(r.Application.ID, "explorer.html")
	}
	if outputs["html"] {
		p := filepath.Join(outDir, outputName(r.Application.ID, "report.html"))
		if err := writeFile(p, func(w io.Writer) error { return report.WriteHTML(w, r, ropt) }); err != nil {
			return fail("writing %s: %v", p, err)
		}
		wrote("Report", p)
	}
	if outputs["explorer"] {
		p := filepath.Join(outDir, outputName(r.Application.ID, "explorer.html"))
		var x *model.Explorer
		if log != nil {
			x = log.Explorer
		}
		xopt := report.ExplorerOptions{}
		if len(o.sources) > 0 || len(fetched) > 0 {
			var err error
			if xopt.Sources, xopt.SourceNotes, err = report.LoadSourcesFrom(r, o.sources, fetched); err != nil {
				return fail("%v", err)
			}
		}
		if outputs["html"] {
			xopt.ReportHref = outputName(r.Application.ID, "report.html")
		}
		if err := writeFile(p, func(w io.Writer) error { return report.WriteExplorer(w, r, x, xopt) }); err != nil {
			return fail("writing %s: %v", p, err)
		}
		wrote("Explorer", p)
	}
	if outputs["json"] {
		p := filepath.Join(outDir, outputName(r.Application.ID, "report.json"))
		if err := writeFile(p, func(w io.Writer) error { return report.WriteJSON(w, r) }); err != nil {
			return fail("writing %s: %v", p, err)
		}
		wrote("JSON", p)
	}
	con.summary(r, written, order, r.ExitCode)
	return r.ExitCode
}

// outputName names an output after its application, such as
// application_1700000000000_0042-report.html, so reports of different
// applications never overwrite each other in one folder. The ID was
// checked against appIDRE, so it is safe in a file name.
func outputName(appID, kind string) string {
	return appID + "-" + kind
}

// eventSourceBrief is the event log read, in a few words.
func eventSourceBrief(l *model.EventLog) string {
	var unpacked int64
	for _, f := range l.Stats.Files {
		unpacked += f.Decompressed
	}
	return fmt.Sprintf("%s events, %s", model.Num(l.Stats.Events), model.Bytes(unpacked))
}

func eventSourceDetail(l *model.EventLog, took time.Duration) (string, string) {
	st := l.Stats
	var raw, unpacked int64
	var bad []string
	for _, f := range st.Files {
		raw += f.Bytes
		unpacked += f.Decompressed
		if f.Error != "" {
			bad = append(bad, f.Name+": "+f.Error)
		}
	}
	detail := fmt.Sprintf("%s (%s), %d file(s), %s unpacked to %s, %s events in %s.",
		st.Layout, st.Codec, len(st.Files), model.Bytes(raw), model.Bytes(unpacked), model.Num(st.Events), took.Round(time.Millisecond))
	status := "read"
	if st.Malformed > 0 {
		detail += fmt.Sprintf(" %s malformed lines skipped (first at %s).", model.Num(st.Malformed), st.FirstMalformed)
		status = "partial"
	}
	if st.Truncated || len(bad) > 0 {
		status = "partial"
		for _, b := range bad {
			detail += " " + b + "."
		}
		if st.InProgress {
			detail += " The log is still in progress (.inprogress), so the end of the run is missing."
		}
	}
	return status, detail
}

func writeFile(path string, fn func(io.Writer) error) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".sparkplain-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := fn(tmp); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Chmod(outFileMode); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if s != "" {
			return s
		}
	}
	return ""
}

// parseSize reads sizes such as 10GiB, 500MB, 2g or 1048576 (bytes).
func parseSize(s string) int64 {
	s = strings.TrimSpace(strings.ToLower(s))
	i := 0
	for i < len(s) && (s[i] >= '0' && s[i] <= '9' || s[i] == '.') {
		i++
	}
	n, err := strconv.ParseFloat(s[:i], 64)
	if err != nil {
		return -1
	}
	mult := map[string]float64{"": 1, "b": 1, "k": 1 << 10, "kb": 1 << 10, "kib": 1 << 10, "m": 1 << 20, "mb": 1 << 20, "mib": 1 << 20,
		"g": 1 << 30, "gb": 1 << 30, "gib": 1 << 30, "t": 1 << 40, "tb": 1 << 40, "tib": 1 << 40}
	m, ok := mult[strings.TrimSpace(s[i:])]
	if !ok {
		return -1
	}
	return int64(n * m)
}

// parseFormats reads -format: a comma-separated list of html, json and
// explorer, where "both" means html and json.
func parseFormats(s string) (map[string]bool, error) {
	out := map[string]bool{}
	for _, f := range strings.Split(s, ",") {
		switch f = strings.TrimSpace(strings.ToLower(f)); f {
		case "html", "json", "explorer":
			out[f] = true
		case "both":
			out["html"], out["json"] = true, true
		case "":
		default:
			return nil, fmt.Errorf("-format takes html, json, explorer or both (comma-separated), not %q", f)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("-format names no outputs")
	}
	return out, nil
}

var errNoEventLog = errors.New("no event log location")

var errNoProfile = errors.New("AWS access needs -profile (pass -profile default for the default profile)")

func isS3(p string) bool { _, _, ok := source.ParseS3(p); return ok }

// awsDeps is how the CLI reaches AWS. Tests replace it: tests never call
// real AWS (CLAUDE.md).
var awsDeps = struct {
	config     func(ctx context.Context, profile, region string) (aws.Config, error)
	emr        func(cfg aws.Config) awsmeta.EMRAPI
	ec2        func(cfg aws.Config) awsmeta.EC2API
	cloudwatch func(cfg aws.Config) awsmeta.CloudWatchAPI
	cloudtrail func(cfg aws.Config) awsmeta.CloudTrailAPI
	sts        func(cfg aws.Config) STSAPI
	// now is the clock AWS retention windows are measured against.
	now func() time.Time
	s3  func(ctx context.Context, cfg aws.Config, bucket string) (source.Store, error)
}{
	config:     source.LoadAWS,
	emr:        awsmeta.NewEMR,
	ec2:        awsmeta.NewEC2,
	cloudwatch: awsmeta.NewCloudWatch,
	cloudtrail: awsmeta.NewCloudTrail,
	sts:        func(cfg aws.Config) STSAPI { return sts.NewFromConfig(cfg) },
	now:        time.Now,
	s3: func(ctx context.Context, cfg aws.Config, bucket string) (source.Store, error) {
		return source.OpenS3(ctx, cfg, bucket)
	},
}

// awsSession loads AWS credentials once, on first use, so offline runs
// never touch them.
type awsSession struct {
	profile, region string
	cfg             *aws.Config
	// described caches each cluster the access check described, so the run
	// does not ask again. Multiple entries are needed when Spark and HBase
	// run on different EMR clusters.
	mu        sync.Mutex // the maps below: the access check describes clusters in parallel
	described map[string]model.Cluster
	descErr   map[string]error
	// at is when the application's YARN started, from its ID; a cluster
	// named rather than given by ID is the one of that name up then.
	at time.Time
	// picked says which cluster each name found, and why.
	picked map[string]awsmeta.Pick
}

// appIDStartRE is the ResourceManager's start time in an application ID.
var appIDStartRE = regexp.MustCompile(`^application_(\d{13})_\d+$`)

// appClusterStart is when the application's YARN ResourceManager started:
// the time in its ID. Zero when the ID does not carry one.
func appClusterStart(appID string) time.Time {
	m := appIDStartRE.FindStringSubmatch(appID)
	if m == nil {
		return time.Time{}
	}
	ms, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return time.Time{}
	}
	return time.UnixMilli(ms)
}

// pickWhy says how a named cluster was found, for the console and the
// access check: "found by name etl: the one up when …", or "".
func (a *awsSession) pickWhy(name string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if p, ok := a.picked[name]; ok {
		return fmt.Sprintf("found by name %s: %s", p.Name, p.Why)
	}
	return ""
}

// regionName is the region the credentials use, once loaded.
func (a *awsSession) regionName() string {
	if a.cfg == nil {
		return a.region
	}
	return a.cfg.Region
}

func (a *awsSession) config(ctx context.Context) (aws.Config, error) {
	if a.profile == "" {
		// SPEC §2: the profile is always explicit, so it is clear whose
		// credentials read the logs.
		return aws.Config{}, errNoProfile
	}
	if a.cfg == nil {
		profile := a.profile
		if profile == "default" {
			profile = "" // the SDK's default chain, including environment credentials
		}
		cfg, err := awsDeps.config(ctx, profile, a.region)
		if err != nil {
			return cfg, fmt.Errorf("AWS credentials (profile %q): %w", a.profile, err)
		}
		a.cfg = &cfg
	}
	return *a.cfg, nil
}

// cluster finds the cluster by ID or name and describes it.
func (a *awsSession) cluster(ctx context.Context, id, name string) (model.Cluster, error) {
	return a.clusterFor(ctx, id, name, false)
}

// hbaseCluster is a separate HBase cluster, by ID or by name. The
// application's ID says nothing about it, so a name also finds the one
// running now when none of that name was up when the application's YARN
// started.
func (a *awsSession) hbaseCluster(ctx context.Context, id, name string) (model.Cluster, error) {
	return a.clusterFor(ctx, id, name, true)
}

func (a *awsSession) clusterFor(ctx context.Context, id, name string, upNowFallback bool) (model.Cluster, error) {
	key := id + "\x00" + name + "\x00" + strconv.FormatBool(upNowFallback)
	a.mu.Lock()
	c, ok := a.described[key]
	err, failed := a.descErr[key]
	a.mu.Unlock()
	if ok {
		return c, nil
	}
	if failed {
		return model.Cluster{}, err
	}
	c, err = a.describe(ctx, id, name, upNowFallback)
	a.mu.Lock()
	defer a.mu.Unlock()
	if err == nil {
		if a.described == nil {
			a.described = map[string]model.Cluster{}
		}
		a.described[key] = c
	} else if !errors.Is(err, errNoProfile) {
		if a.descErr == nil {
			a.descErr = map[string]error{}
		}
		a.descErr[key] = err
	}
	return c, err
}

func (a *awsSession) describe(ctx context.Context, id, name string, upNowFallback bool) (model.Cluster, error) {
	cfg, err := a.config(ctx)
	if err != nil {
		return model.Cluster{}, err
	}
	api := awsDeps.emr(cfg)
	if id == "" {
		p, err := awsmeta.PickByName(ctx, api, name, a.at, upNowFallback)
		if err != nil {
			return model.Cluster{}, err
		}
		a.mu.Lock()
		if a.picked == nil {
			a.picked = map[string]awsmeta.Pick{}
		}
		a.picked[name] = p
		a.mu.Unlock()
		id = p.ID
	}
	return awsmeta.Describe(ctx, api, id)
}

// resolve finds the event log at a local path or an s3:// location.
func (a *awsSession) resolve(ctx context.Context, loc, appID string, lim eventlog.Limits) (*eventlog.Input, error) {
	bucket, key, ok := source.ParseS3(loc)
	if !ok {
		return eventlog.Resolve(loc, appID, lim)
	}
	cfg, err := a.config(ctx)
	if err != nil {
		return nil, &eventlog.SourceError{Class: eventlog.ClassAccessDenied, Err: err}
	}
	st, err := awsDeps.s3(ctx, cfg, bucket)
	if err != nil {
		return nil, &eventlog.SourceError{Class: source.ClassOf(err), Err: err}
	}
	return eventlog.ResolveStore(ctx, st, key, appID, lim)
}

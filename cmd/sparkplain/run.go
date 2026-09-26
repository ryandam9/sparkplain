package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/analyze"
	"github.com/ryandam9/sparkplain/internal/sparkplain/eventlog"
	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/report"
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

var appIDRE = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$`)

type options struct {
	profile, region, configPath, clusterID, clusterName, appID string
	eventLog, from, out, format, maxSize                       string
	workers                                                    int
	timeout, windowPad                                         time.Duration
	noCloudWatch, noCloudTrail, showVersion                    bool
}

func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("sparkplain", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var o options
	fs.StringVar(&o.profile, "profile", "", "named AWS profile (online mode; phase 2)")
	fs.StringVar(&o.region, "region", "", "AWS region override (online mode; phase 2)")
	fs.StringVar(&o.configPath, "config", "", "YAML defaults file (default ~/.config/sparkplain/config.yaml)")
	fs.StringVar(&o.clusterID, "cluster-id", "", "EMR cluster ID (online mode; phase 2)")
	fs.StringVar(&o.clusterName, "cluster-name", "", "EMR cluster name (online mode; phase 2)")
	fs.StringVar(&o.appID, "app-id", "", "Spark application ID, e.g. application_1700000000000_0042 (required)")
	fs.StringVar(&o.eventLog, "eventlog", "", "event log: local file, rolling eventlog_v2_* folder, folder of logs, or History Server zip")
	fs.StringVar(&o.from, "from", "", "offline log folder (phase 2)")
	fs.StringVar(&o.out, "out", "", "output folder (default ~/sparkplain/<yyyy-mm-dd>/<app-id>/)")
	fs.StringVar(&o.format, "format", "", "outputs, comma-separated: html, json, explorer (default all three; both = html,json)")
	fs.IntVar(&o.workers, "workers", 16, "fetch concurrency (online mode; phase 2)")
	fs.StringVar(&o.maxSize, "max-size", "", "largest file or zip entry to read, e.g. 10GiB (default 10GiB)")
	fs.DurationVar(&o.timeout, "overall-timeout", 0, "deadline for the whole run (default 30m)")
	fs.BoolVar(&o.noCloudWatch, "no-cloudwatch", false, "skip CloudWatch enrichment (phase 3)")
	fs.BoolVar(&o.noCloudTrail, "no-cloudtrail", false, "skip CloudTrail enrichment (phase 3)")
	fs.DurationVar(&o.windowPad, "window-pad", 5*time.Minute, "padding on the AWS query window (phase 3)")
	fs.BoolVar(&o.showVersion, "version", false, "print the version and exit")
	fs.Usage = func() {
		fmt.Fprintf(stderr, "Usage: sparkplain -app-id <application id> -eventlog <path> [flags]\n\n")
		fmt.Fprintf(stderr, "Turns one Spark application's event log into report.html, report.json and explorer.html.\n\nFlags:\n")
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
	fail := func(format string, a ...any) int {
		fmt.Fprintf(stderr, "sparkplain: "+format+"\n", a...)
		return exitFatal
	}
	if fs.NArg() > 0 {
		return fail("unexpected argument %q (flags go before values, e.g. -eventlog <path>)", fs.Arg(0))
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })

	if o.appID == "" {
		fs.Usage()
		return fail("-app-id is required")
	}
	if !appIDRE.MatchString(o.appID) {
		return fail("-app-id %q does not look like a Spark application ID", o.appID)
	}
	for _, name := range []string{"profile", "cluster-id", "cluster-name", "from"} {
		if set[name] {
			return fail("-%s is for online mode, which arrives in phase 2. This version reads a local event log: pass -eventlog <path>", name)
		}
	}
	for _, name := range []string{"region", "workers", "no-cloudwatch", "no-cloudtrail", "window-pad"} {
		if set[name] {
			fmt.Fprintf(stderr, "sparkplain: note: -%s only applies to online mode and is ignored\n", name)
		}
	}

	cfgPath, explicit := o.configPath, o.configPath != ""
	if !explicit {
		cfgPath = defaultConfigPath()
	}
	cfg, err := loadConfig(cfgPath, explicit)
	if err != nil {
		return fail("%v", err)
	}
	outputs, err := parseFormats(firstNonEmpty(o.format, cfg.Format, "html,json,explorer"))
	if err != nil {
		return fail("%v", err)
	}
	maxSize := int64(10 << 30)
	if s := firstNonEmpty(o.maxSize, cfg.MaxSize); s != "" {
		if maxSize = parseSize(s); maxSize <= 0 {
			return fail("-max-size %q is not a size (try 10GiB or 500MiB)", s)
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
	evPath := o.eventLog
	if evPath == "" && cfg.EventLogPrefix != "" {
		evPath = cfg.EventLogPrefix
		fmt.Fprintf(stderr, "sparkplain: using eventlog-prefix %s from %s\n", evPath, cfgPath)
	}
	if evPath == "" {
		return fail("this version needs an event log: pass -eventlog <path> (a file, rolling folder, folder of logs or History Server zip)")
	}
	if strings.HasPrefix(evPath, "s3://") || strings.HasPrefix(evPath, "s3a://") {
		return fail("reading the event log from S3 arrives in phase 2. Download it (or use the History Server's Download button) and pass the local path")
	}
	outDir := firstNonEmpty(o.out, cfg.Out)
	if outDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fail("no home folder for the default output path; pass -out: %v", err)
		}
		outDir = filepath.Join(home, "sparkplain", time.Now().Format("2006-01-02"), o.appID)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	src := model.SourceStatus{Name: "Spark event log", Location: evPath}
	var log *model.EventLog
	in, err := eventlog.Resolve(evPath, o.appID, eventlog.Limits{MaxObjectBytes: maxSize})
	switch {
	case err != nil && eventlog.ErrorClass(err) == eventlog.ClassNotFound:
		return fail("%v", err)
	case err != nil:
		src.Status, src.Class, src.Detail = "error", eventlog.ErrorClass(err), err.Error()
		fmt.Fprintf(stderr, "sparkplain: could not read the event log (%s): %v\n", src.Class, err)
	default:
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
				fmt.Fprintln(stderr, "sparkplain: interrupted")
				return exitInterrupted
			}
			log.Stats.Notes = append(log.Stats.Notes, fmt.Sprintf("Reading stopped at the %s deadline (-overall-timeout); later events are missing.", timeout))
			log.Stats.Truncated = true
			src.Class = eventlog.ClassTimeout
		}
		if id := log.Application.ID; id != "" && id != o.appID {
			return fail("the event log at %s belongs to %s, not %s", evPath, id, o.appID)
		}
		src.Status, src.Detail = eventSourceDetail(log, time.Since(start))
		if log.Stats.Events == 0 {
			// Nothing usable: say why, and let every section show that it
			// needs the event log instead of an empty application.
			src.Status, src.Class = "error", eventlog.ClassCorrupt
			fmt.Fprintf(stderr, "sparkplain: no events could be read from %s\n", evPath)
			log = nil
		}
	}

	r := analyze.Run(analyze.Input{
		Tool:        "sparkplain " + version,
		Mode:        "offline-eventlog",
		GeneratedAt: time.Now(),
		TimeZone:    loc.String(),
		EventLog:    log,
		EventSource: src,
		Thresholds:  cfg.Thresholds.apply(analyze.DefaultThresholds()),
	})
	if r.Application.ID == "" {
		r.Application.ID = o.appID
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fail("creating %s: %v", outDir, err)
	}
	var written []string
	ropt := report.Options{Location: loc}
	if outputs["explorer"] {
		ropt.ExplorerHref = "explorer.html"
	}
	if outputs["html"] {
		p := filepath.Join(outDir, "report.html")
		if err := writeFile(p, func(w io.Writer) error { return report.WriteHTML(w, r, ropt) }); err != nil {
			return fail("writing %s: %v", p, err)
		}
		written = append(written, p)
	}
	if outputs["explorer"] {
		p := filepath.Join(outDir, "explorer.html")
		var x *model.Explorer
		if log != nil {
			x = log.Explorer
		}
		xopt := report.ExplorerOptions{}
		if outputs["html"] {
			xopt.ReportHref = "report.html"
		}
		if err := writeFile(p, func(w io.Writer) error { return report.WriteExplorer(w, r, x, xopt) }); err != nil {
			return fail("writing %s: %v", p, err)
		}
		written = append(written, p)
	}
	if outputs["json"] {
		p := filepath.Join(outDir, "report.json")
		if err := writeFile(p, func(w io.Writer) error { return report.WriteJSON(w, r) }); err != nil {
			return fail("writing %s: %v", p, err)
		}
		written = append(written, p)
	}
	crit, warn := 0, 0
	for _, f := range r.Findings {
		switch f.Severity {
		case model.Critical:
			crit++
		case model.Warning:
			warn++
		}
	}
	fmt.Fprintf(stdout, "%s: %s, %d findings (%d critical, %d warning)\n", r.Application.ID, r.Application.Status, len(r.Findings), crit, warn)
	for _, p := range written {
		fmt.Fprintln(stdout, "wrote", p)
	}
	if r.ExitCode == exitPartial {
		fmt.Fprintln(stderr, "sparkplain: partial report (exit 3): see the Sources panel for what is missing")
	}
	return r.ExitCode
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
	if err := tmp.Chmod(0o644); err != nil {
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

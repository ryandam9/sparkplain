package yarnlog

import (
	"context"
	"fmt"
	"io"
	"maps"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ryandam9/sparkplain/internal/sparkplain/model"
	"github.com/ryandam9/sparkplain/internal/sparkplain/source"
)

// Plan says which logs to read for one application under a cluster's log
// root (<LogUri>/<cluster-id>/, SPEC §2). Listing stays inside the
// application's own prefixes: its containers, the steps that may have
// submitted it, and the nodes it ran on.
type Plan struct {
	// Root is the log root's key prefix, such as "emr-logs/j-1ABC/", or ""
	// for a local copy of it.
	Root string
	// AppFolder says Root holds one application's container folders
	// directly (container_*/stderr.gz), as -from may give.
	AppFolder bool
	AppID     string
	// Steps are the step IDs to look through for the one that submitted
	// AppID (from ListSteps); nil looks through every step folder.
	Steps []string
	// Instances are the node folders to read (the nodes that ran the
	// application and the primary node); nil reads every one.
	Instances []string
	// Lifetimes are when each instance started and ended (zero while it
	// runs), so HBase's logs are listed only on nodes up during the run:
	// ListInstances names every instance a cluster ever had, and a long-lived
	// cluster that scales can have thousands. Nil lists them all.
	Lifetimes map[string][2]time.Time
	// Others maps the short host names of the cluster's other nodes to
	// their instance IDs. A node the application's own container logs name
	// as a driver's host (an earlier attempt's, which the event log does
	// not describe) is read too.
	Others map[string]string
	// Since skips NodeManager and ResourceManager logs last written before
	// it, which end before the application started. Zero keeps all.
	Since time.Time
	// Until is when the application ended. With Since, it keeps HBase's
	// logs, which hold every application, to the application's time; when
	// both are zero, the container logs' first and last times are used.
	Until time.Time
	// WindowFrom says where Since and Until came from, for the report.
	WindowFrom string
	Limits     source.Limits
	// SkipHBase leaves HBase Master and region-server logs for a separate
	// collector, for example when HBase runs on another EMR cluster.
	SkipHBase bool
	// Off names the sources the run was told not to read ("Step logs",
	// "Node logs", "HBase server logs") and why; each gets a Sources row
	// saying so, and nothing of it is listed or read, except the
	// NodeManager logs of the nodes that ran the application (AppNodes).
	Off map[string]string
	// HostIDs maps the short host name of every instance to its ID, so
	// the nodes the application's own logs name can be found (AppNodes).
	// Guessed says Instances is a guess, every node up during the run, as
	// there was no event log to say which ran the application: once the
	// container logs name them, only those and Primary are read.
	HostIDs map[string]string
	Guessed bool
	Primary []string
	// MaxFiles caps the files read per kind of log; default 5000.
	MaxFiles int
	// Loc is the zone the cluster writes log times (and the hour in rolled
	// HBase log names) in; nil is UTC.
	Loc *time.Location
}

// Collection is what Collect read.
type Collection struct {
	Files []model.LogFile
	// Steps are the steps that submitted the application.
	Steps []string
	// Sources are the Sources-panel rows: container, step and node logs.
	Sources []model.SourceStatus
	// HBaseNodes is how many nodes' applications/hbase/ folders were
	// listed (0 when the whole node/ folder was), and HBaseOther the other
	// files found there, which are not Master or region server logs: say
	// what was there when none of them is.
	HBaseNodes int
	HBaseOther []string
	// AppNodes are the instances the application's container logs say
	// ran its driver or executors.
	AppNodes []string
}

// Collect lists and reads the application's container, step and node logs
// and classifies each. It never fails: what cannot be listed or read is
// reported in the Sources rows with its error class.
func Collect(ctx context.Context, st source.Store, p Plan) Collection {
	if p.MaxFiles <= 0 {
		p.MaxFiles = 5000
	}
	var c Collection
	c.containers(ctx, st, p)
	if p.AppFolder {
		for _, name := range []string{"Step logs", "Node logs"} {
			c.Sources = append(c.Sources, model.SourceStatus{Name: name, Status: "none", Location: st.Location(p.Root),
				Detail: "The -from folder holds one application's containers, so it has no step or node logs. Pass a copy of the cluster's whole log folder to read them."})
		}
		return c
	}
	if !c.off(st, p, "Step logs", "steps/") {
		c.steps(ctx, st, p)
	}
	p = p.withDriverNodes(c.Files)
	c.AppNodes = appNodes(c.Files, p.HostIDs)
	if p.Guessed && len(c.AppNodes) > 0 {
		p.Instances = append(slices.Clone(p.Primary), c.AppNodes...)
	}
	switch {
	case p.Off["Node logs"] == "":
		c.nodes(ctx, st, p)
	case len(c.AppNodes) > 0:
		// Off, but the nodes that ran the application are always read:
		// their NodeManager logs hold what YARN offered on each.
		q := p
		q.Instances = c.AppNodes
		c.nodes(ctx, st, q)
		if r := &c.Sources[len(c.Sources)-1]; r.Name == "Node logs" {
			nodes := model.Plural(len(c.AppNodes), "node", "nodes") + " that ran this application"
			if r.Status == "read" || r.Status == "partial" {
				r.Detail = fmt.Sprintf("Node logs are off (%s), but those of the %s were read anyway, for what YARN offered there; no other node's. ", p.Off["Node logs"], nodes) + r.Detail
			} else {
				// Nothing there: the source stays off, as asked.
				r.Status, r.Class = "not-requested", ""
				r.Detail = fmt.Sprintf("Not read: %s. The %s had no NodeManager logs to read anyway.", p.Off["Node logs"], nodes)
			}
		}
	default:
		c.off(st, p, "Node logs", "node/")
	}
	if !p.SkipHBase && !c.off(st, p, "HBase server logs", "node/*/applications/hbase/") {
		c.hbase(ctx, st, p.WithWindow(c.Files))
	}
	return c
}

// off reports whether the run was told not to read a source, and adds its
// Sources row saying so.
func (c *Collection) off(st source.Store, p Plan, name, under string) bool {
	why, ok := p.Off[name]
	if ok {
		c.Sources = append(c.Sources, model.SourceStatus{Name: name, Status: "not-requested", Location: st.Location(p.Root) + under,
			Detail: "Not read: " + why + "."})
	}
	return ok
}

// CollectHBase reads only HBase Master and region-server logs. It is used
// when those logs live under a different EMR cluster's log root.
func CollectHBase(ctx context.Context, st source.Store, p Plan) Collection {
	if p.MaxFiles <= 0 {
		p.MaxFiles = 5000
	}
	var c Collection
	c.hbase(ctx, st, p)
	return c
}

// WithWindow fills the application's time from its logs, when neither
// the event log nor the caller gave it (see Window), and says where it came
// from.
func (p Plan) WithWindow(files []model.LogFile) Plan {
	if !p.Since.IsZero() && !p.Until.IsZero() {
		return p
	}
	since, until, from := Window(files, p.AppID)
	if p.Since.IsZero() {
		p.Since = since
	}
	if p.Until.IsZero() {
		p.Until = until
	}
	if p.WindowFrom == "" {
		p.WindowFrom = from
	}
	return p
}

// Window is when the application ran, from its logs: YARN's application
// summary (the ResourceManager's record of its start and finish, exact)
// when the logs hold it, else the first and last times its containers'
// logs carry, recognised lines or not. From says which; zero times and ""
// when neither is there.
func Window(files []model.LogFile, appID string) (since, until time.Time, from string) {
	for _, f := range files {
		for _, l := range f.Found {
			if l.Kind != model.LogAppSummary || appID != "" && l.Fields["appId"] != "" && l.Fields["appId"] != appID {
				continue
			}
			s, _ := strconv.ParseInt(l.Fields["startTime"], 10, 64)
			e, _ := strconv.ParseInt(l.Fields["finishTime"], 10, 64)
			if s > 0 && e >= s {
				return time.UnixMilli(s).UTC(), time.UnixMilli(e).UTC(), "YARN's application summary"
			}
		}
	}
	widen := func(t time.Time) {
		if t.IsZero() {
			return
		}
		if since.IsZero() || t.Before(since) {
			since = t
		}
		if t.After(until) {
			until = t
		}
	}
	for _, f := range files {
		if f.Container == "" {
			continue
		}
		widen(f.FirstTime)
		widen(f.LastTime)
		for _, l := range f.Found {
			widen(l.Time)
		}
	}
	if !since.IsZero() {
		from = "the times its container logs cover"
	}
	return since, until, from
}

// upDuringRun reports whether an instance was up at some point of the
// application's time, when both are known.
func (p Plan) upDuringRun(id string) bool {
	life, ok := p.Lifetimes[id]
	if !ok {
		return true
	}
	started, ended := life[0], life[1]
	switch {
	case !p.Until.IsZero() && !started.IsZero() && started.After(p.Until):
		return false // joined after the run
	case !p.Since.IsZero() && !ended.IsZero() && ended.Before(p.Since):
		return false // gone before it
	}
	return true
}

// hbase reads HBase's Master and region server logs on every node, for the
// application's time only: a region server can serve the application from
// a node that ran none of its executors.
func (c *Collection) hbase(ctx context.Context, st source.Store, p Plan) {
	prefix := p.Root + "node/"
	src := model.SourceStatus{Name: "HBase server logs", Location: st.Location(prefix)}
	var objs []source.Object
	var listErr error
	if p.Instances == nil {
		objs, listErr = st.List(ctx, prefix)
	} else {
		var ids []string
		for _, id := range append(slices.Clone(p.Instances), slices.Collect(maps.Values(p.Others))...) {
			if !slices.Contains(ids, id) && p.upDuringRun(id) {
				ids = append(ids, id)
			}
		}
		sort.Strings(ids)
		c.HBaseNodes = len(ids)
		var mu sync.Mutex
		forEach(ids, p.Limits.Workers, func(id string) {
			found, err := st.List(ctx, prefix+id+"/applications/hbase/")
			mu.Lock()
			if err != nil && listErr == nil {
				listErr = err
			}
			objs = append(objs, found...)
			mu.Unlock()
		})
		sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })
	}
	// HBase rolls its logs each hour for as long as the cluster runs, so
	// only the hours of the application are read. Those outside it are
	// counted, not listed one by one: a long-lived cluster has thousands.
	g := group{st: st, plan: p}
	hbaseLogs, outside := 0, 0
	for _, o := range objs {
		f := Describe(strings.TrimPrefix(o.Key, p.Root))
		if f.Kind != HBaseMaster && f.Kind != HBaseRegion {
			if strings.Contains(o.Key, "/applications/hbase/") {
				c.HBaseOther = append(c.HBaseOther, path.Base(o.Key))
			}
			continue
		}
		hbaseLogs++
		switch {
		case !f.Hour.IsZero() && !p.Since.IsZero() && InZone(f.Hour, p.Loc, f.Kind).Add(time.Hour).Before(p.Since),
			!f.Hour.IsZero() && !p.Until.IsZero() && InZone(f.Hour, p.Loc, f.Kind).After(p.Until),
			f.Hour.IsZero() && !p.Since.IsZero() && !o.Modified.IsZero() && o.Modified.Before(p.Since):
			outside++
		default:
			g.offer(o, f, true, "")
		}
	}
	if hbaseLogs == 0 {
		if listErr != nil {
			c.Sources = append(c.Sources, listFailed(src, listErr))
		}
		return // no HBase on this cluster, or none in these folders
	}
	if p.Since.IsZero() && p.Until.IsZero() {
		// Without the application's time every hour the cluster ever logged
		// would qualify, and the cap would keep the oldest: read none.
		src.Status = "not-supplied"
		src.Detail = fmt.Sprintf("Found %s, but nothing says when the application ran (no event log, no YARN application summary, and no times in its container logs), so none was read: they cover the cluster's whole life, not this run. Pass -eventlog (the History Server's \"Download\", or a copy in S3), or read the node logs, which hold YARN's summary.",
			model.Plural(hbaseLogs, "HBase log", "HBase logs"))
		c.Sources = append(c.Sources, src)
		return
	}
	skipped := ""
	if outside > 0 {
		skipped = fmt.Sprintf(" %s outside it skipped.", model.Plural(outside, "hourly log", "hourly logs"))
	}
	g.read(ctx)
	switch {
	case len(g.picked) == 0:
		src.Status = "not-supplied"
		src.Detail = "No HBase log covers the application's time." + skipped
	default:
		src.Status, src.Class = g.status()
		if listErr != nil {
			src.Status = "partial"
		}
		src.Detail = g.summary() + ", kept to the application's time" + p.windowText() + "." + skipped + g.problems()
		src.Brief = model.Plural(len(g.files), "file", "files")
		if !p.Since.IsZero() && !p.Until.IsZero() {
			src.Brief += fmt.Sprintf(", %s–%s %s", p.Since.In(p.zone()).Format("15:04"), p.Until.In(p.zone()).Format("15:04"), p.Until.In(p.zone()).Format("MST"))
		}
	}
	src.Files = g.sourceFiles
	c.Files = append(c.Files, g.files...)
	c.Sources = append(c.Sources, src)
}

// withDriverNodes adds the nodes the containers name as a driver's host
// to the node folders read, and reads their logs from that driver's start.
func (p Plan) withDriverNodes(files []model.LogFile) Plan {
	if p.Instances == nil || len(p.Others) == 0 {
		return p
	}
	have := map[string]bool{}
	for _, id := range p.Instances {
		have[id] = true
	}
	for _, f := range files {
		for _, l := range f.Found {
			id := p.Others[shortHost(l.Fields["host"])]
			if l.Kind != model.LogDriverHost || id == "" || have[id] {
				continue
			}
			have[id] = true
			p.Instances = append(slices.Clip(p.Instances), id)
			if !l.Time.IsZero() && (p.Since.IsZero() || l.Time.Before(p.Since)) {
				p.Since = l.Time
			}
		}
	}
	return p
}

// AppHosts are the short host names the application's own container
// logs name for its driver and executors: YarnAllocator's "Launching
// container … on host H", an executor's "Starting executor ID n on host
// H", and the driver's host.
func AppHosts(files []model.LogFile) []string {
	seen := map[string]bool{}
	var out []string
	add := func(h string) {
		if h = shortHost(h); h != "" && !seen[h] {
			seen[h] = true
			out = append(out, h)
		}
	}
	for _, f := range files {
		for _, l := range f.Found {
			if l.Kind == model.LogExecutorHost || l.Kind == model.LogDriverHost {
				add(l.Fields["host"])
			}
		}
		for _, e := range f.DriverEvents {
			if e.Kind == model.DrvContainer {
				add(e.Host)
			}
		}
	}
	sort.Strings(out)
	return out
}

// appNodes are the instance IDs of the hosts AppHosts names.
func appNodes(files []model.LogFile, ids map[string]string) []string {
	var out []string
	for _, h := range AppHosts(files) {
		if id := ids[h]; id != "" && !slices.Contains(out, id) {
			out = append(out, id)
		}
	}
	return out
}

// shortHost is a host name without its domain, or an IP address as the
// name EMR gives it, so ip-10-0-2-10.ec2.internal, 10.0.2.10 and
// ip-10-0-2-10.us-east-1.compute.internal match.
func shortHost(h string) string {
	h = strings.ToLower(strings.TrimSpace(h))
	if strings.Count(h, ".") == 3 && strings.Trim(h, "0123456789.") == "" {
		return "ip-" + strings.ReplaceAll(h, ".", "-")
	}
	if i := strings.IndexByte(h, '.'); i > 0 {
		return h[:i]
	}
	return h
}

func (c *Collection) containers(ctx context.Context, st source.Store, p Plan) {
	prefix := p.Root + "containers/" + p.AppID + "/"
	if p.AppFolder {
		prefix = p.Root
	}
	src := model.SourceStatus{Name: "Container logs", Location: st.Location(prefix)}
	objs, err := st.List(ctx, prefix)
	if err != nil {
		c.Sources = append(c.Sources, listFailed(src, err))
		return
	}
	g := group{st: st, plan: p}
	for _, o := range objs {
		f := Describe("containers/" + p.AppID + "/" + strings.TrimPrefix(o.Key, prefix))
		g.offer(o, f, f.Kind == ContainerStderr || f.Kind == ContainerStdout, "not a log sparkplain reads")
	}
	g.read(ctx)
	containers := map[string]bool{}
	for _, f := range g.files {
		containers[f.Container] = true
	}
	switch {
	case len(g.picked) == 0:
		src.Status = "not-supplied"
		src.Detail = "No container logs for this application. EMR copies them to the cluster's log URI every few minutes and when the application ends; a cluster without a log URI keeps them only on its nodes, which are gone once it ends."
	default:
		src.Status, src.Class = g.status()
		src.Detail = fmt.Sprintf("%s from %d containers.%s", g.summary(), len(containers), g.problems())
		src.Brief = fmt.Sprintf("%s from %s", model.Plural(len(g.files), "file", "files"), model.Plural(len(containers), "container", "containers"))
	}
	src.Files = g.sourceFiles
	c.Files = append(c.Files, g.files...)
	c.Sources = append(c.Sources, src)
}

func (c *Collection) steps(ctx context.Context, st source.Store, p Plan) {
	prefix := p.Root + "steps/"
	src := model.SourceStatus{Name: "Step logs", Location: st.Location(prefix)}
	cands := p.Steps
	var objs []source.Object
	if cands == nil {
		all, err := st.List(ctx, prefix)
		if err != nil {
			c.Sources = append(c.Sources, listFailed(src, err))
			return
		}
		objs = all
	} else {
		var mu sync.Mutex
		var firstErr error
		forEach(cands, p.Limits.Workers, func(step string) {
			found, err := st.List(ctx, prefix+step+"/")
			mu.Lock()
			defer mu.Unlock()
			if err != nil && firstErr == nil {
				firstErr = err
			}
			objs = append(objs, found...)
		})
		if firstErr != nil && len(objs) == 0 {
			c.Sources = append(c.Sources, listFailed(src, firstErr))
			return
		}
		sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })
	}
	// Read every candidate's stderr to find the step that submitted the
	// application, then that step's controller log.
	g := group{st: st, plan: p}
	var controllers []source.Object
	for _, o := range objs {
		f := Describe(strings.TrimPrefix(o.Key, p.Root))
		switch f.Kind {
		case StepStderr:
			g.offer(o, f, true, "")
		case StepController, StepStdout: // read for the step that submitted the application
			controllers = append(controllers, o)
		default:
			g.offer(o, f, false, "not a log sparkplain reads")
		}
	}
	g.read(ctx)
	mine := map[string]bool{}
	var kept []model.LogFile
	for _, f := range g.files {
		for _, l := range f.Found {
			if l.Kind == model.LogSubmitted && l.Fields["app"] == p.AppID {
				mine[f.Step] = true
			}
		}
	}
	for _, f := range g.files {
		if mine[f.Step] {
			kept = append(kept, f)
		}
	}
	for i := range g.sourceFiles {
		if s := &g.sourceFiles[i]; s.Status == "read" && !mine[Describe(strings.TrimPrefix(g.keys[i], p.Root)).Step] {
			s.Detail = "another application's step"
		}
	}
	cg := group{st: st, plan: p}
	for _, o := range controllers {
		f := Describe(strings.TrimPrefix(o.Key, p.Root))
		cg.offer(o, f, mine[f.Step], "another application's step")
	}
	cg.read(ctx)
	kept = append(kept, cg.files...)
	for s := range mine {
		c.Steps = append(c.Steps, s)
	}
	sort.Strings(c.Steps)
	src.Files = append(g.sourceFiles, cg.sourceFiles...)
	switch {
	case len(mine) == 0 && g.failed() == 0:
		src.Status = "none"
		src.Detail = fmt.Sprintf("Looked through %d step logs; no step submitted this application. It may have been started on the primary node, by Livy or from a notebook.", len(g.picked))
	case len(mine) == 0:
		src.Status, src.Class = g.status()
		src.Detail = fmt.Sprintf("No readable step log names this application.%s", g.problems())
	default:
		src.Status, src.Class = cg.status()
		if g.failed() > 0 {
			src.Status = "partial"
		}
		src.Detail = fmt.Sprintf("Step %s submitted this application. Read %s.%s", strings.Join(c.Steps, ", "), filesSummary(kept), g.problems()+cg.problems())
		src.Brief = "step " + strings.Join(c.Steps, ", ") + " submitted it"
	}
	c.Files = append(c.Files, kept...)
	c.Sources = append(c.Sources, src)
}

func (c *Collection) nodes(ctx context.Context, st source.Store, p Plan) {
	prefix := p.Root + "node/"
	src := model.SourceStatus{Name: "Node logs", Location: st.Location(prefix)}
	var objs []source.Object
	var listErr error
	if p.Instances == nil {
		objs, listErr = st.List(ctx, prefix)
	} else {
		var mu sync.Mutex
		forEach(p.Instances, p.Limits.Workers, func(id string) {
			for _, sub := range []string{"/applications/hadoop-yarn/", "/bootstrap-actions/"} {
				found, err := st.List(ctx, prefix+id+sub)
				mu.Lock()
				if err != nil && listErr == nil {
					listErr = err
				}
				objs = append(objs, found...)
				mu.Unlock()
			}
		})
		sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })
	}
	if listErr != nil && len(objs) == 0 {
		c.Sources = append(c.Sources, listFailed(src, listErr))
		return
	}
	g := group{st: st, plan: p}
	for _, o := range objs {
		f := Describe(strings.TrimPrefix(o.Key, p.Root))
		switch f.Kind {
		case NodeManager, ResourceManager:
			if !p.Since.IsZero() && !o.Modified.IsZero() && o.Modified.Before(p.Since) {
				g.offer(o, f, false, "last written before the application started")
				continue
			}
			g.offer(o, f, true, "")
		case Bootstrap:
			g.offer(o, f, true, "")
		default:
			if p.Instances == nil {
				continue // a whole local node folder: only the logs above are of interest
			}
			g.offer(o, f, false, "not a log sparkplain reads")
		}
	}
	g.read(ctx)
	nodes := map[string]bool{}
	for _, f := range g.files {
		nodes[f.Instance] = true
	}
	switch {
	case len(g.picked) == 0:
		src.Status = "not-supplied"
		src.Detail = "No NodeManager, ResourceManager or bootstrap logs for the nodes this application ran on."
	default:
		src.Status, src.Class = g.status()
		if listErr != nil {
			src.Status = "partial"
		}
		src.Detail = fmt.Sprintf("%s from %d nodes.%s", g.summary(), len(nodes), g.problems())
		src.Brief = fmt.Sprintf("%s from %s", model.Plural(len(g.files), "file", "files"), model.Plural(len(nodes), "node", "nodes"))
	}
	src.Files = g.sourceFiles
	c.Files = append(c.Files, g.files...)
	c.Sources = append(c.Sources, src)
}

// group reads one kind of log: the objects offered, the ones picked, and
// what came of each.
type group struct {
	st          source.Store
	plan        Plan
	picked      []source.Object
	pickedFiles []File
	keys        []string // key of each sourceFiles row
	sourceFiles []model.SourceFile
	files       []model.LogFile
	errs        map[string]int // error class → count
}

// offer adds an object: picked to read, or listed as skipped and why.
func (g *group) offer(o source.Object, f File, pick bool, why string) {
	if pick && len(g.picked) >= g.plan.MaxFiles {
		pick, why = false, fmt.Sprintf("over the %d-file limit for this kind of log", g.plan.MaxFiles)
	}
	if !pick {
		g.keys = append(g.keys, o.Key)
		g.sourceFiles = append(g.sourceFiles, model.SourceFile{Location: g.st.Location(o.Key), Bytes: o.Size, Status: "skipped", Detail: why})
		return
	}
	g.picked = append(g.picked, o)
	g.pickedFiles = append(g.pickedFiles, f)
}

// read fetches and classifies the picked objects.
func (g *group) read(ctx context.Context) {
	if len(g.picked) == 0 {
		return
	}
	g.errs = map[string]int{}
	// One result per file read: an object, or each entry of a zip. Fetch
	// calls fn once per zip entry, all from the object's own goroutine, so
	// each object's slice is appended to by one goroutine only.
	results := make([][]entryResult, len(g.picked))
	index := map[string]int{}
	for i, o := range g.picked {
		index[o.Key] = i
		g.plan.Limits.Progress.Add(o.Size)
	}
	reads := source.Fetch(ctx, g.st, g.picked, g.plan.Limits, func(o source.Object, name string, r io.Reader) error {
		i := index[o.Key]
		if len(results[i]) == 0 { // the object's first file: count it once
			defer g.plan.Limits.Progress.Done()
			defer g.plan.Limits.Progress.Read(o.Size)
		}
		cr := &countReader{r: r}
		// name is the key, or key!entry inside a zip: lines cite the entry.
		res, err := Classify(cr, g.st.Location(name), g.pickedFiles[i], Options{AppID: g.plan.AppID, From: g.plan.Since, To: g.plan.Until, Loc: g.plan.Loc})
		bytes := o.Size // as stored, like every other log file
		if name != o.Key {
			bytes = cr.n // a zip entry's own size, unpacked
		}
		results[i] = append(results[i], entryResult{res: res, bytes: bytes})
		return err
	})
	for i, rd := range reads {
		f := g.pickedFiles[i]
		row := model.SourceFile{Location: rd.Where, Bytes: rd.Object.Size, Status: "read"}
		if rd.Err != nil {
			row.Status, row.Class, row.Detail = "error", source.ClassOf(rd.Err), rd.Err.Error()
			g.errs[row.Class]++
		}
		g.keys = append(g.keys, rd.Object.Key)
		g.sourceFiles = append(g.sourceFiles, row)
		for _, er := range results[i] {
			res := er.res
			if res.Read == 0 && rd.Err != nil {
				continue
			}
			g.files = append(g.files, model.LogFile{Location: res.Name, Kind: string(f.Kind), Container: f.Container, Step: f.Step,
				Instance: f.Instance, Host: f.Host, Bytes: er.bytes, Lines: res.Read, Dropped: res.Dropped, Found: res.Lines, HBaseSplits: res.Splits, HBaseScans: res.Scans, HBaseRegionEvents: res.RegionEvents, DriverEvents: res.DriverEvents,
				TaskLogs: res.TaskLogs, TaskLogsCut: res.TaskLogsCut, Untied: res.Untied, StageData: res.StageData, DataUntied: res.DataUntied,
				FirstTime: res.FirstTime, LastTime: res.LastTime})
		}
	}
}

// entryResult is what one file (or zip entry) gave, and its size.
type entryResult struct {
	res   Result
	bytes int64
}

// countReader counts the bytes read through it.
type countReader struct {
	r io.Reader
	n int64
}

func (c *countReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (g *group) failed() int {
	n := 0
	for _, v := range g.errs {
		n += v
	}
	return n
}

// status is read, partial (some files failed) or error (all failed), with
// the most common error class.
func (g *group) status() (string, string) {
	n := g.failed()
	switch {
	case n == 0:
		return "read", ""
	case n == len(g.picked):
		return "error", g.topClass()
	}
	return "partial", g.topClass()
}

func (g *group) topClass() string {
	best, bestN := "", 0
	for c, n := range g.errs {
		if n > bestN || (n == bestN && c < best) {
			best, bestN = c, n
		}
	}
	return best
}

func (g *group) summary() string { return "Read " + filesSummary(g.files) }

func filesSummary(files []model.LogFile) string {
	var bytes, lines int64
	found := 0
	for _, f := range files {
		bytes += f.Bytes
		lines += f.Lines
		for _, l := range f.Found {
			found += l.Count
		}
	}
	return fmt.Sprintf("%d files (%s compressed, %s lines; %s recognised)", len(files), model.Bytes(bytes), model.Num(lines), model.Num(int64(found)))
}

// problems says what could not be read, by error class.
func (g *group) problems() string {
	if g.failed() == 0 {
		return ""
	}
	var parts []string
	for c, n := range g.errs {
		parts = append(parts, fmt.Sprintf("%d %s", n, c))
	}
	sort.Strings(parts)
	return fmt.Sprintf(" %d of %d files could not be read (%s); the Files list says which.", g.failed(), len(g.picked), strings.Join(parts, ", "))
}

func listFailed(src model.SourceStatus, err error) model.SourceStatus {
	src.Status, src.Class = "error", source.ClassOf(err)
	src.Detail = "Could not list the logs: " + err.Error()
	return src
}

// forEach runs fn over items with at most workers at once.
func forEach(items []string, workers int, fn func(string)) {
	if workers <= 0 {
		workers = 16
	}
	sem := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for _, it := range items {
		wg.Add(1)
		sem <- struct{}{}
		go func(it string) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(it)
		}(it)
	}
	wg.Wait()
}

// LogRoot returns the bucket and key prefix of a cluster's log root from
// its LogUri (s3://, s3n:// or s3a://), such as "emr-logs/j-1ABC/".
func LogRoot(logURI, clusterID string) (bucket, prefix string, ok bool) {
	bucket, key, ok := source.ParseS3(logURI)
	if !ok {
		return "", "", false
	}
	key = strings.Trim(key, "/")
	return bucket, strings.TrimPrefix(path.Join(key, clusterID)+"/", "/"), true
}

// zone is the cluster's log zone, UTC when not set.
func (p Plan) zone() *time.Location {
	if p.Loc == nil {
		return time.UTC
	}
	return p.Loc
}

// windowText names the window HBase's logs were kept to, in the cluster's
// log zone: " (2026-10-02 09:12–09:17 AEST, from the times its container
// logs cover)".
func (p Plan) windowText() string {
	if p.Since.IsZero() || p.Until.IsZero() {
		return ""
	}
	since, until := p.Since.In(p.zone()), p.Until.In(p.zone())
	layout := "15:04"
	if since.YearDay() != until.YearDay() || since.Year() != until.Year() {
		layout = "2006-01-02 15:04"
	}
	s := " (" + since.Format("2006-01-02 15:04") + "–" + until.Format(layout) + " " + until.Format("MST")
	if p.WindowFrom != "" {
		s += ", from " + p.WindowFrom
	}
	return s + ")"
}

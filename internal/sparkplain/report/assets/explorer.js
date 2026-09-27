// sparkplain explorer: draws the whole page from the JSON embedded in
// #sp-data. Every value goes into the page through textContent, never as
// markup, so nothing from the event log can inject HTML.
(function () {
  "use strict";
  var D = JSON.parse(document.getElementById("sp-data").textContent);
  var root = document.documentElement;
  var main = document.getElementById("sp-main");

  // ---------- helpers ----------
  function el(tag, attrs) {
    var n = document.createElement(tag);
    if (attrs) Object.keys(attrs).forEach(function (k) {
      var v = attrs[k];
      if (v == null || v === false) return;
      if (k === "text") n.textContent = v;
      else if (k === "cls") n.className = v;
      else if (k.slice(0, 2) === "on") n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v === true ? "" : v);
    });
    for (var i = 2; i < arguments.length; i++) add(n, arguments[i]);
    return n;
  }
  function add(n, c) {
    if (c == null || c === false) return;
    if (Array.isArray(c)) { c.forEach(function (x) { add(n, x); }); return; }
    n.appendChild(typeof c === "object" ? c : document.createTextNode(String(c)));
  }
  function link(href, text, cls) { return el("a", { href: href, text: text, cls: cls }); }
  function objs(t) {
    return t.rows.map(function (r) {
      var o = {};
      t.cols.forEach(function (c, i) { o[c] = r[i]; });
      return o;
    });
  }
  function colIdx(cols) { var m = {}; cols.forEach(function (c, i) { m[c] = i; }); return m; }

  var nf = new Intl.NumberFormat();
  function num(n) { return n == null ? "—" : nf.format(n); }
  function bytes(b) {
    if (b == null) return "—";
    var u = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"], i = 0, v = b;
    while (Math.abs(v) >= 1024 && i < u.length - 1) { v /= 1024; i++; }
    return (i === 0 ? String(v) : v.toFixed(1)) + " " + u[i];
  }
  function dur(ms) {
    if (ms == null) return "—";
    if (ms < 1000) return Math.round(ms) + " ms";
    var s = ms / 1000;
    if (s < 60) return (s < 10 ? s.toFixed(1) : Math.round(s)) + " s";
    var m = Math.floor(s / 60), rs = Math.round(s - m * 60);
    if (rs === 60) { m++; rs = 0; }
    if (m < 60) return m + " min" + (rs ? " " + rs + " s" : "");
    var h = Math.floor(m / 60), rm = m - h * 60;
    if (h < 48) return h + " h" + (rm ? " " + rm + " min" : "");
    var d = Math.floor(h / 24), rh = h - d * 24;
    return d + " d" + (rh ? " " + rh + " h" : "");
  }
  function pct(f) { return f == null || !isFinite(f) ? "—" : (f * 100 < 10 ? (f * 100).toFixed(1) : Math.round(f * 100)) + "%"; }
  var tfmt = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
  var hmfmt = new Intl.DateTimeFormat(undefined, { hour: "2-digit", minute: "2-digit", hour12: false });
  var dfmt = new Intl.DateTimeFormat(undefined, { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
  function when(ms, full) {
    if (!ms) return "—";
    var d = new Date(ms);
    return el("time", { datetime: d.toISOString(), title: d.toISOString(), text: (full ? dfmt : tfmt).format(d) });
  }
  function span(a, b) { return a && b ? b - a : null; }
  var STATUS = { succeeded: "Succeeded", failed: "Failed", running: "Running", incomplete: "Incomplete", skipped: "Skipped", pending: "Pending", unknown: "Unknown", killed: "Killed" };
  function status(s) { return el("span", { cls: "st" }, el("span", { cls: "statusdot " + (s || "") }), STATUS[s] || s || "Unknown"); }
  var REF = { stage: "#stage/", job: "#job/", executor: "#executor/", query: "#query/" };
  function refHref(ref) {
    var i = (ref || "").indexOf(":");
    return i > 0 && REF[ref.slice(0, i)] ? REF[ref.slice(0, i)] + encodeURIComponent(ref.slice(i + 1)) : null;
  }
  function src(s) { return s ? (D.files[s[0]] || "?") + ":" + s[1] : ""; }
  function fact(label, value, explain) {
    return el("div", { cls: "fact" }, el("span", { cls: "l", text: label }), el("span", { cls: "v" }, value), explain ? el("span", { cls: "x", text: explain }) : null);
  }
  function section(title, lede) {
    var s = el("section");
    if (title) s.appendChild(el("div", { cls: "sechead" }, el("h2", { text: title })));
    if (lede) s.appendChild(el("p", { cls: "lede", text: lede }));
    return s;
  }
  function explain(text) { return el("p", { cls: "explain", text: text }); }

  // ---------- data ----------
  var jobs = objs(D.jobs), stages = objs(D.stages), execs = objs(D.executors), queries = objs(D.sql), rdds = objs(D.rdds);
  // Container, step and node logs, and what was found in each line.
  var logFiles = D.logs || [], LC = colIdx(D.logCols || []), logByLoc = {};
  logFiles.forEach(function (f, i) {
    f.i = i;
    logByLoc[f.loc] = i;
    f.rows = f.found.map(function (r) { var o = {}; (D.logCols || []).forEach(function (c, k) { o[c] = r[k]; }); o.file = f; return o; });
  });
  var LOG_KIND = { exception: "Exception", traceback: "Python traceback", "out-of-memory": "Out of memory", "memory-kill": "Memory kill", "container-exit": "Container exit",
    "app-exit": "Application master exit", "lost-executor": "Lost executor", "task-error": "Task error", signal: "Signal", "access-denied": "Access denied",
    kerberos: "Kerberos", metastore: "Metastore", hbase: "HBase", identity: "Identity", submit: "spark-submit command", submitted: "Submitted application",
    resource: "Uploaded file", "step-status": "Step status", "app-report": "YARN report", "app-summary": "YARN summary", bootstrap: "Bootstrap", error: "Error" };
  var FILE_KIND = { "container-stderr": "Container stderr", "container-stdout": "Container stdout", "step-controller": "Step controller", "step-stderr": "Step stderr",
    nodemanager: "NodeManager", resourcemanager: "ResourceManager", bootstrap: "Bootstrap log", "bootstrap-output": "Bootstrap action output" };
  function sevPill(sev) {
    var k = { critical: "crit", warning: "part", info: "info" }[sev] || "info";
    return el("span", { cls: "pill " + k, text: { critical: "Critical", warning: "Warning", info: "Info" }[sev] || sev });
  }
  // logHref links a cited file:line to the log's page when it is one of
  // the logs read.
  function logHref(loc) {
    var m = /^(.*):(\d+)(?:-\d+)?$/.exec(loc || "");
    if (!m || logByLoc[m[1]] == null) return null;
    return "#logs/" + logByLoc[m[1]] + ":" + m[2];
  }
  // shortLoc is a log's path below the cluster's log folder.
  function shortLoc(loc) {
    var m = /(?:^|\/)((?:containers|steps|node)\/.*)$/.exec(loc);
    return m ? m[1] : loc;
  }
  var SRC_LABEL = { read: "Read", partial: "Partly read", error: "Could not read", "not-supplied": "Not supplied", none: "Nothing for this app", "not-requested": "Not requested", "not-yet": "Not in this version" };
  function logWho(f) {
    if (f.exec === "driver") return "driver";
    if (f.exec === "am") return "application master";
    if (f.exec) return "executor " + f.exec;
    if (f.container) return f.container;
    if (f.step) return "step " + f.step;
    return f.host || f.instance || "";
  }
  var exclusions = objs(D.exclusions), runningTasks = objs(D.runningTasks), blockKinds = objs(D.blockKinds);
  // extLink opens a log link in a new tab; such links point at the cluster's
  // NodeManagers, which may be gone once the cluster ends.
  function extLink(href, text) { return el("a", { href: href, text: text, target: "_blank", rel: "noopener noreferrer" }); }
  function linkMap(m) {
    var ks = Object.keys(m || {}).sort();
    return ks.length ? el("span", null, ks.map(function (k, i) { return [i ? " · " : "", /^https?:\/\//.test(m[k]) ? extLink(m[k], k) : k + ": " + m[k]]; })) : null;
  }
  var T = colIdx(D.taskCols), C = colIdx(D.cellCols);
  var jobByID = {}, stagesByID = {}, execByID = {}, queryByID = {};
  jobs.forEach(function (j) { jobByID[j.id] = j; });
  stages.forEach(function (s) { s.key = s.id + "." + s.attempt; (stagesByID[s.id] = stagesByID[s.id] || []).push(s); });
  execs.forEach(function (x) { execByID[x.id] = x; });
  queries.forEach(function (q) { queryByID[q.id] = q; });
  function stageLink(s) { return link("#stage/" + s.key, s.attempt ? s.id + " (attempt " + (s.attempt + 1) + ")" : String(s.id)); }
  function execLink(id) { return id == null ? "—" : link("#executor/" + encodeURIComponent(id), id); }
  function execName(i) { return D.execs[i]; }
  function jobTasks(j) {
    var ok = 0, all = 0;
    (j.stages || []).forEach(function (id) { (stagesByID[id] || []).forEach(function (s) { ok += s.ok; all += s.tasks; }); });
    return [ok, all];
  }
  function queryStatus(q) { return q.error ? "failed" : q.end ? "succeeded" : "running"; }

  // ---------- table ----------
  // cols: {h, v: row => sort value, f: row => display, num, title}
  function table(opts) {
    var wrap = el("div", { cls: "tblbox" });
    var state = { sort: opts.sort == null ? -1 : opts.sort, dir: opts.dir || "desc", q: "", shown: opts.page || 200, extra: null };
    var tools = el("div", { cls: "bar-tools" });
    var count = el("span", { cls: "count" });
    if (opts.filter) {
      var box = el("input", { cls: "filter", type: "search", placeholder: opts.filter, "aria-label": opts.filter });
      box.addEventListener("input", function () { state.q = box.value.trim().toLowerCase(); state.shown = opts.page || 200; draw(); });
      tools.appendChild(box);
    }
    if (opts.select) {
      var sel = el("select", { "aria-label": opts.select.label });
      opts.select.options.forEach(function (o) { sel.appendChild(el("option", { value: o[0], text: o[1] })); });
      sel.addEventListener("change", function () { state.extra = sel.value; state.shown = opts.page || 200; draw(); });
      tools.appendChild(sel);
    }
    tools.appendChild(count);
    if (opts.filter || opts.select) wrap.appendChild(tools);
    var box2 = el("div", { cls: "tbl" });
    var t = el("table", { cls: opts.cls });
    var head = el("tr");
    opts.cols.forEach(function (c, i) {
      var th = el("th", { cls: c.num ? "num" : null, title: c.title, text: c.h });
      if (c.v) {
        th.setAttribute("data-sort", c.num ? "num" : "text");
        th.setAttribute("tabindex", "0");
        var go = function () { state.dir = state.sort === i && state.dir === "desc" ? "asc" : "desc"; state.sort = i; draw(); };
        th.addEventListener("click", go);
        th.addEventListener("keydown", function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); go(); } });
      }
      head.appendChild(th);
    });
    t.appendChild(el("thead", null, head));
    var body = el("tbody");
    t.appendChild(body);
    box2.appendChild(t);
    wrap.appendChild(box2);
    var more = el("button", { type: "button", cls: "more", hidden: true });
    more.addEventListener("click", function () { state.shown += opts.page || 200; draw(); });
    wrap.appendChild(more);
    function draw() {
      var rows = opts.rows.filter(function (r) {
        if (opts.select && state.extra && !opts.select.test(r, state.extra)) return false;
        return !state.q || opts.text(r).toLowerCase().indexOf(state.q) >= 0;
      });
      if (state.sort >= 0) {
        var c = opts.cols[state.sort], k = state.dir === "asc" ? 1 : -1;
        rows = rows.slice().sort(function (a, b) {
          var x = c.v(a), y = c.v(b);
          if (x == null) x = c.num ? -Infinity : "";
          if (y == null) y = c.num ? -Infinity : "";
          return (c.num ? x - y : String(x).localeCompare(String(y), undefined, { numeric: true })) * k;
        });
      }
      head.querySelectorAll("th").forEach(function (th, i) {
        th.removeAttribute("data-dir");
        th.setAttribute("aria-sort", "none");
        if (i === state.sort) { th.setAttribute("data-dir", state.dir); th.setAttribute("aria-sort", state.dir === "asc" ? "ascending" : "descending"); }
      });
      body.textContent = "";
      rows.slice(0, state.shown).forEach(function (r) {
        var tr = el("tr", { cls: opts.rowCls ? opts.rowCls(r) : null });
        opts.cols.forEach(function (c) { tr.appendChild(el("td", { cls: c.num ? "num" : null }, c.f(r))); });
        body.appendChild(tr);
      });
      if (!rows.length) body.appendChild(el("tr", null, el("td", { colspan: opts.cols.length, text: opts.empty || "Nothing to show." })));
      count.textContent = rows.length === opts.rows.length ? num(rows.length) + " rows" : num(rows.length) + " of " + num(opts.rows.length) + " rows";
      more.hidden = rows.length <= state.shown;
      more.textContent = "Show " + num(Math.min(opts.page || 200, rows.length - state.shown)) + " more of " + num(rows.length - state.shown);
    }
    draw();
    return wrap;
  }
  function numCol(h, key, fmt, title) { return { h: h, num: true, title: title, v: function (r) { return r[key]; }, f: function (r) { return (fmt || num)(r[key]); } }; }

  // ---------- charts (drawn by the D3 kit in the chart layer) ----------
  var charts = [];
  function chartSlot(cls, draw) {
    var d = el("div", { cls: "chart xchart " + (cls || "") }, el("div", { cls: "wait", text: "Drawing chart…" }));
    charts.push({ el: d, draw: draw });
    return d;
  }

  // ---------- header ----------
  var a = D.app;
  document.getElementById("sp-tool").textContent = D.tool;
  var head = document.getElementById("sp-head");
  add(head, el("h1", null, a.name || "Spark application", " ", el("span", { cls: "status " + ({ succeeded: "ok", failed: "crit", incomplete: "warn", running: "warn" }[a.status] || "na"), text: STATUS[a.status] || "Unknown" })));
  add(head, el("div", { cls: "idline" },
    el("span", null, "App ", el("b", { cls: "mono", text: a.id || "unknown" }), a.attempt ? " · attempt " + a.attempt : ""),
    a.spark ? el("span", null, el("b", { text: "Spark " + a.spark }), a.master ? " · " + a.master : "", a.deploy ? " · " + a.deploy + " mode" : "") : null,
    a.user ? el("span", null, "User ", el("b", { text: a.user })) : null,
    a.start ? el("span", null, el("b", { cls: "num" }, when(a.start, true), " → ", a.end ? when(a.end) : "still running"), " · ", dur(a.duration)) : null,
    a.driverLogs && Object.keys(a.driverLogs).length ? el("span", { title: "Links to the cluster; they stop working once it is gone" }, "Driver logs ", linkMap(a.driverLogs)) : null));
  if (D.reportHref) { var rl = document.getElementById("sp-report"); rl.href = D.reportHref; rl.hidden = false; }
  var zone = "";
  try { zone = Intl.DateTimeFormat().resolvedOptions().timeZone || ""; } catch (e) {}
  document.querySelectorAll(".tzlabel").forEach(function (n) { n.textContent = "Times are in " + (zone || "your browser's time zone") + " (your browser)."; });

  // Theme toggle, shared with the report.
  try {
    var saved = localStorage.getItem("sparkplain-theme");
    if (saved === "light" || saved === "dark") root.setAttribute("data-theme", saved);
  } catch (e) {}
  var tb = document.getElementById("themebtn");
  tb.hidden = false;
  tb.addEventListener("click", function () {
    var dark = root.getAttribute("data-theme") === "dark" || (!root.getAttribute("data-theme") && window.matchMedia && matchMedia("(prefers-color-scheme: dark)").matches);
    root.setAttribute("data-theme", dark ? "light" : "dark");
    try { localStorage.setItem("sparkplain-theme", dark ? "light" : "dark"); } catch (e) {}
    redrawCharts();
  });

  // ---------- views ----------
  var TABS = [["overview", "Overview"]].concat(D.anatomy ? [["anatomy", "At a glance"]] : [], [["timeline", "Timeline"], ["jobs", "Jobs", jobs.length], ["stages", "Stages", stages.length], ["executors", "Executors", execs.length],
    ["sql", "SQL / DataFrame", queries.length], ["storage", "Storage", rdds.length], ["code", "Code"], ["environment", "Environment"], ["log", "Event log"]]);
  if (D.aws) TABS.push(["cluster", "Cluster", D.aws.nodes.length]);
  if (logFiles.length || (D.logSources || []).length) TABS.push(["logs", "Logs", logFiles.length]);
  var tabs = document.getElementById("sp-tabs");
  TABS.forEach(function (t) { tabs.appendChild(el("a", { href: "#" + t[0], "data-tab": t[0] }, t[1], t[2] != null ? el("span", { cls: "n", text: num(t[2]) }) : null)); });

  var views = {};
  views.overview = function () {
    var out = [];
    if ((D.accessGaps || []).length) {
      out.push(el("div", { cls: "missing access" }, el("h3", { text: "No access to " + D.accessGaps.length + (D.accessGaps.length === 1 ? " source" : " sources") + ": parts of this page are missing" }),
        el("ul", null, D.accessGaps.map(function (g) { return el("li", null, el("b", { text: g.source }), ": without it this page cannot show " + g.missing + ". It needs " + g.needs + "."); })),
        el("p", { text: "sparkplain carried on with everything else. The Logs tab lists each error." })));
    }
    var story = el("div", { cls: "story" }, el("h2", { text: "What happened" }));
    story.appendChild(el("ul", null, (D.summary || []).map(function (s) { return el("li", { text: s }); })));
    out.push(story);
    if (D.kpis && D.kpis.length) {
      out.push(el("div", { cls: "kpis" }, D.kpis.map(function (k) {
        return el("div", { cls: "kpi" }, el("span", { cls: "l", text: k.label }), el("span", { cls: "v" + (k.tone ? " " + k.tone : "") }, k.value, k.unit ? el("small", { text: " " + k.unit }) : null), el("span", { cls: "x", text: k.explain }));
      })));
    }
    var ov = section("Over time", "Tasks running across the run, and when each job ran.");
    ov.appendChild(chartSlot("", "running"));
    ov.appendChild(chartSlot("tall", "jobsTimeline"));
    ov.appendChild(chartSlot("", "dataOverTime"));
    out.push(ov);
    var fs = section("Findings", D.findings.length ? "Problems and notes found in this run. Evidence links open the stage, job or executor it concerns." : "No findings for this run.");
    var list = el("div", { cls: "findings" });
    D.findings.forEach(function (f, i) {
      var sev = { critical: "crit", warning: "warn", info: "info" }[f.sev] || "info";
      list.appendChild(el("article", { cls: "finding " + sev, id: "finding-" + (i + 1) }, el("div", { cls: "stripe" }), el("div", { cls: "body" },
        el("div", { cls: "t" }, el("span", { cls: "fnum " + sev, text: String(i + 1) }), el("span", { cls: "pill " + ({ crit: "crit", warn: "part", info: "info" }[sev]), text: { crit: "Critical", warn: "Warning", info: "Info" }[sev] }), el("h3", { text: f.title })),
        el("div", { cls: "fpart what" }, el("span", { cls: "k", text: { crit: "Error", warn: "Problem", info: "Note" }[sev] }), el("div", null, el("p", { text: f.expl }))),
        (f.ev || []).length ? el("div", { cls: "fpart evid" }, el("span", { cls: "k", text: "Evidence" }), el("div", null, f.ev.map(function (e) {
          var h = refHref(e[1]), lh = logHref(e[2]);
          return el("div", { cls: "ev" }, h ? link(h, e[0]) : e[0], e[2] ? [" · ", lh ? link(lh, e[2]) : e[2]] : "");
        }))) : null,
        f.fix ? el("div", { cls: "fpart try" }, el("span", { cls: "k", text: "Try" }), el("div", null, el("p", { cls: "fix", text: f.fix }))) : null)));
    });
    fs.appendChild(list);
    out.push(fs);
    if (runningTasks.length) {
      var rs = section("Still running when the log ended", "These tasks started but the log has no end for them: the application was still running when the log was copied, or it stopped without closing the log." + (D.runningCapped ? " More tasks were running than are listed." : ""));
      rs.appendChild(runningTable(runningTasks));
      out.push(rs);
    }
    if (D.critical && D.critical.length) {
      var cp = section("Critical path", "The chain of stages that set how long the longest job (job " + D.criticalJob + ") took: each waited for the one before it. Speeding up anything else would not shorten that job.");
      cp.appendChild(el("p", null, D.critical.map(function (id, i) { var st = (stagesByID[id] || [])[0]; return [i ? " → " : "", st ? stageLink(st) : String(id), st ? " (" + dur(span(st.submitted, st.completed)) + ")" : ""]; })));
      out.push(cp);
    }
    var slow = stages.filter(function (s) { return s.completed && s.submitted; }).sort(function (x, y) { return (y.completed - y.submitted) - (x.completed - x.submitted); }).slice(0, 5);
    if (slow.length) {
      var ss = section("Longest stages", "Where the run spent its time. Open one for its task summary, slowest tasks and per-executor breakdown.");
      ss.appendChild(stageTable(slow, { page: 5 }));
      out.push(ss);
    }
    if (D.notes.length) {
      var ns = el("div", { cls: "missing" }, el("h3", { text: "About this data" }), el("ul", null, D.notes.map(function (n) { return el("li", { text: n }); })));
      out.push(ns);
    }
    return out;
  };

  function runningTable(rows) {
    return table({
      rows: rows, sort: 0, dir: "asc", page: 100,
      cols: [
        numCol("Task", "task"),
        { h: "Stage", num: true, v: function (t) { return t.stage; }, f: function (t) { return link("#stage/" + t.stage + "." + t.stageAttempt, String(t.stage)); } },
        numCol("Partition", "partition"),
        { h: "Attempt", num: true, v: function (t) { return t.attempt; }, f: function (t) { return num(t.attempt + 1) + (t.spec ? " (speculative)" : ""); } },
        { h: "Executor", v: function (t) { return t.exec; }, f: function (t) { return execLink(t.exec); } },
        { h: "Host", v: function (t) { return t.host; }, f: function (t) { return t.host; } },
        { h: "Launched", num: true, v: function (t) { return t.launched; }, f: function (t) { return when(t.launched); } },
        { h: "Running for", num: true, title: "From launch to the last event in the log", v: function (t) { return t.launched; }, f: function (t) { return dur(lastEvent - t.launched); } }
      ]
    });
  }
  function exclusionTable(rows) {
    return table({
      rows: rows, sort: 0, dir: "asc", page: 100,
      cols: [
        { h: "When", num: true, v: function (x) { return x.time; }, f: function (x) { return when(x.time); } },
        { h: "What", v: function (x) { return x.kind + x.target; }, f: function (x) { return x.kind === "executor" ? el("span", null, "Executor ", execLink(x.target)) : "Node " + x.target; } },
        { h: "For", v: function (x) { return x.scope; }, f: function (x) { return x.scope === "stage" ? el("span", null, "Stage ", link("#stage/" + x.stage + "." + x.stageAttempt, String(x.stage))) : "The whole application"; } },
        { h: "Because of", num: true, v: function (x) { return x.failures; }, f: function (x) { return x.kind === "executor" ? num(x.failures) + " failed task" + (x.failures === 1 ? "" : "s") : num(x.failures) + " excluded executor" + (x.failures === 1 ? "" : "s"); } },
        { h: "Lifted", num: true, v: function (x) { return x.lifted; }, f: function (x) { return x.lifted ? el("span", null, when(x.lifted), el("span", { cls: "sub", text: "after " + dur(x.lifted - x.time) })) : (x.scope === "stage" ? "when the stage ended" : "not in this log"); } }
      ]
    });
  }
  var EXCL_EXPLAIN = "With spark.excludeOnFailure.enabled, Spark stops scheduling on an executor (or a whole node) after tasks fail there, for one stage or the rest of the application, until spark.excludeOnFailure.timeout passes.";
  var lastEvent = Math.max(a.end || 0, (function () { var m = 0; runningTasks.forEach(function (t) { m = Math.max(m, t.launched); }); stages.forEach(function (s) { m = Math.max(m, s.completed || s.submitted || 0); }); return m; })());
  function jobTable(rows, o) {
    o = o || {};
    return table({
      rows: rows, sort: 0, dir: "asc", page: o.page, filter: o.noFilter ? null : "Filter jobs by ID, description or status",
      text: function (j) { return j.id + " " + (j.desc || "") + " " + (j.name || "") + " " + j.status + " " + (j.group || ""); },
      rowCls: function (j) { return j.status === "failed" ? "failedrow" : null; },
      cols: [
        { h: "Job", num: true, v: function (j) { return j.id; }, f: function (j) { return link("#job/" + j.id, String(j.id)); } },
        { h: "Description", v: function (j) { return j.desc || j.name; }, f: function (j) { return el("span", null, j.desc || j.name, j.desc && j.name ? el("span", { cls: "sub", text: j.name }) : null); } },
        { h: "Status", v: function (j) { return j.status; }, f: function (j) { return status(j.status); } },
        { h: "Submitted", num: true, v: function (j) { return j.submitted; }, f: function (j) { return when(j.submitted); } },
        { h: "Duration", num: true, v: function (j) { return span(j.submitted, j.completed); }, f: function (j) { return dur(span(j.submitted, j.completed)); } },
        { h: "Stages", num: true, v: function (j) { return j.stages.length; }, f: function (j) { return num(j.stages.length); } },
        { h: "Tasks done", num: true, title: "Successful tasks out of all task attempts", v: function (j) { return jobTasks(j)[1]; }, f: function (j) { var t = jobTasks(j); return num(t[0]) + " / " + num(t[1]); } },
        { h: "Query", num: true, v: function (j) { return j.sql; }, f: function (j) { return j.sql == null ? "—" : link("#query/" + j.sql, String(j.sql)); } }
      ]
    });
  }
  views.timeline = function () {
    var s = section("Timeline", "The whole run on one time axis: what Spark was running, on how many executors, and when the cluster waited on the driver.");
    s.appendChild(chartSlot("", "runTimeline"));
    return s;
  };

  views.jobs = function () {
    var s = section("Jobs", "A job is one action in the code, such as count, collect or a write. Each runs as one or more stages.");
    s.appendChild(chartSlot("tall", "jobsTimeline"));
    s.appendChild(jobTable(jobs));
    return s;
  };
  views.job = function (id) {
    var j = jobByID[id];
    if (!j) return notFound("Job " + id);
    var s = section("Job " + j.id + ": " + (j.desc || j.name));
    s.insertBefore(el("div", { cls: "crumbs" }, link("#jobs", "Jobs"), " / job " + j.id), s.firstChild);
    s.appendChild(el("div", { cls: "facts" },
      fact("Status", status(j.status)),
      fact("Ran", el("span", null, when(j.submitted, true), " → ", when(j.completed)), dur(span(j.submitted, j.completed))),
      j.sql != null ? fact("SQL query", link("#query/" + j.sql, "Query " + j.sql), "The DataFrame or SQL query this job belongs to.") : null,
      j.group ? fact("Job group", j.group) : null,
      fact("Code", j.name, "Where in the code the action ran.")));
    if (j.failure) s.appendChild(el("pre", { cls: "plan", text: j.failure }));
    s.appendChild(codePanel(j.code, j.submitted, j.id));
    var jp = Object.keys(j.props || {}).sort();
    if (jp.length) {
      s.appendChild(el("h3", { text: "Settings for this job" }));
      s.appendChild(explain("Properties this job carried that differ from the application's settings: the scheduler pool, job group, and settings the code changed in its session. Values of keys that look like secrets are redacted."));
      s.appendChild(el("div", { cls: "tbl" }, el("table", null, el("thead", null, el("tr", null, el("th", { text: "Property" }), el("th", { text: "Value" }))),
        el("tbody", null, jp.map(function (k) { return el("tr", null, el("td", { cls: "mono", text: k }), el("td", { cls: "mono", text: j.props[k] })); })))));
    }
    if (j.failureStack) s.appendChild(el("details", null, el("summary", { text: "Stack trace of the job's failure" }), el("div", { cls: "inner" }, el("pre", { cls: "plan", text: j.failureStack }))));
    var jw = el("div", { cls: "dagwrap", hidden: true });
    s.appendChild(jw);
    jobDag(jw, j.id, null);
    var rows = [];
    j.stages.forEach(function (sid) { (stagesByID[sid] || []).forEach(function (st) { rows.push(st); }); });
    s.appendChild(el("h3", { text: "Stages" }));
    s.appendChild(stageTable(rows, { noFilter: true }));
    if (j.src) s.appendChild(el("p", { cls: "srcref", text: "Job start: " + src(j.src) }));
    return s;
  };

  function skew(s) { return s.p50 > 0 && s.tasks > 1 ? s.max / s.p50 : null; }
  function stageTable(rows, o) {
    o = o || {};
    return table({
      rows: rows, sort: o.sort == null ? 0 : o.sort, dir: o.dir || "asc", page: o.page,
      filter: o.noFilter ? null : "Filter stages by ID, name or status",
      select: o.noFilter ? null : { label: "Stage status", options: [["", "All statuses"], ["succeeded", "Succeeded"], ["failed", "Failed"], ["skipped", "Skipped"], ["running", "Running or incomplete"]],
        test: function (s, v) { return v === "running" ? s.status === "running" || s.status === "incomplete" : s.status === v; } },
      text: function (s) { return s.key + " " + s.name + " " + s.status; },
      rowCls: function (s) { return s.status === "failed" ? "failedrow" : null; },
      cols: [
        { h: "Stage", num: true, v: function (s) { return s.id + s.attempt / 100; }, f: stageLink },
        { h: "Name", v: function (s) { return s.name; }, f: function (s) { return s.name; } },
        { h: "Status", v: function (s) { return s.status; }, f: function (s) { return status(s.status); } },
        { h: "Submitted", num: true, v: function (s) { return s.submitted; }, f: function (s) { return when(s.submitted); } },
        { h: "Duration", num: true, v: function (s) { return span(s.submitted, s.completed); }, f: function (s) { return dur(span(s.submitted, s.completed)); } },
        { h: "Tasks", num: true, title: "Successful tasks / all attempts; failures in red", v: function (s) { return s.tasks; }, f: function (s) { return el("span", null, num(s.ok) + " / " + num(s.tasks), s.failed ? el("span", { cls: "bad", text: " · " + num(s.failed) + " failed" }) : null); } },
        { h: "CPU share", num: true, title: "Task CPU time as a share of task run time; low means the tasks mostly waited (I/O, shuffle, GC)", v: function (s) { return s.run ? s.cpuNs / 1e6 / s.run : null; }, f: function (s) { var c = s.run ? s.cpuNs / 1e6 / s.run : null; return el("span", { cls: c != null && c < 0.3 && s.run > 60000 ? "warnv" : null, text: pct(c) }); } },
        numCol("Input", "input", bytes, "Bytes read from files and tables"),
        numCol("Output", "output", bytes, "Bytes written to files and tables"),
        numCol("Shuffle read", "shRead", bytes, "Bytes fetched from other stages' output"),
        numCol("Shuffle write", "shWrite", bytes, "Bytes written for later stages to read"),
        numCol("Disk spill", "diskSpill", bytes, "Data that did not fit in memory and went to local disk"),
        { h: "Slowest ÷ median", num: true, title: "How much longer the slowest task took than the median one", v: skew, f: function (s) { var k = skew(s); return k == null ? "—" : el("span", { cls: k > 5 ? "warnv" : null, text: k.toFixed(1) + "×" }); } }
      ]
    });
  }
  views.stages = function () {
    var s = section("Stages", "A stage is a set of tasks that run the same code on different partitions of the data. Retried stages show each attempt.");
    s.appendChild(chartSlot("", "stageTimes"));
    s.appendChild(chartSlot("", "stageData"));
    if (stages.some(function (st) { return st.diskSpill > 0 || st.memSpill > 0; })) s.appendChild(chartSlot("", "stageSpill"));
    s.appendChild(chartSlot("", "stageSplit"));
    s.appendChild(stageTable(stages));
    return s;
  };

  var METRICS = [
    ["durationMs", "Duration", dur, "Wall-clock time from launch to finish."],
    ["runTimeMs", "Executor run time", dur, "Time the task's code ran on the executor."],
    ["gcTimeMs", "GC time", dur, "Time the JVM spent collecting garbage during the task."],
    ["deserializeMs", "Task deserialization", dur, "Time to unpack the task before running it; high on a cold executor."],
    ["deserializeCpuMs", "Deserialization CPU", dur, "CPU time spent unpacking the task."],
    ["schedulerDelayMs", "Scheduler delay", dur, "Time not spent running, unpacking or returning the task: launch overhead and waiting on the driver."],
    ["resultSerializationMs", "Result serialization", dur, "Time to package the task's result for the driver."],
    ["gettingResultMs", "Getting result", dur, "Time the driver spent fetching a result too big to send directly (over spark.task.maxDirectResultSize)."],
    ["resultSizeBytes", "Result size", bytes, "Size of the result each task sent to the driver."],
    ["recordsRead", "Rows read", num, "Rows from input plus shuffle."],
    ["inputBytes", "Input size", bytes, "Bytes read from files and tables."],
    ["shuffleReadBytes", "Shuffle read", bytes, "Bytes fetched from earlier stages."],
    ["shuffleRecordsRead", "Shuffle rows read", num, "Rows fetched from earlier stages."],
    ["shuffleRemoteBytes", "Shuffle read from other hosts", bytes, "Shuffle bytes fetched over the network rather than from this host."],
    ["shuffleRemoteToDiskBytes", "Shuffle fetched to disk", bytes, "Remote shuffle blocks too big for memory, written to disk as they arrived."],
    ["shuffleRemoteRequestsMs", "Shuffle fetch requests", dur, "Time the remote shuffle fetch requests took."],
    ["shuffleFetchWaitMs", "Shuffle fetch wait", dur, "Time spent waiting for shuffle data to arrive."],
    ["shuffleWriteBytes", "Shuffle write", bytes, "Bytes written for later stages."],
    ["shuffleWriteTimeMs", "Shuffle write time", dur, "Time spent writing shuffle files."],
    ["memorySpillBytes", "Memory spill", bytes, "Size in memory of data that had to be spilled."],
    ["diskSpillBytes", "Disk spill", bytes, "Bytes written to local disk because data did not fit."],
    ["peakExecutionMemory", "Peak execution memory", bytes, "Most memory the task used for sorts, joins and aggregations."],
    ["outputBytes", "Output size", bytes, "Bytes written to files and tables."]
  ];
  var SKEWY = { durationMs: 1, runTimeMs: 1, recordsRead: 1, inputBytes: 1, shuffleReadBytes: 1, shuffleRecordsRead: 1, schedulerDelayMs: 1, resultSizeBytes: 1 };
  var LOC = ["process-local", "node-local", "rack-local", "any host", "no preference"];
  var LOC_EXPLAIN = "Where each task ran relative to its data: in the same executor (process-local), on the same host (node-local), in the same rack, or anywhere.";
  function taskRows(rows) {
    return rows.map(function (r) {
      var o = {};
      D.taskCols.forEach(function (c, i) { o[c] = r[i]; });
      o.execID = execName(o.exec);
      o.at = o.launch && D.t0 ? D.t0 + o.launch : o.launch;
      return o;
    });
  }
  function taskTable(rows, o) {
    var TS = ["succeeded", "failed", "killed"];
    return table({
      rows: taskRows(rows), sort: o.sort, dir: "desc", page: 50,
      rowCls: function (t) { return t.status === 1 ? "failedrow" : null; },
      cols: [
        numCol("Task", "task"), numCol("Index", "index"), numCol("Partition", "part"),
        { h: "Attempt", num: true, v: function (t) { return t.attempt; }, f: function (t) { return num(t.attempt + 1) + (t.spec ? " (speculative)" : ""); } },
        { h: "Executor", v: function (t) { return t.execID; }, f: function (t) { return execLink(t.execID); } },
        { h: "Status", v: function (t) { return t.status; }, f: function (t) { return status(TS[t.status]); } },
        { h: "Launched", num: true, v: function (t) { return t.at; }, f: function (t) { return when(t.at); } },
        numCol("Duration", "dur", dur), numCol("Run", "run", dur), numCol("GC", "gc", dur), numCol("Deserialize", "deser", dur),
        numCol("Fetch wait", "fetch", dur), numCol("Rows read", "rows"), numCol("Input", "input", bytes),
        numCol("Shuffle read", "shRead", bytes), numCol("Shuffle write", "shWrite", bytes), numCol("Spill", "spill", bytes),
        { h: "Locality", title: LOC_EXPLAIN, v: function (t) { return t.loc; }, f: function (t) { return LOC[t.loc] || "—"; } },
        numCol("Scheduler delay", "sched", dur, "Launch overhead and waiting on the driver"), numCol("Result size", "result", bytes),
        { h: "Log line", v: function (t) { return t.line; }, f: function (t) { return el("span", { cls: "srcref", text: t.file >= 0 ? (D.files[t.file] || "?") + ":" + t.line : "" }); } }
      ]
    });
  }
  views.stage = function (key) {
    var st = null;
    stages.forEach(function (s) { if (s.key === key || (!st && String(s.id) === key)) st = s; });
    if (!st) return notFound("Stage " + key);
    var det = D.detail[st.key];
    var s = section("Stage " + st.id + (st.attempt ? " (attempt " + (st.attempt + 1) + ")" : "") + ": " + st.name);
    s.insertBefore(el("div", { cls: "crumbs" }, link("#stages", "Stages"), " / stage " + st.key), s.firstChild);
    var jl = el("span");
    st.jobs.forEach(function (id, i) { if (i) jl.appendChild(document.createTextNode(", ")); jl.appendChild(link("#job/" + id, "Job " + id)); });
    var pl = el("span");
    st.parents.forEach(function (id, i) { if (i) pl.appendChild(document.createTextNode(", ")); var p = (stagesByID[id] || [])[0]; pl.appendChild(p ? stageLink(p) : document.createTextNode(String(id))); });
    s.appendChild(el("div", { cls: "facts" },
      fact("Status", status(st.status)),
      fact("Ran", el("span", null, when(st.submitted, true), " → ", when(st.completed)), dur(span(st.submitted, st.completed))),
      fact("Tasks", num(st.ok) + " succeeded of " + num(st.tasks) + " attempts", (st.failed ? num(st.failed) + " failed, " : "") + (st.killed ? num(st.killed) + " killed, " : "") + num(st.numTasks) + " partitions."),
      fact("Task time", dur(st.dur), "All task attempts' durations added up. CPU time " + dur(st.cpuNs / 1e6) + " (" + pct(st.run ? st.cpuNs / 1e6 / st.run : null) + " of run time)."),
      fact("Jobs", st.jobs.length ? jl : "—", "The actions this stage ran for."),
      fact("Runs after", st.parents.length ? pl : "Nothing: it reads its input directly.", "Stages whose output this stage reads."),
      st.taskType ? fact("Kind", st.taskType === "ResultTask" ? "Result stage" : "Shuffle map stage", st.taskType === "ResultTask" ? "Its tasks return results to the driver or write output." : "Its tasks write shuffle files that later stages read.") : null,
      st.tasks ? fact("Data locality", LOC.map(function (l, i) { return st.loc[i] ? num(st.loc[i]) + " " + l : null; }).filter(Boolean).join(", ") || "—", LOC_EXPLAIN) : null,
      st.gettingMs ? fact("Large results", dur(st.gettingMs) + " spent fetching results", "Results over spark.task.maxDirectResultSize go through the block manager, and the driver fetches them separately. " + bytes(st.resultSize) + " of results in all.") : null,
      st.push && (st.push[0] || st.push[2] || st.push[4]) ? fact("Push-based shuffle", bytes(st.push[1] + st.push[3]) + " read from merged shuffle files", num(st.push[0] + st.push[2]) + " merged blocks; " + num(st.push[4]) + " fell back to unmerged blocks" + (st.push[5] ? "; " + num(st.push[5]) + " corrupt chunks" : "") + ".") : null,
      st.cacheWrites && st.cacheWrites[0] ? fact("Cache writes", num(st.cacheWrites[0]) + " blocks, " + bytes(st.cacheWrites[1]), "Blocks the tasks stored in the cache.") : null));
    var stEx = exclusions.filter(function (e) { return e.scope === "stage" && e.stage === st.id && e.stageAttempt === st.attempt; });
    if (stEx.length) { s.appendChild(el("h3", { text: "Exclusions during this stage" })); s.appendChild(explain(EXCL_EXPLAIN)); s.appendChild(exclusionTable(stEx)); }
    var stRun = runningTasks.filter(function (t) { return t.stage === st.id && t.stageAttempt === st.attempt; });
    if (stRun.length) { s.appendChild(el("h3", { text: "Still running when the log ended" })); s.appendChild(runningTable(stRun)); }
    if (st.failures && st.failures.length) {
      s.appendChild(el("h3", { text: "Why tasks failed" }));
      s.appendChild(table({
        rows: st.failures, page: 20,
        cols: [
          { h: "Kind", f: function (r) { return r[0]; } },
          { h: "Message", f: function (r) { return el("span", null, r[1], r[4] != null ? el("span", { cls: "sub", text: r[4] ? "Spark says the application caused the executor to exit." : "Spark says the application did not cause the exit." }) : null); } },
          { h: "Attempts", num: true, f: function (r) { return num(r[2]); } },
          { h: "Executors", f: function (r) { return el("span", null, r[3].map(function (id, i) { return [i ? ", " : "", execLink(id)]; })); } }
        ]
      }));
      st.failures.forEach(function (r) {
        if (r[5]) s.appendChild(el("details", null, el("summary", { text: "Stack trace: " + clip(r[1], 90) }), el("div", { cls: "inner" }, el("pre", { cls: "plan", text: r[5] }))));
      });
    }
    if (st.failure) { s.appendChild(el("h3", { text: "Why it failed" })); s.appendChild(el("pre", { cls: "plan", text: st.failure })); }
    var sw = el("div", { cls: "dagwrap", hidden: true });
    s.appendChild(sw);
    st.jobs.some(function (jid) { return jobDag(sw, jid, st.id); });
    var ops = D.stageOps[st.key];
    if (ops) {
      var ow = el("div", { cls: "dagwrap", hidden: true });
      s.appendChild(ow);
      drawGraph(ow, ops.layout, function (i) {
        var r = ops.rdds[i];
        return { title: r[2] || r[1], sub: "RDD " + r[0] + (r[2] ? " · " + r[1] : "") + (r[5] ? " · " + num(r[5]) + " cached" : ""),
          cls: r[6] ? "hot" : "", tip: "RDD " + r[0] + " (" + r[1] + ")" + (r[2] ? ", made by " + r[2] : "") + "\n" + (r[3] || "") + "\n" + num(r[4]) + " partitions" + (r[6] ? ", cached as " + r[6] : "") + (r[8] && r[8] !== "DETERMINATE" ? ", output " + r[8].toLowerCase() : "") + (r[7] ? ", barrier" : "") };
      }, { t: "What this stage computes", shows: "The RDDs this stage computes, named by the operation that made each (Spark's stage graph). Data flows down the arrows. Cached RDDs are outlined.",
        read: "Read it top to bottom, like the code. A long chain is fine; what matters is which step is slow, which the task table below shows." });
    }
    s.appendChild(codePanel(st.code, st.submitted, null));
    if (st.details) s.appendChild(el("details", null, el("summary", { text: "Where in the code: the full call stack Spark recorded" }), el("div", { cls: "inner" }, el("pre", { cls: "plan", text: st.details }))));
    if (st.rp || st.pushOn || st.barrier || Object.keys(st.props || {}).length) {
      s.appendChild(el("div", { cls: "facts" },
        st.rp ? fact("Resource profile", "Profile " + st.rp, "The stage ran with stage-level resources rather than the default profile.") : null,
        st.pushOn ? fact("Push-based shuffle", "On, " + num(st.pushMergers) + " merger locations", "Map outputs were pushed to merger services and merged before the reduce side read them.") : null,
        st.barrier ? fact("Barrier stage", "Yes", "All its tasks start together and are retried together, as some ML libraries need.") : null,
        Object.keys(st.props || {}).length ? fact("Own settings", Object.keys(st.props).map(function (k) { return k + " = " + st.props[k]; }).join("; "), "Properties this stage had that its job did not.") : null));
    }
    if (!det || !det.from) {
      s.appendChild(explain(D.collected ? "No task finished in this stage, so there is no task summary." : "Per-task detail was not collected for this run."));
      return s;
    }
    s.appendChild(el("h3", { text: "Summary of " + num(det.m.durationMs ? det.m.durationMs[0] : 0) + " successful tasks" }));
    s.appendChild(explain("Like the History Server's Summary Metrics. Min, quartiles and max; values past the first 64 tasks are from a histogram and within about 3%."));
    s.appendChild(table({
      rows: METRICS.filter(function (m) { return det.m[m[0]] && det.m[m[0]][6] > 0; }), cls: "summ",
      cols: [
        { h: "Metric", f: function (m) { return el("span", null, m[1], el("span", { cls: "sub", text: m[3] })); } },
        { h: "Min", num: true, f: function (m) { return m[2](det.m[m[0]][2]); } },
        { h: "25th percentile", num: true, f: function (m) { return m[2](det.m[m[0]][3]); } },
        { h: "Median", num: true, f: function (m) { return m[2](det.m[m[0]][4]); } },
        { h: "75th percentile", num: true, f: function (m) { return m[2](det.m[m[0]][5]); } },
        { h: "Max", num: true, title: "Highlighted when a work or data metric is over 5× its median", f: function (m) { var q = det.m[m[0]]; return el("span", { cls: SKEWY[m[0]] && q[4] > 0 && q[6] > 5 * q[4] ? "warnv" : null, text: m[2](q[6]) }); } },
        { h: "Total", num: true, f: function (m) { return m[2](det.m[m[0]][1]); } }
      ]
    }));
    s.appendChild(chartSlot("", "stageSplit:" + st.key));
    s.appendChild(chartSlot("", "durationHistogram:" + st.key));
    s.appendChild(chartSlot("", "taskScatter:" + st.key));
    s.appendChild(el("h3", null, "Slowest tasks", el("span", { cls: "sampled", text: "top " + num(det.slow.length) })));
    s.appendChild(taskTable(det.slow, { sort: 6 }));
    s.appendChild(el("h3", null, "Task sample", el("span", { cls: "sampled", text: num(det.sample.length) + " of " + num(det.from) })));
    s.appendChild(explain(det.sample.length < det.from ? "A uniform random sample of this stage's task attempts, kept so the page stays small." : "Every task attempt of this stage."));
    s.appendChild(taskTable(det.sample, { sort: 0 }));
    if (det.cells.length) {
      s.appendChild(el("h3", { text: "By executor" }));
      var mark = focusExec;
      focusExec = null; // a mark lasts one visit
      s.appendChild(cellTable(det.cells, "stage", mark));
      if (mark != null) setTimeout(function () { var tr = main.querySelector("tr.hl"); if (tr) tr.scrollIntoView({ block: "center" }); }, 0);
    } else if (D.cellsCapped) s.appendChild(explain("Per-executor totals stopped before this stage (the app-wide cap was reached)."));
    return s;
  };

  // cellTable lists per-executor totals; mark is an executor ID whose row
  // is highlighted.
  function cellTable(cells, by, mark) {
    var rows = cells.map(function (r) {
      var o = {};
      D.cellCols.forEach(function (c, i) { o[c] = r[i]; });
      o.execID = execName(o.exec);
      return o;
    });
    var cols = [];
    if (by === "stage") cols.push({ h: "Executor", v: function (c) { return c.execID; }, f: function (c) { return execLink(c.execID); } },
      { h: "Host", v: function (c) { var x = execByID[c.execID]; return x ? x.host : ""; }, f: function (c) { var x = execByID[c.execID]; return x ? x.host : "—"; } });
    else cols.push({ h: "Stage", num: true, v: function (c) { return c.stage.id + c.stage.attempt / 100; }, f: function (c) { return stageLink(c.stage); } },
      { h: "Name", v: function (c) { return c.stage.name; }, f: function (c) { return c.stage.name; } });
    cols.push(
      { h: "Tasks", num: true, v: function (c) { return c.tasks; }, f: function (c) { return el("span", null, num(c.ok) + " / " + num(c.tasks), c.failed ? el("span", { cls: "bad", text: " · " + num(c.failed) + " failed" }) : null); } },
      numCol("Task time", "dur", dur), numCol("GC", "gc", dur), numCol("Input", "input", bytes), numCol("Shuffle read", "shRead", bytes),
      numCol("Shuffle write", "shWrite", bytes), numCol("Disk spill", "diskSpill", bytes),
      { h: "Peak heap", num: true, title: "Highest JVM heap sampled while this stage ran", v: function (c) { return c.peakHeap; }, f: function (c) { return c.peakHeap ? bytes(c.peakHeap) : "—"; } });
    return table({ rows: rows, sort: by === "stage" ? 3 : 0, dir: by === "stage" ? "desc" : "asc", page: mark != null ? Math.max(100, rows.length) : 100, cols: cols,
      rowCls: mark != null ? function (c) { return c.execID === mark ? "hl" : null; } : null });
  }

  views.executors = function () {
    var s = section("Executors", "Executors are the worker processes that ran tasks. The driver coordinates and usually runs none.");
    s.appendChild(chartSlot("tall", "executorsTimeline"));
    s.appendChild(chartSlot("", "execTime"));
    s.appendChild(chartSlot("", "execHeapAll"));
    s.appendChild(chartSlot("", "heatmap"));
    if (exclusions.length) {
      s.appendChild(el("h3", { text: "Exclusions" }));
      s.appendChild(explain(EXCL_EXPLAIN));
      s.appendChild(exclusionTable(exclusions));
    }
    s.appendChild(table({
      rows: execs, sort: 0, dir: "asc", filter: "Filter executors by ID, host or reason",
      text: function (x) { return x.id + " " + x.host + " " + (x.reason || ""); },
      rowCls: function (x) { return x.kind === "memory-kill" || x.kind === "lost" ? "failedrow" : null; },
      cols: [
        { h: "Executor", num: true, v: function (x) { return x.id === "driver" ? -1 : parseFloat(x.id); }, f: function (x) { return execLink(x.id); } },
        { h: "Host", v: function (x) { return x.host; }, f: function (x) { return x.host || "—"; } },
        numCol("Cores", "cores"),
        { h: "Added", num: true, v: function (x) { return x.added; }, f: function (x) { return when(x.added); } },
        { h: "Removed", num: true, v: function (x) { return x.removed; }, f: function (x) { return x.removed ? el("span", null, when(x.removed), el("span", { cls: "sub", text: x.reason })) : "running at end"; } },
        { h: "Tasks", num: true, v: function (x) { return x.tasks; }, f: function (x) { return el("span", null, num(x.ok) + " / " + num(x.tasks), x.failed ? el("span", { cls: "bad", text: " · " + num(x.failed) + " failed" }) : null); } },
        numCol("Task time", "dur", dur),
        { h: "CPU share", num: true, title: "Task CPU time as a share of task run time", v: function (x) { return x.run ? x.cpuNs / 1e6 / x.run : null; }, f: function (x) { return pct(x.run ? x.cpuNs / 1e6 / x.run : null); } },
        { h: "GC share", num: true, title: "Share of task run time spent in garbage collection", v: function (x) { return x.run ? x.gc / x.run : null; }, f: function (x) { var g = x.run ? x.gc / x.run : null; return el("span", { cls: g > 0.1 ? "warnv" : null, text: pct(g) }); } },
        numCol("Input", "input", bytes), numCol("Shuffle read", "shRead", bytes), numCol("Shuffle write", "shWrite", bytes),
        { h: "Peak heap", num: true, v: function (x) { return x.peakHeap; }, f: function (x) { return x.peakHeap ? el("span", null, bytes(x.peakHeap), el("span", { cls: "sub", text: "of " + bytes(D.heapBytes) })) : "—"; } }
      ]
    }));
    return s;
  };
  views.executor = function (id) {
    var x = execByID[id];
    if (!x) {
      var only = logFiles.filter(function (f) { return f.exec === id; });
      if (!only.length) return notFound("Executor " + id);
      var s0 = section(id === "driver" ? "Driver" : "Executor " + id, "The event log has no record of it, so only its own container logs are shown.");
      s0.insertBefore(el("div", { cls: "crumbs" }, link("#executors", "Executors"), " / " + id), s0.firstChild);
      only.forEach(function (f) { s0.appendChild(el("p", null, link("#logs/" + f.i, FILE_KIND[f.kind] || f.kind), el("span", { cls: "sub mono", text: f.loc }))); });
      s0.appendChild(logLinesTable([].concat.apply([], only.map(function (f) { return f.rows; })), true));
      return s0;
    }
    var s = section(id === "driver" ? "Driver" : "Executor " + id);
    s.insertBefore(el("div", { cls: "crumbs" }, link("#executors", "Executors"), " / " + id), s.firstChild);
    s.appendChild(el("div", { cls: "facts" },
      fact("Host", x.host || "—"),
      fact("Lifetime", el("span", null, when(x.added, true), " → ", x.removed ? when(x.removed) : "end of run"), x.reason || (a.end ? "No removal was logged; it ran until the application ended." : "Still running when the log ended.")),
      fact("Cores", num(x.cores), "Tasks it could run at once."),
      fact("Tasks", num(x.ok) + " succeeded of " + num(x.tasks), (x.failed ? num(x.failed) + " failed. " : "") + "Task time " + dur(x.dur) + ", GC " + pct(x.run ? x.gc / x.run : null) + " of run time."),
      fact("Peak memory", x.peakHeap ? bytes(x.peakHeap) + " heap of " + bytes(D.heapBytes) : "Not recorded", "Execution " + bytes(x.peakExec) + ", storage " + bytes(x.peakStorage) + (x.peakRss ? ", process RSS " + bytes(x.peakRss) : "") + "."),
      fact("Data", bytes(x.input) + " read, " + bytes(x.output) + " written", "Shuffle: " + bytes(x.shRead) + " read, " + bytes(x.shWrite) + " written. Spill to disk " + bytes(x.diskSpill) + "."),
      x.minorGc || x.majorGc ? fact("Garbage collection", num(x.minorGc) + " minor (" + dur(x.minorGcMs) + "), " + num(x.majorGc) + " major (" + dur(x.majorGcMs) + ")", "Collections the JVM reported by the time of its last sample. Major collections pause everything and are the ones to watch.") : null,
      x.unified || x.vmem ? fact("More memory", (x.unified ? bytes(x.unified) + " unified (execution plus storage)" : "") + (x.vmem ? (x.unified ? "; " : "") + bytes(x.vmem) + " virtual" : ""), "Peaks Spark sampled; virtual memory counts reserved address space, not RAM used.") : null,
      x.tasks ? fact("Task overheads", dur(x.sched) + " scheduler delay, " + bytes(x.resultSize) + " of results", "Summed over its tasks.") : null,
      x.startupMs ? fact("Startup", dur(x.startupMs), "From asking the cluster manager for this executor to it registering with the driver.") : null,
      Object.keys(x.logs || {}).length ? fact("Logs", linkMap(x.logs), "Links to the cluster; they stop working once it is gone.") : null,
      Object.keys(x.resources || {}).length ? fact("Resources", Object.keys(x.resources).map(function (k) { return x.resources[k] + " " + k; }).join(", "), "Extra resources such as GPUs, by how many addresses Spark assigned.") : null,
      x.bmRemoved ? fact("Block manager left", when(x.bmRemoved, true), "Cached blocks on this executor were lost then.") : null));
    if (Object.keys(x.attrs || {}).length) s.appendChild(el("details", null, el("summary", { text: "Container attributes (" + Object.keys(x.attrs).length + ")" }),
      el("div", { cls: "inner" }, el("dl", { cls: "kv" }, Object.keys(x.attrs).sort().map(function (k) { return [el("dt", { text: k }), el("dd", { cls: "mono", text: x.attrs[k] })]; })))));
    var mine = exclusions.filter(function (e) { return e.kind === "executor" && e.target === id || e.kind === "node" && e.target === x.host; });
    if (mine.length) { s.appendChild(el("h3", { text: "Exclusions" })); s.appendChild(explain(EXCL_EXPLAIN)); s.appendChild(exclusionTable(mine)); }

    var idx = D.execs.indexOf(id), cells = [];
    if (idx >= 0) stages.forEach(function (st) {
      var det = D.detail[st.key];
      if (!det) return;
      det.cells.forEach(function (r) { if (r[C.exec] === idx) { var o = {}; D.cellCols.forEach(function (c, i) { o[c] = r[i]; }); o.stage = st; cells.push(o); } });
    });
    if (D.collected && id !== "driver") { s.appendChild(el("h3", { text: "Peak heap by stage" })); s.appendChild(chartSlot("", "execHeap:" + id)); }
    s.appendChild(el("h3", { text: "Stages it worked on" }));
    if (!cells.length) s.appendChild(explain(D.collected ? "No stage × executor totals for this executor." : "Per-task detail was not collected for this run."));
    else s.appendChild(table({
      rows: cells, sort: 0, dir: "asc", page: 100,
      cols: [
        { h: "Stage", num: true, v: function (c) { return c.stage.id + c.stage.attempt / 100; }, f: function (c) { return stageLink(c.stage); } },
        { h: "Name", v: function (c) { return c.stage.name; }, f: function (c) { return c.stage.name; } },
        { h: "Tasks", num: true, v: function (c) { return c.tasks; }, f: function (c) { return el("span", null, num(c.ok) + " / " + num(c.tasks), c.failed ? el("span", { cls: "bad", text: " · " + num(c.failed) + " failed" }) : null); } },
        numCol("Task time", "dur", dur), numCol("Input", "input", bytes), numCol("Shuffle read", "shRead", bytes), numCol("Shuffle write", "shWrite", bytes),
        { h: "Peak heap", num: true, v: function (c) { return c.peakHeap; }, f: function (c) { return c.peakHeap ? bytes(c.peakHeap) : "—"; } }
      ]
    }));
    var mineLogs = logFiles.filter(function (f) { return f.exec === id; });
    if (mineLogs.length) {
      s.appendChild(el("h3", { text: "From its logs" }));
      s.appendChild(explain("What sparkplain recognised in this " + (id === "driver" ? "driver" : "executor") + "'s own container logs. The event log records what Spark decided; these lines are what the process itself wrote."));
      s.appendChild(logLinesTable([].concat.apply([], mineLogs.map(function (f) { return f.rows; })), true));
    }
    if (x.src) s.appendChild(el("p", { cls: "srcref", text: "Added: " + src(x.src) }));
    return s;
  };

  // logLinesTable lists recognised log lines, most severe first, each
  // opening to its detail (the cause chain, the traceback).
  function logLinesTable(lines, withFile, hit) {
    var rank = { critical: 0, warning: 1, info: 2 };
    return table({
      rows: lines, sort: withFile ? 0 : 1, dir: "asc", page: 100, filter: "Filter lines",
      text: function (l) { return l.kind + " " + l.text + " " + (l.detail || []).join(" "); },
      rowCls: function (l) { return hit && l.line === hit ? "hitrow" : null; },
      cols: [
        { h: "Severity", v: function (l) { return rank[l.sev] * 1e12 + (l.time || 0); }, f: function (l) { return sevPill(l.sev); } },
        { h: "Line", num: true, v: function (l) { return l.line; }, f: function (l) {
          return el("span", null, withFile ? link("#logs/" + l.file.i + ":" + l.line, String(l.line)) : String(l.line), l.end ? el("span", { cls: "sub", text: "to " + l.end }) : null, withFile ? el("span", { cls: "sub", text: FILE_KIND[l.file.kind] || l.file.kind }) : null);
        } },
        { h: "Time", num: true, v: function (l) { return l.time || 0; }, f: function (l) { return l.time ? when(l.time) : "—"; } },
        { h: "What", v: function (l) { return LOG_KIND[l.kind] || l.kind; }, f: function (l) { return LOG_KIND[l.kind] || l.kind; } },
        { h: "Line text", v: function (l) { return l.text; }, f: function (l) {
          var extra = (l.detail || []).length || Object.keys(l.fields || {}).length;
          var head = el("span", { cls: "mono" }, l.text, l.count > 1 ? el("span", { cls: "sub", text: num(l.count) + " times, last at line " + l.last }) : null);
          if (!extra) return head;
          return el("details", { cls: "logline" }, el("summary", null, head), el("div", { cls: "inner" },
            (l.detail || []).length ? el("pre", { cls: "mono", text: l.detail.join("\n") }) : null,
            Object.keys(l.fields || {}).length ? el("dl", { cls: "kv" }, Object.keys(l.fields).sort().map(function (k) { return [el("dt", { text: k }), el("dd", { cls: "mono", text: l.fields[k] })]; })) : null));
        } }
      ]
    });
  }

  views.sql = function () {
    var s = section("SQL / DataFrame", "Each query is one DataFrame action or SQL statement. Open one for its plan with row counts and time per operator.");
    if (queries.length) s.appendChild(chartSlot("", "sqlTimeline"));
    s.appendChild(table({
      rows: queries, sort: 0, dir: "asc", filter: "Filter queries by ID, description or table",
      text: function (q) { return q.id + " " + q.desc + " " + q.reads.join(" ") + " " + q.writes.join(" "); },
      rowCls: function (q) { return q.error ? "failedrow" : null; },
      cols: [
        { h: "Query", num: true, v: function (q) { return q.id; }, f: function (q) { return link("#query/" + q.id, String(q.id)); } },
        { h: "Description", v: function (q) { return q.desc; }, f: function (q) { return el("span", null, q.desc, q.root != null ? el("span", { cls: "sub" }, "Runs inside ", link("#query/" + q.root, "query " + q.root)) : null); } },
        { h: "Status", v: queryStatus, f: function (q) { return status(queryStatus(q)); } },
        { h: "Started", num: true, v: function (q) { return q.start; }, f: function (q) { return when(q.start); } },
        { h: "Duration", num: true, v: function (q) { return span(q.start, q.end); }, f: function (q) { return dur(span(q.start, q.end)); } },
        { h: "Jobs", v: function (q) { return q.jobs.length; }, f: function (q) { return el("span", null, q.jobs.map(function (id, i) { return [i ? ", " : "", link("#job/" + id, String(id))]; })); } },
        { h: "Reads", v: function (q) { return q.reads.join(", "); }, f: function (q) { return q.reads.join(", ") || "—"; } },
        { h: "Writes", v: function (q) { return q.writes.join(", "); }, f: function (q) { return q.writes.join(", ") || "—"; } }
      ]
    }));
    return s;
  };
  function metricText(m) {
    var v = m[2], t = m[1];
    if (v == null) return "not recorded";
    if (t === "size") return bytes(v);
    if (t === "timing") return dur(v);
    if (t === "nsTiming") return dur(v / 1e6);
    if (t === "average") return null; // Spark keeps per-task averages, not a meaningful total
    return num(v);
  }
  // spreadText shows a metric's per-task min, median and max, and the task
  // and stage that hit the max, as Spark's SQL tab does. Spark stores
  // "average" metrics per task as ten times the value.
  function spreadText(m) {
    var f = function (v) {
      if (m[1] === "size") return bytes(v);
      if (m[1] === "timing") return dur(v);
      if (m[1] === "nsTiming") return dur(v / 1e6);
      if (m[1] === "average") return (v / 10).toFixed(1);
      return num(v);
    };
    return "min " + f(m[4]) + ", median " + f(m[5]) + ", max " + f(m[6]) + " in task " + m[7] + " of stage " + m[8];
  }
  views.query = function (id) {
    var q = queryByID[id];
    if (!q) return notFound("Query " + id);
    var s = section("Query " + q.id + ": " + q.desc);
    s.insertBefore(el("div", { cls: "crumbs" }, link("#sql", "SQL / DataFrame"), " / query " + q.id), s.firstChild);
    s.appendChild(el("div", { cls: "facts" },
      fact("Status", status(queryStatus(q))),
      fact("Ran", el("span", null, when(q.start, true), " → ", when(q.end)), dur(span(q.start, q.end))),
      fact("Jobs", q.jobs.length ? el("span", null, q.jobs.map(function (id, i) { return [i ? ", " : "", link("#job/" + id, "Job " + id)]; })) : "None", "The Spark jobs this query ran."),
      fact("Data", (q.reads.length ? "Reads " + q.reads.join(", ") : "Reads no tables or files") + (q.writes.length ? "; writes " + q.writes.join(", ") : ""))));
    var kids = queries.filter(function (c) { return c.root === q.id; });
    if (q.root != null || kids.length || (q.tags && q.tags.length)) s.appendChild(el("div", { cls: "facts" },
      q.root != null ? fact("Runs inside", link("#query/" + q.root, "Query " + q.root), "This is a sub-query or a command's inner query; its time is part of the parent's.") : null,
      kids.length ? fact("Inner queries", el("span", null, kids.map(function (c, i) { return [i ? ", " : "", link("#query/" + c.id, "Query " + c.id)]; })), "Queries that ran inside this one.") : null,
      q.tags && q.tags.length ? fact("Job tags", q.tags.join(", "), "Tags the code set on this query's jobs.") : null));
    if (q.error) { s.appendChild(el("h3", { text: "Error" })); s.appendChild(el("pre", { cls: "plan", text: q.error })); }
    var mk = Object.keys(q.modified || {}).sort();
    if (mk.length) {
      s.appendChild(el("h3", { text: "Session settings in force" }));
      s.appendChild(explain("SQL settings the session had changed from their defaults when this query ran. Values of keys that look like secrets are redacted."));
      s.appendChild(el("div", { cls: "tbl" }, el("table", null, el("thead", null, el("tr", null, el("th", { text: "Setting" }), el("th", { text: "Value" }))),
        el("tbody", null, mk.map(function (k) { return el("tr", null, el("td", { cls: "mono", text: k }), el("td", { cls: "mono", text: q.modified[k] })); })))));
    }
    var ad = D.adaptive[String(q.id)];
    if (ad && ad.length) {
      s.appendChild(el("h3", { text: "Metrics adaptive execution added" }));
      s.appendChild(explain("Adaptive execution registered these after planning; the log does not say which operator each belongs to."));
      s.appendChild(el("p", { cls: "metricv" }, ad.map(function (m, i) {
        var v = m[1] === "average" && m.length > 3 ? spreadText(m) : metricText(m);
        return v == null ? null : [i ? " · " : "", m[0] + " ", el("b", { text: v }), m.length > 3 && m[1] !== "average" ? " (" + spreadText(m) + ")" : ""];
      })));
    }
    var op = q.optimizer;
    if (op) {
      s.appendChild(el("h3", { text: "Optimizer (EMR)" }));
      s.appendChild(explain("EMR records how long Spark's query optimizer spent on each rule. " + num(op[1]) + " rules ran for " + dur(op[0] / 1e6) + " in all; " + num(op[2]) + " of them changed the plan. The slowest " + num(op[3].length) + " are listed; report.json has all of them."));
      s.appendChild(table({ rows: op[3], sort: 1, dir: "desc", page: 30, cols: [
        { h: "Rule", v: function (r) { return r[0]; }, f: function (r) { return el("span", { cls: "mono", text: r[0] }); } },
        { h: "Time", num: true, v: function (r) { return r[1]; }, f: function (r) { return dur(r[1] / 1e6); } },
        { h: "Runs", num: true, v: function (r) { return r[2]; }, f: function (r) { return num(r[2]); } },
        { h: "Changed the plan", num: true, title: "Runs that changed the plan, and their time", v: function (r) { return r[3]; }, f: function (r) { return r[3] ? num(r[3]) + " (" + dur(r[4] / 1e6) + ")" : "—"; } }
      ] }));
      var ok2 = Object.keys(op[4] || {});
      if (ok2.length) s.appendChild(el("p", { cls: "metricv", text: ok2.map(function (k) { return k + ": " + op[4][k]; }).join(" · ") }));
    }
    var g = D.graphs[String(q.id)];
    if (g && g.length) {
      s.appendChild(el("h3", { text: "Plan" }));
      s.appendChild(explain("The final physical plan, after adaptive re-planning. In the graph, data flows down from the scans to the result; the table lists the same operators from the result back to the scans, indented by depth, with every metric. Metrics are totals across all tasks."));
      var pw = el("div", { cls: "dagwrap", hidden: true });
      s.appendChild(pw);
      planGraph(pw, q.id);
      var rows = [], seen = {};
      (function walk(i, depth) {
        if (seen[i] || !g[i]) return;
        seen[i] = true;
        rows.push({ n: g[i], depth: depth, i: i });
        (g[i].c || []).forEach(function (c) { walk(c, depth + 1); });
      })(0, 0);
      s.appendChild(table({
        rows: rows, cls: "tree", page: 500,
        cols: [
          { h: "Operator", f: function (r) { return el("span", { cls: "op" }, el("span", { cls: "ind", style: "width:" + r.depth * 16 + "px" }), el("b", { text: r.n.n }), r.n.d ? el("span", { cls: "sub", text: r.n.d }) : null); } },
          { h: "Metrics", f: function (r) {
            var parts = (r.n.m || []).map(function (m) {
              var v = metricText(m);
              if (m[1] === "average" && m.length > 3) v = spreadText(m);
              if (v == null || v === "not recorded" || m[2] === 0) return null;
              return el("span", { title: m.length > 3 ? m[3] + " tasks reported it" : null }, m[0] + " ", el("b", { text: v }), m.length > 3 && m[1] !== "average" ? " (" + spreadText(m) + ")" : "");
            }).filter(Boolean);
            return el("span", { cls: "metricv" }, parts.length ? parts.map(function (p, i) { return [i ? " · " : "", p]; }) : "—");
          } }
        ]
      }));
    } else s.appendChild(explain(D.collected ? "No plan was kept for this query." : "Per-task detail was not collected for this run, so there is no plan graph."));
    s.appendChild(codePanel(q.code, q.start, null));
    if (q.details) s.appendChild(el("details", null, el("summary", { text: "Where in the code: the full call stack Spark recorded" }), el("div", { cls: "inner" }, el("pre", { cls: "plan", text: q.details }))));
    if (q.plan) {
      s.appendChild(el("details", null, el("summary", { text: "Physical plan text" + (q.planCut ? " (cut; the JSON export has the full text)" : "") }), el("div", { cls: "inner" }, el("pre", { cls: "plan", text: q.plan }))));
    }
    if (q.src) s.appendChild(el("p", { cls: "srcref", text: "Query start: " + src(q.src) }));
    return s;
  };

  views.storage = function () {
    var s = section("Storage", "Data the code cached or persisted, so later actions could reuse it instead of recomputing.");
    s.appendChild(table({
      rows: rdds, sort: 0, dir: "asc", empty: "Nothing was cached.",
      cols: [
        numCol("RDD", "id"),
        { h: "Name", v: function (r) { return r.name; }, f: function (r) { return r.name; } },
        { h: "Storage level", v: function (r) { return r.level; }, f: function (r) { return r.level; } },
        numCol("Partitions", "partitions"),
        { h: "First stage", num: true, v: function (r) { return r.firstStage; }, f: function (r) { var p = (stagesByID[r.firstStage] || [])[0]; return p ? stageLink(p) : String(r.firstStage); } },
        { h: "In memory", num: true, v: function (r) { return r.mem; }, f: function (r) { return r.sizeKnown ? bytes(r.mem) : "not recorded"; } },
        { h: "On disk", num: true, v: function (r) { return r.disk; }, f: function (r) { return r.sizeKnown ? bytes(r.disk) : "not recorded"; } },
        { h: "Released", v: function (r) { return r.unpersisted ? 1 : 0; }, f: function (r) { return r.unpersisted ? "yes (unpersisted)" : "no"; } }
      ]
    }));
    if (rdds.some(function (r) { return !r.sizeKnown; })) s.appendChild(explain("Cached sizes are logged only when spark.eventLog.logBlockUpdates.enabled=true."));
    rdds.forEach(function (r) {
      if (!r.executors || !r.executors.length) return;
      s.appendChild(el("details", null, el("summary", { text: "Where RDD " + r.id + " was cached (" + r.executors.length + " executor" + (r.executors.length === 1 ? "" : "s") + ")" }),
        el("div", { cls: "inner" }, table({ rows: r.executors, sort: 3, dir: "desc", page: 100, cols: [
          { h: "Executor", v: function (p) { return p[0]; }, f: function (p) { return execLink(p[0]); } },
          { h: "Host", v: function (p) { return p[1]; }, f: function (p) { return p[1]; } },
          { h: "Blocks", num: true, v: function (p) { return p[2]; }, f: function (p) { return num(p[2]); } },
          { h: "In memory", num: true, v: function (p) { return p[3]; }, f: function (p) { return bytes(p[3]); } },
          { h: "On disk", num: true, v: function (p) { return p[4]; }, f: function (p) { return bytes(p[4]); } },
          { h: "Storage level", v: function (p) { return p[5]; }, f: function (p) { return p[5]; } }
        ] }))));
    });
    var data = objs(D.data);
    if (data.length) {
      s.appendChild(el("h3", { text: "Tables and paths" }));
      s.appendChild(explain("What the run read, wrote, created, altered or dropped, from SQL plans and the catalog."));
      s.appendChild(table({ rows: data, sort: 1, dir: "asc", filter: "Filter by name", text: function (d) { return d.name + " " + d.access; }, cols: [
        { h: "Access", v: function (d) { return d.access; }, f: function (d) { return d.access; } },
        { h: "Kind", v: function (d) { return d.kind; }, f: function (d) { return d.kind; } },
        { h: "Name", v: function (d) { return d.name; }, f: function (d) { return el("span", { cls: "mono", text: d.name }); } },
        { h: "Format", v: function (d) { return d.format; }, f: function (d) { return d.format || "—"; } },
        { h: "Log line", v: function (d) { return d.src ? d.src[1] : 0; }, f: function (d) { return el("span", { cls: "srcref", text: src(d.src) }); } }
      ] }));
    }
    if (blockKinds.length) {
      s.appendChild(el("h3", { text: "Block manager activity" }));
      s.appendChild(explain("Every block Spark stored, by kind: rdd blocks are cached partitions, broadcast blocks are broadcast variables, taskresult blocks are results too big to send directly."));
      s.appendChild(table({ rows: blockKinds, sort: 1, dir: "desc", cols: [
        { h: "Kind", v: function (k) { return k.kind; }, f: function (k) { return k.kind; } },
        numCol("Updates", "updates"), numCol("Largest in memory", "maxMem", bytes), numCol("Largest on disk", "maxDisk", bytes)
      ] }));
    }
    return s;
  };

  views.environment = function () {
    var s = section("Environment", "Versions and settings the driver reported. Values of keys that look like secrets are redacted.");
    var rt = objs(D.runtime);
    if (rt.length) s.appendChild(table({
      rows: rt, cls: "runtime", page: 500,
      cols: [
        { h: "Group", f: function (r) { return r.group; } },
        { h: "Item", f: function (r) { return el("span", null, r.label, el("span", { cls: "sub", text: r.explain })); } },
        { h: "Value", f: function (r) { return r.missing ? el("i", { text: r.value || "not recorded" }) : el("span", { cls: "mono", text: r.value }); } },
        { h: "Read from", f: function (r) { return el("span", { cls: "srcref", text: r.from }); } }
      ]
    }));
    var profs = objs(D.profiles);
    if (profs.length) {
      s.appendChild(el("h3", { text: "Resource profiles" }));
      s.appendChild(explain("What each executor and task asked for. Profile 0 is the default; others come from stage-level scheduling."));
      s.appendChild(table({ rows: profs, sort: 0, dir: "asc", cols: [
        numCol("Profile", "id"), numCol("Executor cores", "cores"),
        { h: "Executor memory", num: true, v: function (p) { return p.memMiB; }, f: function (p) { return p.memMiB ? bytes(p.memMiB * 1048576) : "default"; } },
        { h: "Overhead", num: true, v: function (p) { return p.overheadMiB; }, f: function (p) { return p.overheadMiB ? bytes(p.overheadMiB * 1048576) : "default"; } },
        { h: "Off-heap", num: true, v: function (p) { return p.offHeapMiB; }, f: function (p) { return p.offHeapMiB ? bytes(p.offHeapMiB * 1048576) : "—"; } },
        { h: "Python", num: true, v: function (p) { return p.pysparkMiB; }, f: function (p) { return p.pysparkMiB ? bytes(p.pysparkMiB * 1048576) : "—"; } },
        { h: "CPUs per task", num: true, v: function (p) { return p.taskCpus; }, f: function (p) { return p.taskCpus || "—"; } },
        { h: "Other", f: function (p) { var o = []; Object.keys(p.execOther).forEach(function (k) { o.push(k + " " + p.execOther[k] + " per executor"); }); Object.keys(p.taskOther).forEach(function (k) { o.push(k + " " + p.taskOther[k] + " per task"); }); return o.join("; ") || "—"; } }
      ] }));
    }
    var box = el("input", { cls: "filter", type: "search", placeholder: "Search settings", "aria-label": "Search settings" });
    s.appendChild(box);
    var groups = el("div", { cls: "findings" });
    (D.config || []).forEach(function (g) {
      var d = el("details", { cls: "cfggroup" }, el("summary", { text: g.name + " (" + num(g.entries.length) + ")" }));
      d.appendChild(el("div", { cls: "tbl" }, el("table", null,
        el("thead", null, el("tr", null, el("th", { text: "Setting" }), el("th", { text: "Value" }), el("th", { text: "Spark default" }))),
        el("tbody", null, g.entries.map(function (e) {
          return el("tr", { cls: e[3] ? "nondefault" : null }, el("td", { cls: "mono" }, e[0], e[4] ? el("span", { cls: "sub", text: e[4] }) : null), el("td", { cls: "mono", text: e[1] }), el("td", { cls: "mono", text: e[2] || "" }));
        })))));
      groups.appendChild(d);
    });
    box.addEventListener("input", function () {
      var q = box.value.trim().toLowerCase();
      groups.querySelectorAll("tbody tr").forEach(function (tr) { tr.hidden = q !== "" && tr.textContent.toLowerCase().indexOf(q) < 0; });
      groups.querySelectorAll("details").forEach(function (d) { if (q !== "") d.open = d.querySelector("tbody tr:not([hidden])") !== null; });
    });
    s.appendChild(groups);
    return s;
  };



  // ---------- graphs (laid out in Go, drawn here as SVG) ----------
  var SVG = "http://www.w3.org/2000/svg";
  function sv(tag, attrs) {
    var n = document.createElementNS(SVG, tag);
    Object.keys(attrs || {}).forEach(function (k) { n.setAttribute(k, attrs[k]); });
    return n;
  }
  function clip(t, n) { t = t || ""; return t.length > n ? t.slice(0, n - 1) + "…" : t; }
  // drawGraph fills wrap with the layout; info(i) gives node i's title, sub
  // line, link, extra class and tooltip.
  function drawGraph(wrap, lay, info, g) {
    var svg = sv("svg", { "class": "dag", viewBox: "0 0 " + lay.w + " " + lay.h, width: lay.w, height: lay.h, role: "img" });
    var W = 200, H = 48;
    lay.edges.forEach(function (e) {
      var a = lay.pos[e[0]], b = lay.pos[e[1]];
      var x1 = a[0] + W / 2, y1 = a[1] + H, x2 = b[0] + W / 2, y2 = b[1], my = (y1 + y2) / 2;
      svg.appendChild(sv("path", { "class": "edge", d: "M" + x1 + "," + y1 + " C" + x1 + "," + my + " " + x2 + "," + my + " " + x2 + "," + (y2 - 4), "marker-end": "url(#sp-arrow)" }));
    });
    var defs = sv("defs"), m = sv("marker", { id: "sp-arrow", viewBox: "0 0 8 8", refX: "4", refY: "4", markerWidth: "7", markerHeight: "7", orient: "auto" });
    var mp = sv("path", { d: "M0,0 L8,4 L0,8 z" });
    mp.style.fill = "var(--faint)";
    m.appendChild(mp); defs.appendChild(m); svg.insertBefore(defs, svg.firstChild);
    lay.pos.forEach(function (p, i) {
      var inf = info(i);
      var g = sv("g", { "class": "node" + (inf.cls ? " " + inf.cls : ""), transform: "translate(" + p[0] + "," + p[1] + ")" });
      var t = sv("title"); t.textContent = inf.tip || inf.title; g.appendChild(t);
      g.appendChild(sv("rect", { width: W, height: H, rx: 6 }));
      var t1 = sv("text", { x: 10, y: 19 }); t1.textContent = clip(inf.title, 30); g.appendChild(t1);
      var t2 = sv("text", { x: 10, y: 37, "class": "sub" }); t2.textContent = clip(inf.sub, 32); g.appendChild(t2);
      if (inf.href) { var a = sv("a", { href: inf.href }); a.appendChild(g); svg.appendChild(a); } else svg.appendChild(g);
    });
    wrap.textContent = "";
    if (g.t) wrap.appendChild(el("h4", { text: g.t }));
    wrap.appendChild(svg);
    add(wrap, guideNodes(g));
    wrap.hidden = false;
  }
  function jobDag(wrap, jobID, hot) {
    var dag = D.jobDags[String(jobID)];
    if (!dag) return false;
    drawGraph(wrap, dag.layout, function (i) {
      var sid = dag.stages[i], atts = stagesByID[sid] || [], st = atts[atts.length - 1];
      if (!st) return { title: "Stage " + sid, sub: "not logged", cls: "skipped" };
      return { title: "Stage " + sid + (st.attempt ? " (attempt " + (st.attempt + 1) + ")" : ""), sub: STATUS[st.status] + " · " + dur(span(st.submitted, st.completed)) + " · " + num(st.tasks) + " tasks",
        href: "#stage/" + st.key, cls: (sid === hot ? "hot" : "") + (st.status === "failed" ? " failed" : "") + (st.status === "skipped" ? " skipped" : ""), tip: "Stage " + sid + ": " + st.name };
    }, { t: "Job " + jobID + "'s stages", shows: "Arrows point from a stage to the stages that read its output. " + (hot != null ? "This stage is outlined. " : "") + "Failed stages have a red outline and skipped ones (their output already existed) a dashed one; click a stage to open it.",
      read: "Each arrow is a shuffle: data written by one stage and read over the network by the next, so fewer arrows usually means less data moved. Skipped stages are good news: Spark reused output it already had." });
    return true;
  }
  function planGraph(wrap, qid) {
    var g = D.graphs[String(qid)], lay = D.planLayouts[String(qid)];
    if (!g || !lay) return false;
    drawGraph(wrap, lay, function (i) {
      var n = g[i], rows = null, time = null;
      (n.m || []).forEach(function (m) {
        if (m[2] == null) return;
        if (m[0] === "number of output rows") rows = m[2];
        else if (time == null && (m[1] === "timing" || m[1] === "nsTiming") && m[2] > 0) time = m[1] === "nsTiming" ? m[2] / 1e6 : m[2];
      });
      var sub = rows != null ? num(rows) + " rows" : "";
      if (time != null) sub += (sub ? " · " : "") + dur(time);
      return { title: n.n, sub: sub || " ", tip: n.n + (n.d ? "\n" + n.d : "") };
    }, { t: "Query plan", shows: "Data flows down the arrows, from the scans at the top to the result at the bottom. Rows and time are totals across tasks; the table below lists every metric.",
      read: "Row counts should shrink as filters and aggregations apply. A step whose rows jump up (often a join) or that takes most of the time is where to look first." });
    return true;
  }

  // ---------- chart layer ----------
  // Every chart is drawn by the D3 kit below from the page's own data, so
  // the explorer makes no network requests.
  function waitText(c, text) { var w = c.el.querySelector(".wait"); if (w) w.textContent = text; }
  // guideNodes explains a chart: what it shows, and how to read it (which
  // way is good, and what pattern to look for).
  function guideNodes(g) {
    return [g.shows ? el("p", { cls: "cap", text: g.shows }) : null,
      g.read ? el("p", { cls: "read" }, el("b", { text: "How to read it " }), g.read) : null];
  }
  // frame replaces the slot's placeholder with a titled plot area and its
  // guide: g has t (the title), shows and read.
  function frame(c, g) {
    c.el.textContent = "";
    if (g.t) c.el.appendChild(el("h4", { text: g.t }));
    var plot = el("div", { cls: "plot" });
    c.el.appendChild(plot);
    add(c.el, guideNodes(g));
    return plot;
  }
  function statusColor(s) { return s === "succeeded" ? V.series : s === "failed" ? V.fail : V.neutral; }
  var appEnd = a.end || (function () { var m = a.start || 0; stages.forEach(function (s) { m = Math.max(m, s.completed || 0); }); return m; })();

  // timeline draws one lane per row (row, bar, color, start, end, tip): a
  // bar from start to end, labelled inside when it fits. The time axis sits
  // above the rows and stays in view while a long list scrolls. Rows past
  // 600 are left out, and the guide says so.
  function timeline(c, rows, g, emptyText) {
    if (!rows.length) { waitText(c, emptyText); return; }
    var capRows = rows.slice(0, 600);
    var plot = frame(c, { t: g.t, read: g.read, shows: g.shows + (rows.length > capRows.length ? " Showing the first " + num(capRows.length) + " of " + num(rows.length) + "." : "") });
    c.el.classList.add("scrolly");
    var rowH = 28, axH = 26, w = Math.max(plot.clientWidth || 0, 280);
    var labelW = Math.min(170, Math.round(w * 0.3)), right = w - 18;
    var t0 = d3.min(capRows, function (r) { return r.start; }), t1 = d3.max(capRows, function (r) { return Math.max(r.end, r.start + 1); });
    var x = d3.scaleTime().domain([t0, t1]).range([labelW, right]);
    var tf = t1 - t0 < 10 * 60000 ? tfmt : hmfmt, ticks = x.ticks(timeTicks(right - labelW, tf));
    d3.select(plot.parentNode).insert("svg", function () { return plot; }).attr("class", "d3c tlaxis").attr("width", w).attr("height", axH).attr("aria-hidden", "true")
      .append("g").attr("class", "ax xax").attr("transform", "translate(0," + (axH - 1) + ")")
      .call(d3.axisTop(x).tickValues(ticks).tickFormat(function (d) { return tf.format(d); }).tickSizeOuter(0).tickPadding(4))
      .call(leanEnds, x, labelW, right);
    var P = plotSvg(plot, capRows.length * rowH, g.t);
    P.svg.append("g").attr("class", "ax").selectAll("line").data(ticks).join("line")
      .attr("x1", x).attr("x2", x).attr("y1", 0).attr("y2", P.h);
    var row = P.svg.append("g").selectAll("g").data(capRows).join("g").attr("class", "row")
      .attr("transform", function (r, i) { return "translate(0," + i * rowH + ")"; });
    row.append("rect").attr("class", "hit").attr("x", 0).attr("width", P.w).attr("height", rowH);
    row.append("text").attr("class", "rl").attr("x", labelW - 8).attr("y", rowH / 2).attr("dy", "0.35em").attr("text-anchor", "end")
      .text(function (r) { return fitChars(r.row, labelW - 12); });
    row.each(function (r) {
      var x0 = x(r.start), bw = Math.max(x(Math.max(r.end, r.start + 1)) - x0, 2), gr = d3.select(this);
      gr.append("rect").attr("class", "tlbar").attr("x", x0).attr("y", 5).attr("width", bw).attr("height", rowH - 10).attr("rx", 3).style("fill", r.color);
      if (bw > 44 && r.bar) gr.append("text").attr("class", "bl").attr("x", x0 + 6).attr("y", rowH / 2).attr("dy", "0.35em").text(fitChars(r.bar, bw - 12));
    });
    hover(row, function (r) { return r.tip; });
    if (c.link) linkify(row, c.link, function (r) { return r.tip; });
  }
  var STATUS_NOTE = " Blue: succeeded. Red: failed. Grey: running, incomplete or skipped.";

  function metric(name, stat, scope) {
    return ((D.aws || {}).metrics || []).filter(function (s) { return s.name === name && (!stat || s.stat === stat) && (!scope || s.scope === scope); });
  }
  // ---------- chart kit: D3, embedded in the page ----------
  // Charts drawn with D3 need no network. Their colours are CSS variables,
  // so a theme switch restyles them without a redraw.
  var V = { series: "var(--viz-series)", fail: "var(--viz-fail)", neutral: "var(--viz-neutral)", line: "var(--line)",
    viz: ["var(--viz-1)", "var(--viz-2)", "var(--viz-3)", "var(--viz-4)", "var(--viz-5)"] };
  var tipEl = null;
  // tipShow puts text in the shared tooltip, beside the pointer, or above
  // the focused mark when the keyboard moved there.
  function tipShow(ev, text) {
    if (!tipEl) { tipEl = el("div", { cls: "sp-tip", role: "tooltip" }); document.body.appendChild(tipEl); }
    tipEl.textContent = text;
    tipEl.hidden = false;
    var x, y;
    if (ev.clientX != null && ev.type !== "focus") { x = ev.clientX; y = ev.clientY; } else { var b = ev.target.getBoundingClientRect(); x = b.left + b.width / 2; y = b.top; }
    var w = tipEl.offsetWidth, h = tipEl.offsetHeight;
    tipEl.style.left = Math.max(8, Math.min(x + 12, window.innerWidth - w - 8)) + "px";
    tipEl.style.top = (y - h - 12 < 8 ? y + 18 : y - h - 12) + "px";
  }
  function tipHide() { if (tipEl) tipEl.hidden = true; }
  // hover gives each mark in sel a tooltip.
  function hover(sel, tip) {
    sel.on("pointerenter pointermove", function (ev, d) { tipShow(ev, tip(d)); }).on("pointerleave", tipHide);
  }
  // linkify makes each mark in sel with an href open it on click, and from
  // the keyboard: focusable, Enter or Space opens, the tooltip shows on focus.
  function linkify(sel, href, label) {
    sel.filter(function (d) { return href(d); }).classed("go", true)
      .attr("tabindex", 0).attr("role", "link").attr("aria-label", label)
      .on("click", function (ev, d) { tipHide(); location.hash = href(d); })
      .on("keydown", function (ev, d) { if (ev.key === "Enter" || ev.key === " ") { ev.preventDefault(); tipHide(); location.hash = href(d); } })
      .on("focus", function (ev, d) { tipShow(ev, label(d)); }).on("blur", tipHide);
  }
  // plotSvg adds an SVG as wide as the plot area; charts redraw on resize.
  function plotSvg(plot, h, title) {
    var w = Math.max(plot.clientWidth || 0, 280);
    var svg = d3.select(plot).append("svg").attr("class", "d3c").attr("width", w).attr("height", h)
      .attr("viewBox", "0 0 " + w + " " + h).attr("role", "group").attr("aria-label", title || "Chart");
    return { svg: svg, w: w, h: h };
  }
  // legendNode names each series' colour, above the plot.
  function legendNode(items) {
    return el("div", { cls: "legend" }, items.map(function (it) { return el("span", null, el("i", { cls: "sw", style: "background:" + it.color }), it.label); }));
  }
  // Axis units: ticks fall on round numbers of the unit that suits the
  // largest value (200 MiB, 5 min), not of the base unit (bytes, ms).
  var UNITS = { bytes: [[1, "B"], [1024, "KiB"], [1048576, "MiB"], [1073741824, "GiB"], [1099511627776, "TiB"]],
    ms: [[1, "ms"], [1000, "s"], [60000, "min"], [3600000, "h"]], count: [[1, ""]], pct: [[1, "%"]] };
  var FMT = { bytes: bytes, ms: dur, count: function (v) { return num(Math.round(v * 10) / 10); }, pct: function (v) { return Math.round(v) + "%"; } };
  // unitAxis is a linear scale from 0 to max over range (so the largest
  // bar, such as a full heap, fills it), with round ticks in the chosen
  // unit and their labels.
  function unitAxis(kind, max, range, n) {
    var u = UNITS[kind][0];
    max = max || 1;
    UNITS[kind].forEach(function (x) { if (max >= 2 * x[0]) u = x; });
    var f = d3.format("~g");
    var ticks = d3.ticks(0, max / u[0], n);
    if (kind === "count") ticks = ticks.filter(Number.isInteger); // no half tasks
    return { x: d3.scaleLinear().domain([0, max]).range(range), ticks: ticks.map(function (t) { return t * u[0]; }),
      label: function (v) { return v === 0 ? "0" : f(v / u[0]) + (u[1] === "%" ? "%" : u[1] ? " " + u[1] : ""); } };
  }
  // timeTicks is how many time ticks fit in px: labels with seconds need
  // more room than hours and minutes.
  function timeTicks(px, tf) { return Math.max(2, Math.floor(px / (tf === tfmt ? 110 : 80))); }
  // leanEnds turns the labels of a bottom axis that sit near either end
  // of [lo, hi] inward, so none runs off the chart.
  function leanEnds(ax, x, lo, hi) {
    ax.selectAll(".tick text").attr("text-anchor", function (d) { var px = x(d); return px > hi - 28 ? "end" : px < lo + 28 ? "start" : "middle"; });
  }
  // fitChars cuts s to about px pixels of 11px text, keeping the full text
  // for the tooltip.
  function fitChars(s, px) { var n = Math.max(4, Math.floor(px / 6.3)); return s.length <= n ? s : s.slice(0, n - 1) + "…"; }

  // hbarChart draws horizontal bars, stacked from series, one row each,
  // with the row's total at its end (series marked rest, such as unused
  // space, are drawn but not counted; a series' detail(row), when given,
  // adds to its tooltip); rows link to a page when link returns one.
  function hbarChart(c, rows, series, g, kind, link) {
    var format = FMT[kind];
    if (!rows.length) { waitText(c, "Nothing to chart for this run."); return; }
    // a series with nothing in it only clutters the legend
    series = series.filter(function (sr) { return rows.some(function (r) { return sr.value(r) > 0; }); });
    var plot = frame(c, g);
    if (series.length > 1) plot.parentNode.insertBefore(legendNode(series), plot);
    var rowH = 26, top = 4, bandH = rows.length * rowH;
    var P = plotSvg(plot, top + bandH + 26, g.t);
    var labelW = Math.min(170, Math.round(P.w * 0.36)), noteW = 76;
    var val = function (sr, r) { return Math.max(sr.value(r), 0) || 0; };
    var sum = function (r, all) { return series.reduce(function (s, sr) { return s + (all || !sr.rest ? val(sr, r) : 0); }, 0); };
    var U = unitAxis(kind, d3.max(rows, function (r) { return sum(r, true); }), [labelW, P.w - noteW], Math.max(2, Math.floor((P.w - labelW - noteW) / 80))), x = U.x;
    var ax = P.svg.append("g").attr("class", "ax").attr("transform", "translate(0," + (top + bandH) + ")")
      .call(d3.axisBottom(x).tickValues(U.ticks).tickFormat(U.label).tickSize(-bandH).tickPadding(6));
    ax.select(".domain").remove();
    var row = P.svg.append("g").selectAll("g").data(rows).join("g").attr("class", "row")
      .attr("transform", function (r, i) { return "translate(0," + (top + i * rowH) + ")"; });
    row.append("rect").attr("class", "hit").attr("x", 0).attr("width", P.w).attr("height", rowH);
    row.append("text").attr("class", "rl").attr("x", labelW - 8).attr("y", rowH / 2).attr("dy", "0.35em").attr("text-anchor", "end")
      .text(function (r) { return fitChars(r.label, labelW - 12); });
    row.each(function (r) {
      var at = 0, gr = d3.select(this);
      series.forEach(function (sr) {
        var v = val(sr, r);
        if (!v) return;
        gr.append("rect").datum({ tip: r.label + "\n" + sr.label + ": " + format(v) + (sr.detail ? " (" + sr.detail(r) + ")" : "") })
          .attr("x", x(at)).attr("y", rowH * 0.14).attr("width", Math.max(x(at + v) - x(at), 1)).attr("height", rowH * 0.72).style("fill", sr.color);
        at += v;
      });
      gr.append("text").attr("class", "note").attr("x", x(at) + 6).attr("y", rowH / 2).attr("dy", "0.35em").text(format(sum(r)));
    });
    hover(row.selectAll("rect:not(.hit)"), function (d) { return d.tip; });
    var summary = function (r) { return r.label + ": " + series.map(function (sr) { return sr.label + " " + format(val(sr, r)); }).join(", "); };
    hover(row.select(".rl"), summary);
    if (link) linkify(row, link, summary);
  }
  // timeChart draws series over one time axis: lines, areas or steps. A
  // cursor snaps to the nearest sample and the tooltip lists every series
  // there. A series has label, color, points ([ms, value], in time order),
  // and optionally step (a value holds until the next point), area (fill
  // opacity under it), dash and dots. opts.max fixes the top of the scale.
  function timeChart(c, series, g, kind, opts) {
    opts = opts || {};
    series = series.filter(function (s) { return s.points.length; });
    if (!series.length) { waitText(c, "Nothing to chart for this run."); return; }
    var format = FMT[kind];
    var plot = frame(c, g);
    plot.parentNode.insertBefore(legendNode(series), plot);
    var H = 280, m = { l: 64, r: 18, t: 10, b: 28 };
    var P = plotSvg(plot, H, g.t);
    var t0 = d3.min(series, function (s) { return s.points[0][0]; }), t1 = d3.max(series, function (s) { return s.points[s.points.length - 1][0]; });
    if (t1 <= t0) t1 = t0 + 1000;
    var x = d3.scaleTime().domain([t0, t1]).range([m.l, P.w - m.r]);
    var ymax = opts.max != null ? opts.max : d3.max(series, function (s) { return d3.max(s.points, function (p) { return p[1]; }); });
    var U = unitAxis(kind, ymax, [H - m.b, m.t], Math.max(2, Math.floor((H - m.t - m.b) / 45))), y = U.x;
    var tf = t1 - t0 < 10 * 60000 ? tfmt : hmfmt;
    P.svg.append("g").attr("class", "ax").attr("transform", "translate(" + m.l + ",0)")
      .call(d3.axisLeft(y).tickValues(U.ticks).tickFormat(U.label).tickSize(-(P.w - m.l - m.r)).tickPadding(6))
      .select(".domain").remove();
    P.svg.append("g").attr("class", "ax xax").attr("transform", "translate(0," + (H - m.b) + ")")
      .call(d3.axisBottom(x).ticks(timeTicks(P.w - m.l - m.r, tf)).tickFormat(function (d) { return tf.format(d); }).tickSizeOuter(0).tickPadding(6))
      .call(leanEnds, x, m.l, P.w - m.r);
    var X = function (p) { return x(p[0]); }, Y = function (p) { return y(Math.min(p[1], U.x.domain()[1])); };
    series.forEach(function (s) {
      var curve = s.step ? d3.curveStepAfter : d3.curveLinear, pts = s.points.filter(function (p) { return p[1] != null; });
      if (s.area != null) P.svg.append("path").attr("d", d3.area().curve(curve).x(X).y0(y(0)).y1(Y)(pts)).style("fill", s.color).style("fill-opacity", s.area);
      P.svg.append("path").attr("class", "ln").attr("d", d3.line().curve(curve).x(X).y(Y)(pts)).style("stroke", s.color).style("stroke-dasharray", s.dash || null);
      if (s.dots) P.svg.append("g").selectAll("circle").data(pts).join("circle").attr("cx", X).attr("cy", Y).attr("r", 2.5).style("fill", s.color);
    });
    // the cursor: the nearest sample time, and each series' value there
    var times = [];
    series.forEach(function (s) { s.points.forEach(function (p) { times.push(p[0]); }); });
    times = Array.from(new Set(times)).sort(function (p, q) { return p - q; });
    var valueAt = function (s, t) {
      if (s.step) { var i = d3.bisectRight(s.points.map(function (p) { return p[0]; }), t) - 1; return i < 0 ? null : s.points[i][1]; }
      var j = d3.bisectCenter(s.points.map(function (p) { return p[0]; }), t);
      return s.points[j][0] === t ? s.points[j][1] : null;
    };
    var cur = P.svg.append("line").attr("class", "cursor").attr("y1", m.t).attr("y2", H - m.b).style("display", "none");
    P.svg.append("rect").attr("class", "hit").attr("x", m.l).attr("y", m.t).attr("width", P.w - m.l - m.r).attr("height", H - m.t - m.b)
      .on("pointermove", function (ev) {
        var t = times[d3.bisectCenter(times, +x.invert(d3.pointer(ev)[0]))];
        cur.attr("x1", x(t)).attr("x2", x(t)).style("display", null);
        tipShow(ev, dfmt.format(new Date(t)) + series.map(function (s) { var v = valueAt(s, t); return v == null ? "" : "\n" + s.label + ": " + format(v); }).join(""));
      })
      .on("pointerleave", function () { cur.style("display", "none"); tipHide(); });
  }
  // colChart draws one vertical column per entry of cols (label, value,
  // tip, and optionally href), with a y axis in round units and, when
  // opts.ref is set ({value, label}), a dashed line across at that value.
  // Axis labels are thinned so they never overlap.
  function colChart(c, cols, g, kind, opts) {
    opts = opts || {};
    var plot = frame(c, g);
    var legend = [{ label: opts.series || "", color: opts.color || V.series }];
    if (opts.ref) legend.push({ label: opts.ref.label, color: V.neutral });
    if (opts.series) plot.parentNode.insertBefore(legendNode(legend), plot);
    var H = 280, m = { l: 64, r: 18, t: 10, b: 30 };
    var P = plotSvg(plot, H, g.t);
    // a few columns stay column-width rather than filling the chart
    var x = d3.scaleBand().domain(d3.range(cols.length)).range([m.l, Math.min(P.w - m.r, m.l + cols.length * 90)]).paddingInner(0.12);
    var top = Math.max(d3.max(cols, function (d) { return d.value; }) || 0, opts.ref ? opts.ref.value : 0);
    var U = unitAxis(kind, top, [H - m.b, m.t], Math.max(2, Math.floor((H - m.t - m.b) / 45))), y = U.x;
    P.svg.append("g").attr("class", "ax").attr("transform", "translate(" + m.l + ",0)")
      .call(d3.axisLeft(y).tickValues(U.ticks).tickFormat(U.label).tickSize(-(P.w - m.l - m.r)).tickPadding(6))
      .select(".domain").remove();
    var widest = d3.max(cols, function (d) { return d.label.length; }) * 6.3 + 10;
    var every = Math.max(1, Math.ceil(widest / Math.max(x.step(), 1)));
    P.svg.append("g").attr("class", "ax xax").attr("transform", "translate(0," + (H - m.b) + ")")
      .call(d3.axisBottom(x).tickValues(d3.range(0, cols.length, every)).tickFormat(function (i) { return cols[i].label; }).tickSizeOuter(0).tickPadding(6));
    var bar = P.svg.append("g").selectAll("rect").data(cols).join("rect")
      .attr("x", function (d, i) { return x(i); }).attr("width", Math.max(x.bandwidth(), 1))
      .attr("y", function (d) { return y(d.value); }).attr("height", function (d) { return Math.max(H - m.b - y(d.value), d.value > 0 ? 1 : 0); })
      .style("fill", opts.color || V.series);
    hover(bar, function (d) { return d.tip; });
    linkify(bar, function (d) { return d.href; }, function (d) { return d.tip; });
    if (opts.ref) P.svg.append("line").attr("class", "ref").attr("x1", m.l).attr("x2", P.w - m.r).attr("y1", y(opts.ref.value)).attr("y2", y(opts.ref.value)).style("stroke", V.neutral);
  }
  // scatterChart draws one mark per point (x, y, tip, bad): circles, and
  // triangles in the failure colour for bad points, with both axes in
  // round units from zero. labels are the two legend entries, the x axis
  // title and, optionally, the y axis title; it returns the plot area.
  function scatterChart(c, pts, g, xKind, yKind, labels) {
    var plot = frame(c, g);
    plot.parentNode.insertBefore(legendNode([{ label: labels[0], color: V.series }].concat(pts.some(function (p) { return p.bad; }) ? [{ label: labels[1], color: V.fail }] : [])), plot);
    var H = 300, m = { l: 64, r: 18, t: 10, b: 44 };
    var P = plotSvg(plot, H, g.t);
    var XU = unitAxis(xKind, d3.max(pts, function (p) { return p.x; }), [m.l, P.w - m.r], Math.max(2, Math.floor((P.w - m.l - m.r) / 80)));
    var YU = unitAxis(yKind, d3.max(pts, function (p) { return p.y; }), [H - m.b, m.t], Math.max(2, Math.floor((H - m.t - m.b) / 45)));
    P.svg.append("g").attr("class", "ax").attr("transform", "translate(" + m.l + ",0)")
      .call(d3.axisLeft(YU.x).tickValues(YU.ticks).tickFormat(YU.label).tickSize(-(P.w - m.l - m.r)).tickPadding(6)).select(".domain").remove();
    P.svg.append("g").attr("class", "ax").attr("transform", "translate(0," + (H - m.b) + ")")
      .call(d3.axisBottom(XU.x).tickValues(XU.ticks).tickFormat(XU.label).tickSize(-(H - m.t - m.b)).tickPadding(6)).call(leanEnds, XU.x, m.l, P.w - m.r).select(".domain").remove();
    P.svg.append("text").attr("class", "axt").attr("x", (m.l + P.w - m.r) / 2).attr("y", H - 6).attr("text-anchor", "middle").text(labels[2]);
    if (labels[3]) P.svg.append("text").attr("class", "axt").attr("transform", "translate(12," + ((m.t + H - m.b) / 2) + ") rotate(-90)").attr("text-anchor", "middle").text(labels[3]);
    // good points first, so failures draw on top
    var order = pts.filter(function (p) { return !p.bad; }).concat(pts.filter(function (p) { return p.bad; }));
    var tri = d3.symbol(d3.symbolTriangle, 60)();
    var mk = P.svg.append("g").selectAll("path").data(order).join("path").attr("class", "pt")
      .attr("d", function (p) { return p.bad ? tri : d3.symbol(d3.symbolCircle, 30)(); })
      .attr("transform", function (p) { return "translate(" + XU.x(p.x) + "," + YU.x(p.y) + ")"; })
      .style("fill", function (p) { return p.bad ? V.fail : V.series; });
    hover(mk, function (p) { return p.tip; });
    return plot;
  }
  // Task scatter axes: what goes across and what goes up, remembered while
  // the page is open.
  var SCATTER = { x: "start", y: "dur" };
  var SCATTER_X = {
    start: { label: "Started", kind: "ms", v: function (r, started) { return started(r); } },
    input: { label: "Input read", kind: "bytes", v: function (r) { return r[T.input]; } },
    rows: { label: "Rows read", kind: "count", v: function (r) { return r[T.rows]; } },
    shRead: { label: "Shuffle read", kind: "bytes", v: function (r) { return r[T.shRead]; } }
  };
  var SCATTER_Y = {
    dur: { label: "Duration", kind: "ms", v: function (r) { return r[T.dur]; }, read: "Higher marks took longer." },
    gc: { label: "Garbage collection", kind: "ms", v: function (r) { return r[T.gc]; }, read: "High marks spent long in garbage collection: too little memory per task, or large objects." },
    fetch: { label: "Waiting for shuffle data", kind: "ms", v: function (r) { return r[T.fetch]; }, read: "High marks waited long for data from other executors: a busy network, a slow or lost executor, or skewed shuffle blocks." },
    sched: { label: "Scheduler delay", kind: "ms", v: function (r) { return r[T.sched]; }, read: "High marks waited long to start or to report back: a busy driver or large task closures." },
    spill: { label: "Spilled", kind: "bytes", v: function (r) { return r[T.spill]; }, read: "High marks ran out of execution memory and wrote to disk." }
  };

  // ---------- stage × executor heatmap ----------
  // One square per executor (row) and stage (column), coloured by the
  // chosen metric, either as is or as a multiple of the stage's median
  // executor, so an executor that did far more (or took far longer) than
  // its peers in a stage stands out.
  var HEAT = { metric: "dur", mode: "abs", all: false };
  var HEAT_METRICS = [["dur", "Task time", "ms"], ["gcShare", "GC share of task time", "share"], ["input", "Input", "bytes"], ["shRead", "Shuffle read", "bytes"],
    ["shWrite", "Shuffle write", "bytes"], ["diskSpill", "Spilled to disk", "bytes"], ["peakHeap", "Peak heap", "bytes"], ["failed", "Failed tasks", "count"], ["tasks", "Tasks", "count"]];
  var HEAT_MAX_CELLS = 5000, HEAT_STAGES = 30, HEAT_EXECS = 50;
  var focusExec = null; // the executor row to highlight when a square opens its stage
  function heatValue(cell, metric) {
    if (metric === "gcShare") return cell[C.dur] > 0 ? cell[C.gc] / cell[C.dur] : null;
    return cell[C[metric]];
  }
  function heatFormat(kind, v) { return v == null ? "—" : kind === "share" ? pct(v) : kind === "count" ? num(v) : FMT[kind](v); }
  // heatCells are a stage's per-executor totals where tasks ran (the
  // driver has heap samples for every stage but usually runs none).
  function heatCells(st) { var d = D.detail[st.key]; return d ? d.cells.filter(function (r) { return r[C.tasks] > 0; }) : []; }
  function heatmap(c) {
    var withCells = stages.filter(function (st) { return heatCells(st).length; });
    if (!withCells.length) { waitText(c, D.collected ? "No per-executor totals were collected for any stage." : "Per-task detail was not collected for this run."); return; }
    var m = HEAT_METRICS.filter(function (x) { return x[0] === HEAT.metric; })[0], kind = m[2], rel = HEAT.mode === "rel";
    // every executor that ran tasks in these stages, with its task time
    var execTime = {};
    withCells.forEach(function (st) { heatCells(st).forEach(function (r) { execTime[r[C.exec]] = (execTime[r[C.exec]] || 0) + r[C.dur]; }); });
    var execIdx = Object.keys(execTime).map(Number);
    var canAll = withCells.length * execIdx.length <= HEAT_MAX_CELLS;
    var all = HEAT.all && canAll;
    var cols = all ? withCells : withCells.slice().sort(function (p, q) { return q.dur - p.dur; }).slice(0, HEAT_STAGES);
    cols.sort(function (p, q) { return p.id - q.id || p.attempt - q.attempt; });
    var rows = all ? execIdx : execIdx.slice().sort(function (p, q) { return execTime[q] - execTime[p]; }).slice(0, HEAT_EXECS);
    rows.sort(function (p, q) { return String(D.execs[p]).localeCompare(String(D.execs[q]), undefined, { numeric: true }); });
    var inRows = {}; rows.forEach(function (x) { inRows[x] = 1; });
    // squares, and each stage's median executor for the relative view
    var cells = [], median = {};
    cols.forEach(function (st, ci) {
      var vals = [];
      heatCells(st).forEach(function (r) {
        var v = heatValue(r, m[0]);
        if (v != null) vals.push(v);
        if (inRows[r[C.exec]]) cells.push({ st: st, ci: ci, ri: rows.indexOf(r[C.exec]), r: r, v: v });
      });
      median[st.key] = d3.median(vals) || 0;
    });
    cells.forEach(function (d) { d.shown = d.v == null ? null : rel ? (median[d.st.key] > 0 ? d.v / median[d.st.key] : null) : d.v; });
    var top = d3.max(cells, function (d) { return d.shown; }) || 0;
    var trimmed = cols.length < withCells.length || rows.length < execIdx.length;
    var plot = frame(c, { t: "Which executor behaved differently?",
      shows: "Each square is one executor's part of one stage: rows are executors, columns are stages. " +
        (trimmed ? "Showing the " + num(cols.length) + " of " + num(withCells.length) + " stages with the most task time and the " + num(rows.length) + " of " + num(execIdx.length) + " executors with the most task time. " : "Every stage and executor. ") +
        (D.cellsCapped ? "Collection stopped per-executor totals partway (the app-wide cap), so later stages have none. " : "") +
        "Click a square to open the stage with that executor's row marked.",
      read: rel ? "Each square is a multiple of the stage's median executor: pale is typical, dark is 3× or more. A dark square in an otherwise pale column is one executor that got more data or ran slower in that stage (skew); a row dark across many columns points at that executor or its node."
        : "Darker is more. A row darker than the rest across many stages is an executor, or its node, that was slower or got more data; one dark square in an even column is skew in that stage. Empty squares ran no tasks there." });
    // controls
    var pick = el("select", { "aria-label": "What to compare" });
    HEAT_METRICS.forEach(function (x) { pick.appendChild(el("option", { value: x[0], text: x[1], selected: x[0] === HEAT.metric })); });
    pick.addEventListener("change", function () { HEAT.metric = pick.value; drawSlot(c); });
    var mode = el("select", { "aria-label": "Absolute or relative" },
      el("option", { value: "abs", text: "As measured", selected: !rel }), el("option", { value: "rel", text: "Against the stage's median executor", selected: rel }));
    mode.addEventListener("change", function () { HEAT.mode = mode.value; drawSlot(c); });
    var tools = el("div", { cls: "bar-tools" }, pick, mode);
    if (canAll && (trimmed || HEAT.all)) {
      var btn = el("button", { type: "button", cls: "more", text: HEAT.all ? "Show the busiest only" : "Show all " + num(withCells.length) + " stages and " + num(execIdx.length) + " executors" });
      btn.addEventListener("click", function () { HEAT.all = !HEAT.all; drawSlot(c); });
      tools.appendChild(btn);
    }
    var fill = m[0] === "failed" ? V.fail : V.viz[0];
    tools.appendChild(el("span", { cls: "heatkey" }, el("span", { text: rel ? "typical" : "0" }), el("i", { style: "background:linear-gradient(90deg,transparent," + fill + ")" }),
      el("span", { text: rel ? "3× or more" : heatFormat(kind, top) })));
    plot.parentNode.insertBefore(tools, plot);
    // layout: squares shrink to fit, down to 12 px, then the chart scrolls sideways
    var labelW = 96, headH = 56, rowH = 18, avail = Math.max(plot.clientWidth || 0, 280) - labelW - 8;
    var cw = Math.max(12, Math.min(36, Math.floor(avail / cols.length)));
    var W = labelW + cols.length * cw + 28, H = headH + rows.length * rowH; // room for the last slanted label
    plot.classList.add("heatwrap");
    // sized in CSS too, since the shared .chart svg rule would squeeze it to
    // the card; wider than the card, it scrolls sideways instead
    var svg = d3.select(plot).append("svg").attr("class", "d3c heat").attr("width", W).attr("height", H).style("width", W + "px").style("height", H + "px").attr("role", "group").attr("aria-label", "Which executor behaved differently?");
    var shade = rel ? function (v) { return Math.min(v / 3, 1); } : function (v) { return top > 0 ? Math.sqrt(v / top) : 0; };
    svg.append("g").selectAll("rect").data(rows).join("rect").attr("class", "lane").attr("x", labelW).attr("y", function (x, i) { return headH + i * rowH; })
      .attr("width", cols.length * cw).attr("height", rowH - 2);
    var sq = svg.append("g").selectAll("rect").data(cells.filter(function (d) { return d.shown != null; })).join("rect").attr("class", "sq")
      .attr("x", function (d) { return labelW + d.ci * cw + 1; }).attr("y", function (d) { return headH + d.ri * rowH; })
      .attr("width", cw - 2).attr("height", rowH - 2).style("fill", fill).style("fill-opacity", function (d) { return 0.06 + 0.94 * shade(d.shown); });
    var execLabel = function (i) { var id = D.execs[i], x = execByID[id]; return "Executor " + id + (x && x.host ? " on " + x.host.split(".")[0] : ""); };
    hover(sq, function (d) {
      var r = d.r, med = median[d.st.key];
      return execLabel(r[C.exec]) + " · Stage " + d.st.key + "\n" + m[1] + ": " + heatFormat(kind, d.v) +
        (med > 0 ? " (" + (d.v / med).toFixed(1) + "× the stage's median executor, " + heatFormat(kind, med) + ")" : "") +
        "\n" + num(r[C.tasks]) + (r[C.tasks] === 1 ? " task" : " tasks") + (r[C.failed] ? ", " + num(r[C.failed]) + " failed" : "") + ", task time " + dur(r[C.dur]);
    });
    sq.on("click", function (ev, d) { tipHide(); focusExec = D.execs[d.r[C.exec]]; location.hash = "#stage/" + d.st.key; });
    var rl = svg.append("g").selectAll("text").data(rows).join("text").attr("class", "rl").attr("x", labelW - 6)
      .attr("y", function (x, i) { return headH + i * rowH + rowH / 2 - 1; }).attr("dy", "0.35em").attr("text-anchor", "end")
      .text(function (x) { return fitChars("Executor " + D.execs[x], labelW - 10); });
    linkify(rl, function (x) { return "#executor/" + encodeURIComponent(D.execs[x]); }, execLabel);
    var cl = svg.append("g").selectAll("text").data(cols).join("text").attr("class", "cl")
      .attr("transform", function (st, i) { return "translate(" + (labelW + i * cw + cw / 2 + 3) + "," + (headH - 6) + ") rotate(-60)"; })
      .text(function (st) { return st.key; });
    linkify(cl, function (st) { return "#stage/" + st.key; }, function (st) { return stageName(st); });
  }
  // ---------- run timeline ----------
  // Everything that ran, on one time axis: queries, jobs and stages as
  // lanes of bars, executors alive, tasks running against task slots, and
  // node CPU and waiting containers when CloudWatch was read. Driver gaps
  // (no job running) are shaded behind every track. One overlay handles
  // the pointer: hovering moves a cursor across every track and lists what
  // was happening then, and dims what is unrelated to the bar under it;
  // dragging zooms every track; a click opens the bar under it.
  var TL = { zoom: null };
  var TL_LANES = { query: 4, job: 6, stage: 10 };
  // pack puts overlapping bars in separate lanes, up to max; bars that
  // find no lane are counted, not drawn.
  function pack(items, max) {
    var ends = [], dropped = 0;
    items.slice().sort(function (p, q) { return p.start - q.start; }).forEach(function (it) {
      for (var i = 0; i < ends.length; i++) if (ends[i] <= it.start) break;
      if (i === ends.length && ends.length >= max) { dropped++; it.lane = -1; return; }
      ends[i] = it.end;
      it.lane = i;
    });
    return { lanes: Math.max(1, ends.length), dropped: dropped };
  }
  // stepAt is a step series' value at t ([ms, value] points in time order).
  function stepAt(pts, t) { var i = d3.bisectRight(pts.map(function (p) { return p[0]; }), t) - 1; return i < 0 ? null : pts[i][1]; }
  function runTimeline(c) {
    // the span covers the application and everything in it, in case a
    // clock or a cut-off log puts a job outside the application's times
    var starts = [a.start], ends = [appEnd];
    jobs.concat(stages).forEach(function (x) { starts.push(x.submitted); ends.push(x.completed); });
    queries.forEach(function (q) { starts.push(q.start); ends.push(q.end); });
    var t0 = d3.min(starts.filter(Boolean)), t1 = d3.max(ends.filter(Boolean));
    if (!t0 || !t1 || t1 <= t0) { waitText(c, "The log has no start and end times to draw."); return; }
    var dom = TL.zoom || [t0, t1];
    var clip = function (s, e) { return e > dom[0] && s < dom[1]; };
    // bars
    var qBars = queries.filter(function (q) { return q.start; }).map(function (q) { return { kind: "query", id: q.id, start: q.start, end: q.end || t1, status: queryStatus(q), q: q, label: "Query " + q.id + (q.desc ? ": " + q.desc : "") }; });
    var jBars = jobs.filter(function (j) { return j.submitted; }).map(function (j) { return { kind: "job", id: j.id, start: j.submitted, end: j.completed || t1, status: j.status, j: j, label: "Job " + j.id + ": " + (j.desc || j.name || "") }; });
    var sBars = stages.filter(function (s) { return s.submitted; }).map(function (s) { return { kind: "stage", id: s.key, start: s.submitted, end: s.completed || t1, status: s.status, s: s, label: stageName(s) }; });
    var packs = { query: pack(qBars, TL_LANES.query), job: pack(jBars, TL_LANES.job), stage: pack(sBars, TL_LANES.stage) };
    // links between bars, for dimming what is unrelated to the hovered one
    var jobOfStage = {}, queryOfJob = {};
    jobs.forEach(function (j) { (j.stages || []).forEach(function (sid) { (jobOfStage[sid] = jobOfStage[sid] || []).push(j.id); }); if (j.sql != null) queryOfJob[j.id] = j.sql; });
    function related(b) {
      var r = { query: {}, job: {}, stage: {} };
      var addJob = function (jid) { r.job[jid] = 1; if (queryOfJob[jid] != null) r.query[queryOfJob[jid]] = 1; var j = jobByID[jid]; if (j) (j.stages || []).forEach(function (sid) { (stagesByID[sid] || []).forEach(function (s) { r.stage[s.key] = 1; }); }); };
      if (b.kind === "query") { r.query[b.id] = 1; (b.q.jobs || []).forEach(addJob); }
      if (b.kind === "job") addJob(b.id);
      if (b.kind === "stage") { r.stage[b.id] = 1; (jobOfStage[b.s.id] || []).forEach(function (jid) { r.job[jid] = 1; if (queryOfJob[jid] != null) r.query[queryOfJob[jid]] = 1; }); }
      return r;
    }
    // series
    var workers = execs.filter(function (x) { return x.id !== "driver" && x.added; });
    var evs = [];
    workers.forEach(function (x) { evs.push([x.added, 1, x.cores]); if (x.removed) evs.push([x.removed, -1, -x.cores]); });
    evs.sort(function (p, q) { return p[0] - q[0]; });
    var alive = [[t0, 0]], slots = [[t0, 0]], n = 0, k = 0;
    evs.forEach(function (e) { n += e[1]; k += e[2]; alive.push([e[0], n]); slots.push([e[0], k]); });
    alive.push([t1, n]); slots.push([t1, k]);
    var R = D.running, busy = [];
    if (R && R.busy.length) R.busy.forEach(function (b, i) { busy.push([R.start + i * R.bucketMs, b / R.bucketMs]); });
    var cpu = metric("CPUUtilization", "Average"), waiting = metric("ContainerPending");
    // tracks, top to bottom
    var laneH = { query: 14, job: 14, stage: 9 };
    var tracks = [];
    if (qBars.length) tracks.push({ kind: "query", title: "Queries", h: packs.query.lanes * laneH.query + 4 });
    tracks.push({ kind: "job", title: "Jobs", h: packs.job.lanes * laneH.job + 4 });
    if (sBars.length) tracks.push({ kind: "stage", title: "Stages", h: packs.stage.lanes * laneH.stage + 4 });
    tracks.push({ kind: "execs", title: "Executors alive", short: "Executors", h: 54 });
    if (busy.length) tracks.push({ kind: "tasks", title: "Tasks running", short: "Tasks", h: 70 });
    if (cpu.length) tracks.push({ kind: "cpu", title: "Node CPU", short: "CPU", h: 60 });
    if (waiting.length) tracks.push({ kind: "wait", title: "Containers waiting", short: "Waiting", h: 44 });
    var gap = 10, axisH = 24, y = axisH;
    tracks.forEach(function (tr) { tr.y = y; y += tr.h + gap; });
    var H = y;
    var dropped = packs.query.dropped + packs.job.dropped + packs.stage.dropped;
    var plot = frame(c, { t: "What happened when",
      shows: "Every query, job and stage on one time axis, above the executors alive, the tasks running against the task slots (executor cores)" + (cpu.length ? ", node CPU" : "") + (waiting.length ? ", containers waiting for room" : "") +
        ". Shaded stretches are driver gaps: no job was running. Red marks are failed stages and executors lost or killed." +
        (dropped ? " " + num(dropped) + " bars that overlapped too many others are left out; zoom in or use the Jobs and Stages tabs." : "") +
        " The event log has garbage collection per task, not over time, so it is on the stage pages instead.",
      read: "Drag across any track to zoom all of them; hover to see everything at that moment and to fade what is unrelated to the bar under the pointer; click a bar to open it. Look for shaded stretches (the cluster waited on the driver), tasks running well under the slots (idle cores), and executors dropping while work still ran." });
    var tools = el("div", { cls: "bar-tools" });
    if (TL.zoom) {
      var reset = el("button", { type: "button", cls: "more", text: "Show the whole run" });
      reset.addEventListener("click", function () { TL.zoom = null; drawSlot(c); });
      tools.appendChild(reset);
      tools.appendChild(el("span", { cls: "count", text: "Zoomed to " + tfmt.format(new Date(dom[0])) + " – " + tfmt.format(new Date(dom[1])) + " (" + dur(dom[1] - dom[0]) + ")" }));
    } else tools.appendChild(el("span", { cls: "count", text: "Drag across the chart to zoom in." }));
    plot.parentNode.insertBefore(tools, plot);
    var P = plotSvg(plot, H, "What happened when");
    // track titles are bold 11 px, about 7 px a character; short names
    // when the full ones do not fit
    var lw = Math.min(130, Math.round(P.w * 0.24)), right = P.w - 12;
    tracks.forEach(function (tr) { tr.name = tr.title.length * 7 + 10 > lw && tr.short ? tr.short : tr.title; });
    var x = d3.scaleTime().domain(dom).range([lw, right]);
    var X = function (t) { return x(Math.max(dom[0], Math.min(dom[1], t))); };
    var tf = dom[1] - dom[0] < 10 * 60000 ? tfmt : hmfmt;
    P.svg.append("defs").append("clipPath").attr("id", "tlclip").append("rect").attr("x", lw).attr("y", 0).attr("width", right - lw).attr("height", H);
    // driver gaps behind everything
    P.svg.append("g").attr("clip-path", "url(#tlclip)").selectAll("rect").data((D.gaps || []).filter(function (g) { return clip(g[0], g[1]); })).join("rect").attr("class", "tlgap")
      .attr("x", function (g) { return X(g[0]); }).attr("width", function (g) { return Math.max(X(g[1]) - X(g[0]), 1); }).attr("y", axisH).attr("height", H - axisH);
    P.svg.append("g").attr("class", "ax xax").attr("transform", "translate(0," + (axisH - 2) + ")")
      .call(d3.axisTop(x).ticks(timeTicks(right - lw, tf)).tickFormat(function (d) { return tf.format(d); }).tickSize(-(H - axisH)).tickSizeOuter(0).tickPadding(4))
      .call(leanEnds, x, lw, right).select(".domain").remove();
    var body = P.svg.append("g").attr("clip-path", "url(#tlclip)");
    var barsByKind = { query: qBars, job: jBars, stage: sBars }, drawn = [];
    tracks.forEach(function (tr) {
      P.svg.append("text").attr("class", "tlt").attr("x", lw - 8).attr("y", tr.y + Math.min(tr.h, 28) / 2).attr("dy", "0.35em").attr("text-anchor", "end").text(tr.name).append("title").text(tr.title);
      if (barsByKind[tr.kind]) {
        var lh = laneH[tr.kind], list = barsByKind[tr.kind].filter(function (b) { return b.lane >= 0 && clip(b.start, b.end); });
        list.forEach(function (b) { b.y = tr.y + 2 + b.lane * lh; b.h = lh - 2; drawn.push(b); });
        body.append("g").selectAll("rect").data(list).join("rect").attr("class", function (b) { return "tlbar k-" + b.kind; })
          .attr("x", function (b) { return X(b.start); }).attr("width", function (b) { return Math.max(X(b.end) - X(b.start), 1.5); })
          .attr("y", function (b) { return b.y; }).attr("height", function (b) { return b.h; }).attr("rx", 2).style("fill", function (b) { return statusColor(b.status); });
        if (tr.kind === "stage") body.append("g").selectAll("path").data(list.filter(function (b) { return b.status === "failed" && b.s.completed; })).join("path").attr("class", "tlmark")
          .attr("d", d3.symbol(d3.symbolDiamond, 40)()).attr("transform", function (b) { return "translate(" + X(b.end) + "," + (b.y + b.h / 2) + ")"; });
        return;
      }
      var series = tr.kind === "execs" ? [{ pts: alive, step: true, cls: "s1", area: true }] :
        tr.kind === "tasks" ? [{ pts: busy, cls: "s1", area: true }, { pts: slots, step: true, cls: "ref" }] :
        tr.kind === "cpu" ? cpu.map(function (sr, i) { return { pts: sr.points, cls: "c" + (i % 5) }; }) :
        waiting.map(function (sr) { return { pts: sr.points, step: true, cls: "sf", area: true }; });
      var ymax = tr.kind === "cpu" ? 100 : Math.max(1, d3.max(series, function (sr) { return d3.max(sr.pts, function (p) { return p[1]; }); }));
      var yy = d3.scaleLinear().domain([0, ymax]).range([tr.y + tr.h, tr.y + 2]);
      P.svg.append("text").attr("class", "tlv").attr("x", lw - 8).attr("y", tr.y + tr.h - 2).attr("text-anchor", "end").text(tr.kind === "cpu" ? "0–100%" : "0–" + num(Math.round(ymax)));
      P.svg.append("line").attr("class", "tlbase").attr("x1", lw).attr("x2", right).attr("y1", tr.y + tr.h).attr("y2", tr.y + tr.h);
      series.forEach(function (sr) {
        var curve = sr.step ? d3.curveStepAfter : d3.curveLinear;
        if (sr.area) body.append("path").attr("class", "tla " + sr.cls).attr("d", d3.area().curve(curve).x(function (p) { return x(p[0]); }).y0(tr.y + tr.h).y1(function (p) { return yy(p[1]); })(sr.pts));
        body.append("path").attr("class", "tll " + sr.cls).attr("d", d3.line().curve(curve).x(function (p) { return x(p[0]); }).y(function (p) { return yy(p[1]); })(sr.pts));
      });
      if (tr.kind === "execs") body.append("g").selectAll("path").data(workers.filter(function (w) { return (w.kind === "memory-kill" || w.kind === "lost") && w.removed && clip(w.removed, w.removed); })).join("path").attr("class", "tlmark")
        .attr("d", d3.symbol(d3.symbolTriangle, 44)()).attr("transform", function (w) { return "translate(" + X(w.removed) + "," + (tr.y + 6) + ")"; });
      tr.yy = yy;
    });
    // one overlay: cursor, hover, drag to zoom, click to open
    var cur = P.svg.append("line").attr("class", "cursor").attr("y1", axisH).attr("y2", H).style("display", "none");
    var sel = P.svg.append("rect").attr("class", "tlsel").attr("y", axisH).attr("height", H - axisH).style("display", "none");
    var hit = P.svg.append("rect").attr("class", "hit").attr("x", lw).attr("y", axisH).attr("width", right - lw).attr("height", H - axisH);
    var barAt = function (px, py) { var t = +x.invert(px); for (var i = drawn.length - 1; i >= 0; i--) { var b = drawn[i]; if (py >= b.y && py <= b.y + b.h && t >= b.start && t <= Math.max(b.end, +x.invert(X(b.start) + 2))) return b; } return null; };
    var down = null, lit = null;
    function light(b) {
      if (b === lit) return;
      lit = b;
      var r = b ? related(b) : null;
      body.selectAll(".tlbar").classed("dim", function (d) { return r ? !r[d.kind][d.id] : false; });
    }
    // ids says which are running: "Job 4 running", "Jobs 4, 5 and 6 running"
    function ids(what, list) {
      if (!list.length) return "";
      var shown = list.slice(0, 8), more = list.length - shown.length;
      var names = shown.length === 1 && !more ? shown[0] : more ? shown.join(", ") + " and " + more + " more" : shown.slice(0, -1).join(", ") + " and " + shown[shown.length - 1];
      return what + (list.length > 1 ? "s " : " ") + names + " running";
    }
    function describe(t, b) {
      var lines = [dfmt.format(new Date(t))];
      if (b) lines.push(b.label + " · " + (STATUS[b.status] || b.status) + ", " + dur(b.end - b.start));
      var g = (D.gaps || []).filter(function (g) { return g[0] <= t && t < g[1]; })[0];
      var js = jBars.filter(function (j) { return j.start <= t && t < j.end; }).map(function (j) { return j.id; });
      var ss = sBars.filter(function (s) { return s.start <= t && t < s.end; }).map(function (s) { return s.id; });
      if (g) lines.push("No job running: driver gap of " + dur(g[1] - g[0]));
      else lines.push(ids("Job", js) || "No job running");
      if (ss.length) lines.push(ids("Stage", ss));
      lines.push("Executors alive: " + num(stepAt(alive, t) || 0) + " (" + num(stepAt(slots, t) || 0) + " task slots)");
      if (busy.length) { var bi = Math.floor((t - R.start) / R.bucketMs); if (bi >= 0 && bi < busy.length) lines.push("Tasks running: " + num(Math.round(busy[bi][1] * 10) / 10) + " on average"); }
      cpu.forEach(function (sr) { var v = stepAt(sr.points, t); if (v != null) lines.push("CPU " + sr.scope + ": " + Math.round(v) + "%"); });
      waiting.forEach(function (sr) { var v = stepAt(sr.points, t); if (v != null) lines.push("Containers waiting: " + num(v)); });
      return lines.join("\n");
    }
    hit.on("pointerdown", function (ev) { down = d3.pointer(ev)[0]; hit.node().setPointerCapture && hit.node().setPointerCapture(ev.pointerId); })
      .on("pointermove", function (ev) {
        var p = d3.pointer(ev), px = Math.max(lw, Math.min(right, p[0]));
        cur.attr("x1", px).attr("x2", px).style("display", null);
        if (down != null && Math.abs(px - down) > 4) { sel.attr("x", Math.min(px, down)).attr("width", Math.abs(px - down)).style("display", null); tipHide(); return; }
        var b = barAt(px, p[1]);
        light(b);
        hit.style("cursor", b ? "pointer" : "crosshair");
        tipShow(ev, describe(+x.invert(px), b));
      })
      .on("pointerup", function (ev) {
        var p = d3.pointer(ev), px = Math.max(lw, Math.min(right, p[0])), from = down;
        down = null; sel.style("display", "none");
        if (from != null && Math.abs(px - from) > 4) {
          var z = [+x.invert(Math.min(px, from)), +x.invert(Math.max(px, from))];
          if (z[1] - z[0] >= 50) { TL.zoom = z; tipHide(); drawSlot(c); }
          return;
        }
        var b = barAt(px, p[1]);
        if (b) { tipHide(); location.hash = b.kind === "query" ? "#query/" + b.id : b.kind === "job" ? "#job/" + b.id : "#stage/" + b.id; }
      })
      .on("pointerleave", function () { if (down == null) { cur.style("display", "none"); light(null); tipHide(); } });
  }
  // stageName is one short line, so the axis labels every bar: the call
  // site ("count at Foo.java:0") is cut to its operation.
  function stageName(st) { return "Stage " + st.id + (st.attempt ? "." + st.attempt : "") + " · " + (st.name || "").split(" at ")[0].slice(0, 16); }
  // SPLIT are the parts of a stage's task time (its "split" column, the
  // model's TimeSplit: scheduler delay, deserializing, computing, GC,
  // shuffle fetch wait, shuffle write, result, other), grouped as the
  // report groups them; tooltips of grouped series name their parts.
  var SPLIT = [
    { label: "Starting", color: V.viz[4], value: function (r) { return r.st.split[0] + r.st.split[1]; }, detail: function (r) { return "scheduler delay " + dur(r.st.split[0]) + ", deserializing " + dur(r.st.split[1]); } },
    { label: "Computing", color: V.viz[0], value: function (r) { return r.st.split[2]; } },
    { label: "Garbage collection", color: V.viz[1], value: function (r) { return r.st.split[3]; } },
    { label: "Shuffle", color: V.viz[2], value: function (r) { return r.st.split[4] + r.st.split[5]; }, detail: function (r) { return "waiting for data " + dur(r.st.split[4]) + ", writing " + dur(r.st.split[5]); } },
    { label: "Sending the result", color: V.viz[3], value: function (r) { return r.st.split[6]; } },
    { label: "Other", color: V.neutral, value: function (r) { return r.st.split[7]; } }
  ];
  var SPLIT_READ = "More computing is better. A large starting share means tasks were too small or the driver was busy; shuffle means data moving between executors; garbage collection above about 10% means memory pressure; a large grey part means waiting on files, S3 or Python. Spark measures these separately and they can overlap a little, so the split is approximate.";
  function splitTotal(st) { return st.split ? st.split.reduce(function (x, y) { return x + y; }, 0) : 0; }
  function topBy(list, n, key) { return list.filter(function (x) { return key(x) > 0; }).sort(function (a2, b2) { return key(b2) - key(a2); }).slice(0, n); }
  var DRAW = {
    stageTimes: function (c) {
      var rows = topBy(stages, 20, function (st) { return span(st.submitted, st.completed) || 0; });
      hbarChart(c, rows.map(function (st) { return { label: stageName(st), st: st }; }), [
        { label: "Succeeded", color: V.viz[0], value: function (r) { return r.st.status === "failed" ? 0 : span(r.st.submitted, r.st.completed); } },
        { label: "Failed", color: V.fail, value: function (r) { return r.st.status === "failed" ? span(r.st.submitted, r.st.completed) : 0; } }
      ], { t: "The longest stages", shows: "The longest stages, from submission to completion. Click a bar to open the stage.",
        read: "Shorter is better. The top few bars are where speeding things up helps most; red bars failed." }, "ms", function (r) { return "#stage/" + r.st.key; });
    },
    stageData: function (c) {
      var moved = function (st) { return st.input + st.shRead + st.shWrite + st.output + st.diskSpill; };
      var rows = topBy(stages, 20, moved).map(function (st) { return { label: stageName(st), st: st }; });
      var field = function (k) { return function (r) { return r.st[k]; }; };
      hbarChart(c, rows, [
        { label: "Read", color: V.viz[0], value: field("input") }, { label: "Shuffle read", color: V.viz[1], value: field("shRead") },
        { label: "Shuffle write", color: V.viz[2], value: field("shWrite") }, { label: "Written", color: V.viz[3], value: field("output") },
        { label: "Spilled to disk", color: V.viz[4], value: field("diskSpill") }
      ], { t: "Data each stage moved", shows: "The stages that moved the most data: read, shuffled between executors, written out and spilled to disk.",
        read: "Longer bars moved more data. Shuffle (orange and green) is the costly part, since it crosses disk and network; spill (pink) should be absent." }, "bytes", function (r) { return "#stage/" + r.st.key; });
    },
    stageSpill: function (c) {
      var rows = topBy(stages, 20, function (st) { return st.memSpill + st.diskSpill; }).map(function (st) { return { label: stageName(st), st: st }; });
      hbarChart(c, rows, [
        { label: "Spilled (size in memory)", color: V.viz[1], value: function (r) { return r.st.memSpill; } },
        { label: "Written to disk", color: V.viz[4], value: function (r) { return r.st.diskSpill; } }
      ], { t: "Spill by stage", shows: "Spill by stage: data that did not fit in memory while the stage ran, and what it came to on disk.",
        read: "Smaller is better, and none is ideal. Large spill means the stage's data did not fit in memory; more partitions or more memory per task help." }, "bytes", function (r) { return "#stage/" + r.st.key; });
    },
    stageSplit: function (c, key) {
      var list = key ? stages.filter(function (st) { return st.key === key; }) : topBy(stages, 20, splitTotal);
      var rows = list.filter(function (st) { return splitTotal(st) > 0; }).map(function (st) { return { label: stageName(st), st: st }; });
      hbarChart(c, rows, SPLIT, { t: "Where stage time went", read: SPLIT_READ,
        shows: (key ? "How this stage's tasks spent their time, added up over every task." : "For the stages with the most task time (every task's time added up): how the tasks spent it. Click a bar to open the stage.") +
          " Starting is scheduler delay and unpacking the task; shuffle is waiting for data from other executors and writing it out; other is the rest of run time, such as reading files or waiting on Python." },
        "ms", key ? null : function (r) { return "#stage/" + r.st.key; });
    },
    heatmap: function (c) { heatmap(c); },
    runTimeline: function (c) { runTimeline(c); },
    dataOverTime: function (c) {
      var done = stages.filter(function (st) { return st.completed; }).sort(function (a2, b2) { return a2.completed - b2.completed; });
      if (done.length < 2) { waitText(c, "Too few finished stages to chart."); return; }
      var t = a.start || done[0].submitted, inB = [[t, 0]], sh = [[t, 0]], out = [[t, 0]], ib = 0, sb = 0, ob = 0;
      done.forEach(function (st) { ib += st.input; sb += st.shWrite; ob += st.output; inB.push([st.completed, ib]); sh.push([st.completed, sb]); out.push([st.completed, ob]); });
      timeChart(c, [
        { label: "Read", color: V.viz[0], points: inB, step: true, area: 0.08 },
        { label: "Shuffle write", color: V.viz[2], points: sh, step: true, area: 0.08 },
        { label: "Written", color: V.viz[3], points: out, step: true, area: 0.08 }
      ], { t: "Data over time", shows: "Data read, shuffled and written, added up as each stage finished.",
        read: "Steep rises are where the work happened. Long flat stretches are time spent on something other than moving data, such as driver code or waiting for executors." }, "bytes");
    },
    execTime: function (c) {
      var rows = topBy(execs.filter(function (x) { return x.id !== "driver"; }), 30, function (x) { return x.run; }).map(function (x) { return { label: "Executor " + x.id, x: x }; });
      hbarChart(c, rows, [
        { label: "Computing", color: V.viz[0], value: function (r) { return r.x.cpuNs / 1e6; } },
        { label: "Garbage collection", color: V.viz[1], value: function (r) { return r.x.gc; } },
        { label: "Other or waiting", color: V.neutral, value: function (r) { return r.x.run - r.x.cpuNs / 1e6 - r.x.gc; } }
      ], { t: "Where executor time went", shows: "Task run time on each executor: computing on the JVM, collecting garbage, or neither (waiting for shuffle data, storage or Python workers).",
        read: "More computing is better. Garbage collection above about 10% of the bar means memory pressure; a large grey part means tasks waited instead of computing." }, "ms", function (r) { return "#executor/" + encodeURIComponent(r.x.id); });
    },
    execHeapAll: function (c) {
      if (!D.heapBytes) { waitText(c, "The executors' heap size is not known."); return; }
      var rows = topBy(execs.filter(function (x) { return x.id !== "driver"; }), 30, function (x) { return x.peakHeap || 0; }).map(function (x) { return { label: "Executor " + x.id, x: x }; });
      hbarChart(c, rows, [
        { label: "Peak heap", color: V.viz[0], value: function (r) { return r.x.peakHeap; } },
        { label: "Heap not used at peak", color: V.line, rest: true, value: function (r) { return D.heapBytes - r.x.peakHeap; } }
      ], { t: "Peak heap per executor", shows: "Each executor's peak Java heap against the " + bytes(D.heapBytes) + " it had. Peaks are sampled, so short spikes can be missed.",
        read: "A bar that nearly fills its row (over 90%) risks running out of memory. Bars that stay well short mean executors had more heap than they used and could be smaller." }, "bytes", function (r) { return "#executor/" + encodeURIComponent(r.x.id); });
    },
    nodeMemory: function (c) {
      var rows = D.aws.nodes.filter(function (n) { return n.yarnMem; }).map(function (n) { return { label: n.host.split(".")[0], n: n }; });
      var execBytes = function (n) { return (n.execMem || 0) * (n.peakExecs || n.executors.length); };
      hbarChart(c, rows, [
        { label: "Driver container", color: V.viz[1], value: function (r) { return r.n.driverMem || 0; } },
        { label: "Executor containers (at once)", color: V.viz[0], value: function (r) { return execBytes(r.n); } },
        { label: "Free", color: V.line, rest: true, value: function (r) { return r.n.yarnMem - (r.n.driverMem || 0) - execBytes(r.n); } }
      ], { t: "What YARN placed on each node", shows: "What YARN placed on each node, against the memory the node offered.",
        read: "Free space helps only if it is at least one executor container wide: smaller gaps are memory paid for but unusable. A node that is mostly free did little work for this run." }, "bytes");
    },
    clusterContainers: function (c) {
      var list = metric("ContainerAllocated").concat(metric("ContainerPending"));
      if (!list.length) { waitText(c, "CloudWatch had no container counts for this run."); return; }
      timeChart(c, list.map(function (s) {
        var waiting = s.name === "ContainerPending";
        return { label: waiting ? "Waiting" : "Allocated", color: waiting ? V.fail : V.series, points: s.points, step: true, area: 0.12 };
      }), { t: "Containers on the cluster", shows: "Containers YARN had placed and containers waiting for room, across the whole cluster, every minute. This application ran from " + (a.start ? tfmt.format(new Date(a.start)) : "?") + " to " + (a.end ? tfmt.format(new Date(a.end)) : "?") + ".",
        read: "Waiting should stay at zero. Waiting while YARN memory is free means the containers were too big to fit on any node; waiting with memory full means the cluster was too small or busy." }, "count");
    },
    nodeCPU: function (c) {
      var list = metric("CPUUtilization", "Average");
      if (!list.length) { waitText(c, "CloudWatch had no CPU figures for these nodes."); return; }
      var name = {};
      D.aws.nodes.forEach(function (n) { if (n.instance) name[n.instance.id] = n.instance.id + (n.driver ? " (driver)" : n.executors.length ? " (" + n.executors.length + " executors)" : n.instance.role === "MASTER" ? " (primary)" : " (idle)"); });
      timeChart(c, list.map(function (s, i) { return { label: name[s.scope] || s.scope, color: V.viz[i % V.viz.length], dash: i >= V.viz.length ? "5 3" : null, points: s.points, dots: true }; }),
        { t: "Node CPU", shows: "Each node's CPU, averaged over EC2's 5-minute periods: the whole machine, so daemons and other applications count too.",
          read: "Higher means busier. Staying above about 85% means tasks queued for CPU. Low CPU on a node that ran executors suggests its tasks waited on disk, network or Python." }, "pct", { max: 100 });
    },
    running: function (c) {
      var R = D.running;
      if (!R || !R.busy.length) { waitText(c, D.collected ? "No tasks finished, so there is nothing to chart." : "Per-task detail was not collected for this run."); return; }
      var workers = execs.filter(function (x) { return x.id !== "driver"; });
      function slotsAt(t) { var n = 0; workers.forEach(function (x) { if (x.added && x.added <= t && t < (x.removed || appEnd + 1)) n += x.cores; }); return n; }
      var busy = [], slots = [];
      R.busy.forEach(function (b, i) { var t = R.start + i * R.bucketMs; busy.push([t, Math.round(b / R.bucketMs * 10) / 10]); slots.push([t, slotsAt(t + R.bucketMs / 2)]); });
      timeChart(c, [
        { label: "Tasks running (average)", color: V.series, points: busy, area: 0.25 },
        { label: "Task slots (executor cores)", color: V.neutral, points: slots, step: true, dash: "4 4" }
      ], { t: "Tasks running against task slots", shows: "Average tasks running in each " + dur(R.bucketMs) + " bucket, against the cores of the executors alive then. Task times are stamped by the driver, so running tasks can briefly exceed the slots.",
        read: "The filled area should reach the dashed line: every core busy. Gaps below it are idle cores, often from the driver working alone, one slow task holding a stage, or too few partitions." }, "count");
    },
    jobsTimeline: function (c) {
      c.link = function (r) { return "#job/" + r.id; };
      timeline(c, jobs.filter(function (j) { return j.submitted; }).map(function (j) {
        var end = j.completed || appEnd;
        return { id: j.id, row: "Job " + j.id, bar: (j.desc || j.name || "").slice(0, 80), color: statusColor(j.status), start: j.submitted, end: end,
          tip: "Job " + j.id + ": " + (j.desc || j.name) + "\n" + (STATUS[j.status] || j.status) + ", " + dur(end - j.submitted) };
      }), { t: "Jobs over time", shows: "When each job ran. Click a bar to open the job." + STATUS_NOTE,
        read: "Longer bars took longer. Gaps between bars are time the driver spent outside Spark jobs (planning, Python code or waiting); bars that overlap ran at the same time." }, "No job has a start time.");
    },
    executorsTimeline: function (c) {
      c.link = function (r) { return "#executor/" + encodeURIComponent(r.id); };
      timeline(c, execs.filter(function (x) { return x.id !== "driver" && x.added; }).map(function (x) {
        var bad = x.kind === "memory-kill" || x.kind === "lost";
        var end = x.removed || appEnd;
        return { id: x.id, row: "Executor " + x.id, bar: x.host || "", color: bad ? V.fail : x.removed ? V.neutral : V.series, start: x.added, end: end,
          tip: "Executor " + x.id + " on " + x.host + "\n" + (x.removed ? "Removed: " + (x.reason || "no reason logged") : "Ran to the end") + ", " + dur(end - x.added) };
      }).concat(exclusions.filter(function (x) { return x.scope === "application"; }).map(function (x) {
        var end = x.lifted || appEnd;
        return { id: x.kind === "executor" ? x.target : "", row: (x.kind === "executor" ? "Executor " : "Node ") + x.target + " excluded", bar: num(x.failures) + " failures", color: V.fail, start: x.time, end: end,
          tip: (x.kind === "executor" ? "Executor " : "Node ") + x.target + " excluded after " + num(x.failures) + " failures" + (x.lifted ? ", lifted after " + dur(x.lifted - x.time) : "") };
      })), { t: "Executor lifetimes", shows: "How long each executor lived. Blue: ran to the end. Red: killed or lost, or a period it was excluded. Grey: removed for another reason, such as being idle. Click one to open it.",
        read: "Long blue bars are healthy. Any red deserves a look. Many short grey bars mean dynamic allocation added and removed executors often, which costs start-up time." }, "No executor was logged.");
    },
    sqlTimeline: function (c) {
      c.link = function (r) { return "#query/" + r.id; };
      timeline(c, queries.filter(function (q) { return q.start; }).map(function (q) {
        var st = queryStatus(q), end = q.end || appEnd;
        return { id: q.id, row: "Query " + q.id, bar: (q.desc || "").slice(0, 80), color: statusColor(st), start: q.start, end: end,
          tip: "Query " + q.id + ": " + q.desc + "\n" + (STATUS[st] || st) + ", " + dur(end - q.start) };
      }), { t: "Queries over time", shows: "When each query ran. Click a bar to open its plan." + STATUS_NOTE,
        read: "Longer bars took longer, so they are where tuning pays off; bars that overlap ran at the same time." }, "No query was logged.");
    },
    durationHistogram: function (c, key) {
      var det = D.detail[key];
      if (!det || !det.h.length) { waitText(c, "No successful task to chart."); return; }
      colChart(c, det.h.map(function (b) {
        var range = b[0] === b[1] ? dur(b[0]) : dur(b[0]) + " to " + dur(b[1]);
        return { label: dur(b[0]), value: b[2], tip: range + ": " + num(b[2]) + " tasks" };
      }), { t: "Task durations", shows: "Successful tasks by how long they took (every task, not a sample). Each column is a range of durations, labelled by where it starts.",
        read: "One tall group on the left is ideal: tasks took similar, short times. A long tail to the right means a few tasks held the stage up, usually because of skewed data." }, "count", { color: V.series });
    },
    taskScatter: function (c, key) {
      var det = D.detail[key];
      if (!det || !det.sample.length) { waitText(c, "No task to chart."); return; }
      var seen = {}, rows = [];
      det.slow.concat(det.sample).forEach(function (r) { if (!seen[r[T.task]]) { seen[r[T.task]] = 1; rows.push(r); } });
      var st0 = null;
      stages.forEach(function (s) { if (s.key === key) st0 = s.submitted; });
      rows.forEach(function (r) { var at = (D.t0 || 0) + r[T.launch]; if (st0 == null || at < st0) st0 = at; });
      var started = function (r) { return (D.t0 || 0) + r[T.launch] - st0; };
      var xa = SCATTER_X[SCATTER.x], ya = SCATTER_Y[SCATTER.y];
      var fmtOf = function (kind, v) { return kind === "count" ? num(v) : FMT[kind](v); };
      var isDefault = SCATTER.x === "start" && SCATTER.y === "dur";
      var plot = scatterChart(c, rows.map(function (r) {
        var xv = xa.v(r, started), yv = ya.v(r);
        return { x: xv, y: yv, bad: r[T.status] !== 0,
          tip: "Task " + r[T.task] + " (partition " + r[T.index] + ") on executor " + execName(r[T.exec]) + "\nStarted " + dur(started(r)) + " into the stage, took " + dur(r[T.dur]) +
            (isDefault ? "\n" + num(r[T.rows]) + " rows read" : "\n" + xa.label + ": " + fmtOf(xa.kind, xv) + "\n" + ya.label + ": " + fmtOf(ya.kind, yv)) };
      }), { t: isDefault ? "When tasks started and how long they took" : SCATTER.x === "start" ? ya.label + ", by when each task started" : ya.label + " against " + xa.label.toLowerCase(),
        shows: "Each mark is a task" + (isDefault ? ": when it started, counted from the start of the stage, and how long it took. " : ": " + xa.label.toLowerCase() + " across, " + ya.label.toLowerCase() + " up. ") +
          (det.sample.length < det.from ? "From a sample of " + num(det.sample.length) + " of " + num(det.from) + " tasks plus the slowest " + num(det.slow.length) + "." : "Every task of this stage."),
        read: isDefault ? "Dots should form a low, even band. Dots far above the rest are stragglers (check whether they read more rows); triangles failed. Vertical stripes are waves: one per round of task slots."
          : SCATTER.x === "start" ? ya.read + " Vertical stripes are waves, one per round of task slots; triangles failed."
          : "Marks that climb from left to right mean " + xa.label.toLowerCase() + " explains the " + ya.label.toLowerCase() + ": a few far to the right are skew in the data. Marks high up at the left are slow for another reason, such as a busy node, garbage collection or waiting. " + ya.read + " Triangles failed." },
        xa.kind, ya.kind, ["Succeeded", "Failed or killed", xa.label + (SCATTER.x === "start" ? ", after the stage began" : ""), ya.label]);
      if (!plot) return;
      // axis choices, above the chart
      var pick = function (label, opts, cur, set) {
        var s = el("select", { "aria-label": label });
        Object.keys(opts).forEach(function (k) { s.appendChild(el("option", { value: k, text: opts[k].label, selected: k === cur })); });
        s.addEventListener("change", function () { set(s.value); drawSlot(c); });
        return s;
      };
      plot.parentNode.insertBefore(el("div", { cls: "bar-tools" },
        el("span", { cls: "count", text: "Up:" }), pick("Up the chart", SCATTER_Y, SCATTER.y, function (v) { SCATTER.y = v; }),
        el("span", { cls: "count", text: "Across:" }), pick("Across the chart", SCATTER_X, SCATTER.x, function (v) { SCATTER.x = v; })), plot.previousSibling);
    },
    execHeap: function (c, id) {
      var idx = D.execs.indexOf(id), pts = [];
      if (idx >= 0) stages.forEach(function (st) {
        var det = D.detail[st.key];
        if (det) det.cells.forEach(function (r) { if (r[C.exec] === idx && r[C.peakHeap] > 0) pts.push([st, r[C.peakHeap]]); });
      });
      if (!pts.length) { waitText(c, "No heap samples were logged for this executor (spark.eventLog.logStageExecutorMetrics turns them on)."); return; }
      colChart(c, pts.map(function (p) {
        return { label: "Stage " + p[0].key, value: p[1], href: "#stage/" + p[0].key, tip: "Stage " + p[0].key + " (" + p[0].name + "): peak heap " + bytes(p[1]) + (D.heapBytes ? " of " + bytes(D.heapBytes) : "") };
      }), { t: "Peak heap by stage", shows: "The highest Java heap use sampled while each stage ran on this executor, against the heap it was given (dashed). Samples are peaks, so short spikes can be missed. Click a column to open the stage.",
        read: "Bars close to the dashed line risk running out of memory; bars well below it for every stage mean the heap is bigger than this work needs." }, "bytes",
        { series: "Peak heap", ref: D.heapBytes ? { value: D.heapBytes, label: "Configured heap (" + bytes(D.heapBytes) + ")" } : null });
    }
  };
  function drawSlot(c) {
    var name = c.draw.split(":")[0], arg = c.draw.slice(name.length + 1);
    if (!DRAW[name] || !document.body.contains(c.el)) return;
    try { DRAW[name](c, arg); } catch (e) { waitText(c, "This chart could not be drawn: " + e.message); }
  }
  var resizeTimer;
  window.addEventListener("resize", function () { clearTimeout(resizeTimer); resizeTimer = setTimeout(redrawCharts, 250); });


  // ---------- code ----------
  // Where in the application each job, stage and query ran, as Spark
  // recorded it, and the source itself when -source supplied it.
  var srcIdx = {};
  (D.sources || []).forEach(function (sf, i) { sf.logged.forEach(function (n) { srcIdx[n] = i; }); });
  function codeHref(c) { var i = srcIdx[c[0]]; return i == null ? null : "#code/" + i + "/" + c[1]; }
  function codeLabel(c) { var f = c[0].replace(/^.*[\/\\]/, ""); return f + ":" + c[1] + (c[2] ? " in " + c[2].replace(/^.*\./, "") : c[3] ? " (" + c[3] + ")" : ""); }
  var CODE_NONE = "Spark recorded no line of your code for this. PySpark records one only for some actions (collect does; count and write do not). To label it, call sc.setJobDescription(\"what this does\") before the action, or set the call site with sc.setLocalProperty(\"callSite.short\", \"load_claims() at etl.py:120\").";
  function snippet(c, around) {
    var i = srcIdx[c[0]];
    if (i == null) return null;
    var sf = D.sources[i], from = Math.max(1, c[1] - around), to = Math.min(sf.lines.length, c[1] + around);
    var rows = [];
    for (var n = from; n <= to; n++) rows.push(el("tr", { cls: n === c[1] ? "hit" : null }, el("td", { cls: "ln", text: String(n) }), el("td", { cls: "src", text: sf.lines[n - 1] })));
    return el("div", { cls: "codebox" }, el("div", { cls: "codehead" }, link(codeHref(c), sf.path)), el("table", { cls: "code" }, el("tbody", null, rows)));
  }
  // codePanel shows where something ran: its frames, the code around the
  // innermost one, or why nothing was recorded.
  function codePanel(code, when, selfJob) {
    var box = el("section", null, el("h3", { text: "Code" }));
    if (!code || !code.length) {
      box.appendChild(explain(CODE_NONE));
      var before = null, after = null;
      jobs.forEach(function (j) {
        if (!j.code.length || j.id === selfJob || !j.submitted || !when) return;
        if (j.submitted <= when && (!before || j.submitted > before.submitted)) before = j;
        if (j.submitted > when && (!after || j.submitted < after.submitted)) after = j;
      });
      if (before || after) box.appendChild(el("p", { cls: "note" }, "Nearest recorded code: ",
        before ? el("span", null, "before it, ", link("#job/" + before.id, "job " + before.id), " at ", codeLabel(before.code[0])) : null,
        before && after ? "; " : "",
        after ? el("span", null, "after it, ", link("#job/" + after.id, "job " + after.id), " at ", codeLabel(after.code[0])) : null, "."));
      return box;
    }
    box.appendChild(el("ol", { cls: "frames" }, code.map(function (c, i) {
      var h = codeHref(c);
      return el("li", null, h ? link(h, codeLabel(c)) : codeLabel(c), el("span", { cls: "sub", text: (i ? "called from " : "") + c[0] }));
    })));
    var sn = snippet(code[0], 6);
    if (sn) box.appendChild(sn);
    else if (!(D.sources || []).length) box.appendChild(el("p", { cls: "note", text: "Run sparkplain with -source <your code folder> to see the code here." }));
    return box;
  }
  // codeUses maps "file:line" to the jobs, stages and queries that ran there.
  var codeUses = (function () {
    var m = {};
    function addAll(kind, items, getCode) {
      items.forEach(function (it) {
        (getCode(it) || []).forEach(function (c, depth) {
          var k = c[0] + ":" + c[1];
          var u = m[k] || (m[k] = { c: c, jobs: [], stages: [], queries: [], ms: 0 });
          if (depth === 0 || kind !== "stages") u[kind].push(it);
          if (kind === "jobs" && depth === 0) u.ms += span(it.submitted, it.completed) || 0;
        });
      });
    }
    addAll("jobs", jobs, function (j) { return j.code; });
    addAll("stages", stages, function (s) { return s.code; });
    addAll("queries", queries, function (q) { return q.code; });
    return m;
  })();
  function usesCell(u) {
    return el("span", null,
      u.jobs.map(function (j, i) { return [i ? ", " : "", link("#job/" + j.id, "job " + j.id)]; }),
      u.stages.length ? [u.jobs.length ? " · " : "", u.stages.map(function (st, i) { return [i ? ", " : "", link("#stage/" + st.key, "stage " + st.id)]; })] : null,
      u.queries.length ? [" · ", u.queries.map(function (q, i) { return [i ? ", " : "", link("#query/" + q.id, "query " + q.id)]; })] : null);
  }
  views.code = function (arg) {
    var s = section("Code", "Where in your application each job, stage and query ran, as Spark recorded it." + ((D.sources || []).length ? " Your code is shown beside what ran each line." : " Run sparkplain with -source <your code folder> to see the code itself here."));
    var withCode = jobs.filter(function (j) { return j.code.length; }).length;
    s.appendChild(explain(num(withCode) + " of " + num(jobs.length) + " jobs have a recorded location. " + (withCode < jobs.length ? CODE_NONE : "")));
    var uses = Object.keys(codeUses).map(function (k) { return codeUses[k]; });
    if (uses.length) s.appendChild(table({
      rows: uses, sort: 3, dir: "desc", filter: "Filter by file or function", text: function (u) { return u.c[0] + " " + (u.c[2] || "") + " " + (u.c[3] || ""); },
      cols: [
        { h: "Where", v: function (u) { return u.c[0] + ":" + (100000 + u.c[1]); }, f: function (u) { var h = codeHref(u.c); return el("span", null, h ? link(h, codeLabel(u.c)) : codeLabel(u.c), el("span", { cls: "sub", text: u.c[0] })); } },
        { h: "What ran there", f: usesCell },
        { h: "Jobs", num: true, v: function (u) { return u.jobs.length; }, f: function (u) { return num(u.jobs.length); } },
        { h: "Job time", num: true, v: function (u) { return u.ms; }, f: function (u) { return u.ms ? dur(u.ms) : "—"; } }
      ]
    }));
    (D.sources || []).forEach(function (sf, i) {
      var byLine = {};
      uses.forEach(function (u) { if (srcIdx[u.c[0]] === i) (byLine[u.c[1]] = byLine[u.c[1]] || []).push(u); });
      var rows = sf.lines.map(function (line, n) {
        var here = byLine[n + 1];
        return el("tr", { id: "code-" + i + "-" + (n + 1), cls: here ? "hit" : null },
          el("td", { cls: "ln", text: String(n + 1) }), el("td", { cls: "src", text: line }),
          el("td", { cls: "uses" }, here ? here.map(function (u) { return el("div", null, usesCell(u), u.ms ? el("span", { cls: "sub", text: dur(u.ms) + " of job time" }) : null); }) : null));
      });
      s.appendChild(el("div", { cls: "codebox" }, el("div", { cls: "codehead", text: sf.path + (sf.cut ? " (cut)" : "") }),
        el("div", { cls: "codescroll" }, el("table", { cls: "code side" }, el("tbody", null, rows)))));
    });
    if ((D.sourceNotes || []).length) s.appendChild(el("div", { cls: "missing" }, el("h3", { text: "About the source" }), el("ul", null, D.sourceNotes.map(function (n) { return el("li", { text: n }); }))));
    if (arg) setTimeout(function () {
      var p = arg.split("/"), row = document.getElementById("code-" + p[0] + "-" + p[1]);
      if (row) { row.scrollIntoView({ block: "center" }); row.classList.add("focus"); }
    }, 0);
    return s;
  };

  views.log = function () {
    var st = D.logStats;
    var s = section("Event log", "The file sparkplain read, and every kind of event in it. Unknown events or fields mean a Spark version newer than sparkplain knows; they are counted, not lost silently.");
    if (!st) { s.appendChild(explain("The event log could not be read.")); return s; }
    s.appendChild(el("div", { cls: "facts" },
      fact("Input", el("span", { cls: "mono", text: st.input }), st.layout + " layout, " + st.codec + " compression" + (st.inProgress ? ", still in progress" : "") + (st.truncated ? ", cut off" : "") + "."),
      fact("Events", num(st.events) + " events on " + num(st.lines) + " lines", (st.malformedLines ? num(st.malformedLines) + " malformed lines skipped. " : "") + (st.redactedValues ? num(st.redactedValues) + " setting values redacted." : "")),
      Object.keys(st.unknownEvents || {}).length || Object.keys(st.unknownFields || {}).length ?
        fact("Not recognised", num(Object.keys(st.unknownEvents || {}).length) + " event types, " + num(Object.keys(st.unknownFields || {}).length) + " fields", "Written by a Spark or EMR version sparkplain does not know yet; listed below.") :
        fact("Recognised", "Every event and field", "Each one is listed in sparkplain's field inventory as used or deliberately set aside.")));
    if (st.files && st.files.length) s.appendChild(table({ rows: st.files, sort: 0, dir: "asc", cols: [
      { h: "File", v: function (f) { return f.name; }, f: function (f) { return el("span", { cls: "mono", text: f.name }); } },
      { h: "Codec", v: function (f) { return f.codec; }, f: function (f) { return f.codec; } },
      { h: "Size", num: true, v: function (f) { return f.bytes; }, f: function (f) { return bytes(f.bytes); } },
      { h: "Unpacked", num: true, v: function (f) { return f.decompressedBytes; }, f: function (f) { return bytes(f.decompressedBytes); } },
      { h: "Lines", num: true, v: function (f) { return f.lines; }, f: function (f) { return num(f.lines); } },
      { h: "Problem", v: function (f) { return f.error || ""; }, f: function (f) { return f.error || "—"; } }
    ] }));
    var types = Object.keys(st.byType || {}).map(function (k) { return { name: k, n: st.byType[k], unknown: (st.unknownEvents || {})[k] != null }; });
    s.appendChild(el("h3", { text: "Events by type" }));
    s.appendChild(table({ rows: types, sort: 1, dir: "desc", filter: "Filter event types", text: function (t) { return t.name; }, cols: [
      { h: "Event", v: function (t) { return t.name; }, f: function (t) { return el("span", { cls: "mono" }, t.name.replace(/^org\.apache\.spark\.[a-z.]*\./, ""), t.unknown ? el("span", { cls: "bad", text: " · not recognised" }) : null); } },
      { h: "Count", num: true, v: function (t) { return t.n; }, f: function (t) { return num(t.n); } }
    ] }));
    var uf = Object.keys(st.unknownFields || {});
    if (uf.length) {
      s.appendChild(el("h3", { text: "Fields not recognised" }));
      s.appendChild(table({ rows: uf.map(function (k) { return { k: k, n: st.unknownFields[k] }; }), sort: 1, dir: "desc", cols: [
        { h: "Event and field", v: function (r) { return r.k; }, f: function (r) { return el("span", { cls: "mono", text: r.k }); } },
        { h: "Count", num: true, v: function (r) { return r.n; }, f: function (r) { return num(r.n); } }
      ] }));
    }
    if (st.notes && st.notes.length) s.appendChild(el("div", { cls: "missing" }, el("h3", { text: "Notes from reading it" }), el("ul", null, st.notes.map(function (n) { return el("li", { text: n }); }))));
    return s;
  };

  views.cluster = function () {
    var A = D.aws;
    if (!A) return notFound("Cluster");
    var s = section("Cluster", "The nodes the cluster had while this application ran, how busy they were, and what AWS calls they made. From the EMR, EC2, CloudWatch and CloudTrail APIs.");
    var gs = (A.groups || []).map(function (g) { return fact({ MASTER: "Primary", CORE: "Core", TASK: "Task" }[g.role] || g.role, g.instanceTypes.join(", ") + (g.market ? " · " + ({ SPOT: "spot", ON_DEMAND: "on-demand" }[g.market] || g.market) : ""), num(g.requested) + " requested" + (g.running ? ", " + num(g.running) + " running now" : "") + (g.fleet ? " (instance fleet)" : "") + "."); });
    if (gs.length) s.appendChild(el("div", { cls: "facts" }, gs));
    if (A.security) {
      var p = A.security, on = function (b) { return b ? "on" : "off"; };
      s.appendChild(el("h3", { text: "Security configuration: " + p.name }));
      s.appendChild(el("div", { cls: "facts" }, fact("Encryption at rest", on(p.atRestEncryption) + (p.s3Encryption ? " (S3 " + p.s3Encryption + ")" : "")), fact("Encryption in transit", on(p.inTransitEncryption)),
        fact("Kerberos", p.kerberos || "off"), fact("Lake Formation", on(p.lakeFormation)), fact("Runtime roles", on(p.runtimeRoles))));
    }
    s.appendChild(el("h3", { text: "Nodes" }));
    s.appendChild(table({
      rows: A.nodes, sort: 0, dir: "asc",
      rowCls: function (n) { return n.instance && !n.executors.length && !n.driver && n.instance.role !== "MASTER" ? "failedrow" : null; },
      cols: [
        { h: "Host", v: function (n) { return n.host; }, f: function (n) { return el("span", { cls: "mono", text: n.host }); } },
        { h: "Ran", v: function (n) { return n.executors.length; }, f: function (n) {
          if (!n.executors.length) return n.driver ? "the driver only" : n.instance && n.instance.role === "MASTER" ? "primary node" : el("span", { cls: "bad", text: "nothing" });
          return el("span", null, n.driver ? "driver, " : "", n.executors.map(function (x, i) { return [i ? ", " : "", execLink(x)]; }));
        } },
        { h: "Instance", v: function (n) { return n.instance ? n.instance.id : ""; }, f: function (n) {
          var i = n.instance;
          return i ? el("span", null, el("span", { cls: "mono", text: i.id }), el("span", { cls: "sub", text: [({ MASTER: "primary", CORE: "core", TASK: "task" }[i.role] || ""), i.type, i.vcpu ? i.vcpu + " vCPU, " + bytes(i.memoryBytes) : "", ({ SPOT: "spot", ON_DEMAND: "on-demand" }[i.market] || "")].filter(Boolean).join(" · ") })) : "—";
        } },
        { h: "YARN offered", num: true, v: function (n) { return n.yarnMem || 0; }, f: function (n) { return n.yarnMem ? bytes(n.yarnMem) + ", " + n.yarnCores + " vCores" : "—"; } },
        { h: "Node CPU", num: true, v: function (n) { return n.cpuAvg == null ? -1 : n.cpuAvg; }, f: function (n) { return n.cpuAvg == null ? "—" : el("span", null, Math.round(n.cpuAvg) + "% avg", el("span", { cls: "sub", text: Math.round(n.cpuPeak) + "% peak" })); } }
      ]
    }));
    if (A.nodes.some(function (n) { return n.yarnMem; })) s.appendChild(chartSlot("", "nodeMemory"));
    if ((A.metricFacts || []).length) s.appendChild(el("div", { cls: "facts" }, A.metricFacts.map(function (f) { return fact(f.label, f.value, f.explain); })));
    if (A.metrics.length) { s.appendChild(chartSlot("", "clusterContainers")); s.appendChild(chartSlot("", "nodeCPU")); }
    if ((A.metricsGaps || []).length) s.appendChild(el("div", { cls: "missing" }, el("h3", { text: "Not in CloudWatch" }), el("ul", null, A.metricsGaps.map(function (g) { return el("li", { text: g }); }))));
    if (A.callUsers && A.callUsers.length) {
      s.appendChild(el("h3", { text: "AWS calls" }));
      s.appendChild(explain(num(A.callEvents) + " calls recorded by CloudTrail for " + A.callUsers.join(", ") + " while the application ran, including EMR's own agents on those nodes."));
      if (A.denied.length) s.appendChild(table({ rows: A.denied, sort: 0, dir: "asc", cols: [
        { h: "Time", num: true, v: function (e) { return Date.parse(e.time); }, f: function (e) { return when(Date.parse(e.time)); } },
        { h: "Refused call", v: function (e) { return e.service + " " + e.action; }, f: function (e) { return el("span", null, el("span", { cls: "mono", text: e.service + " " + e.action }), (e.resources || []).map(function (r) { return el("span", { cls: "sub mono", text: r }); }), e.role ? el("span", { cls: "sub", text: "as " + e.role }) : null); } },
        { h: "Error", v: function (e) { return e.errorCode; }, f: function (e) { return el("span", null, el("span", { cls: "bad", text: e.errorCode }), e.message ? el("span", { cls: "sub", text: e.message }) : null); } }
      ] }));
      s.appendChild(table({ rows: A.calls, sort: 2, dir: "desc", filter: "Filter calls", text: function (x) { return x.service + " " + x.action; }, cols: [
        { h: "Service", v: function (x) { return x.service; }, f: function (x) { return el("span", { cls: "mono", text: x.service }); } },
        { h: "Action", v: function (x) { return x.action; }, f: function (x) { return el("span", { cls: "mono", text: x.action }); } },
        { h: "Calls", num: true, v: function (x) { return x.count; }, f: function (x) { return num(x.count); } },
        { h: "Errors", num: true, v: function (x) { return x.errors; }, f: function (x) { return x.errors ? el("span", { cls: "bad", text: num(x.errors) }) : "0"; } }
      ] }));
      if ((A.callsGaps || []).length) s.appendChild(el("div", { cls: "missing" }, el("h3", { text: "Not in CloudTrail's lookup" }), el("ul", null, A.callsGaps.map(function (g) { return el("li", { text: g }); }))));
    }
    return s;
  };

  views.logs = function (arg) {
    if (arg != null && arg !== "") {
      var parts = String(arg).split(":"), f = logFiles[+parts[0]];
      if (!f) return notFound("Log " + arg);
      var s = section(FILE_KIND[f.kind] || f.kind);
      s.insertBefore(el("div", { cls: "crumbs" }, link("#logs", "Logs"), " / " + (logWho(f) || f.kind)), s.firstChild);
      s.appendChild(el("div", { cls: "facts" },
        fact("Object", el("span", null, el("span", { cls: "mono", text: f.loc }), f.href ? el("span", { cls: "sub" }, extLink(f.href, "Open in the S3 console")) : null), "Where it was read from. The console link needs your own AWS sign-in; this page never fetches it."),
        f.exec ? fact("Ran", f.exec === "am" ? "the application master" : execLink(f.exec), f.host ? "On " + f.host + "." : null) : null,
        f.container ? fact("Container", el("span", { cls: "mono", text: f.container })) : null,
        f.step ? fact("Step", el("span", { cls: "mono", text: f.step })) : null,
        f.instance ? fact("Node", el("span", { cls: "mono", text: f.instance }), f.host || null) : null,
        fact("Read", bytes(f.bytes) + " compressed, " + num(f.lines) + " lines", num(f.found.length) + " recognised" + (f.dropped ? "; " + num(f.dropped) + " more distinct lines were over the per-file limit" : "") + (f.cut ? "; " + num(f.cut) + " more are in report.json" : "") + ".")));
      if (!f.rows.length) s.appendChild(explain("Nothing in this log matched a rule: no errors, exits or identity lines."));
      else s.appendChild(logLinesTable(f.rows, false, parts[1] ? +parts[1] : null));
      return s;
    }
    var s2 = section("Logs", "The container, step and node logs read for this application, and what sparkplain recognised in each: errors with their causes, exits, memory kills, and who the application ran as.");
    if (D.cluster) {
      var c = D.cluster;
      s2.appendChild(el("div", { cls: "facts" },
        fact("Cluster", el("span", null, el("span", { cls: "mono", text: c.id }), c.name ? " (" + c.name + ")" : ""), c.release + ", " + c.state + (c.reason ? ": " + c.reason : "") + "."),
        fact("Log URI", el("span", { cls: "mono", text: c.logUri || "none" }), "Where EMR copies the cluster's logs."),
        fact("Instance profile", c.profile || "none", "The IAM role the nodes' processes use for AWS calls."),
        fact("Nodes", num(c.instances.length), c.instances.map(function (i) { return i.id + (i.primary ? " (primary)" : "") + (i.type ? " " + i.type : "") + (i.market ? " " + i.market : ""); }).join(", "))));
    }
    (D.logSources || []).forEach(function (x) {
      s2.appendChild(el("div", { cls: "logsrc" }, el("h3", null, x.name, " ", el("span", { cls: "pill " + ({ read: "full", partial: "part", error: "crit" }[x.status] || "none"), text: SRC_LABEL[x.status] || x.status }), x.class ? el("span", { cls: "sub", text: x.class }) : null),
        x.loc ? el("p", { cls: "mono sub", text: x.loc }) : null, el("p", { text: x.detail }),
        (x.skipped || []).length ? el("details", null, el("summary", { text: num(x.skipped.length + (x.more || 0)) + " objects skipped or unreadable" }), el("div", { cls: "inner" },
          table({ rows: x.skipped, sort: 0, dir: "asc", page: 100, cols: [
            { h: "Object", v: function (o) { return o.location; }, f: function (o) { return el("span", { cls: "mono", text: o.location }); } },
            { h: "Size", num: true, v: function (o) { return o.bytes; }, f: function (o) { return bytes(o.bytes); } },
            { h: "Why", v: function (o) { return o.status + " " + (o.detail || ""); }, f: function (o) { return el("span", { cls: o.status === "error" ? "bad" : null, text: (o.errorClass ? o.errorClass + ": " : "") + (o.detail || o.status) }); } }
          ] }), x.more ? explain(num(x.more) + " more are listed in report.json.") : null)) : null));
    });
    if (!logFiles.length) { s2.appendChild(explain("No container, step or node logs were read. Run sparkplain with -cluster-id (reads them from S3) or -from (a local copy).")); return s2; }
    var worst = function (f) { return f.found.reduce(function (m, r) { return Math.min(m, { critical: 0, warning: 1, info: 2 }[r[LC.sev]]); }, 3); };
    s2.appendChild(el("h3", { text: "Files" }));
    s2.appendChild(table({
      rows: logFiles, sort: 2, dir: "asc", filter: "Filter by path, executor or kind",
      text: function (f) { return f.loc + " " + f.kind + " " + logWho(f); },
      rowCls: function (f) { return worst(f) === 0 ? "failedrow" : null; },
      cols: [
        { h: "Log", v: function (f) { return f.loc; }, f: function (f) { return el("span", { title: f.loc }, link("#logs/" + f.i, FILE_KIND[f.kind] || f.kind), el("span", { cls: "sub mono", text: shortLoc(f.loc) })); } },
        { h: "Process", v: function (f) { return logWho(f); }, f: function (f) { return f.exec && f.exec !== "am" ? execLink(f.exec) : logWho(f) || "—"; } },
        { h: "Worst", v: worst, f: function (f) { var w = worst(f); return w < 3 ? sevPill(["critical", "warning", "info"][w]) : "—"; } },
        { h: "Found", num: true, v: function (f) { return f.found.length; }, f: function (f) { return num(f.found.length); } },
        { h: "Lines", num: true, v: function (f) { return f.lines; }, f: function (f) { return num(f.lines); } },
        { h: "Size", num: true, v: function (f) { return f.bytes; }, f: function (f) { return bytes(f.bytes); } }
      ]
    }));
    var problems = [];
    logFiles.forEach(function (f) { f.rows.forEach(function (l) { if (l.sev !== "info") problems.push(l); }); });
    if (problems.length) {
      s2.appendChild(el("h3", { text: "Errors and warnings across all logs" }));
      s2.appendChild(logLinesTable(problems, true));
    }
    return s2;
  };

  function notFound(what) { return section(what + " is not in this log", "It may have been cut off, or the link is from another run."); }

  // ---------- routing ----------
  // ---------- at a glance ----------
  // The anatomy diagram is drawn in Go (the same picture as the report's);
  // D3 adds zoom and pan, double-click to zoom to a part, and hover that
  // lights up every badge of the same finding.
  views.anatomy = function () {
    var s = section("The run at a glance", "The whole run in one picture: the cluster, what each node offered YARN, the containers placed there (to scale), and inside an executor the heap's regions with how far each peaked.");
    var wrap = el("div", { cls: "anatwrap" });
    wrap.innerHTML = D.anatomy; // drawn and escaped in Go
    var tools = el("div", { cls: "bar-tools anattools" });
    s.appendChild(tools);
    s.appendChild(wrap);
    add(s, guideNodes({ shows: "Numbered badges are findings, pinned to the part they are about; click one to read it. Hover anything for exact values.",
      read: "Hatched space on a node is memory nobody used: narrower than an executor, it could not hold one. Red outlines are executors killed or lost. Inside the executor, a peak line near the end of the heap means it nearly ran out." }));
    anatomyZoom(wrap, tools);
    return s;
  };
  function anatomyZoom(wrap, tools) {
    var d3 = window.d3, node = wrap.querySelector("svg.anat");
    if (!d3 || !node) return;
    var layer = document.createElementNS(SVG, "g");
    Array.prototype.slice.call(node.childNodes).forEach(function (c) { if (c.nodeName !== "defs") layer.appendChild(c); });
    node.appendChild(layer);
    var svg = d3.select(node), zl = d3.select(layer);
    // Drag pans and ctrl+wheel zooms; a plain wheel still scrolls the page.
    var zoom = d3.zoom().scaleExtent([0.5, 8])
      .filter(function (ev) { return !ev.button && (ev.type !== "wheel" || ev.ctrlKey || ev.metaKey); })
      .on("zoom", function (ev) { zl.attr("transform", ev.transform); });
    svg.call(zoom).on("dblclick.zoom", null);
    // Animate unless the viewer asked for reduced motion.
    var still = window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    function go(ms, f, arg) { if (still) svg.call(f, arg); else svg.transition().duration(ms).call(f, arg); }
    function btn(label, f) { var b = el("button", { type: "button", cls: "more", text: label }); b.addEventListener("click", f); tools.appendChild(b); }
    btn("Zoom in", function () { go(250, zoom.scaleBy, 1.5); });
    btn("Zoom out", function () { go(250, zoom.scaleBy, 1 / 1.5); });
    btn("Fit", function () { go(300, zoom.transform, d3.zoomIdentity); });
    tools.appendChild(el("span", { cls: "count", text: "Drag to move · Ctrl + wheel to zoom · double-click a node or panel to zoom to it" }));
    var vb = node.viewBox.baseVal;
    svg.selectAll("g.node, g.jvm, g.rmpanel").on("dblclick", function (ev) {
      ev.preventDefault();
      var b = this.getBBox(), k = Math.min(8, 0.9 * Math.min(vb.width / b.width, vb.height / b.height));
      go(450, zoom.transform, d3.zoomIdentity.translate(vb.width / 2 - k * (b.x + b.width / 2), vb.height / 2 - k * (b.y + b.height / 2)).scale(k));
    });
    svg.selectAll("[data-finding]")
      .on("mouseenter", function () { svg.selectAll('[data-finding="' + this.getAttribute("data-finding") + '"]').classed("hl", true); })
      .on("mouseleave", function () { svg.selectAll(".hl").classed("hl", false); });
  }
  // #finding/3 opens the overview at that finding.
  views.finding = function (n) {
    var out = views.overview();
    setTimeout(function () { var f = document.getElementById("finding-" + n); if (f) { f.scrollIntoView({ block: "start" }); f.classList.add("flash"); } }, 0);
    return out;
  };

  function route() {
    var h = (location.hash || "#overview").slice(1);
    var slash = h.indexOf("/");
    var name = slash < 0 ? h : h.slice(0, slash), arg = null;
    try { arg = slash < 0 ? null : decodeURIComponent(h.slice(slash + 1)); } catch (e) { arg = h.slice(slash + 1); }
    if (!views[name]) { name = "overview"; arg = null; }
    var tab = { job: "jobs", stage: "stages", executor: "executors", query: "sql", finding: "overview" }[name] || name;
    tabs.querySelectorAll("a").forEach(function (a) { if (a.getAttribute("data-tab") === tab) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
    charts.length = 0;
    tipHide();
    main.textContent = "";
    add(main, views[name](arg));
    drawCharts();
    window.scrollTo(0, 0);
  }
  function drawCharts() { charts.forEach(drawSlot); }
  function redrawCharts() { drawCharts(); }
  window.addEventListener("hashchange", route);
  route();
})();

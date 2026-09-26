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

  // ---------- charts (drawn by the chart layer once Google Charts loads) ----------
  var charts = [];
  function chartSlot(cls, draw) {
    var d = el("div", { cls: "chart gchart " + (cls || "") }, el("div", { cls: "wait", text: "Loading chart…" }));
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
  var TABS = [["overview", "Overview"], ["jobs", "Jobs", jobs.length], ["stages", "Stages", stages.length], ["executors", "Executors", execs.length],
    ["sql", "SQL / DataFrame", queries.length], ["storage", "Storage", rdds.length], ["code", "Code"], ["environment", "Environment"], ["log", "Event log"]];
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
    (D.summary || []).forEach(function (s) { story.appendChild(el("p", { text: s })); });
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
    D.findings.forEach(function (f) {
      var sev = { critical: "crit", warning: "warn", info: "info" }[f.sev] || "info";
      list.appendChild(el("article", { cls: "finding " + sev }, el("div", { cls: "stripe" }), el("div", { cls: "body" },
        el("div", { cls: "t" }, el("span", { cls: "pill " + ({ crit: "crit", warn: "part", info: "info" }[sev]), text: { crit: "Critical", warn: "Warning", info: "Info" }[sev] }), el("h3", { text: f.title })),
        el("p", { text: f.expl }),
        (f.ev || []).map(function (e) {
          var h = refHref(e[1]), lh = logHref(e[2]);
          return el("div", { cls: "ev" }, h ? link(h, e[0]) : e[0], e[2] ? [" · ", lh ? link(lh, e[2]) : e[2]] : "");
        }),
        f.fix ? el("p", { cls: "fix", text: f.fix }) : null)));
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
    s.appendChild(el("div", { cls: "grid2" }, chartSlot("", "stageTimes"), chartSlot("", "stageData")));
    if (stages.some(function (st) { return st.diskSpill > 0 || st.memSpill > 0; })) s.appendChild(chartSlot("", "stageSpill"));
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
      s.appendChild(el("h3", { text: "What this stage computes" }));
      var ow = el("div", { cls: "dagwrap", hidden: true });
      s.appendChild(ow);
      drawGraph(ow, ops.layout, function (i) {
        var r = ops.rdds[i];
        return { title: r[2] || r[1], sub: "RDD " + r[0] + (r[2] ? " · " + r[1] : "") + (r[5] ? " · " + num(r[5]) + " cached" : ""),
          cls: r[6] ? "hot" : "", tip: "RDD " + r[0] + " (" + r[1] + ")" + (r[2] ? ", made by " + r[2] : "") + "\n" + (r[3] || "") + "\n" + num(r[4]) + " partitions" + (r[6] ? ", cached as " + r[6] : "") + (r[8] && r[8] !== "DETERMINATE" ? ", output " + r[8].toLowerCase() : "") + (r[7] ? ", barrier" : "") };
      }, "The RDDs this stage computes, named by the operation that made each (Spark's stage graph). Data flows down the arrows. Cached RDDs are outlined.");
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
    var g = el("div", { cls: "grid2" });
    g.appendChild(chartSlot("", "durationHistogram:" + st.key));
    g.appendChild(chartSlot("", "taskScatter:" + st.key));
    s.appendChild(g);
    s.appendChild(el("h3", null, "Slowest tasks", el("span", { cls: "sampled", text: "top " + num(det.slow.length) })));
    s.appendChild(taskTable(det.slow, { sort: 6 }));
    s.appendChild(el("h3", null, "Task sample", el("span", { cls: "sampled", text: num(det.sample.length) + " of " + num(det.from) })));
    s.appendChild(explain(det.sample.length < det.from ? "A uniform random sample of this stage's task attempts, kept so the page stays small." : "Every task attempt of this stage."));
    s.appendChild(taskTable(det.sample, { sort: 0 }));
    if (det.cells.length) {
      s.appendChild(el("h3", { text: "By executor" }));
      s.appendChild(cellTable(det.cells, "stage"));
    } else if (D.cellsCapped) s.appendChild(explain("Per-executor totals stopped before this stage (the app-wide cap was reached)."));
    return s;
  };

  function cellTable(cells, by) {
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
    return table({ rows: rows, sort: by === "stage" ? 3 : 0, dir: by === "stage" ? "desc" : "asc", page: 100, cols: cols });
  }

  views.executors = function () {
    var s = section("Executors", "Executors are the worker processes that ran tasks. The driver coordinates and usually runs none.");
    s.appendChild(chartSlot("tall", "executorsTimeline"));
    s.appendChild(el("div", { cls: "grid2" }, chartSlot("", "execTime"), chartSlot("", "execHeapAll")));
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
  function drawGraph(wrap, lay, info, caption) {
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
    wrap.appendChild(svg);
    if (caption) wrap.appendChild(el("p", { cls: "cap", text: caption }));
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
    }, "Job " + jobID + "'s stages. Arrows point from a stage to the stages that read its output. " + (hot != null ? "This stage is outlined. " : "") + "Failed stages have a red outline and skipped ones (their output already existed) a dashed one; click a stage to open it.");
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
    }, "Data flows down the arrows, from the scans at the top to the result at the bottom. Rows and time are totals across tasks; the table below lists every metric.");
    return true;
  }

  // ---------- chart layer: Google Charts, loaded on demand ----------
  // Google Charts cannot be self-hosted, so this is the page's only network
  // request. Release 52 is pinned (a frozen version) rather than "current".
  var LOADER = "https://www.gstatic.com/charts/loader.js", GC_VERSION = "52";
  var gc = { state: "idle", queue: [] };
  var banner = document.getElementById("sp-banner");
  function showBanner(text) { banner.textContent = text; banner.hidden = false; }
  function loadCharts(cb) {
    if (gc.state === "ready") { cb(); return; }
    if (gc.state === "failed") return;
    gc.queue.push(cb);
    if (gc.state === "loading") return;
    gc.state = "loading";
    var fail = function () {
      if (gc.state === "ready") return;
      gc.state = "failed";
      showBanner("Charts could not load. They are drawn with Google Charts, which this page loads from www.gstatic.com, so they need internet access. Everything else on the page works without it.");
      charts.forEach(function (c) { waitText(c, "Chart unavailable: Google Charts could not be loaded."); });
    };
    var slow = setTimeout(function () { if (gc.state === "loading") showBanner("Charts are still loading from www.gstatic.com. Tables and details work in the meantime."); }, 10000);
    var s = document.createElement("script");
    s.src = LOADER;
    s.async = true;
    s.onerror = function () { clearTimeout(slow); fail(); };
    s.onload = function () {
      try {
        google.charts.load(GC_VERSION, { packages: ["corechart", "timeline"] });
        google.charts.setOnLoadCallback(function () {
          clearTimeout(slow);
          gc.state = "ready";
          banner.hidden = true;
          var q = gc.queue; gc.queue = [];
          q.forEach(function (f) { f(); });
        });
      } catch (e) { clearTimeout(slow); fail(); }
    };
    document.head.appendChild(s);
  }
  function waitText(c, text) { var w = c.el.querySelector(".wait"); if (w) w.textContent = text; }

  function css(name) { return getComputedStyle(root).getPropertyValue(name).trim(); }
  function theme() {
    return { ink: css("--ink"), muted: css("--muted"), line: css("--line"), font: css("--sans").replace(/"/g, "'") || "sans-serif",
      series: css("--viz-series"), fail: css("--viz-fail"), neutral: css("--viz-neutral"),
      // categorical slots, validated for colour-blind separation (report.css)
      viz: [css("--viz-1"), css("--viz-2"), css("--viz-3"), css("--viz-4"), css("--viz-5")] };
  }
  function baseOpts(th, extra) {
    var o = {
      backgroundColor: "transparent", fontName: th.font, fontSize: 12, height: 280,
      chartArea: { left: 72, right: 20, top: 30, bottom: 48, width: "100%", height: "100%" },
      legend: { position: "top", alignment: "start", textStyle: { color: th.ink } },
      hAxis: axis(th), vAxis: axis(th),
      tooltip: { textStyle: { color: "#15212B" } }
    };
    Object.keys(extra || {}).forEach(function (k) { o[k] = extra[k]; });
    return o;
  }
  function withFormat(axis, f) { axis.format = f; return axis; }
  // axis is the recessive default axis, with extra settings merged in.
  function axis(th, extra) {
    var o = { textStyle: { color: th.muted }, gridlines: { color: th.line }, minorGridlines: { color: "transparent" }, baselineColor: th.line, titleTextStyle: { color: th.muted, italic: false } };
    Object.keys(extra || {}).forEach(function (k) { o[k] = extra[k]; });
    return o;
  }
  // frame replaces the slot's placeholder with a plot area and a caption.
  function frame(c, caption) {
    c.el.textContent = "";
    var plot = el("div", { cls: "plot" });
    c.el.appendChild(plot);
    if (caption) c.el.appendChild(el("p", { cls: "cap", text: caption }));
    return plot;
  }
  function statusColor(th, s) { return s === "succeeded" ? th.series : s === "failed" ? th.fail : th.neutral; }
  var appEnd = a.end || (function () { var m = a.start || 0; stages.forEach(function (s) { m = Math.max(m, s.completed || 0); }); return m; })();

  function timeline(c, th, rows, caption, emptyText) {
    if (!rows.length) { waitText(c, emptyText); return; }
    var capRows = rows.slice(0, 600);
    var plot = frame(c, caption + (rows.length > capRows.length ? " Showing the first " + num(capRows.length) + " of " + num(rows.length) + "." : ""));
    c.el.classList.add("scrolly");
    var dt = new google.visualization.DataTable();
    dt.addColumn({ type: "string", id: "Row" });
    dt.addColumn({ type: "string", id: "Bar" });
    dt.addColumn({ type: "string", role: "style" });
    dt.addColumn({ type: "string", role: "tooltip" });
    dt.addColumn({ type: "date", id: "Start" });
    dt.addColumn({ type: "date", id: "End" });
    capRows.forEach(function (r) { dt.addRow([r.row, r.bar, r.color, r.tip, new Date(r.start), new Date(Math.max(r.end, r.start + 1))]); });
    var ch = new google.visualization.Timeline(plot);
    ch.draw(dt, { height: Math.min(capRows.length, 600) * 28 + 60, backgroundColor: css("--surface"), fontName: th.font,
      alternatingRowStyle: false,
      timeline: { showBarLabels: true, rowLabelStyle: { color: th.ink, fontName: th.font, fontSize: 12 }, barLabelStyle: { fontName: th.font, fontSize: 11 } },
      hAxis: { format: "HH:mm:ss" }, avoidOverlappingGridLines: false, tooltip: { isHtml: false } });
    if (c.link) google.visualization.events.addListener(ch, "select", function () { var sel = ch.getSelection()[0]; if (sel && sel.row != null) location.hash = c.link(capRows[sel.row]); });
  }
  var STATUS_NOTE = " Blue: succeeded. Red: failed. Grey: running, incomplete or skipped.";

  // seriesTable joins metric series on their timestamps into one table.
  function seriesTable(list, label) {
    var times = {}, cols = list.map(function (s) { var m = {}; s.points.forEach(function (p) { m[p[0]] = p[1]; times[p[0]] = 1; }); return m; });
    var dt = new google.visualization.DataTable();
    dt.addColumn("datetime", "Time");
    list.forEach(function (s) { dt.addColumn("number", label(s)); });
    Object.keys(times).map(Number).sort(function (x, y) { return x - y; }).forEach(function (t) {
      dt.addRow([new Date(t)].concat(cols.map(function (m) { return m[t] == null ? null : m[t]; })));
    });
    return dt;
  }
  function metric(name, stat, scope) {
    return ((D.aws || {}).metrics || []).filter(function (s) { return s.name === name && (!stat || s.stat === stat) && (!scope || s.scope === scope); });
  }
  // hbarChart draws horizontal bars, stacked from series, one row each;
  // rows link to a page when link returns one.
  function hbarChart(c, th, rows, series, caption, format, link) {
    if (!rows.length) { waitText(c, "Nothing to chart for this run."); return; }
    // a series with nothing in it only clutters the legend
    series = series.filter(function (sr) { return rows.some(function (r) { return sr.value(r) > 0; }); });
    var dt = new google.visualization.DataTable();
    dt.addColumn("string", "Row");
    series.forEach(function (sr) { dt.addColumn("number", sr.label); dt.addColumn({ type: "string", role: "tooltip" }); });
    rows.forEach(function (r) {
      var row = [r.label];
      series.forEach(function (sr) { var v = sr.value(r); row.push(v, r.label + "\n" + sr.label + ": " + format(v)); });
      dt.addRow(row);
    });
    var plot = frame(c, caption);
    var ch = new google.visualization.BarChart(plot);
    ch.draw(dt, baseOpts(th, { isStacked: true, height: Math.max(160, rows.length * 26 + 70), colors: series.map(function (sr) { return sr.color; }),
      chartArea: { left: 170, right: 20, top: series.length > 3 ? 48 : 30, bottom: 36, width: "100%", height: "100%" }, bar: { groupWidth: "72%" }, legend: series.length > 1 ? { position: "top", alignment: "start", maxLines: 3, textStyle: { color: th.ink } } : { position: "none" },
      vAxis: axis(th, { textStyle: { color: th.ink, fontSize: 11 } }), hAxis: axis(th, { minValue: 0, format: "short" }) }));
    if (link) google.visualization.events.addListener(ch, "select", function () { var sel = ch.getSelection()[0]; if (sel && sel.row != null) location.hash = link(rows[sel.row]); });
  }
  // stageName is one short line, so the axis labels every bar: the call
  // site ("count at Foo.java:0") is cut to its operation.
  function stageName(st) { return "Stage " + st.id + (st.attempt ? "." + st.attempt : "") + " · " + (st.name || "").split(" at ")[0].slice(0, 16); }
  function topBy(list, n, key) { return list.filter(function (x) { return key(x) > 0; }).sort(function (a2, b2) { return key(b2) - key(a2); }).slice(0, n); }
  var DRAW = {
    stageTimes: function (c, th) {
      var rows = topBy(stages, 20, function (st) { return span(st.submitted, st.completed) || 0; });
      hbarChart(c, th, rows.map(function (st) { return { label: stageName(st), st: st }; }), [
        { label: "Succeeded", color: th.viz[0], value: function (r) { return r.st.status === "failed" ? 0 : span(r.st.submitted, r.st.completed) / 1000; } },
        { label: "Failed", color: th.fail, value: function (r) { return r.st.status === "failed" ? span(r.st.submitted, r.st.completed) / 1000 : 0; } }
      ], "The longest stages, in seconds from submission to completion. Click a bar to open the stage.", function (v) { return dur(v * 1000); }, function (r) { return "#stage/" + r.st.key; });
    },
    stageData: function (c, th) {
      var moved = function (st) { return st.input + st.shRead + st.shWrite + st.output + st.diskSpill; };
      var rows = topBy(stages, 20, moved).map(function (st) { return { label: stageName(st), st: st }; });
      var mib = function (k) { return function (r) { return r.st[k] / 1048576; }; };
      hbarChart(c, th, rows, [
        { label: "Read", color: th.viz[0], value: mib("input") }, { label: "Shuffle read", color: th.viz[1], value: mib("shRead") },
        { label: "Shuffle write", color: th.viz[2], value: mib("shWrite") }, { label: "Written", color: th.viz[3], value: mib("output") },
        { label: "Spilled to disk", color: th.viz[4], value: mib("diskSpill") }
      ], "The stages that moved the most data, in MiB: read, shuffled between executors, written out and spilled to disk.", function (v) { return bytes(v * 1048576); }, function (r) { return "#stage/" + r.st.key; });
    },
    stageSpill: function (c, th) {
      var rows = topBy(stages, 20, function (st) { return st.memSpill + st.diskSpill; }).map(function (st) { return { label: stageName(st), st: st }; });
      hbarChart(c, th, rows, [
        { label: "Spilled (size in memory)", color: th.viz[1], value: function (r) { return r.st.memSpill / 1048576; } },
        { label: "Written to disk", color: th.viz[4], value: function (r) { return r.st.diskSpill / 1048576; } }
      ], "Spill by stage, in MiB: data that did not fit in memory while the stage ran, and what it came to on disk.", function (v) { return bytes(v * 1048576); }, function (r) { return "#stage/" + r.st.key; });
    },
    dataOverTime: function (c, th) {
      var done = stages.filter(function (st) { return st.completed; }).sort(function (a2, b2) { return a2.completed - b2.completed; });
      if (done.length < 2) { waitText(c, "Too few finished stages to chart."); return; }
      var dt = new google.visualization.DataTable();
      dt.addColumn("datetime", "Time");
      ["Read", "Shuffle write", "Written"].forEach(function (l) { dt.addColumn("number", l + " (MiB)"); });
      var inB = 0, sh = 0, out = 0;
      dt.addRow([new Date(a.start || done[0].submitted), 0, 0, 0]);
      done.forEach(function (st) { inB += st.input; sh += st.shWrite; out += st.output; dt.addRow([new Date(st.completed), inB / 1048576, sh / 1048576, out / 1048576]); });
      var plot = frame(c, "Data read, shuffled and written, in MiB, added up as each stage finished: a steep rise is where the work happened.");
      new google.visualization.SteppedAreaChart(plot).draw(dt, baseOpts(th, { colors: [th.viz[0], th.viz[2], th.viz[3]], areaOpacity: 0.08, connectSteps: true, isStacked: false,
        hAxis: withFormat(baseOpts(th).hAxis, "HH:mm:ss"), vAxis: axis(th, { minValue: 0, format: "short" }) }));
    },
    execTime: function (c, th) {
      var rows = topBy(execs.filter(function (x) { return x.id !== "driver"; }), 30, function (x) { return x.run; }).map(function (x) { return { label: "Executor " + x.id, x: x }; });
      var sec = function (f) { return function (r) { return Math.max(f(r.x), 0) / 1000; }; };
      hbarChart(c, th, rows, [
        { label: "Computing", color: th.viz[0], value: sec(function (x) { return x.cpuNs / 1e6; }) },
        { label: "Garbage collection", color: th.viz[1], value: sec(function (x) { return x.gc; }) },
        { label: "Other or waiting", color: th.neutral, value: sec(function (x) { return x.run - x.cpuNs / 1e6 - x.gc; }) }
      ], "Task run time on each executor, in seconds: computing on the JVM, collecting garbage, or neither (waiting for shuffle data, storage or Python workers).", function (v) { return dur(v * 1000); }, function (r) { return "#executor/" + encodeURIComponent(r.x.id); });
    },
    execHeapAll: function (c, th) {
      if (!D.heapBytes) { waitText(c, "The executors' heap size is not known."); return; }
      var rows = topBy(execs.filter(function (x) { return x.id !== "driver"; }), 30, function (x) { return x.peakHeap || 0; }).map(function (x) { return { label: "Executor " + x.id, x: x }; });
      hbarChart(c, th, rows, [
        { label: "Peak heap", color: th.viz[0], value: function (r) { return r.x.peakHeap / 1048576; } },
        { label: "Heap not used at peak", color: th.line, value: function (r) { return Math.max(D.heapBytes - r.x.peakHeap, 0) / 1048576; } }
      ], "Each executor's peak Java heap against the " + bytes(D.heapBytes) + " it had, in MiB. Peaks are sampled, so short spikes can be missed.", function (v) { return bytes(v * 1048576); }, function (r) { return "#executor/" + encodeURIComponent(r.x.id); });
    },
    nodeMemory: function (c, th) {
      var rows = D.aws.nodes.filter(function (n) { return n.yarnMem; }).map(function (n) { return { label: n.host.split(".")[0], n: n }; });
      var mib = function (f) { return function (r) { return Math.max(f(r.n), 0) / 1048576; }; };
      var execBytes = function (n) { return (n.execMem || 0) * (n.peakExecs || n.executors.length); };
      hbarChart(c, th, rows, [
        { label: "Driver container", color: th.viz[1], value: mib(function (n) { return n.driverMem || 0; }) },
        { label: "Executor containers (at once)", color: th.viz[0], value: mib(execBytes) },
        { label: "Free", color: th.line, value: mib(function (n) { return n.yarnMem - (n.driverMem || 0) - execBytes(n); }) }
      ], "What YARN placed on each node, in MiB, against the memory the node offered. Free space smaller than one executor's container cannot hold another executor.", function (v) { return bytes(v * 1048576); });
    },
    clusterContainers: function (c, th) {
      var list = metric("ContainerAllocated").concat(metric("ContainerPending"));
      if (!list.length) { waitText(c, "CloudWatch had no container counts for this run."); return; }
      var plot = frame(c, "Containers YARN had placed and containers waiting for room, across the whole cluster, every minute. This application ran from " + (a.start ? tfmt.format(new Date(a.start)) : "?") + " to " + (a.end ? tfmt.format(new Date(a.end)) : "?") + ". Waiting while memory is free means the containers were too big to fit.");
      new google.visualization.SteppedAreaChart(plot).draw(seriesTable(list, function (s) { return s.name === "ContainerPending" ? "Waiting" : "Allocated"; }), baseOpts(th, {
        colors: [th.series, th.fail], areaOpacity: 0.12, connectSteps: true, isStacked: false, hAxis: withFormat(baseOpts(th).hAxis, "HH:mm"), vAxis: axis(th, { title: "Containers", minValue: 0, format: "#,###" })
      }));
    },
    nodeCPU: function (c, th) {
      var list = metric("CPUUtilization", "Average");
      if (!list.length) { waitText(c, "CloudWatch had no CPU figures for these nodes."); return; }
      var name = {};
      D.aws.nodes.forEach(function (n) { if (n.instance) name[n.instance.id] = n.instance.id + (n.driver ? " (driver)" : n.executors.length ? " (" + n.executors.length + " executors)" : n.instance.role === "MASTER" ? " (primary)" : " (idle)"); });
      var plot = frame(c, "Each node's CPU, averaged over EC2's 5-minute periods: the whole machine, so daemons and other applications count too.");
      new google.visualization.LineChart(plot).draw(seriesTable(list, function (s) { return name[s.scope] || s.scope; }), baseOpts(th, {
        lineWidth: 2, pointSize: 5, hAxis: withFormat(baseOpts(th).hAxis, "HH:mm"), vAxis: axis(th, { title: "CPU %", minValue: 0, maxValue: 100 })
      }));
    },
    running: function (c, th) {
      var R = D.running;
      if (!R || !R.busy.length) { waitText(c, D.collected ? "No tasks finished, so there is nothing to chart." : "Per-task detail was not collected for this run."); return; }
      var workers = execs.filter(function (x) { return x.id !== "driver"; });
      function slotsAt(t) { var n = 0; workers.forEach(function (x) { if (x.added && x.added <= t && t < (x.removed || appEnd + 1)) n += x.cores; }); return n; }
      var dt = new google.visualization.DataTable();
      dt.addColumn("datetime", "Time");
      dt.addColumn("number", "Tasks running (average)");
      dt.addColumn("number", "Task slots (executor cores)");
      R.busy.forEach(function (b, i) { var t = R.start + i * R.bucketMs; dt.addRow([new Date(t), Math.round(b / R.bucketMs * 10) / 10, slotsAt(t + R.bucketMs / 2)]); });
      var plot = frame(c, "Average tasks running in each " + dur(R.bucketMs) + " bucket, against the cores of the executors alive then. Task times are stamped by the driver, so running tasks can briefly exceed the slots.");
      new google.visualization.ComboChart(plot).draw(dt, baseOpts(th, {
        seriesType: "area", colors: [th.series, th.neutral],
        series: { 0: { areaOpacity: 0.25, lineWidth: 2 }, 1: { type: "steppedArea", areaOpacity: 0, lineWidth: 2, lineDashStyle: [4, 4] } },
        hAxis: withFormat(baseOpts(th).hAxis, "HH:mm:ss")
      }));
    },
    jobsTimeline: function (c, th) {
      c.link = function (r) { return "#job/" + r.id; };
      timeline(c, th, jobs.filter(function (j) { return j.submitted; }).map(function (j) {
        var end = j.completed || appEnd;
        return { id: j.id, row: "Job " + j.id, bar: (j.desc || j.name || "").slice(0, 80), color: statusColor(th, j.status), start: j.submitted, end: end,
          tip: "Job " + j.id + ": " + (j.desc || j.name) + "\n" + (STATUS[j.status] || j.status) + ", " + dur(end - j.submitted) };
      }), "When each job ran. Click a bar to open the job." + STATUS_NOTE, "No job has a start time.");
    },
    executorsTimeline: function (c, th) {
      c.link = function (r) { return "#executor/" + encodeURIComponent(r.id); };
      timeline(c, th, execs.filter(function (x) { return x.id !== "driver" && x.added; }).map(function (x) {
        var bad = x.kind === "memory-kill" || x.kind === "lost";
        var end = x.removed || appEnd;
        return { id: x.id, row: "Executor " + x.id, bar: x.host || "", color: bad ? th.fail : x.removed ? th.neutral : th.series, start: x.added, end: end,
          tip: "Executor " + x.id + " on " + x.host + "\n" + (x.removed ? "Removed: " + (x.reason || "no reason logged") : "Ran to the end") + ", " + dur(end - x.added) };
      }).concat(exclusions.filter(function (x) { return x.scope === "application"; }).map(function (x) {
        var end = x.lifted || appEnd;
        return { id: x.kind === "executor" ? x.target : "", row: (x.kind === "executor" ? "Executor " : "Node ") + x.target + " excluded", bar: num(x.failures) + " failures", color: th.fail, start: x.time, end: end,
          tip: (x.kind === "executor" ? "Executor " : "Node ") + x.target + " excluded after " + num(x.failures) + " failures" + (x.lifted ? ", lifted after " + dur(x.lifted - x.time) : "") };
      })), "How long each executor lived. Blue: ran to the end. Red: killed or lost, or a period it was excluded. Grey: removed for another reason, such as being idle. Click one to open it.", "No executor was logged.");
    },
    sqlTimeline: function (c, th) {
      c.link = function (r) { return "#query/" + r.id; };
      timeline(c, th, queries.filter(function (q) { return q.start; }).map(function (q) {
        var st = queryStatus(q), end = q.end || appEnd;
        return { id: q.id, row: "Query " + q.id, bar: (q.desc || "").slice(0, 80), color: statusColor(th, st), start: q.start, end: end,
          tip: "Query " + q.id + ": " + q.desc + "\n" + (STATUS[st] || st) + ", " + dur(end - q.start) };
      }), "When each query ran. Click a bar to open its plan." + STATUS_NOTE, "No query was logged.");
    },
    durationHistogram: function (c, th, key) {
      var det = D.detail[key];
      if (!det || !det.h.length) { waitText(c, "No successful task to chart."); return; }
      var dt = new google.visualization.DataTable();
      dt.addColumn("string", "Duration");
      dt.addColumn("number", "Tasks");
      dt.addColumn({ type: "string", role: "tooltip" });
      det.h.forEach(function (b) { var range = b[0] === b[1] ? dur(b[0]) : dur(b[0]) + " to " + dur(b[1]); dt.addRow([dur(b[0]), b[2], range + ": " + num(b[2]) + " tasks"]); });
      var plot = frame(c, "Successful tasks by how long they took (every task, not a sample). A long tail on the right means a few tasks held the stage up.");
      new google.visualization.ColumnChart(plot).draw(dt, baseOpts(th, { colors: [th.series], legend: { position: "none" }, bar: { groupWidth: "88%" },
        hAxis: axis(th, { textStyle: { color: th.muted, fontSize: 10 }, slantedText: true, slantedTextAngle: 40 }), vAxis: axis(th, { title: "Tasks", format: "#,###", minValue: 0 }) }));
    },
    taskScatter: function (c, th, key) {
      var det = D.detail[key];
      if (!det || !det.sample.length) { waitText(c, "No task to chart."); return; }
      var seen = {}, pts = [];
      det.slow.concat(det.sample).forEach(function (r) { if (!seen[r[T.task]]) { seen[r[T.task]] = 1; pts.push(r); } });
      var st0 = null;
      stages.forEach(function (s) { if (s.key === key) st0 = s.submitted; });
      pts.forEach(function (r) { var at = (D.t0 || 0) + r[T.launch]; if (st0 == null || at < st0) st0 = at; });
      var dt = new google.visualization.DataTable();
      dt.addColumn("number", "Started (s after the stage)");
      dt.addColumn("number", "Succeeded");
      dt.addColumn({ type: "string", role: "tooltip" });
      dt.addColumn("number", "Failed or killed");
      dt.addColumn({ type: "string", role: "tooltip" });
      pts.forEach(function (r) {
        var at = ((D.t0 || 0) + r[T.launch] - st0) / 1000, secs = r[T.dur] / 1000, bad = r[T.status] !== 0;
        var tip = "Task " + r[T.task] + " (partition " + r[T.index] + ") on executor " + execName(r[T.exec]) + "\n" + dur(r[T.dur]) + ", " + num(r[T.rows]) + " rows read";
        dt.addRow(bad ? [at, null, null, secs, tip] : [at, secs, tip, null, null]);
      });
      var plot = frame(c, "Each dot is a task: when it started, counted from the start of the stage, and how long it took. Dots high above the rest are stragglers; check whether they read more rows. " +
        (det.sample.length < det.from ? "From a sample of " + num(det.sample.length) + " of " + num(det.from) + " tasks plus the slowest " + num(det.slow.length) + "." : "Every task of this stage."));
      new google.visualization.ScatterChart(plot).draw(dt, baseOpts(th, { colors: [th.series, th.fail], pointSize: 5, dataOpacity: 0.75,
        series: { 0: { pointShape: "circle" }, 1: { pointShape: "triangle", pointSize: 8 } },
        hAxis: axis(th, { title: "Started, seconds after the stage began", minValue: 0 }),
        vAxis: axis(th, { title: "Duration (s)", minValue: 0 }) }));
    },
    execHeap: function (c, th, id) {
      var idx = D.execs.indexOf(id), pts = [];
      if (idx >= 0) stages.forEach(function (st) {
        var det = D.detail[st.key];
        if (det) det.cells.forEach(function (r) { if (r[C.exec] === idx && r[C.peakHeap] > 0) pts.push([st, r[C.peakHeap]]); });
      });
      if (!pts.length) { waitText(c, "No heap samples were logged for this executor (spark.eventLog.logStageExecutorMetrics turns them on)."); return; }
      var dt = new google.visualization.DataTable();
      dt.addColumn("string", "Stage");
      dt.addColumn("number", "Peak heap (MiB)");
      dt.addColumn({ type: "string", role: "tooltip" });
      dt.addColumn("number", "Configured heap (MiB)");
      pts.forEach(function (p) { dt.addRow(["Stage " + p[0].key, Math.round(p[1] / 1048576), "Stage " + p[0].key + " (" + p[0].name + "): peak heap " + bytes(p[1]), D.heapBytes ? Math.round(D.heapBytes / 1048576) : null]); });
      var plot = frame(c, "The highest Java heap use sampled while each stage ran on this executor, against the heap it was given. Samples are peaks, so short spikes can be missed.");
      new google.visualization.ComboChart(plot).draw(dt, baseOpts(th, { seriesType: "bars", colors: [th.series, th.neutral],
        series: { 1: { type: "line", lineWidth: 2, lineDashStyle: [4, 4], pointSize: 0 } }, bar: { groupWidth: "80%" },
        hAxis: axis(th, { textStyle: { color: th.muted, fontSize: 10 }, slantedText: true }),
        vAxis: axis(th, { title: "MiB", format: "#,###", minValue: 0 }) }));
    }
  };
  function drawSlot(c) {
    var name = c.draw.split(":")[0], arg = c.draw.slice(name.length + 1);
    if (!DRAW[name] || !document.body.contains(c.el)) return;
    try { DRAW[name](c, theme(), arg); } catch (e) { waitText(c, "This chart could not be drawn: " + e.message); }
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
  function route() {
    var h = (location.hash || "#overview").slice(1);
    var slash = h.indexOf("/");
    var name = slash < 0 ? h : h.slice(0, slash), arg = null;
    try { arg = slash < 0 ? null : decodeURIComponent(h.slice(slash + 1)); } catch (e) { arg = h.slice(slash + 1); }
    if (!views[name]) { name = "overview"; arg = null; }
    var tab = { job: "jobs", stage: "stages", executor: "executors", query: "sql" }[name] || name;
    tabs.querySelectorAll("a").forEach(function (a) { if (a.getAttribute("data-tab") === tab) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
    charts.length = 0;
    main.textContent = "";
    add(main, views[name](arg));
    drawCharts();
    window.scrollTo(0, 0);
  }
  function drawCharts() {
    if (!charts.length) return;
    var mine = charts.slice();
    loadCharts(function () { mine.forEach(drawSlot); });
  }
  function redrawCharts() { if (gc.state === "ready") charts.forEach(drawSlot); }
  window.addEventListener("hashchange", route);
  route();
})();

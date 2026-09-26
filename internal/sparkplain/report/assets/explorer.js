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
    var box2 = el("div", { cls: "tbl" + (opts.scroll ? " scroll" : "") });
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
  window.SP = { D: D, el: el, bytes: bytes, dur: dur, num: num, pct: pct, charts: charts, stages: stages, execs: execs, jobs: jobs, T: T, C: C };

  // ---------- header ----------
  var a = D.app;
  document.getElementById("sp-tool").textContent = D.tool;
  var head = document.getElementById("sp-head");
  add(head, el("h1", null, a.name || "Spark application", " ", el("span", { cls: "status " + ({ succeeded: "ok", failed: "crit", incomplete: "warn", running: "warn" }[a.status] || "na"), text: STATUS[a.status] || "Unknown" })));
  add(head, el("div", { cls: "idline" },
    el("span", null, "App ", el("b", { cls: "mono", text: a.id || "unknown" }), a.attempt ? " · attempt " + a.attempt : ""),
    a.spark ? el("span", null, el("b", { text: "Spark " + a.spark }), a.master ? " · " + a.master : "", a.deploy ? " · " + a.deploy + " mode" : "") : null,
    a.user ? el("span", null, "User ", el("b", { text: a.user })) : null,
    a.start ? el("span", null, el("b", { cls: "num" }, when(a.start, true), " → ", a.end ? when(a.end) : "still running"), " · ", dur(a.duration)) : null));
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
    ["sql", "SQL / DataFrame", queries.length], ["storage", "Storage", rdds.length], ["environment", "Environment"]];
  var tabs = document.getElementById("sp-tabs");
  TABS.forEach(function (t) { tabs.appendChild(el("a", { href: "#" + t[0], "data-tab": t[0] }, t[1], t[2] != null ? el("span", { cls: "n", text: num(t[2]) }) : null)); });

  var views = {};
  views.overview = function () {
    var out = [];
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
    out.push(ov);
    var fs = section("Findings", D.findings.length ? "Problems and notes found in this run. Evidence links open the stage, job or executor it concerns." : "No findings for this run.");
    var list = el("div", { cls: "findings" });
    D.findings.forEach(function (f) {
      var sev = { critical: "crit", warning: "warn", info: "info" }[f.sev] || "info";
      list.appendChild(el("article", { cls: "finding " + sev }, el("div", { cls: "stripe" }), el("div", { cls: "body" },
        el("div", { cls: "t" }, el("span", { cls: "pill " + ({ crit: "crit", warn: "part", info: "info" }[sev]), text: { crit: "Critical", warn: "Warning", info: "Info" }[sev] }), el("h3", { text: f.title })),
        el("p", { text: f.expl }),
        (f.ev || []).map(function (e) {
          var h = refHref(e[1]);
          return el("div", { cls: "ev" }, h ? link(h, e[0]) : e[0], e[2] ? " · " + e[2] : "");
        }),
        f.fix ? el("p", { cls: "fix", text: f.fix }) : null)));
    });
    fs.appendChild(list);
    out.push(fs);
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
    s.appendChild(stageTable(stages));
    return s;
  };

  var METRICS = [
    ["durationMs", "Duration", dur, "Wall-clock time from launch to finish."],
    ["runTimeMs", "Executor run time", dur, "Time the task's code ran on the executor."],
    ["gcTimeMs", "GC time", dur, "Time the JVM spent collecting garbage during the task."],
    ["deserializeMs", "Task deserialization", dur, "Time to unpack the task before running it; high on a cold executor."],
    ["recordsRead", "Rows read", num, "Rows from input plus shuffle."],
    ["inputBytes", "Input size", bytes, "Bytes read from files and tables."],
    ["shuffleReadBytes", "Shuffle read", bytes, "Bytes fetched from earlier stages."],
    ["shuffleRecordsRead", "Shuffle rows read", num, "Rows fetched from earlier stages."],
    ["shuffleFetchWaitMs", "Shuffle fetch wait", dur, "Time spent waiting for shuffle data to arrive."],
    ["shuffleWriteBytes", "Shuffle write", bytes, "Bytes written for later stages."],
    ["memorySpillBytes", "Memory spill", bytes, "Size in memory of data that had to be spilled."],
    ["diskSpillBytes", "Disk spill", bytes, "Bytes written to local disk because data did not fit."],
    ["peakExecutionMemory", "Peak execution memory", bytes, "Most memory the task used for sorts, joins and aggregations."],
    ["outputBytes", "Output size", bytes, "Bytes written to files and tables."]
  ];
  var SKEWY = { durationMs: 1, runTimeMs: 1, recordsRead: 1, inputBytes: 1, shuffleReadBytes: 1, shuffleRecordsRead: 1 };
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
      rows: taskRows(rows), sort: o.sort, dir: "desc", page: 50, scroll: true,
      rowCls: function (t) { return t.status === 1 ? "failedrow" : null; },
      cols: [
        numCol("Task", "task"), numCol("Partition", "index"),
        { h: "Attempt", num: true, v: function (t) { return t.attempt; }, f: function (t) { return num(t.attempt + 1) + (t.spec ? " (speculative)" : ""); } },
        { h: "Executor", v: function (t) { return t.execID; }, f: function (t) { return execLink(t.execID); } },
        { h: "Status", v: function (t) { return t.status; }, f: function (t) { return status(TS[t.status]); } },
        { h: "Launched", num: true, v: function (t) { return t.at; }, f: function (t) { return when(t.at); } },
        numCol("Duration", "dur", dur), numCol("Run", "run", dur), numCol("GC", "gc", dur), numCol("Deserialize", "deser", dur),
        numCol("Fetch wait", "fetch", dur), numCol("Rows read", "rows"), numCol("Input", "input", bytes),
        numCol("Shuffle read", "shRead", bytes), numCol("Shuffle write", "shWrite", bytes), numCol("Spill", "spill", bytes),
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
      fact("Task time", dur(st.dur), "All task attempts' durations added up."),
      fact("Jobs", st.jobs.length ? jl : "—", "The actions this stage ran for."),
      fact("Runs after", st.parents.length ? pl : "Nothing: it reads its input directly.", "Stages whose output this stage reads.")));
    if (st.failure) { s.appendChild(el("h3", { text: "Why it failed" })); s.appendChild(el("pre", { cls: "plan", text: st.failure })); }
    s.appendChild(el("div", { cls: "dagwrap", "data-dag": st.key, hidden: true }));
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
        { h: "GC share", num: true, title: "Share of task run time spent in garbage collection", v: function (x) { return x.run ? x.gc / x.run : null; }, f: function (x) { var g = x.run ? x.gc / x.run : null; return el("span", { cls: g > 0.1 ? "warnv" : null, text: pct(g) }); } },
        numCol("Input", "input", bytes), numCol("Shuffle read", "shRead", bytes), numCol("Shuffle write", "shWrite", bytes),
        { h: "Peak heap", num: true, v: function (x) { return x.peakHeap; }, f: function (x) { return x.peakHeap ? el("span", null, bytes(x.peakHeap), el("span", { cls: "sub", text: "of " + bytes(D.heapBytes) })) : "—"; } }
      ]
    }));
    return s;
  };
  views.executor = function (id) {
    var x = execByID[id];
    if (!x) return notFound("Executor " + id);
    var s = section(id === "driver" ? "Driver" : "Executor " + id);
    s.insertBefore(el("div", { cls: "crumbs" }, link("#executors", "Executors"), " / " + id), s.firstChild);
    s.appendChild(el("div", { cls: "facts" },
      fact("Host", x.host || "—"),
      fact("Lifetime", el("span", null, when(x.added, true), " → ", x.removed ? when(x.removed) : "end of run"), x.reason || (a.end ? "No removal was logged; it ran until the application ended." : "Still running when the log ended.")),
      fact("Cores", num(x.cores), "Tasks it could run at once."),
      fact("Tasks", num(x.ok) + " succeeded of " + num(x.tasks), (x.failed ? num(x.failed) + " failed. " : "") + "Task time " + dur(x.dur) + ", GC " + pct(x.run ? x.gc / x.run : null) + " of run time."),
      fact("Peak memory", x.peakHeap ? bytes(x.peakHeap) + " heap of " + bytes(D.heapBytes) : "Not recorded", "Execution " + bytes(x.peakExec) + ", storage " + bytes(x.peakStorage) + (x.peakRss ? ", process RSS " + bytes(x.peakRss) : "") + "."),
      fact("Data", bytes(x.input) + " read, " + bytes(x.output) + " written", "Shuffle: " + bytes(x.shRead) + " read, " + bytes(x.shWrite) + " written. Spill to disk " + bytes(x.diskSpill) + ".")));
    var idx = D.execs.indexOf(id), cells = [];
    if (idx >= 0) stages.forEach(function (st) {
      var det = D.detail[st.key];
      if (!det) return;
      det.cells.forEach(function (r) { if (r[C.exec] === idx) { var o = {}; D.cellCols.forEach(function (c, i) { o[c] = r[i]; }); o.stage = st; cells.push(o); } });
    });
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
    if (x.src) s.appendChild(el("p", { cls: "srcref", text: "Added: " + src(x.src) }));
    return s;
  };

  views.sql = function () {
    var s = section("SQL / DataFrame", "Each query is one DataFrame action or SQL statement. Open one for its plan with row counts and time per operator.");
    s.appendChild(table({
      rows: queries, sort: 0, dir: "asc", filter: "Filter queries by ID, description or table",
      text: function (q) { return q.id + " " + q.desc + " " + q.reads.join(" ") + " " + q.writes.join(" "); },
      rowCls: function (q) { return q.error ? "failedrow" : null; },
      cols: [
        { h: "Query", num: true, v: function (q) { return q.id; }, f: function (q) { return link("#query/" + q.id, String(q.id)); } },
        { h: "Description", v: function (q) { return q.desc; }, f: function (q) { return q.desc; } },
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
    if (q.error) { s.appendChild(el("h3", { text: "Error" })); s.appendChild(el("pre", { cls: "plan", text: q.error })); }
    var g = D.graphs[String(q.id)];
    if (g && g.length) {
      s.appendChild(el("h3", { text: "Plan" }));
      s.appendChild(explain("The final physical plan (after adaptive re-planning). Data flows from the leaves up to the root. Metrics are totals across all tasks."));
      s.appendChild(el("div", { cls: "dagwrap", "data-plan": String(q.id), hidden: true }));
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
            var parts = (r.n.m || []).map(function (m) { var v = metricText(m); return v == null || v === "not recorded" || m[2] === 0 ? null : el("span", null, m[0] + " ", el("b", { text: v })); }).filter(Boolean);
            return el("span", { cls: "metricv" }, parts.length ? parts.map(function (p, i) { return [i ? " · " : "", p]; }) : "—");
          } }
        ]
      }));
    } else s.appendChild(explain(D.collected ? "No plan was kept for this query." : "Per-task detail was not collected for this run, so there is no plan graph."));
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
    if (window.SPDraw) window.SPDraw(main);
    drawCharts();
    window.scrollTo(0, 0);
  }
  function drawCharts() { if (window.SPCharts) window.SPCharts(charts); else charts.forEach(function (c) { var w = c.el.querySelector(".wait"); if (w) w.textContent = chartsState; }); }
  function redrawCharts() { if (window.SPCharts) window.SPCharts(charts); }
  var chartsState = "Charts are not available in this version.";
  window.SPRoute = route;
  window.SPChartsState = function (s) { chartsState = s; drawCharts(); };
  window.addEventListener("hashchange", route);
  route();
})();

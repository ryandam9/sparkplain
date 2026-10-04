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
  // foldAll is a pair of buttons that open or close every fold in scope.
  function foldAll(scope) {
    var set = function (open) { return function () { scope.querySelectorAll("details").forEach(function (d) { d.open = open; }); }; };
    return el("span", { cls: "foldall" }, el("button", { type: "button", text: "Open all", onclick: set(true) }), el("button", { type: "button", text: "Close all", onclick: set(false) }));
  }
  // Printing opens every fold, then puts them back.
  var shut = [];
  window.addEventListener("beforeprint", function () {
    shut = [].slice.call(document.querySelectorAll("#sp-main details:not([open])"));
    shut.forEach(function (d) { d.open = true; });
  });
  window.addEventListener("afterprint", function () { shut.forEach(function (d) { d.open = false; }); shut = []; });
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
    if ((ref || "").indexOf("node:") === 0) return "#cluster"; // the nodes
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
  // proseNodes renders a finding's text as the report does: each line a
  // paragraph, and lines that start with "- " a bulleted list.
  function proseNodes(text, cls) {
    var out = [], list = null;
    String(text || "").split("\n").forEach(function (line) {
      line = line.trim();
      if (line.indexOf("- ") === 0) {
        if (!list) { list = el("ul", { cls: cls }); out.push(list); }
        list.appendChild(el("li", { text: line.slice(2) }));
      } else {
        list = null;
        if (line) out.push(el("p", { cls: cls, text: line }));
      }
    });
    return out;
  }
  function explain(text) { return el("p", { cls: "explain", text: text }); }
  // bulletNote is an explanation as short bullet points, with an optional
  // heading: easier to scan than a paragraph.
  function bulletNote(title, items) {
    return el("div", { cls: "explain bullets" }, title ? el("h4", { cls: "bnh", text: title }) : null, el("ul", null, items.map(function (t) { return el("li", { text: t }); })));
  }

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
    kerberos: "Kerberos", metastore: "Metastore", hbase: "HBase", "hbase-use": "HBase table use", "hbase-server": "HBase server event", "hbase-scan": "HBase scan printed", localized: "Localized file", classpath: "Missing class", identity: "Identity", submit: "spark-submit command", submitted: "Submitted application",
    resource: "Uploaded file", "step-status": "Step status", "app-report": "YARN report", "app-summary": "YARN summary", bootstrap: "Bootstrap", error: "Error" };
  var FILE_KIND = { "container-stderr": "Container stderr", "container-stdout": "Container stdout", "step-controller": "Step controller", "step-stderr": "Step stderr", "step-stdout": "Step stdout",
    nodemanager: "NodeManager", resourcemanager: "ResourceManager", bootstrap: "Bootstrap log", "bootstrap-output": "Bootstrap action output",
    "hbase-master": "HBase Master", "hbase-regionserver": "HBase region server" };
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
  if ((D.hbaseTasks || []).length) TABS.push(["hbaseTasks", "HBase tasks", D.hbaseTasks.length + (D.hbaseTasksCut || 0)]);
  if (D.taskStories) TABS.push(["tasks", "Task stories", D.taskStories.tasks.length + (D.taskStories.cut || 0)]);
  if (D.taskStories && D.taskStories.tasks.length) TABS.splice(D.anatomy ? 3 : 2, 0, ["replay", "Replay"]);
  if (logFiles.length || (D.logSources || []).length) TABS.push(["logs", "Logs", logFiles.length]);
  var tabs = document.getElementById("sp-tabs");
  // Less-used tabs sit under More (a native disclosure, so it works from the
  // keyboard); the row keeps the ones a diagnosis starts from.
  var MORE = { storage: 1, code: 1, environment: 1, log: 1, logs: 1, hbaseTasks: 1, tasks: 1 };
  var tabLink = function (t) { return el("a", { href: "#" + t[0], "data-tab": t[0] }, t[1], t[2] != null ? el("span", { cls: "n", text: num(t[2]) }) : null); };
  TABS.filter(function (t) { return !MORE[t[0]]; }).forEach(function (t) { tabs.appendChild(tabLink(t)); });
  var moreTabs = TABS.filter(function (t) { return MORE[t[0]]; });
  var moreBox = null, moreLabel = null;
  if (moreTabs.length) {
    moreLabel = el("span", { text: "More" });
    moreBox = el("details", { cls: "tabmore" }, el("summary", null, moreLabel), el("div", { cls: "menu" }, moreTabs.map(tabLink)));
    moreBox.addEventListener("click", function (e) { if (e.target.closest("a")) moreBox.open = false; });
    document.addEventListener("click", function (e) { if (moreBox.open && !moreBox.contains(e.target)) moreBox.open = false; });
    moreBox.addEventListener("keydown", function (e) { if (e.key === "Escape") { moreBox.open = false; moreBox.querySelector("summary").focus(); } });
    tabs.appendChild(moreBox);
  }

  var views = {};
  views.overview = function () {
    var out = [];
    if ((D.accessGaps || []).length) {
      out.push(el("div", { cls: "missing access" }, el("h3", { text: "No access to " + D.accessGaps.length + (D.accessGaps.length === 1 ? " source" : " sources") + ": parts of this page are missing" }),
        el("ul", null, D.accessGaps.map(function (g) { return el("li", null, el("b", { text: g.source }), ": without it this page cannot show " + g.missing + ". It needs " + g.needs + "."); })),
        el("p", { text: "sparkplain carried on with everything else. The Logs tab lists each error." })));
    }
    var story = el("div", { cls: "story" }, el("h2", { text: "What happened" }));
    story.appendChild(el("ul", null, (D.summary || []).map(function (s) { return el("li", null, proseNodes(s, "s")); })));
    out.push(story);
    if (D.kpis && D.kpis.length) {
      out.push(el("div", { cls: "kpis" }, D.kpis.map(function (k) {
        return el("div", { cls: "kpi" }, el("span", { cls: "l", text: k.label }), el("span", { cls: "v" + (k.tone ? " " + k.tone : "") }, k.value, k.unit ? el("small", { text: " " + k.unit }) : null), el("span", { cls: "x", text: k.explain }));
      })));
    }
    var fs = section("Findings", D.findings.length ? "Problems and notes found in this run. Evidence links open the stage, job or executor it concerns." : "No findings for this run.");
    var list = el("div", { cls: "findings" });
    D.findings.forEach(function (f, i) {
      var sev = { critical: "crit", warning: "warn", info: "info" }[f.sev] || "info";
      // Critical and warning findings start open; info starts closed.
      list.appendChild(el("article", { cls: "finding " + sev, id: "finding-" + (i + 1) }, el("div", { cls: "stripe" }), el("details", { cls: "body", open: sev !== "info" },
        el("summary", { cls: "t" }, el("span", { cls: "fnum " + sev, text: String(i + 1) }), el("span", { cls: "pill " + ({ crit: "crit", warn: "part", info: "info" }[sev]), text: { crit: "Critical", warn: "Warning", info: "Info" }[sev] }), el("h3", { text: f.title })),
        el("div", { cls: "fbody" }, el("div", { cls: "fpart what" }, el("h4", { cls: "k", text: { crit: "Error", warn: "Problem", info: "Note" }[sev] }), el("div", null, proseNodes(f.expl, "expl"))),
        (f.ev || []).length ? el("div", { cls: "fpart evid" }, el("h4", { cls: "k", text: "Evidence" }), el("div", null, f.ev.map(function (e) {
          var h = refHref(e[1]), lh = logHref(e[2]);
          return el("div", { cls: "ev" }, h ? link(h, e[0]) : e[0], e[2] ? [" · ", lh ? link(lh, e[2]) : e[2]] : "");
        }))) : null,
        f.fix ? el("div", { cls: "fpart try" }, el("h4", { cls: "k", text: "Try" }), el("div", null, proseNodes(f.fix, "fix"))) : null))));
    });
    if (D.findings.length > 1) fs.appendChild(foldAll(list));
    fs.appendChild(list);
    out.push(fs);
    if (runningTasks.length) {
      var rs = section("Still running when the log ended", "These tasks started, but the log has no end for them. The application was still running when the log was copied, or it stopped without closing the log." + (D.runningCapped ? " More tasks were running than are listed." : ""));
      rs.appendChild(runningTable(runningTasks));
      out.push(rs);
    }
    if (stages.some(function (st) { return st.submitted && st.completed; })) {
      var hs = section("Stages worth a look", "The stages to look at first, ranked, each with its reason. The Stages tab has every stage and other views.");
      hs.appendChild(chartSlot("", "stageAttention:compact"));
      out.push(hs);
    }
    if (D.runPath) out.push(runPathSection());
    if ((D.resources || []).length) {
      // what the run used of what it had, one line each; no combined score
      var us = section("What the run used", "Each card is what the application used of what it had, and what that means. Where a bar has a green band, that is the healthy range.");
      var GROUPS = [["cpu", "CPU"], ["memory", "Memory"], ["stability", "Stability"]];
      us.appendChild(el("div", { cls: "uses" }, GROUPS.map(function (g) {
        var cards = D.resources.filter(function (u) { return u.group === g[0]; });
        if (!cards.length) return null;
        return el("div", { cls: "usegrp" }, el("h3", { text: g[1] }), cards.map(function (u) {
          var pc = function (v) { return (Math.max(0, Math.min(v, 1)) * 100).toFixed(1) + "%"; };
          var gauge = null;
          if (u.share >= 0) {
            var band = u.band && u.band.length === 2 ? u.band : null;
            gauge = el("div", { cls: "ugauge", role: "img", "aria-label": u.label + ": " + u.value + (band ? ", healthy from " + Math.round(band[0] * 100) + "% to " + Math.round(band[1] * 100) + "%" : "") },
              el("div", { cls: "utrack" },
                band ? el("i", { cls: "uband", style: "left:" + pc(band[0]) + ";width:" + pc(band[1] - band[0]) }) : null,
                el("i", { cls: "ufill", style: "width:" + (u.share > 0 ? pc(Math.max(u.share, 0.01)) : "0") })),
              el("div", { cls: "uscale" }, el("span", { text: "0" }), el("span", { text: "100%" })));
          }
          return el("div", { cls: "use" + (u.tone ? " " + u.tone : "") },
            el("div", { cls: "utop" }, el("span", { cls: "l", text: u.label }), u.verdict ? el("span", { cls: "chip " + (u.tone || ""), text: u.verdict }) : null),
            el("div", { cls: "v", text: u.value }),
            u.detail ? el("div", { cls: "d", text: u.detail }) : null,
            gauge,
            el("div", { cls: "x", text: u.explain }));
        }));
      })));
      out.push(us);
    }
    // the diagnosis first, then the charts over time
    var ov = section("Over time", "Tasks running across the run, and when each job ran.");
    ov.appendChild(chartSlot("", "running"));
    ov.appendChild(chartSlot("tall", "jobsTimeline"));
    ov.appendChild(chartSlot("", "dataOverTime"));
    out.push(ov);
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
  var EXCL_EXPLAIN = "When spark.excludeOnFailure.enabled is true and tasks fail on an executor (or a whole node), Spark stops scheduling tasks there. This applies for one stage or for the rest of the application, until spark.excludeOnFailure.timeout passes.";
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

  // ---------- what a stage read and wrote ----------
  // A stage's data rows: access, kind, name, format, parts, sized, bytes,
  // sources, line, files the SQL plan names.
  var DATA_CELL = 4;
  var DATA_EXPLAIN = "The folders and tables this stage's tasks read and wrote. A folder stands for the files in it and in its partition folders (such as year=2024). " +
    "A file split is a file, or a piece of a large file, that a task's log names as read. The size adds the length of each piece. For a columnar file such as Parquet, this is more than the task read, because it reads only the columns that it needs. " +
    "Files written are the files the tasks closed (EMRFS logs each one), and the size is the bytes they uploaded. \"At least\" means some files' sizes were not logged. " +
    "Known from says which sources name it: the executors' logs, the SQL plan, or the HBase section's scans and writes. A plan names what was read and written, but not how much.";
  function dataWhat(d) { return d[1] === "hbase" ? "HBase table " + d[2] : d[1] === "table" ? "table " + d[2] : d[2]; }
  function dataParts(d) {
    if (!d[4]) return "";
    var u = d[1] === "hbase" ? "region" : d[0] === "read" ? "file split" : "file";
    return num(d[4]) + " " + u + (d[4] === 1 ? "" : "s");
  }
  function dataSize(d) { return d[6] ? (d[5] < d[4] ? "at least " : "") + bytes(d[6]) : ""; }
  function dataLabel(d) {
    var f = [dataParts(d), dataSize(d)].filter(Boolean).join(", ");
    return (d[0] === "write" ? "wrote " : "read ") + dataWhat(d) + (f ? ": " + f : "");
  }
  function stageDataPanel(st) {
    var box = el("div");
    box.appendChild(el("h3", { text: "What it read and wrote" }));
    if (!st.data || !st.data.length) {
      box.appendChild(explain("No log or plan names a folder or table that this stage read or wrote. It is possible that it read only shuffle data, cached data or data that the code made. Or sparkplain did not read the logs of its executors."));
      return box;
    }
    box.appendChild(explain(DATA_EXPLAIN));
    box.appendChild(table({
      rows: st.data, page: 20,
      cols: [
        { h: "Did", v: function (d) { return d[0]; }, f: function (d) { return d[0] === "write" ? "Wrote" : "Read"; } },
        { h: "Folder or table", v: function (d) { return d[2]; }, f: function (d) {
          return el("span", null, el("span", { cls: "mono", text: dataWhat(d) }), d[9] && d[9].length ? el("span", { cls: "sub", text: "Files the SQL plan names: " + d[9].join(", ") }) : null); } },
        { h: "Format", v: function (d) { return d[3]; }, f: function (d) { return d[3] || "—"; } },
        { h: "Files or regions", num: true, title: "File splits read, files written, or HBase regions read, as the executors' logs name them", v: function (d) { return d[4]; }, f: function (d) { return dataParts(d) || "—"; } },
        { h: "Size", num: true, title: "For a read, the file splits' lengths added up; for a write, the bytes uploaded", v: function (d) { return d[6]; }, f: function (d) { return dataSize(d) || "—"; } },
        { h: "Known from", v: function (d) { return d[7].join(", "); }, f: function (d) { return d[7].join(", "); } },
        { h: "First line", v: function (d) { return src(d[8]); }, f: function (d) { return el("span", { cls: "srcref dl", text: src(d[8]) }); } }
      ]
    }));
    return box;
  }

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
        { h: "Your code", title: "The line of your code that ran the stage: its own call site, or, for a stage named after Spark's own code, its job's action", v: function (s) { return s.mine ? s.mine[0] + ":" + s.mine[1] : ""; },
          f: function (s) { if (!s.mine) return "—"; var h = codeHref(s.mine); return el("span", null, h ? link(h, codeLabel(s.mine)) : el("span", { cls: "mono", text: codeLabel(s.mine) }), s.mine[4] ? el("span", { cls: "sub", text: s.mine[4] }) : null); } },
        { h: "Reads / writes", title: "The folders and tables the stage read and wrote, from its executors' logs, its SQL plan and the HBase section", v: function (s) { return (s.data || []).map(function (d) { return d[2]; }).join(" "); },
          f: function (s) {
            if (!s.data || !s.data.length) return "—";
            return el("span", null, s.data.slice(0, DATA_CELL).map(function (d) { return el("span", { cls: "dl", title: d[2] + " (from " + d[7].join(", ") + ")", text: clip(dataLabel(d), 120) }); }),
              s.data.length > DATA_CELL ? el("span", { cls: "sub", text: "and " + (s.data.length - DATA_CELL) + " more on the stage's page" }) : null);
          } },
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
  // The Stages page's charts, one at a time; the choice stays while the page is open.
  var STAGE_VIEWS = [
    { id: "stageAttention", label: "Worth a look", title: "The stages worth a look, ranked, with the reason for each" },
    { id: "stageTimes", label: "Duration", title: "The longest stages" },
    { id: "stageData", label: "Data", title: "Data each stage moved" },
    { id: "stageSplit", label: "Time", title: "Where stage time went" },
    { id: "stageSkew", label: "Skew", title: "Which stages have straggler tasks" },
    { id: "stageSpill", label: "Spill", title: "Spill by stage", when: function () { return stages.some(function (st) { return st.diskSpill > 0 || st.memSpill > 0; }); } },
    // a scatter needs data on both axes: hidden when no stage handled any
    { id: "stageHealth", label: "Time vs data", title: "Stages placed by how long they ran against the data they handled, sized by tasks", when: function () { return stages.filter(function (st) { return stageMoved(st) > 0; }).length >= 2; } }
  ];
  var STAGE_VIEW = "stageAttention";
  views.stages = function () {
    var s = section("Stages", "A stage is a set of tasks that run the same code on different partitions of the data. Retried stages show each attempt.");
    // one chart at a time, chosen here; the table below stays
    var choices = STAGE_VIEWS.filter(function (v) { return !v.when || v.when(); });
    if (!choices.some(function (v) { return v.id === STAGE_VIEW; })) STAGE_VIEW = choices[0].id;
    var slot = chartSlot("", STAGE_VIEW), slotRec = charts[charts.length - 1];
    var picker = el("div", { cls: "viewpick", role: "group", "aria-label": "Stage chart" });
    choices.forEach(function (v) {
      var b = el("button", { type: "button", text: v.label, "aria-pressed": v.id === STAGE_VIEW ? "true" : "false", title: v.title });
      b.addEventListener("click", function () {
        STAGE_VIEW = v.id;
        picker.querySelectorAll("button").forEach(function (x) { x.setAttribute("aria-pressed", x === b ? "true" : "false"); });
        slotRec.draw = v.id;
        drawSlot(slotRec);
      });
      picker.appendChild(b);
    });
    s.appendChild(picker);
    s.appendChild(slot);
    s.appendChild(stageTable(stages));
    var fromLogs = Object.keys(D.hbaseScans || {}).filter(function (k) { return D.hbaseScans[k].fromLogs; });
    if (fromLogs.length) {
      s.appendChild(el("h3", { text: "HBase scans from the executors' logs" }));
      s.appendChild(explain("There is no event log, so these stages are known only from the split lines their tasks logged."));
      var ul = el("ul");
      fromLogs.forEach(function (k) { var x = D.hbaseScans[k]; ul.appendChild(el("li", null, link("#stage/" + k, "Stage " + x.stageId + (x.attempt ? " (attempt " + (x.attempt + 1) + ")" : "")), ": scan of " + x.table)); });
      s.appendChild(ul);
    }
    return s;
  };

  // views.hbaseTasks lists every task attempt that read an HBase region,
  // across all stages, as the report's HBase tasks table does, in full.
  views.hbaseTasks = function () {
    var tasks = D.hbaseTasks || [], sums = D.hbaseTaskStages || [], evs = D.hbaseRegionEvents || [];
    var s = section("HBase tasks", "Every task attempt that read an HBase region with TableInputFormat, across all stages, in stage, partition and attempt order. A retried task has a row per attempt.");
    s.appendChild(explain("The region comes from the split line that its executor logged. The stage and task come from the executor thread named on that line, or with the event log, from the scan's tie. Times run from the task's Running line to its Finished line, or come from the event log, which also has its rows. — means not known."));
    function msrc(o) { return o && o.file ? o.file + ":" + (o.line || "") : ""; }
    function host(h) { return h ? el("span", { cls: "mono", title: h, text: String(h).split(".")[0] }) : "—"; }
    function key(st, att) { return st + "." + (att || 0); }
    function stageCell(st, att) {
      if (st < 0) return "—";
      var label = String(st) + (att ? "." + att : "");
      return (D.hbaseScans || {})[key(st, att)] || stagesByID[st] ? link("#stage/" + key(st, att), label) : label;
    }
    var load = D.hbaseLoad || [];
    if (load.length) {
      s.appendChild(el("h3", { text: "Region server load over time" }));
      s.appendChild(chartSlot("", "hbaseLoad"));
      s.appendChild(table({
        rows: load, sort: 2,
        cols: [
          { h: "Region server", v: function (l) { return l.server; }, f: function (l) { return host(l.server); } },
          { h: "Tasks", num: true, v: function (l) { return l.tasks; }, f: function (l) { return num(l.tasks); } },
          { h: "Task time", num: true, v: function (l) { return l.taskMs; }, f: function (l) { return dur(l.taskMs); } },
          { h: "Busy for", num: true, v: function (l) { return l.busyMs; }, f: function (l) { return dur(l.busyMs); } },
          { h: "Most at once", num: true, v: function (l) { return l.peak; }, f: function (l) { return num(l.peak); } },
          { h: "When", v: function (l) { return l.peakAt || ""; }, f: function (l) { return when(l.peakAt ? Date.parse(l.peakAt) : 0); } },
          { h: "On average while busy", num: true, v: function (l) { return l.busyMs ? l.taskMs / l.busyMs : 0; }, f: function (l) { return l.busyMs ? (l.taskMs / l.busyMs).toFixed(1) + " tasks" : "—"; } },
          { h: "Served most of the scans running", v: function (l) { return l.hot ? Date.parse(l.hot.to) - Date.parse(l.hot.from) : -1; },
            f: function (l) { return l.hot ? el("span", null, when(Date.parse(l.hot.from)), " to ", when(Date.parse(l.hot.to)), " (" + dur(Date.parse(l.hot.to) - Date.parse(l.hot.from)) + "): up to " + num(l.hot.tasks) + " of " + num(l.hot.all)) : "—"; } }
        ]
      }));
      s.appendChild(explain("Busy for is the time when at least one scan task read from the server. On average while busy is its task time divided by that time. A stretch counts as serving most of the scans when it served at least hbase-hotspot-share of the running scans, and at least 4. That share is 75%, unless the config file sets it."));
    }
    s.appendChild(el("h3", { text: "By stage" }));
    s.appendChild(table({
      rows: sums, sort: 0, dir: "asc",
      cols: [
        { h: "Stage", num: true, v: function (r) { return r.stage < 0 ? 1e15 : r.stage * 1000 + r.stageAttempt; }, f: function (r) { return r.stage < 0 ? "not known" : stageCell(r.stage, r.stageAttempt); } },
        { h: "Tables", v: function (r) { return (r.tables || []).join(", "); }, f: function (r) { return el("span", { cls: "mono", text: (r.tables || []).join(", ") }); } },
        { h: "Task attempts", num: true, v: function (r) { return r.tasks; }, f: function (r) { return num(r.tasks); } },
        { h: "Failed", num: true, v: function (r) { return r.failed || 0; }, f: function (r) { return r.failed ? num(r.failed) : "—"; } },
        { h: "Regions", num: true, v: function (r) { return r.regions; }, f: function (r) { return num(r.regions); } },
        { h: "Region servers", num: true, v: function (r) { return r.servers; }, f: function (r) { return num(r.servers); } },
        { h: "First start", v: function (r) { return r.start || ""; }, f: function (r) { return when(r.start ? Date.parse(r.start) : 0); } },
        { h: "Last end", v: function (r) { return r.end || ""; }, f: function (r) { return when(r.end ? Date.parse(r.end) : 0); } },
        { h: "Ran for", num: true, v: function (r) { return span(Date.parse(r.start), Date.parse(r.end)) || -1; }, f: function (r) { var d = span(Date.parse(r.start), Date.parse(r.end)); return d != null && d >= 0 ? dur(d) : "—"; } }
      ]
    }));
    s.appendChild(el("h3", { text: "Every task" }));
    var rows = tasks.map(function (t, i) { return { i: i, t: t }; });
    s.appendChild(table({
      rows: rows, sort: 0, dir: "asc", page: 50, filter: "Filter by stage, table, row key, region, server or executor",
      cols: [
        { h: "Stage", num: true, v: function (r) { return r.i; }, f: function (r) { return stageCell(r.t.stage, r.t.stageAttempt); } },
        { h: "Task", num: true, v: function (r) { return r.t.partition; }, f: function (r) { return r.t.partition < 0 ? "—" : r.t.partition + "." + r.t.attempt; } },
        { h: "TID", num: true, v: function (r) { return r.t.taskId; }, f: function (r) { return r.t.taskId < 0 ? "—" : String(r.t.taskId); } },
        { h: "Table", v: function (r) { return r.t.table; }, f: function (r) { return el("span", { cls: "mono", text: r.t.table }); } },
        { h: "Start row", v: function (r) { return r.t.startRow; }, f: function (r) { return r.t.startRow ? el("span", { cls: "mono", text: r.t.startRow }) : el("span", { cls: "sub", text: "(first row)" }); } },
        { h: "End row", v: function (r) { return r.t.endRow; }, f: function (r) { return r.t.endRow ? el("span", { cls: "mono", text: r.t.endRow }) : el("span", { cls: "sub", text: "(last row)" }); } },
        { h: "Region", v: function (r) { return r.t.region; }, f: function (r) { return el("span", { cls: "mono", text: r.t.region }); } },
        { h: "Region server", v: function (r) { return r.t.server; }, f: function (r) { return host(r.t.server); } },
        { h: "Executor", v: function (r) { return r.t.executorId || ""; }, f: function (r) { return el("span", null, r.t.executorId || "—", r.t.host ? el("span", { cls: "sub", title: r.t.host, text: String(r.t.host).split(".")[0] }) : null); } },
        { h: "Started", v: function (r) { return r.t.start || ""; }, f: function (r) { return when(r.t.start ? Date.parse(r.t.start) : 0); } },
        { h: "Ended", v: function (r) { return r.t.end || ""; }, f: function (r) { return when(r.t.end ? Date.parse(r.t.end) : 0); } },
        { h: "Took", num: true, v: function (r) { return r.t.timed ? r.t.durationMs : -1; }, f: function (r) { return r.t.timed ? el("span", null, dur(r.t.durationMs), el("span", { cls: "sub", text: (r.t.slow ? "slow · " : "") + r.t.timeFrom })) : "—"; } },
        { h: "Outcome", v: function (r) { return r.t.outcome || ""; }, f: function (r) { return r.t.outcome || "—"; } },
        { h: "Estimated size", num: true, v: function (r) { return r.t.sizeBytes || 0; }, f: function (r) { return r.t.sizeBytes ? bytes(r.t.sizeBytes) : "—"; } },
        { h: "Rows", num: true, v: function (r) { return r.t.rowsKnown ? r.t.rows : -1; }, f: function (r) { return r.t.rowsKnown ? num(r.t.rows) : "—"; } },
        { h: "Its server logged", v: function (r) { return (r.t.events || []).length; }, f: function (r) {
            var es = (r.t.events || []).map(function (k) { return evs[k]; }).filter(Boolean);
            return es.length ? el("span", null, es.map(function (e) { return el("span", { cls: "sub", title: e.detail, text: e.event + (e.count > 1 ? " ×" + e.count : "") + (e.durationMs ? " " + dur(e.durationMs) : "") }); })) : "—"; } },
        { h: "Found in", v: function (r) { return msrc(r.t.source); }, f: function (r) { return el("span", { cls: "srcref" }, msrc(r.t.source), r.t.endSource ? el("br") : null, r.t.endSource ? msrc(r.t.endSource) : null, r.t.taskSource ? el("br") : null, r.t.taskSource ? msrc(r.t.taskSource) : null); } }
      ],
      text: function (r) { var t = r.t; return [t.stage < 0 ? "" : "stage " + t.stage, t.table, t.startRow, t.endRow, t.region, t.server, t.executorId || "", t.host || "", t.outcome || "", t.taskId < 0 ? "" : "TID " + t.taskId].join(" "); }
    }));
    if (D.hbaseTasksCut) s.appendChild(explain(num(D.hbaseTasksCut) + " more task attempts are left out to keep the page small; the JSON report lists them all."));
    if (evs.length) {
      s.appendChild(el("h3", { text: "What the region servers logged about the regions read" }));
      s.appendChild(explain("What HBase's servers logged about a region that the run read, while one of its tasks read it. This includes flushes, compactions, the region going offline (closed on one server, opened on another), and moves and splits that the Master ran. It also includes refused writes and slow calls. Events on the region of a slow task (at least twice its stage's median) come first."));
      s.appendChild(table({
        rows: evs.map(function (e, i) { return { i: i, e: e }; }), sort: 0, dir: "asc", page: 50, filter: "Filter by event, region, server or task",
        cols: [
          { h: "#", num: true, v: function (r) { return r.i; }, f: function (r) { return String(r.i + 1); } },
          { h: "When", v: function (r) { return r.e.time; }, f: function (r) { return el("span", null, r.e.count > 1 ? when(Date.parse(r.e.first)) : null, r.e.count > 1 ? " to " : null, when(Date.parse(r.e.time))); } },
          { h: "Event", v: function (r) { return r.e.event; }, f: function (r) { return r.e.event + (r.e.count > 1 ? " ×" + num(r.e.count) : ""); } },
          { h: "Region", v: function (r) { return r.e.region; }, f: function (r) { return el("span", { cls: "mono", text: r.e.region }); } },
          { h: "Server", v: function (r) { return r.e.host || ""; }, f: function (r) { return host(r.e.host); } },
          { h: "What it logged", v: function (r) { return r.e.detail || ""; }, f: function (r) { return r.e.detail || "—"; } },
          { h: "Took", num: true, v: function (r) { return r.e.durationMs || 0; }, f: function (r) { return r.e.durationMs ? dur(r.e.durationMs) : "—"; } },
          { h: "While these tasks read it", v: function (r) { return (r.e.slow || []).length; }, f: function (r) { return el("span", null, (r.e.tasks || []).join("; "), (r.e.slow || []).length ? el("span", { cls: "sub", text: "slow: " + r.e.slow.join("; ") }) : null); } },
          { h: "Found in", v: function (r) { return msrc(r.e.source); }, f: function (r) { return el("span", { cls: "srcref", text: msrc(r.e.source) }); } }
        ],
        text: function (r) { return [r.e.event, r.e.region, r.e.host || "", (r.e.tasks || []).join(" ")].join(" "); }
      }));
    }
    if (tasks.some(function (t) { return !t.rowsKnown; })) s.appendChild(explain("Rows are shown only for tasks the event log records (one successful attempt per partition); the executors' logs do not count rows."));
    return s;
  };

  // ---------- task stories ----------
  // What each task did, as its executor logged it: views.tasks lists every
  // task with its totals, views.task one task's steps in order. A step is
  // [ms since the task started, kind, bytes, n, ms, name, line, bytes over
  // the network, storage memory free].
  var TS = D.taskStories;
  var STEP = {
    start: function () { return "Started."; },
    broadcast: function (s) { return "Began reading " + s[5] + ": " + bytes(s[2]) + " in " + num(s[3]) + " piece" + (s[3] === 1 ? "" : "s") + ", as Spark estimated it."; },
    "broadcast-read": function (s) { return "Had " + s[5] + " after " + dur(s[4]) + "."; },
    shuffle: function (s) { return "Asked for " + num(s[3]) + " shuffle block" + (s[3] === 1 ? "" : "s") + " (" + bytes(s[2]) + ", Spark's estimate) from the previous stage's output: " + bytes(s[2] - (s[7] || 0)) + " on its own node, " + bytes(s[7] || 0) + " over the network."; },
    fetch: function (s) { return s[3] ? "Started " + num(s[3]) + " remote fetch" + (s[3] === 1 ? "" : "es") + " over the network in " + dur(s[4]) + "." : "Needed nothing over the network; set up in " + dur(s[4]) + "."; },
    spill: function (s) { return "Spilled " + bytes(s[2]) + " from memory to disk."; },
    cache: function (s) { return "Cached " + s[5] + " in memory (" + bytes(s[2]) + "); " + bytes(s[8] || 0) + " of storage memory free after."; },
    drop: function (s) { return "Dropped " + num(s[3]) + " cached block" + (s[3] === 1 ? "" : "s") + " from memory to make room; " + bytes(s[8] || 0) + " free after."; },
    "no-room": function (s) { return "Could not cache " + s[5] + ": not enough storage memory (" + bytes(s[2]) + " computed so far)."; },
    input: function (s) { return "Read " + s[5] + (s[2] ? " (" + bytes(s[2]) + " of file)" : "") + "."; },
    output: function (s) { return "Wrote " + s[5] + "."; },
    commit: function (s) { return "Committed its output in " + dur(s[4]) + "."; },
    problem: function (s) { return "Logged: " + s[5]; },
    end: function (s) { return "Finished, sending a " + bytes(s[2]) + " result back to the driver."; },
    "result-too-big": function (s) { return "Finished, but its " + bytes(s[2]) + " result was over spark.driver.maxResultSize and was dropped."; },
    failed: function () { return "Failed."; },
    killed: function () { return "Was killed."; }
  };
  function storyFile(t) { var x = TS.executors[t.file]; return x ? x.source.file : ""; }
  function storyRef(t, line) { return storyFile(t) + ":" + line; }
  function storyTook(t) { if (!t.start || !t.end) return -1; return Date.parse(t.end) - Date.parse(t.start); }
  function storyStage(t) { return t.stage < 0 ? "—" : stagesByID[t.stage] ? link("#stage/" + t.stage + "." + t.stageAttempt, String(t.stage) + (t.stageAttempt ? "." + t.stageAttempt : "")) : String(t.stage); }
  function storyFacts(t, title, one) {
    // one task: only what it did
    function has(n) { return !one || !!n; }
    return el("div", { cls: "facts" },
      has(t.broadcasts) && fact("Broadcasts read", num(t.broadcasts || 0) + (t.broadcasts ? " · " + bytes(t.broadcastBytes) + " · " + dur(t.broadcastMs || 0) : ""), "Variables the driver shared with the executors, read once per executor by the first task to need them. Sizes are Spark's estimates."),
      has(t.shuffleBlocks) && fact("Shuffle blocks read", num(t.shuffleBlocks || 0) + (t.shuffleBlocks ? " · " + bytes(t.shuffleBytes) : ""), bytes(t.shuffleLocalBytes || 0) + " from " + title + " own node and " + bytes(t.shuffleRemoteBytes || 0) + " over the network (" + num(t.remoteBlocks || 0) + " blocks in " + num(t.remoteFetches || 0) + " requests). Sizes are Spark's estimates from the map outputs, within a few percent."),
      has(t.inputs) && fact("Input read", num(t.inputs || 0) + (t.inputBytes ? " · " + bytes(t.inputBytes) : ""), "Files, file ranges or HBase regions opened; the size adds up file ranges, not bytes read."),
      has(t.cachedBlocks || t.dropped || t.notCached) && fact("Cached in memory", num(t.cachedBlocks || 0) + (t.cachedBlocks ? " · " + bytes(t.cachedBytes) : ""), "Cached partitions stored in executor memory." + (t.dropped ? " " + num(t.dropped) + " blocks dropped to make room." : "") + (t.notCached ? " " + num(t.notCached) + " did not fit." : "")),
      has(t.spills) && fact("Spilled to disk", t.spills ? num(t.spills) + " times · " + bytes(t.spillBytes) : "none", "Data a sort or aggregation could not keep in memory, as its in-memory size."),
      has(t.outputs) && fact("Files written", num(t.outputs || 0) + (t.outputBytes ? " · " + bytes(t.outputBytes) : ""), "Files closed after writing (EMRFS logs each one); the size is the bytes uploaded for them."),
      has(t.commits) && fact("Output committed", num(t.commits || 0) + (t.commits ? " · " + dur(t.commitMs) : ""), "Task outputs moved into place at the end of a write."),
      has(t.resultBytes) && fact("Results sent back", bytes(t.resultBytes || 0), "What the tasks returned to the driver, serialized."),
      has(t.warnings || t.errors) && fact("Warnings and errors", num(t.warnings || 0) + " · " + num(t.errors || 0), "Lines logged at WARN and ERROR as a task's, besides its end."));
  }
  views.tasks = function () {
    var s = section("Task stories", "What each task did, as its executor logged it. This includes the broadcast variables it read and the shuffle blocks it fetched, from its own node or over the network. It also includes the files or HBase regions it read, the blocks it cached and the data it spilled to disk. Last are the files it wrote, the output it committed and the result it sent back.");
    s.appendChild(explain(TS.byThread && !TS.byTid ? "Every line is told apart by the executor thread that logged it, which names its task." :
      "Lines are told apart by the task they name (TID) and, otherwise, by being the only task their executor was running" + (TS.byThread ? "; " + num(TS.byThread) + " tasks by the thread that logged them" : "") + ". Lines no task could be found for are counted per executor."));
    s.appendChild(storyFacts(TS.totals, "the task's"));
    var F = D.flows;
    if (F) {
      s.appendChild(el("h3", { text: "Data moved and memory over time" }));
      s.appendChild(explain("The shuffle data that each task asked for, from its own node and over the network. It also shows what tasks spilled and cached, and how much storage memory each executor had left after each block that it cached or dropped. Shuffle sizes are Spark's estimates from the map outputs. The event log has the exact bytes."));
      if ((F.local || []).length) s.appendChild(chartSlot("", "flows"));
      if (F.executors.some(function (e) { return (e.free || []).length; })) s.appendChild(chartSlot("", "storage"));
      if ((F.broadcasts || []).length) {
        s.appendChild(el("h3", { text: "Broadcast variables read" }));
        s.appendChild(table({
          rows: F.broadcasts, sort: 1,
          cols: [
            { h: "Broadcast", v: function (b) { return b.name; }, f: function (b) { return b.name; } },
            { h: "Size", num: true, v: function (b) { return b.bytes; }, f: function (b) { return bytes(b.bytes); } },
            { h: "Pieces", num: true, v: function (b) { return b.pieces; }, f: function (b) { return num(b.pieces); } },
            { h: "Executors", num: true, v: function (b) { return b.executors; }, f: function (b) { return num(b.executors); } },
            { h: "Read time, all executors", num: true, v: function (b) { return b.readMs; }, f: function (b) { return dur(b.readMs); } },
            { h: "Slowest read", num: true, v: function (b) { return b.maxMs; }, f: function (b) { return el("span", null, dur(b.maxMs), b.slowestOn ? el("span", { cls: "sub", text: "executor " + b.slowestOn }) : null); } },
            { h: "First read", v: function (b) { return b.first; }, f: function (b) { return when(Date.parse(b.first)); } },
            { h: "Found in", v: function (b) { return b.source.file; }, f: function (b) { return el("span", { cls: "srcref", text: b.source.file + ":" + b.source.line }); } }
          ]
        }));
      }
      if ((F.spills || []).length) {
        s.appendChild(el("h3", { text: "Spills by stage" }));
        s.appendChild(table({
          rows: F.spills, sort: 3,
          cols: [
            { h: "Stage", num: true, v: function (x) { return x.stage; }, f: function (x) { return x.stage < 0 ? "lines naming no task" : storyStage({ stage: x.stage, stageAttempt: x.attempt }); } },
            { h: "Tasks that spilled", num: true, v: function (x) { return x.tasks; }, f: function (x) { return x.tasks ? num(x.tasks) : "—"; } },
            { h: "Spills", num: true, v: function (x) { return x.spills; }, f: function (x) { return num(x.spills); } },
            { h: "Spilled", num: true, v: function (x) { return x.bytes; }, f: function (x) { return bytes(x.bytes); } },
            { h: "Most by one task", num: true, v: function (x) { return x.mostBytes; }, f: function (x) { return x.mostBytes ? el("span", null, bytes(x.mostBytes), " ", link("#task/" + x.mostTask, "TID " + x.mostTask)) : "—"; } }
          ]
        }));
      }
    }
    s.appendChild(el("h3", { text: "Per executor" }));
    s.appendChild(table({
      rows: TS.executors, sort: 1,
      cols: [
        { h: "Executor", v: function (x) { return x.executor; }, f: function (x) { return el("span", null, execByID[x.executor] ? execLink(x.executor) : x.executor, x.host ? el("span", { cls: "sub", title: x.host, text: String(x.host).split(".")[0] }) : null); } },
        { h: "Tasks", num: true, v: function (x) { return x.tasks; }, f: function (x) { return num(x.tasks); } },
        { h: "Broadcasts", num: true, v: function (x) { return x.totals.broadcastBytes || 0; }, f: function (x) { return x.totals.broadcasts ? el("span", null, num(x.totals.broadcasts), el("span", { cls: "sub", text: bytes(x.totals.broadcastBytes) + " · " + dur(x.totals.broadcastMs || 0) })) : "—"; } },
        { h: "Shuffle, own node", num: true, v: function (x) { return x.totals.shuffleLocalBytes || 0; }, f: function (x) { return x.totals.shuffleReads ? bytes(x.totals.shuffleLocalBytes || 0) : "—"; } },
        { h: "Shuffle, network", num: true, v: function (x) { return x.totals.shuffleRemoteBytes || 0; }, f: function (x) { return x.totals.shuffleReads ? el("span", null, bytes(x.totals.shuffleRemoteBytes || 0), el("span", { cls: "sub", text: num(x.totals.remoteBlocks || 0) + " blocks" })) : "—"; } },
        { h: "Input", num: true, v: function (x) { return x.totals.inputBytes || 0; }, f: function (x) { return x.totals.inputs ? el("span", null, num(x.totals.inputs), x.totals.inputBytes ? el("span", { cls: "sub", text: bytes(x.totals.inputBytes) }) : null) : "—"; } },
        { h: "Cached", num: true, v: function (x) { return x.totals.cachedBytes || 0; }, f: function (x) { return x.totals.cachedBlocks ? bytes(x.totals.cachedBytes) : "—"; } },
        { h: "Spilled", num: true, v: function (x) { return x.totals.spillBytes || 0; }, f: function (x) { return x.totals.spills ? bytes(x.totals.spillBytes) : "—"; } },
        { h: "Commits", num: true, v: function (x) { return x.totals.commitMs || 0; }, f: function (x) { return x.totals.commits ? el("span", null, num(x.totals.commits), el("span", { cls: "sub", text: dur(x.totals.commitMs) })) : "—"; } },
        { h: "Results", num: true, v: function (x) { return x.totals.resultBytes || 0; }, f: function (x) { return bytes(x.totals.resultBytes || 0); } },
        { h: "Lines not tied to a task", num: true, v: function (x) { return x.untied.lines || 0; }, f: function (x) { return x.untied.lines ? num(x.untied.lines) : "—"; } },
        { h: "Log", v: function (x) { return x.source.file; }, f: function (x) { return el("span", { cls: "srcref", text: x.source.file }); } }
      ]
    }));
    s.appendChild(el("h3", { text: "Every task" }));
    s.appendChild(table({
      rows: TS.tasks, sort: 4, dir: "asc", page: 50, filter: "Filter by stage, TID, executor, outcome or what it read",
      cols: [
        { h: "Stage", num: true, v: function (t) { return t.stage * 1000 + t.stageAttempt; }, f: storyStage },
        { h: "Task", num: true, v: function (t) { return t.partition; }, f: function (t) { return t.partition < 0 ? "—" : t.partition + "." + t.attempt; } },
        { h: "TID", num: true, v: function (t) { return t.taskId; }, f: function (t) { return link("#task/" + t.taskId, String(t.taskId)); } },
        { h: "Executor", v: function (t) { return t.executor || ""; }, f: function (t) { return t.executor || "—"; } },
        { h: "Started", v: function (t) { return t.start || ""; }, f: function (t) { return when(t.start ? Date.parse(t.start) : 0); } },
        { h: "Took", num: true, v: storyTook, f: function (t) { var d = storyTook(t); return d < 0 ? "—" : d === 0 ? "under 1 s" : dur(d); } },
        { h: "Outcome", v: function (t) { return t.outcome || ""; }, f: function (t) { return t.outcome || "no end logged"; } },
        { h: "Broadcasts", num: true, v: function (t) { return t.broadcastBytes || 0; }, f: function (t) { return t.broadcasts ? bytes(t.broadcastBytes) : "—"; } },
        { h: "Shuffle, own node", num: true, v: function (t) { return t.shuffleLocalBytes || 0; }, f: function (t) { return t.shuffleReads ? bytes(t.shuffleLocalBytes || 0) : "—"; } },
        { h: "Shuffle, network", num: true, v: function (t) { return t.shuffleRemoteBytes || 0; }, f: function (t) { return t.shuffleReads ? bytes(t.shuffleRemoteBytes || 0) : "—"; } },
        { h: "Input", v: function (t) { return t.input || ""; }, f: function (t) { return t.inputs ? el("span", null, el("span", { cls: "mono", text: t.input }), t.inputs > 1 ? el("span", { cls: "sub", text: "and " + num(t.inputs - 1) + " more" }) : null, t.inputBytes ? el("span", { cls: "sub", text: bytes(t.inputBytes) }) : null) : "—"; } },
        { h: "Cached", num: true, v: function (t) { return t.cachedBytes || 0; }, f: function (t) { return t.cachedBlocks ? bytes(t.cachedBytes) : "—"; } },
        { h: "Spilled", num: true, v: function (t) { return t.spillBytes || 0; }, f: function (t) { return t.spills ? bytes(t.spillBytes) : "—"; } },
        { h: "Commit", num: true, v: function (t) { return t.commitMs || 0; }, f: function (t) { return t.commits ? dur(t.commitMs) : "—"; } },
        { h: "Result", num: true, v: function (t) { return t.resultBytes || 0; }, f: function (t) { return t.resultBytes ? bytes(t.resultBytes) : "—"; } },
        { h: "What it said", v: function (t) { return t.error || t.problem || ""; }, f: function (t) { return t.error || t.problem || "—"; } }
      ],
      text: function (t) { return ["stage " + t.stage, "TID " + t.taskId, t.executor || "", t.outcome || "", t.input || "", t.error || "", t.problem || ""].join(" "); }
    }));
    if (TS.cut) s.appendChild(explain(num(TS.cut) + " more task stories are left out to keep the page small; the JSON report lists them all."));
    if ((TS.missing || []).length) s.appendChild(el("div", { cls: "missing" }, el("h3", { text: "Not shown" }), el("ul", null, TS.missing.map(function (m) { return el("li", { text: m }); }))));
    return s;
  };
  var storyByTID = null;
  views.task = function (arg) {
    if (!storyByTID) { storyByTID = {}; TS.tasks.forEach(function (t) { storyByTID[t.taskId] = t; }); }
    var t = storyByTID[arg];
    if (!t) return section("Task " + arg, "This task has no story: its executor's log was not read, or it is past the page's cap (the JSON report lists every task).");
    var s = section("Task " + (t.partition < 0 ? "" : t.partition + "." + t.attempt + " ") + "(TID " + t.taskId + ")" + (t.stage < 0 ? "" : " in stage " + t.stage + (t.stageAttempt ? "." + t.stageAttempt : "")),
      "What this task did, in order, as executor " + (t.executor || "?") + " logged it" + (t.host ? " on " + String(t.host).split(".")[0] : "") + ".");
    s.insertBefore(el("div", { cls: "crumbs" }, link("#tasks", "Task stories"), " / TID " + t.taskId), s.firstChild);
    var d = storyTook(t);
    s.appendChild(el("div", { cls: "facts" },
      fact("Stage", storyStage(t), "The stage this task ran a partition of."),
      fact("Executor", t.executor && execByID[t.executor] ? execLink(t.executor) : t.executor || "—", t.host || ""),
      fact("Ran", t.start ? el("span", null, when(Date.parse(t.start)), t.end ? " to " : "", t.end ? when(Date.parse(t.end)) : "") : "—", d < 0 ? "Its end is not in the log." : "Took " + (d === 0 ? "under 1 s" : dur(d)) + ", from its Running line to its end."),
      fact("Outcome", t.outcome || "no end logged", t.error || (t.resultVia === "BlockManager" ? "Its result was large, so it went through the block manager." : ""))));
    s.appendChild(storyFacts(t, "the task's", true));
    s.appendChild(el("h3", { text: "What it did" }));
    s.appendChild(table({
      rows: (t.s || []).map(function (st, i) { return { i: i, st: st }; }), sort: 0, dir: "asc",
      cols: [
        { h: "After", num: true, v: function (r) { return r.i; }, f: function (r) { return "+" + dur(r.st[0]); } },
        { h: "What it did", v: function (r) { return r.st[1]; }, f: function (r) { var w = STEP[r.st[1]]; return w ? w(r.st) : r.st[1]; } },
        { h: "Found in", v: function (r) { return r.st[6]; }, f: function (r) { return el("span", { cls: "srcref", text: storyRef(t, r.st[6]) }); } }
      ]
    }));
    if (t.cutSteps) s.appendChild(explain(num(t.cutSteps) + " more steps are not kept; its totals above count them."));
    if (t.tiedBy === "tid") s.appendChild(explain("This executor's log prints no thread. A line that does not name its task belongs to a task only when that task was the only one running on the executor. As a result, the totals can miss lines that the executor logged while other tasks ran."));
    return s;
  };

  // ---------- replay ----------
  // views.replay plays the run back from its logs: each executor's task
  // slots filling with the tasks it ran (coloured by stage), the jobs and
  // stages running on the driver, each executor's shuffle reads over the
  // network and storage memory left, and a ticker telling each event in
  // words. Everything comes from the task stories and the flows, at the
  // times the logs give (whole seconds in Spark's default layout).
  var RP = null;
  // What the "What happened" list can show: its value, label and levels.
  var RP_SHOW = [["tasks", "Jobs, stages and tasks", ["run", "task"]], ["run", "Jobs and stages only", ["run"]], ["all", "Everything, step by step", ["run", "task", "step"]]];
  var RP_TAG = { job: "Job", stage: "Stage", start: "Started", end: "Finished", failed: "Failed", killed: "Killed", broadcast: "Broadcast", "broadcast-read": "Broadcast",
    shuffle: "Shuffle", spill: "Spill", cache: "Cache", drop: "Evicted", "no-room": "No room", input: "Read", output: "Wrote", commit: "Commit", problem: "Logged" };
  var RP_VERB = { start: "started", end: "finished", failed: "failed", killed: "were killed" };
  var RP_STAGES = 8, RP_JOBS = 5, RP_EVENTS = 14;
  function replayModel() {
    if (RP) return RP;
    var ts = TS.tasks.filter(function (t) { return t.start; }).map(function (t) {
      var s = Date.parse(t.start), e = t.end ? Date.parse(t.end) : null;
      return { task: t, s: s, e: e, ex: t.executor || "?" };
    }).sort(function (a2, b2) { return a2.s - b2.s || a2.task.taskId - b2.task.taskId; });
    var t0 = a.start || (ts.length ? ts[0].s : 0), t1 = a.end || 0;
    ts.forEach(function (x) { t0 = Math.min(t0, x.s); t1 = Math.max(t1, x.e || x.s); });
    ts.forEach(function (x) { if (x.e == null) x.e = t1; x.end = x.e; });
    // executors, by host, each with a slot per core (more when it ran more at once)
    var ex = {}, order = [];
    function execOf(id) {
      if (!ex[id]) {
        var info = execByID[id] || {}, fx = (TS.executors || []).filter(function (e) { return e.executor === id; })[0] || {};
        ex[id] = { id: id, host: info.host || fx.host || "", cores: info.cores || 0, slots: [], added: info.added || null, removed: info.removed || null };
        order.push(id);
      }
      return ex[id];
    }
    // a slot is free from the logged end of its last task; a task within
    // one logged second is then drawn for 400 ms so it shows
    ts.forEach(function (x) {
      var e = execOf(x.ex), k = 0;
      while (k < e.slots.length && e.slots[k] > x.s) k++;
      e.slots[k] = x.end;
      x.slot = k;
      if (x.e <= x.s) x.e = x.s + 400;
    });
    order.forEach(function (id) { var e = ex[id]; e.nslots = Math.max(e.cores, e.slots.length, 1); });
    order.sort(function (p, q) { var x = +p, y = +q; return isNaN(x) || isNaN(y) ? String(p).localeCompare(String(q)) : x - y; });
    // what happened, in words, in time order. Jobs and stages are events
    // of their own ("run"); a task's start and end are grouped with the
    // others of its stage that did the same in the same second ("task");
    // what a task did in between is a "step", one event each.
    var evs = [], groups = {};
    function who(x) { return "TID " + x.task.taskId + (x.task.stage >= 0 ? " (stage " + x.task.stage + ", partition " + x.task.partition + ")" : "") + " on executor " + x.ex; }
    function group(t, kind, x, text) {
      var k = Math.floor(t / 1000) + "|" + kind + "|" + x.task.stage + "." + x.task.stageAttempt, g = groups[k];
      if (!g) { g = groups[k] = { at: t, kind: kind, level: "task", stage: x.task.stage, attempt: x.task.stageAttempt, members: [], execs: [] }; evs.push(g); }
      g.at = Math.min(g.at, t);
      g.members.push({ at: t, tid: x.task.taskId, text: text });
      if (g.execs.indexOf(x.ex) < 0) g.execs.push(x.ex);
    }
    var SAY = {
      broadcast: function (s) { return "began reading " + s[5] + " (" + bytes(s[2]) + ")"; },
      "broadcast-read": function (s) { return "had " + s[5] + " after " + dur(s[4]); },
      shuffle: function (s) { return "fetched " + num(s[3]) + " shuffle blocks: " + bytes(s[2] - (s[7] || 0)) + " on its node, " + bytes(s[7] || 0) + " over the network"; },
      spill: function (s) { return "spilled " + bytes(s[2]) + " to disk"; },
      cache: function (s) { return "cached " + s[5] + " (" + bytes(s[2]) + "; " + bytes(s[8] || 0) + " storage free)"; },
      drop: function (s) { return "dropped " + num(s[3]) + " cached blocks to make room"; },
      "no-room": function (s) { return "could not cache " + s[5]; },
      input: function (s) { return "read " + s[5]; },
      output: function (s) { return "wrote " + s[5]; },
      commit: function (s) { return "committed its output in " + dur(s[4]); },
      problem: function (s) { return "logged: " + s[5]; }
    };
    ts.forEach(function (x) {
      group(x.s, "start", x, who(x) + " started");
      (x.task.s || []).forEach(function (st) {
        if (SAY[st[1]]) evs.push({ at: x.s + st[0], kind: st[1], level: "step", stage: x.task.stage, text: who(x) + " " + SAY[st[1]](st) });
      });
      var o = x.task.outcome;
      if (x.task.end) group(x.e, o === "failed" || o === "killed" ? o : "end", x, who(x) + (o === "failed" ? " failed" + (x.task.error ? ": " + x.task.error : "") : o === "killed" ? " was killed" : " finished" + (x.task.resultBytes ? ", sending " + bytes(x.task.resultBytes) + " back" : "")));
    });
    jobs.forEach(function (j) {
      if (j.submitted) evs.push({ at: j.submitted, kind: "job", level: "run", text: "Job " + j.id + " started: " + (j.desc || j.name || "") });
      if (j.completed) evs.push({ at: j.completed, kind: "job", level: "run", bad: j.status === "failed", text: "Job " + j.id + (j.status === "failed" ? " failed" : " finished") });
    });
    stages.forEach(function (st) {
      var n = "Stage " + st.id + (st.attempt ? "." + st.attempt : "");
      if (st.submitted) evs.push({ at: st.submitted, kind: "stage", level: "run", stage: st.id, text: n + " started: " + num(st.numTasks) + " tasks (" + st.name + ")" });
      if (st.completed) evs.push({ at: st.completed, kind: "stage", level: "run", stage: st.id, bad: st.status === "failed", text: n + (st.status === "failed" ? " failed" : " finished") });
    });
    evs.sort(function (p, q) { return p.at - q.at; });
    evs.forEach(function (e) { if (e.members) e.members.sort(function (p, q) { return p.at - q.at || p.tid - q.tid; }); });
    // each view of the list (RP_SHOW) numbers its own events, 1 first
    var shown = {};
    RP_SHOW.forEach(function (m) { shown[m[0]] = []; });
    evs.forEach(function (e, i) {
      RP_SHOW.forEach(function (m) { if (m[2].indexOf(e.level) >= 0) shown[m[0]].push(i); });
    });
    RP = { ts: ts, t0: t0, t1: Math.max(t1, t0 + 1000), ex: ex, order: order, evs: evs, shown: shown };
    return RP;
  }
  function stageColor(stage) { return stage < 0 ? V.neutral : V.viz[stage % V.viz.length]; }
  function seriesAt(ps, t) { // the last point at or before t
    var lo = 0, hi = (ps || []).length - 1, best = null;
    while (lo <= hi) { var mid = (lo + hi) >> 1; if (Date.parse(ps[mid].t) <= t) { best = ps[mid]; lo = mid + 1; } else hi = mid - 1; }
    return best;
  }
  views.replay = function () {
    var M = replayModel();
    var s = section("Replay", "Watch the run again, step by step, from its logs.");
    s.appendChild(bulletNote("What you see", [
      "Executors (right): each box is one slot where a task can run. A coloured box means a task is running there.",
      "Driver (left): the jobs and stages running at that moment.",
      "Events (left): the latest events, as a numbered list.",
      "Press Play, or drag the slider, to move through the run."
    ]));
    s.appendChild(bulletNote("Good to know", [
      "Times come from the logs" + (TS.byThread ? "." : ", which record whole seconds only. A task shorter than a second flashes by."),
      "The logs do not say which core ran a task, so a task is shown in the first free box.",
      num(M.ts.length) + " tasks on " + num(M.order.length) + " executor" + (M.order.length === 1 ? "" : "s") + (TS.cut ? ". " + num(TS.cut) + " more tasks are not shown (too many for the page)" : "") + "."
    ]));
    var span0 = M.t1 - M.t0;
    var state = { now: M.t0, playing: false, last: 0 };
    var playBtn = el("button", { cls: "more", type: "button", text: "Play" });
    var restart = el("button", { cls: "more", type: "button", text: "Restart" });
    var stepBtn = el("button", { cls: "more", type: "button", text: "Next event" });
    var speed = el("select", { "aria-label": "Playback speed" });
    [["fit60", "The run in 1 minute"], ["fit20", "The run in 20 seconds"], ["fit180", "The run in 3 minutes"], ["1", "Real time"], ["10", "10× real time"], ["100", "100× real time"]].forEach(function (o) { speed.appendChild(el("option", { value: o[0], text: o[1] })); });
    var scrub = el("input", { type: "range", min: "0", max: "1000", value: "0", "aria-label": "Replay position", cls: "rp-scrub" });
    var clock = el("span", { cls: "count" });
    s.appendChild(el("div", { cls: "bar-tools" }, playBtn, restart, stepBtn, speed, clock));
    s.appendChild(scrub);
    var show = el("select", { "aria-label": "What the list shows" });
    RP_SHOW.forEach(function (o) { show.appendChild(el("option", { value: o[0], text: o[1] })); });
    var stage = el("div", { cls: "rp" });
    var driver = el("div", { cls: "rp-driver" });
    var legend = el("div", { cls: "rp-legend" });
    var grid = el("div", { cls: "rp-grid" });
    var ticker = el("ol", { cls: "rp-ticker", "aria-live": "off" });
    stage.appendChild(el("div", { cls: "rp-col" }, el("h3", { text: "Driver" }), driver,
      el("div", { cls: "rp-head" }, el("h3", { text: "What happened" }), show),
      bulletNote(null, ["Newest at the top.", "Tasks of the same stage that started or finished in the same second are grouped on one line. Click the line to see each task."]), ticker));
    stage.appendChild(el("div", { cls: "rp-col" }, el("h3", { text: "Executors" }), legend, grid,
      bulletNote("How to read an executor box", [
        "Each small box is one task slot. Empty means nothing is running there.",
        "\"p12\" means partition 12 (a partition is one slice of the data; each task works on one).",
        "The colour shows the stage; the legend above names it. There are only " + V.viz.length + " colours, so two stages can share one. Hover over a box to see its stage and task.",
        "Network in: data this executor pulled from other machines in the last " + dur((D.flows || {}).stepMs || 0) + ".",
        "Storage free: memory this executor still had for saved (cached) data.",
        "The grey bars are amounts, not stages."
      ])));
    s.appendChild(stage);
    // executor cards, side by side, in executor order, each naming its host
    var cards = {};
    var flowEx = {};
    ((D.flows || {}).executors || []).forEach(function (e) {
      var maxFree = 0, maxRemote = 0;
      (e.free || []).forEach(function (p) { maxFree = Math.max(maxFree, p.v); });
      (e.remote || []).forEach(function (p) { maxRemote = Math.max(maxRemote, p.v); });
      flowEx[e.executor] = { e: e, maxFree: maxFree, maxRemote: maxRemote };
    });
    M.order.forEach(function (id) {
      var e = M.ex[id], h = e.host || "unknown host";
      var slots = [];
      var row = el("div", { cls: "rp-slots", style: "grid-template-columns:repeat(" + Math.min(e.nslots, 6) + ",minmax(0,1fr))" });
      for (var k = 0; k < e.nslots; k++) { var sl = el("div", { cls: "rp-slot" }); slots.push(sl); row.appendChild(sl); }
      var net = el("div", { cls: "rp-bar gauge" }, el("span")), mem = el("div", { cls: "rp-bar gauge" }, el("span"));
      var netTxt = el("span", { cls: "rp-val" }), memTxt = el("span", { cls: "rp-val" });
      var card = el("div", { cls: "rp-exec" },
        el("div", { cls: "rp-exhead" }, el("span", null, "Executor ", execByID[id] ? execLink(id) : String(id)), el("span", { cls: "sub", title: h, text: (e.cores ? e.cores + " cores" : num(e.nslots) + " at once") + " · " + String(h).split(".")[0] })), row,
        flowEx[id] && flowEx[id].maxRemote ? el("div", { cls: "rp-gauge" }, el("span", { text: "Network in" }), net, netTxt) : null,
        flowEx[id] && flowEx[id].maxFree ? el("div", { cls: "rp-gauge" }, el("span", { text: "Storage free" }), mem, memTxt) : null);
      cards[id] = { card: card, slots: slots, net: net.firstChild, mem: mem.firstChild, netTxt: netTxt, memTxt: memTxt };
      grid.appendChild(card);
    });
    function rpStageName(id) { var st = (stagesByID[id] || [])[0]; return st ? st.name : ""; }
    function dot(stageID) { return el("span", { cls: "rp-dot", style: "background:" + stageColor(stageID) }); }
    function plural(n, one, many) { return num(n) + " " + (n === 1 ? one : many); }
    // a task's line: its TID linked to its page, then what it did
    function member(m) { var id = "TID " + m.tid; return el("span", null, link("#task/" + m.tid, id), m.text.slice(id.length)); }
    // one line of the list: a job or stage, a step, or a group of tasks,
    // counting only its tasks up to now
    function eventItem(n, e, t) {
      var body;
      if (e.members) {
        var ms = e.members.filter(function (m) { return m.at <= t; });
        var of = e.stage >= 0 ? " of stage " + e.stage + (e.attempt ? "." + e.attempt : "") : "";
        if (ms.length === 1) body = el("span", null, dot(e.stage), member(ms[0]));
        else {
          var execs = e.execs.length > 3 ? plural(e.execs.length, "executor", "executors") : (e.execs.length === 1 ? "executor " : "executors ") + e.execs.join(", ");
          body = el("details", null, el("summary", null, dot(e.stage), plural(ms.length, "task", "tasks") + of + " " + RP_VERB[e.kind] + " on " + execs),
            el("ul", { cls: "rp-members" }, ms.map(function (m) { return el("li", null, member(m)); })));
        }
      } else body = el("span", null, e.stage != null && e.stage >= 0 ? dot(e.stage) : null, e.text);
      var bad = e.bad || e.kind === "failed" || e.kind === "killed";
      return el("li", { cls: "rp-ev " + e.level + (bad ? " bad" : "") }, el("span", { cls: "rp-n", text: num(n) }), el("span", { cls: "rp-time", text: tfmt.format(e.at) }),
        el("span", { cls: "rp-tag" + (bad ? " bad" : e.level === "run" ? " run" : ""), text: RP_TAG[e.kind] || e.kind }), el("span", { cls: "rp-txt" }, body));
    }
    var drawn = { legend: null, ticker: null };
    function draw() {
      var t = state.now;
      clock.textContent = tfmt.format(t) + " · " + dur(t - M.t0) + " of " + dur(span0);
      scrub.value = String(Math.round(1000 * (t - M.t0) / span0));
      // task slots
      var busy = {}, busyStages = {};
      M.ts.forEach(function (x) {
        if (x.s <= t && t < x.e) { busy[x.ex + "/" + x.slot] = x; busyStages[x.task.stage + "." + x.task.stageAttempt] = x.task.stage; }
      });
      M.order.forEach(function (id) {
        var c = cards[id];
        c.slots.forEach(function (sl, k) {
          var x = busy[id + "/" + k];
          var key = x ? x.task.taskId : "";
          if (sl.getAttribute("data-k") === String(key)) return;
          sl.setAttribute("data-k", String(key));
          sl.textContent = "";
          sl.className = "rp-slot" + (x ? " on" : "");
          sl.style.background = x ? stageColor(x.task.stage) : "";
          sl.title = x ? (x.task.stage >= 0 ? "Stage " + x.task.stage + ", " : "") + "partition " + x.task.partition + " (task " + x.task.taskId + ")" : "Idle";
          // the partition only: the colour and the legend tell the stage,
          // and the box's tooltip gives both, so a long label never spills
          if (x) sl.appendChild(link("#task/" + x.task.taskId, "p" + x.task.partition));
        });
        var f = flowEx[id];
        if (f && f.maxRemote) {
          var p = seriesAt(f.e.remote, t), v = p && t - Date.parse(p.t) < D.flows.stepMs ? p.v : 0;
          c.net.style.width = (100 * v / f.maxRemote) + "%";
          c.netTxt.textContent = v ? bytes(v) : "—";
        }
        if (f && f.maxFree) {
          var q = seriesAt(f.e.free, t);
          c.mem.style.width = q ? (100 * q.v / f.maxFree) + "%" : "100%";
          c.memTxt.textContent = q ? bytes(q.v) : "nothing cached";
        }
      });
      // driver: jobs and stages running, numbered
      driver.textContent = "";
      var runJobs = jobs.filter(function (j) { return j.submitted && j.submitted <= t && (!j.completed || t < j.completed); });
      var runStages = stages.filter(function (st) { return st.submitted && st.submitted <= t && (!st.completed || t < st.completed); });
      if (!runJobs.length && !runStages.length) driver.appendChild(el("p", { cls: "sub", text: t <= M.t0 ? "Starting." : t >= M.t1 ? "The run has ended." : "No job running: the driver was busy with its own work, or waiting." }));
      if (runJobs.length) {
        driver.appendChild(el("ul", { cls: "rp-jobs" }, runJobs.slice(0, RP_JOBS).map(function (j) { return el("li", { cls: "rp-job" }, link("#job/" + j.id, "Job " + j.id), " ", el("span", { cls: "sub inline", text: clip(j.desc || j.name || "", 80) })); })));
        if (runJobs.length > RP_JOBS) driver.appendChild(el("p", { cls: "sub", text: "and " + plural(runJobs.length - RP_JOBS, "more job", "more jobs") + " running" }));
      }
      if (runStages.length) {
        driver.appendChild(el("ol", { cls: "rp-list" }, runStages.slice(0, RP_STAGES).map(function (st) {
          var done = 0;
          M.ts.forEach(function (x) { if (x.task.stage === st.id && x.task.stageAttempt === st.attempt && x.e <= t && x.task.outcome === "finished") done++; });
          var n = st.numTasks || 1;
          return el("li", { cls: "rp-stage" }, el("div", null, dot(st.id), link("#stage/" + st.key, "Stage " + st.id + (st.attempt ? "." + st.attempt : "")), " ",
            el("span", { cls: "sub inline", text: num(done) + " of " + num(n) + " tasks done" })), el("div", { cls: "rp-bar" }, el("span", { style: "width:" + Math.min(100, 100 * done / n) + "%;background:" + stageColor(st.id) })));
        })));
        if (runStages.length > RP_STAGES) driver.appendChild(el("p", { cls: "sub", text: "and " + plural(runStages.length - RP_STAGES, "more stage", "more stages") + " running" }));
      }
      // the legend: what each colour is, for the stages running now
      runStages.forEach(function (st) { busyStages[st.id + "." + st.attempt] = st.id; });
      var keys = Object.keys(busyStages).sort(function (p, q) { return parseFloat(p) - parseFloat(q); });
      if (drawn.legend !== keys.join(" ")) {
        drawn.legend = keys.join(" ");
        legend.textContent = "";
        legend.appendChild(el("span", { cls: "rp-legtitle", text: "Task colour = its stage:" }));
        if (!keys.length) legend.appendChild(el("span", { cls: "sub inline", text: "no task running" }));
        keys.forEach(function (k) {
          var id = busyStages[k];
          legend.appendChild(el("span", { cls: "rp-chip", title: rpStageName(id) }, dot(id), id < 0 ? "Stage unknown" : "Stage " + k.replace(/\.0$/, ""), rpStageName(id) ? el("span", { cls: "sub inline", text: clip(rpStageName(id), 40) }) : null));
        });
      }
      // the last events up to now, newest first, redrawn only when they change
      var idx = M.shown[show.value] || [];
      var hi = 0, lo = idx.length;
      while (hi < lo) { var mid = (hi + lo) >> 1; if (M.evs[idx[mid]].at <= t) hi = mid + 1; else lo = mid; }
      var sig = show.value + "/" + hi;
      for (var j = hi - 1; j >= Math.max(0, hi - RP_EVENTS); j--) {
        var g = M.evs[idx[j]];
        if (g.members && g.members[g.members.length - 1].at > g.at) sig += "/" + g.members.filter(function (m) { return m.at <= t; }).length;
      }
      if (drawn.ticker !== sig) {
        drawn.ticker = sig;
        ticker.textContent = "";
        if (!hi) ticker.appendChild(el("li", { cls: "sub", text: "Nothing yet." }));
        for (var i = hi - 1; i >= Math.max(0, hi - RP_EVENTS); i--) ticker.appendChild(eventItem(i + 1, M.evs[idx[i]], t));
      }
    }
    show.addEventListener("change", draw);
    function speedNow() {
      var v = speed.value;
      return v.slice(0, 3) === "fit" ? span0 / (1000 * +v.slice(3)) : +v;
    }
    function frame(now) {
      if (!state.playing || !document.body.contains(stage)) { state.playing = false; playBtn.textContent = "Play"; return; }
      if (state.last) state.now = Math.min(M.t1, state.now + (now - state.last) * speedNow());
      state.last = now;
      draw();
      if (state.now >= M.t1) { state.playing = false; playBtn.textContent = "Play again"; return; }
      requestAnimationFrame(frame);
    }
    playBtn.addEventListener("click", function () {
      if (state.playing) { state.playing = false; playBtn.textContent = "Play"; return; }
      if (state.now >= M.t1) state.now = M.t0;
      state.playing = true; state.last = 0; playBtn.textContent = "Pause";
      requestAnimationFrame(frame);
    });
    restart.addEventListener("click", function () { state.now = M.t0; state.last = 0; draw(); });
    stepBtn.addEventListener("click", function () {
      state.playing = false; playBtn.textContent = "Play";
      var next = (M.shown[show.value] || []).map(function (i) { return M.evs[i]; }).filter(function (e) { return e.at > state.now; })[0];
      state.now = next ? next.at : M.t1;
      draw();
    });
    scrub.addEventListener("input", function () { state.now = M.t0 + span0 * (+scrub.value) / 1000; state.last = 0; draw(); });
    draw(); // paused at the start: it moves only when the reader presses Play
    return s;
  };

  // nodeKinds says how many nodes of each role: " (1 primary, 3 core, 80 task)".
  function nodeKinds(nodes) {
    var n = {};
    nodes.forEach(function (x) { n[x.kind] = (n[x.kind] || 0) + 1; });
    var parts = ["primary", "core", "task", "other"].filter(function (k) { return n[k]; }).map(function (k) { return num(n[k]) + " " + k; });
    return parts.length ? " (" + parts.join(", ") + ")" : "";
  }

  // logScanPage is a scan stage known only from its executors' logs (no
  // event log): the scan panel alone.
  function logScanPage(key) {
    var x = (D.hbaseScans || {})[key] || (D.hbaseScans || {})[key + ".0"];
    if (!x || !x.fromLogs) return null;
    var s = section("Stage " + x.stageId + (x.attempt ? " (attempt " + (x.attempt + 1) + ")" : "") + ": TableInputFormat scan of " + x.table,
      "Known only from the executors' logs: there is no event log, so the stage's tasks, rows and code are not shown.");
    s.insertBefore(el("div", { cls: "crumbs" }, link("#stages", "Stages"), " / stage " + x.stageId + "." + x.attempt), s.firstChild);
    hbaseScanPanel(s, x);
    return s;
  }

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
        numCol("Peak execution memory", "peakExec", bytes, "Most memory the task held at once for sorts, joins and aggregations"),
        { h: "Locality", title: LOC_EXPLAIN, v: function (t) { return t.loc; }, f: function (t) { return LOC[t.loc] || "—"; } },
        numCol("Scheduler delay", "sched", dur, "Launch overhead and waiting on the driver"), numCol("Result size", "result", bytes),
        { h: "Log line", v: function (t) { return t.line; }, f: function (t) { return el("span", { cls: "srcref", text: t.file >= 0 ? (D.files[t.file] || "?") + ":" + t.line : "" }); } }
      ]
    });
  }
  views.stage = function (key) {
    var st = null;
    stages.forEach(function (s) { if (s.key === key || (!st && String(s.id) === key)) st = s; });
    if (!st) return logScanPage(key) || notFound("Stage " + key);
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
      }, { t: "What this stage computes",
          axes: [["Boxes", "The RDDs (datasets) this stage computes, each named by the operation that made it. Cached ones are outlined."], ["Arrows", "Data flows down the arrows, from what the stage reads to what it produces."]],
          read: ["Read it top to bottom, like the code.", "A long chain is fine; what matters is which step is slow, which the task table below shows."] });
    }
    s.appendChild(codePanel(st.code, st.submitted, null, st.mine));
    s.appendChild(stageDataPanel(st));
    var hsc = (D.hbaseScans || {})[st.key];
    if (hsc) hbaseScanPanel(s, hsc);
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

  // hbaseScanPanel shows what a TableInputFormat scan stage read, region
  // by region, as the report's HBase section does.
  function hbaseScanPanel(s, x) {
    function msrc(o) { return o && o.file ? o.file + ":" + (o.line || "") : ""; }
    function host(h) { return el("span", { cls: "mono", title: h, text: String(h || "").split(".")[0] }); }
    function row(k) { return k ? el("span", { cls: "mono", text: k }) : el("span", { cls: "sub", text: "(table edge)" }); }
    var regions = x.regions || [], servers = x.servers || [];
    var norows = x.fromLogs || x.rebuilt; // rows are only in the event log
    s.appendChild(el("h3", { text: "HBase regions read: " + x.table }));
    s.appendChild(explain("TableInputFormat makes one split per region the scan overlaps, in key order, and Spark's partition n reads split n. " + (x.fromLogs ? "Each split line names its task by the executor thread that logged it. Each region's time runs from that task's Running line to its Finished line. A region whose task logged no end, or only failed, shows no time." : x.rebuilt ? "sparkplain rebuilt the run from the driver's log. Each region's time is the duration of its task, as the driver logged it. The rows for each region are only in the event log. " + (x.tiedBy === "task" ? "Each region's task is the one named by the executor thread on its split line." : "Each region's task is checked against the executor that logged its split.") : x.tiedBy === "task" ? "Each region's rows and time come from the task named by the executor thread on its split line, so the tie is exact." : "Each region's rows and time come from its task, checked against the executor that logged its split.")));
    s.appendChild(el("div", { cls: "facts" },
      fact("Key range read", el("span", { cls: "mono", text: x.rows }), "From the executors' split lines: each region's range cut to the scan's start and stop rows."),
      fact("Regions read", num(regions.length + (x.regionsCut || 0)) + " on " + num(servers.length) + " region server" + (servers.length === 1 ? "" : "s"), "One task per region, so the stage cannot run more tasks at once than this."),
      norows ? fact("Rows returned", "needs the event log", "Spark records the rows each task read only in the event log; the logs do not say.") : fact("Rows returned", num(x.totalRows), "Rows the scan returned to Spark, after its filters ran on the region servers. Spark counts no bytes for HBase input."),
      x.sizedRegions ? fact("Estimated size", bytes(x.sizeBytes) + (x.sizedRegions < regions.length ? " · " + num(x.sizedRegions) + " of " + num(regions.length) + " regions" : ""), "HBase's estimate of each region's size on disk (Input split length), not bytes sent over the network.") : null,
      x.scan ? fact("Scan as the job defined it", el("span", { cls: "mono", text: ((x.facts || []).filter(function (f) { return f[0] === "Rows"; })[0] || ["", ""])[1] }), (x.facts || []).filter(function (f) { return f[0] !== "Rows"; }).map(function (f) { return f[0] + ": " + f[1]; }).join(". ") + ". From " + msrc(x.scan.source) + ".") : null));
    if (x.filter && x.filter.length) {
      s.appendChild(explain("Filters, which the region servers apply before returning rows:"));
      s.appendChild(el("pre", { cls: "plan", text: x.filter.join("\n") }));
    } else if (!x.scan) {
      s.appendChild(explain("The scan's columns and filters are not in the logs: TableInputFormat logs only each region's key range. Run sparkplain -decode-scan - on the job's scan string, or have a non-production run print it as a sparkplain-scan line."));
    }
    if (!x.tied) s.appendChild(explain("Rows and time per region are not shown: " + x.untied));
    s.appendChild(table({
      rows: servers, sort: norows ? 4 : x.tied ? 2 : 1,
      cols: [
        { h: "Region server", v: function (r) { return r.server; }, f: function (r) { return host(r.server); } },
        { h: "Regions", num: true, v: function (r) { return r.regions; }, f: function (r) { return num(r.regions); } },
        { h: "Rows", num: true, v: function (r) { return r.rows; }, f: function (r) { return x.tied && !norows ? num(r.rows) : "—"; } },
        { h: "Estimated size", num: true, v: function (r) { return r.sizeBytes; }, f: function (r) { return r.sizeBytes ? bytes(r.sizeBytes) : "—"; } },
        { h: "Task time", num: true, v: function (r) { return r.taskMs; }, f: function (r) { return x.tied ? dur(r.taskMs) : "—"; } }
      ]
    }));
    var rows = regions.map(function (g, i) { return { i: i, g: g }; });
    s.appendChild(table({
      rows: rows, sort: 0, dir: "asc", page: 50, filter: "Filter by row key, server or region",
      cols: [
        { h: "#", num: true, v: function (r) { return r.i; }, f: function (r) { return String(r.i); } },
        { h: "Start row", v: function (r) { return r.g.startRow; }, f: function (r) { return row(r.g.startRow); } },
        { h: "End row", v: function (r) { return r.g.endRow; }, f: function (r) { return row(r.g.endRow); } },
        { h: "Region server", v: function (r) { return r.g.server; }, f: function (r) { return host(r.g.server); } },
        { h: "Rows", num: true, v: function (r) { return r.g.task && !norows ? r.g.task.rows : -1; }, f: function (r) { return r.g.task && !norows ? num(r.g.task.rows) : "—"; } },
        { h: "Took", num: true, v: function (r) { return r.g.task ? r.g.task.durationMs : -1; }, f: function (r) { return r.g.task ? dur(r.g.task.durationMs) : "—"; } },
        { h: "Executor", v: function (r) { return r.g.task ? r.g.task.executorId : ""; }, f: function (r) { return r.g.task ? el("span", null, x.fromLogs ? (r.g.task.executorId || "—") : execLink(r.g.task.executorId), el("span", { cls: "sub", text: "task " + r.g.task.taskId })) : "—"; } },
        { h: "Estimated size", num: true, v: function (r) { return r.g.sizeBytes || 0; }, f: function (r) { return r.g.sizeBytes ? bytes(r.g.sizeBytes) : "—"; } },
        { h: "Region", v: function (r) { return r.g.region; }, f: function (r) { return el("span", { cls: "mono", text: r.g.region }); } },
        { h: "Found in", v: function (r) { return msrc(r.g.source); }, f: function (r) { return el("span", { cls: "srcref" }, msrc(r.g.source), r.g.task ? el("br") : null, r.g.task ? msrc(r.g.task.source) : null); } }
      ],
      text: function (r) { return [r.g.startRow, r.g.endRow, r.g.server, r.g.region].join(" "); }
    }));
    if (x.regionsCut) s.appendChild(explain(num(x.regionsCut) + " more regions are left out to keep the page small; the JSON report lists them all."));
  }

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
    // which executor behaved differently first; the charts after it explain one
    s.appendChild(chartSlot("", "heatmap"));
    s.appendChild(chartSlot("tall", "executorsTimeline"));
    s.appendChild(chartSlot("", "execTime"));
    s.appendChild(chartSlot("", "execHeapAll"));
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
      s.appendChild(explain("The final physical plan, after adaptive re-planning. In the graph, data flows down from the scans to the result. The table lists the same operators from the result back to the scans, indented by depth, with every metric. Metrics are totals across all tasks."));
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
    // who ran it, and with what access
    var id = D.identity;
    if (id) {
      s.appendChild(el("h3", { id: "access", text: "Identity and access" }));
      if ((id.facts || []).length) s.appendChild(el("div", { cls: "facts" }, id.facts.map(function (f) { return fact(f.label, f.value, f.explain); })));
      if ((id.missing || []).length) s.appendChild(bulletNote("Not shown yet", id.missing));
      s.appendChild(el("h3", { text: "Versions and settings" }));
    }
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
    var groups = el("div", { cls: "findings" });
    s.appendChild(el("div", { cls: "filterbar" }, box, foldAll(groups)));
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
    (lay.edges || []).forEach(function (e) { // a graph of one node has no edges (null)
      var a = lay.pos[e[0]], b = lay.pos[e[1]];
      var x1 = a[0] + W / 2, y1 = a[1] + H, x2 = b[0] + W / 2, y2 = b[1], my = (y1 + y2) / 2;
      svg.appendChild(sv("path", { "class": "edge" + (g.edgeCls ? " " + g.edgeCls(e) : ""), d: "M" + x1 + "," + y1 + " C" + x1 + "," + my + " " + x2 + "," + my + " " + x2 + "," + (y2 - 4), "marker-end": "url(#sp-arrow)" }));
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
    if (g.t) wrap.appendChild(el("h4", { cls: "ctitle", text: g.t }));
    wrap.appendChild(svg);
    add(wrap, guideNodes(g));
    wrap.hidden = false;
  }
  // runPathSection shows what held up completion (JobsSection.RunPath):
  // the chain as a graph, its totals by kind, or the steps as a list when
  // the chain is too long to draw.
  var PATH_KIND = { stage: "Stages", driver: "The driver alone (no job running)", scheduling: "Scheduling (a job running, no stage on the chain)" };
  function runPathSection() {
    var P = D.runPath, steps = P.steps, byKey = {};
    stages.forEach(function (st) { byKey[st.key] = st; });
    var total = {}, run = 0;
    steps.forEach(function (x) { total[x[0]] = (total[x[0]] || 0) + (x[3] - x[2]); run += x[3] - x[2]; });
    var s = section("What held up completion", "The chain of work that set when the application finished, traced back from its end. Speeding up anything on it shortens the run; a stage off it can be slow without delaying the end.");
    s.appendChild(el("div", { cls: "facts" }, ["stage", "driver", "scheduling"].filter(function (k) { return total[k]; }).map(function (k) {
      return fact(PATH_KIND[k], dur(total[k]), pct(total[k] / run) + " of the " + dur(run) + " run");
    })));
    var drawn = P.drawn, hidden = steps.length - drawn.length;
    var info = function (i) {
      var x = i < drawn.length ? steps[drawn[i]] : null;
      if (x && x[0] !== "stage") return { title: x[0] === "driver" ? "The driver alone" : "Scheduling", sub: dur(x[3] - x[2]) + " · " + tfmt.format(new Date(x[2])), cls: x[0] === "driver" ? "gapnode" : "waitnode",
        tip: x[0] === "driver" ? "No job was running for " + dur(x[3] - x[2]) + ": the driver planned, listed files, ran code outside Spark or handled collected results." : "A job was running but no stage on the chain for " + dur(x[3] - x[2]) + ": Spark was submitting the next stage, or waiting on tasks elsewhere." };
      var key = x ? x[1] : P.extra[i - drawn.length], st = byKey[key];
      if (!st) return { title: "Stage " + key, sub: "not logged", cls: x ? "onpath" : "ctx" };
      if (!x && !st.submitted) return { title: "Stage " + st.id, sub: "skipped (output reused)", href: "#stage/" + st.key, cls: "ctx skipped", tip: "Stage " + st.id + ": " + st.name };
      var d = x ? x[3] - x[2] : span(st.submitted, st.completed);
      return { title: "Stage " + st.id + (st.attempt ? " (attempt " + (st.attempt + 1) + ")" : ""), sub: (x ? "" : "finished earlier · ") + dur(d) + " · " + (st.name || "").split(" at ")[0],
        href: "#stage/" + st.key, cls: (x ? "onpath" : "ctx") + (st.status === "failed" ? " failed" : ""), tip: "Stage " + st.id + ": " + st.name + (x ? "" : "\nAnother parent of a stage on the chain; it finished first, so it did not hold anything up.") };
    };
    var g = { t: "The chain", run: RUN("chain"),
      axes: [["Boxes", "Read down. Each box is a stage, or a pause, that waited for the box above it. That box is its slowest parent stage, or at the start of a job, the job before."],
        ["Shaded boxes", "Pauses: time with no stage on the chain running (the driver working alone, or Spark between stages)."]].concat(P.extra.length ? [["Muted boxes", "Other parents of stages on the chain. They finished first, so they held nothing up."]] : []),
      read: ["The boxes add up to the whole run, so shortening the longest ones shortens the run.", "A long driver pause is time the cluster waited on the driver; the driver gaps finding says more.", "A long stage box: open the stage to see where its time went."],
      note: (hidden ? num(hidden) + " pauses shorter than " + dur(Math.max(1000, run / 100)) + " are left out of the drawing but counted above. " : "") + "Click a stage to open it.",
      edgeCls: function (e) { return e[0] < drawn.length && e[1] < drawn.length ? "chain" : "branch"; } };
    var wrap = el("div", { cls: "dagwrap" });
    if (P.graph) drawGraph(wrap, P.graph, info, g);
    else {
      wrap.appendChild(el("h4", { cls: "ctitle", text: "The chain (" + num(drawn.length) + " steps, too many to draw)" }));
      wrap.appendChild(el("ol", null, drawn.map(function (x, i) { var inf = info(i); return el("li", null, inf.href ? link(inf.href, inf.title) : inf.title, " · " + inf.sub); })));
    }
    s.appendChild(wrap);
    return s;
  }
  function jobDag(wrap, jobID, hot) {
    var dag = D.jobDags[String(jobID)];
    if (!dag) return false;
    drawGraph(wrap, dag.layout, function (i) {
      var sid = dag.stages[i], atts = stagesByID[sid] || [], st = atts[atts.length - 1];
      if (!st) return { title: "Stage " + sid, sub: "not logged", cls: "skipped" };
      return { title: "Stage " + sid + (st.attempt ? " (attempt " + (st.attempt + 1) + ")" : ""), sub: STATUS[st.status] + " · " + dur(span(st.submitted, st.completed)) + " · " + num(st.tasks) + " tasks",
        href: "#stage/" + st.key, cls: (sid === hot ? "hot" : "") + (st.status === "failed" ? " failed" : "") + (st.status === "skipped" ? " skipped" : ""), tip: "Stage " + sid + ": " + st.name };
    }, { t: "Job " + jobID + "'s stages",
      axes: [["Boxes", "The job's stages." + (hot != null ? " This stage is outlined." : "") + " Red outline: failed. Dashed: skipped, because its output already existed."], ["Arrows", "From a stage to the stages that read its output. Each arrow is a shuffle: data written by one stage and read over the network by the next."]],
      read: ["Fewer arrows usually means less data moved.", "Skipped stages are good news: Spark reused output it already had."],
      note: "Click a stage to open it." });
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
    }, { t: "Query plan",
      axes: [["Boxes", "The query's steps (operators), with the rows each produced and the time it took, added up across tasks."], ["Arrows", "Data flows down, from the scans at the top to the result at the bottom."]],
      read: ["Row counts should shrink as filters and aggregations apply.", "A step whose rows jump up (often a join), or that takes most of the time, is where to look first."],
      note: "The table below lists every metric." });
    return true;
  }

  // ---------- chart layer ----------
  // Every chart is drawn by the D3 kit below from the page's own data, so
  // the explorer makes no network requests.
  function waitText(c, text) { var w = c.el.querySelector(".wait"); if (w) w.textContent = text; }
  // guideNodes explains a chart, the same way as the report's chartGuide:
  // axes names what each axis or mark stands for ([label, meaning] pairs:
  // "Across", "Up", "Rows", "Bar length"...), read says how to read it (a
  // string, or a list shown as bullets), and note is fine print.
  function guideNodes(g) {
    return [g.axes && g.axes.length ? el("dl", { cls: "axes" }, g.axes.map(function (a) { return el("div", null, el("dt", { text: a[0] }), el("dd", { text: a[1] })); })) : null,
      pointNodes("read", "How to read it", g.read),
      pointNodes("run", "In this run", (g.run || []).map(function (p) { return p.finding ? link("#finding/" + p.finding, p.text) : p.text; })),
      g.note ? el("p", { cls: "cap", text: g.note }) : null];
  }
  // RUN is a chart's "In this run", worked out in Go (runNotes).
  function RUN(chart) { return (D.runNotes || {})[chart] || []; }
  // pointNodes is a headed paragraph for one point, or a headed bullet
  // list for several.
  function pointNodes(cls, title, pts) {
    pts = pts == null ? [] : [].concat(pts).filter(Boolean);
    if (!pts.length) return null;
    var head = el("h5", { cls: "glabel", text: title });
    if (pts.length === 1) return el("div", { cls: cls }, head, el("p", null, pts[0]));
    return el("div", { cls: cls }, head, el("ul", null, pts.map(function (p) { return el("li", null, p); })));
  }
  // frame replaces the slot's placeholder with a titled plot area and its
  // guide: g has t (the title), axes, read and note.
  function frame(c, g) {
    c.el.textContent = "";
    if (g.t) c.el.appendChild(el("h4", { cls: "ctitle", text: g.t }));
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
    var plot = frame(c, { t: g.t, axes: g.axes, read: g.read, note: [g.note, rows.length > capRows.length ? "Showing the first " + num(capRows.length) + " of " + num(rows.length) + "." : ""].filter(Boolean).join(" ") });
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
  // whole keeps only ticks that are whole units (1 GiB, 2 GiB, not 0.5 GiB).
  function unitAxis(kind, max, range, n, whole) {
    var u = UNITS[kind][0];
    max = max || 1;
    UNITS[kind].forEach(function (x) { if (max >= 2 * x[0]) u = x; });
    var f = d3.format("~g");
    var ticks = d3.ticks(0, max / u[0], n);
    if (kind === "count" || whole) ticks = ticks.filter(Number.isInteger); // no half tasks, or half GiBs where asked
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
  // inkOn says whether text on a shape's fill reads better dark ("dark")
  // or white (""), by WCAG contrast against the fill as drawn; charts
  // redraw on a theme switch, so it follows the theme's colours.
  function inkOn(node) {
    var m = (getComputedStyle(node).fill || "").match(/\d+(\.\d+)?/g);
    if (!m || m.length < 3) return "";
    var lum = [0, 1, 2].reduce(function (s, i) {
      var c = +m[i] / 255;
      return s + [0.2126, 0.7152, 0.0722][i] * (c <= 0.03928 ? c / 12.92 : Math.pow((c + 0.055) / 1.055, 2.4));
    }, 0);
    return (lum + 0.05) / 0.06 > 1.05 / (lum + 0.05) ? "dark" : "";
  }

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
    // a series with legend: false shares the entry of the one before it
    var keyed = series.filter(function (sr) { return sr.legend !== false; });
    if (keyed.length > 1) plot.parentNode.insertBefore(legendNode(keyed), plot);
    var rowH = 30, top = 4, bandH = rows.length * rowH;
    var P = plotSvg(plot, top + bandH + 26, g.t);
    // room on the right for the longest note, such as "5.9 GiB of 6.0 GiB · 5 of 8 vCPU"
    var noteText = function (r) { return g.rowNote ? g.rowNote(r) : format(sum(r)); };
    var labelW = Math.min(170, Math.round(P.w * 0.36)), noteW = Math.min(Math.max(76, 12 + 6.4 * d3.max(rows, function (r) { return noteText(r).length; })), Math.round(P.w * 0.3));
    var val = function (sr, r) { return Math.max(sr.value(r), 0) || 0; };
    var sum = function (r, all) { return series.reduce(function (s, sr) { return s + (all || !sr.rest ? val(sr, r) : 0); }, 0); };
    var U = unitAxis(kind, d3.max(rows, function (r) { return sum(r, true); }), [labelW, P.w - noteW], Math.max(2, Math.floor((P.w - labelW - noteW) / 80)), g.wholeTicks), x = U.x;
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
        var tip = r.label + "\n" + (sr.tip ? sr.tip(r) : sr.label + ": " + format(v)) + (sr.detail ? " (" + sr.detail(r) + ")" : "");
        var w = Math.max(x(at + v) - x(at), 1), sc = sr.fill ? sr.fill(r) : sr.color;
        var piece = gr.append("rect").datum({ tip: tip }).attr("x", x(at)).attr("y", rowH * 0.12).attr("width", w).attr("height", rowH * 0.76).style("fill", sc);
        // text inside the piece: the first of its labels that fits, dark or
        // white, whichever stands out more on the piece's colour
        var fit = (sr.text ? sr.text(r) : []).filter(function (t) { return t.length * 6.9 <= w - 10; })[0];
        if (fit) gr.append("text").datum({ tip: tip }).attr("class", "inbar " + inkOn(piece.node())).attr("x", x(at) + w / 2).attr("y", rowH / 2).attr("dy", "0.35em").attr("text-anchor", "middle").text(fit);
        at += v;
      });
      gr.append("text").attr("class", "note").attr("x", x(at) + 6).attr("y", rowH / 2).attr("dy", "0.35em").text(noteText(r));
    });
    hover(row.selectAll("rect:not(.hit), text.inbar"), function (d) { return d.tip; });
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
  // title and, optionally, the y axis title. With groups ({label, color}
  // each), a point's g picks its colour and bad points stay triangles. It
  // returns the plot area.
  function scatterChart(c, pts, g, xKind, yKind, labels, groups) {
    var plot = frame(c, g);
    var anyBad = pts.some(function (p) { return p.bad; });
    // with groups, colour names the group and a triangle marks a failed task
    plot.parentNode.insertBefore(legendNode(groups ? groups.concat(anyBad ? [{ label: "▲ " + labels[1], color: "transparent" }] : [])
      : [{ label: labels[0], color: V.series }].concat(anyBad ? [{ label: labels[1], color: V.fail }] : [])), plot);
    var H = 300, m = { l: 64, r: 18, t: 10, b: 44 };
    var P = plotSvg(plot, H, g.t);
    var XU = unitAxis(xKind, d3.max(pts, function (p) { return p.x; }), [m.l, P.w - m.r], Math.max(2, Math.floor((P.w - m.l - m.r) / 100)));
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
      .style("fill", function (p) { return groups ? groups[p.g].color : p.bad ? V.fail : V.series; });
    hover(mk, function (p) { return p.tip; });
    return plot;
  }
  // Task scatter axes: what goes across and what goes up, remembered while
  // the page is open.
  var SCATTER = { x: "start", y: "dur", colour: "status" };
  // SCATTER_COLOUR are the colour choices: by status (the default: failed
  // tasks are triangles in every mode), by executor (the five with the
  // most tasks here, the rest grey), or by locality.
  var SCATTER_COLOUR = { status: { label: "Status" }, executor: { label: "Executor" }, locality: { label: "Locality" } };
  var SCATTER_X = {
    start: { label: "Started", kind: "ms", v: function (r, started) { return started(r); } },
    input: { label: "Input read", kind: "bytes", v: function (r) { return r[T.input]; } },
    rows: { label: "Rows read", kind: "count", v: function (r) { return r[T.rows]; } },
    shRead: { label: "Shuffle read", kind: "bytes", v: function (r) { return r[T.shRead]; } },
    part: { label: "Partition", kind: "count", v: function (r) { return r[T.part]; } },
    peakExec: { label: "Peak execution memory", kind: "bytes", v: function (r) { return r[T.peakExec]; } }
  };
  var SCATTER_Y = {
    dur: { label: "Duration", kind: "ms", v: function (r) { return r[T.dur]; }, read: "Higher marks took longer." },
    gc: { label: "Garbage collection", kind: "ms", v: function (r) { return r[T.gc]; }, read: "High marks spent long in garbage collection: too little memory per task, or large objects." },
    fetch: { label: "Waiting for shuffle data", kind: "ms", v: function (r) { return r[T.fetch]; }, read: "High marks waited long for data from other executors: a busy network, a slow or lost executor, or skewed shuffle blocks." },
    sched: { label: "Scheduler delay", kind: "ms", v: function (r) { return r[T.sched]; }, read: "High marks waited long to start or to report back: a busy driver or large task closures." },
    spill: { label: "Spilled", kind: "bytes", v: function (r) { return r[T.spill]; }, read: "High marks ran out of execution memory and wrote to disk." },
    peakExec: { label: "Peak execution memory", kind: "bytes", v: function (r) { return r[T.peakExec]; }, read: "High marks held the most memory at once for sorts, joins and aggregations. If they also spill or spend long in garbage collection, give each task more memory or split the work into more partitions." },
    rows: { label: "Rows read", kind: "count", v: function (r) { return r[T.rows]; }, read: "High marks read more rows than the rest: skewed data, often a hot key." },
    result: { label: "Result size", kind: "bytes", v: function (r) { return r[T.result]; }, read: "High marks sent large results to the driver, which holds them all in memory." }
  };

  // ---------- stage health map ----------
  // One bubble per stage: how long it ran across, the data it handled up
  // (read, shuffled in and out, written: honest for one stage, though a
  // total over stages would count each shuffle twice), sized by tasks.
  // Both axes are logarithmic. Outlines and small marks, not colour alone,
  // flag failed, skewed, spilled and critical-path stages.
  var HEALTH_MAX = 200, HEALTH_COMPACT = 30;
  // logAxis is a log scale over [lo, hi] with round ticks: powers of ten
  // for time (and minutes, hours), powers of 32 for bytes, and finer steps
  // when the range is too narrow for three of those.
  function logAxis(kind, lo, hi, range) {
    if (hi / Math.max(lo, 1) < 16) {
      // a narrow range: widen it around its middle so a few round ticks fit
      var mid = Math.sqrt(Math.max(lo, 1) * Math.max(hi, 1));
      lo = mid / 8; hi = mid * 8;
    }
    var x = d3.scaleLog().domain([Math.max(lo / 1.6, 1), Math.max(hi * 1.6, lo * 2, 2)]).range(range).clamp(true);
    var cands = kind === "ms" ? [1, 10, 100, 1e3, 1e4, 6e4, 6e5, 36e5, 36e6, 864e5]
      : [1, 32, 1024, 32768, 1048576, 33554432, 1073741824, 34359738368, 1099511627776, 35184372088832];
    var d = x.domain(), inside = function (v) { return v >= d[0] && v <= d[1]; }, ticks = cands.filter(inside);
    if (ticks.length < 3) {
      // a narrow range: finer round steps (1, 3, 10… for time; powers of 4 for bytes)
      var fine = [];
      if (kind === "ms") [1, 10, 100, 1e3, 1e4, 1e5, 1e6, 1e7].forEach(function (v) { fine.push(v, 3 * v); });
      else for (var b = 1; b < 1e15; b *= 4) fine.push(b);
      ticks = fine.filter(inside);
    }
    return { x: x, ticks: ticks, label: function (v) { return (kind === "ms" ? dur(v) : bytes(v)).replace(".0 ", " "); } };
  }
  function stageMoved(st) { return st.input + st.shRead + st.shWrite + st.output; }
  // A node's executor containers at its busiest, for the node chart: each
  // named when the executors that ran there are the ones held at once (when
  // some replaced others, which were together is not known), with its vCPUs.
  var EXEC_COLORS = [V.viz[0], V.viz[2], V.viz[3], V.viz[4]]; // never the driver's orange
  var EXEC_SWATCH = "linear-gradient(90deg," + EXEC_COLORS.map(function (c2, i) { return c2 + " " + (25 * i) + "% " + (25 * (i + 1)) + "%"; }).join(",") + ")";
  function nodeExecs(n) {
    var k = n.peakExecs || n.executors.length, ids = n.executors.slice().sort(function (p, q) { return (+p) - (+q) || String(p).localeCompare(String(q)); });
    var coresOf = function (id) { return (execByID[id] || {}).cores || 0; }, out = [];
    for (var i = 0; i < k; i++) out.push(ids.length === k ? { id: ids[i], cores: coresOf(ids[i]) } : { id: "", cores: ids.length ? coresOf(ids[0]) : 0 });
    return out;
  }
  function execPieceText(p) {
    var cpu = p.cores ? p.cores + " vCPU" : "";
    if (p.id && cpu) return ["Executor " + p.id + " · " + cpu, "E" + p.id + " · " + cpu, cpu, String(p.cores)];
    if (p.id) return ["Executor " + p.id, "E" + p.id];
    return cpu ? ["Executor · " + cpu, cpu, String(p.cores)] : [];
  }
  // ---------- stages worth a look ----------
  // A ranked list, read like text: failed stages first, then those on the
  // chain that held up the run's end, then those with stragglers, disk
  // spill or a retry, then the rest; longest first within each. Each row
  // has one bar (how long it ran, all on one scale) and its reasons in
  // words, so nothing needs a key.
  var ATTN_ROWS = 15, ATTN_COMPACT = 8, ATTN_ALL = 300, attnAll = false;
  function stageCrit() {
    var crit = {};
    ((D.runPath || {}).steps || []).forEach(function (x) { if (x[0] === "stage") crit[x[1]] = 1; });
    return crit;
  }
  // attnName is a stage's action and the line of the application's code
  // that ran it ("count · etl.py:42"), or its name when Spark recorded none.
  function attnName(st) {
    var act = (st.name || "").split(" at ")[0];
    return st.mine ? act + " · " + st.mine[0] + ":" + st.mine[1] : clip(st.name || "", 70);
  }
  function stageAttention(c, compact) {
    var crit = stageCrit(), wall = function (st) { return st.completed - st.submitted; };
    var all = stages.filter(function (st) { return st.submitted && st.completed > st.submitted; });
    if (!all.length) { waitText(c, "No stage has a start and an end to rank."); return; }
    // When most stages ran one after another, nearly all are on the
    // critical path and saying so tells the reader nothing: rank by
    // duration and say why instead.
    var onPath = all.filter(function (st) { return crit[st.key]; }).length, pathAll = onPath > all.length / 2;
    if (pathAll) crit = {};
    function reasons(st) {
      var r = [];
      if (st.status === "failed") r.push(["bad", "Failed"]);
      if (crit[st.key]) r.push(["crit", "On the critical path"]);
      if (stageSkewed(st)) r.push(["warn", "Slowest task " + (st.max / st.p50).toFixed(1) + "× the median"]);
      if (st.diskSpill > 0) r.push(["warn", "Spilled " + bytes(st.diskSpill) + " to disk"]);
      if (st.attempt > 0) r.push(["warn", "Retry (attempt " + (st.attempt + 1) + ")"]);
      return r;
    }
    function group(st) { return st.status === "failed" ? 0 : crit[st.key] ? 1 : stageSkewed(st) || st.diskSpill > 0 || st.attempt > 0 ? 2 : 3; }
    var ranked = all.slice().sort(function (p, q) { return group(p) - group(q) || wall(q) - wall(p); });
    var limit = compact ? ATTN_COMPACT : attnAll ? ATTN_ALL : ATTN_ROWS, shown = ranked.slice(0, limit);
    var longest = d3.max(all, wall), runMs = (a.end && a.start) ? a.end - a.start : 0;
    var plot = frame(c, { t: "Which stages are worth a look?", run: RUN("attention"),
      axes: [["Rows", "One stage each, ranked. Failed stages come first, then those on the critical path (the chain of work that held up the end of the run). Next come stages with straggler tasks, disk spill or a retry, then the rest. In each group, the longest come first."], ["Bar", "How long the stage ran, all rows on one scale, so the longest stands out."], ["Reasons", "Why it is worth a look, in words. A row with none is simply one of the longest."]],
      read: ["Start at the top: a failed or critical-path stage decides how long the run took.", "A long bar with \"Slowest task … × the median\": one partition held more data than the rest; open the stage and compare its slowest tasks.", "\"Spilled … to disk\": the stage ran short of memory for a sort, join or aggregation; more partitions or more executor memory help.", "A long bar with no reason: much work, or few tasks that each did much work. Open the stage to see where its time went."],
      note: (pathAll ? num(onPath) + " of the " + num(all.length) + " stages are on the critical path (the run's stages mostly ran one after another), so they are ranked by how long they ran. " : "") +
        "Showing " + num(shown.length) + " of " + num(all.length) + " stages. Click a row to open the stage." });
    var ol = el("ol", { cls: "attn" });
    shown.forEach(function (st, i) {
      var w = wall(st), rs = reasons(st), moved = stageMoved(st);
      var nt = st.tasks || st.numTasks || 0, facts = [num(nt) + (nt === 1 ? " task" : " tasks")];
      if (moved > 0) facts.push(bytes(moved) + " handled");
      if (runMs > 0) facts.push(pct(w / runMs) + " of the run");
      ol.appendChild(el("li", { cls: "attn-row" + (st.status === "failed" ? " failed" : "") },
        el("span", { cls: "attn-n", text: num(i + 1) }),
        el("span", { cls: "attn-name" }, link("#stage/" + st.key, "Stage " + st.id + (st.attempt ? "." + st.attempt : "")), el("span", { cls: "sub", title: st.name, text: attnName(st) })),
        el("span", { cls: "attn-bar", title: dur(w) }, el("span", { style: "width:" + Math.max(1, 100 * w / longest) + "%" })),
        el("span", { cls: "attn-dur" }, dur(w), el("span", { cls: "sub", text: facts.join(" · ") })),
        el("span", { cls: "attn-why" }, rs.length ? rs.map(function (x) { return el("span", { cls: "attn-chip " + x[0], text: x[1] }); }) : el("span", { cls: "sub", text: "One of the longest" }))));
    });
    plot.appendChild(ol);
    if (!compact && ranked.length > ATTN_ROWS) {
      var btn = el("button", { type: "button", cls: "more", text: attnAll ? "Show the top " + ATTN_ROWS : "Show all " + num(Math.min(ranked.length, ATTN_ALL)) + " stages" });
      btn.addEventListener("click", function () { attnAll = !attnAll; drawSlot(c); });
      plot.appendChild(el("div", { cls: "bar-tools" }, btn));
    }
  }
  function stageSkewed(st) { return st.p50 > 0 && st.max >= 5 * st.p50 && st.max >= 1000; }
  function stageHealth(c, compact) {
    var crit = {}; // stages on the chain that held up completion
    ((D.runPath || {}).steps || []).forEach(function (x) { if (x[0] === "stage") crit[+x[1].split(".")[0]] = 1; });
    var all = stages.filter(function (st) { return st.submitted && st.completed > st.submitted; });
    if (!all.length) { waitText(c, "No stage has a start and an end to plot."); return; }
    var wall = function (st) { return st.completed - st.submitted; };
    // the longest stages, keeping every failed and critical-path one
    var limit = compact ? HEALTH_COMPACT : HEALTH_MAX, list = all;
    if (all.length > limit) {
      var keep = {};
      all.slice().sort(function (p, q) { return wall(q) - wall(p); }).slice(0, limit).forEach(function (st) { keep[st.key] = 1; });
      list = all.filter(function (st) { return keep[st.key] || st.status === "failed" || crit[st.id]; });
    }
    if (list.filter(function (st) { return stageMoved(st) > 0; }).length < 2) {
      waitText(c, "Hardly any stage recorded data handled, so placing stages by time against data would put them all on one line. \"Worth a look\" ranks them instead.");
      return;
    }
    var plot = frame(c, { t: "Time against data handled", run: RUN("health"),
      axes: [["Across", "How long the stage ran (logarithmic: each step multiplies)."], ["Up", "The data it handled: read, shuffled in and out, and written (logarithmic)."], ["Bubbles", "One per stage; bigger bubbles ran more tasks. Outlines and marks flag failed, skewed, spilled and critical-path stages."]],
      read: ["Far right and low: a long stage that handled little data. Look at where its time went (scheduler delay, garbage collection, Python), or if it had too few tasks.", "Far right and high: big work that took its time.", "A small bubble far right: a few tasks that each did much work. More partitions can help.", "Much data is not a problem by itself."],
      note: (list.length < all.length ? "Showing the " + num(list.length) + " longest of " + num(all.length) + " stages, and every failed or critical-path one. " : "") + "Click a bubble to open the stage." });
    var mark = function (sym, color, text) { return el("span", null, el("b", { cls: "hmk", style: "color:" + color, text: sym }), text); };
    plot.parentNode.insertBefore(el("div", { cls: "legend" }, mark("●", V.series, "Stage (size: tasks)"), mark("○", V.fail, "Red outline: failed"),
      mark("◎", "var(--accent)", "Thick outline: on the critical path"), mark("▲", "var(--warn)", "Skewed: slowest task over 5× the median"), mark("■", V.viz[4], "Spilled to disk")), plot);
    // bubbles small enough not to pile up; the "none" row lifted clear of
    // the time labels, with room above it for bubbles nudged apart
    var rMax = compact ? 9 : 11;
    var H = compact ? 260 : 340, m = { l: 70, r: 20, t: 14, b: 44 };
    var P = plotSvg(plot, H, "Time against data handled");
    var moved = list.map(stageMoved).filter(function (v) { return v > 0; });
    var X = logAxis("ms", d3.min(list, wall), d3.max(list, wall), [m.l + rMax, P.w - m.r - rMax]);
    var yNone = H - m.b - rMax - 4;
    var Y = moved.length ? logAxis("bytes", d3.min(moved), d3.max(moved), [yNone - 3 * rMax - 6, m.t + rMax]) : null;
    P.svg.append("g").attr("class", "ax").attr("transform", "translate(0," + (H - m.b) + ")")
      .call(d3.axisBottom(X.x).tickValues(X.ticks).tickFormat(X.label).tickSize(-(H - m.t - m.b)).tickPadding(6)).call(leanEnds, X.x, m.l, P.w - m.r).select(".domain").remove();
    if (Y) P.svg.append("g").attr("class", "ax").attr("transform", "translate(" + m.l + ",0)")
      .call(d3.axisLeft(Y.x).tickValues(Y.ticks).tickFormat(Y.label).tickSize(-(P.w - m.l - m.r)).tickPadding(6)).select(".domain").remove();
    if (list.some(function (st) { return !stageMoved(st); })) P.svg.append("text").attr("class", "axt").attr("x", m.l - 8).attr("y", yNone).attr("dy", "0.35em").attr("text-anchor", "end").text("none");
    P.svg.append("text").attr("class", "axt").attr("x", (m.l + P.w - m.r) / 2).attr("y", H - 6).attr("text-anchor", "middle").text("How long the stage ran");
    P.svg.append("text").attr("class", "axt").attr("transform", "translate(12," + ((m.t + H - m.b) / 2) + ") rotate(-90)").attr("text-anchor", "middle").text("Data handled");
    var r = d3.scaleSqrt().domain([0, d3.max(list, function (st) { return st.tasks || st.numTasks || 1; })]).range([3, rMax]);
    // place each bubble where it belongs, then nudge it the least distance
    // that clears the ones placed before it (stages with no data only
    // upward, within their row's band), so none hides another
    var placed = [], at = {};
    list.slice().sort(function (p, q) { return wall(p) - wall(q); }).forEach(function (st) {
      var v = stageMoved(st), x = X.x(wall(st)), y0 = v > 0 && Y ? Y.x(v) : yNone, rr = r(st.tasks || st.numTasks || 1), y = y0;
      var clear = function (yy) { return placed.every(function (b) { var dx = b[0] - x, dy = b[1] - yy; return dx * dx + dy * dy >= (b[2] + rr + 1) * (b[2] + rr + 1); }); };
      for (var k = 1; !clear(y) && k <= 12; k++) {
        var d = Math.ceil(k / 2) * (rr + 1);
        y = v > 0 ? y0 + (k % 2 ? -d : d) : y0 - k * (rr + 1) / 2;
        if (v <= 0 && y < yNone - 3 * rMax) { y = y0; break; }
      }
      placed.push([x, y, rr]);
      at[st.key] = [x, y];
    });
    var pos = function (st) { return at[st.key]; };
    // big bubbles first, so small ones stay on top and clickable
    var order = list.slice().sort(function (p, q) { return (q.tasks || 0) - (p.tasks || 0); });
    var g = P.svg.append("g").selectAll("g").data(order).join("g").attr("class", "hb")
      .attr("transform", function (st) { var p = pos(st); return "translate(" + p[0] + "," + p[1] + ")"; });
    g.append("circle").attr("r", function (st) { return r(st.tasks || st.numTasks || 1); })
      .attr("class", function (st) { return "hbc" + (st.status === "failed" ? " failed" : "") + (crit[st.id] ? " crit" : ""); });
    g.filter(stageSkewed).append("path").attr("class", "hskew").attr("d", d3.symbol(d3.symbolTriangle, 34)()).attr("transform", function (st) { return "translate(0," + (-r(st.tasks || 1) - 6) + ")"; });
    g.filter(function (st) { return st.diskSpill > 0; }).append("rect").attr("class", "hspill").attr("width", 6).attr("height", 6)
      .attr("x", function (st) { return r(st.tasks || 1) + 1; }).attr("y", -3);
    var label = function (st) {
      var bits = ["Stage " + st.key + " (" + (st.name || "").split(" at ")[0] + "), " + dur(wall(st)) + ", " + bytes(stageMoved(st)) + " handled, " + num(st.tasks) + " tasks"];
      if (st.status === "failed") bits.push("failed");
      if (stageSkewed(st)) bits.push("skewed: slowest task " + (st.max / st.p50).toFixed(1) + "× the median");
      if (st.diskSpill > 0) bits.push("spilled " + bytes(st.diskSpill) + " to disk");
      if (crit[st.id]) bits.push("on the critical path");
      return bits.join(", ");
    };
    var tip = function (st) {
      return label(st).split(", ").slice(0, 1).join("") + "\nRead " + bytes(st.input) + " · shuffle read " + bytes(st.shRead) + " · shuffle write " + bytes(st.shWrite) + " · written " + bytes(st.output) +
        (st.diskSpill ? "\nSpilled to disk " + bytes(st.diskSpill) : "") +
        (st.run ? "\nCPU " + pct(st.cpuNs / 1e6 / st.run) + " of run time, GC " + pct(st.gc / st.run) : "") +
        (st.p50 ? "\nMedian task " + dur(st.p50) + ", slowest " + dur(st.max) + " (" + (st.max / st.p50).toFixed(1) + "×)" : "") +
        (st.status === "failed" ? "\nFailed" : "") + (crit[st.id] ? "\nOn the critical path" : "");
    };
    hover(g, tip);
    linkify(g, function (st) { return "#stage/" + st.key; }, label);
  }

  // ---------- cross-stage skew ----------
  // One row per stage: its task times from fastest to slowest, drawn as
  // multiples of that stage's median, so a stage of 100 ms tasks and one of
  // 10 min tasks compare on the same footing. Sorted by p95 over median.
  var SKEW_ROWS = 40, skewAll = false;
  function skewLevel(r) { return r >= 5 ? "severe" : r >= 3 ? "strong" : r >= 2 ? "mild" : ""; }
  function stageSkewChart(c) {
    var rows = stages.filter(function (st) { return st.durN >= 3 && st.p50 > 0; }).map(function (st) {
      var q = D.detail[st.key] && D.detail[st.key].m.durationMs; // count, sum, min, p25, p50, p75, max
      return { st: st, min: st.durMin, p25: q ? q[3] : null, p50: st.p50, p75: q ? q[5] : null, p95: st.p95, max: st.max, r95: st.p95 / st.p50, rMax: st.max / st.p50 };
    }).sort(function (p, q) { return q.r95 - p.r95 || q.rMax - p.rMax; });
    var dropped = stages.length - rows.length;
    if (!rows.length) { waitText(c, "No stage had three or more successful tasks, so there is no spread to compare."); return; }
    var shown = skewAll ? rows.slice(0, 300) : rows.slice(0, SKEW_ROWS);
    var plot = frame(c, { t: "Which stages have straggler tasks?", run: RUN("skew"),
      axes: [["Rows", "One stage each, the most skewed at the top (by 95th percentile over median)."], ["Across", "Task time as a multiple of the stage's median task; the line at 1× is the median."], ["Marks", "The line runs from the fastest task to the slowest. The box runs from the quarter mark to the three-quarter mark. The dot is the 95th percentile (the time that 19 tasks in 20 beat)."]],
      read: ["Short rows around 1×: the stage's tasks took similar times.", "A dot far right: one task in twenty took several times the median. 2× is mild, 3× strong, 5× severe (a guide, not a rule).", "A line reaching far past the dot: one or two stragglers. Open the stage and compare the rows that the slowest tasks read, to see if the data was skewed."],
      note: (shown.length < rows.length ? "Showing the " + num(shown.length) + " most skewed of " + num(rows.length) + ". " : "") + (dropped ? num(dropped) + " stages with fewer than three successful tasks are left out. " : "") + "Every successful task counts, not a sample. Click a row to open the stage." });
    if (rows.length > SKEW_ROWS) {
      var btn = el("button", { type: "button", cls: "more", text: skewAll ? "Show the " + SKEW_ROWS + " most skewed" : "Show all " + num(Math.min(rows.length, 300)) + " stages" });
      btn.addEventListener("click", function () { skewAll = !skewAll; drawSlot(c); });
      plot.parentNode.insertBefore(el("div", { cls: "bar-tools" }, btn), plot);
    }
    var rowH = 24, top = 4, bandH = shown.length * rowH;
    var P = plotSvg(plot, top + bandH + 28, "Which stages have straggler tasks?");
    // the note names the p95 multiple and its level; phones get the multiple alone
    var wide = P.w >= 560, labelW = Math.min(170, Math.round(P.w * 0.34)), noteW = wide ? 132 : 46;
    var lo = Math.min(0.5, d3.min(shown, function (r) { return Math.max(r.min / r.p50, 0.01); })), hi = Math.max(4, d3.max(shown, function (r) { return r.rMax; }));
    var x = d3.scaleLog().domain([lo, hi * 1.1]).range([labelW, P.w - noteW]).clamp(true);
    // keep 1× and thin the rest so labels never touch on a narrow chart
    var ticks = [], lastX = -1e9;
    [0.01, 0.1, 0.25, 0.5, 1, 2, 3, 5, 10, 20, 50, 100, 1000].filter(function (v) { return v >= lo && v <= hi * 1.1; }).forEach(function (v) {
      if (x(v) - lastX >= 34 || v === 1) { if (v === 1 && ticks.length && x(1) - x(ticks[ticks.length - 1]) < 34) ticks.pop(); ticks.push(v); lastX = x(v); }
    });
    P.svg.append("g").attr("class", "ax").attr("transform", "translate(0," + (top + bandH) + ")")
      .call(d3.axisBottom(x).tickValues(ticks).tickFormat(function (v) { return v + "×"; }).tickSize(-bandH).tickPadding(6)).call(leanEnds, x, labelW, P.w - noteW).select(".domain").remove();
    P.svg.append("line").attr("class", "medline").attr("x1", x(1)).attr("x2", x(1)).attr("y1", top).attr("y2", top + bandH);
    var row = P.svg.append("g").selectAll("g").data(shown).join("g").attr("class", "row")
      .attr("transform", function (r, i) { return "translate(0," + (top + i * rowH) + ")"; });
    row.append("rect").attr("class", "hit").attr("x", 0).attr("width", P.w).attr("height", rowH);
    row.append("text").attr("class", "rl").attr("x", labelW - 8).attr("y", rowH / 2).attr("dy", "0.35em").attr("text-anchor", "end").text(function (r) { return fitChars(stageName(r.st), labelW - 12); });
    var X = function (v, r) { return x(Math.max(v / r.p50, lo)); }, mid = rowH / 2;
    row.append("line").attr("class", "skw").attr("x1", function (r) { return X(r.min, r); }).attr("x2", function (r) { return X(r.max, r); }).attr("y1", mid).attr("y2", mid);
    row.filter(function (r) { return r.p25 != null; }).append("rect").attr("class", "skb").attr("x", function (r) { return X(r.p25, r); }).attr("y", mid - 6)
      .attr("width", function (r) { return Math.max(X(r.p75, r) - X(r.p25, r), 2); }).attr("height", 12);
    row.append("line").attr("class", "skm").attr("x1", x(1)).attr("x2", x(1)).attr("y1", mid - 7).attr("y2", mid + 7);
    row.append("circle").attr("class", function (r) { return "skp " + skewLevel(r.r95); }).attr("cx", function (r) { return X(r.p95, r); }).attr("cy", mid).attr("r", 4.5);
    row.append("text").attr("class", "note").attr("x", P.w - noteW + 8).attr("y", mid).attr("dy", "0.35em")
      .text(function (r) { return wide ? "p95 " + r.r95.toFixed(1) + "×" + (skewLevel(r.r95) ? " · " + skewLevel(r.r95) : "") : r.r95.toFixed(1) + "×"; });
    var label = function (r) {
      return stageName(r.st) + ": " + num(r.st.durN) + " successful tasks; median " + dur(r.p50) + ", 95th percentile " + dur(r.p95) + " (" + r.r95.toFixed(1) + "×), slowest " + dur(r.max) + " (" + r.rMax.toFixed(1) + "×)" +
        (skewLevel(r.r95) ? ", " + skewLevel(r.r95) + " skew" : "") + (r.st.failed ? ", " + num(r.st.failed) + " failed attempts" : "");
    };
    hover(row, function (r) {
      return stageName(r.st) + "\n" + num(r.st.durN) + " successful tasks" + (r.st.failed ? ", " + num(r.st.failed) + " failed attempts" : "") +
        "\nFastest " + dur(r.min) + (r.p25 != null ? " · quarter " + dur(r.p25) : "") + " · median " + dur(r.p50) + (r.p75 != null ? " · three-quarter " + dur(r.p75) : "") +
        "\n95th percentile " + dur(r.p95) + " (" + r.r95.toFixed(1) + "×) · slowest " + dur(r.max) + " (" + r.rMax.toFixed(1) + "×)";
    });
    linkify(row, function (r) { return "#stage/" + r.st.key; }, label);
  }

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
    var plot = frame(c, { t: "Which executor behaved differently?", run: RUN("heatmap"),
      axes: [["Rows", "One executor each."], ["Columns", "One stage each."], ["Squares", rel ? "The executor's share of that stage, as a multiple of the stage's median executor: pale is typical, dark is 3× or more. Empty: it ran no tasks there." : "The executor's share of that stage: darker is more. Empty: it ran no tasks there."]],
      read: ["A dark square in an otherwise pale column: one executor got more data, or ran slower, in that stage (skew).", "A row dark across many columns: that executor, or its node, was slower or got more data throughout."],
      note: (trimmed ? "Showing the " + num(cols.length) + " of " + num(withCells.length) + " stages with the most task time and the " + num(rows.length) + " of " + num(execIdx.length) + " executors with the most task time. " : "Every stage and executor. ") + (D.cellsCapped ? "Collection stopped per-executor totals partway (the app-wide cap), so later stages have none. " : "") + "Click a square to open the stage with that executor's row marked." });
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
    var plot = frame(c, { t: "What happened when", run: RUN("timeline"),
      axes: [["Across", "Time of day, shared by every track; drag across any track to zoom them all."], ["Tracks", "Queries, jobs and stages as bars; then the executors alive, the tasks running against the task slots (executor cores)" + (cpu.length ? ", node CPU" : "") + (waiting.length ? ", and containers waiting for room" : "") + "."], ["Shading and red", "Shaded stretches are driver gaps, when no job was running. Red marks are failed stages and executors lost or killed."]],
      read: ["Shaded stretches: the cluster waited on the driver.", "Tasks running well under the slots: idle cores.", "Executors dropping while work still ran: capacity was lost."],
      note: (dropped ? num(dropped) + " bars that overlapped too many others are left out; zoom in or use the Jobs and Stages tabs. " : "") + "Hover to see everything at that moment; click a bar to open it. Garbage collection is per task in the event log, not over time, so it is on the stage pages." });
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
  var SPLIT_READ = ["More computing is better.", "A large starting share: tasks were too small, or the driver was busy.", "A large shuffle share: much data moved between executors.",
    "Garbage collection above about 10%: memory pressure.", "A large other share: waiting on files, S3 or Python."];
  function splitTotal(st) { return st.split ? st.split.reduce(function (x, y) { return x + y; }, 0) : 0; }
  function topBy(list, n, key) { return list.filter(function (x) { return key(x) > 0; }).sort(function (a2, b2) { return key(b2) - key(a2); }).slice(0, n); }
  var DRAW = {
    stageTimes: function (c) {
      var rows = topBy(stages, 20, function (st) { return span(st.submitted, st.completed) || 0; });
      hbarChart(c, rows.map(function (st) { return { label: stageName(st), st: st }; }), [
        { label: "Succeeded", color: V.viz[0], value: function (r) { return r.st.status === "failed" ? 0 : span(r.st.submitted, r.st.completed); } },
        { label: "Failed", color: V.fail, value: function (r) { return r.st.status === "failed" ? span(r.st.submitted, r.st.completed) : 0; } }
      ], { t: "The longest stages", run: RUN("stages"),
          axes: [["Rows", "One stage each, the longest at the top."], ["Bar length", "How long the stage ran, from when it was submitted to when it finished. Red: it failed."]],
          read: ["Shorter is better.", "The top few bars are where speeding things up shortens the run most."],
          note: "Click a bar to open the stage." }, "ms", function (r) { return "#stage/" + r.st.key; });
    },
    stageData: function (c) {
      var moved = function (st) { return st.input + st.shRead + st.shWrite + st.output + st.diskSpill; };
      var rows = topBy(stages, 20, moved).map(function (st) { return { label: stageName(st), st: st }; });
      var field = function (k) { return function (r) { return r.st[k]; }; };
      hbarChart(c, rows, [
        { label: "Read", color: V.viz[0], value: field("input") }, { label: "Shuffle read", color: V.viz[1], value: field("shRead") },
        { label: "Shuffle write", color: V.viz[2], value: field("shWrite") }, { label: "Written", color: V.viz[3], value: field("output") },
        { label: "Spilled to disk", color: V.viz[4], value: field("diskSpill") }
      ], { t: "Data each stage moved", run: RUN("data"),
          axes: [["Rows", "The stages that moved the most data."], ["Bar length", "Bytes, split into what the stage read, shuffled in, shuffled out, wrote and spilled to disk."]],
          read: ["Longer bars moved more data. Much data is not a problem by itself.", "Shuffle is the costly part: it goes through local disk and across the network between executors.", "Spill should be absent: it means the data did not fit in memory."] }, "bytes", function (r) { return "#stage/" + r.st.key; });
    },
    stageSpill: function (c) {
      var rows = topBy(stages, 20, function (st) { return st.memSpill + st.diskSpill; }).map(function (st) { return { label: stageName(st), st: st }; });
      hbarChart(c, rows, [
        { label: "Spilled (size in memory)", color: V.viz[1], value: function (r) { return r.st.memSpill; } },
        { label: "Written to disk", color: V.viz[4], value: function (r) { return r.st.diskSpill; } }
      ], { t: "Spill by stage", run: RUN("spill"),
          axes: [["Rows", "The stages that spilled, most first."], ["Bar length", "Bytes spilled: the data's size as it was held in memory, and what it came to on disk (smaller, because it is compressed)."]],
          read: ["None is ideal.", "Spill means tasks had less memory than their data needed, so Spark wrote part of it to disk and read it back, which is slow.", "More partitions (less data per task) or more memory per executor core help."] }, "bytes", function (r) { return "#stage/" + r.st.key; });
    },
    stageSplit: function (c, key) {
      var list = key ? stages.filter(function (st) { return st.key === key; }) : topBy(stages, 20, splitTotal);
      var rows = list.filter(function (st) { return splitTotal(st) > 0; }).map(function (st) { return { label: stageName(st), st: st }; });
      hbarChart(c, rows, SPLIT, { t: "Where stage time went", read: SPLIT_READ, run: key ? [] : RUN("split"),
          axes: [["Rows", key ? "This stage." : "The stages with the most task time."], ["Bar length", "The total time of all tasks, split by what it was spent on. The parts are starting (scheduler delay and unpacking the task), computing, garbage collection and shuffle (waiting for data from other executors, and writing it). The rest is sending the result, and other (reading files, waiting on Python)."]],
          note: "Spark measures these separately and they can overlap a little, so the split is approximate." + (key ? "" : " Click a bar to open the stage.") },
        "ms", key ? null : function (r) { return "#stage/" + r.st.key; });
    },
    heatmap: function (c) { heatmap(c); },
    stageHealth: function (c, arg) { stageHealth(c, arg === "compact"); },
    stageAttention: function (c, arg) { stageAttention(c, arg === "compact"); },
    stageSkew: function (c) { stageSkewChart(c); },
    runTimeline: function (c) { runTimeline(c); },
    flows: function (c) {
      var F = D.flows, pts = function (ps) { return (ps || []).map(function (p) { return [Date.parse(p.t), p.v]; }); };
      var series = [{ label: "Shuffle read, own node", color: V.viz[1], points: pts(F.local), step: true },
        { label: "Shuffle read, over the network", color: V.viz[2], points: pts(F.remote), step: true, area: 0.12 }];
      if (F.spillBytes) series.push({ label: "Spilled to disk", color: V.viz[4], points: pts(F.spill), step: true });
      if (F.cachedBytes) series.push({ label: "Cached in memory", color: V.viz[0], dash: "5 3", points: pts(F.cached), step: true });
      timeChart(c, series, { t: "Data moved over time", run: RUN("flows"),
          axes: [["Across", "Time of day, while the tasks ran."], ["Up", "Bytes in each " + dur(F.stepMs) + " step, as the executors logged them. Shuffle data shows when each task started to read it (Spark's estimate from the map outputs). Spilled or cached data shows when Spark spilled or cached it."]],
          read: ["The network line high next to the own-node line: most shuffle data came from other nodes, which costs network time and their disks' reads.", "Spikes of spill: tasks ran out of execution memory at those times; the stage running then needed more memory or more partitions.", "Caching that stops while the job still runs, with drops in the storage memory chart: storage memory was full."] }, "bytes");
    },
    storage: function (c) {
      var ex = D.flows.executors.filter(function (e) { return (e.free || []).length; }).sort(function (a2, b2) { return (a2.minFree || 0) - (b2.minFree || 0); });
      var series = ex.slice(0, 8).map(function (e, i) { return { label: "Executor " + e.executor, color: V.viz[i % V.viz.length], dash: i >= V.viz.length ? "5 3" : null, step: true, points: e.free.map(function (p) { return [Date.parse(p.t), p.v]; }) }; });
      timeChart(c, series, { t: "Storage memory left over time", run: RUN("memory"),
          axes: [["Across", "Time of day, from each executor's first cached block to its last."], ["Up", "Storage memory free on the executor after each block it cached or dropped, as its MemoryStore logged it (the 8 executors with the least left)."]],
          read: ["A line that falls towards zero: that executor's cache filled up; the next blocks pushed older ones out or were not cached.", "A line that jumps back up: blocks were dropped to make room, and will be computed again when used.", "Lines that stay high: the cache had room to spare."] }, "bytes");
    },
    hbaseLoad: function (c) {
      var load = D.hbaseLoad || [], pts = function (ps) { return (ps || []).map(function (p) { return [Date.parse(p.t), p.v]; }); };
      var series = load.slice(0, 8).map(function (l, i) { return { label: String(l.server).split(".")[0], color: V.viz[i % V.viz.length], dash: i >= V.viz.length ? "5 3" : null, points: pts(l.points), step: true }; });
      if (load.length > 1) series.push({ label: "All region servers", color: V.neutral, dash: "2 3", points: pts(D.hbaseLoadTotal), step: true });
      timeChart(c, series, { t: "Region server load over time", run: RUN("hbaseLoad"),
          axes: [["Across", "Time of day, while the run's HBase scans ran."], ["Up", "HBase scan tasks reading from the region server at once: the most in each " + dur(D.hbaseLoadStepMs) + " step. One line per region server (the 8 busiest), the dotted line all of them together."]],
          read: ["Lines of similar height: the reads were spread across the region servers.", "One line close to the dotted line, with the others low: that server served almost all the reads. The reads of its regions waited in a queue on it.", "A server's line that stays up after the others drop: its regions took longest, and the stage waited on them."] }, "count");
    },
    dataOverTime: function (c) {
      var done = stages.filter(function (st) { return st.completed; }).sort(function (a2, b2) { return a2.completed - b2.completed; });
      if (done.length < 2) { waitText(c, "Too few finished stages to chart."); return; }
      var t = a.start || done[0].submitted, inB = [[t, 0]], sh = [[t, 0]], out = [[t, 0]], ib = 0, sb = 0, ob = 0;
      done.forEach(function (st) { ib += st.input; sb += st.shWrite; ob += st.output; inB.push([st.completed, ib]); sh.push([st.completed, sb]); out.push([st.completed, ob]); });
      timeChart(c, [
        { label: "Read", color: V.viz[0], points: inB, step: true, area: 0.08 },
        { label: "Shuffle write", color: V.viz[2], points: sh, step: true, area: 0.08 },
        { label: "Written", color: V.viz[3], points: out, step: true, area: 0.08 }
      ], { t: "Data over time", run: RUN("dataOverTime"),
          axes: [["Across", "Time of day, while the application ran."], ["Up", "Bytes so far: a running total of data read, shuffled and written, added as each stage finished."]],
          read: ["Steep rises show when the work occurred.", "Long flat stretches are time spent not moving data: driver code, planning, or waiting for executors."] }, "bytes");
    },
    execTime: function (c) {
      var rows = topBy(execs.filter(function (x) { return x.id !== "driver"; }), 30, function (x) { return x.run; }).map(function (x) { return { label: "Executor " + x.id, x: x }; });
      hbarChart(c, rows, [
        { label: "Computing", color: V.viz[0], value: function (r) { return r.x.cpuNs / 1e6; } },
        { label: "Garbage collection", color: V.viz[1], value: function (r) { return r.x.gc; } },
        { label: "Other or waiting", color: V.neutral, value: function (r) { return r.x.run - r.x.cpuNs / 1e6 - r.x.gc; } }
      ], { t: "Where executor time went", run: RUN("execTime"),
          axes: [["Rows", "One executor each."], ["Bar length", "The total run time of its tasks. It is split into computing on the JVM, garbage collection, and other or waiting (for shuffle data, storage or Python workers)."]],
          read: ["More computing is better.", "Garbage collection above about 10% of a bar means memory pressure.", "A large other-or-waiting part means tasks waited instead of computing. In PySpark, time spent in Python counts there."] }, "ms", function (r) { return "#executor/" + encodeURIComponent(r.x.id); });
    },
    execHeapAll: function (c) {
      if (!D.heapBytes) { waitText(c, "The executors' heap size is not known."); return; }
      var rows = topBy(execs.filter(function (x) { return x.id !== "driver"; }), 30, function (x) { return x.peakHeap || 0; }).map(function (x) { return { label: "Executor " + x.id, x: x }; });
      hbarChart(c, rows, [
        { label: "Peak heap", color: V.viz[0], value: function (r) { return r.x.peakHeap; } },
        { label: "Heap not used at peak", color: V.line, rest: true, value: function (r) { return D.heapBytes - r.x.peakHeap; } }
      ], { t: "Peak heap per executor", run: RUN("peakHeap"),
          axes: [["Rows", "One executor each."], ["Bar length", "The most Java heap it used, against the " + bytes(D.heapBytes) + " it was given (the full row)."]],
          read: ["Over 90% of the row: it risked running out of memory.", "Bars that stay well short: executors had more heap than they used and could be smaller."],
          note: "Heap is sampled, so short spikes can be missed." }, "bytes", function (r) { return "#executor/" + encodeURIComponent(r.x.id); });
    },
    nodeMemory: function (c) {
      var rows = D.aws.nodes.filter(function (n) { return n.yarnMem; }).map(function (n) { return { label: n.host.split(".")[0], n: n, ex: nodeExecs(n) }; });
      if (!rows.length) { waitText(c, D.aws.nodeMemNote); return; }
      var most = d3.max(rows, function (r) { return r.ex.length; }) || 0;
      var execBytes = function (r) { return (r.n.execMem || 0) * r.ex.length; };
      var series = [{ label: "Driver container", color: V.viz[1], value: function (r) { return r.n.driverMem || 0; }, text: function () { return ["Driver", "D"]; } }];
      // one series per executor container a node held at once, each its own colour
      for (var i = 0; i < most; i++) (function (i) {
        series.push({ label: "Executor containers (one colour each)", legend: i === 0, color: EXEC_SWATCH,
          fill: function () { return EXEC_COLORS[i % EXEC_COLORS.length]; },
          value: function (r) { return i < r.ex.length ? r.n.execMem || 0 : 0; },
          tip: function (r) { var p = r.ex[i]; return (p.id ? "Executor " + p.id + "'s container" : "An executor container") + ": " + bytes(r.n.execMem || 0) + (p.cores ? ", " + p.cores + " vCPU" : ""); },
          text: function (r) { return execPieceText(r.ex[i]); } });
      })(i);
      series.push({ label: "Free", color: V.line, rest: true, value: function (r) { return r.n.yarnMem - (r.n.driverMem || 0) - execBytes(r); } });
      hbarChart(c, rows, series, { t: "CPU and memory the executors took on each node", run: RUN("nodeMemory"),
          axes: [["Rows", "One worker node each."], ["Bar length", "The memory the node offered YARN. Coloured pieces are what this application took at its busiest: the driver's container, then one piece per executor container, each in its own colour. The rest is free."], ["Inside a piece", "Which executor it is and the vCPUs it had, such as \"Executor 3 · 4 vCPU\" (shortened when the piece is narrow)."], ["Right", "Memory taken of what the node offered, and vCPUs taken of what it offered."]],
          read: ["Count the coloured pieces to see how many executors ran on the node at once.", "A full bar: the node was used well.", "Free space helps only if it is at least one executor container wide. Smaller gaps are memory paid for but unusable.", "A mostly free node did little work for this run."],
          rowNote: function (r) {
            var used = (r.n.driverMem || 0) + execBytes(r), cpu = r.ex.reduce(function (s, p) { return s + (p.cores || 0); }, 0);
            return bytes(used) + " of " + bytes(r.n.yarnMem) + (cpu && r.n.yarnCores ? " · " + cpu + " of " + r.n.yarnCores + " vCPU" : "");
          },
          note: D.aws.nodeMemNote, wholeTicks: true }, "bytes");
    },
    clusterContainers: function (c) {
      var list = metric("ContainerAllocated").concat(metric("ContainerPending"));
      if (!list.length) { waitText(c, "CloudWatch had no container counts for this run."); return; }
      timeChart(c, list.map(function (s) {
        var waiting = s.name === "ContainerPending";
        return { label: waiting ? "Waiting" : "Allocated", color: waiting ? V.fail : V.series, points: s.points, step: true, area: 0.12 };
      }), { t: "Containers on the cluster", run: RUN("containers"),
          axes: [["Across", "Time of day. This application ran from " + (a.start ? tfmt.format(new Date(a.start)) : "?") + " to " + (a.end ? tfmt.format(new Date(a.end)) : "?") + "."], ["Up", "Containers across the whole cluster, every minute: those YARN had placed, and those waiting for room."]],
          read: ["Waiting should stay at zero.", "Waiting while YARN memory is free: the containers were too big to fit on any one node.", "Waiting with memory full: the cluster was too small, or busy with other applications."] }, "count");
    },
    nodeCPU: function (c) {
      var list = metric("CPUUtilization", "Average");
      if (!list.length) { waitText(c, "CloudWatch had no CPU figures for these nodes."); return; }
      var name = {};
      D.aws.nodes.forEach(function (n) { if (n.instance) name[n.instance.id] = n.instance.id + (n.driver ? " (driver)" : n.executors.length ? " (" + n.executors.length + " executors)" : n.instance.role === "MASTER" ? " (primary)" : " (idle)"); });
      timeChart(c, list.map(function (s, i) { return { label: name[s.scope] || s.scope, color: V.viz[i % V.viz.length], dash: i >= V.viz.length ? "5 3" : null, points: s.points, dots: true }; }),
        { t: "Node CPU", run: RUN("nodeCPU"),
          axes: [["Across", "Time of day, while the application ran."], ["Up", "CPU use of the whole machine, from 0 to 100%, one line per node, averaged over EC2's 5-minute periods."]],
          read: ["Above about 85% for long: tasks queued for CPU.", "Low CPU on a node that ran executors: its tasks were waiting on disk, the network or Python rather than computing.", "Daemons and other applications on the node count too."] }, "pct", { max: 100 });
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
      ], { t: "Tasks running against task slots", run: RUN("running"),
        axes: [["Across", "Time of day, while the application ran."], ["Up", "Tasks. The filled area is the tasks running, averaged over each " + dur(R.bucketMs) + "; the dashed line is the task slots: the cores of the executors alive then."]],
        read: ["The filled area should reach the dashed line: every core busy.", "Space between them is idle cores you are paying for. Common causes: the driver working alone, one slow task holding up a stage, or too few partitions."],
        note: "Task times are stamped by the driver, so running tasks can briefly go past the slots." }, "count");
    },
    jobsTimeline: function (c) {
      c.link = function (r) { return "#job/" + r.id; };
      timeline(c, jobs.filter(function (j) { return j.submitted; }).map(function (j) {
        var end = j.completed || appEnd;
        return { id: j.id, row: "Job " + j.id, bar: (j.desc || j.name || "").slice(0, 80), color: statusColor(j.status), start: j.submitted, end: end,
          tip: "Job " + j.id + ": " + (j.desc || j.name) + "\n" + (STATUS[j.status] || j.status) + ", " + dur(end - j.submitted) };
      }), { t: "Jobs over time", run: RUN("jobs"),
        axes: [["Across", "Time of day, while the application ran."], ["Rows", "One job each: its bar runs from when it was submitted to when it finished."], ["Colour", STATUS_NOTE.trim()]],
        read: ["Longer bars took longer.", "Gaps between bars are time the driver spent outside Spark jobs: planning, Python code or waiting.", "Bars that overlap ran at the same time."],
        note: "Click a bar to open the job." }, "No job has a start time.");
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
      })), { t: "Executor lifetimes", run: RUN("lifetimes"),
        axes: [["Across", "Time of day, while the application ran."], ["Rows", "One executor each, from when it was added to when it was removed."], ["Colour", "Blue: ran to the end. Red: killed or lost, or a period it was excluded. Grey: removed for another reason, such as being idle."]],
        read: ["Long blue bars are healthy.", "Any red deserves a look.", "Many short grey bars: dynamic allocation added and removed executors often, which costs start-up time."],
        note: "Click one to open it." }, "No executor was logged.");
    },
    sqlTimeline: function (c) {
      c.link = function (r) { return "#query/" + r.id; };
      timeline(c, queries.filter(function (q) { return q.start; }).map(function (q) {
        var st = queryStatus(q), end = q.end || appEnd;
        return { id: q.id, row: "Query " + q.id, bar: (q.desc || "").slice(0, 80), color: statusColor(st), start: q.start, end: end,
          tip: "Query " + q.id + ": " + q.desc + "\n" + (STATUS[st] || st) + ", " + dur(end - q.start) };
      }), { t: "Queries over time", run: RUN("queries"),
        axes: [["Across", "Time of day, while the application ran."], ["Rows", "One SQL statement or DataFrame action each, from when it started to when it finished."], ["Colour", STATUS_NOTE.trim()]],
        read: ["Longer bars took longer, so they are where tuning pays off.", "Bars that overlap ran at the same time."],
        note: "Click a bar to open its plan." }, "No query was logged.");
    },
    durationHistogram: function (c, key) {
      var det = D.detail[key];
      if (!det || !det.h.length) { waitText(c, "No successful task to chart."); return; }
      colChart(c, det.h.map(function (b) {
        var range = b[0] === b[1] ? dur(b[0]) : dur(b[0]) + " to " + dur(b[1]);
        return { label: dur(b[0]), value: b[2], tip: range + ": " + num(b[2]) + " tasks" };
      }), { t: "Task durations",
        axes: [["Across", "How long a task took, in ranges; each column is labelled by where its range starts."], ["Up", "How many successful tasks fell in that range (every task, not a sample)."]],
        read: ["One tall group on the left is ideal: tasks took similar, short times.", "A long tail to the right: a few tasks held the stage up, usually because of skewed data."] }, "count", { color: V.series });
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
      var groups = null, groupOf = null;
      if (SCATTER.colour === "executor") {
        var n = {};
        rows.forEach(function (r) { n[r[T.exec]] = (n[r[T.exec]] || 0) + 1; });
        var top = Object.keys(n).sort(function (p, q) { return n[q] - n[p]; }).slice(0, 5), idx = {};
        groups = top.map(function (e, i) { idx[e] = i; return { label: "Executor " + execName(+e), color: V.viz[i] }; });
        if (Object.keys(n).length > top.length) groups.push({ label: "Other executors", color: V.neutral });
        groupOf = function (r) { return idx[r[T.exec]] != null ? idx[r[T.exec]] : groups.length - 1; };
      } else if (SCATTER.colour === "locality") {
        groups = LOC.map(function (l, i) { return { label: l, color: V.viz[i % V.viz.length] }; }).concat([{ label: "Not recorded", color: V.neutral }]);
        groupOf = function (r) { return r[T.loc] >= 0 && r[T.loc] < LOC.length ? r[T.loc] : LOC.length; };
        var used = {};
        rows.forEach(function (r) { used[groupOf(r)] = 1; });
        var keep = groups.map(function (gr, i) { return used[i] ? i : -1; }).filter(function (i) { return i >= 0; }), remap = {};
        keep.forEach(function (i, j) { remap[i] = j; });
        groups = keep.map(function (i) { return groups[i]; });
        var base = groupOf;
        groupOf = function (r) { return remap[base(r)]; };
      }
      var plot = scatterChart(c, rows.map(function (r) {
        var xv = xa.v(r, started), yv = ya.v(r);
        return { x: xv, y: yv, bad: r[T.status] !== 0, g: groupOf ? groupOf(r) : 0,
          tip: "Task " + r[T.task] + " (partition " + r[T.index] + ") on executor " + execName(r[T.exec]) + "\nStarted " + dur(started(r)) + " into the stage, took " + dur(r[T.dur]) +
            (isDefault ? "\n" + num(r[T.rows]) + " rows read" : "\n" + xa.label + ": " + fmtOf(xa.kind, xv) + "\n" + ya.label + ": " + fmtOf(ya.kind, yv)) };
      }), { t: isDefault ? "When tasks started and how long they took" : SCATTER.x === "start" ? ya.label + ", by when each task started" : ya.label + " against " + xa.label.toLowerCase(),
        axes: [["Across", SCATTER.x === "start" ? "When the task started, counted from the start of the stage." : xa.label + "."], ["Up", ya.label + "."], ["Marks", "One per task. Triangles failed; the colour picker above chooses what colour shows."]],
        read: isDefault ? ["Dots should form a low, even band.", "Dots far above the rest are stragglers. Compare the rows that they read with the rows that the others read.", "Vertical stripes are waves: one per round of task slots."]
          : SCATTER.x === "start" ? [ya.read, "Vertical stripes are waves, one per round of task slots."]
          : ["Marks that climb from left to right: " + xa.label.toLowerCase() + " explains the " + ya.label.toLowerCase() + ", and a few far to the right are skew in the data.", "Marks high up at the left are slow for another reason, such as a busy node, garbage collection or waiting.", ya.read],
        note: det.sample.length < det.from ? "From a sample of " + num(det.sample.length) + " of " + num(det.from) + " tasks, plus the slowest " + num(det.slow.length) + "." : "Every task of this stage." },
        xa.kind, ya.kind, ["Succeeded", "Failed or killed", xa.label + (SCATTER.x === "start" ? ", after the stage began" : ""), ya.label], groups);
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
        el("span", { cls: "count", text: "Across:" }), pick("Across the chart", SCATTER_X, SCATTER.x, function (v) { SCATTER.x = v; }),
        el("span", { cls: "count", text: "Colour:" }), pick("Colour the marks by", SCATTER_COLOUR, SCATTER.colour, function (v) { SCATTER.colour = v; })), plot.previousSibling);
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
      }), { t: "Peak heap by stage",
        axes: [["Across", "One column per stage this executor ran."], ["Up", "The most Java heap sampled while the stage ran here; the dashed line is the heap the executor was given."]],
        read: ["Columns close to the dashed line risked running out of memory.", "Columns well below it for every stage: the heap is bigger than this work needs."],
        note: "Heap is sampled, so short spikes can be missed. Click a column to open the stage." }, "bytes",
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


  // ---------- syntax highlighting ----------
  // A small highlighter for the languages -source reads (CLAUDE.md allows no
  // script library but D3): comments, strings, numbers, keywords, built-ins,
  // decorators and the names def, class and the like introduce. It returns
  // [class, text] tokens per line, turned into text nodes, never HTML, so a
  // line of code cannot inject markup; strings and comments that span lines
  // carry over. Unknown file types are left plain.
  var HL = (function () {
    function words(s) { var m = {}; s.split(" ").forEach(function (w) { m[w] = 1; }); return m; }
    var LANGS = {
      py: { line: "#", strs: ['"""', "'''", '"', "'"], prefix: /^[rRbBuUfF]{1,2}(?=['"])/, deco: true,
        kw: words("and as assert async await break class continue def del elif else except False finally for from global if import in is lambda None nonlocal not or pass raise return True try while with yield match case"),
        bi: words("print len range int float str bool list dict set tuple open sum min max abs sorted enumerate zip map filter isinstance type super self cls"),
        defs: words("def class") },
      scala: { line: "//", block: ["/*", "*/"], strs: ['"""', '"', "'"], deco: true,
        kw: words("abstract case catch class def do else extends false final finally for forSome if implicit import lazy match new null object override package private protected return sealed super this throw trait true try type val var while with yield given using enum then"),
        bi: words("println Some None Seq List Map Set Option String Int Long Double Boolean Array Unit"), defs: words("def class object trait") },
      java: { line: "//", block: ["/*", "*/"], strs: ['"""', '"', "'"], deco: true,
        kw: words("abstract assert boolean break byte case catch char class const continue default do double else enum extends final finally float for if implements import instanceof int interface long native new null package private protected public return short static super switch synchronized this throw throws transient try void volatile while true false var record yield"),
        bi: words("String Integer Long Double Boolean List Map Set System Object"), defs: words("class interface enum record") },
      kt: { line: "//", block: ["/*", "*/"], strs: ['"""', '"', "'"], deco: true,
        kw: words("as break class continue do else false for fun if in interface is null object package return super this throw true try typealias val var when while import private public internal protected override open data sealed companion lateinit suspend"),
        bi: words("println listOf mapOf setOf String Int Long Double Boolean Unit"), defs: words("fun class object interface") },
      sql: { line: "--", block: ["/*", "*/"], strs: ["'", '"'], ci: true,
        kw: words("select from where group by order having limit join inner left right full outer cross on as and or not in is null like between case when then else end distinct union all insert into values update set delete create table view with over partition asc desc cast true false exists"),
        bi: words("count sum avg min max coalesce date_trunc year month to_date row_number rank dense_rank lag lead") },
      r: { line: "#", strs: ['"', "'"],
        kw: words("if else repeat while function for in next break TRUE FALSE NULL Inf NaN NA return"),
        bi: words("library print c list paste nrow ncol length sum mean") }
    };
    var EXT = { py: "py", scala: "scala", sc: "scala", java: "java", kt: "kt", kts: "kt", sql: "sql", r: "r" };
    function langOf(path) { var m = /\.([A-Za-z]+)$/.exec(path || ""); return m ? LANGS[EXT[m[1].toLowerCase()]] || null : null; }
    var NUM = /^(?:0[xX][0-9a-fA-F_]+|\d[\d_]*(?:\.\d+)?(?:[eE][+-]?\d+)?)[jJlLfFdD]?/, ID = /^[A-Za-z_][A-Za-z0-9_]*/, WORD = /[A-Za-z0-9_]/;
    // tokens splits a file's lines; each line is a list of [class, text],
    // class "" for plain text.
    function tokens(lines, lang) {
      var open = null, out = [];
      lines.forEach(function (line) {
        var toks = [], plain = "", i = 0, afterDef = false;
        function push(cls, text) { if (plain) { toks.push(["", plain]); plain = ""; } if (text) toks.push([cls, text]); }
        while (i < line.length) {
          var rest = line.slice(i);
          if (open) { // inside a string or comment begun on an earlier line
            var j = rest.indexOf(open.end);
            if (j < 0) { push(open.cls, rest); i = line.length; break; }
            push(open.cls, rest.slice(0, j + open.end.length)); i += j + open.end.length; open = null; continue;
          }
          if (lang.line && rest.slice(0, lang.line.length) === lang.line) { push("c", rest); break; }
          if (lang.block && rest.slice(0, 2) === lang.block[0]) {
            var e = rest.indexOf(lang.block[1], 2);
            if (e < 0) { push("c", rest); open = { end: lang.block[1], cls: "c" }; break; }
            push("c", rest.slice(0, e + 2)); i += e + 2; continue;
          }
          var prev = i ? line[i - 1] : "";
          var pre = !WORD.test(prev) && lang.prefix && lang.prefix.exec(rest), p = pre ? pre[0].length : 0, q = null;
          for (var k = 0; k < lang.strs.length; k++) if (rest.substr(p, lang.strs[k].length) === lang.strs[k]) { q = lang.strs[k]; break; }
          if (q) {
            var from = p + q.length, end = -1;
            if (q.length === 3) end = rest.indexOf(q, from);
            else for (var m = from; m < rest.length; m++) { if (rest[m] === "\\") { m++; continue; } if (rest[m] === q) { end = m; break; } }
            if (end < 0) { push("s", rest); if (q.length === 3) open = { end: q, cls: "s" }; break; }
            push("s", rest.slice(0, end + q.length)); i += end + q.length; continue;
          }
          if (lang.deco && rest[0] === "@" && ID.test(rest.slice(1))) { var d = "@" + ID.exec(rest.slice(1))[0]; push("d", d); i += d.length; continue; }
          var nm = WORD.test(prev) ? null : NUM.exec(rest);
          if (nm) { push("n", nm[0]); i += nm[0].length; continue; }
          var id = WORD.test(prev) ? null : ID.exec(rest);
          if (id) {
            var w = id[0], key = lang.ci ? w.toLowerCase() : w;
            if (afterDef) { push("f", w); afterDef = false; }
            else if (prev === ".") plain += w; // a method or field: spark.conf.set is not set()
            else if (lang.kw[key]) { push("k", w); afterDef = !!(lang.defs && lang.defs[w]); }
            else if (lang.bi && lang.bi[key]) push("b", w);
            else plain += w;
            i += w.length; continue;
          }
          plain += rest[0]; i++;
        }
        push("", "");
        out.push(toks);
      });
      return out;
    }
    return { langOf: langOf, tokens: tokens };
  })();
  // ---------- end syntax highlighting ----------
  // srcCell draws one line of a source file, highlighted when its language
  // is known; each file is tokenised once.
  function srcCell(sf, n) {
    if (sf._hl === undefined) { var lang = HL.langOf(sf.path); sf._hl = lang ? HL.tokens(sf.lines, lang) : null; }
    if (!sf._hl) return el("td", { cls: "src", text: sf.lines[n] });
    return el("td", { cls: "src" }, sf._hl[n].map(function (t) { return t[0] ? el("span", { cls: "tk-" + t[0], text: t[1] }) : t[1]; }));
  }

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
    for (var n = from; n <= to; n++) rows.push(el("tr", { cls: n === c[1] ? "hit" : null }, el("td", { cls: "ln", text: String(n) }), srcCell(sf, n - 1)));
    return el("div", { cls: "codebox" }, el("div", { cls: "codehead" }, link(codeHref(c), sf.path)), el("table", { cls: "code" }, el("tbody", null, rows)));
  }
  // codePanel shows where something ran: its frames, the code around the
  // innermost one, or why nothing was recorded.
  // mine, for a stage, is its line of the application's code (a code row,
  // with how it was found fifth): its own call site's, or its job's action when its
  // call site is Spark's own (PythonRDD.scala and the like).
  function codePanel(code, when, selfJob, mine) {
    var box = el("section", null, el("h3", { text: "Code" }));
    var CTX = D.sourceContext || 20;
    if (mine && mine[4]) {
      var near = /^nearest/.test(mine[4]);
      box.appendChild(explain(near ? "Spark recorded no line of your code for this stage (PySpark records none for some actions, such as write, saveAsTable and sql). The nearest line it recorded before the stage started is below; the code that started this stage likely comes soon after it." :
        "This stage is named after Spark's own code (" + (code && code.length ? codeLabel(code[0]) : "no call site") + "). The line of your code that ran it is " + mine[4] + ":"));
      var mh = codeHref(mine);
      box.appendChild(el("p", null, mh ? link(mh, codeLabel(mine)) : el("span", { cls: "mono", text: codeLabel(mine) })));
      var ms = snippet(mine, CTX);
      if (ms) box.appendChild(ms);
      else if (!(D.sources || []).length) box.appendChild(el("p", { cls: "note", text: "Run sparkplain with -source <your code folder>, or set source: in the config file, to see the code here." }));
      return box;
    }
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
    // the code around the first frame that is the application's own
    var sn = null;
    for (var i = 0; i < code.length && !sn; i++) sn = snippet(mine && i === 0 ? mine : code[i], CTX);
    if (sn) box.appendChild(sn);
    else if (!(D.sources || []).length) box.appendChild(el("p", { cls: "note", text: "Run sparkplain with -source <your code folder>, or set source: in the config file, to see the code here." }));
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
          el("td", { cls: "ln", text: String(n + 1) }), srcCell(sf, n),
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
          if (!n.executors.length) return n.driver ? el("span", null, "the driver only", n.noRoom ? el("span", { cls: "sub", text: n.noRoom }) : null) : n.instance && n.instance.role === "MASTER" ? "primary node" : el("span", { cls: "bad", text: "nothing" });
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
    if (A.nodes.some(function (n) { return n.yarnMem; }) || A.nodeMemNote) s.appendChild(chartSlot("", "nodeMemory"));
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
    var s2 = section("Logs", "The container, step and node logs that sparkplain read for this application. For each log, it shows what sparkplain recognised: errors with their causes, exits, memory kills, and the user that the application ran as.");
    if (D.cluster) {
      var c = D.cluster;
      s2.appendChild(el("div", { cls: "facts" },
        fact("Cluster", el("span", null, el("span", { cls: "mono", text: c.id }), c.name ? " (" + c.name + ")" : ""), c.release + ", " + c.state + (c.reason ? ": " + c.reason : "") + "."),
        fact("Log URI", el("span", { cls: "mono", text: c.logUri || "none" }), "Where EMR copies the cluster's logs."),
        fact("Instance profile", c.profile || "none", "The IAM role the nodes' processes use for AWS calls."),
        fact("Nodes up during the run", num(c.nodes.length) + nodeKinds(c.nodes), "The nodes the run could use, as At a glance counts them" + (c.allInstances > c.nodes.length ? ", of " + num(c.allInstances) + " instances the cluster has had (ended ones included)" : "") + ". Listed below by role.")));
      if (c.nodes.length) {
        s2.appendChild(el("h3", { text: "Nodes up during the run" }));
        s2.appendChild(table({
          rows: c.nodes.map(function (n, i) { return { i: i, n: n }; }), sort: 0, dir: "asc", page: 100, filter: "Filter by role, instance, type or host",
          cols: [
            { h: "Role", v: function (r) { return r.i; }, f: function (r) { return r.n.kind; } },
            { h: "#", num: true, v: function (r) { return r.n.seq; }, f: function (r) { return String(r.n.seq); } },
            { h: "Instance", v: function (r) { return r.n.id; }, f: function (r) { return el("span", { cls: "mono", text: r.n.id }); } },
            { h: "Host", v: function (r) { return r.n.host; }, f: function (r) { return el("span", { cls: "mono", title: r.n.host, text: String(r.n.host || "").split(".")[0] }); } },
            { h: "Type", v: function (r) { return r.n.type || ""; }, f: function (r) { return (r.n.type || "—") + (r.n.vcpu ? " · " + r.n.vcpu + " vCPU" : "") + (r.n.memoryBytes ? " · " + bytes(r.n.memoryBytes) : ""); } },
            { h: "Market", v: function (r) { return r.n.market || ""; }, f: function (r) { return r.n.market ? r.n.market.toLowerCase().replace("_", "-") : "—"; } },
            { h: "Joined", v: function (r) { return r.n.ready || ""; }, f: function (r) { return when(r.n.ready ? Date.parse(r.n.ready) : 0, true); } },
            { h: "Ended", v: function (r) { return r.n.ended || ""; }, f: function (r) { return r.n.ended ? when(Date.parse(r.n.ended), true) : "still up"; } },
            { h: "Ran for this application", v: function (r) { return (r.n.driver ? 1000 : 0) + (r.n.executors || 0); }, f: function (r) { return r.n.driver ? "the driver" + (r.n.executors ? " and " + num(r.n.executors) + " executors" : "") : r.n.executors ? num(r.n.executors) + " executor" + (r.n.executors === 1 ? "" : "s") : "nothing"; } }
          ],
          text: function (r) { return [r.n.kind, r.n.id, r.n.host, r.n.type || "", r.n.market || ""].join(" "); }
        }));
      }
    }
    var srcs = el("div", { cls: "logsrcs" });
    (D.logSources || []).forEach(function (x) {
      // Each source folds; one that was not read in full starts open.
      srcs.appendChild(el("details", { cls: "logsrc", open: x.status === "partial" || x.status === "error" }, el("summary", null, el("h3", null, x.name, " ", el("span", { cls: "pill " + ({ read: "full", partial: "part", error: "crit" }[x.status] || "none"), text: SRC_LABEL[x.status] || x.status }), x.class ? el("span", { cls: "sub", text: x.class }) : null)),
        el("div", { cls: "inner" }, x.loc ? el("p", { cls: "mono sub", text: x.loc }) : null, el("p", { text: x.detail }),
        (x.skipped || []).length ? el("details", null, el("summary", { text: num(x.skipped.length + (x.more || 0)) + " objects skipped or unreadable" }), el("div", { cls: "inner" },
          table({ rows: x.skipped, sort: 0, dir: "asc", page: 100, cols: [
            { h: "Object", v: function (o) { return o.location; }, f: function (o) { return el("span", { cls: "mono", text: o.location }); } },
            { h: "Size", num: true, v: function (o) { return o.bytes; }, f: function (o) { return bytes(o.bytes); } },
            { h: "Why", v: function (o) { return o.status + " " + (o.detail || ""); }, f: function (o) { return el("span", { cls: o.status === "error" ? "bad" : null, text: (o.errorClass ? o.errorClass + ": " : "") + (o.detail || o.status) }); } }
          ] }), x.more ? explain(num(x.more) + " more are listed in report.json.") : null)) : null)));
    });
    if ((D.logSources || []).length > 1) s2.appendChild(foldAll(srcs));
    s2.appendChild(srcs);
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

  function notFound(what) { return section(what + " is not in this log", "It is possible that the log was cut off, or that the link is from another run."); }

  // ---------- routing ----------
  // ---------- at a glance ----------
  // The anatomy diagram is drawn in Go (the same picture as the report's);
  // D3 adds zoom and pan, double-click to zoom to a part, and hover that
  // lights up every badge of the same finding.
  views.anatomy = function (arg) {
    var s = section("The run at a glance", "The whole run in one picture. It shows the cluster, what each node offered YARN and the containers on each node, to scale. Inside an executor, it shows the regions of the heap and the peak of each. Click an executor to see inside it.");
    // #anatomy/<id>: that executor drawn in full, above the diagram
    if (arg != null) {
      var panel = (D.anatomyExecutors || {})[arg];
      var box = el("div", { cls: "anatexec" });
      box.appendChild(el("div", { cls: "crumbs" }, link("#anatomy", "The whole run"), " / executor " + arg,
        execByID[arg] ? [" · ", link("#executor/" + encodeURIComponent(arg), "Open executor " + arg + "'s page")] : null));
      if (panel) {
        var pw = el("div", { cls: "anatwrap" });
        pw.innerHTML = panel; // drawn and escaped in Go
        box.appendChild(pw);
      } else {
        box.appendChild(explain("Executor " + arg + " cannot be drawn region by region: its memory settings are only in the event log" + (D.anatomyExecutors ? ", or it is past the page's limit of executors drawn" : "") + "."));
      }
      s.appendChild(box);
    }
    var wrap = el("div", { cls: "anatwrap" });
    wrap.innerHTML = D.anatomy; // drawn and escaped in Go
    var tools = el("div", { cls: "bar-tools anattools" });
    s.appendChild(tools);
    s.appendChild(wrap);
    add(s, guideNodes(D.anatomyGuide || {}));
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
    tools.appendChild(el("span", { cls: "count", text: "Click an executor to see inside it · drag to move · Ctrl + wheel to zoom · double-click a node or panel to zoom to it" }));
    var vb = node.viewBox.baseVal;
    svg.selectAll("g.node, g.jvm, g.rmpanel").on("dblclick", function (ev) {
      ev.preventDefault();
      var b = this.getBBox(), k = Math.min(8, 0.9 * Math.min(vb.width / b.width, vb.height / b.height));
      go(450, zoom.transform, d3.zoomIdentity.translate(vb.width / 2 - k * (b.x + b.width / 2), vb.height / 2 - k * (b.y + b.height / 2)).scale(k));
    });
    // The whole executor card opens it drawn in full, not only its name.
    svg.selectAll("g[data-exec]").style("cursor", "pointer").on("click", function (ev) {
      if (ev.defaultPrevented || ev.target.closest("[data-finding]")) return; // a drag, or a finding's badge
      ev.preventDefault();
      location.hash = "#anatomy/" + encodeURIComponent(this.getAttribute("data-exec"));
    });
    svg.selectAll("[data-finding]")
      .on("mouseenter", function () { svg.selectAll('[data-finding="' + this.getAttribute("data-finding") + '"]').classed("hl", true); })
      .on("mouseleave", function () { svg.selectAll(".hl").classed("hl", false); });
  }
  // #finding/3 opens the overview at that finding.
  views.finding = function (n) {
    var out = views.overview();
    setTimeout(function () { var f = document.getElementById("finding-" + n); if (f) { var d = f.querySelector("details"); if (d) d.open = true; f.scrollIntoView({ block: "start" }); f.classList.add("flash"); } }, 0);
    return out;
  };

  function route() {
    var h = (location.hash || "#overview").slice(1);
    var slash = h.indexOf("/");
    var name = slash < 0 ? h : h.slice(0, slash), arg = null;
    try { arg = slash < 0 ? null : decodeURIComponent(h.slice(slash + 1)); } catch (e) { arg = h.slice(slash + 1); }
    if (!views[name]) { name = "overview"; arg = null; }
    var tab = { job: "jobs", stage: "stages", executor: "executors", query: "sql", finding: "overview", task: "tasks" }[name] || name;
    tabs.querySelectorAll("a").forEach(function (a) { if (a.getAttribute("data-tab") === tab) a.setAttribute("aria-current", "page"); else a.removeAttribute("aria-current"); });
    if (moreBox) {
      // a tab under More shows its name on the menu while it is open
      var inMore = moreTabs.filter(function (t) { return t[0] === tab; })[0];
      moreBox.classList.toggle("cur", !!inMore);
      moreLabel.textContent = inMore ? "More: " + inMore[1] : "More";
    }
    charts.length = 0;
    tipHide();
    main.textContent = "";
    var body = el("div", { cls: "tabbody" });
    add(body, views[name](arg));
    main.appendChild(body);
    headingLevels(body);
    // The contents fill after the charts draw, with their titles; they
    // sit outside the page's column, so the charts keep its full width.
    var toc = tocHeads(body).length + charts.length >= 2 ? pageToc() : null;
    if (toc) main.insertBefore(toc, body);
    drawCharts();
    if (toc && toc.querySelectorAll("li").length < 2) main.removeChild(toc);
    window.scrollTo(0, 0);
  }
  // headingLevels closes any jump in a tab's heading levels (an h4
  // straight under an h2 becomes an h3), so the page's outline has no
  // gaps for screen readers; classes, not tags, give each its look.
  function headingLevels(root) {
    var last = 1;
    [].slice.call(root.querySelectorAll("h2, h3, h4, h5, h6")).forEach(function (h) {
      var lv = +h.tagName.charAt(1);
      if (lv > last + 1) {
        lv = last + 1;
        var n = document.createElement("h" + lv);
        for (var i = 0; i < h.attributes.length; i++) n.setAttribute(h.attributes[i].name, h.attributes[i].value);
        while (h.firstChild) n.appendChild(h.firstChild);
        h.parentNode.replaceChild(n, h);
      }
      last = lv;
    });
  }
  // The contents ("On this page") list a tab's sections and parts: its h2
  // and h3 headings and its charts' titles, not the small labels in guides
  // and findings. A click scrolls to one without changing the address,
  // which names the tab. In the margin beside the page when it is wide
  // enough, else folded above it; none for a tab with fewer than two parts.
  function tocHeads(root) {
    return [].slice.call(root.querySelectorAll("h2, h3, h4, h5, h6")).filter(function (h) {
      if (h.closest(".finding, .fpart, .explain") || /\b(glabel|bnh|k)\b/.test(h.className)) return false;
      if (/\bctitle\b/.test(h.className)) return true;
      return /^H[23]$/.test(h.tagName) && !h.closest(".chart, .xchart, .dagwrap");
    }).filter(function (h) { return tocText(h); });
  }
  // tocText is a heading's own words, without its status pill or note.
  function tocText(h) {
    var c = h.cloneNode(true);
    c.querySelectorAll(".pill, .sub, .n, .note").forEach(function (x) { x.remove(); });
    return c.textContent.replace(/\s+/g, " ").trim();
  }
  function pageToc() {
    var box = el("details", { cls: "ontoc" }, el("summary", { text: "On this page" }), el("ol"));
    box.open = !window.matchMedia || matchMedia("(min-width: 1800px)").matches; // open in the margin, folded above the page
    return el("nav", { cls: "pagetoc", "aria-label": "On this page" }, box);
  }
  // fillToc lists the headings again after each draw, because a redraw
  // makes new chart titles.
  function fillToc() {
    var list = main.querySelector(".pagetoc ol"), body = main.querySelector(".tabbody");
    if (!list || !body) return;
    list.textContent = "";
    tocHeads(body).forEach(function (h) {
      list.appendChild(el("li", { cls: h.tagName === "H2" ? null : "sub" }, el("a", { href: "#", text: tocText(h), onclick: function (ev) {
        ev.preventDefault();
        h.scrollIntoView({ behavior: "smooth", block: "start" });
      } })));
    });
  }
  function drawCharts() {
    charts.forEach(drawSlot);
    var body = main.querySelector(".tabbody");
    if (body) headingLevels(body); // the charts' titles and guides
    fillToc();
  }
  function redrawCharts() { drawCharts(); }
  window.addEventListener("hashchange", route);
  route();
})();

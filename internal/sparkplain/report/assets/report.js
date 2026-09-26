// sparkplain report behaviour. Everything here is optional: the page reads
// correctly without it (times then stay in the zone sparkplain rendered them).
(function () {
  "use strict";
  var root = document.documentElement;

  // Theme toggle, remembered per browser.
  try {
    var saved = localStorage.getItem("sparkplain-theme");
    if (saved === "light" || saved === "dark") root.setAttribute("data-theme", saved);
  } catch (e) {}
  var btn = document.getElementById("themebtn");
  if (btn) {
    btn.hidden = false;
    btn.addEventListener("click", function () {
      var dark = root.getAttribute("data-theme") === "dark" ||
        (!root.getAttribute("data-theme") && window.matchMedia && matchMedia("(prefers-color-scheme: dark)").matches);
      var next = dark ? "light" : "dark";
      root.setAttribute("data-theme", next);
      try { localStorage.setItem("sparkplain-theme", next); } catch (e) {}
    });
  }

  // Times: show every [data-time] in the viewer's own time zone.
  if (window.Intl && Intl.DateTimeFormat) {
    var zone = Intl.DateTimeFormat().resolvedOptions().timeZone || "";
    var opts = {
      hms: { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false },
      hm: { hour: "2-digit", minute: "2-digit", hour12: false },
      dt: { day: "numeric", month: "short", year: "numeric", hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }
    };
    var fmts = {};
    Object.keys(opts).forEach(function (k) { fmts[k] = new Intl.DateTimeFormat(undefined, opts[k]); });
    var abbr = "";
    try {
      var parts = new Intl.DateTimeFormat(undefined, { timeZoneName: "short" }).formatToParts(new Date());
      parts.forEach(function (p) { if (p.type === "timeZoneName") abbr = p.value; });
    } catch (e) {}
    document.querySelectorAll("[data-time]").forEach(function (el) {
      var d = new Date(el.getAttribute("data-time"));
      if (isNaN(d)) return;
      var f = fmts[el.getAttribute("data-fmt") || "hms"] || fmts.hms;
      el.textContent = f.format(d);
      el.setAttribute("title", d.toISOString());
    });
    document.querySelectorAll(".tzlabel").forEach(function (el) {
      el.textContent = (abbr || zone) + (zone && abbr !== zone ? " (" + zone + ", your browser)" : " (your browser)");
    });
  }

  // Sortable tables: click a header with data-sort.
  document.querySelectorAll("table").forEach(function (table) {
    var heads = table.querySelectorAll("th[data-sort]");
    heads.forEach(function (th) {
      th.setAttribute("tabindex", "0");
      th.setAttribute("aria-sort", "none");
      function sort() {
        var idx = Array.prototype.indexOf.call(th.parentNode.children, th);
        var dir = th.getAttribute("data-dir") === "desc" ? "asc" : "desc";
        heads.forEach(function (h) { h.removeAttribute("data-dir"); h.setAttribute("aria-sort", "none"); });
        th.setAttribute("data-dir", dir);
        th.setAttribute("aria-sort", dir === "asc" ? "ascending" : "descending");
        var numeric = th.getAttribute("data-sort") === "num";
        var body = table.tBodies[0];
        var rows = Array.prototype.slice.call(body.rows);
        rows.sort(function (a, b) {
          var x = a.cells[idx], y = b.cells[idx];
          var av = x ? (x.getAttribute("data-v") || x.textContent) : "";
          var bv = y ? (y.getAttribute("data-v") || y.textContent) : "";
          var c = numeric ? (parseFloat(av) || 0) - (parseFloat(bv) || 0) : av.localeCompare(bv, undefined, { numeric: true });
          return dir === "asc" ? c : -c;
        });
        rows.forEach(function (r) { body.appendChild(r); });
      }
      th.addEventListener("click", sort);
      th.addEventListener("keydown", function (e) { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); sort(); } });
    });
  });

  // Configuration search.
  var filter = document.getElementById("configfilter");
  if (filter) {
    filter.hidden = false;
    filter.addEventListener("input", function () {
      var q = filter.value.trim().toLowerCase();
      document.querySelectorAll("#config .cfg tbody tr").forEach(function (tr) {
        tr.hidden = q !== "" && tr.textContent.toLowerCase().indexOf(q) < 0;
      });
      document.querySelectorAll("#config details.cfggroup").forEach(function (d) {
        if (q !== "") d.open = d.querySelector("tbody tr:not([hidden])") !== null;
      });
    });
  }
})();

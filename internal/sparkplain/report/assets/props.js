// Configuration keys in text (spark.executor.memory, and key=value such as
// spark.sql.adaptive.enabled=true) show as code, so they stand out from
// the words around them. Both pages load this before their own script; it
// marks the page once, then what the explorer adds as it draws a tab.
(function () {
  "use strict";
  // A key starts with a known prefix after a space or punctuation, not
  // inside a class name (org.apache.spark.SparkContext) or a path.
  var KEY = /(^|[^\w.$\/-])((?:spark|yarn|hbase|hive|hadoop|fs|dfs|mapreduce|parquet)(?:\.[A-Za-z0-9_-]+)+(?:=[^\s,;()]*[^\s,;().:])?)/g;
  // Text that is already code, or is drawn, stays as it is.
  var SKIP = "code, pre, kbd, samp, script, style, textarea, svg, .mono, .ev, .srcref, .codebox";

  function markText(node) {
    var s = node.nodeValue, parts = [], last = 0, m;
    KEY.lastIndex = 0;
    while ((m = KEY.exec(s))) {
      var at = m.index + m[1].length;
      if (at > last) parts.push(document.createTextNode(s.slice(last, at)));
      var c = document.createElement("code");
      c.className = "prop";
      c.textContent = m[2];
      parts.push(c);
      last = at + m[2].length;
    }
    if (!parts.length) return;
    if (last < s.length) parts.push(document.createTextNode(s.slice(last)));
    var f = document.createDocumentFragment();
    parts.forEach(function (p) { f.appendChild(p); });
    node.parentNode.replaceChild(f, node);
  }

  // markProps marks the keys in the text under root.
  function markProps(root) {
    var start = root.nodeType === 1 ? root : root.parentNode;
    if (!start || (start.closest && start.closest(SKIP))) return;
    if (root.nodeType === 3) { markText(root); return; }
    if (root.nodeType !== 1) return;
    var w = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode: function (n) {
        if (!/(spark|yarn|hbase|hive|hadoop|fs|dfs|mapreduce|parquet)\./.test(n.nodeValue)) return NodeFilter.FILTER_REJECT;
        return n.parentNode.closest(SKIP) ? NodeFilter.FILTER_REJECT : NodeFilter.FILTER_ACCEPT;
      }
    });
    var found = [];
    while (w.nextNode()) found.push(w.currentNode);
    found.forEach(markText);
  }

  markProps(document.body);
  if (window.MutationObserver) {
    new MutationObserver(function (records) {
      records.forEach(function (r) { r.addedNodes.forEach(function (n) { if (n.parentNode) markProps(n); }); });
    }).observe(document.body, { childList: true, subtree: true });
  }
})();

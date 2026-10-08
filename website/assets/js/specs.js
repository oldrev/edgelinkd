(function () {
  "use strict";

  var page = document.querySelector("[data-specs-index-url]");
  if (!page) return;
  var indexUrl = page.getAttribute("data-specs-index-url");
  var version = document.getElementById("spec-version");
  var error = document.getElementById("spec-error");
  var dashboard = document.getElementById("spec-dashboard");
  var current = null;
  var reports = {};

  function el(tag, cls, text) {
    var node = document.createElement(tag);
    if (cls) node.className = cls;
    if (text !== undefined) node.textContent = text;
    return node;
  }

  function totals(report) {
    var t = report.totals || {};
    return { upstream: t.js || 0, covered: t.covered || 0, skipped: t.skipped || 0,
      missing: t.missing || 0, extra: t.extra || 0 };
  }

  function pct(value, total) { return total ? Math.round(value / total * 1000) / 10 : 0; }

  function renderSummary(report) {
    var t = totals(report), summary = document.getElementById("spec-summary");
    summary.replaceChildren();
    [["Upstream tests", t.upstream, ""], ["Covered", t.covered, "is-covered"],
      ["Skipped", t.skipped, "is-skipped"], ["Missing", t.missing, "is-missing"],
      ["Extra", t.extra, "is-extra"]].forEach(function (item) {
        var card = el("div", "spec-summary-card " + item[2]);
        card.appendChild(el("span", "spec-summary-label", item[0]));
        card.appendChild(el("strong", "spec-summary-value", String(item[1])));
        summary.appendChild(card);
      });
    document.getElementById("spec-coverage-label").textContent = pct(t.covered, t.upstream) + "% covered";
  }

  function categoryTotals(category) {
    var out = { covered: 0, skipped: 0, missing: 0, extra: 0 };
    category.nodes.forEach(function (node) {
      var t = node.totals || {};
      Object.keys(out).forEach(function (key) { out[key] += t[key] || 0; });
    });
    return out;
  }

  function renderCategories(report) {
    var host = document.getElementById("spec-category-chart");
    host.replaceChildren();
    (report.categories || []).forEach(function (category) {
      var t = categoryTotals(category), total = t.covered + t.skipped + t.missing;
      var row = el("div", "spec-category-row"), head = el("div", "spec-category-head");
      head.appendChild(el("strong", "", category.category));
      head.appendChild(el("span", "", t.covered + "/" + total));
      row.appendChild(head);
      var bar = el("div", "spec-bar");
      ["covered", "skipped", "missing"].forEach(function (key) {
        if (t[key]) { var part = el("span", "spec-bar-" + key); part.style.width = pct(t[key], total) + "%"; bar.appendChild(part); }
      });
      row.appendChild(bar);
      host.appendChild(row);
    });
  }

  function flatten(report) {
    var out = [];
    (report.categories || []).forEach(function (category) {
      (category.nodes || []).forEach(function (node) {
        (node.specs || []).forEach(function (spec) {
          out.push({ category: category.category, node: node.node, title: spec.title, status: spec.status });
        });
      });
    });
    return out;
  }

  function renderNodes(report) {
    var host = document.getElementById("spec-node-list"), query = (document.getElementById("spec-node-search").value || "").toLowerCase();
    var status = document.getElementById("spec-status-filter").value;
    host.replaceChildren();
    (report.categories || []).forEach(function (category) {
      (category.nodes || []).forEach(function (node) {
        var visible = !query || node.node.toLowerCase().indexOf(query) >= 0;
        var specs = (node.specs || []).filter(function (spec) { return status === "all" || spec.status === status; });
        if (!visible || !specs.length) return;
        var t = node.totals || {}, total = (t.covered || 0) + (t.skipped || 0) + (t.missing || 0);
        var row = el("div", "spec-node-row"), title = el("div", "spec-node-title");
        title.appendChild(el("strong", "", node.node));
        title.appendChild(el("span", "", category.category + " · " + (t.covered || 0) + "/" + total));
        row.appendChild(title);
        var bar = el("div", "spec-bar");
        ["covered", "skipped", "missing"].forEach(function (key) { if (t[key]) { var part = el("span", "spec-bar-" + key); part.style.width = pct(t[key], total) + "%"; bar.appendChild(part); } });
        row.appendChild(bar); host.appendChild(row);
      });
    });
  }

  function renderTests(report) {
    var query = (document.getElementById("spec-test-search").value || "").toLowerCase();
    var rows = flatten(report).filter(function (item) { return !query || item.title.toLowerCase().indexOf(query) >= 0; });
    var tbody = document.getElementById("spec-test-table"); tbody.replaceChildren();
    rows.slice(0, 250).forEach(function (item) {
      var tr = document.createElement("tr");
      [item.status, item.category, item.node, item.title].forEach(function (value, index) {
        var td = el("td", index === 0 ? "spec-status spec-status-" + value : "", value); tr.appendChild(td);
      });
      tbody.appendChild(tr);
    });
    document.getElementById("spec-test-count").textContent = rows.length + " tests" + (rows.length > 250 ? " (showing 250)" : "");
  }

  function render(report, meta) {
    current = report; dashboard.hidden = false; error.hidden = true;
    document.getElementById("spec-meta").textContent = (meta.ref || report.ref || "") + " · " + (report.node_red && report.node_red.version ? "Node-RED " + report.node_red.version + " · " : "") + (report.generated_at || "");
    renderSummary(report); renderCategories(report); renderNodes(report); renderTests(report);
  }

  function loadReport(name) {
    var meta = reports[name]; if (!meta) return;
    version.disabled = true;
    fetch(new URL(meta.url, indexUrl).toString(), { cache: "no-cache" }).then(function (response) { if (!response.ok) throw new Error("Report unavailable"); return response.json(); }).then(function (report) { render(report, meta); version.disabled = false; }).catch(function (e) { error.textContent = e.message; error.hidden = false; dashboard.hidden = true; version.disabled = false; });
  }

  fetch(indexUrl, { cache: "no-cache" }).then(function (response) { if (!response.ok) throw new Error("Coverage index unavailable"); return response.json(); }).then(function (index) {
    (index.reports || []).forEach(function (item) { reports[item.name] = item; var option = el("option", "", item.name); option.value = item.name; version.appendChild(option); });
    version.replaceChildren.apply(version, Array.prototype.slice.call(version.children));
    version.disabled = false;
    if (!index.reports || !index.reports.length) throw new Error("No coverage reports published yet");
    version.value = reports.master ? "master" : index.reports[0].name; loadReport(version.value);
  }).catch(function (e) { error.textContent = e.message; error.hidden = false; });

  version.addEventListener("change", function () { loadReport(version.value); });
  ["spec-node-search", "spec-status-filter"].forEach(function (id) { document.getElementById(id).addEventListener("input", function () { if (current) renderNodes(current); }); });
  document.getElementById("spec-test-search").addEventListener("input", function () { if (current) renderTests(current); });
})();

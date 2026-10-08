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

  var statusColors = {
    covered: "#25803d", skipped: "#c5a227", missing: "#c32d38", extra: "#3d6db5"
  };

  function svgEl(name, attrs) {
    var node = document.createElementNS("http://www.w3.org/2000/svg", name);
    Object.keys(attrs || {}).forEach(function (key) { node.setAttribute(key, attrs[key]); });
    return node;
  }

  function splitLayout(items, x, y, width, height) {
    if (!items.length) return [];
    if (items.length === 1) return [{ item: items[0], x: x, y: y, width: width, height: height }];
    var total = items.reduce(function (sum, item) { return sum + Math.max(item.weight, 1); }, 0);
    var target = total / 2, running = 0, split = 1, best = Infinity;
    items.slice(0, -1).forEach(function (item, index) {
      running += Math.max(item.weight, 1);
      var distance = Math.abs(target - running);
      if (distance < best) { best = distance; split = index + 1; }
    });
    var first = items.slice(0, split), second = items.slice(split);
    var firstWeight = first.reduce(function (sum, item) { return sum + Math.max(item.weight, 1); }, 0);
    var ratio = firstWeight / total;
    if (width >= height) {
      return splitLayout(first, x, y, width * ratio, height).concat(
        splitLayout(second, x + width * ratio, y, width * (1 - ratio), height));
    }
    return splitLayout(first, x, y, width, height * ratio).concat(
      splitLayout(second, x, y + height * ratio, width, height * (1 - ratio)));
  }

  function testGrid(count, x, y, width, height) {
    if (!count) return [];
    var gap = 1, columns = Math.max(1, Math.ceil(Math.sqrt(count * width / Math.max(height, 1))));
    var rows = Math.ceil(count / columns);
    var side = Math.max(1, Math.min((width - gap * (columns - 1)) / columns,
      (height - gap * (rows - 1)) / rows));
    return Array.from({ length: count }, function (_, index) {
      return { x: x + (index % columns) * (side + gap), y: y + Math.floor(index / columns) * (side + gap), side: side };
    });
  }

  function addSvgText(parent, text, x, y, attrs) {
    var label = svgEl("text", Object.assign({ x: x, y: y }, attrs || {}));
    label.textContent = text;
    parent.appendChild(label);
  }

  function renderCategories(report) {
    var host = document.getElementById("spec-category-chart");
    host.replaceChildren();
    var svg = svgEl("svg", { viewBox: "0 0 1200 680", role: "img", "aria-labelledby": "spec-map-title spec-map-desc" });
    var title = svgEl("title", { id: "spec-map-title" }); title.textContent = "Node-RED specification coverage map"; svg.appendChild(title);
    var desc = svgEl("desc", { id: "spec-map-desc" }); desc.textContent = "Each square represents one test. Frames group categories and nodes."; svg.appendChild(desc);
    svg.appendChild(svgEl("rect", { x: 0, y: 0, width: 1200, height: 680, rx: 8, fill: "var(--surface-2)" }));
    var categories = (report.categories || []).map(function (category) {
      return { value: category, weight: Math.max(1, (category.nodes || []).reduce(function (sum, node) { return sum + (node.specs || []).length; }, 0)) };
    });
    splitLayout(categories, 8, 8, 1184, 664).forEach(function (categoryBox) {
      var category = categoryBox.item.value, cx = categoryBox.x, cy = categoryBox.y;
      var cw = categoryBox.width, ch = categoryBox.height;
      svg.appendChild(svgEl("rect", { x: cx, y: cy, width: cw, height: ch, rx: 5, fill: "var(--surface)", stroke: "var(--line-strong)", "stroke-width": 2 }));
      if (cw > 100 && ch > 35) addSvgText(svg, category.category, cx + 9, cy + 20, { fill: "var(--text)", "font-size": 15, "font-weight": 700 });
      var innerX = cx + 7, innerY = cy + (ch > 35 ? 28 : 5), innerW = Math.max(cw - 14, 1), innerH = Math.max(ch - (ch > 35 ? 34 : 10), 1);
      var nodes = (category.nodes || []).map(function (node) { return { value: node, weight: Math.max(1, (node.specs || []).length) }; });
      splitLayout(nodes, innerX, innerY, innerW, innerH).forEach(function (nodeBox) {
        var node = nodeBox.item.value, nx = nodeBox.x, ny = nodeBox.y, nw = nodeBox.width, nh = nodeBox.height;
        var nodeRect = svgEl("rect", { x: nx, y: ny, width: nw, height: nh, rx: 3, fill: "var(--surface-2)", stroke: "var(--line-strong)", "stroke-width": 1 });
        var nodeTotals = node.totals || {};
        var nodeTitle = category.category + " / " + node.node + " · " + (nodeTotals.covered || 0) + "/" + ((nodeTotals.covered || 0) + (nodeTotals.skipped || 0) + (nodeTotals.missing || 0)) + " covered";
        var nodeTip = svgEl("title", {}); nodeTip.textContent = nodeTitle; nodeRect.appendChild(nodeTip); svg.appendChild(nodeRect);
        if (nw > 70 && nh > 24) addSvgText(svg, node.node, nx + 4, ny + 15, { fill: "var(--text-2)", "font-size": 11, "font-weight": 650 });
        var gridX = nx + 4, gridY = ny + (nh > 24 ? 20 : 3), gridW = Math.max(nw - 8, 1), gridH = Math.max(nh - (nh > 24 ? 24 : 6), 1);
        testGrid((node.specs || []).length, gridX, gridY, gridW, gridH).forEach(function (cell, index) {
          var spec = node.specs[index];
          var square = svgEl("rect", { x: cell.x, y: cell.y, width: cell.side, height: cell.side, rx: 0.8, fill: statusColors[spec.status] || statusColors.extra });
          var tip = svgEl("title", {}); tip.textContent = node.node + " · " + spec.title + " · " + spec.status; square.appendChild(tip); svg.appendChild(square);
        });
      });
    });
    host.appendChild(svg);
  }

  function piePath(cx, cy, radius, start, end) {
    var x1 = cx + radius * Math.cos(start), y1 = cy + radius * Math.sin(start);
    var x2 = cx + radius * Math.cos(end), y2 = cy + radius * Math.sin(end);
    return ["M", cx, cy, "L", x1, y1, "A", radius, radius, 0, end - start > Math.PI ? 1 : 0, 1, x2, y2, "Z"].join(" ");
  }

  function renderPie(report) {
    var host = document.getElementById("spec-pie-chart"); host.replaceChildren();
    var t = totals(report), values = [
      { key: "covered", label: "Covered", value: t.covered }, { key: "skipped", label: "Skipped", value: t.skipped },
      { key: "missing", label: "Missing", value: t.missing }, { key: "extra", label: "Extra", value: t.extra }
    ], total = values.reduce(function (sum, item) { return sum + item.value; }, 0);
    var wrap = el("div", "spec-pie-layout"), svg = svgEl("svg", { viewBox: "0 0 240 240", role: "img", "aria-label": "Test status distribution" });
    svg.appendChild(svgEl("circle", { cx: 120, cy: 120, r: 96, fill: "var(--surface-2)" }));
    var angle = -Math.PI / 2;
    values.forEach(function (item) {
      if (!item.value || !total) return;
      var end = angle + Math.PI * 2 * item.value / total;
      var path = svgEl("path", { d: piePath(120, 120, 96, angle, end), fill: statusColors[item.key] });
      var tip = svgEl("title", {}); tip.textContent = item.label + ": " + item.value + " (" + pct(item.value, total) + "%)"; path.appendChild(tip); svg.appendChild(path); angle = end;
    });
    wrap.appendChild(svg);
    var legend = el("div", "spec-pie-legend");
    values.forEach(function (item) {
      var row = el("div", "spec-pie-legend-row"), swatch = el("span", "spec-pie-swatch"); swatch.style.background = statusColors[item.key];
      row.appendChild(swatch); row.appendChild(el("span", "", item.label)); row.appendChild(el("strong", "", String(item.value))); legend.appendChild(row);
    });
    wrap.appendChild(legend); host.appendChild(wrap);
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
        var statuses = el("div", "spec-node-statuses");
        ["covered", "skipped", "missing", "extra"].forEach(function (key) {
          if (t[key]) statuses.appendChild(el("span", "spec-node-status spec-node-status-" + key, key + " " + t[key]));
        });
        row.appendChild(statuses); host.appendChild(row);
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
    renderSummary(report); renderCategories(report); renderPie(report); renderNodes(report); renderTests(report);
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

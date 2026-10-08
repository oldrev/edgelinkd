#!/bin/python3

import re
import argparse
import ast
import difflib
import os
import json
import shutil
import subprocess
import sys
import tempfile
import pytest
import io
import contextlib
import platform
from math import ceil, sqrt
from pathlib import Path
from datetime import datetime, timezone
from xml.sax.saxutils import escape as xml_escape

from colorama import init as colorama_init
from colorama import Fore
from colorama import Style


_SCRIPT_PATH = os.path.abspath(__file__)
_SCRIPT_DIR = os.path.dirname(_SCRIPT_PATH)
TESTS_DIR = os.path.join(_SCRIPT_DIR, '..', "tests")

JS_IT_PATTERN = re.compile(r"""^\s*it\s*\(\s*(['"].*?['"]+)\s*,\s*""")
PY_IT_PATTERN = re.compile(r"""\@.*it\s*\(\s*(['"].*?['"]+)\s*\)\s*""")

# The markers below are Unicode; a console with a legacy code page (GBK, cp1252, ...) cannot
# encode them and would abort the run half way through. Fall back to ASCII when that happens.
try:
    '✓×'.encode(sys.stdout.encoding or 'utf-8')
    CHECK, CROSS = '✓', '×'
except (UnicodeEncodeError, LookupError):
    CHECK, CROSS = 'ok', 'x'


def load_json(json_path):
    with open(json_path, 'r', encoding='utf-8') as fp:
        return json.load(fp)


def extract_it_strings_js(red_dir, file_path) -> list[str]:
    specs = []
    # `mocha` is invoked from inside the Node-RED checkout, so resolve both paths first: a
    # relative checkout path (as documented in AGENTS.md) would otherwise be looked up relative
    # to the checkout itself and match no test file at all.
    red_dir = os.path.abspath(red_dir)
    file_path = os.path.abspath(file_path)
    # Use delete=False to avoid permission issues on Windows, and close the handle before the
    # subprocess runs: Windows will not let mocha open a file this process still holds.
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as report_file:
        report_file_path = report_file.name
        report_file.close()

    original_cwd = os.getcwd()
    os.chdir(red_dir)
    try:
        # Pass argv directly. On Windows, ``shell=True`` with a list only executes the
        # first item (``mocha``) and drops the test path/options, leaving an empty report.
        local_mocha = os.path.join(red_dir, "node_modules", ".bin", "mocha.cmd" if platform.system() == "Windows" else "mocha")
        mocha = local_mocha if os.path.exists(local_mocha) else "mocha"
        result = subprocess.run([
            mocha,
            os.path.relpath(file_path, red_dir), "--dry-run", "--reporter=json", "--exit",
            "--reporter-options", f"output={report_file_path}"
        ], cwd=red_dir, capture_output=True, text=True)

        # Read the report after mocha finishes
        if result.returncode != 0:
            raise RuntimeError(
                f"mocha failed for {file_path} (exit {result.returncode}):\n"
                f"{result.stdout}\n{result.stderr}"
            )
        if os.path.exists(report_file_path) and os.path.getsize(report_file_path) > 0:
            report = load_json(report_file_path)
            for test in report['tests']:
                specs.append(test['fullTitle'].rstrip())
        else:
            raise RuntimeError(f"mocha produced no JSON report for {file_path}")
    finally:
        os.chdir(original_cwd)
        # Clean up the temporary file
        try:
            if os.path.exists(report_file_path):
                os.unlink(report_file_path)
        except OSError:
            pass  # Ignore cleanup errors

    return specs


def extract_it_strings_py(file_path) -> list[dict]:
    specs = []
    skipped_tests = _skipped_test_keys(file_path)
    # Use delete=False to avoid permission issues on Windows, and close the handle before pytest
    # runs: Windows will not let it open a file this process still holds.
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as report_file:
        report_file_path = report_file.name
        report_file.close()

    output_capture = io.StringIO()
    with contextlib.redirect_stdout(output_capture), contextlib.redirect_stderr(output_capture):
        pytest.main(["-q", "--co", "--disable-warnings", "-p", "no:skip",
                    "--json-report", f"--json-report-file={report_file_path}", file_path])

    try:
        # Read the report after pytest finishes
        if os.path.exists(report_file_path):
            report = load_json(report_file_path)
            for coll in report['collectors']:
                for result in coll['result']:
                    if "title" in result:
                        node_parts = result.get("nodeid", "").split("::")
                        function_name = node_parts[-1].split("[", 1)[0]
                        if len(node_parts) >= 3:
                            test_key = f"{node_parts[-2]}::{function_name}"
                        else:
                            test_key = function_name
                        specs.append({
                            "title": result['fullTitle'].rstrip(),
                            "skipped": test_key in skipped_tests,
                        })
    finally:
        # Clean up the temporary file
        try:
            if os.path.exists(report_file_path):
                os.unlink(report_file_path)
        except OSError:
            pass  # Ignore cleanup errors

    return specs


def read_json() -> list[list]:
    json_path = os.path.join(_SCRIPT_DIR, 'specs_diff.json')
    with open(json_path, 'r', encoding='utf-8') as file:
        json_text = file.read()
        return json.loads(json_text)


def print_sep(text=''):
    terminal_size = shutil.get_terminal_size()
    filled_text = text.ljust(terminal_size.columns, '-')
    print(filled_text)

def print_subtitle(text=''):
    terminal_size = shutil.get_terminal_size()
    filled_text = text.ljust(terminal_size.columns, '.')
    print(filled_text)


STATUS_INFO = {
    "covered": {
        "symbol": " ",
        "label": "Covered",
        "markdown": ":white_check_mark:",
        "color": "#43a047",
    },
    "missing": {
        "symbol": "-",
        "label": "Missing from EdgeLinkd",
        "markdown": ":x:",
        "color": "#e53935",
    },
    "extra": {
        "symbol": "+",
        "label": "Extra in EdgeLinkd",
        "markdown": ":large_blue_circle:",
        "color": "#1e88e5",
    },
    "skipped": {
        "symbol": "~",
        "label": "Skipped",
        "markdown": ":grey_question:",
        "color": "#9e9e9e",
    },
}


def _has_skip_decorator(decorators):
    for decorator in decorators:
        target = decorator.func if isinstance(decorator, ast.Call) else decorator
        if isinstance(target, ast.Attribute) and target.attr in ("skip", "skipif"):
            return True
    return False


def _contains_runtime_skip(node):
    return any(
        isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and isinstance(call.func.value, ast.Name)
        and call.func.value.id == "pytest"
        and call.func.attr == "skip"
        for call in ast.walk(node)
    )


def _skipped_test_keys(file_path):
    """Find test methods skipped by a marker or an explicit ``pytest.skip()`` call."""
    with open(file_path, "r", encoding="utf-8") as source:
        tree = ast.parse(source.read(), filename=file_path)
    skipped = set()
    for class_node in (node for node in tree.body if isinstance(node, ast.ClassDef)):
        class_skipped = _has_skip_decorator(class_node.decorator_list)
        for child in class_node.body:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
                class_skipped or _has_skip_decorator(child.decorator_list) or _contains_runtime_skip(child)
            ):
                skipped.add(f"{class_node.name}::{child.name}")
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and (
            _has_skip_decorator(node.decorator_list) or _contains_runtime_skip(node)
        ):
            skipped.add(node.name)
    return skipped


def compare_specs(js_specs, py_specs):
    """Return one status record for every upstream or ported ``it()`` test.

    ``difflib.Differ`` emits ``?`` helper lines for character-level hints. Those lines are
    useful in a terminal diff, but they are not tests and made the old Markdown report lose
    information. SequenceMatcher gives us a stable, test-level comparison instead.
    """
    py_records = [
        spec if isinstance(spec, dict) else {"title": spec, "skipped": False}
        for spec in py_specs
    ]
    py_titles = [spec["title"] for spec in py_records]
    records = []
    matcher = difflib.SequenceMatcher(a=js_specs, b=py_titles, autojunk=False)
    for tag, js_start, js_end, py_start, py_end in matcher.get_opcodes():
        if tag == "equal":
            records.extend(
                {
                    "status": "skipped" if py_records[index]["skipped"] else "covered",
                    "title": title,
                }
                for index, title in zip(range(py_start, py_end), js_specs[js_start:js_end])
            )
        elif tag in ("delete", "replace"):
            records.extend({"status": "missing", "title": title} for title in js_specs[js_start:js_end])
        if tag in ("insert", "replace"):
            records.extend(
                {
                    "status": "skipped" if spec["skipped"] else "extra",
                    "title": spec["title"],
                }
                for spec in py_records[py_start:py_end]
            )
    return records


def report_totals(report):
    totals = {"covered": 0, "missing": 0, "extra": 0, "skipped": 0}
    for category in report:
        for node in category["nodes"]:
            for test in node["specs"]:
                totals[test["status"]] += 1
    totals["js"] = totals["covered"] + totals["skipped"] + totals["missing"]
    totals["python"] = totals["covered"] + totals["skipped"] + totals["extra"]
    return totals


def _markdown_escape(text):
    return text.replace("|", "\\|").replace("\n", " ")


def generate_markdown_table(rows):
    headers = ["Status", "Spec Test"]
    table = "| " + " | ".join(headers) + " |\n"
    table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    for row in rows:
        status = row["status"]
        marker = STATUS_INFO[status]["markdown"]
        title = _markdown_escape(row["title"])
        if status == "covered":
            title = f"~~{title}~~"
        elif status == "missing":
            title = f"**{title}**"
        table += f"| {marker} | {title} |\n"
    return table


def _split_layout(items, x, y, width, height):
    """Lay out weighted items with recursive, deterministic rectangles.

    This is a compact slice-and-dice treemap. It keeps the hierarchy visible while leaving
    each node enough room for its grid of leaf squares.
    """
    if not items:
        return []
    if len(items) == 1:
        return [(items[0][0], x, y, width, height)]

    total = sum(item[1] for item in items) or len(items)
    target = total / 2
    running = 0
    split_at = 1
    best_distance = float("inf")
    for index, item in enumerate(items[:-1], start=1):
        running += item[1]
        distance = abs(target - running)
        if distance < best_distance:
            best_distance = distance
            split_at = index

    first = items[:split_at]
    second = items[split_at:]
    first_weight = sum(item[1] for item in first)
    ratio = first_weight / total
    if width >= height:
        first_width = width * ratio
        return (
            _split_layout(first, x, y, first_width, height)
            + _split_layout(second, x + first_width, y, width - first_width, height)
        )
    first_height = height * ratio
    return (
        _split_layout(first, x, y, width, first_height)
        + _split_layout(second, x, y + first_height, width, height - first_height)
    )


def _test_grid_layout(count, x, y, width, height):
    if count <= 0:
        return []
    gap = 1.0
    available_width = max(width - gap, 1)
    available_height = max(height - gap, 1)
    columns = max(1, ceil(sqrt(count * available_width / available_height)))
    rows = ceil(count / columns)
    side = max(1, min(
        (available_width - gap * (columns - 1)) / columns,
        (available_height - gap * (rows - 1)) / rows,
    ))
    cells = []
    for index in range(count):
        column = index % columns
        row = index // columns
        cells.append((x + column * (side + gap), y + row * (side + gap), side))
    return cells


def _svg_text(text, x, y, size, fill="#263238", weight="normal", anchor="start"):
    return (
        f'<text x="{x:.2f}" y="{y:.2f}" font-family="system-ui,Segoe UI,sans-serif" '
        f'font-size="{size}px" fill="{fill}" font-weight="{weight}" text-anchor="{anchor}">'
        f"{xml_escape(str(text))}</text>"
    )


def generate_svg(report, totals, width=1800, height=1100):
    """Generate a self-contained nested-squares SVG for a report."""
    margin = 28
    header_height = 92
    chart_x = margin
    chart_y = header_height
    chart_width = width - margin * 2
    chart_height = height - header_height - margin
    category_items = [
        (category["category"], sum(len(node["specs"]) for node in category["nodes"]) or 1)
        for category in report
    ]
    category_rects = {item[0]: item[1:] for item in _split_layout(category_items, chart_x, chart_y, chart_width, chart_height)}
    parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {width} {height}" '
        'role="img" aria-labelledby="title description">',
        '<title id="title">Node-RED specification coverage</title>',
        '<desc id="description">Nested squares: each small square is one it test; frames group categories and nodes.</desc>',
        f'<rect width="{width}" height="{height}" fill="#f5f7f9"/>',
        _svg_text("Node-RED specification coverage", margin, 34, 26, weight="700"),
        _svg_text(
            f'{totals["covered"]} covered, {totals["skipped"]} skipped / {totals["js"]} upstream tests '
            f'({totals["covered"] / totals["js"]:.1%} of upstream)',
            margin,
            64,
            16,
            fill="#546e7a",
        ) if totals["js"] else _svg_text("No upstream tests found", margin, 64, 16, fill="#546e7a"),
    ]

    legend_x = width - margin
    legend_y = 30
    legend_entries = [("covered", "Covered"), ("skipped", "Skipped"), ("missing", "Missing"), ("extra", "Extra")]
    for index, (status, label) in enumerate(legend_entries):
        x = legend_x - (len(legend_entries) - index) * 112
        parts.append(f'<rect x="{x:.2f}" y="{legend_y:.2f}" width="14" height="14" rx="2" fill="{STATUS_INFO[status]["color"]}"/>')
        parts.append(_svg_text(label, x + 20, legend_y + 12, 13, fill="#455a64"))

    for category in report:
        category_name = category["category"]
        category_x, category_y, category_width, category_height = category_rects[category_name]
        parts.append(
            f'<g class="category" data-category="{xml_escape(category_name)}">'
            f'<title>{xml_escape(category_name)}</title>'
            f'<rect x="{category_x:.2f}" y="{category_y:.2f}" width="{category_width:.2f}" height="{category_height:.2f}" '
            'fill="#ffffff" fill-opacity="0.65" stroke="#455a64" stroke-width="2"/>'
        )
        if category_width >= 110 and category_height >= 28:
            parts.append(_svg_text(category_name, category_x + 8, category_y + 20, 15, fill="#263238", weight="700"))

        inner_x = category_x + 8
        inner_y = category_y + 28
        inner_width = max(category_width - 16, 1)
        inner_height = max(category_height - 36, 1)
        node_items = [(node["node"], max(len(node["specs"]), 1)) for node in category["nodes"]]
        node_rects = {item[0]: item[1:] for item in _split_layout(node_items, inner_x, inner_y, inner_width, inner_height)}
        for node in category["nodes"]:
            node_name = node["node"]
            node_x, node_y, node_width, node_height = node_rects[node_name]
            parts.append(
                f'<g class="node" data-node="{xml_escape(node_name)}">'
                f'<title>{xml_escape(category_name)} / {xml_escape(node_name)}</title>'
                f'<rect x="{node_x:.2f}" y="{node_y:.2f}" width="{node_width:.2f}" height="{node_height:.2f}" '
                'fill="#eceff1" fill-opacity="0.72" stroke="#90a4ae" stroke-width="1.5"/>'
            )
            if node_width >= 76 and node_height >= 24:
                parts.append(_svg_text(node_name, node_x + 5, node_y + 16, 12, fill="#37474f", weight="600"))
            cell_x = node_x + 5
            cell_y = node_y + (22 if node_height >= 24 else 3)
            cell_width = max(node_width - 10, 1)
            cell_height = max(node_height - (27 if node_height >= 24 else 8), 1)
            for test, (x, y, side) in zip(node["specs"], _test_grid_layout(len(node["specs"]), cell_x, cell_y, cell_width, cell_height)):
                status = test["status"]
                title = f'{category_name} / {node_name} / {test["title"]} — {STATUS_INFO[status]["label"]}'
                parts.append(
                    f'<rect x="{x:.2f}" y="{y:.2f}" width="{side:.2f}" height="{side:.2f}" rx="1" '
                    f'fill="{STATUS_INFO[status]["color"]}" stroke="#ffffff" stroke-width="0.5">'
                    f'<title>{xml_escape(title)}</title></rect>'
                )
            parts.append("</g>")
        parts.append("</g>")
    parts.extend([
        f'<rect x="{chart_x:.2f}" y="{chart_y:.2f}" width="{chart_width:.2f}" height="{chart_height:.2f}" '
        'fill="none" stroke="#263238" stroke-width="2" pointer-events="none"/>',
        "</svg>",
    ])
    return "\n".join(parts) + "\n"


def write_report(markdown_path, svg_path, report, totals, svg_width, svg_height):
    if markdown_path:
        markdown_file = Path(markdown_path)
        markdown_file.parent.mkdir(parents=True, exist_ok=True)
        with markdown_file.open("w", encoding="utf-8") as md_file:
            md_file.write("# Node-RED Spec Tests Diff\n")
            md_file.write(
                "This file is automatically generated by `scripts/specs_diff.py`. "
                "The report compares EdgeLinkd's ported tests with the pinned Node-RED tests.\n\n"
            )
            md_file.write(
                f"**Coverage:** {totals['covered']} covered, {totals['skipped']} skipped, "
                f"{totals['missing']} missing, {totals['extra']} extra "
                f"({totals['covered']}/{totals['js']} upstream tests executed).\n\n"
            )
            if svg_path:
                svg_link = os.path.relpath(svg_path, markdown_file.parent).replace(os.sep, "/")
                md_file.write(f"![Nested squares coverage chart]({svg_link})\n\n")
            md_file.write("The nested-squares chart uses one small square per `it()` test. "
                          "Frames group categories and nodes; green is covered, gray is skipped, red is missing, and blue is extra.\n\n")
            first_section = True
            for category in report:
                if not first_section:
                    md_file.write("\n")
                first_section = False
                md_file.write(f"## {category['category']}\n")
                for node in category["nodes"]:
                    md_file.write(f"### {node['node']}\n")
                    md_file.write(generate_markdown_table(node["specs"]))
    if svg_path:
        svg_file = Path(svg_path)
        svg_file.parent.mkdir(parents=True, exist_ok=True)
        svg_file.write_text(generate_svg(report, totals, svg_width, svg_height), encoding="utf-8")


def _git_value(args, default=None):
    """Read repository metadata without making report generation depend on Git."""
    try:
        return subprocess.check_output(["git", *args], text=True, stderr=subprocess.DEVNULL).strip() or default
    except (OSError, subprocess.CalledProcessError):
        return default


def write_json_report(json_path, report, totals, nr_path, label=None, commit=None):
    """Write the stable, machine-readable report consumed by the project website."""
    path = Path(json_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    package_path = Path(nr_path) / "package.json"
    node_red_version = None
    if package_path.exists():
        try:
            node_red_version = json.loads(package_path.read_text(encoding="utf-8")).get("version")
        except (OSError, json.JSONDecodeError):
            pass

    enriched = []
    for category in report:
        nodes = []
        for node in category["nodes"]:
            node_totals = {status: sum(test["status"] == status for test in node["specs"])
                           for status in STATUS_INFO}
            nodes.append({
                "node": node["node"],
                "python_spec_file": node.get("python_spec_file"),
                "node_red_spec_file": node.get("node_red_spec_file"),
                "totals": node_totals,
                "specs": node["specs"],
            })
        enriched.append({"category": category["category"], "nodes": nodes})

    payload = {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "ref": label or _git_value(["branch", "--show-current"], "unknown"),
        "commit": commit or _git_value(["rev-parse", "HEAD"]),
        "node_red": {"version": node_red_version},
        "totals": {**totals, "coverage": totals["covered"] / totals["js"] if totals["js"] else 0},
        "categories": enriched,
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Compare Node-RED and EdgeLinkd it() specs and optionally write Markdown and a nested-squares SVG chart.")
    parser.add_argument('NR_PATH', type=str,
                        help="Path to the directory of Node-RED")
    parser.add_argument('-o', "--output", type=str, default=None,
                        help="The output path to a Markdown file")
    parser.add_argument('--svg-output', type=str, default=None,
                        help="Write a nested-squares SVG chart to this path")
    parser.add_argument('--json-output', type=str, default=None,
                        help="Write a machine-readable JSON report to this path")
    parser.add_argument('--label', type=str, default=None,
                        help="Branch or release label to store in the JSON report")
    parser.add_argument('--commit', type=str, default=None,
                        help="Commit SHA to store in the JSON report")
    parser.add_argument('--no-fail', action='store_true',
                        help="Return success even when upstream tests are missing")
    parser.add_argument('--svg-width', type=int, default=1400,
                        help="SVG canvas width in pixels (default: 1400)")
    parser.add_argument('--svg-height', type=int, default=900,
                        help="SVG canvas height in pixels (default: 900)")
    args = parser.parse_args()

    if args.svg_width < 400 or args.svg_height < 300:
        parser.error("--svg-width must be at least 400 and --svg-height at least 300")

    colorama_init()

    categories = read_json()

    report = []
    for cat in categories:
        md_cat = {"category": cat["category"], "nodes": []}
        for triple in cat["nodes"]:
            md_node = {
                "node": triple[0],
                "python_spec_file": triple[1],
                "node_red_spec_file": triple[2],
                "specs": [],
            }
            py_path = os.path.join(os.path.normpath(os.path.join(TESTS_DIR, triple[1])))
            js_path = os.path.join(args.NR_PATH, triple[2])
            js_specs = extract_it_strings_js(args.NR_PATH, js_path)
            js_specs.sort()
            py_specs = extract_it_strings_py(py_path)
            py_specs.sort(key=lambda spec: spec["title"])

            md_node["specs"] = compare_specs(js_specs, py_specs)
            missing_count = sum(test["status"] == "missing" for test in md_node["specs"])
            skipped_count = sum(test["status"] == "skipped" for test in md_node["specs"])
            covered_count = len(js_specs) - missing_count - skipped_count
            if missing_count:
                node_color = Fore.RED
                node_status = CROSS
            elif skipped_count:
                node_color = Fore.YELLOW
                node_status = "~"
            else:
                node_color = Fore.GREEN
                node_status = CHECK
            if not missing_count and not skipped_count:
                print_subtitle(
                    f'''{node_color}* [{node_status}]{Style.RESET_ALL} "{triple[0]}" ({covered_count}/{len(js_specs)}) ''')
            else:
                print_subtitle(
                    f'''{node_color}* [{node_status}]{Style.RESET_ALL} "{triple[0]}" {node_color}({covered_count}/{len(js_specs)}){Style.RESET_ALL} ''')
            for test in md_node["specs"]:
                if test["status"] == "missing":
                    print(f'\t{Fore.RED}- It: {Style.RESET_ALL}{test["title"]}')
                elif test["status"] == "extra":
                    print(f'\t{Fore.BLUE}+ It: {Style.RESET_ALL}{test["title"]}')
                elif test["status"] == "skipped":
                    print(f'\t{Fore.YELLOW}~ It: {Style.RESET_ALL}{test["title"]}')
            md_cat["nodes"].append(md_node)
        report.append(md_cat)

    totals = report_totals(report)
    print_sep("")
    print("Total:")
    print(f"JS specs:\t{str(totals['js']).rjust(8)}")
    print(f"Python specs:\t{str(totals['python']).rjust(8)}")
    print(f"Covered:\t{str(totals['covered']).rjust(8)}")
    pc = "{:>{}.1%}".format(totals["covered"] / totals["js"], 8) if totals["js"] else "     n/a"
    print(f"Percent:\t{pc}")

    write_report(args.output, args.svg_output, report, totals, args.svg_width, args.svg_height)
    if args.json_output:
        write_json_report(args.json_output, report, totals, args.NR_PATH, args.label, args.commit)

    if totals["missing"] and not args.no_fail:
        exit(-1)
    else:
        exit(0)

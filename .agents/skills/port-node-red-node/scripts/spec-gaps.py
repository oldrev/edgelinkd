#!/usr/bin/env python3
"""List the Node-RED spec tests that have not been ported to the pytest suite yet.

This is a fast, dependency-free *approximation* of the authoritative checker
``scripts/specs_diff.py`` (which needs Node.js + mocha + pytest-json-report and which
also joins the ``describe`` prefix and does fuzzy matching). Use this script to answer
"what is left to port for this node?" and ``specs_diff.py`` to prove a port is complete.

Usage
-----
    # one node: JS spec vs the ported Python tests
    python spec-gaps.py --js 3rd-party/node-red/test/nodes/core/function/16-range_spec.js \
                        --py tests/nodes/function/test_range_node.py

    # every pair registered in scripts/specs_diff.json
    python spec-gaps.py --all

Exit code is 0 when nothing is missing, 1 when at least one title is missing.
"""

from __future__ import annotations

import argparse
import ast
import json
import re
import sys
from pathlib import Path

# it('title', function() {...}) / it.skip('title', ...) / it.only("title", ...)
# Line based on purpose: a leading `//` or `*` marks a commented-out test and is skipped.
JS_IT_RE = re.compile(r"""\bit\s*(?:\.\s*(?:only|skip))?\s*\(\s*(?P<q>['"`])(?P<title>.+?)(?P=q)\s*,""")

# @pytest.mark.it('title') / @pytest.mark.it("""title""") ...
PY_IT_RE = re.compile(r"""@pytest\.mark\.it\s*\(\s*(?P<q>'''|\"\"\"|'|")(?P<title>.*?)(?P=q)\s*\)""", re.DOTALL)


def _configure_stdout() -> None:
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass


def extract_js_titles(path: Path) -> list[str]:
    titles: list[str] = []
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("*") or stripped.startswith("/*"):
            continue
        match = JS_IT_RE.search(line)
        if match:
            titles.append(match.group("title"))
    return titles


def extract_py_titles(path: Path) -> list[str]:
    """Collect ``@pytest.mark.it(...)`` titles from a test file.

    Parsed with ``ast`` rather than a regex so escaped quotes, triple quotes and implicit
    string concatenation are all handled exactly; the regex is only a fallback for files
    that do not parse.
    """
    source = path.read_text(encoding="utf-8", errors="replace")
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return [m.group("title") for m in PY_IT_RE.finditer(source)]

    decorated: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call) or not isinstance(decorator.func, ast.Attribute):
                continue
            if decorator.func.attr != "it" or not decorator.args:
                continue
            try:
                value = ast.literal_eval(decorator.args[0])
            except (ValueError, SyntaxError):
                continue
            if isinstance(value, str):
                decorated.append((decorator.lineno, value))
    decorated.sort(key=lambda item: item[0])
    return [title for _, title in decorated]


def diff_pair(js_path: Path, py_path: Path) -> tuple[list[str], list[str]]:
    js_titles = extract_js_titles(js_path)
    py_titles = extract_py_titles(py_path)
    py_seen = set(py_titles)
    js_seen = set(js_titles)
    missing = [t for t in dict.fromkeys(js_titles) if t not in py_seen]
    extra = [t for t in dict.fromkeys(py_titles) if t not in js_seen]
    return missing, extra


def report_pair(label: str, js_path: Path, py_path: Path) -> bool:
    """Print the diff for one node; return True when something is missing."""
    print(f"== {label} ==")
    print(f"   js: {js_path}")
    print(f"   py: {py_path}")

    if not js_path.exists():
        print(f"   ERROR: JS spec not found (is 3rd-party/node-red populated?)")
        return True
    if not py_path.exists():
        print(f"   ERROR: Python test not found")
        return True

    js_titles = extract_js_titles(js_path)
    py_titles = extract_py_titles(py_path)
    missing, extra = diff_pair(js_path, py_path)

    print(f"   titles: js={len(js_titles)} py={len(py_titles)}")
    if missing:
        print(f"   MISSING ({len(missing)}):")
        for title in missing:
            print(f"     - {title}")
    if extra:
        print(f"   EXTRA ({len(extra)}):")
        for title in extra:
            print(f"     + {title}")
    if not missing and not extra:
        print("   OK: every upstream title is present")
    print()
    return bool(missing)


def resolve_map_paths(map_path: Path, nr_root: Path) -> list[tuple[str, Path, Path]]:
    repo_root = map_path.resolve().parent.parent
    tests_dir = repo_root / "tests"
    pairs: list[tuple[str, Path, Path]] = []
    for category in json.loads(map_path.read_text(encoding="utf-8")):
        for name, py_rel, js_rel in category["nodes"]:
            pairs.append((name, nr_root / js_rel, tests_dir / py_rel))
    return pairs


def main() -> int:
    _configure_stdout()

    script_repo_root = Path(__file__).resolve().parents[4]
    if not (script_repo_root / "scripts" / "specs_diff.json").exists():
        script_repo_root = Path.cwd()

    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--js", type=Path, help="path to the Node-RED *_spec.js file")
    parser.add_argument("--py", type=Path, help="path to the ported tests/nodes/.../test_*.py file")
    parser.add_argument("--all", action="store_true", help="check every pair registered in scripts/specs_diff.json")
    parser.add_argument(
        "--nr-root",
        type=Path,
        default=script_repo_root / "3rd-party" / "node-red",
        help="Node-RED checkout root (default: <repo>/3rd-party/node-red)",
    )
    parser.add_argument(
        "--map",
        type=Path,
        default=script_repo_root / "scripts" / "specs_diff.json",
        help="registry mapping Node-RED specs to our tests (default: <repo>/scripts/specs_diff.json)",
    )
    args = parser.parse_args()

    if args.all:
        if not args.map.exists():
            print(f"ERROR: registry not found: {args.map}")
            return 1
        pairs = resolve_map_paths(args.map, args.nr_root)
        incomplete = 0
        clean = 0
        for name, js_path, py_path in pairs:
            if report_pair(name, js_path, py_path):
                incomplete += 1
            else:
                clean += 1
        print(f"SUMMARY: {clean} complete, {incomplete} incomplete, {len(pairs)} registered")
        return 1 if incomplete else 0

    if not args.js or not args.py:
        parser.error("either use --all, or pass both --js and --py")

    return 1 if report_pair(args.js.stem, args.js, args.py) else 0


if __name__ == "__main__":
    sys.exit(main())

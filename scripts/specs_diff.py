#!/bin/python3

import re
import argparse
import ast
import difflib
import os
import json
import shutil
import subprocess
import tempfile
import pytest
import io
import contextlib

from colorama import init as colorama_init
from colorama import Fore
from colorama import Style


_SCRIPT_PATH = os.path.abspath(__file__)
_SCRIPT_DIR = os.path.dirname(_SCRIPT_PATH)
TESTS_DIR = os.path.join(_SCRIPT_DIR, '..', "tests")

JS_IT_PATTERN = re.compile(r"""^\s*it\s*\(\s*(['"].*?['"]+)\s*,\s*""")
PY_IT_PATTERN = re.compile(r"""\@.*it\s*\(\s*(['"].*?['"]+)\s*\)\s*""")

def load_json(json_path):
    with open(json_path, 'r') as fp:
        return json.load(fp)


def extract_it_strings_js(red_dir, file_path) -> list[str]:
    specs = []
    # Use delete=False to avoid permission issues on Windows
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as report_file:
        report_file_path = report_file.name

    original_cwd = os.getcwd()
    os.chdir(red_dir)
    try:
        result = subprocess.run([
            'mocha',
            file_path, "--dry-run", "--reporter=json", "--exit",
            "--reporter-options", f"output={report_file_path}"
        ], shell=True)

        # Read the report after mocha finishes
        if os.path.exists(report_file_path):
            report = load_json(report_file_path)
            for test in report['tests']:
                specs.append(test['fullTitle'].rstrip())
    finally:
        os.chdir(original_cwd)
        # Clean up the temporary file
        try:
            if os.path.exists(report_file_path):
                os.unlink(report_file_path)
        except OSError:
            pass  # Ignore cleanup errors

    return specs


def extract_it_strings_py(file_path) -> list[str]:
    specs = []
    # Use delete=False to avoid permission issues on Windows
    with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.json') as report_file:
        report_file_path = report_file.name

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
                        specs.append(result['fullTitle'].rstrip())
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

def generate_markdown_table(rows):
    headers = ["Status", "Spec Test"]
    table = "| " + " | ".join(headers) + " |\n"
    table += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    for row in rows:
        if row[0] == " ":
            table += f"| :white_check_mark: | ~~{row[1]}~~ |\n"
        if row[0] == "-":
            table += f"| :x: | **{row[1]}** |\n"
    return table


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scan a .js file, extract lines containing it('arbitrary text') or it(\"arbitrary text\"), and print the text with a four-digit number prefix.")
    parser.add_argument('NR_PATH', type=str,
                        help="Path to the directory of Node-RED")
    parser.add_argument('-o', "--output", type=str, default=None,
                        help="The output path to a Markdown file")
    args = parser.parse_args()

    colorama_init()

    categories = read_json()

    markdown = []

    total_js_count = 0
    total_py_count = 0
    for cat in categories:
        md_cat = {"category": cat["category"], "nodes": []}
        for triple in cat["nodes"]:
            md_node = {"node": triple[0], "specs": []}
            py_path = os.path.join(os.path.normpath(os.path.join(TESTS_DIR, triple[1])))
            js_path = os.path.join(args.NR_PATH, triple[2])
            js_specs = extract_it_strings_js(args.NR_PATH, js_path)
            js_specs.sort()
            py_specs = extract_it_strings_py(py_path)
            py_specs.sort()

            diff = difflib.Differ().compare(js_specs, py_specs)
            # differences = [line for line in diff if line.startswith(
            #    '-') or line.startswith('+')]
            differences = [line for line in diff]
            total_js_count += len(js_specs)
            total_py_count += len(py_specs)
            if len(py_specs) >= len(js_specs):
                print_subtitle(
                    f'''{Fore.GREEN}* [✓]{Style.RESET_ALL} "{triple[0]}" ({len(py_specs)}/{len(js_specs)}) ''')
            else:
                print_subtitle(
                    f'''{Fore.RED}* [×]{Style.RESET_ALL} "{triple[0]}" {Fore.RED}({len(py_specs)}/{len(js_specs)}){Style.RESET_ALL} ''')
            for s in differences:
                if s[0] == '-':
                    print(f'\t{Fore.RED}{s[0]} It: {Style.RESET_ALL}{s[2:]}')
                    md_node["specs"].append(["-", s[2:]])
                elif s[0] == '+':
                    print(f'\t{Fore.GREEN}{s[0]} It: {Style.RESET_ALL}{s[2:]}')
                    md_node["specs"].append(["+", s[2:]])
                elif s[0] == '?':
                    print(f'\t{Fore.YELLOW}{s[0]} It: {Style.RESET_ALL}{s[2:]}')
                    md_node["specs"].append(["*", s[2:]])
                else:
                    print(f'\t{Style.DIM}{s[0]} It: {s[2:]}{Style.RESET_ALL}')
                    md_node["specs"].append([" ", s[2:]])
            md_node["specs"].sort(key=lambda x: x[0])
            md_cat["nodes"].append(md_node)
        markdown.append(md_cat)

    print_sep("")
    print("Total:")
    print(f"JS specs:\t{str(total_js_count).rjust(8)}")
    print(f"Python specs:\t{str(total_py_count).rjust(8)}")
    pc = "{:>{}.1%}".format(total_py_count * 1.0 / total_js_count, 8)
    print(f"Percent:\t{pc}")

    if args.output != None:
        with open(args.output, 'w', encoding='utf-8') as md_file:
            md_file.write("# Node-RED Spec Tests Diff\n")
            md_file.write(
                "This file is automatically generated by the script `scripts/specs_diff.py` to compare the specification " +
                "compliance of EdgeLink and Node-RED nodes. \n")
            for cat in markdown:
                md_file.write(f"## {cat['category']}\n")
                for node in cat["nodes"]:
                    md_file.write(f"### {node['node']}\n")
                    md_file.write(generate_markdown_table(node["specs"]))

    if total_py_count < total_js_count:
        exit(-1)
    else:
        exit(0)

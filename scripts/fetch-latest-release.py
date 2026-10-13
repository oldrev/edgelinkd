#!/usr/bin/env python3
"""Fetch the latest beta release for the Hugo build."""

import json
import os
import re
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "website" / "data" / "release.json"
API = "https://api.github.com/repos/oldrev/edgelinkd/releases?per_page=100"
TARGETS = (
    "x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
    "armv7-unknown-linux-gnueabihf", "armv7-unknown-linux-gnueabi",
)


def release_key(release):
    if release.get("draft") or not release.get("prerelease"):
        return None
    match = re.fullmatch(r"v(\d+)\.(\d+)\.(\d+)-beta(?:\.([1-9]\d*))?", release.get("tag_name", ""))
    if not match:
        return None
    names = [asset["name"] for asset in release.get("assets", [])]
    if not all(any(name.startswith(f"edgelinkd-dist-{target}-") and name.endswith(
        ".zip" if "windows" in target else ".tar.gz"
    ) for name in names) for target in TARGETS):
        return None
    return tuple(int(part or 0) for part in match.groups())

request = urllib.request.Request(API, headers={"Accept": "application/vnd.github+json", "User-Agent": "edgelinkd-website"})
token = os.environ.get("GITHUB_TOKEN")
if token:
    request.add_header("Authorization", f"Bearer {token}")

try:
    with urllib.request.urlopen(request, timeout=15) as response:
        releases = json.load(response)
    candidates = [(release_key(release), release) for release in releases]
    beta = max((item for item in candidates if item[0] is not None), key=lambda item: item[0], default=(None, None))[1]
    if beta:
        OUTPUT.write_text(json.dumps({"version": beta["tag_name"].removeprefix("v"), "url": beta["html_url"]}) + "\n", encoding="utf-8")
        print(f"Latest beta release: {beta['tag_name']}")
except Exception as error:
    print(f"GitHub release lookup failed; keeping existing release data if available: {error}")

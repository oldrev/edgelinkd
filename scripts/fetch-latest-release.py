#!/usr/bin/env python3
"""Fetch the latest beta release for the Hugo build."""

import json
import os
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "website" / "data" / "release.json"
API = "https://api.github.com/repos/oldrev/edgelinkd/releases?per_page=30"

request = urllib.request.Request(API, headers={"Accept": "application/vnd.github+json", "User-Agent": "edgelinkd-website"})
token = os.environ.get("GITHUB_TOKEN")
if token:
    request.add_header("Authorization", f"Bearer {token}")

try:
    with urllib.request.urlopen(request, timeout=15) as response:
        releases = json.load(response)
    beta = next((release for release in releases if release.get("prerelease") and "beta" in release.get("tag_name", "")), None)
    if beta:
        OUTPUT.write_text(json.dumps({"version": beta["tag_name"].removeprefix("v"), "url": beta["html_url"]}) + "\n", encoding="utf-8")
        print(f"Latest beta release: {beta['tag_name']}")
except Exception as error:
    print(f"GitHub release lookup failed, using checked-in fallback: {error}")

#!/usr/bin/env python3
"""Require a unique numbered beta tag matching the Cargo workspace version."""

import re
import sys
import tomllib
from pathlib import Path

root = Path(__file__).resolve().parent.parent
with (root / "Cargo.toml").open("rb") as manifest:
    version = tomllib.load(manifest)["workspace"]["package"]["version"]
if not re.fullmatch(r"\d+\.\d+\.\d+-beta\.[1-9]\d*", version):
    sys.exit("Release version must use MAJOR.MINOR.PATCH-beta.N (N >= 1)")
if sys.argv[1] != f"v{version}":
    sys.exit(f"Release tag {sys.argv[1]!r} must match Cargo.toml: v{version}")
print(f"Release version verified: {version}")

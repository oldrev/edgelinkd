#!/usr/bin/env python3
"""Require a stable or numbered beta tag matching the Cargo workspace version."""

import re
import sys
import tomllib
from pathlib import Path

root = Path(__file__).resolve().parent.parent


def release_tag_matches(version, tag):
    if not re.fullmatch(r"\d+\.\d+\.\d+(?:-beta\.[1-9]\d*)?", version):
        return False
    return tag == f"v{version}"


def main():
    with (root / "Cargo.toml").open("rb") as manifest:
        version = tomllib.load(manifest)["workspace"]["package"]["version"]
    if not release_tag_matches(version, sys.argv[1]):
        if not re.fullmatch(r"\d+\.\d+\.\d+(?:-beta\.[1-9]\d*)?", version):
            sys.exit("Release version must be MAJOR.MINOR.PATCH or MAJOR.MINOR.PATCH-beta.N (N >= 1)")
        sys.exit(f"Release tag {sys.argv[1]!r} must match Cargo.toml: v{version}")
    print(f"Release version verified: {version}")


if __name__ == "__main__":
    main()

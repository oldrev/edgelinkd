#!/usr/bin/env python3

import subprocess
import sys
import json
import os
import argparse
from pathlib import PurePosixPath

parser = argparse.ArgumentParser(description="Run test binaries using qemu-*.")
parser.add_argument("qemu", help="The QEMU command (qemu-arm)")
parser.add_argument("toolchain_prefix", help="The toolchain prefix (e.g., arm-linux-gnueabihf)")
parser.add_argument("cargo_output", help="The path to the cargo-output.json file")
parser.add_argument(
    "--target-triple",
    help="The Rust target triple; inferred from cargo_output when omitted",
)
args = parser.parse_args()

qemu_cmd = args.qemu
toolchain_prefix = args.toolchain_prefix
cargo_output_path = args.cargo_output


def infer_target_triple(path):
    """Infer the target directory used by Cargo from target/<triple>/... output."""
    parts = PurePosixPath(path.replace("\\", "/")).parts
    try:
        target_index = parts.index("target")
    except ValueError:
        return None
    if target_index + 1 >= len(parts):
        return None
    candidate = parts[target_index + 1]
    # Host artifacts are stored in target/<profile>/..., so a profile name here is not a triple.
    if candidate in {"debug", "release", "ci"}:
        return None
    return candidate


target_triple = args.target_triple or infer_target_triple(cargo_output_path)


def is_target_executable(path):
    if not target_triple:
        return True
    normalized = path.replace("\\", "/")
    marker = f"/target/{target_triple}/"
    return marker in normalized or normalized.startswith(f"target/{target_triple}/")

try:
    with open(cargo_output_path, 'r') as f:
        cargo_output = [json.loads(line) for line in f]
except FileNotFoundError:
    print(f"Error: {cargo_output_path} not found. Please run cargo test first.")
    sys.exit(1)

test_binaries = []
skipped_binaries = []
for entry in cargo_output:
    executable = entry.get("executable")
    if entry.get("profile", {}).get("test") is not True or not executable:
        continue
    if is_target_executable(executable):
        test_binaries.append(executable)
    else:
        skipped_binaries.append(executable)

if skipped_binaries:
    print(f"Skipping {len(skipped_binaries)} host test binaries for target {target_triple or 'unknown'}")

if not test_binaries:
    print("No test binaries found.")
    sys.exit(0)

exit_code = 0

for test_binary in test_binaries:
    print(f"Running test binary: {test_binary}")

    result = subprocess.run([qemu_cmd, "-L", f"/usr/{toolchain_prefix}", test_binary])
    
    if result.returncode != 0:
        print(f"Test failed: {test_binary}")
        exit_code = 1

sys.exit(exit_code)


#!/usr/bin/env python3

import subprocess
import sys
import json
import os
import argparse

parser = argparse.ArgumentParser(description="Run test binaries using qemu-*.")
parser.add_argument("qemu", help="The QEMU command (qemu-arm)")
parser.add_argument("toolchain_prefix", help="The toolchain prefix (e.g., arm-linux-gnueabihf)")
parser.add_argument("cargo_output", help="The path to the cargo-output.json file")
args = parser.parse_args()

qemu_cmd = args.qemu
toolchain_prefix = args.toolchain_prefix
cargo_output_path = args.cargo_output

try:
    with open(cargo_output_path, 'r') as f:
        cargo_output = [json.loads(line) for line in f]
except FileNotFoundError:
    print(f"Error: {cargo_output_path} not found. Please run cargo test first.")
    sys.exit(1)

test_binaries = [
    entry['executable'] for entry in cargo_output
    if entry.get('profile', {}).get('test') == True
]

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


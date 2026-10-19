#!/usr/bin/env python3

import subprocess
import sys
import json
import argparse
from pathlib import Path, PurePosixPath


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


def is_target_executable(path, target_triple):
    if not target_triple:
        return True
    normalized = path.replace("\\", "/")
    marker = f"/target/{target_triple}/"
    return marker in normalized or normalized.startswith(f"target/{target_triple}/")


def resolve_executable_path(path, cargo_output_path):
    """Map cross-rs container paths (``/target/...``) to the runner workspace."""
    executable = Path(path)
    if executable.exists():
        return executable

    normalized = path.replace("\\", "/")
    marker = "/target/"
    marker_index = normalized.find(marker)
    if marker_index < 0:
        return executable

    target_root = Path(cargo_output_path).resolve().parents[2]
    return target_root / normalized[marker_index + len(marker) :]


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run test binaries using qemu-*.")
    parser.add_argument("qemu", help="The QEMU command (qemu-arm)")
    parser.add_argument("toolchain_prefix", help="The toolchain prefix (e.g., arm-linux-gnueabihf)")
    parser.add_argument("cargo_output", help="The path to the cargo-output.json file")
    parser.add_argument(
        "--target-triple",
        help="The Rust target triple; inferred from cargo_output when omitted",
    )
    args = parser.parse_args(argv)

    target_triple = args.target_triple or infer_target_triple(args.cargo_output)

    try:
        with open(args.cargo_output, "r") as f:
            cargo_output = [json.loads(line) for line in f]
    except FileNotFoundError:
        print(f"Error: {args.cargo_output} not found. Please run cargo test first.")
        return 1

    test_binaries = []
    skipped_binaries = []
    for entry in cargo_output:
        executable = entry.get("executable")
        if entry.get("profile", {}).get("test") is not True or not executable:
            continue
        if is_target_executable(executable, target_triple):
            test_binaries.append(executable)
        else:
            skipped_binaries.append(executable)

    if skipped_binaries:
        print(f"Skipping {len(skipped_binaries)} host test binaries for target {target_triple or 'unknown'}")

    if not test_binaries:
        print("No test binaries found.")
        return 0

    exit_code = 0
    for test_binary in test_binaries:
        resolved_binary = resolve_executable_path(test_binary, args.cargo_output)
        print(f"Running test binary: {resolved_binary}")
        result = subprocess.run([args.qemu, "-L", f"/usr/{args.toolchain_prefix}", str(resolved_binary)])
        if result.returncode != 0:
            print(f"Test failed: {resolved_binary}")
            exit_code = 1

    return exit_code


if __name__ == "__main__":
    sys.exit(main())


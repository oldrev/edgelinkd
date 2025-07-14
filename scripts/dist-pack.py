#!/usr/bin/env python3
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

import zipfile

EXCLUDE_PREFIXES = [
    "libedgelink_pymod.so",
    "libedgelink_macro.so",
    "edgelink_pymod.dll",
    "edgelink_macro.dll"
]

try:
    import tomlkit
except ImportError:
    print("Please install tomlkit first: pip install tomlkit", file=sys.stderr)
    sys.exit(-1)

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"

def get_latest_commit_datetime():
    try:
        result = subprocess.run(
            ["git", "log", "-1", "--format=%cd", "--date=format:%Y%m%d%H%M"],
            cwd=ROOT,
            stdout=subprocess.PIPE,
            encoding="utf-8",
            check=True
        )
        return result.stdout.strip()
    except Exception as e:
        print(f"Failed to get latest commit datetime: {e}", file=sys.stderr)
        sys.exit(-1)

def merge_toml_files(file1, file2, out_file):
    try:
        with open(file1, "r", encoding="utf-8") as f1, open(file2, "r", encoding="utf-8") as f2:
            doc1 = tomlkit.parse(f1.read())
            doc2 = tomlkit.parse(f2.read())
        # doc2 overrides doc1
        merged = doc1
        for k, v in doc2.items():
            merged[k] = v
        with open(out_file, "w", encoding="utf-8") as f:
            f.write(tomlkit.dumps(merged))
    except Exception as e:
        print(f"Failed to merge TOML files: {e}", file=sys.stderr)
        sys.exit(-1)

def copytree(src, dst):
    if not os.path.exists(src):
        return
    try:
        shutil.copytree(src, dst, dirs_exist_ok=True)
    except Exception as e:
        print(f"Failed to copy directory {src} to {dst}: {e}", file=sys.stderr)
        sys.exit(-1)


def main():
    parser = argparse.ArgumentParser(description="Package EdgeLinkd for multiple targets")
    parser.add_argument(
        "--target",
        default="x86_64-pc-windows-msvc",
        choices=[
            "x86_64-pc-windows-msvc",
            "x86_64-pc-windows-gnu",
            "x86_64-unknown-linux-gnu",
            "aarch64-unknown-linux-gnu",
            "armv7-unknown-linux-gnueabihf",
            "armv7-unknown-linux-gnueabi"
        ],
        help="Target triple: x86_64-pc-windows-msvc, x86_64-pc-windows-gnu, x86_64-unknown-linux-gnu, aarch64-unknown-linux-gnu, armv7-unknown-linux-gnueabihf, armv7-unknown-linux-gnueabi"
    )
    parser.add_argument("--branch", default="master", choices=["dev", "master"], help="Branch to package")
    parser.add_argument("--mode", default="release", choices=["release", "debug"], help="Build mode")
    parser.add_argument(
        "--outdir",
        default=str(ROOT / "build-out"),
        help="Output directory for the package (default: ./build-out)"
    )
    args = parser.parse_args()

    # 1. Get packaging time and branch
    dt_str = get_latest_commit_datetime()
    is_nightly = args.branch == "dev"
    # Determine archive format by target
    is_windows = args.target.startswith("x86_64-pc-windows") or args.target.startswith("x86_64-pc-windows-gnu")
    is_linux = "linux" in args.target
    if is_nightly:
        base_name = f"edgelinkd-dist-{args.target}-nightly-{dt_str}"
    else:
        base_name = f"edgelinkd-dist-{args.target}-{dt_str}"
    if is_windows:
        out_name = base_name + ".zip"
    elif is_linux:
        out_name = base_name + ".tar.gz"
    else:
        out_name = base_name + ".zip"  # fallback

    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp = Path(tmpdir)
            # 2. Merge configuration files
            merged_toml = tmp / "edgelinkd.toml"
            merge_toml_files(ROOT / "edgelinkd.toml", ROOT / "edgelinkd.prod.toml", merged_toml)

            # 3. Copy README and LICENSE
            shutil.copy(ROOT / "README.md", tmp / "README.md")
            shutil.copy(ROOT / "LICENSE", tmp / "LICENSE")

            # 4. bin directory
            bin_dir = tmp / "bin"
            bin_dir.mkdir()
            # Executable and DLLs
            target_dir = ROOT / "target" / args.target / args.mode
            for f in target_dir.glob("edgelinkd.exe"):
                shutil.copy(f, bin_dir / f.name)
            for dll in target_dir.glob("*.dll"):
                if any(dll.name.startswith(prefix) for prefix in EXCLUDE_PREFIXES):
                    continue
                shutil.copy(dll, bin_dir / dll.name)

            # 5. bin/ui_static
            ui_static_src = ROOT / "target" / "ui_static"
            if ui_static_src.exists():
                print(f"Copying ui_static from {ui_static_src} to {bin_dir / 'ui_static'}", file=sys.stderr)
                copytree(ui_static_src, bin_dir / "ui_static")
            else:
                print(f"Error: {ui_static_src} directory not found, aborting packaging.", file=sys.stderr)
                sys.exit(-1)

            # 6. shared directory
            shared_dir = tmp / "shared"
            shared_dir.mkdir()
            (shared_dir / ".keep-me").write_text("placeholder for future docs/help\n", encoding="utf-8")

            # 7. Packaging
            outdir = Path(args.outdir)
            outdir.mkdir(parents=True, exist_ok=True)
            archive_path = outdir / out_name
            if out_name.endswith(".zip"):
                with zipfile.ZipFile(archive_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
                    for path in tmp.rglob("*"):
                        print(f"Adding to archive: {path.relative_to(tmp)}", file=sys.stderr)
                        zf.write(path, path.relative_to(tmp))
            elif out_name.endswith(".tar.gz"):
                import tarfile
                with tarfile.open(archive_path, "w:gz") as tf:
                    for path in tmp.rglob("*"):
                        print(f"Adding to archive: {path.relative_to(tmp)}", file=sys.stderr)
                        tf.add(path, arcname=path.relative_to(tmp))
            else:
                print(f"Unknown archive format for {archive_path}", file=sys.stderr)
                sys.exit(-1)
            print(f"Package complete: {archive_path}", file=sys.stderr)
    except Exception as e:
        print(f"Packaging failed: {e}", file=sys.stderr)
        sys.exit(-1)
    sys.exit(0)

if __name__ == "__main__":
    main()

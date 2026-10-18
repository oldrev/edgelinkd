import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "fetch-latest-release.py"
SPEC = importlib.util.spec_from_file_location("fetch_latest_release", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)
TARGETS = (
    "x86_64-pc-windows-msvc", "x86_64-unknown-linux-gnu", "aarch64-unknown-linux-gnu",
    "armv7-unknown-linux-gnueabihf", "armv7-unknown-linux-gnueabi",
)


def release(tag, prerelease):
    return {
        "tag_name": tag,
        "prerelease": prerelease,
        "assets": [
            {"name": f"edgelinkd-dist-{target}-4.1.16{'.zip' if 'windows' in target else '.tar.gz'}"}
            for target in TARGETS
        ],
    }


def test_stable_release_is_preferred_over_newer_beta():
    stable = release("v4.1.16", False)
    newer_beta = release("v4.1.17-beta.1", True)

    assert MODULE.select_latest_release([stable, newer_beta]) is stable


def test_latest_beta_is_selected_when_no_stable_release_exists():
    older_beta = release("v4.1.16-beta.2", True)
    newer_beta = release("v4.1.17-beta.1", True)

    assert MODULE.select_latest_release([older_beta, newer_beta]) is newer_beta

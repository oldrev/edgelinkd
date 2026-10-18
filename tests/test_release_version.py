import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "check-release-version.py"
SPEC = importlib.util.spec_from_file_location("check_release_version", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_stable_release_tag_matches_stable_version():
    assert MODULE.release_tag_matches("4.1.16", "v4.1.16")


def test_beta_release_tag_matches_numbered_beta_version():
    assert MODULE.release_tag_matches("4.1.16-beta.1", "v4.1.16-beta.1")


def test_stable_release_tag_does_not_match_beta_version():
    assert not MODULE.release_tag_matches("4.1.16-beta.1", "v4.1.16")

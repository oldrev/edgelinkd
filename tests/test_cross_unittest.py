import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "cross-unittest.py"


def load_script_module():
    spec = importlib.util.spec_from_file_location("cross_unittest", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_resolves_cross_container_target_path(tmp_path):
    module = load_script_module()
    cargo_output = tmp_path / "target" / "armv7-unknown-linux-gnueabihf" / "ci" / "cargo.json"
    executable = "/target/armv7-unknown-linux-gnueabihf/ci/deps/example"

    resolved = module.resolve_executable_path(executable, cargo_output)

    assert resolved == cargo_output.parents[2] / "armv7-unknown-linux-gnueabihf" / "ci" / "deps" / "example"

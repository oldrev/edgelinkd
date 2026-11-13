"""Oracle vectors generated directly by pinned iconv-lite, not Python codec tables."""
import json
import os
from pathlib import Path

import pytest
from tests import run_single_node_with_msgs_ntimes

VECTORS = json.loads((Path(__file__).parents[2] / "resources/file_encodings.json").read_text(encoding="utf8"))["fixtures"]
pytestmark = pytest.mark.skipif(
    os.getenv("EDGELINK_FILE_ENCODINGS_LEGACY") != "1",
    reason="optional file_encodings_legacy feature: build it and set EDGELINK_FILE_ENCODINGS_LEGACY=1",
)


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: v["label"])
async def test_legacy_decode_all_bytes(vector, tmp_path):
    path = tmp_path / "source"
    path.write_bytes(bytes(vector["bytes"]))
    result = await run_single_node_with_msgs_ntimes(
        {"type": "file in", "filename": str(path), "format": "utf8", "encoding": vector["label"]}, [{}], 1)
    assert result[0]["payload"] == vector["text"]


@pytest.mark.asyncio
@pytest.mark.parametrize("vector", VECTORS, ids=lambda v: v["label"])
async def test_legacy_encode_replacement_and_reverse_mapping(vector, tmp_path):
    path = tmp_path / "destination"
    await run_single_node_with_msgs_ntimes(
        {"type": "file", "filename": str(path), "encoding": vector["label"],
         "appendNewline": False, "overwriteFile": True}, [{"payload": vector["input"]}], 1)
    assert path.read_bytes() == bytes(vector["encoded"])

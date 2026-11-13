"""Exercise the extension-module bridge inside its Python host, including nested values."""

import pytest

from tests import run_single_node_with_msgs_ntimes


@pytest.mark.asyncio
async def test_tagged_buffers_roundtrip_without_becoming_integer_arrays():
    buffer = {"$__el_type": "Buffer", "data": [0, 127, 255]}
    message = {"payload": [buffer, {"nested": buffer}, [0, 127, 255]]}
    output = await run_single_node_with_msgs_ntimes({"type": "change", "rules": []}, [message], 1)
    assert output[0]["payload"] == message["payload"]

    # Reinjection exercises deserialization of the tagged values returned by Rust.
    repeated = await run_single_node_with_msgs_ntimes({"type": "change", "rules": []}, output, 1)
    assert repeated[0]["payload"] == message["payload"]


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [[256], [-1], [True], [1.5]])
async def test_tagged_buffer_rejects_invalid_bytes(data):
    with pytest.raises((ValueError, TypeError), match="Buffer byte"):
        await run_single_node_with_msgs_ntimes(
            {"type": "change", "rules": []}, [{"payload": {"$__el_type": "Buffer", "data": data}}], 1)


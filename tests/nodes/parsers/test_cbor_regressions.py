import pytest
from tests import run_single_node_with_msgs_ntimes


@pytest.mark.asyncio
async def test_nested_property_roundtrip_preserves_other_fields():
    node = {"type": "cbor", "property": "nested.value"}
    original = {"nested": {"value": {"a": [1, True, "x"]}}, "topic": "retained"}
    packed = await run_single_node_with_msgs_ntimes(node, [original], 1)
    assert packed[0]["nested"]["value"]["$__el_type"] == "Buffer"
    decoded = await run_single_node_with_msgs_ntimes(node, [packed[0]], 1)
    assert decoded[0]["nested"] == original["nested"]
    assert decoded[0]["topic"] == "retained"


@pytest.mark.asyncio
async def test_byte_string_output_remains_tagged():
    output = await run_single_node_with_msgs_ntimes(
        {"type": "cbor"}, [{"payload": {"$__el_type": "Buffer", "data": [0x43, 0, 128, 255]}}], 1)
    assert output[0]["payload"] == {"$__el_type": "Buffer", "data": [0, 128, 255]}


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [[-1], [256], [True], [1.5]])
async def test_invalid_buffer_tag_is_rejected(data):
    with pytest.raises((ValueError, TypeError)):
        await run_single_node_with_msgs_ntimes(
            {"type": "cbor"}, [{"payload": {"$__el_type": "Buffer", "data": data}}], 1)

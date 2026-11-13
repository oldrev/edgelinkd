import pytest
from tests import *


@pytest.mark.describe('cbor node')
class TestCborNode:
    @pytest.mark.asyncio
    @pytest.mark.it("should be loaded with correct defaults")
    async def test_0001(self):
        out = await run_single_node_with_msgs_ntimes(
            {"type": "cbor", "name": "cbor1"}, [{"payload": {"ok": True}}], 1)
        assert out[0]["payload"]["$__el_type"] == "Buffer"

    @pytest.mark.asyncio
    @pytest.mark.it('should pack an object')
    async def test_0002(self):
        payload = {"A": 1, "B": "string", "C": True, "D": [1, True, "string"], "E": {"Y": 9, "Z": "string"}}
        out = await run_single_node_with_msgs_ntimes({"type": "cbor"}, [{"payload": payload}], 1)
        encoded = out[0]["payload"]
        assert encoded["$__el_type"] == "Buffer"
        assert len(encoded["data"]) == 47

    @pytest.mark.asyncio
    @pytest.mark.it('should unpack a Buffer')
    async def test_0003(self):
        encoded = bytes.fromhex(
            "b90005614101614266737472696e676143f561448301f566737472696e67"
            "6145b90002615909615a66737472696e67")
        out = await run_single_node_with_msgs_ntimes(
            {"type": "cbor"}, [{"payload": {"$__el_type": "Buffer", "data": list(encoded)}}], 1)
        assert out[0]["payload"] == {
            "A": 1, "B": "string", "C": True, "D": [1, True, "string"], "E": {"Y": 9, "Z": "string"}}

    @pytest.mark.asyncio
    @pytest.mark.it('should error if the buffer fails to decode')
    async def test_0004(self):
        tab, node_id, catch_id, end_id = map(red_id, ["tab", "cbor", "catch", "end"])
        flows = [
            {"id": tab, "type": "tab"},
            {"id": node_id, "z": tab, "type": "cbor", "wires": [[]]},
            {"id": catch_id, "z": tab, "type": "catch", "scope": [node_id], "wires": [[end_id]]},
            {"id": end_id, "z": tab, "type": "test-once"},
        ]
        out = await run_flow_with_msgs_ntimes(
            flows, [{"nid": node_id, "msg": {"payload": {"$__el_type": "Buffer", "data": [0x87]}}}], 1)
        assert "Bad decode" in out[0]["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.timeout(10)
    @pytest.mark.it('ignore msg with no payload')
    async def test_0005(self):
        tab, node_id, end_id = map(red_id, ["tab", "cbor", "end"])
        flows = [
            {"id": tab, "type": "tab"},
            {"id": node_id, "z": tab, "type": "cbor", "wires": [[end_id]]},
            {"id": end_id, "z": tab, "type": "test-once"},
        ]
        out = await run_flow_for_seconds(flows, [{"nid": node_id, "msg": {"topic": 1}}], 0.1, node_id)
        assert out == []

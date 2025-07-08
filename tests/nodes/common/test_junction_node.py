import time
import json
import pytest

from tests import *

@pytest.mark.describe('junction node')
class TestJunctionNode:

    @pytest.mark.asyncio
    @pytest.mark.it('junction node should work')
    async def test_0001(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "z": "100", "type": "junction", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "foo"


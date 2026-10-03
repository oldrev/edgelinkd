"""Ported specs for the `status` node.

Upstream: `3rd-party/node-red/test/nodes/core/common/25-status_spec.js` (v4.0.9). The node forwards
every message it receives unchanged, so its single spec checks what reaches the next node.
"""
import pytest
from tests import *


@pytest.mark.describe('status Node')
class TestStatusNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should output a message when called')
    async def test_should_output_a_message_when_called(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "status", "name": "status", "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        message = {"text": "Oh dear", "source": {"id": "12345", "type": "testnode", "name": "fred"}}
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": message}], 1)
        assert msgs[0]["text"] == "Oh dear"
        assert msgs[0]["source"]["id"] == "12345"
        assert msgs[0]["source"]["type"] == "testnode"
        assert msgs[0]["source"]["name"] == "fred"

"""Ported specs for the ``complete`` node.

Upstream: ``3rd-party/node-red/test/nodes/core/common/24-complete_spec.js``.
"""

import pytest
from tests import *


@pytest.mark.describe("complete Node")
class TestCompleteNode:

    @pytest.mark.asyncio
    @pytest.mark.it("should output a message when called")
    async def test_should_output_a_message_when_called(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "complete", "name": "status", "scope": [], "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        message = {"text": "Oh dear", "source": {"id": "12345", "type": "testnode", "name": "fred"}}
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": message}], 1)
        assert msgs == [message]

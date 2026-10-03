"""Ported specs for the `comment` node.

Upstream: `3rd-party/node-red/test/nodes/core/common/90-comment_spec.js` (v4.0.9). The node is a
label on the workspace: it has no input, no output and no behaviour, and upstream's single spec only
loads it and reads the deployed node's own `name` property. The bridge cannot read a node's
properties back, so the port verifies what it can observe - the flow deploys with the comment node in
it and keeps running around it.
"""
import pytest
from tests import *


@pytest.mark.describe('comment Node')
class TestCommentNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_should_be_loaded(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "3", "z": "100", "type": "comment", "name": "comment", "wires": []},
            {"id": "1", "z": "100", "type": "change", "name": "change", "rules": [
                {"t": "set", "p": "payload", "pt": "msg", "to": "loaded", "tot": "str"}], "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": "in"}}], 1)
        assert msgs[0]["payload"] == "loaded"

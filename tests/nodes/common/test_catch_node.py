import time
import json
import pytest

from tests import *

@pytest.mark.describe('catch Node')
class TestCatchNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should output a message when called')
    async def test_it_should_output_a_message_when_called(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "z": "100", "type": "function", "name":"func1", "func": 'throw new Error("big error");', "wires": []},
            {"id": "2", "z": "100", "type": "catch", "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "foo"}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "foo"
        assert "error" in msgs[0]
        assert "source" in msgs[0]["error"]
        assert "message" in msgs[0]["error"]
        assert msgs[0]["error"]["source"]["type"] == "function"
        assert msgs[0]["error"]["source"]["name"] == "func1"


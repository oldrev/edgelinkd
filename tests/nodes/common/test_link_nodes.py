import json
import pytest
import time

from tests import *


@pytest.mark.describe('link Node')
class TestInjectNode:

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded (link in)')
    async def test_it_should_be_loaded_link_in(self):
        pass

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded (link out)')
    async def test_it_should_be_loaded_link_out(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should be linked')
    async def test_it_should_be_linked(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "z": "100", "type": "link out",
                "name": "link-out", "links": ["2"]},
            {"id": "2", "z": "100", "type": "link in",
                "name": "link-out", "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"}
        ]
        injections = [
            {'payload': 'hello'},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == 'hello'

    @pytest.mark.asyncio
    @pytest.mark.it('should be linked to multiple nodes')
    async def test_it_should_be_linked_to_multiple_nodes(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "z": "100", "type": "link out",
                "name": "link-out", "links": ["2", "3"]},
            {"id": "2", "z": "100", "type": "link in",
                "name": "link-in0", "wires": [["4"]]},
            {"id": "3", "z": "100", "type": "link in",
                "name": "link-in1", "wires": [["4"]]},
            {"id": "4", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'hello'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 2)
        assert msgs[0]["payload"] == 'hello'

    @pytest.mark.asyncio
    @pytest.mark.it('''should be linked from multiple nodes''')
    async def test_0003(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "z": "100", "type": "link out",
                "name": "link-out0", "links": ["3"]},
            {"id": "2", "z": "100", "type": "link out",
                "name": "link-out1", "links": ["3"]},
            {"id": "3", "z": "100", "type": "link in",
                "name": "link-in", "wires": [["4"]]},
            {"id": "4", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'hello'}},
            {"nid": "2", "msg": {'payload': 'hello'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 2)
        assert msgs[0]["payload"] == 'hello'
        assert msgs[1]["payload"] == 'hello'

    @pytest.mark.describe('link-call node')
    class TestLinkCallNode:

        @pytest.mark.asyncio
        @pytest.mark.it('should call static link-in node and get response')
        async def test_id_should_call_static_link_in_node_and_get_response(self):
            flows = [
                {"id": "100", "type": "tab"},  # flow 1
                {"id": "200", "type": "tab"},  # flow 2
                {"id": "1", "z": "100", "type": "link in", "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "function",
                    "func": 'msg.payload = "123"; return msg;', "wires": [["3"]]},
                {"id": "3", "z": "100", "type": "link out", "mode": "return"},
                {"id": "4", "z": "200", "type": "link call",
                    "links": ["1"], "wires": [["5"]]},
                {"id": "5", "z": "200", "type": "test-once"}
            ]
            injections = [
                {"nid": "4", "msg": {'payload': 'hello'}},
            ]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
            assert msgs[0]["payload"] == "123"

        @pytest.mark.asyncio
        @pytest.mark.it('should call link-in node by name and get response')
        async def test_it_should_call_link_in_node_by_name_and_get_response(self):
            payload = float(time.time())
            flows = [
                {"id": "100", "type": "tab", "label": "Flow 1"},
                {"id": "200", "type": "tab", "label": "Flow 2"},
                {"id": "1", "z": "100", "type": "link in",
                    "name": "double payload", "wires": [["3"]]},
                {"id": "2", "z": "200", "type": "link in",
                    "name": "double payload", "wires": [["3"]]},
                {"id": "3", "z": "100", "type": "function",
                    "func": 'msg.payload += msg.payload; return msg;', "wires": [["4"]]},
                {"id": "4", "z": "100", "type": "link out", "mode": "return"},
                {"id": "5", "z": "100", "type": "link call",
                    "linkType": "dynamic", "links": [], "wires": [["6"]]},
                {"id": "6", "z": "100", "type": "test-once"}
            ]
            injections = [
                {"nid": "5", "msg": {'payload': payload, 'target': 'double payload'}},
            ]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
            print(msgs)
            assert int(round(msgs[0]["payload"])) == int(round(payload + payload))

        @pytest.mark.asyncio
        @pytest.mark.it('should timeout waiting for link return')
        async def test_should_timeout_waiting_for_link_return(self):
            # The `link out` is in its normal (non-return) mode, so it never answers the call: after
            # `timeout` the original message travels back through the catch node carrying the error.
            flows = [
                {"id": "100", "type": "tab", "label": "Flow 1"},
                {"id": "1", "z": "100", "type": "link in", "name": "double payload", "wires": [["3"]]},
                {"id": "3", "z": "100", "type": "function",
                    "func": "msg.payload += msg.payload; return msg;", "wires": [["4"]]},
                {"id": "4", "z": "100", "type": "link out", "mode": ""},
                {"id": "5", "z": "100", "type": "link call", "linkType": "static", "timeout": "0.5",
                    "links": ["1"], "wires": [["6"]]},
                {"id": "7", "z": "100", "type": "catch", "scope": ["5"], "uncaught": True, "wires": [["6"]]},
                {"id": "6", "z": "100", "type": "test-once"},
            ]
            injections = [
                {"nid": "5", "msg": {"payload": "hello", "target": "double payload"}},
            ]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1, timeout=1.5)
            assert msgs[0]["target"] == "double payload"
            assert msgs[0]["error"]["message"] == "timeout"
            assert "source" in msgs[0]["error"]

        @pytest.mark.asyncio
        @pytest.mark.it('should raise error due to multiple targets on same tab')
        async def test_should_raise_error_due_to_multiple_targets_on_same_tab(self):
            flows = [
                {"id": "100", "type": "tab", "label": "Flow 1"},
                {"id": "1", "z": "100", "type": "link in", "name": "double payload", "wires": [["3"]]},
                {"id": "2", "z": "100", "type": "link in", "name": "double payload", "wires": [["3"]]},
                {"id": "3", "z": "100", "type": "function",
                    "func": "msg.payload += msg.payload; return msg;", "wires": [["4"]]},
                {"id": "4", "z": "100", "type": "link out", "mode": "return"},
                {"id": "5", "z": "100", "type": "link call", "linkType": "dynamic", "links": [], "wires": [["6"]]},
                {"id": "7", "z": "100", "type": "catch", "scope": ["5"], "uncaught": True, "wires": [["6"]]},
                {"id": "6", "z": "100", "type": "test-once"},
            ]
            injections = [
                {"nid": "5", "msg": {"payload": "hello", "target": "double payload"}},
            ]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
            assert msgs[0]["target"] == "double payload"
            assert "Multiple link-in nodes" in msgs[0]["error"]["message"]
            assert "source" in msgs[0]["error"]

        @pytest.mark.asyncio
        @pytest.mark.it('should raise error due to multiple targets on different tabs')
        async def test_should_raise_error_due_to_multiple_targets_on_different_tabs(self):
            flows = [
                {"id": "100", "type": "tab", "label": "Flow 1"},
                {"id": "200", "type": "tab", "label": "Flow 2"},
                {"id": "300", "type": "tab", "label": "Flow 3"},
                {"id": "1", "z": "200", "type": "link in", "name": "double payload", "wires": [["3"]]},
                {"id": "2", "z": "300", "type": "link in", "name": "double payload", "wires": [["3"]]},
                {"id": "3", "z": "100", "type": "function",
                    "func": "msg.payload += msg.payload; return msg;", "wires": [["4"]]},
                {"id": "4", "z": "100", "type": "link out", "mode": "return"},
                {"id": "5", "z": "100", "type": "link call", "linkType": "dynamic", "links": [], "wires": [["6"]]},
                {"id": "7", "z": "100", "type": "catch", "scope": ["5"], "uncaught": True, "wires": [["6"]]},
                {"id": "6", "z": "100", "type": "test-once"},
            ]
            injections = [
                {"nid": "5", "msg": {"payload": "hello", "target": "double payload"}},
            ]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
            assert msgs[0]["target"] == "double payload"
            assert "Multiple link-in nodes" in msgs[0]["error"]["message"]
            assert "source" in msgs[0]["error"]

        @pytest.mark.skip(reason="the spec redeploys the flow (`helper.setFlows`) to rename a "
                                 "duplicate link-in and then calls the new name: the pytest bridge "
                                 "loads the engine once per test and has no redeploy API")
        @pytest.mark.asyncio
        @pytest.mark.it('should not raise error after deploying a name change to a duplicate link-in node')
        async def test_should_not_raise_error_after_deploying_a_name_change(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should allow nested link-call flows')
        async def test_it_should_allow_nested_link_call_flows_link_call_node(self):
            payload = float(time.time())
            flows = [
                # Multiply by 2 link flow
                {"id": "100", "type": "tab", "label": "Flow 1"},
                {"id": "1", "z": "100", "type": "link in", "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "function",
                    "func": "msg.payload *= 2.0; return msg;", "wires": [["3"]]},
                {"id": "3", "z": "100", "type": "link out", "mode": "return"},

                # Multiply by 3 link flow
                {"id": "4", "z": "100", "type": "link in", "wires": [["5"]]},
                {"id": "5", "z": "100", "type": "function",
                    "func": "msg.payload *= 3.0; return msg;", "wires": [["6"]]},
                {"id": "6", "z": "100", "type": "link out", "mode": "return"},

                # Multiply by 6 link flow
                {"id": "7", "z": "100", "type": "link in", "wires": [["8"]]},
                {"id": "8", "z": "100", "type": "link call", "links": ["1"], "wires": [["9"]]},
                {"id": "9", "z": "100", "type": "link call", "links": ["4"], "wires": [["10"]]},
                {"id": "10", "z": "100", "type": "link out", "mode": "return"},

                # Test Flow Entry
                {"id": "11", "z": "100", "type": "link call",
                    "links": ["7"], "wires": [["999"]]},
                {"id": "999", "z": "100", "type": "test-once"},
            ]
            injections = [
                {"nid": "11", "msg": {'payload': 4.0}},
            ]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
            assert msgs[0]["payload"] == 24.0

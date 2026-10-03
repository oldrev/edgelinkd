import copy

import pytest
from tests import *

@pytest.mark.describe('SORT node')
class TestSortNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_loaded(self):
        node = {"type": "sort", "order": "ascending", "as_num": False, "name": "SortNode"}
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": [1, 2, 3]}], 1)
        assert node["name"] == "SortNode"

    # Test elem sorting (not number, ascending)
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, not number, ascending)')
    async def test_sort_payload_elem_not_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": False}
        injections = [{"payload": ["200", "4", "30", "1000"]}]
        expected = ["1000", "200", "30", "4"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, not number, ascending)')
    async def test_sort_msg_prop_elem_not_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": False, "target": "data", "targetType": "msg", "msgKey": "", "msgKeyType": "elem"}
        injections = [{"data": ["200", "4", "30", "1000"]}]
        expected = ["1000", "200", "30", "4"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["data"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (not number, ascending)')
    async def test_sort_group_payload_not_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": False, "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["1000", "200", "30", "4"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"payload": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        # Sort output by original index to check correct order
        sorted_out = sorted(out, key=lambda m: expected.index(m["payload"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (not number, ascending)')
    async def test_sort_group_prop_not_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": False, "targetType": "seq", "seqKey": "data", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["1000", "200", "30", "4"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"data": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["data"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    # Test elem sorting (not number, descending)
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, not number, descending)')
    async def test_sort_payload_elem_not_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": False}
        injections = [{"payload": ["200", "4", "30", "1000"]}]
        expected = ["4", "30", "200", "1000"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, not number, descending)')
    async def test_sort_msg_prop_elem_not_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": False, "target": "data", "targetType": "msg", "msgKey": "", "msgKeyType": "elem"}
        injections = [{"data": ["200", "4", "30", "1000"]}]
        expected = ["4", "30", "200", "1000"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["data"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (not number, descending)')
    async def test_sort_group_payload_not_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": False, "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["4", "30", "200", "1000"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"payload": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["payload"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (not number, descending)')
    async def test_sort_group_prop_not_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": False, "targetType": "seq", "seqKey": "data", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["4", "30", "200", "1000"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"data": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["data"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    # Test elem sorting (number, ascending)
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, number, ascending)')
    async def test_sort_payload_elem_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": True}
        injections = [{"payload": ["200", "4", "30", "1000"]}]
        expected = ["4", "30", "200", "1000"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, number, ascending)')
    async def test_sort_msg_prop_elem_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": True, "target": "data", "targetType": "msg", "msgKey": "", "msgKeyType": "elem"}
        injections = [{"data": ["200", "4", "30", "1000"]}]
        expected = ["4", "30", "200", "1000"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["data"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (number, ascending)')
    async def test_sort_group_payload_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": True, "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["4", "30", "200", "1000"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"payload": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["payload"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (number, ascending)')
    async def test_sort_group_prop_number_ascending(self):
        node = {"type": "sort", "order": "ascending", "as_num": True, "targetType": "seq", "seqKey": "data", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["4", "30", "200", "1000"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"data": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["data"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    # Test elem sorting (number, descending)
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, number, descending)')
    async def test_sort_payload_elem_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": True}
        injections = [{"payload": ["200", "4", "30", "1000"]}]
        expected = ["1000", "200", "30", "4"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, number, descending)')
    async def test_sort_msg_prop_elem_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": True, "target": "data", "targetType": "msg", "msgKey": "", "msgKeyType": "elem"}
        injections = [{"data": ["200", "4", "30", "1000"]}]
        expected = ["1000", "200", "30", "4"]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["data"] == expected

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (number, descending)')
    async def test_sort_group_payload_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": True, "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["1000", "200", "30", "4"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"payload": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["payload"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (number, descending)')
    async def test_sort_group_prop_number_descending(self):
        node = {"type": "sort", "order": "descending", "as_num": True, "targetType": "seq", "seqKey": "data", "seqKeyType": "msg"}
        data_in = ["200", "4", "30", "1000"]
        expected = ["1000", "200", "30", "4"]
        msgs = []
        for i, v in enumerate(data_in):
            msg = {"data": v, "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        out = await run_single_node_with_msgs_ntimes(node, msgs, 4)
        sorted_out = sorted(out, key=lambda m: expected.index(m["data"]))
        for i, msg in enumerate(sorted_out):
            assert msg["parts"]["index"] == i

    # JSONata expression tests (skip because not supported)
    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (exp, not number, ascending)')
    async def test_sort_payload_exp_not_number_ascending(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group (exp, not number, ascending)')
    async def test_sort_group_exp_not_number_ascending(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group (exp, not number, descending)')
    async def test_sort_group_exp_not_number_descending(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (exp, not number, descending)')
    async def test_sort_payload_exp_not_number_descending(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload of objects')
    async def test_sort_payload_objects(self):
        pass

    # Context tests (skip because not fully supported)
    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata (a $flowContext/$globalContext key)")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload by context (exp, not number, ascending)')
    async def test_sort_payload_by_context(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata (a $flowContext/$globalContext key)")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group by context (exp, not number, ascending)')
    async def test_sort_group_by_context(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata (a $flowContext/$globalContext key)")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload by persistable context (exp, not number, descending)')
    async def test_sort_payload_by_persistable_context(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata (a $flowContext/$globalContext key)")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group by persistable context (exp, not number, descending)')
    async def test_sort_group_by_persistable_context(self):
        pass

    # Error handling tests
    @pytest.mark.skip(reason="Rust gap: the sort node does not evaluate its key as JSONata")
    @pytest.mark.asyncio
    @pytest.mark.it('should handle JSONata script error')
    async def test_handle_jsonata_error(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should handle too many pending messages')
    async def test_handle_too_many_pending(self):
        # Upstream sets nodeMessageBufferMaxLength = 2, sends four messages of an incomplete group
        # and expects `sort.too-many` to be reported: once the buffer holds more messages than the
        # limit, the group that has been waiting longest is dropped and the reason is reported on
        # its last message.
        config = copy.deepcopy(TEST_EDGELINLKD_CONFIG)
        config["runtime"]["flow"] = {"node_message_buffer_max_length": 2}
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "sort", "order": "ascending", "as_num": False,
             "target": "payload", "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg",
             "wires": [["3"]]},
            {"id": "2", "z": "100", "type": "catch", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        injections = []
        for i in range(4):
            injections.append({"payload": f"V{i}", "parts": {"id": "X", "index": i, "count": 4}})
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1, config=config)
        # Only the dropped group is reported: the sort node itself emits nothing.
        assert msgs[0]["error"]["message"] == "Too many pending messages in sort node"

    @pytest.mark.skip(reason="upstream asserts the sort.clear node log written when the node closes, and the harness cannot observe node logs")
    @pytest.mark.asyncio
    @pytest.mark.it('should clear pending messages on close')
    async def test_clear_pending_on_close(self):
        pass

    @pytest.mark.describe('messaging API')
    class TestMessagingApi:
        """The `done()` contract: when a node finishes a message, its `complete`/`catch` nodes hear
        about it. The harness times the outputs with `_since_start_ms`, which is what the upstream
        specs assert with `Date.now()` offsets."""

        async def _run_done_contract(self, target_type, injections, seconds=0.3):
            flows = [
                {"id": "100", "type": "tab"},
                {"id": "1", "z": "100", "type": "sort", "order": "ascending", "as_num": False,
                 "target": "payload", "targetType": target_type, "seqKey": "payload", "seqKeyType": "msg",
                 "wires": [[]]},
                {"id": "2", "z": "100", "type": "complete", "scope": ["1"], "uncaught": False, "wires": [["4"]]},
                {"id": "3", "z": "100", "type": "catch", "scope": ["1"], "uncaught": False, "wires": [["4"]]},
                {"id": "4", "z": "100", "type": "test-once"},
            ]
            scheduled = [{"nid": "1", "msg": msg, "delay_ms": delay} for msg, delay in injections]
            return await run_flow_for_seconds_scheduled(flows, scheduled, seconds)

        @pytest.mark.asyncio
        @pytest.mark.it('should call done() when message is sent (payload)')
        async def test_should_call_done_when_message_is_sent_payload(self):
            msgs = await self._run_done_contract("msg", [({"seq": 0, "payload": [1, 3, 2]}, 0)])
            assert len(msgs) == 1
            # The completion carries the message as the node left it (the sorted payload); upstream
            # only asserts that it has one.
            assert "payload" in msgs[0]
            # The node sends and finishes in one go, so the completion arrives at once (upstream
            # allows 100ms).
            assert msgs[0]["_since_start_ms"] < 100

        @pytest.mark.skip(reason="Rust gap: the sort node completes each message as soon as it is "
                                 "handled, while Node-RED holds `done()` for the messages buffered into "
                                 "a sequence until that sequence is complete (so upstream sees the "
                                 "completions at the end of the sequence, not on arrival)")
        @pytest.mark.asyncio
        @pytest.mark.it('should call done() when message is sent (sequence)')
        async def test_should_call_done_when_message_is_sent_sequence(self):
            pass

        @pytest.mark.skip(reason="Rust gap: the sort node completes each message as soon as it is "
                                 "handled, so the completion timings this spec asserts while the "
                                 "node message buffer overflows are not reproduced")
        @pytest.mark.asyncio
        @pytest.mark.it('should call done() regardless of buffer overflow (same group)')
        async def test_should_call_done_regardless_of_buffer_overflow_same_group(self):
            pass

        @pytest.mark.skip(reason="Rust gap: the sort node completes each message as soon as it is "
                                 "handled, so the completion timings this spec asserts while the "
                                 "node message buffer overflows are not reproduced")
        @pytest.mark.asyncio
        @pytest.mark.it('should call done() regardless of buffer overflow (different group)')
        async def test_should_call_done_regardless_of_buffer_overflow_different_group(self):
            pass

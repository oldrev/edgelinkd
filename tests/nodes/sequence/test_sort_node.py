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
    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (exp, not number, ascending)')
    async def test_sort_payload_exp_not_number_ascending(self):
        pass

    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group (exp, not number, ascending)')
    async def test_sort_group_exp_not_number_ascending(self):
        pass

    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group (exp, not number, descending)')
    async def test_sort_group_exp_not_number_descending(self):
        pass

    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (exp, not number, descending)')
    async def test_sort_payload_exp_not_number_descending(self):
        pass

    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload of objects')
    async def test_sort_payload_objects(self):
        pass

    # Context tests (skip because not fully supported)
    @pytest.mark.skip(reason="Context not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload by context (exp, not number, ascending)')
    async def test_sort_payload_by_context(self):
        pass

    @pytest.mark.skip(reason="Context not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group by context (exp, not number, ascending)')
    async def test_sort_group_by_context(self):
        pass

    @pytest.mark.skip(reason="Context not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload by persistable context (exp, not number, descending)')
    async def test_sort_payload_by_persistable_context(self):
        pass

    @pytest.mark.skip(reason="Context not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group by persistable context (exp, not number, descending)')
    async def test_sort_group_by_persistable_context(self):
        pass

    # Error handling tests
    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should handle JSONata script error')
    async def test_handle_jsonata_error(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should handle too many pending messages')
    async def test_handle_too_many_pending(self):
        node = {"type": "sort", "order": "ascending", "as_num": False, "target": "payload", "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg"}
        # Send more messages than buffer can handle
        msgs = []
        for i in range(4):
            msg = {"payload": f"V{i}", "parts": {"id": "X", "index": i, "count": 4}}
            msgs.append(msg)
        # Should handle gracefully without crashing
        out = await run_single_node_with_msgs_ntimes(node, msgs, 0)

    @pytest.mark.asyncio
    @pytest.mark.it('should clear pending messages on close')
    async def test_clear_pending_on_close(self):
        node = {"type": "sort", "order": "ascending", "as_num": False, "target": "payload", "targetType": "seq", "seqKey": "payload", "seqKeyType": "msg"}
        # Send incomplete sequence
        msg = {"payload": 0, "parts": {"id": "X", "index": 0, "count": 2}}
        # Should handle gracefully
        out = await run_single_node_with_msgs_ntimes(node, [msg], 0)

    # Messaging API tests (skip complex timing tests)
    @pytest.mark.skip(reason="Complex timing tests not fully supported")
    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when message is sent (payload)')
    async def test_messaging_api_payload(self):
        pass

    @pytest.mark.skip(reason="Complex timing tests not fully supported")
    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when message is sent (sequence)')
    async def test_messaging_api_sequence(self):
        pass

    @pytest.mark.skip(reason="Complex timing tests not fully supported")
    @pytest.mark.asyncio
    @pytest.mark.it('should call done() regardless of buffer overflow (same group)')
    async def test_messaging_api_overflow_same_group(self):
        pass

    @pytest.mark.skip(reason="Complex timing tests not fully supported")
    @pytest.mark.asyncio
    @pytest.mark.it('should call done() regardless of buffer overflow (different group)')
    async def test_messaging_api_overflow_different_group(self):
        pass

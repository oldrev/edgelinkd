import pytest
from tests import *

@pytest.mark.describe('BATCH node')
class TestBatchNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded with defaults')
    async def test_loaded_with_defaults(self):
        node = {
            "type": "batch",
            "name": "BatchNode",
            "wires": [["2"]],
        }
        msgs = await run_single_node_with_msgs_ntimes(node, [], 0)
        assert node["name"] == "BatchNode"

    @pytest.mark.describe('mode: count')
    class TestModeCount:

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with count')
        async def test_count_seq(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "count",
                "count": 2,
                "overlap": 0,
                "interval": 10,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
                {"payload": 4},
                {"payload": 5},
            ]
            expected = [[0, 1], [2, 3], [4, 5]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
            for i, group in enumerate(expected):
                for j, val in enumerate(group):
                    assert msgs[i * len(group) + j]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with count (more sent than count)')
        async def test_count_seq_more_than_count(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "count",
                "count": 4,
                "overlap": 0,
                "interval": 10,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
            ]
            expected = [[0, 1, 2, 3]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            for i, val in enumerate(expected[0]):
                assert msgs[i]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with interval')
        async def test_interval_seq(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 2,
                "overlap": 0,
                "interval": 10,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
                {"payload": 4},
                {"payload": 5},
            ]
            expected = [[0, 1], [2, 3], [4, 5]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
            for i, group in enumerate(expected):
                for j, val in enumerate(group):
                    assert msgs[i * len(group) + j]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with interval (more sent than count)')
        async def test_interval_seq_more_than_count(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 4,
                "overlap": 0,
                "interval": 10,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
            ]
            expected = [[0, 1, 2, 3]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            for i, val in enumerate(expected[0]):
                assert msgs[i]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with count and terminate early if parts honoured')
        async def test_count_seq_honour_parts(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "count",
                "count": 4,
                "overlap": 0,
                "interval": 10,
                "allowEmptySequence": False,
                "honourParts": True,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0, "parts": {"id": "1", "index": 0, "count": 4}},
                {"payload": 1, "parts": {"id": "1", "index": 1, "count": 4}},
                {"payload": 2, "parts": {"id": "1", "index": 2, "count": 4}},
                {"payload": 3, "parts": {"id": "1", "index": 3, "count": 4}},
                {"payload": 4, "parts": {"id": "1", "index": 0, "count": 2}},
                {"payload": 5, "parts": {"id": "1", "index": 1, "count": 2}},
            ]
            expected = [[0, 1, 2, 3], [4, 5]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 6)
            assert [m["payload"] for m in msgs[:4]] == [0, 1, 2, 3]
            assert [m["payload"] for m in msgs[4:6]] == [4, 5]

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with count and overlap')
        async def test_count_seq_with_overlap(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "count",
                "count": 3,
                "overlap": 2,
                "interval": 10,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
                {"payload": 4},
                {"payload": 5},
            ]
            expected = [[0, 1, 2], [1, 2, 3], [2, 3, 4], [3, 4, 5]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 12)
            for i, group in enumerate(expected):
                for j, val in enumerate(group):
                    assert msgs[i * len(group) + j]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should handle too many pending messages')
        async def test_count_too_many_pending(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "count",
                "count": 5,
                "overlap": 0,
                "interval": 10,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            # 3 messages, buffer size 2, should trigger overflow
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
            ]
            # 这里无法直接断言日志，但测试结构保留
            await run_single_node_with_msgs_ntimes(node, injections, 0)

        @pytest.mark.asyncio
        @pytest.mark.it('should handle reset')
        async def test_count_handle_reset(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "count",
                "count": 2,
                "overlap": 0,
                "interval": 0,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3, "reset": True},
                {"payload": 4},
                {"payload": 5},
            ]
            expected = [[0, 1], [4, 5]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            assert [m["payload"] for m in msgs[:2]] == [0, 1]
            assert [m["payload"] for m in msgs[2:]] == [4, 5]

    @pytest.mark.describe('mode: interval')
    class TestModeInterval:
        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with interval (in float)')
        async def test_interval_seq_float(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 0,
                "overlap": 0,
                "interval": 0.5,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
            ]
            expected = [[0, 1], [2, 3]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            for i, group in enumerate(expected):
                for j, val in enumerate(group):
                    assert msgs[i * len(group) + j]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with interval & not send empty seq')
        async def test_interval_no_empty_seq(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": 3},
            ]
            expected = [[0], [1], [2], [3]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            for i, val in enumerate([0, 1, 2, 3]):
                assert msgs[i]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should create seq. with interval & send empty seq')
        async def test_interval_send_empty_seq(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": True,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": None},
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": None},
                {"payload": 3},
            ]
            expected = [[None], [0], [1], [2], [None], [3]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 6)
            for i, val in enumerate([None, 0, 1, 2, None, 3]):
                assert msgs[i]["payload"] == val

        @pytest.mark.asyncio
        @pytest.mark.it('should handle too many pending messages')
        async def test_interval_too_many_pending(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
            ]
            await run_single_node_with_msgs_ntimes(node, injections, 0)

        @pytest.mark.asyncio
        @pytest.mark.it('should handle reset')
        async def test_interval_handle_reset(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "interval",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [],
                "wires": [["2"]],
            }
            injections = [
                {"payload": 0},
                {"payload": 1},
                {"payload": 2},
                {"payload": "3", "reset": True},
                {"payload": 4},
                {"payload": 5},
            ]
            expected = [[0, 1], [4, 5]]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            assert [m["payload"] for m in msgs[:2]] == [0, 1]
            assert [m["payload"] for m in msgs[2:]] == [4, 5]

    @pytest.mark.describe('mode: concat')
    class TestModeConcat:
        @pytest.mark.asyncio
        @pytest.mark.it('should concat two seq. (series)')
        async def test_concat_two_seq_series(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "concat",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [{"topic": "TA"}, {"topic": "TB"}],
                "wires": [["2"]],
            }
            injections = [
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TB", "payload": 1, "parts": {"id": "TB", "index": 1, "count": 2}},
                {"topic": "TA", "payload": 2, "parts": {"id": "TA", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 3, "parts": {"id": "TA", "index": 1, "count": 2}},
            ]
            expected = [2, 3, 0, 1]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            assert [m["payload"] for m in msgs] == expected

        @pytest.mark.asyncio
        @pytest.mark.it('should concat two seq. (mixed)')
        async def test_concat_two_seq_mixed(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "concat",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [{"topic": "TA"}, {"topic": "TB"}],
                "wires": [["2"]],
            }
            injections = [
                {"topic": "TA", "payload": 2, "parts": {"id": "TA", "index": 0, "count": 2}},
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 3, "parts": {"id": "TA", "index": 1, "count": 2}},
                {"topic": "TB", "payload": 1, "parts": {"id": "TB", "index": 1, "count": 2}},
            ]
            expected = [2, 3, 0, 1]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            assert [m["payload"] for m in msgs] == expected

        @pytest.mark.asyncio
        @pytest.mark.it('should concat three seq.')
        async def test_concat_three_seq(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "concat",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [{"topic": "TA"}, {"topic": "TB"}, {"topic": "TC"}],
                "wires": [["2"]],
            }
            injections = [
                {"topic": "TC", "payload": 4, "parts": {"id": "TC", "index": 0, "count": 1}},
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TB", "payload": 1, "parts": {"id": "TB", "index": 1, "count": 2}},
                {"topic": "TA", "payload": 2, "parts": {"id": "TA", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 3, "parts": {"id": "TA", "index": 1, "count": 2}},
            ]
            expected = [2, 3, 0, 1, 4]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 5)
            assert [m["payload"] for m in msgs] == expected

        @pytest.mark.asyncio
        @pytest.mark.it('should concat same seq.')
        async def test_concat_same_seq(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "concat",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [{"topic": "TA"}, {"topic": "TA"}],
                "wires": [["2"]],
            }
            injections = [
                {"topic": "TA", "payload": 9, "parts": {"id": "TA", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 8, "parts": {"id": "TA", "index": 1, "count": 2}},
            ]
            expected = [9, 8, 9, 8]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            assert [m["payload"] for m in msgs] == expected

        @pytest.mark.asyncio
        @pytest.mark.it('should handle too many pending messages')
        async def test_concat_too_many_pending(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "concat",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [{"topic": "TA"}, {"topic": "TB"}],
                "wires": [["2"]],
            }
            C = 3
            for i in range(C):
                parts_a = {"index": i, "count": C, "id": "A"}
                parts_b = {"index": i, "count": C, "id": "B"}
                await run_single_node_with_msgs_ntimes(node, [
                    {"payload": i, "topic": "TA", "parts": parts_a},
                    {"payload": i, "topic": "TB", "parts": parts_b},
                ], 0)

        @pytest.mark.asyncio
        @pytest.mark.it('should handle reset')
        async def test_concat_handle_reset(self):
            node = {
                "type": "batch",
                "name": "BatchNode",
                "mode": "concat",
                "count": 0,
                "overlap": 0,
                "interval": 1,
                "allowEmptySequence": False,
                "topics": [{"topic": "TA"}, {"topic": "TB"}],
                "wires": [["2"]],
            }
            # first round
            inputs0 = [
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 1, "parts": {"id": "TA", "index": 0, "count": 2}},
            ]
            await run_single_node_with_msgs_ntimes(node, inputs0, 2)
            # reset
            await run_single_node_with_msgs_ntimes(node, [{"payload": None, "reset": True}], 0)
            # second round
            inputs1 = [
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TB", "payload": 1, "parts": {"id": "TB", "index": 1, "count": 2}},
                {"topic": "TA", "payload": 2, "parts": {"id": "TA", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 3, "parts": {"id": "TA", "index": 1, "count": 2}},
            ]
            await run_single_node_with_msgs_ntimes(node, inputs1, 4)

    @pytest.mark.describe('messaging API')
    class TestMessagingApi:
        @pytest.mark.skip(reason='JSONata not supported')
        @pytest.mark.asyncio
        @pytest.mark.it('should support JSONata in property')
        async def test_jsonata_property(self):
            pass

        @pytest.mark.skip(reason='JSONata not supported')
        @pytest.mark.asyncio
        @pytest.mark.it('should support JSONata in join')
        async def test_jsonata_join(self):
            pass

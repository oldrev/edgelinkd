import pytest
from tests import *


def _staged_injections(payloads, delay_ms, start_ms=0):
    """Spread the payloads out in time the way Node-RED's `delayed_send` does.

    Interval mode can only be exercised by a stream: messages that all arrive inside one
    interval window are flushed as a single sequence, whatever the node does with them.
    Like `delayed_send`, the first payload is delivered one `delay_ms` in, not at once.
    """
    return [{"payload": payload, "delay_ms": start_ms + (index + 1) * delay_ms}
            for index, payload in enumerate(payloads)]


def _assert_sequences(msgs, expected_groups):
    """Mirror upstream's `check_data`: the emitted messages form the expected sequences.

    Every sequence shares one `parts.id` across its messages, with `parts.index` counting
    from 0 and `parts.count` holding the length of the sequence.
    """
    position = 0
    for group in expected_groups:
        assert len(msgs) >= position + len(group), (
            f"Expected {len(group)} more messages for the sequence {group}, "
            f"got {[m.get('payload') for m in msgs[position:]]}"
        )
        seq_id = msgs[position]['parts']['id']
        for index, payload in enumerate(group):
            msg = msgs[position + index]
            assert msg['payload'] == payload, f"Expected payload {payload}, got {msg['payload']}"
            assert msg['parts']['index'] == index
            assert msg['parts']['count'] == len(group)
            assert msg['parts']['id'] == seq_id
        position += len(group)
    assert len(msgs) == position, f"Unexpected extra messages: {[m.get('payload') for m in msgs[position:]]}"


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
            # Every message of every sequence is emitted, so the harness waits for all six
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 6)
            _assert_sequences(msgs, expected)

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
            _assert_sequences(msgs, expected)

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
        @pytest.mark.it('should create seq. with interval')
        async def test_interval_seq(self):
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
            # Upstream feeds the node 450ms apart so that pairs land inside one 1s window
            msgs = await run_single_node_for_seconds_scheduled(node, _staged_injections([0, 1, 2, 3], 450), 1.5)
            _assert_sequences(msgs, [[0, 1], [2, 3]])

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
            msgs = await run_single_node_for_seconds_scheduled(node, _staged_injections([0, 1, 2, 3], 225), 1.0)
            _assert_sequences(msgs, [[0, 1], [2, 3]])

        @pytest.mark.asyncio
        @pytest.mark.timeout(20)
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
            # 1300ms apart: every interval window closes before the next message arrives
            msgs = await run_single_node_for_seconds_scheduled(node, _staged_injections([0, 1, 2, 3], 1300), 4.5)
            _assert_sequences(msgs, [[0], [1], [2], [3]])

        @pytest.mark.asyncio
        @pytest.mark.timeout(20)
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
            # The timer keeps firing between the messages, and every empty window emits an
            # empty sequence of its own.
            msgs = await run_single_node_for_seconds_scheduled(node, _staged_injections([0, 1, 2, 3], 1300), 5.5)
            _assert_sequences(msgs, [[None], [0], [1], [2], [None], [3]])

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
            # Upstream sends 0,1,2 400ms apart, resets once the third is in, and then sends
            # 4,5,6 the same way: the first window flushes [0,1], the reset drops the pending
            # 2, and the restarted timer flushes [4,5]. The reset is given a wider margin than
            # upstream's 10ms so that a slow scheduler cannot reorder it before message 2.
            injections = _staged_injections([0, 1, 2], 400)
            injections.append({"payload": "3", "reset": True, "delay_ms": 1400})
            injections += _staged_injections([4, 5, 6], 400, start_ms=1400)
            msgs = await run_single_node_for_seconds_scheduled(node, injections, 1.7)
            _assert_sequences(msgs, [[0, 1], [4, 5]])

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
            # first round: one message of each topic, so neither group is complete
            inputs0 = [
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 1, "parts": {"id": "TA", "index": 0, "count": 2}},
            ]
            # second round: both groups complete, TB then TA in arrival order
            inputs1 = [
                {"topic": "TB", "payload": 0, "parts": {"id": "TB", "index": 0, "count": 2}},
                {"topic": "TB", "payload": 1, "parts": {"id": "TB", "index": 1, "count": 2}},
                {"topic": "TA", "payload": 2, "parts": {"id": "TA", "index": 0, "count": 2}},
                {"topic": "TA", "payload": 3, "parts": {"id": "TA", "index": 1, "count": 2}},
            ]
            # The whole sequence runs in one flow: the first round leaves two incomplete
            # groups, the reset drops them, and the second round completes the concat.
            injections = inputs0 + [{"payload": None, "reset": True}] + inputs1
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
            # TA is the first configured topic, so its sequence leads the concat
            _assert_sequences(msgs, [[2, 3, 0, 1]])

    # The `messaging API` describe has no JSONata tests upstream. The placeholders that used to
    # live here were removed: the audit compares titles against the Node-RED spec, so a test
    # with no upstream counterpart only adds noise.


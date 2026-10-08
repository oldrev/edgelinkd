import pytest
from tests import *


def _flow(**settings):
    return [
        {"id": "0", "type": "tab"},
        {"id": "1", "z": "0", "type": "join", "wires": [[]], **settings},
        {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "wires": [["4"]]},
        {"id": "3", "z": "0", "type": "catch", "scope": ["1"], "wires": [["4"]]},
        {"id": "4", "z": "0", "type": "test-once"},
    ]


def _part(value, gid, index, count=2, kind="array", **extra):
    return {"payload": value, "parts": {"id": gid, "index": index, "count": count, "type": kind, **extra}}


@pytest.mark.asyncio
async def test_join_reset_only_releases_the_selected_sequence():
    inputs = [
        _part("A0", "A", 0), _part("B0", "B", 0),
        {"reset": True, "payload": "reset", "parts": {"id": "A"}},
        _part("B1", "B", 1),
    ]
    flows = _flow(mode="auto")
    # Also observe output and tag it so completion and output cannot be confused.
    flows[1]["wires"] = [["5"]]
    flows.extend([
        {"id": "5", "z": "0", "type": "function", "func": 'msg.joined = true; return msg;', "wires": [["4"]]}
    ])
    out = await run_flow_with_msgs_ntimes(flows, inputs, 5)
    assert [msg["payload"] for msg in out if msg.get("joined")] == [["B0", "B1"]]
    assert [msg["payload"] for msg in out if not msg.get("joined")] == ["A0", "reset", "B0", "B1"]


@pytest.mark.asyncio
async def test_join_custom_useparts_false_removes_current_parts_but_restores_parent():
    parent = {"id": "outer", "index": 4, "count": 5}
    out = await run_single_node_with_msgs_ntimes(
        {"type": "join", "mode": "custom", "build": "array", "count": 2, "useparts": False},
        [_part("a", "A", 7, parts=parent), _part("b", "B", 9, parts=parent)], 1)
    assert out[0]["payload"] == ["a", "b"]
    assert out[0]["parts"] == parent
    out = await run_flow_with_msgs_ntimes(_flow(mode="custom", count=1, useparts=False),
                                         [_part("a", "A", 0)], 1)
    assert "parts" not in out[0]


@pytest.mark.asyncio
async def test_join_retains_earlier_fields_and_uses_latest_message_properties():
    out = await run_single_node_with_msgs_ntimes(
        {"type": "join", "mode": "custom", "count": 2},
        [{"payload": "a", "first_only": True, "topic": "first"},
         {"payload": "b", "second_only": True, "topic": "last"}], 1)
    assert out[0]["payload"] == ["a", "b"]
    assert out[0]["first_only"] is True and out[0]["second_only"] is True
    assert out[0]["topic"] == "last"


@pytest.mark.asyncio
async def test_join_accumulation_emits_on_each_update_and_complete_starts_fresh():
    node = {"type": "join", "mode": "custom", "build": "object", "count": 2, "accumulate": True}
    out = await run_single_node_with_msgs_ntimes(node, [
        {"topic": "a", "payload": 1}, {"topic": "b", "payload": 2},
        {"topic": "a", "payload": 3}, {"complete": True},
        {"topic": "c", "payload": 4}, {"topic": "d", "payload": 5},
    ], 4)
    assert [msg["payload"] for msg in out] == [
        {"a": 1, "b": 2}, {"a": 3, "b": 2}, {"a": 3, "b": 2}, {"c": 4, "d": 5}]
    assert all("complete" not in msg for msg in out)


@pytest.mark.asyncio
async def test_join_empty_string_parts_count_as_received():
    out = await run_single_node_with_msgs_ntimes({"type": "join", "mode": "auto"},
        [_part("", "A", 0, kind="string", ch=":"), _part("b", "A", 1, kind="string", ch=":")], 1)
    assert out[0]["payload"] == ":b"


@pytest.mark.asyncio
async def test_join_auto_reassembles_array_chunks():
    out = await run_single_node_with_msgs_ntimes({"type": "join", "mode": "auto"},
        [_part([3], "A", 1, len=2), _part([1, 2], "A", 0, len=2)], 1)
    assert out[0]["payload"] == [1, 2, 3]


@pytest.mark.asyncio
async def test_join_blank_custom_count_uses_parts_count_and_group_id():
    out = await run_single_node_with_msgs_ntimes({"type": "join", "mode": "custom", "count": ""},
        [_part("A0", "A", 9), _part("B0", "B", 0), _part("A1", "A", 0), _part("B1", "B", 9)], 2)
    assert [msg["payload"] for msg in out] == [["A0", "A1"], ["B0", "B1"]]


@pytest.mark.asyncio
async def test_join_reduce_initial_value_is_evaluated_for_each_group():
    node = {"type": "join", "mode": "reduce", "reduceExp": "$A+payload",
            "reduceInit": "start", "reduceInitType": "flow"}
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "function", "wires": [["5"]],
              "func": 'flow.set("start",msg.start); return msg;'},
             {"id": "5", "z": "0", **node, "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    inputs = [{**_part(1, "A", 0, count=1), "start": 10},
              {**_part(2, "B", 0, count=1), "start": 20}]
    # Set the second context value after the first group has been reduced.
    out = await run_flow_for_seconds_scheduled(flows,
        [{"nid": "1", "msg": msg, "delay_ms": i * 100} for i, msg in enumerate(inputs)], 0.2)
    assert [msg["payload"] for msg in out] == [11, 22]


@pytest.mark.asyncio
async def test_join_reduce_failure_completes_remaining_messages_once():
    flows = _flow(mode="reduce", reduceExp="$A + $number(payload)", reduceInit="0", reduceInitType="num")
    inputs = [_part("1", "A", 1), _part("bad", "A", 0)]
    out = await run_flow_for_seconds(flows, inputs, 0.05)
    assert len(out) == 2
    assert [msg["payload"] for msg in out if "error" in msg] == ["bad"]
    assert [msg["payload"] for msg in out if "error" not in msg] == ["1"]


@pytest.mark.asyncio
async def test_join_reduce_overflow_releases_all_groups():
    config = copy.deepcopy(TEST_EDGELINLKD_CONFIG)
    config["runtime"]["flow"] = {"node_message_buffer_max_length": 2}
    flows = _flow(mode="reduce", reduceExp="$A+payload", reduceInit="0", reduceInitType="num")
    out = await edgelink.run_flows_for_once(0.05, flows,
        [("1", _part(value, gid, 0), 0) for value, gid in [(1, "A"), (2, "B"), (3, "C")]], config)
    assert len(out) == 3
    assert [msg["payload"] for msg in out if "error" in msg] == [3]
    assert sorted(msg["payload"] for msg in out if "error" not in msg) == [1, 2]


@pytest.mark.asyncio
async def test_join_buffer_completion_error_releases_pending_messages():
    inputs = [_part([65], "A", 1, kind="buffer"), _part([66], "A", 2, kind="buffer")]
    # Enough received parts, but an absent index 0 makes the buffer invalid.
    out = await run_flow_for_seconds(_flow(mode="auto"), inputs, 0.05)
    assert len(out) == 2
    assert [msg["payload"] for msg in out if "error" in msg] == [[66]]
    assert [msg["payload"] for msg in out if "error" not in msg] == [[65]]
    assert "missing buffer part" in next(msg["error"]["message"] for msg in out if "error" in msg)


@pytest.mark.asyncio
async def test_join_timed_completion_releases_pending_messages():
    out = await run_flow_for_seconds(
        _flow(mode="custom", build="string", joiner=",", timeout=0.05, count=""),
        [{"payload": "a"}, {"payload": "b"}], 0.05)
    assert [msg["payload"] for msg in out] == ["a", "b"]
    assert all("error" not in msg and 40 < msg["_since_start_ms"] < 150 for msg in out)


@pytest.mark.asyncio
async def test_join_count_accepts_fractional_editor_value():
    out = await run_single_node_with_msgs_ntimes({"type": "join", "mode": "custom", "count": "2.5"},
        [{"payload": 1}, {"payload": 2}, {"payload": 3}], 1)
    assert out[0]["payload"] == [1, 2, 3]

@pytest.mark.asyncio
async def test_join_flattened_chunks_skip_sparse_holes():
    inputs = [
        _part([1, 2], "A", 0, count=0, len=2),
        {**_part([5], "A", 2, count=0, len=2), "complete": True},
    ]
    out = await run_single_node_with_msgs_ntimes({"type": "join", "mode": "auto"}, inputs, 1)
    assert out[0]["payload"] == [1, 2, 5]


@pytest.mark.asyncio
async def test_join_duplicate_null_matches_upstream_undefined_comparison():
    inputs = [_part(None, "A", 0), _part(None, "A", 0)]
    out = await run_single_node_with_msgs_ntimes({"type": "join", "mode": "auto"}, inputs, 1)
    assert out[0]["payload"] == [None]

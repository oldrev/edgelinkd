import pytest
from tests import *


@pytest.mark.asyncio
@pytest.mark.parametrize("value, expected", [
    ({"a": 1, "b": 2}, [1, 2]),
    ([3, 4], [3, 4]),
    ("a\nb", ["a", "b"]),
])
async def test_split_preserves_nested_parts_and_group_identity(value, expected):
    previous = {"id": "outer", "index": 3, "count": 9, "parts": {"id": "root"}}
    out = await run_single_node_with_msgs_ntimes(
        {"type": "split"}, [{"payload": value, "parts": previous}], 2)
    assert [msg["payload"] for msg in out] == expected
    assert len({msg["parts"]["id"] for msg in out}) == 1
    assert all(msg["parts"]["parts"] == previous for msg in out)
    assert [msg["parts"]["index"] for msg in out] == [0, 1]


@pytest.mark.asyncio
async def test_split_length_stream_consumes_remainder_once():
    out = await run_single_node_with_msgs_ntimes(
        {"type": "split", "spltType": "len", "splt": "2", "stream": True},
        [{"payload": s} for s in ["1", "2", "34", "5", "6"]], 3)
    assert [msg["payload"] for msg in out] == ["12", "34", "56"]
    assert [msg["parts"]["index"] for msg in out] == [0, 1, 2]
    assert all("count" not in msg["parts"] for msg in out)


@pytest.mark.asyncio
async def test_split_buffer_stream_handles_separator_across_chunks():
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "function", "wires": [["2"]],
              "func": "msg.payload = new Uint8Array(msg.payload).buffer; return msg;"},
             {"id": "2", "z": "0", "type": "split", "spltType": "bin", "splt": "[0,255]",
              "stream": True, "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    out = await run_flow_with_msgs_ntimes(flows,
        [{"payload": v} for v in [[128, 0], [255, 129, 0, 255], [130, 0], [255]]], 3)
    assert [msg["payload"] for msg in out] == [[128], [129], [130]]
    assert [msg["parts"]["index"] for msg in out] == [0, 1, 2]
    assert all(msg["parts"]["type"] == "buffer" and msg["parts"]["ch"] == [0, 255] for msg in out)


@pytest.mark.asyncio
async def test_split_buffer_length_stream_consumes_remainder_once():
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "function", "wires": [["2"]],
              "func": "msg.payload = new Uint8Array(msg.payload).buffer; return msg;"},
             {"id": "2", "z": "0", "type": "split", "spltType": "len", "splt": "2",
              "stream": True, "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    out = await run_flow_with_msgs_ntimes(flows, [{"payload": v} for v in [[1], [2], [3, 4], [5], [6]]], 3)
    assert [msg["payload"] for msg in out] == [[1, 2], [3, 4], [5, 6]]
    assert all("count" not in msg["parts"] for msg in out)


@pytest.mark.asyncio
async def test_split_array_length_accepts_editor_string_and_numeric_object_keys():
    out = await run_single_node_with_msgs_ntimes(
        {"type": "split", "arraySplt": "2"}, [{"payload": [1, 2, 3]}], 2)
    assert [msg["payload"] for msg in out] == [[1, 2], [3]]
    out = await run_single_node_with_msgs_ntimes(
        {"type": "split", "addname": "meta.key"},
        [{"payload": {"10": "ten", "2": "two", "a": "a"}}], 3)
    assert [msg["meta"]["key"] for msg in out] == ["2", "10", "a"]


@pytest.mark.asyncio
async def test_split_utf16_length_and_unsupported_surrogate_boundary():
    out = await run_single_node_with_msgs_ntimes(
        {"type": "split", "splt": 2, "spltType": "len"}, [{"payload": "\U0001f600ab"}], 2)
    assert [msg["payload"] for msg in out] == ["\U0001f600", "ab"]
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "split", "splt": 1, "spltType": "len", "wires": [[]]},
             {"id": "2", "z": "0", "type": "catch", "scope": ["1"], "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    out = await run_flow_with_msgs_ntimes(flows, [{"payload": "\U0001f600"}], 1)
    assert "UTF-16 surrogate pair" in out[0]["error"]["message"]


@pytest.mark.asyncio
async def test_sort_array_mode_ignores_sequence_metadata_and_handles_nested_target():
    out = await run_single_node_with_msgs_ntimes(
        {"type": "sort", "targetType": "msg", "target": "data.values"},
        [{"data": {"values": [3, 1, 2]}, "parts": {"id": "X", "index": 0, "count": 2}, "reset": True}], 1)
    assert out[0]["data"]["values"] == [1, 2, 3]
    assert out[0]["parts"] == {"id": "X", "index": 0, "count": 2}


@pytest.mark.asyncio
async def test_sort_sequence_drops_messages_without_valid_parts():
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "sort", "targetType": "seq", "wires": [["3"]]},
             {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    out = await run_flow_with_msgs_ntimes(flows,
        [{"payload": [3, 1, 2]}, {"payload": [2, 1], "parts": {"id": "X"}}], 2)
    assert [msg["payload"] for msg in out] == [[3, 1, 2], [2, 1]]


@pytest.mark.asyncio
async def test_sort_sequence_numeric_ids_count_updates_and_stability():
    injections = [
        {"payload": 2, "seq": 0, "parts": {"id": 7, "index": 0}},
        {"payload": 1, "seq": 1, "parts": {"id": "B", "index": 0, "count": 2}},
        {"payload": 2, "seq": 2, "parts": {"id": 7, "index": 1}},
        {"payload": 0, "seq": 3, "parts": {"id": "B", "index": 1, "count": 2}},
        {"payload": 1, "seq": 4, "parts": {"id": 7, "index": 2, "count": 3}},
    ]
    out = await run_single_node_with_msgs_ntimes({"type": "sort", "targetType": "seq"}, injections, 5)
    assert [msg["seq"] for msg in out] == [3, 1, 4, 0, 2]
    assert [msg["parts"]["index"] for msg in out] == [0, 1, 0, 1, 2]


@pytest.mark.asyncio
async def test_sort_numeric_mode_uses_javascript_number_coercion():
    out = await run_single_node_with_msgs_ntimes({"type": "sort", "as_num": True},
        [{"payload": ["0x10", True, " 2 ", "", None, False, []]}], 1)
    assert out[0]["payload"] == ["", None, False, [], True, " 2 ", "0x10"]


@pytest.mark.asyncio
async def test_sort_jsonata_failure_completes_remaining_sequence_messages():
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "sort", "targetType": "seq", "seqKeyType": "jsonata",
              "seqKey": "$number(payload)", "wires": [[]]},
             {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "wires": [["4"]]},
             {"id": "3", "z": "0", "type": "catch", "scope": ["1"], "wires": [["4"]]},
             {"id": "4", "z": "0", "type": "test-once"}]
    out = await run_flow_with_msgs_ntimes(flows,
        [{"payload": "1", "seq": 0, "parts": {"id": "A", "index": 0, "count": 2}},
         {"payload": "bad", "seq": 1, "parts": {"id": "A", "index": 1, "count": 2}}], 2)
    assert len([msg for msg in out if "error" in msg]) == 1
    assert next(msg for msg in out if "error" in msg)["seq"] == 1
    assert next(msg for msg in out if "error" not in msg)["seq"] == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [[], {}, False, 42])
async def test_split_empty_collection_or_scalar_completes_without_output(value):
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "split", "wires": [["3"]]},
             {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    out = await run_flow_for_seconds(flows, [{"payload": value}], 0.05)
    assert len(out) == 1
    assert out[0]["payload"] == value


@pytest.mark.asyncio
@pytest.mark.parametrize("settings, expected_count", [
    ({}, 1), ({"spltType": "len", "splt": "2"}, 0)])
async def test_split_empty_string_retains_upstream_count(settings, expected_count):
    out = await run_single_node_with_msgs_ntimes({"type": "split", **settings}, [{"payload": ""}], 1)
    assert out[0]["payload"] == ""
    assert out[0]["parts"]["count"] == expected_count


@pytest.mark.asyncio
async def test_sort_jsonata_array_error_is_caught_without_completion():
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "sort", "targetType": "msg",
              "msgKey": "$unknown()", "msgKeyType": "jsonata", "wires": [["4"]]},
             {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "wires": [["4"]]},
             {"id": "3", "z": "0", "type": "catch", "scope": ["1"], "wires": [["4"]]},
             {"id": "4", "z": "0", "type": "test-once"}]
    out = await run_flow_for_seconds(flows, [{"payload": [2, 1]}], 0.05)
    assert len(out) == 1
    assert "Invalid sort expression" in out[0]["error"]["message"]

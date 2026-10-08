import copy
import pytest
from tests import *


async def _array(data, expected, **config):
    msgs = await run_single_node_with_msgs_ntimes(
        {"type": "sort", "targetType": "msg", **config}, [{"data": data, "payload": data}], 1)
    assert msgs[0][config.get("target", "payload")] == expected


async def _sequence(data, expected, prop="payload", **config):
    msgs = [{"seq": i, prop: value, "parts": {"id": "X", "index": i, "count": len(data), "extra": "kept"}}
            for i, value in enumerate(data)]
    out = await run_single_node_with_msgs_ntimes({"type": "sort", "targetType": "seq", **config}, msgs, len(data))
    assert [msg[prop] for msg in out] == expected
    assert [msg["parts"]["index"] for msg in out] == list(range(len(data)))
    assert all(msg["parts"]["count"] == len(data) and msg["parts"]["id"] == "X"
               and msg["parts"]["extra"] == "kept" for msg in out)


async def _context(sequence, scope, weights, expected, order="ascending", store=False):
    names = ["first", "second", "third", "fourth"]
    suffix = ',"memory"' if store else ""
    expression = f'${scope}Context({"payload" if sequence else "$"}{suffix})'
    node = {"id": "2", "z": "0", "type": "sort", "targetType": "seq" if sequence else "msg",
            "target": "data", "order": order, "wires": [["3"]]}
    node.update({"seqKey": expression, "seqKeyType": "jsonata"} if sequence else
                {"msgKey": expression, "msgKeyType": "jsonata"})
    # Seed the actual flow/global stores through the function node, as upstream seeds context().
    init = ";".join(f'{scope}.set("{name}","{value}"{suffix})' for name, value in zip(names, weights))
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "function", "func": init + ";return msg;", "wires": [["2"]]},
             node, {"id": "3", "z": "0", "type": "test-once"}]
    injections = ([{"payload": name, "parts": {"id": "X", "index": i, "count": 4}}
                   for i, name in enumerate(names)] if sequence else [{"data": names}])
    out = await run_flow_with_msgs_ntimes(flows, injections, 4 if sequence else 1)
    assert ([msg["payload"] for msg in out] if sequence else out[0]["data"]) == expected
    if sequence:
        assert [msg["parts"]["index"] for msg in out] == list(range(4))


def _observed_flow(target_type="seq", **settings):
    return [{"id": "0", "type": "tab"},
            {"id": "1", "z": "0", "type": "sort", "targetType": target_type, "wires": [[]], **settings},
            {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "wires": [["4"]]},
            {"id": "3", "z": "0", "type": "catch", "scope": ["1"], "wires": [["4"]]},
            {"id": "4", "z": "0", "type": "test-once"}]


async def _done(target_type, entries, times):
    config = copy.deepcopy(TEST_EDGELINLKD_CONFIG)
    config["runtime"]["flow"] = {"node_message_buffer_max_length": 2}
    injections = [("1", msg, delay) for msg, delay in entries]
    out = await edgelink.run_flows_for_once(max(times) / 1000 + 0.2,
                                          _observed_flow(target_type), injections, config)
    assert len(out) == len(entries)
    assert sorted(msg["seq"] for msg in out) == list(range(len(entries)))
    for msg in out:
        assert abs(msg["_since_start_ms"] - times[msg["seq"]]) < 100
    return out


def _part(seq, payload, gid, index, count):
    return {"seq": seq, "payload": payload, "parts": {"id": gid, "index": index, "count": count}}


@pytest.mark.describe('SORT node')
class TestSortNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_0001(self):
        await _array([3, 1, 2], [1, 2, 3], name="SortNode")

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, not number, ascending)')
    async def test_0002(self):
        await _array(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], target='payload', order='ascending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, not number, ascending)')
    async def test_0003(self):
        await _array(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], target='data', order='ascending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (not number, ascending)')
    async def test_0004(self):
        await _sequence(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], prop='payload', seqKey='payload', order='ascending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (not number, ascending)')
    async def test_0005(self):
        await _sequence(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], prop='data', seqKey='data', order='ascending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, not number, descending)')
    async def test_0006(self):
        await _array(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], target='payload', order='descending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, not number, descending)')
    async def test_0007(self):
        await _array(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], target='data', order='descending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (not number, descending)')
    async def test_0008(self):
        await _sequence(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], prop='payload', seqKey='payload', order='descending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (not number, descending)')
    async def test_0009(self):
        await _sequence(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], prop='data', seqKey='data', order='descending', as_num=False)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, number, ascending)')
    async def test_0010(self):
        await _array(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], target='payload', order='ascending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, number, ascending)')
    async def test_0011(self):
        await _array(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], target='data', order='ascending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (number, ascending)')
    async def test_0012(self):
        await _sequence(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], prop='payload', seqKey='payload', order='ascending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (number, ascending)')
    async def test_0013(self):
        await _sequence(["200", "4", "30", "1000"], ['4', '30', '200', '1000'], prop='data', seqKey='data', order='ascending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (elem, number, descending)')
    async def test_0014(self):
        await _array(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], target='payload', order='descending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort msg prop (elem, number, descending)')
    async def test_0015(self):
        await _array(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], target='data', order='descending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/payload (number, descending)')
    async def test_0016(self):
        await _sequence(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], prop='payload', seqKey='payload', order='descending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group/prop (number, descending)')
    async def test_0017(self):
        await _sequence(["200", "4", "30", "1000"], ['1000', '200', '30', '4'], prop='data', seqKey='data', order='descending', as_num=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (exp, not number, ascending)')
    async def test_0018(self):
        await _array(["C200", "A4", "B30", "D1000"], ["D1000", "C200", "B30", "A4"], target="data", msgKey="$substring($,1)", msgKeyType="jsonata")

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group (exp, not number, ascending)')
    async def test_0019(self):
        await _sequence(["C200", "A4", "B30", "D1000"], ["D1000", "C200", "B30", "A4"], seqKey="$substring(payload,1)", seqKeyType="jsonata")

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group (exp, not number, descending)')
    async def test_0020(self):
        await _array(["C200", "A4", "B30", "D1000"], ["A4", "B30", "C200", "D1000"], target="data", order="descending", msgKey="$substring($,1)", msgKeyType="jsonata")

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload (exp, not number, descending)')
    async def test_0021(self):
        await _sequence(["C200", "A4", "B30", "D1000"], ["A4", "B30", "C200", "D1000"], order="descending", seqKey="$substring(payload,1)", seqKeyType="jsonata")

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload of objects')
    async def test_0022(self):
        await _array([{"val": v} for v in ["200", "4", "30", "1000"]], [{"val": v} for v in ["4", "30", "200", "1000"]], target="data", as_num=True, msgKey="val", msgKeyType="jsonata")

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload by context (exp, not number, ascending)')
    async def test_0023(self):
        await _context(False, "flow", ["3", "1", "2", "4"], ["second", "third", "first", "fourth"])

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group by context (exp, not number, ascending)')
    async def test_0024(self):
        await _context(True, "global", ["4", "1", "3", "2"], ["second", "fourth", "third", "first"])

    @pytest.mark.asyncio
    @pytest.mark.it('should sort payload by persistable context (exp, not number, descending)')
    async def test_0025(self):
        await _context(False, "global", ["3", "1", "2", "4"], ["fourth", "first", "third", "second"], "descending", True)

    @pytest.mark.asyncio
    @pytest.mark.it('should sort message group by persistable context (exp, not number, descending)')
    async def test_0026(self):
        await _context(True, "flow", ["4", "1", "3", "2"], ["first", "third", "fourth", "second"], "descending", True)

    @pytest.mark.asyncio
    @pytest.mark.it('should handle JSONata script error')
    async def test_0027(self):
        out = await run_flow_for_seconds(_observed_flow(seqKey="$unknown()", seqKeyType="jsonata"),
                                         [_part(0, "A", "X", 0, 2), _part(1, "B", "X", 1, 2)], 0.15)
        assert len(out) == 2
        errors = [msg for msg in out if "error" in msg]
        assert len(errors) == 1 and errors[0]["seq"] == 1
        assert "Invalid sort expression" in errors[0]["error"]["message"]
        assert [msg["seq"] for msg in out if "error" not in msg] == [0]

    @pytest.mark.asyncio
    @pytest.mark.it('should handle too many pending messages')
    async def test_0028(self):
        config = copy.deepcopy(TEST_EDGELINLKD_CONFIG)
        config["runtime"]["flow"] = {"node_message_buffer_max_length": 2}
        flows = _observed_flow()
        out = await edgelink.run_flows_for_once(0.15, flows,
            [("1", _part(i, f"V{i}", "X", i, 4), 0) for i in range(4)], config)
        assert len(out) == 3
        errors = [msg for msg in out if "error" in msg]
        assert len(errors) == 1 and errors[0]["seq"] == 2
        assert errors[0]["error"]["message"] == "Too many pending messages in sort node"
        assert sorted(msg["seq"] for msg in out if "error" not in msg) == [0, 1]

    @pytest.mark.asyncio
    @pytest.mark.it('should clear pending messages on close')
    async def test_0029(self):
        with pytest.raises(RuntimeError, match="Timed out"):
            await run_single_node_with_msgs_ntimes({"type": "sort", "targetType": "seq"},
                                                   [_part(0, 0, "X", 0, 2)], 1, timeout=0.15)
        logs = edgelink.take_node_logs()
        assert any(evt["type"] == "sort" and evt["id"] == "0000000000000001"
                   and evt["msg"] == "clear pending message in sort node" for evt in logs)

    @pytest.mark.describe('messaging API')
    class TestMessagingApi:
        @pytest.mark.asyncio
        @pytest.mark.it('should call done() when message is sent (payload)')
        async def test_0001(self):
            await _done("msg", [({"seq": 0, "payload": [1, 3, 2]}, 0)], [0])

        @pytest.mark.asyncio
        @pytest.mark.it('should call done() when message is sent (sequence)')
        async def test_0002(self):
            await _done("seq", [(_part(0, 3, "A", 0, 2), 0), (_part(1, 2, "A", 1, 2), 500)], [500, 500])

        @pytest.mark.asyncio
        @pytest.mark.it('should call done() regardless of buffer overflow (same group)')
        async def test_0003(self):
            await _done("seq", [(_part(0, 1, "A", 0, 3), 0), (_part(1, 3, "A", 1, 3), 500), (_part(2, 2, "A", 2, 3), 1000)], [1000, 1000, 1000])

        @pytest.mark.asyncio
        @pytest.mark.it('should call done() regardless of buffer overflow (different group)')
        async def test_0004(self):
            out = await _done("seq", [(_part(0, 1, "A", 0, 2), 0), (_part(1, 3, "B", 0, 2), 500),
                                      (_part(2, 5, "C", 0, 2), 1000), (_part(3, 2, "B", 1, 2), 1200),
                                      (_part(4, 4, "C", 1, 2), 1500)], [1000, 1200, 1500, 1200, 1500])
            assert [msg["seq"] for msg in out if "error" in msg] == [0]

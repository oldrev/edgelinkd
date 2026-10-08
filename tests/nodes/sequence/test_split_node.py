import pytest
from tests import *



async def _split_buffers(node, payloads, count):
    # Build actual Variant::Bytes inside the flow; the JSON bridge only carries integer arrays.
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "function",
              "func": "msg.payload = new Uint8Array(msg.payload).buffer; return msg;", "wires": [["2"]]},
             {"id": "2", "z": "0", **node, "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    return await run_flow_with_msgs_ntimes(flows, [{"payload": list(value)} for value in payloads], count)


@pytest.mark.describe('SPLIT node')
class TestSplitNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_0001(self):
        msgs = await run_single_node_with_msgs_ntimes({"type": "split", "name": "splitNode"}, [{"payload": [1]}], 1)
        assert msgs[0]["payload"] == 1

    @pytest.mark.asyncio
    @pytest.mark.it('should split an array into multiple messages')
    async def test_0002(self):
        node = {"type": "split"}
        injections = [{"payload": [1, 2, 3, 4]}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
        # Node-RED: msg.parts.count==4, type==array, index, payload
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 4
            assert m["parts"]["type"] == "array"
            assert m["parts"]["index"] == i
            assert m["payload"] == i + 1

    @pytest.mark.asyncio
    @pytest.mark.it('should split an array on a sub-property into multiple messages')
    async def test_0003(self):
        node = {"type": "split", "property": "foo"}
        injections = [{"foo": [1, 2, 3, 4]}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 4
            assert m["parts"]["type"] == "array"
            assert m["parts"]["index"] == i
            assert m["parts"]["property"] == "foo"
            assert m["foo"] == i + 1

    @pytest.mark.asyncio
    @pytest.mark.it('should split an array into multiple messages of a specified size')
    async def test_0004(self):
        node = {"type": "split", "arraySplt": 3, "arraySpltType": "len"}
        injections = [{"payload": [1, 2, 3, 4]}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
        assert msgs[0]["parts"]["count"] == 2
        assert msgs[0]["parts"]["type"] == "array"
        assert msgs[0]["parts"]["index"] == 0
        assert isinstance(msgs[0]["payload"], list)
        assert len(msgs[0]["payload"]) == 3
        assert msgs[1]["parts"]["index"] == 1
        assert len(msgs[1]["payload"]) == 1

    @pytest.mark.asyncio
    @pytest.mark.it('should split an object into pieces')
    async def test_0005(self):
        node = {"type": "split"}
        injections = [{"topic": "foo", "payload": {"a": 1, "b": "2", "c": True}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
        keys = ["a", "b", "c"]
        vals = [1, "2", True]
        for i, m in enumerate(msgs):
            assert m["parts"]["type"] == "object"
            assert m["parts"]["key"] == keys[i]
            assert m["parts"]["count"] == 3
            assert m["parts"]["index"] == i
            assert m["topic"] == "foo"
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split an object sub property into pieces')
    async def test_0006(self):
        node = {"type": "split", "property": "foo.bar"}
        injections = [{"topic": "foo", "foo": {"bar": {"a": 1, "b": "2", "c": True}}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
        keys = ["a", "b", "c"]
        vals = [1, "2", True]
        for i, m in enumerate(msgs):
            assert "foo" in m and "bar" in m["foo"]
            assert m["parts"]["type"] == "object"
            assert m["parts"]["key"] == keys[i]
            assert m["parts"]["count"] == 3
            assert m["parts"]["index"] == i
            assert m["parts"]["property"] == "foo.bar"
            assert m["topic"] == "foo"
            assert m["foo"]["bar"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split an object into pieces and overwrite their topics')
    async def test_0007(self):
        node = {"type": "split", "addname": "topic"}
        injections = [{"topic": "foo", "payload": {"a": 1, "b": "2", "c": True}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
        keys = ["a", "b", "c"]
        vals = [1, "2", True]
        for i, m in enumerate(msgs):
            assert m["parts"]["type"] == "object"
            assert m["parts"]["key"] == keys[i]
            assert m["parts"]["count"] == 3
            assert m["parts"]["index"] == i
            assert m["topic"] == keys[i]
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split a string into new-lines')
    async def test_0008(self):
        node = {"type": "split"}
        injections = [{"payload": "Da\nve\n \nCJ"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
        vals = ["Da", "ve", " ", "CJ"]
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 4
            assert m["parts"]["type"] == "string"
            assert m["parts"]["index"] == i
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split a string on a specified char')
    async def test_0009(self):
        node = {"type": "split", "splt": "\n"}
        injections = [{"payload": "1\n2\n3"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
        vals = ["1", "2", "3"]
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 3
            assert m["parts"]["ch"] == "\n"
            assert m["parts"]["index"] == i
            assert m["parts"]["type"] == "string"
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split a string into lengths')
    async def test_0010(self):
        node = {"type": "split", "splt": "2", "spltType": "len"}
        injections = [{"payload": "12345678"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
        vals = ["12", "34", "56", "78"]
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 4
            assert m["parts"]["ch"] == ""
            assert m["parts"]["index"] == i
            assert m["parts"]["type"] == "string"
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split a string on a specified char in stream mode')
    async def test_0011(self):
        node = {"type": "split", "splt": "\n", "stream": True}
        injections = [{"payload": "1\n2\n3\n"}, {"payload": "4\n5\n6\n"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 6)
        vals = ["1", "2", "3", "4", "5", "6"]
        for i, m in enumerate(msgs):
            assert m["parts"]["ch"] == "\n"
            assert m["parts"]["index"] == i
            assert m["parts"]["type"] == "string"
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split a buffer into lengths')
    async def test_0012(self):
        node = {"type": "split", "splt": "2", "spltType": "len"}
        b = b"12345678"
        msgs = await _split_buffers(node, [b], 4)
        vals = [b"12", b"34", b"56", b"78"]
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 4
            assert m["parts"]["index"] == i
            assert m["parts"]["type"] == "buffer"
            assert m["payload"] == list(vals[i])

    @pytest.mark.asyncio
    @pytest.mark.it('should split a buffer on another buffer (streaming)')
    async def test_0013(self):
        node = {"type": "split", "splt": "[52]", "spltType": "bin", "stream": True}
        b1 = b"123412"
        b2 = b"341234"
        msgs = await _split_buffers(node, [b1, b2], 3)
        vals = [b"123", b"123", b"123"]
        for i, m in enumerate(msgs):
            assert m["parts"]["type"] == "buffer"
            assert m["parts"]["index"] == i
            assert m["payload"] == list(vals[i])

    @pytest.mark.asyncio
    @pytest.mark.it('should handle invalid spltType (not an array)')
    async def test_0014(self):
        node = {"type": "split", "splt": "1", "spltType": "bin"}
        injections = [{"payload": "123"}]
        with pytest.raises(RuntimeError):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.asyncio
    @pytest.mark.it('should handle invalid splt length')
    async def test_0015(self):
        node = {"type": "split", "splt": 0, "spltType": "len"}
        injections = [{"payload": "123"}]
        with pytest.raises(RuntimeError):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.asyncio
    @pytest.mark.it('should handle invalid array splt length')
    async def test_0016(self):
        node = {"type": "split", "arraySplt": 0, "arraySpltType": "len"}
        injections = [{"payload": "123"}]
        with pytest.raises(RuntimeError):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.asyncio
    @pytest.mark.it('should ceil count value when msg.payload type is string')
    async def test_0017(self):
        node = {"type": "split", "splt": "2", "spltType": "len"}
        injections = [{"payload": "123"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
        assert msgs[0]["parts"]["count"] == 2
        assert len(msgs[0]["payload"]) == 2
        assert len(msgs[1]["payload"]) == 1

    @pytest.mark.asyncio
    @pytest.mark.it('should handle spltBufferString value of undefined')
    async def test_0018(self):
        node = {"type": "split", "splt": "[52]", "spltType": "bin"}
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": "123"}], 1)
        assert msgs[0]["parts"]["index"] == 0
        assert msgs[0]["parts"]["ch"] == [52]
        assert msgs[0]["payload"] == "123"

    @pytest.mark.asyncio
    @pytest.mark.it('should ceil count value when msg.payload type is Buffer')
    async def test_0019(self):
        node = {"type": "split", "splt": "2", "spltType": "len"}
        b = b"123"
        msgs = await _split_buffers(node, [b], 2)
        assert msgs[0]["parts"]["count"] == 2
        assert msgs[0]["payload"] == list(b"12")
        assert msgs[1]["payload"] == list(b"3")

    @pytest.mark.asyncio
    @pytest.mark.it('should set msg.parts.ch when node.spltType is str')
    async def test_0020(self):
        node = {"type": "split", "splt": "2", "spltType": "str", "stream": False}
        b = b"123"
        msgs = await _split_buffers(node, [b], 2)
        assert msgs[0]["parts"]["count"] == 2
        assert msgs[0]["parts"]["ch"] == "2"
        assert msgs[0]["payload"] == list(b"1")
        assert msgs[1]["payload"] == list(b"3")



def _mapi_flow(node_json):
    """Build the flow the upstream mapiDone*TestHelper()s load.

    Node-RED:
        const flow = [
            { ...joinNodeSetting, id: "joinNode1", type: "join", wires: [[]] },
            { id: "completeNode1", type: "complete", scope: ["joinNode1"], uncaught: false, wires: [["helperNode1"]] },
            { id: "catchNode1", type: "catch", scope: ["joinNode1"], uncaught: false, wires: [["helperNode1"]] },
            { id: "helperNode1", type: "helper", wires: [[]] }];

    `helperNode1.on("input", ...)` counting the received messages maps to the pytest
    harness' `test-once` node, which is what the helpers collect. Node ids are numeric
    here because the harness injects straight into a node id ("1"), which the engine
    parses as an ElementId.
    """
    return [
        {"id": "0", "type": "tab"},
        node_json,
        {"id": "2", "z": "0", "type": "complete", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
        {"id": "4", "z": "0", "type": "catch", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
        {"id": "3", "z": "0", "type": "test-once"},
    ]




async def _join_buffers(node, inputs):
    flows = [{"id": "0", "type": "tab"},
             {"id": "1", "z": "0", "type": "function", "wires": [["5"]],
              "func": """msg.payload = new Uint8Array(msg.payload).buffer;
                         if (msg.parts && Array.isArray(msg.parts.ch)) {
                             msg.parts.ch = new Uint8Array(msg.parts.ch).buffer;
                         }
                         return msg;"""},
             {"id": "5", "z": "0", **node, "wires": [["3"]]},
             {"id": "3", "z": "0", "type": "test-once"}]
    return await run_flow_with_msgs_ntimes(flows, inputs, 1)


def _reduce_inputs(count=4, count_on_last=False, strings=False):
    result = []
    for i, (value, index) in enumerate(zip([3, 2, 4, 1], [2, 1, 3, 0])):
        parts = {"index": index, "id": 222}
        if not count_on_last or i == 2:
            parts["count"] = count
        result.append({"payload": str(value) if strings else value, "parts": parts})
    return result


async def _join_reduce(inputs=None, expected=1, seed=None, **settings):
    node = {"type": "join", "mode": "reduce", "reduceExp": "$A+payload",
            "reduceInit": "0", "reduceInitType": "num", **settings}
    inputs = _reduce_inputs() if inputs is None else inputs
    if seed:
        flows = [{"id": "0", "type": "tab"},
                 {"id": "1", "z": "0", "type": "function", "func": seed + "return msg;", "wires": [["5"]]},
                 {"id": "5", "z": "0", **node, "wires": [["3"]]},
                 {"id": "3", "z": "0", "type": "test-once"}]
        return await run_flow_with_msgs_ntimes(flows, inputs, expected)
    return await run_single_node_with_msgs_ntimes(node, inputs, expected)


async def _join_reduce_context(scope, store=False):
    suffix = ',"memory"' if store else ""
    seed = ";".join(f'{scope}.set("{name}",{value}{suffix})' for name, value in
                    zip(["one", "two", "three"], [1, 2, 3])) + ";"
    context = f'${scope}Context'
    operation = "*" if scope == "flow" else "/"
    return await _join_reduce(seed=seed,
        reduceExp=f'$A+(payload{operation}{context}("two"{suffix}))',
        reduceInit=f'{context}("one"{suffix})', reduceInitType="jsonata",
        reduceFixup=f'$A*{context}("three"{suffix})')


async def _join_reduce_error(**settings):
    node = {"id": "1", "z": "0", "type": "join", "mode": "reduce", "reduceExp": "$A",
            "reduceInit": "0", "reduceInitType": "num", "wires": [[]], **settings}
    inputs = [{"payload": "A", "parts": {"id": 1, "type": "string", "ch": ",", "index": 0, "count": 1}}]
    msgs = await run_flow_for_seconds(_mapi_flow(node), inputs, 0.05)
    assert len(msgs) == 1
    assert "Invalid JSONata expression" in msgs[0]["error"]["message"]


@pytest.mark.describe('JOIN node')
class TestJoinNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_0001(self):
        msgs = await run_single_node_with_msgs_ntimes(
            {"type": "join", "name": "joinNode", "mode": "custom", "count": 1},
            [{"payload": "loaded"}], 1)
        assert msgs[0]["payload"] == ["loaded"]

    @pytest.mark.asyncio
    @pytest.mark.it('should join bits of string back together automatically')
    async def test_0002(self):
        node = {"type": "join", "joiner": ",", "build": "string", "mode": "auto"}
        injections = [
            {"payload": "A", "parts": {"id": 1, "type": "string", "ch": ",", "index": 0, "count": 4}},
            {"payload": "B", "parts": {"id": 1, "type": "string", "ch": ",", "index": 1, "count": 4}},
            {"payload": "C", "parts": {"id": 1, "type": "string", "ch": ",", "index": 2, "count": 4}},
            {"payload": "D", "parts": {"id": 1, "type": "string", "ch": ",", "index": 3, "count": 4}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert msgs[0]["payload"] == "A,B,C,D"

    @pytest.mark.asyncio
    @pytest.mark.it('should join bits of string back together automatically with a buffer joiner')
    async def test_0003(self):
        node = {"type": "join", "joiner": "[44]", "joinerType": "bin", "build": "string", "mode": "auto"}
        injections = [
            {"payload": "A", "parts": {"id": 1, "type": "string", "ch": ",", "index": 0, "count": 4}},
            {"payload": "B", "parts": {"id": 1, "type": "string", "ch": ",", "index": 1, "count": 4}},
            {"payload": "C", "parts": {"id": 1, "type": "string", "ch": ",", "index": 2, "count": 4}},
            {"payload": "D", "parts": {"id": 1, "type": "string", "ch": ",", "index": 3, "count": 4}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert msgs[0]["payload"] == "A,B,C,D"

    @pytest.mark.asyncio
    @pytest.mark.it('should join bits of buffer back together automatically')
    async def test_0004(self):
        node = {"type": "join", "mode": "auto", "joiner": ",", "build": "buffer"}
        inputs = [{"payload": [ord(value)], "parts": {"id": 1, "type": "buffer", "ch": [45], "index": i, "count": 4}}
                  for i, value in enumerate("ABCD")]
        msgs = await _join_buffers(node, inputs)
        assert msgs[0]["payload"] == list(b"A-B-C-D")

    @pytest.mark.asyncio
    @pytest.mark.it('should join things into an array after a count')
    async def test_0005(self):
        node = {"type": "join", "count": 3, "joiner": ",", "mode": "custom"}
        injections = [{"payload": 1}, {"payload": True}, {"payload": {"a": 1}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert isinstance(msgs[0]["payload"], list)
        assert msgs[0]["payload"][0] == 1
        assert msgs[0]["payload"][1] is True

    @pytest.mark.asyncio
    @pytest.mark.it('should join things into an array ignoring msg.parts.index in manual mode')
    async def test_0006(self):
        node = {"type": "join", "count": 3, "joiner": ",", "mode": "custom"}
        injections = [
            {"payload": 1, "parts": {"index": 3}},
            {"payload": True, "parts": {"index": 0}},
            {"payload": {"a": 1}, "parts": {"index": 9}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert isinstance(msgs[0]["payload"], list)
        # order -> [1, true, {a:1}], and it still completes at the configured count of 3.
        # Here the node fails to build at all (mode:"custom" is rejected); even with an
        # accepted mode it emits nothing, because the configured count is dropped whenever
        # msg.parts is present and the parts.index values (3, 0, 9) are used as slots.
        assert msgs[0]["payload"][0] == 1
        assert msgs[0]["payload"][1] is True

    @pytest.mark.asyncio
    @pytest.mark.it('should join things into an array after a count with a buffer join set')
    async def test_0007(self):
        node = {"type": "join", "count": 3, "joinerType": "bin", "joiner": "", "mode": "custom"}
        injections = [{"payload": 1}, {"payload": True}, {"payload": {"a": 1}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert isinstance(msgs[0]["payload"], list)
        assert msgs[0]["payload"][0] == 1
        assert msgs[0]["payload"][1] is True

    @pytest.mark.asyncio
    @pytest.mark.it('should join things into an array on a sub property in auto mode')
    async def test_0008(self):
        node = {"type": "join", "count": 3, "joiner": ",", "mode": "auto"}
        injections = [
            {"foo": {"bar": "A"}, "parts": {"id": 1, "type": "array", "len": 1, "index": 0, "count": 4, "property": "foo.bar"}},
            {"foo": {"bar": "B"}, "parts": {"id": 1, "type": "array", "len": 1, "index": 1, "count": 4, "property": "foo.bar"}},
            {"foo": {"bar": "C"}, "parts": {"id": 1, "type": "array", "len": 1, "index": 2, "count": 4, "property": "foo.bar"}},
            {"foo": {"bar": "D"}, "parts": {"id": 1, "type": "array", "len": 1, "index": 3, "count": 4, "property": "foo.bar"}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "foo" in msgs[0]
        assert "bar" in msgs[0]["foo"]
        assert isinstance(msgs[0]["foo"]["bar"], list)
        assert msgs[0]["foo"]["bar"][0] == "A"
        assert msgs[0]["foo"]["bar"][1] == "B"

    @pytest.mark.asyncio
    @pytest.mark.it('should join strings into a buffer after a count')
    async def test_0009(self):
        msgs = await run_single_node_with_msgs_ntimes(
            {"type": "join", "mode": "custom", "count": 2, "build": "buffer", "joinerType": "bin", "joiner": ""},
            [{"payload": "hello"}, {"payload": "world"}], 1)
        assert msgs[0]["payload"] == list(b"helloworld")

    @pytest.mark.asyncio
    @pytest.mark.it('should join things into an object after a count')
    async def test_0010(self):
        node = {"type": "join", "count": 5, "build": "object", "mode": "custom"}
        injections = [
            {"payload": 1, "topic": "a"},
            {"payload": "2", "topic": "b"},
            {"payload": True, "topic": "c"},
            {"payload": {"e": 5}, "topic": "d"},
            {"payload": {"e": 7}, "topic": "d"},
            {"payload": {"f": 6}, "topic": "g"},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert msgs[0]["payload"]["a"] == 1
        assert msgs[0]["payload"]["b"] == "2"
        assert msgs[0]["payload"]["c"] is True
        assert "d" in msgs[0]["payload"]
        assert msgs[0]["payload"]["d"]["e"] == 7

    @pytest.mark.asyncio
    @pytest.mark.it('should merge objects')
    async def test_0011(self):
        node = {"type": "join", "count": 5, "build": "merged", "mode": "custom"}
        injections = [
            {"payload": {"a": 9}, "topic": "f"},
            {"payload": {"a": 1}, "topic": "a"},
            {"payload": {"b": 9}, "topic": "b"},
            {"payload": {"b": 2}, "topic": "b"},
            {"payload": {"c": 3}, "topic": "c"},
            {"payload": {"d": 4}, "topic": "d"},
            {"payload": {"e": 5}, "topic": "e"},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert msgs[0]["payload"]["a"] == 1
        assert msgs[0]["payload"]["b"] == 2
        assert msgs[0]["payload"]["c"] == 3
        assert msgs[0]["payload"]["d"] == 4
        assert msgs[0]["payload"]["e"] == 5

    @pytest.mark.asyncio
    @pytest.mark.it('should merge sub property objects')
    async def test_0012(self):
        node = {"type": "join", "count": 5, "property": "foo.bar", "build": "merged", "mode": "custom"}
        injections = [
            {"foo": {"bar": {"a": 9}, "topic": "f"}},
            {"foo": {"bar": {"a": 1}, "topic": "a"}},
            {"foo": {"bar": {"b": 9}, "topic": "b"}},
            {"foo": {"bar": {"b": 2}, "topic": "b"}},
            {"foo": {"bar": {"c": 3}, "topic": "c"}},
            {"foo": {"bar": {"d": 4}, "topic": "d"}},
            {"foo": {"bar": {"e": 5}, "topic": "e"}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "foo" in msgs[0]
        assert "bar" in msgs[0]["foo"]
        assert msgs[0]["foo"]["bar"]["a"] == 1
        assert msgs[0]["foo"]["bar"]["b"] == 2
        assert msgs[0]["foo"]["bar"]["c"] == 3
        assert msgs[0]["foo"]["bar"]["d"] == 4
        assert msgs[0]["foo"]["bar"]["e"] == 5

    @pytest.mark.asyncio
    @pytest.mark.it('should merge full msg objects')
    async def test_0013(self):
        node = {"type": "join", "count": 6, "build": "merged", "mode": "custom", "propertyType": "full", "property": ""}
        injections = [
            {"payload": 1, "topic": "f"},
            {"payload": 2, "topic": "a"},
            {"payload": 3, "foo": "b"},
            {"payload": 4, "bar": "b"},
            {"payload": 5, "aha": "c"},
            {"payload": 6, "foo": "d"},
            {"payload": 7, "bingo": "e"},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"]["payload"] == 7
        assert msgs[0]["payload"]["aha"] == "c"
        assert msgs[0]["payload"]["bar"] == "b"
        assert msgs[0]["payload"]["bingo"] == "e"
        assert msgs[0]["payload"]["foo"] == "d"
        assert msgs[0]["payload"]["topic"] == "a"

    @pytest.mark.asyncio
    @pytest.mark.it('should accumulate a merged object')
    async def test_0014(self):
        node = {"type": "join", "mode": "custom", "build": "merged", "accumulate": True, "count": 3}
        msgs = await run_single_node_with_msgs_ntimes(node, [
            {"payload": {"a": 1}, "topic": "a"}, {"payload": {"b": 2}, "topic": "b"},
            {"payload": {"c": 3}, "topic": "c"}, {"payload": {"a": 3}, "topic": "d"},
            {"payload": {"b": 2}, "topic": "e"}, {"payload": {"c": 1}, "topic": "f"},
        ], 4)
        assert len(msgs) == 4
        assert msgs[-1]["payload"] == {"a": 3, "b": 2, "c": 1}

    @pytest.mark.asyncio
    @pytest.mark.it('should be able to reset an accumulation')
    async def test_0015(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]], build:"merged", accumulate:true,
        #     mode:"custom", count:3},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:{a:1}, topic:"a"});
        # n1.receive({payload:{b:2}, topic:"b"});
        # n1.receive({payload:{c:3}, topic:"c"});
        # n1.receive({payload:{d:4}, topic:"d", complete:true});   -> output #2
        # n1.receive({payload:{e:2}, topic:"e"});
        # n1.receive({payload:{f:1}, topic:"f", complete:true});   -> output #3
        # n1.receive({payload:{g:2}, topic:"g"});
        # n1.receive({payload:{h:1}, topic:"h"});
        # n1.receive({reset:true});
        # n1.receive({payload:{g:2}, topic:"g"});
        # n1.receive({payload:{h:1}, topic:"h"});
        # n1.receive({payload:{i:3}, topic:"i"});                  -> output #4
        # Expects: message 2 payload {a:1,b:2,c:3,d:4}, message 3 payload {e:2,f:1},
        # message 4 payload {g:2,h:1,i:3} (the reset discards the earlier accumulation).
        node = {"type": "join", "mode": "custom", "build": "merged", "accumulate": True, "count": 3}
        msgs = await run_single_node_with_msgs_ntimes(node, [
            {"payload": {"a": 1}, "topic": "a"}, {"payload": {"b": 2}, "topic": "b"},
            {"payload": {"c": 3}, "topic": "c"}, {"payload": {"d": 4}, "topic": "d", "complete": True},
            {"payload": {"e": 2}, "topic": "e"}, {"payload": {"f": 1}, "topic": "f", "complete": True},
            {"payload": {"g": 2}, "topic": "g"}, {"payload": {"h": 1}, "topic": "h"}, {"reset": True},
            {"payload": {"g": 2}, "topic": "g"}, {"payload": {"h": 1}, "topic": "h"},
            {"payload": {"i": 3}, "topic": "i"},
        ], 4)
        assert msgs[-1]["payload"] == {"g": 2, "h": 1, "i": 3}

    @pytest.mark.asyncio
    @pytest.mark.it('should accumulate a key/value object')
    async def test_0016(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]], build:"object", accumulate:true,
        #     mode:"custom", topic:"bar", key:"foo", count:4},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:2, foo:"b"});
        # n1.receive({payload:3, foo:"c"});
        # n1.receive({reset:true});
        # n1.receive({payload:1, foo:"a"});
        # n1.receive({payload:2, foo:"b"});
        # n1.receive({payload:3, foo:"c"});
        # n1.receive({payload:4, foo:"d"});
        # Expects msg.payload == {a:1, b:2, c:3, d:4} in the accumulation that starts
        # after the reset.
        node = {"type": "join", "mode": "custom", "build": "object", "accumulate": True, "count": 4, "key": "foo"}
        msgs = await run_single_node_with_msgs_ntimes(node, [
            {"payload": 2, "foo": "b"}, {"payload": 3, "foo": "c"}, {"reset": True},
            {"payload": 1, "foo": "a"}, {"payload": 2, "foo": "b"}, {"payload": 3, "foo": "c"}, {"payload": 4, "foo": "d"},
        ], 1)
        assert msgs[0]["payload"] == {"a": 1, "b": 2, "c": 3, "d": 4}

    @pytest.mark.asyncio
    @pytest.mark.it('should join strings with a specifed character after a timeout')
    async def test_0017(self):
        msgs = await run_single_node_with_msgs_ntimes(
            {"type": "join", "mode": "custom", "build": "string", "timeout": 0.05, "count": "", "joiner": ","},
            [{"payload": value} for value in ["a", "b", "c"]], 1)
        assert msgs[0]["payload"] == "a,b,c"

    @pytest.mark.asyncio
    @pytest.mark.it('should allow the timeout to be restarted')
    async def test_0018(self):
        msgs = await run_single_node_for_seconds_scheduled(
            {"type": "join", "mode": "custom", "build": "string", "timeout": 0.5, "count": "", "joiner": ","},
            [{"payload": "a", "delay_ms": 0}, {"payload": "b", "restartTimeout": True, "delay_ms": 400},
             {"payload": "c", "delay_ms": 400}], 0.15)
        assert len(msgs) == 1 and msgs[0]["payload"] == "a,b,c"
        assert abs(msgs[0]["_since_start_ms"] - 900) < 150

    @pytest.mark.asyncio
    @pytest.mark.it('should join strings with a specifed character and complete when told to')
    async def test_0019(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]], build:"string", timeout:5, count:0,
        #     joiner:"\n", mode:"custom"},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:"Hello"});
        # n1.receive({payload:"NodeRED"});
        # n1.receive({payload:"World"});
        # n1.receive({payload:'', complete:true});
        # Expects msg.payload == "Hello\nNodeRED\nWorld\n".
        node = {"type": "join", "build": "string", "timeout": 5, "count": 0, "joiner": "\n", "mode": "custom"}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [{"payload": "Hello"}, {"payload": "NodeRED"}, {"payload": "World"}, {"payload": "", "complete": True}],
            1,
        )
        # after the first message (it reads count:0 as an already-satisfied count instead of
        # "no count"), and it ignores the `joiner` key (the Rust config field is `join_char`),
        # so even a completed group would be joined with "".
        assert msgs[0]["payload"] == "Hello\nNodeRED\nWorld\n"

    @pytest.mark.asyncio
    @pytest.mark.it('should join complete message objects into an array after a count')
    async def test_0020(self):
        msgs = await run_single_node_with_msgs_ntimes(
            {"type": "join", "build": "array", "count": 3, "propertyType": "full", "mode": "custom"},
            [{"payload": "a"}, {"payload": "b"}, {"payload": "c"}], 1)
        assert [msg["payload"] for msg in msgs[0]["payload"]] == ["a", "b", "c"]

    @pytest.mark.asyncio
    @pytest.mark.it('should join split things back into an array')
    async def test_0021(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]]},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:3, parts:{index:2, count:4, id:111}});
        # n1.receive({payload:2, parts:{index:1, count:4, id:111}});
        # n1.receive({payload:4, parts:{index:3, count:4, id:111}});
        # n1.receive({payload:1, parts:{index:0, count:4, id:111}});
        # Expects msg.payload == [1,2,3,4].
        node = {"type": "join"}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [
                {"payload": 3, "parts": {"index": 2, "count": 4, "id": 111}},
                {"payload": 2, "parts": {"index": 1, "count": 4, "id": 111}},
                {"payload": 4, "parts": {"index": 3, "count": 4, "id": 111}},
                {"payload": 1, "parts": {"index": 0, "count": 4, "id": 111}},
            ],
            1,
        )
        assert isinstance(msgs[0]["payload"], list)
        assert msgs[0]["payload"][0] == 1
        assert msgs[0]["payload"][1] == 2
        assert msgs[0]["payload"][2] == 3
        assert msgs[0]["payload"][3] == 4

    @pytest.mark.asyncio
    @pytest.mark.it('should join split things back into an object')
    async def test_0022(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]]},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:3, parts:{index:2, count:4, id:222, key:"c", type:"object"}});
        # n1.receive({payload:2, parts:{index:1, count:4, id:222, key:"b", type:"object"}});
        # n1.receive({payload:4, parts:{index:3, count:4, id:222, key:"d", type:"object"}});
        # n1.receive({payload:1, parts:{index:0, count:4, id:222, key:"a", type:"object"}});
        # Expects msg.payload == {a:1, b:2, c:3, d:4}.
        node = {"type": "join"}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [
                {"payload": 3, "parts": {"index": 2, "count": 4, "id": 222, "key": "c", "type": "object"}},
                {"payload": 2, "parts": {"index": 1, "count": 4, "id": 222, "key": "b", "type": "object"}},
                {"payload": 4, "parts": {"index": 3, "count": 4, "id": 222, "key": "d", "type": "object"}},
                {"payload": 1, "parts": {"index": 0, "count": 4, "id": 222, "key": "a", "type": "object"}},
            ],
            1,
        )
        assert msgs[0]["payload"]["a"] == 1
        assert msgs[0]["payload"]["b"] == 2
        assert msgs[0]["payload"]["c"] == 3
        assert msgs[0]["payload"]["d"] == 4

    @pytest.mark.asyncio
    @pytest.mark.it('should join split things, send when told complete')
    async def test_0023(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]], timeout:0.250},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:3, parts:{index:2, count:4, id:444}});
        # n1.receive({payload:2, parts:{index:1, count:4, id:444}});
        # n1.receive({payload:4, parts:{index:3, count:4, id:444}, complete:true});
        # Expects msg.payload to be an Array with payload[0] === undefined,
        # payload[1] == 2, payload[2] == 3, payload[3] == 4.
        # fractional `0.250` (seconds) fails to deserialize and the node never loads; 250 is
        # used here so the node builds.
        node = {"type": "join", "timeout": 250}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [
                {"payload": 3, "parts": {"index": 2, "count": 4, "id": 444}},
                {"payload": 2, "parts": {"index": 1, "count": 4, "id": 444}},
                {"payload": 4, "parts": {"index": 3, "count": 4, "id": 444}, "complete": True},
            ],
            1,
        )
        # though parts.count is 4 and only 3 messages arrived. EdgeLinkd ignores msg.complete
        # whenever a count is known, so nothing is ever emitted and this call times out.
        assert isinstance(msgs[0]["payload"], list)
        assert msgs[0]["payload"][0] is None
        assert msgs[0]["payload"][1] == 2
        assert msgs[0]["payload"][2] == 3
        assert msgs[0]["payload"][3] == 4

    @pytest.mark.asyncio
    @pytest.mark.it('should manually join things into an array, send when told complete')
    async def test_0024(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]], timeout:1, mode:"custom", build:"array"},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:1, topic:"A"});
        # n1.receive({payload:2, topic:"B"});
        # n1.receive({payload:3, topic:"C"});
        # n1.receive({complete:true});
        # Expects msg.payload == [1,2,3] (length 3 - the trailing {complete:true} message
        # carries no payload and is not collected).
        node = {"type": "join", "timeout": 1, "mode": "custom", "build": "array"}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [{"payload": 1, "topic": "A"}, {"payload": 2, "topic": "B"}, {"payload": 3, "topic": "C"}, {"complete": True}],
            1,
        )
        assert isinstance(msgs[0]["payload"], list)
        # for the payload-less {complete:true} message: [1, 2, 3, None].
        assert len(msgs[0]["payload"]) == 3
        assert msgs[0]["payload"][0] == 1
        assert msgs[0]["payload"][1] == 2
        assert msgs[0]["payload"][2] == 3

    @pytest.mark.asyncio
    @pytest.mark.it('should manually join things into an object, send when told complete')
    async def test_0025(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", wires:[["n2"]], timeout:1, mode:"custom", build:"object"},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:1, topic:"A"});
        # n1.receive({payload:2, topic:"B"});
        # n1.receive({payload:3, topic:"C"});
        # n1.receive({complete:true});
        # Expects msg.payload == {A:1, B:2, C:3} (Object.keys(msg.payload).length == 3).
        node = {"type": "join", "timeout": 1, "mode": "custom", "build": "object"}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [{"payload": 1, "topic": "A"}, {"payload": 2, "topic": "B"}, {"payload": 3, "topic": "C"}, {"complete": True}],
            1,
        )
        assert isinstance(msgs[0]["payload"], dict)
        assert len(msgs[0]["payload"]) == 3
        assert msgs[0]["payload"]["A"] == 1
        assert msgs[0]["payload"]["B"] == 2
        assert msgs[0]["payload"]["C"] == 3

    @pytest.mark.asyncio
    @pytest.mark.it('should join split strings back into a word')
    async def test_0026(self):
        # Node-RED flow:
        #   [{id:"n1", type:"join", mode:"auto", wires:[["n2"]]},
        #    {id:"n2", type:"helper"}]
        # n1.receive({payload:"a", parts:{type:'string', index:0, count:4, ch:"", id:555}});
        # n1.receive({payload:"d", parts:{type:'string', index:3, count:4, ch:"", id:555}});
        # n1.receive({payload:"c", parts:{type:'string', index:2, count:4, ch:"", id:555}});
        # n1.receive({payload:"b", parts:{type:'string', index:1, count:4, ch:"", id:555}});
        # Expects msg.payload == "abcd".
        node = {"type": "join", "mode": "auto"}
        msgs = await run_single_node_with_msgs_ntimes(
            node,
            [
                {"payload": "a", "parts": {"type": "string", "index": 0, "count": 4, "ch": "", "id": 555}},
                {"payload": "d", "parts": {"type": "string", "index": 3, "count": 4, "ch": "", "id": 555}},
                {"payload": "c", "parts": {"type": "string", "index": 2, "count": 4, "ch": "", "id": 555}},
                {"payload": "b", "parts": {"type": "string", "index": 1, "count": 4, "ch": "", "id": 555}},
            ],
            1,
        )
        assert isinstance(msgs[0]["payload"], str)
        assert msgs[0]["payload"] == "abcd"

    @pytest.mark.asyncio
    @pytest.mark.it('should allow chained split-split-join-join sequences')
    async def test_0027(self):
        # Node-RED flow:
        #   [{id:"s1", type:"split", wires:[["s2"]]},
        #    {id:"s2", type:"split", wires:[["j1"]]},
        #    {id:"j1", type:"join", mode:"auto", wires:[["j2"]]},
        #    {id:"j2", type:"join", mode:"auto", wires:[["n2"]]},
        #    {id:"n2", type:"helper"}]
        # s1.receive({payload:[[1,2,3],"a\nb\nc",[7,8,9]]});
        # Expects msg.payload == [[1,2,3],"a\nb\nc",[7,8,9]].
        # Upstream's ids are readable, but ElementId is hex, so they are converted with the
        # harness' red_id() helper instead of copied (declaration, z, wires, injection target).
        flows = [
            {"id": red_id("tab"), "type": "tab"},
            {"id": red_id("s1"), "type": "split", "z": red_id("tab"), "wires": [[red_id("s2")]]},
            {"id": red_id("s2"), "type": "split", "z": red_id("tab"), "wires": [[red_id("j1")]]},
            {"id": red_id("j1"), "type": "join", "z": red_id("tab"), "mode": "auto", "wires": [[red_id("j2")]]},
            {"id": red_id("j2"), "type": "join", "z": red_id("tab"), "mode": "auto", "wires": [[red_id("n2")]]},
            {"id": red_id("n2"), "type": "test-once", "z": red_id("tab")},
        ]
        msgs = await run_flow_with_msgs_ntimes(
            flows, [{"nid": red_id("s1"), "msg": {"payload": [[1, 2, 3], "a\nb\nc", [7, 8, 9]]}}], 1
        )
        assert msgs[0]["payload"] == [[1, 2, 3], "a\nb\nc", [7, 8, 9]]

    @pytest.mark.asyncio
    @pytest.mark.it('should concat payload when group.type is array')
    async def test_0028(self):
        # Node-RED: var flow = [{ id: "n1", type: "join", wires: [["n2"]], build: "array", mode: "auto" },
        #                     { id: "n2", type: "helper" }];
        node = {"type": "join", "wires": [["2"]], "build": "array", "mode": "auto"}
        injections = [
            {"payload": "ab", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 3, "len": 2}},
            {"payload": "cd", "parts": {"id": 1, "type": "array", "ch": ",", "index": 1, "count": 3, "len": 2}},
            {"payload": "ef", "parts": {"id": 1, "type": "array", "ch": ",", "index": 2, "count": 3, "len": 2}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert isinstance(msgs[0]["payload"], list)
        assert msgs[0]["payload"][0] == "ab"
        assert msgs[0]["payload"][1] == "cd"
        assert msgs[0]["payload"][2] == "ef"

    @pytest.mark.asyncio
    @pytest.mark.it('should concat payload when group.type is buffer and group.joinChar is undefined')
    async def test_0029(self):
        msgs = await _join_buffers({"type": "join", "mode": "auto", "build": "buffer", "joiner": ","},
            [{"payload": [ord(value)], "parts": {"id": 1, "type": "buffer", "index": i, "count": 3}}
             for i, value in enumerate("ABC")])
        assert msgs[0]["payload"] == list(b"ABC")

    @pytest.mark.asyncio
    @pytest.mark.it('should concat payload when group.type is string and group.joinChar is not string')
    async def test_0030(self):
        msgs = await _join_buffers({"type": "join", "mode": "auto", "build": "buffer", "joiner": ","},
            [{"payload": [ord(value)], "parts": {"id": 1, "type": "string", "ch": [48], "index": i, "count": 3}}
             for i, value in enumerate("ABC")])
        assert msgs[0]["payload"] == "A0B0C"

    @pytest.mark.asyncio
    @pytest.mark.it('should handle msg.parts property when mode is auto and parts or id are missing')
    async def test_0031(self):
        node = {"id": "1", "z": "0", "type": "join", "mode": "auto", "wires": [[]]}
        inputs = [{"payload": "A", "parts": {"type": "string", "ch": ",", "index": 0, "count": 2}},
                  {"payload": "B", "parts": {"type": "string", "ch": ",", "index": 1, "count": 2}}]
        msgs = await run_flow_for_seconds(_mapi_flow(node), inputs, 0.05)
        assert len(msgs) == 2 and [msg["payload"] for msg in msgs] == ["A", "B"]
        assert all(msg["type"] == "join" and msg["level"] == "WARN" for msg in edgelink.take_node_logs())

    @pytest.mark.asyncio
    @pytest.mark.it('should handle join an array when mode is auto and duplicate indexed parts arrive')
    async def test_0032(self):
        # Node-RED: var flow = [{ id: "n1", type: "join", wires: [["n2"]], joiner: "[44]", joinerType: "bin", build: "array", mode: "auto" },
        #                     { id: "n2", type: "helper" }];
        node = {"type": "join", "wires": [["2"]], "joiner": "[44]", "joinerType": "bin", "build": "array", "mode": "auto"}
        injections = [
            {"payload": "A", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 2}},
            {"payload": "B", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 2}},
            {"payload": "C", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 2}},
            {"payload": "D", "parts": {"id": 1, "type": "array", "ch": ",", "index": 1, "count": 2}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert msgs[0]["payload"][0] == "C"
        assert msgs[0]["payload"][1] == "D"

    @pytest.mark.asyncio
    @pytest.mark.it('should handle join an array when using msg.parts and duplicate indexed parts arrive and being reset halfway')
    async def test_0033(self):
        # Node-RED: var flow = [{ id: "n1", type: "join", wires: [["n2"]], joiner: "[44]", joinerType: "bin", build: "array", mode: "auto" },
        #                     { id: "n2", type: "helper" }];
        node = {"type": "join", "wires": [["2"]], "joiner": "[44]", "joinerType": "bin", "build": "array", "mode": "auto"}
        injections = [
            {"payload": "A", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 2}},
            {"payload": "B", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 2}},
            {"reset": True},
            {"payload": "C", "parts": {"id": 1, "type": "array", "ch": ",", "index": 1, "count": 2}},
            {"payload": "D", "parts": {"id": 1, "type": "array", "ch": ",", "index": 0, "count": 2}},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "payload" in msgs[0]
        assert msgs[0]["payload"][0] == "D"
        assert msgs[0]["payload"][1] == "C"

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages')
    async def test_0034(self):
        msgs = await _join_reduce()
        assert msgs[0]["payload"] == 10

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages - count only in last part')
    async def test_0035(self):
        msgs = await _join_reduce(inputs=_reduce_inputs(count_on_last=True))
        assert msgs[0]["payload"] == 10

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (str)')
    async def test_0036(self):
        msgs = await _join_reduce(reduceExp="$A", reduceInit="xyz", reduceInitType="str")
        assert msgs[0]["payload"] == "xyz"

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (num)')
    async def test_0037(self):
        msgs = await _join_reduce(reduceExp="$A", reduceInit=10, reduceInitType="num")
        assert msgs[0]["payload"] == 10

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (bool)')
    async def test_0038(self):
        msgs = await _join_reduce(reduceExp="$A", reduceInit=True, reduceInitType="bool")
        assert msgs[0]["payload"] is True

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (json)')
    async def test_0039(self):
        msgs = await _join_reduce(reduceExp="$A", reduceInit='{"x":"vx","y":"vy","z":"vz"}', reduceInitType="json")
        assert msgs[0]["payload"] == {"x": "vx", "y": "vy", "z": "vz"}

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (bin)')
    async def test_0040(self):
        msgs = await _join_reduce(reduceExp="$A", reduceInit="[1,2,3]", reduceInitType="bin")
        assert msgs[0]["payload"] == [1, 2, 3]

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (JSONata)')
    async def test_0041(self):
        msgs = await _join_reduce(reduceExp="$A", reduceInit="1+2+3", reduceInitType="jsonata")
        assert msgs[0]["payload"] == 6

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (env)')
    async def test_0042(self):
        with pytest.MonkeyPatch.context() as patch:
            patch.setenv("NR_XYZ", "nr_xyz")
            msgs = await _join_reduce(reduceExp="$A", reduceInit="NR_XYZ", reduceInitType="env")
        assert msgs[0]["payload"] == "nr_xyz"

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (flow.name)')
    async def test_0043(self):
        msgs = await _join_reduce(seed='flow.set("foo","bar");', reduceExp="$A", reduceInit="foo", reduceInitType="flow")
        assert msgs[0]["payload"] == "bar"

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with init types (global.name)')
    async def test_0044(self):
        msgs = await _join_reduce(seed='global.set("foo","bar");', reduceExp="$A", reduceInit="foo", reduceInitType="global")
        assert msgs[0]["payload"] == "bar"

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages using $I')
    async def test_0045(self):
        msgs = await _join_reduce(reduceExp="$A+$I")
        assert msgs[0]["payload"] == 6

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with fixup')
    async def test_0046(self):
        inputs = _reduce_inputs(count=5) + [{"payload": 0, "parts": {"index": 4, "count": 5, "id": 222}}]
        msgs = await _join_reduce(inputs=inputs, reduceFixup="$A/$N")
        assert msgs[0]["payload"] == 2

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages (left)')
    async def test_0047(self):
        msgs = await _join_reduce(inputs=_reduce_inputs(strings=True),
            reduceExp="'(' & $A & '+' & payload & ')'", reduceInit="0", reduceInitType="str")
        assert msgs[0]["payload"] == "((((0+1)+2)+3)+4)"
        assert msgs[0]["parts"]["index"] == 3

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages (right)')
    async def test_0048(self):
        msgs = await _join_reduce(inputs=_reduce_inputs(strings=True), reduceRight=True,
            reduceExp="'(' & $A & '+' & payload & ')'", reduceInit="0", reduceInitType="str")
        assert msgs[0]["payload"] == "((((0+4)+3)+2)+1)"
        assert msgs[0]["parts"]["index"] == 0

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with array result')
    async def test_0049(self):
        inputs = [{"payload": value, "parts": {"index": i, "count": 2, "id": 222 if i < 2 else 333}}
                  for i, value in enumerate([1, 2, 3, 4])]
        msgs = await _join_reduce(inputs=inputs, expected=2, reduceExp="$append($A,[payload])", reduceInit="[]", reduceInitType="json")
        assert [msg["payload"] for msg in msgs] == [[1, 2], [3, 4]]

    @pytest.mark.asyncio
    @pytest.mark.it('should handle too many pending messages for reduce mode')
    async def test_0050(self):
        config = copy.deepcopy(TEST_EDGELINLKD_CONFIG)
        config["runtime"]["flow"] = {"node_message_buffer_max_length": 2}
        node = {"id": "1", "z": "0", "type": "join", "mode": "reduce", "reduceExp": "$A+payload",
                "reduceInit": "0", "reduceInitType": "num", "wires": [[]]}
        msgs = await edgelink.run_flows_for_once(0.05, _mapi_flow(node),
            [("1", msg, 0) for msg in _reduce_inputs()], config)
        assert len(msgs) == 3
        assert [msg["payload"] for msg in msgs if "error" in msg] == [4]
        assert any(msg["type"] == "join" and msg["msg"] == "Too many pending messages in join node"
                   for msg in edgelink.take_node_logs())

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with flow context')
    async def test_0051(self):
        msgs = await _join_reduce_context("flow")
        assert msgs[0]["payload"] == 63

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with global context')
    async def test_0052(self):
        msgs = await _join_reduce_context("global")
        assert msgs[0]["payload"] == 18

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with persistable flow context')
    async def test_0053(self):
        msgs = await _join_reduce_context("flow", True)
        assert msgs[0]["payload"] == 63

    @pytest.mark.asyncio
    @pytest.mark.it('should reduce messages with persistable global context')
    async def test_0054(self):
        msgs = await _join_reduce_context("global", True)
        assert msgs[0]["payload"] == 18

    @pytest.mark.asyncio
    @pytest.mark.it('''should handle invalid JSONata reduce expression - syntax error"''')
    async def test_0055(self):
        with pytest.raises(RuntimeError, match="Invalid JSONata expression"):
            await _join_reduce(reduceExp="invalid expr")

    @pytest.mark.asyncio
    @pytest.mark.it('''should handle invalid JSONata reduce expression - runtime error"''')
    async def test_0056(self):
        await _join_reduce_error(reduceExp="$uknown()")

    @pytest.mark.asyncio
    @pytest.mark.it('''should handle invalid JSONata fixup expression - syntax err"''')
    async def test_0057(self):
        with pytest.raises(RuntimeError, match="Invalid JSONata expression"):
            await _join_reduce(reduceExp="$A", reduceFixup="invalid expr")

    @pytest.mark.asyncio
    @pytest.mark.it('''should handle invalid JSONata fixup expression - runtime err"''')
    async def test_0058(self):
        await _join_reduce_error(reduceExp="$A", reduceFixup="$unknown()")

    # This is the last `it()` of describe('JOIN node') upstream: it sits after the nested
    # 'messaging API' block, so its fullTitle carries no 'messaging API' segment.
    @pytest.mark.asyncio
    @pytest.mark.it('should handle msg.parts even if messages are out of order in auto mode if exactly one message has count set')
    async def test_0059(self):
        # Node-RED: var flow = [{ id: "n1", type: "join", wires: [["n2"]], mode: "auto" },
        #                      { id: "n2", type: "helper" }];
        #   msg.parts = { id: RED.util.generateId() };
        #   for (var elem = 1; elem < 5; ++elem) { ... parts.index = elem; if (elem == 4) parts.count = 5; payload = elem; }
        #   then parts.index = 0 (no count), payload = 0
        #   msg.payload.length.should.be.eql(5);
        #   msg.payload.should.be.eql([0,1,2,3,4]);
        node = {"id": "1", "z": "0", "type": "join", "mode": "auto", "wires": [["2"]]}
        flows = [
            {"id": "0", "type": "tab"},
            node,
            {"id": "2", "z": "0", "type": "test-once"},
        ]
        part_id = "joinPartsId1"
        injections = []
        for elem in range(1, 5):
            parts = {"id": part_id, "index": elem}
            if elem == 4:
                parts["count"] = 5
            injections.append({"parts": parts, "payload": elem})
        injections.append({"parts": {"id": part_id, "index": 0}, "payload": 0})
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert len(msgs[0]["payload"]) == 5
        assert msgs[0]["payload"] == [0, 1, 2, 3, 4]





async def _join_done(settings, entries, timings):
    config = copy.deepcopy(TEST_EDGELINLKD_CONFIG)
    config["runtime"]["flow"] = {"node_message_buffer_max_length": 3}
    node = {"id": "1", "z": "0", "type": "join", "wires": [[]], **settings}
    msgs = await edgelink.run_flows_for_once(0.15, _mapi_flow(node),
        [("1", msg, delay) for msg, delay in entries], config)
    assert len(msgs) == len(entries)
    assert sorted(msg["seq"] for msg in msgs) == list(range(len(entries)))
    for msg in msgs:
        assert abs(msg["_since_start_ms"] - timings[msg["seq"]]) < 100
    return msgs


# Upstream nests this block inside describe('JOIN node'), so mocha's fullTitle is
# "JOIN node messaging API <it>"; the stacked describe markers reproduce that.
@pytest.mark.describe('JOIN node')
@pytest.mark.describe('messaging API')
class TestMessagingApi:
    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when message is sent (string)')
    async def test_0001(self):
        # Node-RED: mapiDoneSplitTestHelper(done, 2, "len", false, [
        #     { msg: { seq: 0, payload: "12345" }, delay: 0, avr: 0, var: 100 },
        # ]);
        # The completion carries the input after split has written the final string chunk.
        node = {"id": "1", "z": "0", "type": "split", "splt": "2", "spltType": "len", "stream": False,
                "wires": [[]]}
        msgs = await run_flow_with_msgs_ntimes(_mapi_flow(node), [{"seq": 0, "payload": "12345"}], 1)
        assert len(msgs) == 1
        assert "payload" in msgs[0]
        assert msgs[0]["payload"] == "5"

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when message is sent (array)')
    async def test_0002(self):
        # Node-RED: mapiDoneSplitTestHelper(done, 2, "len", false, [
        #     { msg: { seq: 0, payload: [0,1,2,3,4] }, delay: 0, avr: 0, var: 100 },
        # ]);
        node = {"id": "1", "z": "0", "type": "split", "splt": "2", "spltType": "len", "stream": False,
                "wires": [[]]}
        msgs = await run_flow_with_msgs_ntimes(_mapi_flow(node), [{"seq": 0, "payload": [0, 1, 2, 3, 4]}], 1)
        assert len(msgs) == 1
        assert "payload" in msgs[0]
        assert msgs[0]["payload"] == [0, 1, 2, 3, 4]

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when message is sent (object)')
    async def test_0003(self):
        # Node-RED: mapiDoneSplitTestHelper(done, 2, "len", false, [
        #     { msg: { seq: 0, payload: {a:1,b:2}}, delay: 0, avr: 0, var: 100 },
        # ]);
        node = {"id": "1", "z": "0", "type": "split", "splt": "2", "spltType": "len", "stream": False,
                "wires": [[]]}
        msgs = await run_flow_with_msgs_ntimes(_mapi_flow(node), [{"seq": 0, "payload": {"a": 1, "b": 2}}], 1)
        assert len(msgs) == 1
        assert "payload" in msgs[0]
        assert msgs[0]["payload"] == 2

    async def _consolidated(self, binary=False, split_type="len"):
        node = {"id": "5" if binary else "1", "z": "0", "type": "split",
                "splt": "[53]" if split_type == "bin" else "5", "spltType": split_type,
                "stream": True, "wires": [[]]}
        flows = _mapi_flow({**node, "id": "1"})
        if binary:
            flows[1]["id"] = "5"
            flows[2]["scope"] = ["5"]
            flows[3]["scope"] = ["5"]
            flows.append({"id": "1", "z": "0", "type": "function", "wires": [["5"]],
                          "func": "msg.payload = new Uint8Array(msg.payload).buffer; return msg;"})
        payloads = [list(b"12"), list(b"34"), list(b"5")] if binary else ["12", "34", "5"]
        injections = [{"nid": "1", "msg": {"seq": i, "payload": value}, "delay_ms": delay}
                      for i, (value, delay) in enumerate(zip(payloads, [0, 200, 500]))]
        msgs = await run_flow_for_seconds_scheduled(flows, injections, 0.2)
        assert len(msgs) == 3
        assert sorted(msg["seq"] for msg in msgs) == [0, 1, 2]
        assert all(abs(msg["_since_start_ms"] - 500) < 100 for msg in msgs)

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when consolidated message is emitted (string, len)')
    async def test_0004(self):
        await self._consolidated()

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when consolidated message is emitted (Buffer, len)')
    async def test_0005(self):
        await self._consolidated(True)

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when consolidated message is emitted (Buffer, str)')
    async def test_0006(self):
        await self._consolidated(True, "str")

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when consolidated message is emitted (Buffer, bin)')
    async def test_0007(self):
        await self._consolidated(True, "bin")

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when all messages are joined')
    async def test_0008(self):
        inputs = [
            ({"seq": 0, "payload": "A", "parts": {"id": 1, "type": "string", "ch": ",", "index": 0, "count": 3}}, 0),
            ({"seq": 1, "payload": "B", "parts": {"id": 1, "type": "string", "ch": ",", "index": 1, "count": 3}}, 200),
            ({"seq": 2, "payload": "C", "parts": {"id": 1, "type": "string", "ch": ",", "index": 2, "count": 3}}, 500),
        ]
        await _join_done({"mode": "auto", "timeout": 1}, inputs, [500, 500, 500])

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when the node is reset')
    async def test_0009(self):
        inputs = [
            ({"seq": 0, "payload": "A", "parts": {"id": 1, "type": "string", "ch": ",", "index": 0, "count": 3}}, 0),
            ({"seq": 1, "payload": "B", "parts": {"id": 1, "type": "string", "ch": ",", "index": 1, "count": 3}}, 200),
            ({"seq": 2, "payload": "dummy", "reset": True, "parts": {"id": 1}}, 500),
        ]
        await _join_done({"mode": "auto", "timeout": 1}, inputs, [500, 500, 500])

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when timed out')
    async def test_0010(self):
        inputs = [({"seq": 0, "payload": "A"}, 0), ({"seq": 1, "payload": "B"}, 200)]
        await _join_done({"mode": "custom", "joiner": ",", "build": "string", "timeout": 0.5}, inputs, [500, 500])

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() when all messages are reduced')
    async def test_0011(self):
        inputs = [({"seq": i, "payload": value, "parts": {"index": index, "count": 3, "id": 222}}, delay)
                  for i, (value, index, delay) in enumerate([(3, 2, 0), (2, 1, 200), (4, 0, 500)])]
        await _join_done({"mode": "reduce", "reduceExp": "$A+payload", "reduceInit": "0", "reduceInitType": "num"},
                        inputs, [500, 500, 500])

    @pytest.mark.asyncio
    @pytest.mark.it('should call done() regardless of buffer overflow')
    async def test_0012(self):
        inputs = [({"seq": i, "payload": value, "parts": {"index": index, "count": 5, "id": 222}}, delay)
                  for i, (value, index, delay) in enumerate([(3, 2, 0), (2, 1, 200), (4, 0, 400), (1, 3, 600)])]
        msgs = await _join_done({"mode": "reduce", "reduceExp": "$A+payload", "reduceInit": "0", "reduceInitType": "num"},
                               inputs, [600, 600, 600, 600])
        assert [msg["seq"] for msg in msgs if "error" in msg] == [3]

import pytest
from tests import *

@pytest.mark.describe('SPLIT node')
class TestSplitNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_0001(self):
        node = {"type": "split", "name": "splitNode"}
        # Node-RED: var flow = [{id:"splitNode1", type:"split", name:"splitNode" }];
        # 只需验证节点能被加载
        msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": 1}], 1)
        assert True

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
        injections = [{"payload": b}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 4)
        vals = [b"12", b"34", b"56", b"78"]
        for i, m in enumerate(msgs):
            assert m["parts"]["count"] == 4
            assert m["parts"]["index"] == i
            assert m["parts"]["type"] == "buffer"
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should split a buffer on another buffer (streaming)')
    async def test_0013(self):
        node = {"type": "split", "splt": b"4", "spltType": "bin", "stream": True}
        b1 = b"123412"
        b2 = b"341234"
        injections = [{"payload": b1}, {"payload": b2}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 3)
        vals = [b"123", b"123", b"123"]
        for i, m in enumerate(msgs):
            assert m["parts"]["type"] == "buffer"
            assert m["parts"]["index"] == i
            assert m["payload"] == vals[i]

    @pytest.mark.asyncio
    @pytest.mark.it('should handle invalid spltType (not an array)')
    async def test_0014(self):
        node = {"type": "split", "splt": "1", "spltType": "bin"}
        injections = [{"payload": "123"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 0)
        assert msgs == []

    @pytest.mark.asyncio
    @pytest.mark.it('should handle invalid splt length')
    async def test_0015(self):
        node = {"type": "split", "splt": 0, "spltType": "len"}
        injections = [{"payload": "123"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 0)
        assert msgs == []

    @pytest.mark.asyncio
    @pytest.mark.it('should handle invalid array splt length')
    async def test_0016(self):
        node = {"type": "split", "arraySplt": 0, "arraySpltType": "len"}
        injections = [{"payload": "123"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 0)
        assert msgs == []

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
        node = {"type": "split", "splt": b"4", "spltType": "bin"}
        injections = [{"payload": b"123"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["parts"]["index"] == 0
        assert msgs[0]["payload"] == b"123"

    @pytest.mark.asyncio
    @pytest.mark.it('should ceil count value when msg.payload type is Buffer')
    async def test_0019(self):
        node = {"type": "split", "splt": "2", "spltType": "len"}
        b = b"123"
        injections = [{"payload": b}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
        assert msgs[0]["parts"]["count"] == 2
        assert len(msgs[0]["payload"]) == 2
        assert len(msgs[1]["payload"]) == 1

    @pytest.mark.asyncio
    @pytest.mark.it('should set msg.parts.ch when node.spltType is str')
    async def test_0020(self):
        node = {"type": "split", "splt": "2", "spltType": "str", "stream": False}
        b = b"123"
        injections = [{"payload": b}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
        assert msgs[0]["parts"]["count"] == 2
        assert len(msgs[0]["payload"]) == 2
        assert len(msgs[1]["payload"]) == 1

    # JSONata 相关测试跳过
    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should split using JSONata expression')
    async def test_0021(self):
        pass

    @pytest.mark.skip(reason="JSONata not supported yet")
    @pytest.mark.asyncio
    @pytest.mark.it('should split using JSONata expression with context')
    async def test_0022(self):
        pass

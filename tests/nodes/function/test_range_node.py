import json
import asyncio
import os

from tests import *

async def _generic_range_test(action, minin, maxin, minout, maxout, round, a_payload, expected_result):
    node = {"type": "range", "minin": minin, "maxin": maxin, "minout": minout,
            "maxout": maxout, "action": action, "round": round}
    msgs = await run_with_single_node_ntimes(
        'num',
        isinstance(a_payload, str) and a_payload or json.dumps(a_payload),
        node, 1, once=True, topic='t1')
    assert msgs[0]['payload'] == expected_result


@pytest.mark.describe('range Node')
class TestRangeNode:

    @pytest.mark.asyncio
    @pytest.mark.it('''ranges numbers up tenfold''')
    async def test_0001(self):
        await _generic_range_test("scale", 0, 100, 0, 1000, False, 50, 500)

    @pytest.mark.asyncio
    @pytest.mark.it('''ranges numbers down such as centimetres to metres''')
    async def test_0002(self):
        await _generic_range_test("scale", 0, 100, 0, 1, False, 55, 0.55)

    @pytest.mark.asyncio
    @pytest.mark.it('''wraps numbers down say for degree/rotation reading 1/2''')
    async def test_0003(self):
        # 1/2 around wrap => "one and a half turns"
        await _generic_range_test("roll", 0, 10, 0, 360, True, 15, 180)

    @pytest.mark.asyncio
    @pytest.mark.it('''wraps numbers around say for degree/rotation reading 1/3''')
    async def test_0004(self):
        # 1/3 around wrap => "one and a third turns"
        await _generic_range_test("roll", 0, 10, 0, 360, True, 13.3333, 120)

    @pytest.mark.asyncio
    @pytest.mark.it('''wraps numbers around say for degree/rotation reading 1/4''')
    async def test_0005(self):
        # 1/4 around wrap => "one and a quarter turns"
        await _generic_range_test("roll", 0, 10, 0, 360, True, 12.5, 90)

    @pytest.mark.asyncio
    @pytest.mark.it('''wraps numbers down say for degree/rotation reading 1/4''')
    async def test_0006(self):
        # 1/4 backwards wrap => "one and a quarter turns backwards"
        await _generic_range_test("roll", 0, 10, 0, 360, True, -12.5, 270)

    @pytest.mark.asyncio
    @pytest.mark.it('''wraps numbers around say for degree/rotation reading 0''')
    async def test_0007(self):
        await _generic_range_test("roll", 0, 10, 0, 360, True, -10, 0)

    @pytest.mark.asyncio
    @pytest.mark.it('''clamps numbers within a range - over max''')
    async def test_0008(self):
        '''clamps numbers within a range - over max'''
        await _generic_range_test("clamp", 0, 10, 0, 1000, False, 111, 1000)

    @pytest.mark.asyncio
    @pytest.mark.it('''clamps numbers within a range - below min''')
    async def test_0009(self):
        await _generic_range_test("clamp", 0, 10, 0, 1000, False, -1, 0)

    @pytest.mark.asyncio
    @pytest.mark.it('''drops msg if in drop mode and input outside range''')
    async def test_0010(self):
        node = {
            "type": "range", "minin": 2, "maxin": 8, "minout": 20, "maxout": 80,
            "action": "drop", "round": True
        }
        injections = [
            {'payload': "1.0"},
            {'payload': "9.0"},
            {'payload': "5.0"},
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]['payload'] == 50

    @pytest.mark.asyncio
    @pytest.mark.it('''just passes on msg if payload not present''')
    async def test_0011(self):
        node = {
            "type": "range", "minin": 0, "maxin": 100, "minout": 0, "maxout": 100,
            "action": "scale", "round": True
        }
        injections = [{}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert 'payload' not in msgs[0]

    @pytest.mark.asyncio
    @pytest.mark.it('reports if input is not a number')
    async def test_it_reports_if_input_is_not_a_number(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "z": "100", "type": "range", 
             "minin": 0, "maxin": 0, "minout": 0, "maxout": 0, "action": "scale", "round": True, },
            {"id": "2", "z": "100", "type": "catch", "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {"payload": "NOT A NUMBER"}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert 'error' in msgs[0]

import pytest
import os

from tests import *

# 0001 should do something with the catch node

@pytest.mark.describe('function node')
class TestFunctionNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should send returned message using send()')
    async def test_it_should_send_returned_message_using_send_0(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "node.send(msg);"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.asyncio
    @pytest.mark.it('should send returned message')
    async def test_it_should_send_returned_message(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "return msg;"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]['topic'] == 'bar'
        assert msgs[0]['payload'] == 'foo'

    @pytest.mark.asyncio
    @pytest.mark.it('should send returned message using send()')
    async def test_it_should_send_returned_message_using_send_1(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "node.send(msg);"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.asyncio
    @pytest.mark.it('should allow accessing node.id and node.name and node.outputCount')
    async def test_it_should_allow_accessing_node_id_and_node_name_and_node_output_count(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "name": "test-function", "wires": [["2"]], "outputs": 2,
                "func": "return [{ topic: node.name, payload:node.id, outputCount: node.outputCount }];",
             },
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': ''}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "0000000000000001"
        assert msgs[0]["topic"] == "test-function"
        assert msgs[0]["outputCount"] == 2

    async def _test_send_cloning(self, args):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"], ["2"]],
                "func": f"node.send({args}); msg.payload = 'changed';"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should clone single message sent using send()')
    async def test_it_should_clone_single_message_sent_using_send_2(self):
        await self._test_send_cloning("msg")

    # Not supported, yet

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should not clone single message sent using send(,false)')
    async def test_it_should_not_clone_single_message_sent_using_send_false(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "node.send(msg,false); msg.payload = 'changed';"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "changed"

    @pytest.mark.asyncio
    @pytest.mark.it('should clone first message sent using send() - array 1')
    async def test_it_should_clone_first_message_sent_using_send_array_1(self):
        await self._test_send_cloning("[msg]")

    @pytest.mark.asyncio
    @pytest.mark.it('should clone first message sent using send() - array 2')
    async def test_it_should_clone_first_message_sent_using_send_array_2(self):
        await self._test_send_cloning("[[msg],[null]]")

    @pytest.mark.asyncio
    @pytest.mark.it('should clone first message sent using send() - array 3')
    async def test_it_should_clone_first_message_sent_using_send_array_3(self):
        await self._test_send_cloning("[null,msg]")

    @pytest.mark.asyncio
    @pytest.mark.it('should clone first message sent using send() - array 3')
    async def test_it_should_clone_first_message_sent_using_send_array_3_1(self):
        await self._test_send_cloning("[null,[msg]]")

    @pytest.mark.asyncio
    @pytest.mark.it('should pass through _topic')
    async def test_it_should_pass_through__topic(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "return msg;"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar', '_topic': 'barz'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["_topic"] == "barz"

    @pytest.mark.asyncio
    @pytest.mark.it('should send to multiple outputs')
    async def test_it_should_send_to_multiple_outputs(self):
        node = {
            "type": "function",
            "func": "var msg2 = RED.util.cloneMessage(msg); msg2.payload='p2'; return [msg, msg2];",
            "wires": [["3"], ["3"]]
        }
        msgs = await run_with_single_node_ntimes('str', 'foo', node, 2, once=True, topic='bar')
        assert msgs[0]['topic'] == 'bar'
        assert msgs[0]['topic'] == msgs[1]['topic']
        assert msgs[0]['payload'] != msgs[1]['payload']
        assert sorted([msgs[0]['payload'], msgs[1]['payload']]) == ['foo', 'p2']

    @pytest.mark.asyncio
    @pytest.mark.it('should send to multiple messages')
    async def test_it_should_send_to_multiple_message(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "return [[{payload: 1},{payload: 2}]];"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            # TODO FIXME, MSGID SHOULD ALLOWED i64/u64
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar', '_msgid': '1234'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 2)
        assert msgs[0]['_msgid'] == msgs[1]['_msgid'] == 0x1234
        assert msgs[0]['payload'] == 1
        assert msgs[1]['payload'] == 2

    # TODO the testing frame has no way to handle time-out for now

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should allow input to be discarded by returning null')
    async def test_it_should_allow_input_to_be_discarded_by_returning_null(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "return null;"},
            {"id": "2", "z": "100", "type": "test-once"}
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 0)

    @pytest.mark.asyncio
    @pytest.mark.it('should handle null amongst valid messages')
    async def test_it_should_handle_null_amongst_valid_messages(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "return [[msg,null,msg],null];"},
            {"id": "2", "z": "100", "type": "test-once"},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 2)
        assert len(msgs) == 2

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in global context')
    async def test_it_should_get_keys_in_global_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "change", "z": "100", "rules": [
                {"t": "set", "p": "count", "pt": "global", "to": "0", "tot": "num"}
            ], "reg": False, "name": "changeNode", "wires": [["2"]]},
            {"id": "2", "type": "function", "z": "100", "wires": [
                ["3"]], "func": "msg.payload=global.keys();return msg;"},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ['count']

    async def _test_non_object_message(self, function_text):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "2", "type": "function", "z": "100", "wires": [
                ["3"]], "func": function_text},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        # assert msgs[0]["level"] == "ERROR"
        # assert msgs[0]["id"] == '0000000000000001'
        # assert msgs[0]["type"] == 'function'
        # assert msgs[0]["msg"] == 'function.error.non-message-returned'

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should drop and log non-object message types - string')
    async def test_it_should_drop_and_log_non_object_message_types_string(self):
        await self._test_non_object_message('return "foo"')

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should drop and log non-object message types - buffer')
    async def test_it_should_drop_and_log_non_object_message_types_buffer(self):
        await self._test_non_object_message('return Buffer.from("hello")')

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should drop and log non-object message types - array')
    async def test_it_should_drop_and_log_non_object_message_types_array(self):
        await self._test_non_object_message('return [[[1,2,3]]]')

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should drop and log non-object message types - boolean')
    async def test_it_should_drop_and_log_non_object_message_types_boolean(self):
        await self._test_non_object_message('return true')

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should drop and log non-object message types - number')
    async def test_it_should_drop_and_log_non_object_message_types_number(self):
        await self._test_non_object_message('return 123')

    @pytest.mark.asyncio
    @pytest.mark.it('should set node context')
    async def test_it_should_set_node_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "context.set('count','0'); msg.count=context.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set persistable node context (w/o callback)')
    async def test_it_should_set_persistable_node_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "context.set('count','0','memory1'); msg.count=context.get('count', 'memory1'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set two persistable node context (w/o callback)')
    async def test_it_should_set_two_persistable_node_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": r'''
                context.set('count','0','memory1');
                context.set('count','1','memory2');
                msg.count0 = context.get('count','memory1');
                msg.count1 = context.get('count','memory2');
                return msg;'''},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count0"] == "0"
        assert msgs[0]["count1"] == "1"

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should set two persistable node context (single call, w/o callback)')
    async def test_it_should_set_two_persistable_node_context_single_call_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"""
                context.set(['count1', 'count2'], ['0', '1'], 'memory1', err => {
                    msg.count0 = context.get('count1', 'memory1');
                    msg.count1 = context.get('count2', 'memory1');
                }); 
                return msg;
             """},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count0"] == "0"
        assert msgs[0]["count1"] == "1"

    @pytest.mark.asyncio
    @pytest.mark.it('should set persistable node context (w callback)')
    async def test_it_should_set_persistable_node_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0','memory1', function (err) { msg.count=context.get('count', 'memory1'); node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set two persistable node context (w callback)')
    async def test_it_should_set_two_persistable_node_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"""
                context.set('count','0','memory1', function (err) { 
                    msg.count0 = context.get('count','memory1');
                    context.set('count', '1', 'memory2', function (err) { 
                        msg.count1 = context.get('count','memory2');
                        node.send(msg); 
                    }); 
                });
            """},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count0"] == "0"
        assert msgs[0]["count1"] == "1"

    @pytest.mark.asyncio
    @pytest.mark.it('should set default persistable node context')
    async def test_it_should_set_default_persistable_node_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0'); msg.count=context.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get node context')
    async def test_it_should_get_node_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0'); msg.payload=context.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable node context (w/o callback)')
    async def test_it_should_get_persistable_node_context__w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0','memory1'); msg.payload=context.get('count','memory1');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable node context (w/ callback)')
    async def test_it_should_get_persistable_node_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0','memory1'); context.get('count','memory1',function (err, val) { msg.payload=val; node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in node context')
    async def test_it_should_get_keys_in_node_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0'); msg.payload=context.keys();return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in persistable node context (w/o callback)')
    async def test_it_should_get_keys_in_persistable_node_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0','memory1'); msg.payload=context.keys('memory1');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in persistable node context (w/ callback)')
    async def test_it_should_get_keys_in_persistable_node_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"context.set('count','0','memory1'); context.keys('memory1', function(err, keys) { msg.payload=keys; node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in default persistable node context')
    async def test_it_should_get_keys_in_default_persistable_node_context(self):
        # n1.context().set("count","0","memory1");
        # n1.context().set("number","1","memory2");
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":  # FIXME TODO
             r"context.set('count','0'); context.set('number','1','memory2'); msg.payload=context.keys();return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]
    #

    @pytest.mark.asyncio
    @pytest.mark.it('should set flow context')
    async def test_it_should_set_flow_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0'); msg.count=flow.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set persistable flow context (w/o callback)')
    async def test_it_should_set_persistable_flow_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "flow.set('count','0','memory1'); msg.count=flow.get('count', 'memory1'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set two persistable flow context (w/o callback)')
    async def test_it_should_set_two_persistable_flow_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": r'''
                flow.set('count','0','memory1');
                flow.set('count','1','memory2');
                msg.count0 = flow.get('count','memory1');
                msg.count1 = flow.get('count','memory2');
                return msg;'''},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count0"] == "0"
        assert msgs[0]["count1"] == "1"

    @pytest.mark.asyncio
    @pytest.mark.it('should set persistable flow context (w/ callback)')
    async def test_it_should_set_persistable_flow_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0','memory1', function (err) { msg.count=flow.get('count', 'memory1'); node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set two persistable flow context (w/ callback)')
    async def test_it_should_set_two_persistable_flow_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"""
                flow.set('count','0','memory1', function (err) { 
                    msg.count0 = flow.get('count','memory1');
                    flow.set('count', '1', 'memory2', function (err) { 
                        msg.count1 = flow.get('count','memory2');
                        node.send(msg); 
                    }); 
                });
            """},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count0"] == "0"
        assert msgs[0]["count1"] == "1"

    @pytest.mark.asyncio
    @pytest.mark.it('should get flow context')
    async def test_it_should_get_flow_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0'); msg.payload=flow.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable flow context (w/o callback)')
    async def test_it_should_get_persistable_flow_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0','memory1'); msg.payload=flow.get('count','memory1');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable flow context (w/ callback)')
    async def test_it_should_get_persistable_flow_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0','memory1'); flow.get('count','memory1',function (err, val) { msg.payload=val; node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get flow context')
    async def test_it_should_get_flow_context_2(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0'); msg.payload=context.flow.get('count');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in flow context')
    async def test_it_should_get_keys_in_flow_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0'); msg.payload=flow.keys();return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in persistable flow context (w/o callback)')
    async def test_it_should_get_keys_in_persistable_flow_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0','memory1'); msg.payload=flow.keys('memory1');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]

    @pytest.mark.asyncio
    @pytest.mark.it('should get keys in persistable flow context (w/ callback)')
    async def test_it_should_get_keys_in_persistable_flow_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"flow.set('count','0','memory1'); flow.keys('memory1', function(err, keys) { msg.payload=keys; node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == ["count"]

    @pytest.mark.asyncio
    @pytest.mark.it('should set global context')
    async def test_it_should_set_global_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0'); msg.count=global.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set persistable global context (w/o callback)')
    async def test_it_should_set_persistable_global_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [
                ["2"]], "func": "global.set('count','0','memory1'); msg.count=global.get('count', 'memory1'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should set persistable global context (w/ callback)')
    async def test_it_should_set_persistable_global_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0','memory1', function (err) { msg.count=global.get('count', 'memory1'); node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"
        assert msgs[0]["count"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get global context')
    async def test_it_should_get_global_context(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0'); msg.payload=global.get('count'); return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable global context (w/o callback)')
    async def test_it_should_get_persistable_global_context_w_o_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0', 'memory1'); msg.payload=global.get('count', 'memory1');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable global context (w/ callback)')
    async def test_it_should_get_persistable_global_context_w_callback(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0', 'memory1'); global.get('count', 'memory1', function (err, val) { msg.payload=val; node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get global context')
    async def test_it_should_get_global_context_2(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0'); msg.payload=context.global.get('count');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable global context (w/o callback)')
    async def test_it_should_get_persistable_global_context_w_o_callback_2(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0', 'memory1'); msg.payload=context.global.get('count','memory1');return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    @pytest.mark.asyncio
    @pytest.mark.it('should get persistable global context (w/ callback)')
    async def test_it_should_get_persistable_global_context_w_callback_2(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func":
             r"global.set('count','0', 'memory1'); context.global.get('count','memory1', function (err, val) { msg.payload = val; node.send(msg); });"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "0"

    # Not finished, yet
    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should handle setTimeout()')
    async def test_it_should_handle_settimeout(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]],
             "func": r"setTimeout(() => node.send(msg), 100);"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should handle setInterval()')
    async def test_it_should_handle_setinterval(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]],
             "func": r"setInterval(() => node.send(msg), 100);"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should handle clearInterval()')
    async def test_it_should_handle_clearinterval(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]],
             "func": r"var id=setInterval(null,100);setTimeout(()=>{clearInterval(id);node.send(msg);},500);"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.asyncio
    @pytest.mark.it('should allow accessing node.id')
    async def test_id_should_allow_accessing_node_id(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]], "func": "msg.payload = node.id; return msg;"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]['payload'] == '0000000000000001'

    @pytest.mark.asyncio
    @pytest.mark.it('should allow accessing node.name')
    async def test_id_should_allow_accessing_node_name(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]],
                "func": "msg.payload = node.name; return msg;", "name": "name of node"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo', 'topic': 'bar'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]['payload'] == 'name of node'

    class TestEnvVar:
        def setup_method(self, method):
            os.environ["_TEST_FOO_"] = "hello"

        def teardown_method(self, method):
            del os.environ["_TEST_FOO_"]

        @pytest.mark.asyncio
        @pytest.mark.it('should allow accessing env vars')
        async def test_it_should_allow_accessing_env_vars(self):
            node = {
                "type": "function",
                "func": "msg.payload = env.get('_TEST_FOO_'); return msg;",
                "wires": [["3"]]
            }
            msgs = await run_with_single_node_ntimes(payload_type='str', payload='foo', node_json=node, nexpected=1, once=True, topic='bar')
            assert msgs[0]['topic'] == 'bar'
            assert msgs[0]['payload'] == 'hello'

    @pytest.mark.asyncio
    @pytest.mark.it('should execute initialization')
    async def test_it_should_execute_initialization(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]],
                "func": "msg.payload = global.get('X'); return msg;", "initialize": "global.set('X','bar');"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]['payload'] == 'bar'

    @pytest.mark.asyncio
    @pytest.mark.it('should wait completion of initialization')
    async def test_it_should_wait_completion_of_initializationn(self):
        flows = [
            {"id": "100", "type": "tab"},  # flow 1
            {"id": "1", "type": "function", "z": "100", "wires": [["2"]],
             "func": "msg.payload = global.get('X'); return msg;",
             "initialize": "global.set('X', '-'); return new Promise((resolve, reject) => setTimeout(() => { global.set('X','bar'); resolve(); }, 500));"},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        injections = [
            {"nid": "1", "msg": {'payload': 'foo'}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]['payload'] == 'bar'

    @pytest.mark.describe('finalize function')
    class TestFinalizeFunction:
        pass

    @pytest.mark.describe('init function')
    class TestInitFunction:
        pass

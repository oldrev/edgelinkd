"""Ported specs for the `debug` node.

Upstream: `3rd-party/node-red/test/nodes/core/common/21-debug_spec.js` (v4.0.9). The upstream spec
opens the editor's WebSocket and reads the records the node publishes for the sidebar
(`RED.comms.publish("debug", ...)`); `take_debug_messages()` is the bridge's stand-in for that
socket, and `take_node_logs()` stands in for the `helper.log()` events the console output produces.

A debug node has no output, so a run never produces the message the harness waits for: every test
injects into the node and then expects the `RuntimeError` ("Timed out") the harness raises after its
timeout. Both channels are drained *before* that error is propagated, which is why the published
records are still there afterwards.

One difference to keep in mind while reading the assertions: Node-RED's spec loads its node without
a tab, which its runtime files under the implicit `global` flow, so upstream asserts
`path: "global"`. A flow here always lives in a tab, so the records carry the tab's id instead; that
is a property of the two test harnesses, not of the node.
"""
import json

import pytest
from tests import *


_TAB = red_id("tab")

#: Marker for a record field the test compares itself, because the runtime's text differs from
#: Node-RED's in a way the spec does not pin down (the key order of a sorted object map).
_SEPARATE = object()


def _debug_node(**overrides) -> dict:
    """A debug node carrying the flow properties the upstream spec sets for it."""
    node = {"id": red_id("n1"), "type": "debug", "z": _TAB, "wires": []}
    node.update(overrides)
    return node


async def _run(node_json: dict, message: dict, timeout: float = 0.2) -> list[dict]:
    """Inject one message into the node and return the records it published to the sidebar."""
    flows = [{"id": _TAB, "type": "tab"}, node_json]
    injections = [{"nid": node_json["id"], "msg": message}]
    with pytest.raises(RuntimeError):
        await run_flow_with_msgs_ntimes(flows, injections, 1, timeout=timeout)
    return take_debug_messages()


async def _run_function_into_debug(func: str, **debug_overrides) -> list[dict]:
    """The debug node behind a `function` node that builds the payload.

    Node-RED's specs inject a `Buffer` straight into the debug node. This bridge injects JSON, which
    has no buffer type, so the buffered payloads are built in a function node instead: the JS bridge
    turns an `ArrayBuffer` into the runtime's `Bytes`, which is the value the upstream spec gets from
    `Buffer.alloc()`/`Buffer.from()`.
    """
    flows = [
        {"id": _TAB, "type": "tab"},
        {"id": red_id("fn"), "type": "function", "z": _TAB, "outputs": 1, "func": func,
         "wires": [[red_id("n1")]]},
        _debug_node(**debug_overrides),
    ]
    injections = [{"nid": red_id("fn"), "msg": {"payload": None}}]
    with pytest.raises(RuntimeError):
        await run_flow_with_msgs_ntimes(flows, injections, 1, timeout=0.2)
    return take_debug_messages()


def _published(entry: dict) -> dict:
    """The fields of a record that the upstream spec compares, `None` ones left out.

    The sidebar record is `{id, name, path, topic, property, msg, format}` and Node-RED leaves a key
    out entirely when it is `undefined`; the runtime's record has fixed fields, so the `None` values
    (an unnamed node, no `topic`, `complete: "true"`) are dropped to compare like for like. The
    runtime's `_msgid` and publish time are not part of the upstream assertions.
    """
    fields = ("id", "name", "path", "topic", "property", "msg", "format")
    return {key: entry[key] for key in fields if entry.get(key) is not None}


async def _published_once(node_json: dict, message: dict, **expected) -> dict:
    """Run one message through the node, expect exactly one record, and check its fields.

    Like the upstream `should.eql`, the record must carry exactly these fields - so a field that
    upstream leaves out (an unset `property`) has to be left out here too. A field whose value the
    test itself compares (the JSON text of an object, whose key order is not upstream's) is passed
    as `_SEPARATE`.
    """
    entries = await _run(node_json, message)
    assert len(entries) == 1, entries
    # `topic` is only part of the expected fields when the upstream spec's message carries one.
    expected.setdefault("id", red_id("n1"))
    expected.setdefault("path", _TAB)
    published = _published(entries[0])
    for field, value in expected.items():
        if value is not _SEPARATE:
            assert published.get(field) == value, (field, published)
    assert set(published) == set(expected), published
    return entries[0]


def _log_events() -> list[dict]:
    """The `node.log()` events of the run, in the shape Node-RED's `helper.log()` records.

    Upstream's events are `{level, id, type, msg, path}`; the runtime also reports the node `name`,
    which no upstream debug assertion looks at.
    """
    fields = ("level", "id", "type", "msg", "path")
    return [{key: event[key] for key in fields if key in event} for event in take_node_logs()]


def _console_event(msg: str) -> dict:
    """The single `INFO` log event the node's console output produced."""
    return {"level": "INFO", "id": red_id("n1"), "type": "debug", "msg": msg, "path": _TAB}


@pytest.mark.describe('debug node')
class TestDebugNode:

    @pytest.mark.skip(reason="the spec asserts on the deployed node's own properties, which the "
                             "pytest bridge cannot read back: it only observes messages and events")
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_should_be_loaded(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should publish on input')
    async def test_should_publish_on_input(self):
        await _published_once(_debug_node(name="Debug"), {"payload": "test"},
                              name="Debug", msg="test", format="string[4]", property="payload")

    @pytest.mark.asyncio
    @pytest.mark.it('should publish to console')
    async def test_should_publish_to_console(self):
        await _published_once(_debug_node(console="true"), {"payload": "test"},
                              msg="test", format="string[4]", property="payload")
        assert _log_events() == [_console_event("test")]

    @pytest.mark.asyncio
    @pytest.mark.it('should publish complete message')
    async def test_should_publish_complete_message(self):
        entry = await _published_once(_debug_node(complete="true"), {"payload": "test"},
                                      format="Object", msg=_SEPARATE)
        assert entry["msg"] == '{"payload":"test"}'

    @pytest.mark.asyncio
    @pytest.mark.it('should publish complete message to console')
    async def test_should_publish_complete_message_to_console(self):
        entry = await _published_once(_debug_node(complete="true", console="true"),
                                      {"payload": "test"}, format="Object", msg=_SEPARATE)
        assert entry["msg"] == '{"payload":"test"}'
        assert _log_events() == [_console_event("\n{ payload: 'test' }")]

    @pytest.mark.asyncio
    @pytest.mark.it('should publish other property')
    async def test_should_publish_other_property(self):
        await _published_once(_debug_node(complete="foo"), {"payload": "test", "foo": "bar"},
                              msg="bar", property="foo", format="string[3]")

    @pytest.mark.asyncio
    @pytest.mark.it('should publish multi-level properties')
    async def test_should_publish_multi_level_properties(self):
        await _published_once(_debug_node(complete="foo.bar"), {"payload": "test", "foo": {"bar": "bar"}},
                              msg="bar", property="foo.bar", format="string[3]")

    @pytest.mark.skip(reason="Rust gap: an Error is not a value type in this message model, so a JS "
                             "Error arrives as a plain object and cannot be labelled 'error'")
    @pytest.mark.asyncio
    @pytest.mark.it('should publish an Error')
    async def test_should_publish_an_error(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should publish a boolean')
    async def test_should_publish_a_boolean(self):
        await _published_once(_debug_node(), {"payload": True}, msg="true", format="boolean", property="payload")

    @pytest.mark.asyncio
    @pytest.mark.it('should publish a number')
    async def test_should_publish_a_number(self):
        await _published_once(_debug_node(console="true"), {"payload": 7},
                              msg="7", format="number", property="payload")

    @pytest.mark.skip(reason="Rust gap: the message model is JSON, which cannot carry a NaN")
    @pytest.mark.asyncio
    @pytest.mark.it('should publish a NaN')
    async def test_should_publish_a_nan(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should publish with no payload')
    async def test_should_publish_with_no_payload(self):
        await _published_once(_debug_node(), {}, msg="(undefined)", format="undefined", property="payload")

    @pytest.mark.asyncio
    @pytest.mark.it('should publish a null')
    async def test_should_publish_a_null(self):
        await _published_once(_debug_node(), {"payload": None}, msg="(undefined)", format="null", property="payload")

    @pytest.mark.asyncio
    @pytest.mark.it('should publish an object')
    async def test_should_publish_an_object(self):
        entry = await _published_once(_debug_node(), {"payload": {"type": "foo"}},
                                      format="Object", property="payload", msg=_SEPARATE)
        assert json.loads(entry["msg"]) == {"type": "foo"}

    @pytest.mark.asyncio
    @pytest.mark.it('should publish an object with no-prototype-builtins')
    async def test_should_publish_an_object_with_no_prototype_builtins(self):
        entry = await _published_once(_debug_node(), {"payload": {"type": "foo"}},
                                      format="Object", property="payload", msg=_SEPARATE)
        assert json.loads(entry["msg"]) == {"type": "foo"}

    @pytest.mark.asyncio
    @pytest.mark.it('should publish an object with overriden hasOwnProperty')
    async def test_should_publish_an_object_with_overriden_has_own_property(self):
        entry = await _published_once(_debug_node(), {"payload": {"type": "foo", "hasOwnProperty": None}},
                                      format="Object", property="payload", msg=_SEPARATE)
        # The runtime's message objects are sorted maps, so the JSON text has its keys in a different
        # order than Node-RED's: compare the decoded value, not the string.
        assert json.loads(entry["msg"]) == {"type": "foo", "hasOwnProperty": None}

    @pytest.mark.asyncio
    @pytest.mark.it('should publish an array')
    async def test_should_publish_an_array(self):
        await _published_once(_debug_node(), {"payload": [0, 1, 2, 3]},
                              msg="[0,1,2,3]", format="array[4]", property="payload")

    @pytest.mark.skip(reason="Rust gap: the message model is JSON, which cannot carry a circular "
                             "reference, so its '[Circular ~]' marker is not reproduced")
    @pytest.mark.asyncio
    @pytest.mark.it('should publish an object with circular references')
    async def test_should_publish_an_object_with_circular_references(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should publish an object to console')
    async def test_should_publish_an_object_to_console(self):
        entry = await _published_once(_debug_node(console="true"), {"payload": {"type": "foo"}},
                                      format="Object", property="payload", msg=_SEPARATE)
        assert json.loads(entry["msg"]) == {"type": "foo"}
        assert _log_events() == [_console_event("\n{ type: 'foo' }")]

    @pytest.mark.asyncio
    @pytest.mark.it('should publish a string after a newline to console if the string contains \\n')
    async def test_should_publish_a_string_after_a_newline_to_console_if_the_string_contains_n(self):
        await _published_once(_debug_node(console="true"), {"payload": "test\ntest"},
                              msg="test\ntest", format="string[9]", property="payload")
        assert _log_events() == [_console_event("\ntest\ntest")]

    @pytest.mark.asyncio
    @pytest.mark.it('should publish complete message with edit')
    async def test_should_publish_complete_message_with_edit(self):
        # `targetType: "jsonata"` makes `complete` the expression of the debugged value, and the
        # record then carries no `property` field at all.
        await _published_once(
            _debug_node(name="Debug", complete='"<" & payload & ">"', targetType="jsonata"),
            {"payload": "test"}, name="Debug", msg="<test>", format="string[6]")

    @pytest.mark.asyncio
    @pytest.mark.it('should truncate a long message')
    async def test_should_truncate_a_long_message(self):
        await _published_once(_debug_node(), {"payload": "X" * 1001},
                              msg="X" * 1000 + "...", format="string[1001]", property="payload")

    @pytest.mark.asyncio
    @pytest.mark.it('should truncate a long string in the object')
    async def test_should_truncate_a_long_string_in_the_object(self):
        entry = await _published_once(_debug_node(), {"payload": {"foo": "X" * 1001}},
                                      format="Object", property="payload", msg=_SEPARATE)
        assert json.loads(entry["msg"]) == {"foo": "X" * 1000 + "..."}

    @pytest.mark.asyncio
    @pytest.mark.it('should truncate a large array')
    async def test_should_truncate_a_large_array(self):
        entry = await _published_once(_debug_node(), {"payload": ["X"] * 1001},
                                      format="array[1001]", property="payload", msg=_SEPARATE)
        assert json.loads(entry["msg"]) == {
            "__enc__": True, "type": "array", "data": ["X"] * 1000, "length": 1001,
        }

    @pytest.mark.asyncio
    @pytest.mark.it('should truncate a large array in the object')
    async def test_should_truncate_a_large_array_in_the_object(self):
        entry = await _published_once(_debug_node(), {"payload": {"foo": ["X"] * 1001}},
                                      format="Object", property="payload", msg=_SEPARATE)
        assert json.loads(entry["msg"]) == {
            "foo": {"__enc__": True, "type": "array", "data": ["X"] * 1000, "length": 1001},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('should truncate a large buffer')
    async def test_should_truncate_a_large_buffer(self):
        entries = await _run_function_into_debug("msg.payload = new Uint8Array(501).fill(0x22).buffer; return msg;")
        assert len(entries) == 1, entries
        assert _published(entries[0]) == {
            "id": red_id("n1"), "path": _TAB, "msg": "2" * 1000, "format": "buffer[501]",
            "property": "payload",
        }

    @pytest.mark.asyncio
    @pytest.mark.it('should truncate a large buffer in the object')
    async def test_should_truncate_a_large_buffer_in_the_object(self):
        entries = await _run_function_into_debug(
            "msg.payload = {foo: new Uint8Array(1001).fill(88).buffer}; return msg;")
        assert len(entries) == 1, entries
        assert _published(entries[0]) == {
            "id": red_id("n1"), "path": _TAB, "format": "Object", "property": "payload",
            "msg": json.dumps({
                "foo": {"type": "Buffer", "data": [88] * 1000, "__enc__": True, "length": 1001},
            }, separators=(",", ":")),
        }

    @pytest.mark.asyncio
    @pytest.mark.it('should convert Buffer to hex')
    async def test_should_convert_buffer_to_hex(self):
        entries = await _run_function_into_debug(
            "msg.payload = new Uint8Array([72,69,76,76,79]).buffer; return msg;")
        assert len(entries) == 1, entries
        assert _published(entries[0]) == {
            "id": red_id("n1"), "path": _TAB, "msg": "48454c4c4f", "format": "buffer[5]",
            "property": "payload",
        }

    @pytest.mark.skip(reason="Rust gap: the pytest bridge starts no admin HTTP server, so the "
                             "POST /debug/:id/:state call the spec makes cannot be reached")
    @pytest.mark.asyncio
    @pytest.mark.it('should publish when active')
    async def test_should_publish_when_active(self):
        pass

    @pytest.mark.skip(reason="Rust gap: the pytest bridge starts no admin HTTP server, so the "
                             "POST /debug/:id/:state call the spec makes cannot be reached")
    @pytest.mark.asyncio
    @pytest.mark.it('should not publish when inactive')
    async def test_should_not_publish_when_inactive(self):
        pass

    @pytest.mark.describe('post')
    class TestPost:

        @pytest.mark.skip(reason="Rust gap: the pytest bridge starts no admin HTTP server, so the "
                                 "POST /debug/:id/:state call the spec makes cannot be reached")
        @pytest.mark.asyncio
        @pytest.mark.it('should return 404 on invalid state')
        async def test_should_return_404_on_invalid_state(self):
            pass

        @pytest.mark.skip(reason="Rust gap: the pytest bridge starts no admin HTTP server, so the "
                                 "POST /debug/:id/:state call the spec makes cannot be reached")
        @pytest.mark.asyncio
        @pytest.mark.it('should return 404 on invalid node')
        async def test_should_return_404_on_invalid_node(self):
            pass

        @pytest.mark.skip(reason="Rust gap: the bulk POST /debug/:state endpoint of Node-RED's admin "
                                 "API is not implemented")
        @pytest.mark.asyncio
        @pytest.mark.it('should return 400 for invalid bulk disable')
        async def test_should_return_400_for_invalid_bulk_disable(self):
            pass

        @pytest.mark.skip(reason="Rust gap: the bulk POST /debug/:state endpoint of Node-RED's admin "
                                 "API is not implemented")
        @pytest.mark.asyncio
        @pytest.mark.it('should return success for bulk disable')
        async def test_should_return_success_for_bulk_disable(self):
            pass

    @pytest.mark.describe('get')
    class TestGet:

        @pytest.mark.skip(reason="Rust gap: the editor's debug view asset (debug/view/view.html) is "
                                 "served by the Node-RED editor, which this runtime does not ship")
        @pytest.mark.asyncio
        @pytest.mark.it('should return the view.html')
        async def test_should_return_the_view_html(self):
            pass

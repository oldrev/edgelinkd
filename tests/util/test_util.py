"""The `@node-red/util` spec (`test/unit/@node-red/util/lib/util_spec.js`), driven through a flow.

Node-RED hands the whole `RED.util` module to every `function` node (`10-function.js` spreads it
into the sandbox), so the only way the pytest bridge can reach the port is inside a function node:
each spec below runs its body there and reads the result back through `test-once`.

Binary values are the one place where the sandbox cannot spell Node's code verbatim: there is no
`Buffer` global, binary is an `ArrayBuffer`/`Uint8Array` (`Variant::Bytes`), so the specs that
upstream writes with `Buffer.from(...)` build the same bytes with `RED.util.ensureBuffer(...)` or
`new Uint8Array([...])`.
"""
import json

import pytest

from tests import *

JQ = json.dumps


async def _js(body: str):
    """Run `body` as the body of a `function` node and return the JSON it left in `msg.result`.

    `body` runs where the sandbox exposes `RED.util`, so it can call the port directly; it must
    leave a JSON string in `msg.result` (the snippet is always finished with `return msg;`).
    """
    node = {"type": "function", "func": body + "\nreturn msg;"}
    msgs = await run_single_node_with_msgs_ntimes(node, [{}], 1)
    return json.loads(msgs[0]["result"])


async def _eval(expr: str):
    """Evaluate `expr` against the ported `RED.util`; return `("ok", value)` or `("error", code)`."""
    raw = await _js(
        "try {\n"
        f"    var __v = ({expr});\n"
        "    msg.result = JSON.stringify({ r: 'ok', v: (__v === undefined ? null : __v) });\n"
        "} catch (err) {\n"
        "    msg.result = JSON.stringify({ r: 'error', v: (err && err.code) || String(err) });\n"
        "}"
    )
    return raw["r"], raw.get("v")


async def _value(expr: str):
    """Evaluate `expr`, failing the test if it throws. `undefined` and `null` both read as `None`."""
    kind, value = await _eval(expr)
    assert kind == "ok", f"{expr} threw: {value}"
    return value


async def _error(expr: str):
    """Evaluate `expr`, failing the test unless it throws. Returns the error code (or message)."""
    kind, value = await _eval(expr)
    assert kind == "error", f"{expr} did not throw (returned {value!r})"
    return value


async def _set_property(api: str, initial_js: str, prop: str, value_js: str, create_missing=None):
    """`RED.util.<api>(<initial>, prop, value[, createMissing])`, reporting the mutated object."""
    extra = "" if create_missing is None else f", {str(create_missing).lower()}"
    return await _js(
        f"var __m = {initial_js};\n"
        f"var __r = RED.util.{api}(__m, {JQ(prop)}, {value_js}{extra});\n"
        "msg.result = JSON.stringify({ result: __r, message: __m });"
    )


async def _set_message_property(initial_js: str, prop: str, value_js: str, create_missing=None):
    return await _set_property("setMessageProperty", initial_js, prop, value_js, create_missing)


async def _set_object_property(initial_js: str, prop: str, value_js: str, create_missing=None):
    return await _set_property("setObjectProperty", initial_js, prop, value_js, create_missing)


async def _encode(js_value: str, max_length: int | None = None):
    """`RED.util.encodeObject({msg: <js_value>}, opts)`; returns `{"format": ..., "msg": ...}`."""
    opts = "undefined" if max_length is None else "{ maxLength: %d }" % max_length
    return await _js(
        "var __encoded = RED.util.encodeObject({ msg: (" + js_value + ") }, " + opts + ");\n"
        "msg.result = JSON.stringify({ format: __encoded.format, msg: __encoded.msg });"
    )


# =============================================================================================
# normalisePropertyExpression helpers (the upstream spec's testABC/testInvalid/testToString)
# =============================================================================================

async def _test_abc(text, expected):
    assert await _value(f"RED.util.normalisePropertyExpression({JQ(text)})") == expected


async def _test_abc_with_message(text, msg, expected):
    assert await _value(f"RED.util.normalisePropertyExpression({JQ(text)}, {JQ(msg)})") == expected


async def _test_invalid(text, msg=None):
    kind, value = await _eval(f"RED.util.normalisePropertyExpression({JQ(text)}, {JQ(msg)})")
    assert kind == "error", f"normalisePropertyExpression({text!r}) did not throw (returned {value!r})"


async def _test_to_string(text, msg, expected):
    assert await _value(f"RED.util.normalisePropertyExpression({JQ(text)}, {JQ(msg)}, true)") == expected


# =============================================================================================

@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('generateId')
class TestGenerateId:

    @pytest.mark.asyncio
    @pytest.mark.it('generates an id')
    async def test_0001(self):
        assert await _value("RED.util.generateId() !== RED.util.generateId()") is True


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('compareObjects')
class TestCompareObjects:

    @pytest.mark.asyncio
    @pytest.mark.it('numbers')
    async def test_0001(self):
        assert await _value("""[
            RED.util.compareObjects(0,0),
            RED.util.compareObjects(0,1),
            RED.util.compareObjects(1000,1001),
            RED.util.compareObjects(1000,1000),
            RED.util.compareObjects(0,"0"),
            RED.util.compareObjects(1,"1"),
            RED.util.compareObjects(0,null),
            RED.util.compareObjects(0,undefined),
        ]""") == [True, False, False, True, False, False, False, False]

    @pytest.mark.asyncio
    @pytest.mark.it('strings')
    async def test_0002(self):
        assert await _value("""[
            RED.util.compareObjects("",""),
            RED.util.compareObjects("a","a"),
            RED.util.compareObjects("",null),
            RED.util.compareObjects("",undefined),
        ]""") == [True, True, False, False]

    @pytest.mark.asyncio
    @pytest.mark.it('arrays')
    async def test_0003(self):
        assert await _value("""[
            RED.util.compareObjects(["a"],["a"]),
            RED.util.compareObjects(["a"],["a","b"]),
            RED.util.compareObjects(["a","b"],["b"]),
            RED.util.compareObjects(["a"],"a"),
            RED.util.compareObjects([[1],["a"]],[[1],["a"]]),
            RED.util.compareObjects([[1],["a"]],[["a"],[1]]),
        ]""") == [True, False, False, False, True, False]

    @pytest.mark.asyncio
    @pytest.mark.it('objects')
    async def test_0004(self):
        assert await _value("""[
            RED.util.compareObjects({"a":1},{"a":1,"b":1}),
            RED.util.compareObjects({"a":1,"b":1},{"a":1,"b":1}),
            RED.util.compareObjects({"b":1,"a":1},{"a":1,"b":1}),
        ]""") == [False, True, True]

    @pytest.mark.asyncio
    @pytest.mark.it('Buffer')
    async def test_0005(self):
        # Upstream builds these with `Buffer.from(...)`; the sandbox's binary type is a Uint8Array,
        # which is what `ensureBuffer` returns. The comparison is over the same bytes either way.
        assert await _value("""[
            RED.util.compareObjects(RED.util.ensureBuffer("hello"),RED.util.ensureBuffer("hello")),
            RED.util.compareObjects(RED.util.ensureBuffer("hello"),RED.util.ensureBuffer("hello ")),
            RED.util.compareObjects(RED.util.ensureBuffer("hello"),"hello"),
        ]""") == [True, False, False]


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('ensureString')
class TestEnsureString:

    @pytest.mark.asyncio
    @pytest.mark.it('strings are preserved')
    async def test_0001(self):
        assert await _value("RED.util.ensureString('string')") == 'string'

    @pytest.mark.asyncio
    @pytest.mark.it('Buffer is converted')
    async def test_0002(self):
        result = await _js(
            "var __s = RED.util.ensureString(RED.util.ensureBuffer('foo'));\n"
            "msg.result = JSON.stringify({ s: __s, type: typeof __s });"
        )
        assert result == {"s": "foo", "type": "string"}

    @pytest.mark.asyncio
    @pytest.mark.it('Object is converted to JSON')
    async def test_0003(self):
        result = await _js(
            "var __s = RED.util.ensureString({foo: 'bar'});\n"
            "msg.result = JSON.stringify({ type: typeof __s, value: JSON.parse(__s) });"
        )
        assert result == {"type": "string", "value": {"foo": "bar"}}

    @pytest.mark.asyncio
    @pytest.mark.it('stringifies other things')
    async def test_0004(self):
        result = await _js(
            "var __s = RED.util.ensureString(123);\n"
            "msg.result = JSON.stringify({ type: typeof __s, s: __s });"
        )
        assert result == {"type": "string", "s": "123"}


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('ensureBuffer')
class TestEnsureBuffer:

    @pytest.mark.asyncio
    @pytest.mark.it('Buffers are preserved')
    async def test_0001(self):
        assert await _value("""(function() {
            var b = RED.util.ensureBuffer('');
            return RED.util.ensureBuffer(b) === b;
        })()""") is True

    @pytest.mark.asyncio
    @pytest.mark.it('string is converted')
    async def test_0002(self):
        # `Buffer.isBuffer(b)` reads as "the result is a byte array" here: Uint8Array is the type
        # `ensureBuffer` returns (and that the Rust side reads back as `Variant::Bytes`).
        result = await _js("""var b = RED.util.ensureBuffer('foo');
            var expected = [102, 111, 111];
            var same = b.length === expected.length;
            for (var i = 0; i < expected.length; i++) { same = same && b[i] === expected[i]; }
            msg.result = JSON.stringify({ isBuffer: b instanceof Uint8Array, same: same });""")
        assert result == {"isBuffer": True, "same": True}

    @pytest.mark.asyncio
    @pytest.mark.it('Object is converted to JSON')
    async def test_0003(self):
        # Upstream reads the bytes back with `JSON.parse(b)`, relying on Node coercing the Buffer
        # to a UTF-8 string. The sandbox's byte type is a Uint8Array, whose `toString()` is
        # `"123,34,..."`, so the same bytes are decoded through `ensureString` here; the JSON
        # round-trip is what upstream's assertion is about. (`b` is a Uint8Array, not a Buffer -
        # that difference is the documented gap, see README.)
        result = await _js("""var b = RED.util.ensureBuffer({foo: "bar"});
            msg.result = JSON.stringify({
                isBuffer: b instanceof Uint8Array,
                decoded: JSON.parse(RED.util.ensureString(b))
            });""")
        assert result == {"isBuffer": True, "decoded": {"foo": "bar"}}

    @pytest.mark.asyncio
    @pytest.mark.it('stringifies other things')
    async def test_0004(self):
        result = await _js("""var b = RED.util.ensureBuffer(123);
            var expected = [49, 50, 51];
            var same = b.length === expected.length;
            for (var i = 0; i < expected.length; i++) { same = same && b[i] === expected[i]; }
            msg.result = JSON.stringify({ isBuffer: b instanceof Uint8Array, same: same });""")
        assert result == {"isBuffer": True, "same": True}


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('cloneMessage')
class TestCloneMessage:

    @pytest.mark.asyncio
    @pytest.mark.it('clones a simple message')
    async def test_0001(self):
        result = await _js("""var __m = {string:"hi",array:[1,2,3],object:{a:1,subobject:{b:2}}};
            var cloned = RED.util.cloneMessage(__m);
            msg.result = JSON.stringify({
                equal: JSON.stringify(cloned) === JSON.stringify(__m),
                notSame: cloned !== __m,
                arrayNotSame: cloned.array !== __m.array,
                objectNotSame: cloned.object !== __m.object,
                subobjectNotSame: cloned.object.subobject !== __m.object.subobject,
                hasReq: Object.prototype.hasOwnProperty.call(cloned, 'req'),
                hasRes: Object.prototype.hasOwnProperty.call(cloned, 'res')
            });""")
        assert result == {
            "equal": True,
            "notSame": True,
            "arrayNotSame": True,
            "objectNotSame": True,
            "subobjectNotSame": True,
            "hasReq": False,
            "hasRes": False,
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not clone http req/res properties')
    async def test_0002(self):
        result = await _js("""var __m = {req:{a:1},res:{b:2}};
            var cloned = RED.util.cloneMessage(__m);
            msg.result = JSON.stringify({
                equal: JSON.stringify(cloned) === JSON.stringify(__m),
                notSame: cloned !== __m,
                reqSame: cloned.req === __m.req,
                resSame: cloned.res === __m.res
            });""")
        assert result == {"equal": True, "notSame": True, "reqSame": True, "resSame": True}

    @pytest.mark.asyncio
    @pytest.mark.it('handles undefined values without throwing an error')
    async def test_0003(self):
        assert await _value("RED.util.cloneMessage(undefined) === undefined") is True


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('getObjectProperty')
class TestGetObjectProperty:

    @pytest.mark.asyncio
    @pytest.mark.it('gets a property beginning with "msg."')
    async def test_0001(self):
        # getMessageProperty strips off `msg.` prefixes.
        # getObjectProperty does not
        assert await _value("RED.util.getObjectProperty({msg:{a:'foo'},a:'bar'},'msg.a')") == "foo"


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('getMessageProperty')
class TestGetMessageProperty:

    @pytest.mark.asyncio
    @pytest.mark.it('retrieves a simple property')
    async def test_0001(self):
        assert await _value(
            "[RED.util.getMessageProperty({a:'foo'},'msg.a'), RED.util.getMessageProperty({a:'foo'},'a')]"
        ) == ["foo", "foo"]

    @pytest.mark.asyncio
    @pytest.mark.it('retrieves a nested property')
    async def test_0002(self):
        assert await _value("""[
            RED.util.getMessageProperty({a:"foo",b:{foo:1,bar:2}},"msg.b[msg.a]"),
            RED.util.getMessageProperty({a:"bar",b:{foo:1,bar:2}},"b[msg.a]"),
        ]""") == [1, 2]

    @pytest.mark.asyncio
    @pytest.mark.it('should return undefined if property does not exist')
    async def test_0003(self):
        assert await _value("RED.util.getMessageProperty({a:'foo'},'msg.b') === undefined") is True

    @pytest.mark.asyncio
    @pytest.mark.it('should throw error if property parent does not exist')
    async def test_0004(self):
        await _error("RED.util.getMessageProperty({a:'foo'},'msg.a.b.c')")

    @pytest.mark.asyncio
    @pytest.mark.it('retrieves a property with array syntax')
    async def test_0005(self):
        assert await _value("""[
            RED.util.getMessageProperty({a:["foo","bar"]},"msg.a[0]"),
            RED.util.getMessageProperty({a:[null,{b:"foo"}]},"a[1].b"),
            RED.util.getMessageProperty({a:[[["foo"]]]},"a[0][0][0]"),
        ]""") == ["foo", "foo", "foo"]


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('setObjectProperty')
class TestSetObjectProperty:

    @pytest.mark.asyncio
    @pytest.mark.it('set a property beginning with "msg."')
    async def test_0001(self):
        # setMessageProperty strips off `msg.` prefixes.
        # setObjectProperty does not
        assert await _set_object_property("{}", "msg.a", "'bar'") == {
            "result": True,
            "message": {"msg": {"a": "bar"}},
        }


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('setMessageProperty')
class TestSetMessageProperty:

    @pytest.mark.asyncio
    @pytest.mark.it('sets a property')
    async def test_0001(self):
        assert await _set_message_property("""{a:"foo"}""", "msg.a", "'bar'") == {
            "result": True,
            "message": {"a": "bar"},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('sets a deep level property')
    async def test_0002(self):
        assert await _set_message_property("""{a:{b:{c:"foo"}}}""", "msg.a.b.c", "'bar'") == {
            "result": True,
            "message": {"a": {"b": {"c": "bar"}}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('creates missing parent properties by default')
    async def test_0003(self):
        assert await _set_message_property("""{a:{}}""", "msg.a.b.c", "'bar'") == {
            "result": True,
            "message": {"a": {"b": {"c": "bar"}}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing parent properties')
    async def test_0004(self):
        assert await _set_message_property("""{a:{}}""", "msg.a.b.c", "'bar'", False) == {
            "result": False,
            "message": {"a": {}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing parent properties of array')
    async def test_0005(self):
        assert await _set_message_property("""{a:{}}""", "msg.a.b[1].c", "'bar'", False) == {
            "result": False,
            "message": {"a": {}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing parent properties of string')
    async def test_0006(self):
        assert await _set_message_property("""{a:"foo"}""", "msg.a.b.c", "'bar'", False) == {
            "result": False,
            "message": {"a": "foo"},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not set property of existing string property')
    async def test_0007(self):
        assert await _set_message_property("""{a:"foo"}""", "msg.a.b", "'bar'", False) == {
            "result": False,
            "message": {"a": "foo"},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not set property of existing number property')
    async def test_0008(self):
        assert await _set_message_property("""{a:123}""", "msg.a.b", "'bar'", False) == {
            "result": False,
            "message": {"a": 123},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing parent properties of number')
    async def test_0009(self):
        assert await _set_message_property("""{a:123}""", "msg.a.b.c", "'bar'", False) == {
            "result": False,
            "message": {"a": 123},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not set property of existing boolean property')
    async def test_0010(self):
        assert await _set_message_property("""{a:true}""", "msg.a.b", "'bar'", False) == {
            "result": False,
            "message": {"a": True},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing parent properties of boolean')
    async def test_0011(self):
        assert await _set_message_property("""{a:true}""", "msg.a.b.c", "'bar'", False) == {
            "result": False,
            "message": {"a": True},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('deletes property if value is undefined')
    async def test_0012(self):
        assert await _set_message_property("""{a:{b:{c:"foo"}}}""", "msg.a.b.c", "undefined") == {
            "result": True,
            "message": {"a": {"b": {}}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing parent properties if value is undefined')
    async def test_0013(self):
        assert await _set_message_property("""{a:{}}""", "msg.a.b.c", "undefined") == {
            "result": False,
            "message": {"a": {}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('sets a property with array syntax')
    async def test_0014(self):
        assert await _set_message_property("""{a:{b:["foo",{c:["",""]}]}}""", "msg.a.b[1].c[1]", "'bar'") == {
            "result": True,
            "message": {"a": {"b": ["foo", {"c": ["", "bar"]}]}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('creates missing array elements - final property')
    async def test_0015(self):
        assert await _set_message_property("""{a:[]}""", "msg.a[2]", "'bar'") == {
            "result": True,
            "message": {"a": [None, None, "bar"]},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('creates missing array elements - mid property')
    async def test_0016(self):
        assert await _set_message_property("{}", "msg.a[2].b", "'bar'") == {
            "result": True,
            "message": {"a": [None, None, {"b": "bar"}]},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('creates missing array elements - multi-arrays')
    async def test_0017(self):
        assert await _set_message_property("{}", "msg.a[2][2]", "'bar'") == {
            "result": True,
            "message": {"a": [None, None, [None, None, "bar"]]},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing array elements - mid property')
    async def test_0018(self):
        assert await _set_message_property("""{a:[]}""", "msg.a[1][1]", "'bar'", False) == {
            "result": False,
            "message": {"a": []},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('does not create missing array elements - final property')
    async def test_0019(self):
        # check it has not been misinterpreted
        assert await _set_message_property("""{a:{}}""", "msg.a.b[2]", "'bar'", False) == {
            "result": False,
            "message": {"a": {}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('deletes property inside array if value is undefined')
    async def test_0020(self):
        assert await _set_message_property("""{a:[1,2,3]}""", "msg.a[1]", "undefined") == {
            "result": True,
            "message": {"a": [1, 3]},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('handles nested message property references')
    async def test_0021(self):
        assert await _set_object_property("""{a:"foo",b:{}}""", "b[msg.a]", "'bar'") == {
            "result": True,
            "message": {"a": "foo", "b": {"foo": "bar"}},
        }

    @pytest.mark.asyncio
    @pytest.mark.it('handles nested message property references')
    async def test_0022(self):
        assert await _set_object_property("""{a:"foo",b:{"foo":[0,0,0]}}""", "b[msg.a][2]", "'bar'") == {
            "result": True,
            "message": {"a": "foo", "b": {"foo": [0, 0, "bar"]}},
        }


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('evaluateNodeProperty')
class TestEvaluateNodeProperty:

    @pytest.mark.asyncio
    @pytest.mark.it('returns string')
    async def test_0001(self):
        assert await _value("RED.util.evaluateNodeProperty('hello','str')") == 'hello'

    @pytest.mark.asyncio
    @pytest.mark.it('returns number')
    async def test_0002(self):
        assert await _value("RED.util.evaluateNodeProperty('0123','num')") == 123

    @pytest.mark.asyncio
    @pytest.mark.it('returns evaluated json')
    async def test_0003(self):
        assert await _value("""RED.util.evaluateNodeProperty('{"a":123}','json')""") == {"a": 123}

    @pytest.mark.asyncio
    @pytest.mark.it('returns regex')
    async def test_0004(self):
        assert await _value("RED.util.evaluateNodeProperty('^abc$','re').toString()") == '/^abc$/'

    @pytest.mark.asyncio
    @pytest.mark.it('returns boolean')
    async def test_0005(self):
        assert await _value("""[
            RED.util.evaluateNodeProperty('true','bool'),
            RED.util.evaluateNodeProperty('TrUe','bool'),
            RED.util.evaluateNodeProperty('false','bool'),
            RED.util.evaluateNodeProperty('','bool'),
        ]""") == [True, True, False, False]

    @pytest.mark.asyncio
    @pytest.mark.it('returns date - default format')
    async def test_0006(self):
        result = await _js("""var __result = RED.util.evaluateNodeProperty('','date');
            msg.result = JSON.stringify({ diff: Math.abs(Date.now() - __result) });""")
        assert result["diff"] <= 50

    @pytest.mark.asyncio
    @pytest.mark.it('returns date - iso format')
    async def test_0007(self):
        assert await _value(
            r"/^\d\d\d\d-\d\d-\d\dT\d\d:\d\d:\d\d\.\d+Z$/.test(RED.util.evaluateNodeProperty('iso','date'))"
        ) is True

    @pytest.mark.asyncio
    @pytest.mark.it('returns bin')
    async def test_0008(self):
        assert await _value("""(function () {
            var result = RED.util.evaluateNodeProperty('[1, 2]','bin');
            return [result[0], result[1]];
        })()""") == [1, 2]

    @pytest.mark.asyncio
    @pytest.mark.it('throws an error if buffer data is not array or string')
    async def test_0009(self):
        assert await _error("RED.util.evaluateNodeProperty('12','bin')") == "INVALID_BUFFER_DATA"

    @pytest.mark.asyncio
    @pytest.mark.it('returns msg property')
    async def test_0010(self):
        assert await _value("RED.util.evaluateNodeProperty('foo.bar','msg',{},{foo:{bar:\"123\"}})") == "123"

    @pytest.mark.asyncio
    @pytest.mark.it('throws an error if callback is not defined')
    async def test_0011(self):
        await _error("""RED.util.evaluateNodeProperty(' ','msg',{},{foo:{bar:"123"}})""")

    @pytest.mark.asyncio
    @pytest.mark.it('returns flow property')
    async def test_0012(self):
        assert await _value("""RED.util.evaluateNodeProperty('foo.bar','flow',{
            context:function() { return {
                flow: { get: function(k) {
                    if (k === 'foo.bar') {
                        return '123';
                    } else {
                        return null;
                    }
                }}
            }}
        },{})""") == "123"

    @pytest.mark.asyncio
    @pytest.mark.it('returns global property')
    async def test_0013(self):
        assert await _value("""RED.util.evaluateNodeProperty('foo.bar','global',{
            context:function() { return {
                global: { get: function(k) {
                    if (k === 'foo.bar') {
                        return '123';
                    } else {
                        return null;
                    }
                }}
            }}
        },{})""") == "123"

    @pytest.mark.skip(reason="JSONata is implemented by the EdgeLinkd Rust runtime and is not exposed "
                             "to the function node sandbox, so evaluateNodeProperty('jsonata') fails "
                             "loudly instead of returning a result")
    @pytest.mark.asyncio
    @pytest.mark.it('returns jsonata result')
    async def test_0014(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('returns null')
    async def test_0015(self):
        assert await _value("RED.util.evaluateNodeProperty(null,'null') === null") is True


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('evaluateNodeProperty')
@pytest.mark.describe('environment variable')
class TestEvaluateNodePropertyEnvironmentVariable:

    @pytest.fixture(autouse=True)
    def _env(self, monkeypatch):
        monkeypatch.setenv("NR_TEST_A", "foo")
        monkeypatch.setenv("NR_TEST_B", "${NR_TEST_A}")

    @pytest.mark.asyncio
    @pytest.mark.it('returns an environment variable - NR_TEST_A')
    async def test_0001(self):
        assert await _value("RED.util.evaluateNodeProperty('NR_TEST_A','env')") == 'foo'

    @pytest.mark.asyncio
    @pytest.mark.it('returns an environment variable - ${NR_TEST_A}')
    async def test_0002(self):
        assert await _value("RED.util.evaluateNodeProperty('${NR_TEST_A}','env')") == 'foo'

    @pytest.mark.asyncio
    @pytest.mark.it('returns an environment variable - ${NR_TEST_A')
    async def test_0003(self):
        assert await _value("RED.util.evaluateNodeProperty('${NR_TEST_A','env')") == ''

    @pytest.mark.asyncio
    @pytest.mark.it('returns an environment variable - foo${NR_TEST_A}bar')
    async def test_0004(self):
        assert await _value("RED.util.evaluateNodeProperty('123${NR_TEST_A}456','env')") == '123foo456'

    @pytest.mark.asyncio
    @pytest.mark.it('returns an environment variable - foo${NR_TEST_B}bar')
    async def test_0005(self):
        assert await _value("RED.util.evaluateNodeProperty('123${NR_TEST_B}456','env')") == '123${NR_TEST_A}456'


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('normalisePropertyExpression')
class TestNormalisePropertyExpression:

    @pytest.mark.asyncio
    @pytest.mark.it('pass a.b.c')
    async def test_0001(self):
        await _test_abc('a.b.c', ['a', 'b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it('pass a["b"]["c"]')
    async def test_0002(self):
        await _test_abc('a["b"]["c"]', ['a', 'b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it('pass a["b"].c')
    async def test_0003(self):
        await _test_abc('a["b"].c', ['a', 'b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a['b'].c")
    async def test_0004(self):
        await _test_abc("a['b'].c", ['a', 'b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a[0].c")
    async def test_0005(self):
        await _test_abc("a[0].c", ['a', 0, 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a.0.c")
    async def test_0006(self):
        await _test_abc("a.0.c", ['a', 0, 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a['a.b[0]'].c")
    async def test_0007(self):
        await _test_abc("a['a.b[0]'].c", ['a', 'a.b[0]', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a[0][0][0]")
    async def test_0008(self):
        await _test_abc("a[0][0][0]", ['a', 0, 0, 0])

    @pytest.mark.asyncio
    @pytest.mark.it("pass '1.2.3.4'")
    async def test_0009(self):
        await _test_abc("'1.2.3.4'", ['1.2.3.4'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass 'a.b'[1]")
    async def test_0010(self):
        await _test_abc("'a.b'[1]", ['a.b', 1])

    @pytest.mark.asyncio
    @pytest.mark.it("pass 'a.b'.c")
    async def test_0011(self):
        await _test_abc("'a.b'.c", ['a.b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a[msg.b]")
    async def test_0012(self):
        await _test_abc("a[msg.b]", ["a", ["msg", "b"]])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a[msg[msg.b]]")
    async def test_0013(self):
        await _test_abc("a[msg[msg.b]]", ["a", ["msg", ["msg", "b"]]])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a[msg.b]")
    async def test_0014(self):
        await _test_abc("a[msg.b]", ["a", ["msg", "b"]])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a[msg.b]")
    async def test_0015(self):
        await _test_abc("a[msg.b]", ["a", ["msg", "b"]])

    @pytest.mark.asyncio
    @pytest.mark.it("""pass a[msg['b]"[']]""")
    async def test_0016(self):
        await _test_abc("""a[msg['b]"[']]""", ["a", ['msg', 'b]"[']])

    @pytest.mark.asyncio
    @pytest.mark.it("""pass a[msg['b][']]""")
    async def test_0017(self):
        await _test_abc("""a[msg['b][']]""", ["a", ['msg', "b]["]])

    @pytest.mark.asyncio
    @pytest.mark.it("pass b[msg.a][2]")
    async def test_0018(self):
        await _test_abc("b[msg.a][2]", ["b", ["msg", "a"], 2])

    @pytest.mark.asyncio
    @pytest.mark.it("pass b[msg.a][2] (with message)")
    async def test_0019(self):
        await _test_abc_with_message("b[msg.a][2]", {"a": "foo"}, ["b", "foo", 2])

    @pytest.mark.asyncio
    @pytest.mark.it('pass a.$b.c')
    async def test_0020(self):
        await _test_abc('a.$b.c', ['a', '$b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it('pass a["$b"].c')
    async def test_0021(self):
        await _test_abc('a["$b"].c', ['a', '$b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it('pass a._b.c')
    async def test_0022(self):
        await _test_abc('a._b.c', ['a', '_b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it('pass a["_b"].c')
    async def test_0023(self):
        await _test_abc('a["_b"].c', ['a', '_b', 'c'])

    @pytest.mark.asyncio
    @pytest.mark.it("pass a['a.b[0]'].c")
    async def test_0024(self):
        await _test_to_string("a['a.b[0]'].c", None, 'a["a.b[0]"]["c"]')

    @pytest.mark.asyncio
    @pytest.mark.it("pass a.b.c")
    async def test_0025(self):
        await _test_to_string("a.b.c", None, 'a["b"]["c"]')

    @pytest.mark.asyncio
    @pytest.mark.it('pass a[msg.c][0]["fred"]')
    async def test_0026(self):
        await _test_to_string('a[msg.c][0]["fred"]', {"c": "123"}, 'a["123"][0]["fred"]')

    @pytest.mark.asyncio
    @pytest.mark.it("fail a'b'.c")
    async def test_0027(self):
        await _test_invalid("a'b'.c")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a['b'.c")
    async def test_0028(self):
        await _test_invalid("a['b'.c")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[]")
    async def test_0029(self):
        await _test_invalid("a[]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a]")
    async def test_0030(self):
        await _test_invalid("a]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[")
    async def test_0031(self):
        await _test_invalid("a[")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[0d]")
    async def test_0032(self):
        await _test_invalid("a[0d]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a['")
    async def test_0033(self):
        await _test_invalid("a['")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[']")
    async def test_0034(self):
        await _test_invalid("a[']")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[0']")
    async def test_0035(self):
        await _test_invalid("a[0']")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a.[0]")
    async def test_0036(self):
        await _test_invalid("a.[0]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail [0]")
    async def test_0037(self):
        await _test_invalid("[0]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[0")
    async def test_0038(self):
        await _test_invalid("a[0")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a.")
    async def test_0039(self):
        await _test_invalid("a.")

    @pytest.mark.asyncio
    @pytest.mark.it("fail .a")
    async def test_0040(self):
        await _test_invalid(".a")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a. b")
    async def test_0041(self):
        await _test_invalid("a. b")

    @pytest.mark.asyncio
    @pytest.mark.it("fail  a.b")
    async def test_0042(self):
        await _test_invalid(" a.b")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[0].[1]")
    async def test_0043(self):
        await _test_invalid("a[0].[1]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a['']")
    async def test_0044(self):
        await _test_invalid("a['']")

    @pytest.mark.asyncio
    @pytest.mark.it("fail 'a.b'c")
    async def test_0045(self):
        await _test_invalid("'a.b'c")

    @pytest.mark.asyncio
    @pytest.mark.it("fail <blank>")
    async def test_0046(self):
        await _test_invalid("")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[b]")
    async def test_0047(self):
        await _test_invalid("a[b]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[msg.]")
    async def test_0048(self):
        await _test_invalid("a[msg.]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[msg[]")
    async def test_0049(self):
        await _test_invalid("a[msg[]")

    @pytest.mark.asyncio
    @pytest.mark.it("fail a[msg.[]]")
    async def test_0050(self):
        await _test_invalid("a[msg.[]]")

    @pytest.mark.asyncio
    @pytest.mark.it("""fail a[msg['af]]""")
    async def test_0051(self):
        await _test_invalid("""a[msg['af]]""")

    @pytest.mark.asyncio
    @pytest.mark.it("fail b[msg.undefined][2] (with message)")
    async def test_0052(self):
        await _test_invalid("b[msg.undefined][2]", {})


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('normaliseNodeTypeName')
class TestNormaliseNodeTypeName:

    async def _normalise(self, text, expected):
        assert await _value(f"RED.util.normaliseNodeTypeName({JQ(text)})") == expected

    @pytest.mark.asyncio
    @pytest.mark.it('pass blank')
    async def test_0001(self):
        await self._normalise("", "")

    @pytest.mark.asyncio
    @pytest.mark.it('pass ab1')
    async def test_0002(self):
        await self._normalise("ab1", "ab1")

    @pytest.mark.asyncio
    @pytest.mark.it('pass AB1')
    async def test_0003(self):
        await self._normalise("AB1", "aB1")

    @pytest.mark.asyncio
    @pytest.mark.it('pass a b 1')
    async def test_0004(self):
        await self._normalise("a b 1", "aB1")

    @pytest.mark.asyncio
    @pytest.mark.it('pass a-b-1')
    async def test_0005(self):
        await self._normalise("a-b-1", "aB1")

    @pytest.mark.asyncio
    @pytest.mark.it('pass  ab1 ')
    async def test_0006(self):
        await self._normalise(" ab1 ", "ab1")

    @pytest.mark.asyncio
    @pytest.mark.it('pass _a_b_1_')
    async def test_0007(self):
        await self._normalise("_a_b_1_", "aB1")

    @pytest.mark.asyncio
    @pytest.mark.it('pass http request')
    async def test_0008(self):
        await self._normalise("http request", "httpRequest")

    @pytest.mark.asyncio
    @pytest.mark.it('pass HttpRequest')
    async def test_0009(self):
        await self._normalise("HttpRequest", "httpRequest")


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('prepareJSONataExpression')
class TestPrepareJSONataExpression:

    @pytest.mark.skip(reason="JSONata is implemented by the EdgeLinkd Rust runtime and is not exposed "
                             "to the function node sandbox: prepareJSONataExpression fails loudly "
                             "instead of returning an expression object")
    @pytest.mark.asyncio
    @pytest.mark.it('prepares an expression')
    async def test_0001(self):
        pass

    @pytest.mark.skip(reason="JSONata is implemented by the EdgeLinkd Rust runtime and is not exposed "
                             "to the function node sandbox: prepareJSONataExpression fails loudly "
                             "instead of returning an expression object")
    @pytest.mark.asyncio
    @pytest.mark.it('prepares a legacyMode expression')
    async def test_0002(self):
        pass


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('evaluateJSONataExpression')
class TestEvaluateJSONataExpression:
    """Every upstream case needs a JSONata engine inside the sandbox; EdgeLinkd evaluates JSONata
    in Rust (`runtime/jsonata`) and does not expose that engine to JavaScript, so the whole
    describe is out of scope and the port fails loudly (`NOT_SUPPORTED`) instead."""

    _REASON = ("JSONata is implemented by the EdgeLinkd Rust runtime and is not exposed to the "
               "function node sandbox")

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('evaluates an expression')
    async def test_0001(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('evaluates a legacyMode expression')
    async def test_0002(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('accesses flow context from an expression')
    async def test_0003(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('accesses undefined environment variable from an expression')
    async def test_0004(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('accesses environment variable from an expression')
    async def test_0005(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('accesses moment from an expression')
    async def test_0006(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('accesses moment-timezone from an expression')
    async def test_0007(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('handles non-existant flow context variable')
    async def test_0008(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('handles non-existant global context variable')
    async def test_0009(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('handles async flow context access')
    async def test_0010(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('handles async global context access')
    async def test_0011(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('handles persistable store in flow context access')
    async def test_0012(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('handles persistable store in global context access')
    async def test_0013(self):
        pass

    @pytest.mark.skip(reason=_REASON)
    @pytest.mark.asyncio
    @pytest.mark.it('callbacks with error when invalid expression was specified')
    async def test_0014(self):
        pass


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('encodeObject')
class TestEncodeObject:

    @pytest.mark.asyncio
    @pytest.mark.it('encodes Error with message')
    async def test_0001(self):
        result = await _encode("(function(){ var e = new Error('encode error'); e.name = 'encodeError'; return e; })()")
        assert result["format"] == "error"
        assert json.loads(result["msg"]) == {"name": "encodeError", "message": "encode error"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes Error without message')
    async def test_0002(self):
        result = await _encode("""(function(){
            var e = new Error();
            e.name = 'encodeError';
            e.toString = function(){ return 'error message'; };
            return e;
        })()""")
        assert result["format"] == "error"
        assert json.loads(result["msg"]) == {"name": "encodeError", "message": "error message"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes Buffer')
    async def test_0003(self):
        # `Buffer.from("abc")` is spelled with the sandbox's byte type; the hex output is the same.
        # `maxLength` caps the hex string too, which is what upstream's char-by-char assertions see.
        result = await _encode("new Uint8Array([97, 98, 99])", max_length=4)
        assert result["format"] == "buffer[3]"
        assert result["msg"] == "6162"

    @pytest.mark.asyncio
    @pytest.mark.it('encodes function')
    async def test_0004(self):
        result = await _encode("(function(){})")
        assert result == {"format": "function", "msg": "[function]"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes boolean')
    async def test_0005(self):
        result = await _encode("true")
        assert result == {"format": "boolean", "msg": "true"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes number')
    async def test_0006(self):
        result = await _encode("123")
        assert result == {"format": "number", "msg": "123"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes 0')
    async def test_0007(self):
        result = await _encode("0")
        assert result == {"format": "number", "msg": "0"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes null')
    async def test_0008(self):
        result = await _encode("null")
        assert result == {"format": "null", "msg": "(undefined)"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes undefined')
    async def test_0009(self):
        result = await _encode("undefined")
        assert result == {"format": "undefined", "msg": "(undefined)"}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes string')
    async def test_0010(self):
        result = await _encode("'1234567890'", max_length=6)
        assert result == {"format": "string[10]", "msg": "123456..."}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes Map')
    async def test_0011(self):
        result = await _encode("""(function(){
            var m = new Map();
            m.set("a",1);
            m.set("b",2);
            return m;
        })()""")
        assert result["format"] == "map"
        assert json.loads(result["msg"]) == {"__enc__": True, "type": "map", "data": {"a": 1, "b": 2}, "length": 2}

    @pytest.mark.asyncio
    @pytest.mark.it('encodes Set')
    async def test_0012(self):
        result = await _encode("""(function(){
            var m = new Set();
            m.add("a");
            m.add("b");
            return m;
        })()""")
        assert result["format"] == "set[2]"
        assert json.loads(result["msg"]) == {"__enc__": True, "type": "set", "data": ["a", "b"], "length": 2}


@pytest.mark.describe('@node-red/util/util')
@pytest.mark.describe('encodeObject')
@pytest.mark.describe('encode object')
class TestEncodeObjectOfObject:

    @pytest.mark.asyncio
    @pytest.mark.it('object')
    async def test_0001(self):
        result = await _encode("{'foo':'bar'}")
        assert result["format"] == "Object"
        assert json.loads(result["msg"])["foo"] == 'bar'

    @pytest.mark.asyncio
    @pytest.mark.it('object whose name includes error')
    async def test_0002(self):
        result = await _encode("""(function(){
            function MyErrorObj(){
                this.name = 'my error obj';
                this.message = 'my error message';
            }
            return new MyErrorObj();
        })()""")
        assert result["format"] == "MyErrorObj"
        assert json.loads(result["msg"]) == {"name": "my error obj", "message": "my error message"}

    @pytest.mark.asyncio
    @pytest.mark.it('object with undefined property')
    async def test_0003(self):
        result = await _encode("{a:1,b:undefined,c:3}")
        assert result["format"] == "Object"
        encoded = json.loads(result["msg"])
        assert encoded["a"] == 1
        assert encoded["c"] == 3
        assert encoded["b"] == {"__enc__": True, "type": "undefined"}

    @pytest.mark.asyncio
    @pytest.mark.it('object with no prototype builtins')
    async def test_0004(self):
        result = await _encode("""(function(){
            var payload = new Object(null);
            payload.c = 3;
            return {b:payload};
        })()""")
        assert result["format"] == "Object"
        assert json.loads(result["msg"])["b"]["c"] == 3

    @pytest.mark.asyncio
    @pytest.mark.it('object with overriden hasOwnProperty')
    async def test_0005(self):
        result = await _encode("{b:{hasOwnProperty:null}}")
        assert result["format"] == "Object"
        encoded = json.loads(result["msg"])
        assert "hasOwnProperty" in encoded["b"]

    @pytest.mark.asyncio
    @pytest.mark.it('object with Map property')
    async def test_0006(self):
        result = await _encode("""(function(){
            var m = new Map();
            m.set("a",1);
            m.set("b",2);
            return {"aMap":m};
        })()""")
        assert result["format"] == "Object"
        assert json.loads(result["msg"])["aMap"] == {
            "__enc__": True, "type": "map", "data": {"a": 1, "b": 2}, "length": 2,
        }

    @pytest.mark.asyncio
    @pytest.mark.it('object with Set property')
    async def test_0007(self):
        result = await _encode("""(function(){
            var m = new Set();
            m.add("a");
            m.add("b");
            return {"aSet":m};
        })()""")
        assert result["format"] == "Object"
        assert json.loads(result["msg"])["aSet"] == {
            "__enc__": True, "type": "set", "data": ["a", "b"], "length": 2,
        }

    @pytest.mark.asyncio
    @pytest.mark.it('constructor of IncomingMessage')
    async def test_0008(self):
        result = await _encode("(function(){ function IncomingMessage(){}; return new IncomingMessage(); })()")
        assert result["format"] == "Object"
        assert json.loads(result["msg"]) == {}

    @pytest.mark.asyncio
    @pytest.mark.it('_req key in msg')
    async def test_0009(self):
        result = await _encode("{'_req':123}")
        assert result["format"] == "Object"
        encoded = json.loads(result["msg"])
        assert encoded["_req"]["__enc__"] is True
        assert encoded["_req"]["type"] == 'internal'

    @pytest.mark.asyncio
    @pytest.mark.it('_res key in msg')
    async def test_0010(self):
        result = await _encode("{'_res':123}")
        assert result["format"] == "Object"
        encoded = json.loads(result["msg"])
        assert encoded["_res"]["__enc__"] is True
        assert encoded["_res"]["type"] == 'internal'

    @pytest.mark.asyncio
    @pytest.mark.it('array of error')
    async def test_0011(self):
        result = await _encode("[new Error('encode error')]")
        assert result["format"] == "array[1]"
        assert json.loads(result["msg"])[0] == 'Error: encode error'

    @pytest.mark.asyncio
    @pytest.mark.it('long array in msg')
    async def test_0012(self):
        # Like upstream, this only checks the encoding markers: `maxLength` also truncates the
        # `type` field itself, so `type` is not part of the contract here.
        result = await _encode("{array:[1,2,3,4]}", max_length=2)
        assert result["format"] == "Object"
        encoded = json.loads(result["msg"])["array"]
        assert encoded["__enc__"] is True
        assert encoded["data"] == [1, 2]
        assert encoded["length"] == 4

    @pytest.mark.asyncio
    @pytest.mark.it('array of string')
    async def test_0013(self):
        result = await _encode("['abcde','12345']", max_length=3)
        assert result["format"] == "array[2]"
        assert json.loads(result["msg"]) == ['abc...', '123...']

    @pytest.mark.asyncio
    @pytest.mark.it('array containing undefined')
    async def test_0014(self):
        result = await _encode("[1,undefined,3]")
        assert result["format"] == "array[3]"
        encoded = json.loads(result["msg"])
        assert encoded[0] == 1
        assert encoded[2] == 3
        assert encoded[1] == {"__enc__": True, "type": "undefined"}

    @pytest.mark.asyncio
    @pytest.mark.it('array of function')
    async def test_0015(self):
        result = await _encode("[function(){}]")
        assert result["format"] == "array[1]"
        assert json.loads(result["msg"])[0] == {"__enc__": True, "type": "function"}

    @pytest.mark.asyncio
    @pytest.mark.it('array of number')
    async def test_0016(self):
        result = await _encode("[1,2,3]", max_length=2)
        assert result["format"] == "array[3]"
        encoded = json.loads(result["msg"])
        assert encoded["__enc__"] is True
        assert encoded["data"] == [1, 2]
        assert encoded["length"] == 3

    @pytest.mark.asyncio
    @pytest.mark.it('array of special number')
    async def test_0017(self):
        result = await _encode("[NaN,Infinity,-Infinity]")
        assert result["format"] == "array[3]"
        encoded = json.loads(result["msg"])
        assert encoded[0] == {"__enc__": True, "type": "number", "data": "NaN"}
        assert encoded[1]["data"] == 'Infinity'
        assert encoded[2]["data"] == '-Infinity'

    @pytest.mark.asyncio
    @pytest.mark.it('constructor of Buffer in msg')
    async def test_0018(self):
        result = await _encode("{buffer: new Uint8Array([1,2,3,4])}", max_length=2)
        assert result["format"] == "Object"
        encoded = json.loads(result["msg"])["buffer"]
        assert encoded["__enc__"] is True
        assert encoded["length"] == 4
        assert encoded["data"] == [1, 2]

    @pytest.mark.asyncio
    @pytest.mark.it('constructor of ServerResponse')
    async def test_0019(self):
        result = await _encode("(function(){ function ServerResponse(){}; return new ServerResponse(); })()")
        assert result["format"] == "Object"
        assert json.loads(result["msg"]) == '[internal]'

    @pytest.mark.asyncio
    @pytest.mark.it('constructor of Socket in msg')
    async def test_0020(self):
        result = await _encode("(function(){ function Socket(){}; return { socket: new Socket() }; })()")
        assert result["format"] == "Object"
        assert json.loads(result["msg"])["socket"] == '[internal]'

    @pytest.mark.asyncio
    @pytest.mark.it('object which fails to serialise')
    async def test_0021(self):
        result = await _encode("""{
            obj:{
                cantserialise:{
                    message:'this will not be displayed',
                    toJSON: function(val) {
                        throw 'this exception should have been caught';
                    },
                },
                canserialise:{
                    message:'this should be displayed',
                }
            },
        }""")
        assert result["format"] == "error"
        assert 'cantserialise' in result["msg"]
        assert 'this exception should have been caught' in result["msg"]
        assert 'canserialise' in result["msg"]

    @pytest.mark.asyncio
    @pytest.mark.it('object which fails to serialise - different error type')
    async def test_0022(self):
        result = await _encode("""{
            obj:{
                cantserialise:{
                    message:'this will not be displayed',
                    toJSON: function(val) {
                        throw new Error('this exception should have been caught');
                    },
                },
                canserialise:{
                    message:'this should be displayed',
                }
            },
        }""")
        assert result["format"] == "error"
        assert 'cantserialise' in result["msg"]
        assert 'this exception should have been caught' in result["msg"]
        assert 'canserialise' in result["msg"]

    @pytest.mark.asyncio
    @pytest.mark.it('very large object which fails to serialise should be truncated')
    async def test_0023(self):
        result = await _encode("""(function(){
            var big = '';
            for (var i = 0; i < 1000; i++) {
                big += 'some more string ';
            }
            return {
                obj:{
                    big: big,
                    cantserialise:{
                        message:'this will not be displayed',
                        toJSON: function(val) {
                            throw new Error('this exception should have been caught');
                        },
                    },
                    canserialise:{
                        message:'this should be displayed',
                    }
                },
            };
        })()""")
        assert result["format"] == "error"
        assert len(json.loads(result["msg"])["message"]) <= 1000

    @pytest.mark.asyncio
    @pytest.mark.it('test bad toString')
    async def test_0024(self):
        result = await _encode("""(function(){
            var __v = { mystrangeobj: "hello" };
            __v.toString = function(){
                throw new Error('Exception in toString - should have been caught');
            };
            __v.constructor = { name: "strangeobj" };
            return __v;
        })()""")
        assert '[Type not printable]' in result["msg"]

    @pytest.mark.asyncio
    @pytest.mark.it('test bad object constructor')
    async def test_0025(self):
        # Upstream only checks that a throwing `constructor` getter does not escape encodeObject.
        result = await _encode("""(function(){
            var __v = {
                mystrangeobj: "hello",
                constructor: {
                    get name(){
                        throw new Error('Exception in constructor name');
                    }
                }
            };
            return __v;
        })()""")
        assert result["format"]

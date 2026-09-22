// Prelude script for every `function` node
//
// `RED.util` is a port of Node-RED's `@node-red/util/lib/util.js`. Node-RED spreads that module
// into the function node sandbox (`RED: { util: { ...RED.util, getSetting: ... } }` in
// `nodes/core/function/10-function.js`), which is what this file reproduces.
//
// Two deliberate differences from the Node.js original, both forced by the embedded sandbox:
//
//  * There is no Node.js `Buffer`. Binary values cross the Rust <-> JavaScript boundary as
//    `ArrayBuffer`/`Uint8Array` (see `Variant::Bytes` in `model/variant/js_support.rs`), so the
//    byte helpers below work on that type: `ensureBuffer` yields a `Uint8Array` and
//    `ensureString` decodes one as UTF-8.
//  * JSONata is implemented by the Rust runtime (`runtime/jsonata`) and is not exposed to
//    JavaScript, so `prepareJSONataExpression`/`evaluateJSONataExpression` throw instead of
//    returning a plausible-looking value.
const RED = (function () {
    const hasOwnProperty = Object.prototype.hasOwnProperty;

    // ---------------------------------------------------------------------------------------
    // Byte helpers - the sandbox's stand-in for Node's `Buffer`
    // ---------------------------------------------------------------------------------------

    /// `Buffer.isBuffer(o)`
    function isBytes(value) {
        return value instanceof ArrayBuffer || ArrayBuffer.isView(value);
    }

    /// View any binary value as a `Uint8Array` without copying.
    function toU8(value) {
        if (value instanceof Uint8Array) {
            return value;
        }
        if (value instanceof ArrayBuffer) {
            return new Uint8Array(value);
        }
        return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
    }

    /// `Buffer#toString()`: decode UTF-8, replacing malformed sequences with U+FFFD.
    function utf8Decode(bytes) {
        var out = '';
        var i = 0;
        while (i < bytes.length) {
            var b0 = bytes[i];
            var cp;
            var extra;
            if (b0 < 0x80) {
                cp = b0;
                extra = 0;
            } else if ((b0 & 0xe0) === 0xc0) {
                cp = b0 & 0x1f;
                extra = 1;
            } else if ((b0 & 0xf0) === 0xe0) {
                cp = b0 & 0x0f;
                extra = 2;
            } else if ((b0 & 0xf8) === 0xf0) {
                cp = b0 & 0x07;
                extra = 3;
            } else {
                out += '\ufffd';
                i += 1;
                continue;
            }
            if (i + extra >= bytes.length) {
                out += '\ufffd';
                i += 1;
                continue;
            }
            var ok = true;
            for (var j = 1; j <= extra; j++) {
                var b = bytes[i + j];
                if ((b & 0xc0) !== 0x80) {
                    ok = false;
                    break;
                }
                cp = (cp << 6) | (b & 0x3f);
            }
            if (!ok) {
                out += '\ufffd';
                i += 1;
                continue;
            }
            i += extra + 1;
            if (cp > 0x10ffff) {
                out += '\ufffd';
            } else if (cp > 0xffff) {
                cp -= 0x10000;
                out += String.fromCharCode(0xd800 + (cp >> 10), 0xdc00 + (cp & 0x3ff));
            } else {
                out += String.fromCharCode(cp);
            }
        }
        return out;
    }

    /// `Buffer.from(string)`: encode UTF-8.
    function utf8Encode(str) {
        var out = [];
        for (var i = 0; i < str.length; i++) {
            var cp = str.charCodeAt(i);
            if (cp >= 0xd800 && cp <= 0xdbff && i + 1 < str.length) {
                var low = str.charCodeAt(i + 1);
                if (low >= 0xdc00 && low <= 0xdfff) {
                    cp = ((cp - 0xd800) << 10) + (low - 0xdc00) + 0x10000;
                    i++;
                }
            }
            if (cp < 0x80) {
                out.push(cp);
            } else if (cp < 0x800) {
                out.push(0xc0 | (cp >> 6), 0x80 | (cp & 0x3f));
            } else if (cp < 0x10000) {
                out.push(0xe0 | (cp >> 12), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
            } else {
                out.push(0xf0 | (cp >> 18), 0x80 | ((cp >> 12) & 0x3f), 0x80 | ((cp >> 6) & 0x3f), 0x80 | (cp & 0x3f));
            }
        }
        return new Uint8Array(out);
    }

    /// `Buffer#toString('hex')`
    function hexEncode(bytes) {
        var out = '';
        for (var i = 0; i < bytes.length; i++) {
            out += (bytes[i] < 16 ? '0' : '') + bytes[i].toString(16);
        }
        return out;
    }

    /// A small `util.inspect` for the `encodeObject` error path (Node's own is not portable).
    function inspect(value, depth, seen) {
        depth = depth || 0;
        seen = seen || [];
        if (value === null) {
            return 'null';
        }
        var type = typeof value;
        if (type === 'undefined') {
            return 'undefined';
        }
        if (type === 'string') {
            return "'" + value + "'";
        }
        if (type === 'number' || type === 'boolean' || type === 'bigint') {
            return String(value);
        }
        if (type === 'symbol') {
            return value.toString();
        }
        if (type === 'function') {
            return value.name ? '[Function: ' + value.name + ']' : '[Function]';
        }
        if (isBytes(value)) {
            return '<Binary ' + hexEncode(toU8(value)) + '>';
        }
        if (seen.indexOf(value) !== -1) {
            return '[Circular]';
        }
        if (depth > 4) {
            return Array.isArray(value) ? '[Array]' : '[Object]';
        }
        if (value instanceof Date) {
            return value.toISOString();
        }
        if (value instanceof RegExp) {
            return value.toString();
        }
        seen = seen.concat([value]);
        if (Array.isArray(value)) {
            var items = [];
            for (var i = 0; i < value.length; i++) {
                try {
                    items.push(inspect(value[i], depth + 1, seen));
                } catch (e) {
                    items.push('[Thrown]');
                }
            }
            return '[ ' + items.join(', ') + ' ]';
        }
        var ctor = '';
        try {
            if (value.constructor && value.constructor.name) {
                ctor = value.constructor.name + ' ';
            }
        } catch (e) {
            // A throwing `constructor` getter must not break the error path
        }
        var parts = [];
        var keys = [];
        try {
            keys = Object.keys(value);
        } catch (e) {
            return ctor + '{ ... }';
        }
        for (var k = 0; k < keys.length; k++) {
            var key = keys[k];
            var desc = Object.getOwnPropertyDescriptor(value, key);
            if (desc && (desc.get || desc.set) && !hasOwnProperty.call(desc, 'value')) {
                parts.push(key + ': [Getter]');
                continue;
            }
            try {
                parts.push(key + ': ' + inspect(desc.value, depth + 1, seen));
            } catch (e) {
                parts.push(key + ': [Thrown]');
            }
        }
        return ctor + '{ ' + parts.join(', ') + ' }';
    }

    /// `json-stringify-safe`: `JSON.stringify` that renders cycles instead of throwing.
    function safeJSONStringify(obj, replacer) {
        var stack = [];
        var keys = [];

        function cycleReplacer(key, value) {
            if (stack[0] === value) {
                return '[Circular ~]';
            }
            return '[Circular ~.' + keys.slice(0, stack.indexOf(value)).join('.') + ']';
        }

        function serializer(key, value) {
            if (stack.length > 0) {
                var thisPos = stack.indexOf(this);
                if (thisPos !== -1) {
                    stack.splice(thisPos + 1);
                    keys.splice(thisPos, Infinity, key);
                } else {
                    stack.push(this);
                    keys.push(key);
                }
                if (stack.indexOf(value) !== -1) {
                    value = cycleReplacer.call(this, key, value);
                }
            } else {
                stack.push(value);
            }
            return replacer == null ? value : replacer.call(this, key, value);
        }

        return JSON.stringify(obj, serializer);
    }

    /// Safely returns the object constructor name.
    function constructorName(obj) {
        return obj && obj.constructor ? obj.constructor.name : '';
    }

    function createError(code, message) {
        var e = new Error(message);
        e.code = code;
        return e;
    }

    // ---------------------------------------------------------------------------------------
    // The util module
    // ---------------------------------------------------------------------------------------

    /**
     * Generates a pseudo-unique-random id.
     */
    function generateId() {
        var bytes = [];
        for (var i = 0; i < 8; i++) {
            bytes.push(Math.round(0xff * Math.random()).toString(16).padStart(2, '0'));
        }
        return bytes.join("");
    }

    /**
     * Converts the provided argument to a String, using type-dependent methods.
     */
    function ensureString(o) {
        if (isBytes(o)) {
            return utf8Decode(toU8(o));
        } else if (typeof o === "object") {
            return JSON.stringify(o);
        } else if (typeof o === "string") {
            return o;
        }
        return "" + o;
    }

    /**
     * Converts the provided argument to bytes, using type-dependent methods.
     *
     * Node-RED returns a `Buffer`; the sandbox has no `Buffer`, so this returns the binary type it
     * does have: a `Uint8Array` (which the Rust side reads back as `Variant::Bytes`).
     */
    function ensureBuffer(o) {
        if (isBytes(o)) {
            return toU8(o);
        } else if (typeof o === "object") {
            o = JSON.stringify(o);
        } else if (typeof o !== "string") {
            o = "" + o;
        }
        return utf8Encode(o);
    }

    /**
     * Safely clones a message object. This handles msg.req/msg.res objects that must not be cloned.
     */
    function cloneMessage(msg) {
        if (typeof msg !== "undefined" && msg !== null) {
            // Temporary fix for #97
            // TODO: remove this http-node-specific fix somehow
            var req = msg.req;
            var res = msg.res;
            delete msg.req;
            delete msg.res;
            var m = __cloneDeep(msg);
            if (req) {
                m.req = req;
                msg.req = req;
            }
            if (res) {
                m.res = res;
                msg.res = res;
            }
            return m;
        }
        return msg;
    }

    function __cloneDeep(value, map) {
        map = map || new WeakMap();
        if (value === null || typeof value !== 'object') {
            return value;
        }
        if (map.has(value)) {
            return map.get(value);
        }
        if (value instanceof Date) {
            return new Date(value.getTime());
        }
        if (value instanceof RegExp) {
            return new RegExp(value);
        }
        if (value instanceof ArrayBuffer) {
            return value.slice(0);
        }
        if (ArrayBuffer.isView(value)) {
            var copy = new Uint8Array(value.byteLength);
            copy.set(new Uint8Array(value.buffer, value.byteOffset, value.byteLength));
            return copy;
        }
        if (value instanceof Map) {
            var mapCopy = new Map();
            map.set(value, mapCopy);
            value.forEach(function (v, k) {
                mapCopy.set(__cloneDeep(k, map), __cloneDeep(v, map));
            });
            return mapCopy;
        }
        if (value instanceof Set) {
            var setCopy = new Set();
            map.set(value, setCopy);
            value.forEach(function (v) {
                setCopy.add(__cloneDeep(v, map));
            });
            return setCopy;
        }
        if (Array.isArray(value)) {
            var clonedArray = value.map(function (item) {
                return __cloneDeep(item, map);
            });
            map.set(value, clonedArray);
            return clonedArray;
        }

        var clonedObj = {};
        map.set(value, clonedObj);
        for (var key in value) {
            if (hasOwnProperty.call(value, key)) {
                clonedObj[key] = __cloneDeep(value[key], map);
            }
        }
        return clonedObj;
    }

    /**
     * Compares two objects, handling various JavaScript types.
     */
    function compareObjects(obj1, obj2) {
        var i;
        if (obj1 === obj2) {
            return true;
        }
        if (obj1 == null || obj2 == null) {
            return false;
        }

        var isArray1 = Array.isArray(obj1);
        var isArray2 = Array.isArray(obj2);
        if (isArray1 != isArray2) {
            return false;
        }
        if (isArray1 && isArray2) {
            if (obj1.length !== obj2.length) {
                return false;
            }
            for (i = 0; i < obj1.length; i++) {
                if (!compareObjects(obj1[i], obj2[i])) {
                    return false;
                }
            }
            return true;
        }

        var isBuffer1 = isBytes(obj1);
        var isBuffer2 = isBytes(obj2);
        if (isBuffer1 != isBuffer2) {
            return false;
        }
        if (isBuffer1 && isBuffer2) {
            var bytes1 = toU8(obj1);
            var bytes2 = toU8(obj2);
            if (bytes1.length !== bytes2.length) {
                return false;
            }
            for (i = 0; i < bytes1.length; i++) {
                if (bytes1[i] !== bytes2[i]) {
                    return false;
                }
            }
            return true;
        }

        if (typeof obj1 !== 'object' || typeof obj2 !== 'object') {
            return false;
        }
        var keys1 = Object.keys(obj1);
        var keys2 = Object.keys(obj2);
        if (keys1.length != keys2.length) {
            return false;
        }
        for (var k in obj1) {
            if (hasOwnProperty.call(obj1, k)) {
                if (!compareObjects(obj1[k], obj2[k])) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Parses a property expression, such as `msg.foo.bar[3]` to validate it and convert it to a
     * canonical version expressed as an Array of property names.
     *
     * For example, `a["b"].c` returns `['a','b','c']`
     *
     * If `msg` is provided, any internal cross-references will be evaluated against that object.
     * Otherwise, it will return a nested set of properties.
     */
    function normalisePropertyExpression(str, msg, toString) {
        // This must be kept in sync with validatePropertyExpression
        // in editor/js/ui/utils.js

        var length = str.length;
        if (length === 0) {
            throw createError("INVALID_EXPR", "Invalid property expression: zero-length");
        }
        var parts = [];
        var start = 0;
        var inString = false;
        var inBox = false;
        var quoteChar;
        var v;
        for (var i = 0; i < length; i++) {
            var c = str[i];
            if (!inString) {
                if (c === "'" || c === '"') {
                    if (i != start) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected " + c + " at position " + i);
                    }
                    inString = true;
                    quoteChar = c;
                    start = i + 1;
                } else if (c === '.') {
                    if (i === 0) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected . at position 0");
                    }
                    if (start != i) {
                        v = str.substring(start, i);
                        if (/^\d+$/.test(v)) {
                            parts.push(parseInt(v));
                        } else {
                            parts.push(v);
                        }
                    }
                    if (i === length - 1) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unterminated expression");
                    }
                    // Next char is first char of an identifier: a-z 0-9 $ _
                    if (!/[a-z0-9\$\_]/i.test(str[i + 1])) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected " + str[i + 1] + " at position " + (i + 1));
                    }
                    start = i + 1;
                } else if (c === '[') {
                    if (i === 0) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected " + c + " at position " + i);
                    }
                    if (start != i) {
                        parts.push(str.substring(start, i));
                    }
                    if (i === length - 1) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unterminated expression");
                    }
                    // Start of a new expression. If it starts with msg it is a nested expression
                    // Need to scan ahead to find the closing bracket
                    if (/^msg[.\[]/.test(str.substring(i + 1))) {
                        var depth = 1;
                        var inLocalString = false;
                        var localStringQuote;
                        for (var j = i + 1; j < length; j++) {
                            if (/["']/.test(str[j])) {
                                if (inLocalString) {
                                    if (str[j] === localStringQuote) {
                                        inLocalString = false;
                                    }
                                } else {
                                    inLocalString = true;
                                    localStringQuote = str[j];
                                }
                            }
                            if (str[j] === '[') {
                                depth++;
                            } else if (str[j] === ']') {
                                depth--;
                            }
                            if (depth === 0) {
                                try {
                                    if (msg) {
                                        var crossRefProp = getMessageProperty(msg, str.substring(i + 1, j));
                                        if (crossRefProp === undefined) {
                                            throw createError("INVALID_EXPR", "Invalid expression: undefined reference at position " + (i + 1) + " : " + str.substring(i + 1, j));
                                        }
                                        parts.push(crossRefProp);
                                    } else {
                                        parts.push(normalisePropertyExpression(str.substring(i + 1, j), msg));
                                    }
                                    inBox = false;
                                    i = j;
                                    start = j + 1;
                                    break;
                                } catch (err) {
                                    throw createError("INVALID_EXPR", "Invalid expression started at position " + (i + 1));
                                }
                            }
                        }
                        if (depth > 0) {
                            throw createError("INVALID_EXPR", "Invalid property expression: unmatched '[' at position " + i);
                        }
                        continue;
                    } else if (!/["'\d]/.test(str[i + 1])) {
                        // Next char is either a quote or a number
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected " + str[i + 1] + " at position " + (i + 1));
                    }
                    start = i + 1;
                    inBox = true;
                } else if (c === ']') {
                    if (!inBox) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected " + c + " at position " + i);
                    }
                    if (start != i) {
                        v = str.substring(start, i);
                        if (/^\d+$/.test(v)) {
                            parts.push(parseInt(v));
                        } else {
                            throw createError("INVALID_EXPR", "Invalid property expression: unexpected array expression at position " + start);
                        }
                    }
                    start = i + 1;
                    inBox = false;
                } else if (c === ' ') {
                    throw createError("INVALID_EXPR", "Invalid property expression: unexpected ' ' at position " + i);
                }
            } else {
                if (c === quoteChar) {
                    if (i - start === 0) {
                        throw createError("INVALID_EXPR", "Invalid property expression: zero-length string at position " + start);
                    }
                    parts.push(str.substring(start, i));
                    // If inBox, next char must be a ]. Otherwise it may be [ or .
                    if (inBox && !/\]/.test(str[i + 1])) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected array expression at position " + start);
                    } else if (!inBox && i + 1 !== length && !/[\[\.]/.test(str[i + 1])) {
                        throw createError("INVALID_EXPR", "Invalid property expression: unexpected " + str[i + 1] + " expression at position " + (i + 1));
                    }
                    start = i + 1;
                    inString = false;
                }
            }
        }
        if (inBox || inString) {
            throw createError("INVALID_EXPR", "Invalid property expression: unterminated expression");
        }
        if (start < length) {
            parts.push(str.substring(start));
        }

        if (toString) {
            var result = parts.shift();
            while (parts.length > 0) {
                var p = parts.shift();
                if (typeof p === 'string') {
                    if (/"/.test(p)) {
                        p = "'" + p + "'";
                    } else {
                        p = '"' + p + '"';
                    }
                }
                result = result + "[" + p + "]";
            }
            return result;
        }

        return parts;
    }

    /**
     * Gets a property of a message object.
     *
     * Unlike `getObjectProperty`, this function will strip `msg.` from the front of the property
     * expression if present.
     */
    function getMessageProperty(msg, expr) {
        if (expr.indexOf('msg.') === 0) {
            expr = expr.substring(4);
        }
        return getObjectProperty(msg, expr);
    }

    /**
     * Gets a property of an object.
     *
     * - `pet.type` will return `"cat"`.
     * - `pet.name` will return `undefined`
     * - `car` will return `undefined`
     * - `car.type` will throw an Error (as `car` does not exist)
     */
    function getObjectProperty(msg, expr) {
        var result = null;
        var msgPropParts = normalisePropertyExpression(expr, msg);
        msgPropParts.reduce(function (obj, key) {
            result = (typeof obj[key] !== "undefined" ? obj[key] : undefined);
            return result;
        }, msg);
        return result;
    }

    /**
     * Sets a property of a message object.
     *
     * Unlike `setObjectProperty`, this function will strip `msg.` from the front of the property
     * expression if present.
     */
    function setMessageProperty(msg, prop, value, createMissing) {
        if (prop.indexOf('msg.') === 0) {
            prop = prop.substring(4);
        }
        return setObjectProperty(msg, prop, value, createMissing);
    }

    /**
     * Sets a property of an object.
     */
    function setObjectProperty(msg, prop, value, createMissing) {
        if (typeof createMissing === 'undefined') {
            createMissing = (typeof value !== 'undefined');
        }
        var msgPropParts = normalisePropertyExpression(prop, msg);
        var length = msgPropParts.length;
        var obj = msg;
        var key;
        for (var i = 0; i < length - 1; i++) {
            key = msgPropParts[i];
            if (typeof key === 'string' || (typeof key === 'number' && !Array.isArray(obj))) {
                if (hasOwnProperty.call(obj, key)) {
                    if (length > 1 && ((typeof obj[key] !== "object" && typeof obj[key] !== "function") || obj[key] === null)) {
                        // Break out early as we cannot create a property beneath
                        // this type of value
                        return false;
                    }
                    obj = obj[key];
                } else if (createMissing) {
                    if (typeof msgPropParts[i + 1] === 'string') {
                        obj[key] = {};
                    } else {
                        obj[key] = [];
                    }
                    obj = obj[key];
                } else {
                    return false;
                }
            } else if (typeof key === 'number') {
                // obj is an array
                if (obj[key] === undefined) {
                    if (createMissing) {
                        if (typeof msgPropParts[i + 1] === 'string') {
                            obj[key] = {};
                        } else {
                            obj[key] = [];
                        }
                        obj = obj[key];
                    } else {
                        return false;
                    }
                } else {
                    obj = obj[key];
                }
            }
        }
        key = msgPropParts[length - 1];
        if (typeof value === "undefined") {
            if (typeof key === 'number' && Array.isArray(obj)) {
                obj.splice(key, 1);
            } else {
                delete obj[key];
            }
        } else {
            if (typeof obj === "object" && obj !== null) {
                obj[key] = value;
            } else {
                // Cannot set a property of a non-object/array
                return false;
            }
        }
        return true;
    }

    /**
     * Get value of an environment variable.
     *
     * Node-RED's sandbox rebinds this so that the `node` argument is the function node itself and
     * the flow cannot be overridden; the group/flow/engine lookups all live behind the sandbox
     * `env` object, which - like Node-RED's `Flow#getSetting` - looks the name up verbatim.
     * `${}` interpolation is `evaluateEnvProperty`'s job, not this function's.
     */
    function getSetting(node, name) {
        if (node) {
            if (name === "NR_NODE_NAME") {
                return node.name;
            }
            if (name === "NR_NODE_ID") {
                return node.id;
            }
            if (name === "NR_NODE_PATH") {
                return node._path;
            }
        }
        return env.get(name);
    }

    /**
     * Checks if a String contains any Environment Variable specifiers and returns it with their
     * values substituted in place.
     */
    function evaluateEnvProperty(value, node) {
        var result;
        if (/^\${[^}]+}$/.test(value)) {
            // ${ENV_VAR}
            var name = value.substring(2, value.length - 1);
            result = getSetting(node, name);
        } else if (!/\$\{\S+}/.test(value)) {
            // ENV_VAR
            result = getSetting(node, value);
        } else {
            // FOO${ENV_VAR}BAR
            return value.replace(/\${([^}]+)}/g, function (match, name) {
                var val = getSetting(node, name);
                return (val === undefined) ? "" : val;
            });
        }
        return (result === undefined) ? "" : result;
    }

    /**
     * Parses a context property string, as generated by the TypedInput, to extract the store name
     * if present.
     *
     * For example, `#:(file)::foo` results in `{ store: "file", key: "foo" }`.
     */
    function parseContextStore(key) {
        var parts = {};
        var m = /^#:\((\S+?)\)::(.*)$/.exec(key);
        if (m) {
            parts.store = m[1];
            parts.key = m[2];
        } else {
            parts.key = key;
        }
        return parts;
    }

    /**
     * Evaluates a property value according to its type.
     */
    function evaluateNodeProperty(value, type, node, msg, callback) {
        var result = value;
        if (type === 'str') {
            result = "" + value;
        } else if (type === 'num') {
            result = Number(value);
        } else if (type === 'json') {
            result = JSON.parse(value);
        } else if (type === 're') {
            result = new RegExp(value);
        } else if (type === 'date') {
            if (!value) {
                result = Date.now();
            } else if (value === 'object') {
                result = new Date();
            } else if (value === 'iso') {
                result = (new Date()).toISOString();
            } else {
                // Node-RED formats with moment (`.format(value)`), which the sandbox does not have
                throw createError("NOT_SUPPORTED", "evaluateNodeProperty('date') cannot format a date: the EdgeLinkd function node sandbox has no moment");
            }
        } else if (type === 'bin') {
            var data = JSON.parse(value);
            if (Array.isArray(data) || (typeof (data) === "string")) {
                result = (typeof data === "string") ? utf8Encode(data) : new Uint8Array(data);
            } else {
                throw createError("INVALID_BUFFER_DATA", "Not string or array");
            }
        } else if (type === 'msg' && msg) {
            try {
                result = getMessageProperty(msg, value);
            } catch (err) {
                if (callback) {
                    callback(err);
                } else {
                    throw err;
                }
                return;
            }
        } else if ((type === 'flow' || type === 'global') && node) {
            var contextKey = parseContextStore(value);
            if (/\[msg/.test(contextKey.key)) {
                // The key has a nest msg. reference to evaluate first
                contextKey.key = normalisePropertyExpression(contextKey.key, msg, true);
            }
            result = node.context()[type].get(contextKey.key, contextKey.store, callback);
            if (callback) {
                return;
            }
        } else if (type === 'bool') {
            result = /^true$/i.test(value);
        } else if (type === 'jsonata') {
            var expr = prepareJSONataExpression(value, node);
            result = evaluateJSONataExpression(expr, msg, callback);
            if (callback) {
                return;
            }
        } else if (type === 'env') {
            result = evaluateEnvProperty(value, node);
        }
        if (callback) {
            callback(null, result);
        } else {
            return result;
        }
    }

    /**
     * Prepares a JSONata expression for evaluation.
     *
     * NOT SUPPORTED: Node-RED's JSONata lives in the `jsonata` npm package; EdgeLinkd evaluates
     * JSONata in Rust (`runtime/jsonata`) and does not expose the engine to JavaScript. Rather than
     * returning a plausible-looking value, this fails loudly.
     */
    function prepareJSONataExpression(value, node) {
        throw createError("NOT_SUPPORTED", "RED.util.prepareJSONataExpression is not supported: JSONata is implemented by the EdgeLinkd Rust runtime and is not available inside the function node sandbox");
    }

    /**
     * Evaluates a JSONata expression. See `prepareJSONataExpression` - not supported.
     */
    function evaluateJSONataExpression(expr, msg, callback) {
        throw createError("NOT_SUPPORTED", "RED.util.evaluateJSONataExpression is not supported: JSONata is implemented by the EdgeLinkd Rust runtime and is not available inside the function node sandbox");
    }

    /**
     * Normalise a node type name to camel case.
     *
     * For example: `a-random node type` will normalise to `aRandomNodeType`
     */
    function normaliseNodeTypeName(name) {
        var result = name.replace(/[^a-zA-Z0-9]/g, " ");
        result = result.trim();
        result = result.replace(/ +/g, " ");
        result = result.replace(/ ./g,
            function (s) {
                return s.charAt(1).toUpperCase();
            }
        );
        result = result.charAt(0).toLowerCase() + result.slice(1);
        return result;
    }

    /**
     * Encode an object to JSON without losing information about non-JSON types such as binary
     * values and Functions.
     *
     * *This function is closely tied to its reverse within the editor*
     */
    function encodeObject(msg, opts) {
        var debuglength = 1000;
        try {
            if (opts && hasOwnProperty.call(opts, 'maxLength')) {
                debuglength = opts.maxLength;
            }
            var msgType = typeof msg.msg;
            if (msg.msg instanceof Error) {
                msg.format = "error";
                var errorMsg = {};
                if (msg.msg.name) {
                    errorMsg.name = msg.msg.name;
                }
                if (hasOwnProperty.call(msg.msg, 'message')) {
                    errorMsg.message = msg.msg.message;
                } else {
                    errorMsg.message = msg.msg.toString();
                }
                msg.msg = JSON.stringify(errorMsg);
            } else if (isBytes(msg.msg)) {
                var rawBytes = toU8(msg.msg);
                msg.format = "buffer[" + rawBytes.length + "]";
                msg.msg = hexEncode(rawBytes);
                if (msg.msg.length > debuglength) {
                    msg.msg = msg.msg.substring(0, debuglength);
                }
            } else if (msg.msg && msgType === 'object') {
                try {
                    msg.format = constructorName(msg.msg) || "Object";
                    // Handle special case of msg.req/res objects from HTTP In node
                    if (msg.format === "IncomingMessage" || msg.format === "ServerResponse") {
                        msg.format = "Object";
                    }
                } catch (err) {
                    msg.format = "Object";
                }
                if (/error/i.test(msg.format)) {
                    msg.msg = JSON.stringify({
                        name: msg.msg.name,
                        message: msg.msg.message
                    });
                } else {
                    var isArray = Array.isArray(msg.msg);
                    var needsStringify = isArray;
                    if (isArray) {
                        msg.format = "array[" + msg.msg.length + "]";
                        if (msg.msg.length > debuglength) {
                            msg.msg = {
                                __enc__: true,
                                type: "array",
                                data: msg.msg.slice(0, debuglength),
                                length: msg.msg.length
                            };
                        }
                    } else if (constructorName(msg.msg) === "Set") {
                        msg.format = "set[" + msg.msg.size + "]";
                        msg.msg = {
                            __enc__: true,
                            type: "set",
                            data: Array.from(msg.msg).slice(0, debuglength),
                            length: msg.msg.size
                        };
                        needsStringify = true;
                    } else if (constructorName(msg.msg) === "Map") {
                        msg.format = "map";
                        msg.msg = {
                            __enc__: true,
                            type: "map",
                            data: Object.fromEntries(Array.from(msg.msg.entries()).slice(0, debuglength)),
                            length: msg.msg.size
                        };
                        needsStringify = true;
                    } else if (constructorName(msg.msg) === "RegExp") {
                        msg.format = 'regexp';
                        msg.msg = msg.msg.toString();
                    }
                    if (needsStringify || (msg.format === "Object")) {
                        msg.msg = safeJSONStringify(msg.msg, function (key, value) {
                            if (key === '_req' || key === '_res') {
                                value = {
                                    __enc__: true,
                                    type: "internal"
                                };
                            } else if (value instanceof Error) {
                                value = value.toString();
                            } else if (Array.isArray(value) && value.length > debuglength) {
                                value = {
                                    __enc__: true,
                                    type: "array",
                                    data: value.slice(0, debuglength),
                                    length: value.length
                                };
                            } else if (typeof value === 'string') {
                                if (value.length > debuglength) {
                                    value = value.substring(0, debuglength) + "...";
                                }
                            } else if (typeof value === 'function') {
                                value = {
                                    __enc__: true,
                                    type: "function"
                                };
                            } else if (typeof value === 'number') {
                                if (isNaN(value) || value === Infinity || value === -Infinity) {
                                    value = {
                                        __enc__: true,
                                        type: "number",
                                        data: value.toString()
                                    };
                                }
                            } else if (typeof value === 'bigint') {
                                value = {
                                    __enc__: true,
                                    type: 'bigint',
                                    data: value.toString()
                                };
                            } else if (isBytes(value)) {
                                // Node reaches this shape through `Buffer#toJSON()`, which emits
                                // `type`/`data` and leaves the replacer to add `__enc__`/`length`;
                                // the sandbox's bytes are a Uint8Array with no `toJSON`, so build
                                // the same object - in the same key order, so that the encoded
                                // string is byte-identical to Node-RED's.
                                var encodedBytes = Array.from(toU8(value));
                                var byteLength = encodedBytes.length;
                                if (byteLength > debuglength) {
                                    encodedBytes = encodedBytes.slice(0, debuglength);
                                }
                                value = {
                                    type: "Buffer",
                                    data: encodedBytes,
                                    __enc__: true,
                                    length: byteLength
                                };
                            } else if (value && value.constructor) {
                                if (constructorName(value) === "ServerResponse") {
                                    value = "[internal]";
                                } else if (constructorName(value) === "Socket") {
                                    value = "[internal]";
                                } else if (constructorName(value) === "Set") {
                                    value = {
                                        __enc__: true,
                                        type: "set",
                                        data: Array.from(value).slice(0, debuglength),
                                        length: value.size
                                    };
                                } else if (constructorName(value) === "Map") {
                                    value = {
                                        __enc__: true,
                                        type: "map",
                                        data: Object.fromEntries(Array.from(value.entries()).slice(0, debuglength)),
                                        length: value.size
                                    };
                                } else if (constructorName(value) === "RegExp") {
                                    value = {
                                        __enc__: true,
                                        type: "regexp",
                                        data: value.toString()
                                    };
                                }
                            } else if (value === undefined) {
                                value = {
                                    __enc__: true,
                                    type: "undefined"
                                };
                            }
                            return value;
                        });
                    } else {
                        try {
                            msg.msg = msg.msg.toString();
                        } catch (e) {
                            msg.msg = "[Type not printable]" + inspect(msg.msg, 0, []);
                        }
                    }
                }
            } else if (msgType === "function") {
                msg.format = "function";
                msg.msg = "[function]";
            } else if (msgType === "boolean") {
                msg.format = "boolean";
                msg.msg = msg.msg.toString();
            } else if (msgType === "number") {
                msg.format = "number";
                msg.msg = msg.msg.toString();
            } else if (msgType === "bigint") {
                msg.format = "bigint";
                msg.msg = {
                    __enc__: true,
                    type: 'bigint',
                    data: msg.msg.toString()
                };
            } else if (msg.msg === null || msgType === "undefined") {
                msg.format = (msg.msg === null) ? "null" : "undefined";
                msg.msg = "(undefined)";
            } else {
                msg.format = "string[" + msg.msg.length + "]";
                if (msg.msg.length > debuglength) {
                    msg.msg = msg.msg.substring(0, debuglength) + "...";
                }
            }
            return msg;
        } catch (e) {
            msg.format = "error";
            var encodeErrorMsg = {};
            if (e.name) {
                encodeErrorMsg.name = e.name;
            }
            if (hasOwnProperty.call(e, 'message')) {
                encodeErrorMsg.message = 'encodeObject Error: [' + e.message + '] Value: ' + inspect(msg.msg, 0, []);
            } else {
                encodeErrorMsg.message = 'encodeObject Error: [' + e.toString() + '] Value: ' + inspect(msg.msg, 0, []);
            }
            if (encodeErrorMsg.message.length > debuglength) {
                encodeErrorMsg.message = encodeErrorMsg.message.substring(0, debuglength);
            }
            msg.msg = JSON.stringify(encodeErrorMsg);
            return msg;
        }
    }

    return {
        util: {
            encodeObject: encodeObject,
            ensureString: ensureString,
            ensureBuffer: ensureBuffer,
            cloneMessage: cloneMessage,
            compareObjects: compareObjects,
            generateId: generateId,
            getMessageProperty: getMessageProperty,
            setMessageProperty: setMessageProperty,
            getObjectProperty: getObjectProperty,
            setObjectProperty: setObjectProperty,
            evaluateNodeProperty: evaluateNodeProperty,
            normalisePropertyExpression: normalisePropertyExpression,
            normaliseNodeTypeName: normaliseNodeTypeName,
            prepareJSONataExpression: prepareJSONataExpression,
            evaluateJSONataExpression: evaluateJSONataExpression,
            parseContextStore: parseContextStore,
            getSetting: getSetting
        }
    };
})();

# Python Flow Buffer Bridge

The Python flow-test bridge represents binary `Variant::Bytes` values using a reserved tagged object:

```python
{"$__el_type": "Buffer", "data": [0, 127, 255]}
```

`data` must be a list of integer bytes in the inclusive range 0 through 255. Booleans, floats, negative values, and values above 255 are rejected. Conversion is recursive, so tagged Buffers work inside message payloads, arrays, nested objects, and Python `ContextStore` values. Bridge output uses the same tag, preserving the distinction between bytes and ordinary integer arrays.

The runtime also recognizes Node-RED's serialized Buffer form (`{"type":"Buffer","data":[...]}`) when deserializing message values. Rust code should use `Variant::Bytes` directly.

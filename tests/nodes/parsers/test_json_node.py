import pytest
from tests import *

@pytest.mark.describe('JSON node')
class TestJsonNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should convert a valid json string to a javascript object')
    async def test_json_string_to_object(self):
        node = {"type": "json"}
        json_str = ' {"employees":[{"firstName":"John", "lastName":"Smith"}]}\r\n '
        injections = [{"payload": json_str, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "employees" in msgs[0]["payload"]
        assert msgs[0]["payload"]["employees"][0]["firstName"] == "John"
        assert msgs[0]["payload"]["employees"][0]["lastName"] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a buffer of a valid json string to a javascript object')
    async def test_buffer_json_string_to_object(self):
        node = {"type": "json", "action": "obj"}
        json_str = list(b' {"employees":[{"firstName":"John", "lastName":"Smith"}]}\r\n ')
        injections = [{"payload": json_str, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "employees" in msgs[0]["payload"]
        assert msgs[0]["payload"]["employees"][0]["firstName"] == "John"
        assert msgs[0]["payload"]["employees"][0]["lastName"] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a javascript object to a json string')
    async def test_object_to_json_string(self):
        node = {"type": "json"}
        obj = {"employees": [{"firstName": "John", "lastName": "Smith"}]}
        injections = [{"payload": obj}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == '{"employees":[{"firstName":"John","lastName":"Smith"}]}'

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a array to a json string')
    async def test_array_to_json_string(self):
        node = {"type": "json"}
        arr = [1, 2, 3]
        injections = [{"payload": arr}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == '[1,2,3]'

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a boolean to a json string')
    async def test_bool_to_json_string(self):
        node = {"type": "json"}
        injections = [{"payload": True}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == 'true'

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a json string to a boolean')
    async def test_json_string_to_bool(self):
        node = {"type": "json"}
        injections = [{"payload": "true"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] is True

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a number to a json string')
    async def test_number_to_json_string(self):
        node = {"type": "json"}
        injections = [{"payload": 2019}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == '2019'

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a json string to a number')
    async def test_json_string_to_number(self):
        node = {"type": "json"}
        injections = [{"payload": "1962"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == 1962

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse an invalid json string')
    async def test_invalid_json_string(self):
        node = {"type": "json"}
        injections = [{"payload": "foo", "topic": "bar"}]
        msg = await _catch_json_error(node, injections)
        assert msg["error"]["source"]["type"] == "json"
        assert msg["error"]["message"]
        if isinstance(injections[0].get("schema"), dict):
            assert msg["schemaError"]
            assert msg["schemaError"][0]["keyword"] == "type"
            assert msg["schemaError"][0]["dataPath"] == ".number"
            assert msg["schemaError"][0]["params"] == {"type": "number"}
            assert msg["schemaError"][0]["message"] == "should be number"
            assert "json.errors.schema-error" in msg["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.it('should pass straight through if no payload set')
    async def test_no_payload(self):
        node = {"type": "json"}
        injections = [{"topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "payload" not in msgs[0]

    @pytest.mark.asyncio
    @pytest.mark.it('should ensure the result is a json string')
    async def test_ensure_result_is_json_string(self):
        node = {"type": "json", "action": "str"}
        obj = {"employees": [{"firstName": "John", "lastName": "Smith"}]}
        injections = [
            {"payload": obj, "topic": "bar"},
            {"payload": '{"employees":[{"firstName":"John","lastName":"Smith"}]}', "topic": "bar"}
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
        assert msgs[0]["payload"] == '{"employees":[{"firstName":"John","lastName":"Smith"}]}'
        assert msgs[1]["payload"] == '{"employees":[{"firstName":"John","lastName":"Smith"}]}'

    @pytest.mark.asyncio
    @pytest.mark.it('should ensure the result is a JS Object')
    async def test_ensure_result_is_js_object(self):
        node = {"type": "json", "action": "obj"}
        obj = {"employees": [{"firstName": "John", "lastName": "Smith"}]}
        injections = [
            {"payload": obj, "topic": "bar"},
            {"payload": '{"employees":[{"firstName":"John","lastName":"Smith"}]}', "topic": "bar"}
        ]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
        for msg in msgs:
            assert msg["topic"] == "bar"
            assert "employees" in msg["payload"]
            assert msg["payload"]["employees"][0]["firstName"] == "John"
            assert msg["payload"]["employees"][0]["lastName"] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should handle any msg property - receive existing string')
    async def test_any_msg_property_string(self):
        node = {"type": "json", "property": "one.two"}
        json_str = '{"employees":[{"firstName":"John", "lastName":"Smith"}]}'
        injections = [{"payload": "", "one": {"two": json_str}, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "one" in msgs[0]
        assert "two" in msgs[0]["one"]
        assert "employees" in msgs[0]["one"]["two"]
        assert msgs[0]["one"]["two"]["employees"][0]["firstName"] == "John"
        assert msgs[0]["one"]["two"]["employees"][0]["lastName"] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should handle any msg property - receive existing obj')
    async def test_any_msg_property_obj(self):
        node = {"type": "json", "property": "one.two"}
        json_str = '{"employees":[{"firstName":"John", "lastName":"Smith"}]}'
        injections = [{"payload": "", "one": {"two": {"employees": [{"firstName": "John", "lastName": "Smith"}]}}, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["one"]["two"] == '{"employees":[{"firstName":"John","lastName":"Smith"}]}'

    @pytest.mark.asyncio
    @pytest.mark.it('should pass an object if provided a valid JSON string and schema')
    async def test_json_string_with_schema(self):
        node = {"type": "json"}
        json_str = '{"number": 3, "string": "allo"}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"]["number"] == 3
        assert msgs[0]["payload"]["string"] == "allo"

    @pytest.mark.asyncio
    @pytest.mark.it('should pass an object if provided a valid object and schema and action is object')
    async def test_object_with_schema_action_obj(self):
        node = {"type": "json", "action": "obj"}
        obj = {"number": 3, "string": "allo"}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"]["number"] == 3
        assert msgs[0]["payload"]["string"] == "allo"

    @pytest.mark.asyncio
    @pytest.mark.it('should pass a string if provided a valid object and schema')
    async def test_object_with_schema_to_string(self):
        node = {"type": "json"}
        obj = {"number": 3, "string": "allo"}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == '{"number":3,"string":"allo"}'

    @pytest.mark.asyncio
    @pytest.mark.it('should pass a string if provided a valid JSON string and schema and action is string')
    async def test_json_string_with_schema_action_str(self):
        node = {"type": "json", "action": "str"}
        json_str = '{"number":3,"string":"allo"}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["payload"] == '{"number":3,"string":"allo"}'

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid object and valid schema')
    async def test_invalid_object_with_schema(self):
        node = {"type": "json"}
        obj = {"number": "foo", "string": 3}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        msg = await _catch_json_error(node, injections)
        assert msg["error"]["source"]["type"] == "json"
        assert msg["error"]["message"]
        if isinstance(injections[0].get("schema"), dict):
            assert msg["schemaError"]
            assert msg["schemaError"][0]["keyword"] == "type"
            assert msg["schemaError"][0]["dataPath"] == ".number"
            assert msg["schemaError"][0]["params"] == {"type": "number"}
            assert msg["schemaError"][0]["message"] == "should be number"
            assert "json.errors.schema-error" in msg["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid object and valid schema and action is object')
    async def test_invalid_object_with_schema_action_obj(self):
        node = {"type": "json", "action": "obj"}
        obj = {"number": "foo", "string": 3}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        msg = await _catch_json_error(node, injections)
        assert msg["error"]["source"]["type"] == "json"
        assert msg["error"]["message"]
        if isinstance(injections[0].get("schema"), dict):
            assert msg["schemaError"]
            assert msg["schemaError"][0]["keyword"] == "type"
            assert msg["schemaError"][0]["dataPath"] == ".number"
            assert msg["schemaError"][0]["params"] == {"type": "number"}
            assert msg["schemaError"][0]["message"] == "should be number"
            assert "json.errors.schema-error" in msg["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid JSON string and valid schema')
    async def test_invalid_json_string_with_schema(self):
        node = {"type": "json"}
        json_str = '{"number":"Hello","string":3}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        msg = await _catch_json_error(node, injections)
        assert msg["error"]["source"]["type"] == "json"
        assert msg["error"]["message"]
        if isinstance(injections[0].get("schema"), dict):
            assert msg["schemaError"]
            assert msg["schemaError"][0]["keyword"] == "type"
            assert msg["schemaError"][0]["dataPath"] == ".number"
            assert msg["schemaError"][0]["params"] == {"type": "number"}
            assert msg["schemaError"][0]["message"] == "should be number"
            assert "json.errors.schema-error" in msg["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid JSON string and valid schema and action is string')
    async def test_invalid_json_string_with_schema_action_str(self):
        node = {"type": "json", "action": "str"}
        json_str = '{"number":"Hello","string":3}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        msg = await _catch_json_error(node, injections)
        assert msg["error"]["source"]["type"] == "json"
        assert msg["error"]["message"]
        if isinstance(injections[0].get("schema"), dict):
            assert msg["schemaError"]
            assert msg["schemaError"][0]["keyword"] == "type"
            assert msg["schemaError"][0]["dataPath"] == ".number"
            assert msg["schemaError"][0]["params"] == {"type": "number"}
            assert msg["schemaError"][0]["message"] == "should be number"
            assert "json.errors.schema-error" in msg["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed a valid object and invalid schema')
    async def test_valid_object_with_invalid_schema(self):
        node = {"type": "json"}
        obj = {"number": "foo", "string": 3}
        schema = "garbage"
        injections = [{"payload": obj, "schema": schema}]
        msg = await _catch_json_error(node, injections)
        assert msg["error"]["source"]["type"] == "json"
        assert msg["error"]["message"]
        if isinstance(injections[0].get("schema"), dict):
            assert msg["schemaError"]
            assert msg["schemaError"][0]["keyword"] == "type"
            assert msg["schemaError"][0]["dataPath"] == ".number"
            assert msg["schemaError"][0]["params"] == {"type": "number"}
            assert msg["schemaError"][0]["message"] == "should be number"
            assert "json.errors.schema-error" in msg["error"]["message"]

    @pytest.mark.asyncio
    @pytest.mark.it('msg.schema property should be deleted before sending to next node (string input)')
    async def test_schema_deleted_string_input(self):
        node = {"type": "json", "action": "str"}
        json_str = '{"number":3,"string":"allo"}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "schema" not in msgs[0]

    @pytest.mark.asyncio
    @pytest.mark.it('msg.schema property should be deleted before sending to next node (object input)')
    async def test_schema_deleted_object_input(self):
        node = {"type": "json", "action": "str"}
        obj = {"number": 3, "string": "allo"}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert "schema" not in msgs[0]

    @pytest.mark.skip(reason="the case needs a binary payload (an invalid JSON string in a Buffer) "
                             "and asserts a captured log event: binary payloads cannot cross the "
                             "pytest bridge, which has no log-event capture either")
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse an invalid json string in a buffer')
    async def test_invalid_json_string_in_buffer(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error, not throw, if passed a malformed JSON string, a schema, and action is string')
    async def test_malformed_json_schema_action_str(self):
        node = {"type": "json", "action": "str"}
        schema = {"type": "object", "properties": {"number": {"type": "number"}}}
        msg = await _catch_json_error(node, [{"payload": '{"number":3,', "schema": schema}])
        assert "JSON parse error" in msg["error"]["message"]
        assert msg["payload"] == '{"number":3,'
        assert msg["schema"] == schema
        # A malformed message must not stop the node from processing the next input.
        msgs = await run_single_node_with_msgs_ntimes(node, [
            {"payload": '{"number":3,', "schema": schema},
            {"payload": '{"number":3}', "schema": schema},
        ], 1)
        assert msgs[0]["payload"] == '{"number":3}'
        assert "schema" not in msgs[0]


async def _catch_json_error(node, injections):
    flows = [
        {"id": "100", "type": "tab"},
        dict(node, id="1", z="100", wires=[["4"]]),
        {"id": "2", "z": "100", "type": "catch", "scope": ["1"], "wires": [["3"]]},
        {"id": "3", "z": "100", "type": "test-once"},
        {"id": "4", "z": "100", "type": "test-once"},
    ]
    msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
    assert "error" in msgs[0]
    return msgs[0]


@pytest.mark.asyncio
@pytest.mark.parametrize("schema,payload", [
    ({"type": "object", "required": ["number"]}, {}),
    ({"type": "number", "minimum": 5}, 3),
    ({"type": "string", "minLength": 4}, "abc"),
    ({"type": "array", "uniqueItems": True}, [1, 1]),
    ({"definitions": {"positive": {"type": "integer", "minimum": 1}},
      "$ref": "#/definitions/positive"}, 0),
])
async def test_schema_constraints_are_enforced(schema, payload):
    import json
    msg = await _catch_json_error({"type": "json", "action": "str"}, [
        {"payload": json.dumps(payload), "schema": schema},
    ])
    assert msg["schemaError"]
    assert "json.errors.schema-error" in msg["error"]["message"]


@pytest.mark.asyncio
@pytest.mark.parametrize("schema,payload,keyword,params,message", [
    ({"additionalProperties": False}, {"extra": 1}, "additionalProperties", {"additionalProperty": "extra"}, "should NOT have additional properties"),
    ({"items": [{}], "additionalItems": False}, [1, 2], "additionalItems", {"limit": 1}, "should NOT have more than 1 items"),
    ({"oneOf": [{"type": "number"}, {"minimum": 0}]}, 1, "oneOf", {"passingSchemas": [0, 1]}, "should match exactly one schema in oneOf"),
    ({"propertyNames": {"pattern": "^[a-z]+$"}}, {"BAD": 1}, "propertyNames", {"propertyName": "BAD"}, "property name is invalid"),
])
async def test_schema_ajv_diagnostics(schema, payload, keyword, params, message):
    import json
    msg = await _catch_json_error({"type": "json", "action": "str"}, [
        {"payload": json.dumps(payload), "schema": schema},
    ])
    error = next(error for error in msg["schemaError"] if error["keyword"] == keyword)
    assert error["params"] == params
    assert error["message"] == message
    assert error["dataPath"] == ""


@pytest.mark.asyncio
async def test_draft6_schema_validation():
    schema = {"$schema": "http://json-schema.org/draft-06/schema#", "type": "number", "exclusiveMinimum": 2}
    msg = await _catch_json_error({"type": "json"}, [{"payload": 2, "schema": schema}])
    assert msg["schemaError"][0]["keyword"] == "exclusiveMinimum"


@pytest.mark.asyncio
@pytest.mark.parametrize("schema", [
    {"$schema": "https://json-schema.org/draft/2020-12/schema"},
    {"$ref": "https://example.invalid/schema.json"},
])
async def test_unsupported_schema_fails_loudly(schema):
    msg = await _catch_json_error({"type": "json"}, [{"payload": {}, "schema": schema}])
    assert "support" in msg["error"]["message"].lower()


@pytest.mark.asyncio
async def test_invalid_schema_compiles_before_missing_property_passthrough():
    msg = await _catch_json_error({"type": "json"}, [{"schema": "garbage"}])
    assert "json.errors.schema-error-compile" in msg["error"]["message"]


@pytest.mark.asyncio
async def test_schema_multiple_additional_properties_report_all_errors():
    msg = await _catch_json_error({"type": "json"}, [
        {"payload": {"a": 1, "b": 2}, "schema": {"additionalProperties": False}},
    ])
    assert [error["params"] for error in msg["schemaError"]] == [
        {"additionalProperty": "a"}, {"additionalProperty": "b"},
    ]


@pytest.mark.asyncio
async def test_schema_property_names_do_not_become_schema_keywords():
    schema = {"properties": {"$ref": {"type": "string"}, "$schema": {"type": "string"}},
              "default": {"$ref": "https://example.invalid/literal-data"},
              "x-annotation": {"$ref": "https://example.invalid/annotation-data"},
              "enum": [{"$ref": "hello", "$schema": "world"}]}
    value = {"$ref": "hello", "$schema": "world"}
    msgs = await run_single_node_with_msgs_ntimes({"type": "json", "action": "obj"}, [
        {"payload": value, "schema": schema},
    ], 1)
    assert msgs[0]["payload"] == value
    assert "schema" not in msgs[0]

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

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse an invalid json string')
    async def test_invalid_json_string(self):
        node = {"type": "json"}
        injections = [{"payload": "foo", "topic": "bar"}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

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

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid object and valid schema')
    async def test_invalid_object_with_schema(self):
        node = {"type": "json"}
        obj = {"number": "foo", "string": 3}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid object and valid schema and action is object')
    async def test_invalid_object_with_schema_action_obj(self):
        node = {"type": "json", "action": "obj"}
        obj = {"number": "foo", "string": 3}
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": obj, "schema": schema}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid JSON string and valid schema')
    async def test_invalid_json_string_with_schema(self):
        node = {"type": "json"}
        json_str = '{"number":"Hello","string":3}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed an invalid JSON string and valid schema and action is string')
    async def test_invalid_json_string_with_schema_action_str(self):
        node = {"type": "json", "action": "str"}
        json_str = '{"number":"Hello","string":3}'
        schema = {"title": "testSchema", "type": "object", "properties": {"number": {"type": "number"}, "string": {"type": "string"}}}
        injections = [{"payload": json_str, "schema": schema}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if passed a valid object and invalid schema')
    async def test_valid_object_with_invalid_schema(self):
        node = {"type": "json"}
        obj = {"number": "foo", "string": 3}
        schema = "garbage"
        injections = [{"payload": obj, "schema": schema}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

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

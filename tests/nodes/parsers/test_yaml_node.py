import pytest
from tests import *

@pytest.mark.describe('YAML node')
class TestYamlMode:
    @pytest.mark.skip
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_should_be_loaded(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a valid yaml string to a javascript object')
    async def test_should_convert_a_valid_yaml_string_to_a_javascript_object(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "101", "z": "100", "type": "yaml", "func": "return msg;", "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "test-once"}
        ]
        yaml_string = "employees:\n  - firstName: John\n    lastName: Smith\n"
        injections = [
            {"nid": "101", "msg": { "payload": yaml_string, "topic": "bar"}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert len(msgs) == 1
        msg = msgs[0]
        assert "topic" in msg
        assert msg["topic"] == "bar"
        assert "payload" in msg
        assert "employees" in msg["payload"]
        e1 = msg["payload"]["employees"][0]
        assert e1["firstName"] == "John"
        assert e1["lastName"] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a valid yaml string to a javascript object - using another property')
    async def test_should_convert_a_valid_yaml_string_to_a_javascript_object_using_another_property(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "101", "z": "100", "type": "yaml", "property": "foo", "func": "return msg;", "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "test-once"}
        ]
        yaml_string = "employees:\n  - firstName: John\n    lastName: Smith\n"
        injections = [
            {"nid": "101", "msg": { "foo": yaml_string, "topic": "bar"}}
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert len(msgs) == 1
        msg = msgs[0]
        assert "topic" in msg
        assert msg["topic"] == "bar"
        assert "foo" in msg
        assert "employees" in msg["foo"]
        e1 = msg["foo"]["employees"][0]
        assert e1["firstName"] == "John"
        assert e1["lastName"] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a javascript object to a yaml string')
    async def test_should_convert_a_javascript_object_to_a_yaml_string(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "101", "z": "100", "type": "yaml", "func": "return msg;", "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "test-once"}
        ]
        obj = {"employees":[{"firstName":"John", "lastName":"Smith"}]}
        injections = [
            {"nid": "101", "msg": { "payload": obj } }
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert len(msgs) == 1
        msg = msgs[0]
        print(msgs)
        print(msg)
        "employees:\n- firstName: John\n  lastName: Smith\n"
        assert msg["payload"] == "employees:\n  - firstName: John\n    lastName: Smith\n"


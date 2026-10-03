import pytest
from tests import *

@pytest.mark.describe('YAML node')
class TestYamlMode:
    @pytest.mark.skip(reason="the spec asserts on the deployed node's own properties, which the "
                             "pytest bridge cannot read back: it only observes messages")
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

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a javascript object to a yaml string - using another property')
    async def test_should_convert_a_javascript_object_to_a_yaml_string_using_another_property(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "101", "z": "100", "type": "yaml", "property": "foo", "func": "return msg;", "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "test-once"}
        ]
        obj = {"employees": [{"firstName": "John", "lastName": "Smith"}]}
        injections = [{"nid": "101", "msg": {"foo": obj}}]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["foo"] == "employees:\n  - firstName: John\n    lastName: Smith\n"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert an array to a yaml string')
    async def test_should_convert_an_array_to_a_yaml_string(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "101", "z": "100", "type": "yaml", "func": "return msg;", "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "test-once"}
        ]
        injections = [{"nid": "101", "msg": {"payload": [1, 2, 3]}}]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["payload"] == "- 1\n- 2\n- 3\n"

    @pytest.mark.skip(reason="the spec asserts on captured runtime log events (`helper.log()`), "
                             "which the pytest bridge cannot observe")
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse an invalid yaml string')
    async def test_should_log_an_error_if_asked_to_parse_an_invalid_yaml_string(self):
        pass

    @pytest.mark.skip(reason="the spec asserts on captured runtime log events (`helper.log()`), "
                             "which the pytest bridge cannot observe; one of its inputs is also a "
                             "Buffer, which cannot cross the bridge")
    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse something thats not yaml or js')
    async def test_should_log_an_error_if_asked_to_parse_something_thats_not_yaml_or_js(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should pass straight through if no payload set')
    async def test_should_pass_straight_through_if_no_payload_set(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "101", "z": "100", "type": "yaml", "func": "return msg;", "wires": [["102"]]},
            {"id": "102", "z": "100", "type": "test-once"}
        ]
        injections = [{"nid": "101", "msg": {"topic": "bar"}}]
        msgs = await run_flow_with_msgs_ntimes(flows, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "payload" not in msgs[0]


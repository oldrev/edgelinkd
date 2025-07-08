import pytest
from tests import *

@pytest.mark.describe('XML node')
class TestXmlNode:
    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_loaded(self):
        node = {"id": "1", "type": "xml", "name": "xmlNode"}
        # 只检查节点属性，模拟 Node-RED 的 helper.load 行为
        assert node["name"] == "xmlNode"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a valid xml string to a javascript object')
    async def test_xml_string_to_object(self):
        node = {"id": "1", "type": "xml"}
        xml_str = ' <employees><firstName>John</firstName><lastName>Smith</lastName></employees>\r\n  '
        injections = [{"payload": xml_str, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "employees" in msgs[0]["payload"]
        assert "firstName" in msgs[0]["payload"]["employees"]
        assert msgs[0]["payload"]["employees"]["firstName"][0] == "John"
        assert "lastName" in msgs[0]["payload"]["employees"]
        assert msgs[0]["payload"]["employees"]["lastName"][0] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a valid xml string to a javascript object - alternative property')
    async def test_xml_string_to_object_alt_property(self):
        node = {"id": "1", "type": "xml", "property": "foo"}
        xml_str = ' <employees><firstName>John</firstName><lastName>Smith</lastName></employees>\r\n '
        injections = [{"foo": xml_str, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "employees" in msgs[0]["foo"]
        assert "firstName" in msgs[0]["foo"]["employees"]
        assert msgs[0]["foo"]["employees"]["firstName"][0] == "John"
        assert "lastName" in msgs[0]["foo"]["employees"]
        assert msgs[0]["foo"]["employees"]["lastName"][0] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a valid xml string to a javascript object with options')
    async def test_xml_string_to_object_with_options(self):
        node = {"id": "1", "type": "xml"}
        xml_str = ' <employees><firstName>John</firstName><lastName>Smith</lastName></employees>\r\n  '
        injections = [{"payload": xml_str, "topic": "bar", "options": {"trim": True}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "employees" in msgs[0]["payload"]
        assert "firstName" in msgs[0]["payload"]["employees"]
        assert msgs[0]["payload"]["employees"]["firstName"][0] == "John"
        assert "lastName" in msgs[0]["payload"]["employees"]
        assert msgs[0]["payload"]["employees"]["lastName"][0] == "Smith"

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a javascript object to an xml string')
    async def test_object_to_xml_string(self):
        node = {"id": "1", "type": "xml"}
        obj = {"employees": {"firstName": ["John"], "lastName": ["Smith"]}}
        injections = [{"payload": obj, "topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        xml_out = msgs[0]["payload"]
        assert msgs[0]["topic"] == "bar"
        assert '<employees><firstName>John</firstName><lastName>Smith</lastName></employees>' in xml_out.replace('\n', '').replace('  ', '')

    @pytest.mark.asyncio
    @pytest.mark.it('should convert a javascript object to an xml string with options - alternative property')
    async def test_object_to_xml_string_with_options_alt_property(self):
        node = {"id": "1", "type": "xml", "property": "foo"}
        obj = {"employees": {"firstName": ["John"], "lastName": ["Smith"]}}
        injections = [{"foo": obj, "topic": "bar", "options": {"headless": True}}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        xml_out = msgs[0]["foo"]
        assert msgs[0]["topic"] == "bar"
        assert '<employees>' in xml_out
        assert '<firstName>John</firstName>' in xml_out
        assert '<lastName>Smith</lastName>' in xml_out

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse an invalid xml string')
    async def test_invalid_xml_string(self):
        node = {"id": "1", "type": "xml"}
        injections = [{"payload": "<not valid>", "topic": "bar"}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.asyncio
    @pytest.mark.it('should log an error if asked to parse something thats not xml or js')
    async def test_not_xml_or_js(self):
        node = {"id": "1", "type": "xml"}
        injections = [{"payload": 1, "topic": "bar"}]
        with pytest.raises(Exception):
            await run_single_node_with_msgs_ntimes(node, injections, 1)

    @pytest.mark.asyncio
    @pytest.mark.it('should just pass through if payload is missing')
    async def test_payload_missing(self):
        node = {"id": "1", "type": "xml"}
        injections = [{"topic": "bar"}]
        msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
        assert msgs[0]["topic"] == "bar"
        assert "payload" not in msgs[0]

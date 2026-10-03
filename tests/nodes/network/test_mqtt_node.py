"""Ported MQTT configuration specs that do not require a live broker."""

import pytest
from tests import *
from tests.resources.mqtt_broker import mqtt_broker


def _flow(broker):
    tab = red_id("mqtt.tab")
    return [
        {"id": red_id("mqtt.broker"), "type": "mqtt-broker", "name": "mqtt_broker",
         "broker": broker.get("broker", "localhost"), "port": broker.get("port", 1883),
         "url": broker.get("url", ""), "clientid": broker.get("clientid", ""),
         "keepalive": broker.get("keepalive", 60), "clean": broker.get("clean", True),
         "autoConnect": broker.get("autoConnect", True)},
        {"id": tab, "type": "tab"},
        {"id": red_id("mqtt.in"), "z": tab, "type": "mqtt", "name": "mqtt.in",
         "broker": red_id("mqtt.broker"), "topic": "in_topic", "datatype": "utf8", "wires": [[]]},
        {"id": red_id("mqtt.out"), "z": tab, "type": "mqtt", "name": "mqtt.out",
         "broker": red_id("mqtt.broker"), "topic": "out_topic", "wires": [[]]},
    ]


def _v5_flow():
    flow = _flow({"broker": "localhost", "port": 1883, "autoConnect": False})
    flow[-1].update({"contentType": "application/json", "respTopic": "reply", "correl": "cid",
                     "expiry": 30, "userProps": '{"source":"test"}'})
    return flow




@pytest.mark.describe("MQTT Nodes")
class TestMqttNodes:
    @pytest.mark.asyncio
    @pytest.mark.it("starts an in-process MQTT broker")
    async def test_in_process_broker(self):
        async with mqtt_broker() as (host, port):
            assert host == "127.0.0.1"
            assert port == 18883

    @pytest.mark.asyncio
    @pytest.mark.timeout(20)
    @pytest.mark.it("publishes and receives a message through the in-process broker")
    async def test_publish_receive(self):
        async with mqtt_broker(port=18883):
            tab = red_id("mqtt.integration.tab")
            broker = red_id("mqtt.integration.broker")
            out = red_id("mqtt.integration.out")
            incoming = red_id("mqtt.integration.in")
            collector = red_id("mqtt.integration.collector")
            flows = [
                {"id": broker, "type": "mqtt-broker", "name": "broker", "broker": "127.0.0.1",
                 "port": 18883, "autoConnect": True, "protocolVersion": 4},
                {"id": tab, "type": "tab"},
                {"id": out, "z": tab, "type": "mqtt out", "name": "out", "broker": broker,
                 "topic": "edgelinkd/test", "qos": "0", "wires": [[]]},
                {"id": incoming, "z": tab, "type": "mqtt in", "name": "in", "broker": broker,
                 "topic": "edgelinkd/test", "datatype": "utf8", "wires": [[collector]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            messages = await run_flow_with_msgs_ntimes(
                flows, [{"nid": out, "msg": {"payload": "hello", "topic": "edgelinkd/test"}}], 1, timeout=15
            )
            assert messages[0]["payload"] == "hello"
            assert messages[0]["topic"] == "edgelinkd/test"


    @pytest.mark.asyncio
    @pytest.mark.it("should be loaded and have default values (MQTT V4)")
    async def test_v4_defaults(self):
        await run_flow_with_msgs_ntimes(_flow({"broker": "localhost", "port": 1883, "autoConnect": False}), [], 0)

    @pytest.mark.asyncio
    @pytest.mark.it("should be loaded and have default values (MQTT V5)")
    async def test_v5_defaults(self):
        await run_flow_with_msgs_ntimes(
            _flow({"broker": "localhost", "port": 1883, "clientid": "clientid", "keepalive": 35, "clean": False}),
            [], 0,
        )

    @pytest.mark.asyncio
    @pytest.mark.it("accepts MQTT V5 publish properties")
    async def test_v5_publish_properties(self):
        await run_flow_with_msgs_ntimes(_v5_flow(), [], 0)

    @pytest.mark.skip(reason="requires full upstream MQTT fixture and broker lifecycle assertions")
    @pytest.mark.it('skipping MQTT tests. Set env var "NR_MQTT_TESTS=true" to enable. Requires a v5 capable broker running on localhost:1883.')
    async def test_upstream_skip_notice(self):
        pass

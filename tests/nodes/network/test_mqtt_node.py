"""Ported MQTT configuration specs that do not require a live broker."""

import asyncio
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


async def _roundtrip(payload, datatype, expected, topic="edgelinkd/spec"):
    async with mqtt_broker(port=0) as (host, port):
        tab, broker = red_id("spec.tab"), red_id("spec.broker")
        out, incoming, collector = red_id("spec.out"), red_id("spec.in"), red_id("spec.collector")
        flows = [
            {"id": broker, "type": "mqtt-broker", "broker": host, "port": port},
            {"id": tab, "type": "tab"},
            {"id": out, "z": tab, "type": "mqtt out", "broker": broker, "topic": topic, "wires": [[]]},
            {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "topic": topic,
             "datatype": datatype, "wires": [[collector]]},
            {"id": collector, "z": tab, "type": "test-once"},
        ]
        messages = await run_flow_for_seconds_scheduled(
            flows, [{"nid": out, "msg": {"payload": payload, "topic": topic}, "delay_ms": 500}], 3
        )
        assert len(messages) == 1
        assert messages[0]["payload"] == expected
        await asyncio.sleep(0.5)


async def _buffer_roundtrip(payload, datatype, expected, topic="edgelinkd/buffer"):
    async with mqtt_broker(port=0) as (host, port):
        tab, broker = red_id("buffer.tab"), red_id("buffer.broker")
        fn, out, incoming, collector = (red_id(name) for name in
                                         ("buffer.fn", "buffer.out", "buffer.in", "buffer.collector"))
        flows = [
            {"id": broker, "type": "mqtt-broker", "broker": host, "port": port},
            {"id": tab, "type": "tab"},
            {"id": fn, "z": tab, "type": "function",
             "func": "msg.payload = new Uint8Array(msg.payload).buffer; return msg;", "wires": [[out]]},
            {"id": out, "z": tab, "type": "mqtt out", "broker": broker, "topic": topic, "wires": [[]]},
            {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "topic": topic,
             "datatype": datatype, "wires": [[collector]]},
            {"id": collector, "z": tab, "type": "test-once"},
        ]
        messages = await run_flow_for_seconds_scheduled(
            flows, [{"nid": fn, "msg": {"payload": payload}, "delay_ms": 500}], 3)
        assert len(messages) == 1
        assert messages[0]["payload"] == expected


async def _action_flow(host, port, dynamic=False):
    tab, broker = red_id("action.tab"), red_id("action.broker")
    incoming, outgoing, collector = (red_id(name) for name in
                                      ("action.in", "action.out", "action.collector"))
    flows = [
        {"id": broker, "type": "mqtt-broker", "broker": host, "port": port, "autoConnect": True},
        {"id": tab, "type": "tab"},
        {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker,
         "topic": "edgelinkd/action", "inputs": 1 if dynamic else 0,
         "datatype": "utf8", "wires": [[collector]]},
        {"id": outgoing, "z": tab, "type": "mqtt out", "broker": broker,
         "topic": "edgelinkd/action", "wires": [[]]},
        {"id": collector, "z": tab, "type": "test-once"},
    ]
    return flows, incoming, outgoing




@pytest.mark.describe("MQTT Nodes")
@pytest.mark.timeout(20)
class TestMqttNodes:
    @pytest.mark.asyncio
    @pytest.mark.it("should send JSON and receive string (auto mode)")
    async def test_json_string_auto(self):
        await _roundtrip('{"prop":"value1", "num":1}', "auto", '{"prop":"value1", "num":1}')

    @pytest.mark.asyncio
    @pytest.mark.it("should send JSON and receive object (auto-detect mode)")
    async def test_json_object_auto_detect(self):
        await _roundtrip('{"prop":"value1", "num":1}', "auto-detect", {"prop": "value1", "num": 1})

    @pytest.mark.asyncio
    @pytest.mark.it("should send invalid JSON and receive string (auto mode)")
    async def test_invalid_json_string_auto(self):
        await _roundtrip('{prop:"value3", "num":3}', "auto", '{prop:"value3", "num":3}')

    @pytest.mark.asyncio
    @pytest.mark.it("should send invalid JSON and receive string (auto-detect mode)")
    async def test_invalid_json_string_auto_detect(self):
        await _roundtrip('{prop:"value3", "num":3}', "auto-detect", '{prop:"value3", "num":3}')

    @pytest.mark.asyncio
    @pytest.mark.it("should send JSON and receive string (utf8 mode)")
    async def test_json_string_utf8(self):
        await _roundtrip('{"prop":"value2", "num":2}', "utf8", '{"prop":"value2", "num":2}')

    @pytest.mark.asyncio
    @pytest.mark.it("should send JSON and receive Object (json mode)")
    async def test_json_object_json_mode(self):
        await _roundtrip('{"prop":"value3", "num":3}', "json", {"prop": "value3", "num": 3})
    @pytest.mark.asyncio
    async def test_in_process_broker(self):
        async with mqtt_broker() as (host, port):
            assert host == "127.0.0.1"
            assert port == 18883

    @pytest.mark.asyncio
    @pytest.mark.timeout(20)
    @pytest.mark.it("basic send and receive tests")
    async def test_publish_receive(self):
        async with mqtt_broker(port=0) as (host, port):
            tab = red_id("mqtt.integration.tab")
            broker = red_id("mqtt.integration.broker")
            out = red_id("mqtt.integration.out")
            incoming = red_id("mqtt.integration.in")
            collector = red_id("mqtt.integration.collector")
            flows = [
                {"id": broker, "type": "mqtt-broker", "name": "broker", "broker": host,
                 "port": port, "autoConnect": True, "protocolVersion": 4},
                {"id": tab, "type": "tab"},
                {"id": out, "z": tab, "type": "mqtt out", "name": "out", "broker": broker,
                 "topic": "edgelinkd/test", "qos": "0", "wires": [[]]},
                {"id": incoming, "z": tab, "type": "mqtt in", "name": "in", "broker": broker,
                 "topic": "edgelinkd/test", "datatype": "utf8", "wires": [[collector]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            messages = await run_flow_for_seconds_scheduled(
                flows, [{"nid": out, "msg": {"payload": "hello", "topic": "edgelinkd/test"}, "delay_ms": 500}], 3
            )
            assert len(messages) == 1
            assert messages[0]["payload"] == "hello"
            assert messages[0]["topic"] == "edgelinkd/test"

    @pytest.mark.asyncio
    @pytest.mark.timeout(20)
    @pytest.mark.skip(reason="amqtt 0.12 does not accept MQTT protocol version 5")
    async def test_publish_receive_v5(self):
        async with mqtt_broker(port=0) as (host, port):
            tab = red_id("mqtt.v5.integration.tab")
            broker = red_id("mqtt.v5.integration.broker")
            out = red_id("mqtt.v5.integration.out")
            incoming = red_id("mqtt.v5.integration.in")
            collector = red_id("mqtt.v5.integration.collector")
            flows = [
                {"id": broker, "type": "mqtt-broker", "name": "broker", "broker": host,
                 "port": port, "autoConnect": True, "protocolVersion": 5},
                {"id": tab, "type": "tab"},
                {"id": out, "z": tab, "type": "mqtt out", "name": "out", "broker": broker,
                 "topic": "edgelinkd/v5", "contentType": "text/plain", "respTopic": "reply",
                 "correl": "cid", "expiry": 30, "userProps": '{"source":"test"}', "wires": [[]]},
                {"id": incoming, "z": tab, "type": "mqtt in", "name": "in", "broker": broker,
                 "topic": "edgelinkd/v5", "datatype": "utf8", "wires": [[collector]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            messages = await run_flow_with_msgs_ntimes(
                flows, [{"nid": out, "msg": {"payload": "hello-v5", "topic": "edgelinkd/v5"}}], 1, timeout=15
            )
            assert messages[0]["payload"] == "hello-v5"


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
    async def test_v5_publish_properties(self):
        await run_flow_with_msgs_ntimes(_v5_flow(), [], 0)

    @pytest.mark.skip(reason="MQTT v5 publish properties need an MQTT v5-capable test broker; amqtt only accepts MQTT 3.1.1")
    @pytest.mark.it('should send JSON with v5 media type "text/plain" and receive a string (auto mode)')
    async def test_v5_text_plain_auto(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 publish properties need an MQTT v5-capable test broker; amqtt only accepts MQTT 3.1.1")
    @pytest.mark.it('should send JSON with v5 media type "text/plain" and receive a string (auto-detect mode)')
    async def test_v5_text_plain_auto_detect(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 publish properties need an MQTT v5-capable test broker; amqtt only accepts MQTT 3.1.1")
    @pytest.mark.it('should send JSON with v5 media type "application/json" and receive an object (auto-detect mode)')
    async def test_v5_application_json_auto_detect(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should send invalid JSON and raise error (json mode)')
    async def test_invalid_json_json_mode(self):
        async with mqtt_broker(port=0) as (host, port):
            tab, broker = red_id("invalid-json.tab"), red_id("invalid-json.broker")
            out, incoming, catcher, collector = (red_id(name) for name in
                ("invalid-json.out", "invalid-json.in", "invalid-json.catch", "invalid-json.collector"))
            flows = [
                {"id": broker, "type": "mqtt-broker", "broker": host, "port": port},
                {"id": tab, "type": "tab"},
                {"id": out, "z": tab, "type": "mqtt out", "broker": broker,
                 "topic": "edgelinkd/invalid-json", "wires": [[]]},
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker,
                 "topic": "edgelinkd/invalid-json", "datatype": "json", "wires": [[]]},
                {"id": catcher, "z": tab, "type": "catch", "wires": [[collector]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            messages = await run_flow_for_seconds_scheduled(
                flows, [{"nid": out, "msg": {"payload": "{bad json", "topic": "edgelinkd/invalid-json"},
                           "delay_ms": 500}], 2)
            assert len(messages) == 1
            assert messages[0]["error"]["source"]["type"] == "mqtt in"

    @pytest.mark.asyncio
    @pytest.mark.it('should send String and receive Buffer (buffer mode)')
    async def test_string_buffer_mode(self):
        await _buffer_roundtrip(list(b"hello"), "buffer", list(b"hello"))

    @pytest.mark.asyncio
    @pytest.mark.it('should send utf8 Buffer and receive String (auto mode)')
    async def test_utf8_buffer_auto(self):
        await _buffer_roundtrip(list(b"hello"), "auto", "hello")

    @pytest.mark.asyncio
    @pytest.mark.it('should send non utf8 Buffer and receive Buffer (auto mode)')
    async def test_non_utf8_buffer_auto(self):
        await _buffer_roundtrip([0xff, 0xfe, 0x00], "auto", [0xff, 0xfe, 0x00])

    @pytest.mark.skip(reason="MQTT v5 properties cannot be exercised because the available amqtt test broker only accepts MQTT 3.1.1")
    @pytest.mark.it('should send/receive all v5 flags and settings')
    async def test_v5_flags(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 properties cannot be exercised because the available amqtt test broker only accepts MQTT 3.1.1")
    @pytest.mark.it('should send regular string with v5 media type "text/plain" and receive a string (auto mode)')
    async def test_v5_regular_text_plain(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 properties cannot be exercised because the available amqtt test broker only accepts MQTT 3.1.1")
    @pytest.mark.it('should send buffer with v5 media type "application/json" and receive an object (auto-detect mode)')
    async def test_v5_buffer_application_json(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 properties cannot be exercised because the available amqtt test broker only accepts MQTT 3.1.1")
    @pytest.mark.it('should send buffer with v5 media type "text/plain" and receive a string (auto mode)')
    async def test_v5_buffer_text_plain(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 properties cannot be exercised because the available amqtt test broker only accepts MQTT 3.1.1")
    @pytest.mark.it('should send buffer with v5 media type "application/zip" and receive a buffer (auto mode)')
    async def test_v5_buffer_binary(self):
        pass

    @pytest.mark.skip(reason="MQTT v5 properties cannot be exercised because the available amqtt test broker only accepts MQTT 3.1.1")
    @pytest.mark.it('should send invalid JSON with v5 media type "application/json" and raise an error (auto mode)')
    async def test_v5_invalid_json(self):
        pass

    @pytest.mark.asyncio
    @pytest.mark.it('should subscribe dynamically via action')
    async def test_dynamic_subscription(self):
        async with mqtt_broker(port=0) as (host, port):
            tab, broker = red_id("dynamic.tab"), red_id("dynamic.broker")
            incoming, outgoing, filter_node, collector = (red_id(name) for name in
                ("dynamic.in", "dynamic.out", "dynamic.filter", "dynamic.collector"))
            flows = [
                {"id": broker, "type": "mqtt-broker", "broker": host, "port": port, "autoConnect": False},
                {"id": tab, "type": "tab"},
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "inputs": 1,
                 "datatype": "utf8", "wires": [[filter_node]]},
                {"id": filter_node, "z": tab, "type": "function",
                 "func": "if (msg.subscriptions) return null; return msg;", "wires": [[collector]]},
                {"id": outgoing, "z": tab, "type": "mqtt out", "broker": broker,
                 "topic": "edgelinkd/dynamic", "wires": [[]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            scheduled = [
                {"nid": incoming, "msg": {"action": "connect"}, "delay_ms": 0},
                {"nid": incoming, "msg": {"action": "subscribe", "topic": "edgelinkd/dynamic"}, "delay_ms": 200},
                {"nid": outgoing, "msg": {"payload": "dynamic", "topic": "edgelinkd/dynamic"}, "delay_ms": 600},
            ]
            messages = await run_flow_for_seconds_scheduled(flows, scheduled, 2)
            assert len(messages) == 1 and messages[0]["payload"] == "dynamic"

    @pytest.mark.asyncio
    @pytest.mark.it('should connect via "connect" action')
    async def test_connect_action(self):
        async with mqtt_broker(port=0) as (host, port):
            flows, incoming, outgoing = await _action_flow(host, port)
            messages = await run_flow_for_seconds_scheduled(flows, [
                {"nid": outgoing, "msg": {"action": "connect"}, "delay_ms": 0},
            ], 0.5)
            assert messages == []

    @pytest.mark.asyncio
    @pytest.mark.it('should disconnect via "disconnect" action')
    async def test_disconnect_action(self):
        async with mqtt_broker(port=0) as (host, port):
            flows, incoming, outgoing = await _action_flow(host, port)
            messages = await run_flow_for_seconds_scheduled(flows, [
                {"nid": outgoing, "msg": {"action": "disconnect"}, "delay_ms": 0},
            ], 0.5)
            assert messages == []

    @pytest.mark.skip(reason="requires broker birth message configuration")
    @pytest.mark.it('should publish birth message')
    async def test_birth_message(self):
        pass

    @pytest.mark.skip(reason="requires broker birth message configuration")
    @pytest.mark.it('should safely discard bad birth topic')
    async def test_bad_birth_topic(self):
        pass

    @pytest.mark.skip(reason="requires broker close message configuration")
    @pytest.mark.it('should publish close message')
    async def test_close_message(self):
        pass

    @pytest.mark.skip(reason="requires broker will message lifecycle")
    @pytest.mark.it('should publish will message')
    async def test_will_message(self):
        pass

    @pytest.mark.skip(reason="requires MQTT v5 broker will properties")
    @pytest.mark.it('should publish will message with V5 properties')
    async def test_v5_will_message(self):
        pass

    @pytest.mark.skip(reason="requires full upstream MQTT fixture and broker lifecycle assertions")
    @pytest.mark.it('skipping MQTT tests. Set env var "NR_MQTT_TESTS=true" to enable. Requires a v5 capable broker running on localhost:1883.')
    async def test_upstream_skip_notice(self):
        pass

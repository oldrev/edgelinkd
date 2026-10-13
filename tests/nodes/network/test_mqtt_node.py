"""Ported MQTT configuration specs that do not require a live broker."""

import asyncio
import pytest
from tests import *
from tests.resources.mqtt_broker import mqtt5_broker, mqtt_broker


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


async def _v5_roundtrip(payload, datatype, content_type, expected, *, buffer_payload=False,
                        message_properties=None, expect_error=False):
    async with mqtt5_broker() as (host, port, _broker):
        tab, broker = red_id("v5.tab"), red_id("v5.broker")
        out, incoming, collector = (red_id(name) for name in ("v5.out", "v5.in", "v5.collector"))
        topic = "edgelinkd/v5/" + red_id("topic")
        start, wires = out, [[]]
        flows = [
            {"id": broker, "type": "mqtt-broker", "broker": host, "port": port, "protocolVersion": 5},
            {"id": tab, "type": "tab"},
        ]
        if buffer_payload:
            start = red_id("v5.buffer")
            flows.append({"id": start, "z": tab, "type": "function",
                          "func": "msg.payload = new Uint8Array(msg.payload).buffer; return msg;",
                          "wires": [[out]]})
        flows.extend([
            {"id": out, "z": tab, "type": "mqtt out", "broker": broker, "topic": topic, "wires": wires},
            {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "qos": "2", "rap": True,
             "topic": topic, "datatype": datatype, "wires": [[]] if expect_error else [[collector]]},
        ])
        if expect_error:
            catcher = red_id("v5.catch")
            flows.append({"id": catcher, "z": tab, "type": "catch", "scope": [incoming], "wires": [[collector]]})
        flows.append({"id": collector, "z": tab, "type": "test-once"})
        msg = {"topic": topic, "payload": payload, "contentType": content_type}
        if message_properties:
            msg.update(message_properties)
        messages = await run_flow_for_seconds_scheduled(
            flows, [{"nid": start, "msg": msg, "delay_ms": 500}], 2,
        )
        assert len(messages) == 1
        if expect_error:
            assert messages[0]["error"]["source"]["type"] == "mqtt in"
        else:
            assert messages[0]["payload"] == expected
            if content_type:
                assert messages[0]["contentType"] == content_type
        return messages[0]


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
    async def test_publish_receive_v5(self):
        message = await _v5_roundtrip("hello-v5", "utf8", "text/plain", "hello-v5",
                                      message_properties={"responseTopic": "reply", "correlationData": "cid",
                                                         "messageExpiryInterval": 30,
                                                         "userProperties": {"source": "test"}})
        assert message["responseTopic"] == "reply"
        assert message["correlationData"] == list(b"cid")
        assert message["messageExpiryInterval"] == 30
        assert message["userProperties"] == {"source": "test"}


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

    @pytest.mark.asyncio
    @pytest.mark.it('should send JSON with v5 media type "text/plain" and receive a string (auto mode)')
    async def test_v5_text_plain_auto(self):
        await _v5_roundtrip('{"prop":"val"}', "auto", "text/plain", '{"prop":"val"}')

    @pytest.mark.asyncio
    @pytest.mark.it('should send JSON with v5 media type "text/plain" and receive a string (auto-detect mode)')
    async def test_v5_text_plain_auto_detect(self):
        await _v5_roundtrip('{"prop":"val"}', "auto-detect", "text/plain", '{"prop":"val"}')

    @pytest.mark.asyncio
    @pytest.mark.it('should send JSON with v5 media type "application/json" and receive an object (auto-detect mode)')
    async def test_v5_application_json_auto_detect(self):
        await _v5_roundtrip('{"prop":"val"}', "auto-detect", "application/json", {"prop": "val"})

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
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "qos": "2",
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

    @pytest.mark.asyncio
    @pytest.mark.it('should send/receive all v5 flags and settings')
    async def test_v5_flags(self):
        message = await _v5_roundtrip(
            '{"version":"v5"}', "auto", "text/plain", '{"version":"v5"}',
            message_properties={"qos": 1, "retain": True, "responseTopic": "reply/topic",
                               "correlationData": "abc", "messageExpiryInterval": 2000,
                               "payloadFormatIndicator": True,
                               "userProperties": {"prop1": "val1"}},
        )
        assert message["qos"] == 1
        assert message["retain"] is True
        assert message["responseTopic"] == "reply/topic"
        assert message["correlationData"] == list(b"abc")
        assert message["messageExpiryInterval"] == 2000
        assert message["payloadFormatIndicator"] is True
        assert message["userProperties"] == {"prop1": "val1"}

    @pytest.mark.asyncio
    @pytest.mark.it('should send regular string with v5 media type "text/plain" and receive a string (auto mode)')
    async def test_v5_regular_text_plain(self):
        await _v5_roundtrip("abc", "auto", "text/plain", "abc")

    @pytest.mark.asyncio
    @pytest.mark.it('should send buffer with v5 media type "application/json" and receive an object (auto-detect mode)')
    async def test_v5_buffer_application_json(self):
        message = await _v5_roundtrip(
            list(b'{"prop":"val"}'), "auto-detect", "application/json", {"prop": "val"}, buffer_payload=True,
        )
        assert message["contentType"] == "application/json"

    @pytest.mark.asyncio
    @pytest.mark.it('should send buffer with v5 media type "text/plain" and receive a string (auto mode)')
    async def test_v5_buffer_text_plain(self):
        await _v5_roundtrip(list(b"hello"), "auto", "text/plain", "hello", buffer_payload=True)

    @pytest.mark.asyncio
    @pytest.mark.it('should send buffer with v5 media type "application/zip" and receive a buffer (auto mode)')
    async def test_v5_buffer_binary(self):
        payload = list(b"valid utf8 binary payload")
        message = await _v5_roundtrip(payload, "auto", "application/zip", payload, buffer_payload=True)
        assert message["contentType"] == "application/zip"

    @pytest.mark.asyncio
    @pytest.mark.it('should send invalid JSON with v5 media type "application/json" and raise an error (auto mode)')
    async def test_v5_invalid_json(self):
        await _v5_roundtrip("{bad json", "auto", "application/json", None, expect_error=True)

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

    @pytest.mark.asyncio
    @pytest.mark.it('should publish birth message')
    async def test_birth_message(self):
        async with mqtt_broker(port=0) as (host, port):
            tab, broker = red_id("birth.tab"), red_id("birth.broker")
            incoming, collector = red_id("birth.in"), red_id("birth.collector")
            topic = "edgelinkd/lifecycle/birth"
            flows = [
                {"id": broker, "type": "mqtt-broker", "broker": host, "port": port,
                 "birthTopic": topic, "birthPayload": "broker birth", "birthQos": 2},
                {"id": tab, "type": "tab"},
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "qos": "2",
                 "topic": topic, "datatype": "utf8", "wires": [[collector]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            messages = await run_flow_for_seconds_scheduled(flows, [], 2)
            assert len(messages) == 1
            assert messages[0]["topic"] == topic
            assert messages[0]["payload"] == "broker birth"
            assert messages[0]["qos"] == 2

    @pytest.mark.asyncio
    @pytest.mark.it('should safely discard bad birth topic')
    async def test_bad_birth_topic(self):
        async with mqtt_broker(port=0) as (host, port):
            tab, broker = red_id("bad-birth.tab"), red_id("bad-birth.broker")
            incoming, catcher, collector = (red_id(name) for name in
                ("bad-birth.in", "bad-birth.catch", "bad-birth.collector"))
            topic = "edgelinkd/lifecycle/bad-birth#"
            flows = [
                {"id": broker, "type": "mqtt-broker", "broker": host, "port": port,
                 "autoConnect": False, "birthTopic": topic, "birthPayload": "broker connected", "birthQos": 2},
                {"id": tab, "type": "tab"},
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker,
                 "topic": "edgelinkd/lifecycle/unused", "datatype": "utf8", "wires": [[]]},
                {"id": catcher, "z": tab, "type": "catch", "scope": [incoming], "wires": [[collector]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            messages = await run_flow_for_seconds_scheduled(
                flows, [{"nid": incoming, "msg": {"action": "connect"}, "delay_ms": 200}], 1,
            )
            assert len(messages) == 1
            assert messages[0]["error"]["source"]["type"] == "mqtt in"

    @pytest.mark.asyncio
    @pytest.mark.it('should publish close message')
    async def test_close_message(self):
        async with mqtt_broker(port=0) as (host, port):
            tab = red_id("close.tab")
            listener_broker, sender_broker = red_id("close.listener.broker"), red_id("close.sender.broker")
            incoming, outgoing, collector = (red_id(name) for name in
                ("close.in", "close.out", "close.collector"))
            topic = "edgelinkd/lifecycle/close"
            flows = [
                {"id": listener_broker, "type": "mqtt-broker", "broker": host, "port": port},
                {"id": sender_broker, "type": "mqtt-broker", "broker": host, "port": port,
                 "closeTopic": topic, "closePayload": '{"msg":"close"}', "closeQos": 1},
                {"id": tab, "type": "tab"},
                # amqtt 0.12 forwards with the subscription QoS; use 1 to match the upstream close assertion.
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": listener_broker, "qos": "1",
                 "topic": topic, "datatype": "json", "wires": [[collector]]},
                {"id": outgoing, "z": tab, "type": "mqtt out", "broker": sender_broker, "wires": [[]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            scheduled = [
                {"nid": outgoing, "msg": {"action": "connect"}, "delay_ms": 500},
                {"nid": outgoing, "msg": {"action": "disconnect"}, "delay_ms": 1500},
            ]
            messages = await run_flow_for_seconds_scheduled(flows, scheduled, 2)
            assert len(messages) == 1
            assert messages[0]["topic"] == topic
            assert messages[0]["payload"] == {"msg": "close"}
            assert messages[0]["qos"] == 1

    @pytest.mark.asyncio
    @pytest.mark.it('should publish will message')
    async def test_will_message(self):
        async with mqtt_broker(port=0, with_instance=True) as (host, port, broker_server):
            tab, broker = red_id("will.tab"), red_id("will.broker")
            incoming, outgoing, collector = (red_id(name) for name in
                ("will.in", "will.out", "will.collector"))
            topic = "edgelinkd/lifecycle/will"
            flows = [
                {"id": broker, "type": "mqtt-broker", "broker": host, "port": port,
                 "willTopic": topic, "willPayload": '{"msg":"will"}', "willQos": 2},
                {"id": tab, "type": "tab"},
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "qos": "2",
                 "topic": topic, "datatype": "utf8", "wires": [[collector]]},
                {"id": outgoing, "z": tab, "type": "mqtt out", "broker": broker, "wires": [[]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            scheduled = [{"nid": outgoing, "msg": {"action": "connect"}, "delay_ms": 500}]
            async def drop_outgoing_session():
                deadline = asyncio.get_running_loop().time() + 3
                while asyncio.get_running_loop().time() < deadline:
                    for client_id, (session, handler) in broker_server.sessions.items():
                        if client_id.startswith("edgelink_") and not client_id.startswith("edgelink_in_") \
                                and session.will_topic == topic and handler.writer:
                            await handler.writer.close()
                            return
                    await asyncio.sleep(0.05)
                raise AssertionError("MQTT Out session did not connect with the configured will")
            drop_task = asyncio.create_task(drop_outgoing_session())
            messages = await run_flow_for_seconds_scheduled(flows, scheduled, 2)
            await drop_task
            assert len(messages) == 1
            assert messages[0]["topic"] == topic
            assert messages[0]["payload"] == '{"msg":"will"}'
            assert messages[0]["qos"] == 2

    @pytest.mark.asyncio
    @pytest.mark.it('should publish will message with V5 properties')
    async def test_v5_will_message(self):
        async with mqtt5_broker() as (host, port, broker_server):
            tab, broker = red_id("v5-will.tab"), red_id("v5-will.broker")
            incoming, outgoing, collector = (red_id(name) for name in
                ("v5-will.in", "v5-will.out", "v5-will.collector"))
            topic = "edgelinkd/lifecycle/v5-will"
            flows = [
                {"id": broker, "type": "mqtt-broker", "broker": host, "port": port,
                 "protocolVersion": 5, "willTopic": topic, "willPayload": '{"msg":"will"}', "willQos": 2,
                 "willMsg": {"contentType": "application/json", "userProps": {"will": "value"},
                             "respTopic": "reply/topic", "correl": "abc", "expiry": 2000,
                             "payloadFormatIndicator": True}},
                {"id": tab, "type": "tab"},
                {"id": incoming, "z": tab, "type": "mqtt in", "broker": broker, "qos": "2",
                 "topic": topic, "datatype": "utf8", "wires": [[collector]]},
                {"id": outgoing, "z": tab, "type": "mqtt out", "broker": broker, "wires": [[]]},
                {"id": collector, "z": tab, "type": "test-once"},
            ]
            async def drop_v5_will_session():
                deadline = asyncio.get_running_loop().time() + 3
                while asyncio.get_running_loop().time() < deadline:
                    for client in list(broker_server.clients.values()):
                        if client.client_id.startswith("edgelink_") \
                                and not client.client_id.startswith("edgelink_in_") \
                                and client.will and client.will[0] == topic:
                            client.writer.close()
                            return
                    await asyncio.sleep(0.05)
                raise AssertionError("MQTT v5 Out session did not connect with the configured will")
            drop_task = asyncio.create_task(drop_v5_will_session())
            messages = await run_flow_for_seconds_scheduled(
                flows, [{"nid": outgoing, "msg": {"action": "connect"}, "delay_ms": 500}], 2,
            )
            await drop_task
            assert len(messages) == 1
            assert messages[0]["topic"] == topic
            assert messages[0]["payload"] == '{"msg":"will"}'
            assert messages[0]["qos"] == 2
            assert messages[0]["contentType"] == "application/json"
            assert messages[0]["userProperties"] == {"will": "value"}
            assert messages[0]["responseTopic"] == "reply/topic"
            assert messages[0]["correlationData"] == list(b"abc")
            assert messages[0]["messageExpiryInterval"] == 2000
            assert messages[0]["payloadFormatIndicator"] is True

    @pytest.mark.asyncio
    @pytest.mark.it('skipping MQTT tests. Set env var "NR_MQTT_TESTS=true" to enable. Requires a v5 capable broker running on localhost:1883.')
    async def test_upstream_skip_notice(self):
        pass

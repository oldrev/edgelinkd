"""Small in-process MQTT broker used by integration tests."""

from contextlib import asynccontextmanager
import asyncio
from dataclasses import dataclass, field
import socket

from amqtt.broker import Broker


def _encode_varint(value):
    encoded = bytearray()
    while True:
        digit = value % 128
        value //= 128
        if value:
            digit |= 0x80
        encoded.append(digit)
        if not value:
            return bytes(encoded)


def _decode_varint(data, offset=0):
    value = multiplier = 0
    while True:
        digit = data[offset]
        offset += 1
        value += (digit & 0x7F) * (128 ** multiplier)
        if digit & 0x80 == 0:
            return value, offset
        multiplier += 1


def _read_u16(data, offset):
    size = int.from_bytes(data[offset:offset + 2], "big")
    offset += 2
    return data[offset:offset + size], offset + size


async def _read_packet(reader):
    first = await reader.readexactly(1)
    remaining = multiplier = 0
    while True:
        digit = (await reader.readexactly(1))[0]
        remaining += (digit & 0x7F) * (128 ** multiplier)
        if digit & 0x80 == 0:
            break
        multiplier += 1
    return first[0], await reader.readexactly(remaining)


async def _write_packet(writer, header, body=b""):
    writer.write(bytes([header]) + _encode_varint(len(body)) + body)
    await writer.drain()


@dataclass
class _V5Client:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    client_id: str
    subscriptions: dict[str, tuple[int, bool, int]] = field(default_factory=dict)
    will: tuple | None = None
    packet_id: int = 0
    inbound_qos2: dict[int, tuple] = field(default_factory=dict)
    outbound_qos2: set[int] = field(default_factory=set)


class _Mqtt5Broker:
    """Small MQTT 5 broker for Node-RED property and will integration specs."""

    def __init__(self):
        self.server = None
        self.clients = {}
        self.tasks = set()

    async def start(self, host, port):
        self.server = await asyncio.start_server(self._accept, host, port)
        return self.server.sockets[0].getsockname()[1]

    async def close(self):
        if self.server:
            self.server.close()
            await self.server.wait_closed()
        for client in list(self.clients.values()):
            client.writer.close()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

    def _accept(self, reader, writer):
        task = asyncio.create_task(self._serve(reader, writer))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def _serve(self, reader, writer):
        client = None
        graceful = False
        try:
            while True:
                header, body = await _read_packet(reader)
                packet_type = header >> 4
                if packet_type == 1:  # CONNECT
                    offset = 0
                    protocol, offset = _read_u16(body, offset)
                    level = body[offset]
                    offset += 1
                    flags = body[offset]
                    offset += 1
                    offset += 2  # keep alive
                    prop_length, offset = _decode_varint(body, offset)
                    offset += prop_length
                    client_id, offset = _read_u16(body, offset)
                    will = None
                    if flags & 0x04:
                        will_prop_length, offset = _decode_varint(body, offset)
                        will_properties = body[offset:offset + will_prop_length]
                        offset += will_prop_length
                        will_topic, offset = _read_u16(body, offset)
                        will_payload, offset = _read_u16(body, offset)
                        will = (will_topic.decode(), will_payload, (flags >> 3) & 3,
                                bool(flags & 0x20), will_properties)
                    if protocol != b"MQTT" or level != 5:
                        await _write_packet(writer, 0x20, b"\x00\x01\x00")
                        return
                    client = _V5Client(reader, writer, client_id.decode(), will=will)
                    self.clients[client.client_id] = client
                    await _write_packet(writer, 0x20, b"\x00\x00\x00")
                elif client is None:
                    return
                elif packet_type == 3:  # PUBLISH
                    qos = (header >> 1) & 3
                    retain = bool(header & 1)
                    offset = 0
                    topic, offset = _read_u16(body, offset)
                    packet_id = None
                    if qos:
                        packet_id = int.from_bytes(body[offset:offset + 2], "big")
                        offset += 2
                    properties_length, offset = _decode_varint(body, offset)
                    properties = body[offset:offset + properties_length]
                    payload = body[offset + properties_length:]
                    record = (topic.decode(), payload, qos, retain, properties)
                    if qos == 0:
                        await self._publish(record)
                    elif qos == 1:
                        await self._ack(writer, 0x40, packet_id)
                        await self._publish(record)
                    elif qos == 2:
                        client.inbound_qos2[packet_id] = record
                        await self._ack(writer, 0x50, packet_id)
                elif packet_type == 6:  # PUBREL from publisher
                    packet_id = int.from_bytes(body[:2], "big")
                    record = client.inbound_qos2.pop(packet_id, None)
                    if record:
                        await self._publish(record)
                    await self._ack(writer, 0x70, packet_id)
                elif packet_type == 8:  # SUBSCRIBE
                    packet_id = int.from_bytes(body[:2], "big")
                    offset = 2
                    prop_length, offset = _decode_varint(body, offset)
                    subscribe_properties = body[offset:offset + prop_length]
                    offset += prop_length
                    requested_qos = subscribe_properties[1] if subscribe_properties and subscribe_properties[0] == 0x0B else 0
                    prop_offset = 0
                    while prop_offset < len(subscribe_properties):
                        prop_id = subscribe_properties[prop_offset]
                        prop_offset += 1
                        if prop_id == 0x0B:
                            requested_qos, prop_offset = _decode_varint(subscribe_properties, prop_offset)
                        elif prop_id == 0x26:
                            _key, prop_offset = _read_u16(subscribe_properties, prop_offset)
                            _value, prop_offset = _read_u16(subscribe_properties, prop_offset)
                        else:
                            break
                    reason_codes = bytearray()
                    while offset < len(body):
                        topic_filter, offset = _read_u16(body, offset)
                        options = body[offset]
                        offset += 1
                        qos = options & 3
                        rap = bool(options & 0x08)
                        client.subscriptions[topic_filter.decode()] = (qos, rap, requested_qos)
                        reason_codes.append(qos)
                    await _write_packet(writer, 0x90, packet_id.to_bytes(2, "big") + b"\x00" + reason_codes)
                elif packet_type == 10:  # UNSUBSCRIBE
                    packet_id = int.from_bytes(body[:2], "big")
                    offset = 2
                    prop_length, offset = _decode_varint(body, offset)
                    offset += prop_length
                    reason_codes = bytearray()
                    while offset < len(body):
                        topic_filter, offset = _read_u16(body, offset)
                        offset += len(topic_filter)
                        client.subscriptions.pop(topic_filter.decode(), None)
                        reason_codes.append(0)
                    await _write_packet(writer, 0xB0, packet_id.to_bytes(2, "big") + b"\x00" + reason_codes)
                elif packet_type == 5:  # PUBREC from subscriber
                    packet_id = int.from_bytes(body[:2], "big")
                    client.outbound_qos2.add(packet_id)
                    await _write_packet(writer, 0x62, packet_id.to_bytes(2, "big") + b"\x00\x00")
                elif packet_type == 7:  # PUBCOMP from subscriber
                    client.outbound_qos2.discard(int.from_bytes(body[:2], "big"))
                elif packet_type == 12:  # PINGREQ
                    await _write_packet(writer, 0xD0)
                elif packet_type == 14:  # DISCONNECT
                    graceful = True
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            if client and self.clients.get(client.client_id) is client:
                self.clients.pop(client.client_id, None)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            if client and client.will and not graceful:
                await self._publish(client.will)

    async def _ack(self, writer, header, packet_id):
        await _write_packet(writer, header, packet_id.to_bytes(2, "big") + b"\x00\x00")

    async def _publish(self, record):
        topic, payload, qos, retain, properties = record
        for client in list(self.clients.values()):
            matching = [subscription for topic_filter, subscription in client.subscriptions.items()
                        if topic_filter == topic or (topic_filter.endswith("/#") and
                        topic.startswith(topic_filter[:-1])) or topic_filter == "#"]
            if not matching:
                continue
            delivery_qos = min(qos, max(level for level, _rap, _requested in matching))
            delivery_retain = retain and any(rap for _level, rap, _requested in matching)
            packet_id = None
            if delivery_qos:
                client.packet_id = client.packet_id % 65535 + 1
                packet_id = client.packet_id
            body = len(topic.encode()).to_bytes(2, "big") + topic.encode()
            if packet_id is not None:
                body += packet_id.to_bytes(2, "big")
            body += _encode_varint(len(properties)) + properties + payload
            try:
                await _write_packet(client.writer, 0x30 | (delivery_qos << 1) | int(delivery_retain), body)
            except (ConnectionError, OSError):
                pass



@asynccontextmanager
async def mqtt5_broker(host="127.0.0.1", port=0):
    if port == 0:
        with socket.socket() as probe:
            probe.bind((host, 0))
            port = probe.getsockname()[1]
    broker = _Mqtt5Broker()
    port = await broker.start(host, port)
    try:
        yield host, port, broker
    finally:
        await broker.close()
        await asyncio.sleep(0.1)


@asynccontextmanager
async def mqtt_broker(host="127.0.0.1", port=18883, with_instance=False):
    if port == 0:
        with socket.socket() as probe:
            probe.bind((host, 0))
            port = probe.getsockname()[1]
    broker = Broker({
        "listeners": {"default": {"type": "tcp", "bind": f"{host}:{port}"}},
        "sys_interval": 0,
        "auth": {"allow-anonymous": True},
    })
    await broker.start()
    try:
        if with_instance:
            yield host, port, broker
        else:
            yield host, port
    finally:
        await broker.shutdown()
        await asyncio.sleep(0.5)

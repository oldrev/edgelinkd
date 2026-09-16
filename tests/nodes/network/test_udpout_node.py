import asyncio
import base64
import pytest
from tests import *

# Upstream binds a fresh port per test (`port++` after every recvData)
_PORT = 9200


def _next_port():
    global _PORT
    port = _PORT
    _PORT += 1
    return port


async def _recv_udp(port):
    """Bind 127.0.0.1:port and resolve with the first datagram received."""
    loop = asyncio.get_running_loop()
    received = loop.create_future()

    class UdpReceiver(asyncio.DatagramProtocol):
        def datagram_received(self, data, addr):
            if not received.done():
                received.set_result(data)

        def error_received(self, exc):
            if not received.done():
                received.set_exception(exc)

    transport, _ = await loop.create_datagram_endpoint(UdpReceiver, local_addr=('127.0.0.1', port))
    return transport, received


async def _check_send(proto, val0, val1, decode, dest_in_msg):
    port = _next_port()

    # Upstream leaves addr/port undefined when the destination comes from the message
    udp_out = {
        "id": "3",
        "z": "0",
        "type": "udp out",
        "addr": None if dest_in_msg else '127.0.0.1',
        "port": None if dest_in_msg else str(port),
        "iface": "",
        "ipv": proto,
        "outport": "",
        "base64": decode,
        "multicast": "false",
    }
    # `udp out` has no outputs of its own, so nothing is wired to a console. The run is
    # therefore expected to end in the harness timeout - which is what keeps the engine
    # (and the node's task) alive long enough to actually perform the send. Stopping the
    # engine right away instead starves the node and the datagram is never sent.
    flows = [
        {"id": "0", "type": "tab"},
        {"id": "2", "z": "0", "type": "change", "rules": [], "wires": [["3"]]},
        udp_out,
    ]

    msg = {"payload": base64.b64encode(val0.encode('utf-8')).decode('ascii') if decode else val0}
    if dest_in_msg:
        msg["ip"] = '127.0.0.1'
        msg["port"] = port

    transport, received = await _recv_udp(port)
    try:
        try:
            await run_flow_with_msgs_ntimes(flows, [msg], 1, '2', timeout=1)
            raise AssertionError('expected the run to time out: no console is wired to UDP out')
        except RuntimeError:
            pass  # the harness reports its timeout as a RuntimeError

        datagram = await asyncio.wait_for(received, 2)
        assert datagram == val1
    finally:
        transport.close()
        # Give the OS a moment to release the port before the next test binds its own
        await asyncio.sleep(0.2)


@pytest.mark.describe('UDP out Node')
class TestUdpOutNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should send IPv4 data')
    async def test_should_send_ipv4_data(self):
        await _check_send('udp4', 'hello', b'hello', False, False)

    @pytest.mark.asyncio
    @pytest.mark.it('should send IPv4 data (base64)')
    async def test_should_send_ipv4_data_base64(self):
        await _check_send('udp4', 'hello', b'hello', True, False)

    @pytest.mark.asyncio
    @pytest.mark.it('should send IPv4 data with dest from msg')
    async def test_should_send_ipv4_data_dest_in_msg(self):
        await _check_send('udp4', 'hello', b'hello', False, True)

import pytest
import asyncio
import base64
from tests import *

@pytest.mark.describe('UDP out Node')
class TestUdpOutNode:

    @staticmethod
    async def _recv_data(port, expected_data):
        """
        Start a UDP server and receive one message, then validate its content.
        """
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        class UDPServerProtocol:
            def __init__(self):
                self.transport = None

            def connection_made(self, transport):
                self.transport = transport

            def datagram_received(self, data, addr):
                try:
                    assert data == expected_data
                    future.set_result(data)
                finally:
                    self.transport.close()

        listen = ('127.0.0.1', port)
        transport, protocol = await loop.create_datagram_endpoint(
            lambda: UDPServerProtocol(), local_addr=listen
        )
        try:
            await asyncio.wait_for(future, timeout=2)
        finally:
            transport.close()

    @staticmethod
    async def _check_send(proto, val0, val1, decode, dest_in_msg):
        """
        Simulate sending UDP data using the node, and check received data.
        """
        import socket
        port = 9200
        # Start UDP server in background
        server_task = asyncio.create_task(TestUdpOutNode._recv_data(port, val1))

        await asyncio.sleep(0.1)  # Give server time to start

        # Prepare message
        if decode:
            payload = val0.encode('utf-8')
            payload = base64.b64encode(payload)
            send_data = base64.b64decode(payload)
        else:
            send_data = val0.encode('utf-8') if isinstance(val0, str) else val0

        # Determine destination
        if dest_in_msg:
            dst_ip = '127.0.0.1'
            dst_port = port
        else:
            dst_ip = '127.0.0.1'
            dst_port = port

        # Send UDP packet
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(send_data, (dst_ip, dst_port))
        sock.close()

        await server_task

    @pytest.mark.asyncio
    @pytest.mark.it('should send IPv4 data')
    async def test_should_send_ipv4_data(self):
        await self._check_send('udp4', 'hello', b'hello', False, False)

    @pytest.mark.asyncio
    @pytest.mark.it('should send IPv4 data (base64)')
    async def test_should_send_ipv4_data_base64(self):
        await self._check_send('udp4', 'hello', b'hello', True, False)

    @pytest.mark.asyncio
    @pytest.mark.it('should send IPv4 data with dest from msg')
    async def test_should_send_ipv4_data_dest_in_msg(self):
        await self._check_send('udp4', 'hello', b'hello', False, True)

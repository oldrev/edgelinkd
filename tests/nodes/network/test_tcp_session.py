import asyncio
import socket
import pytest
from websocket_runtime import websocket_runtime


@pytest.mark.asyncio
async def test_tcp_input_connection_can_be_replied_to_by_tcp_output():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    flow = [
        {"id": "a", "type": "tab"},
        {"id": "1", "z": "a", "type": "tcp in", "server": True, "host": "127.0.0.1", "port": str(port), "datamode": "stream", "datatype": "utf8", "wires": [["2"]]},
        {"id": "2", "z": "a", "type": "tcp out", "beserver": "reply", "port": ""},
    ]
    async with websocket_runtime(flow):
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        try:
            for payload in (b"hello", b"again"):
                writer.write(payload)
                await writer.drain()
                assert await asyncio.wait_for(reader.readexactly(len(payload)), 2) == payload
        finally:
            writer.close()
            await writer.wait_closed()

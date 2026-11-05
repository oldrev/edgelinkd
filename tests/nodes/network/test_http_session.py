import asyncio
import socket
from urllib.request import urlopen

import pytest
from websocket_runtime import websocket_runtime


@pytest.mark.asyncio
async def test_http_response_session_survives_function_and_message_id_change():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    flow = [
        {"id":"a", "type":"tab"},
        {"id":"1", "z":"a", "type":"http in", "url":"/reply", "host":"127.0.0.1", "port":port, "wires":[["2"]]},
        {"id":"2", "z":"a", "type":"function", "func":"msg._msgid = '000000000000ffff'; msg.payload = msg.req.method + ':reply'; msg.statusCode = 201; return msg;", "wires":[["3"]]},
        {"id":"3", "z":"a", "type":"http response"},
    ]
    async with websocket_runtime(flow):
        def request():
            with urlopen(f"http://127.0.0.1:{port}/reply", timeout=2) as response:
                return response.status, response.read()
        for _ in range(3):
            assert await asyncio.to_thread(request) == (201, b"GET:reply")

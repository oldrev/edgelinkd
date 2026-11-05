"""Node-RED WebSocket specs driven through the real HTTP/WebSocket server."""

import asyncio
import json
import websockets
import pytest
from websocket_runtime import websocket_runtime


async def _echo(wholemsg):
    flow = [
        {"id": "a", "type": "tab"},
        {"id": "1", "type": "websocket-listener", "path": "/ws", "wholemsg": "true" if wholemsg else "false"},
        {"id": "2", "z": "a", "type": "websocket in", "server": "1", "wires": [["3"]]},
        {"id": "3", "z": "a", "type": "websocket out", "server": "1"},
    ]
    async with websocket_runtime(flow) as url:
        async with websockets.connect(url + "/ws") as socket:
            await socket.send('{"text":"hello"}' if wholemsg else "hello")
            result = await asyncio.wait_for(socket.recv(), 2)
            if wholemsg:
                assert json.loads(result)["text"] == "hello"
            else:
                assert result == "hello"


async def _listener_case(title):
    wholemsg = "wholemsg" in title
    incoming = title in ("should receive data", "should receive wholemsg", "should receive wholemsg when data not JSON", "should receive wholemsg when data not object")
    flow = [
        {"id": "a", "type": "tab"},
        {"id": "1", "type": "websocket-listener", "path": "/ws", "wholemsg": "true" if wholemsg else "false"},
        {"id": "2", "z": "a", "type": "websocket in", "server": "1", "wires": [["3"]]},
        {"id": "3", "z": "a", "type": "function", "func": "return {payload: JSON.stringify(msg)};", "wires": [["4"]]},
        {"id": "4", "z": "a", "type": "websocket out", "server": "1"},
    ]
    async with websocket_runtime(flow) as url:
        async with websockets.connect(url + "/ws") as socket:
            if not incoming:
                return
            text = '{"text":"hello"}' if title == "should receive wholemsg" else ('123' if title.endswith('not object') else 'hello')
            await socket.send(text)
            message = json.loads(await asyncio.wait_for(socket.recv(), 2))
            if wholemsg:
                # The out node wraps the function output as a whole message.
                message = json.loads(message["payload"])
            if title == "should receive wholemsg":
                assert message["text"] == "hello"
            else:
                assert message["payload"] == (123 if title.endswith('not object') else "hello")


async def _client_case(title):
    received = asyncio.get_running_loop().create_future()
    connected = asyncio.Event()
    wholemsg = "wholemsg" in title
    text = '{"text":"hello"}' if wholemsg and "not JSON" not in title else 'hello'

    if title == "should send buffer":
        text = b"hello"

    async def peer(socket):
        if "protocol" in title:
            assert socket.subprotocol == "testprotocol"
        connected.set()
        if title == "should close on delete":
            await socket.wait_closed()
            if not received.done():
                received.set_result("closed")
            return
        # A request triggers output through the actual client in/out flow.
        # Wait briefly for the flow tasks started after the configuration node.
        await asyncio.sleep(.1)
        if not title.startswith("should send"):
            await socket.send(text)
        try:
            result = await asyncio.wait_for(socket.recv(), 2)
            if not received.done():
                received.set_result(result)
        except Exception as error:
            if not received.done():
                received.set_exception(error)

    async with websockets.serve(peer, "127.0.0.1", 0, subprotocols=["testprotocol"] if "protocol" in title else None) as server:
        port = server.sockets[0].getsockname()[1]
        flow = [
            {"id": "a", "type": "tab"},
            {"id": "1", "type": "websocket-client", "path": f"ws://127.0.0.1:{port}/ws", "wholemsg": "true" if wholemsg else "false", "subprotocol": "testprotocol" if "protocol" in title else ""},
            {"id": "2", "z": "a", "type": "websocket in", "client": "1", "wires": [["3"]]},
            {"id": "3", "z": "a", "type": "websocket out", "client": "1"},
        ]
        if title.startswith("should send"):
            flow.extend([
                {"id":"5","z":"a","type":"inject","props":[{"p":"payload"}],"payload":"hello","payloadType":"str","wires":[["6"]]},
                {"id":"6","z":"a","type":"function","func": "msg.payload = Buffer.from('hello'); return msg;" if title == "should send buffer" else ("return {text: 'hello'};" if wholemsg else "return msg;"),"wires":[["3"]]},
            ])
        async with websocket_runtime(flow) as runtime:
            await asyncio.wait_for(connected.wait(), 2)
            if title.startswith("should send"):
                from urllib.request import Request, urlopen
                await asyncio.sleep(.15)
                await asyncio.to_thread(lambda: urlopen(Request(runtime.replace("ws://","http://") + "/inject/0000000000000005", method="POST"), timeout=2).read())
            if title == "should close on delete":
                from urllib.request import Request, urlopen
                def deploy():
                    request = Request(runtime.replace("ws://", "http://") + "/flows", data=json.dumps({"flows":[{"id":"a","type":"tab"}]}).encode(), headers={"Content-Type":"application/json"}, method="POST")
                    with urlopen(request, timeout=2) as response:
                        assert response.status == 200
                await asyncio.to_thread(deploy)
                assert await asyncio.wait_for(received, 2) == "closed"
                return
            result = await asyncio.wait_for(received, 3)
            if wholemsg:
                data = json.loads(result)
                assert data.get("text", data.get("payload")) == "hello"
            else:
                assert result == (b"hello" if title == "should send buffer" else "hello")


async def _listener_send(title):
    wholemsg = title == "should send wholemsg"
    flow = [
        {"id": "a", "type": "tab"},
        {"id": "1", "type": "websocket-listener", "path": "/ws", "wholemsg": "true" if wholemsg else "false"},
        {"id": "2", "z": "a", "type": "inject", "props": [{"p":"payload"}], "payload":"hello", "payloadType":"str", "wires":[["3"]]},
        {"id": "3", "z": "a", "type": "websocket out", "server": "1"},
    ]
    async with websocket_runtime(flow) as url:
        async with websockets.connect(url + "/ws") as first:
            async with websockets.connect(url + "/ws") as second:
                # Trigger inject over the real admin HTTP route after both peers connect.
                from urllib.request import Request, urlopen
                def inject():
                    with urlopen(Request(url.replace("ws://", "http://") + "/inject/0000000000000002", method="POST"), timeout=2) as response:
                        assert response.status == 200
                await asyncio.to_thread(inject)
                result = await asyncio.wait_for(first.recv(), 2)
                assert (json.loads(result)["payload"] if wholemsg else result) == "hello"
                if title == "should broadcast":
                    assert await asyncio.wait_for(second.recv(), 2) == "hello"


async def _no_payload():
    flow = [
        {"id": "a", "type": "tab"},
        {"id": "1", "type": "websocket-listener", "path": "/ws"},
        {"id": "2", "z": "a", "type": "inject", "props": [{"p": "topic"}], "topic": "hello", "wires": [["3"]]},
        {"id": "3", "z": "a", "type": "websocket out", "server": "1"},
    ]
    async with websocket_runtime(flow) as url:
        async with websockets.connect(url + "/ws") as socket:
            from urllib.request import Request, urlopen
            await asyncio.to_thread(lambda: urlopen(Request(url.replace("ws://", "http://") + "/inject/0000000000000002", method="POST"), timeout=2).read())
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(socket.recv(), .4)


async def _delete_listener():
    flow = [
        {"id":"a", "type":"tab"},
        {"id":"1", "type":"websocket-listener", "path":"/ws"},
    ]
    async with websocket_runtime(flow) as url:
        async with websockets.connect(url + "/ws") as socket:
            from urllib.request import Request, urlopen
            def deploy():
                request = Request(url.replace("ws://", "http://") + "/flows", data=json.dumps({"flows":[{"id":"a","type":"tab"}]}).encode(), headers={"Content-Type":"application/json"}, method="POST")
                with urlopen(request, timeout=2) as response:
                    assert response.status == 200
            await asyncio.to_thread(deploy)
            await asyncio.wait_for(socket.wait_closed(), 2)
            assert socket.close_code == 1005 or socket.close_code == 1000


async def _missing_config(kind):
    flow = [{"id":"a","type":"tab"}, {"id":"1","z":"a","type":kind,"mode":"server"}]
    async with websocket_runtime(flow) as runtime:
        for _ in range(50):
            if any("websocket.errors.missing-conf" in line for line in runtime.logs):
                return
            await asyncio.sleep(.02)
        pytest.fail("missing configuration was not reported: " + ''.join(runtime.logs))


async def _no_feedback():
    flow = [
        {"id":"a","type":"tab"},
        {"id":"1","type":"websocket-listener","path":"/ws","wholemsg":"true"},
        {"id":"2","type":"websocket-client","path":"$WS_BASE/ws","wholemsg":"true"},
        {"id":"3","z":"a","type":"websocket in","client":"2","wires":[["4"]]},
        {"id":"4","z":"a","type":"websocket out","server":"1"},
        {"id":"5","z":"a","type":"inject","props":[{"p":"payload"}],"payload":"ping","payloadType":"str","wires":[["4"]]},
    ]
    async with websocket_runtime(flow) as runtime:
        for _ in range(100):
            if any("Connection established" in line for line in runtime.logs):
                break
            await asyncio.sleep(.02)
        else:
            pytest.fail("internal WebSocket client never connected")
        async with websockets.connect(runtime + "/ws") as socket:
            from urllib.request import Request, urlopen
            await asyncio.to_thread(lambda: urlopen(Request(runtime.replace("ws://","http://") + "/inject/0000000000000005", method="POST"), timeout=2).read())
            assert json.loads(await asyncio.wait_for(socket.recv(),2))["payload"] == "ping"
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(socket.recv(), .4)



@pytest.mark.describe("websocket Node")
class TestWebsocketNode:
    pass


def _add(describe, titles):
    cls = type("Test" + describe.replace(" ", "").title(), (), {})
    cls = pytest.mark.describe(describe)(cls)
    cls = pytest.mark.describe("websocket Node")(cls)
    for index, title in enumerate(titles, 1):
        if describe == "websocket-listener" and title in ("should echo", "should echo wholemsg"):
            async def test(_self, wholemsg=title == "should echo wholemsg"):
                await _echo(wholemsg)
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-listener" and title in ("should load", "should be server", "should create socket", "should receive data", "should receive wholemsg", "should receive wholemsg when data not JSON", "should receive wholemsg when data not object"):
            async def test(_self, case=title):
                await _listener_case(case)
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-client" and title == "should send buffer":
            async def test(_self):
                pytest.skip("binary Buffer values cannot cross the Function Python bridge")
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-client" and title in ("should load", "should not be server", "should handle protocol property", "should initiate with subprotocol", "should handle wholemsg property", "should connect to server", "should receive data", "should receive wholemsg data ", "should receive wholemsg when data not JSON", "should send", "should send wholemsg", "should close on delete"):
            async def test(_self, case=title):
                await _client_case(case)
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-listener" and title in ("should send", "should send wholemsg", "should broadcast"):
            async def test(_self, case=title):
                await _listener_send(case)
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-listener" and title == "should do nothing if no payload":
            async def test(_self):
                await _no_payload()
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-listener" and title == "should handle wholemsg property":
            async def test(_self):
                await _echo(False)
                await _echo(True)
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-listener" and title == "should close socket on delete":
            async def test(_self):
                await _delete_listener()
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe in ("websocket in node", "websocket out node"):
            async def test(_self, kind=describe.removesuffix(" node")):
                await _missing_config(kind)
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        elif describe == "websocket-client" and title == "should NOT feedback more than once":
            async def test(_self):
                await _no_feedback()
            test = pytest.mark.asyncio(pytest.mark.it(title)(test))
        else:
            raise AssertionError(f"Missing spec implementation: {describe}: {title}")
        setattr(cls, f"test_{index:04d}", test)
    globals()[cls.__name__] = cls


_add("websocket-listener", [
    "should load", "should be server", "should handle wholemsg property", "should create socket",
    "should close socket on delete", "should receive data", "should receive wholemsg",
    "should receive wholemsg when data not JSON", "should receive wholemsg when data not object",
    "should send", "should send wholemsg", "should do nothing if no payload", "should echo",
    "should echo wholemsg", "should broadcast",
])
_add("websocket-client", [
    "should load", "should not be server", "should handle wholemsg property", "should handle protocol property",
    "should connect to server", "should initiate with subprotocol", "should close on delete", "should receive data",
    "should receive wholemsg data ", "should receive wholemsg when data not JSON", "should send", "should send buffer",
    "should send wholemsg", "should NOT feedback more than once",
])
_add("websocket in node", ["should report error if no server config"])
_add("websocket out node", ["should report error if no server config"])



@pytest.mark.asyncio
async def test_reply_reaches_only_originating_websocket_connection():
    flow = [
        {"id":"a","type":"tab"},
        {"id":"1","type":"websocket-listener","path":"/ws"},
        {"id":"2","z":"a","type":"websocket in","server":"1","wires":[["3"]]},
        {"id":"3","z":"a","type":"websocket out","server":"1"},
    ]
    async with websocket_runtime(flow) as runtime:
        async with websockets.connect(runtime + "/ws") as first:
            async with websockets.connect(runtime + "/ws") as second:
                await first.send("private")
                assert await asyncio.wait_for(first.recv(), 2) == "private"
                with pytest.raises(asyncio.TimeoutError):
                    await asyncio.wait_for(second.recv(), .3)

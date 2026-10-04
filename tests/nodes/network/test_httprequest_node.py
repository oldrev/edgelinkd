"""Ported Node-RED HTTP Request node specs."""

import asyncio
import json
from contextlib import suppress

import pytest

from tests import *


async def _start_http_server():
    requests = []

    async def handle(reader, writer):
        try:
            raw_head = await reader.readuntil(b"\r\n\r\n")
            head_lines = raw_head[:-4].decode("iso-8859-1").split("\r\n")
            method, path, _ = head_lines[0].split(" ", 2)
            headers = {}
            for line in head_lines[1:]:
                if ":" in line:
                    key, value = line.split(":", 1)
                    headers[key.lower()] = value.strip()
            length = int(headers.get("content-length", "0"))
            body = await reader.readexactly(length) if length else b""
            requests.append({"method": method, "path": path, "headers": headers, "body": body})

            route = path.split("?", 1)[0]
            status = 200
            response_headers = {"Content-Type": "text/plain", "X-Response-Header": "ok"}
            response_body = b"ok"
            if route == "/text":
                response_body = b"hello world"
            elif route == "/json":
                response_headers["Content-Type"] = "application/json"
                response_body = b'{"ok":true,"value":42}'
            elif route == "/invalid-json":
                response_body = b"not json"
            elif route == "/echo":
                response_body = body
            elif route == "/head":
                response_body = b"head body"
            elif route == "/query":
                response_body = path.encode()
            elif route.startswith("/status/"):
                status = int(route.rsplit("/", 1)[1])
                response_body = f"status {status}".encode()
            elif route == "/options":
                response_body = b"options"
            elif route == "/redirect":
                status = 302
                response_headers["Location"] = "/text"
                response_body = b"redirect"
            if method == "HEAD":
                response_body = b""
            reason = {200: "OK", 201: "Created", 400: "Bad Request", 404: "Not Found", 500: "Internal Server Error"}.get(status, "OK")
            response_headers["Content-Length"] = str(len(response_body))
            response = [f"HTTP/1.1 {status} {reason}\r\n".encode()]
            response.extend(f"{key}: {value}\r\n".encode() for key, value in response_headers.items())
            response.append(b"Connection: close\r\n\r\n")
            response.append(response_body)
            writer.write(b"".join(response))
            await writer.drain()
        finally:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1], requests


async def _stop_http_server(server):
    server.close()
    await server.wait_closed()


async def _run(node, msgs):
    node = {"type": "http request", **node}
    return await run_single_node_with_msgs_ntimes(node, msgs, 1, timeout=5)


@pytest.mark.describe('HTTP Request Node')
class TestHttpRequestNode:
    @pytest.mark.describe('request')
    class TestRequest:
        @pytest.mark.asyncio
        @pytest.mark.it('should get plain text content')
        async def test_0001(self):
                server, port, requests = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/text"}, msgs=[{}])
                    assert msgs[0]["payload"] == "hello world"
                    assert requests[0]["method"] == "GET"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should get JSON content')
        async def test_0002(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/json", "ret": "obj"}, msgs=[{}])
                    assert msgs[0]["payload"] == {"ok": True, "value": 42}
                    assert msgs[0]["statusCode"] == 200
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send the payload as the body of a POST as application/json')
        async def test_0003(self):
                server, port, requests = await _start_http_server()
                payload = {"name": "edgelink", "count": 2}
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "POST", "ret": "txt"}, msgs=[{"payload": payload}])
                    assert json.loads(requests[0]["body"].decode()) == payload
                    assert requests[0]["headers"]["content-type"] == "application/json"
                    assert msgs[0]["payload"] == requests[0]["body"].decode()
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send a payload of 0 as the body of a POST as text/plain')
        async def test_0004(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "POST", "ret": "txt"}, msgs=[{"payload": 0}])
                    assert requests[0]["body"] == b"0"
                    assert requests[0]["headers"]["content-type"].startswith("text/plain")
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send an Object payload as the body of a POST')
        async def test_0005(self):
                server, port, requests = await _start_http_server()
                payload = {"alpha": "one", "answer": 42}
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "POST", "ret": "txt"}, msgs=[{"payload": payload}])
                    assert json.loads(requests[0]["body"].decode()) == payload
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='binary payloads cannot cross the pytest bridge')
        @pytest.mark.it('should send a Buffer as the body of a POST')
        async def test_0006(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='multipart form-data is out of scope for the embedded runtime')
        @pytest.mark.it('should send form-based request')
        async def test_0007(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should send PUT request')
        async def test_0008(self):
                server, port, requests = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "PUT", "ret": "txt"}, msgs=[{"payload": "updated"}])
                    assert requests[0]["method"] == "PUT"
                    assert requests[0]["body"] == b"updated"
                    assert msgs[0]["payload"] == "updated"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send DELETE request')
        async def test_0009(self):
                server, port, requests = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "DELETE", "ret": "txt"}, msgs=[{"payload": "gone"}])
                    assert requests[0]["method"] == "DELETE"
                    assert msgs[0]["payload"] == "gone"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send HEAD request')
        async def test_0010(self):
                server, port, requests = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/head", "method": "HEAD", "ret": "txt"}, msgs=[{}])
                    assert requests[0]["method"] == "HEAD"
                    assert msgs[0]["payload"] == ""
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send PATCH request')
        async def test_0011(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "PATCH", "ret": "txt"}, msgs=[{"payload": "patched"}])
                    assert requests[0]["method"] == "PATCH"
                    assert requests[0]["body"] == b"patched"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send OPTIONS request')
        async def test_0012(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/options", "method": "OPTIONS", "ret": "txt"}, msgs=[{}])
                    assert requests[0]["method"] == "OPTIONS"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='TRACE requests are out of scope for the embedded runtime')
        @pytest.mark.it('should send TRACE request')
        async def test_0013(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='binary payloads cannot cross the pytest bridge')
        @pytest.mark.it('should get Buffer content')
        async def test_0014(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should return plain text when JSON fails to parse')
        async def test_0015(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/invalid-json", "ret": "obj"}, msgs=[{}])
                    assert msgs[0]["payload"] == "not json"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should return the status code')
        async def test_0016(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/status/201", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["statusCode"] == 201
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should use msg.url')
        async def test_0017(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"ret": "txt"}, msgs=[{"url": f"http://127.0.0.1:{port}/text"}])
                    assert msgs[0]["payload"] == "hello world"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='node error events are not exposed by the pytest flow bridge')
        @pytest.mark.it('should output an error when URL is not provided')
        async def test_0018(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should allow the message to provide the url')
        async def test_0019(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"ret": "txt"}, msgs=[{"url": f"http://127.0.0.1:{port}/text"}])
                    assert msgs[0]["payload"] == "hello world"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow the url to contain mustache placeholders')
        async def test_0020(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": "http://127.0.0.1:{{port}}/text", "ret": "txt"}, msgs=[{"port": port}])
                    assert msgs[0]["payload"] == "hello world"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow the url to be missing the http:// prefix')
        async def test_0021(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"127.0.0.1:{port}/text", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["payload"] == "hello world"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='non-HTTP schemes are rejected through node error events unavailable in the pytest bridge')
        @pytest.mark.it('should reject non http:// schemes - node config')
        async def test_0022(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='non-HTTP schemes are rejected through node error events unavailable in the pytest bridge')
        @pytest.mark.it('should reject non http:// schemes - msg.url')
        async def test_0023(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should use msg.method')
        async def test_0024(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "use", "ret": "txt"}, msgs=[{"method": "POST", "payload": "from msg"}])
                    assert requests[0]["method"] == "POST"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should allow the message to provide the method')
        async def test_0025(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/echo", "method": "use", "ret": "txt"}, msgs=[{"method": "PUT", "payload": "from msg"}])
                    assert requests[0]["method"] == "PUT"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should receive msg.responseUrl')
        async def test_0026(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/text", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["responseUrl"] == f"http://127.0.0.1:{port}/text"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should receive msg.responseUrl when redirected')
        async def test_0027(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/redirect", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["payload"] == "hello world"
                    assert msgs[0]["responseUrl"] == f"http://127.0.0.1:{port}/text"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='per-message redirect control is out of scope for the embedded runtime')
        @pytest.mark.it('should prevent following redirect when msg.followRedirects is false')
        async def test_0028(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='timeout error timing is not deterministic in the pytest bridge')
        @pytest.mark.it('should output an error when request timeout occurred')
        async def test_0029(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='timeout error timing is not deterministic in the pytest bridge')
        @pytest.mark.it('should output an error when request timeout occurred when set via msg.requestTimeout')
        async def test_0030(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='request warning events are not exposed by the pytest flow bridge')
        @pytest.mark.it('should show a warning if msg.requestTimeout is not a number')
        async def test_0031(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='request warning events are not exposed by the pytest flow bridge')
        @pytest.mark.it('should show a warning if msg.requestTimeout is negative')
        async def test_0032(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='request warning events are not exposed by the pytest flow bridge')
        @pytest.mark.it('should show a warning if msg.requestTimeout is set to 0')
        async def test_0033(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='request timeout warning events are not exposed by the pytest flow bridge')
        @pytest.mark.it('should pass if response time is faster than timeout set via msg.requestTimeout')
        async def test_0034(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should append query params to url - obj')
        async def test_0035(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/query", "paytoqs": "query", "ret": "txt"}, msgs=[{"payload": {"q": "hello world", "n": 2}}])
                    assert requests[0]["path"].startswith("/query?")
                    assert "q=hello%20world" in requests[0]["path"]
                    assert "n=2" in requests[0]["path"]
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send a msg for non-2xx response status - 400')
        async def test_0036(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/status/400", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["statusCode"] == 400
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send a msg for non-2xx response status - 404')
        async def test_0037(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/status/404", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["statusCode"] == 404
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send a msg for non-2xx response status - 500')
        async def test_0038(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/status/500", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["statusCode"] == 500
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should encode the url to handle special characters')
        async def test_0039(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/query?q=hello world", "ret": "txt"}, msgs=[{}])
                    assert "hello%20world" in requests[0]["path"]
                finally:
                    await _stop_http_server(server)

    @pytest.mark.describe('HTTP header')
    class TestHttpHeader:
        @pytest.mark.asyncio
        @pytest.mark.skip(reason='response cookie parsing is out of scope for the embedded runtime')
        @pytest.mark.it('should receive cookie')
        async def test_0040(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should send cookie with string')
        async def test_0041(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"cookies": {"session": "abc"}}])
                    assert requests[0]["headers"].get("cookie") == "session=abc"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send multiple cookies with string')
        async def test_0042(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"cookies": {"a": "1", "b": "2"}}])
                    assert requests[0]["headers"].get("cookie") in ("a=1; b=2", "b=2; a=1")
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send cookie with object data')
        async def test_0043(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"cookies": {"session": {"value": "abc"}}}])
                    assert requests[0]["headers"].get("cookie") == "session=abc"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send multiple cookies with object data')
        async def test_0044(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"cookies": {"a": {"value": "1"}, "b": {"value": "2"}}}])
                    assert requests[0]["headers"].get("cookie") in ("a=1; b=2", "b=2; a=1")
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='cookie encoding options are out of scope for the embedded runtime')
        @pytest.mark.it('should encode cookie value')
        async def test_0045(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='cookie encoding options are out of scope for the embedded runtime')
        @pytest.mark.it('should encode cookie object')
        async def test_0046(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='cookie encoding options are out of scope for the embedded runtime')
        @pytest.mark.it('should not encode cookie when encode option is false')
        async def test_0047(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should send cookie by msg.headers')
        async def test_0048(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"headers": {"cookie": "session=abc"}}])
                    assert requests[0]["headers"].get("cookie") == "session=abc"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should send multiple cookies by msg.headers')
        async def test_0049(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"headers": {"cookie": "a=1; b=2"}}])
                    assert requests[0]["headers"].get("cookie") == "a=1; b=2"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.it('should convert all HTTP headers into lower case')
        async def test_0050(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["headers"]["x-response-header"] == "ok"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='preserving response header case is out of scope for the embedded runtime')
        @pytest.mark.it('should keep HTTP header case as provided by the user')
        async def test_0051(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should receive HTTP header')
        async def test_0052(self):
                server, port, _ = await _start_http_server()
                try:
                    msgs = await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{}])
                    assert msgs[0]["headers"]["x-response-header"] == "ok"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='Node-RED internal request headers are out of scope for the embedded runtime')
        @pytest.mark.it('should ignore unmodified x-node-red-request-node header')
        async def test_0053(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.it('should use modified msg.headers property')
        async def test_0054(self):
                server, port, requests = await _start_http_server()
                try:
                    await _run(node={"url": f"http://127.0.0.1:{port}/headers", "ret": "txt"}, msgs=[{"headers": {"x-custom": "from-msg"}}])
                    assert requests[0]["headers"]["x-custom"] == "from-msg"
                finally:
                    await _stop_http_server(server)

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='editor UI header configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use ui headers')
        async def test_0055(self):
            pass

    @pytest.mark.describe('protocol')
    class TestProtocol:
        @pytest.mark.asyncio
        @pytest.mark.skip(reason='invalid proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should not use http-proxy-config when invalid url is specified')
        async def test_0056a(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='invalid environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use http_proxy when environment variable is invalid')
        async def test_0056b(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='custom TLS verification is out of scope for the embedded runtime')
        @pytest.mark.it('should use msg.rejectUnauthorized')
        async def test_0056(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='TLS config nodes are out of scope for the embedded runtime')
        @pytest.mark.it('should use tls-config')
        async def test_0057(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='TLS certificate inspection is out of scope for the embedded runtime')
        @pytest.mark.it('should use tls-config and verify serverCert')
        async def test_0058(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='client certificate authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should use tls-config and send client cert')
        async def test_0059(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use env var http_proxy')
        async def test_0060(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use env var https_proxy')
        async def test_0061(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should not use env var http*_proxy when no_proxy is set')
        async def test_0062(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use HTTP_PROXY')
        async def test_0063(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use no_proxy')
        async def test_0064(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='environment proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use NO_PROXY')
        async def test_0065(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='proxy config nodes are out of scope for the embedded runtime')
        @pytest.mark.it('should use http-proxy-config')
        async def test_0066(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='proxy config nodes are out of scope for the embedded runtime')
        @pytest.mark.it('should use http-proxy-config when valid noproxy is specified')
        async def test_0067(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='editor UI proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use UI proxy for statically configured URL')
        async def test_0068(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='editor UI proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use UI proxy for HTTP URL passed in via msg')
        async def test_0069(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='editor UI proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should use UI proxy for HTTPS URL passed in via msg')
        async def test_0070(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='editor UI proxy configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should not use UI proxy if noproxy excludes it')
        async def test_0071(self):
            pass

    @pytest.mark.describe('authentication')
    class TestAuthentication:
        @pytest.mark.asyncio
        @pytest.mark.skip(reason='credential-backed authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - basic')
        async def test_0072(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='credential-backed authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - basic')
        async def test_0073(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='credential-backed authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - bearer')
        async def test_0074(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='proxy authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on proxy server')
        async def test_0075(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='proxy authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should output an error when proxy authentication was failed')
        async def test_0076(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='proxy config nodes are out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on proxy server(http-proxy-config)')
        async def test_0077(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='proxy config nodes are out of scope for the embedded runtime')
        @pytest.mark.it('should output an error when proxy authentication was failed(http-proxy-config)')
        async def test_0078(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='digest authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - digest MD5')
        async def test_0079(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='digest authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - digest MD5 sess')
        async def test_0080(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='digest authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - digest MD5 qop')
        async def test_0081(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='digest authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - digest SHA-256')
        async def test_0082(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='digest authentication is out of scope for the embedded runtime')
        @pytest.mark.it('should authenticate on server - digest SHA-512-256')
        async def test_0083(self):
            pass

    @pytest.mark.describe('file-upload')
    class TestFileUpload:
        @pytest.mark.asyncio
        @pytest.mark.skip(reason='multipart file upload is out of scope for the embedded runtime')
        @pytest.mark.it('should upload a file')
        async def test_0084(self):
            pass

    @pytest.mark.describe('redirect-cookie')
    class TestRedirectCookie:
        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect cookie jars are out of scope for the embedded runtime')
        @pytest.mark.it('should send cookies to the same domain when redirected(no cookies)')
        async def test_0085(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect cookie jars are out of scope for the embedded runtime')
        @pytest.mark.it('should not send cookies to the different domain when redirected(no cookies)')
        async def test_0086(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect cookie jars are out of scope for the embedded runtime')
        @pytest.mark.it('should send cookies to the same domain when redirected(msg.cookies)')
        async def test_0087(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect cookie jars are out of scope for the embedded runtime')
        @pytest.mark.it('should not send cookies to the different domain when redirected(msg.cookies)')
        async def test_0088(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect cookie jars are out of scope for the embedded runtime')
        @pytest.mark.it('should send cookies to the same domain when redirected(msg.headers.cookie)')
        async def test_0089(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect cookie jars are out of scope for the embedded runtime')
        @pytest.mark.it('should not send cookies to the different domain when redirected(msg.headers.cookie)')
        async def test_0090(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='redirect metadata is out of scope for the embedded runtime')
        @pytest.mark.it('should return all redirect information when redirected multiple times')
        async def test_0091(self):
            pass

    @pytest.mark.describe('should parse broken headers')
    class TestBrokenHeaders:
        @pytest.mark.asyncio
        @pytest.mark.skip(reason='insecure HTTP parser configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should accept broken headers')
        async def test_0092(self):
            pass

        @pytest.mark.asyncio
        @pytest.mark.skip(reason='insecure HTTP parser configuration is out of scope for the embedded runtime')
        @pytest.mark.it('should reject broken headers')
        async def test_0093(self):
            pass


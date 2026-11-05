import asyncio
import base64
import socket

import pytest

from tests import TEST_EDGELINLKD_CONFIG, edgelink


async def _flow(node, port, expected_count=1):
    flow = [
        {"id": "0", "type": "tab"},
        {"id": "1", "type": "tcp in", "z": "0", "wires": [["2"]], "port": str(port), **node},
        {"id": "2", "type": "test-once", "z": "0"},
    ]
    return asyncio.ensure_future(edgelink.run_flows_once(expected_count, 5.0, flow, [], TEST_EDGELINLKD_CONFIG))


async def _server_case(node, writes, expected):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0)); port = probe.getsockname()[1]
    task = await _flow({"server": True, "host": "127.0.0.1", **node}, port, len(expected))
    try:
        await asyncio.sleep(0.15)
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        for value in writes:
            writer.write(value)
            await writer.drain()
            await asyncio.sleep(0.03)
        result = await task
        writer.close(); await writer.wait_closed()
    finally:
        if not task.done():
            task.cancel()
    assert [m["payload"] for m in result] == expected


async def _client_case(node, writes, expected):
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0)); port = probe.getsockname()[1]
    connected = asyncio.Event()

    async def peer(reader, writer):
        connected.set()
        for value in writes:
            writer.write(value); await writer.drain(); await asyncio.sleep(0.03)
        await asyncio.sleep(0.1); writer.close()

    server = await asyncio.start_server(peer, "127.0.0.1", port)
    task = await _flow({"server": False, "host": "127.0.0.1", **node}, port, len(expected))
    await asyncio.wait_for(connected.wait(), 2)
    result = await task
    server.close(); await server.wait_closed()
    assert [m["payload"] for m in result] == expected


CASES = [
    ("should recv data (Stream/Buffer)", {"datamode": "stream", "datatype": "buffer", "newline": ""}, [b"foo"], [[102, 111, 111]], False),
    ("should recv data (Stream/String/Delimiter:\\n)", {"datamode": "stream", "datatype": "utf8", "newline": "\n"}, [b"foo\nbar"], ["foo", "bar"], False),
    ("should recv data (Stream/String/Delimiter:o\\n)", {"datamode": "stream", "datatype": "utf8", "newline": "o\n"}, [b"foo\nbar"], ["fo", "bar"], False),
    ("should recv data (Stream/String/Delimiter:o\\n) and reattach o", {"datamode": "stream", "datatype": "utf8", "newline": "o\n", "trim": True}, [b"foo\nbar"], ["foo\n", "bar"], False),
    ("should recv data (Stream/String/No delimiter)", {"datamode": "stream", "datatype": "utf8", "newline": ""}, [b"foo\nbar"], ["foo\nbar"], False),
    ("should recv data (Stream/Base64)", {"datamode": "stream", "datatype": "base64", "newline": ""}, [b"foo"], [base64.b64encode(b"foo").decode()], False),
    ("should recv data (Single/Buffer)", {"datamode": "single", "datatype": "buffer", "newline": ""}, [b"foo"], [[102, 111, 111]], False),
    ("should recv data (Single/String)", {"datamode": "single", "datatype": "utf8", "newline": "\n"}, [b"foo\nbar\nbaz"], ["foo\nbar\nbaz"], False),
    ("should recv data (Stream/Base64)", {"datamode": "single", "datatype": "base64", "newline": ""}, [b"foo"], [base64.b64encode(b"foo").decode()], False),
    ("should recv multiple data (Stream/Buffer)", {"datamode": "stream", "datatype": "buffer", "newline": ""}, [b"foo", b"bar"], [[102,111,111],[98,97,114]], False),
    ("should recv multiple data (Stream/String/Delimiter:\\n)", {"datamode": "stream", "datatype": "utf8", "newline": "\n"}, [b"foo", b"bar\nbaz"], ["foo", "bar", "baz"], False),
    ("should recv multiple data (Stream/String/No delimiter)", {"datamode": "stream", "datatype": "utf8", "newline": ""}, [b"foo", b"bar\nbaz"], ["foobar\nbaz"], False),
    ("should recv multiple data (Stream/Base64)", {"datamode": "stream", "datatype": "base64", "newline": ""}, [b"foo", b"bar"], [base64.b64encode(x).decode() for x in (b"foo", b"bar")], False),
    ("should connect & recv data (Stream/Buffer)", {"datamode": "stream", "datatype": "buffer", "newline": ""}, [b"foo"], [[102,111,111]], True),
    ("should connect & recv data (Stream/String/Delimiter:\\n)", {"datamode": "stream", "datatype": "utf8", "newline": "\n"}, [b"foo\nbar"], ["foo", "bar"], True),
    ("should connect & recv data (Stream/String/No delimiter)", {"datamode": "stream", "datatype": "utf8", "newline": ""}, [b"foo\nbar"], ["foo\nbar"], True),
    ("should connect & recv data (Stream/Base64)", {"datamode": "stream", "datatype": "base64", "newline": ""}, [b"foo"], [base64.b64encode(b"foo").decode()], True),
    ("should connect & recv data (Single/Buffer)", {"datamode": "single", "datatype": "buffer", "newline": ""}, [b"foo"], [[102,111,111]], True),
    ("should connect & recv data (Single/String)", {"datamode": "single", "datatype": "utf8", "newline": "\n"}, [b"foo\nbar\nbaz"], ["foo\nbar\nbaz"], True),
    ("should connect & recv data (Stream/Base64)", {"datamode": "single", "datatype": "base64", "newline": ""}, [b"foo"], [base64.b64encode(b"foo").decode()], True),
]


def _make_case(title, node, writes, expected, client):
    async def test(_self):
        await (_client_case if client else _server_case)(node, writes, expected)
    test.__name__ = "test_" + str(abs(hash(title)))
    test = pytest.mark.asyncio(test)
    test = pytest.mark.it(title)(test)
    return test


@pytest.mark.describe("TCP in Node")
class TestTcpIn:
    pass


for _index, (_title, _node, _writes, _expected, _client) in enumerate(CASES, 1):
    setattr(TestTcpIn, f"test_{_index:04d}", _make_case(_title, _node, _writes, _expected, _client))


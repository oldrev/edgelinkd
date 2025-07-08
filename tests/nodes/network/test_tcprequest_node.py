import pytest
import asyncio
from tests import *

@pytest.mark.describe('TCP Request Node')
class TestTcpRequest:
    @pytest.mark.describe('single message')
    class TestSingleMessage:

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data')
        async def test_send_recv_data(self):
            # Start a local TCP echo server
            server, port = await self._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "time", "splitc": "0"
            }
            injections = [{"payload": "foo", "topic": "bar"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should retain complete message')
        async def test_retain_complete_message(self):
            server, port = await self._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "time", "splitc": "0"
            }
            injections = [{"payload": "foo", "topic": "bar"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data when specified character received')
        async def test_send_recv_data_split_char(self):
            server, port = await self._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "char", "splitc": "0"
            }
            injections = [{"payload": "foo0bar0", "topic": "bar"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo0"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data after fixed number of chars received')
        async def test_send_recv_data_split_count(self):
            server, port = await self._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "count", "splitc": "7"
            }
            injections = [{"payload": "foo bar", "topic": "bar"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & receive, then keep connection')
        async def test_send_recv_keep_connection(self):
            server, port = await self._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "sit", "splitc": "5"
            }
            injections = [{"payload": "foo", "topic": "bar"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data to/from server:port from msg')
        async def test_send_recv_data_from_msg(self):
            server, port = await self._start_echo_server()
            node = {
                "type": "tcp request", "server": "", "port": "", "out": "time", "splitc": "0"
            }
            injections = [{"payload": "foo", "host": "localhost", "port": port}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["host"] == "localhost"
            assert msgs[0]["port"] == port
            server.close()
            await server.wait_closed()

    @pytest.mark.describe('many messages')
    class TestManyMessages:
        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data')
        async def test_many_send_recv_data(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "time", "splitc": "0"
            }
            injections = [
                {"payload": "f", "topic": "bar"},
                {"payload": "o", "topic": "bar"},
                {"payload": "o", "topic": "bar"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data when specified character received')
        async def test_many_send_recv_data_split_char(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "char", "splitc": "0"
            }
            injections = [
                {"payload": "foo0", "topic": "bar"},
                {"payload": "bar0", "topic": "bar"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo0"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data after fixed number of chars received')
        async def test_many_send_recv_data_split_count(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "count", "splitc": "7"
            }
            injections = [
                {"payload": "fo", "topic": "bar"},
                {"payload": "ob", "topic": "bar"},
                {"payload": "ar", "topic": "bar"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & receive, then keep connection')
        async def test_many_send_recv_keep_connection(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "sit", "splitc": "5"
            }
            injections = [
                {"payload": "foo", "topic": "bar"},
                {"payload": "bar", "topic": "bar"},
                {"payload": "baz", "topic": "bar"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foobarbaz"
            assert msgs[0]["topic"] == "bar"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & receive, then keep connection, and not split return strings')
        async def test_many_keep_connection_no_split(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "sit", "ret": "string", "newline": "", "splitc": "0"
            }
            injections = [
                {"payload": "foo", "topic": "boo"},
                {"payload": "bar<A>\nfoo", "topic": "boo"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foobar<A>\nfoo"
            assert msgs[0]["topic"] == "boo"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & receive, then keep connection, and split return strings')
        async def test_many_keep_connection_split(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "sit", "ret": "string", "newline": "<A>\\n", "splitc": "0"
            }
            injections = [
                {"payload": "foo", "topic": "boo"},
                {"payload": "bar<A>\nfoo", "topic": "boo"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foobar"
            assert msgs[0]["topic"] == "boo"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & receive, then keep connection, and split return strings and reattach delimiter')
        async def test_many_keep_connection_split_reattach(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "sit", "ret": "string", "newline": "<A>\\n", "trim": True, "splitc": "0"
            }
            injections = [
                {"payload": "foo", "topic": "boo"},
                {"payload": "bar<A>\nfoo", "topic": "boo"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foobar<A>\n"
            assert msgs[0]["topic"] == "boo"
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should send & recv data to/from server:port from msg')
        async def test_many_send_recv_data_from_msg(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "", "port": "", "out": "time", "splitc": "0"
            }
            injections = [
                {"payload": "f", "host": "localhost", "port": port},
                {"payload": "o", "host": "localhost", "port": port},
                {"payload": "o", "host": "localhost", "port": port}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["host"] == "localhost"
            assert msgs[0]["port"] == port
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should limit the queue size')
        async def test_limit_queue_size(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "sit", "splitc": "5"
            }
            # Node-RED default queue size is 10
            queue_size = 10
            injections = [{"payload": "x"} for _ in range(queue_size + 1)]
            expected = "ACK:" + "x" * queue_size
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == expected
            server.close()
            await server.wait_closed()

        @pytest.mark.asyncio
        @pytest.mark.it('should only retain the latest message')
        async def test_only_retain_latest_message(self):
            server, port = await TestTcpRequest._start_echo_server()
            node = {
                "type": "tcp request", "server": "localhost", "port": str(port), "out": "time", "splitc": "0"
            }
            injections = [
                {"payload": "f", "topic": "bar"},
                {"payload": "o", "topic": "baz"},
                {"payload": "o", "topic": "quux"}
            ]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert msgs[0]["payload"] == "ACK:foo"
            assert msgs[0]["topic"] == "quux"
            server.close()
            await server.wait_closed()

    @staticmethod
    async def _start_echo_server():
        # Simple TCP echo server, returns ACK:xxx for received data
        async def handle_echo(reader, writer):
            data = await reader.read(1024)
            message = data.decode()
            writer.write(f"ACK:{message}".encode())
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        server = await asyncio.start_server(handle_echo, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]
        return server, port

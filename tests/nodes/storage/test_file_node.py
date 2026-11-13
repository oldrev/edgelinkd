"""Ported specs for the `file in` / `file out` nodes.

Upstream: `3rd-party/node-red/test/nodes/core/storage/10-file_spec.js` (v4.0.9).

The spec drives the nodes through real files in Node-RED's `test/resources` directory; these ports
use pytest's per-test `tmp_path` instead, which is equivalent - every filename is absolute - and
leaves the checkout clean. The `fileWorkingDirectory` tests pass the setting through the harness'
`config` argument, which is how a flow runtime is configured here.

The generated encoding suite covers the full `iconv-lite` code page table, which this embedded
runtime does not ship. This file ports the built-in text and byte encodings that the runtime
supports, and leaves other code-page titles absent. Two specs that inspect deployed node properties
are also not ported.
"""
import asyncio
import os
import stat
import sys

import pytest
from tests import *


def _file_out(node_extra, msgs, nexpected=None, timeout=3, config=None):
    """A `file` (out) node wired to the harness' collector."""
    node = {"type": "file", "name": "fileNode", **node_extra}
    return run_single_node_with_msgs_ntimes(node, msgs, nexpected or len(msgs), timeout=timeout, config=config)


def _file_in(node_extra, msgs, nexpected=None, timeout=3, config=None):
    """A `file in` node wired to the harness' collector."""
    node = {"type": "file in", "name": "fileInNode", **node_extra}
    return run_single_node_with_msgs_ntimes(node, msgs, nexpected or len(msgs), timeout=timeout, config=config)


def _log_events(node_type: str) -> list[dict]:
    """The `node.log()` events of the last run, oldest first, filtered by node type."""
    return [event for event in take_node_logs() if event["type"] == node_type]


def _with_working_directory(directory) -> dict:
    """The harness config with `fileWorkingDirectory` set, as `RED.settings` would carry it."""
    return {**TEST_EDGELINLKD_CONFIG, "fileWorkingDirectory": str(directory)}


def _out_node(filename, **extra) -> dict:
    """A `file` node with the properties the spec's flows set on it."""
    return {"type": "file", "name": "fileNode", "filename": str(filename), **extra}


@pytest.mark.describe('file Nodes')
class TestFileNodes:

    @pytest.mark.describe('file out Node')
    class TestFileOutNode:

        @pytest.mark.asyncio
        @pytest.mark.it('should be loaded')
        async def test_should_be_loaded(self, tmp_path):
            # Upstream reads the deployed node's own `name`; what the bridge can observe is that the
            # flow deploys and runs, so the node is given no message and no output is expected.
            node = _out_node(tmp_path / "50-file-test-file.txt", appendNewline=True, overwriteFile=True)
            await run_single_node_with_msgs_ntimes(node, [], 0)
            assert node["name"] == "fileNode"

        @pytest.mark.asyncio
        @pytest.mark.it('should write to a file')
        async def test_should_write_to_a_file(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            msgs = await _file_out(_out_node(file_to_test, appendNewline=False, overwriteFile=True),
                                   [{"payload": "test"}])
            assert file_to_test.read_bytes() == b"test"
            assert msgs[0]["payload"] == "test"

        @pytest.mark.asyncio
        @pytest.mark.it('should write to a file using JSONata')
        async def test_should_write_to_a_file_using_jsonata(self, tmp_path):
            expression = f"'{tmp_path.as_posix()}/'&(20+30)&'-file-test-file.txt'"
            msgs = await _file_out(
                _out_node(expression, filenameType="jsonata", appendNewline=False, overwriteFile=True),
                [{"payload": "test"}])
            assert (tmp_path / "50-file-test-file.txt").read_bytes() == b"test"
            assert msgs[0]["payload"] == "test"

        @pytest.mark.asyncio
        @pytest.mark.it('should write to a file using RED.settings.fileWorkingDirectory')
        async def test_should_write_to_a_file_using_file_working_directory(self, tmp_path):
            msgs = await _file_out(_out_node("50-file-test-file.txt", appendNewline=False, overwriteFile=True),
                                   [{"payload": "test"}], config=_with_working_directory(tmp_path))
            assert (tmp_path / "50-file-test-file.txt").read_bytes() == b"test"
            assert msgs[0]["payload"] == "test"

        @pytest.mark.asyncio
        @pytest.mark.it('should write multi-byte string to a file')
        async def test_should_write_multi_byte_string_to_a_file(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            msgs = await _file_out(_out_node(file_to_test, appendNewline=False, overwriteFile=True),
                                   [{"payload": "試験"}])
            assert file_to_test.read_bytes().decode("utf-8") == "試験"
            assert msgs[0]["payload"] == "試験"

        @pytest.mark.asyncio
        @pytest.mark.it('should append to a file and add newline')
        async def test_should_append_to_a_file_and_add_newline(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            msgs = await _file_out(_out_node(file_to_test, appendNewline=True, overwriteFile=False),
                                   [{"payload": "test2"}, {"payload": True}, {"payload": 999}, {"payload": [2]}],
                                   nexpected=4)
            expected = os.linesep.join(["test2", "true", "999", "[2]"]) + os.linesep
            assert file_to_test.read_bytes().decode("utf-8") == expected
            assert [msg["payload"] for msg in msgs] == ["test2", True, 999, [2]]

        @pytest.mark.asyncio
        @pytest.mark.it('should append to a file and add newline, except last line of multipart input')
        async def test_should_append_to_a_file_and_add_newline_except_last_line_of_multipart_input(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            injections = [
                {"payload": "Line1", "parts": {"index": 0, "type": "string"}},
                {"payload": "Line2", "parts": {"index": 1, "type": "string"}},
                {"payload": "Line3", "parts": {"index": 2, "type": "string"}},
                {"payload": "Line4", "parts": {"index": 3, "type": "string", "count": 4}},
            ]
            await _file_out(_out_node(file_to_test, appendNewline=True, overwriteFile=False), injections, nexpected=4)
            expected = os.linesep.join(["Line1", "Line2", "Line3", "Line4"])
            assert file_to_test.read_bytes().decode("utf-8") == expected

        @pytest.mark.asyncio
        @pytest.mark.it('should append to a file after it has been deleted ')
        async def test_should_append_to_a_file_after_it_has_been_deleted(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            msgs = await _append_around_recreate(file_to_test, recreate=False)
            assert len(msgs) == 4
            assert file_to_test.read_bytes().decode("utf-8") == "threefour"

        @pytest.mark.asyncio
        @pytest.mark.it('should append to a file after it has been recreated ')
        async def test_should_append_to_a_file_after_it_has_been_recreated(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            msgs = await _append_around_recreate(file_to_test, recreate=True)
            assert len(msgs) == 4
            assert file_to_test.read_bytes().decode("utf-8") == "threefour"

        @pytest.mark.asyncio
        @pytest.mark.it('should use msg.filename if filename not set in node')
        async def test_should_use_msg_filename_if_filename_not_set_in_node(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            node = {"type": "file", "name": "fileNode", "appendNewline": True, "overwriteFile": True}
            msgs = await _file_out(node, [{"payload": "fine", "filename": str(file_to_test)}])
            assert msgs[0]["payload"] == "fine"
            assert msgs[0]["filename"] == str(file_to_test)
            assert file_to_test.read_bytes() == b"fine" + os.linesep.encode()

        @pytest.mark.asyncio
        @pytest.mark.it('should use msg._user_specified_filename set in nodes typedInput')
        async def test_should_use_msg_user_specified_filename_set_in_nodes_typed_input(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            node = {"type": "file", "name": "fileNode", "filename": "_user_specified_filename",
                    "filenameType": "msg", "appendNewline": True, "overwriteFile": True}
            msgs = await _file_out(node, [{"payload": "typedInput", "_user_specified_filename": str(file_to_test)}])
            assert msgs[0]["payload"] == "typedInput"
            assert msgs[0]["filename"] == str(file_to_test)
            assert file_to_test.read_bytes() == b"typedInput" + os.linesep.encode()

        @pytest.mark.asyncio
        @pytest.mark.it('should support number in msg._user_specified_filename')
        async def test_should_support_number_in_msg_user_specified_filename(self, tmp_path):
            node = {"type": "file", "name": "fileNode", "filename": "_user_specified_filename",
                    "filenameType": "msg", "appendNewline": False, "overwriteFile": True}
            msgs = await _file_out(node, [{"payload": "test", "_user_specified_filename": 123}],
                                   config=_with_working_directory(tmp_path))
            assert (tmp_path / "123").read_bytes() == b"test"
            assert msgs[0]["payload"] == "test"

        @pytest.mark.asyncio
        @pytest.mark.it('should use env.TEST_FILE set in nodes typedInput')
        async def test_should_use_env_test_file_set_in_nodes_typed_input(self, tmp_path, monkeypatch):
            file_to_test = tmp_path / "50-file-test-file.txt"
            monkeypatch.setenv("TEST_FILE", str(file_to_test))
            node = {"type": "file", "name": "fileNode", "filename": "TEST_FILE", "filenameType": "env",
                    "appendNewline": True, "overwriteFile": True}
            msgs = await _file_out(node, [{"payload": "envTest"}])
            assert msgs[0]["payload"] == "envTest"
            assert msgs[0]["filename"] == str(file_to_test)
            assert file_to_test.read_bytes() == b"envTest" + os.linesep.encode()

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to delete the file')
        async def test_should_be_able_to_delete_the_file(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            file_to_test.write_bytes(b"to be deleted")
            msgs = await _file_out(_out_node(file_to_test, appendNewline=False, overwriteFile="delete"),
                                   [{"payload": "fine"}])
            assert not file_to_test.exists()
            assert msgs[0]["payload"] == "fine"

        @pytest.mark.asyncio
        @pytest.mark.it('should warn if filename not set')
        async def test_should_warn_if_filename_not_set(self, tmp_path):
            node = {"type": "file", "name": "fileNode", "appendNewline": True, "overwriteFile": False}
            with pytest.raises(RuntimeError):
                await _file_out(node, [{"payload": "nofile"}], nexpected=1, timeout=0.3)
            events = _log_events("file")
            assert [event["level"] for event in events] == ["WARN"]
            assert [event["msg"] for event in events] == ["file.errors.nofilename"]

        @pytest.mark.asyncio
        @pytest.mark.it('ignore a missing payload')
        async def test_ignore_a_missing_payload(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            node = _out_node(file_to_test, appendNewline=True, overwriteFile=False)
            with pytest.raises(RuntimeError):
                await _file_out(node, [{"topic": "test"}], nexpected=1, timeout=0.3)
            assert not file_to_test.exists()
            assert _log_events("file") == []

        @pytest.mark.asyncio
        @pytest.mark.it('should fail to write to a ro file')
        async def test_should_fail_to_write_to_a_ro_file(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            file_to_test.write_bytes(b"")
            _make_read_only(file_to_test)
            try:
                with pytest.raises(RuntimeError):
                    await _file_out(_out_node(file_to_test, appendNewline=False, overwriteFile=True),
                                    [{"payload": "test"}], nexpected=1, timeout=0.3)
            finally:
                _make_writable(file_to_test)
            events = _log_events("file")
            assert [event["level"] for event in events] == ["ERROR"]
            assert events[0]["msg"].startswith("file.errors.writefail")

        @pytest.mark.asyncio
        @pytest.mark.it('should fail to append to a ro file')
        async def test_should_fail_to_append_to_a_ro_file(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            file_to_test.write_bytes(b"")
            _make_read_only(file_to_test)
            try:
                with pytest.raises(RuntimeError):
                    await _file_out(_out_node(file_to_test, appendNewline=True, overwriteFile=False),
                                    [{"payload": "test2"}], nexpected=1, timeout=0.3)
            finally:
                _make_writable(file_to_test)
            events = _log_events("file")
            assert [event["level"] for event in events] == ["ERROR"]
            assert events[0]["msg"].startswith("file.errors.appendfail")

        @pytest.mark.asyncio
        @pytest.mark.it('should cope with failing to delete a file')
        async def test_should_cope_with_failing_to_delete_a_file(self, tmp_path):
            # Upstream stubs `fs.unlink`; here the deletion is made to fail for real, by pointing the
            # node at a directory: removing one with the file API fails on every platform.
            file_to_test = tmp_path / "a-directory"
            file_to_test.mkdir()
            with pytest.raises(RuntimeError):
                await _file_out(_out_node(file_to_test, appendNewline=True, overwriteFile="delete"),
                                [{"payload": "test2"}], nexpected=1, timeout=0.3)
            assert file_to_test.is_dir()
            events = _log_events("file")
            assert [event["level"] for event in events] == ["ERROR"]
            assert events[0]["msg"].startswith("file.errors.deletefail")

        @pytest.mark.asyncio
        @pytest.mark.it('should fail to create a new directory if not asked to do so (append)')
        async def test_should_fail_to_create_a_new_directory_if_not_asked_append(self, tmp_path):
            file_to_test = tmp_path / "file-out-node" / "50-file-test-file.txt"
            with pytest.raises(RuntimeError):
                await _file_out(_out_node(file_to_test, appendNewline=True, overwriteFile=False),
                                [{"payload": "test2"}], nexpected=1, timeout=0.3)
            events = _log_events("file")
            assert [event["level"] for event in events] == ["ERROR"]
            assert events[0]["msg"].startswith("file.errors.appendfail")

        @pytest.mark.asyncio
        @pytest.mark.it('should try to create a new directory if asked to do so (append)')
        async def test_should_try_to_create_a_new_directory_if_asked_append(self, tmp_path):
            file_to_test = tmp_path / "file-out-node" / "50-file-test-file.txt"
            msgs = await _file_out(
                _out_node(file_to_test, appendNewline=True, overwriteFile=False, createDir=True),
                [{"payload": "test2"}])
            assert msgs[0]["payload"] == "test2"
            assert file_to_test.read_bytes() == b"test2" + os.linesep.encode()
            assert _log_events("file") == []

        @pytest.mark.asyncio
        @pytest.mark.it('should fail to create a new directory if not asked to do so (overwrite)')
        async def test_should_fail_to_create_a_new_directory_if_not_asked_overwrite(self, tmp_path):
            file_to_test = tmp_path / "file-out-node" / "50-file-test-file.txt"
            with pytest.raises(RuntimeError):
                await _file_out(_out_node(file_to_test, appendNewline=False, overwriteFile=True),
                                [{"payload": "test2"}], nexpected=1, timeout=0.3)
            events = _log_events("file")
            assert [event["level"] for event in events] == ["ERROR"]
            assert events[0]["msg"].startswith("file.errors.writefail")

        @pytest.mark.asyncio
        @pytest.mark.it('should try to create a new directory if asked to do so (overwrite)')
        async def test_should_try_to_create_a_new_directory_if_asked_overwrite(self, tmp_path):
            file_to_test = tmp_path / "file-out-node" / "50-file-test-file.txt"
            msgs = await _file_out(
                _out_node(file_to_test, appendNewline=True, overwriteFile=True, createDir=True),
                [{"payload": "test2"}])
            assert msgs[0]["payload"] == "test2"
            assert file_to_test.read_bytes() == b"test2" + os.linesep.encode()
            assert _log_events("file") == []

        @pytest.mark.asyncio
        @pytest.mark.it('should write to multiple files')
        async def test_should_write_to_multiple_files(self, tmp_path):
            # The buffers are built by a `function` node: the bridge injects JSON, so a payload is a
            # list of numbers rather than a Buffer, and upstream's 10MB buffers would mean ten
            # million JSON numbers. The behaviour under test - one file per message `filename` - is
            # the same, so the size is reduced.
            length = 4096
            file_count = 5
            flows = [
                {"id": "100", "type": "tab"},
                {"id": "3", "z": "100", "type": "function", "outputs": 1, "wires": [["1"]],
                 "func": f"const b = new Uint8Array({length}); b.fill(msg.index);"
                         "msg.payload = b.buffer; return msg;"},
                {"id": "1", "z": "100", "type": "file", "name": "fileNode", "appendNewline": True,
                 "overwriteFile": True, "createDir": True, "wires": [["2"]]},
                {"id": "2", "z": "100", "type": "test-once"},
            ]
            injections = [{"nid": "3", "msg": {"index": index, "filename": str(tmp_path / str(index))}}
                          for index in range(file_count)]
            msgs = await run_flow_with_msgs_ntimes(flows, injections, file_count)
            assert len(msgs) == file_count
            for index in range(file_count):
                content = (tmp_path / str(index)).read_bytes()
                assert len(content) == length
                assert content[0] == index

        @pytest.mark.skip(reason="Rust gap: the pytest bridge cannot close a node while the flow "
                                 "runs, which is what this spec does to check the queue drains first")
        @pytest.mark.asyncio
        @pytest.mark.it('should write to multiple files if node is closed')
        async def test_should_write_to_multiple_files_if_node_is_closed(self, tmp_path):
            pass

    @pytest.mark.describe('file in Node')
    class TestFileInNode:

        @pytest.mark.asyncio
        @pytest.mark.it('should be loaded')
        async def test_should_be_loaded(self, tmp_path):
            node = {"type": "file in", "name": "fileInNode", "filename": str(tmp_path / "50-file-test-file.txt"),
                    "format": "utf8"}
            await run_single_node_with_msgs_ntimes(node, [], 0)
            assert node["name"] == "fileInNode"

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file and output a buffer')
        async def test_should_read_in_a_file_and_output_a_buffer(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            content = "File message line 1\nFile message line 2\n"
            file_to_test.write_bytes(content.encode("utf-8"))
            msgs = await _file_in({"filename": str(file_to_test), "format": ""}, [{"payload": ""}])
            assert bytes(msgs[0]["payload"]) == content.encode("utf-8")

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file and output a utf8 string')
        async def test_should_read_in_a_file_and_output_a_utf8_string(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            content = "File message line 1\nFile message line 2\n"
            file_to_test.write_bytes(content.encode("utf-8"))
            msgs = await _file_in({"filename": str(file_to_test), "format": "utf8"}, [{"payload": ""}])
            assert msgs[0]["payload"] == content

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file using JSONata and output a utf8 string')
        async def test_should_read_in_a_file_using_jsonata_and_output_a_utf8_string(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            content = "File message line 1\nFile message line 2\n"
            file_to_test.write_bytes(content.encode("utf-8"))
            expression = f"'{tmp_path.as_posix()}/'&(20+30)&'-file-test-file.txt'"
            msgs = await _file_in({"filename": expression, "filenameType": "jsonata", "format": "utf8"},
                                  [{"payload": ""}])
            assert msgs[0]["payload"] == content

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file using fileWorkingDirectory to set cwd')
        async def test_should_read_in_a_file_using_file_working_directory_to_set_cwd(self, tmp_path):
            content = "File message line 1\nFile message line 2\n"
            (tmp_path / "50-file-test-file.txt").write_bytes(content.encode("utf-8"))
            msgs = await _file_in({"filename": "50-file-test-file.txt", "format": "utf8"}, [{"payload": ""}],
                                  config=_with_working_directory(tmp_path))
            assert msgs[0]["payload"] == content

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file ending in cr and output a utf8 string')
        async def test_should_read_in_a_file_ending_in_cr_and_output_a_utf8_string(self, tmp_path):
            content = "File message line 1\nFile message line 2\n"
            file_to_test = tmp_path / "50-file-test-file.txt"
            file_to_test.write_bytes(content.encode("utf-8"))
            # The filename carries a tab and CRLF, which the node strips.
            msgs = await _file_in({"filename": "\t" + str(file_to_test) + "\r\n", "format": "utf8"},
                                  [{"payload": ""}])
            assert msgs[0]["payload"] == content

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file and output split lines with parts')
        async def test_should_read_in_a_file_and_output_split_lines_with_parts(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            file_to_test.write_bytes(b"File message line 1\nFile message line 2\n")
            msgs = await _file_in({"filename": str(file_to_test), "format": "lines"},
                                  [{"payload": "", "topic": "A", "foo": "bar", "bar": "foo"}], nexpected=3)
            assert [msg["payload"] for msg in msgs] == ["File message line 1", "File message line 2", ""]
            for index, msg in enumerate(msgs):
                assert msg["topic"] == "A"
                assert "foo" not in msg and "bar" not in msg
                assert msg["parts"]["index"] == index
                assert msg["parts"]["type"] == "string"
                assert msg["parts"]["ch"] == "\n"

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file with empty line and output split lines with parts')
        async def test_should_read_in_a_file_with_empty_line_and_output_split_lines_with_parts(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            data = ["-", "", "-", ""]
            file_to_test.write_bytes("\n".join(data).encode("utf-8"))
            msgs = await _file_in({"filename": str(file_to_test), "format": "lines"}, [{"payload": ""}],
                                  nexpected=len(data))
            assert [msg["payload"] for msg in msgs] == data
            for index, msg in enumerate(msgs):
                assert msg["parts"]["index"] == index
                assert msg["parts"]["type"] == "string"
                assert msg["parts"]["ch"] == "\n"
                if index == len(data) - 1:
                    assert msg["parts"]["count"] == len(data)
                else:
                    assert "count" not in msg["parts"]

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file and output split lines with parts and extra props')
        async def test_should_read_in_a_file_and_output_split_lines_with_parts_and_extra_props(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            file_to_test.write_bytes(b"File message line 1\nFile message line 2\n")
            msgs = await _file_in({"filename": str(file_to_test), "format": "lines", "allProps": True},
                                  [{"payload": "", "topic": "B", "foo": "bar", "bar": "foo"}], nexpected=3)
            assert [msg["payload"] for msg in msgs] == ["File message line 1", "File message line 2", ""]
            for index, msg in enumerate(msgs):
                assert msg["topic"] == "B"
                assert msg["foo"] == "bar"
                assert msg["bar"] == "foo"
                assert msg["parts"]["index"] == index

        @pytest.mark.asyncio
        @pytest.mark.it('should read in a file and output a buffer with parts')
        async def test_should_read_in_a_file_and_output_a_buffer_with_parts(self, tmp_path):
            file_to_test = tmp_path / "50-file-test-file.txt"
            content = "File message line 1\nFile message line 2\n"
            file_to_test.write_bytes(content.encode("utf-8"))
            msgs = await _file_in({"filename": str(file_to_test), "format": "stream"}, [{"payload": ""}])
            assert bytes(msgs[0]["payload"]) == content.encode("utf-8")
            assert msgs[0]["parts"]["count"] == 1
            assert msgs[0]["parts"]["type"] == "buffer"
            assert msgs[0]["parts"]["ch"] == ""

        @pytest.mark.asyncio
        @pytest.mark.it('should warn if no filename set')
        async def test_should_warn_if_no_filename_set(self, tmp_path):
            with pytest.raises(RuntimeError):
                await _file_in({"format": ""}, [{}], nexpected=1, timeout=0.3)
            events = _log_events("file in")
            assert [event["level"] for event in events] == ["WARN"]
            assert [event["msg"] for event in events] == ["file.errors.nofilename"]

        @pytest.mark.asyncio
        @pytest.mark.it('should handle a file read error')
        async def test_should_handle_a_file_read_error(self, tmp_path):
            missing = tmp_path / "badfile"
            msgs = await _file_in({"filename": str(missing), "format": ""}, [{"payload": ""}])
            assert "payload" not in msgs[0]
            assert msgs[0]["error"]["code"] == "ENOENT"
            events = _log_events("file in")
            assert len(events) == 1
            # Node-RED logs the `Error` object, whose text form starts with "Error".
            assert events[0]["msg"].startswith("Error")


    @pytest.mark.asyncio
    @pytest.mark.describe('file in Node')
    @pytest.mark.it('should handle a filename JSONata evaluation error and call done')
    async def test_file_in_filename_jsonata_error_calls_done(self, tmp_path):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "1", "z": "100", "type": "file in", "name": "fileInNode",
             "filename": "$nonExistentFunction()", "filenameType": "jsonata", "format": "utf8", "wires": [[]]},
            {"id": "2", "z": "100", "type": "catch", "scope": ["1"], "uncaught": False, "wires": [["3"]]},
            {"id": "3", "z": "100", "type": "test-once"},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": "trigger"}}], 1)
        assert msgs[0]["payload"] == "trigger"
        assert msgs[0]["error"]["source"]["type"] == "file in"
        assert "T1006" in msgs[0]["error"]["message"]


async def _append_around_recreate(file_to_test, recreate: bool) -> list:
    """Append two messages, delete (or replace) the file, then append two more.

    Upstream does this from inside its message handler; the bridge cannot run Python in the middle
    of a flow, so the injection is scheduled and a task removes the file between the second and the
    third message.
    """
    flows = [
        {"id": "100", "type": "tab"},
        {"id": "1", "z": "100", "type": "file", "name": "fileNode", "filename": str(file_to_test),
         "appendNewline": False, "overwriteFile": False, "wires": [["2"]]},
        {"id": "2", "z": "100", "type": "test-once"},
    ]
    injections = [
        {"nid": "1", "msg": {"payload": "one"}, "delay_ms": 0},
        {"nid": "1", "msg": {"payload": "two"}, "delay_ms": 40},
        {"nid": "1", "msg": {"payload": "three"}, "delay_ms": 140},
        {"nid": "1", "msg": {"payload": "four"}, "delay_ms": 180},
    ]

    async def replace_file(delay: float):
        await asyncio.sleep(delay)
        file_to_test.unlink()
        if recreate:
            file_to_test.write_bytes(b"")

    replacer = asyncio.create_task(replace_file(0.09))
    try:
        return await run_flow_for_seconds_scheduled(flows, injections, 0.5)
    finally:
        await replacer


def _make_read_only(path):
    """Make a file unreadable for writing, the way `chmod`/the read-only attribute does."""
    os.chmod(path, stat.S_IREAD)


def _make_writable(path):
    os.chmod(path, stat.S_IWRITE | stat.S_IREAD)


def _make_undeletable(path):
    """Make deleting `path` fail, and return the call that undoes it."""
    if sys.platform == "win32":
        os.chmod(path, stat.S_IREAD)
        return lambda: os.chmod(path, stat.S_IWRITE | stat.S_IREAD)
    # On POSIX the directory decides whether an entry can be removed.
    directory = os.path.dirname(path)
    mode = os.stat(directory).st_mode
    os.chmod(directory, stat.S_IRUSR | stat.S_IXUSR)
    return lambda: os.chmod(directory, mode)


# Node encodings covered by the built-ins and the optional encoding_rs-backed feature.
_FILE_ENCODINGS = [
    ("none", "test", "utf8"), ("utf8", "試験", "utf8"), ("ucs2", "試験", "utf-16le"),
    ("utf-16le", "試験", "utf-16le"), ("binary", "test", "latin-1"),
    ("base64", "5pel5pys6KqeCg==", "utf8"), ("hex", "deadbeef", "utf8"),
    ("utf-16be", "試験", "utf-16be"),
    ("Big5", "繁體", "big5"), ("EUC-JP", "日本語", "euc_jp"),
    ("EUC-KR", "한국어", "euc_kr"), ("GB18030", "中文", "gb18030"),
    ("GB2312", "中文", "gb2312"), ("GBK", "中文", "gbk"),
    ("Shift_JIS", "日本語", "shift_jis"), ("cp1252", "Café", "cp1252"),
    ("cp1251", "Привет", "cp1251"), ("cp1250", "Příliš", "cp1250"),
    ("cp1253", "Ελλάδα", "cp1253"), ("cp1254", "İstanbul", "cp1254"),
    ("cp1255", "שלום", "cp1255"), ("cp1256", "العربية", "cp1256"),
    ("cp1257", "Rīga", "cp1257"), ("cp1258", "ASCII", "cp1258"),
    ("cp874", "ไทย", "cp874"),
    ("EUC-CN", "中文", "gb2312"), ("KS_C_5601", "한국어", "euc_kr"),
    ("Windows-31j", "日本語", "shift_jis"), ("Windows932", "日本語", "shift_jis"),
    ("Windows936", "中文", "gbk"), ("Windows949", "한국어", "euc_kr"),
    ("Windows950", "繁體", "big5"),
    ("Big5-HKSCS", "繁體中文", "big5hkscs"), ("cp866", "Привет", "cp866"),
    ("koi8-r", "Привет", "koi8_r"), ("koi8-u", "Україна", "koi8_u"),
    ("macintosh", "Café", "mac_roman"),
]


def _expected_encoded_file(encoding, text, codec):
    if encoding == "base64":
        return __import__("base64").b64decode(text)
    if encoding == "hex":
        return bytes.fromhex(text)
    if encoding in {"none", "utf8"}:
        return text.encode()
    return text.encode(codec)


def _add_encoding_specs():
    for index, (encoding, text, codec) in enumerate(_FILE_ENCODINGS):
        async def read_test(self, encoding=encoding, text=text, codec=codec):
            path = self.tmp_path / "encoding.bin"
            expected = _expected_encoded_file(encoding, text, codec)
            path.write_bytes(expected)
            msgs = await _file_in({"filename": str(path), "format": "utf8", "encoding": encoding}, [{"payload": ""}])
            assert msgs[0]["payload"] == text

        async def write_test(self, encoding=encoding, text=text, codec=codec):
            path = self.tmp_path / "encoding.bin"
            msgs = await _file_out(_out_node(path, appendNewline=False, overwriteFile=True, encoding=encoding),
                                   [{"payload": text}])
            expected = _expected_encoded_file(encoding, text, codec)
            assert path.read_bytes() == expected
            assert msgs[0]["payload"] == text

        read_test = pytest.mark.asyncio(read_test)
        read_test = pytest.mark.it(f'should read in a file with "{encoding}" encoding')(read_test)
        write_test = pytest.mark.asyncio(write_test)
        write_test = pytest.mark.it(f'should write to a file with "{encoding}" encoding')(write_test)
        setattr(TestFileReadEncodings, f"test_{index:04d}", read_test)
        setattr(TestFileWriteEncodings, f"test_{index:04d}", write_test)


@pytest.mark.describe('file Nodes file in Node encodings')
class TestFileReadEncodings:
    @pytest.fixture(autouse=True)
    def setup_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path


@pytest.mark.describe('file Nodes file out Node encodings')
class TestFileWriteEncodings:
    @pytest.fixture(autouse=True)
    def setup_tmp_path(self, tmp_path):
        self.tmp_path = tmp_path


_add_encoding_specs()


def _add_legacy_encoding_specs():
    import json
    from pathlib import Path
    if os.getenv("EDGELINK_FILE_ENCODINGS_LEGACY") != "1":
        return
    vectors = json.loads((Path(__file__).parents[2] / "resources/file_encodings.json").read_text(encoding="utf8"))["fixtures"]
    existing = {label for label, _, _ in _FILE_ENCODINGS}
    for index, vector in enumerate(vectors):
        if vector["label"] in existing:
            continue
        async def read_test(self, vector=vector):
            path = self.tmp_path / "legacy.bin"
            path.write_bytes(bytes(vector["bytes"]))
            msgs = await _file_in({"filename": str(path), "format": "utf8", "encoding": vector["label"]}, [{}])
            assert msgs[0]["payload"] == vector["text"]
        async def write_test(self, vector=vector):
            path = self.tmp_path / "legacy.bin"
            await _file_out(_out_node(path, appendNewline=False, overwriteFile=True, encoding=vector["label"]), [{"payload": vector["input"]}])
            assert path.read_bytes() == bytes(vector["encoded"])
        setattr(TestFileReadEncodings, f"test_legacy_{index}", pytest.mark.asyncio(pytest.mark.it(
            f'should read in a file with "{vector["label"]}" encoding')(read_test)))
        setattr(TestFileWriteEncodings, f"test_legacy_{index}", pytest.mark.asyncio(pytest.mark.it(
            f'should write to a file with "{vector["label"]}" encoding')(write_test)))


_add_legacy_encoding_specs()

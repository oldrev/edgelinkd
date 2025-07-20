
import os
import tempfile
import pytest
import aiofiles
import aiofiles.os
from tests import *

@pytest.mark.describe('file Nodes')
class TestFileNodes:

    @pytest.mark.describe('file out Node')
    class TestFileOutNode:
        relative_path_to_file = "50-file-test-file.txt"
        resources_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "resources"))
        file_to_test = os.path.join(resources_dir, relative_path_to_file)

        @pytest.mark.skip
        @pytest.mark.asyncio
        @pytest.mark.it('should be loaded')
        async def test_should_be_loaded(self):
            node = {"type": "file", "name": "fileNode", "filename": self.file_to_test, "appendNewline": True, "overwriteFile": True}
            flow = [node]
            msgs = await run_single_node_with_msgs_ntimes(node, [], 0)
            assert isinstance(node, dict)

        @pytest.mark.asyncio
        @pytest.mark.it('should write to a file')
        async def test_should_write_to_a_file(self):
            node = {"type": "file", "name": "fileNode", "filename": self.file_to_test, "appendNewline": False, "overwriteFile": True}
            injections = [{"payload": "hello world", "topic": "foo"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert await aiofiles.os.path.exists(self.file_to_test)
            async with aiofiles.open(self.file_to_test, "r", encoding="utf-8") as f:
                content = await f.read()
            assert "hello world" in content

        @pytest.mark.asyncio
        @pytest.mark.it('should append to a file and add newline')
        async def test_should_append_to_file_and_add_newline(self):
            node = {"type": "file", "name": "fileNode", "filename": self.file_to_test, "appendNewline": True, "overwriteFile": False}
            injections = [{"payload": "line1"}, {"payload": "line2"}]
            if await aiofiles.os.path.exists(self.file_to_test):
                await aiofiles.os.remove(self.file_to_test)
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 2)
            async with aiofiles.open(self.file_to_test, "r", encoding="utf-8") as f:
                content = await f.read()
            assert content.startswith("line1\n")
            assert "line2" in content

        @pytest.mark.asyncio
        @pytest.mark.it('should be able to delete the file')
        async def test_should_be_able_to_delete_file(self):
            node = {"type": "file", "name": "fileNode", "filename": self.file_to_test, "action": "delete"}
            async with aiofiles.open(self.file_to_test, "w", encoding="utf-8") as f:
                await f.write("to be deleted")
            assert await aiofiles.os.path.exists(self.file_to_test)
            msgs = await run_single_node_with_msgs_ntimes(node, [{"payload": None}], 1)
            assert not await aiofiles.os.path.exists(self.file_to_test)

        @pytest.mark.asyncio
        @pytest.mark.it('should warn if filename not set')
        async def test_should_warn_if_filename_not_set(self):
            node = {"type": "file", "name": "fileNode"}
            with pytest.raises(Exception):
                await run_single_node_with_msgs_ntimes(node, [{"payload": "data"}], 1)

        @pytest.mark.asyncio
        @pytest.mark.it('ignore a missing payload')
        async def test_ignore_missing_payload(self):
            node = {"type": "file", "name": "fileNode", "filename": self.file_to_test}
            msgs = await run_single_node_with_msgs_ntimes(node, [{}], 1)
            exists = await aiofiles.os.path.exists(self.file_to_test)
            size = 0
            if exists:
                stat = await aiofiles.os.stat(self.file_to_test)
                size = stat.st_size
            assert not exists or size == 0

        @pytest.mark.asyncio
        @pytest.mark.it('should use msg.filename if filename not set in node')
        async def test_should_use_msg_filename(self):
            node = {"type": "file", "name": "fileNode"}
            testfile = os.path.join(self.resources_dir, "msg-filename.txt")
            if await aiofiles.os.path.exists(testfile):
                await aiofiles.os.remove(testfile)
            injections = [{"payload": "msg file", "filename": testfile}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert await aiofiles.os.path.exists(testfile)
            async with aiofiles.open(testfile, "r", encoding="utf-8") as f:
                content = await f.read()
            assert "msg file" in content

        @pytest.mark.asyncio
        @pytest.mark.it('should use msg._user_specified_filename set in nodes typedInput')
        async def test_should_use_msg_user_specified_filename(self):
            node = {"type": "file", "name": "fileNode"}
            testfile = os.path.join(self.resources_dir, "user-specified.txt")
            if await aiofiles.os.path.exists(testfile):
                await aiofiles.os.remove(testfile)
            injections = [{"payload": "user file", "_user_specified_filename": testfile}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert await aiofiles.os.path.exists(testfile)
            async with aiofiles.open(testfile, "r", encoding="utf-8") as f:
                content = await f.read()
            assert "user file" in content

        @pytest.mark.asyncio
        @pytest.mark.it('should use env.TEST_FILE set in nodes typedInput')
        async def test_should_use_env_test_file(self, monkeypatch):
            node = {"type": "file", "name": "fileNode", "filename": "${TEST_FILE}"}
            testfile = os.path.join(self.resources_dir, "env-file.txt")
            monkeypatch.setenv("TEST_FILE", testfile)
            if await aiofiles.os.path.exists(testfile):
                await aiofiles.os.remove(testfile)
            injections = [{"payload": "env file"}]
            msgs = await run_single_node_with_msgs_ntimes(node, injections, 1)
            assert await aiofiles.os.path.exists(testfile)
            async with aiofiles.open(testfile, "r", encoding="utf-8") as f:
                content = await f.read()
            assert "env file" in content

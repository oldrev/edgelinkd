import asyncio
import json
import os
import platform
import subprocess
import signal
import copy
import pytest
import importlib.util
import shutil

TEST_EDGELINLKD_CONFIG = {
    "runtime": {
        "context": {
            "default": "memory",
            "stores": {
                "memory": {"provider": "memory"},
                "memory0": {"provider": "memory"},
                "memory1": {"provider": "memory"},
                "memory2": {"provider": "memory"},
            }
        }
    }
}

class EdgelinkError(Exception):
    def __init__(self, message: str, output: bytes):
        self.message = message
        self.output = output

    def __str__(self):
        return f'EdgeLink Error: {self.message}, output: \n{self.output}'


def load_edgelink_mod():

    script_dir = os.path.dirname(os.path.abspath(__file__))
    target = os.getenv('EDGELINK_BUILD_TARGET', '')
    profile = os.getenv('EDGELINK_BUILD_PROFILE', 'debug')

    target_directory = os.path.join(
        script_dir, '..', 'target', target, profile)

    # Determine the operating system and choose the appropriate module name
    if platform.system() == 'Windows':
        # On Windows, Python extensions must have .pyd extension
        # Copy .dll to .pyd only if .pyd is older than .dll
        pyd_path = os.path.join(target_directory, 'edgelink_pymod.pyd')
        dll_path = os.path.join(target_directory, 'edgelink_pymod.dll')
        
        if os.path.exists(dll_path):
            should_copy = False
            if not os.path.exists(pyd_path):
                # .pyd doesn't exist, copy from .dll
                should_copy = True
            else:
                # Both files exist, check modification times
                dll_mtime = os.path.getmtime(dll_path)
                pyd_mtime = os.path.getmtime(pyd_path)
                if dll_mtime > pyd_mtime:
                    # .dll is newer than .pyd, copy it
                    should_copy = True
            
            if should_copy:
                import shutil
                try:
                    shutil.copy2(dll_path, pyd_path)
                    print(f"Copied {dll_path} to {pyd_path} (dll is newer)")
                except Exception as e:
                    raise IOError(f"Failed to copy .dll to .pyd: {e}")
            
            module_path = pyd_path
        elif os.path.exists(pyd_path):
            # Only .pyd exists, use it
            module_path = pyd_path
        else:
            raise IOError(f"Module file not found. Tried: {dll_path}, {pyd_path}")
    else:
        # On Unix-like systems
        module_path = os.path.join(target_directory, 'libedgelink_pymod.so')
        if not os.path.exists(module_path):
            raise IOError(f"Module file not found: {module_path}")

    spec = importlib.util.spec_from_file_location("edgelink_pymod", module_path)
    if spec == None:
        raise RuntimeError(f"Bad Python module!")
    edgelink = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(edgelink)
    return edgelink


edgelink = load_edgelink_mod()

"""
async def start_edgelink_process(el_args: list[str]):
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Determine the operating system and choose the appropriate executable name
    if platform.system() == 'Windows':
        createion_flags = subprocess.CREATE_NEW_PROCESS_GROUP
        myprog_name = 'edgelinkd.exe'
    else:
        createion_flags = 0
        myprog_name = 'edgelinkd'

    target = os.getenv('EDGELINK_BUILD_TARGET', '')
    profile = os.getenv('EDGELINK_BUILD_PROFILE', 'debug')

    myprog_path = os.path.join(
        script_dir, '..', 'target', target, profile, myprog_name)

    qemu_cmd = os.getenv("EDGELINK_QEMU_CMD", None)
    toolchain_triple = os.getenv("EDGELINK_TOOLCHAIN_TRIPLE", None)

    if qemu_cmd and toolchain_triple:
        el_args = ["-L", f"/usr/{toolchain_triple}", myprog_path] + el_args
        myprog_path = qemu_cmd

    process = await asyncio.create_subprocess_exec(
        myprog_path, *el_args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        stdin=asyncio.subprocess.PIPE,
        creationflags=createion_flags
    )
    if not process:
        pytest.exit("Run EdgeLink process failed!")
    return process


async def read_json_from_process(process, nexpected: int, timeout=5):
    # Read from the process's stdout
    all_output = bytearray()
    buffer = ''
    counter = 0
    while True:
        line = await asyncio.wait_for(process.stdout.readline(), timeout)
        if not line:
            break
        print(f"Received> {line}")
        all_output.extend(line)
        buffer += line.decode('utf-8')

        # Look for delimiters \x1E and \n
        while '\x1E' in buffer:
            start, rest = buffer.split('\x1E', 1)
            if '\n' in rest:
                json_str, buffer = rest.split('\n', 1)
                try:
                    json_obj = json.loads(json_str)
                    counter += 1
                    yield json_obj
                    if counter >= nexpected:
                        if platform.system() == 'Windows':
                            # Send CTRL+C signal
                            process.send_signal(signal.CTRL_BREAK_EVENT)
                        else:
                            process.send_signal(signal.SIGINT)
                        # Wait for the process to respond and exit
                        await process.wait()  # Wait for the process to finish
                        return
                except json.JSONDecodeError as e:
                    raise EdgelinkError(
                        f"JSON decode error: {e}", bytes(all_output))
            else:
                break


async def _run_edgelink_with_stdin(input_data: bytes, nexpected: int, timeout=5) -> tuple[bytes, list[dict]]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    el_home_dir = os.path.join(script_dir, 'home')
    el_args = ['-v', '0', '--stdin', '--home', el_home_dir]
    msgs = []
    all_output = bytearray()
    try:
        process = await start_edgelink_process(el_args)
        process.stdin.write(input_data)
        process.stdin.close()
        async for msg in read_json_from_process(process, nexpected, timeout):
            msgs.append(msg)
        return (bytes(all_output), msgs)
    except Exception as e:
        print(f"An error occurred: {e}, killing processing...")
        process.kill()
        raise e
    finally:
        await process.wait()


async def run_edgelink_with_stdin(input_data: bytes, nexpected: int, timeout=5) -> list[dict]:
    result = await asyncio.wait_for(_run_edgelink_with_stdin(input_data, nexpected, timeout), timeout)
    return result[1]


async def run_edgelink(flows_path: str, nexpected: int, timeout: float = 5) -> list[dict]:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    el_home_dir = os.path.join(script_dir, 'home')
    el_args = ['-v', '0', flows_path, '--home', el_home_dir]
    msgs = []
    try:
        process = await start_edgelink_process(el_args)
        async with asyncio.timeout(timeout):
            async for i in read_json_from_process(process, nexpected, timeout):
                msgs.append(i)
            return msgs
    except asyncio.TimeoutError:
        print("Timeout occurred, killing the process.")
        process.kill()
        raise
    except BaseException as e:
        print(f"An error occurred: {e}")
        process.kill()
        raise e
    finally:
        await process.wait()

"""

async def run_with_single_node_ntimes(payload_type: str | None, payload, node_json: object,
                                      nexpected: int, once: bool = True, topic: str | None = None):
    inject = {
        "id": "1",
        "type": "inject",
        "z": "0",
        "name": "",
        "props": [],  # [{"p": "payload"}, {"p": "topic", "vt": "str"}],
        "repeat": once and '' or '0',
        "crontab": "",
        "once": once,
        "onceDelay": 0,
        "topic": topic,
        "wires": [["2"]]
    }
    if payload != None:
        inject['props'].append({'p': 'payload'})
        inject["payload"] = str(payload)
        inject["payloadType"] = payload_type
    if topic != None:
        inject['props'].append({'p': 'topic', 'vt': 'str'})
    user_node = copy.deepcopy(node_json)
    user_node["id"] = "2"
    user_node["z"] = "0"
    if 'wires' not in node_json:
        user_node["wires"] = [["3"]]
    console_node = {"id": "3", "type": "test-once", "z": "0"}
    final_flows_json = [{"id": "0", "type": "tab"},
                        inject, user_node, console_node]
    msgs = await edgelink.run_flows_once(nexpected, 3.0, final_flows_json, [], TEST_EDGELINLKD_CONFIG)
    return msgs


async def run_flow_with_msgs_ntimes(flows_obj: list[object],
                                    msgs: list[object] | None,
                                    nexpected: int, injectee_node_id: str = '1', timeout: float = 3) -> list[object]:
    msgs_to_inject = []
    for msg in msgs:
        msg_injection = None
        if 'nid' in msg and 'msg' in msg:  # We got a raw injection
            msg_injection = (msg['nid'], msg['msg'])
        else:
            msg_injection = (injectee_node_id, msg)
        msgs_to_inject.append(msg_injection)
    msgs = await edgelink.run_flows_once(nexpected, timeout, flows_obj, msgs_to_inject, TEST_EDGELINLKD_CONFIG)
    return msgs


async def run_single_node_with_msgs_ntimes(node_json: object, msgs: list[object] | None,
                                           nexpected: int, injectee_node_id: str = '1', timeout: float = 3):
    user_node = copy.deepcopy(node_json)
    user_node["id"] = "1"
    user_node["z"] = "0"
    if 'wires' not in node_json:
        user_node["wires"] = [["2"]]
    console_node = {"id": "2", "type": "test-once", "z": "0"}
    final_flows_json = [{"id": "0", "type": "tab"}, user_node, console_node]
    return await run_flow_with_msgs_ntimes(final_flows_json, msgs, nexpected, injectee_node_id, timeout)

import asyncio
import hashlib
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

_RED_ID_NAMES: dict[str, str] = {}


def red_id(name: str) -> str:
    """Map an upstream Node-RED spec node id onto a 16-digit hex `ElementId`.

    `ElementId` is a `u64` parsed with `u64::from_str_radix(_, 16)`, so an id may be at most
    16 hex digits (and `to_string()` prints exactly 16). Hex-encoding the upstream name
    therefore only works for names up to 8 bytes long: `helperNode1` becomes 22 digits and is
    rejected with "failed to parse ElementId". Hash the name into 64 bits instead —
    deterministic, always 16 digits, and the upstream name stays readable at the call site.
    Convert the id *and every reference to it* (`z`, `wires`, `scope`, injection targets):

        flows = [
            {"id": red_id("s1"), "type": "split", "z": red_id("tab"), "wires": [[red_id("j1")]]},
            {"id": red_id("j1"), "type": "join", "z": red_id("tab"), "wires": [[red_id("helperNode1")]]},
            {"id": red_id("helperNode1"), "type": "test-once", "z": red_id("tab")},
        ]

    Two different names sharing an id would make the engine reject the flow with "This flow
    node already existed", so a collision is raised here where the cause is obvious. The
    helpers that build a flow for you (`run_single_node_with_msgs_ntimes` and friends) assign
    "1"/"2"/"3" themselves, so this is only needed for hand-written flows.
    """
    digest = hashlib.blake2b(name.encode("utf-8"), digest_size=8).hexdigest()
    previous = _RED_ID_NAMES.setdefault(digest, name)
    if previous != name:
        raise AssertionError(f"red_id() collision: {previous!r} and {name!r} both map to {digest}")
    return digest


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
                                    nexpected: int, injectee_node_id: str = '1', timeout: float = 3,
                                    config: dict | None = None) -> list[object]:
    msgs_to_inject = []
    for msg in msgs:
        msg_injection = None
        if 'nid' in msg and 'msg' in msg:  # We got a raw injection
            msg_injection = (msg['nid'], msg['msg'])
        else:
            msg_injection = (injectee_node_id, msg)
        msgs_to_inject.append(msg_injection)
    msgs = await edgelink.run_flows_once(nexpected, timeout, flows_obj, msgs_to_inject,
                                         TEST_EDGELINLKD_CONFIG if config is None else config)
    return msgs


async def run_single_node_with_msgs_ntimes(node_json: object, msgs: list[object] | None,
                                           nexpected: int, injectee_node_id: str = '1', timeout: float = 3,
                                           config: dict | None = None):
    user_node = copy.deepcopy(node_json)
    user_node["id"] = "1"
    user_node["z"] = "0"
    if 'wires' not in node_json:
        user_node["wires"] = [["2"]]
    console_node = {"id": "2", "type": "test-once", "z": "0"}
    final_flows_json = [{"id": "0", "type": "tab"}, user_node, console_node]
    return await run_flow_with_msgs_ntimes(final_flows_json, msgs, nexpected, injectee_node_id, timeout, config)


async def run_flow_for_seconds(flows_obj: list[object], msgs: list[object] | None,
                               seconds: float, injectee_node_id: str = '1') -> list[object]:
    """Inject the messages and collect every output emitted during `seconds` seconds.

    Mirrors Node-RED's spec helper: inject a burst, sample for `runtimeInMillis`, then count
    whatever arrived. Unlike the `*_ntimes` harness above - which runs until a message count
    is reached - this samples for a duration. Every returned message therefore carries an
    extra `_arrival_ms` field: its arrival offset relative to the first output (so the first
    one is always 0.0), which is how upstream checks the spacing between messages.
    """
    return await run_flow_for_seconds_scheduled(flows_obj, msgs, seconds, injectee_node_id)


async def run_flow_for_seconds_scheduled(flows_obj: list[object], msgs: list[object] | None,
                                         seconds: float, injectee_node_id: str = '1') -> list[object]:
    """`run_flow_for_seconds`, but each message is delivered after its own delay.

    A message is `{"nid": ..., "msg": ..., "delay_ms": ...}` to target a specific node; the
    `delay_ms` key is optional and defaults to an immediate injection. Node-RED's drop-rate
    specs space their injections out in the same way (`setTimeout` in
    `dropRateLimitSECONDSTest`), because a rate limit that drops what arrives too soon can
    only be exercised by a stream, never by a burst.
    """
    msgs_to_inject = []
    for msg in msgs:
        if 'nid' in msg and 'msg' in msg:  # We got a raw injection
            msg_injection = (msg['nid'], msg['msg'], msg.get('delay_ms', 0.0))
        else:
            # `delay_ms` schedules the injection; it is not part of the message itself
            injected = {key: value for key, value in msg.items() if key != 'delay_ms'}
            msg_injection = (injectee_node_id, injected, msg.get('delay_ms', 0.0))
        msgs_to_inject.append(msg_injection)
    return await edgelink.run_flows_for_once(seconds, flows_obj, msgs_to_inject, TEST_EDGELINLKD_CONFIG)


async def run_single_node_for_seconds(node_json: object, msgs: list[object] | None,
                                      seconds: float, injectee_node_id: str = '1') -> list[object]:
    """Single-node variant of `run_flow_for_seconds`; see it for the `_arrival_ms` contract."""
    user_node = copy.deepcopy(node_json)
    user_node["id"] = "1"
    user_node["z"] = "0"
    if 'wires' not in node_json:
        user_node["wires"] = [["2"]]
    console_node = {"id": "2", "type": "test-once", "z": "0"}
    final_flows_json = [{"id": "0", "type": "tab"}, user_node, console_node]
    return await run_flow_for_seconds(final_flows_json, msgs, seconds, injectee_node_id)


async def run_single_node_for_seconds_scheduled(node_json: object, msgs: list[object] | None,
                                                seconds: float, injectee_node_id: str = '1') -> list[object]:
    """Single-node variant of `run_flow_for_seconds_scheduled`."""
    user_node = copy.deepcopy(node_json)
    user_node["id"] = "1"
    user_node["z"] = "0"
    if 'wires' not in node_json:
        user_node["wires"] = [["2"]]
    console_node = {"id": "2", "type": "test-once", "z": "0"}
    final_flows_json = [{"id": "0", "type": "tab"}, user_node, console_node]
    return await run_flow_for_seconds_scheduled(final_flows_json, msgs, seconds, injectee_node_id)

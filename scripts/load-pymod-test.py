import os
import sys
import asyncio
import importlib.util


current_script_path = os.path.abspath(__file__)
current_directory = os.path.dirname(current_script_path)
target_directory = os.path.normpath(os.path.join(current_directory, '..', 'target', 'debug'))
module_path = os.path.join(target_directory, "libedgelink_pymod.so")
spec = importlib.util.spec_from_file_location("edgelink_pymod", module_path)
edgelink = importlib.util.module_from_spec(spec)
spec.loader.exec_module(edgelink)


async def main():

    flows_json = [
        { "id": "100", "type": "tab", "label": "Flow 1" },
        { "id": "1", "z": "100", "type": "test-once" }
    ]
    msgs = [
        ("1", {"payload": "Hello World!"})
    ]
    config = {}
    #fn run_flows_once<'a>(py: Python<'a>, _expected_msgs: usize, _timeout: f64, py_json: &'a PyAny) -> PyResult<&'a PyAny> {
    msgs = await edgelink.run_flows_once(1, 0.2, flows_json, msgs, config)
    print(msgs)

# should sleep for 1s
asyncio.run(main())

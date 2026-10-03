"""Ported specs for the `global-config` node.

Upstream: `3rd-party/node-red/test/nodes/core/common/91-global-config_spec.js` (v4.0.9). The node
carries the workspace's environment variables; Node-RED evaluates them into the global environment
when the flow starts, so every node can reach them (`payloadType: "env"` in the inject specs here).

Upstream's `should be loaded` only reads the deployed node's own `name` property, which the bridge
cannot read back, so the port verifies what it can observe: the global node deploys and the flow keeps
running around it.
"""
import time

import pytest
from tests import *


async def _run_inject_through_global_config(env: list[dict], payload: str) -> list[dict]:
    """Fire an `env`-typed inject through a flow that carries `env` on its `global-config` node."""
    flows = [
        {"id": "100", "type": "tab"},
        # No `z`: a node without a flow belongs to the global configuration.
        {"id": "n1", "type": "global-config", "name": "XYZ", "env": env},
        {"id": "n2", "z": "100", "type": "inject", "once": True, "onceDelay": 0.0, "repeat": "",
         "topic": "t1", "payload": payload, "payloadType": "env", "wires": [["n3"]]},
        {"id": "n3", "z": "100", "type": "test-once"},
    ]
    return await run_flow_with_msgs_ntimes(flows, [], 1)


@pytest.mark.describe('Global Config Node')
class TestGlobalConfigNode:

    @pytest.mark.asyncio
    @pytest.mark.it('should be loaded')
    async def test_should_be_loaded(self):
        flows = [
            {"id": "100", "type": "tab"},
            {"id": "n1", "type": "global-config", "name": "XYZ"},
            {"id": "1", "z": "100", "type": "change", "name": "change", "rules": [
                {"t": "set", "p": "payload", "pt": "msg", "to": "loaded", "tot": "str"}], "wires": [["2"]]},
            {"id": "2", "z": "100", "type": "test-once"},
        ]
        msgs = await run_flow_with_msgs_ntimes(flows, [{"nid": "1", "msg": {"payload": "in"}}], 1)
        assert msgs[0]["payload"] == "loaded"

    @pytest.mark.asyncio
    @pytest.mark.it('should access global environment variable')
    async def test_should_access_global_environment_variable(self):
        msgs = await _run_inject_through_global_config(
            [{"name": "X", "type": "string", "value": "foo"}], "X")
        assert msgs[0]["payload"] == "foo"

    @pytest.mark.asyncio
    @pytest.mark.it('should evaluate a global environment variable that is a JSONata value')
    async def test_should_evaluate_a_global_environment_variable_that_is_a_jsonata_value(self):
        msgs = await _run_inject_through_global_config(
            [{"name": "now-var", "type": "jsonata", "value": "$millis()"}], "now-var")
        # `$millis()` is evaluated when the flow is deployed, so the value is "now" when the inject
        # fires (upstream allows a second of slack for the same reason).
        assert abs(msgs[0]["payload"] - time.time() * 1000) < 1000

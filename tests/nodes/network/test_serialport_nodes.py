import pytest
from tests import *


def base_flow(extra):
    tab = red_id("tab")
    return [{"id": tab, "type": "tab"}, *extra]


@pytest.mark.asyncio
async def test_serial_port_config_is_rejected_without_device():
    tab, config_id, out_id, end_id = map(red_id, ["tab", "config", "out", "end"])
    with pytest.raises(Exception, match="serialport must name a device"):
        await run_flow_with_msgs_ntimes(
            base_flow([
                {"id": config_id, "type": "serial-port", "wires": []},
                {"id": out_id, "type": "serial out", "z": tab, "serial": config_id, "wires": [[end_id]]},
                {"id": end_id, "type": "test-once", "z": tab},
            ]),
            [{"payload": "unused"}], 1, injectee_node_id=out_id,
        )


@pytest.mark.asyncio
async def test_serial_out_rejects_a_missing_config():
    tab, out_id, end_id = map(red_id, ["tab", "out", "end"])
    with pytest.raises(Exception, match="serial.errors.missing-conf"):
        await run_flow_with_msgs_ntimes(
            base_flow([
                {"id": out_id, "type": "serial out", "z": tab, "serial": "f", "wires": [[end_id]]},
                {"id": end_id, "type": "test-once", "z": tab},
            ]),
            [{"payload": "unused"}], 1, injectee_node_id=out_id,
        )


@pytest.mark.asyncio
async def test_serial_control_echoes_settings_and_stops_worker():
    tab, config_id, control_id, end_id = map(red_id, ["tab", "config", "control", "end"])
    flows = base_flow([
        {"id": config_id, "type": "serial-port", "serialport": "__edgelink_missing_serial__", "wires": []},
        {"id": control_id, "type": "serial control", "z": tab, "serial": config_id, "wires": [[end_id]]},
        {"id": end_id, "type": "test-once", "z": tab},
    ])
    out = await run_flow_with_msgs_ntimes(
        flows,
        [{"payload": {"enabled": False}}, {"payload": {"enabled": True}}],
        2,
        injectee_node_id=control_id,
    )
    assert out[0]["payload"]["serialport"] == "__edgelink_missing_serial__"
    assert out[0]["payload"]["enabled"] is False
    assert out[1]["payload"]["enabled"] is True




@pytest.mark.asyncio
async def test_serial_out_reports_disconnected_device_to_catch():
    tab, config_id, out_id, catch_id, end_id = map(red_id, ["tab", "config", "out", "catch", "end"])
    flows = base_flow([
        {"id": config_id, "type": "serial-port", "serialport": "__edgelink_missing_serial__", "wires": []},
        {"id": out_id, "type": "serial out", "z": tab, "serial": config_id, "wires": [[]]},
        {"id": catch_id, "type": "catch", "z": tab, "scope": [out_id], "wires": [[end_id]]},
        {"id": end_id, "type": "test-once", "z": tab},
    ])
    out = await run_flow_with_msgs_ntimes(flows, [{"payload": "hello"}], 1, injectee_node_id=out_id)
    assert out[0]["payload"] == "hello"
    assert "serial port is disconnected" in out[0]["error"]["message"]

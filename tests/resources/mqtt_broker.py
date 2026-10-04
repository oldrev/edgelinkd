"""Small in-process MQTT broker used by integration tests."""

from contextlib import asynccontextmanager
import asyncio
import socket

from amqtt.broker import Broker


@asynccontextmanager
async def mqtt_broker(host="127.0.0.1", port=18883):
    if port == 0:
        with socket.socket() as probe:
            probe.bind((host, 0))
            port = probe.getsockname()[1]
    broker = Broker({
        "listeners": {"default": {"type": "tcp", "bind": f"{host}:{port}"}},
        "sys_interval": 0,
        "auth": {"allow-anonymous": True},
    })
    await broker.start()
    try:
        yield host, port
    finally:
        await broker.shutdown()
        await asyncio.sleep(0.5)

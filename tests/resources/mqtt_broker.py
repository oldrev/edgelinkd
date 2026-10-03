"""Small in-process MQTT broker used by integration tests."""

from contextlib import asynccontextmanager

from amqtt.broker import Broker


@asynccontextmanager
async def mqtt_broker(host="127.0.0.1", port=18883):
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

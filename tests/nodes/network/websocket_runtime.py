"""Run the real web server for network-node specs."""
import asyncio
from contextlib import asynccontextmanager
import json
import os
from pathlib import Path
import socket
import tempfile


class RuntimeUrl(str):
    pass


@asynccontextmanager
async def websocket_runtime(flow):
    root = Path(__file__).resolve().parents[3]
    binary = root / 'target' / os.getenv('EDGELINK_BUILD_TARGET', '') / os.getenv('EDGELINK_BUILD_PROFILE', 'debug') / ('edgelinkd.exe' if os.name == 'nt' else 'edgelinkd')
    with socket.socket() as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix='edgelink-ws-') as folder:
        path = Path(folder) / 'flows.json'
        path.write_text(json.dumps(flow).replace("$WS_BASE", f"ws://127.0.0.1:{port}"), encoding='utf-8')
        process = await asyncio.create_subprocess_exec(str(binary), 'run', str(path), '--bind', f'127.0.0.1:{port}', '--home', folder, '-v', '2', stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
        # Drain logs concurrently so the server cannot block on a full pipe.
        runtime = RuntimeUrl(f'ws://127.0.0.1:{port}')
        runtime.logs = []
        async def read_logs():
            while line := await process.stderr.readline():
                runtime.logs.append(line.decode(errors='replace'))
            return ''.join(runtime.logs).encode()
        logs = asyncio.create_task(read_logs())
        try:
            for _ in range(100):
                if process.returncode is not None:
                    raise RuntimeError((await logs).decode(errors='replace'))
                try:
                    reader, writer = await asyncio.open_connection('127.0.0.1', port)
                    writer.close()
                    await writer.wait_closed()
                    break
                except OSError:
                    await asyncio.sleep(.02)
            else:
                raise TimeoutError('EdgeLinkd web server did not become ready')
            yield runtime
        finally:
            if process.returncode is None:
                process.terminate()
            await process.wait()
            await logs

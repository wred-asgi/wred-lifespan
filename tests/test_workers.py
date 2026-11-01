"""Actual sockets, process signals, and worker replacement (POSIX only)."""

import asyncio
import os
import signal
import socket
import sys
from pathlib import Path

import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(sys.platform != "linux", reason="Linux process signals"),
]


async def test_workers_serve_during_replacement_and_rolling_restart(tmp_path):
    event_dir = tmp_path / "events"
    event_dir.mkdir()
    environment = os.environ.copy()
    # Carry the exact Wred installation/source path used by this test process.
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(Path(__file__).parent), *[str(Path(p).resolve()) for p in sys.path if p]]
    )
    environment["WRED_LIFESPAN_EVENTS"] = str(event_dir)

    def pids(event):
        return {int(path.stem) for path in event_dir.glob(f"*.{event}")}

    async def wait_for(predicate):
        async with asyncio.timeout(15):
            while not predicate():
                assert process.returncode is None, log_path.read_text()
                await asyncio.sleep(0.025)

    log_path = tmp_path / "server.log"
    counts = [0] * 8
    seen = set()
    transition = False
    disconnects = []
    stop = asyncio.Event()
    with socket.socket() as listener, log_path.open("w") as log:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
        listener.listen(128)
        process = await asyncio.create_subprocess_exec(
            *[
                sys.executable,
                "-m",
                "uvicorn",
                "worker_app:app",
                "--fd",
                str(listener.fileno()),
                "--workers",
                "2",
                "--lifespan",
                "on",
                "--no-access-log",
                "--timeout-graceful-shutdown",
                "5",
            ],
            env=environment,
            pass_fds=(listener.fileno(),),
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        try:
            await wait_for(lambda: len(pids("started")) == 2)
            original = pids("started")
            async with httpx.AsyncClient(
                base_url=f"http://127.0.0.1:{port}",
                timeout=5,
                limits=httpx.Limits(max_connections=8, max_keepalive_connections=0),
            ) as client:

                async def traffic(index):
                    while not stop.is_set():
                        during_restart = transition
                        try:
                            response = await client.get("/")
                        except (
                            httpx.RemoteProtocolError,
                            httpx.ReadError,
                            httpx.ConnectError,
                        ):
                            # Uvicorn closes idle sockets during shutdown. Track
                            # transport losses only in the explicit restart window.
                            if not during_restart:
                                raise
                            disconnects.append(index)
                            continue
                        assert response.status_code == 200
                        result = response.json()
                        assert result["ready"] is True
                        seen.add(result["pid"])
                        counts[index] += 1

                async with asyncio.timeout(45), asyncio.TaskGroup() as tasks:
                    for index in range(len(counts)):
                        tasks.create_task(traffic(index))
                    await wait_for(lambda: sum(counts) >= 100 and original <= seen)
                    # Gracefully stop one worker; the supervisor must replace it.
                    transition = True
                    victim = min(original)
                    os.kill(victim, signal.SIGTERM)
                    await wait_for(lambda: len(pids("started")) == 3)
                    await wait_for(lambda: victim in pids("stopped"))
                    replacement = pids("started") - original
                    await wait_for(lambda: replacement <= seen)
                    before_restart = pids("started")
                    process.send_signal(signal.SIGHUP)
                    await wait_for(lambda: len(pids("started")) == 5)
                    await wait_for(lambda: before_restart <= pids("stopped"))
                    await wait_for(lambda: pids("started") - before_restart <= seen)
                    transition = False
                    stable_target = sum(counts) + 1000
                    await wait_for(lambda: sum(counts) >= stable_target)
                    stop.set()
            process.send_signal(signal.SIGTERM)
            code = await asyncio.wait_for(process.wait(), timeout=10)
            assert code == 0, log_path.read_text()
            assert pids("started") == pids("stopped")
            assert all(count > 0 for count in counts)
            print(
                f"{sum(counts)} successful requests; {len(seen)} worker PIDs; "
                f"{len(disconnects)} restart transport disconnects; all cleaned up"
            )
        finally:
            # Reap the process tree even when an assertion or deadline fails.
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(process.wait(), timeout=5)
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await asyncio.wait_for(process.wait(), timeout=5)

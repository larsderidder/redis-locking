"""
An isolated Redis-compatible server for protocol interoperability tests.
"""

import shutil
import subprocess
import time
from typing import Iterator

import pytest
from redis import Redis
from redis.exceptions import ConnectionError


@pytest.fixture(scope="module")
def redis_socket(tmp_path_factory) -> Iterator[str]:
    """
    Start a disposable server with a private Unix socket and no persistence or TCP port.
    """
    executable = shutil.which("redis-server")
    if executable is None:
        pytest.skip("redis-server is required for real-server interoperability tests")

    directory = tmp_path_factory.mktemp("redis")
    socket = str(directory / "redis.sock")
    command = [
        executable,
        "--port",
        "0",
        "--unixsocket",
        socket,
        "--unixsocketperm",
        "700",
        "--save",
        "",
        "--appendonly",
        "no",
        "--dir",
        str(directory),
    ]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.STDOUT)
    client = Redis(unix_socket_path=socket, socket_connect_timeout=0.1, socket_timeout=0.1)
    try:
        deadline = time.monotonic() + 5
        while True:
            if process.poll() is not None:
                pytest.fail(f"Redis exited during startup with code {process.returncode}")
            try:
                client.ping()
                break
            except ConnectionError:
                if time.monotonic() >= deadline:
                    pytest.fail("Redis did not start its Unix socket")
                time.sleep(0.01)
        yield socket
    finally:
        client.close()
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)

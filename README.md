# redis-locking

Small convenience helpers for named Redis locks.

`redis-locking` does not implement a new distributed locking algorithm. It wraps
[`python-redis-lock`](https://github.com/ionelmc/python-redis-lock) with a small
application-level API for consistent lock naming, optional lock disabling,
decorators, and busy-check hooks.

## Install

Install from source:

```bash
git clone https://github.com/larsderidder/redis-locking.git
cd redis-locking
python -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install .
```

## Usage

```python
from redis_locking import LockSettings, acquire_lock, build_key, is_locked, lock_key, locked

settings = LockSettings(key_prefix="example:", lock_expire=120, acquire_timeout=10)

with acquire_lock(redis_conn, "task:sync", settings=settings):
    pass

if is_locked(redis_conn, "task:sync", settings=settings):
    print("Busy")

key = build_key("process:", "daily", "sync")

@locked(redis_conn, key, settings=settings)
def run_task():
    pass
```

## Busy checks

Some systems need a second readiness check after the Redis lock is acquired. For
example, a device, API, or external job runner may still report that it is busy.
Use `lock_key` for that pattern:

```python
def is_device_busy(lock_name: str, timeout: int | None) -> bool:
    return ask_device_if_busy(lock_name, timeout=timeout)

with lock_key(
    redis_conn,
    "device:42",
    settings=settings,
    check_fn=is_device_busy,
    check_retries=2,
    check_delay=5,
    check_timeout=30,
):
    pass
```

## Domain wrappers

You can keep lock naming consistent by creating focused wrappers:

```python
DEFAULT_LOCKS = LockSettings(key_prefix="svc:", lock_expire=120, auto_renewal=True)

def lock_process(redis_conn, process_name: str):
    key = build_key("process:", process_name)
    return acquire_lock(redis_conn, key, settings=DEFAULT_LOCKS, blocking=False)

def is_process_locked(redis_conn, process_name: str) -> bool:
    key = build_key("process:", process_name)
    return is_locked(redis_conn, key, settings=DEFAULT_LOCKS)
```

## Development

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -U pip
python -m pip install -e ".[dev]"
pytest
```

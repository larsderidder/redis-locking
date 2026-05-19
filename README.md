# redis-locking

[![PyPI](https://img.shields.io/pypi/v/redis-locking.svg)](https://pypi.org/project/redis-locking/)
[![Python Versions](https://img.shields.io/pypi/pyversions/redis-locking.svg)](https://pypi.org/project/redis-locking/)

**redis-locking** is a small, practical layer for named Redis locks.

```python
from redis import Redis
from redis_locking import LockSettings, acquire_lock, build_key

redis_conn = Redis()
locks = LockSettings(key_prefix="billing:", lock_expire=120, acquire_timeout=10)

with acquire_lock(redis_conn, build_key("invoice", 42), settings=locks):
    send_invoice(42)
```

Redis already has good locking primitives, and `python-redis-lock` handles the hard part of acquiring and releasing locks safely. This package focuses on the application code around it: namespacing, consistent settings, decorators, local lock disabling, and the occasional extra check that some device or external process is ready.

It is intentionally boring, which is useful when the alternative is twelve slightly different lock helpers spread across a codebase, all with their own timeout defaults and naming conventions.

## Installing

```console
$ python -m pip install redis-locking
```

`redis-locking` supports Python 3.8+ and uses [`python-redis-lock`](https://github.com/ionelmc/python-redis-lock) as its backend.

## A small example

```python
from redis import Redis
from redis_locking import LockSettings, acquire_lock, is_locked

redis_conn = Redis.from_url("redis://localhost:6379/0")
settings = LockSettings(key_prefix="worker:", lock_expire=120, acquire_timeout=5)

if is_locked(redis_conn, "daily-sync", settings=settings):
    print("Someone else is already running this")

with acquire_lock(redis_conn, "daily-sync", settings=settings):
    run_daily_sync()
```

By default, `acquire_lock` waits up to `acquire_timeout` seconds. Set `blocking=False` when the worker should skip work that is already running:

```python
from redis_locking import LockAcquireError

try:
    with acquire_lock(redis_conn, "daily-sync", settings=settings, blocking=False):
        run_daily_sync()
except LockAcquireError:
    print("Skipped, lock is already held")
```

## Lock settings

Most services are easier to reason about when lock behavior is configured once, then reused everywhere.

```python
from redis_locking import LockSettings

LOCKS = LockSettings(
    key_prefix="api:",
    acquire_timeout=10,
    lock_expire=120,
    auto_renewal=True,
)
```

Available settings:

- `key_prefix`: prefix all lock keys for a service or environment.
- `acquire_timeout`: maximum seconds to wait when acquiring a blocking lock.
- `lock_expire`: Redis expiry for the lock key.
- `blocking`: wait for the lock by default, or fail immediately.
- `auto_renewal`: let `python-redis-lock` renew the lock while work is running.
- `disabled`: bypass locks, useful for local development, tests, or one-off scripts.

## Busy checks

Sometimes the Redis key is only part of the story. You might lock a device, gateway, or job runner, then still need to ask it whether it is actually ready; `lock_key` puts that extra check behind the same context manager.

```python
from typing import Optional

from redis_locking import lock_key


def is_gateway_busy(lock_name: str, timeout: Optional[int]) -> bool:
    return gateway_client.is_busy(lock_name, timeout=timeout)


with lock_key(
    redis_conn,
    "gateway:42",
    settings=LOCKS,
    check_fn=is_gateway_busy,
    check_retries=2,
    check_delay=5,
    check_timeout=30,
):
    update_gateway(42)
```

Because the check runs after the Redis lock is acquired, only one worker asks the external resource at a time. If the check keeps reporting busy, `LockBusyError` is raised and the Redis lock is released.

## Decorators

For jobs where the lock name is static, the decorator keeps the call site small.

```python
from redis_locking import locked


@locked(redis_conn, "reports:nightly", settings=LOCKS)
def build_nightly_report():
    generate_report()
```

## Domain wrappers

The best use of this package is usually one layer deeper. Define names once, then use those wrappers everywhere.

```python
from typing import Optional

from redis_locking import LockSettings, acquire_lock, build_key, is_locked

PROCESS_LOCKS = LockSettings(key_prefix="orders:", lock_expire=120, auto_renewal=True)


def lock_process(redis_conn, process_name: str, entity_id: Optional[int] = None):
    key = build_key("process", process_name, entity_id)
    return acquire_lock(redis_conn, key, settings=PROCESS_LOCKS, blocking=False)


def is_process_locked(redis_conn, process_name: str, entity_id: Optional[int] = None) -> bool:
    key = build_key("process", process_name, entity_id)
    return is_locked(redis_conn, key, settings=PROCESS_LOCKS)
```

Now the rest of the application does not have to remember key prefixes, expiry values, or whether a particular lock should block. It just calls `lock_process` and gets on with the work.

## API

```python
from redis_locking import (
    LockAcquireError,
    LockBusyError,
    LockCheckError,
    LockSettings,
    acquire_lock,
    build_key,
    is_locked,
    locked,
    lock_key,
)
```

## Development

```console
$ python -m venv .venv
$ . .venv/bin/activate
$ python -m pip install -U pip
$ python -m pip install -e ".[dev]"
$ pytest
```

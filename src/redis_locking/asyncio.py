"""
Async helpers using the same Redis lock protocol as python-redis-lock.
"""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from functools import wraps
from secrets import token_hex
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

from redis_locking.locks import (
    LockAcquireError,
    LockBusyError,
    LockCheckError,
    LockSettings,
    _apply_prefix,
    _require_backend,
    _with_overrides,
    build_key,
)

__all__ = [
    "LockAcquireError",
    "LockBusyError",
    "LockCheckError",
    "LockSettings",
    "acquire_lock",
    "build_key",
    "is_locked",
    "lock_key",
    "locked",
]

CheckFn = Callable[[str, Optional[int]], Awaitable[bool]]


class _AsyncLock:
    """
    Speak python-redis-lock's SET NX, signal-list, and ownership-checking protocol.
    """

    def __init__(self, redis_conn: Any, key: str, settings: LockSettings) -> None:
        """
        Keep one ownership token and its Redis scripts for this acquisition.
        """
        backend = _require_backend()
        self.redis = redis_conn
        self.key = key
        self.name = "lock:" + key
        self.signal = "lock-signal:" + key
        self.token = token_hex(18)
        self.settings = settings
        self.expire = int(settings.lock_expire) if settings.lock_expire else None
        if self.expire is not None and self.expire <= 0:
            raise ValueError("lock_expire must be a positive number of seconds or zero for no expiry")
        self.timeout = int(settings.acquire_timeout) if settings.blocking and settings.acquire_timeout else None
        if self.timeout is not None and self.timeout < 0:
            raise backend.InvalidTimeout("acquire_timeout cannot be negative")
        if self.expire and self.timeout and self.timeout > self.expire and not settings.auto_renewal:
            raise backend.TimeoutTooLarge("acquire_timeout cannot be greater than lock_expire")
        if settings.auto_renewal and self.expire is None:
            raise ValueError("auto_renewal requires a positive lock_expire")
        self.unlock = redis_conn.register_script(backend.UNLOCK_SCRIPT)
        self.extend = redis_conn.register_script(backend.EXTEND_SCRIPT)
        self.renewer: Optional[asyncio.Task] = None

    async def acquire(self) -> bool:
        """
        Wait for release notifications without blocking the event loop.
        """
        deadline = time.monotonic() + self.timeout if self.timeout else None
        while True:
            if await self.redis.set(self.name, self.token, nx=True, ex=self.expire):
                if self.settings.auto_renewal:
                    self.renewer = asyncio.create_task(self._renew())
                return True
            if not self.settings.blocking:
                return False

            remaining = deadline - time.monotonic() if deadline is not None else None
            if remaining is not None and remaining <= 0:
                return False
            wait = remaining
            if self.expire and (wait is None or self.expire < wait):
                wait = self.expire
            # Expiry does not emit a signal; retry SET after the server-side wait ends.
            await self.redis.blpop(self.signal, timeout=wait or 0)

    async def _renew(self) -> None:
        """
        Extend our lease while the context is active, stopping if ownership is lost.
        """
        assert self.expire is not None
        while True:
            await asyncio.sleep(self.expire * 2 / 3)
            result = await self.extend(keys=(self.name, self.signal), args=(self.token, self.expire))
            if result != 0:
                raise LockAcquireError(f"Lock ownership lost for {self.key}")

    async def release(self) -> None:
        """
        Release only our token and wake callers of either API atomically.
        """
        try:
            if self.renewer is not None:
                self.renewer.cancel()
                try:
                    await self.renewer
                except asyncio.CancelledError:
                    pass
        finally:
            result = await self.unlock(keys=(self.name, self.signal), args=(self.token, 1000))
            if result not in (0, 1):
                raise RuntimeError(f"Unexpected unlock result for {self.key}: {result}")


@asynccontextmanager
async def acquire_lock(
    redis_conn: Any,
    key: str,
    settings: Optional[LockSettings] = None,
    **overrides: Any,
) -> AsyncIterator[None]:
    """
    Acquire a lock shared with synchronous callers and release it on exit.
    """
    config = _with_overrides(settings or LockSettings(), overrides)
    if config.disabled:
        yield
        return

    full_key = _apply_prefix(key, config.key_prefix)
    lock = _AsyncLock(redis_conn, full_key, config)
    try:
        if not await lock.acquire():
            raise LockAcquireError(full_key)
        yield
    finally:
        # A cancellation can arrive after SET succeeded but before its response was read.
        cleanup = asyncio.create_task(lock.release())
        cancelled = False
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cancelled = True
        cleanup.result()
        if cancelled:
            raise asyncio.CancelledError


async def is_locked(redis_conn: Any, key: str, settings: Optional[LockSettings] = None) -> bool:
    """
    Return whether either API holds a lock for this resource.
    """
    config = settings or LockSettings()
    if config.disabled:
        return False
    name = "lock:" + _apply_prefix(key, config.key_prefix)
    return bool(await redis_conn.exists(name))


@asynccontextmanager
async def lock_key(
    redis_conn: Any,
    key: str,
    settings: Optional[LockSettings] = None,
    check_fn: Optional[CheckFn] = None,
    check_retries: int = 1,
    check_delay: int = 30,
    check_timeout: Optional[int] = None,
    **overrides: Any,
) -> AsyncIterator[None]:
    """
    Acquire a shared lock and optionally await an external readiness check.
    """
    if check_retries < 0 or check_delay < 0:
        raise ValueError("check_retries and check_delay cannot be negative")
    config = _with_overrides(settings or LockSettings(), overrides)
    full_key = _apply_prefix(key, config.key_prefix)
    async with acquire_lock(redis_conn, key, config):
        if check_fn is not None:
            for attempt in range(check_retries + 1):
                try:
                    busy = await check_fn(full_key, check_timeout)
                except Exception as exc:
                    raise LockCheckError(f"check failed for {full_key}") from exc
                if not busy:
                    break
                if attempt == check_retries:
                    raise LockBusyError(full_key)
                await asyncio.sleep(check_delay)
        yield


def locked(
    redis_conn: Any, key: str, **lock_kwargs: Any
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """
    Decorate a coroutine so it runs inside a shared Redis lock.
    """

    def decorator(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        """
        Wrap a coroutine with async lock acquisition.
        """

        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Await the coroutine inside the configured lock.
            """
            async with acquire_lock(redis_conn, key, **lock_kwargs):
                return await func(*args, **kwargs)

        return wrapper

    return decorator

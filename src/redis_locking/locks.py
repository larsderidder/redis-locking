"""
Convenience helpers for named Redis locks.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from functools import wraps
from typing import Any, Callable, Optional

redis_lock: Any
try:
    import redis_lock
except ImportError:  # pragma: no cover - handled at runtime or by tests.
    redis_lock = None


class LockAcquireError(RuntimeError):
    """
    Raised when a lock cannot be acquired.
    """


class LockBusyError(RuntimeError):
    """
    Raised when a busy check reports that the resource is unavailable.
    """


class LockCheckError(RuntimeError):
    """
    Raised when a busy check fails.
    """


@dataclass(frozen=True)
class LockSettings:
    """
    Configuration shared by the lock helpers.
    """

    acquire_timeout: int = 30
    lock_expire: int = 120
    blocking: bool = True
    auto_renewal: bool = False
    disabled: bool = False
    key_prefix: str = ""


CheckFn = Callable[[str, Optional[int]], bool]


def _require_backend() -> Any:
    """
    Return the redis-lock backend module or raise a helpful error.
    """
    if redis_lock is None:
        raise ImportError("python-redis-lock is required for Redis lock support")
    return redis_lock


def _with_overrides(settings: LockSettings, overrides: dict[str, Any]) -> LockSettings:
    """
    Return settings with dataclass fields overridden.
    """
    return replace(settings, **overrides) if overrides else settings


def _apply_prefix(key: str, prefix: str) -> str:
    """
    Apply a configured prefix to a Redis lock key.
    """
    return f"{prefix}{key}" if prefix else key


def _create_lock(redis_conn: Any, key: str, settings: LockSettings) -> Any:
    """
    Create a backend lock instance for a full Redis key.
    """
    backend = _require_backend()
    return backend.Lock(
        redis_conn,
        key,
        expire=settings.lock_expire,
        auto_renewal=settings.auto_renewal,
    )


def _run_check(
    key: str,
    check_fn: CheckFn,
    retries: int,
    delay_seconds: int,
    timeout_seconds: Optional[int],
) -> None:
    """
    Run a busy check until it passes or retries are exhausted.
    """
    for attempt in range(retries + 1):
        try:
            busy = check_fn(key, timeout_seconds)
        except Exception as exc:
            raise LockCheckError(f"check failed for {key}") from exc
        if not busy:
            return
        if attempt < retries:
            time.sleep(delay_seconds)
    raise LockBusyError(key)


@contextmanager
def acquire_lock(
    redis_conn: Any,
    key: str,
    settings: Optional[LockSettings] = None,
    **overrides: Any,
) -> Iterator[None]:
    """
    Acquire a named Redis lock and release it when the context exits.
    """
    config = _with_overrides(settings or LockSettings(), overrides)
    if config.disabled:
        yield
        return

    full_key = _apply_prefix(key, config.key_prefix)
    lock = _create_lock(redis_conn, full_key, config)
    timeout = config.acquire_timeout if config.blocking else None
    acquired = lock.acquire(blocking=config.blocking, timeout=timeout)
    if not acquired:
        raise LockAcquireError(full_key)

    try:
        yield
    finally:
        try:
            lock.release()
        except _require_backend().NotAcquired:
            pass


@contextmanager
def lock_key(
    redis_conn: Any,
    key: str,
    settings: Optional[LockSettings] = None,
    check_fn: Optional[CheckFn] = None,
    check_retries: int = 1,
    check_delay: int = 30,
    check_timeout: Optional[int] = None,
    **overrides: Any,
) -> Iterator[None]:
    """
    Acquire a lock and optionally run a busy check before yielding.
    """
    config = _with_overrides(settings or LockSettings(), overrides)
    full_key = _apply_prefix(key, config.key_prefix)
    with acquire_lock(redis_conn, key, config):
        if check_fn is not None:
            _run_check(full_key, check_fn, check_retries, check_delay, check_timeout)
        yield


def is_locked(redis_conn: Any, key: str, settings: Optional[LockSettings] = None) -> bool:
    """
    Return whether a named Redis lock currently exists.
    """
    config = settings or LockSettings()
    if config.disabled:
        return False
    full_key = _apply_prefix(key, config.key_prefix)
    lock = _create_lock(redis_conn, full_key, config)
    return bool(lock.locked())


def build_key(prefix: str, *parts: object) -> str:
    """
    Build a namespaced lock key from non-None parts.
    """
    base = ":".join(str(part) for part in parts if part is not None)
    return _apply_prefix(base, prefix)


def locked(redis_conn: Any, key: str, **lock_kwargs: Any) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorate a function so it runs inside a Redis lock.
    """

    def decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        """
        Wrap a function with lock acquisition.
        """

        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            """
            Execute the wrapped function inside the configured lock.
            """
            with acquire_lock(redis_conn, key, **lock_kwargs):
                return func(*args, **kwargs)

        return wrapper

    return decorator

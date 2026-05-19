"""
Tests for Redis lock helpers.
"""

from unittest import mock

import pytest

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


class FakeNotAcquired(RuntimeError):
    """
    Fake backend exception for release races.
    """


class FakeBackend:
    """
    Fake redis_lock module.
    """

    NotAcquired = FakeNotAcquired

    def __init__(self, lock):
        """
        Store the fake lock returned by Lock.
        """
        self.lock = lock
        self.calls = []

    def Lock(self, redis_conn, name, expire=None, auto_renewal=False):
        """
        Return a fake lock while recording construction arguments.
        """
        self.calls.append((redis_conn, name, expire, auto_renewal))
        return self.lock


class FakeLock:
    """
    Fake lock implementing the python-redis-lock API subset.
    """

    def __init__(self, acquired=True, locked=False):
        """
        Store fake lock state.
        """
        self.acquired = acquired
        self.locked_value = locked
        self.acquire_calls = []
        self.released = False
        self.release_error = None

    def acquire(self, blocking=True, timeout=None):
        """
        Record acquire arguments and return configured state.
        """
        self.acquire_calls.append((blocking, timeout))
        return self.acquired

    def release(self):
        """
        Mark the lock as released or raise a configured error.
        """
        if self.release_error is not None:
            raise self.release_error
        self.released = True

    def locked(self):
        """
        Return configured lock existence state.
        """
        return self.locked_value


@pytest.fixture
def redis_conn():
    """
    Return a placeholder Redis connection.
    """
    return object()


def test_acquire_lock_releases(redis_conn) -> None:
    """
    Release the backend lock after a successful context.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with acquire_lock(redis_conn, "key"):
            pass

    assert lock.released is True
    assert lock.acquire_calls == [(True, 30)]


def test_acquire_lock_failed(redis_conn) -> None:
    """
    Raise a package exception when acquire fails.
    """
    lock = FakeLock(acquired=False)
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with pytest.raises(LockAcquireError, match="key"):
            with acquire_lock(redis_conn, "key"):
                pass


def test_acquire_lock_nonblocking_uses_no_timeout(redis_conn) -> None:
    """
    Do not pass timeout when acquiring a non-blocking lock.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with acquire_lock(redis_conn, "key", blocking=False):
            pass

    assert lock.acquire_calls == [(False, None)]


def test_acquire_lock_applies_settings(redis_conn) -> None:
    """
    Apply prefix, expiry, and auto-renewal settings.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)
    settings = LockSettings(key_prefix="svc:", lock_expire=45, auto_renewal=True)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with acquire_lock(redis_conn, "key", settings=settings):
            pass

    assert backend.calls == [(redis_conn, "svc:key", 45, True)]


def test_acquire_lock_ignores_not_acquired_on_release(redis_conn) -> None:
    """
    Ignore backend NotAcquired errors during release.
    """
    lock = FakeLock()
    lock.release_error = FakeNotAcquired("expired")
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with acquire_lock(redis_conn, "key"):
            pass


def test_lock_key_runs_check_with_prefixed_key(redis_conn) -> None:
    """
    Run the busy check with the full key after lock acquisition.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)
    check = mock.Mock(return_value=False)
    settings = LockSettings(key_prefix="svc:")

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with lock_key(redis_conn, "key", settings=settings, check_fn=check, check_retries=0):
            pass

    check.assert_called_once_with("svc:key", None)
    assert backend.calls == [(redis_conn, "svc:key", 120, False)]


def test_lock_key_busy(redis_conn) -> None:
    """
    Raise when the busy check never passes.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with pytest.raises(LockBusyError, match="key"):
            with lock_key(redis_conn, "key", check_fn=lambda key, timeout: True, check_retries=0):
                pass


def test_lock_key_check_error(redis_conn) -> None:
    """
    Wrap busy check exceptions.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)

    def check_fn(key, timeout):
        """
        Raise a fake check failure.
        """
        raise RuntimeError("boom")

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with pytest.raises(LockCheckError, match="check failed"):
            with lock_key(redis_conn, "key", check_fn=check_fn, check_retries=0):
                pass


def test_is_locked(redis_conn) -> None:
    """
    Return the backend lock existence state.
    """
    lock = FakeLock(locked=True)
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        assert is_locked(redis_conn, "key") is True


def test_lock_settings_disabled(redis_conn) -> None:
    """
    Skip backend lock construction when locks are disabled.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)
    settings = LockSettings(disabled=True)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        with acquire_lock(redis_conn, "key", settings=settings):
            pass

    assert backend.calls == []


def test_is_locked_returns_false_when_disabled(redis_conn) -> None:
    """
    Treat disabled locks as unlocked.
    """
    lock = FakeLock(locked=True)
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):
        assert is_locked(redis_conn, "key", settings=LockSettings(disabled=True)) is False


def test_build_key() -> None:
    """
    Join key parts and skip None values.
    """
    assert build_key("svc:", "a", None, "b") == "svc:a:b"


def test_locked_decorator(redis_conn) -> None:
    """
    Run decorated functions under a lock while preserving metadata.
    """
    lock = FakeLock()
    backend = FakeBackend(lock)

    with mock.patch("redis_locking.locks.redis_lock", backend):

        @locked(redis_conn, "job:1")
        def work():
            """
            Return a fixed value.
            """
            return "ok"

        assert work() == "ok"
        assert work.__name__ == "work"

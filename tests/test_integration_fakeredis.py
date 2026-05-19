"""
Integration tests using fakeredis and python-redis-lock.
"""

from typing import Optional

import fakeredis
import pytest

from redis_locking import LockAcquireError, LockSettings, acquire_lock, is_locked, lock_key


@pytest.fixture
def redis_conn():
    """
    Return an isolated fake Redis connection.
    """
    return fakeredis.FakeRedis()


def test_acquire_lock_blocks_competing_lock(redis_conn) -> None:
    """
    Prevent a second holder from acquiring the same lock.
    """
    settings = LockSettings(blocking=False, lock_expire=30)

    with acquire_lock(redis_conn, "job", settings=settings):
        assert is_locked(redis_conn, "job", settings=settings) is True
        with pytest.raises(LockAcquireError, match="job"):
            with acquire_lock(redis_conn, "job", settings=settings):
                pass

    assert is_locked(redis_conn, "job", settings=settings) is False


def test_key_prefix_is_used_for_lock_identity(redis_conn) -> None:
    """
    Keep prefixed and unprefixed lock names separate.
    """
    prefixed = LockSettings(key_prefix="svc:", blocking=False, lock_expire=30)
    plain = LockSettings(blocking=False, lock_expire=30)

    with acquire_lock(redis_conn, "job", settings=prefixed):
        assert is_locked(redis_conn, "job", settings=prefixed) is True
        assert is_locked(redis_conn, "job", settings=plain) is False


def test_lock_key_runs_busy_check_after_acquire(redis_conn) -> None:
    """
    Run busy checks while the Redis lock is held.
    """
    settings = LockSettings(key_prefix="svc:", blocking=False, lock_expire=30)
    seen = []

    def check(lock_name: str, timeout: Optional[int]) -> bool:
        """
        Record the full lock name and report that work may proceed.
        """
        seen.append((lock_name, timeout, is_locked(redis_conn, "job", settings=settings)))
        return False

    with lock_key(redis_conn, "job", settings=settings, check_fn=check, check_timeout=5):
        assert is_locked(redis_conn, "job", settings=settings) is True

    assert seen == [("svc:job", 5, True)]
    assert is_locked(redis_conn, "job", settings=settings) is False

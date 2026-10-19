"""
Async lock interoperability with the synchronous python-redis-lock backend.
"""

from __future__ import annotations

import asyncio
import threading
from typing import AsyncIterator

import fakeredis
import fakeredis.aioredis
import pytest
import pytest_asyncio
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from redis_locking import LockAcquireError, LockSettings, acquire_lock, is_locked
from redis_locking import asyncio as async_locks


@pytest_asyncio.fixture(params=["fake", "redis-server"])
async def clients(request) -> AsyncIterator[tuple[Redis, AsyncRedis]]:
    """
    Connect both APIs to the same isolated fake or real Redis-compatible server.
    """
    if request.param == "fake":
        server = fakeredis.FakeServer()
        sync_client = fakeredis.FakeRedis(server=server)
        async_client = fakeredis.aioredis.FakeRedis(server=server)
    else:
        socket = request.getfixturevalue("redis_socket")
        sync_client = Redis(unix_socket_path=socket)
        sync_client.flushdb()
        async_client = AsyncRedis(unix_socket_path=socket)
    try:
        yield sync_client, async_client
    finally:
        close = getattr(async_client, "aclose", async_client.close)
        await close()
        sync_client.close()


@pytest.mark.asyncio
async def test_sync_holder_blocks_async_acquisition(clients) -> None:
    """
    Both APIs contend on the same prefixed resource.
    """
    sync_client, async_client = clients
    settings = LockSettings(key_prefix="svc:", blocking=False)
    with acquire_lock(sync_client, "job", settings=settings):
        assert await async_locks.is_locked(async_client, "job", settings=settings)
        with pytest.raises(LockAcquireError, match="svc:job"):
            async with async_locks.acquire_lock(async_client, "job", settings=settings):
                pytest.fail("The synchronous holder must exclude the async holder")

    async with async_locks.acquire_lock(async_client, "job", settings=settings):
        assert is_locked(sync_client, "job", settings=settings)

    assert not is_locked(sync_client, "job", settings=settings)


@pytest.mark.asyncio
async def test_async_holder_blocks_sync_acquisition(clients) -> None:
    """
    Synchronous callers cannot acquire a lock owned by the async API.
    """
    sync_client, async_client = clients
    settings = LockSettings(blocking=False)
    async with async_locks.acquire_lock(async_client, "job", settings=settings):
        with pytest.raises(LockAcquireError, match="job"):
            with acquire_lock(sync_client, "job", settings=settings):
                pytest.fail("The async holder must exclude the synchronous holder")

    with acquire_lock(sync_client, "job", settings=settings):
        assert await async_locks.is_locked(async_client, "job", settings=settings)


@pytest.mark.asyncio
async def test_async_release_wakes_sync_waiter(clients, monkeypatch) -> None:
    """
    Releasing asynchronously notifies a synchronous caller blocked on the signal list.
    """
    sync_client, async_client = clients
    waiting = threading.Event()
    original_blpop = sync_client.blpop

    def blpop(*args, **kwargs):
        """
        Signal when the synchronous worker enters its blocking Redis command.
        """
        waiting.set()
        return original_blpop(*args, **kwargs)

    def wait_for_lock() -> bool:
        """
        Acquire in another thread without blocking the event loop.
        """
        with acquire_lock(sync_client, "job", acquire_timeout=5):
            return is_locked(sync_client, "job")

    monkeypatch.setattr(sync_client, "blpop", blpop)
    loop = asyncio.get_running_loop()
    async with async_locks.acquire_lock(async_client, "job"):
        waiter = loop.run_in_executor(None, wait_for_lock)
        assert await loop.run_in_executor(None, waiting.wait, 2)

    assert await asyncio.wait_for(waiter, timeout=2)
    assert not await async_locks.is_locked(async_client, "job")


@pytest.mark.asyncio
async def test_sync_release_wakes_async_waiter(clients, monkeypatch) -> None:
    """
    An async waiter uses the notification emitted by the synchronous backend.
    """
    sync_client, async_client = clients
    waiting = asyncio.Event()
    original_blpop = async_client.blpop

    async def blpop(*args, **kwargs):
        """
        Signal when the async worker enters its blocking Redis command.
        """
        waiting.set()
        return await original_blpop(*args, **kwargs)

    async def wait_for_lock() -> bool:
        """
        Wait for the synchronous holder to release the resource.
        """
        async with async_locks.acquire_lock(async_client, "job", acquire_timeout=5):
            return is_locked(sync_client, "job")

    monkeypatch.setattr(async_client, "blpop", blpop)
    with acquire_lock(sync_client, "job"):
        waiter = asyncio.create_task(wait_for_lock())
        try:
            await asyncio.wait_for(waiting.wait(), timeout=2)
        except BaseException:
            waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)
            raise

    assert await asyncio.wait_for(waiter, timeout=2)
    assert not is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_async_timeout_preserves_sync_holder(clients) -> None:
    """
    Timing out neither enters the body nor releases another caller's lock.
    """
    sync_client, async_client = clients
    with acquire_lock(sync_client, "job"):
        with pytest.raises(LockAcquireError, match="job"):
            async with async_locks.acquire_lock(async_client, "job", acquire_timeout=1):
                pytest.fail("The holder has not released its lock")
        assert is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_release_preserves_replacement_owner(clients) -> None:
    """
    A stale holder cannot release a resource acquired by someone else.
    """
    sync_client, async_client = clients
    async with async_locks.acquire_lock(async_client, "job"):
        await async_client.set("lock:job", "replacement-owner", ex=30)

    assert sync_client.get("lock:job") == b"replacement-owner"
    assert not sync_client.exists("lock-signal:job")


@pytest.mark.asyncio
async def test_cancelled_acquisition_cleans_up_accepted_set(clients, monkeypatch) -> None:
    """
    Cancellation after Redis accepted SET must not leave our lock behind.
    """
    sync_client, async_client = clients
    accepted = asyncio.Event()
    original_set = async_client.set

    async def set_then_wait(*args, **kwargs):
        """
        Delay the response after the lock has been written to Redis.
        """
        await original_set(*args, **kwargs)
        accepted.set()
        await asyncio.Event().wait()

    async def worker() -> None:
        """
        Attempt acquisition while its response is deliberately delayed.
        """
        async with async_locks.acquire_lock(async_client, "job"):
            pytest.fail("Acquisition has not returned")

    monkeypatch.setattr(async_client, "set", set_then_wait)
    task = asyncio.create_task(worker())
    await asyncio.wait_for(accepted.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_repeated_cancellation_does_not_interrupt_release(clients, monkeypatch) -> None:
    """
    A second cancellation must not abort ownership-checked cleanup.
    """
    sync_client, async_client = clients
    entered = asyncio.Event()
    releasing = asyncio.Event()
    finish_release = asyncio.Event()
    original_register = async_client.register_script

    def register_script(script):
        """
        Delay the release script to make cancellation during cleanup deterministic.
        """
        execute = original_register(script)

        async def delayed(*args, **kwargs):
            """
            Wait until the test allows the ownership check and release.
            """
            releasing.set()
            await finish_release.wait()
            return await execute(*args, **kwargs)

        return delayed

    async def worker() -> None:
        """
        Hold the resource until cancelled.
        """
        async with async_locks.acquire_lock(async_client, "job"):
            entered.set()
            await asyncio.Event().wait()

    monkeypatch.setattr(async_client, "register_script", register_script)
    task = asyncio.create_task(worker())
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    await asyncio.wait_for(releasing.wait(), timeout=2)
    task.cancel()
    finish_release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_auto_renewal_keeps_sync_callers_excluded(clients) -> None:
    """
    Renew the lease while the async body runs beyond its original expiry.
    """
    sync_client, async_client = clients
    settings = LockSettings(lock_expire=1, acquire_timeout=1, auto_renewal=True)
    before = asyncio.all_tasks()
    async with async_locks.acquire_lock(async_client, "job", settings=settings):
        await asyncio.sleep(1.2)
        with pytest.raises(LockAcquireError):
            with acquire_lock(sync_client, "job", blocking=False):
                pytest.fail("The async holder must still own the renewed lock")

    assert not is_locked(sync_client, "job")
    assert asyncio.all_tasks() == before


@pytest.mark.asyncio
async def test_async_waiter_recovers_after_holder_expires(clients) -> None:
    """
    Recover from a crashed holder even though expiry emits no release notification.
    """
    sync_client, async_client = clients
    sync_client.set("lock:job", "crashed-owner", ex=1)
    async with async_locks.acquire_lock(async_client, "job", acquire_timeout=1, lock_expire=1):
        assert is_locked(sync_client, "job")
        assert sync_client.get("lock:job") != b"crashed-owner"


@pytest.mark.asyncio
async def test_disabled_lock_never_uses_redis() -> None:
    """
    Local disabling works without a Redis connection.
    """
    settings = LockSettings(disabled=True)
    async with async_locks.acquire_lock(None, "job", settings=settings):
        assert not await async_locks.is_locked(None, "job", settings=settings)


@pytest.mark.asyncio
async def test_async_busy_check_retries_under_lock(clients) -> None:
    """
    Await readiness checks while holding the resource against synchronous callers.
    """
    sync_client, async_client = clients
    seen = []

    async def check(key: str, timeout: int) -> bool:
        """
        Report busy once and ready on the next attempt.
        """
        seen.append((key, timeout, is_locked(sync_client, "job", LockSettings(key_prefix="svc:"))))
        return len(seen) == 1

    async with async_locks.lock_key(
        async_client, "job", key_prefix="svc:", check_fn=check, check_retries=1, check_delay=0, check_timeout=5
    ):
        assert seen == [("svc:job", 5, True), ("svc:job", 5, True)]

    assert not is_locked(sync_client, "job", LockSettings(key_prefix="svc:"))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["busy", "exception"])
async def test_async_busy_check_failure_releases_lock(clients, failure) -> None:
    """
    Readiness failures leave the resource available to other callers.
    """
    sync_client, async_client = clients

    async def check(key: str, timeout: int) -> bool:
        """
        Exercise both a busy response and an external check failure.
        """
        if failure == "exception":
            raise ValueError("device unavailable")
        return True

    error = async_locks.LockBusyError if failure == "busy" else async_locks.LockCheckError
    with pytest.raises(error):
        async with async_locks.lock_key(async_client, "job", check_fn=check, check_retries=0):
            pytest.fail("An unavailable resource must not enter the context")
    assert not is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_async_decorator_returns_result_and_releases_on_error(clients) -> None:
    """
    Decorated coroutines retain metadata and release after success or failure.
    """
    sync_client, async_client = clients

    @async_locks.locked(async_client, "job")
    async def work(fail: bool = False) -> str:
        """
        Check ownership and either return or raise inside the protected operation.
        """
        assert is_locked(sync_client, "job")
        if fail:
            raise ValueError("work failed")
        return "done"

    assert work.__name__ == "work"
    assert await work() == "done"
    assert not is_locked(sync_client, "job")
    with pytest.raises(ValueError, match="work failed"):
        await work(fail=True)
    assert not is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_failed_renewal_preserves_new_owner(clients) -> None:
    """
    Report lost ownership without deleting a successor's lease.
    """
    sync_client, async_client = clients
    with pytest.raises(LockAcquireError, match="ownership lost"):
        async with async_locks.acquire_lock(async_client, "job", auto_renewal=True, lock_expire=1):
            sync_client.set("lock:job", "replacement-owner", ex=30)
            await asyncio.sleep(0.8)
    assert sync_client.get("lock:job") == b"replacement-owner"


@pytest.mark.asyncio
@pytest.mark.parametrize("overrides", [{"check_retries": -1}, {"check_delay": -1}])
async def test_invalid_check_settings_fail_before_acquisition(overrides) -> None:
    """
    Invalid retries must not silently skip a required readiness check.
    """
    with pytest.raises(ValueError):
        async with async_locks.lock_key(None, "job", **overrides):
            pytest.fail("Invalid settings must be rejected")


@pytest.mark.asyncio
async def test_async_callers_exclude_each_other_and_isolate_prefixes(clients) -> None:
    """
    Share lock identity within a namespace without blocking unrelated namespaces.
    """
    sync_client, async_client = clients
    settings = LockSettings(key_prefix="svc:", blocking=False)
    async with async_locks.acquire_lock(async_client, "job", settings=settings):
        with pytest.raises(LockAcquireError):
            async with async_locks.acquire_lock(async_client, "job", settings=settings):
                pytest.fail("Another async caller already owns this resource")
        assert is_locked(sync_client, "job", settings=settings)
        assert not await async_locks.is_locked(async_client, "job")
        async with async_locks.acquire_lock(async_client, "job", blocking=False):
            assert is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_lock_without_expiry_is_released(clients) -> None:
    """
    Explicitly non-expiring locks still use ownership-checked release.
    """
    sync_client, async_client = clients
    async with async_locks.acquire_lock(async_client, "job", lock_expire=0, acquire_timeout=0):
        assert sync_client.ttl("lock:job") == -1
    assert not is_locked(sync_client, "job")


@pytest.mark.asyncio
async def test_notifications_do_not_reset_acquisition_deadline(clients) -> None:
    """
    Spurious notifications cannot keep an async waiter blocked past its timeout.
    """
    sync_client, async_client = clients

    async def notify() -> None:
        """
        Wake the waiter repeatedly without releasing its resource.
        """
        while True:
            await async_client.lpush("lock-signal:job", 1)
            await asyncio.sleep(0.02)

    async def wait_for_lock() -> None:
        """
        Fail after the total acquisition timeout, regardless of notifications.
        """
        with pytest.raises(LockAcquireError):
            async with async_locks.acquire_lock(async_client, "job", acquire_timeout=1):
                pytest.fail("The synchronous holder has not released its lock")

    with acquire_lock(sync_client, "job"):
        notifier = asyncio.create_task(notify())
        try:
            await asyncio.wait_for(wait_for_lock(), timeout=2)
            assert is_locked(sync_client, "job")
        finally:
            notifier.cancel()
            await asyncio.gather(notifier, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancelled_real_waiter_preserves_holder(redis_socket, monkeypatch) -> None:
    """
    A cancelled BLPOP disconnects cleanly without releasing the current holder.
    """
    waiting = asyncio.Event()
    with Redis(unix_socket_path=redis_socket) as sync_client:
        sync_client.flushdb()
        async with AsyncRedis(unix_socket_path=redis_socket) as async_client:
            original_blpop = async_client.blpop

            async def blpop(*args, **kwargs):
                """
                Expose the point where the task begins waiting for release.
                """
                waiting.set()
                return await original_blpop(*args, **kwargs)

            async def worker() -> None:
                """
                Wait for a resource held by the synchronous caller.
                """
                async with async_locks.acquire_lock(async_client, "job"):
                    pytest.fail("The synchronous holder still owns this resource")

            monkeypatch.setattr(async_client, "blpop", blpop)
            with acquire_lock(sync_client, "job"):
                owner = sync_client.get("lock:job")
                task = asyncio.create_task(worker())
                await asyncio.wait_for(waiting.wait(), timeout=2)
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await asyncio.wait_for(task, timeout=2)
                assert sync_client.get("lock:job") == owner

            assert not await async_locks.is_locked(async_client, "job")

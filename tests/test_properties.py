"""
Lock ownership must match a model across mixed API operations and acquisition races.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, AsyncIterator, Optional

import fakeredis
import fakeredis.aioredis
import pytest
from hypothesis import example, given, settings, strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)
from redis import Redis
from redis.asyncio import Redis as AsyncRedis

from redis_locking import LockAcquireError, LockSettings, acquire_lock, is_locked
from redis_locking import asyncio as async_locks

lock_names = st.text(alphabet=st.characters(blacklist_categories=["Cs"]), max_size=16)
resource_indexes = st.integers(min_value=0, max_value=2)


@pytest.fixture(scope="module", params=["fake", "redis-server"])
def redis_backend(request: pytest.FixtureRequest) -> Optional[str]:
    """
    Reuse only the server, giving every generated example its own clients and database state.
    """
    return request.getfixturevalue("redis_socket") if request.param == "redis-server" else None


@asynccontextmanager
async def redis_clients(socket: Optional[str]) -> AsyncIterator[tuple[Redis, AsyncRedis]]:
    """
    Connect both APIs to an isolated database and close them on the same event loop.
    """
    sync_client: Redis
    async_client: AsyncRedis
    if socket is None:
        server = fakeredis.FakeServer()
        sync_client = fakeredis.FakeRedis(server=server)
        async_client = fakeredis.aioredis.FakeRedis(server=server)
    else:
        sync_client = Redis(unix_socket_path=socket)
        sync_client.flushdb()
        async_client = AsyncRedis(unix_socket_path=socket)
    try:
        yield sync_client, async_client
    finally:
        close = getattr(async_client, "aclose", async_client.close)
        try:
            await close()
        finally:
            sync_client.close()


@dataclass(eq=False)
class HeldLock:
    """
    Remember the context and opaque token belonging to a single acquisition.
    """

    name: str
    context: Any
    asynchronous: bool
    token: bytes


class LockOwnershipMachine(RuleBasedStateMachine):
    """
    Compare public locking APIs with an owner map, retaining contexts after lease expiry.
    """

    def __init__(self, socket: Optional[str]) -> None:
        """
        Keep clients on one event loop for the entire generated operation sequence.
        """
        super().__init__()
        self.loop = asyncio.new_event_loop()
        self.connections = redis_clients(socket)
        self.sync_client, self.async_client = self.loop.run_until_complete(self.connections.__aenter__())
        self.names: list[str] = []
        self.owners: dict[str, HeldLock] = {}
        self.held: list[HeldLock] = []

    @initialize(names=st.lists(lock_names, min_size=3, max_size=3, unique=True))
    def resources(self, names: list[str]) -> None:
        """
        Generate distinct resource identities, including empty and Unicode names.
        """
        self.names = names

    @rule(resource=resource_indexes, asynchronous=st.booleans(), split=st.integers(min_value=0, max_value=16))
    def acquire(self, resource: int, asynchronous: bool, split: int) -> None:
        """
        Only an unowned resource can be acquired, regardless of API or prefix split.
        """
        name = self.names[resource]
        split = min(split, len(name))
        config = LockSettings(key_prefix=name[:split], blocking=False, lock_expire=0)
        context: Any
        if asynchronous:
            context = async_locks.acquire_lock(self.async_client, name[split:], settings=config)
        else:
            context = acquire_lock(self.sync_client, name[split:], settings=config)

        if name in self.owners:
            with pytest.raises(LockAcquireError):
                self._enter(context, asynchronous)
                # Track unexpected acquisitions so a regression cannot leak a context.
                self.held.append(HeldLock(name, context, asynchronous, b""))
            return

        self._enter(context, asynchronous)
        token: Any = self.sync_client.get("lock:" + name)
        lease = HeldLock(name, context, asynchronous, token)
        self.held.append(lease)
        assert isinstance(token, bytes)
        self.owners[name] = lease

    @precondition(lambda self: bool(self.held))
    @rule(data=st.data())
    def release(self, data: st.DataObject) -> None:
        """
        Releasing an old context must preserve any successor's ownership.
        """
        index = data.draw(st.integers(min_value=0, max_value=len(self.held) - 1), label="lease")
        self._release(self.held[index])

    @rule(resource=resource_indexes)
    def expire(self, resource: int) -> None:
        """
        Force server-side expiry without sleeps, retaining the original holder's context.
        """
        name = self.names[resource]
        self.sync_client.pexpire("lock:" + name, 0)
        self.owners.pop(name, None)

    @invariant()
    def ownership_matches_model(self) -> None:
        """
        Both APIs must agree with the model, and unrelated resources must remain unchanged.
        """
        for name in self.names:
            owner = self.owners.get(name)
            assert is_locked(self.sync_client, name) == (owner is not None)
            assert self.loop.run_until_complete(async_locks.is_locked(self.async_client, name)) == (owner is not None)
            assert self.sync_client.get("lock:" + name) == (owner.token if owner is not None else None)

    def _enter(self, context: Any, asynchronous: bool) -> None:
        """
        Enter either public context manager while preserving the shared event loop.
        """
        if asynchronous:
            self.loop.run_until_complete(context.__aenter__())
        else:
            context.__enter__()

    def _release(self, lease: HeldLock) -> None:
        """
        Close a context and update the model only when that context is still the owner.
        """
        try:
            if lease.asynchronous:
                self.loop.run_until_complete(lease.context.__aexit__(None, None, None))
            else:
                lease.context.__exit__(None, None, None)
        finally:
            self.held.remove(lease)
        if self.owners.get(lease.name) is lease:
            del self.owners[lease.name]

    def teardown(self) -> None:
        """
        Verify final cleanup and isolate the next generated sequence even after a failure.
        """
        try:
            for lease in reversed(self.held[:]):
                self._release(lease)
            self.ownership_matches_model()
        finally:
            try:
                self.loop.run_until_complete(self.connections.__aexit__(None, None, None))
            finally:
                self.loop.close()


def test_lock_ownership_model(redis_backend: Optional[str]) -> None:
    """
    Explore mixed acquisitions, release orders, prefix aliases, and expired ownership.
    """
    run_state_machine_as_test(
        lambda: LockOwnershipMachine(redis_backend),
        settings=settings(max_examples=100, stateful_step_count=35, deadline=None),
    )


@pytest.mark.asyncio
@settings(max_examples=50, deadline=None)
@given(modes=st.lists(st.booleans(), min_size=2, max_size=8), key=lock_names, prefix=lock_names)
@example(modes=[True, False], key="", prefix="")
@example(modes=[True, True, True], key="job", prefix="svc:")
@example(modes=[False, False, False], key="job", prefix="svc:")
async def test_acquisition_race_has_one_owner(
    redis_backend: Optional[str], modes: list[bool], key: str, prefix: str
) -> None:
    """
    Keep the winner inside its context until all generated competitors have tried acquiring.
    """
    async with redis_clients(redis_backend) as (sync_client, async_client):
        config = LockSettings(key_prefix=prefix, blocking=False, lock_expire=0)
        start = asyncio.Event()
        release = asyncio.Event()
        attempted = [asyncio.Event() for _ in modes]
        winners = []

        async def compete(index: int, asynchronous: bool) -> None:
            """
            Attempt nonblocking acquisition, holding successful ownership across an await.
            """
            await start.wait()
            try:
                if asynchronous:
                    async with async_locks.acquire_lock(async_client, key, settings=config):
                        winners.append(index)
                        attempted[index].set()
                        await release.wait()
                else:
                    with acquire_lock(sync_client, key, settings=config):
                        winners.append(index)
                        attempted[index].set()
                        await release.wait()
            except LockAcquireError:
                pass
            finally:
                attempted[index].set()

        tasks = [asyncio.create_task(compete(index, mode)) for index, mode in enumerate(modes)]
        try:
            start.set()
            await asyncio.wait_for(asyncio.gather(*(attempt.wait() for attempt in attempted)), timeout=5)
            assert len(winners) == 1
            assert is_locked(sync_client, key, settings=config)
            assert await async_locks.is_locked(async_client, key, settings=config)
        finally:
            release.set()
            await asyncio.gather(*tasks)

        assert not is_locked(sync_client, key, settings=config)
        assert not await async_locks.is_locked(async_client, key, settings=config)

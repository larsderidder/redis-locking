# Changelog

## 0.2.0 (2026-10-21)

### Added

- Add `redis_locking.asyncio` with async `acquire_lock`, `lock_key`, and `is_locked` helpers, plus a `locked` decorator for coroutines.
- Coordinate sync and async callers through shared ownership keys and release notifications. Callers must connect to the same Redis database and use matching prefixed keys.
- Support async lease renewal and readiness callbacks. Cancellation waits for cleanup, including when Redis accepted an acquisition before its response arrived.
- Apply a total async acquisition timeout across release notifications, so repeated notifications cannot restart the timeout.

### Compatibility

- Preserve the existing top-level synchronous API, shared `LockSettings`, exception classes, and defaults. Async helpers are imported separately from `redis_locking.asyncio`.
- Install `redis-locking[asyncio]` for async support. The extra requires redis-py 5 through 7; redis-py 8.1 failed the real-server waiter-cancellation test and is excluded.
- Require Redis 6.0 or later for async blocking waits with fractional timeouts. Python support remains 3.8 or later.
- Limit `python-redis-lock` to the 4.x series because the async implementation shares its Lua scripts and notification protocol.
- Report async renewal failures when the context exits. Lease expiry, stalled renewal, or Redis failover can still allow overlapping work; the library does not issue fencing tokens.

### Tests and packaging

- Add stateful Hypothesis tests that check an ownership model across mixed API operations, including expiry and stale-owner release. Generated acquisition races check that only one caller wins; the model also covers separate resource names and prefix aliases.
- Exercise interoperability against fakeredis and an isolated Redis-compatible server. Expand the CI matrix to Python 3.8 through 3.14.
- Include the full test suite and its configuration in source distributions, along with the changelog.
- Pin GitHub Actions references to immutable commit SHAs in the test and publishing workflows.

## 0.1.1

- Add fakeredis integration coverage for lock contention and key prefixes, with readiness callbacks checked under the acquired lock.

## 0.1.0

- Initial release with synchronous named locks, shared settings, key helpers, readiness checks, and a locking decorator.

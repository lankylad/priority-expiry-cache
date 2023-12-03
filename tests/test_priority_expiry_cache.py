"""Tests for Priority Expiry Cache."""

import pytest
from hypothesis import given, assume

from priority_expiry import Cache
from tests import strategies
from tests.strategies import FakeClock, set_entries


@given(entry=strategies.entries())
def test_set_and_get(entry):
    """Confirm that a single entry can be set and retrieved from the cache."""

    cache = Cache(clock=FakeClock())

    with cache.context(priority=1, expiry_duration=1):
        cache[entry.key] = entry.value

    assert cache[entry.key] is entry.value


@given(entries=strategies.entries_lists())
def test_set_and_get_multiple_values(entries):
    """Confirm that multiple entries can be set and retrieved from the cache."""

    cache = Cache(clock=FakeClock())

    set_entries(cache, entries)

    for entry in entries:
        assert cache[entry.key] is entry.value


@given(entries=strategies.entries_lists())
def test_get_twice(entries):
    """Confirm that entries still exist after being accessed."""
    cache = Cache(clock=FakeClock())

    set_entries(cache, entries)

    for entry in entries:
        assert cache[entry.key] is entry.value

    for entry in entries:
        assert cache[entry.key] is entry.value


@given(entries=strategies.entries_lists())
def test_overwrite(entries):
    """Confirm that entries can be overwritten with new values."""
    cache = Cache(clock=FakeClock())

    set_entries(cache, entries)

    for entry in entries:
        cache[entry.key] = 0

    for entry in entries:
        assert cache[entry.key] == 0


@given(entries=strategies.entries_lists(min_expiry=1, n_expiry=20))
def test_evict_expired(entries):
    """Confirm that the correct entries are evicted when some have expired."""

    clock = FakeClock()
    start_time = clock()
    next_time = start_time + 10
    assume(any(entry.expiry_duration < next_time for entry in entries))
    cache = Cache(clock=clock)
    set_entries(cache, entries)
    clock.advance(next_time - start_time)

    cache.evict()

    for entry in entries:
        if start_time + entry.expiry_duration < next_time:
            with pytest.raises(KeyError):
                _ = cache[entry.key]
        else:
            assert cache[entry.key] is entry.value


@given(
    entries=strategies.entries_lists(
        min_priority=0, n_priority=5, min_key=0, n_key=10000
    ),
    lowest_priority_entry=strategies.entries(min_priority=6, min_key=10001),
)
def test_evict_lowest_priority(entries, lowest_priority_entry):
    """Confirm that the correct lowest priority entry is evicted when no
    entries have expired."""

    entries.append(lowest_priority_entry)
    cache = Cache(clock=FakeClock())
    set_entries(cache, entries)

    cache.evict()

    for entry in entries:
        if entry is lowest_priority_entry:
            with pytest.raises(KeyError):
                _ = cache[entry.key]
        else:
            assert cache[entry.key] is entry.value


@given(
    entries=strategies.entries_lists(
        min_expiry=2, min_priority=0, n_priority=5, min_key=0, n_key=10000
    ),
    lowest_priority_entries=strategies.entries_lists(
        min_expiry=2,
        min_priority=6,
        n_priority=1,
        min_key=10000,
        n_key=10000,
    ),
    lru_lowest_priority_entry=strategies.entries(
        min_priority=6, n_priority=1, min_key=20000
    ),
)
def test_evict_lowest_priority_least_recently_used_set(
    entries, lowest_priority_entries, lru_lowest_priority_entry
):
    """Confirm that the correct lowest priority least recently used entry is
    evicted when no entries have expired.

    The not least recently used entries are last updated by a set operation.
    """
    clock = FakeClock()
    cache = Cache(clock=clock)
    set_entries(cache, [lru_lowest_priority_entry] + entries + lowest_priority_entries)
    clock.advance(1)
    set_entries(cache, lowest_priority_entries)

    cache.evict()

    with pytest.raises(KeyError):
        _ = cache[lru_lowest_priority_entry.key]
    for entry in lowest_priority_entries:
        assert cache[entry.key] is entry.value
    for entry in entries:
        assert cache[entry.key] is entry.value


@given(
    entries=strategies.entries_lists(
        min_expiry=2, min_priority=0, n_priority=5, min_key=0, n_key=10000
    ),
    lowest_priority_entries=strategies.entries_lists(
        min_expiry=2,
        min_priority=6,
        n_priority=1,
        min_key=10000,
        n_key=10000,
    ),
    lru_lowest_priority_entry=strategies.entries(
        min_priority=6, n_priority=1, min_key=20000
    ),
)
def test_evict_lowest_priority_least_recently_used_get(
    entries, lowest_priority_entries, lru_lowest_priority_entry
):
    """Confirm that the correct lowest priority least recently used entry is
    evicted when no entries have expired.

    The not least recently used entries are last updated by a get operation.
    """
    clock = FakeClock()
    cache = Cache(clock=clock)
    set_entries(cache, [lru_lowest_priority_entry] + entries + lowest_priority_entries)
    clock.advance(1)
    for entry in lowest_priority_entries:
        _ = cache[entry.key]

    cache.evict()

    with pytest.raises(KeyError):
        _ = cache[lru_lowest_priority_entry.key]
    for entry in lowest_priority_entries:
        assert cache[entry.key] is entry.value
    for entry in entries:
        assert cache[entry.key] is entry.value


@given(
    expiring_entries=strategies.entries_lists(
        min_expiry=1,
        n_expiry=9,
        min_priority=0,
        n_priority=5,
        min_key=0,
        n_key=9999,
    ),
    expiring_low_priority_entries=strategies.entries_lists(
        min_expiry=1,
        n_expiry=8,
        min_priority=6,
        n_priority=1,
        min_key=10000,
        n_key=9999,
    ),
    expiring_lru_lowest_priority_entry=strategies.entries(
        min_expiry=1,
        n_expiry=9,
        min_priority=6,
        n_priority=1,
        min_key=20000,
        n_key=9999,
    ),
    surviving_entries=strategies.entries_lists(
        min_expiry=10,
        n_expiry=8,
        min_priority=0,
        n_priority=5,
        min_key=30000,
        n_key=9999,
    ),
    surviving_low_priority_entries=strategies.entries_lists(
        min_expiry=10,
        n_expiry=8,
        min_priority=6,
        n_priority=1,
        min_key=40000,
        n_key=9999,
    ),
    surviving_lru_lowest_priority_entry=strategies.entries(
        min_expiry=10,
        n_expiry=8,
        min_priority=6,
        n_priority=1,
        min_key=50000,
        n_key=9999,
    ),
)
def test_evict_expired_then_lowest_priority(
    expiring_entries,
    expiring_low_priority_entries,
    expiring_lru_lowest_priority_entry,
    surviving_entries,
    surviving_low_priority_entries,
    surviving_lru_lowest_priority_entry,
):
    """Confirm that a double eviction evicts the correct expired entries as
    well as the lowest priority, least recently used entry that has not
    expired."""
    clock = FakeClock()
    cache = Cache(clock=clock)
    set_entries(
        cache,
        expiring_entries
        + expiring_low_priority_entries
        + [expiring_lru_lowest_priority_entry]
        + surviving_entries
        + surviving_low_priority_entries
        + [surviving_lru_lowest_priority_entry],
    )
    clock.advance(1)
    set_entries(cache, expiring_low_priority_entries + surviving_low_priority_entries)
    clock.advance(9)

    cache.evict()  # evict expired
    cache.evict()  # evict lru lowest

    with pytest.raises(KeyError):
        _ = cache[surviving_lru_lowest_priority_entry.key]
    with pytest.raises(KeyError):
        _ = cache[expiring_lru_lowest_priority_entry.key]
    for entry in expiring_entries:
        with pytest.raises(KeyError):
            _ = cache[entry.key]
    for entry in expiring_low_priority_entries:
        with pytest.raises(KeyError):
            _ = cache[entry.key]
    for entry in surviving_entries:
        assert cache[entry.key] is entry.value
    for entry in surviving_low_priority_entries:
        assert cache[entry.key] is entry.value

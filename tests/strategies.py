"""Strategies, test objects and helpers for Priority Expiry Cache tests."""

from dataclasses import dataclass, field
from typing import Hashable, Any, List
from hypothesis import strategies as st

from priority_expiry import Cache


@dataclass(eq=True, order=True)
class Entry:
    """A representation of an entry that is to be inserted into the cache."""

    insertion_order: int = field(compare=True)
    key: Hashable = field(compare=False)
    value: Any = field(compare=False)
    priority: int = field(compare=False)
    expiry_duration: int = field(compare=False)


@dataclass
class FakeClock:
    """A fake clock to be used for testing in place of time.monotonic_ns."""

    current_time: int = 0

    def advance(self, duration: int):
        self.current_time += duration

    def __call__(self) -> int:
        return self.current_time


def entries(
    min_priority=0, n_priority=5, min_expiry=1, n_expiry=20, min_key=0, n_key=10000
) -> st.SearchStrategy:
    """A hypothesis search strategy for entries."""

    return st.builds(
        Entry,
        insertion_order=st.integers(),
        key=st.integers(min_value=min_key, max_value=min_key + n_key - 1),
        value=st.integers(min_value=0, max_value=10000),
        priority=st.integers(
            min_value=min_priority, max_value=min_priority + n_priority - 1
        ),
        expiry_duration=st.integers(
            min_value=min_expiry, max_value=min_expiry + n_expiry - 1
        ),
    )


def entries_lists(**kwargs):
    """A hypothesis search strategy for lists of entries."""
    return st.lists(
        entries(**kwargs),
        unique_by=(
            lambda entry: entry.key,
            lambda entry: entry.value,
            lambda entry: entry.insertion_order,
        ),
    )


def set_entries(cache: Cache, entries: List[Entry]) -> None:
    """Helper function to input many entries into a cache.

    Entries are input in sorted order to make it easy for tests to permutate
    different entry orders.
    """

    for entry in sorted(entries):
        with cache.context(entry.priority, entry.expiry_duration):
            cache[entry.key] = entry.value

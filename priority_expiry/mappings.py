"""Mappings module containing the Priority Expiry Cache."""

import contextlib
import time
from typing import (
    TypeVar,
    Hashable,
    MutableMapping,
    Callable,
    Tuple,
    Iterator,
    Optional,
)
from weakref import WeakValueDictionary

from priority_expiry.data_structures import (
    Priority,
    Time,
    EmptyTree,
    Quadtree,
    Node,
)

KT = TypeVar("KT", bound=Hashable)
VT = TypeVar("VT")


class KeyDoesNotExist(KeyError):
    """Raised when key cannot be found in the cache."""


class KeyExpired(KeyError):
    """Raised when key exists in cache, but it has expired."""


class Cache(MutableMapping[KT, VT]):
    """Priority Expiry Cache.

    Adding and accessing key-value entries is supported using the
    MutableMapping interface.

    Entries are evicted as they expire. If no items have expired, then the
    lowest priority entry is selected for eviction. If there are multiple
    entries with the same priority, then the least recently used entry is
    evicted.

    Eviction runs in `log(p)+log(e)` time, where `p` = number of unique
    priorities and `e` = number of unique expiries. This is achieved using
    a point quadtree data structure for efficiently managing the
    two-dimensional space of expiries and priorities.

    Maps for keys and existing points to the nodes of the quadtree are
    internally kept. This allows O(1) complexity for some operations by
    avoiding traversing the tree. These operations include:
     * Adding a new entry with the same expiry and priority of an existing
        entry.
     * Accessing the value of an entry.
     * Updating the value of an entry.
     * Deleting an entry.
    These maps use weakrefs to the nodes. This means that the maps will be
    automatically cleaned when expired and empty nodes are removed from the
    quadtree.

    Attributes:
        clock: Callable that should return the current time. It recommended
            that `time.monotonic_ns` is used so that:
             * time is dealt with in integers rather than floats. This is
             important as time is used frequently for comparisons. Allowing
             floats could lead to unexpected behaviour.
             * The clock is monotonic - a clock that can go backwards will
             lead to unexpected behaviour.
        default_expiry_duration: The default duration
            (using the same units as clock) after which an entry will become
            invalid from when it was last set or accessed.
        default_priority: The default priority an entry when it is set. Note
            that a higher value means a lower priority and vice versa.
    """

    def __init__(
        self,
        clock: Callable[[], Time] = time.monotonic_ns,
        default_expiry_duration: int = int(1e9),
        default_priority: Priority = 0,
    ):
        self.clock = clock
        self.default_expiry_duration = default_expiry_duration
        self.default_priority = default_priority

        self._tree: Quadtree[KT, VT] = Quadtree()
        self._expiry_priority_nodes: WeakValueDictionary[
            Tuple[Time, Priority], Node
        ] = WeakValueDictionary()
        self._key_nodes: WeakValueDictionary[KT, Node] = WeakValueDictionary()

        self._context_expiry_duration = self.default_expiry_duration
        self._context_priority = self.default_priority

    def evict(self) -> None:
        """Evict entries form the cache.

        First attempts to remove expired entries.

        If no expired entries are  present, then the lowest priority is removed.

        If there are multiple entries with the lowest priority, then the entry
        with that priority that was least recently used is removed.

        If multiple entries with the lowest priority were used at the same
        time, then only one of those entries will be removed - the choice as to
        which is undefined.
        """

        now = self.clock()
        any_removed = self._tree.prune_expired(now)
        if any_removed:
            return

        try:
            key = self._tree.prune_lowest_priority()
        except EmptyTree:
            return
        try:
            del self._key_nodes[key]
        except KeyError:
            # the key may have already been removed by the weakref callback
            #  if it was the only item in the node.
            return

    @contextlib.contextmanager
    def context(
        self,
        priority: Optional[Priority] = None,
        expiry_duration: Optional[Time] = None,
    ):
        """Context manager for controlling the priority and expiry of entries
        added to the cache.

        >>> cache = Cache()
        >>> with cache.context(priority=10, expiry_duration=1):
        ...     cache["test_key"] = "test_value"
        ...

        Args:
            priority: Priority that will be given to entries set within the
                context. If `None`, then the default priority will be used.
            expiry_duration: Duration that will be given to entries set within
                the context. If `None`, then the default priority will be used.
        """

        if priority is not None:
            self._context_priority = priority
        else:
            self._context_priority = self.default_priority
        if expiry_duration is not None:
            self._context_expiry_duration = expiry_duration
        else:
            self._context_expiry_duration = self.default_expiry_duration
        try:
            yield
        finally:
            self._context_priority = self.default_priority
            self._context_expiry_duration = self.default_expiry_duration

    def __delitem__(self, key: KT):
        """Deletes an entry from the cache, identified by its key"""

        node = self._key_nodes.pop(key)
        node.delete_entry(key)

    def __getitem__(self, key: KT) -> VT:
        """Retrieves the value of an entry from the cache.

        Note that this operation will reset the entry's timestamp used for
        determining LRU according to the current time reported by the clock.
        """

        try:
            node = self._key_nodes[key]
        except KeyError:
            raise KeyDoesNotExist
        now = self.clock()
        if node.expired(now):
            raise KeyExpired

        value = node.access_entry(key, now)
        return value

    def __len__(self) -> int:
        """Returns the number of valid entries in the cache.

        >>> cache = Cache()
        >>> cache["first_key"] = 0
        >>> assert len(cache) == 1
        >>> cache["second_key"] = 0
        >>> assert len(cache) == 2
        >>> cache["second_key"] = 1
        >>> assert len(cache) == 2
        """

        return len(
            [
                key
                for key, node in self._key_nodes.items()
                if not node.expired(self.clock()) and key in node._data
            ]
        )

    def __iter__(self) -> Iterator[KT]:
        """Allows for iteration of the keys of valid entries in the cache."""

        return (
            key
            for key, node in self._key_nodes.items()
            if not node.expired(self.clock()) and key in node._data
        )

    def __setitem__(self, key: KT, value: VT) -> None:
        """Set the value of a keyed entry.

        If an entry for the given key does not exist, then a new entry simply
        inserted into the cache.

        If an entry for a given key already exists, then it is deleted and a
        new entry with this key is added to the cache with new expiry and
        priority values according to the current context.

        Args:
            key: The key of the entry
            value: The value of the entry
        """

        now = self.clock()
        expiry = now + self._context_expiry_duration
        assert expiry > now

        priority = self._context_priority

        if key in self._key_nodes:
            del self[key]

        try:
            node = self._expiry_priority_nodes[(expiry, priority)]
        except KeyError:
            node = self._tree.insert(priority, expiry)

        node.add_entry(key, value, now)
        self._key_nodes[key] = node
        self._expiry_priority_nodes[(expiry, priority)] = node

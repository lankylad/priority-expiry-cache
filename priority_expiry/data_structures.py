"""data_structures module containing the quadtree used by the
Priority Expiry Cache."""

import enum
from bisect import bisect_left
from collections import deque
from dataclasses import dataclass, field
from typing import Hashable, Optional, Dict, Union, List, TypeVar, Generic

Priority = int
Time = int

KT = TypeVar("KT", bound=Hashable)
VT = TypeVar("VT")


class QuadrantIndex(enum.Enum):
    """Indexes for referencing the four quadrants of each node of the Quadtree.

    Attributes:
        ONE: older or equal expiry, higher priority
        TWO: older or equal expiry, lower or equal priority
        THREE: newer expiry, higher priority
        FOUR: newer expiry, lower or equal priority
    """

    ONE = True, True
    TWO = True, False
    THREE = False, True
    FOUR = False, False


OLDER_QUADS = {QuadrantIndex.ONE, QuadrantIndex.TWO}
YOUNGER_QUADS = {QuadrantIndex.THREE, QuadrantIndex.FOUR}
BEST_QUADS = {QuadrantIndex.ONE, QuadrantIndex.THREE}
WORST_QUADS = {QuadrantIndex.TWO, QuadrantIndex.FOUR}


class EmptyNode(ValueError):
    """Raised when a quad tree node has no data"""


class EmptyTree(ValueError):
    """Raised when there are no nodes in a quad tree with data"""


@dataclass(order=True, eq=True)
class Entry(Generic[KT, VT]):
    """Item for storing cache entries in Nodes.

    These support ordering by last_used. This is useful for keeping track of
    and quickly determining the least recently used entry in the event of
    priority ties.
    """

    last_used: Time = field(compare=True)
    key: KT = field(compare=False)
    value: VT = field(compare=False)


@dataclass
class Quadtree(Generic[KT, VT]):
    """The top-level object to be used for performing operations on the
    Quadtree.

    This implements some methods in common with Node and this is exploited with
    duck-typing where it is convenient to consider this object as the parent
    node of the root node.
    """

    _root: Optional["Node[KT, VT]"] = None

    def insert(self, priority: Priority, expiry: Time) -> "Node":
        """Creates and inserts a new node into the tree with the given priority
        and expiry.
        """

        if self._root is None:
            self._root = Node(expiry, priority, self)
            return self._root
        return self._root.insert(priority, expiry)

    def replace(self, node: "Node"):
        """This has the effect of replacing the root node with another node.

        This is a common method with Node and is used when recursing up through
        the nodes to replace unneeded nodes.

        This may occur if the current root node is expired or empty and can be
        replaced by one of its child nodes.
        """

        self._root = node
        node.parent = self

    def delete(self, node: "Node"):
        """This has the effect of deleting the root node and in fact the entire
        tree.

        This is a common method with Node and is used when recursing up through
        the nodes to remove unneeded nodes.

        This may occur if the current root node is expired or empty and has
        no child nodes.
        """

        assert node is self._root
        self._root = None

    def prune_expired(self, now: Time) -> bool:
        """Remove expired nodes.

        Note that some expired nodes cannot be easily removed from the tree as
        they continue to support the division of exactly two quadrants. In this
        case the entries those nodes contain are deleted, and are left in
        place. These nodes will be able to eventually be removed in future
        invocations as other nodes around them become expired.

        It may be possible to restructure the tree to allow the removal of
        these nodes, however this significantly increase the algorithms
        complexity (in terms of statements and branches) and will possibly
        increase the time complexity beyond `log(e)+log(p)`.

        Args:
            now: the current time.

        Returns:
            bool: True indicates that at least one entry was removed from the
                cache as a result of pruning expired nodes.
        """

        if self._root is None:
            return False
        return self._root.prune_expired(now)

    def prune_lowest_priority(self) -> KT:
        """Remove the entry from the tree with the lowest priority.

        This is done with a breadth first search to find the node with the
        lowest priority with the least recently used entry. Paths that
        cannot lead to lower priority node than the currently lowest found
        are avoided.

        The least recently used entry is removed from the found node.

        Raises:
            EmptyTree: there are no nodes or entries in the tree to be removed.

        Returns:
            KT: The key of the removed entry.
        """

        if self._root is None:
            raise EmptyTree

        lowest: Node = self._root
        if lowest.empty:
            stack = list(lowest.quadrants.values())
        else:
            stack = lowest.lower_priority_nodes
        while stack:
            node: Node = stack.pop()
            if node.empty:
                stack.extend(node.quadrants.values())
                continue
            if lowest.empty:
                lowest = node
            elif node.priority > lowest.priority or (
                node.priority == lowest.priority and node.lru_time < lowest.lru_time
            ):
                lowest = node
            stack.extend(node.lower_priority_nodes)

        if lowest.empty:
            raise EmptyTree

        return lowest.pop_lru()


@dataclass
class Node(Generic[KT, VT]):
    """Node of a Quadtree.

    It holds all entries with the same expiry and priority as this node.

    Attributes:
        expiry: The expiry time of the node.
        priority: The priority of the node. Note that a lower value means a
            higher priority.
        parent: The parent node of this node. This may return to the Quadtree
            object if this is the root node.
        quadrants: The child nodes. If a child node does not exist for a
            quadrant then it won't exist at all in this mapping.
    """

    expiry: Time
    priority: Priority
    parent: Union["Node[KT, VT]", Quadtree[KT, VT]]
    quadrants: Dict[QuadrantIndex, "Node[KT, VT]"] = field(
        default_factory=dict, init=False, repr=False
    )

    _data: Dict[KT, Entry[KT, VT]] = field(default_factory=dict, init=False, repr=False)
    _lru_queue: deque[Entry[KT, VT]] = field(
        default_factory=deque, init=False, repr=False
    )

    def add_entry(self, key: KT, value: VT, now: Time) -> None:
        """Adds new entry to this node.

        Args:
            key: Key of the entry.
            value: Value of the entry.
            now: Current time. Used to keep track of when this entry was last
                used.
        """

        entry = Entry(now, key, value)
        self._data[key] = entry
        self._lru_queue.append(entry)

    def delete_entry(self, key: KT, clean: bool = True) -> None:
        """Deletes an entry

        Raises:
            KeyError: the entry does not exist in this node.

        Args:
            key: The key of the entry to be deleted.
            clean: Whether to clean the node after the entry has been deleted.
        """

        entry = self._data.pop(key)
        idx = bisect_left(self._lru_queue, entry)
        while (
            idx < len(self._lru_queue)
            and self._lru_queue[idx].last_used == entry.last_used
        ):
            if self._lru_queue[idx] is entry:
                del self._lru_queue[idx]
                break
            idx += 1
        else:
            raise Exception("entry unexpectedly missing for lru queue")
        if clean:
            self.clean()

    def pop_lru(self) -> KT:
        """Removes and returns the key of the least recently used entry.

        Returns:
            KT: The key of the least recently used entry.
        """

        if self.empty:
            raise EmptyNode

        least_recent = self._lru_queue.popleft()
        del self._data[least_recent.key]
        self.clean()
        return least_recent.key

    def access_entry(self, key: KT, now: Time) -> VT:
        """Gets the value of an entry for a given key

        Raises:
            KeyError: the entry does not exist in this node.

        Args:
            key: The entry key for the requested value.
            now: The current time. Used to update the last_used timestamp of
                the entry for determining LRU.

        Returns:
            VT: The value associated with the key.
        """

        value = self._data[key].value
        self.delete_entry(key, clean=False)
        self.add_entry(key, value, now)
        return value

    def clear_entries(self) -> None:
        """Remove all entries from this node.

        This may be invoked when the node has expired.
        """

        self._data.clear()
        self._lru_queue.clear()

    @property
    def lru_time(self) -> Time:
        """The last used time of the oldest entry in this Node.

        Raises:
            EmptyNode: the node has no entries and therefore this node isn't
                relevant to LRU.

        Returns:
            Time: The last used time of the oldest entry in this Node
        """
        if self.empty:
            raise EmptyNode
        return self._lru_queue[0].last_used

    @property
    def empty(self) -> bool:
        """Indicates whether this node has any entries."""
        return not bool(self._lru_queue)

    @property
    def deep_empty(self) -> bool:
        """Indicates whether this node or any of its descendents have any
        entries."""
        return self.empty and all(node.deep_empty for node in self.quadrants.values())

    def expired(self, now: Time) -> bool:
        """Indicates whether this node has expired given a time.

        Args:
            now: The current time.

        Returns:
            bool: True if this node has expired.
        """
        return self.expiry < now

    def quadrant_index(self, node: "Node[KT, VT]") -> QuadrantIndex:
        """The quadrant that a node would be placed in on this node."""
        return QuadrantIndex(
            (node.expiry <= self.expiry, node.priority < self.priority)
        )

    def insert(self, priority: Priority, expiry: Time) -> "Node[KT, VT]":
        """Inserts a new node.

        If there is a free spot in the quadrant for the given priority and
        expiry, then a node is created and inserted in the quadrant as child
        to this node.

        Otherwise, the priority-expiry point is given to the node currently
        in the matching quadrant to be inserted. This recursion continues until
        a place for the point is found.

        Args:
            priority: The priority of the new node to be inserted
            expiry: The expiry of the new node to be inserted
        Returns:
            Node: the node that was created.
        """

        index = QuadrantIndex((expiry <= self.expiry, priority < self.priority))
        try:
            return self.quadrants[index].insert(priority, expiry)
        except KeyError:
            self.quadrants[index] = Node(expiry, priority, self)
            return self.quadrants[index]

    def replace(self, node: "Node[KT, VT]"):
        """Inserts a node into this node's direct quadrant references.

        This is intended to be used to replace an existing node with another
        one.
        """

        self.quadrants[self.quadrant_index(node)] = node
        node.parent = self

    def delete(self, node: "Node[KT, VT]"):
        """Deletes a node from this node's quadrant references."""

        del self.quadrants[self.quadrant_index(node)]

    def clean(self) -> None:
        """Attempts to remove this node from the Quadtree if it has no
        entries."""

        if self.empty:
            if not self.quadrants:
                self.parent.delete(self)
            if len(self.quadrants) == 1:
                self.parent.replace(self.quadrants.popitem()[1])

    def prune_expired(self, now: Time) -> bool:
        """Prunes expired sections of the tree.

        See Quadtree.prune_expired for more information.

        Args:
            now: The time used to determine whether nodes have expired.

        Returns:
            bool: True if at least one node with data has expired.
                False otherwise.
        """
        result = False
        if self.expired(now):
            result |= not self.empty
            for index in OLDER_QUADS & set(self.quadrants):
                result |= not self.quadrants[index].deep_empty
                del self.quadrants[index]

            for quad in list(self.quadrants.values()):
                result |= quad.prune_expired(now)

            self.clear_entries()

        for index in OLDER_QUADS & set(self.quadrants):
            result |= self.quadrants[index].prune_expired(now)

        self.clean()

        return result

    @property
    def lower_priority_nodes(self) -> List["Node[KT, VT]"]:
        """Nodes from this node's quadrant references that have a lower
        priority."""

        return [self.quadrants[index] for index in set(self.quadrants) & WORST_QUADS]

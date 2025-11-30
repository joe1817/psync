# Copyright (c) 2025 Joe Walter
# GNU General Public License v3.0

from collections.abc import MutableSet, Sequence
from typing import TypeVar, Generic, AbstractSet, Iterator, Any
from types import NotImplementedType

try:
	from ordered_set import OrderedSet

except ImportError:
	T = TypeVar("T")

	class OrderedSet(MutableSet[T], Generic[T]): # type: ignore [no-redef]
		'''A compatibility layer for OrderedSet that uses the keys of a standard Python dict for ordered, unique storage (requires Python 3.7+).'''

		def __init__(self, _set: AbstractSet[T]|None = None) -> None:
			self._items: dict[T, None] = {}
			if _set:
				self.update(_set)

		# --- Required Methods for MutableSet ---

		def __contains__(self, element: Any) -> bool:
			return element in self._items

		def __iter__(self) -> Iterator[T]:
			return iter(self._items)

		def __len__(self) -> int:
			return len(self._items)

		def add(self, element: T) -> None:
			self._items[element] = None

		def discard(self, element: T) -> None:
			self._items.pop(element, None)

		# --- Other Useful Methods ---

		def update(self, _set: AbstractSet[T]) -> None:
			super().__ior__(_set)

		def __repr__(self) -> str:
			elements = ", ".join(repr(x) for x in self._items.keys())
			return f"OrderedSet([{elements}])"

		def __eq__(self, other: Any) -> bool|NotImplementedType:
			if isinstance(other, OrderedSet):
				return self._items.keys() == other._items.keys()
			return NotImplemented # TODO check what ordered_set.OrderedSet when testing a standard collection

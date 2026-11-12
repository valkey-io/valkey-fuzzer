"""The keyed registry the catalog fills and then freezes, and its registration errors."""

from collections.abc import Iterable, Mapping
from types import MappingProxyType
from typing import Any, Generic, Optional, TypeVar

T = TypeVar("T")


class RegistrationError(ValueError):
    """A registration is duplicated, unknown, or incompatible with its registry."""


class DuplicateRegistrationError(RegistrationError):
    def __init__(self, label: str, key: Any):
        self.key = key
        super().__init__(f"{label} {key!r} is already registered")


class UnknownRegistrationError(RegistrationError):
    def __init__(self, label: str, key: Any):
        self.key = key
        super().__init__(f"{label} {key!r} is not registered")


class FrozenRegistryError(RegistrationError):
    """A registry was changed after it was frozen."""


class BaseRegistry(Generic[T]):
    """Keyed registry that is filled during startup and then frozen.

    The catalog registers everything up front; after :meth:`freeze`
    the registry is read-only, so execution never observes it changing.
    """

    label = "entry"
    entry_type: type = object

    def __init__(self) -> None:
        self._entries: dict[Any, T] = {}
        self._frozen = False

    def key(self, entry: T) -> Any:
        raise NotImplementedError

    def check(self, entry: T) -> None:
        """Reject an entry that cannot belong to this registry."""

        if not isinstance(entry, self.entry_type):
            raise TypeError(f"{self.label} registry entries must be {self.entry_type.__name__} values")

    def validate_many(self, entries: Iterable[T]) -> tuple[T, ...]:
        """Check that ``entries`` could be registered now, without registering them."""

        if self._frozen:
            raise FrozenRegistryError(f"the {self.label} registry is frozen")
        entries = tuple(entries)
        seen = set()
        for entry in entries:
            self.check(entry)
            key = self.key(entry)
            if key in seen or key in self._entries:
                raise DuplicateRegistrationError(self.label, key)
            seen.add(key)
        return entries

    def register_many(self, entries: Iterable[T]) -> None:
        for entry in self.validate_many(entries):
            self._entries[self.key(entry)] = entry

    def freeze(self) -> None:
        self._frozen = True

    def resolve(self, key: Any) -> T:
        try:
            return self._entries[key]
        except (KeyError, TypeError) as exc:
            raise UnknownRegistrationError(self.label, key) from exc

    def get(self, key: Any) -> Optional[T]:
        return self._entries.get(key)

    @property
    def registered_ids(self) -> tuple[Any, ...]:
        return tuple(sorted(self._entries))

    def snapshot(self) -> Mapping[Any, T]:
        return MappingProxyType({key: self._entries[key] for key in sorted(self._entries)})

    def __contains__(self, key: object) -> bool:
        return key in self._entries

    def __len__(self) -> int:
        return len(self._entries)

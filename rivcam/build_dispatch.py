from __future__ import annotations

from typing import Callable, Dict, MutableMapping, Optional

from rivcam.parsers import Version
from rivcam.utils.logging import LOGGER

_REGISTRY: Dict[str, Dict[Version, Callable]] = {}


def register(*, event: str, version: Version) -> Callable[[Callable], Callable]:
    """Register a versioned handler for an event.

    Args:
      event: Event name (e.g., "build_clip", "build_groups").
      version: Parser/build spec version to bind this handler to.

    Returns:
      A decorator that registers the target function under (event, version).

    Raises:
      RuntimeError: If a handler is already registered for (event, version).
    """
    def _wrap(fn: Callable) -> Callable:
        bucket = _REGISTRY.setdefault(event, {})
        if version in bucket:
            existing = bucket[version]
            raise RuntimeError(
                f"Duplicate registration for event '{event}' version '{version}': "
                f"{existing.__name__} already registered"
            )
        bucket[version] = fn
        LOGGER.debug(
            "Registered handler",
            extra={"event": event, "version": str(version), "handler": fn.__name__},
        )
        return fn
    return _wrap


def resolve(event: str, version: Version) -> Callable:
    """Resolve a handler for the given (event, version).

    Args:
      event: Event name to resolve.
      version: Version to resolve.

    Returns:
      The registered handler callable.

    Raises:
      RuntimeError: If no handlers exist for the event, or none for the version.
    """
    bucket: Optional[MutableMapping[Version, Callable]] = _REGISTRY.get(event)
    if not bucket:
        raise RuntimeError(f"No handlers registered for event '{event}'")
    fn = bucket.get(version)
    if fn is None:
        raise RuntimeError(f"No handler for event '{event}' and version '{version}'")
    return fn
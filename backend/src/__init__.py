"""Backend package exports."""

from __future__ import annotations

from typing import Any

__all__ = ["app"]


def __getattr__(name: str) -> Any:
    if name == "app":
        from .app import app  # local import to avoid heavy dependencies during tests

        return app
    raise AttributeError(f"module 'backend.src' has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(__all__)

"""Three-line train API for the bake-off. jsonl/polars go through aspara; others stay in this spike."""

from __future__ import annotations

from typing import Any

from .factory import Settings, create_backend
from .protocol import ExperimentBackend

_backend: ExperimentBackend | None = None


def init(
    project: str | None = None,
    name: str | None = None,
    config: dict[str, Any] | None = None,
    tags: list[str] | None = None,
    notes: str | None = None,
    *,
    settings: Settings | None = None,
) -> ExperimentBackend:
    global _backend
    if _backend is not None:
        _backend.close()
        _backend = None
    resolved = settings or Settings.from_env()
    backend = create_backend(resolved)
    backend.init_run(project or "default", name or "run", config=config, tags=tags, notes=notes)
    _backend = backend
    return backend


def log(data: dict[str, float], step: int | None = None) -> None:
    if _backend is None:
        raise RuntimeError("No active run. Call init() first.")
    if step is None:
        raise RuntimeError("spike log() requires step= (train loop always passes it)")
    _backend.save_metrics(data, step=step)


def finish(exit_code: int = 0) -> None:
    global _backend
    if _backend is None:
        return
    _backend.finish_run(exit_code=exit_code)
    _backend.close()
    _backend = None


def abandon() -> None:
    """Close without finish_run. Experiment-only stand-in for a crash."""
    global _backend
    if _backend is None:
        return
    _backend.close()
    _backend = None


def reset() -> None:
    """Drop an active backend after a failed flush. Experiment only."""
    global _backend
    if _backend is None:
        return
    store = getattr(_backend, "store", None)
    if store is not None and hasattr(store, "_buffer"):
        store._buffer = []
    try:
        _backend.close()
    except Exception:
        pass
    _backend = None


def get_backend() -> ExperimentBackend | None:
    return _backend

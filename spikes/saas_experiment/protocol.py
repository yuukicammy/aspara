from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol

import polars as pl


class ReplicaMode(str, Enum):
    """Where log() and Dashboard read. Matches the experiment plan."""

    REMOTE = "remote"
    WRITE_REPLICA = "write_replica"
    READ_REPLICA = "read_replica"
    BOTH = "both"


@dataclass(frozen=True)
class RunRecord:
    name: str
    run_id: str | None = None
    tags: list[str] = field(default_factory=list)
    notes: str = ""
    is_finished: bool = False
    status: str = "wip"
    start_time_ms: int | None = None
    finish_time_ms: int | None = None


class ExperimentBackend(Protocol):
    """Storage used by the bake-off. Train script only calls init/log/finish."""

    name: str
    replica_mode: ReplicaMode

    def init_run(
        self,
        project: str,
        run: str,
        *,
        config: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
    ) -> None: ...

    def save_metrics(self, metrics: dict[str, float], step: int) -> None: ...

    def finish_run(self, exit_code: int = 0) -> None: ...

    def load_metrics(self, project: str, run: str) -> pl.DataFrame:
        """Wide Polars table: timestamp, step, _<metric>."""
        ...

    def get_run(self, project: str, run: str) -> RunRecord | None: ...

    def list_runs(self, project: str) -> list[RunRecord]: ...

    def close(self) -> None: ...

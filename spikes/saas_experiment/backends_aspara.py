from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import polars as pl

from .protocol import ReplicaMode, RunRecord


@dataclass
class AsparaFsBackend:
    """B0 jsonl / B1 polars via unchanged aspara.init → log → finish."""

    storage_backend: str
    data_dir: str
    replica_mode: ReplicaMode = ReplicaMode.REMOTE
    name: str = "jsonl"
    _project: str | None = None
    _run: str | None = None

    def __post_init__(self) -> None:
        self.name = self.storage_backend

    def init_run(
        self,
        project: str,
        run: str,
        *,
        config: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
    ) -> None:
        import os

        import aspara

        # aspara resolves env over the argument. B0/B1 must not inherit a leftover value.
        os.environ.pop("ASPARA_STORAGE_BACKEND", None)

        kwargs: dict[str, Any] = {
            "project": project,
            "name": run,
            "dir": self.data_dir,
            "config": config,
            "tags": tags,
            "notes": notes,
        }
        if self.storage_backend != "jsonl":
            kwargs["storage_backend"] = self.storage_backend
        aspara.init(**kwargs)
        self._project = project
        self._run = run

    def save_metrics(self, metrics: dict[str, float], step: int) -> None:
        import aspara

        aspara.log(metrics, step=step)

    def finish_run(self, exit_code: int = 0) -> None:
        import aspara

        aspara.finish(exit_code=exit_code, quiet=True)

    def load_metrics(self, project: str, run: str) -> pl.DataFrame:
        from aspara.storage.metrics import create_metrics_storage

        storage = create_metrics_storage(
            backend=self.storage_backend,
            base_dir=self.data_dir,
            project_name=project,
            run_name=run,
        )
        try:
            return storage.load()
        finally:
            storage.close()

    def get_run(self, project: str, run: str) -> RunRecord | None:
        from aspara.catalog import RunCatalog
        from aspara.exceptions import ProjectNotFoundError, RunNotFoundError

        catalog = RunCatalog(self.data_dir)
        try:
            info = catalog.get(project, run)
        except (ProjectNotFoundError, RunNotFoundError):
            return None
        return RunRecord(
            name=info.name,
            run_id=info.run_id,
            tags=list(info.tags),
            is_finished=info.is_finished,
            status=info.status.value,
        )

    def list_runs(self, project: str) -> list[RunRecord]:
        from aspara.catalog import RunCatalog
        from aspara.exceptions import ProjectNotFoundError

        catalog = RunCatalog(self.data_dir)
        try:
            infos = catalog.get_runs(project)
        except ProjectNotFoundError:
            return []
        return [
            RunRecord(
                name=info.name,
                run_id=info.run_id,
                tags=list(info.tags),
                is_finished=info.is_finished,
                status=info.status.value,
            )
            for info in infos
        ]

    def close(self) -> None:
        return

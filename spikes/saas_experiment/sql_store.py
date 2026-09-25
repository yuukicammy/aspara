from __future__ import annotations

import json
import time
import uuid
from collections.abc import Sequence
from typing import Any

import polars as pl

from .protocol import ReplicaMode, RunRecord
from .schema import SqlExecutor


def _now_ms() -> int:
    return int(time.time() * 1000)


class SqlStore:
    """Metrics + run/project meta on a SQL executor. Models are never stored."""

    def __init__(self, executor: SqlExecutor, *, flush_every: int, replica_mode: ReplicaMode) -> None:
        self._ex = executor
        self._flush_every = flush_every
        self.replica_mode = replica_mode
        self._project: str | None = None
        self._run: str | None = None
        self._buffer: list[tuple[Any, ...]] = []
        self._writable = replica_mode in {ReplicaMode.REMOTE, ReplicaMode.WRITE_REPLICA, ReplicaMode.BOTH}

    def ensure_schema(self, schema_sql: str) -> None:
        if self._ex.dialect == "sqlite":
            try:
                cols = self._ex.fetchall("PRAGMA table_info(metrics)")
            except Exception:
                cols = []
            names = {str(c[1]) for c in cols}
            if cols and "timestamp_ms" not in names:
                self._ex.executescript(
                    "DROP TABLE IF EXISTS metrics; DROP TABLE IF EXISTS runs; DROP TABLE IF EXISTS projects;"
                )
                self._ex.commit()
        self._ex.executescript(schema_sql)
        self._ex.commit()

    def init_run(
        self,
        project: str,
        run: str,
        *,
        config: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
    ) -> None:
        if not self._writable:
            msg = f"replica mode {self.replica_mode.value} cannot init/write"
            raise RuntimeError(msg)
        self._flush()
        self._project = project
        self._run = run
        ts = _now_ms()
        run_id = uuid.uuid4().hex
        self._upsert_project(project, ts)
        sql = self._ex.rewrite(
            """
            INSERT INTO runs (project, name, run_id, tags_json, notes, config_json, is_finished, status, start_time_ms)
            VALUES (?, ?, ?, ?, ?, ?, 0, 'wip', ?)
            ON CONFLICT (project, name) DO UPDATE SET
              run_id = excluded.run_id,
              tags_json = excluded.tags_json,
              notes = excluded.notes,
              config_json = excluded.config_json,
              is_finished = 0,
              exit_code = NULL,
              status = 'wip',
              start_time_ms = excluded.start_time_ms,
              finish_time_ms = NULL
            """
        )
        self._ex.execute(
            sql,
            (
                project,
                run,
                run_id,
                json.dumps(tags or []),
                notes or "",
                json.dumps(config or {}),
                ts,
            ),
        )
        self._ex.commit()

    def save_metrics(self, metrics: dict[str, float], step: int) -> None:
        if not self._writable:
            msg = f"replica mode {self.replica_mode.value} cannot write metrics"
            raise RuntimeError(msg)
        if self._project is None or self._run is None:
            raise RuntimeError("init_run first")
        ts = _now_ms()
        for name, value in metrics.items():
            self._buffer.append((self._project, self._run, step, ts, name, float(value)))
        if self._flush_every > 0 and len(self._buffer) >= self._flush_every:
            self._flush()

    def finish_run(self, exit_code: int = 0) -> None:
        if not self._writable:
            msg = f"replica mode {self.replica_mode.value} cannot finish"
            raise RuntimeError(msg)
        self._flush()
        if self._project is None or self._run is None:
            return
        status = "completed" if exit_code == 0 else "failed"
        sql = self._ex.rewrite(
            """
            UPDATE runs
            SET is_finished = 1, exit_code = ?, status = ?, finish_time_ms = ?
            WHERE project = ? AND name = ?
            """
        )
        self._ex.execute(sql, (exit_code, status, _now_ms(), self._project, self._run))
        self._ex.commit()

    def load_metrics(self, project: str, run: str) -> pl.DataFrame:
        sql = self._ex.rewrite("SELECT step, timestamp_ms, name, value FROM metrics WHERE project = ? AND run = ? ORDER BY step, name")
        rows = self._ex.fetchall(sql, (project, run))
        return rows_to_wide(rows)

    def get_run(self, project: str, run: str) -> RunRecord | None:
        sql = self._ex.rewrite(
            "SELECT name, run_id, tags_json, notes, is_finished, status, start_time_ms, finish_time_ms FROM runs WHERE project = ? AND name = ?"
        )
        row = self._ex.fetchone(sql, (project, run))
        if row is None:
            return None
        return _run_from_row(row)

    def list_runs(self, project: str) -> list[RunRecord]:
        sql = self._ex.rewrite(
            "SELECT name, run_id, tags_json, notes, is_finished, status, start_time_ms, finish_time_ms FROM runs WHERE project = ? ORDER BY name"
        )
        return [_run_from_row(row) for row in self._ex.fetchall(sql, (project,))]

    def close(self) -> None:
        self._flush()
        self._ex.close()

    def _upsert_project(self, project: str, ts: int) -> None:
        sql = self._ex.rewrite(
            """
            INSERT INTO projects (name, created_at_ms, updated_at_ms)
            VALUES (?, ?, ?)
            ON CONFLICT (name) DO UPDATE SET updated_at_ms = excluded.updated_at_ms
            """
        )
        self._ex.execute(sql, (project, ts, ts))

    def _flush(self) -> None:
        if not self._buffer:
            return
        # One HTTP/round-trip when the executor can batch. D1 caps 100 bound params,
        # so a flush is several multi-VALUES INSERTs in one request.
        chunk = max(1, self._ex.max_values_per_insert)
        buf = self._buffer
        value = "(?, ?, ?, ?, ?, ?)"
        statements: list[tuple[str, list[Any]]] = []
        for i in range(0, len(buf), chunk):
            part = buf[i : i + chunk]
            sql = "INSERT INTO metrics (project, run, step, timestamp_ms, name, value) VALUES " + ", ".join([value] * len(part))
            params: list[Any] = []
            for row in part:
                params.extend(row)
            statements.append((self._ex.rewrite(sql), params))
        if hasattr(self._ex, "execute_batch"):
            self._ex.execute_batch(statements)
        else:
            for sql, params in statements:
                self._ex.execute(sql, params)
        self._ex.commit()
        self._buffer = []


def rows_to_wide(rows: Sequence[tuple[Any, ...]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema={"timestamp": pl.Datetime("ms"), "step": pl.Int64})
    long = pl.DataFrame({
        "step": [int(r[0]) for r in rows],
        "timestamp_ms": [int(r[1]) for r in rows],
        "name": [str(r[2]) for r in rows],
        "value": [float(r[3]) for r in rows],
    })
    wide = long.pivot(values="value", index=["step", "timestamp_ms"], on="name")
    rename = {col: f"_{col}" for col in wide.columns if col not in {"step", "timestamp_ms"}}
    wide = wide.rename(rename).rename({"timestamp_ms": "timestamp"})
    wide = wide.with_columns(pl.col("timestamp").cast(pl.Datetime("ms")))
    return wide.sort(["timestamp", "step"])


def _run_from_row(row: tuple[Any, ...]) -> RunRecord:
    tags_raw = row[2]
    tags = json.loads(tags_raw) if isinstance(tags_raw, str) else (tags_raw or [])
    return RunRecord(
        name=str(row[0]),
        run_id=row[1],
        tags=list(tags),
        notes=str(row[3] or ""),
        is_finished=bool(row[4]),
        status=str(row[5]),
        start_time_ms=row[6],
        finish_time_ms=row[7],
    )

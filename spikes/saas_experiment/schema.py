from __future__ import annotations

from collections.abc import Iterable, Sequence
from typing import Any, Protocol

SCHEMA_SQLITE = """
CREATE TABLE IF NOT EXISTS projects (
  name TEXT PRIMARY KEY,
  notes TEXT NOT NULL DEFAULT '',
  tags_json TEXT NOT NULL DEFAULT '[]',
  created_at_ms INTEGER,
  updated_at_ms INTEGER
);
CREATE TABLE IF NOT EXISTS runs (
  project TEXT NOT NULL,
  name TEXT NOT NULL,
  run_id TEXT,
  tags_json TEXT NOT NULL DEFAULT '[]',
  notes TEXT NOT NULL DEFAULT '',
  params_json TEXT NOT NULL DEFAULT '{}',
  config_json TEXT NOT NULL DEFAULT '{}',
  summary_json TEXT NOT NULL DEFAULT '{}',
  is_finished INTEGER NOT NULL DEFAULT 0,
  exit_code INTEGER,
  status TEXT NOT NULL DEFAULT 'wip',
  start_time_ms INTEGER,
  finish_time_ms INTEGER,
  PRIMARY KEY (project, name)
);
CREATE TABLE IF NOT EXISTS metrics (
  project TEXT NOT NULL,
  run TEXT NOT NULL,
  step INTEGER NOT NULL,
  timestamp_ms INTEGER NOT NULL,
  name TEXT NOT NULL,
  value REAL NOT NULL,
  PRIMARY KEY (project, run, step, name)
);
CREATE INDEX IF NOT EXISTS metrics_run_step ON metrics (project, run, step);
"""

SCHEMA_POSTGRES = """
CREATE TABLE IF NOT EXISTS projects (
  name TEXT PRIMARY KEY,
  notes TEXT NOT NULL DEFAULT '',
  tags_json TEXT NOT NULL DEFAULT '[]',
  created_at_ms BIGINT,
  updated_at_ms BIGINT
);
CREATE TABLE IF NOT EXISTS runs (
  project TEXT NOT NULL,
  name TEXT NOT NULL,
  run_id TEXT,
  tags_json TEXT NOT NULL DEFAULT '[]',
  notes TEXT NOT NULL DEFAULT '',
  params_json TEXT NOT NULL DEFAULT '{}',
  config_json TEXT NOT NULL DEFAULT '{}',
  summary_json TEXT NOT NULL DEFAULT '{}',
  is_finished INTEGER NOT NULL DEFAULT 0,
  exit_code INTEGER,
  status TEXT NOT NULL DEFAULT 'wip',
  start_time_ms BIGINT,
  finish_time_ms BIGINT,
  PRIMARY KEY (project, name)
);
CREATE TABLE IF NOT EXISTS metrics (
  project TEXT NOT NULL,
  run TEXT NOT NULL,
  step BIGINT NOT NULL,
  timestamp_ms BIGINT NOT NULL,
  name TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (project, run, step, name)
);
CREATE INDEX IF NOT EXISTS metrics_run_step ON metrics (project, run, step);
"""


class SqlExecutor(Protocol):
    dialect: str
    max_values_per_insert: int

    def executescript(self, sql: str) -> None: ...

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None: ...

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None: ...

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]: ...

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None: ...

    def commit(self) -> None: ...

    def close(self) -> None: ...

    def rewrite(self, sql: str) -> str: ...


def sqlite_rewrite(sql: str) -> str:
    return sql


def postgres_rewrite(sql: str) -> str:
    return sql.replace("?", "%s")

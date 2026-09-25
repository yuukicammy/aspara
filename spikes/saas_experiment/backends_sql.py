from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import polars as pl

from .executors import D1Executor, DbapiExecutor, PostgresExecutor, SqliteExecutor
from .protocol import ReplicaMode, RunRecord
from .schema import SCHEMA_POSTGRES, SCHEMA_SQLITE
from .sql_store import SqlStore


@dataclass
class SqlBackend:
    store: SqlStore
    name: str
    replica_mode: ReplicaMode
    _on_finish: Any = None
    _on_before_read: Any = None

    def init_run(
        self,
        project: str,
        run: str,
        *,
        config: dict[str, Any] | None = None,
        tags: list[str] | None = None,
        notes: str | None = None,
    ) -> None:
        self.store.init_run(project, run, config=config, tags=tags, notes=notes)

    def save_metrics(self, metrics: dict[str, float], step: int) -> None:
        self.store.save_metrics(metrics, step)

    def finish_run(self, exit_code: int = 0) -> None:
        self.store.finish_run(exit_code=exit_code)
        if self._on_finish is not None:
            self._on_finish()

    def load_metrics(self, project: str, run: str) -> pl.DataFrame:
        if self._on_before_read is not None:
            self._on_before_read()
        return self.store.load_metrics(project, run)

    def get_run(self, project: str, run: str) -> RunRecord | None:
        if self._on_before_read is not None:
            self._on_before_read()
        return self.store.get_run(project, run)

    def list_runs(self, project: str) -> list[RunRecord]:
        if self._on_before_read is not None:
            self._on_before_read()
        return self.store.list_runs(project)

    def close(self) -> None:
        self.store.close()


def sqlite_backend(path: str, *, flush_every: int, replica_mode: ReplicaMode, name: str = "sqlite") -> SqlBackend:
    store = SqlStore(SqliteExecutor(path), flush_every=flush_every, replica_mode=replica_mode)
    store.ensure_schema(SCHEMA_SQLITE)
    return SqlBackend(store=store, name=name, replica_mode=replica_mode)


def postgres_backend(url: str, *, flush_every: int, replica_mode: ReplicaMode, name: str = "postgres") -> SqlBackend:
    try:
        import psycopg
    except ImportError as exc:
        from .errors import MissingDependencyError

        raise MissingDependencyError("psycopg", name) from exc
    conn = psycopg.connect(url)
    store = SqlStore(PostgresExecutor(conn), flush_every=flush_every, replica_mode=replica_mode)
    store.ensure_schema(SCHEMA_POSTGRES)
    return SqlBackend(store=store, name=name, replica_mode=replica_mode)


def d1_backend(
    *,
    account_id: str,
    database_id: str,
    token: str,
    flush_every: int,
    replica_mode: ReplicaMode,
) -> SqlBackend:
    store = SqlStore(
        D1Executor(account_id=account_id, database_id=database_id, token=token),
        flush_every=flush_every,
        replica_mode=replica_mode,
    )
    store.ensure_schema(SCHEMA_SQLITE)
    return SqlBackend(store=store, name="d1", replica_mode=replica_mode)


def turso_serverless_backend(url: str, token: str, *, flush_every: int, replica_mode: ReplicaMode) -> SqlBackend:
    try:
        import turso_serverless
    except ImportError as exc:
        from .errors import MissingDependencyError

        raise MissingDependencyError("turso_serverless", "turso_serverless") from exc
    conn = turso_serverless.connect(url, auth_token=token)
    store = SqlStore(
        DbapiExecutor(conn, use_batch=True),
        flush_every=flush_every,
        replica_mode=replica_mode,
    )
    store.ensure_schema(SCHEMA_SQLITE)
    return SqlBackend(store=store, name="turso_serverless", replica_mode=replica_mode)


def turso_libsql_backend(url: str, token: str, *, flush_every: int, replica_mode: ReplicaMode) -> SqlBackend:
    try:
        import libsql
    except ImportError as exc:
        from .errors import MissingDependencyError

        raise MissingDependencyError("libsql", "turso_libsql") from exc
    conn = libsql.connect(database=url, auth_token=token)
    store = SqlStore(DbapiExecutor(conn), flush_every=flush_every, replica_mode=replica_mode)
    store.ensure_schema(SCHEMA_SQLITE)
    return SqlBackend(store=store, name="turso_libsql", replica_mode=replica_mode)


def turso_embedded_backend(
    path: str,
    url: str,
    token: str,
    *,
    flush_every: int,
    replica_mode: ReplicaMode,
) -> SqlBackend:
    try:
        import libsql
    except ImportError as exc:
        from .errors import MissingDependencyError

        raise MissingDependencyError("libsql", "turso_embedded") from exc
    conn = libsql.connect(path, sync_url=url, auth_token=token)
    if hasattr(conn, "sync"):
        conn.sync()

    def before_read() -> None:
        if hasattr(conn, "sync"):
            conn.sync()

    store = SqlStore(DbapiExecutor(conn), flush_every=flush_every, replica_mode=replica_mode)
    store.ensure_schema(SCHEMA_SQLITE)
    return SqlBackend(store=store, name="turso_embedded", replica_mode=replica_mode, _on_before_read=before_read)


def turso_sync_backend(
    path: str,
    url: str,
    token: str,
    *,
    flush_every: int,
    replica_mode: ReplicaMode,
) -> SqlBackend:
    try:
        import turso.sync
    except ImportError as exc:
        from .errors import MissingDependencyError

        raise MissingDependencyError("pyturso", "turso_sync") from exc
    conn = _connect_turso_sync(path, url, token)

    def on_finish() -> None:
        if replica_mode in {ReplicaMode.WRITE_REPLICA, ReplicaMode.BOTH} and hasattr(conn, "push"):
            conn.push()

    def before_read() -> None:
        if replica_mode in {ReplicaMode.READ_REPLICA, ReplicaMode.BOTH} and hasattr(conn, "pull"):
            conn.pull()

    store = SqlStore(DbapiExecutor(conn), flush_every=flush_every, replica_mode=replica_mode)
    store.ensure_schema(SCHEMA_SQLITE)
    return SqlBackend(
        store=store,
        name="turso_sync",
        replica_mode=replica_mode,
        _on_finish=on_finish,
        _on_before_read=before_read,
    )


def _connect_turso_sync(path: str, url: str, token: str) -> Any:
    import os

    import turso.sync

    last: Exception | None = None
    for attempt in range(3):
        try:
            return turso.sync.connect(path, remote_url=url, auth_token=token)
        except Exception as exc:
            last = exc
            for extra in ("", "-wal", "-shm", "-info", "-changes"):
                candidate = f"{path}{extra}"
                if os.path.exists(candidate):
                    os.remove(candidate)
            time.sleep(1.5 * (attempt + 1))
    assert last is not None
    raise last

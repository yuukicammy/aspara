from __future__ import annotations

import json
import sqlite3
import urllib.error
import urllib.request
from collections.abc import Iterable, Sequence
from typing import Any

from .schema import postgres_rewrite, sqlite_rewrite


class SqliteExecutor:
    dialect = "sqlite"
    max_values_per_insert = 400

    def __init__(self, path: str) -> None:
        self._conn = sqlite3.connect(path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")

    def rewrite(self, sql: str) -> str:
        return sqlite_rewrite(sql)

    def executescript(self, sql: str) -> None:
        self._conn.executescript(sql)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self._conn.execute(sql, tuple(params))

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        self._conn.executemany(sql, list(seq))

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        return list(self._conn.execute(sql, tuple(params)).fetchall())

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None:
        return self._conn.execute(sql, tuple(params)).fetchone()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class DbapiExecutor:
    """sqlite3-shaped connections: libsql, turso_serverless, pyturso.sync."""

    dialect = "sqlite"
    max_values_per_insert = 100

    def __init__(self, conn: Any, *, use_batch: bool = False) -> None:
        self._conn = conn
        self._use_batch = use_batch

    def rewrite(self, sql: str) -> str:
        return sqlite_rewrite(sql)

    def executescript(self, sql: str) -> None:
        if hasattr(self._conn, "executescript"):
            self._conn.executescript(sql)
            return
        for stmt in _split_sql(sql):
            self._conn.execute(stmt)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        self._conn.execute(sql, tuple(params))

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        rows = list(seq)
        if not rows:
            return
        if self._use_batch and hasattr(self._conn, "batch"):
            statements = [(sql, tuple(row)) for row in rows]
            self._conn.batch(statements, mode="deferred")
            return
        if hasattr(self._conn, "executemany"):
            self._conn.executemany(sql, rows)
            return
        for row in rows:
            self._conn.execute(sql, tuple(row))

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        cur = self._conn.execute(sql, tuple(params))
        return list(cur.fetchall())

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None:
        cur = self._conn.execute(sql, tuple(params))
        return cur.fetchone()

    def commit(self) -> None:
        commit = getattr(self._conn, "commit", None)
        if commit is not None:
            commit()

    def close(self) -> None:
        close = getattr(self._conn, "close", None)
        if close is not None:
            close()


class PostgresExecutor:
    dialect = "postgres"
    max_values_per_insert = 400

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def rewrite(self, sql: str) -> str:
        return postgres_rewrite(sql)

    def executescript(self, sql: str) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql)

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        with self._conn.cursor() as cur:
            cur.execute(sql, tuple(params))

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        with self._conn.cursor() as cur:
            cur.executemany(sql, list(seq))

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        with self._conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return list(cur.fetchall())

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None:
        with self._conn.cursor() as cur:
            cur.execute(sql, tuple(params))
            return cur.fetchone()

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


class D1Executor:
    """Cloudflare D1 HTTP API. No extra package. Param cap is 100."""

    dialect = "sqlite"
    max_values_per_insert = 16

    def __init__(self, *, account_id: str, database_id: str, token: str) -> None:
        self._url = f"https://api.cloudflare.com/client/v4/accounts/{account_id}/d1/database/{database_id}/query"
        self._token = token

    def rewrite(self, sql: str) -> str:
        return sqlite_rewrite(sql)

    def executescript(self, sql: str) -> None:
        self._request({"sql": sql})

    def execute(self, sql: str, params: Sequence[Any] = ()) -> None:
        body: dict[str, Any] = {"sql": sql}
        if params:
            body["params"] = list(params)
        self._request(body)

    def executemany(self, sql: str, seq: Iterable[Sequence[Any]]) -> None:
        batch = [{"sql": sql, "params": list(row)} for row in seq]
        if not batch:
            return
        self._request({"batch": batch})

    def execute_batch(self, statements: Sequence[tuple[str, Sequence[Any]]]) -> None:
        batch = [{"sql": sql, "params": list(params)} for sql, params in statements]
        if not batch:
            return
        self._request({"batch": batch})

    def fetchall(self, sql: str, params: Sequence[Any] = ()) -> list[tuple[Any, ...]]:
        body: dict[str, Any] = {"sql": sql}
        if params:
            body["params"] = list(params)
        payload = self._request(body)
        return _d1_rows(payload)

    def fetchone(self, sql: str, params: Sequence[Any] = ()) -> tuple[Any, ...] | None:
        rows = self.fetchall(sql, params)
        return rows[0] if rows else None

    def commit(self) -> None:
        return

    def close(self) -> None:
        return

    def _request(self, body: dict[str, Any]) -> Any:
        data = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self._url,
            data=data,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except TimeoutError:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"D1 HTTP {exc.code}: {detail}") from exc
        if not payload.get("success", True):
            raise RuntimeError(f"D1 error: {payload}")
        return payload


def _d1_rows(payload: Any) -> list[tuple[Any, ...]]:
    result = payload.get("result") or []
    if not result:
        return []
    first = result[0]
    rows = first.get("results") or []
    out: list[tuple[Any, ...]] = []
    for row in rows:
        if isinstance(row, dict):
            out.append(tuple(row.values()))
        elif isinstance(row, list):
            out.append(tuple(row))
        else:
            out.append((row,))
    return out


def _split_sql(script: str) -> list[str]:
    return [part.strip() for part in script.split(";") if part.strip()]

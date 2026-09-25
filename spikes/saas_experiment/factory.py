from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import BackendConfigError, UnsupportedReplicaError
from .protocol import ExperimentBackend, ReplicaMode

# Replica kinds this spike can actually drive. Skip others as 非対応.
BACKENDS: dict[str, tuple[str, ...]] = {
    "jsonl": ("remote",),
    "polars": ("remote",),
    "sqlite": ("remote",),
    "litefs": ("remote", "write_replica", "read_replica", "both"),
    "postgres": ("remote", "read_replica"),
    "neon": ("remote", "read_replica"),
    "tiger": ("remote", "read_replica"),
    "d1": ("remote",),
    "turso_serverless": ("remote",),
    "turso_libsql": ("remote",),
    "turso_sync": ("write_replica", "read_replica", "both"),
    "turso_embedded": ("read_replica",),
}


@dataclass(frozen=True)
class Settings:
    backend: str
    mode: ReplicaMode
    data_dir: str
    db_path: str | None
    db_url: str | None
    db_token: str | None
    read_url: str | None
    flush_every: int
    d1_account: str | None
    d1_database: str | None
    d1_token: str | None

    @classmethod
    def from_env(cls, *, backend: str | None = None, mode: str | None = None) -> Settings:
        name = backend or os.environ.get("ASPARA_EXPERIMENT_BACKEND", "jsonl")
        mode_raw = mode or os.environ.get("ASPARA_EXPERIMENT_MODE", "remote")
        flush_raw = os.environ.get("ASPARA_EXPERIMENT_FLUSH_EVERY", "100")
        if name.startswith("turso"):
            db_url = os.environ.get("TURSO_DATABASE_URL")
            db_token = os.environ.get("TURSO_AUTH_TOKEN")
        elif name in {"postgres", "neon", "tiger"}:
            db_url = os.environ.get("ASPARA_EXPERIMENT_DB_URL") or os.environ.get("DATABASE_URL")
            db_token = os.environ.get("ASPARA_EXPERIMENT_DB_TOKEN")
        else:
            db_url = os.environ.get("ASPARA_EXPERIMENT_DB_URL")
            db_token = os.environ.get("ASPARA_EXPERIMENT_DB_TOKEN")
        return cls(
            backend=name,
            mode=ReplicaMode(mode_raw),
            data_dir=os.environ.get("ASPARA_EXPERIMENT_DATA_DIR", os.environ.get("ASPARA_DATA_DIR", "./saas-experiment-data")),
            db_path=os.environ.get("ASPARA_EXPERIMENT_DB_PATH"),
            db_url=db_url,
            db_token=db_token,
            read_url=os.environ.get("ASPARA_EXPERIMENT_READ_URL"),
            flush_every=int(flush_raw),
            d1_account=os.environ.get("ASPARA_EXPERIMENT_D1_ACCOUNT"),
            d1_database=os.environ.get("ASPARA_EXPERIMENT_D1_DATABASE"),
            d1_token=os.environ.get("ASPARA_EXPERIMENT_D1_TOKEN") or os.environ.get("CLOUDFLARE_API_TOKEN"),
        )


def create_backend(settings: Settings) -> ExperimentBackend:
    supported = BACKENDS.get(settings.backend)
    if supported is None:
        raise BackendConfigError(f"unknown backend {settings.backend!r}. known: {', '.join(BACKENDS)}")
    if settings.mode.value not in supported:
        raise UnsupportedReplicaError(settings.backend, settings.mode.value, supported)

    if settings.backend in {"jsonl", "polars"}:
        from .backends_aspara import AsparaFsBackend

        return AsparaFsBackend(
            storage_backend=settings.backend,
            data_dir=settings.data_dir,
            replica_mode=settings.mode,
        )

    flush = settings.flush_every
    mode = settings.mode

    if settings.backend in {"sqlite", "litefs"}:
        from .backends_sql import sqlite_backend

        path = settings.db_path or os.path.join(settings.data_dir, f"{settings.backend}.db")
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        return sqlite_backend(path, flush_every=flush, replica_mode=mode, name=settings.backend)

    if settings.backend in {"postgres", "neon", "tiger"}:
        from .backends_sql import postgres_backend

        url = settings.read_url if mode is ReplicaMode.READ_REPLICA else settings.db_url
        if not url:
            raise BackendConfigError(f"{settings.backend} needs ASPARA_EXPERIMENT_DB_URL (read replica: ASPARA_EXPERIMENT_READ_URL)")
        return postgres_backend(url, flush_every=flush, replica_mode=mode, name=settings.backend)

    if settings.backend == "d1":
        from .backends_sql import d1_backend

        if not (settings.d1_account and settings.d1_database and settings.d1_token):
            raise BackendConfigError("d1 needs ASPARA_EXPERIMENT_D1_ACCOUNT, D1_DATABASE, and D1_TOKEN")
        return d1_backend(
            account_id=settings.d1_account,
            database_id=settings.d1_database,
            token=settings.d1_token,
            flush_every=flush,
            replica_mode=mode,
        )

    if settings.backend == "turso_serverless":
        from .backends_sql import turso_serverless_backend

        url, token = _require_url_token(settings, "turso_serverless")
        return turso_serverless_backend(url, token, flush_every=flush, replica_mode=mode)

    if settings.backend == "turso_libsql":
        from .backends_sql import turso_libsql_backend

        url, token = _require_url_token(settings, "turso_libsql")
        return turso_libsql_backend(url, token, flush_every=flush, replica_mode=mode)

    if settings.backend == "turso_embedded":
        from .backends_sql import turso_embedded_backend

        url, token = _require_url_token(settings, "turso_embedded")
        path = settings.db_path or os.path.join(settings.data_dir, "turso-embedded.db")
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        return turso_embedded_backend(path, url, token, flush_every=flush, replica_mode=mode)

    if settings.backend == "turso_sync":
        from .backends_sql import turso_sync_backend

        url, token = _require_url_token(settings, "turso_sync")
        path = settings.db_path or os.path.join(settings.data_dir, "turso-sync.db")
        os.makedirs(settings.data_dir, exist_ok=True)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        return turso_sync_backend(path, url, token, flush_every=flush, replica_mode=mode)

    raise BackendConfigError(f"unwired backend {settings.backend!r}")


def _require_url_token(settings: Settings, backend: str) -> tuple[str, str]:
    if not settings.db_url or not settings.db_token:
        raise BackendConfigError(f"{backend} needs TURSO_DATABASE_URL and TURSO_AUTH_TOKEN")
    return settings.db_url, settings.db_token

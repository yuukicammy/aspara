"""E5 / E6 / E7 on the chosen path. Train scripts still only call init / log / finish."""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path
from typing import Any

from .api import abandon, finish, init, log, reset
from .factory import Settings, create_backend
from .protocol import ReplicaMode
from .timing import timed
from .workload import step_metrics

E5_STEPS = 250
E6_STEPS = 80
E7_STEPS = 10_000
CHILD_FLUSH = 100


def run_e5(settings: Settings, *, project: str, data_root: str) -> dict[str, Any]:
    root = Path(data_root)
    root.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%H%M%S")
    return {
        "id": "e5",
        "backend": settings.backend,
        "mode": settings.mode.value,
        "project": project,
        "steps": E5_STEPS,
        "crash_after_finish": _capture(
            lambda: _e5_crash_after_finish(settings, project=project, run=f"e5-crash-{stamp}", root=root)
        ),
        "disconnect": _capture(lambda: _e5_disconnect(settings, project=project, run=f"e5-cut-{stamp}", root=root)),
        "resume": _capture(lambda: _e5_resume(settings, project=project, run=f"e5-resume-{stamp}", root=root)),
        "two_jobs": _capture(lambda: _e5_two_jobs(settings, project=project, stamp=stamp, root=root)),
    }


def run_e6(settings: Settings, *, data_root: str) -> dict[str, Any]:
    root = Path(data_root)
    root.mkdir(parents=True, exist_ok=True)
    stamp = time.strftime("%H%M%S")
    tenant_a = f"tenant-a-{stamp}"
    tenant_b = f"tenant-b-{stamp}"
    write_dir = root / "e6-write"
    dash_dir = root / "e6-dash"
    write_settings = replace(settings, data_dir=str(write_dir), db_path=str(write_dir / "replica.db"))
    os.makedirs(write_dir, exist_ok=True)

    init(project=tenant_a, name="shared-name", notes="alpha", settings=write_settings)
    for step in range(E6_STEPS):
        log(step_metrics(step), step=step)
    finish()

    init(project=tenant_b, name="shared-name", notes="beta", settings=write_settings)
    for step in range(E6_STEPS):
        log(step_metrics(step), step=step)
    finish()

    local = _inspect_tenants(write_settings, tenant_a, tenant_b)
    remote = None
    if settings.backend == "turso_sync":
        dash_settings = replace(
            settings,
            mode=ReplicaMode.READ_REPLICA,
            data_dir=str(dash_dir),
            db_path=str(dash_dir / "replica.db"),
        )
        os.makedirs(dash_dir, exist_ok=True)
        remote = _inspect_tenants(dash_settings, tenant_a, tenant_b)
        remote["replica_projects"] = _sqlite_projects(dash_dir / "replica.db")
    local["replica_projects"] = _sqlite_projects(write_dir / "replica.db")
    mixed = sorted(set(local["replica_projects"]))
    return {
        "id": "e6",
        "backend": settings.backend,
        "mode": settings.mode.value,
        "steps": E6_STEPS,
        "tenant_a": tenant_a,
        "tenant_b": tenant_b,
        "list_isolated": local["a_names"] == ["shared-name"] and local["b_names"] == ["shared-name"],
        "notes_isolated": local["a_notes"] == "alpha" and local["b_notes"] == "beta",
        "same_run_name_ok": local["a_rows"] == E6_STEPS and local["b_rows"] == E6_STEPS,
        "replica_file_has_both_projects": set(mixed) >= {tenant_a, tenant_b},
        "local": local,
        "remote_after_pull": remote,
    }


def run_e7(settings: Settings, *, project: str, run: str, data_root: str, steps: int = E7_STEPS) -> dict[str, Any]:
    root = Path(data_root)
    root.mkdir(parents=True, exist_ok=True)
    chosen_dir = root / "e7-chosen"
    chosen = replace(settings, data_dir=str(chosen_dir), db_path=str(chosen_dir / "replica.db"))
    os.makedirs(chosen_dir, exist_ok=True)
    _train(chosen, project=project, run=run, steps=steps)
    query_settings = (
        replace(chosen, mode=ReplicaMode.WRITE_REPLICA) if settings.backend == "turso_sync" else chosen
    )
    chosen_query = _pipeline(query_settings, project=project, run=run)
    chosen_with_pull = _pipeline(chosen, project=project, run=run) if settings.backend == "turso_sync" else chosen_query

    baselines: dict[str, Any] = {}
    for name in ("jsonl", "polars"):
        base_dir = root / f"e7-{name}"
        base = Settings(
            backend=name,
            mode=ReplicaMode.REMOTE,
            data_dir=str(base_dir),
            db_path=None,
            db_url=None,
            db_token=None,
            read_url=None,
            flush_every=settings.flush_every,
            d1_account=None,
            d1_database=None,
            d1_token=None,
        )
        os.makedirs(base_dir, exist_ok=True)
        _train(base, project=project, run=run, steps=steps)
        baselines[name] = _pipeline(base, project=project, run=run)

    b0 = baselines["jsonl"]
    return {
        "id": "e7",
        "backend": settings.backend,
        "mode": settings.mode.value,
        "project": project,
        "run": run,
        "steps": steps,
        "chosen_query": chosen_query,
        "chosen_with_pull": chosen_with_pull,
        "chosen": chosen_query,
        "b0_jsonl": b0,
        "b1_polars": baselines["polars"],
        "vs_b0": {
            "query_load": _ratio(chosen_query.get("query_s"), b0.get("query_s")),
            "lttb": _ratio(chosen_query.get("compress_s"), b0.get("compress_s")),
            "msgpack": _ratio(chosen_query.get("pack_s"), b0.get("pack_s")),
            "query_plus_lttb_plus_pack": _ratio(
                (chosen_query.get("query_s") or 0) + (chosen_query.get("compress_s") or 0) + (chosen_query.get("pack_s") or 0),
                (b0.get("query_s") or 0) + (b0.get("compress_s") or 0) + (b0.get("pack_s") or 0),
            ),
            "connect_plus_pull_plus_query": _ratio(chosen_with_pull.get("load_s"), b0.get("load_s")),
        },
    }


def e5_child(*, settings: Settings, project: str, run: str, steps: int, crash: str) -> int:
    """Subprocess helper. crash=no_finish exits without finish/close."""
    init(project=project, name=run, settings=settings)
    for step in range(steps):
        log(step_metrics(step), step=step)
    if crash == "no_finish":
        os._exit(1)
    finish()
    return 0


def _train(settings: Settings, *, project: str, run: str, steps: int) -> None:
    init(project=project, name=run, settings=settings)
    for step in range(steps):
        log(step_metrics(step), step=step)
    finish()


def _e5_crash_after_finish(settings: Settings, *, project: str, run: str, root: Path) -> dict[str, Any]:
    write_dir = root / f"{run}-write"
    dash_dir = root / f"{run}-dash"
    write = replace(settings, data_dir=str(write_dir), db_path=str(write_dir / "replica.db"))
    os.makedirs(write_dir, exist_ok=True)
    _train(write, project=project, run=run, steps=E5_STEPS)
    local = _snapshot(write, project, run, raw_local=True)
    remote = _dashboard_snapshot(settings, dash_dir, project, run)
    return {
        "run": run,
        "local_rows": local["rows"],
        "local_finished": local["finished"],
        "remote_rows": None if remote is None else remote["rows"],
        "remote_finished": None if remote is None else remote["finished"],
        "cloud_complete": remote is None or (remote["rows"] == E5_STEPS and remote["finished"] is True),
        "three_line": "init / log / finish only",
    }


def _e5_disconnect(settings: Settings, *, project: str, run: str, root: Path) -> dict[str, Any]:
    write_dir = root / f"{run}-write"
    dash_dir = root / f"{run}-dash"
    write = replace(settings, data_dir=str(write_dir), db_path=str(write_dir / "replica.db"))
    os.makedirs(write_dir, exist_ok=True)
    child = _spawn_child(write, project=project, run=run, steps=E5_STEPS, crash="no_finish")
    child.wait()
    local = _snapshot(write, project, run, raw_local=True)
    remote = _dashboard_snapshot(settings, dash_dir, project, run)
    expected_flushed = (E5_STEPS * 5 // CHILD_FLUSH) * CHILD_FLUSH // 5
    return {
        "run": run,
        "child_returncode": child.returncode,
        "local_rows": local["rows"],
        "local_finished": local["finished"],
        "remote_rows": None if remote is None else remote["rows"],
        "remote_finished": None if remote is None else remote["finished"],
        "unflushed_lost_ok": local["rows"] < E5_STEPS,
        "remote_missing_until_finish": remote is None or remote["rows"] == 0,
        "note": f"flush_every={CHILD_FLUSH}; ~{expected_flushed} steps may be on disk if last commit landed",
    }


def _e5_resume(settings: Settings, *, project: str, run: str, root: Path) -> dict[str, Any]:
    write_dir = root / f"{run}-write"
    write = replace(settings, data_dir=str(write_dir), db_path=str(write_dir / "replica.db"))
    os.makedirs(write_dir, exist_ok=True)
    init(project=project, name=run, settings=write)
    for step in range(200):
        log(step_metrics(step), step=step)
    abandon()
    after_abandon = _snapshot(write, project, run, raw_local=True)

    reinit_error = None
    relog_error = None
    init(project=project, name=run, settings=write)
    try:
        log(step_metrics(0), step=0)
        abandon()
    except Exception as exc:
        relog_error = f"{type(exc).__name__}: {exc}"
        reset()
    after_reinit = _snapshot(write, project, run, raw_local=True)
    return {
        "run": run,
        "after_abandon_rows": after_abandon["rows"],
        "after_abandon_finished": after_abandon["finished"],
        "reinit_refused_like_main": reinit_error is not None,
        "reinit_error": reinit_error,
        "relog_same_step_error": relog_error,
        "after_reinit_rows": after_reinit["rows"],
        "run_id_changed": after_abandon["run_id"] != after_reinit["run_id"] and after_reinit["run_id"] is not None,
        "main_resume": "main is init(resume=True) and continues max(step)+1. spike 3-line init has no resume",
    }


def _e5_two_jobs(settings: Settings, *, project: str, stamp: str, root: Path) -> dict[str, Any]:
    a_dir = root / f"e5-job-a-{stamp}"
    b_dir = root / f"e5-job-b-{stamp}"
    clash_a = root / f"e5-clash-a-{stamp}"
    clash_b = root / f"e5-clash-b-{stamp}"
    dash = root / f"e5-jobs-dash-{stamp}"
    run_a = f"e5-job-a-{stamp}"
    run_b = f"e5-job-b-{stamp}"
    shared = f"e5-shared-{stamp}"

    sa = replace(settings, data_dir=str(a_dir), db_path=str(a_dir / "replica.db"))
    sb = replace(settings, data_dir=str(b_dir), db_path=str(b_dir / "replica.db"))
    os.makedirs(a_dir, exist_ok=True)
    os.makedirs(b_dir, exist_ok=True)
    pa = _spawn_child(sa, project=project, run=run_a, steps=E5_STEPS, crash="none")
    pb = _spawn_child(sb, project=project, run=run_b, steps=E5_STEPS, crash="none")
    pa.wait()
    pb.wait()

    different = _dashboard_list(settings, dash, project) if settings.backend == "turso_sync" else {
        "names": sorted({run_a, run_b}),
        "rows": {
            run_a: _snapshot(sa, project, run_a, raw_local=True)["rows"],
            run_b: _snapshot(sb, project, run_b, raw_local=True)["rows"],
        },
    }

    ca = replace(settings, data_dir=str(clash_a), db_path=str(clash_a / "replica.db"))
    cb = replace(settings, data_dir=str(clash_b), db_path=str(clash_b / "replica.db"))
    os.makedirs(clash_a, exist_ok=True)
    os.makedirs(clash_b, exist_ok=True)
    first = _spawn_child(ca, project=project, run=shared, steps=E5_STEPS, crash="none")
    first.wait()
    second = _spawn_child(cb, project=project, run=shared, steps=E5_STEPS, crash="none")
    second.wait()
    clash_remote = _dashboard_snapshot(settings, dash / "clash", project, shared)
    return {
        "different_names": {
            "a_code": pa.returncode,
            "b_code": pb.returncode,
            "both_ok": pa.returncode == 0 and pb.returncode == 0,
            "visible": different,
        },
        "same_name": {
            "first_code": first.returncode,
            "second_code": second.returncode,
            "second_failed": second.returncode != 0,
            "remote_rows": None if clash_remote is None else clash_remote["rows"],
            "note": "metrics PK is (project, run, step, name); second push of the same steps conflicts",
        },
    }


def _ratio(num: float | None, den: float | None) -> float | None:
    if num is None or den is None or den == 0:
        return None
    return num / den


def _pipeline(settings: Settings, *, project: str, run: str) -> dict[str, Any]:
    import msgpack

    from aspara.dashboard.utils.compression import compress_metrics

    backend, connect_s = timed(lambda: create_backend(settings))
    try:
        df, query_s = timed(lambda: backend.load_metrics(project, run))
    finally:
        backend.close()
    cols = list(df.columns)
    metric_cols = [c for c in cols if c.startswith("_")]
    compressed, compress_s = timed(lambda: compress_metrics(df))
    payload = {"project": project, "metrics": {name: {run: series} for name, series in compressed.items()}}
    packed, pack_s = timed(lambda: msgpack.packb(payload, use_single_float=True))
    json_bytes = len(json.dumps(payload).encode())
    after = {name: len(series["values"]) for name, series in compressed.items()}
    load_s = connect_s + query_s
    return {
        "wide": "timestamp" in cols and "step" in cols and bool(metric_cols),
        "columns": cols,
        "rows_before": int(df.height),
        "points_after": after,
        "lttb_applied": any(n < df.height for n in after.values()) if df.height else False,
        "connect_s": connect_s,
        "query_s": query_s,
        "load_s": load_s,
        "compress_s": compress_s,
        "pack_s": pack_s,
        "query_plus_lttb_plus_pack_s": query_s + compress_s + pack_s,
        "msgpack_bytes": 0 if packed is None else len(packed),
        "json_bytes": json_bytes,
        "msgpack_smaller": packed is not None and len(packed) < json_bytes,
        "includes_pull": settings.mode in {ReplicaMode.READ_REPLICA, ReplicaMode.BOTH},
    }


def _inspect_tenants(settings: Settings, tenant_a: str, tenant_b: str) -> dict[str, Any]:
    backend = create_backend(settings)
    try:
        a_runs = backend.list_runs(tenant_a)
        b_runs = backend.list_runs(tenant_b)
        a = backend.get_run(tenant_a, "shared-name")
        b = backend.get_run(tenant_b, "shared-name")
        a_df = backend.load_metrics(tenant_a, "shared-name")
        b_df = backend.load_metrics(tenant_b, "shared-name")
        leaked_a = [r.name for r in a_runs if r.notes == "beta"]
        leaked_b = [r.name for r in b_runs if r.notes == "alpha"]
    finally:
        backend.close()
    return {
        "a_names": [r.name for r in a_runs],
        "b_names": [r.name for r in b_runs],
        "a_notes": None if a is None else a.notes,
        "b_notes": None if b is None else b.notes,
        "a_rows": int(a_df.height),
        "b_rows": int(b_df.height),
        "leaked_into_a": leaked_a,
        "leaked_into_b": leaked_b,
    }


def _capture(fn: Any) -> dict[str, Any]:
    try:
        return fn()
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _snapshot(settings: Settings, project: str, run: str, *, raw_local: bool = False) -> dict[str, Any]:
    if raw_local and settings.db_path:
        return _snapshot_sqlite_file(settings.db_path, project, run)
    inspect = settings
    if settings.backend == "turso_sync" and settings.mode is ReplicaMode.BOTH:
        inspect = replace(settings, mode=ReplicaMode.WRITE_REPLICA)
    backend = create_backend(inspect)
    try:
        rec = backend.get_run(project, run)
        df = backend.load_metrics(project, run)
    finally:
        backend.close()
    return {
        "rows": int(df.height),
        "finished": None if rec is None else rec.is_finished,
        "status": None if rec is None else rec.status,
        "run_id": None if rec is None else rec.run_id,
    }


def _snapshot_sqlite_file(path: str, project: str, run: str) -> dict[str, Any]:
    if not Path(path).exists():
        return {"rows": 0, "finished": None, "status": None, "run_id": None, "missing_file": True}
    conn = sqlite3.connect(path)
    try:
        rec = conn.execute(
            "SELECT run_id, is_finished, status FROM runs WHERE project = ? AND name = ?",
            (project, run),
        ).fetchone()
        n = conn.execute(
            "SELECT COUNT(DISTINCT step) FROM metrics WHERE project = ? AND run = ?",
            (project, run),
        ).fetchone()[0]
    except sqlite3.Error as exc:
        return {"rows": 0, "finished": None, "status": None, "run_id": None, "sqlite_error": str(exc)}
    finally:
        conn.close()
    return {
        "rows": int(n or 0),
        "finished": None if rec is None else bool(rec[1]),
        "status": None if rec is None else rec[2],
        "run_id": None if rec is None else rec[0],
    }


def _dashboard_snapshot(settings: Settings, dash_dir: Path, project: str, run: str) -> dict[str, Any] | None:
    if settings.backend != "turso_sync":
        return None
    os.makedirs(dash_dir, exist_ok=True)
    dash = replace(
        settings,
        mode=ReplicaMode.READ_REPLICA,
        data_dir=str(dash_dir),
        db_path=str(dash_dir / "replica.db"),
    )
    return _snapshot(dash, project, run)


def _dashboard_list(settings: Settings, dash_dir: Path, project: str) -> dict[str, Any]:
    os.makedirs(dash_dir, exist_ok=True)
    dash = replace(
        settings,
        mode=ReplicaMode.READ_REPLICA,
        data_dir=str(dash_dir),
        db_path=str(dash_dir / "replica.db"),
    )
    backend = create_backend(dash)
    try:
        runs = backend.list_runs(project)
        rows = {r.name: int(backend.load_metrics(project, r.name).height) for r in runs}
    finally:
        backend.close()
    return {"names": [r.name for r in runs], "rows": rows}


def _sqlite_projects(path: Path) -> list[str]:
    if not path.exists():
        return []
    conn = sqlite3.connect(path)
    try:
        try:
            rows = conn.execute("SELECT name FROM projects ORDER BY name").fetchall()
        except sqlite3.Error:
            return []
    finally:
        conn.close()
    return [str(r[0]) for r in rows]


def _spawn_child(settings: Settings, *, project: str, run: str, steps: int, crash: str) -> subprocess.Popen[str]:
    env = os.environ.copy()
    env["ASPARA_EXPERIMENT_BACKEND"] = settings.backend
    env["ASPARA_EXPERIMENT_MODE"] = settings.mode.value
    env["ASPARA_EXPERIMENT_DATA_DIR"] = settings.data_dir
    env["ASPARA_EXPERIMENT_FLUSH_EVERY"] = str(CHILD_FLUSH)
    if settings.db_path:
        env["ASPARA_EXPERIMENT_DB_PATH"] = settings.db_path
    if settings.db_url:
        env["TURSO_DATABASE_URL"] = settings.db_url
    if settings.db_token:
        env["TURSO_AUTH_TOKEN"] = settings.db_token
    main = Path(__file__).resolve().parent / "__main__.py"
    return subprocess.Popen(
        [
            sys.executable,
            str(main),
            "e5-child",
            "--project",
            project,
            "--run",
            run,
            "--steps",
            str(steps),
            "--crash",
            crash,
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

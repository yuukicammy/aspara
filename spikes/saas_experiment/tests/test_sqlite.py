from __future__ import annotations

from pathlib import Path

from saas_experiment.errors import UnsupportedReplicaError
from saas_experiment.factory import Settings, create_backend
from saas_experiment.protocol import ReplicaMode
from saas_experiment.workload import step_metrics


def _sqlite_settings(tmp_path: Path, *, mode: str = "remote") -> Settings:
    return Settings(
        backend="sqlite",
        mode=ReplicaMode(mode),
        data_dir=str(tmp_path),
        db_path=str(tmp_path / "bench.db"),
        db_url=None,
        db_token=None,
        read_url=None,
        flush_every=10,
        d1_account=None,
        d1_database=None,
        d1_token=None,
    )


def test_sqlite_log_load_list(tmp_path: Path) -> None:
    settings = _sqlite_settings(tmp_path)
    backend = create_backend(settings)
    backend.init_run("p", "r1", config={"lr": 0.1}, tags=["bench"])
    for step in range(20):
        backend.save_metrics(step_metrics(step), step=step)
    backend.finish_run(0)

    df = backend.load_metrics("p", "r1")
    assert df.height == 20
    assert "step" in df.columns
    assert "_loss" in df.columns
    assert "_acc" in df.columns
    run = backend.get_run("p", "r1")
    assert run is not None
    assert run.is_finished
    assert backend.list_runs("p")[0].name == "r1"
    backend.close()


def test_sqlite_read_replica_rejects_write(tmp_path: Path) -> None:
    settings = _sqlite_settings(tmp_path, mode="read_replica")
    try:
        create_backend(settings)
        raise AssertionError("litefs-style read replica is not on sqlite")
    except UnsupportedReplicaError as exc:
        assert exc.backend == "sqlite"


def test_unknown_backend(tmp_path: Path) -> None:
    settings = _sqlite_settings(tmp_path)
    settings = Settings(
        backend="nope",
        mode=ReplicaMode.REMOTE,
        data_dir=settings.data_dir,
        db_path=settings.db_path,
        db_url=None,
        db_token=None,
        read_url=None,
        flush_every=1,
        d1_account=None,
        d1_database=None,
        d1_token=None,
    )
    try:
        create_backend(settings)
        raise AssertionError("expected config error")
    except Exception as exc:
        assert "unknown backend" in str(exc)

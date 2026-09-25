from __future__ import annotations

from pathlib import Path

from saas_experiment.factory import Settings, create_backend
from saas_experiment.protocol import ReplicaMode
from saas_experiment.workload import step_metrics


def test_jsonl_three_line_path(tmp_path: Path) -> None:
    settings = Settings(
        backend="jsonl",
        mode=ReplicaMode.REMOTE,
        data_dir=str(tmp_path),
        db_path=None,
        db_url=None,
        db_token=None,
        read_url=None,
        flush_every=1,
        d1_account=None,
        d1_database=None,
        d1_token=None,
    )
    backend = create_backend(settings)
    backend.init_run("p", "r1")
    backend.save_metrics(step_metrics(0), step=0)
    backend.save_metrics(step_metrics(1), step=1)
    backend.finish_run(0)
    df = backend.load_metrics("p", "r1")
    assert df.height == 2
    assert "_loss" in df.columns
    listed = backend.list_runs("p")
    assert [r.name for r in listed] == ["r1"]
    backend.close()

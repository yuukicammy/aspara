from __future__ import annotations

from pathlib import Path

from saas_experiment.factory import Settings
from saas_experiment.protocol import ReplicaMode
from saas_experiment.scenarios import run_e5, run_e6, run_e7


def _sqlite(tmp_path: Path) -> Settings:
    return Settings(
        backend="sqlite",
        mode=ReplicaMode.REMOTE,
        data_dir=str(tmp_path),
        db_path=str(tmp_path / "replica.db"),
        db_url=None,
        db_token=None,
        read_url=None,
        flush_every=100,
        d1_account=None,
        d1_database=None,
        d1_token=None,
    )


def test_e5_crash_and_resume_on_sqlite(tmp_path: Path) -> None:
    result = run_e5(_sqlite(tmp_path / "e5"), project="p", data_root=str(tmp_path / "e5"))
    crash = result["crash_after_finish"]
    assert crash["local_rows"] == 250
    assert crash["local_finished"] is True
    resume = result["resume"]
    assert resume["after_abandon_rows"] == 200
    assert resume["after_abandon_finished"] is False
    assert resume["reinit_refused_like_main"] is False
    assert resume["relog_same_step_error"]
    assert resume["after_reinit_rows"] == 200
    cut = result["disconnect"]
    assert cut["local_rows"] < 250
    assert cut["local_finished"] is not True


def test_e6_two_projects_isolated(tmp_path: Path) -> None:
    result = run_e6(_sqlite(tmp_path / "e6"), data_root=str(tmp_path / "e6"))
    assert result["list_isolated"] is True
    assert result["notes_isolated"] is True
    assert result["same_run_name_ok"] is True
    assert result["replica_file_has_both_projects"] is True
    assert result["local"]["leaked_into_a"] == []
    assert result["local"]["leaked_into_b"] == []


def test_e7_wide_lttb_msgpack(tmp_path: Path) -> None:
    result = run_e7(
        _sqlite(tmp_path / "e7"),
        project="p",
        run="e7-0000",
        data_root=str(tmp_path / "e7"),
        steps=1_200,
    )
    chosen = result["chosen"]
    assert chosen["wide"] is True
    assert chosen["rows_before"] == 1_200
    assert chosen["lttb_applied"] is True
    assert chosen["points_after"]["loss"] == 1_000
    assert chosen["msgpack_smaller"] is True
    assert "query_s" in chosen
    assert "compress_s" in chosen
    assert "vs_b0" in result
    assert result["b0_jsonl"]["wide"] is True
    assert result["b1_polars"]["wide"] is True

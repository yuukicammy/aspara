from saas_experiment.cli import main


def test_backends_lists_candidates(capsys) -> None:
    assert main(["backends"]) == 0
    out = capsys.readouterr().out
    assert "turso_sync" in out
    assert "d1" in out
    assert "neon" in out
    assert "litefs" in out

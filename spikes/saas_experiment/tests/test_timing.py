from saas_experiment.timing import percentile, summarize


def test_percentile_and_summarize() -> None:
    samples = [0.1, 0.2, 0.3, 0.4, 1.0]
    stats = summarize(samples)
    assert stats.n == 5
    assert abs(stats.median_s - 0.3) < 1e-9
    assert percentile(sorted(samples), 0.95) >= 0.4
    empty = summarize([])
    assert empty.n == 0

"""Fixed load for every backend. Do not change between B0 and E*."""

METRIC_NAMES = ("loss", "acc", "lr", "grad_norm", "epoch_time")
DEFAULT_STEPS = 10_000
DEFAULT_COMPARE_RUNS = 10
DEFAULT_LIST_RUNS = 100
DEFAULT_PROJECT = "saas-bench"


def step_metrics(step: int) -> dict[str, float]:
    return {
        "loss": 1.0 / (step + 1),
        "acc": min(0.99, step / 10_000),
        "lr": 0.001,
        "grad_norm": 0.1 + (step % 50) * 0.001,
        "epoch_time": 0.02,
    }


def run_name(prefix: str, index: int) -> str:
    return f"{prefix}-{index:04d}"

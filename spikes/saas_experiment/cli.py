from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import replace
from pathlib import Path

from .factory import BACKENDS, Settings, create_backend
from .timing import summarize, timed
from .workload import (
    DEFAULT_COMPARE_RUNS,
    DEFAULT_LIST_RUNS,
    DEFAULT_PROJECT,
    DEFAULT_STEPS,
    run_name,
    step_metrics,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Aspara SaaS DB bake-off (spike). Does not modify src/aspara.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("backends", help="list backends and replica modes")

    train = sub.add_parser("train", help="init → log × N → finish")
    _add_common(train)
    train.add_argument("--project", default=DEFAULT_PROJECT)
    train.add_argument("--run", default="train-0000")
    train.add_argument("--steps", type=int, default=DEFAULT_STEPS)

    read = sub.add_parser("read", help="open one run, compare, list")
    _add_common(read)
    read.add_argument("--project", default=DEFAULT_PROJECT)
    read.add_argument("--run", default="train-0000")
    read.add_argument("--compare-prefix", default="compare")
    read.add_argument("--compare-n", type=int, default=DEFAULT_COMPARE_RUNS)
    read.add_argument("--list", action="store_true")

    seed = sub.add_parser("seed", help="write extra runs for compare/list")
    _add_common(seed)
    seed.add_argument("--project", default=DEFAULT_PROJECT)
    seed.add_argument("--prefix", default="seed")
    seed.add_argument("--n", type=int, default=DEFAULT_LIST_RUNS)
    seed.add_argument("--steps", type=int, default=1, help="metrics steps per seeded run")

    e5 = sub.add_parser("e5", help="crash / resume / disconnect / two jobs")
    _add_common(e5)
    e5.add_argument("--project", default="saas-e5e7")

    e6 = sub.add_parser("e6", help="two tenants")
    _add_common(e6)

    e7 = sub.add_parser("e7", help="load → LTTB → msgpack vs B0/B1")
    _add_common(e7)
    e7.add_argument("--project", default="saas-e5e7")
    e7.add_argument("--run", default="e7-0000")
    e7.add_argument("--steps", type=int, default=10_000)

    child = sub.add_parser("e5-child", help=argparse.SUPPRESS)
    _add_common(child)
    child.add_argument("--project", required=True)
    child.add_argument("--run", required=True)
    child.add_argument("--steps", type=int, required=True)
    child.add_argument("--crash", choices=("none", "no_finish"), default="none")

    args = parser.parse_args(argv)
    if args.cmd == "backends":
        return cmd_backends()
    settings = Settings.from_env(backend=args.backend, mode=args.mode)
    updates = {}
    if args.data_dir:
        updates["data_dir"] = args.data_dir
    if args.flush_every is not None:
        updates["flush_every"] = args.flush_every
    if updates:
        settings = replace(settings, **updates)
    if args.cmd == "train":
        return cmd_train(settings, project=args.project, run=args.run, steps=args.steps)
    if args.cmd == "read":
        return cmd_read(
            settings,
            project=args.project,
            run=args.run,
            compare_prefix=args.compare_prefix,
            compare_n=args.compare_n,
            do_list=args.list,
        )
    if args.cmd == "seed":
        return cmd_seed(settings, project=args.project, prefix=args.prefix, n=args.n, steps=args.steps)
    if args.cmd == "e5":
        from .scenarios import run_e5

        print(json.dumps(run_e5(settings, project=args.project, data_root=settings.data_dir), indent=2))
        return 0
    if args.cmd == "e6":
        from .scenarios import run_e6

        print(json.dumps(run_e6(settings, data_root=settings.data_dir), indent=2))
        return 0
    if args.cmd == "e7":
        from .scenarios import run_e7

        print(json.dumps(run_e7(settings, project=args.project, run=args.run, data_root=settings.data_dir, steps=args.steps), indent=2))
        return 0
    if args.cmd == "e5-child":
        from .scenarios import e5_child

        return e5_child(settings=settings, project=args.project, run=args.run, steps=args.steps, crash=args.crash)
    return 1


def _add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--backend", default=None)
    parser.add_argument("--mode", default=None, help="remote | write_replica | read_replica | both")
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--flush-every", type=int, default=None)


def cmd_backends() -> int:
    rows = [{"backend": name, "modes": list(modes)} for name, modes in BACKENDS.items()]
    print(json.dumps(rows, indent=2))
    return 0


def cmd_train(settings: Settings, *, project: str, run: str, steps: int) -> int:
    from .api import finish, init, log

    os.makedirs(settings.data_dir, exist_ok=True)
    log_samples: list[float] = []
    _, init_s = timed(lambda: init(project=project, name=run, settings=settings))
    loop_start = time.perf_counter()
    for step in range(steps):
        _, elapsed = timed(lambda s=step: log(step_metrics(s), step=s))
        log_samples.append(elapsed)
    loop_s = time.perf_counter() - loop_start
    _, finish_s = timed(lambda: finish())
    stats = summarize(log_samples)
    payload = {
        "id": "train",
        "backend": settings.backend,
        "mode": settings.mode.value,
        "project": project,
        "run": run,
        "steps": steps,
        "init_s": init_s,
        "loop_s": loop_s,
        "finish_s": finish_s,
        "log": stats.as_dict(),
    }
    print(json.dumps(payload, indent=2))
    return 0


def cmd_read(
    settings: Settings,
    *,
    project: str,
    run: str,
    compare_prefix: str,
    compare_n: int,
    do_list: bool,
) -> int:
    backend = create_backend(settings)
    try:
        df, open_s = timed(lambda: backend.load_metrics(project, run))
        compare_s = None
        if compare_n > 0:
            names = [run_name(compare_prefix, i) for i in range(compare_n)]

            def _compare() -> int:
                n = 0
                for name in names:
                    backend.load_metrics(project, name)
                    n += 1
                return n

            _, compare_s = timed(_compare)
        list_s = None
        listed = None
        if do_list:
            runs, list_s = timed(lambda: backend.list_runs(project))
            listed = len(runs)
        payload = {
            "id": "read",
            "backend": settings.backend,
            "mode": settings.mode.value,
            "open_s": open_s,
            "open_rows": int(df.height),
            "compare_s": compare_s,
            "compare_n": compare_n,
            "list_s": list_s,
            "list_n": listed,
        }
        print(json.dumps(payload, indent=2))
    finally:
        backend.close()
    return 0


def cmd_seed(settings: Settings, *, project: str, prefix: str, n: int, steps: int) -> int:
    from .api import finish, init, log

    os.makedirs(settings.data_dir, exist_ok=True)
    start = time.perf_counter()
    for i in range(n):
        name = run_name(prefix, i)
        init(project=project, name=name, settings=settings)
        for step in range(steps):
            log(step_metrics(step), step=step)
        finish()
    payload = {
        "id": "seed",
        "backend": settings.backend,
        "mode": settings.mode.value,
        "n": n,
        "steps": steps,
        "total_s": time.perf_counter() - start,
    }
    print(json.dumps(payload, indent=2))
    return 0


def _repo_spikes() -> Path:
    return Path(__file__).resolve().parent.parent


if __name__ == "__main__":
    sys.path.insert(0, str(_repo_spikes()))
    raise SystemExit(main())

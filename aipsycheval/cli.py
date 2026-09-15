"""Command line interface: `aipsycheval run` and `aipsycheval dashboard`."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

from .config import ConfigError, load_config


def _run(args: argparse.Namespace) -> int:
    from .llm import LLMError
    from .simulation import Simulation
    from .storage import RunStore

    try:
        config = load_config(args.config, output_dir=args.output)
        if args.concurrency:
            config.simulation.concurrency = args.concurrency
        simulation = Simulation(config)
        states = RunStore(config.output_dir).load_states() if config.output_dir.exists() and not args.dry_run else {}
        print(simulation.describe(states))
        if args.dry_run:
            print("\nDry run: nothing was executed.")
            return 0
        print()
        failures = asyncio.run(simulation.run())
        return 1 if failures else 0
    except (ConfigError, LLMError) as error:
        print(f"Error: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("\nInterrupted. Progress is checkpointed; re-run the same command to resume.", file=sys.stderr)
        return 130


def _dashboard(args: argparse.Namespace) -> int:
    try:
        from .dashboard.app import create_app
    except ImportError as error:
        print(f"Error: {error}. Install the dashboard dependencies with: pip install -e .", file=sys.stderr)
        return 2
    data_dir = Path(args.data_dir).resolve()
    if not data_dir.is_dir():
        print(f"Error: {data_dir} is not a directory of result CSVs.", file=sys.stderr)
        return 2
    app = create_app(data_dir)
    print(f"Serving results from {data_dir}\nOpen http://{args.host}:{args.port} in your browser (Ctrl+C to stop).")
    app.run(host=args.host, port=args.port, debug=False, threaded=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="aipsycheval", description="Evaluate AI psychotherapists with simulated patients.")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Run or resume a simulation from a YAML config.")
    run.add_argument("config", help="Path to a run config, e.g. configs/quickstart_mock.yaml")
    run.add_argument("--output", help="Override output_dir from the config.")
    run.add_argument("--concurrency", type=int, help="Override simulation.concurrency (pairings run in parallel).")
    run.add_argument("--dry-run", action="store_true", help="Validate the config and print the plan without calling any model.")
    run.set_defaults(func=_run)

    dash = sub.add_parser("dashboard", help="Open the interactive dashboard for a results directory.")
    dash.add_argument("data_dir", nargs="?", default="dataset", help="Directory with result CSVs (default: dataset, the dissertation data).")
    dash.add_argument("--host", default="127.0.0.1")
    dash.add_argument("--port", type=int, default=8080)
    dash.set_defaults(func=_dashboard)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())

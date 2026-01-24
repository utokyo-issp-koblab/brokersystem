#!/usr/bin/env python3
"""Run formatter, type checker, and tests using Poetry.

This script is cross-platform and intended as the standard dev entrypoint.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from collections.abc import Iterable


def run_step(cmd: list[str]) -> int:
    print("+", " ".join(cmd), flush=True)
    return subprocess.run(cmd, check=False).returncode


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check", action="store_true", help="Run black in check-only mode"
    )
    parser.add_argument("--skip-black", action="store_true", help="Skip black")
    parser.add_argument("--skip-pyright", action="store_true", help="Skip pyright")
    parser.add_argument("--skip-pytest", action="store_true", help="Skip pytest")
    args = parser.parse_args(argv)

    if shutil.which("poetry") is None:
        print("poetry not found; install it to run checks.", file=sys.stderr)
        return 1

    steps: list[list[str]] = []
    if not args.skip_black:
        black_cmd = ["poetry", "run", "black", "."]
        if args.check:
            black_cmd.append("--check")
        steps.append(black_cmd)
    if not args.skip_pyright:
        steps.append(["poetry", "run", "pyright"])
    if not args.skip_pytest:
        steps.append(["poetry", "run", "pytest"])

    for cmd in steps:
        code = run_step(cmd)
        if code != 0:
            return code

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

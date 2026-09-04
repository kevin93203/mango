#!/usr/bin/env python3
"""Small deterministic task used by the Mango workflow examples."""

import argparse
import sys
import time


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--name", default="python-task")
    parser.add_argument("--steps", type=int, default=3)
    parser.add_argument("--delay", type=float, default=0.25)
    parser.add_argument("--fail", action="store_true")
    args = parser.parse_args()

    if args.steps < 1:
        print("steps must be positive", file=sys.stderr)
        return 2

    print(f"[{args.name}] started")
    for step in range(1, args.steps + 1):
        print(f"[{args.name}] step {step}/{args.steps}")
        time.sleep(args.delay)

    if args.fail:
        print(f"[{args.name}] intentional failure", file=sys.stderr)
        return 7
    print(f"[{args.name}] completed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

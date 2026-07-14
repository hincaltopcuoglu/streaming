"""
Tiny CLI bridge so the Spark driver can write the model snapshot to Redis
without importing api.state directly inside the JVM process.
Usage:
    python -m api.cli save_state --weights-json '[0.12, -0.04]' \
        --intercept -0.9069 \
        --meta-json '{"update_count": 7, "last_accuracy": 0.68}'
Why a subprocess instead of importing api.state from spark/job.py?
PySpark's driver already initializes a Python process for the executor
side; importing extra C-extension packages there is sometimes fragile.
A subprocess is a 10-line bullet-proof bridge.
"""
import argparse
import json
import sys
import api.state as state


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("save_state")
    p.add_argument("--weights-json", required=True)
    p.add_argument("--intercept", type=float, required=True)
    p.add_argument("--meta-json", required=True)
    sub.add_parser("reset")
    sub.add_parser("ping")
    args = parser.parse_args()
    if args.cmd == "save_state":
        state.save_state(
            weights=json.loads(args.weights_json),
            intercept=args.intercept,
            meta=json.loads(args.meta_json),
        )
        print("OK saved")
    elif args.cmd == "reset":
        state.reset_state()
        print("OK reset")
    elif args.cmd == "ping":
        print("OK" if state.health_check() else "DOWN")
if __name__ == "__main__":
    main()


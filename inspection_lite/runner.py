from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys
import traceback
import time


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("job_json", help="path to job JSON")
    parser.add_argument("--duration-sec", type=int, default=5)
    parser.add_argument("--fail", action="store_true", help="raise a sample exception")
    parser.add_argument("--fail-message", default="sample runner failure")
    args = parser.parse_args(argv)

    job_path = Path(args.job_json)
    job = json.loads(job_path.read_text(encoding="utf-8"))
    cancel_file = os.getenv("INSPECTION_CANCEL_FILE")

    print(f"runner started job_id={job['job_id']} process={job.get('process')}", flush=True)
    if args.fail:
        try:
            raise RuntimeError(args.fail_message)
        except RuntimeError:
            traceback.print_exc(file=sys.stderr)
            sys.exit(1)

    for second in range(args.duration_sec):
        if cancel_file and Path(cancel_file).exists():
            print(f"runner canceled job_id={job['job_id']}", flush=True)
            sys.exit(130)
        print(f"runner progress job_id={job['job_id']} second={second + 1}", flush=True)
        time.sleep(1)
    print(f"runner done job_id={job['job_id']}", flush=True)


if __name__ == "__main__":
    main()

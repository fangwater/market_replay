#!/usr/bin/env python3
"""Persistent daily scheduler for PM2; execution uses the process TZ setting."""

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path


SCRIPTS = Path(__file__).resolve().parent
if str(SCRIPTS) not in sys.path:
    sys.path.insert(0, str(SCRIPTS))

from daily_sync import run_once


RUN_HOUR = 18
RUN_MINUTE = 15


def seconds_until_next_run():
    now = datetime.now()
    scheduled = now.replace(hour=RUN_HOUR, minute=RUN_MINUTE, second=0, microsecond=0)
    if scheduled <= now:
        scheduled += timedelta(days=1)
    return max(1, int((scheduled - now).total_seconds()))


def main():
    if hasattr(time, "tzset"):
        time.tzset()
    while True:
        seconds = seconds_until_next_run()
        print("next daily sync in %d seconds" % seconds, flush=True)
        time.sleep(seconds)
        try:
            run_once()
        except Exception as error:
            print("daily sync failed: %s" % error, file=sys.stderr, flush=True)


if __name__ == "__main__":
    raise SystemExit(main())

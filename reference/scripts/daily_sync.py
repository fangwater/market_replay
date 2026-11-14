#!/usr/bin/env python3
"""Run one guarded OpenCTP synchronization; intended for PM2's daily schedule."""

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from cn_futures_ref import services, storage
from cn_futures_ref.models import DEFAULT_SESSION_SOURCE_URL, DEFAULT_SOURCE_URL


def required(name):
    value = os.environ.get(name)
    if not value:
        raise RuntimeError("%s is required" % name)
    return value


def run_once():
    database_url = required("CN_FUTURES_REF_DATABASE_URL")
    lock_connection = storage.connect(database_url)
    try:
        if not storage.try_daily_lock(lock_connection):
            print(json.dumps({"status": "skipped", "reason": "another daily sync holds the lock"}))
            return 0
        try:
            result = services.sync(
                database_url,
                os.environ.get("CN_FUTURES_REF_SOURCE_URL", DEFAULT_SOURCE_URL),
                timeout=int(os.environ.get("CN_FUTURES_REF_SYNC_TIMEOUT", "30")),
                retries=int(os.environ.get("CN_FUTURES_REF_SYNC_RETRIES", "2")),
            )
            result["tradingSessions"] = services.sync_continuous_sessions(
                database_url,
                os.environ.get(
                    "CN_FUTURES_REF_SESSION_SOURCE_URL", DEFAULT_SESSION_SOURCE_URL
                ),
                timeout=int(os.environ.get("CN_FUTURES_REF_SYNC_TIMEOUT", "30")),
                retries=int(os.environ.get("CN_FUTURES_REF_SYNC_RETRIES", "2")),
            )
            print(json.dumps(result, ensure_ascii=False))
            return 0
        finally:
            storage.release_daily_lock(lock_connection)
            lock_connection.commit()
    finally:
        lock_connection.close()


if __name__ == "__main__":
    try:
        raise SystemExit(run_once())
    except Exception as error:
        print("error: %s" % error, file=sys.stderr)
        raise SystemExit(1)

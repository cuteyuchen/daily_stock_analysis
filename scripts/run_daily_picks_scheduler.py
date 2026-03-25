# -*- coding: utf-8 -*-
"""每日热点推荐定时任务 runner。"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from src.scheduler import run_with_schedule
from src.services.daily_opportunity_service import DailyOpportunityService
from src.services.daily_picks_repository import DailyPicksRepository

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIR = Path.cwd() / "data" / "daily_picks"
OUTPUT_DIR = Path(os.getenv("DAILY_PICKS_OUTPUT_DIR", str(DEFAULT_OUTPUT_DIR)))
LOCK_FILE = Path(os.getenv("DAILY_PICKS_LOCK_FILE", str(OUTPUT_DIR / "daily_picks.lock")))
LOCK_TTL_SECONDS = int(os.getenv("DAILY_PICKS_LOCK_TTL_SECONDS", "7200"))
JOB_TIMEOUT_SECONDS = int(os.getenv("DAILY_PICKS_JOB_TIMEOUT_SECONDS", "900"))
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def _write_latest_snapshot(payload: Dict[str, Any]) -> None:
    filename = OUTPUT_DIR / f"daily_picks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    (OUTPUT_DIR / "latest.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    logger.info("daily picks snapshot written: %s", filename)


def _run_generation(queue: mp.Queue) -> None:
    try:
        service = DailyOpportunityService()
        queue.put({"ok": True, "payload": service.generate_and_save(top_k=5, source="scheduled")})
    except Exception as exc:  # noqa: BLE001
        queue.put({"ok": False, "error": str(exc)})


def _acquire_lock() -> bool:
    if LOCK_FILE.exists():
        age_seconds = max(0.0, time.time() - LOCK_FILE.stat().st_mtime)
        if age_seconds < LOCK_TTL_SECONDS:
            logger.warning("daily picks scheduler lock exists, skip duplicate run: %s", LOCK_FILE)
            return False
        logger.warning("detected stale daily picks lock, removing it: %s", LOCK_FILE)
        LOCK_FILE.unlink(missing_ok=True)

    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    LOCK_FILE.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "started_at": datetime.now().isoformat(),
                "timeout_seconds": JOB_TIMEOUT_SECONDS,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return True


def _release_lock() -> None:
    LOCK_FILE.unlink(missing_ok=True)


def _persist_failure(error_message: str, started_at: datetime) -> Dict[str, Any]:
    payload = DailyOpportunityService.build_failure_payload(
        error_message,
        source="scheduled",
        started_at=started_at,
        finished_at=datetime.now(),
    )
    repo = DailyPicksRepository()
    record_id = repo.save_run(payload, source="scheduled")
    payload["record_id"] = record_id
    _write_latest_snapshot(payload)
    return payload


def run_daily_picks_job() -> None:
    started_at = datetime.now()
    if not _acquire_lock():
        _persist_failure("scheduler lock active, skipped duplicate trigger", started_at)
        return

    queue: mp.Queue = mp.Queue()
    process = mp.Process(target=_run_generation, args=(queue,), daemon=True)
    process.start()

    try:
        process.join(timeout=JOB_TIMEOUT_SECONDS)
        if process.is_alive():
            process.terminate()
            process.join(timeout=10)
            payload = _persist_failure(
                f"daily picks scheduler timed out after {JOB_TIMEOUT_SECONDS}s",
                started_at,
            )
            logger.error("daily picks scheduler timed out: record_id=%s", payload.get("record_id"))
            return

        result = queue.get_nowait() if not queue.empty() else {"ok": False, "error": "worker exited without payload"}
        if not result.get("ok"):
            payload = _persist_failure(str(result.get("error") or "worker failed"), started_at)
            logger.error("daily picks scheduler failed: record_id=%s", payload.get("record_id"))
            return

        payload = result["payload"]
        _write_latest_snapshot(payload)
        logger.info("daily picks generated successfully: record_id=%s", payload.get("record_id"))
    finally:
        _release_lock()


def main() -> None:
    schedule_time = os.getenv("DAILY_PICKS_SCHEDULE_TIME", "15:10")
    run_immediately = os.getenv("DAILY_PICKS_RUN_IMMEDIATELY", "false").lower() == "true"
    logger.info(
        "Starting daily picks scheduler, time=%s, run_immediately=%s, timeout=%ss",
        schedule_time,
        run_immediately,
        JOB_TIMEOUT_SECONDS,
    )
    run_with_schedule(run_daily_picks_job, schedule_time=schedule_time, run_immediately=run_immediately)


if __name__ == "__main__":
    main()

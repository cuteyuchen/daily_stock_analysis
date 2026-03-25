# -*- coding: utf-8 -*-
"""每日热点推荐定时任务 runner。"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from pathlib import Path

from src.scheduler import run_with_schedule
from src.services.daily_opportunity_service import DailyOpportunityService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
)
logger = logging.getLogger(__name__)

OUTPUT_DIR = Path('/opt/daily_stock_analysis/reports/daily_picks')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def run_daily_picks_job() -> None:
    service = DailyOpportunityService()
    result = service.generate_and_save(top_k=5, source='scheduled')
    filename = OUTPUT_DIR / f"daily_picks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    filename.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    logger.info("daily picks generated: %s", filename)


def main() -> None:
    schedule_time = os.getenv('DAILY_PICKS_SCHEDULE_TIME', '15:10')
    run_immediately = os.getenv('DAILY_PICKS_RUN_IMMEDIATELY', 'false').lower() == 'true'
    logger.info('Starting daily picks scheduler, time=%s, run_immediately=%s', schedule_time, run_immediately)
    run_with_schedule(run_daily_picks_job, schedule_time=schedule_time, run_immediately=run_immediately)


if __name__ == '__main__':
    main()

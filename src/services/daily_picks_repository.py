# -*- coding: utf-8 -*-
"""每日推荐结果存取。"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import desc, select, func

from src.storage import DailyPickRun, DatabaseManager

logger = logging.getLogger(__name__)


class DailyPicksRepository:
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def save_run(self, payload: Dict[str, Any], source: str = "manual") -> Optional[int]:
        recommendations = payload.get("recommendations") or []
        record = DailyPickRun(
            source=source,
            strategy_version=str(payload.get("strategy_version") or "mvp_v1"),
            generated_at=datetime.now(),
            pick_count=len(recommendations),
            market_news_json=json.dumps(payload.get("market_news") or [], ensure_ascii=False),
            sector_rankings_json=json.dumps(payload.get("sector_rankings") or {}, ensure_ascii=False),
            recommendations_json=json.dumps(recommendations, ensure_ascii=False),
            payload_json=json.dumps(payload, ensure_ascii=False),
        )
        with self.db.get_session() as session:
            try:
                session.add(record)
                session.commit()
                session.refresh(record)
                return record.id
            except Exception as exc:
                session.rollback()
                logger.error("保存 daily picks 失败: %s", exc, exc_info=True)
                return None

    def list_runs(self, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
        offset = max(page - 1, 0) * limit
        with self.db.get_session() as session:
            total = session.execute(select(func.count()).select_from(DailyPickRun)).scalar() or 0
            rows = session.execute(
                select(DailyPickRun)
                .order_by(desc(DailyPickRun.generated_at))
                .offset(offset)
                .limit(limit)
            ).scalars().all()
        return [self._to_summary(item) for item in rows], int(total)

    def get_run(self, record_id: int) -> Optional[Dict[str, Any]]:
        with self.db.get_session() as session:
            row = session.get(DailyPickRun, record_id)
            if row is None:
                return None
            return self._to_detail(row)

    @staticmethod
    def _to_summary(row: DailyPickRun) -> Dict[str, Any]:
        recommendations = json.loads(row.recommendations_json or "[]")
        top_names = [item.get("stock_name") or item.get("stock_code") for item in recommendations[:3]]
        return {
            "id": row.id,
            "source": row.source,
            "strategy_version": row.strategy_version,
            "generated_at": row.generated_at.isoformat() if row.generated_at else None,
            "pick_count": row.pick_count,
            "top_names": top_names,
        }

    @staticmethod
    def _to_detail(row: DailyPickRun) -> Dict[str, Any]:
        return {
            "id": row.id,
            "source": row.source,
            "strategy_version": row.strategy_version,
            "generated_at": row.generated_at.isoformat() if row.generated_at else None,
            "pick_count": row.pick_count,
            "market_news": json.loads(row.market_news_json or "[]"),
            "sector_rankings": json.loads(row.sector_rankings_json or "{}"),
            "recommendations": json.loads(row.recommendations_json or "[]"),
            "payload": json.loads(row.payload_json or "{}"),
        }

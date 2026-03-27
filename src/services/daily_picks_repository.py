# -*- coding: utf-8 -*-
"""每日推荐结果存取。"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import desc, func, select

from src.storage import DailyPickRun, DatabaseManager

logger = logging.getLogger(__name__)


class DailyPicksRepository:
    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager.get_instance()

    def save_run(self, payload: Dict[str, Any], source: str = "manual") -> Optional[int]:
        recommendations = payload.get("recommendations") or []
        record = DailyPickRun(
            source=source,
            strategy_version=str(payload.get("strategy_version") or "daily_picks_v3"),
            generated_at=datetime.now(),
            pick_count=int(payload.get("output_count") or len(recommendations)),
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
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                logger.error("保存 daily picks 失败: %s", exc, exc_info=True)
                return None

    def list_runs(self, page: int = 1, limit: int = 20) -> Tuple[List[Dict[str, Any]], int]:
        offset = max(page - 1, 0) * limit
        with self.db.get_session() as session:
            total = session.execute(select(func.count()).select_from(DailyPickRun)).scalar() or 0
            rows = (
                session.execute(
                    select(DailyPickRun)
                    .order_by(desc(DailyPickRun.generated_at))
                    .offset(offset)
                    .limit(limit)
                )
                .scalars()
                .all()
            )
        return [self._to_summary(item) for item in rows], int(total)

    def get_run(self, record_id: int) -> Optional[Dict[str, Any]]:
        with self.db.get_session() as session:
            row = session.get(DailyPickRun, record_id)
            if row is None:
                return None
            return self._to_detail(row)

    def delete_run(self, record_id: int) -> bool:
        """删除指定 id 的推荐记录，成功返回 True。"""
        with self.db.get_session() as session:
            row = session.get(DailyPickRun, record_id)
            if row is None:
                return False
            try:
                session.delete(row)
                session.commit()
                return True
            except Exception as exc:  # noqa: BLE001
                session.rollback()
                logger.error("删除 daily picks 记录失败: %s", exc, exc_info=True)
                return False

    @staticmethod
    def _safe_payload(row: DailyPickRun) -> Dict[str, Any]:
        try:
            return json.loads(row.payload_json or "{}")
        except Exception:  # noqa: BLE001
            return {}

    def _to_summary(self, row: DailyPickRun) -> Dict[str, Any]:
        recommendations = json.loads(row.recommendations_json or "[]")
        payload = self._safe_payload(row)
        top_names = [item.get("stock_name") or item.get("stock_code") for item in recommendations[:3]]
        return {
            "id": row.id,
            "source": row.source,
            "strategy_version": row.strategy_version,
            "generated_at": row.generated_at.isoformat() if row.generated_at else None,
            "pick_count": row.pick_count,
            "output_count": payload.get("output_count", row.pick_count),
            "candidate_count": payload.get("candidate_count"),
            "run_status": payload.get("run_status", "success"),
            "degraded": bool(payload.get("degraded", False)),
            "confidence": payload.get("confidence"),
            "generation_layer": payload.get("generation_layer"),
            "error_summary": payload.get("error_summary") or [],
            "top_names": top_names,
        }

    def _to_detail(self, row: DailyPickRun) -> Dict[str, Any]:
        payload = self._safe_payload(row)
        return {
            "id": row.id,
            "source": row.source,
            "strategy_version": row.strategy_version,
            "generated_at": row.generated_at.isoformat() if row.generated_at else None,
            "pick_count": row.pick_count,
            "run_status": payload.get("run_status", "success"),
            "degraded": bool(payload.get("degraded", False)),
            "started_at": payload.get("started_at"),
            "finished_at": payload.get("finished_at"),
            "duration_ms": payload.get("duration_ms"),
            "candidate_count": payload.get("candidate_count"),
            "output_count": payload.get("output_count", row.pick_count),
            "confidence": payload.get("confidence"),
            "risk_note": payload.get("risk_note"),
            "generation_layer": payload.get("generation_layer"),
            "generation_note": payload.get("generation_note"),
            "error_summary": payload.get("error_summary") or [],
            "source_summary": payload.get("source_summary") or {},
            "used_sources": payload.get("used_sources") or [],
            "failed_sources": payload.get("failed_sources") or [],
            "market_news": json.loads(row.market_news_json or "[]"),
            "sector_rankings": json.loads(row.sector_rankings_json or "{}"),
            "recommendations": json.loads(row.recommendations_json or "[]"),
            "payload": payload,
        }

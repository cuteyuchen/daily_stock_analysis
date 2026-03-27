# -*- coding: utf-8 -*-
"""每日热点推荐落库接口。"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from src.services.daily_opportunity_service import DailyOpportunityService
from src.services.daily_picks_repository import DailyPicksRepository

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post('/generate')
def generate_daily_picks(
    top_k: int = Query(5, ge=1, le=10),
):
    try:
        service = DailyOpportunityService()
        return service.generate_and_save(top_k=top_k, source='manual')
    except Exception as exc:
        logger.error('generate_daily_picks failed: %s', exc, exc_info=True)
        raise HTTPException(status_code=500, detail='generate_daily_picks failed')


@router.get('')
def list_daily_picks(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
):
    repo = DailyPicksRepository()
    items, total = repo.list_runs(page=page, limit=limit)
    return {
        'page': page,
        'limit': limit,
        'total': total,
        'items': items,
    }


@router.get('/{record_id}')
def get_daily_pick_detail(record_id: int):
    repo = DailyPicksRepository()
    item = repo.get_run(record_id)
    if item is None:
        raise HTTPException(status_code=404, detail='record not found')
    return item


@router.delete('/{record_id}')
def delete_daily_pick(record_id: int):
    repo = DailyPicksRepository()
    if not repo.delete_run(record_id):
        raise HTTPException(status_code=404, detail='record not found')
    return {'ok': True, 'deleted_id': record_id}

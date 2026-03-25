# -*- coding: utf-8 -*-
"""每日机会推荐接口（MVP）"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from api.v1.schemas.common import ErrorResponse, SuccessResponse
from src.services.daily_opportunity_service import DailyOpportunityService

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get(
    "/daily-picks",
    response_model=SuccessResponse,
    responses={
        200: {"description": "Top5 推荐结果"},
        500: {"description": "服务器错误", "model": ErrorResponse},
    },
    summary="获取每日热点推荐股票（MVP）",
    description="聚合市场新闻、热门板块，并输出 5 只 A 股热点方向候选与操作建议。",
)
def get_daily_picks(
    top_k: int = Query(5, ge=1, le=10, description="返回推荐数量，默认 5"),
) -> SuccessResponse:
    try:
        service = DailyOpportunityService()
        data = service.generate_recommendations(top_k=top_k)
        return SuccessResponse(success=True, message="ok", data=data)
    except Exception as exc:
        logger.error("生成每日机会推荐失败: %s", exc, exc_info=True)
        raise HTTPException(
            status_code=500,
            detail={
                "error": "daily_picks_failed",
                "message": "生成每日机会推荐失败",
            },
        )

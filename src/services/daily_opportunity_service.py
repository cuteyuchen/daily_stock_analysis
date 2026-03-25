# -*- coding: utf-8 -*-
"""
每日机会推荐服务（MVP）

目标：
1. 聚合市场新闻、热门板块、热门股票候选
2. 根据简单规则生成 Top5 A 股推荐
3. 为后续定时任务和页面展示提供统一服务入口
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import pandas as pd
from data_provider import DataFetcherManager
from src.search_service import SearchService
from src.services.stock_service import StockService
from src.services.daily_picks_repository import DailyPicksRepository

logger = logging.getLogger(__name__)


class DailyOpportunityService:
    """热点机会推荐服务（第一版 MVP）。"""

    DEFAULT_THEMES = [
        "A股 市场热点",
        "A股 财经新闻",
    ]

    def __init__(self):
        self.manager = DataFetcherManager()
        self.stock_service = StockService()
        self.search_service = SearchService()
        self.repo = DailyPicksRepository()

    def get_market_news(self, max_per_query: int = 2) -> List[Dict[str, Any]]:
        """抓取市场新闻摘要。"""
        items: List[Dict[str, Any]] = []
        seen = set()

        for query in self.DEFAULT_THEMES:
            try:
                response = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name="A股市场",
                    max_results=max_per_query,
                    focus_keywords=query.split(),
                )
            except Exception as exc:
                logger.warning("市场新闻搜索失败 %s: %s", query, exc)
                continue

            if not response or not getattr(response, "results", None):
                continue

            for result in response.results:
                url = getattr(result, "url", None) or ""
                if url in seen:
                    continue
                seen.add(url)
                items.append(
                    {
                        "query": query,
                        "title": getattr(result, "title", ""),
                        "snippet": getattr(result, "snippet", ""),
                        "url": url,
                        "source": getattr(result, "source", ""),
                        "published_date": getattr(result, "published_date", None),
                    }
                )

        return items[:12]

    def get_sector_rankings(self, top_n: int = 10) -> Dict[str, List[Dict[str, Any]]]:
        """获取热门/冷门板块。"""
        try:
            top, bottom = self.manager.get_sector_rankings(n=top_n)
            return {
                "top": top or [],
                "bottom": bottom or [],
            }
        except Exception as exc:
            logger.warning("获取板块排行失败: %s", exc)
            return {"top": [], "bottom": []}

    def _get_stock_list(self, limit: int = 20) -> List[Dict[str, Any]]:
        """从底层 fetcher 拉取股票列表。"""
        for fetcher in getattr(self.manager, "_fetchers", []):
            if not hasattr(fetcher, "get_stock_list"):
                continue
            try:
                df = fetcher.get_stock_list()
                if df is None or getattr(df, "empty", True):
                    continue
                if not isinstance(df, pd.DataFrame):
                    continue
                rows: List[Dict[str, Any]] = []
                code_col = None
                name_col = None
                for col in df.columns:
                    low = str(col).lower()
                    if code_col is None and low in {"code", "ts_code", "symbol"}:
                        code_col = col
                    if name_col is None and low in {"name", "stock_name", "简称", "证券简称"}:
                        name_col = col
                if code_col is None:
                    continue
                for _, row in df.head(limit).iterrows():
                    code = str(row.get(code_col) or "").strip()
                    if not code:
                        continue
                    rows.append({
                        "stock_code": code[-6:],
                        "stock_name": str(row.get(name_col) or "").strip() if name_col else "",
                    })
                if rows:
                    return rows
            except Exception as exc:
                logger.warning("获取股票列表失败 %s: %s", getattr(fetcher, 'name', 'unknown'), exc)
        return []

    def _derive_stock_candidates_from_sectors(self, top_sectors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        从强势板块反推候选股：
        - 获取一批股票列表
        - 查询每只股票所属板块
        - 若命中强势板块，则加入候选
        - 再用实时行情做基础排序
        """
        strong_sectors = []
        sector_map: Dict[str, Dict[str, Any]] = {}
        for sector in top_sectors[:8]:
            sector_name = str(sector.get("name") or "").strip()
            if not sector_name:
                continue
            strong_sectors.append(sector_name)
            sector_map[sector_name] = sector

        stocks = self._get_stock_list(limit=20)
        candidates: List[Dict[str, Any]] = []

        for item in stocks:
            if len(candidates) >= 8:
                break
            stock_code = item.get("stock_code")
            if not stock_code:
                continue
            try:
                boards = self.manager.get_belong_boards(stock_code)
            except Exception:
                boards = []
            if not boards:
                continue

            matched_sector = None
            for board in boards:
                board_name = str(board.get("name") or board.get("board_name") or "").strip()
                if not board_name:
                    continue
                for sector_name in strong_sectors:
                    if sector_name in board_name or board_name in sector_name:
                        matched_sector = sector_name
                        break
                if matched_sector:
                    break
            if not matched_sector:
                continue

            quote = self.stock_service.get_realtime_quote(stock_code)
            if not quote:
                continue

            change_pct = quote.get("change_percent") or 0
            amount = quote.get("amount") or 0
            score = 50
            try:
                score += min(max(float(change_pct), -5), 10) * 2
            except Exception:
                pass
            try:
                score += min(float(amount) / 1e8, 20)
            except Exception:
                pass
            sector_info = sector_map.get(matched_sector, {})
            sector_change_pct = sector_info.get("change_pct")
            try:
                if sector_change_pct is not None:
                    score += min(max(float(sector_change_pct), -2), 8) * 2
            except Exception:
                pass

            candidates.append(
                {
                    "stock_code": stock_code,
                    "stock_name": quote.get("stock_name") or item.get("stock_name") or stock_code,
                    "sector_name": matched_sector,
                    "sector_change_pct": sector_change_pct,
                    "quote": quote,
                    "score": round(score, 2),
                    "boards": boards,
                }
            )

        candidates.sort(key=lambda x: x.get("score", 0), reverse=True)
        if not candidates:
            for sector in top_sectors[:5]:
                sector_name = str(sector.get("name") or "").strip()
                if not sector_name:
                    continue
                candidates.append(
                    {
                        "stock_code": None,
                        "stock_name": f"{sector_name}热点候选",
                        "sector_name": sector_name,
                        "sector_change_pct": sector.get("change_pct"),
                        "quote": {},
                        "score": 60,
                        "boards": [],
                    }
                )

        dedup: List[Dict[str, Any]] = []
        seen = set()
        for item in candidates:
            code = item.get("stock_code")
            if code in seen:
                continue
            seen.add(code)
            dedup.append(item)
        return dedup[:20]

    def generate_recommendations(self, top_k: int = 5) -> Dict[str, Any]:
        """生成每日 Top5 推荐（MVP 版）。"""
        generated_at = datetime.now().isoformat()
        market_news = self.get_market_news()
        sector_rankings = self.get_sector_rankings(top_n=10)
        sector_candidates = self._derive_stock_candidates_from_sectors(sector_rankings.get("top", []))

        recommendations: List[Dict[str, Any]] = []
        for idx, candidate in enumerate(sector_candidates[:top_k], start=1):
            sector_name = candidate.get("sector_name") or "未知板块"
            sector_change_pct = candidate.get("sector_change_pct")
            stock_code = candidate.get("stock_code")
            stock_name = candidate.get("stock_name")
            quote = candidate.get("quote") or {}
            change_pct = quote.get("change_percent")
            amount = quote.get("amount")
            matched_news = [n for n in market_news if sector_name[:4] in (n.get("title") or "") or sector_name[:4] in (n.get("snippet") or "")]
            reason_parts = [f"所属板块“{sector_name}”位于当日强势板块前列"]
            if sector_change_pct is not None:
                reason_parts.append(f"板块涨幅约 {sector_change_pct}%")
            if change_pct is not None:
                reason_parts.append(f"个股当日涨跌幅约 {change_pct}%")
            if matched_news:
                reason_parts.append("相关新闻热度较高")
            if amount:
                try:
                    reason_parts.append(f"成交额约 {round(float(amount)/1e8, 2)} 亿")
                except Exception:
                    pass

            recommendations.append(
                {
                    "rank": idx,
                    "stock_code": stock_code,
                    "stock_name": stock_name,
                    "sector_name": sector_name,
                    "sector_change_pct": sector_change_pct,
                    "score": candidate.get("score", max(60, 100 - idx * 6)),
                    "recommend_reason": "；".join(reason_parts),
                    "operation_advice": "建议优先观察强势板块内的放量龙头，回踩不破关键均线可分批关注；若高开过多或冲高回落明显，优先观望。",
                    "risk_warning": "当前推荐为规则筛选结果，未结合盘口、公告、停牌、监管问询等事件风险，请务必自行复核。",
                    "related_news": matched_news[:3],
                    "quote": quote,
                }
            )

        return {
            "generated_at": generated_at,
            "strategy_version": "mvp_v1",
            "generation_note": "当前为 MVP 版本；若外部新闻/板块数据源不稳定，结果可能退化为板块热点候选。",
            "market_news": market_news,
            "sector_rankings": sector_rankings,
            "candidate_count": len(sector_candidates),
            "recommendations": recommendations,
        }

    def generate_and_save(self, top_k: int = 5, source: str = "manual") -> Dict[str, Any]:
        payload = self.generate_recommendations(top_k=top_k)
        record_id = self.repo.save_run(payload, source=source)
        payload["record_id"] = record_id
        return payload

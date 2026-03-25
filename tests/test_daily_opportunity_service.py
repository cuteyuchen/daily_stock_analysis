# -*- coding: utf-8 -*-
"""Tests for the stabilized daily picks generation service."""

from __future__ import annotations

import unittest
import sys
import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pandas as pd

if "newspaper" not in sys.modules:
    mock_np = MagicMock()
    mock_np.Article = MagicMock()
    mock_np.Config = MagicMock()
    sys.modules["newspaper"] = mock_np

from src.search_service import SearchResult, SearchResponse
from src.services.daily_opportunity_service import DailyOpportunityService


class _SectorFetcher:
    name = "AkshareFetcher"
    priority = 0

    def __init__(self, top=None, bottom=None):
        self._top = top if top is not None else [{"name": "人工智能", "change_pct": 4.5}]
        self._bottom = bottom if bottom is not None else []

    def get_sector_rankings(self, n: int = 5):
        return self._top[:n], self._bottom[:n]


class _StockListFetcher:
    name = "BaostockFetcher"
    priority = 1

    def get_stock_list(self):
        return pd.DataFrame(
            [
                {"code": "600001", "name": "算力一号"},
                {"code": "600002", "name": "算力二号"},
                {"code": "600003", "name": "半导体一号"},
            ]
        )


class _QuoteFetcher:
    name = "EfinanceFetcher"
    priority = 0

    def __init__(self, quotes):
        self._quotes = quotes

    def get_realtime_quote(self, stock_code: str):
        return self._quotes.get(stock_code)


class _CountingEmptyQuoteFetcher:
    def __init__(self, name: str = "EfinanceFetcher", priority: int = 0):
        self.name = name
        self.priority = priority
        self.calls = 0

    def get_realtime_quote(self, stock_code: str):
        self.calls += 1
        return None


class _CountingQuoteFetcher:
    def __init__(self, quotes, name: str = "JoinQuantFetcher", priority: int = 1):
        self.name = name
        self.priority = priority
        self._quotes = quotes
        self.calls = 0

    def get_realtime_quote(self, stock_code: str):
        self.calls += 1
        return self._quotes.get(stock_code)


class _BoardFetcher:
    name = "JoinQuantFetcher"
    priority = 0

    def __init__(self, mapping):
        self._mapping = mapping

    def get_belong_board(self, stock_code: str):
        boards = self._mapping.get(stock_code, [])
        if not boards:
            return pd.DataFrame()
        return pd.DataFrame([{"板块名称": item} for item in boards])


class _FakeManager:
    def __init__(self, fetchers):
        self._fetchers = fetchers

    @staticmethod
    def _normalize_belong_boards(raw):
        if raw is None or getattr(raw, "empty", False):
            return []
        return [{"name": str(row.get("板块名称") or "")} for _, row in raw.iterrows()]


class DailyOpportunityServiceTestCase(unittest.TestCase):
    def _build_service_with_fetchers(self, fetchers):
        manager = _FakeManager(fetchers)
        repo = MagicMock()
        fake_search = MagicMock()
        fake_search.search_stock_news.return_value = SearchResponse(
            query="A股 市场热点",
            provider="SerpAPI",
            success=True,
            results=[],
        )

        with patch("src.services.daily_opportunity_service.DataFetcherManager", return_value=manager), \
             patch("src.services.daily_opportunity_service.get_config", return_value=SimpleNamespace(stock_list=[])), \
             patch("src.services.daily_opportunity_service.get_search_service", return_value=fake_search), \
             patch("src.services.daily_opportunity_service.DailyPicksRepository", return_value=repo):
            return DailyOpportunityService()

    def _build_service(self, *, include_news: bool, include_sector: bool):
        quotes = {
            "600001": {
                "stock_name": "算力一号",
                "current_price": 12.3,
                "change_percent": 5.6,
                "amount": 8.5e8,
                "turnover_rate": 10.2,
                "volume_ratio": 1.8,
            },
            "600002": {
                "stock_name": "算力二号",
                "current_price": 9.8,
                "change_percent": 4.1,
                "amount": 6.1e8,
                "turnover_rate": 8.8,
                "volume_ratio": 1.6,
            },
            "600003": {
                "stock_name": "半导体一号",
                "current_price": 18.6,
                "change_percent": 3.5,
                "amount": 5.6e8,
                "turnover_rate": 6.8,
                "volume_ratio": 1.4,
            },
        }
        boards = {
            "600001": ["人工智能"],
            "600002": ["人工智能"],
            "600003": ["半导体"],
        }
        fetchers = [
            _QuoteFetcher(quotes),
            _BoardFetcher(boards),
            _StockListFetcher(),
        ]
        if include_sector:
            fetchers.insert(0, _SectorFetcher())

        manager = _FakeManager(fetchers)
        repo = MagicMock()

        if include_news:
            response = SearchResponse(
                query="A股 市场热点",
                provider="BaiduSearch",
                success=True,
                results=[
                    SearchResult(
                        title="人工智能方向热度升温",
                        snippet="算力和大模型相关方向继续活跃",
                        url="https://example.com/news/1",
                        source="example.com",
                        published_date="2026-03-25",
                    )
                ],
            )
        else:
            response = SearchResponse(query="A股 市场热点", provider="BaiduSearch", success=False, results=[], error_message="news failed")

        fake_search = MagicMock()
        fake_search.search_stock_news.return_value = response

        with patch("src.services.daily_opportunity_service.DataFetcherManager", return_value=manager), \
             patch("src.services.daily_opportunity_service.get_config", return_value=SimpleNamespace(stock_list=[])), \
             patch("src.services.daily_opportunity_service.get_search_service", return_value=fake_search), \
             patch("src.services.daily_opportunity_service.DailyPicksRepository", return_value=repo):
            return DailyOpportunityService()

    def test_generates_real_stock_recommendations_with_news_and_sector(self):
        service = self._build_service(include_news=True, include_sector=True)

        payload = service.generate_recommendations(top_k=2)

        self.assertEqual(payload["run_status"], "success")
        self.assertFalse(payload["degraded"])
        self.assertEqual(payload["output_count"], 2)
        self.assertTrue(all(item.get("stock_code") for item in payload["recommendations"]))
        self.assertTrue(all("热点候选" not in item["stock_name"] for item in payload["recommendations"]))
        self.assertEqual(payload["recommendations"][0]["sector_name"], "人工智能")

    def test_falls_back_to_real_stock_pool_when_news_and_sector_fail(self):
        service = self._build_service(include_news=False, include_sector=False)

        payload = service.generate_recommendations(top_k=2)

        self.assertEqual(payload["run_status"], "degraded")
        self.assertTrue(payload["degraded"])
        self.assertEqual(payload["output_count"], 2)
        self.assertTrue(all(item.get("stock_code") for item in payload["recommendations"]))
        self.assertTrue(all("热点候选" not in item["stock_name"] for item in payload["recommendations"]))
        self.assertIn("板块排行不可用，已回退到全市场简化评分。", payload["error_summary"])

    def test_warmup_disables_empty_quote_provider_for_remaining_scan(self):
        empty_fetcher = _CountingEmptyQuoteFetcher()
        working_fetcher = _CountingQuoteFetcher(
            {
                "600001": {"stock_name": "一号", "current_price": 10.0},
                "600002": {"stock_name": "二号", "current_price": 11.0},
            }
        )
        service = self._build_service_with_fetchers([empty_fetcher, working_fetcher])
        ctx = service._new_run_context()

        service._warm_quote_cache(["600001", "600002"], ctx)
        quote = service._get_quote("600002", "二号", ctx, {})

        self.assertIsNotNone(quote)
        self.assertEqual(empty_fetcher.calls, 1)
        self.assertEqual(working_fetcher.calls, 2)

    def test_quote_provider_is_circuit_broken_after_repeated_empty_results(self):
        service = self._build_service_with_fetchers([])
        ctx = service._new_run_context()

        service._record_runtime_provider_result(ctx, "quote", "PytdxFetcher", "empty")
        service._record_runtime_provider_result(ctx, "quote", "PytdxFetcher", "empty")
        service._record_runtime_provider_result(ctx, "quote", "PytdxFetcher", "empty")

        self.assertIn("PytdxFetcher", ctx["runtime_hints"]["disabled_quote_fetchers"])

    def test_quote_provider_with_success_is_not_disabled_by_later_empty_results(self):
        service = self._build_service_with_fetchers([])
        ctx = service._new_run_context()

        service._record_runtime_provider_result(ctx, "quote", "TushareFetcher", "ok")
        service._record_runtime_provider_result(ctx, "quote", "TushareFetcher", "empty")
        service._record_runtime_provider_result(ctx, "quote", "TushareFetcher", "empty")
        service._record_runtime_provider_result(ctx, "quote", "TushareFetcher", "empty")

        self.assertNotIn("TushareFetcher", ctx["runtime_hints"]["disabled_quote_fetchers"])

    def test_ai_reasoning_enriches_recommendation_with_news_connection_and_signal_breakdown(self):
        service = self._build_service(include_news=True, include_sector=True)
        ai_payload = {
            "market_sentiment": "人工智能方向情绪偏强",
            "picks": [
                {
                    "stock_code": "600001",
                    "recommend_reason": "AI 判断：人工智能热点与量价强度共振，成交额与换手率说明资金承接较好。",
                    "operation_advice": "优先观察回踩承接，分批跟踪，不宜情绪顶点追高。",
                    "risk_warning": "若热点降温或量能衰减，需要及时收缩仓位。",
                    "news_connection": "新闻中提到算力和大模型持续活跃，算力一号与人工智能板块直接相关。",
                    "signal_breakdown": {
                        "technical": "涨幅 5.6%，量比 1.8，短线技术面偏强。",
                        "sentiment": "人工智能主题新闻热度最高，情绪面占优。",
                        "capital": "成交额 8.5 亿、换手率 10.2%，资金活跃度较高。",
                        "sector": "人工智能板块涨幅 4.5%，板块强度形成共振。",
                    },
                    "related_news": [
                        {
                            "title": "人工智能方向热度升温",
                            "relation_reason": "新闻直接提到了算力与大模型催化。",
                        }
                    ],
                }
            ],
        }

        with patch("src.services.daily_opportunity_service.GeminiAnalyzer", create=True) as analyzer_cls:
            analyzer = analyzer_cls.return_value
            analyzer.generate_text.return_value = json.dumps(ai_payload, ensure_ascii=False)
            payload = service.generate_recommendations(top_k=1)

        recommendation = payload["recommendations"][0]
        self.assertEqual(
            recommendation.get("news_connection"),
            "新闻中提到算力和大模型持续活跃，算力一号与人工智能板块直接相关。",
        )
        self.assertEqual(
            recommendation.get("signal_breakdown", {}).get("capital"),
            "成交额 8.5 亿、换手率 10.2%，资金活跃度较高。",
        )
        self.assertEqual(
            recommendation.get("related_news", [{}])[0].get("relation_reason"),
            "新闻直接提到了算力与大模型催化。",
        )


if __name__ == "__main__":
    unittest.main()

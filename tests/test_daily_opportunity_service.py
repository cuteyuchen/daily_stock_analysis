# -*- coding: utf-8 -*-
"""Tests for the stabilized daily picks generation service."""

from __future__ import annotations

import unittest
import sys
import json
from datetime import datetime, timedelta
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
        self.assertNotIn("temperature", analyzer.generate_text.call_args.kwargs)

    def test_stock_pool_ignores_configured_watchlist_and_uses_network_source_only(self):
        fetchers = [_StockListFetcher()]
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
             patch("src.services.daily_opportunity_service.get_config", return_value=SimpleNamespace(stock_list=["600519", "300750"])), \
             patch("src.services.daily_opportunity_service.get_search_service", return_value=fake_search), \
             patch("src.services.daily_opportunity_service.DailyPicksRepository", return_value=repo):
            service = DailyOpportunityService()

        ctx = service._new_run_context()
        pool = service._get_stock_pool(limit=10, ctx=ctx)

        codes = [item["stock_code"] for item in pool]
        self.assertNotIn("600519", codes)
        self.assertNotIn("300750", codes)
        self.assertEqual(codes[:3], ["600001", "600002", "600003"])

    def test_market_news_searches_recent_one_week_window(self):
        service = self._build_service(include_news=True, include_sector=True)
        ctx = service._new_run_context()

        service.get_market_news(max_per_query=3, ctx=ctx)

        for call in service.search_service.search_stock_news.call_args_list:
            self.assertEqual(call.kwargs.get("days_override"), 7)

    def test_market_news_filters_out_stale_results_even_if_search_returns_them(self):
        service = self._build_service(include_news=True, include_sector=True)
        today = datetime.now().date()
        recent_day = (today - timedelta(days=2)).isoformat()
        stale_day = (today - timedelta(days=12)).isoformat()
        service.search_service.search_stock_news.return_value = SearchResponse(
            query="A股 热点事件",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title="机器人产业链催化继续发酵",
                    snippet="人形机器人方向热度继续升温。",
                    url="https://example.com/fresh",
                    source="example.com",
                    published_date=recent_day,
                ),
                SearchResult(
                    title="去年热门题材复盘",
                    snippet="这是 12 天前的旧内容，不应进入 daily picks。",
                    url="https://example.com/stale",
                    source="example.com",
                    published_date=stale_day,
                ),
            ],
        )
        ctx = service._new_run_context()

        items = service.get_market_news(max_per_query=5, ctx=ctx)

        self.assertEqual([item["url"] for item in items], ["https://example.com/fresh"])

    def test_market_news_filters_out_last_year_roundup_articles_even_if_date_is_recent(self):
        service = self._build_service(include_news=True, include_sector=True)
        today = datetime.now().date().isoformat()
        last_year = datetime.now().year - 1
        service.search_service.search_stock_news.return_value = SearchResponse(
            query="A股 热点事件",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title=f"{last_year}年A股12大热门概念盘点，这些牛股涨超100%",
                    snippet="年度盘点内容，不适合作为今天的热点驱动。",
                    url="https://example.com/roundup",
                    source="example.com",
                    published_date=today,
                ),
                SearchResult(
                    title="机器人产业链催化继续发酵",
                    snippet="人形机器人方向热度继续升温。",
                    url="https://example.com/fresh",
                    source="example.com",
                    published_date=today,
                ),
            ],
        )
        ctx = service._new_run_context()

        items = service.get_market_news(max_per_query=5, ctx=ctx)

        self.assertEqual([item["url"] for item in items], ["https://example.com/fresh"])

    def test_market_news_filters_out_prior_year_articles_even_without_roundup_keyword(self):
        service = self._build_service(include_news=True, include_sector=True)
        today = datetime.now().date().isoformat()
        last_year = datetime.now().year - 1
        service.search_service.search_stock_news.return_value = SearchResponse(
            query="A股 产业催化",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title=f"{last_year}年11月26日 A股产业动态与个股核心逻辑梳理",
                    snippet="旧年份内容，即使被搜索引擎重新抓取也不应进入结果。",
                    url="https://example.com/old-year",
                    source="example.com",
                    published_date=today,
                ),
                SearchResult(
                    title="机器人产业链催化继续发酵",
                    snippet="人形机器人方向热度继续升温。",
                    url="https://example.com/fresh",
                    source="example.com",
                    published_date=today,
                ),
            ],
        )
        ctx = service._new_run_context()

        items = service.get_market_news(max_per_query=5, ctx=ctx)

        self.assertEqual([item["url"] for item in items], ["https://example.com/fresh"])

    def test_market_news_filters_out_generic_info_portal_titles(self):
        service = self._build_service(include_news=True, include_sector=True)
        today = datetime.now().date().isoformat()
        service.search_service.search_stock_news.return_value = SearchResponse(
            query="A股 热门板块",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title="A股 热门板块的最新相关信息",
                    snippet="",
                    url="https://example.com/info-portal",
                    source="example.com",
                    published_date=today,
                ),
                SearchResult(
                    title="算力板块走强，液冷与服务器链条同步活跃",
                    snippet="液冷服务器与算力租赁方向共振。",
                    url="https://example.com/fresh",
                    source="example.com",
                    published_date=today,
                ),
            ],
        )
        ctx = service._new_run_context()

        items = service.get_market_news(max_per_query=5, ctx=ctx)

        self.assertEqual([item["url"] for item in items], ["https://example.com/fresh"])

    def test_market_news_filters_out_encyclopedia_and_tool_pages(self):
        service = self._build_service(include_news=True, include_sector=True)
        today = datetime.now().date().isoformat()
        service.search_service.search_stock_news.return_value = SearchResponse(
            query="A股 盘中异动",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title="股票异动(连续3个交易日股价振...) - 百度百科",
                    snippet="解释性百科内容，不是热点新闻。",
                    url="https://baike.baidu.com/item/test",
                    source="baike.baidu.com",
                    published_date=today,
                ),
                SearchResult(
                    title="算力板块午后再度拉升，多股放量走强",
                    snippet="算力租赁与液冷方向继续活跃。",
                    url="https://example.com/fresh",
                    source="example.com",
                    published_date=today,
                ),
            ],
        )
        ctx = service._new_run_context()

        items = service.get_market_news(max_per_query=5, ctx=ctx)

        self.assertEqual([item["url"] for item in items], ["https://example.com/fresh"])

    def test_market_news_filters_out_educational_and_chart_pages(self):
        service = self._build_service(include_news=True, include_sector=True)
        today = datetime.now().date().isoformat()
        service.search_service.search_stock_news.return_value = SearchResponse(
            query="A股 盘中异动",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title="盘口异动_行情_走势图—东方财富网",
                    snippet="这是行情工具页，不是热点新闻。",
                    url="https://quote.eastmoney.com/changes?from=center",
                    source="quote.eastmoney.com",
                    published_date=today,
                ),
                SearchResult(
                    title="什么是异动?如何卡异动?在哪看异动?触发异动后如何处置?",
                    snippet="这是解释性文章，不是近一周热点。",
                    url="https://caifuhao.eastmoney.com/news/test",
                    source="caifuhao.eastmoney.com",
                    published_date=today,
                ),
                SearchResult(
                    title="机器人概念午后异动拉升，多股快速冲高",
                    snippet="机器人链条午后再度走强。",
                    url="https://example.com/fresh",
                    source="example.com",
                    published_date=today,
                ),
            ],
        )
        ctx = service._new_run_context()

        items = service.get_market_news(max_per_query=5, ctx=ctx)

        self.assertEqual([item["url"] for item in items], ["https://example.com/fresh"])

    def test_market_news_queries_prioritize_hotspot_event_terms(self):
        service = self._build_service(include_news=True, include_sector=True)
        ctx = service._new_run_context()

        service.get_market_news(max_per_query=3, ctx=ctx)

        queries = [" ".join(call.kwargs.get("focus_keywords") or []) for call in service.search_service.search_stock_news.call_args_list]
        self.assertIn("A股 热点事件", queries)
        self.assertIn("A股 热门板块", queries)
        self.assertNotIn("A股 财经新闻", queries)

    def test_ai_prompt_requires_fresh_hotspot_to_sector_to_stock_reasoning(self):
        service = self._build_service(include_news=True, include_sector=True)

        prompt = service._build_ai_prompt(
            [
                {
                    "stock_code": "600001",
                    "stock_name": "算力一号",
                    "sector_name": "人工智能",
                    "sector_change_pct": 4.5,
                    "score": 88.5,
                    "signal_breakdown": {"technical": "量价强势"},
                    "news_connection": "热点与算力主题相关。",
                    "related_news": [
                        {
                            "title": "机器人产业链催化继续发酵",
                            "snippet": "人形机器人方向热度继续升温。",
                            "source": "example.com",
                            "published_date": "2026-03-25",
                        }
                    ],
                    "quote": {"change_percent": 5.6},
                }
            ],
            [
                {
                    "query": "A股 热点事件",
                    "title": "机器人产业链催化继续发酵",
                    "snippet": "人形机器人方向热度继续升温。",
                    "source": "example.com",
                    "published_date": "2026-03-25",
                }
            ],
            [{"name": "人工智能", "change_pct": 4.5}],
            top_k=5,
            degraded=False,
        )

        self.assertIn("只能使用近3天内的热点新闻", prompt)
        self.assertIn("禁止重新排序", prompt)

    def test_merge_ai_related_news_preserves_url_from_similar_market_news_title(self):
        service = self._build_service(include_news=True, include_sector=True)

        merged = service._merge_ai_related_news(
            [
                {
                    "title": "2025年A股12大热门概念盘点，这些牛股涨超100%",
                    "url": "https://example.com/deepseek-hot",
                    "source": "example.com",
                    "published_date": "2026-03-26",
                }
            ],
            [
                {
                    "title": "2025年A股12大热门概念盘点,这些牛股涨超100%",
                    "relation_reason": "AI 热潮仍在延续。",
                }
            ],
        )

        self.assertEqual(merged[0]["url"], "https://example.com/deepseek-hot")

    def test_merge_ai_related_news_can_reuse_global_market_news_url_when_candidate_has_no_direct_match(self):
        service = self._build_service(include_news=True, include_sector=True)

        merged = service._merge_ai_related_news(
            [],
            [
                {
                    "title": "市场概览:今日A股市场热点纷呈,光伏、电力、商业航天、人形机器人...",
                    "relation_reason": "光伏、电力属于新能源主线，与公司业务方向相关。",
                }
            ],
            [
                {
                    "title": "...市场概览:今日A股市场热点纷呈,光伏、电力、商业航天、人形机器人...",
                    "url": "https://example.com/market-overview",
                    "source": "example.com",
                    "published_date": "2026-03-26",
                }
            ],
        )

        self.assertEqual(
            merged[0].get("url"),
            "https://example.com/market-overview",
        )


if __name__ == "__main__":
    unittest.main()

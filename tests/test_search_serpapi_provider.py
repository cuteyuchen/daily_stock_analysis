# -*- coding: utf-8 -*-
"""Regression tests for SerpAPI Chinese-news behavior."""

from __future__ import annotations

import sys
import unittest
from types import ModuleType
from unittest.mock import MagicMock, patch

# Mock newspaper before search_service import (optional dependency)
if "newspaper" not in sys.modules:
    mock_np = MagicMock()
    mock_np.Article = MagicMock()
    mock_np.Config = MagicMock()
    sys.modules["newspaper"] = mock_np

from src.search_service import SearchResponse, SearchResult, SearchService, SerpAPISearchProvider


class _FakeGoogleSearch:
    response_payload = {"organic_results": []}
    params_calls = []

    def __init__(self, params):
        type(self).params_calls.append(params)

    def get_dict(self):
        return type(self).response_payload

    @classmethod
    def reset(cls) -> None:
        cls.response_payload = {"organic_results": []}
        cls.params_calls = []


def _fake_serpapi_module() -> ModuleType:
    module = ModuleType("serpapi")
    module.GoogleSearch = _FakeGoogleSearch
    return module


class _FakeTavilyClient:
    response_payload = {"results": []}
    search_calls = []

    def __init__(self, api_key=None, **_kwargs):
        self.api_key = api_key

    def search(self, **kwargs):
        type(self).search_calls.append(kwargs)
        return type(self).response_payload

    @classmethod
    def reset(cls) -> None:
        cls.response_payload = {"results": []}
        cls.search_calls = []


def _fake_tavily_module() -> ModuleType:
    module = ModuleType("tavily")
    module.TavilyClient = _FakeTavilyClient
    return module


class TestSerpAPISearchProvider(unittest.TestCase):
    def _patch_serpapi(self, payload):
        _FakeGoogleSearch.reset()
        _FakeGoogleSearch.response_payload = payload
        return patch.dict(sys.modules, {"serpapi": _fake_serpapi_module()})

    def test_provider_uses_baidu_engine_for_chinese_query(self) -> None:
        provider = SerpAPISearchProvider(["dummy_key"])

        with self._patch_serpapi(
            {
                "organic_results": [
                    {
                        "title": "A股 财经新闻",
                        "link": "https://www.baidu.com/s?wd=A股",
                        "snippet": "中文财经结果",
                        "source": "百度",
                        "date": "2026-03-25",
                    }
                ]
            }
        ), patch("src.search_service.fetch_url_content") as mock_fetch_content:
            resp = provider.search("A股 财经新闻", max_results=3, days=3)

        self.assertTrue(resp.success)
        self.assertEqual(_FakeGoogleSearch.params_calls[0]["engine"], "baidu")
        self.assertEqual(_FakeGoogleSearch.params_calls[0]["rn"], 3)
        self.assertEqual(resp.results[0].source, "百度")
        mock_fetch_content.assert_not_called()

    def test_search_stock_news_prefers_serpapi_before_tavily_for_chinese_query(self) -> None:
        _FakeGoogleSearch.reset()
        _FakeGoogleSearch.response_payload = {
            "organic_results": [
                {
                    "title": "A股 市场热点",
                    "link": "https://www.baidu.com/s?wd=A股+市场热点",
                    "snippet": "中文热点结果",
                    "source": "百度",
                    "date": "2026-03-25",
                }
            ]
        }
        _FakeTavilyClient.reset()
        _FakeTavilyClient.response_payload = {
            "results": [
                {
                    "title": "Irrelevant English result",
                    "url": "https://example.com/english",
                    "content": "English coverage",
                    "published_date": "2026-03-25T10:00:00Z",
                }
            ]
        }

        with patch.dict(
            sys.modules,
            {
                "serpapi": _fake_serpapi_module(),
                "tavily": _fake_tavily_module(),
            },
        ):
            service = SearchService(
                tavily_keys=["tavily-key"],
                serpapi_keys=["serpapi-key"],
                searxng_public_instances_enabled=False,
                news_max_age_days=3,
                news_strategy_profile="short",
            )
            resp = service.search_stock_news(
                "market",
                "A股市场",
                max_results=3,
                focus_keywords=["A股", "财经新闻"],
            )

        self.assertTrue(resp.success)
        self.assertEqual(resp.provider, "SerpAPI")
        self.assertEqual(_FakeGoogleSearch.params_calls[0]["engine"], "baidu")
        self.assertEqual(_FakeTavilyClient.search_calls, [])

    def test_search_stock_news_keeps_serpapi_baidu_results_without_dates(self) -> None:
        _FakeGoogleSearch.reset()
        _FakeGoogleSearch.response_payload = {
            "organic_results": [
                {
                    "title": "A股 财经新闻的最新相关信息",
                    "link": "https://www.baidu.com/s?tn=news&wd=A股+财经新闻",
                    "snippet": "中文热点结果",
                    "source": "baidu.com",
                },
                {
                    "title": "A股盘中异动追踪",
                    "link": "https://finance.example.com/a-share-news",
                    "snippet": "盘中热点跟踪",
                    "source": "财经站",
                },
            ]
        }
        _FakeTavilyClient.reset()
        _FakeTavilyClient.response_payload = {
            "results": [
                {
                    "title": "English fallback result",
                    "url": "https://example.com/english",
                    "content": "English coverage",
                    "published_date": "2026-03-25T10:00:00Z",
                }
            ]
        }

        with patch.dict(
            sys.modules,
            {
                "serpapi": _fake_serpapi_module(),
                "tavily": _fake_tavily_module(),
            },
        ):
            service = SearchService(
                tavily_keys=["tavily-key"],
                serpapi_keys=["serpapi-key"],
                searxng_public_instances_enabled=False,
                news_max_age_days=3,
                news_strategy_profile="short",
            )
            resp = service.search_stock_news(
                "market",
                "A股市场",
                max_results=3,
                focus_keywords=["A股", "财经新闻"],
            )

        self.assertTrue(resp.success)
        self.assertEqual(resp.provider, "SerpAPI")
        self.assertEqual(len(resp.results), 2)
        self.assertEqual(_FakeTavilyClient.search_calls, [])
        self.assertTrue(all(result.published_date for result in resp.results))

    def test_search_stock_news_respects_days_override(self) -> None:
        class _Provider:
            name = "FakeNews"
            is_available = True

            def __init__(self):
                self.days_calls = []

            def search(self, query, max_results=5, days=7, **_kwargs):
                self.days_calls.append(days)
                return SearchResponse(
                    query=query,
                    provider=self.name,
                    success=True,
                    results=[
                        SearchResult(
                            title="A股 热点",
                            snippet="近一周新闻",
                            url="https://example.com/news",
                            source="example.com",
                            published_date="2026-03-25",
                        )
                    ],
                )

        provider = _Provider()
        service = SearchService(
            searxng_public_instances_enabled=False,
            news_max_age_days=3,
            news_strategy_profile="short",
        )
        service._providers = [provider]

        resp = service.search_stock_news(
            "market",
            "A股市场",
            max_results=3,
            focus_keywords=["A股", "财经新闻"],
            days_override=7,
        )

        self.assertTrue(resp.success)
        self.assertEqual(provider.days_calls, [7])


if __name__ == "__main__":
    unittest.main()

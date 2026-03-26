# -*- coding: utf-8 -*-
"""Tests for daily picks v3 enhancements: scoring, filtering, diversity, output."""

from __future__ import annotations

import sys
import unittest
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


# ── Helpers ──────────────────────────────────────────────

class _SectorFetcher:
    name = "AkshareFetcher"
    priority = 0

    def __init__(self, top=None, bottom=None):
        self._top = top if top is not None else [
            {"name": "人工智能", "change_pct": 4.5},
            {"name": "半导体", "change_pct": 3.2},
            {"name": "新能源", "change_pct": 2.8},
        ]
        self._bottom = bottom if bottom is not None else []

    def get_sector_rankings(self, n: int = 5):
        return self._top[:n], self._bottom[:n]


class _StockListFetcher:
    name = "BaostockFetcher"
    priority = 1

    def __init__(self, stocks=None):
        self._stocks = stocks or [
            {"code": "600001", "name": "算力一号"},
            {"code": "600002", "name": "算力二号"},
            {"code": "600003", "name": "半导体一号"},
            {"code": "600004", "name": "新能源龙头"},
            {"code": "600005", "name": "消费白马"},
            {"code": "600006", "name": "军工先锋"},
        ]

    def get_stock_list(self):
        return pd.DataFrame(self._stocks)


class _QuoteFetcher:
    name = "EfinanceFetcher"
    priority = 0

    def __init__(self, quotes):
        self._quotes = quotes

    def get_realtime_quote(self, stock_code: str):
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


def _build_service(quotes, boards, stocks=None, news=True, sectors=None, sector_fetcher=None):
    fetchers = [
        _QuoteFetcher(quotes),
        _BoardFetcher(boards),
        _StockListFetcher(stocks),
    ]
    if sector_fetcher:
        fetchers.insert(0, sector_fetcher)
    elif sectors is not None:
        fetchers.insert(0, _SectorFetcher(top=sectors))
    else:
        fetchers.insert(0, _SectorFetcher())

    manager = _FakeManager(fetchers)
    repo = MagicMock()

    if news:
        today = datetime.now().date().isoformat()
        response = SearchResponse(
            query="A股 热点事件",
            provider="BaiduSearch",
            success=True,
            results=[
                SearchResult(
                    title="人工智能方向热度升温",
                    snippet="算力和大模型相关方向继续活跃",
                    url="https://example.com/news/1",
                    source="example.com",
                    published_date=today,
                ),
                SearchResult(
                    title="国务院发布新能源产业支持政策",
                    snippet="政策明确加大对光伏和储能领域的补贴力度",
                    url="https://example.com/news/2",
                    source="example.com",
                    published_date=today,
                ),
            ],
        )
    else:
        response = SearchResponse(
            query="A股 热点事件",
            provider="BaiduSearch",
            success=False,
            results=[],
            error_message="news failed",
        )

    fake_search = MagicMock()
    fake_search.search_stock_news.return_value = response

    with patch("src.services.daily_opportunity_service.DataFetcherManager", return_value=manager), \
         patch("src.services.daily_opportunity_service.get_config", return_value=SimpleNamespace(stock_list=[])), \
         patch("src.services.daily_opportunity_service.get_search_service", return_value=fake_search), \
         patch("src.services.daily_opportunity_service.DailyPicksRepository", return_value=repo):
        return DailyOpportunityService()


# ── Scoring Tests ────────────────────────────────────────

class ScoringTestCase(unittest.TestCase):
    """Test the dimensional scoring model."""

    def test_news_policy_score_zero_when_no_news(self):
        score = DailyOpportunityService._compute_news_policy_score([], [])
        self.assertEqual(score, 0.0)

    def test_news_policy_score_increases_with_news_count(self):
        now = datetime.now().date().isoformat()
        one_news = [{"title": "AI热点", "snippet": "", "published_date": now}]
        two_news = one_news + [{"title": "芯片热点", "snippet": "", "published_date": now}]

        score1 = DailyOpportunityService._compute_news_policy_score(one_news, ["人工智能"])
        score2 = DailyOpportunityService._compute_news_policy_score(two_news, ["人工智能"])
        self.assertGreater(score2, score1)

    def test_news_policy_score_boosts_for_policy_keywords(self):
        now = datetime.now().date().isoformat()
        normal = [{"title": "AI热点来了", "snippet": "", "published_date": now}]
        policy = [{"title": "国务院发布AI政策支持", "snippet": "", "published_date": now}]

        score_normal = DailyOpportunityService._compute_news_policy_score(normal, ["人工智能"])
        score_policy = DailyOpportunityService._compute_news_policy_score(policy, ["人工智能"])
        self.assertGreater(score_policy, score_normal)

    def test_news_freshness_scoring_prefers_recent(self):
        today = datetime.now().date().isoformat()
        old = (datetime.now() - timedelta(days=6)).date().isoformat()
        recent = [{"title": "AI", "snippet": "", "published_date": today}]
        stale = [{"title": "AI", "snippet": "", "published_date": old}]

        score_recent = DailyOpportunityService._compute_news_policy_score(recent, ["AI"])
        score_stale = DailyOpportunityService._compute_news_policy_score(stale, ["AI"])
        self.assertGreater(score_recent, score_stale)

    def test_technical_score_prefers_moderate_gains(self):
        moderate = {"change_percent": 4.0, "volume_ratio": 1.8, "amplitude": 5, "amount": 5e8}
        extreme = {"change_percent": 9.7, "volume_ratio": 1.8, "amplitude": 5, "amount": 5e8}

        score_moderate = DailyOpportunityService._compute_technical_score(moderate)
        score_extreme = DailyOpportunityService._compute_technical_score(extreme)
        self.assertGreater(score_moderate, score_extreme)

    def test_risk_penalty_for_st_is_max(self):
        candidate = {"stock_name": "*ST退市", "quote": {"change_percent": 0, "amplitude": 0, "amount": 1e8, "turnover_rate": 5}}
        penalty = DailyOpportunityService._compute_risk_penalty(candidate)
        self.assertEqual(penalty, float(DailyOpportunityService.MAX_RISK_PENALTY))

    def test_risk_penalty_for_limit_up(self):
        candidate = {"stock_name": "正常股", "quote": {"change_percent": 10.0, "amplitude": 3, "amount": 1e9, "turnover_rate": 8}}
        penalty = DailyOpportunityService._compute_risk_penalty(candidate)
        self.assertGreaterEqual(penalty, 15)

    def test_total_score_computation(self):
        service = DailyOpportunityService.__new__(DailyOpportunityService)
        service.SCORING_WEIGHTS = DailyOpportunityService.SCORING_WEIGHTS
        breakdown = {
            "news_policy": 60,
            "catalyst": 50,
            "sector_heat": 70,
            "stock_heat": 40,
            "technical": 55,
            "capital_flow": 65,
            "fundamental": 0,
            "risk_penalty": 5,
        }
        total = service._compute_total_score(breakdown)
        expected = (
            60 * 25 / 100
            + 50 * 15 / 100
            + 70 * 15 / 100
            + 40 * 10 / 100
            + 55 * 20 / 100
            + 65 * 10 / 100
            + 0 * 5 / 100
            - 5
        )
        self.assertAlmostEqual(total, round(expected, 2), places=2)

    def test_score_breakdown_present_in_recommendations(self):
        quotes = {
            "600001": {"stock_name": "算力一号", "current_price": 12.3, "change_percent": 5.6, "amount": 8.5e8, "turnover_rate": 10.2, "volume_ratio": 1.8, "amplitude": 5, "open": 11.5, "high": 12.5, "low": 11.3, "prev_close": 11.6},
            "600002": {"stock_name": "算力二号", "current_price": 9.8, "change_percent": 4.1, "amount": 6.1e8, "turnover_rate": 8.8, "volume_ratio": 1.6, "amplitude": 4, "open": 9.4, "high": 10.0, "low": 9.3, "prev_close": 9.4},
        }
        boards = {"600001": ["人工智能"], "600002": ["人工智能"]}
        service = _build_service(quotes, boards)
        payload = service.generate_recommendations(top_k=2)

        for rec in payload["recommendations"]:
            self.assertIn("score_breakdown", rec)
            bd = rec["score_breakdown"]
            self.assertIn("news_policy", bd)
            self.assertIn("technical", bd)
            self.assertIn("risk_penalty", bd)

    def test_weak_only_news_cannot_produce_strong_score(self):
        """4-7 天新闻仅做弱辅助，不能单独支撑高分。"""
        old_date = (datetime.now() - timedelta(days=6)).date().isoformat()
        weak_news = [
            {"title": "旧新闻一", "snippet": "", "published_date": old_date},
            {"title": "旧新闻二", "snippet": "", "published_date": old_date},
            {"title": "旧新闻三", "snippet": "", "published_date": old_date},
        ]
        score = DailyOpportunityService._compute_news_policy_score(weak_news, ["AI"])
        # 仅弱辅助新闻时得分应显著低于 3 条主窗口新闻
        today = datetime.now().date().isoformat()
        strong_news = [
            {"title": "AI1", "snippet": "", "published_date": today},
            {"title": "AI2", "snippet": "", "published_date": today},
            {"title": "AI3", "snippet": "", "published_date": today},
        ]
        strong_score = DailyOpportunityService._compute_news_policy_score(strong_news, ["AI"])
        self.assertLess(score, strong_score * 0.5)

    def test_news_beyond_hard_max_ignored(self):
        """超过 NEWS_HARD_MAX_DAYS 的新闻完全不计分。"""
        very_old = (datetime.now() - timedelta(days=10)).date().isoformat()
        news = [{"title": "远古政策", "snippet": "国务院发布", "published_date": very_old}]
        score = DailyOpportunityService._compute_news_policy_score(news, [])
        # 超期新闻被跳过，无 policy bonus，无 freshness bonus，但 very_old 被 continue 跳过
        # 唯一可能得分来源：空主题无兜底分
        self.assertEqual(score, 0.0)

    def test_fundamental_source_in_breakdown(self):
        """score_breakdown 应包含 fundamental_source=not_available。"""
        service = DailyOpportunityService.__new__(DailyOpportunityService)
        candidate = {
            "stock_name": "测试股", "quote": {"change_percent": 3, "amount": 5e8, "turnover_rate": 5, "volume_ratio": 1.2, "amplitude": 4},
            "matched_news": [], "matched_theme_names": [], "sector_name": None, "sector_change_pct": None,
        }
        bd = service._compute_score_breakdown(candidate, [], [], [])
        self.assertEqual(bd["fundamental"], 0.0)
        self.assertEqual(bd["fundamental_source"], "not_available")


# ── Hard Filter Tests ────────────────────────────────────

class HardFilterTestCase(unittest.TestCase):
    """Test the hard filter (ST, limit-up, liquidity)."""

    def test_st_stocks_are_filtered(self):
        candidates = [
            {"stock_code": "600001", "stock_name": "*ST退市", "quote": {"change_percent": 2, "open": 5, "high": 5.1, "low": 4.9, "amount": 1e8}},
            {"stock_code": "600002", "stock_name": "正常股票", "quote": {"change_percent": 3, "open": 10, "high": 10.5, "low": 9.8, "amount": 5e8}},
        ]
        filtered, removed = DailyOpportunityService._apply_hard_filters(candidates)
        codes = [c["stock_code"] for c in filtered]
        self.assertNotIn("600001", codes)
        self.assertIn("600002", codes)
        self.assertEqual(len(removed), 1)
        self.assertIn("ST", removed[0]["reason"])

    def test_limit_up_one_board_filtered(self):
        candidates = [
            {"stock_code": "600003", "stock_name": "一字板", "quote": {"change_percent": 10.0, "open": 11.0, "high": 11.0, "low": 11.0, "amount": 3e8}},
        ]
        filtered, removed = DailyOpportunityService._apply_hard_filters(candidates)
        self.assertEqual(len(filtered), 0)
        self.assertIn("一字板", removed[0]["reason"])

    def test_very_low_liquidity_filtered(self):
        candidates = [
            {"stock_code": "600004", "stock_name": "冷门股", "quote": {"change_percent": 1.0, "open": 5, "high": 5.1, "low": 4.9, "amount": 1e7}},
        ]
        filtered, removed = DailyOpportunityService._apply_hard_filters(candidates)
        self.assertEqual(len(filtered), 0)
        self.assertIn("流动性", removed[0]["reason"])

    def test_normal_stock_passes_filter(self):
        candidates = [
            {"stock_code": "600005", "stock_name": "正常标的", "quote": {"change_percent": 4, "open": 20, "high": 21, "low": 19.5, "amount": 5e8}},
        ]
        filtered, removed = DailyOpportunityService._apply_hard_filters(candidates)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(len(removed), 0)

    def test_limit_up_non_yizi_passes_filter(self):
        """非一字板涨停（有实际振幅）不应被硬过滤，仅被 risk_penalty 覆盖。"""
        candidates = [
            {"stock_code": "600006", "stock_name": "强势涨停", "quote": {"change_percent": 10.0, "open": 10.0, "high": 11.0, "low": 9.8, "amount": 8e8}},
        ]
        filtered, removed = DailyOpportunityService._apply_hard_filters(candidates)
        self.assertEqual(len(filtered), 1)
        self.assertEqual(len(removed), 0)

    def test_hard_filter_min_amount_configurable(self):
        """成交额阈值应使用类常量，可通过子类覆盖。"""
        candidates = [
            {"stock_code": "600007", "stock_name": "低量股", "quote": {"change_percent": 2, "open": 5, "high": 5.1, "low": 4.9, "amount": 1.5e7}},
        ]
        # 默认阈值 2e7，应被过滤
        filtered, _ = DailyOpportunityService._apply_hard_filters(candidates)
        self.assertEqual(len(filtered), 0)

        # 降低阈值后应通过
        original = DailyOpportunityService.HARD_FILTER_MIN_AMOUNT
        try:
            DailyOpportunityService.HARD_FILTER_MIN_AMOUNT = 1e7
            filtered2, _ = DailyOpportunityService._apply_hard_filters(candidates)
            self.assertEqual(len(filtered2), 1)
        finally:
            DailyOpportunityService.HARD_FILTER_MIN_AMOUNT = original


# ── Diversity Tests ──────────────────────────────────────

class DiversityTestCase(unittest.TestCase):
    """Test the diversity constraint."""

    def test_same_sector_capped_at_two(self):
        # When alternative sectors exist, same-sector count must be capped.
        candidates = [
            {"stock_code": "600001", "stock_name": "AI一号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 95},
            {"stock_code": "600002", "stock_name": "AI二号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 93},
            {"stock_code": "600003", "stock_name": "AI三号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 91},
            {"stock_code": "600004", "stock_name": "半导体一号", "sector_name": "半导体", "matched_theme_names": ["半导体"], "score": 89},
            {"stock_code": "600005", "stock_name": "新能源一号", "sector_name": "新能源", "matched_theme_names": ["新能源"], "score": 87},
            {"stock_code": "600006", "stock_name": "AI四号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 85},
            {"stock_code": "600007", "stock_name": "医药一号", "sector_name": "医药", "matched_theme_names": ["医药"], "score": 83},
        ]
        result = DailyOpportunityService._diversify_candidates(candidates, top_k=5)
        ai_count = sum(1 for c in result[:5] if c["sector_name"] == "人工智能")
        self.assertLessEqual(ai_count, DailyOpportunityService.SAME_SECTOR_MAX)

    def test_mixed_sectors_preserved(self):
        candidates = [
            {"stock_code": "600001", "stock_name": "AI一号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 90},
            {"stock_code": "600002", "stock_name": "AI二号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 88},
            {"stock_code": "600003", "stock_name": "半导体", "sector_name": "半导体", "matched_theme_names": ["半导体"], "score": 85},
            {"stock_code": "600004", "stock_name": "新能源", "sector_name": "新能源", "matched_theme_names": ["新能源"], "score": 82},
            {"stock_code": "600005", "stock_name": "AI三号", "sector_name": "人工智能", "matched_theme_names": ["人工智能"], "score": 80},
        ]
        result = DailyOpportunityService._diversify_candidates(candidates, top_k=4)
        sectors = [c["sector_name"] for c in result[:4]]
        self.assertIn("半导体", sectors)
        self.assertIn("新能源", sectors)

    def test_diversity_still_fills_top_k(self):
        candidates = [
            {"stock_code": f"60000{i}", "stock_name": f"Stock{i}", "sector_name": "人工智能", "matched_theme_names": [], "score": 90 - i}
            for i in range(10)
        ]
        result = DailyOpportunityService._diversify_candidates(candidates, top_k=5)
        self.assertEqual(len(result), 5)

    def test_normalize_theme_canonical_passthrough(self):
        """规范名本身应直接返回，不做额外变换。"""
        self.assertEqual(DailyOpportunityService._normalize_theme("人工智能"), "人工智能")
        self.assertEqual(DailyOpportunityService._normalize_theme("半导体"), "半导体")

    def test_normalize_theme_alias_mapping(self):
        """别名应归一到对应规范名。"""
        self.assertEqual(DailyOpportunityService._normalize_theme("AI应用"), "人工智能")
        self.assertEqual(DailyOpportunityService._normalize_theme("芯片"), "半导体")
        self.assertEqual(DailyOpportunityService._normalize_theme("光伏"), "新能源")

    def test_normalize_theme_unknown_passthrough(self):
        """未在别名表中的名称应原样返回。"""
        self.assertEqual(DailyOpportunityService._normalize_theme("区块链"), "区块链")
        self.assertEqual(DailyOpportunityService._normalize_theme("  区块链  "), "区块链")

    def test_diversity_deduplicates_aliased_themes(self):
        """'AI应用' 和 '人工智能' 属于同一规范主题，应受同主题上限约束。"""
        candidates = [
            {"stock_code": "600001", "stock_name": "AI一号", "sector_name": "AI应用", "matched_theme_names": ["AI应用"], "score": 95},
            {"stock_code": "600002", "stock_name": "AI二号", "sector_name": "算力", "matched_theme_names": ["算力"], "score": 93},
            {"stock_code": "600003", "stock_name": "AI三号", "sector_name": "大模型", "matched_theme_names": ["大模型"], "score": 91},
            {"stock_code": "600004", "stock_name": "半导体一号", "sector_name": "半导体", "matched_theme_names": ["半导体"], "score": 89},
            {"stock_code": "600005", "stock_name": "新能源一号", "sector_name": "新能源", "matched_theme_names": ["新能源"], "score": 87},
            {"stock_code": "600006", "stock_name": "医药一号", "sector_name": "医药", "matched_theme_names": ["医药"], "score": 85},
        ]
        result = DailyOpportunityService._diversify_candidates(candidates, top_k=5)
        # AI应用、算力、大模型 all normalize to 人工智能 — should be capped at SAME_SECTOR_MAX
        ai_codes = {"600001", "600002", "600003"}
        ai_in_top5 = sum(1 for c in result[:5] if c["stock_code"] in ai_codes)
        self.assertLessEqual(ai_in_top5, DailyOpportunityService.SAME_SECTOR_MAX)


# ── Entry Hint & Tags Tests ─────────────────────────────

class OutputFieldsTestCase(unittest.TestCase):
    """Test new output fields: entry_hint, reason_tags, risk_tags, etc."""

    def test_entry_hint_suggests_waiting_for_high_gain(self):
        candidate = {"quote": {"change_percent": 8.0, "volume_ratio": 2.0, "amplitude": 6}}
        hint = DailyOpportunityService._build_entry_hint(candidate)
        self.assertIn("观望", hint)

    def test_entry_hint_suggests_breakout_for_moderate_gain(self):
        candidate = {"quote": {"change_percent": 5.0, "volume_ratio": 2.0, "amplitude": 5}}
        hint = DailyOpportunityService._build_entry_hint(candidate)
        self.assertIn("突破", hint)

    def test_reason_tags_include_news_driven(self):
        candidate = {"matched_news": [{"title": "test"}], "matched_theme_names": ["AI"], "sector_name": "人工智能", "quote": {"change_percent": 3, "volume_ratio": 1.5, "amount": 5e8}}
        tags = DailyOpportunityService._build_reason_tags(candidate)
        self.assertIn("新闻驱动", tags)
        self.assertIn("热点主题", tags)

    def test_risk_tags_flag_chase_high(self):
        candidate = {"quote": {"change_percent": 9.0, "amplitude": 10, "turnover_rate": 12, "amount": 1e9}, "matched_news": []}
        tags = DailyOpportunityService._build_risk_tags(candidate)
        self.assertIn("追高风险", tags)

    def test_stop_loss_hint_references_prev_close(self):
        candidate = {"quote": {"current_price": 12.0, "prev_close": 11.5, "low": 11.3}}
        hint = DailyOpportunityService._build_stop_loss_hint(candidate)
        self.assertIn("止损", hint)

    def test_fundamental_score_zero_for_st(self):
        score = DailyOpportunityService._compute_fundamental_score("*ST测试")
        self.assertEqual(score, 0.0)

    def test_fundamental_score_always_zero_without_data(self):
        score = DailyOpportunityService._compute_fundamental_score("正常股票")
        self.assertEqual(score, 0.0)


# ── Integration / Compatibility Tests ────────────────────

class IntegrationTestCase(unittest.TestCase):
    """Test that the optimized pipeline produces compatible output."""

    def test_full_pipeline_output_has_all_required_fields(self):
        quotes = {
            "600001": {"stock_name": "算力一号", "current_price": 12.3, "change_percent": 5.6, "amount": 8.5e8, "turnover_rate": 10.2, "volume_ratio": 1.8, "amplitude": 5, "open": 11.5, "high": 12.5, "low": 11.3, "prev_close": 11.6},
            "600002": {"stock_name": "算力二号", "current_price": 9.8, "change_percent": 4.1, "amount": 6.1e8, "turnover_rate": 8.8, "volume_ratio": 1.6, "amplitude": 4, "open": 9.4, "high": 10.0, "low": 9.3, "prev_close": 9.4},
            "600003": {"stock_name": "半导体一号", "current_price": 18.6, "change_percent": 3.5, "amount": 5.6e8, "turnover_rate": 6.8, "volume_ratio": 1.4, "amplitude": 4, "open": 18.0, "high": 18.8, "low": 17.9, "prev_close": 18.0},
            "600004": {"stock_name": "新能源龙头", "current_price": 25.0, "change_percent": 2.8, "amount": 4e8, "turnover_rate": 5, "volume_ratio": 1.2, "amplitude": 3, "open": 24.5, "high": 25.2, "low": 24.3, "prev_close": 24.3},
            "600005": {"stock_name": "消费白马", "current_price": 35.0, "change_percent": 1.5, "amount": 3e8, "turnover_rate": 3, "volume_ratio": 1.0, "amplitude": 2, "open": 34.5, "high": 35.2, "low": 34.3, "prev_close": 34.5},
            "600006": {"stock_name": "军工先锋", "current_price": 15.0, "change_percent": 3.0, "amount": 4e8, "turnover_rate": 7, "volume_ratio": 1.3, "amplitude": 4, "open": 14.5, "high": 15.2, "low": 14.4, "prev_close": 14.6},
        }
        boards = {
            "600001": ["人工智能"],
            "600002": ["人工智能"],
            "600003": ["半导体"],
            "600004": ["新能源"],
            "600005": ["消费"],
            "600006": ["军工"],
        }
        service = _build_service(quotes, boards)
        payload = service.generate_recommendations(top_k=5)

        self.assertEqual(payload["output_count"], 5)
        self.assertIn("strategy_version", payload)
        self.assertEqual(payload["strategy_version"], "daily_picks_v3")
        self.assertIn("recommendations", payload)

        for rec in payload["recommendations"]:
            self.assertIn("rank", rec)
            self.assertIn("stock_code", rec)
            self.assertIn("stock_name", rec)
            self.assertIn("score", rec)
            self.assertIn("score_breakdown", rec)
            self.assertIn("reason_tags", rec)
            self.assertIn("risk_tags", rec)
            self.assertIn("entry_hint", rec)
            self.assertIn("stop_loss_hint", rec)
            self.assertIn("recommend_reason", rec)
            self.assertIn("operation_advice", rec)
            self.assertIn("risk_warning", rec)
            self.assertIn("news_connection", rec)
            self.assertIn("signal_breakdown", rec)
            self.assertIn("quote", rec)
            self.assertIn("confidence", rec)
            self.assertIn("risk_note", rec)
            # Audit trail fields
            self.assertIn("program_rank", rec)
            self.assertIn("final_rank", rec)
            # Legacy fields still present
            self.assertIn("related_news", rec)
            self.assertIn("sector_name", rec)

    def test_output_still_produces_5_stocks(self):
        quotes = {
            f"60000{i}": {"stock_name": f"Stock{i}", "current_price": 10 + i, "change_percent": 3 + i * 0.5, "amount": 5e8, "turnover_rate": 8, "volume_ratio": 1.5, "amplitude": 4, "open": 10, "high": 11, "low": 9.5, "prev_close": 10}
            for i in range(1, 7)
        }
        boards = {f"60000{i}": ["测试板块"] for i in range(1, 7)}
        service = _build_service(quotes, boards)
        payload = service.generate_recommendations(top_k=5)

        self.assertEqual(len(payload["recommendations"]), 5)

    def test_degraded_mode_without_news_still_works(self):
        quotes = {
            "600001": {"stock_name": "测试一号", "current_price": 10, "change_percent": 3, "amount": 5e8, "turnover_rate": 8, "volume_ratio": 1.5, "amplitude": 4, "open": 9.5, "high": 10.2, "low": 9.4, "prev_close": 9.7},
            "600002": {"stock_name": "测试二号", "current_price": 11, "change_percent": 2, "amount": 4e8, "turnover_rate": 6, "volume_ratio": 1.3, "amplitude": 3, "open": 10.8, "high": 11.1, "low": 10.7, "prev_close": 10.8},
        }
        boards = {"600001": ["板块A"], "600002": ["板块B"]}
        service = _build_service(quotes, boards, news=False)
        payload = service.generate_recommendations(top_k=2)

        self.assertEqual(payload["output_count"], 2)
        self.assertTrue(payload["degraded"])
        self.assertTrue(all(r.get("stock_code") for r in payload["recommendations"]))

    def test_hard_filter_removes_st_from_final_output(self):
        quotes = {
            "600001": {"stock_name": "*ST退市", "current_price": 3, "change_percent": 1, "amount": 5e7, "turnover_rate": 5, "volume_ratio": 1.0, "amplitude": 3, "open": 3, "high": 3.1, "low": 2.9, "prev_close": 3.0},
            "600002": {"stock_name": "正常股票", "current_price": 15, "change_percent": 4, "amount": 8e8, "turnover_rate": 10, "volume_ratio": 1.5, "amplitude": 5, "open": 14.5, "high": 15.2, "low": 14.3, "prev_close": 14.4},
        }
        boards = {"600001": ["风险板块"], "600002": ["人工智能"]}
        service = _build_service(quotes, boards)
        payload = service.generate_recommendations(top_k=2)

        stock_names = [r["stock_name"] for r in payload["recommendations"]]
        self.assertNotIn("*ST退市", stock_names)

    def test_llm_failure_does_not_break_pipeline(self):
        quotes = {
            "600001": {"stock_name": "算力一号", "current_price": 12, "change_percent": 5, "amount": 8e8, "turnover_rate": 10, "volume_ratio": 1.8, "amplitude": 5, "open": 11.5, "high": 12.5, "low": 11.3, "prev_close": 11.6},
            "600002": {"stock_name": "半导体一号", "current_price": 18, "change_percent": 3.5, "amount": 5e8, "turnover_rate": 7, "volume_ratio": 1.4, "amplitude": 4, "open": 17.5, "high": 18.2, "low": 17.3, "prev_close": 17.4},
        }
        boards = {"600001": ["人工智能"], "600002": ["半导体"]}
        service = _build_service(quotes, boards)

        with patch("src.services.daily_opportunity_service.GeminiAnalyzer", create=True) as analyzer_cls:
            analyzer_cls.return_value.generate_text.side_effect = RuntimeError("LLM down")
            payload = service.generate_recommendations(top_k=2)

        self.assertEqual(payload["output_count"], 2)
        self.assertTrue(all(r.get("stock_code") for r in payload["recommendations"]))


if __name__ == "__main__":
    unittest.main()

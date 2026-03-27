# -*- coding: utf-8 -*-
"""稳定优先的 daily picks 生成服务。"""

from __future__ import annotations

import json
import logging
import math
import re
import time
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

from data_provider.base import DataFetcherManager, normalize_stock_code, summarize_exception
from src.config import get_config
from src.search_service import get_search_service
from src.services.daily_picks_repository import DailyPicksRepository

try:
    from json_repair import repair_json
except Exception:  # pragma: no cover - optional dependency should already exist in runtime
    repair_json = None

try:
    from src.analyzer import GeminiAnalyzer
except Exception:  # pragma: no cover - fail-open for optional AI path
    GeminiAnalyzer = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class DailyOpportunityService:
    """稳定优先的每日热点推荐服务。"""

    DEFAULT_NEWS_THEMES = [
        "A股 热点事件",
        "A股 热门板块",
        "A股 产业催化",
        "A股 盘中异动",
    ]
    DEFAULT_OPERATION_ADVICE = (
        "建议优先跟踪放量强势且仍具流动性的标的，分批观察，不宜在大幅高开或一致性过强时盲目追涨。"
    )
    DEFAULT_RISK_WARNING = (
        "当前推荐为规则筛选与外部数据综合结果，未覆盖公告、停复牌、监管问询与盘口异动等实时风险，请务必自行复核。"
    )
    THEME_ALIASES: Dict[str, List[str]] = {
        "人工智能": ["AI", "算力", "大模型", "机器人", "智能体"],
        "半导体": ["芯片", "晶圆", "封装", "存储"],
        "新能源": ["光伏", "储能", "风电", "锂电", "新能源车"],
        "消费电子": ["苹果链", "折叠屏", "面板", "手机"],
        "低空经济": ["飞行汽车", "无人机", "空域"],
        "军工": ["卫星", "导弹", "军工信息化"],
        "医药": ["创新药", "医疗器械", "减肥药"],
    }
    STOCK_POOL_LIMIT = 600
    QUOTE_SCAN_LIMIT = 240
    ENRICH_LIMIT = 40
    PROVIDER_EMPTY_DISABLE_THRESHOLD = {
        "quote": 3,
        "board": 3,
    }
    NEWS_FRESHNESS_DAYS = 7
    NEWS_PRIORITY_DAYS = 3
    NEWS_HARD_MAX_DAYS = 7
    SCORING_WEIGHTS: Dict[str, int] = {
        "news_policy": 25,
        "catalyst": 15,
        "sector_heat": 15,
        "stock_heat": 10,
        "technical": 20,
        "capital_flow": 10,
        "fundamental": 5,
    }
    MAX_RISK_PENALTY = 20
    SAME_SECTOR_MAX = 2
    SAME_THEME_MAX = 2
    HARD_FILTER_MIN_AMOUNT = 2e7  # 成交额硬过滤阈值（元），可通过子类或配置覆盖
    POLICY_KEYWORDS = (
        "政策", "国务院", "发改委", "工信部", "证监会", "央行", "财政部",
        "部署", "规划", "意见", "通知", "纲要", "方案", "补贴", "减税",
    )
    ROUNDUP_KEYWORDS = ("盘点", "回顾", "复盘", "年终", "年度", "全年", "最强音")
    GENERIC_PORTAL_KEYWORDS = ("最新相关信息",)
    LOW_SIGNAL_SOURCE_KEYWORDS = ("baike.baidu.com", "nourl.ubs.baidu.com", "emdatah5.eastmoney.com")
    LOW_SIGNAL_TITLE_KEYWORDS = ("走势图", "行情", "资金流向")

    def __init__(self):
        self.config = get_config()
        self.manager = DataFetcherManager()
        self.search_service = get_search_service()
        self.repo = DailyPicksRepository()

    def _new_run_context(self) -> Dict[str, Any]:
        return {
            "started_at": datetime.now(),
            "warnings": [],
            "used_sources": set(),
            "failed_sources": set(),
            "source_summary": {
                "news": [],
                "sector_rankings": [],
                "stock_list": [],
                "quote_warmup": [],
                "quote_stats": {"attempted": 0, "succeeded": 0, "failed": 0, "providers": {}},
                "board_stats": {"attempted": 0, "succeeded": 0, "failed": 0, "providers": {}},
                "ai_reasoning": [],
            },
            "runtime_hints": {
                "disabled_quote_fetchers": set(),
                "disabled_board_fetchers": set(),
                "provider_runtime_state": {
                    "quote": {},
                    "board": {},
                },
            },
        }

    @staticmethod
    def _finalize_source_summary(summary: Dict[str, Any]) -> Dict[str, Any]:
        return summary

    @staticmethod
    def _append_unique(items: List[str], value: str) -> None:
        if value and value not in items:
            items.append(value)

    def _append_warning(self, ctx: Dict[str, Any], message: str) -> None:
        self._append_unique(ctx["warnings"], message)

    @classmethod
    def _parse_news_date(cls, raw_value: Any) -> Optional[datetime]:
        value = str(raw_value or "").strip()
        if not value:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None

    @classmethod
    def _looks_like_roundup_article(cls, title: str, snippet: str) -> bool:
        text = f"{title} {snippet}".strip()
        if not text:
            return False
        year_matches = [int(item) for item in re.findall(r"(20\d{2})年", text)]
        current_year = datetime.now().year
        has_old_year = any(year < current_year for year in year_matches)
        has_roundup_keyword = any(keyword in text for keyword in cls.ROUNDUP_KEYWORDS)
        return has_old_year and has_roundup_keyword

    @classmethod
    def _looks_like_old_year_article(cls, title: str, snippet: str) -> bool:
        text = f"{title} {snippet}".strip()
        if not text:
            return False
        year_matches = [int(item) for item in re.findall(r"(20\d{2})年", text)]
        if not year_matches:
            return False
        current_year = datetime.now().year
        return any(year < current_year for year in year_matches) and current_year not in year_matches

    @classmethod
    def _looks_like_generic_portal_article(cls, title: str, snippet: str) -> bool:
        text = f"{title} {snippet}".strip()
        if not text:
            return True
        return any(keyword in text for keyword in cls.GENERIC_PORTAL_KEYWORDS)

    @classmethod
    def _looks_like_low_signal_source(cls, item: Dict[str, Any]) -> bool:
        source = str(item.get("source") or "").strip().lower()
        url = str(item.get("url") or "").strip().lower()
        text = f"{item.get('title') or ''} {item.get('snippet') or ''}"
        if "百科" in text:
            return True
        return any(keyword in source or keyword in url for keyword in cls.LOW_SIGNAL_SOURCE_KEYWORDS)

    @classmethod
    def _looks_like_educational_or_tool_article(cls, title: str, snippet: str) -> bool:
        text = f"{title} {snippet}".strip()
        if not text:
            return False
        if text.startswith("什么是"):
            return True
        return any(keyword in text for keyword in cls.LOW_SIGNAL_TITLE_KEYWORDS)

    @classmethod
    def _is_recent_hotspot_news(cls, item: Dict[str, Any]) -> bool:
        title = str(item.get("title") or "")
        snippet = str(item.get("snippet") or "")
        if (
            cls._looks_like_roundup_article(title, snippet)
            or cls._looks_like_old_year_article(title, snippet)
            or cls._looks_like_generic_portal_article(title, snippet)
            or cls._looks_like_low_signal_source(item)
            or cls._looks_like_educational_or_tool_article(title, snippet)
        ):
            return False

        published_at = cls._parse_news_date(item.get("published_date"))
        if published_at is None:
            return True

        cutoff = datetime.now() - timedelta(days=cls.NEWS_FRESHNESS_DAYS)
        return published_at >= cutoff

    def _mark_source(self, ctx: Dict[str, Any], provider: str, result: str) -> None:
        if result == "ok":
            ctx["used_sources"].add(provider)
        elif result == "failed":
            ctx["failed_sources"].add(provider)

    def _record_provider_stat(self, stats: Dict[str, Any], provider: str, result: str) -> None:
        provider_stats = stats["providers"].setdefault(provider, {"ok": 0, "failed": 0, "empty": 0})
        provider_stats[result] = provider_stats.get(result, 0) + 1
        if result == "ok":
            stats["succeeded"] += 1
        elif result == "failed":
            stats["failed"] += 1

    def _runtime_disabled_fetchers(self, ctx: Dict[str, Any], kind: str) -> set[str]:
        hints = ctx.setdefault("runtime_hints", {})
        return hints.setdefault(f"disabled_{kind}_fetchers", set())

    def _runtime_provider_state(self, ctx: Dict[str, Any], kind: str, provider: str) -> Dict[str, int]:
        hints = ctx.setdefault("runtime_hints", {})
        state_bucket = hints.setdefault("provider_runtime_state", {}).setdefault(kind, {})
        return state_bucket.setdefault(provider, {"ok": 0, "empty": 0, "failed": 0})

    def _record_runtime_provider_result(
        self,
        ctx: Dict[str, Any],
        kind: str,
        provider: str,
        result: str,
        *,
        disable_on_first_miss: bool = False,
    ) -> None:
        state = self._runtime_provider_state(ctx, kind, provider)
        if result in {"ok", "empty", "failed"}:
            state[result] = state.get(result, 0) + 1
        if result == "ok":
            return

        if disable_on_first_miss:
            self._runtime_disabled_fetchers(ctx, kind).add(provider)
            return

        threshold = self.PROVIDER_EMPTY_DISABLE_THRESHOLD.get(kind, 0)
        if threshold and state.get("ok", 0) == 0 and state.get("empty", 0) + state.get("failed", 0) >= threshold:
            self._runtime_disabled_fetchers(ctx, kind).add(provider)

    def _is_runtime_provider_disabled(self, ctx: Dict[str, Any], kind: str, provider: str) -> bool:
        return provider in self._runtime_disabled_fetchers(ctx, kind)

    @staticmethod
    def _sort_fetchers(fetchers: Iterable[Any], kind: str) -> List[Any]:
        preference_map = {
            "sector": {
                "AkshareFetcher": 0,
                "EfinanceFetcher": 1,
                "JoinQuantFetcher": 3,
                "PytdxFetcher": 4,
                "TushareFetcher": 9,
            },
            "stock_list": {
                "JoinQuantFetcher": 0,
                "BaostockFetcher": 1,
                "PytdxFetcher": 2,
                "TushareFetcher": 9,
            },
            "quote": {
                "EfinanceFetcher": 0,
                "AkshareFetcher": 1,
                "PytdxFetcher": 2,
                "TushareFetcher": 9,
            },
            "board": {
                "JoinQuantFetcher": 0,
                "EfinanceFetcher": 1,
                "TushareFetcher": 9,
            },
        }
        weights = preference_map.get(kind, {})
        return sorted(fetchers, key=lambda item: (weights.get(getattr(item, "name", ""), 5), getattr(item, "priority", 99)))

    def _iter_fetchers_for(self, kind: str) -> List[Any]:
        capability_map = {
            "sector": "get_sector_rankings",
            "stock_list": "get_stock_list",
            "quote": "get_realtime_quote",
            "board": "get_belong_board",
        }
        capability = capability_map[kind]
        fetchers = [fetcher for fetcher in getattr(self.manager, "_fetchers", []) if hasattr(fetcher, capability)]
        return self._sort_fetchers(fetchers, kind)

    @staticmethod
    def _normalize_stock_rows(df: pd.DataFrame, limit: int) -> List[Dict[str, Any]]:
        if df is None or df.empty:
            return []
        code_col = None
        name_col = None
        for col in df.columns:
            low = str(col).lower()
            if code_col is None and low in {"code", "ts_code", "symbol", "index"}:
                code_col = col
            if name_col is None and low in {"name", "stock_name", "display_name", "简称", "证券简称"}:
                name_col = col
        if code_col is None:
            return []

        if len(df) <= limit:
            sampled = df
        else:
            indexes = sorted(
                {
                    min(len(df) - 1, round(i * (len(df) - 1) / max(limit - 1, 1)))
                    for i in range(limit)
                }
            )
            sampled = df.iloc[indexes]

        rows: List[Dict[str, Any]] = []
        for _, row in sampled.iterrows():
            code = normalize_stock_code(str(row.get(code_col) or "").strip())
            if not code:
                continue
            rows.append(
                {
                    "stock_code": code[-6:] if code[-6:].isdigit() else code,
                    "stock_name": str(row.get(name_col) or "").strip() if name_col else "",
                }
            )
        return rows

    @staticmethod
    def _dedup_stock_pool(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for item in rows:
            code = normalize_stock_code(str(item.get("stock_code") or ""))
            if not code or code in seen:
                continue
            seen.add(code)
            deduped.append(
                {
                    "stock_code": code,
                    "stock_name": str(item.get("stock_name") or "").strip(),
                }
            )
        return deduped

    def _get_stock_pool(self, limit: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        pool: List[Dict[str, Any]] = []
        for fetcher in self._iter_fetchers_for("stock_list"):
            start = time.time()
            try:
                df = fetcher.get_stock_list()
                rows = self._normalize_stock_rows(df, limit=limit)
                duration_ms = int((time.time() - start) * 1000)
                if rows:
                    ctx["source_summary"]["stock_list"].append(
                        {
                            "provider": fetcher.name,
                            "result": "ok",
                            "duration_ms": duration_ms,
                            "count": len(rows),
                        }
                    )
                    self._mark_source(ctx, fetcher.name, "ok")
                    pool.extend(rows)
                    break
                ctx["source_summary"]["stock_list"].append(
                    {
                        "provider": fetcher.name,
                        "result": "empty",
                        "duration_ms": duration_ms,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                _, reason = summarize_exception(exc)
                ctx["source_summary"]["stock_list"].append(
                    {
                        "provider": fetcher.name,
                        "result": "failed",
                        "duration_ms": int((time.time() - start) * 1000),
                        "error": reason,
                    }
                )
                self._mark_source(ctx, fetcher.name, "failed")
                self._append_warning(ctx, f"股票池获取失败（{fetcher.name}）：{reason}")
        return self._dedup_stock_pool(pool)[:limit]

    def _quote_to_dict(self, quote: Any, stock_code: str, stock_name: str) -> Dict[str, Any]:
        if quote is None:
            return {}
        if isinstance(quote, dict):
            price = quote.get("price") or quote.get("current_price")
            change_pct = quote.get("change_pct") or quote.get("change_percent")
            change_amount = quote.get("change_amount") or quote.get("change")
            return {
                "stock_code": stock_code,
                "stock_name": quote.get("name") or quote.get("stock_name") or stock_name or stock_code,
                "current_price": price,
                "change": change_amount,
                "change_percent": change_pct,
                "open": quote.get("open") or quote.get("open_price"),
                "high": quote.get("high"),
                "low": quote.get("low"),
                "prev_close": quote.get("pre_close") or quote.get("prev_close"),
                "volume": quote.get("volume"),
                "amount": quote.get("amount"),
                "turnover_rate": quote.get("turnover_rate"),
                "volume_ratio": quote.get("volume_ratio"),
                "amplitude": quote.get("amplitude"),
                "source": quote.get("source"),
                "update_time": datetime.now().isoformat(),
            }

        source = getattr(getattr(quote, "source", None), "value", None) or getattr(quote, "source", None)
        return {
            "stock_code": stock_code,
            "stock_name": getattr(quote, "name", None) or stock_name or stock_code,
            "current_price": getattr(quote, "price", None),
            "change": getattr(quote, "change_amount", None),
            "change_percent": getattr(quote, "change_pct", None),
            "open": getattr(quote, "open_price", None),
            "high": getattr(quote, "high", None),
            "low": getattr(quote, "low", None),
            "prev_close": getattr(quote, "pre_close", None),
            "volume": getattr(quote, "volume", None),
            "amount": getattr(quote, "amount", None),
            "turnover_rate": getattr(quote, "turnover_rate", None),
            "volume_ratio": getattr(quote, "volume_ratio", None),
            "amplitude": getattr(quote, "amplitude", None),
            "source": source,
            "update_time": datetime.now().isoformat(),
        }

    def _quote_from_fetcher(self, fetcher: Any, stock_code: str) -> Tuple[Optional[Dict[str, Any]], str]:
        if fetcher.name == "AkshareFetcher":
            for source in ("em", "sina", "tencent"):
                quote = fetcher.get_realtime_quote(stock_code, source=source)
                quote_dict = self._quote_to_dict(quote, stock_code, "")
                if quote_dict.get("current_price"):
                    return quote_dict, f"{fetcher.name}:{source}"
            return None, f"{fetcher.name}:all"
        quote = fetcher.get_realtime_quote(stock_code)
        quote_dict = self._quote_to_dict(quote, stock_code, "")
        if quote_dict.get("current_price"):
            return quote_dict, fetcher.name
        return None, fetcher.name

    def _warm_quote_cache(self, stock_codes: List[str], ctx: Dict[str, Any]) -> None:
        if not stock_codes:
            return
        first_code = normalize_stock_code(stock_codes[0])
        for fetcher in self._iter_fetchers_for("quote"):
            if self._is_runtime_provider_disabled(ctx, "quote", fetcher.name):
                continue
            if fetcher.name == "TushareFetcher":
                continue
            start = time.time()
            try:
                quote, provider_label = self._quote_from_fetcher(fetcher, first_code)
                result = "ok" if quote else "empty"
                self._record_runtime_provider_result(
                    ctx,
                    "quote",
                    fetcher.name,
                    result,
                    disable_on_first_miss=(result != "ok"),
                )
                ctx["source_summary"]["quote_warmup"].append(
                    {
                        "provider": provider_label,
                        "result": result,
                        "duration_ms": int((time.time() - start) * 1000),
                    }
                )
                if quote:
                    self._mark_source(ctx, fetcher.name, "ok")
                    return
            except Exception as exc:  # noqa: BLE001
                _, reason = summarize_exception(exc)
                ctx["source_summary"]["quote_warmup"].append(
                    {
                        "provider": fetcher.name,
                        "result": "failed",
                        "duration_ms": int((time.time() - start) * 1000),
                        "error": reason,
                    }
                )
                self._record_runtime_provider_result(
                    ctx,
                    "quote",
                    fetcher.name,
                    "failed",
                    disable_on_first_miss=True,
                )
                self._mark_source(ctx, fetcher.name, "failed")

    def _get_quote(
        self,
        stock_code: str,
        stock_name: str,
        ctx: Dict[str, Any],
        quote_cache: Dict[str, Optional[Dict[str, Any]]],
    ) -> Optional[Dict[str, Any]]:
        code = normalize_stock_code(stock_code)
        if code in quote_cache:
            return quote_cache[code]

        stats = ctx["source_summary"]["quote_stats"]
        stats["attempted"] += 1
        for fetcher in self._iter_fetchers_for("quote"):
            if self._is_runtime_provider_disabled(ctx, "quote", fetcher.name):
                continue
            start = time.time()
            try:
                quote, provider_label = self._quote_from_fetcher(fetcher, code)
                if quote:
                    quote["stock_name"] = quote.get("stock_name") or stock_name or code
                    self._record_provider_stat(stats, provider_label, "ok")
                    self._record_runtime_provider_result(ctx, "quote", fetcher.name, "ok")
                    self._mark_source(ctx, fetcher.name, "ok")
                    quote_cache[code] = quote
                    return quote
                self._record_provider_stat(stats, provider_label, "empty")
                self._record_runtime_provider_result(ctx, "quote", fetcher.name, "empty")
            except Exception as exc:  # noqa: BLE001
                _, reason = summarize_exception(exc)
                self._record_provider_stat(stats, fetcher.name, "failed")
                self._record_runtime_provider_result(ctx, "quote", fetcher.name, "failed")
                self._append_warning(ctx, f"实时行情获取失败（{fetcher.name} {code}）：{reason}")
                self._mark_source(ctx, fetcher.name, "failed")
                if fetcher.name == "TushareFetcher" and any(keyword in reason.lower() for keyword in ("权限", "积分", "forbidden", "permission")):
                    logger.warning("Tushare 权限不足，daily picks 已自动跳过实时行情补充: %s", reason)
            finally:
                _ = start
        quote_cache[code] = None
        return None

    def _get_boards(
        self,
        stock_code: str,
        ctx: Dict[str, Any],
        board_cache: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        code = normalize_stock_code(stock_code)
        if code in board_cache:
            return board_cache[code]

        stats = ctx["source_summary"]["board_stats"]
        stats["attempted"] += 1
        for fetcher in self._iter_fetchers_for("board"):
            if self._is_runtime_provider_disabled(ctx, "board", fetcher.name):
                continue
            try:
                raw = fetcher.get_belong_board(code)
                boards = self.manager._normalize_belong_boards(raw)  # type: ignore[attr-defined]
                if boards:
                    self._record_provider_stat(stats, fetcher.name, "ok")
                    self._record_runtime_provider_result(ctx, "board", fetcher.name, "ok")
                    self._mark_source(ctx, fetcher.name, "ok")
                    board_cache[code] = boards
                    return boards
                self._record_provider_stat(stats, fetcher.name, "empty")
                self._record_runtime_provider_result(ctx, "board", fetcher.name, "empty")
            except Exception as exc:  # noqa: BLE001
                _, reason = summarize_exception(exc)
                self._record_provider_stat(stats, fetcher.name, "failed")
                self._record_runtime_provider_result(ctx, "board", fetcher.name, "failed")
                self._append_warning(ctx, f"所属板块获取失败（{fetcher.name} {code}）：{reason}")
                self._mark_source(ctx, fetcher.name, "failed")
        board_cache[code] = []
        return []

    def get_market_news(self, max_per_query: int, ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        seen = set()

        for query in self.DEFAULT_NEWS_THEMES:
            start = time.time()
            try:
                response = self.search_service.search_stock_news(
                    stock_code="market",
                    stock_name="A股市场",
                    max_results=max_per_query,
                    focus_keywords=query.split(),
                    days_override=7,
                )
                duration_ms = int((time.time() - start) * 1000)
                ctx["source_summary"]["news"].append(
                    {
                        "query": query,
                        "provider": response.provider,
                        "result": "ok" if response.results else ("empty" if response.success else "failed"),
                        "duration_ms": duration_ms,
                        "count": len(response.results or []),
                        "error": response.error_message,
                    }
                )
                self._mark_source(ctx, response.provider, "ok" if response.results else "failed")
                if not response.results:
                    continue

                for result in response.results:
                    url = getattr(result, "url", "") or ""
                    if not url or url in seen:
                        continue
                    seen.add(url)
                    items.append(
                        {
                            "query": query,
                            "provider": response.provider,
                            "title": getattr(result, "title", ""),
                            "snippet": getattr(result, "snippet", ""),
                            "url": url,
                            "source": getattr(result, "source", ""),
                            "published_date": getattr(result, "published_date", None),
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                _, reason = summarize_exception(exc)
                ctx["source_summary"]["news"].append(
                    {
                        "query": query,
                        "provider": "unknown",
                        "result": "failed",
                        "duration_ms": int((time.time() - start) * 1000),
                        "error": reason,
                    }
                )
                self._append_warning(ctx, f"市场新闻搜索失败（{query}）：{reason}")

        items = [item for item in items if self._is_recent_hotspot_news(item)]
        return items[: max_per_query * len(self.DEFAULT_NEWS_THEMES)]

    def get_sector_rankings(self, top_n: int, ctx: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        for fetcher in self._iter_fetchers_for("sector"):
            start = time.time()
            try:
                data = fetcher.get_sector_rankings(top_n)
                duration_ms = int((time.time() - start) * 1000)
                if data and data[0] is not None and data[1] is not None:
                    top, bottom = data
                    ctx["source_summary"]["sector_rankings"].append(
                        {
                            "provider": fetcher.name,
                            "result": "ok",
                            "duration_ms": duration_ms,
                            "top_count": len(top or []),
                            "bottom_count": len(bottom or []),
                        }
                    )
                    self._mark_source(ctx, fetcher.name, "ok")
                    return {"top": top or [], "bottom": bottom or []}

                ctx["source_summary"]["sector_rankings"].append(
                    {
                        "provider": fetcher.name,
                        "result": "empty",
                        "duration_ms": duration_ms,
                    }
                )
            except Exception as exc:  # noqa: BLE001
                _, reason = summarize_exception(exc)
                ctx["source_summary"]["sector_rankings"].append(
                    {
                        "provider": fetcher.name,
                        "result": "failed",
                        "duration_ms": int((time.time() - start) * 1000),
                        "error": reason,
                    }
                )
                self._mark_source(ctx, fetcher.name, "failed")
                if fetcher.name == "TushareFetcher" and any(keyword in reason.lower() for keyword in ("权限", "积分", "forbidden", "permission")):
                    logger.warning("Tushare 板块排行接口权限不足，daily picks 已自动降级: %s", reason)
                else:
                    self._append_warning(ctx, f"板块排行获取失败（{fetcher.name}）：{reason}")
        return {"top": [], "bottom": []}

    def _extract_themes(
        self,
        market_news: List[Dict[str, Any]],
        top_sectors: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        theme_stats: Dict[str, Dict[str, Any]] = {}

        def ensure_theme(name: str) -> Dict[str, Any]:
            return theme_stats.setdefault(name, {"name": name, "hits": 0, "news_refs": []})

        for sector in top_sectors[:10]:
            name = str(sector.get("name") or "").strip()
            if name:
                ensure_theme(name)

        for name in self.THEME_ALIASES:
            ensure_theme(name)

        for news in market_news:
            text = f"{news.get('title') or ''} {news.get('snippet') or ''}"
            for theme_name, payload in list(theme_stats.items()):
                if theme_name and theme_name in text:
                    payload["hits"] += 1
                    self._append_unique(payload["news_refs"], news.get("title") or "")
            for theme_name, aliases in self.THEME_ALIASES.items():
                if any(alias and alias in text for alias in aliases):
                    payload = ensure_theme(theme_name)
                    payload["hits"] += 1
                    self._append_unique(payload["news_refs"], news.get("title") or "")

        ranked = [item for item in theme_stats.values() if item["hits"] > 0 or item["name"]]
        ranked.sort(key=lambda item: (item["hits"], item["name"]), reverse=True)
        return ranked[:12]

    @staticmethod
    def _base_quote_score(quote: Dict[str, Any]) -> float:
        change_pct = float(quote.get("change_percent") or 0)
        amount = float(quote.get("amount") or 0)
        turnover_rate = float(quote.get("turnover_rate") or 0)
        volume_ratio = float(quote.get("volume_ratio") or 0)
        amplitude = float(quote.get("amplitude") or 0)

        score = 38.0
        score += min(max(change_pct, -3.0), 8.5) * 3.4
        if amount > 0:
            score += min(math.log10(amount + 1), 11.0) * 4.2
        score += min(max(turnover_rate, 0.0), 15.0) * 1.2
        score += min(max(volume_ratio, 0.0), 3.0) * 2.5

        if change_pct >= 9.5:
            score -= 15
        elif change_pct <= -2:
            score -= 8
        if amplitude >= 12:
            score -= 6
        if amount and amount < 5e7:
            score -= 4
        return round(score, 2)

    # ------------------------------------------------------------------
    # Dimensional scoring (0-100 each)
    # ------------------------------------------------------------------

    @classmethod
    def _compute_news_policy_score(
        cls,
        matched_news: List[Dict[str, Any]],
        matched_theme_names: List[str],
    ) -> float:
        """计算新闻政策维度得分。

        新闻窗口策略（最终设计）：
        - 搜索窗口 = ``NEWS_FRESHNESS_DAYS`` (7 天)
        - 主窗口 ≤ ``NEWS_PRIORITY_DAYS`` (3 天)：高权重，freshness_bonus 最高 25
        - 弱辅助窗口 4-7 天：仅弱补充，得分上限 15，无 freshness_bonus
        - > ``NEWS_HARD_MAX_DAYS`` (7 天)：完全忽略，不计入任何维度
        """
        if not matched_news and not matched_theme_names:
            return 0.0

        now = datetime.now()
        primary_count = 0  # 0-3 天主窗口新闻数
        weak_count = 0     # 4-7 天弱辅助新闻数
        has_policy = False
        freshness_bonus = 0.0

        for news in matched_news:
            pub = cls._parse_news_date(news.get("published_date"))
            if pub is not None:
                age_days = (now - pub).total_seconds() / 86400
                if age_days > cls.NEWS_HARD_MAX_DAYS:
                    continue  # 超过硬上限完全忽略
            text = f"{news.get('title') or ''} {news.get('snippet') or ''}"
            if any(kw in text for kw in cls.POLICY_KEYWORDS):
                has_policy = True
            if pub is not None:
                if age_days <= 1:
                    freshness_bonus = max(freshness_bonus, 25.0)
                    primary_count += 1
                elif age_days <= cls.NEWS_PRIORITY_DAYS:
                    freshness_bonus = max(freshness_bonus, 15.0)
                    primary_count += 1
                else:
                    # 4-7 天：仅做弱辅助，不增加 freshness_bonus
                    weak_count += 1
            else:
                freshness_bonus = max(freshness_bonus, 3.0)
                weak_count += 1  # 无日期视为弱辅助

        # 数量得分仅基于主窗口新闻
        score = 0.0
        if primary_count >= 3:
            score += 55
        elif primary_count == 2:
            score += 40
        elif primary_count == 1:
            score += 25
        elif weak_count > 0:
            # 仅有弱辅助新闻时给予极低基础分，不能单独支撑强推荐
            score += min(weak_count, 3) * 5  # 最多 15

        score += freshness_bonus
        if has_policy:
            score += 20
        return min(round(score, 2), 100.0)

    @staticmethod
    def _compute_catalyst_score(
        matched_theme_names: List[str],
        themes: List[Dict[str, Any]],
    ) -> float:
        if not matched_theme_names:
            return 0.0
        theme_lookup = {str(t.get("name") or "").strip(): t for t in themes}
        score = 0.0
        total_hits = 0
        for name in matched_theme_names:
            info = theme_lookup.get(name)
            if info:
                total_hits += info.get("hits", 0)
        if total_hits >= 4:
            score = 70
        elif total_hits >= 3:
            score = 55
        elif total_hits >= 2:
            score = 40
        elif total_hits >= 1:
            score = 25
        score += min(len(matched_theme_names), 3) * 10
        return min(round(score, 2), 100.0)

    @staticmethod
    def _compute_sector_heat_score(
        sector_name: Optional[str],
        sector_change_pct: Any,
        top_sectors: List[Dict[str, Any]],
    ) -> float:
        if not sector_name:
            return 0.0
        score = 30.0
        if sector_change_pct is not None:
            try:
                pct = float(sector_change_pct)
                score += min(max(pct, 0.0), 8.0) * 5
            except (TypeError, ValueError):
                pass
        for idx, sector in enumerate(top_sectors):
            if str(sector.get("name") or "").strip() == sector_name:
                if idx < 3:
                    score += 20
                elif idx < 5:
                    score += 10
                elif idx < 8:
                    score += 5
                break
        return min(round(score, 2), 100.0)

    @staticmethod
    def _compute_stock_heat_score(quote: Dict[str, Any]) -> float:
        turnover = float(quote.get("turnover_rate") or 0)
        volume_ratio = float(quote.get("volume_ratio") or 0)
        amount = float(quote.get("amount") or 0)
        score = 20.0
        score += min(turnover, 20.0) * 2
        if volume_ratio >= 2.0:
            score += 15
        elif volume_ratio >= 1.5:
            score += 10
        elif volume_ratio >= 1.2:
            score += 5
        if amount >= 2e9:
            score += 15
        elif amount >= 1e9:
            score += 10
        elif amount >= 5e8:
            score += 5
        return min(round(score, 2), 100.0)

    @staticmethod
    def _compute_technical_score(quote: Dict[str, Any]) -> float:
        change_pct = float(quote.get("change_percent") or 0)
        volume_ratio = float(quote.get("volume_ratio") or 0)
        amplitude = float(quote.get("amplitude") or 0)
        amount = float(quote.get("amount") or 0)

        score = 30.0
        if 2.0 <= change_pct <= 7.0:
            score += 30
        elif 0.5 <= change_pct < 2.0:
            score += 20
        elif 7.0 < change_pct < 9.5:
            score += 10
        elif change_pct < 0:
            score -= 10

        if 1.2 <= volume_ratio <= 3.0:
            score += 15
        elif volume_ratio > 3.0:
            score += 5

        if amplitude < 8:
            score += 10
        elif amplitude >= 12:
            score -= 10

        if amount >= 3e8:
            score += 5

        if change_pct >= 9.5:
            score -= 20
        return min(max(round(score, 2), 0.0), 100.0)

    @staticmethod
    def _compute_capital_flow_score(quote: Dict[str, Any]) -> float:
        amount = float(quote.get("amount") or 0)
        turnover = float(quote.get("turnover_rate") or 0)
        score = 20.0
        if amount > 0:
            score += min(math.log10(amount + 1), 11.0) * 5
        if 3.0 <= turnover <= 15.0:
            score += 20
        elif turnover > 15.0:
            score += 10
        return min(round(score, 2), 100.0)

    @staticmethod
    def _compute_fundamental_score(stock_name: str) -> float:
        """无真实基本面数据接入时返回 0。

        当前尚未对接 PE/PB/ROE 等结构化基本面数据源，因此所有标的
        基本面维度一律返回 0，不伪装为真实评分。ST 标识仅用于
        risk_penalty，不在此处重复体现。
        """
        return 0.0

    @classmethod
    def _compute_risk_penalty(cls, candidate: Dict[str, Any]) -> float:
        quote = candidate.get("quote") or {}
        stock_name = str(candidate.get("stock_name") or "")
        change_pct = float(quote.get("change_percent") or 0)
        amplitude = float(quote.get("amplitude") or 0)
        amount = float(quote.get("amount") or 0)
        turnover = float(quote.get("turnover_rate") or 0)

        penalty = 0.0
        if "ST" in stock_name.upper() or "*ST" in stock_name.upper():
            return float(cls.MAX_RISK_PENALTY)
        if change_pct >= 9.5:
            penalty += 15
        elif change_pct >= 8.0:
            penalty += 8
        if amplitude >= 14:
            penalty += 5
        elif amplitude >= 12:
            penalty += 3
        if amount > 0 and amount < 5e7:
            penalty += 5
        if turnover >= 25:
            penalty += 3
        return min(round(penalty, 2), float(cls.MAX_RISK_PENALTY))

    def _compute_score_breakdown(
        self,
        candidate: Dict[str, Any],
        market_news: List[Dict[str, Any]],
        themes: List[Dict[str, Any]],
        top_sectors: List[Dict[str, Any]],
    ) -> Dict[str, float]:
        quote = candidate.get("quote") or {}
        matched_news = candidate.get("matched_news") or []
        matched_theme_names = candidate.get("matched_theme_names") or []
        sector_name = candidate.get("sector_name")
        sector_change_pct = candidate.get("sector_change_pct")
        stock_name = str(candidate.get("stock_name") or "")

        return {
            "news_policy": self._compute_news_policy_score(matched_news, matched_theme_names),
            "catalyst": self._compute_catalyst_score(matched_theme_names, themes),
            "sector_heat": self._compute_sector_heat_score(sector_name, sector_change_pct, top_sectors),
            "stock_heat": self._compute_stock_heat_score(quote),
            "technical": self._compute_technical_score(quote),
            "capital_flow": self._compute_capital_flow_score(quote),
            "fundamental": self._compute_fundamental_score(stock_name),
            "fundamental_source": "not_available",
            "risk_penalty": self._compute_risk_penalty(candidate),
        }

    def _compute_total_score(self, breakdown: Dict[str, float]) -> float:
        total = 0.0
        for key, weight in self.SCORING_WEIGHTS.items():
            total += breakdown.get(key, 0.0) * weight / 100.0
        total -= breakdown.get("risk_penalty", 0.0)
        return round(total, 2)

    # ------------------------------------------------------------------
    # Hard filters
    # ------------------------------------------------------------------

    @classmethod
    def _apply_hard_filters(cls, candidates: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, str]]]:
        filtered: List[Dict[str, Any]] = []
        removed: List[Dict[str, str]] = []
        for c in candidates:
            name = str(c.get("stock_name") or "")
            quote = c.get("quote") or {}
            change_pct = float(quote.get("change_percent") or 0)
            amount = float(quote.get("amount") or 0)
            open_price = quote.get("open")
            high = quote.get("high")
            low = quote.get("low")

            reason = None
            if "ST" in name.upper() or "*ST" in name.upper():
                reason = "ST/*ST 剔除"
            elif change_pct >= 9.9 and open_price and high and low:
                try:
                    if abs(float(high) - float(low)) < 0.02:
                        reason = "一字板，无法参与"
                except (TypeError, ValueError):
                    pass
                # 非一字板涨停 → 不硬过滤，仅打标（risk_penalty 已覆盖）
            elif amount > 0 and amount < cls.HARD_FILTER_MIN_AMOUNT:
                reason = f"成交额过低（<{cls.HARD_FILTER_MIN_AMOUNT / 1e4:.0f}万），流动性严重不足"

            if reason:
                removed.append({"stock_code": c.get("stock_code", ""), "reason": reason})
            else:
                filtered.append(c)
        return filtered, removed

    # ------------------------------------------------------------------
    # Diversity constraint
    # ------------------------------------------------------------------

    @classmethod
    def _normalize_theme(cls, name: str) -> str:
        """将主题名称归一化到 THEME_ALIASES 中的规范名。

        主题识别链路：
        1. ``_extract_themes`` 从新闻+板块排行收集候选主题，通过 THEME_ALIASES
           将别名（如"AI""算力""大模型"）映射回规范名（如"人工智能"）。
        2. ``_match_sector`` 用个股概念板块匹配 top_sectors / themes，返回的
           matched_theme_names 可能是板块排行原名（如"AI应用"）而非规范名。
        3. 本方法用于在多样性去重时再做一次归一化，避免同语义不同名称绕过约束。
        当 name 不命中任何别名时原样返回——此时该名称自身就是去重 key。
        """
        stripped = name.strip()
        for canonical, aliases in cls.THEME_ALIASES.items():
            if stripped == canonical:
                return canonical
            if any(alias and alias in stripped for alias in aliases):
                return canonical
        return stripped

    @classmethod
    def _diversify_candidates(cls, candidates: List[Dict[str, Any]], top_k: int) -> List[Dict[str, Any]]:
        """按板块和主题做多样性约束。

        - sector_name 直接用于板块计数（同名 ≤ SAME_SECTOR_MAX）。
        - primary_theme 通过 ``_normalize_theme`` 归一化后用于主题计数
          （同规范主题 ≤ SAME_THEME_MAX），防止同语义不同名称绕过约束。
        - 被约束跳过的候选进入 deferred 队列；若主队列不足 top_k 则从
          deferred 按原顺序回填，保证输出数量尽可能满足。
        """
        result: List[Dict[str, Any]] = []
        sector_count: Dict[str, int] = {}
        theme_count: Dict[str, int] = {}
        deferred: List[Dict[str, Any]] = []

        for c in candidates:
            sector = str(c.get("sector_name") or "").strip()
            themes = c.get("matched_theme_names") or []
            primary_theme = cls._normalize_theme(themes[0]) if themes else ""

            sector_ok = not sector or sector_count.get(sector, 0) < cls.SAME_SECTOR_MAX
            theme_ok = not primary_theme or theme_count.get(primary_theme, 0) < cls.SAME_THEME_MAX

            if sector_ok and theme_ok:
                result.append(c)
                if sector:
                    sector_count[sector] = sector_count.get(sector, 0) + 1
                if primary_theme:
                    theme_count[primary_theme] = theme_count.get(primary_theme, 0) + 1
                if len(result) >= top_k:
                    break
            else:
                deferred.append(c)

        if len(result) < top_k:
            for c in deferred:
                result.append(c)
                if len(result) >= top_k:
                    break
        return result

    # ------------------------------------------------------------------
    # Entry hint & tags
    # ------------------------------------------------------------------

    @staticmethod
    def _build_entry_hint(candidate: Dict[str, Any]) -> str:
        quote = candidate.get("quote") or {}
        change_pct = float(quote.get("change_percent") or 0)
        volume_ratio = float(quote.get("volume_ratio") or 0)
        amplitude = float(quote.get("amplitude") or 0)

        if change_pct >= 7.0:
            return "当前涨幅较大，建议观望等回踩确认，不宜追高"
        if change_pct >= 4.0 and volume_ratio >= 1.5:
            return "放量上攻中，可考虑轻仓追突破，设好止损"
        if 1.0 <= change_pct < 4.0 and amplitude < 6:
            return "温和放量，适合分批建仓，等待进一步确认"
        if change_pct < 1.0 and volume_ratio >= 1.2:
            return "缩量回踩或低位蓄势，适合等回踩支撑位介入"
        return "建议先观察量价配合情况，不急于介入"

    @staticmethod
    def _build_stop_loss_hint(candidate: Dict[str, Any]) -> str:
        quote = candidate.get("quote") or {}
        current_price = quote.get("current_price")
        prev_close = quote.get("prev_close")
        low = quote.get("low")
        if current_price and prev_close:
            try:
                stop = round(float(prev_close) * 0.95, 2)
                return f"建议止损参考：前收价95%附近（约{stop}），或跌破当日低点{low or '--'}"
            except (TypeError, ValueError):
                pass
        return "建议根据个股支撑位和自身风险偏好设定止损"

    @staticmethod
    def _build_reason_tags(candidate: Dict[str, Any]) -> List[str]:
        tags: List[str] = []
        if candidate.get("matched_news"):
            tags.append("新闻驱动")
        if candidate.get("matched_theme_names"):
            tags.append("热点主题")
        if candidate.get("sector_name"):
            tags.append("强势板块")
        quote = candidate.get("quote") or {}
        change_pct = float(quote.get("change_percent") or 0)
        volume_ratio = float(quote.get("volume_ratio") or 0)
        if 2.0 <= change_pct <= 7.0 and volume_ratio >= 1.3:
            tags.append("量价配合")
        amount = float(quote.get("amount") or 0)
        if amount >= 1e9:
            tags.append("资金活跃")
        return tags

    @staticmethod
    def _build_risk_tags(candidate: Dict[str, Any]) -> List[str]:
        tags: List[str] = []
        quote = candidate.get("quote") or {}
        change_pct = float(quote.get("change_percent") or 0)
        amplitude = float(quote.get("amplitude") or 0)
        turnover = float(quote.get("turnover_rate") or 0)
        amount = float(quote.get("amount") or 0)
        if change_pct >= 8.0:
            tags.append("追高风险")
        if amplitude >= 12:
            tags.append("高振幅")
        if turnover >= 20:
            tags.append("高换手")
        if amount > 0 and amount < 1e8:
            tags.append("流动性偏弱")
        if not candidate.get("matched_news"):
            tags.append("新闻支撑弱")
        return tags

    def _match_sector(
        self,
        boards: List[Dict[str, Any]],
        top_sectors: List[Dict[str, Any]],
        themes: List[Dict[str, Any]],
    ) -> Tuple[Optional[Dict[str, Any]], List[str]]:
        board_names = [
            str(board.get("name") or board.get("board_name") or board.get("板块名称") or "").strip()
            for board in boards
        ]
        board_names = [name for name in board_names if name]
        matched_keywords: List[str] = []

        for sector in top_sectors:
            sector_name = str(sector.get("name") or "").strip()
            if not sector_name:
                continue
            if any(sector_name in board_name or board_name in sector_name for board_name in board_names):
                matched_keywords.append(sector_name)
                return sector, matched_keywords

        for theme in themes:
            theme_name = str(theme.get("name") or "").strip()
            if not theme_name:
                continue
            if any(theme_name in board_name or board_name in theme_name for board_name in board_names):
                matched_keywords.append(theme_name)
                return {"name": theme_name, "change_pct": None}, matched_keywords
        return None, matched_keywords

    def _build_market_candidates(
        self,
        stock_pool: List[Dict[str, Any]],
        top_sectors: List[Dict[str, Any]],
        themes: List[Dict[str, Any]],
        market_news: List[Dict[str, Any]],
        ctx: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        quote_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        board_cache: Dict[str, List[Dict[str, Any]]] = {}
        candidates: List[Dict[str, Any]] = []

        self._warm_quote_cache([item["stock_code"] for item in stock_pool[:8]], ctx)
        scan_pool = stock_pool[: self.QUOTE_SCAN_LIMIT]

        for item in scan_pool:
            quote = self._get_quote(item["stock_code"], item.get("stock_name", ""), ctx, quote_cache)
            if not quote or not quote.get("current_price"):
                continue

            candidates.append(
                {
                    "stock_code": item["stock_code"],
                    "stock_name": quote.get("stock_name") or item.get("stock_name") or item["stock_code"],
                    "quote": quote,
                    "score": self._base_quote_score(quote),
                    "sector_name": None,
                    "sector_change_pct": None,
                    "boards": [],
                    "matched_news": [],
                    "matched_theme_names": [],
                    "score_breakdown": {},
                }
            )

        candidates.sort(key=lambda item: item.get("score", 0), reverse=True)
        enrich_targets = candidates[: self.ENRICH_LIMIT]
        theme_lookup = {str(theme.get("name") or "").strip(): theme for theme in themes}

        for candidate in enrich_targets:
            boards = self._get_boards(candidate["stock_code"], ctx, board_cache)
            candidate["boards"] = boards
            matched_sector, matched_keywords = self._match_sector(boards, top_sectors, themes)
            candidate["matched_theme_names"] = matched_keywords
            if matched_sector:
                candidate["sector_name"] = matched_sector.get("name")
                candidate["sector_change_pct"] = matched_sector.get("change_pct")

            matched_news = []
            for news in market_news:
                text = f"{news.get('title') or ''} {news.get('snippet') or ''}"
                for keyword in matched_keywords or []:
                    if keyword and keyword in text:
                        matched_news.append(news)
                        break
                if not matched_keywords and candidate["sector_name"] and str(candidate["sector_name"]) in text:
                    matched_news.append(news)
                if len(matched_news) >= 3:
                    break
            candidate["matched_news"] = matched_news[:3]

            breakdown = self._compute_score_breakdown(candidate, market_news, themes, top_sectors)
            candidate["score_breakdown"] = breakdown
            candidate["score"] = self._compute_total_score(breakdown)

        logger.info(
            "候选池构建完成：扫描 %d / enriched %d / 有效 %d",
            len(scan_pool),
            len(enrich_targets),
            sum(1 for c in enrich_targets if c.get("score_breakdown")),
        )
        candidates.sort(key=lambda item: item.get("score", 0), reverse=True)
        return candidates

    def _build_stock_pool_fallback(self, stock_pool: List[Dict[str, Any]], count: int) -> List[Dict[str, Any]]:
        fallback_candidates: List[Dict[str, Any]] = []
        for item in stock_pool[:count]:
            fallback_candidates.append(
                {
                    "stock_code": item["stock_code"],
                    "stock_name": item.get("stock_name") or item["stock_code"],
                    "quote": {},
                    "score": 42.0,
                    "sector_name": None,
                    "sector_change_pct": None,
                    "boards": [],
                    "matched_news": [],
                    "matched_theme_names": [],
                    "score_breakdown": {},
                }
            )
        return fallback_candidates

    @staticmethod
    def _candidate_confidence(candidate: Dict[str, Any], degraded: bool) -> str:
        has_quote = bool(candidate.get("quote"))
        has_sector = bool(candidate.get("sector_name"))
        has_news = bool(candidate.get("matched_news"))
        if has_quote and has_sector and has_news and not degraded:
            return "high"
        if has_quote and (has_sector or has_news):
            return "medium"
        if has_quote:
            return "medium" if not degraded else "low"
        return "low"

    @staticmethod
    def _candidate_risk_note(candidate: Dict[str, Any], degraded: bool) -> str:
        quote = candidate.get("quote") or {}
        change_pct = float(quote.get("change_percent") or 0)
        amount = float(quote.get("amount") or 0)
        turnover_rate = float(quote.get("turnover_rate") or 0)
        amplitude = float(quote.get("amplitude") or 0)

        notes: List[str] = []
        if change_pct >= 8.5:
            notes.append("短线涨幅较大，需防范追高回撤。")
        if amount and amount < 5e7:
            notes.append("成交额偏低，流动性风险需留意。")
        if turnover_rate >= 20:
            notes.append("换手率偏高，波动可能放大。")
        if amplitude >= 12:
            notes.append("振幅较大，盘中回撤风险上升。")
        if degraded:
            notes.append("本次结果包含降级链路，可信度低于正常状态。")
        if not notes:
            notes.append("注意结合公告、量价承接与次日一致性继续复核。")
        return " ".join(notes)

    @staticmethod
    def _build_signal_breakdown(candidate: Dict[str, Any]) -> Dict[str, str]:
        quote = candidate.get("quote") or {}
        change_pct = quote.get("change_percent")
        volume_ratio = quote.get("volume_ratio")
        amount = quote.get("amount")
        turnover_rate = quote.get("turnover_rate")
        sector_name = candidate.get("sector_name")
        sector_change_pct = candidate.get("sector_change_pct")
        matched_news = candidate.get("matched_news") or []
        matched_theme_names = candidate.get("matched_theme_names") or []

        technical_parts: List[str] = []
        if change_pct is not None:
            technical_parts.append(f"涨跌幅约 {change_pct}%")
        if volume_ratio is not None:
            technical_parts.append(f"量比约 {volume_ratio}")
        if not technical_parts:
            technical_parts.append("技术面信息有限，主要依据实时行情强弱排序")

        sentiment_parts: List[str] = []
        if matched_theme_names:
            sentiment_parts.append(f"命中热点主题：{'、'.join(matched_theme_names[:3])}")
        if matched_news:
            sentiment_parts.append(f"关联新闻 {len(matched_news)} 条")
        if not sentiment_parts:
            sentiment_parts.append("情绪面以热点主题和新闻共振为主，当前直接匹配较少")

        capital_parts: List[str] = []
        if amount:
            try:
                capital_parts.append(f"成交额约 {round(float(amount) / 1e8, 2)} 亿")
            except (TypeError, ValueError):
                pass
        if turnover_rate is not None:
            capital_parts.append(f"换手率约 {turnover_rate}%")
        if not capital_parts:
            capital_parts.append("资金面信息有限，默认按成交与换手活跃度排序")

        sector_parts: List[str] = []
        if sector_name:
            if sector_change_pct is not None:
                sector_parts.append(f"所属板块“{sector_name}”涨幅约 {sector_change_pct}%")
            else:
                sector_parts.append(f"所属方向与“{sector_name}”热点主题相关")
        elif matched_theme_names:
            sector_parts.append(f"当前主要映射到主题“{matched_theme_names[0]}”")
        else:
            sector_parts.append("当前主要依赖个股量价和流动性信号")

        return {
            "technical": "，".join(technical_parts),
            "sentiment": "，".join(sentiment_parts),
            "capital": "，".join(capital_parts),
            "sector": "，".join(sector_parts),
        }

    @staticmethod
    def _build_news_connection(candidate: Dict[str, Any]) -> str:
        matched_news = candidate.get("matched_news") or []
        sector_name = candidate.get("sector_name")
        theme_names = candidate.get("matched_theme_names") or []
        stock_name = candidate.get("stock_name") or candidate.get("stock_code") or "该标的"
        if matched_news:
            news = matched_news[0]
            news_title = str(news.get("title") or "相关新闻").strip()
            if sector_name:
                return f"新闻“{news_title}”与{stock_name}所属的“{sector_name}”方向直接相关。"
            if theme_names:
                return f"新闻“{news_title}”直接指向主题“{theme_names[0]}”，与{stock_name}存在题材联动。"
            return f"新闻“{news_title}”与{stock_name}的当前热点路径存在直接关联。"
        if sector_name:
            return f"{stock_name}所属板块“{sector_name}”与当日热点方向一致。"
        if theme_names:
            return f"{stock_name}命中了热点主题“{theme_names[0]}”，但直接新闻映射较弱。"
        return f"{stock_name}主要基于技术面、资金面与流动性强度进入推荐池。"

    @staticmethod
    def _extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
        raw = (text or "").strip()
        if not raw:
            return None
        if raw.startswith("```"):
            raw = raw.strip("`")
            if "\n" in raw:
                raw = raw.split("\n", 1)[1]
            if raw.endswith("```"):
                raw = raw[:-3]
            raw = raw.strip()
        start = raw.find("{")
        end = raw.rfind("}")
        if start >= 0 and end > start:
            raw = raw[start : end + 1]
        try:
            return json.loads(raw)
        except Exception:
            if repair_json is not None:
                try:
                    repaired = repair_json(raw)
                    return json.loads(repaired)
                except Exception:
                    return None
        return None

    def _build_ai_prompt(
        self,
        recommendations: List[Dict[str, Any]],
        market_news: List[Dict[str, Any]],
        top_sectors: List[Dict[str, Any]],
        *,
        top_k: int,
        degraded: bool,
    ) -> str:
        candidate_payload = []
        for item in recommendations[: min(len(recommendations), max(top_k * 3, 8))]:
            candidate_payload.append(
                {
                    "stock_code": item.get("stock_code"),
                    "stock_name": item.get("stock_name"),
                    "sector_name": item.get("sector_name"),
                    "sector_change_pct": item.get("sector_change_pct"),
                    "score": item.get("score"),
                    "signal_breakdown": item.get("signal_breakdown"),
                    "news_connection": item.get("news_connection"),
                    "related_news": [
                        {
                            "title": news.get("title"),
                            "snippet": news.get("snippet"),
                            "source": news.get("source"),
                            "published_date": news.get("published_date"),
                        }
                        for news in (item.get("related_news") or [])[:3]
                    ],
                    "quote": item.get("quote") or {},
                }
            )

        prompt_payload = {
            "task": (
                "以下候选股已由程序按多维评分排序。你的任务是：为每只候选股补充结构化的推荐理由、操作建议、风险提示和新闻关联解释。"
                "不要改变候选股的排序和选择——排序由程序评分决定，你只负责解释。"
                "只能从候选池中选股，不得虚构股票或数据。"
            ),
            "constraints": {
                "top_k": top_k,
                "degraded": degraded,
                "must_cover": ["新闻关联", "技术面", "情绪", "资金", "板块逻辑", "风险提示", "操作建议"],
                "output_json_only": True,
                "fresh_news_only": "只能使用近3天内的热点新闻，超过3天需明确标注时效",
                "do_not_reorder": "禁止重新排序，按给定候选顺序输出",
            },
            "market_news": [
                {
                    "query": news.get("query"),
                    "title": news.get("title"),
                    "snippet": news.get("snippet"),
                    "source": news.get("source"),
                    "published_date": news.get("published_date"),
                }
                for news in market_news[:8]
            ],
            "top_sectors": [
                {
                    "name": sector.get("name"),
                    "change_pct": sector.get("change_pct"),
                }
                for sector in top_sectors[:8]
            ],
            "candidates": candidate_payload,
            "response_schema": {
                "market_sentiment": "一句话描述当前热点/情绪",
                "summary": "一句话总结本次推荐逻辑",
                "picks": [
                    {
                        "stock_code": "候选股票代码",
                        "recommend_reason": "1-2句综合推荐理由",
                        "operation_advice": "1句操作建议",
                        "risk_warning": "1句风险提示",
                        "news_connection": "解释该股与热门新闻/热点主题的直接联系",
                        "signal_breakdown": {
                            "technical": "技术面理由",
                            "sentiment": "情绪面理由",
                            "capital": "资金面理由",
                            "sector": "板块/题材理由",
                        },
                        "related_news": [
                            {
                                "title": "新闻标题",
                                "relation_reason": "这条新闻为什么与该股相关",
                            }
                        ],
                    }
                ],
            },
        }
        return (
            "你是A股每日热点推荐的解释助手。候选股已由程序按多维评分排序完毕，你不需要重新排序。"
            "你的任务是：为每只候选股生成结构化的推荐理由、操作建议、风险提示和新闻关联解释。"
            "只能使用近3天内的热点新闻，且优先依据热点事件/题材解释个股入选逻辑。"
            "禁止输出 Markdown，禁止补充候选池外股票，禁止重新排序，只返回 JSON。\n"
            f"{json.dumps(prompt_payload, ensure_ascii=False)}"
        )

    def _merge_ai_related_news(
        self,
        base_news: List[Dict[str, Any]],
        ai_news: List[Dict[str, Any]],
        fallback_news: Optional[List[Dict[str, Any]]] = None,
    ) -> List[Dict[str, Any]]:
        def normalize_title(value: str) -> str:
            text = (value or "").strip()
            return (
                text.replace("，", ",")
                .replace("：", ":")
                .replace("（", "(")
                .replace("）", ")")
                .replace(" ", "")
                .lower()
            )

        merged: List[Dict[str, Any]] = []
        news_sources = [base_news]
        if fallback_news:
            news_sources.append(fallback_news)

        base_lookup: Dict[str, Dict[str, Any]] = {}
        normalized_lookup: Dict[str, Dict[str, Any]] = {}
        for news_items in news_sources:
            for item in news_items:
                title = str(item.get("title") or "").strip()
                if not title:
                    continue
                base_lookup.setdefault(title, dict(item))
                normalized_lookup.setdefault(normalize_title(title), dict(item))

        for ai_item in ai_news[:3]:
            title = str(ai_item.get("title") or "").strip()
            item = dict(base_lookup.get(title, {}))
            if not item and title:
                normalized_title = normalize_title(title)
                item = dict(normalized_lookup.get(normalized_title, {}))
                if not item:
                    for candidate_title, candidate_item in normalized_lookup.items():
                        if normalized_title and (
                            normalized_title in candidate_title or candidate_title in normalized_title
                        ):
                            item = dict(candidate_item)
                            break
            if not item:
                item = {
                    "title": title or "相关新闻",
                    "source": ai_item.get("source"),
                    "published_date": ai_item.get("published_date"),
                    "url": ai_item.get("url"),
                }
            if ai_item.get("relation_reason"):
                item["relation_reason"] = ai_item.get("relation_reason")
            merged.append(item)
        if merged:
            return merged
        return base_news

    def _apply_ai_reasoning(
        self,
        recommendations: List[Dict[str, Any]],
        market_news: List[Dict[str, Any]],
        top_sectors: List[Dict[str, Any]],
        *,
        degraded: bool,
        top_k: int,
        ctx: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        if not recommendations or GeminiAnalyzer is None:
            return recommendations

        start = time.time()
        summary_bucket = ctx["source_summary"].setdefault("ai_reasoning", [])
        try:
            analyzer = GeminiAnalyzer()
            if hasattr(analyzer, "is_available") and not analyzer.is_available():
                summary_bucket.append(
                    {
                        "provider": "DailyPicksAI",
                        "result": "empty",
                        "duration_ms": int((time.time() - start) * 1000),
                        "reason": "llm_unavailable",
                    }
                )
                return recommendations

            prompt = self._build_ai_prompt(
                recommendations,
                market_news,
                top_sectors,
                top_k=top_k,
                degraded=degraded,
            )
            response_text = analyzer.generate_text(prompt, max_tokens=2500)
            payload = self._extract_json_payload(response_text or "")
            if not payload or not isinstance(payload.get("picks"), list):
                summary_bucket.append(
                    {
                        "provider": "DailyPicksAI",
                        "result": "empty",
                        "duration_ms": int((time.time() - start) * 1000),
                        "reason": "invalid_json",
                    }
                )
                return recommendations

            rec_lookup = {
                normalize_stock_code(str(item.get("stock_code") or "")): item
                for item in recommendations
                if item.get("stock_code")
            }
            ai_pick_lookup: Dict[str, Dict[str, Any]] = {}
            for ai_item in payload.get("picks", []):
                code = normalize_stock_code(str(ai_item.get("stock_code") or ""))
                if code and code in rec_lookup:
                    ai_pick_lookup[code] = ai_item

            # Preserve programmatic ordering — LLM only enriches explanation fields
            enriched: List[Dict[str, Any]] = []
            for item in recommendations:
                code = normalize_stock_code(str(item.get("stock_code") or ""))
                merged = dict(item)
                ai_item = ai_pick_lookup.get(code)
                if ai_item:
                    if ai_item.get("recommend_reason"):
                        merged["recommend_reason"] = ai_item.get("recommend_reason")
                    if ai_item.get("operation_advice"):
                        merged["operation_advice"] = ai_item.get("operation_advice")
                    if ai_item.get("risk_warning"):
                        merged["risk_warning"] = ai_item.get("risk_warning")
                    if ai_item.get("news_connection"):
                        merged["news_connection"] = ai_item.get("news_connection")
                    if isinstance(ai_item.get("signal_breakdown"), dict):
                        merged["signal_breakdown"] = ai_item.get("signal_breakdown")
                    if isinstance(ai_item.get("related_news"), list):
                        merged["related_news"] = self._merge_ai_related_news(
                            list(merged.get("related_news") or []),
                            ai_item.get("related_news") or [],
                            market_news,
                        )
                    if ai_item.get("entry_hint"):
                        merged["entry_hint"] = ai_item.get("entry_hint")
                enriched.append(merged)

            if not ai_pick_lookup:
                summary_bucket.append(
                    {
                        "provider": "DailyPicksAI",
                        "result": "empty",
                        "duration_ms": int((time.time() - start) * 1000),
                        "reason": "no_matching_pick",
                    }
                )
                return recommendations

            summary_bucket.append(
                {
                    "provider": "DailyPicksAI",
                    "result": "ok",
                    "duration_ms": int((time.time() - start) * 1000),
                    "enriched_count": len(ai_pick_lookup),
                    "market_sentiment": payload.get("market_sentiment"),
                    "summary": payload.get("summary"),
                }
            )
            self._mark_source(ctx, "DailyPicksAI", "ok")
            result = enriched[:top_k]
            for idx, item in enumerate(result, start=1):
                item["final_rank"] = idx
            logger.info(
                "LLM 增强后排序审计：%s",
                ", ".join(
                    f"{r.get('stock_code','?')}(program={r.get('program_rank','?')},final={r.get('final_rank','?')})"
                    for r in result
                ),
            )
            return result
        except Exception as exc:  # noqa: BLE001
            _, reason = summarize_exception(exc)
            summary_bucket.append(
                {
                    "provider": "DailyPicksAI",
                    "result": "failed",
                    "duration_ms": int((time.time() - start) * 1000),
                    "error": reason,
                }
            )
            self._append_warning(ctx, f"AI 推荐增强失败：{reason}")
            self._mark_source(ctx, "DailyPicksAI", "failed")
            return recommendations

    def _build_recommendations(
        self,
        candidates: List[Dict[str, Any]],
        market_news: List[Dict[str, Any]],
        degraded: bool,
        top_k: int,
    ) -> List[Dict[str, Any]]:
        recommendations: List[Dict[str, Any]] = []
        seen = set()
        for candidate in candidates:
            code = candidate.get("stock_code")
            if not code or code in seen:
                continue
            seen.add(code)
            recommendations.append(candidate)
            if len(recommendations) >= top_k:
                break

        result: List[Dict[str, Any]] = []
        for idx, candidate in enumerate(recommendations, start=1):
            quote = candidate.get("quote") or {}
            sector_name = candidate.get("sector_name")
            signal_breakdown = self._build_signal_breakdown(candidate)
            news_connection = self._build_news_connection(candidate)
            reason_parts = []
            if sector_name:
                reason_parts.append(f"所属板块“{sector_name}”位于当日强势方向")
            elif candidate.get("matched_theme_names"):
                reason_parts.append(f"与热点主题“{candidate['matched_theme_names'][0]}”存在关联")

            if quote.get("change_percent") is not None:
                reason_parts.append(f"个股当日涨跌幅约 {quote.get('change_percent')}%")
            if quote.get("amount"):
                try:
                    reason_parts.append(f"成交额约 {round(float(quote['amount']) / 1e8, 2)} 亿")
                except (TypeError, ValueError):
                    pass
            if quote.get("turnover_rate") is not None:
                reason_parts.append(f"换手率约 {quote.get('turnover_rate')}%")
            if candidate.get("matched_news"):
                reason_parts.append("相关新闻/题材热度较高")
            elif market_news and sector_name:
                reason_parts.append("市场热点方向与板块强度存在共振")
            elif quote:
                reason_parts.append("量价与流动性表现优于降级池内多数标的")
            else:
                reason_parts.append("外部行情源波动时仍保留为真实股票兜底候选")

            result.append(
                {
                    "rank": idx,
                    "program_rank": idx,
                    "stock_code": candidate.get("stock_code"),
                    "stock_name": candidate.get("stock_name"),
                    "sector_name": sector_name,
                    "sector_change_pct": candidate.get("sector_change_pct"),
                    "score": round(float(candidate.get("score") or 0), 2),
                    "score_breakdown": candidate.get("score_breakdown") or {},
                    "reason_tags": self._build_reason_tags(candidate),
                    "risk_tags": self._build_risk_tags(candidate),
                    "entry_hint": self._build_entry_hint(candidate),
                    "stop_loss_hint": self._build_stop_loss_hint(candidate),
                    "recommend_reason": "；".join(reason_parts),
                    "operation_advice": self.DEFAULT_OPERATION_ADVICE,
                    "risk_warning": self.DEFAULT_RISK_WARNING,
                    "related_news": candidate.get("matched_news") or [],
                    "news_connection": news_connection,
                    "signal_breakdown": signal_breakdown,
                    "quote": quote,
                    "confidence": self._candidate_confidence(candidate, degraded),
                    "risk_note": self._candidate_risk_note(candidate, degraded),
                }
            )
        return result

    @staticmethod
    def _overall_confidence(
        *,
        degraded: bool,
        news_count: int,
        sector_count: int,
        output_count: int,
    ) -> str:
        if output_count == 0:
            return "low"
        if not degraded and news_count >= 3 and sector_count >= 3:
            return "high"
        if news_count or sector_count:
            return "medium"
        return "low"

    @staticmethod
    def build_failure_payload(
        error_message: str,
        *,
        source: str = "scheduled",
        started_at: Optional[datetime] = None,
        finished_at: Optional[datetime] = None,
    ) -> Dict[str, Any]:
        started = started_at or datetime.now()
        finished = finished_at or datetime.now()
        duration_ms = max(0, int((finished - started).total_seconds() * 1000))
        return {
            "generated_at": finished.isoformat(),
            "started_at": started.isoformat(),
            "finished_at": finished.isoformat(),
            "duration_ms": duration_ms,
            "strategy_version": "daily_picks_v3",
            "run_status": "failed",
            "degraded": True,
            "generation_layer": "failed",
            "generation_note": "生成失败，已记录失败状态供排障使用。",
            "market_news": [],
            "sector_rankings": {"top": [], "bottom": []},
            "candidate_count": 0,
            "output_count": 0,
            "recommendations": [],
            "confidence": "low",
            "risk_note": "本次任务未能产出推荐，请检查 scheduler/provider 日志。",
            "error_summary": [error_message],
            "source_summary": {
                "news": [],
                "sector_rankings": [],
                "stock_list": [],
                "quote_warmup": [],
                "quote_stats": {"attempted": 0, "succeeded": 0, "failed": 0, "providers": {}},
                "board_stats": {"attempted": 0, "succeeded": 0, "failed": 0, "providers": {}},
                "ai_reasoning": [],
            },
            "used_sources": [],
            "failed_sources": [],
            "source": source,
        }

    def generate_recommendations(self, top_k: int = 5) -> Dict[str, Any]:
        ctx = self._new_run_context()
        market_news = self.get_market_news(max_per_query=3, ctx=ctx)
        sector_rankings = self.get_sector_rankings(top_n=10, ctx=ctx)
        top_sectors = sector_rankings.get("top", [])
        themes = self._extract_themes(market_news, top_sectors)
        stock_pool = self._get_stock_pool(limit=self.STOCK_POOL_LIMIT, ctx=ctx)
        candidates = self._build_market_candidates(stock_pool, top_sectors, themes, market_news, ctx)

        generation_layer = "news_sector_stock"
        degraded_reasons: List[str] = []
        if not market_news:
            degraded_reasons.append("热点新闻源不可用，已回退到板块/量价驱动推荐。")
            generation_layer = "sector_stock"
        if not top_sectors:
            degraded_reasons.append("板块排行不可用，已回退到全市场简化评分。")
            generation_layer = "market_stock"

        # ---- Hard filter ----
        pre_filter_count = len(candidates)
        candidates, removed = self._apply_hard_filters(candidates)
        post_filter_count = len(candidates)
        if removed:
            logger.info(
                "硬过滤剔除 %d 只：%s",
                len(removed),
                "; ".join(f"{r['stock_code']}({r['reason']})" for r in removed[:10]),
            )

        if len(candidates) < top_k:
            degraded_reasons.append("有效行情候选不足，已启用真实股票池兜底。")
            generation_layer = "stock_pool_fallback"
            candidates.extend(self._build_stock_pool_fallback(stock_pool, count=max(top_k * 2, 10)))

        # ---- Diversity ----
        pre_diverse = candidates[: top_k * 3]
        diverse_pool = self._diversify_candidates(pre_diverse, top_k * 2)
        logger.info(
            "多样性处理：输入 %d → 输出 %d",
            len(pre_diverse),
            len(diverse_pool),
        )

        recommendations = self._build_recommendations(diverse_pool, market_news, bool(degraded_reasons), top_k)
        recommendations = self._apply_ai_reasoning(
            recommendations,
            market_news,
            top_sectors,
            degraded=bool(degraded_reasons),
            top_k=top_k,
            ctx=ctx,
        )
        if len(recommendations) < top_k and stock_pool:
            degraded_reasons.append("输出数量不足，已补充低可信度真实股票候选。")
            fallback_candidates = self._build_stock_pool_fallback(stock_pool, count=top_k)
            recommendations = self._build_recommendations(
                diverse_pool + fallback_candidates,
                market_news,
                True,
                top_k,
            )
            recommendations = self._apply_ai_reasoning(
                recommendations,
                market_news,
                top_sectors,
                degraded=True,
                top_k=top_k,
                ctx=ctx,
            )

        # ---- Ensure final_rank is always present (fallback = program_rank) ----
        for idx, r in enumerate(recommendations[:top_k], start=1):
            r.setdefault("final_rank", r.get("program_rank", idx))

        # ---- Final logging ----
        logger.info(
            "最终 Top%d：%s",
            top_k,
            ", ".join(
                f"{r.get('stock_name', '?')}({r.get('stock_code', '?')}) score={r.get('score', 0)}"
                for r in recommendations[:top_k]
            ),
        )
        for r in recommendations[:top_k]:
            bd = r.get("score_breakdown") or {}
            if bd:
                logger.info(
                    "  %s 分项：news=%s catalyst=%s sector=%s stock_heat=%s tech=%s capital=%s fund=%s risk=-%s",
                    r.get("stock_code", "?"),
                    bd.get("news_policy", 0),
                    bd.get("catalyst", 0),
                    bd.get("sector_heat", 0),
                    bd.get("stock_heat", 0),
                    bd.get("technical", 0),
                    bd.get("capital_flow", 0),
                    bd.get("fundamental", 0),
                    bd.get("risk_penalty", 0),
                )

        finished_at = datetime.now()
        duration_ms = int((finished_at - ctx["started_at"]).total_seconds() * 1000)
        degraded = bool(degraded_reasons or ctx["warnings"])
        output_count = len(recommendations)
        run_status = "success" if output_count > 0 and not degraded else ("degraded" if output_count > 0 else "failed")

        error_summary = degraded_reasons + ctx["warnings"][:8]
        if output_count == 0:
            error_summary.append("候选池为空，未能产出推荐。")

        return {
            "generated_at": finished_at.isoformat(),
            "started_at": ctx["started_at"].isoformat(),
            "finished_at": finished_at.isoformat(),
            "duration_ms": duration_ms,
            "strategy_version": "daily_picks_v3",
            "run_status": run_status,
            "degraded": degraded,
            "generation_layer": generation_layer,
            "generation_note": (
                "daily picks v3：多维评分+硬过滤+多样性约束，LLM仅做解释增强。"
            ),
            "market_news": market_news,
            "themes": themes,
            "sector_rankings": sector_rankings,
            "candidate_count": pre_filter_count,
            "filtered_count": pre_filter_count - post_filter_count,
            "output_count": output_count,
            "recommendations": recommendations,
            "confidence": self._overall_confidence(
                degraded=degraded,
                news_count=len(market_news),
                sector_count=len(top_sectors),
                output_count=output_count,
            ),
            "risk_note": (
                "本次结果包含降级链路，建议降低仓位与追涨意愿。"
                if degraded
                else "建议结合次日竞价强弱与行业持续性确认，不宜把规则结果视作唯一决策依据。"
            ),
            "error_summary": error_summary,
            "source_summary": self._finalize_source_summary(ctx["source_summary"]),
            "used_sources": sorted(ctx["used_sources"]),
            "failed_sources": sorted(ctx["failed_sources"] - ctx["used_sources"]),
        }

    def generate_and_save(self, top_k: int = 5, source: str = "manual") -> Dict[str, Any]:
        payload = self.generate_recommendations(top_k=top_k)
        payload["source"] = source
        record_id = self.repo.save_run(payload, source=source)
        payload["record_id"] = record_id
        return payload

    def generate_and_notify(self, top_k: int = 5, source: str = "scheduled") -> Dict[str, Any]:
        """生成推荐、落库并推送到已配置的通知渠道。"""
        payload = self.generate_and_save(top_k=top_k, source=source)
        try:
            from src.notification import get_notification_service
            notifier = get_notification_service()
            if notifier.is_available() and payload.get("output_count", 0) > 0:
                content = self._format_notification(payload)
                notifier.send(content)
                logger.info("daily picks 推送完成")
            elif not notifier.is_available():
                logger.info("通知渠道未配置，跳过 daily picks 推送")
        except Exception as exc:  # noqa: BLE001
            logger.warning("daily picks 推送失败（不影响生成结果）: %s", exc)
        return payload

    @staticmethod
    def _format_notification(payload: Dict[str, Any]) -> str:
        """将推荐结果格式化为 Markdown 通知文本。"""
        lines = ["## 每日热点推荐\n"]
        generated_at = payload.get("generated_at", "")
        if generated_at:
            lines.append(f"**生成时间**：{generated_at[:19].replace('T', ' ')}\n")

        recommendations = payload.get("recommendations") or []
        for rec in recommendations[:5]:
            rank = rec.get("rank", "?")
            name = rec.get("stock_name", "?")
            code = rec.get("stock_code", "?")
            score = rec.get("score", "--")
            sector = rec.get("sector_name", "")
            reason = rec.get("recommend_reason", "")
            entry = rec.get("entry_hint", "")
            risk = rec.get("risk_warning", "")

            lines.append(f"### {rank}. {name}（{code}）")
            if sector:
                lines.append(f"- 板块：{sector}")
            lines.append(f"- 综合评分：{score}")
            if reason:
                lines.append(f"- 推荐理由：{reason}")
            if entry:
                lines.append(f"- 参与方式：{entry}")
            if risk:
                lines.append(f"- 风险提示：{risk}")
            lines.append("")

        confidence = payload.get("confidence", "")
        risk_note = payload.get("risk_note", "")
        if confidence:
            lines.append(f"**可信度**：{confidence}")
        if risk_note:
            lines.append(f"**风险说明**：{risk_note}")

        lines.append("\n> 以上为程序化评分结果，仅供参考，不构成投资建议。")
        return "\n".join(lines)

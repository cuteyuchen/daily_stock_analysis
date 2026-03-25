# -*- coding: utf-8 -*-
"""JoinQuant / 聚宽数据源壳子。

目标：
1. 作为 daily picks 的可选结构化数据补充源；
2. 登录失败、权限不足、SDK 未安装时 fail-open；
3. 先支持股票列表与所属行业/板块查询，不把聚宽写死为主链路硬依赖。
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import pandas as pd

from .base import BaseFetcher, DataSourceUnavailableError, normalize_stock_code
from src.config import get_config

logger = logging.getLogger(__name__)


class JoinQuantFetcher(BaseFetcher):
    """Optional JoinQuant provider for structured CN market metadata."""

    name = "JoinQuantFetcher"
    priority = 1

    def __init__(self) -> None:
        self._enabled = bool(getattr(get_config(), "joinquant_enabled", False))
        self._client = None
        self._logged_unavailable = False
        self._init_client()

    def _fetch_raw_data(self, stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        raise DataSourceUnavailableError("JoinQuantFetcher 当前未实现日线主链路")

    def _normalize_data(self, df: pd.DataFrame, stock_code: str) -> pd.DataFrame:
        return df

    def _init_client(self) -> None:
        config = get_config()
        if not self._enabled:
            return

        username = getattr(config, "joinquant_username", None)
        password = getattr(config, "joinquant_password", None)
        if not username or not password:
            logger.warning("JOINQUANT_ENABLED=true 但未配置账号密码，跳过 JoinQuant 初始化")
            return

        try:
            import jqdatasdk as jq  # type: ignore
        except ImportError:
            logger.warning("未安装 jqdatasdk，JoinQuant provider 将保持禁用")
            return

        try:
            jq.auth(username, password)
            self._client = jq
            logger.info("JoinQuant API 初始化成功")
        except Exception as exc:  # noqa: BLE001
            logger.warning("JoinQuant 登录失败，provider 将继续 fail-open: %s", exc)
            self._client = None

    def is_available(self) -> bool:
        return self._client is not None

    def _ensure_client(self):
        if self._client is None:
            if not self._logged_unavailable:
                logger.info("JoinQuant provider 不可用，跳过相关能力")
                self._logged_unavailable = True
            raise DataSourceUnavailableError("JoinQuant provider 不可用")
        return self._client

    @staticmethod
    def _to_jq_code(stock_code: str) -> str:
        normalized = normalize_stock_code(stock_code)
        if normalized.startswith("HK") or normalized.isalpha():
            return normalized
        if normalized.startswith(("60", "68", "51", "52", "56", "58")):
            return f"{normalized}.XSHG"
        if normalized.startswith(("00", "30", "15", "16", "18", "12")):
            return f"{normalized}.XSHE"
        if normalized.startswith("92"):
            return f"{normalized}.XBEI"
        return normalized

    def get_stock_list(self) -> Optional[pd.DataFrame]:
        try:
            jq = self._ensure_client()
            df = jq.get_all_securities(types=["stock"], date=datetime.now().date())
            if df is None or df.empty:
                return None
            result = df.reset_index()[["index", "display_name"]].rename(
                columns={"index": "code", "display_name": "name"}
            )
            result["code"] = result["code"].astype(str).map(
                lambda value: normalize_stock_code(value.split(".")[0])
            )
            return result
        except Exception as exc:  # noqa: BLE001
            logger.warning("JoinQuant 获取股票列表失败: %s", exc)
            return None

    def get_belong_board(self, stock_code: str) -> Optional[pd.DataFrame]:
        try:
            jq = self._ensure_client()
            jq_code = self._to_jq_code(stock_code)
            industry_info = jq.get_industry([jq_code], date=datetime.now().date())
            stock_industry = industry_info.get(jq_code) if isinstance(industry_info, dict) else None
            if not stock_industry:
                return None

            rows = []
            for value in stock_industry.values():
                if not isinstance(value, dict):
                    continue
                name = value.get("industry_name") or value.get("name")
                if name:
                    rows.append({"板块名称": name})
            if not rows:
                return None
            return pd.DataFrame(rows)
        except Exception as exc:  # noqa: BLE001
            logger.warning("JoinQuant 获取所属行业/板块失败 %s: %s", stock_code, exc)
            return None

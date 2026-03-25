"""Data provider exports.

Concrete fetchers are imported on a best-effort basis so optional third-party
dependencies do not break unrelated code paths at import time.
"""

from __future__ import annotations

import logging

from .base import (
    BaseFetcher,
    DataFetchError,
    DataFetcherManager,
    DataSourceUnavailableError,
    RateLimitError,
    normalize_stock_code,
)
from .us_index_mapping import is_us_index_code, is_us_stock_code

logger = logging.getLogger(__name__)

__all__ = [
    "BaseFetcher",
    "DataFetcherManager",
    "DataFetchError",
    "RateLimitError",
    "DataSourceUnavailableError",
    "normalize_stock_code",
    "is_us_index_code",
    "is_us_stock_code",
]


def _optional_export(module_name: str, symbol_name: str) -> None:
    try:
        module = __import__(f"{__name__}.{module_name}", fromlist=[symbol_name])
        symbol = getattr(module, symbol_name)
    except ModuleNotFoundError as exc:
        logger.debug("Skipping optional data provider %s: %s", symbol_name, exc)
        return
    globals()[symbol_name] = symbol
    __all__.append(symbol_name)


_optional_export("akshare_fetcher", "AkshareFetcher")
_optional_export("efinance_fetcher", "EfinanceFetcher")
_optional_export("alpha_vantage_fetcher", "AlphaVantageFetcher")
_optional_export("finnhub_fetcher", "FinnhubFetcher")
_optional_export("polygon_fetcher", "PolygonFetcher")
_optional_export("tushare_fetcher", "TushareFetcher")
_optional_export("joinquant_fetcher", "JoinQuantFetcher")
_optional_export("akshare_fetcher", "is_hk_stock_code")

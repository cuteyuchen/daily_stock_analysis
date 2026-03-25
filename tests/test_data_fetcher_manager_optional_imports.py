# -*- coding: utf-8 -*-
"""Regression tests for optional fetcher imports in DataFetcherManager."""

from __future__ import annotations

import builtins
import sys
import types
import unittest
from unittest.mock import patch

from data_provider.base import DataFetcherManager


def _build_fetcher_module(module_name: str, class_name: str, priority: int):
    module = types.ModuleType(module_name)

    class _Fetcher:
        name = class_name

        def __init__(self):
            self.priority = priority

    _Fetcher.__name__ = class_name
    setattr(module, class_name, _Fetcher)
    return module


class DataFetcherManagerOptionalImportsTestCase(unittest.TestCase):
    def test_manager_skips_fetcher_when_optional_dependency_missing(self) -> None:
        original_import = builtins.__import__
        fake_modules = {
            "data_provider.akshare_fetcher": _build_fetcher_module("data_provider.akshare_fetcher", "AkshareFetcher", 1),
            "data_provider.tushare_fetcher": _build_fetcher_module("data_provider.tushare_fetcher", "TushareFetcher", 2),
            "data_provider.pytdx_fetcher": _build_fetcher_module("data_provider.pytdx_fetcher", "PytdxFetcher", 2),
            "data_provider.baostock_fetcher": _build_fetcher_module("data_provider.baostock_fetcher", "BaostockFetcher", 3),
            "data_provider.yfinance_fetcher": _build_fetcher_module("data_provider.yfinance_fetcher", "YfinanceFetcher", 4),
        }

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "data_provider.efinance_fetcher":
                raise ModuleNotFoundError("No module named 'fake_useragent'")
            if name in fake_modules:
                module = fake_modules[name]
                sys.modules[name] = module
                return module
            return original_import(name, globals, locals, fromlist, level)

        with patch("builtins.__import__", side_effect=fake_import):
            manager = DataFetcherManager()

        fetcher_names = [fetcher.name for fetcher in manager._fetchers]
        self.assertNotIn("EfinanceFetcher", fetcher_names)
        self.assertEqual(
            fetcher_names,
            ["AkshareFetcher", "TushareFetcher", "PytdxFetcher", "BaostockFetcher", "YfinanceFetcher"],
        )


if __name__ == "__main__":
    unittest.main()

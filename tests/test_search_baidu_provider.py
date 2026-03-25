# -*- coding: utf-8 -*-
"""Tests for the Baidu Search provider wrapper."""

import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

if "newspaper" not in sys.modules:
    mock_np = MagicMock()
    mock_np.Article = MagicMock()
    mock_np.Config = MagicMock()
    sys.modules["newspaper"] = mock_np

from src.search_service import BaiduSearchProvider


class BaiduSearchProviderTestCase(unittest.TestCase):
    @patch("src.search_service._post_with_retry")
    def test_maps_results_and_defaults_missing_date(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "results": [
                {
                    "title": "AI 概念股走强",
                    "snippet": "算力和大模型方向继续活跃。",
                    "url": "https://finance.example.com/a",
                    "source": "财经站",
                }
            ]
        }
        mock_post.return_value = mock_response

        provider = BaiduSearchProvider(["test-key"], base_url="https://example.com/search")
        response = provider.search("A股 市场热点", max_results=3, days=3)

        self.assertTrue(response.success)
        self.assertEqual(response.provider, "BaiduSearch")
        self.assertEqual(len(response.results), 1)
        self.assertEqual(response.results[0].title, "AI 概念股走强")
        self.assertEqual(response.results[0].source, "财经站")
        self.assertEqual(response.results[0].published_date, datetime.now().strftime("%Y-%m-%d"))


if __name__ == "__main__":
    unittest.main()

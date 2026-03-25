# -*- coding: utf-8 -*-
"""Tests for daily picks scheduler hardening paths."""

from __future__ import annotations

import unittest
import sys
from unittest.mock import patch

if "newspaper" not in sys.modules:
    from unittest.mock import MagicMock

    mock_np = MagicMock()
    mock_np.Article = MagicMock()
    mock_np.Config = MagicMock()
    sys.modules["newspaper"] = mock_np

import scripts.run_daily_picks_scheduler as scheduler


class DailyPicksSchedulerTestCase(unittest.TestCase):
    def test_duplicate_lock_persists_skip_result(self) -> None:
        with patch.object(scheduler, "_acquire_lock", return_value=False), \
             patch.object(scheduler, "_persist_failure") as persist_failure:
            scheduler.run_daily_picks_job()

        persist_failure.assert_called_once()
        self.assertIn("scheduler lock active", persist_failure.call_args.args[0])


if __name__ == "__main__":
    unittest.main()

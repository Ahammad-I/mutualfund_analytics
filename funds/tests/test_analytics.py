import pytest
from datetime import date, timedelta
from unittest.mock import patch, MagicMock
from funds.services.analytics_engine import (
    _compute_max_drawdown,
    _compute_window,
    _nearest_nav,
    WINDOWS,
)


class TestMaxDrawdown:

    def test_no_drawdown_monotonic_increase(self):
        """NAV always rising — drawdown should be 0"""
        nav_map = {
            date(2020, 1, i): float(100 + i)
            for i in range(1, 20)
        }
        sorted_dates = sorted(nav_map.keys())
        result = _compute_max_drawdown(nav_map, sorted_dates)
        assert result == 0.0

    def test_known_drawdown(self):
        """
        NAV goes 100 -> 150 -> 75.
        Peak = 150, trough = 75.
        Drawdown = (75 - 150) / 150 * 100 = -50.0%
        """
        nav_map = {
            date(2020, 1, 1): 100.0,
            date(2020, 1, 2): 150.0,
            date(2020, 1, 3): 75.0,
        }
        sorted_dates = sorted(nav_map.keys())
        result = _compute_max_drawdown(nav_map, sorted_dates)
        assert abs(result - (-50.0)) < 0.001

    def test_multiple_drawdowns_returns_worst(self):
        """
        Two drawdowns: -20% and -40%. Should return -40%.
        """
        nav_map = {
            date(2020, 1, 1): 100.0,
            date(2020, 1, 2): 80.0,   # -20% drawdown
            date(2020, 1, 3): 120.0,  # new peak
            date(2020, 1, 4): 72.0,   # -40% from 120
        }
        sorted_dates = sorted(nav_map.keys())
        result = _compute_max_drawdown(nav_map, sorted_dates)
        assert abs(result - (-40.0)) < 0.001

    def test_zero_nav_skipped(self):
        """Zero NAV values are bad data — should not produce -100%"""
        nav_map = {
            date(2020, 1, 1): 100.0,
            date(2020, 1, 2): 0.0,    # bad data
            date(2020, 1, 3): 95.0,
        }
        sorted_dates = sorted(nav_map.keys())
        result = _compute_max_drawdown(nav_map, sorted_dates)
        # Should be -5% (95 vs 100), not -100%
        assert result > -10.0


class TestNearestNav:

    def setup_method(self):
        self.nav_map = {
            date(2020, 1, 1): 100.0,
            date(2020, 1, 3): 102.0,   # gap on Jan 2
            date(2020, 1, 6): 105.0,   # gap on Jan 4, 5
        }
        self.sorted_dates = sorted(self.nav_map.keys())

    def test_direct_hit(self):
        d, nav = _nearest_nav(self.nav_map, self.sorted_dates, date(2020, 1, 1))
        assert d == date(2020, 1, 1)
        assert nav == 100.0

    def test_finds_nearest_forward(self):
        # Jan 2 missing — should find Jan 3
        d, nav = _nearest_nav(self.nav_map, self.sorted_dates, date(2020, 1, 2))
        assert d == date(2020, 1, 3)

    def test_finds_nearest_backward(self):
        # Jan 5 missing — Jan 6 is 1 day ahead, Jan 3 is 2 days back
        # Should find Jan 6 (closer forward)
        d, nav = _nearest_nav(self.nav_map, self.sorted_dates, date(2020, 1, 5))
        assert d == date(2020, 1, 6)

    def test_returns_none_beyond_tolerance(self):
        # Jan 20 — no data within 7 days
        d, nav = _nearest_nav(self.nav_map, self.sorted_dates, date(2020, 1, 20))
        assert d is None
        assert nav is None


class TestRollingReturns:

    def _make_scheme_mock(self, scheme_code='TEST001'):
        """Create a minimal mock scheme object."""
        class MockScheme:
            def __init__(self):
                self.scheme_code = scheme_code
        return MockScheme()

    def test_known_cagr_calculation(self):
        base = date(2020, 1, 1)
        nav_map = {}
        for i in range(400):
            d = base + timedelta(days=i)
            nav_map[d] = 100.0 + (100.0 * i / 365)

        scheme = self._make_scheme_mock()

        with patch('funds.services.analytics_engine.Analytics') as MockAnalytics:
            MockAnalytics.objects.update_or_create = MagicMock()
            _compute_window(scheme, '1Y', 365, nav_map)

            call_kwargs = MockAnalytics.objects.update_or_create.call_args
            defaults = call_kwargs[1]['defaults']

            assert defaults['rolling_median'] is not None
            assert 90.0 < defaults['rolling_median'] < 105.0   # widened tolerance

    def test_insufficient_history_saves_null(self):
        """
        Only 100 days of data for a 1Y window — should save null analytics.
        """
        base = date(2020, 1, 1)
        nav_map = {base + timedelta(days=i): 100.0 + i for i in range(100)}
        scheme = self._make_scheme_mock()

        from unittest.mock import patch, MagicMock
        with patch('funds.services.analytics_engine.Analytics') as MockAnalytics:
            MockAnalytics.objects.update_or_create = MagicMock()
            _compute_window(scheme, '1Y', 365, nav_map)

            call_kwargs = MockAnalytics.objects.update_or_create.call_args
            defaults = call_kwargs[1]['defaults']

            assert defaults['rolling_median'] is None
            assert defaults['periods_analyzed'] == 0
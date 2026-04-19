import time
import pytest
from django.test import TestCase, Client
from funds.models import Scheme, NAVData, Analytics
from datetime import date


class TestAPIResponseTime(TestCase):

    def setUp(self):
        self.client = Client()
        self.scheme = Scheme.objects.create(
            scheme_code='BENCH01',
            scheme_name='Benchmark Fund',
            amc='axis',
            category='midcap',
        )
        # Seed minimal analytics
        Analytics.objects.create(
            scheme=self.scheme,
            window='3Y',
            rolling_min=10.0,
            rolling_max=35.0,
            rolling_median=22.0,
            rolling_p25=15.0,
            rolling_p75=28.0,
            max_drawdown=-30.0,
            cagr_min=10.0,
            cagr_max=35.0,
            cagr_median=22.0,
            data_start=date(2020, 1, 1),
            data_end=date(2023, 1, 1),
            total_days=1096,
            nav_points=750,
            periods_analyzed=400,
        )
        # Seed one NAV record
        NAVData.objects.create(
            scheme=self.scheme,
            date=date(2026, 4, 17),
            nav=100.0,
        )

    def _assert_response_time(self, url, max_ms=200):
        start = time.monotonic()
        response = self.client.get(url)
        elapsed_ms = (time.monotonic() - start) * 1000
        assert response.status_code == 200, f"Got {response.status_code} for {url}"
        assert elapsed_ms < max_ms, (
            f"{url} took {elapsed_ms:.1f}ms — exceeds {max_ms}ms limit"
        )
        return response

    def test_list_funds_under_200ms(self):
        self._assert_response_time('/api/funds/')

    def test_fund_detail_under_200ms(self):
        self._assert_response_time('/api/funds/BENCH01/')

    def test_analytics_under_200ms(self):
        self._assert_response_time('/api/funds/BENCH01/analytics?window=3Y')

    def test_rank_under_200ms(self):
        self._assert_response_time('/api/funds/rank?category=midcap&window=3Y')

    def test_analytics_invalid_window_returns_400(self):
        response = self.client.get('/api/funds/BENCH01/analytics?window=2Y')
        assert response.status_code == 400

    def test_rank_missing_category_returns_400(self):
        response = self.client.get('/api/funds/rank?window=3Y')
        assert response.status_code == 400

    def test_unknown_scheme_returns_404(self):
        response = self.client.get('/api/funds/DOESNOTEXIST/')
        assert response.status_code == 404
import pytest
from unittest.mock import patch, MagicMock
from django.test import TestCase
from funds.models import Scheme, SyncState
from funds.services import pipeline as pipeline_module


class TestPipelineResumability(TestCase):

    def setUp(self):
        # Reset global pipeline status before each test
        pipeline_module._pipeline_status.update({
            'running': False,
            'started_at': None,
            'completed_schemes': [],
            'failed_schemes': [],
            'current_scheme': None,
        })
        self.scheme1 = Scheme.objects.create(
            scheme_code='TEST001',
            scheme_name='Test Fund 1',
            amc='axis',
            category='midcap',
        )
        self.scheme2 = Scheme.objects.create(
            scheme_code='TEST002',
            scheme_name='Test Fund 2',
            amc='hdfc',
            category='smallcap',
        )

    def _make_mock_fetcher(self, side_effects):
        mock = MagicMock()
        mock.fetch_scheme_nav_history.side_effect = side_effects
        return mock

    def test_completed_scheme_is_skipped(self):
        """
        scheme1 backfill_done=True — goes through incremental sync, not backfill.
        Both schemes get fetched, but scheme1's SyncState stays backfill_done=True.
        """
        SyncState.objects.create(
            scheme=self.scheme1,
            status='done',
            backfill_done=True,
        )

        mock_fetcher = self._make_mock_fetcher(
            side_effects=[
                [{'date': '17-04-2026', 'nav': '100.0'}],  # scheme1 incremental
                [{'date': '17-04-2026', 'nav': '100.0'}],  # scheme2 backfill
            ]
        )

        with patch('funds.services.pipeline.MFAPIFetcher', return_value=mock_fetcher), \
            patch('funds.services.pipeline.compute_all'), \
            patch('funds.services.pipeline._bulk_upsert_nav', return_value=1), \
            patch('funds.services.pipeline.MFAPIFetcher.parse_date',
                return_value=None):

            pipeline_module.run_pipeline()

        # Both schemes are fetched — but scheme1 must still be marked backfill_done
        sync1 = SyncState.objects.get(scheme=self.scheme1)
        assert sync1.backfill_done is True   # ← the real invariant being tested

        # scheme2 had no prior state — should now be done
        sync2 = SyncState.objects.get(scheme=self.scheme2)
        assert sync2.status == 'done'

    def test_failed_scheme_is_retried(self):
        """scheme1 status=failed, backfill_done=False — must be retried."""
        SyncState.objects.create(
            scheme=self.scheme1,
            status='failed',
            backfill_done=False,
        )

        mock_fetcher = self._make_mock_fetcher(
            side_effects=[
                [{'date': '17-04-2026', 'nav': '100.0'}],
                [{'date': '17-04-2026', 'nav': '100.0'}],
            ]
        )

        with patch('funds.services.pipeline.MFAPIFetcher', return_value=mock_fetcher), \
             patch('funds.services.pipeline.compute_all'), \
             patch('funds.services.pipeline._bulk_upsert_nav', return_value=1), \
             patch('funds.services.pipeline.MFAPIFetcher.parse_date',
                   return_value=None):

            pipeline_module.run_pipeline()

        calls = mock_fetcher.fetch_scheme_nav_history.call_args_list
        called_codes = [c[0][0] for c in calls]
        assert 'TEST001' in called_codes

    def test_fetch_failure_does_not_stop_other_schemes(self):
        """scheme1 fetch raises — scheme2 must still complete."""
        mock_fetcher = self._make_mock_fetcher(
            side_effects=[
                Exception("Network error"),
                [{'date': '17-04-2026', 'nav': '100.0'}],
            ]
        )

        with patch('funds.services.pipeline.MFAPIFetcher', return_value=mock_fetcher), \
             patch('funds.services.pipeline.compute_all'), \
             patch('funds.services.pipeline._bulk_upsert_nav', return_value=1), \
             patch('funds.services.pipeline.MFAPIFetcher.parse_date',
                   return_value=None):

            result = pipeline_module.run_pipeline()

        status = pipeline_module.get_pipeline_status()
        assert result['status'] == 'completed'
        assert 'TEST001' in status['failed_schemes']
        assert 'TEST002' in status['completed_schemes']

    def test_sync_state_marked_in_progress_before_fetch(self):
        """SyncState must be in_progress BEFORE the API call — crash safety."""
        observed = []

        def fake_fetch(scheme_code):
            state = SyncState.objects.get(scheme__scheme_code=scheme_code)
            observed.append((scheme_code, state.status))
            return [{'date': '17-04-2026', 'nav': '100.0'}]

        mock_fetcher = MagicMock()
        mock_fetcher.fetch_scheme_nav_history.side_effect = fake_fetch

        with patch('funds.services.pipeline.MFAPIFetcher', return_value=mock_fetcher), \
             patch('funds.services.pipeline.compute_all'), \
             patch('funds.services.pipeline._bulk_upsert_nav', return_value=1), \
             patch('funds.services.pipeline.MFAPIFetcher.parse_date',
                   return_value=None):

            pipeline_module.run_pipeline()

        assert len(observed) == 2
        for scheme_code, status in observed:
            assert status == 'in_progress', (
                f"scheme={scheme_code} was '{status}' during fetch"
            )
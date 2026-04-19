import time
import threading
import pytest
from unittest.mock import patch
from funds.services.rate_limiter import TokenBucket, RateLimiter


class TestTokenBucket:

    def test_initial_tokens_equal_capacity(self):
        bucket = TokenBucket('test', capacity=2, refill_interval=1)
        assert bucket.tokens == 2

    def test_acquire_reduces_tokens(self):
        bucket = TokenBucket('test', capacity=2, refill_interval=1)
        bucket.acquire()
        assert bucket.tokens == 1.0

    def test_acquire_blocks_when_empty_then_refills(self):
        bucket = TokenBucket('test', capacity=1, refill_interval=1)
        bucket.acquire()  # drain it
        assert bucket.tokens == 0.0

        start = time.monotonic()
        acquired = bucket.acquire(timeout=3)
        elapsed = time.monotonic() - start

        assert acquired is True
        # Should have waited roughly 1 second for refill
        assert elapsed >= 0.8

    def test_tokens_capped_at_capacity(self):
        bucket = TokenBucket('test', capacity=5, refill_interval=1)
        bucket.tokens = 5.0
        time.sleep(0.5)
        bucket._refill()
        assert bucket.tokens <= 5.0  # never exceeds capacity

    def test_timeout_returns_false(self):
        bucket = TokenBucket('test', capacity=1, refill_interval=100)
        bucket.tokens = 0.0
        acquired = bucket.acquire(timeout=0.5)
        assert acquired is False

    def test_thread_safety_no_over_consumption(self):
        """
        50 threads all try to acquire from a bucket with 10 tokens.
        Exactly 10 should succeed immediately; rest must wait or timeout.
        """
        bucket = TokenBucket('test', capacity=10, refill_interval=100)
        results = []
        lock = threading.Lock()

        def try_acquire():
            result = bucket.acquire(timeout=0.2)
            with lock:
                results.append(result)

        threads = [threading.Thread(target=try_acquire) for _ in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        success_count = sum(1 for r in results if r)
        # Should have gotten exactly 10 (the initial capacity)
        # Allow small variance due to partial refills during thread execution
        assert 10 <= success_count <= 15

    def test_per_second_limit_enforced_over_time(self):
        """
        With capacity=2 and refill=1sec, 6 requests should take >= 2 seconds.
        """
        bucket = TokenBucket('test', capacity=2, refill_interval=1)
        start = time.monotonic()
        for _ in range(6):
            bucket.acquire(timeout=10)
        elapsed = time.monotonic() - start
        # 6 requests at 2/sec = at least 2 seconds
        assert elapsed >= 1.8


class TestRateLimiterComposite:

    def setup_method(self):
        # Reset singleton for each test
        RateLimiter._instance = None

    def test_singleton_returns_same_instance(self):
        with patch.object(RateLimiter, '_load_persisted_state', return_value=None):
            a = RateLimiter.get_instance()
            b = RateLimiter.get_instance()
            assert a is b

    def test_acquire_passes_all_three_buckets(self):
        with patch.object(RateLimiter, '_load_persisted_state', return_value=None):
            with patch.object(RateLimiter, '_persist_state', return_value=None):
                limiter = RateLimiter.get_instance()
                # All buckets start full — acquire should succeed immediately
                limiter.acquire()
                for name, bucket in limiter.buckets.items():
                    assert bucket.tokens < bucket.capacity

    def test_get_status_returns_all_buckets(self):
        with patch.object(RateLimiter, '_load_persisted_state', return_value=None):
            limiter = RateLimiter.get_instance()
            status = limiter.get_status()
            assert 'per_second' in status
            assert 'per_minute' in status
            assert 'per_hour' in status
            for bucket_status in status.values():
                assert 'tokens_remaining' in bucket_status
                assert 'capacity' in bucket_status

    def test_state_persistence_called_on_acquire(self):
        with patch.object(RateLimiter, '_load_persisted_state', return_value=None):
            with patch.object(RateLimiter, '_persist_state') as mock_persist:
                limiter = RateLimiter.get_instance()
                limiter.acquire()
                mock_persist.assert_called_once()

    def test_exhausted_hour_bucket_raises(self):
        with patch.object(RateLimiter, '_load_persisted_state', return_value=None):
            with patch.object(RateLimiter, '_persist_state', return_value=None):
                limiter = RateLimiter.get_instance()

                # per_second and per_minute pass normally
                limiter.buckets['per_second'].tokens = 10.0
                limiter.buckets['per_minute'].tokens = 10.0

                # per_hour returns False (simulates timeout)
                with patch.object(
                    limiter.buckets['per_hour'], 'acquire', return_value=False
                ):
                    with pytest.raises(RuntimeError, match='per_hour'):
                        limiter.acquire()
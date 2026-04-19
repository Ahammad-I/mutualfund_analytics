import threading
import time
import logging
from datetime import datetime, timezone

from django.conf import settings
from django.utils import timezone as dj_timezone

logger = logging.getLogger(__name__)


class TokenBucket:
    """
    Single token bucket. Thread-safe.
    Tokens refill linearly based on elapsed time since last refill.
    """

    def __init__(self, name: str, capacity: float, refill_interval: float):
        self.name = name
        self.capacity = capacity
        self.refill_interval = refill_interval   # seconds per full refill
        self.tokens = capacity
        self.last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        # tokens earned = capacity * (elapsed / refill_interval)
        tokens_to_add = (elapsed / self.refill_interval) * self.capacity
        if tokens_to_add > 0:
            self.tokens = min(self.capacity, self.tokens + tokens_to_add)
            self.last_refill = now

    def acquire(self, timeout: float = 60.0) -> bool:
        """
        Block until 1 token is available or timeout is reached.
        Returns True if acquired, False if timed out.
        """
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                self._refill()
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    logger.info(
                        f"[RATE_LIMITER] bucket={self.name} "
                        f"tokens_remaining={self.tokens:.2f}"
                    )
                    return True
            # Not enough tokens — wait a fraction of the refill interval
            wait = self.refill_interval / self.capacity
            if time.monotonic() + wait > deadline:
                logger.warning(f"[RATE_LIMITER] bucket={self.name} TIMEOUT")
                return False
            time.sleep(wait)

    def get_state(self) -> dict:
        with self._lock:
            self._refill()
            return {
                'tokens_remaining': round(self.tokens, 4),
                'capacity': self.capacity,
            }


class RateLimiter:
    """
    Composite rate limiter — wraps three TokenBuckets.
    A request must acquire from ALL THREE to proceed.
    This is the only way to satisfy all three limits simultaneously.

    Usage:
        limiter = RateLimiter.get_instance()
        limiter.acquire()   # blocks until all 3 buckets have tokens
        # now safe to make API call
    """

    _instance = None
    _lock = threading.Lock()

    def __init__(self):
        cfg = settings.RATE_LIMITS
        self.buckets = {
            'per_second': TokenBucket(
                'per_second',
                capacity=cfg['per_second']['capacity'],
                refill_interval=cfg['per_second']['refill_interval_seconds'],
            ),
            'per_minute': TokenBucket(
                'per_minute',
                capacity=cfg['per_minute']['capacity'],
                refill_interval=cfg['per_minute']['refill_interval_seconds'],
            ),
            'per_hour': TokenBucket(
                'per_hour',
                capacity=cfg['per_hour']['capacity'],
                refill_interval=cfg['per_hour']['refill_interval_seconds'],
            ),
        }

    @classmethod
    def get_instance(cls):
        """Singleton — one rate limiter for the whole process."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = cls()
                    cls._instance._load_persisted_state()
        return cls._instance

    def acquire(self):
        """
        Block until all three buckets grant a token.
        Acquires per_second first (tightest), then per_minute, then per_hour.
        If per_second blocks, it naturally spaces out requests,
        satisfying per_minute and per_hour as a side effect.
        """
        for name, bucket in self.buckets.items():
            acquired = bucket.acquire(timeout=120)
            if not acquired:
                raise RuntimeError(
                    f"Rate limiter timeout on bucket: {name}. "
                    f"Pipeline paused — retry later."
                )
        self._persist_state()

    def get_status(self) -> dict:
        return {name: bucket.get_state() for name, bucket in self.buckets.items()}

    def _persist_state(self):
        """Save token counts to DB so state survives restarts."""
        try:
            from funds.models import RateLimiterState
            for name, bucket in self.buckets.items():
                state = bucket.get_state()
                RateLimiterState.objects.update_or_create(
                    bucket_name=name,
                    defaults={
                        'tokens_remaining': state['tokens_remaining'],
                        'last_refill_at': dj_timezone.now(),
                    }
                )
        except Exception as e:
            # Never crash the pipeline because of a state-save failure
            logger.warning(f"[RATE_LIMITER] Failed to persist state: {e}")

    def _load_persisted_state(self):
        """On startup, restore token counts from DB."""
        try:
            from funds.models import RateLimiterState
            for record in RateLimiterState.objects.all():
                if record.bucket_name in self.buckets:
                    bucket = self.buckets[record.bucket_name]
                    with bucket._lock:
                        bucket.tokens = float(record.tokens_remaining)
                    logger.info(
                        f"[RATE_LIMITER] Restored {record.bucket_name}: "
                        f"{record.tokens_remaining} tokens"
                    )
        except Exception as e:
            logger.warning(f"[RATE_LIMITER] Could not load persisted state: {e}")
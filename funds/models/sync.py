from django.db import models
from .scheme import Scheme


class SyncState(models.Model):
    """
    Tracks pipeline progress per scheme.
    This is your crash-resumability mechanism.
    On restart: pipeline reads this and skips already-completed schemes.
    """

    STATUS_PENDING    = 'pending'
    STATUS_IN_PROGRESS = 'in_progress'
    STATUS_DONE       = 'done'
    STATUS_FAILED     = 'failed'
    STATUS_CHOICES = [
        (STATUS_PENDING,     'Pending'),
        (STATUS_IN_PROGRESS, 'In Progress'),
        (STATUS_DONE,        'Done'),
        (STATUS_FAILED,      'Failed'),
    ]

    scheme         = models.OneToOneField(
        Scheme,
        on_delete=models.CASCADE,
        related_name='sync_state',
        to_field='scheme_code',
    )
    status         = models.CharField(
        max_length=20, choices=STATUS_CHOICES, default=STATUS_PENDING
    )
    backfill_done  = models.BooleanField(default=False)
    oldest_date    = models.DateField(null=True, blank=True)
    latest_date    = models.DateField(null=True, blank=True)
    nav_count      = models.IntegerField(default=0)
    last_synced_at = models.DateTimeField(null=True, blank=True)
    error_message  = models.TextField(blank=True, null=True)

    class Meta:
        db_table = 'sync_state'

    def __str__(self):
        return f"{self.scheme_id} — {self.status}"


class RateLimiterState(models.Model):
    """
    Persists token bucket state across restarts.
    Three rows: per_second, per_minute, per_hour.
    """

    BUCKET_CHOICES = [
        ('per_second', 'Per Second'),
        ('per_minute', 'Per Minute'),
        ('per_hour',   'Per Hour'),
    ]

    bucket_name      = models.CharField(
        max_length=20, choices=BUCKET_CHOICES, unique=True, primary_key=True
    )
    tokens_remaining = models.FloatField()
    last_refill_at   = models.DateTimeField()

    class Meta:
        db_table = 'rate_limiter_state'

    def __str__(self):
        return f"{self.bucket_name}: {self.tokens_remaining} tokens"
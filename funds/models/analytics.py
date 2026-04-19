from django.db import models
from .scheme import Scheme


class Analytics(models.Model):
    WINDOW_CHOICES = [
        ('1Y', '1 Year'),
        ('3Y', '3 Years'),
        ('5Y', '5 Years'),
        ('10Y', '10 Years'),
    ]

    scheme = models.ForeignKey(
        Scheme,
        on_delete=models.CASCADE,
        related_name='analytics',
        to_field='scheme_code',
    )
    window = models.CharField(max_length=5, choices=WINDOW_CHOICES)

    # Rolling returns
    rolling_min    = models.FloatField(null=True)
    rolling_max    = models.FloatField(null=True)
    rolling_median = models.FloatField(null=True)
    rolling_p25    = models.FloatField(null=True)
    rolling_p75    = models.FloatField(null=True)

    # Drawdown
    max_drawdown = models.FloatField(null=True)

    # CAGR distribution
    cagr_min    = models.FloatField(null=True)
    cagr_max    = models.FloatField(null=True)
    cagr_median = models.FloatField(null=True)

    # Data availability metadata
    data_start       = models.DateField(null=True)
    data_end         = models.DateField(null=True)
    total_days       = models.IntegerField(null=True)
    nav_points       = models.IntegerField(null=True)
    periods_analyzed = models.IntegerField(null=True)

    computed_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'analytics'
        unique_together = ('scheme', 'window')
        indexes = [
            models.Index(fields=['scheme', 'window']),
            models.Index(fields=['window', 'rolling_median']),  # for ranking queries
        ]

    def __str__(self):
        return f"{self.scheme_id} | {self.window}"
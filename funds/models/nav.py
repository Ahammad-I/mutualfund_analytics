from django.db import models
from .scheme import Scheme


class NAVData(models.Model):
    """
    One row per (scheme, date). This is the raw time-series data.
    Upsert logic handled at the service layer using get_or_create.
    """

    scheme = models.ForeignKey(
        Scheme,
        on_delete=models.CASCADE,
        related_name='nav_records',
        db_column='scheme_code',
        to_field='scheme_code',
    )
    date = models.DateField()
    nav  = models.DecimalField(max_digits=12, decimal_places=4)

    class Meta:
        db_table = 'nav_data'
        unique_together = ('scheme', 'date')
        # Critical: analytics engine will sort by date constantly
        indexes = [
            models.Index(fields=['scheme', 'date']),
            models.Index(fields=['date']),
        ]

    def __str__(self):
        return f"{self.scheme_id} | {self.date} | {self.nav}"
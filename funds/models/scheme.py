from django.db import models
from django.utils.timezone import now

class Scheme(models.Model):
    """
    Master data for each mutual fund scheme.
    Seeded from config — never fetched dynamically.
    """

    CATEGORY_MIDCAP = 'midcap'
    CATEGORY_SMALLCAP = 'smallcap'
    CATEGORY_CHOICES = [
        (CATEGORY_MIDCAP, 'Equity: Mid Cap Direct Growth'),
        (CATEGORY_SMALLCAP, 'Equity: Small Cap Direct Growth'),
    ]

    AMC_CHOICES = [
        ('axis', 'Axis Mutual Fund'),
        ('hdfc', 'HDFC Mutual Fund'),
        ('icici', 'ICICI Prudential Mutual Fund'),
        ('sbi', 'SBI Mutual Fund'),
        ('kotak', 'Kotak Mahindra Mutual Fund'),
    ]

    scheme_code = models.CharField(max_length=20, unique=True, primary_key=True)
    scheme_name = models.CharField(max_length=255)
    amc         = models.CharField(max_length=20, choices=AMC_CHOICES)
    category    = models.CharField(max_length=20, choices=CATEGORY_CHOICES)
    isin        = models.CharField(max_length=20, blank=True, null=True)
    is_active   = models.BooleanField(default=True)

    created_at = models.DateTimeField(default=now)

    class Meta:
        db_table = 'scheme'

    def __str__(self):
        return f"{self.scheme_code} — {self.scheme_name}"
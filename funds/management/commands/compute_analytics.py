from django.core.management.base import BaseCommand
from funds.services.analytics_engine import compute_all

class Command(BaseCommand):
    help = 'Recompute analytics for all schemes'

    def handle(self, *args, **kwargs):
        self.stdout.write('Computing analytics...')
        compute_all()
        self.stdout.write(self.style.SUCCESS('Done.'))
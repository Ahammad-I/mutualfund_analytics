import logging
import requests
from datetime import datetime

from django.conf import settings

from .rate_limiter import RateLimiter

logger = logging.getLogger(__name__)


class MFAPIFetcher:
    """
    Responsible for all HTTP communication with mfapi.in.
    Every call goes through the rate limiter — no exceptions.
    """

    BASE_URL = settings.MFAPI_BASE_URL

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({'Accept': 'application/json'})
        self.limiter = RateLimiter.get_instance()

    def fetch_scheme_nav_history(self, scheme_code: str) -> list[dict]:
        """
        Fetch full NAV history for a scheme.
        Returns list of {'date': 'DD-MM-YYYY', 'nav': '123.45'} dicts.
        mfapi returns newest-first — we return as-is, pipeline will sort.
        """
        url = f"{self.BASE_URL}/{scheme_code}"

        logger.info(f"[FETCHER] Acquiring rate limit tokens for scheme={scheme_code}")
        self.limiter.acquire()   # <-- blocks here if needed

        logger.info(f"[FETCHER] GET {url}")
        try:
            response = self.session.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.error(
                    f"[FETCHER] 429 Too Many Requests for scheme={scheme_code}. "
                    f"Rate limiter failed to prevent this — check bucket config."
                )
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"[FETCHER] Request failed for scheme={scheme_code}: {e}")
            raise

        data = response.json()

        # mfapi response shape:
        # { "meta": {...}, "data": [{"date": "06-01-2026", "nav": "78.45"}, ...] }
        nav_records = data.get('data', [])
        logger.info(
            f"[FETCHER] scheme={scheme_code} "
            f"fetched {len(nav_records)} NAV records"
        )
        return nav_records

    @staticmethod
    def parse_date(date_str: str):
        """
        mfapi returns dates as 'DD-MM-YYYY'.
        Convert to Python date object.
        """
        return datetime.strptime(date_str, '%d-%m-%Y').date()
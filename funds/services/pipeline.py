import logging
import threading
from datetime import date

from django.utils import timezone
from django.db import transaction

from funds.models import Scheme, NAVData, SyncState
from .fetcher import MFAPIFetcher

from funds.services.analytics_engine import compute_all
logger = logging.getLogger(__name__)

# Global pipeline lock — prevents two syncs running simultaneously
_pipeline_lock = threading.Lock()
_pipeline_status = {
    'running': False,
    'started_at': None,
    'completed_schemes': [],
    'failed_schemes': [],
    'current_scheme': None,
}


def get_pipeline_status() -> dict:
    return dict(_pipeline_status)


def run_pipeline(force_backfill: bool = False):
    """
    Main entry point for the data pipeline.
    - Iterates over all active schemes
    - Skips schemes already fully synced (resumability)
    - Fetches NAV history and stores it
    - Updates SyncState after each scheme (crash safety)

    force_backfill=True re-fetches even completed schemes.
    """
    global _pipeline_status

    if not _pipeline_lock.acquire(blocking=False):
        logger.warning("[PIPELINE] Already running — ignoring trigger")
        return {'status': 'already_running'}

    try:
        _pipeline_status.update({
            'running': True,
            'started_at': timezone.now().isoformat(),
            'completed_schemes': [],
            'failed_schemes': [],
            'current_scheme': None,
        })

        schemes = Scheme.objects.filter(is_active=True)
        fetcher = MFAPIFetcher()

        for scheme in schemes:
            _pipeline_status['current_scheme'] = scheme.scheme_code

            # --- Resumability check ---
            sync_state, _ = SyncState.objects.get_or_create(scheme=scheme)

            if sync_state.backfill_done and not force_backfill:
                logger.info(
                    f"[PIPELINE] scheme={scheme.scheme_code} "
                    f"already backfilled — running incremental sync"
                )
                _run_incremental_sync(scheme, sync_state, fetcher)
            else:
                logger.info(
                    f"[PIPELINE] scheme={scheme.scheme_code} — starting backfill"
                )
                _run_backfill(scheme, sync_state, fetcher)

        _pipeline_status['running'] = False
        _pipeline_status['current_scheme'] = None
        logger.info("[PIPELINE] All schemes processed successfully")
        # After all schemes synced, compute analytics
        logger.info("[PIPELINE] Starting analytics computation")
        compute_all()
        logger.info("[PIPELINE] Analytics computation complete")
        return {'status': 'completed'}

    except Exception as e:
        _pipeline_status['running'] = False
        logger.error(f"[PIPELINE] Fatal error: {e}")
        return {'status': 'failed', 'error': str(e)}

    finally:
        _pipeline_lock.release()


def _run_backfill(scheme, sync_state: SyncState, fetcher: MFAPIFetcher):
    """
    Full history fetch for a scheme.
    Marks sync_state as in_progress before starting,
    done after completing — so a crash mid-way is detectable.
    """
    sync_state.status = SyncState.STATUS_IN_PROGRESS
    sync_state.save(update_fields=['status'])

    try:
        raw_records = fetcher.fetch_scheme_nav_history(scheme.scheme_code)
        saved_count = _bulk_upsert_nav(scheme, raw_records)

        # Update sync state atomically
        dates = [
            MFAPIFetcher.parse_date(r['date'])
            for r in raw_records
            if r.get('nav') not in (None, '', '-')
        ]

        sync_state.status = SyncState.STATUS_DONE
        sync_state.backfill_done = True
        sync_state.nav_count = saved_count
        sync_state.oldest_date = min(dates) if dates else None
        sync_state.latest_date = max(dates) if dates else None
        sync_state.last_synced_at = timezone.now()
        sync_state.error_message = None
        sync_state.save()

        _pipeline_status['completed_schemes'].append(scheme.scheme_code)
        logger.info(
            f"[PIPELINE] Backfill done: scheme={scheme.scheme_code} "
            f"records={saved_count}"
        )

    except Exception as e:
        sync_state.status = SyncState.STATUS_FAILED
        sync_state.error_message = str(e)
        sync_state.save(update_fields=['status', 'error_message'])
        _pipeline_status['failed_schemes'].append(scheme.scheme_code)
        logger.error(
            f"[PIPELINE] Backfill FAILED: scheme={scheme.scheme_code} error={e}"
        )
        # Don't re-raise — let other schemes continue


def _run_incremental_sync(scheme, sync_state: SyncState, fetcher: MFAPIFetcher):
    """
    For already-backfilled schemes: fetch full history again but only
    upsert new records (INSERT OR IGNORE logic via get_or_create).
    Since mfapi has no 'since date' filter, we always fetch all but
    only insert what's new — bulk_upsert is idempotent.
    """
    try:
        raw_records = fetcher.fetch_scheme_nav_history(scheme.scheme_code)
        saved_count = _bulk_upsert_nav(scheme, raw_records)

        sync_state.last_synced_at = timezone.now()
        if raw_records:
            dates = [
                MFAPIFetcher.parse_date(r['date'])
                for r in raw_records
                if r.get('nav') not in (None, '', '-')
            ]
            sync_state.latest_date = max(dates)
            sync_state.nav_count = NAVData.objects.filter(scheme=scheme).count()
        sync_state.save(update_fields=['last_synced_at', 'latest_date', 'nav_count'])

        _pipeline_status['completed_schemes'].append(scheme.scheme_code)
        logger.info(
            f"[PIPELINE] Incremental sync done: scheme={scheme.scheme_code} "
            f"new_records={saved_count}"
        )

    except Exception as e:
        sync_state.status = SyncState.STATUS_FAILED
        sync_state.error_message = str(e)
        sync_state.save(update_fields=['status', 'error_message'])
        _pipeline_status['failed_schemes'].append(scheme.scheme_code)
        logger.error(
            f"[PIPELINE] Incremental sync FAILED: "
            f"scheme={scheme.scheme_code} error={e}"
        )


def _bulk_upsert_nav(scheme, raw_records: list) -> int:
    """
    Efficiently insert NAV records.
    Uses bulk_create with update_conflicts (Django 4.2+) for true upsert.
    Skips records with invalid NAV values (gaps in data).
    Returns count of records processed.
    """
    records_to_insert = []

    for record in raw_records:
        nav_str = record.get('nav', '')
        date_str = record.get('date', '')

        # Skip bad/missing data
        if not nav_str or nav_str in ('-', 'N.A.', ''):
            continue
        if not date_str:
            continue

        try:
            parsed_date = MFAPIFetcher.parse_date(date_str)
            parsed_nav = float(nav_str)
            if parsed_nav <= 0:          # <-- add this
                continue
        except (ValueError, TypeError):
            logger.warning(
                f"[PIPELINE] Skipping bad record: "
                f"scheme={scheme.scheme_code} date={date_str} nav={nav_str}"
            )
            continue
        
        records_to_insert.append(
            NAVData(
                scheme=scheme,
                date=parsed_date,
                nav=parsed_nav,
            )
        )

    if not records_to_insert:
        return 0

    # Upsert: if (scheme, date) already exists, update nav
    with transaction.atomic():
        NAVData.objects.bulk_create(
            records_to_insert,
            update_conflicts=True,
            unique_fields=['scheme', 'date'],
            update_fields=['nav'],
        )

    return len(records_to_insert)
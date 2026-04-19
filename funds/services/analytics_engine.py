import logging
import statistics
from datetime import date, timedelta

from funds.models import NAVData, Analytics, Scheme

logger = logging.getLogger(__name__)

# Window definitions: label -> calendar days
WINDOWS = {
    '1Y':  365,
    '3Y':  1095,
    '5Y':  1825,
    '10Y': 3650,
}

# How many days either side to search for nearest trading day
DATE_TOLERANCE_DAYS = 7


def compute_all():
    """
    Entry point: compute analytics for all active schemes, all windows.
    Called after every sync.
    """
    schemes = Scheme.objects.filter(is_active=True)
    for scheme in schemes:
        logger.info(f"[ANALYTICS] Computing for scheme={scheme.scheme_code}")
        nav_map = _load_nav_map(scheme)

        if len(nav_map) < 30:
            logger.warning(
                f"[ANALYTICS] scheme={scheme.scheme_code} "
                f"has only {len(nav_map)} records — skipping"
            )
            continue

        for window_label, window_days in WINDOWS.items():
            _compute_window(scheme, window_label, window_days, nav_map)

    logger.info("[ANALYTICS] All computations complete")


def compute_for_scheme(scheme_code: str):
    """Compute analytics for a single scheme (all windows)."""
    try:
        scheme = Scheme.objects.get(scheme_code=scheme_code)
    except Scheme.DoesNotExist:
        logger.error(f"[ANALYTICS] Scheme {scheme_code} not found")
        return

    nav_map = _load_nav_map(scheme)
    for window_label, window_days in WINDOWS.items():
        _compute_window(scheme, window_label, window_days, nav_map)


def _load_nav_map(scheme) -> dict:
    """
    Load all NAV records for a scheme into a dict: {date: nav_float}.
    Also build a sorted list of dates for nearest-date lookups.
    Returns the dict — callers build the sorted list themselves.
    """
    records = (
        NAVData.objects
        .filter(scheme=scheme)
        .order_by('date')
        .values_list('date', 'nav')
    )
    return {d: float(nav) for d, nav in records}


def _nearest_nav(nav_map: dict, sorted_dates: list, target_date: date):
    """
    Find NAV for the nearest available trading date to target_date.
    Searches within DATE_TOLERANCE_DAYS in both directions.
    Returns (found_date, nav) or (None, None) if no date found.
    """
    # Direct hit
    if target_date in nav_map:
        return target_date, nav_map[target_date]

    # Search forward and backward within tolerance
    for delta in range(1, DATE_TOLERANCE_DAYS + 1):
        future = target_date + timedelta(days=delta)
        past   = target_date - timedelta(days=delta)
        if future in nav_map:
            return future, nav_map[future]
        if past in nav_map:
            return past, nav_map[past]

    return None, None


def _compute_window(scheme, window_label: str, window_days: int, nav_map: dict):
    """
    Core computation for one scheme + one window.
    
    Rolling returns:
      For every date D in history, find NAV at D and NAV at D + window_days.
      Annualized return = ((nav_end / nav_start) ^ (1/years)) - 1
      Collect all such returns → min, max, median, p25, p75

    Max drawdown:
      Walk through sorted NAVs, track peak, compute trough from peak.

    CAGR distribution = same as rolling returns (each period IS a CAGR).
    """
    sorted_dates = sorted(nav_map.keys())

    if not sorted_dates:
        return

    data_start = sorted_dates[0]
    data_end   = sorted_dates[-1]
    total_days = (data_end - data_start).days
    nav_points = len(sorted_dates)
    years      = window_days / 365.0

    # Need at least window_days of data to compute anything
    if total_days < window_days:
        logger.info(
            f"[ANALYTICS] scheme={scheme.scheme_code} window={window_label} "
            f"insufficient history ({total_days} days < {window_days} needed) "
            f"— saving null analytics"
        )
        Analytics.objects.update_or_create(
            scheme=scheme,
            window=window_label,
            defaults={
                'rolling_min': None, 'rolling_max': None,
                'rolling_median': None, 'rolling_p25': None, 'rolling_p75': None,
                'max_drawdown': None,
                'cagr_min': None, 'cagr_max': None, 'cagr_median': None,
                'data_start': data_start, 'data_end': data_end,
                'total_days': total_days, 'nav_points': nav_points,
                'periods_analyzed': 0,
            }
        )
        return

    # --- Rolling returns ---
    rolling_returns = []

    for start_date in sorted_dates:
        end_target = start_date + timedelta(days=window_days)

        # Stop if end target is beyond available data
        if end_target > data_end + timedelta(days=DATE_TOLERANCE_DAYS):
            break

        nav_start = nav_map[start_date]
        end_date, nav_end = _nearest_nav(nav_map, sorted_dates, end_target)

        if end_date is None:
            continue
        if nav_start <= 0:
            continue

        # Annualized CAGR for this period
        cagr = ((nav_end / nav_start) ** (1.0 / years)) - 1.0
        rolling_returns.append(round(cagr * 100, 4))  # store as percentage

    # --- Max drawdown ---
    max_drawdown = _compute_max_drawdown(nav_map, sorted_dates)

    # --- Percentiles ---
    if len(rolling_returns) >= 4:
        rolling_returns_sorted = sorted(rolling_returns)
        n = len(rolling_returns_sorted)

        median = statistics.median(rolling_returns_sorted)
        p25    = rolling_returns_sorted[int(n * 0.25)]
        p75    = rolling_returns_sorted[int(n * 0.75)]
        r_min  = rolling_returns_sorted[0]
        r_max  = rolling_returns_sorted[-1]
    elif rolling_returns:
        median = statistics.median(rolling_returns)
        p25 = min(rolling_returns)
        p75 = max(rolling_returns)
        r_min = min(rolling_returns)
        r_max = max(rolling_returns)
    else:
        logger.warning(
            f"[ANALYTICS] scheme={scheme.scheme_code} window={window_label} "
            f"zero rolling periods computed"
        )
        median = p25 = p75 = r_min = r_max = None

    Analytics.objects.update_or_create(
        scheme=scheme,
        window=window_label,
        defaults={
            'rolling_min':    round(r_min, 4)   if r_min    is not None else None,
            'rolling_max':    round(r_max, 4)   if r_max    is not None else None,
            'rolling_median': round(median, 4)  if median   is not None else None,
            'rolling_p25':    round(p25, 4)     if p25      is not None else None,
            'rolling_p75':    round(p75, 4)     if p75      is not None else None,
            'max_drawdown':   round(max_drawdown, 4) if max_drawdown is not None else None,
            'cagr_min':    round(r_min, 4)   if r_min    is not None else None,
            'cagr_max':    round(r_max, 4)   if r_max    is not None else None,
            'cagr_median': round(median, 4)  if median   is not None else None,
            'data_start':    data_start,
            'data_end':      data_end,
            'total_days':    total_days,
            'nav_points':    nav_points,
            'periods_analyzed': len(rolling_returns),
        }
    )

    logger.info(
        f"[ANALYTICS] scheme={scheme.scheme_code} window={window_label} "
        f"periods={len(rolling_returns)} median={median}"
    )


def _compute_max_drawdown(nav_map: dict, sorted_dates: list) -> float:
    """
    Walk through NAV history chronologically.
    Skip any zero/negative NAV values — these are data quality issues.
    """
    peak = None
    max_dd = 0.0

    for d in sorted_dates:
        nav = nav_map[d]

        # Skip bad data points
        if nav <= 0:
            continue

        if peak is None or nav > peak:
            peak = nav

        drawdown = ((nav - peak) / peak) * 100
        if drawdown < max_dd:
            max_dd = drawdown

    return max_dd
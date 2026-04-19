from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

from funds.models import Scheme, NAVData, SyncState

from funds.models import Scheme, NAVData, SyncState, Analytics
@api_view(['GET'])
def list_funds(request):
    """
    GET /funds
    Query params: ?category=midcap|smallcap  ?amc=axis|hdfc|icici|sbi|kotak
    """
    qs = Scheme.objects.filter(is_active=True)

    category = request.query_params.get('category')
    amc = request.query_params.get('amc')

    if category:
        qs = qs.filter(category=category)
    if amc:
        qs = qs.filter(amc=amc)

    data = [
        {
            'scheme_code': s.scheme_code,
            'scheme_name': s.scheme_name,
            'amc': s.get_amc_display(),
            'category': s.get_category_display(),
            'isin': s.isin,
        }
        for s in qs
    ]
    return Response({'count': len(data), 'funds': data})


@api_view(['GET'])
def fund_detail(request, scheme_code):
    """
    GET /funds/{scheme_code}
    Returns fund metadata + latest NAV + sync state.
    """
    try:
        scheme = Scheme.objects.get(scheme_code=scheme_code)
    except Scheme.DoesNotExist:
        return Response(
            {'error': f'Scheme {scheme_code} not found'},
            status=status.HTTP_404_NOT_FOUND
        )

    latest_nav = (
        NAVData.objects
        .filter(scheme=scheme)
        .order_by('-date')
        .first()
    )

    try:
        sync = scheme.sync_state
        sync_info = {
            'status': sync.status,
            'backfill_done': sync.backfill_done,
            'nav_count': sync.nav_count,
            'oldest_date': sync.oldest_date,
            'latest_date': sync.latest_date,
            'last_synced_at': sync.last_synced_at,
        }
    except SyncState.DoesNotExist:
        sync_info = {'status': 'never_synced'}

    return Response({
        'scheme_code': scheme.scheme_code,
        'scheme_name': scheme.scheme_name,
        'amc': scheme.get_amc_display(),
        'category': scheme.get_category_display(),
        'isin': scheme.isin,
        'latest_nav': {
            'date': latest_nav.date if latest_nav else None,
            'nav': float(latest_nav.nav) if latest_nav else None,
        },
        'sync': sync_info,
    })
    


@api_view(['GET'])
def fund_analytics(request, scheme_code):
    """
    GET /funds/{scheme_code}/analytics?window=3Y
    """
    window = request.query_params.get('window')
    if not window or window not in ('1Y', '3Y', '5Y', '10Y'):
        return Response(
            {'error': 'window param required: 1Y | 3Y | 5Y | 10Y'},
            status=status.HTTP_400_BAD_REQUEST
        )

    try:
        scheme = Scheme.objects.get(scheme_code=scheme_code)
    except Scheme.DoesNotExist:
        return Response({'error': 'Scheme not found'}, status=404)

    try:
        a = Analytics.objects.get(scheme=scheme, window=window)
    except Analytics.DoesNotExist:
        return Response(
            {'error': f'Analytics not yet computed for {scheme_code} / {window}'},
            status=404
        )

    return Response({
        'fund_code':  scheme.scheme_code,
        'fund_name':  scheme.scheme_name,
        'amc':        scheme.get_amc_display(),
        'category':   scheme.get_category_display(),
        'window':     window,
        'data_availability': {
            'start_date':   a.data_start,
            'end_date':     a.data_end,
            'total_days':   a.total_days,
            'nav_points':   a.nav_points,
        },
        'rolling_periods_analyzed': a.periods_analyzed,
        'rolling_returns': {
            'min':    a.rolling_min,
            'max':    a.rolling_max,
            'median': a.rolling_median,
            'p25':    a.rolling_p25,
            'p75':    a.rolling_p75,
        },
        'max_drawdown': a.max_drawdown,
        'cagr': {
            'min':    a.cagr_min,
            'max':    a.cagr_max,
            'median': a.cagr_median,
        },
        'computed_at': a.computed_at,
    })


@api_view(['GET'])
def rank_funds(request):
    """
    GET /funds/rank?category=midcap&window=3Y&sort_by=median_return&limit=5
    """
    category = request.query_params.get('category')
    window   = request.query_params.get('window')
    sort_by  = request.query_params.get('sort_by', 'median_return')
    limit    = int(request.query_params.get('limit', 5))

    if not category:
        return Response({'error': 'category param required'}, status=400)
    if not window or window not in ('1Y', '3Y', '5Y', '10Y'):
        return Response({'error': 'window param required: 1Y | 3Y | 5Y | 10Y'}, status=400)

    sort_field_map = {
        'median_return': '-rolling_median',   # descending (higher is better)
        'max_drawdown':  '-max_drawdown',     # less negative = better
    }
    order_by = sort_field_map.get(sort_by, '-rolling_median')

    scheme_codes = (
        Scheme.objects
        .filter(is_active=True, category=category)
        .values_list('scheme_code', flat=True)
    )

    analytics_qs = (
        Analytics.objects
        .filter(scheme_id__in=scheme_codes, window=window)
        .select_related('scheme')
        .exclude(rolling_median=None)
        .order_by(order_by)[:limit]
    )

    results = []
    for rank, a in enumerate(analytics_qs, start=1):
        latest_nav = (
            NAVData.objects
            .filter(scheme=a.scheme)
            .order_by('-date')
            .values('date', 'nav')
            .first()
        )
        results.append({
            'rank':        rank,
            'fund_code':   a.scheme.scheme_code,
            'fund_name':   a.scheme.scheme_name,
            'amc':         a.scheme.get_amc_display(),
            f'median_return_{window.lower()}': a.rolling_median,
            f'max_drawdown_{window.lower()}':  a.max_drawdown,
            'current_nav':  float(latest_nav['nav']) if latest_nav else None,
            'last_updated': latest_nav['date'] if latest_nav else None,
        })

    return Response({
        'category':    category,
        'window':      window,
        'sorted_by':   sort_by,
        'total_funds': len(scheme_codes),
        'showing':     len(results),
        'funds':       results,
    })
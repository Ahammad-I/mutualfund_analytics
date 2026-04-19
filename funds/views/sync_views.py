import threading
from rest_framework.decorators import api_view
from rest_framework.response import Response
from rest_framework import status

from funds.services.pipeline import run_pipeline, get_pipeline_status
from funds.services.rate_limiter import RateLimiter


@api_view(['POST'])
def trigger_sync(request):
    """
    POST /sync/trigger
    Starts the pipeline in a background thread.
    Returns immediately with 202 Accepted.
    """
    force = request.data.get('force_backfill', False)

    thread = threading.Thread(
        target=run_pipeline,
        kwargs={'force_backfill': force},
        daemon=True,
    )
    thread.start()

    return Response(
        {'message': 'Sync triggered', 'force_backfill': force},
        status=status.HTTP_202_ACCEPTED
    )


@api_view(['GET'])
def sync_status(request):
    """
    GET /sync/status
    Returns current pipeline state + rate limiter token counts.
    """
    pipeline = get_pipeline_status()
    rate_limiter = RateLimiter.get_instance().get_status()

    return Response({
        'pipeline': pipeline,
        'rate_limiter': rate_limiter,
    })
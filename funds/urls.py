from django.urls import path
from funds.views.fund_views import list_funds, fund_detail, fund_analytics, rank_funds
from funds.views.sync_views import trigger_sync, sync_status

urlpatterns = [
    path('funds/rank', rank_funds, name='rank_funds'),         # MUST be before <scheme_code>
    path('funds/', list_funds, name='list_funds'),
    path('funds/<str:scheme_code>/', fund_detail, name='fund_detail'),
    path('funds/<str:scheme_code>/analytics', fund_analytics, name='fund_analytics'),
    path('sync/trigger', trigger_sync, name='trigger_sync'),
    path('sync/status', sync_status, name='sync_status'),
]
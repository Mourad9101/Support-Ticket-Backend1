from datetime import datetime
from typing import Optional
from fastapi import APIRouter, Depends
from src.db.models import TenantStats
from src.services.analytics_service import AnalyticsService

router = APIRouter()


@router.get("/tenants/{tenant_id}/stats", response_model=TenantStats)
async def get_tenant_stats(
    tenant_id: str,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    analytics_service: AnalyticsService = Depends()
):
    return await analytics_service.get_tenant_stats(tenant_id, from_date, to_date)

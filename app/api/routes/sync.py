from fastapi import APIRouter

from app.api.dependencies import get_sync_service
from app.models.product import SyncResponse

router = APIRouter()


@router.post("/sync", response_model=SyncResponse)
async def trigger_sync():
    """Manually trigger a product sync from Odoo."""
    sync_service = get_sync_service()
    return await sync_service.sync()

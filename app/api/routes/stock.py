from fastapi import APIRouter

from app.api.dependencies import get_stock_update_service
from app.models.product import StockUpdateRequest, StockUpdateResponse

router = APIRouter()


@router.post("/stock/update", response_model=StockUpdateResponse)
async def update_stock(request: StockUpdateRequest):
    """Chained Odoo stock update for a list of purchased items."""
    service = get_stock_update_service()
    return await service.update_stock_for_items(request.items)

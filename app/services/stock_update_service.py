import logging

from app.models.product import (
    StockUpdateItem,
    StockUpdateItemResult,
    StockUpdateResponse,
)
from app.services.odoo_client import OdooClient

logger = logging.getLogger(__name__)


class StockUpdateService:
    def __init__(self, odoo_client: OdooClient):
        self.odoo_client = odoo_client

    async def update_stock_for_items(self, items: list[StockUpdateItem]) -> StockUpdateResponse:
        """Run the chained Odoo stock update flow for each purchased item."""
        results = [await self._update_single(item) for item in items]
        return StockUpdateResponse(results=results)

    async def _update_single(self, item: StockUpdateItem) -> StockUpdateItemResult:
        product = await self.odoo_client.fetch_product_by_name(item.name)

        if product is None:
            return StockUpdateItemResult(
                name=item.name,
                status="not_found",
                stage="fetch_product_by_name",
                purchased_quantity=item.purchased_quantity,
            )

        new_stock = product.stock - item.purchased_quantity

        quant_id = await self.odoo_client.search_stock_quant(product.id)
        if quant_id is None:
            quant_id = await self.odoo_client.create_stock_quant(product.id, new_stock)

        await self.odoo_client.modify_product_stock(quant_id, new_stock)
        applied = await self.odoo_client.apply_stock_quant_changes(quant_id)

        logger.info(
            "Stock update chain completed for '%s' (id=%d, quant_id=%d, prev=%s, bought=%s, new=%s, applied=%s)",
            product.name, product.id, quant_id,
            product.stock, item.purchased_quantity, new_stock, applied,
        )

        return StockUpdateItemResult(
            name=product.name,
            status="completed",
            product_id=product.id,
            quant_id=quant_id,
            previous_stock=product.stock,
            purchased_quantity=item.purchased_quantity,
            new_stock=new_stock,
            applied=applied,
        )

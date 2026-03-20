import logging

from app.models.product import SyncResponse
from app.services.odoo_client import OdooClient
from app.services.vector_store import VectorStore

logger = logging.getLogger(__name__)


class SyncService:
    def __init__(self, odoo_client: OdooClient, vector_store: VectorStore):
        self.odoo_client = odoo_client
        self.vector_store = vector_store

    async def sync(self) -> SyncResponse:
        """Synchronize products from Odoo to ChromaDB."""
        logger.info("Starting product sync from Odoo...")

        try:
            products = await self.odoo_client.fetch_products()
        except Exception:
            logger.exception("Failed to fetch products from Odoo")
            return SyncResponse(
                status="error",
                products_synced=0,
                products_updated=0,
                embeddings_regenerated=0,
            )

        result = self.vector_store.upsert_products(products)

        response = SyncResponse(
            status="completed",
            products_synced=len(products),
            products_updated=result["updated"],
            embeddings_regenerated=result["embeddings_regenerated"],
        )
        logger.info(
            "Sync completed: %d synced, %d updated, %d re-embedded",
            response.products_synced,
            response.products_updated,
            response.embeddings_regenerated,
        )
        return response

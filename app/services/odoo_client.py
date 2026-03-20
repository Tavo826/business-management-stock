import logging

import httpx

from app.config import settings
from app.models.product import Product

logger = logging.getLogger(__name__)

PRODUCT_FIELDS = ["id", "name", "list_price", "qty_available", "description_sale", "categ_id"]


class OdooClient:
    def __init__(self):
        self.base_url = settings.odoo_url.rstrip("/")
        self.headers = {
            "api-key": settings.odoo_api_key,
            "Content-Type": "application/json",
        }

    async def fetch_products(self) -> list[Product]:
        """Fetch all products from Odoo 19 REST API."""
        products = []
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(
                f"{self.base_url}/api/product.product",
                headers=self.headers,
                params={
                    "fields": ",".join(PRODUCT_FIELDS),
                    "limit": 0,
                },
            )
            response.raise_for_status()
            data = response.json()

        for record in data.get("data", []):
            products.append(self._parse_product(record))

        logger.info("Fetched %d products from Odoo", len(products))
        return products

    def _parse_product(self, record: dict) -> Product:
        categ = record.get("categ_id", "")
        if isinstance(categ, list) and len(categ) >= 2:
            category_name = categ[1]
        elif isinstance(categ, dict):
            category_name = categ.get("display_name", categ.get("name", "Sin categoría"))
        else:
            category_name = str(categ) if categ else "Sin categoría"

        description = record.get("description_sale") or ""
        if isinstance(description, bool):
            description = ""

        return Product(
            id=record["id"],
            name=record.get("name", ""),
            price=record.get("list_price", 0.0),
            stock=record.get("qty_available", 0.0),
            description=description,
            category=category_name,
        )

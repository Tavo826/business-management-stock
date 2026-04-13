import logging

import httpx

from app.config import settings
from app.models.product import Product

logger = logging.getLogger(__name__)

PRODUCT_FIELDS = ["id", "name", "x_list_price_iva", "qty_available", "categ_id", "public_description"]


class OdooClient:
    def __init__(self):
        self.base_url = settings.odoo_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {settings.odoo_api_key}",
            "X-Odoo-Database": settings.odoo_db,
            "Content-Type": "application/json",
        }

    async def fetch_products(self) -> list[Product]:
        """Fetch all products from Odoo 19 REST API."""
        products = []
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.base_url}/json/2/product.template/search_read",
                headers=self.headers,
                json={
                    "domain": [["sale_ok", "=", True]],
                    "fields": PRODUCT_FIELDS
                },
            )
            response.raise_for_status()
            data = response.json()

        for record in data:
            products.append(self._parse_product(record))

        logger.info("Fetched %d products from Odoo", len(products))
        return products

    def _parse_product(self, record: dict) -> Product:
        categ = record.get("categ_id", "")
        if isinstance(categ, list) and len(categ) >= 2:
            category_name = categ[1]
        else:
            category_name = str(categ) if categ else "Sin categoría"

        description = record.get("public_description") or ""

        return Product(
            id=record["id"],
            name=record.get("name", ""),
            price=record.get("x_list_price_iva", 0.0),
            stock=record.get("qty_available", 0.0),
            category=category_name,
            description=description,
        )

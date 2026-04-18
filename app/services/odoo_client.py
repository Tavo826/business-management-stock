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
                f"{self.base_url}/json/2/product.product/search_read",
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
    
    
    async def fetch_product_by_name(self, name: str) -> Product | None:
        """Fetch a single product by name from Odoo 19 REST API."""
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.base_url}/json/2/product.product/search_read",
                headers=self.headers,
                json={
                    "domain": [["name", "=", name]],
                    "fields": PRODUCT_FIELDS
                },
            )
            response.raise_for_status()
            data = response.json()

        if not data:
            logger.warning("No product found with name: %s", name)
            return None

        for record in data:
            product = self._parse_product(record)

        logger.info("Fetched product by name '%s' (ID: %d)", name, product.id)

        return product
    
    async def search_stock_quant(self, product_id: int, location_id: int = 5) -> int | None:
        """Search for an existing stock quant for a product at a location."""
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.base_url}/json/2/stock.quant/search_read",
                headers=self.headers,
                json={
                    "domain": [
                        ["product_id", "=", product_id],
                        ["location_id", "=", location_id],
                    ],
                    "fields": ["id"],
                },
            )
            response.raise_for_status()
            data = response.json()

        if not data:
            logger.info("No existing stock quant for product ID: %d at location %d", product_id, location_id)
            return None

        quant_id = data[0].get("id")
        logger.info("Found existing stock quant ID: %d for product ID: %d", quant_id, product_id)
        return quant_id

    async def create_stock_quant(self, product_id: int, quantity: float) -> int:
        """Create a stock quant for a product in Odoo 19 REST API."""
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.base_url}/json/2/stock.quant/create",
                headers=self.headers,
                json={
                    "vals_list": [
                        {
                            "product_id": product_id,
                            "location_id": 5,
                            "inventory_quantity": quantity,
                            "inventory_quantity_set": True,
                        }
                    ]
                },
            )
            response.raise_for_status()
            data = response.json()

        if isinstance(data, list) and len(data) > 0:
            quant_id = data[0]
        else:
            quant_id = None

        if quant_id is None:
            logger.error("Failed to create stock quant for product ID: %d", product_id)
            raise ValueError("Failed to create stock quant")

        logger.info("Created stock quant with ID: %d for product ID: %d", quant_id, product_id)
        return quant_id
    
    async def modify_product_stock(self, product_id: int, quantity: float) -> bool:
        """Modify the stock quantity of a product in Odoo 19 REST API."""
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.base_url}/json/2/stock.quant/write",
                headers=self.headers,
                json={
                    "ids": [product_id],
                    "vals": {
                        "inventory_quantity": quantity,
                        "inventory_quantity_set": True,
                    }
                },
            )
            response.raise_for_status()
            data = response.json()

        return data
    
    async def apply_stock_quant_changes(self, quant_id: int) -> bool:
        """Apply stock quant changes in Odoo 19 REST API."""
        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.post(
                f"{self.base_url}/json/2/stock.quant/action_apply_inventory",
                headers=self.headers,
                json={
                    "ids": [quant_id]
                }
            )
            response.raise_for_status()
            data = response.json()

        if data is None or data is False:
            logger.info("Stock quant changes applied successfully for quant ID: %d", quant_id)
            return True

        logger.error("Failed to apply stock quant changes: %s", data)
        return False

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

"""
Normalizadores de datos para productos.
Estandariza categorías, precios y maneja duplicados.
"""
import logging
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


class ProductNormalizer:
    """
    Normalizador de productos con reglas configurables.

    Funcionalidades:
    - Estandarización de categorías mediante diccionario
    - Unificación de precios (formato, moneda)
    - Detección y manejo de duplicados por SKU
    """

    # Diccionario de categorías para estandarización
    # Mapea variantes a forma canónica
    DEFAULT_CATEGORY_MAPPING = {
        # Ropa
        "camisa": "camisas",
        "camisas": "camisas",
        "shirt": "camisas",
        "shirts": "camisas",
        "remera": "camisetas",
        "remeras": "camisetas",
        "camiseta": "camisetas",
        "camisetas": "camisetas",
        "t-shirt": "camisetas",
        "t-shirts": "camisetas",
        "tshirt": "camisetas",
        "pantalon": "pantalones",
        "pantalones": "pantalones",
        "pants": "pantalones",
        "jeans": "pantalones",
        "jean": "pantalones",
        "short": "shorts",
        "shorts": "shorts",
        "bermuda": "shorts",
        "bermudas": "shorts",
        "vestido": "vestidos",
        "vestidos": "vestidos",
        "dress": "vestidos",
        "dresses": "vestidos",
        "falda": "faldas",
        "faldas": "faldas",
        "skirt": "faldas",
        "skirts": "faldas",
        "chaqueta": "chaquetas",
        "chaquetas": "chaquetas",
        "jacket": "chaquetas",
        "jackets": "chaquetas",
        "abrigo": "abrigos",
        "abrigos": "abrigos",
        "coat": "abrigos",
        "coats": "abrigos",
        # Calzado
        "zapato": "zapatos",
        "zapatos": "zapatos",
        "shoe": "zapatos",
        "shoes": "zapatos",
        "zapatilla": "zapatillas",
        "zapatillas": "zapatillas",
        "sneaker": "zapatillas",
        "sneakers": "zapatillas",
        "tenis": "zapatillas",
        "bota": "botas",
        "botas": "botas",
        "boot": "botas",
        "boots": "botas",
        "sandalia": "sandalias",
        "sandalias": "sandalias",
        "sandal": "sandalias",
        "sandals": "sandalias",
        # Accesorios
        "accesorio": "accesorios",
        "accesorios": "accesorios",
        "accessory": "accesorios",
        "accessories": "accesorios",
        "bolso": "bolsos",
        "bolsos": "bolsos",
        "bag": "bolsos",
        "bags": "bolsos",
        "cartera": "carteras",
        "carteras": "carteras",
        "purse": "carteras",
        "wallet": "billeteras",
        "billetera": "billeteras",
        "billeteras": "billeteras",
        "cinturon": "cinturones",
        "cinturones": "cinturones",
        "belt": "cinturones",
        "belts": "cinturones",
        "gorra": "gorras",
        "gorras": "gorras",
        "cap": "gorras",
        "caps": "gorras",
        "sombrero": "sombreros",
        "sombreros": "sombreros",
        "hat": "sombreros",
        "hats": "sombreros",
        # Otros
        "otro": "otros",
        "otros": "otros",
        "other": "otros",
        "misc": "otros",
        "varios": "otros",
    }

    def __init__(
        self,
        category_mapping: Optional[dict[str, str]] = None,
        default_category: str = "otros",
        price_decimals: int = 2,
        price_multiplier: Decimal = Decimal("1"),
        duplicate_strategy: str = "keep_latest",
    ):
        """
        Inicializa el normalizador.

        Args:
            category_mapping: Mapeo personalizado de categorías
            default_category: Categoría por defecto si no hay match
            price_decimals: Decimales para redondeo de precios
            price_multiplier: Multiplicador de precios (ej: 1000 para convertir a centavos)
            duplicate_strategy: Estrategia para duplicados ('keep_first', 'keep_latest', 'merge')
        """
        self.category_mapping = category_mapping or self.DEFAULT_CATEGORY_MAPPING
        self.default_category = default_category
        self.price_decimals = price_decimals
        self.price_multiplier = price_multiplier
        self.duplicate_strategy = duplicate_strategy

    def normalize_category(self, category: Optional[str]) -> str:
        """
        Normaliza una categoría usando el diccionario de mapeo.

        Args:
            category: Categoría a normalizar

        Returns:
            Categoría normalizada
        """
        if not category:
            return self.default_category

        category_lower = str(category).strip().lower()

        # Buscar match exacto
        if category_lower in self.category_mapping:
            return self.category_mapping[category_lower]

        # Buscar match parcial (la categoría contiene una clave)
        for key, value in self.category_mapping.items():
            if key in category_lower:
                return value

        logger.warning(
            f"Categoría no reconocida: '{category}', usando default '{self.default_category}'"
        )
        return self.default_category

    def normalize_price(
        self,
        price: Any,
        source_currency: Optional[str] = None,
    ) -> Decimal:
        """
        Normaliza un precio a formato estándar.

        Args:
            price: Precio a normalizar (puede ser string, int, float, Decimal)
            source_currency: Moneda origen (para conversiones futuras)

        Returns:
            Precio normalizado como Decimal
        """
        if price is None:
            return Decimal("0")

        # Limpiar string si es necesario
        if isinstance(price, str):
            # Remover símbolos de moneda y separadores
            price = price.replace("$", "").replace("€", "").replace("£", "")
            price = price.replace(",", "").replace(" ", "").strip()

            # Manejar formato europeo (1.234,56 -> 1234.56)
            if "," in price and "." in price:
                if price.rindex(",") > price.rindex("."):
                    price = price.replace(".", "").replace(",", ".")

        try:
            price_decimal = Decimal(str(price))
        except Exception:
            logger.error(f"No se pudo convertir precio: '{price}'")
            return Decimal("0")

        # Aplicar multiplicador si existe
        price_decimal = price_decimal * self.price_multiplier

        # Redondear
        quantize_str = "0." + "0" * self.price_decimals
        price_decimal = price_decimal.quantize(
            Decimal(quantize_str), rounding=ROUND_HALF_UP
        )

        return price_decimal

    def normalize_product(self, product: dict[str, Any]) -> dict[str, Any]:
        """
        Normaliza todos los campos de un producto.

        Args:
            product: Diccionario del producto

        Returns:
            Producto normalizado
        """
        normalized = product.copy()

        # Normalizar categoría
        if "category" in normalized:
            normalized["category"] = self.normalize_category(normalized["category"])

        # Normalizar precio
        if "price" in normalized:
            normalized["price"] = self.normalize_price(normalized["price"])

        # Asegurar stock como entero
        if "stock" in normalized:
            try:
                normalized["stock"] = max(0, int(normalized["stock"]))
            except (ValueError, TypeError):
                normalized["stock"] = 0

        return normalized

    def handle_duplicates(
        self,
        products: list[dict[str, Any]],
        key_field: str = "sku",
        merge_fn: Optional[Callable[[dict, dict], dict]] = None,
    ) -> list[dict[str, Any]]:
        """
        Maneja productos duplicados según la estrategia configurada.

        Args:
            products: Lista de productos
            key_field: Campo a usar como clave para detectar duplicados
            merge_fn: Función personalizada para merge (opcional)

        Returns:
            Lista de productos sin duplicados
        """
        seen: dict[str, dict[str, Any]] = {}
        duplicates_count = 0

        for product in products:
            key = product.get(key_field)

            if key is None:
                logger.warning(f"Producto sin {key_field}, ignorando: {product}")
                continue

            if key not in seen:
                seen[key] = product
            else:
                duplicates_count += 1

                if self.duplicate_strategy == "keep_first":
                    # Mantener el primero, ignorar subsecuentes
                    pass

                elif self.duplicate_strategy == "keep_latest":
                    # Reemplazar con el más reciente
                    seen[key] = product

                elif self.duplicate_strategy == "merge":
                    # Combinar datos
                    if merge_fn:
                        seen[key] = merge_fn(seen[key], product)
                    else:
                        seen[key] = self._default_merge(seen[key], product)

        if duplicates_count > 0:
            logger.info(
                f"Manejados {duplicates_count} duplicados con estrategia '{self.duplicate_strategy}'"
            )

        return list(seen.values())

    def _default_merge(
        self,
        existing: dict[str, Any],
        new: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Merge por defecto: prioriza valores no nulos del nuevo registro.

        Args:
            existing: Registro existente
            new: Nuevo registro

        Returns:
            Registro combinado
        """
        merged = existing.copy()

        for key, value in new.items():
            if value is not None:
                if key == "stock":
                    # Sumar stocks
                    merged[key] = merged.get(key, 0) + value
                elif key == "attributes" and isinstance(value, dict):
                    # Combinar atributos
                    merged[key] = {**merged.get(key, {}), **value}
                else:
                    # Reemplazar con valor más reciente
                    merged[key] = value

        return merged

    def normalize_batch(
        self,
        products: list[dict[str, Any]],
        handle_duplicates: bool = True,
    ) -> list[dict[str, Any]]:
        """
        Normaliza un lote de productos.

        Args:
            products: Lista de productos
            handle_duplicates: Si debe manejar duplicados

        Returns:
            Lista de productos normalizados
        """
        # Normalizar cada producto
        normalized = [self.normalize_product(p) for p in products]

        # Manejar duplicados si está habilitado
        if handle_duplicates:
            normalized = self.handle_duplicates(normalized)

        logger.info(f"Normalizados {len(normalized)} productos")
        return normalized

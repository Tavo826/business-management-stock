"""
Limpiadores de texto para productos.
Normaliza y limpia campos de texto antes de la carga.
"""
import logging
import re
import unicodedata
from typing import Any, Optional

logger = logging.getLogger(__name__)


class TextCleaner:
    """
    Limpiador de texto con múltiples estrategias.

    Funcionalidades:
    - Trim de espacios
    - Remoción de caracteres especiales/raros
    - Unificación de nombres de unidades
    - Normalización Unicode
    """

    # Mapeo de unidades a forma canónica
    UNIT_MAPPING = {
        # Peso
        "kg": "kg",
        "kgs": "kg",
        "KG": "kg",
        "Kg": "kg",
        "kilogramo": "kg",
        "kilogramos": "kg",
        "kilo": "kg",
        "kilos": "kg",
        "g": "g",
        "gr": "g",
        "grs": "g",
        "gramo": "g",
        "gramos": "g",
        "mg": "mg",
        "miligramo": "mg",
        "miligramos": "mg",
        "lb": "lb",
        "lbs": "lb",
        "libra": "lb",
        "libras": "lb",
        "oz": "oz",
        "onza": "oz",
        "onzas": "oz",
        # Volumen
        "l": "l",
        "lt": "l",
        "lts": "l",
        "litro": "l",
        "litros": "l",
        "ml": "ml",
        "mililitro": "ml",
        "mililitros": "ml",
        "gal": "gal",
        "galon": "gal",
        "galones": "gal",
        # Longitud
        "m": "m",
        "mt": "m",
        "mts": "m",
        "metro": "m",
        "metros": "m",
        "cm": "cm",
        "centimetro": "cm",
        "centimetros": "cm",
        "mm": "mm",
        "milimetro": "mm",
        "milimetros": "mm",
        "in": "in",
        "inch": "in",
        "pulgada": "in",
        "pulgadas": "in",
        "ft": "ft",
        "pie": "ft",
        "pies": "ft",
        # Unidades discretas
        "u": "unidad",
        "un": "unidad",
        "und": "unidad",
        "unid": "unidad",
        "unidad": "unidad",
        "unidades": "unidad",
        "pz": "pieza",
        "pza": "pieza",
        "pieza": "pieza",
        "piezas": "pieza",
        "par": "par",
        "pares": "par",
        "docena": "docena",
        "doc": "docena",
        "caja": "caja",
        "cajas": "caja",
        "paquete": "paquete",
        "paq": "paquete",
        "pack": "paquete",
    }

    # Caracteres a remover (regex pattern)
    SPECIAL_CHARS_PATTERN = re.compile(r"[^\w\s\-.,;:()\"'áéíóúñÁÉÍÓÚÑüÜ/&%$#@!¿?¡]")

    # Múltiples espacios
    MULTIPLE_SPACES_PATTERN = re.compile(r"\s+")

    def __init__(
        self,
        normalize_unicode: bool = True,
        remove_special_chars: bool = True,
        normalize_whitespace: bool = True,
    ):
        self.normalize_unicode = normalize_unicode
        self.remove_special_chars = remove_special_chars
        self.normalize_whitespace = normalize_whitespace

    def clean_text(self, text: Optional[str]) -> Optional[str]:
        """
        Limpia un texto aplicando todas las transformaciones configuradas.

        Args:
            text: Texto a limpiar

        Returns:
            Texto limpio o None si era None/vacío
        """
        if text is None:
            return None

        if not isinstance(text, str):
            text = str(text)

        # Trim inicial
        text = text.strip()

        if not text:
            return None

        # Normalización Unicode (NFKC unifica caracteres similares)
        if self.normalize_unicode:
            text = unicodedata.normalize("NFKC", text)

        # Remover caracteres especiales/raros
        if self.remove_special_chars:
            text = self.SPECIAL_CHARS_PATTERN.sub("", text)

        # Normalizar espacios múltiples
        if self.normalize_whitespace:
            text = self.MULTIPLE_SPACES_PATTERN.sub(" ", text)

        return text.strip() if text else None

    def clean_unit(self, unit: Optional[str]) -> str:
        """
        Limpia y normaliza una unidad de medida.

        Args:
            unit: Unidad a normalizar

        Returns:
            Unidad normalizada o 'unidad' por defecto
        """
        if unit is None:
            return "unidad"

        unit_clean = str(unit).strip().lower()

        if not unit_clean:
            return "unidad"

        # Buscar en el mapeo
        normalized = self.UNIT_MAPPING.get(unit_clean)
        if normalized:
            return normalized

        # Intentar match parcial para unidades compuestas (ej: "10 kg")
        for key, value in self.UNIT_MAPPING.items():
            if key in unit_clean:
                return value

        logger.warning(f"Unidad no reconocida: '{unit}', usando valor original")
        return unit_clean

    def clean_sku(self, sku: Optional[str]) -> Optional[str]:
        """
        Limpia un SKU removiendo caracteres no válidos.

        Args:
            sku: SKU a limpiar

        Returns:
            SKU limpio
        """
        if sku is None:
            return None

        # Solo permitir alfanuméricos, guiones y guiones bajos
        sku_clean = re.sub(r"[^\w\-]", "", str(sku).strip().upper())
        return sku_clean if sku_clean else None

    def clean_product(self, product: dict[str, Any]) -> dict[str, Any]:
        """
        Limpia todos los campos de texto de un producto.

        Args:
            product: Diccionario del producto

        Returns:
            Producto con campos limpiados
        """
        cleaned = product.copy()

        # Limpiar campos de texto principales
        if "name" in cleaned:
            cleaned["name"] = self.clean_text(cleaned["name"]) or ""

        if "description" in cleaned:
            cleaned["description"] = self.clean_text(cleaned["description"])

        if "category" in cleaned:
            cleaned["category"] = self.clean_text(cleaned["category"]) or ""

        if "sku" in cleaned:
            cleaned["sku"] = self.clean_sku(cleaned["sku"]) or cleaned.get("sku", "")

        if "unit" in cleaned:
            cleaned["unit"] = self.clean_unit(cleaned["unit"])

        # Limpiar atributos textuales
        if "attributes" in cleaned and isinstance(cleaned["attributes"], dict):
            cleaned["attributes"] = self._clean_attributes(cleaned["attributes"])

        return cleaned

    def _clean_attributes(self, attributes: dict[str, Any]) -> dict[str, Any]:
        """Limpia los valores de atributos que son texto."""
        cleaned_attrs = {}

        for key, value in attributes.items():
            clean_key = self.clean_text(str(key))
            if not clean_key:
                continue

            if isinstance(value, str):
                clean_value = self.clean_text(value)
                if clean_value:
                    cleaned_attrs[clean_key] = clean_value
            else:
                cleaned_attrs[clean_key] = value

        return cleaned_attrs

    def clean_batch(self, products: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Limpia un lote de productos.

        Args:
            products: Lista de productos

        Returns:
            Lista de productos limpios
        """
        cleaned = []
        for product in products:
            try:
                cleaned.append(self.clean_product(product))
            except Exception as e:
                logger.error(f"Error limpiando producto {product.get('sku')}: {e}")
                cleaned.append(product)

        logger.info(f"Limpiados {len(cleaned)} productos")
        return cleaned

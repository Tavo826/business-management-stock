"""
Validadores de datos para productos.
Verifica integridad y consistencia de datos antes de la carga.
"""
import logging
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Any, Optional

logger = logging.getLogger(__name__)


@dataclass
class ValidationError:
    """Representa un error de validación."""

    field: str
    message: str
    value: Any = None


@dataclass
class ValidationResult:
    """Resultado de validación con errores detallados."""

    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[ValidationError] = field(default_factory=list)

    def add_error(self, field: str, message: str, value: Any = None) -> None:
        self.errors.append(ValidationError(field, message, value))
        self.is_valid = False

    def add_warning(self, field: str, message: str, value: Any = None) -> None:
        self.warnings.append(ValidationError(field, message, value))


class ProductValidator:
    """
    Validador de productos con reglas configurables.

    Validaciones:
    - Campos clave no vacíos
    - Valores numéricos mayores a cero
    - Estructura de datos válida
    """

    # Campos requeridos y sus tipos esperados
    REQUIRED_FIELDS = {
        "sku": str,
        "name": str,
        "category": str,
        "price": (int, float, Decimal, str),
        "stock": (int, str),
    }

    # Campos opcionales
    OPTIONAL_FIELDS = {
        "description": str,
        "unit": str,
        "attributes": dict,
    }

    def __init__(
        self,
        require_positive_stock: bool = False,
        min_price: Decimal = Decimal("0.01"),
        max_name_length: int = 500,
        max_description_length: int = 5000,
    ):
        self.require_positive_stock = require_positive_stock
        self.min_price = min_price
        self.max_name_length = max_name_length
        self.max_description_length = max_description_length

    def validate(self, data: dict[str, Any]) -> ValidationResult:
        """
        Valida un diccionario de producto.

        Args:
            data: Diccionario con datos del producto

        Returns:
            ValidationResult con estado y errores/warnings
        """
        result = ValidationResult(is_valid=True)

        # Validar campos requeridos
        self._validate_required_fields(data, result)

        # Si faltan campos críticos, no continuar
        if not result.is_valid:
            return result

        # Validaciones específicas
        self._validate_sku(data.get("sku"), result)
        self._validate_name(data.get("name"), result)
        self._validate_price(data.get("price"), result)
        self._validate_stock(data.get("stock"), result)
        self._validate_description(data.get("description"), result)

        return result

    def _validate_required_fields(
        self, data: dict[str, Any], result: ValidationResult
    ) -> None:
        """Verifica que todos los campos requeridos estén presentes."""
        for field_name, expected_types in self.REQUIRED_FIELDS.items():
            value = data.get(field_name)

            if value is None:
                result.add_error(field_name, f"Campo requerido '{field_name}' faltante")
                continue

            if isinstance(expected_types, tuple):
                if not isinstance(value, expected_types):
                    result.add_error(
                        field_name,
                        f"Tipo inválido para '{field_name}': "
                        f"esperado {expected_types}, recibido {type(value).__name__}",
                        value,
                    )
            elif not isinstance(value, expected_types):
                result.add_error(
                    field_name,
                    f"Tipo inválido para '{field_name}': "
                    f"esperado {expected_types.__name__}, recibido {type(value).__name__}",
                    value,
                )

    def _validate_sku(self, sku: Optional[str], result: ValidationResult) -> None:
        """Valida el SKU del producto."""
        if sku is None:
            return

        sku_str = str(sku).strip()
        if not sku_str:
            result.add_error("sku", "SKU no puede estar vacío")
        elif len(sku_str) > 100:
            result.add_error("sku", f"SKU muy largo: {len(sku_str)} caracteres (max 100)")

    def _validate_name(self, name: Optional[str], result: ValidationResult) -> None:
        """Valida el nombre del producto."""
        if name is None:
            return

        name_str = str(name).strip()
        if not name_str:
            result.add_error("name", "Nombre no puede estar vacío")
        elif len(name_str) > self.max_name_length:
            result.add_warning(
                "name",
                f"Nombre muy largo: {len(name_str)} caracteres "
                f"(max {self.max_name_length}), será truncado",
            )

    def _validate_price(self, price: Any, result: ValidationResult) -> None:
        """Valida el precio del producto."""
        if price is None:
            return

        try:
            price_decimal = Decimal(str(price))
        except (InvalidOperation, ValueError):
            result.add_error("price", f"Precio inválido: '{price}'", price)
            return

        if price_decimal <= 0:
            result.add_error("price", f"Precio debe ser mayor a 0: {price_decimal}")
        elif price_decimal < self.min_price:
            result.add_warning(
                "price",
                f"Precio muy bajo: {price_decimal} (min recomendado: {self.min_price})",
            )

    def _validate_stock(self, stock: Any, result: ValidationResult) -> None:
        """Valida el stock del producto."""
        if stock is None:
            return

        try:
            stock_int = int(stock)
        except (ValueError, TypeError):
            result.add_error("stock", f"Stock inválido: '{stock}'", stock)
            return

        if stock_int < 0:
            result.add_error("stock", f"Stock no puede ser negativo: {stock_int}")
        elif self.require_positive_stock and stock_int == 0:
            result.add_warning("stock", "Stock es cero")

    def _validate_description(
        self, description: Optional[str], result: ValidationResult
    ) -> None:
        """Valida la descripción del producto."""
        if description is None:
            return

        desc_str = str(description).strip()
        if len(desc_str) > self.max_description_length:
            result.add_warning(
                "description",
                f"Descripción muy larga: {len(desc_str)} caracteres "
                f"(max {self.max_description_length}), será truncada",
            )

    def validate_batch(
        self,
        products: list[dict[str, Any]],
        skip_invalid: bool = True,
    ) -> tuple[list[dict[str, Any]], list[tuple[dict[str, Any], ValidationResult]]]:
        """
        Valida un lote de productos.

        Args:
            products: Lista de productos a validar
            skip_invalid: Si True, omite productos inválidos; si False, incluye todos

        Returns:
            Tupla de (productos válidos, lista de (producto, resultado) inválidos)
        """
        valid_products = []
        invalid_products = []

        for product in products:
            result = self.validate(product)

            if result.is_valid:
                valid_products.append(product)
                if result.warnings:
                    logger.warning(
                        f"Producto {product.get('sku', 'UNKNOWN')} tiene warnings: "
                        f"{[w.message for w in result.warnings]}"
                    )
            else:
                invalid_products.append((product, result))
                logger.error(
                    f"Producto inválido {product.get('sku', 'UNKNOWN')}: "
                    f"{[e.message for e in result.errors]}"
                )
                if not skip_invalid:
                    valid_products.append(product)

        logger.info(
            f"Validación completada: {len(valid_products)} válidos, "
            f"{len(invalid_products)} inválidos"
        )

        return valid_products, invalid_products

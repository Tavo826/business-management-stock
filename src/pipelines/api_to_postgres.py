"""
Pipeline ETL: API → PostgreSQL.
Orquesta la extracción, transformación y carga de productos.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from src.extractors.api_extractor import APIExtractor
from src.loaders.postgres_loader import PostgresLoader
from src.transformers.cleaners import TextCleaner
from src.transformers.normalizers import ProductNormalizer
from src.transformers.validators import ProductValidator

logger = logging.getLogger(__name__)


@dataclass
class PipelineResult:
    """Resultado de la ejecución del pipeline."""

    success: bool
    start_time: datetime
    end_time: datetime
    extracted: int = 0
    validated: int = 0
    cleaned: int = 0
    normalized: int = 0
    inserted: int = 0
    updated: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "extracted": self.extracted,
            "validated": self.validated,
            "cleaned": self.cleaned,
            "normalized": self.normalized,
            "inserted": self.inserted,
            "updated": self.updated,
            "errors_count": len(self.errors),
            "warnings_count": len(self.warnings),
        }


class APIToPostgresPipeline:
    """
    Pipeline completo para ETL desde API hasta PostgreSQL.

    Pasos:
    1. Extract: Obtener datos de la API
    2. Validate: Verificar integridad de datos
    3. Clean: Limpiar texto y normalizar formatos
    4. Normalize: Estandarizar categorías, precios, duplicados
    5. Load: Cargar a PostgreSQL con upsert
    """

    def __init__(
        self,
        extractor: Optional[APIExtractor] = None,
        validator: Optional[ProductValidator] = None,
        cleaner: Optional[TextCleaner] = None,
        normalizer: Optional[ProductNormalizer] = None,
        loader: Optional[PostgresLoader] = None,
    ):
        self.extractor = extractor or APIExtractor()
        self.validator = validator or ProductValidator()
        self.cleaner = cleaner or TextCleaner()
        self.normalizer = normalizer or ProductNormalizer()
        self.loader = loader or PostgresLoader()

    async def run(
        self,
        endpoint: str = "/products",
        skip_invalid: bool = True,
        handle_duplicates: bool = True,
    ) -> PipelineResult:
        """
        Ejecuta el pipeline completo.

        Args:
            endpoint: Endpoint de la API para productos
            skip_invalid: Si omitir productos inválidos
            handle_duplicates: Si manejar duplicados

        Returns:
            PipelineResult con estadísticas de ejecución
        """
        start_time = datetime.now()
        errors: list[str] = []
        warnings: list[str] = []

        try:
            # EXTRACT
            logger.info("Starting extraction...")
            async with self.extractor as extractor:
                raw_products = await extractor.fetch_all_products(endpoint)

            extracted_count = len(raw_products)
            logger.info(f"Extracted {extracted_count} products")

            if not raw_products:
                return PipelineResult(
                    success=True,
                    start_time=start_time,
                    end_time=datetime.now(),
                    extracted=0,
                    warnings=["No products found in API"],
                )

            # VALIDATE
            logger.info("Starting validation...")
            valid_products, invalid_products = self.validator.validate_batch(
                raw_products, skip_invalid=skip_invalid
            )
            validated_count = len(valid_products)

            for product, result in invalid_products:
                sku = product.get("sku", "UNKNOWN")
                for error in result.errors:
                    errors.append(f"[{sku}] {error.message}")

            logger.info(f"Validated: {validated_count} valid, {len(invalid_products)} invalid")

            # CLEAN
            logger.info("Starting cleaning...")
            cleaned_products = self.cleaner.clean_batch(valid_products)
            cleaned_count = len(cleaned_products)
            logger.info(f"Cleaned {cleaned_count} products")

            # NORMALIZE
            logger.info("Starting normalization...")
            normalized_products = self.normalizer.normalize_batch(
                cleaned_products, handle_duplicates=handle_duplicates
            )
            normalized_count = len(normalized_products)
            logger.info(f"Normalized {normalized_count} products")

            # LOAD
            logger.info("Starting load to PostgreSQL...")
            async with self.loader as loader:
                inserted, updated, load_errors = await loader.upsert_batch(
                    normalized_products
                )
                errors.extend([f"Load error: {e}" for e in load_errors])

            logger.info(f"Loaded: {inserted} inserted, {updated} updated")

            return PipelineResult(
                success=len(errors) == 0,
                start_time=start_time,
                end_time=datetime.now(),
                extracted=extracted_count,
                validated=validated_count,
                cleaned=cleaned_count,
                normalized=normalized_count,
                inserted=inserted,
                updated=updated,
                errors=errors,
                warnings=warnings,
            )

        except Exception as e:
            logger.exception(f"Pipeline failed: {e}")
            return PipelineResult(
                success=False,
                start_time=start_time,
                end_time=datetime.now(),
                errors=[str(e)],
            )

    async def run_incremental(
        self,
        endpoint: str = "/products",
        since: Optional[datetime] = None,
    ) -> PipelineResult:
        """
        Ejecuta el pipeline de forma incremental.

        Solo procesa productos modificados desde una fecha.

        Args:
            endpoint: Endpoint de la API
            since: Fecha desde la cual buscar actualizaciones

        Returns:
            PipelineResult
        """
        # Agregar parámetro de fecha si la API lo soporta
        params = {}
        if since:
            params["updated_since"] = since.isoformat()

        start_time = datetime.now()

        try:
            async with self.extractor as extractor:
                raw_products = await extractor.fetch_products(endpoint, params)

            if not raw_products:
                return PipelineResult(
                    success=True,
                    start_time=start_time,
                    end_time=datetime.now(),
                    extracted=0,
                    warnings=["No new/updated products found"],
                )

            # Resto del pipeline igual que run()
            return await self._process_products(raw_products, start_time)

        except Exception as e:
            logger.exception(f"Incremental pipeline failed: {e}")
            return PipelineResult(
                success=False,
                start_time=start_time,
                end_time=datetime.now(),
                errors=[str(e)],
            )

    async def _process_products(
        self,
        raw_products: list[dict[str, Any]],
        start_time: datetime,
    ) -> PipelineResult:
        """Procesa productos a través de validate -> clean -> normalize -> load."""
        errors: list[str] = []

        # Validate
        valid_products, invalid_products = self.validator.validate_batch(
            raw_products, skip_invalid=True
        )
        for product, result in invalid_products:
            sku = product.get("sku", "UNKNOWN")
            for error in result.errors:
                errors.append(f"[{sku}] {error.message}")

        # Clean
        cleaned_products = self.cleaner.clean_batch(valid_products)

        # Normalize
        normalized_products = self.normalizer.normalize_batch(cleaned_products)

        # Load
        async with self.loader as loader:
            inserted, updated, load_errors = await loader.upsert_batch(normalized_products)
            errors.extend([f"Load error: {e}" for e in load_errors])

        return PipelineResult(
            success=len(errors) == 0,
            start_time=start_time,
            end_time=datetime.now(),
            extracted=len(raw_products),
            validated=len(valid_products),
            cleaned=len(cleaned_products),
            normalized=len(normalized_products),
            inserted=inserted,
            updated=updated,
            errors=errors,
        )

    async def run_single_product(
        self,
        product_data: dict[str, Any],
    ) -> PipelineResult:
        """
        Procesa un solo producto.

        Útil para webhooks o actualizaciones individuales.

        Args:
            product_data: Datos del producto

        Returns:
            PipelineResult
        """
        start_time = datetime.now()

        try:
            # Validate
            result = self.validator.validate(product_data)
            if not result.is_valid:
                return PipelineResult(
                    success=False,
                    start_time=start_time,
                    end_time=datetime.now(),
                    extracted=1,
                    errors=[e.message for e in result.errors],
                )

            # Clean
            cleaned = self.cleaner.clean_product(product_data)

            # Normalize
            normalized = self.normalizer.normalize_product(cleaned)

            # Load
            async with self.loader as loader:
                await loader.upsert_product(normalized)

            return PipelineResult(
                success=True,
                start_time=start_time,
                end_time=datetime.now(),
                extracted=1,
                validated=1,
                cleaned=1,
                normalized=1,
                inserted=1,
            )

        except Exception as e:
            logger.exception(f"Single product pipeline failed: {e}")
            return PipelineResult(
                success=False,
                start_time=start_time,
                end_time=datetime.now(),
                errors=[str(e)],
            )

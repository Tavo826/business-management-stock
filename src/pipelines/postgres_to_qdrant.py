"""
Pipeline: PostgreSQL → Qdrant.
Genera embeddings y los almacena en la base de datos vectorial.
"""
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional

from src.embeddings.generator import EmbeddingGenerator
from src.loaders.postgres_loader import PostgresLoader
from src.loaders.qdrant_loader import QdrantLoader
from src.models.product import ProductDB

logger = logging.getLogger(__name__)


@dataclass
class EmbeddingPipelineResult:
    """Resultado de la ejecución del pipeline de embeddings."""

    success: bool
    start_time: datetime
    end_time: datetime
    total_products: int = 0
    products_needing_update: int = 0
    embeddings_generated: int = 0
    embeddings_upserted: int = 0
    metadata_only_updates: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float:
        return (self.end_time - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        return {
            "success": self.success,
            "duration_seconds": self.duration_seconds,
            "total_products": self.total_products,
            "products_needing_update": self.products_needing_update,
            "embeddings_generated": self.embeddings_generated,
            "embeddings_upserted": self.embeddings_upserted,
            "metadata_only_updates": self.metadata_only_updates,
            "errors_count": len(self.errors),
        }


class PostgresToQdrantPipeline:
    """
    Pipeline para sincronizar productos de PostgreSQL a Qdrant.

    Comportamiento inteligente:
    - Solo regenera embeddings si cambian campos textuales (name, description, attributes)
    - Actualiza solo metadata (price, stock) sin regenerar embeddings si solo esos cambian
    - Usa hash de texto para detectar cambios eficientemente
    """

    def __init__(
        self,
        postgres_loader: Optional[PostgresLoader] = None,
        embedding_generator: Optional[EmbeddingGenerator] = None,
        qdrant_loader: Optional[QdrantLoader] = None,
    ):
        self.postgres_loader = postgres_loader or PostgresLoader()
        self.embedding_generator = embedding_generator or EmbeddingGenerator()
        self.qdrant_loader = qdrant_loader or QdrantLoader()

    async def run(
        self,
        force_regenerate: bool = False,
        batch_size: int = 50,
    ) -> EmbeddingPipelineResult:
        """
        Ejecuta el pipeline de sincronización.

        Args:
            force_regenerate: Si True, regenera todos los embeddings
            batch_size: Tamaño del batch para procesamiento

        Returns:
            EmbeddingPipelineResult con estadísticas
        """
        start_time = datetime.now()
        errors: list[str] = []

        try:
            async with self.postgres_loader as pg_loader:
                async with self.qdrant_loader as qdrant_loader:
                    async with self.embedding_generator as emb_generator:
                        # Obtener todos los productos de PostgreSQL
                        all_products = await pg_loader.get_all_products()
                        total_products = len(all_products)
                        logger.info(f"Found {total_products} products in PostgreSQL")

                        if total_products == 0:
                            return EmbeddingPipelineResult(
                                success=True,
                                start_time=start_time,
                                end_time=datetime.now(),
                                total_products=0,
                            )

                        # Obtener hashes existentes de Qdrant
                        existing_hashes = {}
                        if not force_regenerate:
                            existing_hashes = await qdrant_loader.get_existing_hashes()
                            logger.info(
                                f"Found {len(existing_hashes)} existing embeddings in Qdrant"
                            )

                        # Clasificar productos
                        products_to_embed: list[ProductDB] = []
                        products_metadata_only: list[ProductDB] = []

                        for product in all_products:
                            current_hash = product.compute_text_hash()
                            existing_hash = existing_hashes.get(product.sku)

                            if force_regenerate or existing_hash is None:
                                # Nuevo producto o regeneración forzada
                                products_to_embed.append(product)
                            elif existing_hash != current_hash:
                                # Texto cambió -> regenerar embedding
                                products_to_embed.append(product)
                            else:
                                # Solo actualizar metadata (price, stock)
                                products_metadata_only.append(product)

                        logger.info(
                            f"Products needing embedding: {len(products_to_embed)}, "
                            f"metadata-only updates: {len(products_metadata_only)}"
                        )

                        # Generar embeddings para productos que lo necesitan
                        embeddings_generated = 0
                        embeddings_upserted = 0

                        if products_to_embed:
                            logger.info(
                                f"Generating embeddings for {len(products_to_embed)} products..."
                            )
                            embeddings = await emb_generator.generate_embeddings_batch(
                                products_to_embed
                            )
                            embeddings_generated = len(embeddings)

                            if embeddings:
                                embeddings_upserted = await qdrant_loader.upsert_batch(
                                    embeddings, batch_size=batch_size
                                )

                        # Actualizar metadata para productos sin cambios textuales
                        metadata_updates = 0
                        if products_metadata_only:
                            logger.info(
                                f"Updating metadata for {len(products_metadata_only)} products..."
                            )
                            updates = [
                                {
                                    "point_id": f"product-{p.id}",
                                    "price": float(p.price),
                                    "stock": p.stock,
                                }
                                for p in products_metadata_only
                            ]
                            metadata_updates = await qdrant_loader.update_metadata_batch(
                                updates
                            )

                        return EmbeddingPipelineResult(
                            success=True,
                            start_time=start_time,
                            end_time=datetime.now(),
                            total_products=total_products,
                            products_needing_update=len(products_to_embed),
                            embeddings_generated=embeddings_generated,
                            embeddings_upserted=embeddings_upserted,
                            metadata_only_updates=metadata_updates,
                            errors=errors,
                        )

        except Exception as e:
            logger.exception(f"Embedding pipeline failed: {e}")
            return EmbeddingPipelineResult(
                success=False,
                start_time=start_time,
                end_time=datetime.now(),
                errors=[str(e)],
            )

    async def run_for_products(
        self,
        skus: list[str],
        force_regenerate: bool = False,
    ) -> EmbeddingPipelineResult:
        """
        Ejecuta el pipeline para productos específicos.

        Args:
            skus: Lista de SKUs a procesar
            force_regenerate: Si regenerar aunque no haya cambios

        Returns:
            EmbeddingPipelineResult
        """
        start_time = datetime.now()

        try:
            async with self.postgres_loader as pg_loader:
                async with self.qdrant_loader as qdrant_loader:
                    async with self.embedding_generator as emb_generator:
                        products = []
                        for sku in skus:
                            product = await pg_loader.get_product_by_sku(sku)
                            if product:
                                products.append(product)

                        if not products:
                            return EmbeddingPipelineResult(
                                success=True,
                                start_time=start_time,
                                end_time=datetime.now(),
                                total_products=0,
                                errors=[f"No products found for SKUs: {skus}"],
                            )

                        # Obtener hashes existentes
                        existing_hashes = {}
                        if not force_regenerate:
                            existing_hashes = await qdrant_loader.get_existing_hashes()

                        # Filtrar productos que necesitan embedding
                        products_to_embed = []
                        for product in products:
                            if force_regenerate or self.embedding_generator.should_regenerate(
                                product, existing_hashes.get(product.sku)
                            ):
                                products_to_embed.append(product)

                        if not products_to_embed:
                            return EmbeddingPipelineResult(
                                success=True,
                                start_time=start_time,
                                end_time=datetime.now(),
                                total_products=len(products),
                                products_needing_update=0,
                            )

                        # Generar y guardar embeddings
                        embeddings = await emb_generator.generate_embeddings_batch(
                            products_to_embed
                        )
                        upserted = await qdrant_loader.upsert_batch(embeddings)

                        return EmbeddingPipelineResult(
                            success=True,
                            start_time=start_time,
                            end_time=datetime.now(),
                            total_products=len(products),
                            products_needing_update=len(products_to_embed),
                            embeddings_generated=len(embeddings),
                            embeddings_upserted=upserted,
                        )

        except Exception as e:
            logger.exception(f"Pipeline for specific products failed: {e}")
            return EmbeddingPipelineResult(
                success=False,
                start_time=start_time,
                end_time=datetime.now(),
                errors=[str(e)],
            )

    async def sync_deletions(self) -> int:
        """
        Sincroniza eliminaciones: remueve de Qdrant productos que ya no existen en PostgreSQL.

        Returns:
            Número de productos eliminados de Qdrant
        """
        async with self.postgres_loader as pg_loader:
            async with self.qdrant_loader as qdrant_loader:
                # Obtener SKUs de ambas fuentes
                pg_products = await pg_loader.get_all_products()
                pg_skus = {p.sku for p in pg_products}

                qdrant_hashes = await qdrant_loader.get_existing_hashes()
                qdrant_skus = set(qdrant_hashes.keys())

                # SKUs en Qdrant que ya no existen en PostgreSQL
                to_delete = qdrant_skus - pg_skus

                deleted = 0
                for sku in to_delete:
                    await qdrant_loader.delete_by_sku(sku)
                    deleted += 1
                    logger.info(f"Deleted {sku} from Qdrant")

                logger.info(f"Sync deletions complete: removed {deleted} products")
                return deleted

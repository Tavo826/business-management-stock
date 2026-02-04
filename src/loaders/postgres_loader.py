"""
Loader para PostgreSQL.
Implementa carga de productos con upsert y tracking de cambios.
"""
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional, Sequence

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from config import settings
from src.models.product import Base, ProductCreate, ProductDB

logger = logging.getLogger(__name__)


class PostgresLoaderError(Exception):
    """Error base para el loader de PostgreSQL."""

    pass


class PostgresLoader:
    """
    Loader async para PostgreSQL con SQLAlchemy.

    Características:
    - Upsert (insert or update on conflict)
    - Transacciones atómicas
    - Tracking de cambios (created_at, updated_at)
    - Batch processing
    """

    def __init__(
        self,
        dsn: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
    ):
        self.dsn = dsn or settings.postgres_dsn

        self.engine = create_async_engine(
            self.dsn,
            pool_size=pool_size,
            max_overflow=max_overflow,
            echo=settings.log_level == "DEBUG",
        )

        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def init_db(self) -> None:
        """Crea las tablas si no existen."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables initialized")

    async def close(self) -> None:
        """Cierra el engine y las conexiones."""
        await self.engine.dispose()
        logger.info("Database connections closed")

    async def __aenter__(self) -> "PostgresLoader":
        await self.init_db()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @asynccontextmanager
    async def session(self) -> AsyncIterator[AsyncSession]:
        """Context manager para sesiones."""
        async with self.async_session() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    async def upsert_product(
        self,
        product_data: dict[str, Any],
        session: Optional[AsyncSession] = None,
    ) -> ProductDB:
        """
        Inserta o actualiza un producto (upsert).

        Args:
            product_data: Datos del producto
            session: Sesión existente o None para crear una nueva

        Returns:
            Producto insertado/actualizado
        """

        async def _upsert(sess: AsyncSession) -> ProductDB:
            # Validar con Pydantic
            validated = ProductCreate(**product_data)

            # Preparar datos para insert
            insert_data = validated.model_dump()

            # Statement de upsert (ON CONFLICT DO UPDATE)
            stmt = insert(ProductDB).values(**insert_data)

            # Columnas a actualizar en conflicto (excluir sku que es la clave)
            update_columns = {
                col.name: col
                for col in stmt.excluded
                if col.name not in ("sku", "id", "created_at")
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=["sku"],
                set_=update_columns,
            ).returning(ProductDB)

            result = await sess.execute(stmt)
            product = result.scalar_one()

            # Calcular y actualizar text_hash
            product.text_hash = product.compute_text_hash()
            sess.add(product)

            return product

        if session:
            return await _upsert(session)
        else:
            async with self.session() as sess:
                return await _upsert(sess)

    async def upsert_batch(
        self,
        products: list[dict[str, Any]],
        batch_size: int = 50,
    ) -> tuple[int, int, list[str]]:
        """
        Inserta/actualiza un lote de productos.

        Args:
            products: Lista de productos
            batch_size: Tamaño del batch para commits

        Returns:
            Tupla de (insertados, actualizados, errores_skus)
        """
        inserted = 0
        updated = 0
        errors: list[str] = []

        async with self.session() as session:
            for i, product_data in enumerate(products):
                try:
                    sku = product_data.get("sku", "UNKNOWN")

                    # Verificar si existe
                    exists = await session.execute(
                        select(ProductDB.id).where(ProductDB.sku == sku)
                    )
                    is_update = exists.scalar_one_or_none() is not None

                    await self.upsert_product(product_data, session)

                    if is_update:
                        updated += 1
                    else:
                        inserted += 1

                    # Flush periódico para batch
                    if (i + 1) % batch_size == 0:
                        await session.flush()
                        logger.info(f"Processed {i + 1}/{len(products)} products")

                except Exception as e:
                    sku = product_data.get("sku", "UNKNOWN")
                    logger.error(f"Error upserting product {sku}: {e}")
                    errors.append(sku)

        logger.info(
            f"Batch complete: {inserted} inserted, {updated} updated, "
            f"{len(errors)} errors"
        )
        return inserted, updated, errors

    async def get_product_by_sku(self, sku: str) -> Optional[ProductDB]:
        """Obtiene un producto por SKU."""
        async with self.session() as session:
            result = await session.execute(
                select(ProductDB).where(ProductDB.sku == sku)
            )
            return result.scalar_one_or_none()

    async def get_all_products(
        self,
        limit: Optional[int] = None,
        offset: int = 0,
    ) -> Sequence[ProductDB]:
        """
        Obtiene todos los productos.

        Args:
            limit: Límite de resultados
            offset: Offset para paginación

        Returns:
            Lista de productos
        """
        async with self.session() as session:
            stmt = select(ProductDB).offset(offset)
            if limit:
                stmt = stmt.limit(limit)

            result = await session.execute(stmt)
            return result.scalars().all()

    async def get_products_for_embedding(
        self,
        existing_hashes: Optional[dict[str, str]] = None,
    ) -> Sequence[ProductDB]:
        """
        Obtiene productos que necesitan (re)generar embeddings.

        Solo retorna productos cuyo text_hash ha cambiado o no existe.

        Args:
            existing_hashes: Diccionario de {sku: text_hash} existentes en Qdrant

        Returns:
            Lista de productos que necesitan embeddings
        """
        async with self.session() as session:
            result = await session.execute(select(ProductDB))
            all_products = result.scalars().all()

        if not existing_hashes:
            return all_products

        # Filtrar productos cuyo hash ha cambiado
        products_to_embed = []
        for product in all_products:
            current_hash = product.compute_text_hash()
            existing_hash = existing_hashes.get(product.sku)

            if existing_hash != current_hash:
                products_to_embed.append(product)
                logger.debug(
                    f"Product {product.sku} needs embedding: "
                    f"hash changed from {existing_hash} to {current_hash}"
                )

        logger.info(
            f"{len(products_to_embed)}/{len(all_products)} products need embedding update"
        )
        return products_to_embed

    async def get_products_by_category(self, category: str) -> Sequence[ProductDB]:
        """Obtiene productos por categoría."""
        async with self.session() as session:
            result = await session.execute(
                select(ProductDB).where(ProductDB.category == category)
            )
            return result.scalars().all()

    async def delete_product(self, sku: str) -> bool:
        """
        Elimina un producto por SKU.

        Returns:
            True si se eliminó, False si no existía
        """
        async with self.session() as session:
            result = await session.execute(
                select(ProductDB).where(ProductDB.sku == sku)
            )
            product = result.scalar_one_or_none()

            if product:
                await session.delete(product)
                logger.info(f"Deleted product {sku}")
                return True

            return False

    async def get_stats(self) -> dict[str, Any]:
        """Obtiene estadísticas de la base de datos."""
        async with self.session() as session:
            # Total de productos
            total_result = await session.execute(
                text("SELECT COUNT(*) FROM products")
            )
            total = total_result.scalar()

            # Por categoría
            category_result = await session.execute(
                text(
                    "SELECT category, COUNT(*) as count "
                    "FROM products GROUP BY category ORDER BY count DESC"
                )
            )
            by_category = {row[0]: row[1] for row in category_result}

            # Stock total
            stock_result = await session.execute(
                text("SELECT SUM(stock) FROM products")
            )
            total_stock = stock_result.scalar() or 0

            return {
                "total_products": total,
                "by_category": by_category,
                "total_stock": total_stock,
            }

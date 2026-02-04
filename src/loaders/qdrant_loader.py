"""
Loader para Qdrant (base de datos vectorial).
Implementa carga y búsqueda de embeddings de productos.
"""
import logging
from typing import Any, Optional

from qdrant_client import AsyncQdrantClient, models
from qdrant_client.http.exceptions import UnexpectedResponse

from config import settings
from src.models.product import ProductEmbedding

logger = logging.getLogger(__name__)


class QdrantLoaderError(Exception):
    """Error base para el loader de Qdrant."""

    pass


class QdrantLoader:
    """
    Loader async para Qdrant.

    Características:
    - Creación automática de colección
    - Upsert de vectores con metadata
    - Actualización de metadata sin regenerar embeddings
    - Búsqueda semántica
    """

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        api_key: Optional[str] = None,
        collection_name: Optional[str] = None,
        vector_size: Optional[int] = None,
    ):
        self.host = host or settings.qdrant_host
        self.port = port or settings.qdrant_port
        self.api_key = api_key or settings.qdrant_api_key
        self.collection_name = collection_name or settings.qdrant_collection
        self.vector_size = vector_size or settings.embedding_dimensions

        self._client: Optional[AsyncQdrantClient] = None

    async def _get_client(self) -> AsyncQdrantClient:
        """Obtiene o crea el cliente de Qdrant."""
        if self._client is None:
            self._client = AsyncQdrantClient(
                host=self.host,
                port=self.port,
                api_key=self.api_key,
            )
        return self._client

    async def close(self) -> None:
        """Cierra el cliente de Qdrant."""
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> "QdrantLoader":
        await self._get_client()
        await self.ensure_collection()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def ensure_collection(self) -> None:
        """
        Crea la colección si no existe.
        Usa distancia coseno para embeddings semánticos.
        """
        client = await self._get_client()

        try:
            collections = await client.get_collections()
            collection_names = [c.name for c in collections.collections]

            if self.collection_name not in collection_names:
                await client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(
                        size=self.vector_size,
                        distance=models.Distance.COSINE,
                    ),
                )
                logger.info(f"Created collection '{self.collection_name}'")

                # Crear índices para payload
                await client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="sku",
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
                await client.create_payload_index(
                    collection_name=self.collection_name,
                    field_name="category",
                    field_schema=models.PayloadSchemaType.KEYWORD,
                )
                logger.info("Created payload indexes")
            else:
                logger.info(f"Collection '{self.collection_name}' already exists")

        except UnexpectedResponse as e:
            raise QdrantLoaderError(f"Error creating collection: {e}") from e

    async def upsert_embedding(self, embedding: ProductEmbedding) -> None:
        """
        Inserta o actualiza un embedding.

        Args:
            embedding: ProductEmbedding a insertar
        """
        client = await self._get_client()

        point = models.PointStruct(
            id=embedding.id,
            vector=embedding.vector,
            payload={
                "sku": embedding.sku,
                "product_id": str(embedding.product_id),
                "category": embedding.category,
                "price": float(embedding.price),
                "stock": embedding.stock,
                "text": embedding.text,
                "text_hash": embedding.text_hash,
            },
        )

        await client.upsert(
            collection_name=self.collection_name,
            points=[point],
        )

    async def upsert_batch(
        self,
        embeddings: list[ProductEmbedding],
        batch_size: int = 100,
    ) -> int:
        """
        Inserta/actualiza un lote de embeddings.

        Args:
            embeddings: Lista de embeddings
            batch_size: Tamaño del batch

        Returns:
            Número de embeddings procesados
        """
        client = await self._get_client()
        processed = 0

        for i in range(0, len(embeddings), batch_size):
            batch = embeddings[i : i + batch_size]

            points = [
                models.PointStruct(
                    id=emb.id,
                    vector=emb.vector,
                    payload={
                        "sku": emb.sku,
                        "product_id": str(emb.product_id),
                        "category": emb.category,
                        "price": float(emb.price),
                        "stock": emb.stock,
                        "text": emb.text,
                        "text_hash": emb.text_hash,
                    },
                )
                for emb in batch
            ]

            await client.upsert(
                collection_name=self.collection_name,
                points=points,
            )

            processed += len(batch)
            logger.info(f"Upserted {processed}/{len(embeddings)} embeddings")

        return processed

    async def update_metadata(
        self,
        point_id: str,
        price: Optional[float] = None,
        stock: Optional[int] = None,
    ) -> None:
        """
        Actualiza metadata sin regenerar el embedding.

        Útil para actualizar precio/stock sin recalcular vectores.

        Args:
            point_id: ID del punto (product-<uuid>)
            price: Nuevo precio (opcional)
            stock: Nuevo stock (opcional)
        """
        if price is None and stock is None:
            return

        client = await self._get_client()

        payload_update = {}
        if price is not None:
            payload_update["price"] = price
        if stock is not None:
            payload_update["stock"] = stock

        await client.set_payload(
            collection_name=self.collection_name,
            payload=payload_update,
            points=[point_id],
        )

        logger.debug(f"Updated metadata for {point_id}: {payload_update}")

    async def update_metadata_batch(
        self,
        updates: list[dict[str, Any]],
    ) -> int:
        """
        Actualiza metadata para múltiples puntos.

        Args:
            updates: Lista de {"point_id": str, "price": float, "stock": int}

        Returns:
            Número de puntos actualizados
        """
        client = await self._get_client()
        updated = 0

        for update in updates:
            point_id = update.get("point_id")
            if not point_id:
                continue

            payload = {}
            if "price" in update:
                payload["price"] = update["price"]
            if "stock" in update:
                payload["stock"] = update["stock"]

            if payload:
                await client.set_payload(
                    collection_name=self.collection_name,
                    payload=payload,
                    points=[point_id],
                )
                updated += 1

        logger.info(f"Updated metadata for {updated} points")
        return updated

    async def get_existing_hashes(self) -> dict[str, str]:
        """
        Obtiene los hashes de texto existentes para todos los productos.

        Returns:
            Diccionario de {sku: text_hash}
        """
        client = await self._get_client()
        hashes = {}

        # Scroll por todos los puntos
        offset = None
        while True:
            results, offset = await client.scroll(
                collection_name=self.collection_name,
                limit=100,
                offset=offset,
                with_payload=["sku", "text_hash"],
                with_vectors=False,
            )

            for point in results:
                if point.payload:
                    sku = point.payload.get("sku")
                    text_hash = point.payload.get("text_hash")
                    if sku and text_hash:
                        hashes[sku] = text_hash

            if offset is None:
                break

        logger.info(f"Retrieved {len(hashes)} existing hashes")
        return hashes

    async def search(
        self,
        query_vector: list[float],
        limit: int = 10,
        category_filter: Optional[str] = None,
        min_stock: Optional[int] = None,
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
    ) -> list[dict[str, Any]]:
        """
        Búsqueda semántica de productos.

        Args:
            query_vector: Vector de búsqueda
            limit: Número máximo de resultados
            category_filter: Filtrar por categoría
            min_stock: Stock mínimo
            min_price: Precio mínimo
            max_price: Precio máximo

        Returns:
            Lista de resultados con score y payload
        """
        client = await self._get_client()

        # Construir filtros
        must_conditions = []

        if category_filter:
            must_conditions.append(
                models.FieldCondition(
                    key="category",
                    match=models.MatchValue(value=category_filter),
                )
            )

        if min_stock is not None:
            must_conditions.append(
                models.FieldCondition(
                    key="stock",
                    range=models.Range(gte=min_stock),
                )
            )

        if min_price is not None:
            must_conditions.append(
                models.FieldCondition(
                    key="price",
                    range=models.Range(gte=min_price),
                )
            )

        if max_price is not None:
            must_conditions.append(
                models.FieldCondition(
                    key="price",
                    range=models.Range(lte=max_price),
                )
            )

        query_filter = None
        if must_conditions:
            query_filter = models.Filter(must=must_conditions)

        results = await client.search(
            collection_name=self.collection_name,
            query_vector=query_vector,
            query_filter=query_filter,
            limit=limit,
        )

        return [
            {
                "id": result.id,
                "score": result.score,
                "payload": result.payload,
            }
            for result in results
        ]

    async def delete_by_sku(self, sku: str) -> bool:
        """
        Elimina un punto por SKU.

        Returns:
            True si se eliminó
        """
        client = await self._get_client()

        await client.delete(
            collection_name=self.collection_name,
            points_selector=models.FilterSelector(
                filter=models.Filter(
                    must=[
                        models.FieldCondition(
                            key="sku",
                            match=models.MatchValue(value=sku),
                        )
                    ]
                )
            ),
        )

        logger.info(f"Deleted point for SKU: {sku}")
        return True

    async def get_collection_info(self) -> dict[str, Any]:
        """Obtiene información de la colección."""
        client = await self._get_client()

        info = await client.get_collection(self.collection_name)

        return {
            "name": self.collection_name,
            "vectors_count": info.vectors_count,
            "points_count": info.points_count,
            "status": info.status.value,
        }

"""
Generador de embeddings usando Voyage AI (partner de Anthropic).
Construye textos semánticamente ricos y genera vectores.
"""
import asyncio
import logging
from typing import Optional, Sequence

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings
from src.models.product import ProductDB, ProductEmbedding

logger = logging.getLogger(__name__)


class EmbeddingGeneratorError(Exception):
    """Error base para el generador de embeddings."""

    pass


class EmbeddingGenerator:
    """
    Generador de embeddings usando Voyage AI.

    Características:
    - Construcción de texto semánticamente rico
    - Batch processing para eficiencia
    - Retry automático con backoff
    - Detección de cambios via hash
    """

    # Voyage AI endpoint
    VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        dimensions: Optional[int] = None,
        batch_size: int = 8,
    ):
        self.api_key = api_key or settings.voyage_api_key or settings.anthropic_api_key
        self.model = model or settings.embedding_model
        self.dimensions = dimensions or settings.embedding_dimensions
        self.batch_size = batch_size

        if not self.api_key:
            raise EmbeddingGeneratorError(
                "API key required. Set VOYAGE_API_KEY or ANTHROPIC_API_KEY"
            )

        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Obtiene o crea el cliente HTTP."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(60.0),
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
            )
        return self._client

    async def close(self) -> None:
        """Cierra el cliente HTTP."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "EmbeddingGenerator":
        await self._get_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    def build_embedding_text(self, product: ProductDB) -> str:
        """
        Construye un texto rico semánticamente para el embedding.

        Incluye información relevante para búsquedas pero evita
        datos que cambian frecuentemente (stock, precio).

        Args:
            product: Producto de la base de datos

        Returns:
            Texto optimizado para embedding
        """
        parts = []

        # Nombre del producto (más importante)
        parts.append(f"Producto: {product.name}")

        # Categoría para contexto
        parts.append(f"Categoría: {product.category}")

        # Descripción si existe
        if product.description:
            parts.append(f"Descripción: {product.description}")

        # Unidad de medida
        if product.unit and product.unit != "unidad":
            parts.append(f"Unidad: {product.unit}")

        # Atributos textuales (color, talla, material, etc.)
        if product.attributes:
            text_attrs = []
            for key, value in product.attributes.items():
                if isinstance(value, str) and value.strip():
                    text_attrs.append(f"{key}: {value}")
                elif isinstance(value, list):
                    # Listas de valores (ej: colores disponibles)
                    str_values = [str(v) for v in value if v]
                    if str_values:
                        text_attrs.append(f"{key}: {', '.join(str_values)}")

            if text_attrs:
                parts.append("Características: " + "; ".join(text_attrs))

        return ". ".join(parts)

    @retry(
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def _call_voyage_api(
        self,
        texts: list[str],
    ) -> list[list[float]]:
        """
        Llama a la API de Voyage AI para generar embeddings.

        Args:
            texts: Lista de textos a vectorizar

        Returns:
            Lista de vectores de embedding
        """
        client = await self._get_client()

        payload = {
            "input": texts,
            "model": self.model,
        }

        try:
            response = await client.post(self.VOYAGE_API_URL, json=payload)
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            logger.error(f"Voyage API error: {e.response.status_code} - {e.response.text}")
            raise EmbeddingGeneratorError(f"API error: {e.response.text}") from e

        data = response.json()

        # Extraer embeddings ordenados por índice
        embeddings_data = sorted(data["data"], key=lambda x: x["index"])
        embeddings = [item["embedding"] for item in embeddings_data]

        return embeddings

    async def generate_embedding(self, product: ProductDB) -> ProductEmbedding:
        """
        Genera el embedding para un solo producto.

        Args:
            product: Producto de la base de datos

        Returns:
            ProductEmbedding con el vector generado
        """
        text = self.build_embedding_text(product)
        text_hash = product.compute_text_hash()

        embeddings = await self._call_voyage_api([text])

        return ProductEmbedding(
            id=f"product-{product.id}",
            vector=embeddings[0],
            sku=product.sku,
            product_id=product.id,
            category=product.category,
            price=product.price,
            stock=product.stock,
            text=text,
            text_hash=text_hash,
        )

    async def generate_embeddings_batch(
        self,
        products: Sequence[ProductDB],
    ) -> list[ProductEmbedding]:
        """
        Genera embeddings para un lote de productos.

        Procesa en batches para respetar límites de la API.

        Args:
            products: Lista de productos

        Returns:
            Lista de ProductEmbedding
        """
        results: list[ProductEmbedding] = []

        for i in range(0, len(products), self.batch_size):
            batch = products[i : i + self.batch_size]

            # Construir textos para el batch
            texts = [self.build_embedding_text(p) for p in batch]

            logger.info(
                f"Generating embeddings for batch {i // self.batch_size + 1} "
                f"({len(batch)} products)"
            )

            try:
                embeddings = await self._call_voyage_api(texts)

                for product, vector, text in zip(batch, embeddings, texts):
                    results.append(
                        ProductEmbedding(
                            id=f"product-{product.id}",
                            vector=vector,
                            sku=product.sku,
                            product_id=product.id,
                            category=product.category,
                            price=product.price,
                            stock=product.stock,
                            text=text,
                            text_hash=product.compute_text_hash(),
                        )
                    )

            except Exception as e:
                logger.error(f"Error generating embeddings for batch: {e}")
                # Continuar con el siguiente batch
                continue

            # Rate limiting suave
            if i + self.batch_size < len(products):
                await asyncio.sleep(0.5)

        logger.info(f"Generated {len(results)} embeddings from {len(products)} products")
        return results

    def should_regenerate(
        self,
        product: ProductDB,
        existing_hash: Optional[str],
    ) -> bool:
        """
        Determina si un producto necesita regenerar su embedding.

        Solo regenera si los campos textuales han cambiado.
        No regenera por cambios en stock o precio.

        Args:
            product: Producto actual
            existing_hash: Hash existente en Qdrant

        Returns:
            True si necesita regenerar
        """
        if existing_hash is None:
            return True

        current_hash = product.compute_text_hash()
        return current_hash != existing_hash

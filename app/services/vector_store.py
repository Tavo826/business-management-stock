import logging

import chromadb

from app.config import settings
from app.models.product import Product, ProductSearchResult
from app.services.embedding import embed_query, embed_texts, generate_product_text

logger = logging.getLogger(__name__)


class VectorStore:
    def __init__(self):
        self.client = chromadb.PersistentClient(path=settings.chroma_persist_dir)
        self.collection = self.client.get_or_create_collection(
            name=settings.chroma_collection_name,
            metadata={"hnsw:space": "cosine"},
        )

    def upsert_products(self, products: list[Product]) -> dict:
        """Insert or update products in ChromaDB.

        Returns a summary with counts of updated and re-embedded products.
        """
        if not products:
            return {"updated": 0, "embeddings_regenerated": 0}

        existing_ids = set(self.collection.get()["ids"])
        products_to_embed: list[Product] = []
        products_to_update_metadata: list[Product] = []

        existing_data = {}
        if existing_ids:
            result = self.collection.get(
                ids=list(existing_ids),
                include=["documents", "metadatas"],
            )
            for i, doc_id in enumerate(result["ids"]):
                existing_data[doc_id] = {
                    "document": result["documents"][i],
                    "metadata": result["metadatas"][i],
                }

        for product in products:
            product_id = str(product.id)
            new_text = generate_product_text(product.name, product.description, product.category)

            if product_id in existing_data:
                old_text = existing_data[product_id]["document"]
                if old_text != new_text:
                    products_to_embed.append(product)
                else:
                    products_to_update_metadata.append(product)
            else:
                products_to_embed.append(product)

        embeddings_regenerated = 0

        if products_to_embed:
            texts = [
                generate_product_text(p.name, p.description, p.category)
                for p in products_to_embed
            ]
            embeddings = embed_texts(texts)

            self.collection.upsert(
                ids=[str(p.id) for p in products_to_embed],
                documents=texts,
                embeddings=embeddings,
                metadatas=[self._build_metadata(p) for p in products_to_embed],
            )
            embeddings_regenerated = len(products_to_embed)
            logger.info("Re-embedded %d products", embeddings_regenerated)

        if products_to_update_metadata:
            self.collection.update(
                ids=[str(p.id) for p in products_to_update_metadata],
                metadatas=[self._build_metadata(p) for p in products_to_update_metadata],
            )
            logger.info("Updated metadata for %d products", len(products_to_update_metadata))

        current_ids = {str(p.id) for p in products}
        removed_ids = existing_ids - current_ids
        if removed_ids:
            self.collection.delete(ids=list(removed_ids))
            logger.info("Removed %d products no longer in Odoo", len(removed_ids))

        return {
            "updated": len(products_to_update_metadata) + embeddings_regenerated,
            "embeddings_regenerated": embeddings_regenerated,
        }

    def search(self, query: str, limit: int = 5, min_stock: float | None = None) -> list[ProductSearchResult]:
        """Search for products by semantic similarity.

        Optionally filter by minimum stock availability.
        """
        query_embedding = embed_query(query)

        where_filter = None
        if min_stock is not None:
            where_filter = {"stock": {"$gte": min_stock}}

        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=limit,
            where=where_filter,
            include=["metadatas", "distances"],
        )

        search_results = []
        if results["ids"] and results["ids"][0]:
            for i, product_id in enumerate(results["ids"][0]):
                metadata = results["metadatas"][0][i]
                distance = results["distances"][0][i]
                relevance_score = round(1 - distance, 4)

                search_results.append(
                    ProductSearchResult(
                        id=int(product_id),
                        name=metadata["name"],
                        price=metadata["price"],
                        stock=metadata["stock"],
                        description=metadata.get("description", ""),
                        category=metadata["category"],
                        relevance_score=relevance_score,
                    )
                )

        return search_results

    def get_product_count(self) -> int:
        return self.collection.count()

    def _build_metadata(self, product: Product) -> dict:
        return {
            "name": product.name,
            "price": product.price,
            "stock": product.stock,
            "description": product.description,
            "category": product.category,
        }

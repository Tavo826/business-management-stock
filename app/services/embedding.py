from google import genai
from google.genai import types

from app.config import settings

client = genai.Client(api_key=settings.google_api_key)

EMBEDDING_MODEL = "gemini-embedding-001"


def generate_product_text(name: str, description: str, category: str) -> str:
    """Build the text representation of a product for embedding.

    Combines name, category, and description into a single string
    optimized for semantic search in Spanish.
    """
    parts = [f"Producto: {name}", f"Categoría: {category}"]
    if description:
        parts.append(f"Descripción: {description}")
    return ". ".join(parts)


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a list of texts using Google's API.

    Processes in batches of 100 to respect API limits.
    """
    all_embeddings: list[list[float]] = []
    batch_size = 100

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        result = client.models.embed_content(
            model=EMBEDDING_MODEL,
            contents=batch,
            config=types.EmbedContentConfig(task_type="RETRIEVAL_DOCUMENT"),
        )
        all_embeddings.extend([e.values for e in result.embeddings])

    return all_embeddings


def embed_query(query: str) -> list[float]:
    """Generate an embedding for a search query."""
    result = client.models.embed_content(
        model=EMBEDDING_MODEL,
        contents=query,
        config=types.EmbedContentConfig(task_type="RETRIEVAL_QUERY"),
    )
    return result.embeddings[0].values

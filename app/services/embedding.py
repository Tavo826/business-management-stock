import logging
import time

from google import genai
from google.genai import types
from google.genai.errors import ClientError

from app.config import settings

logger = logging.getLogger(__name__)

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


def _embed_with_retry(contents, task_type: str, max_retries: int = 5):
    """Call embed_content with exponential backoff on rate limit errors."""
    for attempt in range(max_retries):
        try:
            return client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=contents,
                config=types.EmbedContentConfig(task_type=task_type),
            )
        except ClientError as e:
            if e.code == 429 and attempt < max_retries - 1:
                wait = 62
                logger.warning("Rate limited, retrying in %ds (attempt %d/%d)", wait, attempt + 1, max_retries)
                time.sleep(wait)
            else:
                raise


def embed_texts(texts: list[str]) -> list[list[float]]:
    """Generate embeddings for a list of texts using Google's API.

    Processes in batches of 100 with rate limiting to stay within free tier quotas.
    """
    all_embeddings: list[list[float]] = []
    batch_size = 100

    total_batches = (len(texts) + batch_size - 1) // batch_size

    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        batch_num = i // batch_size + 1
        if i > 0:
            wait = 62
            logger.info("Waiting %ds for rate limit reset before batch %d/%d...", wait, batch_num, total_batches)
            time.sleep(wait)
        result = _embed_with_retry(batch, task_type="RETRIEVAL_DOCUMENT")
        all_embeddings.extend([e.values for e in result.embeddings])
        logger.info("Embedded batch %d/%d (%d texts)", batch_num, total_batches, len(batch))

    return all_embeddings


def embed_query(query: str) -> list[float]:
    """Generate an embedding for a search query."""
    result = _embed_with_retry(query, task_type="RETRIEVAL_QUERY")
    return result.embeddings[0].values

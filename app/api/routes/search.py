from fastapi import APIRouter, Query

from app.api.dependencies import get_vector_store
from app.models.product import SearchResponse

router = APIRouter()


@router.get("/search", response_model=SearchResponse)
async def search_products(
    q: str = Query(..., description="Search query in natural language"),
    limit: int = Query(5, ge=1, le=20, description="Max number of results"),
    available_only: bool = Query(True, description="Only return products with stock > 0"),
):
    """Search products by semantic similarity."""
    vector_store = get_vector_store()
    min_stock = 0.01 if available_only else None
    results = vector_store.search(query=q, limit=limit, min_stock=min_stock)

    return SearchResponse(
        query=q,
        results=results,
        total_found=len(results),
    )

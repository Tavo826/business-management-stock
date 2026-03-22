from pydantic import BaseModel


class Product(BaseModel):
    id: int
    name: str
    price: float
    stock: float
    category: str
    description: str


class ProductSearchResult(BaseModel):
    id: int
    name: str
    price: float
    stock: float
    category: str
    description: str
    relevance_score: float


class SearchResponse(BaseModel):
    query: str
    results: list[ProductSearchResult]
    total_found: int


class SyncResponse(BaseModel):
    status: str
    products_synced: int
    products_updated: int
    embeddings_regenerated: int

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


class StockUpdateItem(BaseModel):
    name: str
    purchased_quantity: float


class StockUpdateRequest(BaseModel):
    items: list[StockUpdateItem]


class StockUpdateItemResult(BaseModel):
    name: str
    status: str
    stage: str | None = None
    product_id: int | None = None
    quant_id: int | None = None
    previous_stock: float | None = None
    purchased_quantity: float | None = None
    new_stock: float | None = None
    applied: bool | None = None


class StockUpdateResponse(BaseModel):
    results: list[StockUpdateItemResult]

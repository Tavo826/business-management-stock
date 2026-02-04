"""Pydantic schemas for API request/response models."""
from pydantic import BaseModel, Field


# Request models
class APIToPostgresRequest(BaseModel):
    """Request body for API to PostgreSQL pipeline."""

    endpoint: str = Field(default="/products", description="API endpoint for products")
    skip_invalid: bool = Field(default=True, description="Skip invalid products")
    handle_duplicates: bool = Field(default=True, description="Handle duplicate SKUs")


class PostgresToQdrantRequest(BaseModel):
    """Request body for PostgreSQL to Qdrant pipeline."""

    force_regenerate: bool = Field(
        default=False, description="Force regeneration of all embeddings"
    )
    batch_size: int = Field(default=50, ge=1, le=500, description="Batch size")


class FullSyncRequest(BaseModel):
    """Request body for full sync pipeline."""

    endpoint: str = Field(default="/products", description="API endpoint for products")
    force_regenerate: bool = Field(
        default=False, description="Force regeneration of embeddings"
    )


# Response models
class PipelineResponse(BaseModel):
    """Response for API to PostgreSQL pipeline."""

    success: bool
    duration_seconds: float
    extracted: int = 0
    validated: int = 0
    cleaned: int = 0
    normalized: int = 0
    inserted: int = 0
    updated: int = 0
    errors: list[str] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)


class EmbeddingPipelineResponse(BaseModel):
    """Response for PostgreSQL to Qdrant pipeline."""

    success: bool
    duration_seconds: float
    total_products: int = 0
    products_needing_update: int = 0
    embeddings_generated: int = 0
    embeddings_upserted: int = 0
    metadata_only_updates: int = 0
    errors: list[str] = Field(default_factory=list)


class SyncDeletionsResponse(BaseModel):
    """Response for sync deletions endpoint."""

    success: bool
    deleted_count: int


class HealthResponse(BaseModel):
    """Response for health check endpoint."""

    status: str
    version: str = "1.0.0"

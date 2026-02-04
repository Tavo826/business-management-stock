"""API routes for ETL pipelines."""
import logging

from fastapi import APIRouter, HTTPException

from src.api.schemas import (
    APIToPostgresRequest,
    EmbeddingPipelineResponse,
    FullSyncRequest,
    HealthResponse,
    PipelineResponse,
    PostgresToQdrantRequest,
    SyncDeletionsResponse,
)
from src.pipelines.api_to_postgres import APIToPostgresPipeline
from src.pipelines.postgres_to_qdrant import PostgresToQdrantPipeline

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/health", response_model=HealthResponse, tags=["Health"])
async def health_check() -> HealthResponse:
    """Check API health status."""
    return HealthResponse(status="healthy")


@router.post(
    "/pipelines/api-to-postgres",
    response_model=PipelineResponse,
    tags=["Pipelines"],
    summary="Run API to PostgreSQL pipeline",
    description="Extracts products from external API, transforms them, and loads into PostgreSQL.",
)
async def run_api_to_postgres(request: APIToPostgresRequest) -> PipelineResponse:
    """Execute the API to PostgreSQL ETL pipeline."""
    logger.info(f"Starting API to PostgreSQL pipeline with endpoint: {request.endpoint}")

    try:
        pipeline = APIToPostgresPipeline()
        result = await pipeline.run(
            endpoint=request.endpoint,
            skip_invalid=request.skip_invalid,
            handle_duplicates=request.handle_duplicates,
        )

        return PipelineResponse(
            success=result.success,
            duration_seconds=result.duration_seconds,
            extracted=result.extracted,
            validated=result.validated,
            cleaned=result.cleaned,
            normalized=result.normalized,
            inserted=result.inserted,
            updated=result.updated,
            errors=result.errors,
            warnings=result.warnings,
        )

    except Exception as e:
        logger.exception(f"API to PostgreSQL pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/pipelines/postgres-to-qdrant",
    response_model=EmbeddingPipelineResponse,
    tags=["Pipelines"],
    summary="Run PostgreSQL to Qdrant pipeline",
    description="Generates embeddings for products and syncs them to Qdrant vector database.",
)
async def run_postgres_to_qdrant(
    request: PostgresToQdrantRequest,
) -> EmbeddingPipelineResponse:
    """Execute the PostgreSQL to Qdrant embedding pipeline."""
    logger.info(
        f"Starting PostgreSQL to Qdrant pipeline (force={request.force_regenerate})"
    )

    try:
        pipeline = PostgresToQdrantPipeline()
        result = await pipeline.run(
            force_regenerate=request.force_regenerate,
            batch_size=request.batch_size,
        )

        return EmbeddingPipelineResponse(
            success=result.success,
            duration_seconds=result.duration_seconds,
            total_products=result.total_products,
            products_needing_update=result.products_needing_update,
            embeddings_generated=result.embeddings_generated,
            embeddings_upserted=result.embeddings_upserted,
            metadata_only_updates=result.metadata_only_updates,
            errors=result.errors,
        )

    except Exception as e:
        logger.exception(f"PostgreSQL to Qdrant pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/pipelines/full-sync",
    response_model=dict,
    tags=["Pipelines"],
    summary="Run full sync pipeline",
    description="Executes both pipelines in sequence: API → PostgreSQL → Qdrant.",
)
async def run_full_sync(request: FullSyncRequest) -> dict:
    """Execute full sync: API to PostgreSQL, then PostgreSQL to Qdrant."""
    logger.info(f"Starting full sync with endpoint: {request.endpoint}")

    try:
        # Step 1: API to PostgreSQL
        api_pipeline = APIToPostgresPipeline()
        api_result = await api_pipeline.run(endpoint=request.endpoint)

        if not api_result.success:
            return {
                "success": False,
                "step_failed": "api-to-postgres",
                "api_to_postgres": PipelineResponse(
                    success=api_result.success,
                    duration_seconds=api_result.duration_seconds,
                    extracted=api_result.extracted,
                    validated=api_result.validated,
                    cleaned=api_result.cleaned,
                    normalized=api_result.normalized,
                    inserted=api_result.inserted,
                    updated=api_result.updated,
                    errors=api_result.errors,
                    warnings=api_result.warnings,
                ).model_dump(),
                "postgres_to_qdrant": None,
            }

        # Step 2: PostgreSQL to Qdrant
        qdrant_pipeline = PostgresToQdrantPipeline()
        qdrant_result = await qdrant_pipeline.run(
            force_regenerate=request.force_regenerate
        )

        return {
            "success": api_result.success and qdrant_result.success,
            "api_to_postgres": PipelineResponse(
                success=api_result.success,
                duration_seconds=api_result.duration_seconds,
                extracted=api_result.extracted,
                validated=api_result.validated,
                cleaned=api_result.cleaned,
                normalized=api_result.normalized,
                inserted=api_result.inserted,
                updated=api_result.updated,
                errors=api_result.errors,
                warnings=api_result.warnings,
            ).model_dump(),
            "postgres_to_qdrant": EmbeddingPipelineResponse(
                success=qdrant_result.success,
                duration_seconds=qdrant_result.duration_seconds,
                total_products=qdrant_result.total_products,
                products_needing_update=qdrant_result.products_needing_update,
                embeddings_generated=qdrant_result.embeddings_generated,
                embeddings_upserted=qdrant_result.embeddings_upserted,
                metadata_only_updates=qdrant_result.metadata_only_updates,
                errors=qdrant_result.errors,
            ).model_dump(),
        }

    except Exception as e:
        logger.exception(f"Full sync pipeline failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post(
    "/pipelines/sync-deletions",
    response_model=SyncDeletionsResponse,
    tags=["Pipelines"],
    summary="Sync deletions",
    description="Removes products from Qdrant that no longer exist in PostgreSQL.",
)
async def run_sync_deletions() -> SyncDeletionsResponse:
    """Sync deletions between PostgreSQL and Qdrant."""
    logger.info("Starting sync deletions")

    try:
        pipeline = PostgresToQdrantPipeline()
        deleted = await pipeline.sync_deletions()

        return SyncDeletionsResponse(success=True, deleted_count=deleted)

    except Exception as e:
        logger.exception(f"Sync deletions failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

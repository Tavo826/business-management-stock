import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from app.api.dependencies import get_sync_service, get_vector_store
from app.api.routes import search, stock, sync
from app.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Inventory RAG Service",
    description="RAG service for agro store inventory powered by ChromaDB and Google Embeddings",
    version="1.0.0",
)

app.include_router(search.router, prefix="/api", tags=["Search"])
app.include_router(sync.router, prefix="/api", tags=["Sync"])
app.include_router(stock.router, prefix="/api", tags=["Stock"])

scheduler = AsyncIOScheduler()


async def scheduled_sync():
    """Run the scheduled product sync."""
    logger.info("Running scheduled sync...")
    sync_service = get_sync_service()
    result = await sync_service.sync()
    logger.info("Scheduled sync result: %s", result.status)


@app.on_event("startup")
async def startup():
    vector_store = get_vector_store()
    product_count = vector_store.get_product_count()
    logger.info("ChromaDB initialized with %d products", product_count)

    if product_count == 0:
        logger.info("No products found, running initial sync...")
        await scheduled_sync()

    if settings.scheduler_enabled:
        scheduler.add_job(
            scheduled_sync,
            "cron",
            hour=settings.sync_cron_hour,
            minute=settings.sync_cron_minute,
            id="product_sync",
            replace_existing=True,
        )
        scheduler.start()
        logger.info(
            "Scheduler started: sync at %02d:%02d daily",
            settings.sync_cron_hour,
            settings.sync_cron_minute,
        )
    else:
        logger.info("Scheduler is disabled")


@app.on_event("shutdown")
async def shutdown():
    if scheduler.running:
        scheduler.shutdown(wait=False)
        logger.info("Scheduler stopped")


@app.get("/health")
async def health():
    vector_store = get_vector_store()
    return {
        "status": "healthy",
        "products_indexed": vector_store.get_product_count(),
    }

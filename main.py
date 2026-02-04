"""
Punto de entrada principal para el sistema ETL.
Permite ejecutar los diferentes pipelines via CLI.
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

# Agregar el directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent))

from config import settings
from src.pipelines.api_to_postgres import APIToPostgresPipeline
from src.pipelines.postgres_to_qdrant import PostgresToQdrantPipeline


def setup_logging() -> None:
    """Configura el logging de la aplicación."""
    logging.basicConfig(
        level=getattr(logging, settings.log_level),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


async def run_api_to_postgres(args: argparse.Namespace) -> int:
    """Ejecuta el pipeline API → PostgreSQL."""
    logging.info("Starting API to PostgreSQL pipeline...")

    pipeline = APIToPostgresPipeline()
    result = await pipeline.run(
        endpoint=args.endpoint,
        skip_invalid=not args.include_invalid,
        handle_duplicates=not args.keep_duplicates,
    )

    logging.info(f"Pipeline completed in {result.duration_seconds:.2f}s")
    logging.info(f"Results: {result.to_dict()}")

    if result.errors:
        logging.error(f"Errors: {result.errors[:10]}")  # Primeros 10 errores

    return 0 if result.success else 1


async def run_postgres_to_qdrant(args: argparse.Namespace) -> int:
    """Ejecuta el pipeline PostgreSQL → Qdrant."""
    logging.info("Starting PostgreSQL to Qdrant pipeline...")

    pipeline = PostgresToQdrantPipeline()
    result = await pipeline.run(
        force_regenerate=args.force,
        batch_size=args.batch_size,
    )

    logging.info(f"Pipeline completed in {result.duration_seconds:.2f}s")
    logging.info(f"Results: {result.to_dict()}")

    if result.errors:
        logging.error(f"Errors: {result.errors}")

    return 0 if result.success else 1


async def run_full_sync(args: argparse.Namespace) -> int:
    """Ejecuta ambos pipelines en secuencia."""
    logging.info("Starting full sync (API → PostgreSQL → Qdrant)...")

    # Pipeline 1: API → PostgreSQL
    logging.info("Step 1/2: API to PostgreSQL")
    api_pipeline = APIToPostgresPipeline()
    api_result = await api_pipeline.run(endpoint=args.endpoint)

    if not api_result.success:
        logging.error("API to PostgreSQL pipeline failed, aborting.")
        return 1

    logging.info(f"Step 1 completed: {api_result.inserted} inserted, {api_result.updated} updated")

    # Pipeline 2: PostgreSQL → Qdrant
    logging.info("Step 2/2: PostgreSQL to Qdrant")
    qdrant_pipeline = PostgresToQdrantPipeline()
    qdrant_result = await qdrant_pipeline.run(force_regenerate=args.force)

    if not qdrant_result.success:
        logging.error("PostgreSQL to Qdrant pipeline failed.")
        return 1

    logging.info(
        f"Step 2 completed: {qdrant_result.embeddings_generated} embeddings generated"
    )

    logging.info("Full sync completed successfully!")
    return 0


async def run_sync_deletions(args: argparse.Namespace) -> int:
    """Sincroniza eliminaciones entre PostgreSQL y Qdrant."""
    logging.info("Syncing deletions...")

    pipeline = PostgresToQdrantPipeline()
    deleted = await pipeline.sync_deletions()

    logging.info(f"Deleted {deleted} products from Qdrant")
    return 0


def run_serve(args: argparse.Namespace) -> int:
    """Inicia el servidor HTTP API."""
    import uvicorn

    logging.info(f"Starting API server on {args.host}:{args.port}")
    uvicorn.run(
        "src.api.app:app",
        host=args.host,
        port=args.port,
        reload=args.reload,
    )
    return 0


def create_parser() -> argparse.ArgumentParser:
    """Crea el parser de argumentos CLI."""
    parser = argparse.ArgumentParser(
        description="ETL Pipeline for Products",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py api-to-postgres --endpoint /api/v1/products
  python main.py postgres-to-qdrant --force
  python main.py full-sync --endpoint /products
  python main.py sync-deletions
  python main.py serve --port 8000
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Pipeline to run")

    # API to PostgreSQL
    api_parser = subparsers.add_parser(
        "api-to-postgres",
        help="Extract from API and load to PostgreSQL",
    )
    api_parser.add_argument(
        "--endpoint",
        default="/products",
        help="API endpoint for products (default: /products)",
    )
    api_parser.add_argument(
        "--include-invalid",
        action="store_true",
        help="Include invalid products instead of skipping",
    )
    api_parser.add_argument(
        "--keep-duplicates",
        action="store_true",
        help="Keep duplicate products instead of merging",
    )

    # PostgreSQL to Qdrant
    qdrant_parser = subparsers.add_parser(
        "postgres-to-qdrant",
        help="Generate embeddings and load to Qdrant",
    )
    qdrant_parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration of all embeddings",
    )
    qdrant_parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Batch size for processing (default: 50)",
    )

    # Full sync
    full_parser = subparsers.add_parser(
        "full-sync",
        help="Run both pipelines in sequence",
    )
    full_parser.add_argument(
        "--endpoint",
        default="/products",
        help="API endpoint for products",
    )
    full_parser.add_argument(
        "--force",
        action="store_true",
        help="Force regeneration of embeddings",
    )

    # Sync deletions
    subparsers.add_parser(
        "sync-deletions",
        help="Remove products from Qdrant that no longer exist in PostgreSQL",
    )

    # Serve API
    serve_parser = subparsers.add_parser(
        "serve",
        help="Start the HTTP API server",
    )
    serve_parser.add_argument(
        "--host",
        default="0.0.0.0",
        help="Host to bind (default: 0.0.0.0)",
    )
    serve_parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port to bind (default: 8000)",
    )
    serve_parser.add_argument(
        "--reload",
        action="store_true",
        help="Enable auto-reload for development",
    )

    return parser


async def async_main(args: argparse.Namespace) -> int:
    """Función principal para comandos async."""
    commands = {
        "api-to-postgres": run_api_to_postgres,
        "postgres-to-qdrant": run_postgres_to_qdrant,
        "full-sync": run_full_sync,
        "sync-deletions": run_sync_deletions,
    }

    handler = commands.get(args.command)
    if handler:
        return await handler(args)

    return 1


def main() -> int:
    """Punto de entrada principal."""
    parser = create_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return 1

    setup_logging()

    # Serve command runs its own event loop (uvicorn)
    if args.command == "serve":
        return run_serve(args)

    # Other commands are async
    return asyncio.run(async_main(args))


if __name__ == "__main__":
    sys.exit(main())

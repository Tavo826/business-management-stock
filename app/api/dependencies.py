from app.services.odoo_client import OdooClient
from app.services.stock_update_service import StockUpdateService
from app.services.sync_service import SyncService
from app.services.vector_store import VectorStore

_odoo_client: OdooClient | None = None
_vector_store: VectorStore | None = None
_sync_service: SyncService | None = None
_stock_update_service: StockUpdateService | None = None


def get_odoo_client() -> OdooClient:
    global _odoo_client
    if _odoo_client is None:
        _odoo_client = OdooClient()
    return _odoo_client


def get_vector_store() -> VectorStore:
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore()
    return _vector_store


def get_sync_service() -> SyncService:
    global _sync_service
    if _sync_service is None:
        _sync_service = SyncService(get_odoo_client(), get_vector_store())
    return _sync_service


def get_stock_update_service() -> StockUpdateService:
    global _stock_update_service
    if _stock_update_service is None:
        _stock_update_service = StockUpdateService(get_odoo_client())
    return _stock_update_service

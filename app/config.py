from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    odoo_url: str
    odoo_api_key: str
    odoo_db: str

    google_api_key: str

    chroma_persist_dir: str = "./data/chroma"
    chroma_collection_name: str = "products"

    sync_cron_hour: int = 5
    sync_cron_minute: int = 0
    scheduler_enabled: bool = True

    api_host: str = "0.0.0.0"
    api_port: int = 8000

    search_default_limit: int = 5

    model_config = {"env_file": ".env"}


settings = Settings()

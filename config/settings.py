"""
Configuración centralizada del proyecto ETL.
Usa variables de entorno para configuración sensible.
"""
from functools import lru_cache
from typing import Optional

from pydantic import Field, PostgresDsn, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuración de la aplicación cargada desde variables de entorno."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # API Source Configuration
    api_base_url: str = Field(..., description="URL base de la API fuente")
    api_key: Optional[str] = Field(None, description="API key si es requerida")
    api_timeout: int = Field(30, description="Timeout en segundos para requests")
    api_max_retries: int = Field(3, description="Número máximo de reintentos")
    api_page_size: int = Field(100, description="Tamaño de página para paginación")

    # PostgreSQL Configuration
    postgres_host: str = Field("localhost", description="Host de PostgreSQL")
    postgres_port: int = Field(5432, description="Puerto de PostgreSQL")
    postgres_user: str = Field(..., description="Usuario de PostgreSQL")
    postgres_password: str = Field(..., description="Contraseña de PostgreSQL")
    postgres_db: str = Field(..., description="Nombre de la base de datos")

    @property
    def postgres_dsn(self) -> str:
        """Genera el DSN de conexión a PostgreSQL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def postgres_dsn_sync(self) -> str:
        """Genera el DSN síncrono para migraciones."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # Qdrant Configuration
    qdrant_host: str = Field("localhost", description="Host de Qdrant")
    qdrant_port: int = Field(6333, description="Puerto de Qdrant")
    qdrant_api_key: Optional[str] = Field(None, description="API key de Qdrant")
    qdrant_collection: str = Field("products", description="Nombre de la colección")

    @property
    def qdrant_url(self) -> str:
        """Genera la URL de conexión a Qdrant."""
        return f"http://{self.qdrant_host}:{self.qdrant_port}"

    # Anthropic/Voyage Configuration
    anthropic_api_key: str = Field(..., description="API key de Anthropic")
    voyage_api_key: Optional[str] = Field(
        None, description="API key de Voyage AI (opcional, usa Anthropic si no existe)"
    )
    embedding_model: str = Field(
        "voyage-3", description="Modelo de embeddings a usar"
    )
    embedding_dimensions: int = Field(
        1024, description="Dimensiones del vector de embedding"
    )

    # ETL Configuration
    batch_size: int = Field(50, description="Tamaño del batch para procesamiento")
    log_level: str = Field("INFO", description="Nivel de logging")

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level debe ser uno de: {valid_levels}")
        return v_upper


@lru_cache
def get_settings() -> Settings:
    """
    Obtiene la configuración cacheada.
    Usa lru_cache para evitar recargar el archivo .env múltiples veces.
    """
    return Settings()


# Instancia global para importación directa
settings = get_settings()

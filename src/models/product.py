"""
Modelos de datos para productos.
Incluye modelos Pydantic para validación y SQLAlchemy para persistencia.
"""
import hashlib
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator
from sqlalchemy import JSON, DateTime, Integer, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


# ============================================================================
# SQLAlchemy Base
# ============================================================================


class Base(DeclarativeBase):
    """Base declarativa para modelos SQLAlchemy."""

    pass


# ============================================================================
# SQLAlchemy ORM Model
# ============================================================================


class ProductDB(Base):
    """Modelo SQLAlchemy para la tabla de productos."""

    __tablename__ = "products"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    sku: Mapped[str] = mapped_column(
        String(100),
        unique=True,
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(500), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    stock: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unit: Mapped[str] = mapped_column(String(50), nullable=False, default="unidad")
    attributes: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    raw_data: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    # Hash de campos textuales para detectar cambios en embeddings
    text_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    # Tracking timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    def compute_text_hash(self) -> str:
        """
        Calcula un hash de los campos textuales relevantes para embeddings.
        Se usa para detectar si es necesario regenerar el embedding.
        """
        text_content = f"{self.name}|{self.description or ''}|{self.category}"

        # Incluir atributos textuales
        if self.attributes:
            attr_text = "|".join(
                f"{k}:{v}"
                for k, v in sorted(self.attributes.items())
                if isinstance(v, str)
            )
            text_content += f"|{attr_text}"

        return hashlib.sha256(text_content.encode()).hexdigest()

    def to_embedding_text(self) -> str:
        """
        Genera el texto enriquecido para crear el embedding.
        Incluye información semánticamente relevante.
        """
        parts = [
            f"Producto: {self.name}",
            f"Categoría: {self.category}",
        ]

        if self.description:
            parts.append(f"Descripción: {self.description}")

        if self.attributes:
            attr_parts = [
                f"{k}: {v}" for k, v in self.attributes.items() if isinstance(v, str)
            ]
            if attr_parts:
                parts.append("Características: " + ", ".join(attr_parts))

        return ". ".join(parts)


# ============================================================================
# Pydantic Models
# ============================================================================


class ProductCreate(BaseModel):
    """Modelo Pydantic para crear/validar productos desde la API."""

    sku: str = Field(..., min_length=1, max_length=100)
    name: str = Field(..., min_length=1, max_length=500)
    description: Optional[str] = Field(None, max_length=5000)
    category: str = Field(..., min_length=1, max_length=200)
    price: Decimal = Field(..., gt=0, decimal_places=2)
    stock: int = Field(..., ge=0)
    unit: str = Field("unidad", max_length=50)
    attributes: dict[str, Any] = Field(default_factory=dict)
    raw_data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("sku", "name", "category")
    @classmethod
    def strip_whitespace(cls, v: str) -> str:
        return v.strip()

    @field_validator("description")
    @classmethod
    def strip_description(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            v = v.strip()
            return v if v else None
        return None


class Product(ProductCreate):
    """Modelo Pydantic completo de producto (incluye campos de DB)."""

    id: uuid.UUID
    text_hash: Optional[str] = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class ProductEmbedding(BaseModel):
    """Modelo para representar un producto con su embedding para Qdrant."""

    id: str = Field(..., description="ID único: product-<uuid>")
    vector: list[float] = Field(..., description="Vector de embedding")
    sku: str
    product_id: uuid.UUID
    category: str
    price: Decimal
    stock: int
    text: str = Field(..., description="Texto usado para generar el embedding")
    text_hash: str = Field(..., description="Hash del texto para detectar cambios")

    def to_qdrant_point(self) -> dict[str, Any]:
        """Convierte a formato de punto para Qdrant."""
        return {
            "id": self.id,
            "vector": self.vector,
            "payload": {
                "sku": self.sku,
                "product_id": str(self.product_id),
                "category": self.category,
                "price": float(self.price),
                "stock": self.stock,
                "text": self.text,
                "text_hash": self.text_hash,
            },
        }

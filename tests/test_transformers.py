"""
Tests para los transformadores (validators, cleaners, normalizers).
"""
from decimal import Decimal

import pytest

from src.transformers.cleaners import TextCleaner
from src.transformers.normalizers import ProductNormalizer
from src.transformers.validators import ProductValidator


class TestProductValidator:
    """Tests para ProductValidator."""

    def setup_method(self):
        self.validator = ProductValidator()

    def test_valid_product(self):
        product = {
            "sku": "SKU001",
            "name": "Test Product",
            "category": "camisas",
            "price": 100.00,
            "stock": 10,
        }
        result = self.validator.validate(product)
        assert result.is_valid is True
        assert len(result.errors) == 0

    def test_missing_required_field(self):
        product = {
            "name": "Test Product",
            "category": "camisas",
            "price": 100.00,
            "stock": 10,
        }
        result = self.validator.validate(product)
        assert result.is_valid is False
        assert any("sku" in e.field for e in result.errors)

    def test_invalid_price_zero(self):
        product = {
            "sku": "SKU001",
            "name": "Test Product",
            "category": "camisas",
            "price": 0,
            "stock": 10,
        }
        result = self.validator.validate(product)
        assert result.is_valid is False
        assert any("price" in e.field for e in result.errors)

    def test_negative_stock(self):
        product = {
            "sku": "SKU001",
            "name": "Test Product",
            "category": "camisas",
            "price": 100,
            "stock": -5,
        }
        result = self.validator.validate(product)
        assert result.is_valid is False
        assert any("stock" in e.field for e in result.errors)


class TestTextCleaner:
    """Tests para TextCleaner."""

    def setup_method(self):
        self.cleaner = TextCleaner()

    def test_clean_text_trim(self):
        result = self.cleaner.clean_text("  hello world  ")
        assert result == "hello world"

    def test_clean_text_multiple_spaces(self):
        result = self.cleaner.clean_text("hello    world")
        assert result == "hello world"

    def test_clean_text_none(self):
        result = self.cleaner.clean_text(None)
        assert result is None

    def test_clean_text_empty(self):
        result = self.cleaner.clean_text("   ")
        assert result is None

    def test_clean_unit_normalization(self):
        assert self.cleaner.clean_unit("KG") == "kg"
        assert self.cleaner.clean_unit("kilogramos") == "kg"
        assert self.cleaner.clean_unit("litro") == "l"
        assert self.cleaner.clean_unit("unidad") == "unidad"

    def test_clean_sku(self):
        assert self.cleaner.clean_sku("sku-001") == "SKU-001"
        assert self.cleaner.clean_sku("sku 001!@#") == "SKU001"


class TestProductNormalizer:
    """Tests para ProductNormalizer."""

    def setup_method(self):
        self.normalizer = ProductNormalizer()

    def test_normalize_category(self):
        assert self.normalizer.normalize_category("camisa") == "camisas"
        assert self.normalizer.normalize_category("CAMISAS") == "camisas"
        assert self.normalizer.normalize_category("shirt") == "camisas"

    def test_normalize_category_unknown(self):
        result = self.normalizer.normalize_category("categoria_rara")
        assert result == "otros"

    def test_normalize_price_string(self):
        result = self.normalizer.normalize_price("$1,234.56")
        assert result == Decimal("1234.56")

    def test_normalize_price_int(self):
        result = self.normalizer.normalize_price(1000)
        assert result == Decimal("1000.00")

    def test_handle_duplicates_keep_latest(self):
        products = [
            {"sku": "SKU001", "name": "Product V1", "stock": 10},
            {"sku": "SKU001", "name": "Product V2", "stock": 20},
            {"sku": "SKU002", "name": "Other Product", "stock": 5},
        ]
        normalizer = ProductNormalizer(duplicate_strategy="keep_latest")
        result = normalizer.handle_duplicates(products)

        assert len(result) == 2
        sku001 = next(p for p in result if p["sku"] == "SKU001")
        assert sku001["name"] == "Product V2"

    def test_handle_duplicates_merge(self):
        products = [
            {"sku": "SKU001", "name": "Product", "stock": 10},
            {"sku": "SKU001", "name": "Product", "stock": 20},
        ]
        normalizer = ProductNormalizer(duplicate_strategy="merge")
        result = normalizer.handle_duplicates(products)

        assert len(result) == 1
        assert result[0]["stock"] == 30  # Stocks sumados

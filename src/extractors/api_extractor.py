"""
Extractor de datos desde API externa.
Implementa cliente HTTP async con retry, paginación y rate limiting.
"""
import logging
from typing import Any, AsyncIterator, Optional

import httpx
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config import settings

logger = logging.getLogger(__name__)


class APIExtractorError(Exception):
    """Error base para el extractor de API."""

    pass


class APIConnectionError(APIExtractorError):
    """Error de conexión a la API."""

    pass


class APIResponseError(APIExtractorError):
    """Error en la respuesta de la API."""

    def __init__(self, status_code: int, message: str):
        self.status_code = status_code
        super().__init__(f"API responded with {status_code}: {message}")


class APIExtractor:
    """
    Cliente async para extraer datos de una API externa.

    Características:
    - Retry automático con backoff exponencial
    - Paginación automática
    - Rate limiting respetuoso
    - Manejo de errores robusto
    """

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        timeout: Optional[int] = None,
        max_retries: Optional[int] = None,
        page_size: Optional[int] = None,
    ):
        self.base_url = (base_url or settings.api_base_url).rstrip("/")
        self.api_key = api_key or settings.api_key
        self.timeout = timeout or settings.api_timeout
        self.max_retries = max_retries or settings.api_max_retries
        self.page_size = page_size or settings.api_page_size

        self._client: Optional[httpx.AsyncClient] = None

    def _get_headers(self) -> dict[str, str]:
        """Construye los headers para las peticiones."""
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _get_client(self) -> httpx.AsyncClient:
        """Obtiene o crea el cliente HTTP."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                headers=self._get_headers(),
                timeout=httpx.Timeout(self.timeout),
            )
        return self._client

    async def close(self) -> None:
        """Cierra el cliente HTTP."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def __aenter__(self) -> "APIExtractor":
        await self._get_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    @retry(
        retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        reraise=True,
    )
    async def _make_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict[str, Any]] = None,
        json_data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """
        Realiza una petición HTTP con retry automático.

        Args:
            method: Método HTTP (GET, POST, etc.)
            endpoint: Endpoint relativo
            params: Parámetros de query string
            json_data: Datos JSON para el body

        Returns:
            Respuesta JSON parseada

        Raises:
            APIConnectionError: Si no se puede conectar
            APIResponseError: Si la API responde con error
        """
        client = await self._get_client()

        try:
            response = await client.request(
                method=method,
                url=endpoint,
                params=params,
                json=json_data,
            )
        except httpx.ConnectError as e:
            logger.error(f"Connection error to {self.base_url}{endpoint}: {e}")
            raise APIConnectionError(f"Cannot connect to API: {e}") from e
        except httpx.TimeoutException as e:
            logger.error(f"Timeout on {self.base_url}{endpoint}: {e}")
            raise APIConnectionError(f"Request timed out: {e}") from e

        if response.status_code >= 400:
            error_msg = response.text[:500] if response.text else "No error message"
            logger.error(
                f"API error {response.status_code} on {endpoint}: {error_msg}"
            )
            raise APIResponseError(response.status_code, error_msg)

        return response.json()

    async def fetch_products(
        self,
        endpoint: str = "/products",
        params: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        """
        Obtiene una lista de productos desde la API.

        Args:
            endpoint: Endpoint de productos
            params: Parámetros adicionales

        Returns:
            Lista de productos como diccionarios
        """
        response = await self._make_request("GET", endpoint, params=params)

        # Adaptar según la estructura de respuesta de tu API
        # Casos comunes:
        if isinstance(response, list):
            return response
        if isinstance(response, dict):
            # Buscar el array de productos en estructuras comunes
            for key in ["data", "products", "items", "results"]:
                if key in response and isinstance(response[key], list):
                    return response[key]
            # Si es un solo producto, retornarlo como lista
            if "id" in response or "sku" in response:
                return [response]

        logger.warning(f"Unexpected response structure: {type(response)}")
        return []

    async def fetch_products_paginated(
        self,
        endpoint: str = "/products",
        page_param: str = "page",
        size_param: str = "limit",
        start_page: int = 1,
    ) -> AsyncIterator[list[dict[str, Any]]]:
        """
        Obtiene productos con paginación automática.

        Args:
            endpoint: Endpoint de productos
            page_param: Nombre del parámetro de página
            size_param: Nombre del parámetro de tamaño
            start_page: Página inicial (1-indexed por defecto)

        Yields:
            Páginas de productos como listas de diccionarios
        """
        current_page = start_page

        while True:
            params = {
                page_param: current_page,
                size_param: self.page_size,
            }

            logger.info(f"Fetching page {current_page} from {endpoint}")
            products = await self.fetch_products(endpoint, params)

            if not products:
                logger.info(f"No more products found at page {current_page}")
                break

            yield products

            if len(products) < self.page_size:
                logger.info(f"Last page reached at {current_page}")
                break

            current_page += 1

    async def fetch_all_products(
        self,
        endpoint: str = "/products",
        page_param: str = "page",
        size_param: str = "limit",
    ) -> list[dict[str, Any]]:
        """
        Obtiene todos los productos paginados en una sola lista.

        Args:
            endpoint: Endpoint de productos
            page_param: Nombre del parámetro de página
            size_param: Nombre del parámetro de tamaño

        Returns:
            Lista completa de todos los productos
        """
        all_products = []

        async for page in self.fetch_products_paginated(
            endpoint, page_param, size_param
        ):
            all_products.extend(page)
            logger.info(f"Accumulated {len(all_products)} products")

        return all_products

    async def fetch_single_product(
        self,
        product_id: str,
        endpoint_template: str = "/products/{id}",
    ) -> Optional[dict[str, Any]]:
        """
        Obtiene un producto específico por ID.

        Args:
            product_id: ID del producto
            endpoint_template: Template del endpoint con {id}

        Returns:
            Producto como diccionario o None si no existe
        """
        endpoint = endpoint_template.format(id=product_id)

        try:
            response = await self._make_request("GET", endpoint)
            return response if response else None
        except APIResponseError as e:
            if e.status_code == 404:
                return None
            raise

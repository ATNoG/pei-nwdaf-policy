"""
Policy Client - Main client class for components to interact with Policy Service.
"""
import asyncio
import json
import logging
from typing import Any, Callable, Coroutine

import aiohttp

from cachetools import TTLCache

logger = logging.getLogger(__name__)

# Type alias for field source - can be a list, URL string, or callable function
FieldSource = (
    list[str]  # Direct list of field names
    | str  # URL to fetch fields from
    | Callable[[], list[str]]  # Sync function returning field names
    | Callable[[], Coroutine[None, None, list[str]]]  # Async function returning field names
)


class ProcessResult:
    """Result of policy data processing."""

    def __init__(
        self,
        allowed: bool,
        data: dict[str, Any] | None = None,
        reason: str = "",
        transformations: list[str] | None = None
    ):
        self.allowed = allowed
        self.data = data
        self.reason = reason
        self.transformations = transformations or []


class PolicyClient:
    """
    Lightweight client SDK for policy enforcement.

    This client provides caching and fail-open functionality
    for components to interact with the Policy Service.
    """

    def __init__(
        self,
        service_url: str,
        component_id: str,
        fields: FieldSource | None = None,
        cache_ttl: int = 60,
        cache_size: int = 1000,
        timeout: int = 5,
        enable_policy: bool = False,
        fail_open: bool = True
    ):
        """
        Initialize the Policy Client.

        Args:
            service_url: URL of the Policy Service (e.g., "http://policy-service:8000")
            component_id: This component's ID
            fields: Field source - can be:
                - list[str]: Direct list of field names
                - str: URL to fetch fields from (HTTP GET expected to return JSON with 'fields' key)
                - Callable: Function that returns field names (sync or async)
            cache_ttl: Cache TTL in seconds
            cache_size: Maximum cache size
            timeout: Request timeout in seconds
            enable_policy: Whether policy enforcement is enabled (policyless mode if False)
            fail_open: If True, allow data through on policy check failures
        """
        self.service_url = service_url.rstrip("/")
        self.component_id = component_id
        self.timeout = timeout
        self.enable_policy = enable_policy
        self.fail_open = fail_open
        self._fields_source = fields

        # Decision cache
        self.cache = TTLCache(maxsize=cache_size, ttl=cache_ttl)

        # Field cache key for callable sources
        self._fields_cache_key = f"{self.component_id}:fields"

        logger.info(
            f"PolicyClient initialized: service={service_url}, "
            f"component={component_id}, enabled={enable_policy}"
        )

    async def _get_fields(self) -> list[str]:
        """
        Resolve field names from the configured source.

        For callable sources, uses TTL cache to allow fields to be refreshed
        as the underlying data changes.

        Returns:
            List of field names
        """
        if self._fields_source is None:
            return []

        # Direct list - no caching needed (static)
        if isinstance(self._fields_source, list):
            return self._fields_source

        # Check TTL cache for callable sources (URLs and functions)
        # This allows fields to be refreshed when the cache expires
        cached_fields = self.cache.get(self._fields_cache_key)
        if cached_fields is not None:
            return cached_fields

        # URL string - fetch from endpoint
        if isinstance(self._fields_source, str):
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(self._fields_source, timeout=aiohttp.ClientTimeout(total=self.timeout)) as response:
                        response.raise_for_status()
                        data = await response.json()
                        # Support both direct list and nested 'fields' key
                        if isinstance(data, list):
                            fields = data
                        elif isinstance(data, dict) and "fields" in data:
                            fields = data["fields"]
                        else:
                            logger.warning(f"Unexpected response format from fields endpoint: {data}")
                            fields = []

                        # Cache the result
                        self.cache[self._fields_cache_key] = fields
                        return fields
            except Exception as e:
                logger.error(f"Failed to fetch fields from URL {self._fields_source}: {e}")
                return []

        # Callable - invoke and get fields
        result = self._fields_source()
        if asyncio.iscoroutine(result):
            # Async callable
            fields = await result
        else:
            # Sync callable
            fields = result

        # Cache the result with TTL
        self.cache[self._fields_cache_key] = fields
        return fields

    async def register_component(
        self,
        component_type: str,
        role: str | None = None,
        data_columns: list[str] | None = None,
        auto_create_attributes: bool = True,
        allowed_fields: dict[str, list[str]] | None = None
    ) -> bool:
        """
        Register this component with the Policy Service.

        Fields are automatically resolved from the configured source if data_columns is not provided.

        Args:
            component_type: Type of component
            role: Optional role to assign
            data_columns: Optional data columns (if not provided, uses resolved fields)
            auto_create_attributes: Whether to auto-create attributes
            allowed_fields: Optional allowed fields per sink

        Returns:
            True if successful
        """
        # Resolve fields if data_columns not provided
        if data_columns is None:
            data_columns = await self._get_fields()

        try:
            await self._request(
                "POST",
                "/api/v1/components",
                {
                    "component_id": self.component_id,
                    "component_type": component_type,
                    "role": role,
                    "data_columns": data_columns,
                    "auto_create_attributes": auto_create_attributes,
                    "allowed_fields": allowed_fields
                }
            )
            logger.info(f"Component registered: {self.component_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to register component: {e}")
            return False

    async def _request(
        self,
        method: str,
        endpoint: str,
        data: dict[str, Any] | None = None
    ) -> dict:
        """
        Make an HTTP request to the Policy Service.

        Args:
            method: HTTP method
            endpoint: API endpoint
            data: Request body

        Returns:
            Response JSON
        """
        url = f"{self.service_url}{endpoint}"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.request(
                    method=method,
                    url=url,
                    json=data,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    response.raise_for_status()
                    return await response.json()

        except asyncio.TimeoutError:
            raise Exception(f"Timeout connecting to Policy Service: {url}")

        except aiohttp.ClientError as e:
            raise Exception(f"Failed to connect to Policy Service: {e}")

    def _generate_cache_key(self, *args) -> str:
        """Generate a cache key from arguments."""
        key_data = json.dumps(args, sort_keys=True, default=str)
        return f"{self.component_id}:{hash(key_data)}"

    async def check_access(
        self,
        source_id: str,
        sink_id: str,
        resource: str = "data",
        action: str = "read"
    ) -> bool:
        """
        Check if access is permitted.

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            resource: Resource type
            action: Action to perform

        Returns:
            True if access is permitted
        """
        if not self.enable_policy:
            return True

        # Check cache
        cache_key = self._generate_cache_key("check", source_id, sink_id, resource, action)
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            response = await self._request(
                "POST",
                "/api/v1/policy/check",
                {
                    "source_id": source_id,
                    "sink_id": sink_id,
                    "resource": resource,
                    "action": action
                }
            )

            allowed = response.get("allowed", False)
            self.cache[cache_key] = allowed
            return allowed

        except Exception as e:
            logger.warning(f"Policy check failed: {e}")
            return self.fail_open

    async def process_data(
        self,
        source_id: str,
        sink_id: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> ProcessResult:
        """
        Check policy and transform data in one call.

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            data: Data to process
            action: Action type

        Returns:
            ProcessResult with allowed status and transformed data
        """
        if not self.enable_policy:
            return ProcessResult(
                allowed=True,
                data=data,
                reason="Policy disabled"
            )

        # Check cache
        data_hash = hash(json.dumps(data, sort_keys=True))
        cache_key = self._generate_cache_key("process", source_id, sink_id, data_hash)
        if cache_key in self.cache:
            return self.cache[cache_key]

        try:
            response = await self._request(
                "POST",
                "/api/v1/policy/process",
                {
                    "source_id": source_id,
                    "sink_id": sink_id,
                    "data": data,
                    "action": action
                }
            )

            result = ProcessResult(
                allowed=response.get("allowed", False),
                data=response.get("data"),
                reason=response.get("reason", ""),
                transformations=response.get("transformations_applied", [])
            )

            self.cache[cache_key] = result
            return result

        except Exception as e:
            logger.warning(f"Policy processing failed: {e}")
            if self.fail_open:
                return ProcessResult(
                    allowed=True,
                    data=data,
                    reason=f"Policy check failed, allowing through: {e}"
                )
            else:
                return ProcessResult(
                    allowed=False,
                    reason=f"Policy check failed: {e}"
                )

    async def register_ml_model(
        self,
        model_id: str,
        model_name: str,
        input_fields: list[str],
        output_fields: list[str],
        data_type: str
    ) -> bool:
        """
        Register an ML model with the Policy Service.

        Args:
            model_id: Unique model identifier
            model_name: Human-readable model name
            input_fields: Input field names
            output_fields: Output field names
            data_type: Data type for role assignment

        Returns:
            True if successful
        """
        try:
            await self._request(
                "POST",
                "/api/v1/ml/models",
                {
                    "model_id": model_id,
                    "model_name": model_name,
                    "input_fields": input_fields,
                    "output_fields": output_fields,
                    "data_type": data_type
                }
            )
            logger.info(f"ML model registered: {model_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to register ML model: {e}")
            return False

    def clear_cache(self) -> None:
        """Clear the decision cache."""
        self.cache.clear()

    def __repr__(self) -> str:
        return f"PolicyClient(component_id={self.component_id}, enabled={self.enable_policy})"


class SyncPolicyClient:
    """
    Synchronous wrapper for PolicyClient for non-async contexts.
    """

    def __init__(self, async_client: PolicyClient):
        self._async_client = async_client

    def check_access(
        self,
        source_id: str,
        sink_id: str,
        resource: str = "data",
        action: str = "read"
    ) -> bool:
        """Synchronous version of check_access."""
        return asyncio.run(self._async_client.check_access(
            source_id, sink_id, resource, action
        ))

    def process_data(
        self,
        source_id: str,
        sink_id: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> ProcessResult:
        """Synchronous version of process_data."""
        return asyncio.run(self._async_client.process_data(
            source_id, sink_id, data, action
        ))

    def register_component(
        self,
        component_type: str,
        role: str | None = None,
        data_columns: list[str] | None = None,
        auto_create_attributes: bool = True,
        allowed_fields: dict[str, list[str]] | None = None
    ) -> bool:
        """Synchronous version of register_component."""
        return asyncio.run(self._async_client.register_component(
            component_type, role, data_columns, auto_create_attributes, allowed_fields
        ))

    def clear_cache(self) -> None:
        """Clear the decision cache."""
        self._async_client.clear_cache()

    def __repr__(self) -> str:
        return f"SyncPolicyClient({self._async_client})"

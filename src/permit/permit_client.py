"""
Permit.io client wrapper with lazy import to avoid compatibility issues.
"""
import os
import asyncio
import json
import logging
from pathlib import Path
from typing import Any, Optional, Set
from src.core.config import PolicyConfig
from src.core.exceptions import PermitConnectionError

logger = logging.getLogger(__name__)


class PermitClient:
    """
    Wrapper for Permit.io client with lazy import.
    """

    def __init__(self, config: PolicyConfig):
        self.config = config
        self._permit = None
        self._api = None
        self._attribute_cache: Set[str] = set()
        self._attribute_cache_path: Optional[Path] = None

        # Load attribute cache on initialization if enabled
        if self.config.PERMIT_ENABLE_ATTRIBUTE_CACHING:
            self._attribute_cache_path = Path(self.config.PERMIT_ATTRIBUTE_CACHE_PATH)
            self._load_attribute_cache()

    async def _preload_existing_attributes(self) -> None:
        """
        Preload existing resource attributes from Permit.io to avoid duplicate API calls.

        This fetches all existing attributes for the 'data' resource and populates the cache,
        preventing unnecessary API calls for attributes that already exist.
        """
        if not self.config.PERMIT_ENABLE_ATTRIBUTE_CACHING:
            return

        try:
            # Import the list method for resource attributes
            from permit import ResourceAttributeRead

            # List all attributes for the 'data' resource
            attributes = await self.api.resource_attributes.list("data")

            # Populate cache with existing attributes
            for attr in attributes:
                cache_key = f"data:{attr.key}"
                self._attribute_cache.add(cache_key)

            # Save to disk
            if self._attribute_cache_path:
                self._save_attribute_cache()

            logger.info(f"Preloaded {len(attributes)} existing attributes from Permit.io into cache")
        except Exception as e:
            logger.warning(f"Failed to preload existing attributes from Permit.io: {e}")
            # Continue without preloading - cache will be populated as we create attributes

    def _load_attribute_cache(self) -> None:
        """Load attribute cache from JSON file if it exists."""
        if not self._attribute_cache_path or not self._attribute_cache_path.exists():
            logger.debug("No attribute cache file found, starting with empty cache")
            return

        try:
            with open(self._attribute_cache_path, "r") as f:
                cached_attrs = json.load(f)
                self._attribute_cache = set(cached_attrs)
            logger.info(f"Loaded {len(self._attribute_cache)} attributes from cache")
        except (json.JSONDecodeError, IOError) as e:
            logger.warning(f"Failed to load attribute cache: {e}, starting with empty cache")
            self._attribute_cache = set()

    def _save_attribute_cache(self) -> None:
        """Save attribute cache to JSON file."""
        if not self._attribute_cache_path:
            return

        try:
            self._attribute_cache_path.parent.mkdir(parents=True, exist_ok=True)
            with open(self._attribute_cache_path, "w") as f:
                json.dump(list(self._attribute_cache), f, indent=2)
            logger.debug(f"Saved {len(self._attribute_cache)} attributes to cache")
        except IOError as e:
            logger.warning(f"Failed to save attribute cache: {e}")

    def invalidate_attribute_cache(self) -> None:
        """Clear the attribute cache (use with caution)."""
        self._attribute_cache.clear()
        logger.info("Attribute cache invalidated")

    def _get_permit_class(self):
        """Lazy import of Permit class to avoid Python 3.14 compatibility issues."""
        try:
            from permit import Permit as _Permit
            return _Permit
        except ImportError:
            raise ImportError("permit package is required. Install with: pip install permit")
        except Exception as e:
            raise PermitConnectionError(f"Error importing permit: {e}")

    @property
    def permit(self):
        """Get or create Permit instance."""
        if self._permit is None:
            Permit = self._get_permit_class()
            self._permit = Permit(
                token=self.config.PERMIT_API_KEY,
                pdp=self.config.PERMIT_PDP_URL,
                pdp_timeout=self.config.PDP_TIMEOUT,
                api_timeout=self.config.API_TIMEOUT,
            )
        return self._permit

    @property
    def api(self):
        """Get Permit API instance."""
        if self._api is None:
            self._api = self.permit.api
        return self._api

    async def check(
        self,
        user: str,
        action: str,
        resource: dict[str, Any],
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Check if a user is permitted to perform an action on a resource.

        Args:
            user: User/component key
            action: Action to perform (read, write, etc.)
            resource: Resource dict with 'type' and 'id'
            context: Additional context for the check

        Returns:
            True if permitted, False otherwise
        """
        try:
            return await self.permit.check(
                user=user,
                action=action,
                resource=resource,
                context=context or {},
            )
        except Exception as e:
            raise PermitConnectionError(f"Permit check failed: {e}")

    async def sync_user(self, user_data: dict[str, Any]) -> None:
        """
        Sync a user/component to Permit.io using API 2.0.0 (async).

        Args:
            user_data: Dictionary with user attributes.
                Should contain: key, email (optional), first_name (optional),
                last_name (optional), attributes (optional)
        """
        import logging
        logger = logging.getLogger(__name__)
        try:
            # Import UserCreate model
            from permit.api.models import UserCreate

            # Permit.io API 2.0.0 uses UserCreate model with api.users.sync()
            # Extract required fields
            user_key = user_data.get("key")
            if not user_key:
                raise ValueError("user_data must contain 'key' field")

            # Build the user sync request with only provided fields
            create_params = {"key": user_key}

            # Add optional fields only if they exist and are not None
            if "email" in user_data and user_data["email"]:
                create_params["email"] = user_data["email"]
            if "first_name" in user_data and user_data["first_name"]:
                create_params["first_name"] = user_data["first_name"]
            if "last_name" in user_data and user_data["last_name"]:
                create_params["last_name"] = user_data["last_name"]
            if "attributes" in user_data:
                create_params["attributes"] = user_data["attributes"]

            logger.info(f"Syncing user to Permit.io: {create_params}")

            # Create UserCreate instance and sync
            user_create = UserCreate(**create_params)
            await self.api.users.sync(user_create)
            logger.info(f"Successfully synced user: {user_key}")
        except Exception as e:
            logger.error(f"Failed to sync user {user_data.get('key')}: {type(e).__name__}: {e}", exc_info=True)
            raise PermitConnectionError(f"Failed to sync user: {e}")

    async def assign_role(
        self, user_key: str, role_key: str, tenant_id: Optional[str] = None
    ) -> None:
        """
        Assign a role to a user (async).

        Args:
            user_key: User/component key
            role_key: Role key
            tenant_id: Tenant ID (defaults to config.PERMIT_DEFAULT_TENANT_ID)
        """
        import logging
        logger = logging.getLogger(__name__)

        if tenant_id is None:
            tenant_id = self.config.PERMIT_DEFAULT_TENANT_ID

        try:
            # Import RoleAssignmentCreate model
            from permit.api.models import RoleAssignmentCreate

            logger.info(f"Assigning role {role_key} to user {user_key} in tenant {tenant_id}")

            # Create RoleAssignmentCreate instance
            assignment = RoleAssignmentCreate(
                user=user_key,
                role=role_key,
                tenant=tenant_id
            )
            await self.api.users.assign_role(assignment)
            logger.info(f"Successfully assigned role {role_key} to user {user_key}")
        except Exception as e:
            logger.error(f"Failed to assign role {role_key} to user {user_key}: {type(e).__name__}: {e}", exc_info=True)
            raise PermitConnectionError(f"Failed to assign role: {e}")

    async def create_resource(
        self,
        resource_key: str,
        resource_data: dict[str, Any],
    ) -> None:
        """
        Create a resource in Permit.io (async).

        Args:
            resource_key: Resource key
            resource_data: Resource data with description, actions, etc.
        """
        try:
            await self.api.resources.create(
                key=resource_key,
                **resource_data,
            )
        except Exception as e:
            raise PermitConnectionError(f"Failed to create resource: {e}")

    async def create_role(self, role_key: str, role_data: dict[str, Any]) -> None:
        """
        Create a role in Permit.io (async).

        Args:
            role_key: Role key
            role_data: Role data with name, description, permissions
        """
        try:
            await self.api.roles.create(
                key=role_key,
                **role_data,
            )
        except Exception as e:
            raise PermitConnectionError(f"Failed to create role: {e}")

    async def create_resource_attribute(
        self,
        resource_key: str,
        attribute_key: str,
        attribute_type: str = "string",
        description: Optional[str] = None,
    ) -> None:
        """
        Create a resource attribute in Permit.io (async).

        Uses an in-memory cache to avoid duplicate API calls. The cache is
        persisted to disk for future restarts.

        Args:
            resource_key: Resource key
            attribute_key: Attribute key
            attribute_type: Attribute type (string, number, boolean, etc.)
            description: Attribute description
        """
        cache_key = f"{resource_key}:{attribute_key}"

        # Check cache first if enabled
        if self.config.PERMIT_ENABLE_ATTRIBUTE_CACHING and cache_key in self._attribute_cache:
            logger.debug(f"Attribute {cache_key} found in cache, skipping API call")
            return

        try:
            from permit import ResourceAttributeCreate

            attribute_data = ResourceAttributeCreate(
                key=attribute_key,
                type=attribute_type,
                description=description or f"Attribute {attribute_key}",
            )

            await self.api.resource_attributes.create(
                resource_key=resource_key,
                attribute_data=attribute_data,
            )

            # Add to cache after successful creation
            if self.config.PERMIT_ENABLE_ATTRIBUTE_CACHING:
                self._attribute_cache.add(cache_key)
                self._save_attribute_cache()

        except Exception as e:
            # If already exists, add to cache anyway (it's there)
            if "already exists" in str(e):
                if self.config.PERMIT_ENABLE_ATTRIBUTE_CACHING:
                    self._attribute_cache.add(cache_key)
                    self._save_attribute_cache()
            else:
                raise PermitConnectionError(f"Failed to create attribute: {e}")

    async def list_users(
        self,
        page: int = 1,
        per_page: Optional[int] = None,
    ) -> list[dict[str, Any]]:
        """
        List all users (components) synced to Permit.io.

        Uses the Permit.io API to retrieve all users, which represent
        registered components in our system.

        Args:
            page: Page number (1-based)
            per_page: Number of results per page (defaults to config value)

        Returns:
            List of user dictionaries with key, attributes, etc.
        """
        if per_page is None:
            per_page = self.config.PERMIT_PAGINATION_PER_PAGE

        try:
            paginated = await self.api.users.list(page=page, per_page=per_page)
            result = []
            for user in paginated.data:
                user_dict = {
                    "key": user.key,
                    "email": getattr(user, "email", None),
                    "first_name": getattr(user, "first_name", None),
                    "last_name": getattr(user, "last_name", None),
                    "attributes": getattr(user, "attributes", {}) or {},
                }
                result.append(user_dict)
            logger.info(f"Listed {len(result)} users from Permit.io (page={page})")
            return result
        except Exception as e:
            logger.error(f"Failed to list users from Permit.io: {type(e).__name__}: {e}", exc_info=True)
            raise PermitConnectionError(f"Failed to list users: {e}")

    async def get_user(self, user_key: str) -> dict[str, Any] | None:
        """
        Get a single user (component) from Permit.io by key.

        Args:
            user_key: The user/component key

        Returns:
            User dictionary or None if not found
        """
        import logging
        logger = logging.getLogger(__name__)
        try:
            user = await self.api.users.get(user_key)
            return {
                "key": user.key,
                "email": getattr(user, "email", None),
                "first_name": getattr(user, "first_name", None),
                "last_name": getattr(user, "last_name", None),
                "attributes": getattr(user, "attributes", {}) or {},
            }
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                logger.info(f"User {user_key} not found in Permit.io")
                return None
            logger.error(f"Failed to get user {user_key}: {type(e).__name__}: {e}", exc_info=True)
            raise PermitConnectionError(f"Failed to get user: {e}")

    async def list_role_assignments(
        self,
        user_key: str | None = None,
        tenant_id: Optional[str] = None,
    ) -> list[dict[str, str]]:
        """
        List role assignments, optionally filtered by user.

        Args:
            user_key: Optional user key to filter by
            tenant_id: Tenant ID (defaults to config.PERMIT_DEFAULT_TENANT_ID)

        Returns:
            List of role assignment dicts with 'user', 'role', 'tenant'
        """
        import logging
        logger = logging.getLogger(__name__)

        if tenant_id is None:
            tenant_id = self.config.PERMIT_DEFAULT_TENANT_ID

        try:
            kwargs = {"tenant_key": tenant_id}
            if user_key:
                kwargs["user_key"] = user_key
            assignments = await self.api.role_assignments.list(**kwargs)
            result = []
            for ra in assignments:
                result.append({
                    "user": ra.user,
                    "role": ra.role,
                    "tenant": ra.tenant,
                })
            return result
        except Exception as e:
            logger.error(f"Failed to list role assignments: {type(e).__name__}: {e}", exc_info=True)
            raise PermitConnectionError(f"Failed to list role assignments: {e}")

    def sync_user_sync(self, user_data: dict[str, Any]) -> None:
        """
        Synchronous wrapper for sync_user.

        Args:
            user_data: Dictionary with user attributes
        """
        asyncio.run(self.sync_user(user_data))

    def assign_role_sync(self, user_key: str, role_key: str, tenant_id: Optional[str] = None) -> None:
        """
        Synchronous wrapper for assign_role.

        Args:
            user_key: User/component key
            role_key: Role key
            tenant_id: Tenant ID (defaults to config.PERMIT_DEFAULT_TENANT_ID)
        """
        asyncio.run(self.assign_role(user_key, role_key, tenant_id))

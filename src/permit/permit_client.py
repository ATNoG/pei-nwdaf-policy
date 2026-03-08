"""
Permit.io client wrapper with lazy import to avoid compatibility issues.
"""
import os
import asyncio
from typing import Any, Optional
from src.core.config import PolicyConfig
from src.core.exceptions import PermitConnectionError


class PermitClient:
    """
    Wrapper for Permit.io client with lazy import.
    """

    def __init__(self, config: PolicyConfig):
        self.config = config
        self._permit = None
        self._api = None

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
        self, user_key: str, role_key: str, tenant_id: str = "default"
    ) -> None:
        """
        Assign a role to a user (async).

        Args:
            user_key: User/component key
            role_key: Role key
            tenant_id: Tenant ID (default: "default")
        """
        import logging
        logger = logging.getLogger(__name__)
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

        Args:
            resource_key: Resource key
            attribute_key: Attribute key
            attribute_type: Attribute type (string, number, boolean, etc.)
            description: Attribute description
        """
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
        except Exception as e:
            # Ignore if already exists
            if "already exists" not in str(e):
                raise PermitConnectionError(f"Failed to create attribute: {e}")

    def sync_user_sync(self, user_data: dict[str, Any]) -> None:
        """
        Synchronous wrapper for sync_user.

        Args:
            user_data: Dictionary with user attributes
        """
        asyncio.run(self.sync_user(user_data))

    def assign_role_sync(self, user_key: str, role_key: str, tenant_id: str = "default") -> None:
        """
        Synchronous wrapper for assign_role.

        Args:
            user_key: User/component key
            role_key: Role key
            tenant_id: Tenant ID (default: "default")
        """
        asyncio.run(self.assign_role(user_key, role_key, tenant_id))

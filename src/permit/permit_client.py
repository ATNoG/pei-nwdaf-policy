"""
Permit.io client wrapper with lazy import to avoid compatibility issues.
"""
import os
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

    async def check_sync(
        self,
        user: str,
        action: str,
        resource: dict[str, Any],
        context: Optional[dict[str, Any]] = None,
    ) -> bool:
        """
        Synchronous version of check for non-async contexts.
        """
        import asyncio
        return await self.check(user, action, resource, context)

    def sync_user(self, user_data: dict[str, Any]) -> None:
        """
        Sync a user/component to Permit.io.

        Args:
            user_data: Dictionary with user attributes
        """
        try:
            self.api.users.sync_user(user_data)
        except Exception as e:
            raise PermitConnectionError(f"Failed to sync user: {e}")

    def assign_role(
        self, user_key: str, role_key: str, tenant_id: str = "default"
    ) -> None:
        """
        Assign a role to a user.

        Args:
            user_key: User/component key
            role_key: Role key
            tenant_id: Tenant ID (default: "default")
        """
        try:
            self.api.users.assign_role(user_key, role_key, tenant_id)
        except Exception as e:
            raise PermitConnectionError(f"Failed to assign role: {e}")

    def create_resource(
        self,
        resource_key: str,
        resource_data: dict[str, Any],
    ) -> None:
        """
        Create a resource in Permit.io.

        Args:
            resource_key: Resource key
            resource_data: Resource data with description, actions, etc.
        """
        try:
            self.api.resources.create(
                key=resource_key,
                **resource_data,
            )
        except Exception as e:
            raise PermitConnectionError(f"Failed to create resource: {e}")

    def create_role(self, role_key: str, role_data: dict[str, Any]) -> None:
        """
        Create a role in Permit.io.

        Args:
            role_key: Role key
            role_data: Role data with name, description, permissions
        """
        try:
            self.api.roles.create(
                key=role_key,
                **role_data,
            )
        except Exception as e:
            raise PermitConnectionError(f"Failed to create role: {e}")

    def create_resource_attribute(
        self,
        resource_key: str,
        attribute_key: str,
        attribute_type: str,
        description: Optional[str] = None,
    ) -> None:
        """
        Create a resource attribute in Permit.io.

        Args:
            resource_key: Resource key
            attribute_key: Attribute key
            attribute_type: Attribute type (string, number, boolean, etc.)
            description: Attribute description
        """
        try:
            self.api.resource_attributes.create(
                resource_key=resource_key,
                attribute_key=attribute_key,
                type=attribute_type,
                description=description or f"Attribute {attribute_key}",
            )
        except Exception as e:
            # Ignore if already exists
            if "already exists" not in str(e):
                raise PermitConnectionError(f"Failed to create attribute: {e}")

    def set_user_attributes(
        self,
        user_key: str,
        attributes: dict[str, Any],
    ) -> None:
        """
        Set attributes for a user.

        Args:
            user_key: User/component key
            attributes: Dictionary of attributes
        """
        try:
            self.api.users.set_attributes(user_key, attributes)
        except Exception as e:
            raise PermitConnectionError(f"Failed to set user attributes: {e}")

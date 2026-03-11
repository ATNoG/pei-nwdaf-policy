"""
Component registration and management service.

The registry is purely in-memory. Only components that actively call
``register_component`` (i.e. are actually running) appear in the list.
There is no restore from Permit.io or a local JSON file on startup, so
stale/test components from previous runs never pollute the dropdown.
"""
from __future__ import annotations
import asyncio
import logging
from typing import Optional, List, TYPE_CHECKING
from src.core.policy import ComponentConfig, PolicyEngine
from src.permit.permit_client import PermitClient
from src.core.config import PolicyConfig
from src.core.exceptions import ComponentRegistrationError, ComponentNotFoundError

if TYPE_CHECKING:
    from src.services.transformer_service import TransformerService

logger = logging.getLogger(__name__)


class ComponentService:
    """
    Service for managing component registration and configuration.
    """

    def __init__(self, policy_engine: PolicyEngine, permit_client: PermitClient, config: PolicyConfig):
        """
        Initialize the component service.

        Args:
            policy_engine: Policy engine instance
            permit_client: Permit.io client
            config: Policy configuration
        """
        self.policy_engine = policy_engine
        self.permit_client = permit_client
        self.config = config
        # Will be set after TransformerService is created (avoids circular init)
        self._transformer_service: Optional[TransformerService] = None

    def set_transformer_service(self, transformer_service: TransformerService) -> None:
        """
        Inject the TransformerService so we can invalidate its field cache
        whenever a component re-registers.
        """
        self._transformer_service = transformer_service



    async def restore_from_permit(self) -> int:
        """
        Restore component registrations from Permit.io (primary source).

        Lists every Permit.io user and checks for a ``component_type``
        attribute — the marker that ``register_component`` sets via
        ``sync_user``.  For each match it rebuilds a ComponentConfig
        from the stored attributes (``allowed_fields``, ``data_columns``,
        etc.) and fetches role assignments so the ``role`` field is
        populated.

        **No connection to the actual components is made.**  This only
        restores the metadata that Policy needs (field lists, roles,
        allowed-fields mappings) so that pipelines and field discovery
        work immediately after a restart, before the components
        re-register themselves.

        Returns:
            Number of components restored.
        """
        try:
            users = await self.permit_client.list_users(per_page=100)
        except Exception as e:
            logger.warning(f"Cannot restore from Permit.io (list_users failed): {e}")
            return 0

        restored = 0

        for user in users:
            attrs = user.get("attributes", {})
            component_type = attrs.get("component_type")

            # Only restore users that were registered as system components
            if not component_type:
                continue

            comp_id = user["key"]

            # Skip already-registered components
            if comp_id in self.policy_engine.components:
                logger.debug(
                    f"Component {comp_id} already registered, skipping Permit.io restore"
                )
                continue

            # Extract allowed_fields (stored as a nested dict attribute)
            allowed_fields = attrs.get("allowed_fields", {})
            if isinstance(allowed_fields, str):
                try:
                    allowed_fields = json.loads(allowed_fields)
                except (json.JSONDecodeError, TypeError):
                    allowed_fields = {}

            # Extract data_columns
            data_columns = attrs.get("data_columns", [])
            if isinstance(data_columns, str):
                try:
                    data_columns = json.loads(data_columns)
                except (json.JSONDecodeError, TypeError):
                    data_columns = []

            # Try to get role(s) from role assignments
            role = None
            additional_roles_list: List[str] = []
            try:
                assignments = await self.permit_client.list_role_assignments(
                    user_key=comp_id
                )
                if assignments:
                    role = assignments[0].get("role")
                    additional_roles_list = [
                        a.get("role") for a in assignments[1:] if a.get("role")
                    ]
            except Exception as e:
                logger.debug(f"Could not fetch role for {comp_id}: {e}")

            # Build remaining attributes (exclude meta keys we handle separately)
            meta_keys = {"component_type", "allowed_fields", "data_columns"}
            extra_attrs = {k: v for k, v in attrs.items() if k not in meta_keys}
            if data_columns:
                extra_attrs["data_columns"] = data_columns

            config = ComponentConfig(
                component_id=comp_id,
                component_type=component_type,
                role=role,
                additional_roles=additional_roles_list,
                allowed_fields=allowed_fields,
                attributes=extra_attrs,
            )

            self.policy_engine.components[comp_id] = config
            restored += 1
            logger.info(
                f"Restored component from Permit.io: {comp_id} "
                f"(type={component_type}, role={role})"
            )

        if restored:
            # Persist what we recovered so next restart is faster
            await self._save_components_to_file()

        logger.info(
            f"Restored {restored} component(s) from Permit.io "
            f"({len(self.policy_engine.components)} total in registry)"
        )
        return restored

    # ==================== Core CRUD ====================

    async def register_component(
        self,
        component_id: str,
        component_type: str,
        role: Optional[str] = None,
        additional_roles: Optional[List[str]] = None,
        permit_user_key: Optional[str] = None,
        data_columns: Optional[List[str]] = None,
        auto_create_attributes: bool = True,
        allowed_fields: Optional[dict[str, List[str]]] = None,
        attributes: Optional[dict[str, any]] = None
    ) -> ComponentConfig:
        """
        Register a component with the policy service.

        The component is persisted to:
          1. In-memory registry (immediate)
          2. Permit.io as a user (via policy engine)
          3. Local JSON file (for restart recovery)

        Args:
            component_id: Unique component identifier
            component_type: Type of component
            role: Optional primary role to assign
            additional_roles: Optional additional roles beyond primary
            permit_user_key: Optional Permit.io user key override (for sharing)
            data_columns: Optional data columns for auto-attribute creation
            auto_create_attributes: Whether to auto-create attributes
            allowed_fields: Optional allowed fields per sink
            attributes: Additional component attributes

        Returns:
            ComponentConfig of registered component
        """
        logger.info(f"ComponentService: Registering component {component_id} of type {component_type}")

        # Build attributes, including data_columns for field discovery
        component_attributes = attributes or {}
        if data_columns:
            component_attributes["data_columns"] = data_columns

        config = ComponentConfig(
            component_id=component_id,
            component_type=component_type,
            role=role,
            additional_roles=additional_roles or [],
            permit_user_key=permit_user_key,
            allowed_fields=allowed_fields or {},
            attributes=component_attributes
        )

        # Register in policy engine (syncs to Permit.io)
        logger.info(f"ComponentService: Calling policy_engine.register_component for {component_id}")
        success = await self.policy_engine.register_component(config)
        if not success:
            raise ComponentRegistrationError(f"Failed to register component: {component_id}")

        # Invalidate any cached field-discovery results that involve this component.
        # Without this, a stale empty-list result would persist even after the
        # component re-registers with newly discovered fields.
        if self._transformer_service is not None:
            cleared = self._transformer_service.invalidate_cache_for_component(component_id)
            if cleared:
                logger.info(f"ComponentService: Cleared {cleared} stale field-cache entries for {component_id}")

        # Auto-create attributes if enabled — fired as a background task so the
        # HTTP response is returned immediately and Permit.io attribute creation
        # (one call per field) does not block the client registration request.
        if auto_create_attributes and data_columns:
            logger.info(
                f"ComponentService: Scheduling background attribute creation "
                f"for {component_id} ({len(data_columns)} field(s))"
            )
            asyncio.create_task(
                self._create_attributes_from_columns(component_id, data_columns)
            )

        logger.info(f"ComponentService: Successfully registered component {component_id}")
        return config

    async def _create_attributes_from_columns(
        self,
        component_id: str,
        columns: List[str]
    ) -> None:
        """
        Auto-create Permit.io resource attributes from data columns in parallel.

        All attribute-creation coroutines are launched concurrently via
        ``asyncio.gather`` so the total wall-clock time is ~1 round-trip
        instead of O(N) sequential round-trips.  Individual failures
        (e.g. "already exists") are swallowed and logged at DEBUG level.

        Args:
            component_id: Component ID (used only for logging)
            columns: List of column names to create attributes for
        """
        async def _create_one(column: str) -> None:
            attr_key = f"{self.config.ATTRIBUTE_PREFIX}{column}"
            try:
                await self.permit_client.create_resource_attribute(
                    resource_key="data",
                    attribute_key=attr_key,
                    attribute_type="string",
                    description=f"Auto-created attribute for column: {column}",
                )
            except Exception as exc:
                # "Already exists" is the normal case on re-registration — ignore
                logger.debug(
                    f"Attribute {attr_key} for {component_id}: {exc}"
                )

        results = await asyncio.gather(
            *[_create_one(col) for col in columns],
            return_exceptions=True,
        )

        errors = [r for r in results if isinstance(r, Exception)]
        if errors:
            logger.warning(
                f"_create_attributes_from_columns: {len(errors)}/{len(columns)} "
                f"attribute(s) failed for {component_id}"
            )
        else:
            logger.info(
                f"_create_attributes_from_columns: created/verified "
                f"{len(columns)} attribute(s) for {component_id}"
            )

    async def unregister_component(self, component_id: str) -> bool:
        """
        Unregister a component.

        Args:
            component_id: Component ID to unregister

        Returns:
            True if successful
        """
        return self.policy_engine.unregister_component(component_id)

    async def get_component(self, component_id: str) -> Optional[ComponentConfig]:
        """
        Get component configuration.

        Args:
            component_id: Component ID

        Returns:
            ComponentConfig or None
        """
        return self.policy_engine.get_component(component_id)

    async def list_components(self) -> List[ComponentConfig]:
        """
        List all registered components.

        Returns:
            List of ComponentConfig
        """
        return self.policy_engine.list_components()

    async def add_field_filter(
        self,
        source: str,
        sink: str,
        allowed_fields: List[str],
        denied_fields: List[str] = None
    ) -> None:
        """
        Add a field filter between components.

        Args:
            source: Source component ID
            sink: Sink component ID
            allowed_fields: Fields to allow (whitelist)
            denied_fields: Fields to deny (blacklist)
        """
        self.policy_engine.add_field_filter(source, sink, allowed_fields, denied_fields)
"""
Component registration and management service.

The registry is purely in-memory. Only components that actively call
``register_component`` (i.e. are actually running) appear in the list.
There is no restore from Permit.io or a local JSON file on startup, so
stale/test components from previous runs never pollute the dropdown.
"""
from __future__ import annotations
import asyncio
import json
import logging
from typing import Optional, List, TYPE_CHECKING, Dict
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

        # Debounced sync state
        self._pending_syncs: Dict[str, ComponentConfig] = {}  # component_id -> ComponentConfig
        self._sync_retries: Dict[str, int] = {}  # component_id -> retry count
        self._sync_lock = asyncio.Lock()
        self._sync_task: Optional[asyncio.Task] = None
        self._sync_event = asyncio.Event()
        self._shutdown_flag = False

    def set_transformer_service(self, transformer_service: TransformerService) -> None:
        """
        Inject the TransformerService so we can invalidate its field cache
        whenever a component re-registers.
        """
        self._transformer_service = transformer_service

    # ==================== Debounced Sync ====================

    async def start_permit_sync(self) -> None:
        """Start the background Permit.io sync task."""
        if self._sync_task is not None and not self._sync_task.done():
            logger.warning("Permit sync task already running")
            return

        self._shutdown_flag = False
        self._sync_task = asyncio.create_task(self._permit_sync_loop())
        logger.info(
            f"Permit.io sync task started (interval={self.config.PERMIT_SYNC_INTERVAL_SECONDS}s)"
        )

    async def stop_permit_sync(self) -> None:
        """Stop the background Permit.io sync task gracefully."""
        self._shutdown_flag = True
        self._sync_event.set()  # Wake up the sync loop immediately

        if self._sync_task and not self._sync_task.done():
            try:
                await asyncio.wait_for(self._sync_task, timeout=5.0)
                logger.info("Permit.io sync task stopped")
            except asyncio.TimeoutError:
                logger.warning("Permit.io sync task did not stop in time")
            except Exception as e:
                logger.error(f"Error stopping Permit sync task: {e}")

        # Process any remaining pending syncs
        await self._process_pending_syncs()

    async def _permit_sync_loop(self) -> None:
        """Background task that processes pending syncs at intervals."""
        while not self._shutdown_flag:
            try:
                # Wait for the configured interval or until woken up
                await asyncio.wait_for(
                    self._sync_event.wait(),
                    timeout=self.config.PERMIT_SYNC_INTERVAL_SECONDS
                )
                self._sync_event.clear()

                if self._shutdown_flag:
                    break

                await self._process_pending_syncs()
            except asyncio.TimeoutError:
                # Normal timeout, process pending syncs
                await self._process_pending_syncs()
            except Exception as e:
                logger.error(f"Error in permit sync loop: {e}", exc_info=True)

    async def _process_pending_syncs(self) -> None:
        """
        Process all pending sync operations.

        Dequeues and processes pending component registrations including:
        - User sync to Permit.io
        - Role assignment
        - Resource attribute creation

        Implements retry logic with exponential backoff for rate-limited requests.
        """
        async with self._sync_lock:
            if not self._pending_syncs:
                return

            pending = self._pending_syncs.copy()
            self._pending_syncs.clear()

        if pending:
            logger.info(f"Processing {len(pending)} pending component sync(s)")
            for component_id, config in pending.items():
                try:
                    # Sync user to Permit.io
                    await self._sync_user_to_permit(config)

                    # Assign roles
                    await self._assign_roles_to_user(config)

                    # Create attributes if data_columns are present
                    data_columns = config.attributes.get("data_columns", [])
                    if data_columns:
                        await self._create_attributes_from_columns(component_id, data_columns)

                    # Success - clear retry count
                    async with self._sync_lock:
                        self._sync_retries.pop(component_id, None)

                    logger.info(f"Successfully synced component {component_id} to Permit.io")
                except Exception as e:
                    error_msg = str(e)
                    is_rate_limited = "429" in error_msg

                    # Check retry count
                    async with self._sync_lock:
                        retry_count = self._sync_retries.get(component_id, 0)
                        max_retries = 5  # Maximum retry attempts

                    if is_rate_limited and retry_count < max_retries:
                        # Exponential backoff: 2^retry_count * base_delay
                        backoff_delay = min(2 ** retry_count * 5, 60)  # Max 60 seconds

                        async with self._sync_lock:
                            self._sync_retries[component_id] = retry_count + 1
                            # Re-queue for retry
                            self._pending_syncs[component_id] = config

                        logger.warning(
                            f"Rate limited for {component_id} (attempt {retry_count + 1}/{max_retries}), "
                            f"re-queueing with {backoff_delay}s backoff"
                        )

                        # Schedule retry with backoff delay
                        asyncio.create_task(self._schedule_retry(component_id, config, backoff_delay))
                    else:
                        # Max retries exceeded or non-rate-limit error
                        async with self._sync_lock:
                            self._sync_retries.pop(component_id, None)

                        if is_rate_limited:
                            logger.error(
                                f"Max retries exceeded for {component_id}, giving up. "
                                f"Component is registered locally but not synced to Permit.io."
                            )
                        else:
                            logger.error(
                                f"Error processing sync for {component_id}: {e}",
                                exc_info=True
                            )

    async def _schedule_retry(self, component_id: str, config: ComponentConfig, delay: float) -> None:
        """Schedule a retry after the specified delay."""
        await asyncio.sleep(delay)
        async with self._sync_lock:
            self._pending_syncs[component_id] = config
        self._sync_event.set()

    def _queue_permit_sync(self, component_id: str, config: ComponentConfig) -> None:
        """
        Queue a component for Permit.io sync (user, roles, attributes).

        If the component is already queued, the config is replaced with the latest one.

        Args:
            component_id: Component ID to sync
            config: ComponentConfig with all registration details
        """
        # If sync interval is 0, sync immediately (legacy behavior)
        if self.config.PERMIT_SYNC_INTERVAL_SECONDS <= 0:
            logger.info(
                f"Sync interval is 0, syncing {component_id} to Permit.io immediately"
            )
            asyncio.create_task(self._sync_component_immediately(config))
            return

        # Queue for batched sync
        asyncio.create_task(self._queue_permit_sync_async(component_id, config))

    async def _queue_permit_sync_async(self, component_id: str, config: ComponentConfig) -> None:
        """Async helper to queue sync operations."""
        async with self._sync_lock:
            # Replace existing config with latest (merges data_columns if needed)
            existing = self._pending_syncs.get(component_id)
            if existing:
                # Merge data_columns to avoid losing fields
                existing_columns = set(existing.attributes.get("data_columns", []))
                new_columns = set(config.attributes.get("data_columns", []))
                merged_columns = list(existing_columns | new_columns)
                if merged_columns:
                    config.attributes = config.attributes or {}
                    config.attributes["data_columns"] = merged_columns

            self._pending_syncs[component_id] = config
            logger.debug(f"Queued component {component_id} for Permit.io sync")

            # Wake up the sync loop
            self._sync_event.set()

    async def _sync_component_immediately(self, config: ComponentConfig) -> None:
        """Immediately sync a component to Permit.io (legacy behavior when interval=0)."""
        try:
            await self._sync_user_to_permit(config)
            await self._assign_roles_to_user(config)
            data_columns = config.attributes.get("data_columns", [])
            if data_columns:
                await self._create_attributes_from_columns(config.component_id, data_columns)
        except Exception as e:
            logger.error(f"Error during immediate sync for {config.component_id}: {e}")

    async def _sync_user_to_permit(self, config: ComponentConfig) -> None:
        """Sync a component user to Permit.io."""
        user_key = config.effective_user_key
        user_data = {
            "key": user_key,
            "attributes": {
                "component_type": config.component_type,
                "allowed_fields": config.allowed_fields,
                **config.attributes,
            }
        }
        # Don't include data_columns in attributes (it's large and not needed for user sync)
        user_data["attributes"].pop("data_columns", None)

        await self.permit_client.sync_user(user_data)
        logger.info(f"Synced user {user_key} to Permit.io")

    async def _assign_roles_to_user(self, config: ComponentConfig) -> None:
        """Assign all roles to a component user in Permit.io."""
        user_key = config.effective_user_key
        for role_key in config.all_roles:
            try:
                await self.permit_client.assign_role(
                    user_key=user_key,
                    role_key=role_key,
                    tenant_id=self.config.PERMIT_DEFAULT_TENANT_ID
                )
                logger.info(f"Assigned role {role_key} to {user_key}")
            except Exception as role_error:
                # Log but don't fail sync if role doesn't exist
                logger.warning(f"Failed to assign role {role_key} to {user_key}: {role_error}")



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
            users = await self.permit_client.list_users(
                per_page=self.config.PERMIT_PAGINATION_PER_PAGE
            )
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

    async def _save_components_to_file(self) -> None:
        """Save current in-memory components to the JSON file."""
        import json
        from pathlib import Path

        config_path = Path(self.config.COMPONENT_CONFIG_PATH)
        config_path.parent.mkdir(parents=True, exist_ok=True)

        components_data = []
        for comp in self.policy_engine.components.values():
            components_data.append({
                "component_id": comp.component_id,
                "component_type": comp.component_type,
                "role": comp.role,
                "additional_roles": list(comp.additional_roles) if comp.additional_roles else [],
                "permit_user_key": comp.permit_user_key,
                "allowed_fields": comp.allowed_fields,
                "attributes": comp.attributes
            })

        with open(config_path, "w") as f:
            json.dump(components_data, f, indent=2)

        logger.info(f"Saved {len(components_data)} components to {config_path}")

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
          1. In-memory registry (immediate) - available for API queries instantly
          2. Permit.io as a user (debounced) - synced at configured intervals
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

        # Store in policy engine registry IMMEDIATELY (for real-time API access)
        # This does NOT sync to Permit.io - that happens via debounced queue
        self.policy_engine.components[component_id] = config
        logger.info(f"ComponentService: Stored {component_id} in local registry")

        # Invalidate any cached field-discovery results that involve this component.
        # Without this, a stale empty-list result would persist even after the
        # component re-registers with newly discovered fields.
        if self._transformer_service is not None:
            cleared = self._transformer_service.invalidate_cache_for_component(component_id)
            if cleared:
                logger.info(f"ComponentService: Cleared {cleared} stale field-cache entries for {component_id}")

        # Queue Permit.io sync (user, roles, attributes) for debounced processing
        # The component is immediately available in the in-memory registry,
        # but Permit.io synchronization happens at configured intervals.
        if auto_create_attributes or role or additional_roles:
            logger.info(
                f"ComponentService: Queuing Permit.io sync for {component_id} "
                f"(will sync within {self.config.PERMIT_SYNC_INTERVAL_SECONDS}s)"
            )
            self._queue_permit_sync(component_id, config)

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
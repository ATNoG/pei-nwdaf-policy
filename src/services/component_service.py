"""
Component registration and management service.
"""
from typing import Optional, List
from src.core.policy import ComponentConfig, PolicyEngine
from src.permit.permit_client import PermitClient
from src.core.config import PolicyConfig
from src.core.exceptions import ComponentRegistrationError, ComponentNotFoundError
from src.models.enums import ComponentType
import logging


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

    async def register_component(
        self,
        component_id: str,
        component_type: str,
        role: Optional[str] = None,
        data_columns: Optional[List[str]] = None,
        auto_create_attributes: bool = True,
        allowed_fields: Optional[dict[str, List[str]]] = None,
        attributes: Optional[dict[str, any]] = None
    ) -> ComponentConfig:
        """
        Register a component with the policy service.

        Args:
            component_id: Unique component identifier
            component_type: Type of component
            role: Optional role to assign
            data_columns: Optional data columns for auto-attribute creation
            auto_create_attributes: Whether to auto-create attributes
            allowed_fields: Optional allowed fields per sink
            attributes: Additional component attributes

        Returns:
            ComponentConfig of registered component
        """
        logger.info(f"ComponentService: Registering component {component_id} of type {component_type}")
        config = ComponentConfig(
            component_id=component_id,
            component_type=component_type,
            role=role,
            allowed_fields=allowed_fields or {},
            attributes=attributes or {}
        )

        # Register in policy engine
        logger.info(f"ComponentService: Calling policy_engine.register_component for {component_id}")
        success = await self.policy_engine.register_component(config)
        if not success:
            raise ComponentRegistrationError(f"Failed to register component: {component_id}")

        # Auto-create attributes if enabled
        if auto_create_attributes and data_columns:
            logger.info(f"ComponentService: Creating attributes for {component_id}")
            await self._create_attributes_from_columns(component_id, data_columns)

        logger.info(f"ComponentService: Successfully registered component {component_id}")
        return config

    async def _create_attributes_from_columns(
        self,
        component_id: str,
        columns: List[str]
    ) -> None:
        """
        Auto-create Permit.io attributes from data columns (async).

        Args:
            component_id: Component ID
            columns: List of column names
        """
        for column in columns:
            attr_key = f"{self.config.ATTRIBUTE_PREFIX}{column}"

            try:
                await self.permit_client.create_resource_attribute(
                    resource_key="data",
                    attribute_key=attr_key,
                    attribute_type="string",
                    description=f"Auto-created attribute for column: {column}"
                )
            except Exception as e:
                # Ignore if already exists
                pass

        # Note: set_user_attributes is not available in Permit.io API 2.0.0
        # Attributes are set during user sync via the sync_user call

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

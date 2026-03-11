"""
Policy Engine - Core policy evaluation and data transformation logic.

This module implements the central policy enforcement engine that:
1. Authorizes interactions using Permit.io
2. Applies field-level data filtering and transformations
3. Manages ML model access control based on data type roles
"""
from dataclasses import dataclass, field
from typing import Any, Optional, Callable
from src.permit.permit_client import PermitClient
from src.core.config import PolicyConfig
from src.core.exceptions import AuthorizationError, ComponentNotFoundError
from src.transformers.pipeline import TransformerPipeline
import asyncio


@dataclass
class ComponentConfig:
    """Configuration for a component in the system."""
    component_id: str
    component_type: str  # "ml_agent", "data_source", "api", "storage", etc.
    allowed_fields: dict[str, list[str]] = field(default_factory=dict)
    # allowed_fields[sink_component] = [field1, field2, ...]
    role: Optional[str] = None
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class FieldFilterConfig:
    """Configuration for field-level filtering between components."""
    source: str
    sink: str
    allowed_fields: list[str] = field(default_factory=list)
    denied_fields: list[str] = field(default_factory=list)


@dataclass
class AccessDecision:
    """Result of an access control decision."""
    allowed: bool
    reason: str = ""
    transformations_applied: list[str] = field(default_factory=list)


class PolicyEngine:
    """
    Central policy evaluation engine using Permit.io for authorization
    and field-level data filtering/transformation.
    """

    def __init__(self, config: PolicyConfig, permit_client: PermitClient):
        """
        Initialize the PolicyEngine.

        Args:
            config: Policy service configuration
            permit_client: Permit.io client wrapper
        """
        self.config = config
        self.permit = permit_client

        # Component registry: component_id -> ComponentConfig
        self.components: dict[str, ComponentConfig] = {}

        # Field filtering rules: (source, sink) -> FieldFilterConfig
        self.field_filters: dict[tuple[str, str], FieldFilterConfig] = {}

        # Transformer pipelines: pipeline_id -> TransformerPipeline
        self.transformer_pipelines: dict[str, TransformerPipeline] = {}

        # ML model data type mapping for role assignment
        self._ml_data_type_roles: dict[str, str] = {
            "network_prediction": "network_predictor_role",
            "fraud_detection": "fraud_detector_role",
            "anomaly_detection": "anomaly_detector_role",
            "latency_forecasting": "network_predictor_role",
            "capacity_planning": "network_predictor_role",
            "churn_prediction": "fraud_detector_role",
        }

    async def register_component(self, config: ComponentConfig) -> bool:
        """
        Register a component and sync it to Permit.io as a user (async).

        Args:
            config: ComponentConfig with component details

        Returns:
            True if successful, False otherwise
        """
        import logging
        logger = logging.getLogger(__name__)
        try:
            # Sync component to Permit.io as a user
            user_data = {
                "key": config.component_id,
                "attributes": {
                    "component_type": config.component_type,
                    "allowed_fields": config.allowed_fields,
                    **config.attributes,
                }
            }
            logger.info(f"Registering component {config.component_id} with role {config.role}")
            await self.permit.sync_user(user_data)

            # Assign role if specified
            if config.role:
                logger.info(f"Assigning role {config.role} to component {config.component_id}")
                try:
                    await self.permit.assign_role(
                        user_key=config.component_id,
                        role_key=config.role,
                        tenant_id="default"
                    )
                except Exception as role_error:
                    # Log but don't fail registration if role doesn't exist
                    logger.warning(f"Failed to assign role {config.role} to component {config.component_id}: {role_error}")
                    logger.warning(f"Component {config.component_id} registered but role assignment skipped. Create the role in Permit.io dashboard or API.")

            self.components[config.component_id] = config
            logger.info(f"Successfully registered component {config.component_id}")
            return True
        except Exception as e:
            logger.error(f"Error registering component {config.component_id}: {type(e).__name__}: {e}", exc_info=True)
            raise AuthorizationError(
                f"Error registering component {config.component_id}",
                source=config.component_id,
                sink="system",
                action="register",
                details={"error": str(e)}
            )

    async def register_ml_agent(
        self,
        agent_id: str,
        model_name: str,
        allowed_outputs: list[str] = None,
        data_type: str = "network_prediction"
    ) -> bool:
        """
        Register an ML agent from MLflow/ML component (async).

        Args:
            agent_id: Unique identifier for the ML agent
            model_name: Name of the ML model
            allowed_outputs: List of output fields this agent can produce
            data_type: Data type for role assignment

        Returns:
            True if successful
        """
        # Determine role based on data type
        role = self._ml_data_type_roles.get(data_type, "network_predictor_role")

        config = ComponentConfig(
            component_id=agent_id,
            component_type="ml_agent",
            role=role,
            allowed_fields={"default": allowed_outputs or []},
            attributes={
                "model_name": model_name,
                "data_type": data_type,
            }
        )
        return await self.register_component(config)

    async def register_data_source(
        self,
        source_id: str,
        source_type: str,
        allowed_fields: dict[str, list[str]] = None
    ) -> bool:
        """
        Register a data source component.

        Args:
            source_id: Unique identifier for the data source
            source_type: Type of data source (database, api, file, etc.)
            allowed_fields: Dict mapping sink components to allowed fields

        Returns:
            True if successful
        """
        config = ComponentConfig(
            component_id=source_id,
            component_type=source_type,
            role="data_source_role",
            allowed_fields=allowed_fields or {}
        )
        return await self.register_component(config)

    def add_field_filter(
        self,
        source: str,
        sink: str,
        allowed_fields: list[str],
        denied_fields: list[str] = None
    ) -> None:
        """
        Add a field filtering rule for data flowing from source to sink.

        Args:
            source: Source component ID
            sink: Sink component ID
            allowed_fields: List of fields to allow through
            denied_fields: List of fields to explicitly block
        """
        key = (source, sink)
        self.field_filters[key] = FieldFilterConfig(
            source=source,
            sink=sink,
            allowed_fields=allowed_fields,
            denied_fields=denied_fields or []
        )

    def set_transformer_pipeline(self, pipeline_id: str, pipeline: TransformerPipeline) -> None:
        """
        Set a transformer pipeline for a specific source-sink pair.

        Args:
            pipeline_id: Unique identifier for the pipeline (usually "source_to_sink")
            pipeline: TransformerPipeline instance
        """
        self.transformer_pipelines[pipeline_id] = pipeline

    def get_transformer_pipeline(self, pipeline_id: str) -> Optional[TransformerPipeline]:
        """Get a transformer pipeline by ID."""
        return self.transformer_pipelines.get(pipeline_id)

    async def check_access(
        self,
        source_id: str,
        sink_id: str,
        data_type: str = "data",
        action: str = "read"
    ) -> AccessDecision:
        """
        Check if source is authorized to send data to sink.

        The policy check supports two scenarios:
        1. Infrastructure sources (e.g., Kafka) - not registered, check sink permissions only
        2. Component sources (e.g., Ingestion-Service) - registered, check if they can send to sink

        Args:
            source_id: Source component ID
            sink_id: Sink component ID (e.g., "data-storage:influx")
            data_type: Type of data being accessed
            action: Action type (read, write, inference, etc.)

        Returns:
            AccessDecision with allowed status and reason
        """
        # Extract the component_id from sink_id (e.g., "data-storage" from "data-storage:influx")
        # This is the actual registered user in Permit.io
        sink_component = sink_id.split(":")[0] if ":" in sink_id else sink_id

        try:
            # Check if sink component exists (if registration required)
            if self.config.REQUIRE_REGISTRATION:
                if sink_component not in self.components:
                    raise ComponentNotFoundError(sink_component)

            # Determine which user to check:
            # - If source is a registered component, check if source can send to sink
            # - If source is not registered (e.g., infrastructure like Kafka), check sink permissions
            # - If source has resource type (e.g., "data-storage:influx"), use base component for check
            user_to_check = source_id
            check_sink_permissions = False

            # Extract base component from resource-typed source (e.g., "data-storage:influx" -> "data-storage")
            source_component = source_id.split(":")[0] if ":" in source_id else source_id

            if source_component not in self.components:
                # Source (or its base component) is not a registered component (e.g., Kafka)
                # Fall back to checking if the sink has permission to perform the action
                user_to_check = sink_component
                check_sink_permissions = True
            else:
                # Source component is registered - use it for the policy check
                user_to_check = source_component

            # Perform the policy check
            import logging
            logger = logging.getLogger(__name__)
            logger.info(f"PERMIT CHECK: user={user_to_check}, action={action}, resource_type={data_type}, resource_id={sink_id}")
            logger.info(f"PERMIT CHECK: check_sink_permissions={check_sink_permissions}, source_id={source_id}, sink_component={sink_component}")

            permitted = await self.permit.check(
                user=user_to_check,
                action=action,
                resource={"type": data_type, "id": sink_id},
                context={
                    "source_component": source_id,
                    "sink_component": sink_component,
                    "check_sink_permissions": check_sink_permissions
                }
            )

            logger.info(f"PERMIT RESULT: permitted={permitted}")

            if not permitted:
                if check_sink_permissions:
                    return AccessDecision(
                        allowed=False,
                        reason=f"Authorization denied: {sink_component} not allowed to {action} {sink_id}"
                    )
                return AccessDecision(
                    allowed=False,
                    reason=f"Authorization denied: {source_id} not allowed to {action} {sink_id}"
                )

            return AccessDecision(
                allowed=True,
                reason="Access granted"
            )

        except ComponentNotFoundError:
            return AccessDecision(
                allowed=False,
                reason=f"Component not found: {sink_component}. Register component first."
            )
        except Exception as e:
            # Fail-open for production safety
            if self.config.REQUIRE_REGISTRATION:
                return AccessDecision(
                    allowed=False,
                    reason=f"Authorization check failed: {e}"
                )
            return AccessDecision(
                allowed=True,
                reason="Authorization check failed, allowing through (fail-open mode)"
            )

    def _filter_data(self, source: str, sink: str, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply field-level filtering to data based on configured rules.

        Args:
            source: Source component ID
            sink: Sink component ID
            data: Data dictionary to filter

        Returns:
            Filtered data dictionary
        """
        key = (source, sink)

        # Check if we have a specific filter for this pair
        if key in self.field_filters:
            filter_config = self.field_filters[key]
            filtered = {}

            # Apply allowed fields whitelist
            if filter_config.allowed_fields:
                for field_name in filter_config.allowed_fields:
                    if field_name in data:
                        filtered[field_name] = data[field_name]
            else:
                filtered = data.copy()

            # Remove denied fields
            for field_name in filter_config.denied_fields:
                filtered.pop(field_name, None)

            return filtered

        # Check component config for allowed fields
        source_config = self.components.get(source)
        if source_config and sink in source_config.allowed_fields:
            allowed = source_config.allowed_fields[sink]
            return {k: v for k, v in data.items() if k in allowed}

        # No filtering configured, return data as-is
        return data

    async def apply_transformations(
        self,
        source_id: str,
        sink_id: str,
        data: dict[str, Any]
    ) -> tuple[dict[str, Any], list[str]]:
        """
        Apply all configured transformations to data.

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            data: Data to transform

        Returns:
            Tuple of (transformed_data, list_of_transformations_applied)
        """
        pipeline_id = f"{source_id}_to_{sink_id}"
        pipeline = self.transformer_pipelines.get(pipeline_id)

        if pipeline is None:
            # No transformations configured, just apply field filtering
            return self._filter_data(source_id, sink_id, data), []

        # Apply field filtering first, then transformations
        filtered = self._filter_data(source_id, sink_id, data)
        transformed = await pipeline.execute(filtered)

        # Get list of applied transformations
        transformations = [step.__class__.__name__ for step in pipeline.transformers]

        return transformed, transformations

    async def interact(
        self,
        source: str,
        sink: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> tuple[bool, dict[str, Any], str, list[str]]:
        """
        Main interaction method - authorize and filter data between components.

        Args:
            source: Source component ID
            sink: Sink component ID
            data: Data to transmit
            action: Action type for authorization check (read, write, etc.)

        Returns:
            Tuple of (allowed, filtered_data, message, transformations_applied)
        """
        # Step 1: Authorize the interaction using Permit.io
        decision = await self.check_access(source, sink, action=action)

        if not decision.allowed:
            return False, {}, decision.reason, []

        # Step 2: Apply field-level filtering and transformations
        filtered_data, transformations = await self.apply_transformations(source, sink, data)

        # Step 3: Return result
        return True, filtered_data, "Data transmitted successfully", transformations

    def interact_sync(
        self,
        source: str,
        sink: str,
        data: dict[str, Any],
        action: str = "read"
    ) -> tuple[bool, dict[str, Any], str, list[str]]:
        """
        Synchronous version of interact method.

        Args:
            source: Source component ID
            sink: Sink component ID
            data: Data to transmit
            action: Action type for authorization check

        Returns:
            Tuple of (allowed, filtered_data, message, transformations_applied)
        """
        return asyncio.run(self.interact(source, sink, data, action))

    async def evaluate_ml_model_access(
        self,
        component_id: str,
        model_id: str,
        model_data_type: str
    ) -> bool:
        """
        Check if a component can access an ML model.

        Args:
            component_id: Component requesting access
            model_id: ML model identifier
            model_data_type: Data type of the model (for role matching)

        Returns:
            True if access is permitted
        """
        try:
            # Get the role for this data type
            expected_role = self._ml_data_type_roles.get(
                model_data_type,
                "network_predictor_role"
            )

            # Check if component has the required role
            component = self.components.get(component_id)
            if not component:
                return not self.config.REQUIRE_REGISTRATION

            if component.role != expected_role:
                return False

            # Check Permit.io permissions
            permitted = await self.permit.check(
                user=component_id,
                action="inference",
                resource={"type": f"model:{model_data_type}", "id": model_id}
            )

            return permitted

        except Exception:
            # Fail-open for production safety
            return not self.config.REQUIRE_REGISTRATION

    def get_component(self, component_id: str) -> Optional[ComponentConfig]:
        """Get component configuration by ID."""
        return self.components.get(component_id)

    def list_components(self) -> list[ComponentConfig]:
        """List all registered components."""
        return list(self.components.values())

    def unregister_component(self, component_id: str) -> bool:
        """
        Unregister a component from the policy engine.

        Args:
            component_id: Component ID to unregister

        Returns:
            True if successful
        """
        if component_id in self.components:
            del self.components[component_id]
            return True
        return False

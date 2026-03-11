"""
ML model access control service.

Registers ML models as first-class policy-managed components (type ``ml_model``)
so they participate in the same pipeline / field-filtering system as Ingestion,
Processor and Storage.
"""
from __future__ import annotations

import logging
from typing import List, Optional

from src.core.policy import ComponentConfig, PolicyEngine
from src.models.enums import MLDataType

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────

ML_DEFAULT_ROLE = "ML"
ML_COMPONENT_PREFIX = "ml-"


def _model_component_id(model_name: str) -> str:
    """Derive the deterministic component ID for a model name."""
    return f"{ML_COMPONENT_PREFIX}{model_name}"


class MLModelService:
    """
    Service for managing ML model access control.

    Each model is registered through the **existing** component infrastructure
    with ``component_type = "ml_model"`` so it gets:
      - an in-memory registry entry
      - a Permit.io user (via ``sync_user``)
      - role assignment(s)
      - heartbeat / re-registration support
      - frontend visibility in ``GET /api/v1/components``
      - transformer pipeline / field-filtering support
    """

    def __init__(self, policy_engine: PolicyEngine):
        self.policy_engine = policy_engine

    # ── Registration ─────────────────────────────────────────────────────

    async def register_model(
        self,
        model_id: str,
        model_name: str,
        input_fields: List[str],
        output_fields: List[str],
        data_type: MLDataType | str,
        architecture: Optional[str] = None,
        permit_user_key: Optional[str] = None,
        additional_roles: Optional[List[str]] = None,
        window_duration_seconds: Optional[int] = None,
    ) -> str:
        """
        Register an ML model as a policy component.

        Args:
            model_id: Unique model identifier (e.g. UUID from MLflow).
            model_name: Human-readable model name (used to derive component ID).
            input_fields: Input field names the model reads during training.
            output_fields: Output field names the model produces.
            data_type: Data type for role assignment (MLDataType or string).
            architecture: Optional model architecture label (``ann``, ``lstm``, …).
            permit_user_key: Optional shared Permit.io user key.
            additional_roles: Optional list of roles beyond the default ``ML`` role.
            window_duration_seconds: Optional window duration in seconds.

        Returns:
            The component_id assigned to the model (``ml-{model_name}``).
        """
        component_id = _model_component_id(model_name)

        data_type_value = data_type.value if isinstance(data_type, MLDataType) else str(data_type)

        # Build rich attributes so the frontend and transformer service
        # can discover model metadata and fields.
        attributes: dict = {
            "model_id": model_id,
            "model_name": model_name,
            "data_type": data_type_value,
            "input_fields": input_fields,
            "output_fields": output_fields,
            # data_columns is what TransformerService.get_component_fields reads
            "data_columns": list(input_fields),
        }
        if architecture is not None:
            attributes["architecture"] = architecture
        if window_duration_seconds is not None:
            attributes["window_duration_seconds"] = window_duration_seconds

        config = ComponentConfig(
            component_id=component_id,
            component_type="ml_model",
            role=ML_DEFAULT_ROLE,
            additional_roles=additional_roles or [],
            permit_user_key=permit_user_key,
            allowed_fields={"default": output_fields},
            attributes=attributes,
        )

        success = await self.policy_engine.register_component(config)
        if not success:
            raise RuntimeError(f"Failed to register ML model component: {component_id}")

        logger.info(
            "Registered ML model %s as component %s (roles=%s, permit_user=%s)",
            model_name,
            component_id,
            config.all_roles,
            config.effective_user_key,
        )
        return component_id

    # ── Listing ──────────────────────────────────────────────────────────

    def list_ml_models(self) -> List[ComponentConfig]:
        """Return only components whose ``component_type`` is ``ml_model``."""
        return [
            c
            for c in self.policy_engine.list_components()
            if c.component_type == "ml_model"
        ]

    # ── Unregistration ───────────────────────────────────────────────────

    def unregister_model(self, model_name: str) -> bool:
        """
        Remove an ML model component from the registry.

        Args:
            model_name: The model name (not the component_id).

        Returns:
            True if the model was found and removed, False otherwise.
        """
        component_id = _model_component_id(model_name)
        removed = self.policy_engine.unregister_component(component_id)
        if removed:
            logger.info("Unregistered ML model component %s", component_id)
        return removed

    # ── Access checking (unchanged logic) ────────────────────────────────

    async def check_model_access(
        self,
        component_id: str,
        model_id: str,
        model_data_type: MLDataType | str,
    ) -> tuple[bool, str]:
        """
        Check if a component can access an ML model.

        Args:
            component_id: Component requesting access.
            model_id: ML model identifier.
            model_data_type: Data type of the model.

        Returns:
            Tuple of (allowed, reason).
        """
        data_type_value = (
            model_data_type.value
            if isinstance(model_data_type, MLDataType)
            else str(model_data_type)
        )
        allowed = await self.policy_engine.evaluate_ml_model_access(
            component_id=component_id,
            model_id=model_id,
            model_data_type=data_type_value,
        )

        if allowed:
            return True, f"Access granted: {component_id} can use model {model_id}"
        return False, f"Access denied: {component_id} not authorized for model {model_id}"

    # ── Data-type inference (unchanged logic) ────────────────────────────

    @staticmethod
    def infer_data_type(output_fields: List[str]) -> MLDataType:
        """
        Infer ML model data type from output fields.

        Args:
            output_fields: List of output field names.

        Returns:
            Inferred ``MLDataType``.
        """
        output_set = {f.lower() for f in output_fields}

        if any(term in output_set for term in ("fraud", "risk", "suspicious")):
            return MLDataType.FRAUD_DETECTION
        if any(term in output_set for term in ("anomaly", "outlier", "deviation")):
            return MLDataType.ANOMALY_DETECTION
        if any(term in output_set for term in ("latency", "delay", "rtt")):
            return MLDataType.LATENCY_FORECASTING
        if any(term in output_set for term in ("capacity", "utilization", "load")):
            return MLDataType.CAPACITY_PLANNING
        if any(term in output_set for term in ("churn", "retention", "attrition")):
            return MLDataType.CHURN_PREDICTION

        return MLDataType.NETWORK_PREDICTION
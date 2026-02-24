"""
ML model access control service.
"""
from typing import List, Optional
from src.core.policy import PolicyEngine
from src.models.enums import MLDataType


class MLModelService:
    """
    Service for managing ML model access control.
    """

    def __init__(self, policy_engine: PolicyEngine):
        """
        Initialize the ML model service.

        Args:
            policy_engine: Policy engine instance
        """
        self.policy_engine = policy_engine

    async def register_model(
        self,
        model_id: str,
        model_name: str,
        input_fields: List[str],
        output_fields: List[str],
        data_type: MLDataType
    ) -> bool:
        """
        Register an ML model with the policy service.

        Args:
            model_id: Unique model identifier
            model_name: Human-readable model name
            input_fields: Input field names
            output_fields: Output field names
            data_type: Data type for role assignment

        Returns:
            True if successful
        """
        return self.policy_engine.register_ml_agent(
            agent_id=model_id,
            model_name=model_name,
            allowed_outputs=output_fields,
            data_type=data_type.value
        )

    async def check_model_access(
        self,
        component_id: str,
        model_id: str,
        model_data_type: MLDataType
    ) -> tuple[bool, str]:
        """
        Check if a component can access an ML model.

        Args:
            component_id: Component requesting access
            model_id: ML model identifier
            model_data_type: Data type of the model

        Returns:
            Tuple of (allowed, reason)
        """
        allowed = await self.policy_engine.evaluate_ml_model_access(
            component_id=component_id,
            model_id=model_id,
            model_data_type=model_data_type.value
        )

        if allowed:
            return True, f"Access granted: {component_id} can use model {model_id}"
        else:
            return False, f"Access denied: {component_id} not authorized for model {model_id}"

    def infer_data_type(self, output_fields: List[str]) -> MLDataType:
        """
        Infer ML model data type from output fields.

        Args:
            output_fields: List of output field names

        Returns:
            Inferred MLDataType
        """
        output_set = {f.lower() for f in output_fields}

        # Check for fraud detection indicators
        if any(term in output_set for term in ["fraud", "risk", "suspicious"]):
            return MLDataType.FRAUD_DETECTION

        # Check for anomaly detection indicators
        if any(term in output_set for term in ["anomaly", "outlier", "deviation"]):
            return MLDataType.ANOMALY_DETECTION

        # Check for latency forecasting indicators
        if any(term in output_set for term in ["latency", "delay", "rtt"]):
            return MLDataType.LATENCY_FORECASTING

        # Check for capacity planning indicators
        if any(term in output_set for term in ["capacity", "utilization", "load"]):
            return MLDataType.CAPACITY_PLANNING

        # Check for churn prediction indicators
        if any(term in output_set for term in ["churn", "retention", "attrition"]):
            return MLDataType.CHURN_PREDICTION

        # Default to network prediction
        return MLDataType.NETWORK_PREDICTION

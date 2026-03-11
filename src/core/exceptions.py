"""
Custom exceptions for the Policy Service.
"""
from typing import Optional, Any


class PolicyError(Exception):
    """Base exception for all policy-related errors."""

    def __init__(self, message: str, details: Optional[dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class PermitConnectionError(PolicyError):
    """Raised when connection to Permit.io PDP fails."""

    pass


class AuthorizationError(PolicyError):
    """Raised when authorization check fails."""

    def __init__(
        self,
        message: str,
        source: str,
        sink: str,
        action: str,
        details: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.source = source
        self.sink = sink
        self.action = action


class TransformationError(PolicyError):
    """Raised when data transformation fails."""

    def __init__(
        self,
        message: str,
        transformer: str,
        data: Optional[dict[str, Any]] = None,
        details: Optional[dict[str, Any]] = None,
    ):
        super().__init__(message, details)
        self.transformer = transformer
        self.data = data


class ComponentNotFoundError(PolicyError):
    """Raised when a component is not found in the registry."""

    def __init__(self, component_id: str):
        super().__init__(f"Component not found: {component_id}")
        self.component_id = component_id


class ComponentRegistrationError(PolicyError):
    """Raised when component registration fails."""

    pass


class TransformerNotFoundError(PolicyError):
    """Raised when a transformer pipeline is not found."""

    def __init__(self, pipeline_id: str):
        super().__init__(f"Transformer pipeline not found: {pipeline_id}")
        self.pipeline_id = pipeline_id


class InvalidConfigurationError(PolicyError):
    """Raised when configuration is invalid."""

    pass


class ModelRegistrationError(PolicyError):
    """Raised when ML model registration fails."""

    def __init__(self, model_id: str, details: Optional[dict[str, Any]] = None):
        super().__init__(f"Failed to register model: {model_id}", details)
        self.model_id = model_id

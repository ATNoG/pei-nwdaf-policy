"""Policy Client SDK - for components to interact with the Policy Service."""

from .client import PolicyClient, ProcessResult
from .kafka_interceptor import (
    PyKafBridgePolicyInterceptor,
    create_pykafbridge_policy_consumer,
    bind_policy_to_topic
)
from .middleware import PolicyMiddleware

__all__ = [
    "PolicyClient",
    "ProcessResult",
    "PyKafBridgePolicyInterceptor",
    "create_pykafbridge_policy_consumer",
    "bind_policy_to_topic",
    "PolicyMiddleware",
]
__version__ = "1.0.0"

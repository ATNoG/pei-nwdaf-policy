"""
Policy service configuration using pydantic settings.
"""
from pydantic_settings import BaseSettings
from typing import Optional


class PolicyConfig(BaseSettings):
    """Configuration for the Policy Service."""

    # Permit.io Configuration
    PERMIT_API_KEY: str
    PERMIT_PDP_URL: str = "http://localhost:7766"
    PDP_TIMEOUT: int = 5
    API_TIMEOUT: int = 5

    # Auto-create attributes from data columns
    AUTO_CREATE_ATTRIBUTES: bool = True
    ATTRIBUTE_PREFIX: str = "attr_"

    # Component registration
    REQUIRE_REGISTRATION: bool = False  # Allow policyless mode
    REGISTRATION_TIMEOUT: int = 30  # seconds

    # Decision caching
    DECISION_CACHE_TTL: int = 60  # seconds
    DECISION_CACHE_SIZE: int = 1000

    # Transformer configuration
    TRANSFORMER_CONFIG_PATH: str = "./configs/transformers.json"

    # API Configuration
    API_HOST: str = "0.0.0.0"
    API_PORT: int = 8000
    API_PREFIX: str = "/api/v1"

    # CORS
    CORS_ORIGINS: list[str] = ["*"]
    CORS_ALLOW_CREDENTIALS: bool = True
    CORS_ALLOW_METHODS: list[str] = ["*"]
    CORS_ALLOW_HEADERS: list[str] = ["*"]

    # Logging
    LOG_LEVEL: str = "INFO"

    # Feature flags
    ENABLE_METRICS: bool = True
    ENABLE_HEALTH_CHECK: bool = True

    class Config:
        env_file = ".env"
        case_sensitive = True


class ClientConfig(BaseSettings):
    """Configuration for the Policy Client SDK."""

    # Policy service URL
    POLICY_SERVICE_URL: str = "http://localhost:8000"
    COMPONENT_ID: str

    # Client behavior
    ENABLE_POLICY: bool = False  # Policyless mode by default
    CACHE_TTL: int = 60  # seconds
    CACHE_SIZE: int = 1000
    TIMEOUT: int = 5  # seconds

    # Fail-open mode
    FAIL_OPEN: bool = True  # If policy check fails, allow data through

    class Config:
        env_file = ".env"
        case_sensitive = True

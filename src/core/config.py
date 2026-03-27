"""
Policy service configuration using pydantic settings.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class PolicyConfig(BaseSettings):
    """Configuration for the Policy Service."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"  # Ignore extra fields from .env file
    )

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

    # Component registration persistence
    COMPONENT_CONFIG_PATH: str = "./configs/components.json"

    # Permit.io synchronization (debounced sync and attribute caching)
    PERMIT_SYNC_INTERVAL_SECONDS: int = 30  # How often to sync to Permit.io (0 = immediate)
    PERMIT_ATTRIBUTE_CACHE_PATH: str = "./configs/permit_attributes.json"
    PERMIT_ENABLE_ATTRIBUTE_CACHING: bool = True
    PERMIT_PAGINATION_PER_PAGE: int = 100  # Pagination for list_users() API calls
    PERMIT_DEFAULT_TENANT_ID: str = "default"  # Default Permit.io tenant ID

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

    # ML Model configuration
    ML_DEFAULT_ROLE: str = "ML"  # Default role for ML models
    ML_COMPONENT_PREFIX: str = "ml-"  # Prefix for ML model component IDs


class ClientConfig(BaseSettings):
    """Configuration for the Policy Client SDK."""

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=True,
        extra="ignore"
    )

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

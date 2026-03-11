"""
Policy evaluation service - main business logic for policy enforcement.
"""
import logging
from typing import Dict, Any
from src.core.policy import PolicyEngine
from src.services.cache_service import CacheService
from src.core.config import PolicyConfig

logger = logging.getLogger(__name__)


class PolicyService:
    """
    Service for evaluating policy decisions and applying transformations.
    """

    def __init__(self, policy_engine: PolicyEngine, config: PolicyConfig):
        """
        Initialize the policy service.

        Args:
            policy_engine: Policy engine instance
            config: Policy configuration
        """
        self.policy_engine = policy_engine
        self.config = config
        self.cache = CacheService(
            maxsize=config.DECISION_CACHE_SIZE,
            ttl=config.DECISION_CACHE_TTL
        )

    async def check_access(
        self,
        source_id: str,
        sink_id: str,
        resource: str = "data",
        action: str = "read"
    ) -> tuple[bool, str]:
        """
        Check if access is permitted (with caching).

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            resource: Resource type
            action: Action to perform

        Returns:
            Tuple of (allowed, reason)
        """
        logger.info(f"Policy check: source_id={source_id}, sink_id={sink_id}, resource={resource}, action={action}")
        logger.info(f"Registered components: {list(self.policy_engine.components.keys())}")

        # Check cache first
        cache_key = (source_id, sink_id, resource, action)
        cached_result = self.cache.get(*cache_key)
        if cached_result is not None:
            logger.info(f"Cache HIT: {cached_result}")
            return cached_result

        # Evaluate policy
        decision = await self.policy_engine.check_access(
            source_id=source_id,
            sink_id=sink_id,
            data_type=resource,
            action=action
        )

        result = (decision.allowed, decision.reason)
        logger.info(f"Policy decision: allowed={decision.allowed}, reason={decision.reason}")
        self.cache.set(result, *cache_key)

        return result

    async def process_data(
        self,
        source_id: str,
        sink_id: str,
        data: Dict[str, Any],
        action: str = "read"
    ) -> tuple[bool, Dict[str, Any], str, list]:
        """
        Check policy and transform data in one call.

        Args:
            source_id: Source component ID
            sink_id: Sink component ID
            data: Data to process
            action: Action type

        Returns:
            Tuple of (allowed, transformed_data, reason, transformations_applied)
        """
        allowed, filtered_data, reason, transformations = await self.policy_engine.interact(
            source=source_id,
            sink=sink_id,
            data=data,
            action=action
        )

        return allowed, filtered_data, reason, transformations

    async def get_stats(self) -> dict:
        """
        Get policy enforcement statistics.

        Returns:
            Statistics dictionary
        """
        return {
            "total_components": len(self.policy_engine.components),
            "total_pipelines": len(self.policy_engine.transformer_pipelines),
            "total_filters": len(self.policy_engine.field_filters),
            "cache_size": len(self.cache)
        }

    def clear_cache(self) -> None:
        """Clear the decision cache."""
        self.cache.clear()

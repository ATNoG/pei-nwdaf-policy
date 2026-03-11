"""
Caching service for policy decisions.
"""
from cachetools import TTLCache
from typing import Any, Optional
import hashlib
import json


class CacheService:
    """
    Thread-safe caching service with TTL for policy decisions.
    """

    def __init__(self, maxsize: int = 1000, ttl: int = 60):
        """
        Initialize the cache service.

        Args:
            maxsize: Maximum number of entries
            ttl: Time-to-live in seconds
        """
        self.cache = TTLCache(maxsize=maxsize, ttl=ttl)

    def _generate_key(self, *args) -> str:
        """Generate a cache key from arguments."""
        key_data = json.dumps(args, sort_keys=True, default=str)
        return hashlib.sha256(key_data.encode()).hexdigest()

    def get(self, *args) -> Optional[Any]:
        """
        Get a value from cache.

        Args:
            *args: Arguments to generate cache key

        Returns:
            Cached value or None
        """
        key = self._generate_key(*args)
        return self.cache.get(key)

    def set(self, value: Any, *args) -> None:
        """
        Set a value in cache.

        Args:
            value: Value to cache
            *args: Arguments to generate cache key
        """
        key = self._generate_key(*args)
        self.cache[key] = value

    def clear(self) -> None:
        """Clear all cache entries."""
        self.cache.clear()

    def __len__(self) -> int:
        return len(self.cache)

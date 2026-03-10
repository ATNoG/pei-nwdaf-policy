"""
Hashing transformer - SHA-256 hashing of sensitive values.
"""
import hashlib
from typing import Any
from policy_client.transformers.base import BaseTransformer


class HashingTransformer(BaseTransformer):
    """
    Hash sensitive field values using SHA-256 with optional salt.
    """

    def __init__(self, fields: list[str], salt: str = ""):
        """
        Initialize the hashing transformer.

        Args:
            fields: List of field names to hash
            salt: Salt string to prepend before hashing (default: "")
        """
        self.fields = set(fields)
        self.salt = salt.encode() if salt else b""

    async def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply SHA-256 hashing to specified fields.

        Args:
            data: Input data dictionary

        Returns:
            Data with hashed values
        """
        result = data.copy()
        for field in self.fields:
            if field in result:
                value = str(result[field]).encode()
                hash_obj = hashlib.sha256(self.salt + value)
                result[field] = f"HASH:{hash_obj.hexdigest()}"
        return result

    def __repr__(self) -> str:
        has_salt = bool(self.salt)
        return f"HashingTransformer(fields={len(self.fields)}, salted={has_salt})"

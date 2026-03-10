"""
Redaction transformer - replaces sensitive values with placeholder.
"""
from typing import Any
from policy_client.transformers.base import BaseTransformer


class RedactionTransformer(BaseTransformer):
    """
    Replace sensitive field values with a placeholder string (e.g., ***).
    """

    def __init__(self, fields: list[str], replacement: str = "***"):
        """
        Initialize the redaction transformer.

        Args:
            fields: List of field names to redact
            replacement: String to replace values with (default: "***")
        """
        self.fields = set(fields)
        self.replacement = replacement

    async def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply redaction to specified fields.

        Args:
            data: Input data dictionary

        Returns:
            Data with redacted values
        """
        result = data.copy()
        for field in self.fields:
            if field in result:
                result[field] = self.replacement
        return result

    def __repr__(self) -> str:
        return f"RedactionTransformer(fields={len(self.fields)}, replacement='{self.replacement}')"

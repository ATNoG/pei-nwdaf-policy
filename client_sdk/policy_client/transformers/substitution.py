"""
Substitution transformer - replaces field values with constants.
"""
from typing import Any
from policy_client.transformers.base import BaseTransformer


class SubstitutionTransformer(BaseTransformer):
    """
    Replace field values with specified constants.
    """

    def __init__(self, substitutions: dict[str, Any]):
        """
        Initialize the substitution transformer.

        Args:
            substitutions: Dictionary mapping field names to replacement values
        """
        self.substitutions = substitutions

    def transform(self, data: dict[str, Any]) -> dict[str, Any]:
        """
        Apply value substitutions to data.

        Args:
            data: Input data dictionary

        Returns:
            Data with substituted values
        """
        result = data.copy()
        for field, replacement in self.substitutions.items():
            if field in result:
                result[field] = replacement
        return result

    def __repr__(self) -> str:
        return f"SubstitutionTransformer(substitutions={len(self.substitutions)})"

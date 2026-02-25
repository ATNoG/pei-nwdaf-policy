"""
Transformer pipeline management service.
"""
import json
import httpx
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any
from src.transformers.pipeline import TransformerPipeline
from src.core.policy import PolicyEngine
from src.core.config import PolicyConfig
from src.core.exceptions import TransformerNotFoundError, InvalidConfigurationError
from src.permit.permit_client import PermitClient


logger = logging.getLogger(__name__)

# Field categories for UI grouping
FIELD_CATEGORIES = {
    "location": [
        "latitude", "longitude", "altitude", "location_accuracy",
        "velocity", "velocity_accuracy", "bearing", "bearing_accuracy"
    ],
    "signal": [
        "rsrp", "rsrq", "rssi", "sinr", "ta", "cqi",
        "ss_rsrp", "ss_rsrq", "ss_sinr"
    ],
    "network": [
        "network", "mcc", "mnc", "earfcn", "cell_index",
        "physical_cellid", "tracking_area_code", "primary_bandwidth",
        "ul_bandwidth", "cellbandwidths"
    ],
    "latency": [
        "mean_latency", "min_latency", "max_latency", "mean_dev_latency"
    ],
    "other": [
        "packet_loss", "no_pings", "server_ip", "device", "MNO", "timestamp"
    ]
}


class TransformerService:
    """
    Service for managing transformer pipelines.
    """

    def __init__(self, policy_engine: PolicyEngine, config: PolicyConfig, permit_client: Optional[PermitClient] = None):
        """
        Initialize the transformer service.

        Args:
            policy_engine: Policy engine instance
            config: Policy configuration
            permit_client: Optional Permit.io client for attribute sync
        """
        self.policy_engine = policy_engine
        self.config = config
        self.config_path = Path(config.TRANSFORMER_CONFIG_PATH)
        self.permit_client = permit_client
        self.data_storage_url = "http://data-storage:8000"

        # Cache for discovered fields
        self._field_cache: Dict[str, List[dict]] = {}
        self._permit_client: Optional[PermitClient] = None

    @property
    def permit_client(self) -> Optional[PermitClient]:
        """Get permit client, initializing if needed."""
        if self._permit_client is None and self.permit_client:
            self._permit_client = self.permit_client
        return self._permit_client

    async def create_pipeline(
        self,
        pipeline_id: str,
        steps: list
    ) -> TransformerPipeline:
        """
        Create a transformer pipeline.

        Args:
            pipeline_id: Unique pipeline identifier (usually "source_to_sink")
            steps: List of transformer step configurations

        Returns:
            Created TransformerPipeline
        """
        pipeline_config = {"steps": steps}
        pipeline = TransformerPipeline.from_config(pipeline_config)

        self.policy_engine.set_transformer_pipeline(pipeline_id, pipeline)

        # Save to file if path is configured
        if self.config_path:
            await self._save_pipeline_to_file(pipeline_id, pipeline_config)

        return pipeline

    async def get_pipeline(self, pipeline_id: str) -> Optional[TransformerPipeline]:
        """
        Get a transformer pipeline.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            TransformerPipeline or None
        """
        return self.policy_engine.get_transformer_pipeline(pipeline_id)

    async def delete_pipeline(self, pipeline_id: str) -> bool:
        """
        Delete a transformer pipeline.

        Args:
            pipeline_id: Pipeline identifier

        Returns:
            True if deleted
        """
        if pipeline_id in self.policy_engine.transformer_pipelines:
            del self.policy_engine.transformer_pipelines[pipeline_id]
            return True
        return False

    async def _save_pipeline_to_file(self, pipeline_id: str, config: dict) -> None:
        """
        Save pipeline configuration to file.

        Args:
            pipeline_id: Pipeline identifier
            config: Pipeline configuration
        """
        try:
            self.config_path.parent.mkdir(parents=True, exist_ok=True)

            # Load existing config or create new
            if self.config_path.exists():
                with open(self.config_path, 'r') as f:
                    all_configs = json.load(f)
            else:
                all_configs = {"pipelines": {}}

            # Add/update pipeline
            all_configs["pipelines"][pipeline_id] = config

            # Save back
            with open(self.config_path, 'w') as f:
                json.dump(all_configs, f, indent=2)

        except Exception as e:
            raise InvalidConfigurationError(f"Failed to save pipeline config: {e}")

    async def load_pipelines_from_file(self) -> Dict[str, TransformerPipeline]:
        """
        Load all pipelines from configuration file.

        Returns:
            Dictionary of pipeline_id -> TransformerPipeline
        """
        if not self.config_path.exists():
            return {}

        try:
            with open(self.config_path, 'r') as f:
                all_configs = json.load(f)

            pipelines = {}
            for pipeline_id, config in all_configs.get("pipelines", {}).items():
                pipeline = TransformerPipeline.from_config(config)
                self.policy_engine.set_transformer_pipeline(pipeline_id, pipeline)
                pipelines[pipeline_id] = pipeline

            return pipelines

        except Exception as e:
            raise InvalidConfigurationError(f"Failed to load pipelines: {e}")

    async def list_pipelines(self) -> Dict[str, dict]:
        """
        List all transformer pipelines.

        Returns:
            Dictionary of pipeline_id -> pipeline config
        """
        return {
            pid: pipeline.to_config()
            for pid, pipeline in self.policy_engine.transformer_pipelines.items()
        }

    # ==================== Field Discovery Methods ====================

    async def discover_fields(self, source: str, sink: str) -> List[dict]:
        """
        Discover available fields for a pipeline by querying Data Storage.

        Args:
            source: Source component name
            sink: Sink component/resource name

        Returns:
            List of field info dicts: {name, type, category, description}
        """
        pipeline_key = f"{source}_to_{sink}"

        # Check cache first
        if pipeline_key in self._field_cache:
            logger.info(f"Returning cached fields for {pipeline_key}")
            return self._field_cache[pipeline_key]

        try:
            # Query Data Storage for schema
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.get(
                    f"{self.data_storage_url}/api/v1/processed/example"
                )
                response.raise_for_status()

                data = response.json()

                # Extract fields from the example data
                fields = self._extract_fields_from_data(data)

                # Cache the results
                self._field_cache[pipeline_key] = fields

                logger.info(f"Discovered {len(fields)} fields for {pipeline_key}")
                return fields

        except httpx.HTTPError as e:
            logger.error(f"Failed to discover fields from Data Storage: {e}")
            # Return default known fields if discovery fails
            return self._get_default_fields()

        except Exception as e:
            logger.error(f"Error during field discovery: {e}")
            return self._get_default_fields()

    def _extract_fields_from_data(self, data: dict) -> List[dict]:
        """
        Extract field information from example data.

        Args:
            data: Example data from Data Storage

        Returns:
            List of field info dicts
        """
        fields = []

        # Handle different data structures
        if isinstance(data, list) and len(data) > 0:
            sample_record = data[0]
        elif isinstance(data, dict):
            # Look for common keys that might contain data
            sample_record = data
        else:
            return self._get_default_fields()

        # Extract field names from the sample
        for field_name in sample_record.keys():
            category = self._get_field_category(field_name)
            field_type = self._infer_field_type(sample_record[field_name])

            fields.append({
                "name": field_name,
                "type": field_type,
                "category": category,
                "description": f"{category.capitalize()} field: {field_name}"
            })

        return fields

    def _get_field_category(self, field_name: str) -> str:
        """
        Get the category for a field name.

        Args:
            field_name: Field name

        Returns:
            Category name
        """
        field_name_lower = field_name.lower()

        for category, fields in FIELD_CATEGORIES.items():
            if any(field_name_lower == f.lower() or field_name_lower in f.lower() for f in fields):
                return category

        # Check for latency-related keywords
        if any(kw in field_name_lower for kw in ["latency", "delay", "rtt"]):
            return "latency"

        # Default category
        return "other"

    def _infer_field_type(self, value: Any) -> str:
        """
        Infer the type of a field from its value.

        Args:
            value: Field value

        Returns:
            Type name (string, number, boolean, etc.)
        """
        if value is None:
            return "null"
        elif isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "string"
        else:
            return "unknown"

    def _get_default_fields(self) -> List[dict]:
        """
        Get default field list when discovery fails.

        Returns:
            List of default field info dicts
        """
        fields = []
        for category, field_list in FIELD_CATEGORIES.items():
            for field_name in field_list:
                fields.append({
                    "name": field_name,
                    "type": "unknown",
                    "category": category,
                    "description": f"{category.capitalize()} field: {field_name}"
                })
        return fields

    async def list_discovered_fields(self) -> Dict[str, List[dict]]:
        """
        List all discovered fields across all cached pipelines.

        Returns:
            Dictionary with pipeline_id -> list of field info
        """
        return self._field_cache.copy()

    async def sync_field_attributes(self, source: str, sink: str) -> Dict[str, Any]:
        """
        Sync discovered fields to Permit.io as resource attributes.

        Creates resource attributes for each field, enabling field-level
        permission control through the UI.

        Args:
            source: Source component name
            sink: Sink component/resource name

        Returns:
            Sync result with status and created attributes
        """
        if not self.permit_client:
            return {
                "status": "error",
                "message": "Permit client not configured",
                "source": source,
                "sink": sink
            }

        # Discover fields first
        fields = await self.discover_fields(source, sink)

        resource_key = sink.replace("-", "_")  # Normalize resource name

        created_attributes = []
        failed_attributes = []

        for field in fields:
            attr_key = f"{self.config.ATTRIBUTE_PREFIX}{field['name']}"

            try:
                await self.permit_client.create_resource_attribute(
                    resource_key=resource_key,
                    attribute_key=attr_key,
                    attribute_type="string",
                    description=f"Field: {field['name']} ({field['category']})"
                )
                created_attributes.append(attr_key)
                logger.info(f"Created Permit.io attribute: {attr_key} on resource {resource_key}")

            except Exception as e:
                failed_attributes.append({
                    "attribute": attr_key,
                    "error": str(e)
                })
                logger.warning(f"Failed to create attribute {attr_key}: {e}")

        return {
            "status": "success" if not failed_attributes else "partial",
            "source": source,
            "sink": sink,
            "resource": resource_key,
            "total_fields": len(fields),
            "created_attributes": created_attributes,
            "failed_attributes": failed_attributes
        }

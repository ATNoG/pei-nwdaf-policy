"""
Transformer pipeline management service.
"""
import json
import logging
from pathlib import Path
from typing import Optional, Dict, List, Any
from src.transformers.pipeline import TransformerPipeline
from src.core.policy import PolicyEngine, ComponentConfig
from src.core.config import PolicyConfig
from src.core.exceptions import TransformerNotFoundError, InvalidConfigurationError
from src.permit.permit_client import PermitClient


logger = logging.getLogger(__name__)


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
        self._permit_client = permit_client

        # Cache for discovered fields
        self._field_cache: Dict[str, List[dict]] = {}

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

    async def get_component_fields(self, component_id: str) -> List[dict]:
        """
        Get all fields from a component's registration.

        Args:
            component_id: Component ID

        Returns:
            List of field info dicts with 'name' and 'type'
        """
        # Check cache first
        if component_id in self._field_cache:
            logger.info(f"Returning cached fields for {component_id}")
            return self._field_cache[component_id]

        # Get component from policy engine
        component_config: Optional[ComponentConfig] = self.policy_engine.get_component(component_id)
        if not component_config:
            logger.warning(f"Component {component_id} not found in registry")
            return []

        # Collect all fields from the component's registration
        field_list = []

        # 1. Get data_columns from attributes (set during registration)
        field_list.extend(component_config.attributes.get("data_columns", []))

        # 2. For ML models, include input_fields and output_fields
        if component_config.component_type == "ml_agent":
            input_fields = component_config.attributes.get("input_fields", [])
            output_fields = component_config.attributes.get("output_fields", [])
            field_list.extend(input_fields)
            field_list.extend(output_fields)

        # 3. Include all fields from allowed_fields (for any sink)
        for sink_fields in component_config.allowed_fields.values():
            field_list.extend(sink_fields)

        # Deduplicate while preserving order
        seen = set()
        unique_fields = []
        for f in field_list:
            if f not in seen:
                seen.add(f)
                unique_fields.append(f)

        # Build simple field info objects
        fields = [{"name": field_name, "type": "string"} for field_name in unique_fields]

        # Cache the results
        self._field_cache[component_id] = fields

        logger.info(f"Retrieved {len(fields)} fields for component {component_id}")
        return fields

    async def discover_fields(self, source: str, sink: str) -> List[dict]:
        """
        Discover available fields for a pipeline from registered component data.

        Args:
            source: Source component ID
            sink: Sink component ID (used to look up source's allowed_fields for this sink)

        Returns:
            List of field info dicts with 'name', 'type', and optionally 'category'
        """
        pipeline_key = f"{source}_to_{sink}"

        # Check cache first
        if pipeline_key in self._field_cache:
            logger.info(f"Returning cached fields for {pipeline_key}")
            return self._field_cache[pipeline_key]

        # Get source component from policy engine
        source_config: Optional[ComponentConfig] = self.policy_engine.get_component(source)
        if not source_config:
            logger.warning(f"Source component {source} not found in registry")
            return []

        # Collect fields from the component's registration
        fields_with_category = []

        # 1. Check for wildcard pattern {"*": ["*"]} - means any sink, all fields
        if "*" in source_config.allowed_fields:
            # Return the sink's data_columns as available fields
            # (kafka can produce whatever fields the sink accepts)
            sink_component_id = sink.split(":")[0] if ":" in sink else sink
            sink_config = self.policy_engine.get_component(sink_component_id)
            if sink_config and sink_config.attributes.get("data_columns"):
                for field_name in sink_config.attributes["data_columns"]:
                    fields_with_category.append({"name": field_name, "type": "string"})
        # 2. Check if component has fields in its allowed_fields for this specific sink
        elif sink in source_config.allowed_fields:
            # Extract category from sink key (e.g., "data-storage:influx" -> "influx")
            if ":" in sink:
                category = sink.split(":", 1)[1]
            else:
                category = sink
            for field_name in source_config.allowed_fields[sink]:
                fields_with_category.append({"name": field_name, "type": "string", "category": category})
        else:
            # 2. No categories - just return fields from data_columns without category
            for field_name in source_config.attributes.get("data_columns", []):
                fields_with_category.append({"name": field_name, "type": "string"})

        # 3. For ML models, add input_fields and output_fields with categories
        if source_config.component_type == "ml_agent":
            for field_name in source_config.attributes.get("input_fields", []):
                fields_with_category.append({"name": field_name, "type": "string", "category": "input"})
            for field_name in source_config.attributes.get("output_fields", []):
                fields_with_category.append({"name": field_name, "type": "string", "category": "output"})

        # Deduplicate while preserving order (keep first occurrence with category if present)
        seen_names = set()
        unique_fields = []
        for f in fields_with_category:
            if f["name"] not in seen_names:
                seen_names.add(f["name"])
                unique_fields.append(f)

        # Cache the results
        self._field_cache[pipeline_key] = unique_fields

        logger.info(f"Discovered {len(unique_fields)} fields for {pipeline_key} from component registry")
        return unique_fields

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
        if not self._permit_client:
            return {
                "status": "error",
                "message": "Permit client not configured",
                "source": source,
                "sink": sink
            }

        # Discover fields first
        fields = await self.discover_fields(source, sink)

        resource_key = sink.replace("-", "_")  # Normalize resource name

        # Create the resource first if it doesn't exist
        try:
            await self._permit_client.create_resource(
                resource_key=resource_key,
                description=f"Data resource: {sink}"
            )
            logger.info(f"Created Permit.io resource: {resource_key}")
        except Exception as e:
            # Resource might already exist, log and continue
            if "already exists" not in str(e).lower():
                logger.warning(f"Failed to create resource {resource_key}: {e}")

        created_attributes = []
        failed_attributes = []

        for field in fields:
            attr_key = f"{self.config.ATTRIBUTE_PREFIX}{field['name']}"

            try:
                await self._permit_client.create_resource_attribute(
                    resource_key=resource_key,
                    attribute_key=attr_key,
                    attribute_type="string",
                    description=f"Field: {field['name']}"
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

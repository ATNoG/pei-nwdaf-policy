"""
Transformer pipeline management service.
"""
import json
from pathlib import Path
from typing import Optional, Dict
from src.transformers.pipeline import TransformerPipeline
from src.core.policy import PolicyEngine
from src.core.config import PolicyConfig
from src.core.exceptions import TransformerNotFoundError, InvalidConfigurationError


class TransformerService:
    """
    Service for managing transformer pipelines.
    """

    def __init__(self, policy_engine: PolicyEngine, config: PolicyConfig):
        """
        Initialize the transformer service.

        Args:
            policy_engine: Policy engine instance
            config: Policy configuration
        """
        self.policy_engine = policy_engine
        self.config = config
        self.config_path = Path(config.TRANSFORMER_CONFIG_PATH)

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

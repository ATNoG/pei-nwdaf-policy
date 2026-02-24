"""
Transformer pipeline router - /api/v1/transformers/*
"""
from fastapi import APIRouter, Depends, HTTPException, status
from src.models.schemas import (
    TransformerPipelineConfig,
    TransformerPipelineResponse,
    TransformerStepConfig,
    ErrorResponse
)
from src.services.transformer_service import TransformerService
from typing import Dict


def get_transformer_service() -> TransformerService:
    """Dependency injection for transformer service."""
    raise NotImplementedError("Use Depends(get_transformer_service) in main.py")


router = APIRouter(prefix="/transformers", tags=["transformers"])


@router.post("/{pipeline_id}", response_model=TransformerPipelineResponse, status_code=status.HTTP_201_CREATED)
async def create_pipeline(
    pipeline_id: str,
    config: TransformerPipelineConfig,
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> TransformerPipelineResponse:
    """
    Create or update a transformer pipeline.

    Args:
        pipeline_id: Unique pipeline identifier
        config: Pipeline configuration
        transformer_service: Transformer service instance

    Returns:
        Created pipeline
    """
    try:
        # Convert steps to dicts
        steps = [step.dict() for step in config.steps]
        pipeline = await transformer_service.create_pipeline(pipeline_id, steps)

        return TransformerPipelineResponse(
            pipeline_id=pipeline_id,
            steps=config.steps,
            transformer_count=len(pipeline)
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "create_failed", "message": str(e)}
        )


@router.get("/{pipeline_id}", response_model=TransformerPipelineResponse)
async def get_pipeline(
    pipeline_id: str,
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> TransformerPipelineResponse:
    """
    Get a transformer pipeline by ID.

    Args:
        pipeline_id: Pipeline identifier
        transformer_service: Transformer service instance

    Returns:
        Pipeline configuration
    """
    pipeline = await transformer_service.get_pipeline(pipeline_id)

    if pipeline is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"}
        )

    config = pipeline.to_config()
    steps = [
        TransformerStepConfig(type=step["type"], params=step.get("params", {}))
        for step in config.get("steps", [])
    ]

    return TransformerPipelineResponse(
        pipeline_id=pipeline_id,
        steps=steps,
        transformer_count=len(pipeline)
    )


@router.get("", response_model=Dict[str, TransformerPipelineResponse])
async def list_pipelines(
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> Dict[str, TransformerPipelineResponse]:
    """
    List all transformer pipelines.

    Args:
        transformer_service: Transformer service instance

    Returns:
        Dictionary of pipeline_id -> pipeline config
    """
    pipelines = await transformer_service.list_pipelines()

    return {
        pid: TransformerPipelineResponse(
            pipeline_id=pid,
            steps=[
                TransformerStepConfig(type=step["type"], params=step.get("params", {}))
                for step in config.get("steps", [])
            ],
            transformer_count=len(config.get("steps", []))
        )
        for pid, config in pipelines.items()
    }


@router.delete("/{pipeline_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_pipeline(
    pipeline_id: str,
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> None:
    """
    Delete a transformer pipeline.

    Args:
        pipeline_id: Pipeline identifier
        transformer_service: Transformer service instance
    """
    success = await transformer_service.delete_pipeline(pipeline_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Pipeline not found: {pipeline_id}"}
        )

"""
Transformer pipeline router - /api/v1/transformers/*

IMPORTANT: Route order matters! More specific routes must be defined BEFORE
catch-all routes like /{pipeline_id}.
"""
from fastapi import APIRouter, Depends, HTTPException, status
from src.models.schemas import (
    TransformerPipelineConfig,
    TransformerPipelineResponse,
    TransformerStepConfig,
    ErrorResponse,
    FieldSyncRequest
)
from src.services.transformer_service import TransformerService
from typing import Dict, List, Any


def get_transformer_service() -> TransformerService:
    """Dependency injection for transformer service."""
    raise NotImplementedError("Use Depends(get_transformer_service) in main.py")


router = APIRouter(prefix="/transformers", tags=["transformers"])


# ==================== Field Discovery Endpoints ====================
# NOTE: These MUST come first before /{pipeline_id} to avoid route conflicts
# since /fields/{source}/{sink} would otherwise match /{pipeline_id} with pipeline_id="fields"

@router.get("/pipelines/{source_id}/{sink_id}", response_model=Dict[str, Any])
async def get_pipeline_config(
    source_id: str,
    sink_id: str,
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> Dict[str, Any]:
    """
    Get pipeline configuration for client-side processing.

    Returns the transformation steps that should be applied
    without actually processing any data. This allows clients
    to perform transformations locally.

    Args:
        source_id: Source component ID
        sink_id: Sink component ID
        transformer_service: Transformer service instance

    Returns:
        Pipeline configuration with pipeline_id and steps
    """
    pipeline_id = f"{source_id}_to_{sink_id}"
    pipeline = await transformer_service.get_pipeline(pipeline_id)

    if pipeline:
        config = pipeline.to_config()
        return {
            "pipeline_id": pipeline_id,
            "steps": config.get("steps", [])
        }
    return {
        "pipeline_id": pipeline_id,
        "steps": []
    }

@router.get("/fields/{source}/{sink}", response_model=List[dict])
async def discover_fields(
    source: str,
    sink: str,
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> List[dict]:
    """
    Discover available fields for a specific pipeline from registered component data.

    Args:
        source: Source component ID
        sink: Sink component ID
        transformer_service: Transformer service instance

    Returns:
        List of field info dictionaries with name and type
    """
    try:
        fields = await transformer_service.discover_fields(source, sink)
        return fields
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "discovery_failed", "message": str(e)}
        )


@router.post("/fields/sync", response_model=Dict[str, Any])
async def sync_field_attributes(
    request: FieldSyncRequest,
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> Dict[str, Any]:
    """
    Sync discovered fields to Permit.io as resource attributes.

    Creates Permit.io resource attributes for each discovered field,
    allowing field-level permission control.

    Args:
        request: Sync request with source and sink
        transformer_service: Transformer service instance

    Returns:
        Sync results with created attributes and status
    """
    try:
        result = await transformer_service.sync_field_attributes(request.source, request.sink)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "sync_failed", "message": str(e)}
        )


@router.get("/fields", response_model=Dict[str, List[dict]])
async def list_discovered_fields(
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> Dict[str, List[dict]]:
    """
    List all discovered fields across all pipelines.

    Returns:
        Dictionary with pipeline_id -> list of field info
    """
    return await transformer_service.list_discovered_fields()


@router.post("/fields/cache/clear", response_model=Dict[str, Any])
async def clear_field_cache(
    transformer_service: TransformerService = Depends(get_transformer_service)
) -> Dict[str, Any]:
    """
    Clear the field discovery cache.

    This forces a refresh of field discovery on the next request.
    Useful after components have dynamically discovered new fields.

    Returns:
        Status message indicating cache was cleared
    """
    cleared_count = len(transformer_service._field_cache)
    transformer_service._field_cache.clear()

    return {
        "status": "success",
        "message": f"Cleared {cleared_count} cached field discoveries",
        "cleared_count": cleared_count
    }


# ==================== Pipeline CRUD Endpoints ====================

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

"""
Policy enforcement router - /api/v1/policy/*
"""
from fastapi import APIRouter, Depends, HTTPException, status
from typing import cast
from src.models.schemas import (
    PolicyCheckRequest,
    PolicyCheckResponse,
    ProcessDataRequest,
    ProcessDataResponse,
    PolicyStatsResponse,
    ErrorResponse
)
from src.services.policy_service import PolicyService


def get_policy_service() -> PolicyService:
    """Dependency injection for policy service."""
    # This will be overridden by FastAPI Depends in main.py
    raise NotImplementedError("Use Depends(get_policy_service) in main.py")


router = APIRouter(prefix="/policy", tags=["policy"])


@router.post("/check", response_model=PolicyCheckResponse)
async def check_policy(
    request: PolicyCheckRequest,
    policy_service: PolicyService = Depends(get_policy_service)
) -> PolicyCheckResponse:
    """
    Check if access is permitted between components.

    Args:
        request: Policy check request
        policy_service: Policy service instance

    Returns:
        Policy check response
    """
    try:
        allowed, reason = await policy_service.check_access(
            source_id=request.source_id,
            sink_id=request.sink_id,
            resource=request.resource,
            action=request.action.value
        )

        return PolicyCheckResponse(allowed=allowed, reason=reason)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "check_failed", "message": str(e)}
        )


@router.post("/process", response_model=ProcessDataResponse)
async def process_data(
    request: ProcessDataRequest,
    policy_service: PolicyService = Depends(get_policy_service)
) -> ProcessDataResponse:
    """
    Check policy and transform data in one call.

    Args:
        request: Data processing request
        policy_service: Policy service instance

    Returns:
        Processed data response
    """
    try:
        allowed, data, reason, transformations = await policy_service.process_data(
            source_id=request.source_id,
            sink_id=request.sink_id,
            data=request.data,
            action=request.action.value
        )

        return ProcessDataResponse(
            allowed=allowed,
            data=data if allowed else None,
            reason=reason,
            transformations_applied=transformations
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "process_failed", "message": str(e)}
        )


@router.get("/stats", response_model=PolicyStatsResponse)
async def get_stats(
    policy_service: PolicyService = Depends(get_policy_service)
) -> PolicyStatsResponse:
    """
    Get policy enforcement statistics.

    Args:
        policy_service: Policy service instance

    Returns:
        Policy statistics
    """
    try:
        stats = await policy_service.get_stats()
        return PolicyStatsResponse(**stats)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "stats_failed", "message": str(e)}
        )


@router.post("/cache/clear")
async def clear_cache(
    policy_service: PolicyService = Depends(get_policy_service),
    transformer_service = None
) -> dict:
    """
    Clear the policy decision cache.

    Args:
        policy_service: Policy service instance

    Returns:
        Success message
    """
    policy_service.clear_cache()

    # Also clear transformer field cache
    from src.main import transformer_service
    transformer_service._field_cache.clear()

    return {"status": "success", "message": "Cache cleared"}

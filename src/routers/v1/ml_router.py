"""
ML model access control router - /api/v1/ml/*
"""
from fastapi import APIRouter, Depends, HTTPException, status
from src.models.schemas import (
    MLModelRegistrationRequest,
    MLModelAccessRequest,
    MLModelAccessResponse,
    ErrorResponse
)
from src.services.ml_model_service import MLModelService


def get_ml_model_service() -> MLModelService:
    """Dependency injection for ML model service."""
    raise NotImplementedError("Use Depends(get_ml_model_service) in main.py")


router = APIRouter(prefix="/ml", tags=["ml"])


@router.post("/models", status_code=status.HTTP_201_CREATED)
async def register_model(
    request: MLModelRegistrationRequest,
    ml_service: MLModelService = Depends(get_ml_model_service)
) -> dict:
    """
    Register an ML model with the policy service.

    Args:
        request: Model registration request
        ml_service: ML model service instance

    Returns:
        Success response
    """
    try:
        success = await ml_service.register_model(
            model_id=request.model_id,
            model_name=request.model_name,
            input_fields=request.input_fields,
            output_fields=request.output_fields,
            data_type=request.data_type
        )

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail={"error": "registration_failed", "message": "Failed to register model"}
            )

        return {
            "status": "success",
            "model_id": request.model_id,
            "message": "Model registered successfully"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "registration_failed", "message": str(e)}
        )


@router.post("/models/check-access", response_model=MLModelAccessResponse)
async def check_model_access(
    request: MLModelAccessRequest,
    ml_service: MLModelService = Depends(get_ml_model_service)
) -> MLModelAccessResponse:
    """
    Check if a component can access an ML model.

    Args:
        request: Model access request
        ml_service: ML model service instance

    Returns:
        Access check response
    """
    try:
        allowed, reason = await ml_service.check_model_access(
            component_id=request.component_id,
            model_id=request.model_id,
            model_data_type=request.model_data_type
        )

        return MLModelAccessResponse(allowed=allowed, reason=reason)

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "check_failed", "message": str(e)}
        )


@router.post("/models/infer-type")
async def infer_data_type(
    output_fields: list[str],
    ml_service: MLModelService = Depends(get_ml_model_service)
) -> dict:
    """
    Infer ML model data type from output fields.

    Args:
        output_fields: List of output field names
        ml_service: ML model service instance

    Returns:
        Inferred data type
    """
    try:
        data_type = ml_service.infer_data_type(output_fields)
        return {"data_type": data_type.value}
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "inference_failed", "message": str(e)}
        )

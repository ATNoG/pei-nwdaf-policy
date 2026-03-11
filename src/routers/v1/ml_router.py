"""
ML model access control router - /api/v1/ml/*
"""
from fastapi import APIRouter, Depends, HTTPException, status
from src.models.schemas import (
    MLModelRegistrationRequest,
    MLModelAccessRequest,
    MLModelAccessResponse,
    MLModelResponse,
    MLModelListResponse,
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

    The model is registered as a first-class policy component (type ``ml_model``)
    so it participates in the pipeline / field-filtering system and is visible
    in the frontend component list.

    Args:
        request: Model registration request
        ml_service: ML model service instance

    Returns:
        Success response with assigned component_id
    """
    try:
        component_id = await ml_service.register_model(
            model_id=request.model_id,
            model_name=request.model_name,
            input_fields=request.input_fields,
            output_fields=request.output_fields,
            data_type=request.data_type,
            architecture=request.architecture,
            permit_user_key=request.permit_user_key,
            additional_roles=request.additional_roles,
            window_duration_seconds=request.window_duration_seconds,
        )

        return {
            "status": "success",
            "model_id": request.model_id,
            "component_id": component_id,
            "message": "Model registered successfully"
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "registration_failed", "message": str(e)}
        )


@router.get("/models", response_model=MLModelListResponse)
async def list_models(
    ml_service: MLModelService = Depends(get_ml_model_service)
) -> MLModelListResponse:
    """
    List all registered ML model components.

    Returns only components whose ``component_type`` is ``ml_model``.
    """
    configs = ml_service.list_ml_models()

    models = []
    for cfg in configs:
        attrs = cfg.attributes
        models.append(
            MLModelResponse(
                component_id=cfg.component_id,
                model_id=attrs.get("model_id", cfg.component_id),
                model_name=attrs.get("model_name", cfg.component_id),
                architecture=attrs.get("architecture"),
                input_fields=attrs.get("input_fields", []),
                output_fields=attrs.get("output_fields", []),
                data_type=attrs.get("data_type", ""),
                roles=cfg.all_roles,
                permit_user_key=cfg.permit_user_key,
            )
        )

    return MLModelListResponse(models=models)


@router.delete("/models/{model_name}", status_code=status.HTTP_204_NO_CONTENT)
async def unregister_model(
    model_name: str,
    ml_service: MLModelService = Depends(get_ml_model_service)
) -> None:
    """
    Unregister an ML model by its model name.

    The component ID is derived as ``ml-{model_name}``.

    Args:
        model_name: Model name (not the component_id)
        ml_service: ML model service instance
    """
    removed = ml_service.unregister_model(model_name)
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"ML model not found: {model_name}"}
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
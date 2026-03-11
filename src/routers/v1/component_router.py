"""
Component management router - /api/v1/components/*
"""
from fastapi import APIRouter, Depends, HTTPException, status
from src.models.schemas import (
    ComponentRegistrationRequest,
    ComponentResponse,
    ComponentListResponse,
    FieldFilterRequest,
    FieldFilterResponse,
    ErrorResponse
)
from src.services.component_service import ComponentService
from typing import cast, Dict, Any


def get_component_service() -> ComponentService:
    """Dependency injection for component service."""
    raise NotImplementedError("Use Depends(get_component_service) in main.py")


router = APIRouter(prefix="/components", tags=["components"])


# ==================== Persistence / Restore Endpoints ====================
# NOTE: These MUST come before /{component_id} to avoid route conflicts

@router.post("/restore", response_model=Dict[str, Any])
async def restore_components_from_permit(
    component_service: ComponentService = Depends(get_component_service)
) -> Dict[str, Any]:
    """
    Restore previously registered components from Permit.io.

    This is useful when the local component file is missing or stale.
    It reads all Permit.io users that have a `component_type` attribute
    and re-creates their in-memory ComponentConfig + persists to file.

    Returns:
        Status and number of components restored
    """
    try:
        restored = await component_service.restore_from_permit()
        total = len(component_service.policy_engine.components)
        return {
            "status": "success",
            "restored_count": restored,
            "total_components": total,
            "message": f"Restored {restored} component(s) from Permit.io ({total} total in registry)"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "restore_failed", "message": str(e)}
        )


@router.post("/persist", response_model=Dict[str, Any])
async def persist_components(
    component_service: ComponentService = Depends(get_component_service)
) -> Dict[str, Any]:
    """
    Force-save all current in-memory components to the local JSON file.

    Normally this happens automatically on every registration, but this
    endpoint allows a manual trigger.

    Returns:
        Status and number of components saved
    """
    try:
        await component_service._save_components_to_file()
        total = len(component_service.policy_engine.components)
        return {
            "status": "success",
            "saved_count": total,
            "message": f"Persisted {total} component(s) to local file"
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "persist_failed", "message": str(e)}
        )


@router.post("", response_model=ComponentResponse, status_code=status.HTTP_201_CREATED)
async def register_component(
    request: ComponentRegistrationRequest,
    component_service: ComponentService = Depends(get_component_service)
) -> ComponentResponse:
    """
    Register a component with the policy service.

    Args:
        request: Component registration request
        component_service: Component service instance

    Returns:
        Registered component configuration
    """
    try:
        config = await component_service.register_component(
            component_id=request.component_id,
            component_type=request.component_type.value,
            role=request.role,
            data_columns=request.data_columns,
            auto_create_attributes=request.auto_create_attributes,
            allowed_fields=request.allowed_fields,
            attributes=request.attributes
        )

        return ComponentResponse(
            component_id=config.component_id,
            component_type=config.component_type,
            role=config.role,
            allowed_fields={k: list(v) for k, v in config.allowed_fields.items()},
            attributes=config.attributes
        )

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail={"error": "registration_failed", "message": str(e)}
        )


@router.get("/{component_id}", response_model=ComponentResponse)
async def get_component(
    component_id: str,
    component_service: ComponentService = Depends(get_component_service)
) -> ComponentResponse:
    """
    Get component configuration by ID.

    Args:
        component_id: Component identifier
        component_service: Component service instance

    Returns:
        Component configuration
    """
    config = await component_service.get_component(component_id)

    if config is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Component not found: {component_id}"}
        )

    return ComponentResponse(
        component_id=config.component_id,
        component_type=config.component_type,
        role=config.role,
        allowed_fields={k: list(v) for k, v in config.allowed_fields.items()},
        attributes=config.attributes
    )


@router.get("", response_model=ComponentListResponse)
async def list_components(
    component_service: ComponentService = Depends(get_component_service)
) -> ComponentListResponse:
    """
    List all registered components.

    Args:
        component_service: Component service instance

    Returns:
        List of components
    """
    configs = await component_service.list_components()

    components = [
        ComponentResponse(
            component_id=c.component_id,
            component_type=c.component_type,
            role=c.role,
            allowed_fields={k: list(v) for k, v in c.allowed_fields.items()},
            attributes=c.attributes
        )
        for c in configs
    ]

    return ComponentListResponse(components=components)


@router.delete("/{component_id}", status_code=status.HTTP_204_NO_CONTENT)
async def unregister_component(
    component_id: str,
    component_service: ComponentService = Depends(get_component_service)
) -> None:
    """
    Unregister a component.

    Args:
        component_id: Component identifier
        component_service: Component service instance
    """
    success = await component_service.unregister_component(component_id)

    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Component not found: {component_id}"}
        )


@router.post("/filters", response_model=FieldFilterResponse, status_code=status.HTTP_201_CREATED)
async def add_field_filter(
    request: FieldFilterRequest,
    component_service: ComponentService = Depends(get_component_service)
) -> FieldFilterResponse:
    """
    Add a field filter between components.

    Args:
        request: Field filter request
        component_service: Component service instance

    Returns:
        Created field filter
    """
    await component_service.add_field_filter(
        source=request.source,
        sink=request.sink,
        allowed_fields=request.allowed_fields,
        denied_fields=request.denied_fields
    )

    return FieldFilterResponse(
        source=request.source,
        sink=request.sink,
        allowed_fields=request.allowed_fields,
        denied_fields=request.denied_fields
    )

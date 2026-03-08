"""
Pydantic schemas for API request/response validation.
"""
from pydantic import BaseModel, Field
from typing import Any, Optional, Dict, List
from src.models.enums import ComponentType, ActionType, TransformerType, MLDataType


# ==================== Policy Check Schemas ====================

class PolicyCheckRequest(BaseModel):
    """Request to check if access is permitted."""
    source_id: str = Field(..., description="Source component ID")
    sink_id: str = Field(..., description="Sink/destination component ID")
    resource: str = Field(default="data", description="Resource type")
    action: ActionType = Field(default=ActionType.READ, description="Action to perform")


class PolicyCheckResponse(BaseModel):
    """Response from policy check."""
    allowed: bool = Field(..., description="Whether access is permitted")
    reason: str = Field(..., description="Reason for decision")


# ==================== Data Processing Schemas ====================

class ProcessDataRequest(BaseModel):
    """Request to check policy and transform data."""
    source_id: str = Field(..., description="Source component ID")
    sink_id: str = Field(..., description="Sink/destination component ID")
    data: Dict[str, Any] = Field(..., description="Data to process")
    action: ActionType = Field(default=ActionType.READ, description="Action type")


class ProcessDataResponse(BaseModel):
    """Response from data processing."""
    allowed: bool = Field(..., description="Whether access is permitted")
    data: Optional[Dict[str, Any]] = Field(None, description="Transformed data if allowed")
    reason: str = Field(..., description="Reason for decision")
    transformations_applied: List[str] = Field(default_factory=list, description="List of transformers applied")


# ==================== Component Schemas ====================

class ComponentRegistrationRequest(BaseModel):
    """Request to register a component."""
    component_id: str = Field(..., description="Unique component identifier")
    component_type: ComponentType = Field(..., description="Type of component")
    role: Optional[str] = Field(None, description="Role to assign")
    data_columns: Optional[List[str]] = Field(None, description="Data columns/field names for this component")
    auto_create_attributes: bool = Field(True, description="Auto-create attributes from columns")
    allowed_fields: Optional[Dict[str, List[str]]] = Field(None, description="Allowed fields per sink")
    attributes: Optional[Dict[str, Any]] = Field(None, description="Additional component attributes")


class ComponentResponse(BaseModel):
    """Component configuration response."""
    component_id: str
    component_type: str
    role: Optional[str]
    allowed_fields: Dict[str, List[str]]
    attributes: Dict[str, Any]


class ComponentListResponse(BaseModel):
    """List of components response."""
    components: List[ComponentResponse]


# ==================== ML Model Schemas ====================

class MLModelRegistrationRequest(BaseModel):
    """Request to register an ML model."""
    model_id: str = Field(..., description="Unique model identifier")
    model_name: str = Field(..., description="Human-readable model name")
    input_fields: List[str] = Field(..., description="Input field names")
    output_fields: List[str] = Field(..., description="Output field names")
    data_type: MLDataType = Field(..., description="Data type for role assignment")


class MLModelAccessRequest(BaseModel):
    """Request to check ML model access."""
    component_id: str = Field(..., description="Component requesting access")
    model_id: str = Field(..., description="ML model identifier")
    model_data_type: MLDataType = Field(..., description="Data type of the model")


class MLModelAccessResponse(BaseModel):
    """Response from ML model access check."""
    allowed: bool
    reason: str


# ==================== Transformer Schemas ====================

class TransformerStepConfig(BaseModel):
    """Configuration for a single transformer step."""
    type: TransformerType = Field(..., description="Type of transformer")
    params: Dict[str, Any] = Field(default_factory=dict, description="Transformer parameters")


class TransformerPipelineConfig(BaseModel):
    """Configuration for a transformer pipeline."""
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    steps: List[TransformerStepConfig] = Field(default_factory=list, description="Transformer steps")


class TransformerPipelineResponse(BaseModel):
    """Transformer pipeline response."""
    pipeline_id: str
    steps: List[TransformerStepConfig]
    transformer_count: int


# ==================== Field Filter Schemas ====================

class FieldFilterRequest(BaseModel):
    """Request to add a field filter."""
    source: str = Field(..., description="Source component ID")
    sink: str = Field(..., description="Sink component ID")
    allowed_fields: List[str] = Field(default_factory=list, description="Fields to allow (whitelist)")
    denied_fields: List[str] = Field(default_factory=list, description="Fields to block (blacklist)")


class FieldFilterResponse(BaseModel):
    """Field filter response."""
    source: str
    sink: str
    allowed_fields: List[str]
    denied_fields: List[str]


class FieldSyncRequest(BaseModel):
    """Request to sync field attributes to Permit.io."""
    source: str = Field(..., description="Source component name")
    sink: str = Field(..., description="Sink component/resource name")


# ==================== Stats/Health Schemas ====================

class PolicyStatsResponse(BaseModel):
    """Policy enforcement statistics."""
    total_components: int = Field(..., description="Number of registered components")
    total_pipelines: int = Field(..., description="Number of transformer pipelines")
    total_filters: int = Field(..., description="Number of field filters")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Service status")
    permit_connected: bool = Field(..., description="Permit.io connection status")


# ==================== Error Schemas ====================

class ErrorResponse(BaseModel):
    """Error response."""
    error: str = Field(..., description="Error type")
    message: str = Field(..., description="Error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")

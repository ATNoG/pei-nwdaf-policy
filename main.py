"""
Policy Service - FastAPI Application Entry Point

This is the main entry point for the Policy Enforcement Service.
It provides a REST API for policy checking, data transformation,
component registration, and ML model access control.
"""
import os
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from src.core.config import PolicyConfig
from src.permit.permit_client import PermitClient
from src.core.policy import PolicyEngine
from src.services.policy_service import PolicyService
from src.services.component_service import ComponentService
from src.services.transformer_service import TransformerService
from src.services.ml_model_service import MLModelService
from src.models.schemas import HealthResponse, ErrorResponse
from src.models.enums import ComponentType

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
config: PolicyConfig = None
permit_client: PermitClient = None
policy_engine: PolicyEngine = None
policy_service: PolicyService = None
component_service: ComponentService = None
transformer_service: TransformerService = None
ml_model_service: MLModelService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    global config, permit_client, policy_engine, policy_service
    global component_service, transformer_service, ml_model_service

    # Startup
    logger.info("Starting Policy Service...")

    # Load configuration
    config = PolicyConfig()
    logger.info(f"Configuration loaded: PDP URL={config.PERMIT_PDP_URL}")

    # Initialize Permit client
    try:
        permit_client = PermitClient(config)
        logger.info("Permit.io client initialized")
    except Exception as e:
        logger.error(f"Failed to initialize Permit.io client: {e}")
        raise

    # Preload existing attributes from Permit.io to avoid duplicate API calls
    if config.PERMIT_ENABLE_ATTRIBUTE_CACHING:
        try:
            await permit_client._preload_existing_attributes()
        except Exception as e:
            logger.warning(f"Failed to preload existing attributes: {e}")

    # Initialize policy engine
    policy_engine = PolicyEngine(config, permit_client)
    logger.info("Policy engine initialized")

    # Initialize services
    policy_service = PolicyService(policy_engine, config)
    component_service = ComponentService(policy_engine, permit_client, config)
    transformer_service = TransformerService(policy_engine, config, permit_client)
    ml_model_service = MLModelService(policy_engine)

    # Wire transformer service into component service so that the field
    # discovery cache is invalidated whenever a component re-registers
    # (prevents stale empty-field results from persisting).
    component_service.set_transformer_service(transformer_service)

    # Start debounced Permit.io sync task if configured
    if config.PERMIT_SYNC_INTERVAL_SECONDS > 0:
        await component_service.start_permit_sync()
        logger.info(
            f"Permit.io sync task started (interval={config.PERMIT_SYNC_INTERVAL_SECONDS}s)"
        )
    else:
        logger.info("Permit.io sync disabled (PERMIT_SYNC_INTERVAL_SECONDS=0), using immediate sync")

    # Load transformer pipelines from file
    try:
        await transformer_service.load_pipelines_from_file()
        logger.info("Transformer pipelines loaded")
    except Exception as e:
        logger.warning(f"Failed to load transformer pipelines: {e}")

    # Registry starts empty — only components that actively register themselves
    # (i.e. are actually running) will appear. No restore from Permit.io or JSON
    # so stale / test components from previous runs never pollute the list.
    logger.info("Component registry starting empty — live components will register on connect")

    # Register Kafka as infrastructure component
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    try:
        await component_service.register_component(
            component_id="kafka",
            component_type=ComponentType.INFRASTRUCTURE,
            role="Messaging",
            data_columns=[],  # No specific fields - wildcard
            allowed_fields={
                # Wildcard pattern - kafka can produce to ANY sink with any fields
                "*": ["*"]
            },
            auto_create_attributes=False
        )
        logger.info(f"Kafka registered as infrastructure component: {kafka_bootstrap}")
    except Exception as e:
        logger.warning(f"Failed to register Kafka component: {e}")

    logger.info("Policy Service started successfully")

    yield

    # Shutdown
    logger.info("Shutting down Policy Service...")

    # Stop the debounced Permit.io sync task
    if component_service:
        try:
            await component_service.stop_permit_sync()
            logger.info("Permit.io sync task stopped")
        except Exception as e:
            logger.warning(f"Error stopping Permit.io sync task: {e}")


# Create FastAPI application
app = FastAPI(
    title="Policy Enforcement Service",
    description="Policy enforcement and data transformation service for MLOps pipeline",
    version="1.0.0",
    lifespan=lifespan
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ==================== Exception Handlers ====================

@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "http_error", "message": exc.detail}
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors."""
    return JSONResponse(
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        content={
            "error": "validation_error",
            "message": "Invalid request data",
            "details": exc.errors()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle general exceptions."""
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "internal_error", "message": "An internal error occurred"}
    )


# ==================== Health & Root Endpoints ====================

@app.get("/", tags=["root"])
async def root():
    """Root endpoint - service info."""
    return {
        "service": "Policy Enforcement Service",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """
    Health check endpoint.

    Returns service status and Permit.io connection status.
    """
    permit_connected = False
    try:
        if permit_client:
            # Try to check a simple permission to verify connection
            await permit_client.check(
                user="health_check",
                action="read",
                resource={"type": "health", "id": "check"}
            )
            permit_connected = True
    except Exception as e:
        logger.warning(f"Permit.io health check failed: {e}")

    return HealthResponse(
        status="healthy" if permit_connected else "degraded",
        permit_connected=permit_connected
    )


# ==================== API Router Registration ====================

from src.routers.v1.policy_router import router as policy_router, get_policy_service
from src.routers.v1.component_router import router as component_router, get_component_service
from src.routers.v1.transformer_router import router as transformer_router, get_transformer_service
from src.routers.v1.ml_router import router as ml_router, get_ml_model_service


# Override dependency injection for services
def get_policy_service_override() -> PolicyService:
    return policy_service


def get_component_service_override() -> ComponentService:
    return component_service


def get_transformer_service_override() -> TransformerService:
    return transformer_service


def get_ml_model_service_override() -> MLModelService:
    return ml_model_service


# Include routers
app.include_router(policy_router, prefix="/api/v1")
app.include_router(component_router, prefix="/api/v1")
app.include_router(transformer_router, prefix="/api/v1")
app.include_router(ml_router, prefix="/api/v1")


# Set dependency overrides on the app (not on routers)
app.dependency_overrides[get_policy_service] = get_policy_service_override
app.dependency_overrides[get_component_service] = get_component_service_override
app.dependency_overrides[get_transformer_service] = get_transformer_service_override
app.dependency_overrides[get_ml_model_service] = get_ml_model_service_override


# ==================== Main Entry Point ====================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host=config.API_HOST if config else "0.0.0.0",
        port=config.API_PORT if config else 8000,
        reload=True,
        log_level="info"
    )

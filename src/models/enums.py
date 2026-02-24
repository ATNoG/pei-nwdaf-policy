"""
Enumerations for the Policy Service.
"""
from enum import Enum


class ComponentType(str, Enum):
    """Types of components in the system."""
    ML_AGENT = "ml_agent"
    DATA_SOURCE = "data_source"
    API = "api"
    STORAGE = "storage"
    ANALYTICS = "analytics"
    INGESTION = "ingestion"
    PROCESSOR = "processor"


class ActionType(str, Enum):
    """Actions that can be performed on resources."""
    READ = "read"
    WRITE = "write"
    DELETE = "delete"
    TRAIN = "train"
    INFERENCE = "inference"
    PREDICT = "predict"


class TransformerType(str, Enum):
    """Types of data transformers."""
    FILTER = "filter"
    REDACTION = "redaction"
    HASHING = "hashing"
    SUBSTITUTION = "substitution"


class MLDataType(str, Enum):
    """Data types for ML models (used for role assignment)."""
    NETWORK_PREDICTION = "network_prediction"
    FRAUD_DETECTION = "fraud_detection"
    ANOMALY_DETECTION = "anomaly_detection"
    LATENCY_FORECASTING = "latency_forecasting"
    CAPACITY_PLANNING = "capacity_planning"
    CHURN_PREDICTION = "churn_prediction"

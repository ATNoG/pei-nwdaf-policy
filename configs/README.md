# Policy Service Configuration

This directory contains configuration files for the Policy Enforcement Service.

## Files

### `permit_resources.yaml`
Defines all resources in Permit.io. This file is used to set up the resource structure in Permit.io, staying within the 25 resource limit of the free tier.

### `permit_roles.yaml`
Defines roles and their permissions. Roles are assigned to components based on their type and function.

### `transformers.json`
Defines data transformation pipelines between component pairs. Each pipeline specifies a series of transformations to apply to data flowing from source to sink.

## Usage

### Setting up Permit.io Resources

1. Log in to your Permit.io dashboard
2. Create resources as defined in `permit_resources.yaml`
3. Create roles as defined in `permit_roles.yaml`

### Configuring Transformers

Edit `transformers.json` to add or modify data transformation pipelines:

```json
{
  "pipelines": {
    "source_component_to_sink_component": {
      "steps": [
        {
          "type": "filter",
          "params": {
            "mode": "whitelist",
            "fields": ["field1", "field2"]
          }
        },
        {
          "type": "redaction",
          "params": {
            "fields": ["sensitive_field"],
            "replacement": "***"
          }
        }
      ]
    }
  }
}
```

## Transformer Types

### Filter
Filter fields by whitelist or blacklist.

```json
{
  "type": "filter",
  "params": {
    "mode": "whitelist",
    "fields": ["allowed_field1", "allowed_field2"]
  }
}
```

### Redaction
Replace field values with a placeholder.

```json
{
  "type": "redaction",
  "params": {
    "fields": ["sensitive_field"],
    "replacement": "***"
  }
}
```

### Hashing
Hash field values using SHA-256.

```json
{
  "type": "hashing",
  "params": {
    "fields": ["field_to_hash"],
    "salt": "optional-salt"
  }
}
```

### Substitution
Replace field values with specific constants.

```json
{
  "type": "substitution",
  "params": {
    "substitutions": {
      "field1": "REDACTED",
      "field2": "NULL"
    }
  }
}
```

## Resource Allocation

The current allocation stays within the 25 resource limit:

- **System (5)**: policy_config, component_registration, transformer_config, train action, inference action
- **Data (10)**: network_metrics, user_behavior, anomaly_scores, predictions, latency_data, throughput_data, cell_metrics, time_series, aggregated, raw
- **Models (5)**: network_predictor, fraud_detector, anomaly_detector, latency_forecaster, capacity_planner
- **Access (5)**: api_access, kafka_topic, database_query, read action, write action

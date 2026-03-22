# Policy Service
A stateless data governance enforcer upon interactions between different interfaces, supporting both control over the transaction of data entirely, as well as a granular per-field processing technique (which in the current context supports **hashing**, **type-default redaction** and omission from the delivered data, i.e. **filtering**).

## Technologies

<div align="center">
  <table>
    <tr>
      <td align="center">
        <a href="https://www.python.org/">
          <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" width="60" height="60" alt="Python">
        </a>
        <br>Python</br>
      </td>
      <td align="center">
        <a href="https://www.permit.io/">
          <img src="https://avatars.githubusercontent.com/u/71775833?s=200&v=4" width="60" height="60" alt="Permit.io">
        </a>
        <br>Permit.io</br>
      </td>
      <td align="center">
        <a href="https://fastapi.tiangolo.com/">
          <img src="https://www.jetbrains.com/guide/assets/fastapi-6837327b.svg" width="60" height="60" alt="FastAPI">
        </a>
        <br>FastAPI</br>
      </td>
    </tr>
  </table>
</div>

## How It Works

1. **Components register\*** with the Policy Service on startup, declaring their roles and data fields (optional, initially).
2. **Policy checks** are performed via Permit's PDP, before data flows between components
3. **Data transformation** is applied automatically: filtering, redacting, or hashing fields, according to posterior configuration

\* **Client SDK** enables seamless integration with minimal code changes. See [registration details](#component-registration)

## Component Registration

### Client SDK

The Policy Client SDK (`client_sdk/`) provides a simple interface for components to integrate with the Policy Service. It supports:

* **Zero shared state**: each component pair has independent policy context;
* **Policyless mode**: components run without enforcement when disabled;
* **Local caching**: reduces latency for repeated checks;
* **Data transformation**: automatic field filtering, redaction, hashing.

#### Installation (for development)

```bash
# Install as a package
cd client_sdk
pip install -e .
```

#### Installation (in a component's requirements)

```bash
# In pyproject.toml file, add this to the dependencies array under [project]
"policy-client-sdk @ git+https://github.com/ATNoG/pei-nwdaf-policy.git#subdirectory=client_sdk",

# In requirements.txt
policy-client-sdk @ git+https://github.com/ATNoG/pei-nwdaf-policy.git#subdirectory=client_sdk
```


#### Registration

When a component starts, it registers itself with the Policy Service:

```python
from policy_client import PolicyClient

policy_client = PolicyClient(
    service_url=POLICY_SERVICE_URL,
    component_id="some_id",
    enable_policy=ENABLE_POLICY
)

await policy_client.register_component(
    component_type="some_name",
    role="Some-Role",
    data_columns=fields_array, # Established fields that will be available for the Policy Service to handle and configure
)
```


## API

Base path: `/api/v1`

### Policy Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/policy/check` | Check if access is permitted |
| `POST` | `/policy/process` | Check policy and transform data |
| `GET` | `/policy/stats` | Get policy enforcement statistics |
| `POST` | `/policy/cache/clear` | Clear decision cache |

### Component Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/components` | Register a component |
| `GET` | `/components/{id}` | Get component details |
| `GET` | `/components` | List all registered components |
| `DELETE` | `/components/{id}` | Unregister a component |

### Transformer Endpoints

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/transformers/{id}` | Create/update transformer pipeline |
| `GET` | `/transformers/{id}` | Get transformer pipeline |
| `GET` | `/transformers` | List all transformer pipelines |


## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `PERMIT_API_KEY` | - | Permit.io API key (required) |
| `PERMIT_PDP_URL` | `http://permit:7000` | Permit.io PDP endpoint |
| `POLICY_ENABLED` | `false` | Enable policy enforcement |
| `DECISION_CACHE_TTL` | `60` | Policy decision cache TTL (seconds) |
| `LOG_LEVEL` | `INFO` | Logging level |


## Ports

| Service | Port | Description |
|---|---|---|
| Policy API | `8788` | REST API endpoint |
| Permit PDP | `7766` | Permit.io PDP (mapped from 7000) |

## Notes

- **Permit.io Setup**: Before running, configure resources and roles in the [Permit.io](https://app.permit.io/) dashboard or use the provided configs in `configs/`
- **Component IDs**: Must be unique and consistent across the system
- **Field Discovery**: Components should declare data fields during registration
- **Testing**: Always test with `POLICY_ENABLED=false` before enabling in production


# Google Analytics Ingestion Generic Lambda
## Important links

### Dimensions and Metrics Selection Tool:
https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/

### Dimensions and Metrics Docs:
https://developers.google.com/analytics/devguides/reporting/data/v1/api-schema

### Getting a certain app id (property) API metadata:
```
ga_client = APIClient(
    auth=f"Bearer {get_access_token(get_secret(auth_path, session))}",
    base_url=base_url,
    login_url=login_url,
)
meta = ga_client.get(f"{ga_app_id}/metadata", clean=True)
```

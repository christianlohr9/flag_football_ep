## The MLflow tracking-server image only (server-side deps, never added to this project's own
## pyproject.toml/uv.lock -- see src/flag_football_ep/model/mlflow_store.py's M3-05-09
## addendum docstring and docs/mlflow-container-platform.md).
##
## Pinned to the exact client `mlflow` version this project runs (mlflow==3.15.1, verified
## installed this session) so client/server protocol behavior matches -- bump both together
## later, not independently.
FROM python:3.12-slim
RUN pip install --no-cache-dir \
    mlflow==3.15.1 \
    psycopg2-binary==2.9.12 \
    boto3==1.43.90
EXPOSE 5000

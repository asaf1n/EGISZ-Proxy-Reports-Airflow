$ErrorActionPreference = "Stop"

Write-Host "Creating secrets from examples..."
if (-not (Test-Path k8s/airflow/airflow-metadata-secret.yaml)) {
    Copy-Item k8s/airflow/airflow-metadata-secret.example.yaml k8s/airflow/airflow-metadata-secret.yaml
}
if (-not (Test-Path k8s/postgres/dwh-credentials-secret.yaml)) {
    Copy-Item k8s/postgres/dwh-credentials-secret.example.yaml k8s/postgres/dwh-credentials-secret.yaml
}

Write-Host "Applying secrets and configs..."
kubectl apply -f k8s/airflow/airflow-metadata-secret.yaml
kubectl apply -f k8s/postgres/dwh-credentials-secret.yaml
kubectl apply -f k8s/airflow/airflow-connections-configmap.yaml

Write-Host "Initializing DB schema..."
kubectl apply -f k8s/postgres/airflow-metadata-db-init-job.yaml

Write-Host "Updating Airflow..."
helm upgrade --install airflow apache-airflow/airflow -f k8s/airflow/values.yaml --timeout 15m

Write-Host "Starting Metabase..."
kubectl apply -f k8s/metabase/metabase.yaml

Write-Host "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app.kubernetes.io/name=metabase --timeout=300s
kubectl wait --for=condition=ready pod -l component=webserver,release=airflow --timeout=300s

Write-Host "Setting up Metabase..."
kubectl exec deploy/metabase -- /bin/bash /app/setup-dashboards.sh
.venv\Scripts\python.exe scripts/apply_metabase_field_filters.py

Write-Host "Done!"

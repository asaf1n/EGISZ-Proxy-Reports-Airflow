$ErrorActionPreference = "Stop"
$ImageTag = Get-Date -Format "yyyyMMddHHmmss"
$AirflowImage = "egisz-airflow-worker:${ImageTag}"
$MetabaseImage = "egisz-monitor-metabase:${ImageTag}"

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

Write-Host "Building Airflow image with current DAG and egisz_elt package..."
docker build -t $AirflowImage -t egisz-airflow-worker:latest -f k8s/airflow/Dockerfile .

Write-Host "Building Metabase image with current dashboard provisioning scripts..."
docker build -t $MetabaseImage -t egisz-monitor-metabase:latest -f metabase/Dockerfile .

Write-Host "Updating Airflow..."
helm upgrade --install airflow apache-airflow/airflow -f k8s/airflow/values.yaml --timeout 15m --set-string images.airflow.tag=$ImageTag

Write-Host "Restarting Airflow pods to pick up the rebuilt image..."
kubectl rollout restart deployment/airflow-scheduler deployment/airflow-webserver
kubectl rollout restart statefulset/airflow-worker statefulset/airflow-triggerer

Write-Host "Starting Metabase..."
kubectl apply -f k8s/metabase/metabase.yaml
kubectl set image deployment/metabase metabase=$MetabaseImage

Write-Host "Waiting for pods to be ready..."
kubectl rollout status deployment/metabase --timeout=300s
kubectl rollout status deployment/airflow-webserver --timeout=300s
kubectl rollout status deployment/airflow-scheduler --timeout=300s
kubectl rollout status statefulset/airflow-worker --timeout=300s
kubectl rollout status statefulset/airflow-triggerer --timeout=300s

Write-Host "Setting up Metabase..."
kubectl exec deploy/metabase -- /bin/bash /app/setup-dashboards.sh
.venv\Scripts\python.exe scripts/apply_metabase_field_filters.py

Write-Host "Done!"

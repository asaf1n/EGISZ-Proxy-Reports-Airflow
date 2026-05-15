param(
    [ValidateSet("All", "Airflow", "Metabase")]
    [string]$Component = "All"
)

$ErrorActionPreference = "Stop"

$ImageTag = Get-Date -Format "yyyyMMddHHmmss"
$AirflowImage = "egisz-airflow-worker:${ImageTag}"
$MetabaseImage = "egisz-metabase:${ImageTag}"

function Invoke-Checked {
    param(
        [string]$Description,
        [scriptblock]$Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "${Description} failed with exit code ${LASTEXITCODE}"
    }
}

function Test-KubernetesConnection {
    Write-Host "Checking Kubernetes context..."
    $context = ""
    try {
        $context = kubectl config current-context 2>$null
    } catch {
        $context = ""
    }
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($context)) {
        throw "kubectl current-context is not set. Enable/select Docker Desktop Kubernetes before running up.ps1."
    }

    try {
        kubectl cluster-info --request-timeout=10s >$null
    } catch {
        throw "Kubernetes cluster '${context}' is not reachable. Check Docker Desktop Kubernetes or kubeconfig."
    }
    if ($LASTEXITCODE -ne 0) {
        throw "Kubernetes cluster '${context}' is not reachable. Check Docker Desktop Kubernetes or kubeconfig."
    }
}

function Initialize-SecretFiles {
    Write-Host "Preparing Kubernetes secrets from examples..."
    if (-not (Test-Path k8s/metabase/metabase-connections-secret.yaml)) {
        Copy-Item k8s/metabase/metabase-connections-secret.example.yaml k8s/metabase/metabase-connections-secret.yaml
    }
}

function Initialize-AirflowInternalMetadataDatabase {
    Write-Host "Ensuring Airflow internal metadata database airflow_db exists when PostgreSQL already has a PVC..."
    $postgresPod = kubectl get pod airflow-postgresql-0 --ignore-not-found -o name
    if ([string]::IsNullOrWhiteSpace($postgresPod)) {
        Write-Host "Airflow PostgreSQL pod is not present yet; Helm will initialize airflow_db on first startup."
        return
    }

    $schedulerPod = kubectl get pods -l component=scheduler,release=airflow --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}' 2>$null
    if ([string]::IsNullOrWhiteSpace($schedulerPod)) {
        Write-Host "Airflow scheduler pod is not running yet; Helm will initialize airflow_db on first startup."
        return
    }

    Invoke-Checked "Ensure airflow_db in internal Airflow PostgreSQL" {
        @'
import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    host="airflow-postgresql.default",
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres",
)
conn.autocommit = True
with conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", ("airflow_db",))
    if cur.fetchone() is None:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("airflow_db")))
conn.close()
print("Airflow internal metadata database airflow_db is present")
'@ | kubectl exec -i pod/$schedulerPod -c scheduler -- python -
    }
}

function Install-Airflow {
    Initialize-SecretFiles
    Test-KubernetesConnection

    Write-Host "Applying Airflow connection secrets..."
    Invoke-Checked "Apply Airflow connection secrets" {
        kubectl apply -f k8s/airflow/airflow-connections-secret.yaml
    }

    Write-Host "Building Airflow image with current DAG and egisz_elt package..."
    Invoke-Checked "Build Airflow image" {
        docker build -t $AirflowImage -t egisz-airflow-worker:latest -f k8s/airflow/Dockerfile .
    }

    Initialize-AirflowInternalMetadataDatabase

    Write-Host "Installing Airflow..."
    Invoke-Checked "Install Airflow Helm release" {
        helm upgrade --install airflow apache-airflow/airflow -f k8s/airflow/values.yaml --timeout 15m --set-string images.airflow.tag=$ImageTag
    }

    Write-Host "Waiting for Airflow Redis broker..."
    Invoke-Checked "Wait for Airflow Redis" {
        kubectl rollout status statefulset/airflow-redis --timeout=300s
    }

    Write-Host "Waiting for Airflow pods to be ready..."
    Invoke-Checked "Wait for Airflow webserver" {
        kubectl rollout status deployment/airflow-webserver --timeout=300s
    }
    Invoke-Checked "Wait for Airflow scheduler" {
        kubectl rollout status deployment/airflow-scheduler --timeout=300s
    }
    Invoke-Checked "Wait for Airflow worker" {
        kubectl rollout status statefulset/airflow-worker --timeout=300s
    }
    Invoke-Checked "Wait for Airflow triggerer" {
        kubectl rollout status statefulset/airflow-triggerer --timeout=300s
    }

    Write-Host "Waiting for Celery worker to connect to the Redis broker..."
    Invoke-Checked "Wait for Celery worker broker connection" {
        @'
import subprocess
import time

deadline = time.time() + 300
marker = "ready."
while time.time() < deadline:
    result = subprocess.run(
        ["kubectl", "logs", "airflow-worker-0", "-c", "worker", "--tail=200"],
        check=True,
        capture_output=True,
        text=True,
    )
    if marker in result.stdout:
        print("Celery worker is connected to Redis and ready.")
        raise SystemExit(0)
    time.sleep(5)

raise SystemExit("Timed out waiting for Celery worker readiness marker in logs.")
'@ | py -
    }

    Write-Host "Airflow is ready. Run 'psql -U postgres -d dwh_egisz -v ON_ERROR_STOP=1 -f db/dwh_init.sql' to initialize the DWH, then unpause egisz_elt_dag."
}

function Install-Metabase {
    Initialize-SecretFiles
    Test-KubernetesConnection

    Write-Host "Applying Metabase connection secrets..."
    Invoke-Checked "Apply Metabase connection secrets" {
        kubectl apply -f k8s/metabase/metabase-connections-secret.yaml
    }

    Write-Host "Building Metabase image with current dashboard provisioning scripts..."
    Invoke-Checked "Build Metabase image" {
        docker build -t $MetabaseImage -t egisz-metabase:latest -f metabase/Dockerfile .
    }

    Write-Host "Starting Metabase PostgreSQL and Metabase..."
    Invoke-Checked "Apply Metabase deployment" {
        kubectl apply -f k8s/metabase/metabase.yaml
    }

    Write-Host "Waiting for Metabase PostgreSQL to be ready..."
    Invoke-Checked "Wait for Metabase PostgreSQL" {
        kubectl rollout status statefulset/metabase-postgres --timeout=120s
    }

    Invoke-Checked "Set Metabase image" {
        kubectl set image deployment/metabase metabase=$MetabaseImage
    }

    Write-Host "Waiting for Metabase pod to be ready..."
    Invoke-Checked "Wait for Metabase deployment" {
        kubectl rollout status deployment/metabase --timeout=300s
    }

    Write-Host "Provisioning Metabase dashboards..."
    Invoke-Checked "Provision Metabase dashboards" {
        kubectl exec deploy/metabase -- /bin/bash /app/setup-dashboards.sh
    }
}

if ($Component -in @("All", "Airflow")) {
    Install-Airflow
}

if ($Component -in @("All", "Metabase")) {
    Install-Metabase
}

Write-Host "Done. Airflow: http://localhost:8080, Metabase: http://localhost:3000"

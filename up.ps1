param(
    [ValidateSet("Start", "Stop", "Airflow", "Metabase", "Stop-Airflow", "Stop-Metabase")]
    [string]$Action = "Start"
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

    # Local override: native-команды (docker buildx, kubectl) пишут прогресс/info
    # в stderr. Глобальный $ErrorActionPreference='Stop' трактует это как
    # terminating error ДО проверки exit-кода — даже если команда отработала
    # успешно. Переходим на Continue в пределах функции и проверяем $LASTEXITCODE
    # сами; это и есть смысл этой обёртки.
    $ErrorActionPreference = 'Continue'
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

function Initialize-EgiszEltNamespace {
    Write-Host "Ensuring namespace egisz-elt exists..."
    $existing = kubectl get namespace egisz-elt --ignore-not-found -o name
    if ([string]::IsNullOrWhiteSpace($existing)) {
        Invoke-Checked "Create namespace egisz-elt" {
            kubectl create namespace egisz-elt
        }
    } else {
        Write-Host "Namespace egisz-elt already exists, skipping."
    }
}

function Install-Airflow {
    Initialize-SecretFiles
    Test-KubernetesConnection
    Initialize-EgiszEltNamespace

    Write-Host "Applying Airflow connection secrets..."
    Invoke-Checked "Apply Airflow connection secrets" {
        kubectl apply -f k8s/airflow/airflow-connections-secret.yaml
    }

    Write-Host "Building Airflow image with current DAG and egisz_elt package..."
    Invoke-Checked "Build Airflow image" {
        # docker buildx прогресс летит в stderr; под $ErrorActionPreference='Stop'
        # это валит скрипт ещё до проверки exit-кода. Сливаем потоки и пускаем
        # через ForEach-Object, чтобы каждая строка стала обычным stdout.
        docker build -t $AirflowImage -t egisz-airflow-worker:latest -f k8s/airflow/Dockerfile . 2>&1 | ForEach-Object { "$_" }
    }

    Initialize-AirflowInternalMetadataDatabase

    Write-Host "Installing Airflow..."
    Invoke-Checked "Install Airflow Helm release" {
        helm upgrade --install airflow apache-airflow/airflow -f k8s/airflow/values.yaml --timeout 15m --set-string images.airflow.tag=$ImageTag
    }

    Write-Host "Restoring Airflow replicas after any previous scale-to-zero stop..."
    Invoke-Checked "Scale Airflow PostgreSQL to 1" {
        kubectl scale statefulset/airflow-postgresql --replicas=1
    }
    Invoke-Checked "Scale Airflow Redis to 1" {
        kubectl scale statefulset/airflow-redis --replicas=1
    }
    Invoke-Checked "Scale Airflow webserver to 1" {
        kubectl scale deployment/airflow-webserver --replicas=1
    }
    Invoke-Checked "Scale Airflow scheduler to 1" {
        kubectl scale deployment/airflow-scheduler --replicas=1
    }
    Invoke-Checked "Scale Airflow worker to 1" {
        kubectl scale statefulset/airflow-worker --replicas=1
    }
    Invoke-Checked "Scale Airflow triggerer to 1" {
        kubectl scale statefulset/airflow-triggerer --replicas=1
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
        # docker buildx прогресс летит в stderr; под $ErrorActionPreference='Stop'
        # это валит скрипт ещё до проверки exit-кода. Сливаем потоки и пускаем
        # через ForEach-Object, чтобы каждая строка стала обычным stdout.
        docker build -t $MetabaseImage -t egisz-metabase:latest -f metabase/Dockerfile . 2>&1 | ForEach-Object { "$_" }
    }

    Write-Host "Starting Metabase PostgreSQL and Metabase..."
    Invoke-Checked "Apply Metabase deployment" {
        kubectl apply -f k8s/metabase/metabase.yaml
    }

    Write-Host "Restoring Metabase replicas after any previous scale-to-zero stop..."
    Invoke-Checked "Scale Metabase PostgreSQL to 1" {
        kubectl scale statefulset/metabase-postgres --replicas=1
    }
    Invoke-Checked "Scale Metabase deployment to 1" {
        kubectl scale deployment/metabase --replicas=1
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

function Stop-Airflow {
    Test-KubernetesConnection

    Write-Host "Scaling down Airflow components without deleting releases or PVCs..."
    Invoke-Checked "Scale Airflow webserver to 0" {
        kubectl scale deployment/airflow-webserver --replicas=0
    }
    Invoke-Checked "Scale Airflow scheduler to 0" {
        kubectl scale deployment/airflow-scheduler --replicas=0
    }
    Invoke-Checked "Scale Airflow worker to 0" {
        kubectl scale statefulset/airflow-worker --replicas=0
    }
    Invoke-Checked "Scale Airflow triggerer to 0" {
        kubectl scale statefulset/airflow-triggerer --replicas=0
    }
    Invoke-Checked "Scale Airflow Redis to 0" {
        kubectl scale statefulset/airflow-redis --replicas=0
    }
    Invoke-Checked "Scale Airflow PostgreSQL to 0" {
        kubectl scale statefulset/airflow-postgresql --replicas=0
    }
}

function Stop-Metabase {
    Test-KubernetesConnection

    Write-Host "Scaling down Metabase components without deleting the metabase_app database PVC..."
    Invoke-Checked "Scale Metabase deployment to 0" {
        kubectl scale deployment/metabase --replicas=0
    }
    Invoke-Checked "Scale Metabase PostgreSQL to 0" {
        kubectl scale statefulset/metabase-postgres --replicas=0
    }
}

if ($Action -in @("Start", "Airflow")) {
    Install-Airflow
}

if ($Action -in @("Start", "Metabase")) {
    Install-Metabase
}

if ($Action -in @("Stop", "Stop-Airflow")) {
    Stop-Airflow
}

if ($Action -in @("Stop", "Stop-Metabase")) {
    Stop-Metabase
}

if ($Action -eq "Start") {
    Write-Host "Done. Airflow: http://localhost:8080, Metabase: http://localhost:3000"
} elseif ($Action -in @("Airflow", "Metabase")) {
    Write-Host "Done. Selected Kubernetes components are running."
} else {
    Write-Host "Done. Selected Kubernetes components were scaled down to zero; PVC-backed data was preserved."
}

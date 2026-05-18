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

function Get-KubernetesSecretDecodedValue {
    param(
        [string]$SecretName,
        [string]$Key
    )

    $encoded = kubectl get secret $SecretName -o "jsonpath={.data.$Key}"
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($encoded)) {
        throw "Cannot read key '$Key' from Kubernetes secret '$SecretName'."
    }

    return [System.Text.Encoding]::UTF8.GetString([System.Convert]::FromBase64String($encoded))
}

function Get-DwhMissingMetabaseObjects {
    param(
        [string]$DbHost,
        [string]$DbPort,
        [string]$DbName,
        [string]$DbUser,
        [string]$Password
    )

    $requiredObjects = @(
        @{ Name = "v_doc_registry_ui"; Kind = "v" },
        @{ Name = "v_doc_timeline_ui"; Kind = "v" },
        @{ Name = "v_stat_semd_types_ui"; Kind = "v" },
        @{ Name = "v_stat_errors_ui"; Kind = "v" },
        @{ Name = "v_stat_orgs_ui"; Kind = "v" },
        @{ Name = "v_stat_daily_ui"; Kind = "v" },
        @{ Name = "v_stat_hourly_ui"; Kind = "v" },
        @{ Name = "v_docs_no_response_ui"; Kind = "m" },
        @{ Name = "v_service_health_ui"; Kind = "v" },
        @{ Name = "v_kpi_summary_ui"; Kind = "v" },
        @{ Name = "v_egisz_transactions_enriched_ui"; Kind = "m" },
        @{ Name = "v_stg_channel_errors_by_document"; Kind = "v" },
        @{ Name = "etl_run_log"; Kind = "r" }
    )

    $quotedObjects = ($requiredObjects | ForEach-Object { "('$($_.Name)','$($_.Kind)')" }) -join ", "
    $sql = "WITH required(name, expected_kind) AS (SELECT * FROM (VALUES $quotedObjects) AS v(name, expected_kind)) SELECT name || ':' || expected_kind || ':' || COALESCE(c.relkind::text, '?') FROM required LEFT JOIN pg_class c ON c.oid = to_regclass('public.' || name) WHERE c.oid IS NULL OR c.relkind <> expected_kind ORDER BY name;"

    # Pipe SQL via stdin and pass PGPASSWORD as an argv element so the password
    # never enters a /bin/sh -c string (quote/$/backslash in the password would
    # otherwise break the command or run arbitrary shell inside the pod).
    $missing = $sql | kubectl exec -i statefulset/metabase-postgres -c postgres -- env "PGPASSWORD=$Password" psql -h $DbHost -p $DbPort -U $DbUser -d $DbName -AtX -v ON_ERROR_STOP=1 -f -
    if ($LASTEXITCODE -ne 0) {
        throw "Failed to query DWH contract before Metabase provisioning."
    }

    if ([string]::IsNullOrWhiteSpace($missing)) {
        return @()
    }

    return @($missing -split "`r?`n" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
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

    $dwhHost = Get-KubernetesSecretDecodedValue -SecretName "metabase-connections" -Key "DWH_DB_HOST"
    $dwhPort = Get-KubernetesSecretDecodedValue -SecretName "metabase-connections" -Key "DWH_DB_PORT"
    $dwhName = Get-KubernetesSecretDecodedValue -SecretName "metabase-connections" -Key "DWH_DB_NAME"
    $dwhUser = Get-KubernetesSecretDecodedValue -SecretName "metabase-connections" -Key "DWH_BI_USER"
    $dwhPassword = Get-KubernetesSecretDecodedValue -SecretName "metabase-connections" -Key "DWH_BI_PASSWORD"
    $dwhInitSqlInPod = "/tmp/dwh_init.sql"

    $missingDwhObjects = Get-DwhMissingMetabaseObjects -DbHost $dwhHost -DbPort $dwhPort -DbName $dwhName -DbUser $dwhUser -Password $dwhPassword
    if ($missingDwhObjects.Count -gt 0) {
        Write-Host "Applying db/dwh_init.sql to dwh_egisz before Metabase provisioning..."
        Write-Host "DWH objects requiring bootstrap: $($missingDwhObjects -join ', ')"
        Invoke-Checked "Scale Metabase deployment down for DWH bootstrap" {
            kubectl scale deployment/metabase --replicas=0
        }
        Invoke-Checked "Wait for Metabase deployment to scale down" {
            kubectl rollout status deployment/metabase --timeout=300s
        }
        Invoke-Checked "Copy DWH bootstrap SQL into Metabase PostgreSQL pod" {
            kubectl cp db/dwh_init.sql metabase-postgres-0:${dwhInitSqlInPod} -c postgres
        }
        Invoke-Checked "Bootstrap DWH schema for Metabase" {
            # argv form keeps the password out of any /bin/sh -c string
            kubectl exec statefulset/metabase-postgres -c postgres -- env "PGPASSWORD=$dwhPassword" psql -h $dwhHost -p $dwhPort -U $dwhUser -d $dwhName -v ON_ERROR_STOP=1 -f $dwhInitSqlInPod
        }
        Invoke-Checked "Scale Metabase deployment back up after DWH bootstrap" {
            kubectl scale deployment/metabase --replicas=1
        }
    } else {
        Write-Host "DWH contract already satisfies Metabase requirements; skipping db/dwh_init.sql bootstrap."
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

    Write-Host "Verifying Metabase cards..."
    Invoke-Checked "Verify Metabase cards" {
        kubectl exec deploy/metabase -- /bin/bash /app/verify-cards.sh
    }
}

if ($Component -in @("All", "Airflow")) {
    Install-Airflow
}

if ($Component -in @("All", "Metabase")) {
    Install-Metabase
}

Write-Host "Done. Airflow: http://localhost:8080, Metabase: http://localhost:3000"

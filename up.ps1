param(
    [ValidateSet("All", "Airflow", "Metabase")]
    [string]$Component = "All"
)

$ErrorActionPreference = "Stop"

$ImageTag = Get-Date -Format "yyyyMMddHHmmss"
$AirflowImage = "egisz-airflow-worker:${ImageTag}"
$MetabaseImage = "egisz-metabase:${ImageTag}"

function Get-EnvOrDefault {
    param(
        [string]$Name,
        [string]$DefaultValue
    )

    $value = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($value)) {
        return $DefaultValue
    }
    return $value
}

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

function Ensure-DwhDatabasePrivileges {
    $env:EGISZ_PG_HOST = Get-EnvOrDefault "EGISZ_PG_HOST" "127.0.0.1"
    $env:EGISZ_PG_PORT = Get-EnvOrDefault "EGISZ_PG_PORT" "5432"
    $env:EGISZ_PG_ADMIN_USER = Get-EnvOrDefault "EGISZ_PG_ADMIN_USER" "postgres"
    $env:EGISZ_PG_ADMIN_PASSWORD = Get-EnvOrDefault "EGISZ_PG_ADMIN_PASSWORD" "postgres"
    $env:EGISZ_DWH_DB = Get-EnvOrDefault "EGISZ_DWH_DB" "dwh_egisz"
    $env:EGISZ_DWH_ELT_USER = Get-EnvOrDefault "EGISZ_DWH_ELT_USER" "egisz"
    $env:EGISZ_DWH_ELT_PASSWORD = Get-EnvOrDefault "EGISZ_DWH_ELT_PASSWORD" "egisz"

    @'
import os
import psycopg2
from psycopg2 import sql

host = os.environ["EGISZ_PG_HOST"]
port = int(os.environ["EGISZ_PG_PORT"])
admin_user = os.environ["EGISZ_PG_ADMIN_USER"]
admin_password = os.environ["EGISZ_PG_ADMIN_PASSWORD"]
dwh_db = os.environ["EGISZ_DWH_DB"]
elt_user = os.environ["EGISZ_DWH_ELT_USER"]
elt_password = os.environ["EGISZ_DWH_ELT_PASSWORD"]

admin_conn = psycopg2.connect(
    host=host,
    port=port,
    user=admin_user,
    password=admin_password,
    database="postgres",
)
admin_conn.autocommit = True
with admin_conn.cursor() as cur:
    cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (elt_user,))
    if cur.fetchone() is None:
        cur.execute(
            sql.SQL("CREATE ROLE {} LOGIN PASSWORD %s").format(sql.Identifier(elt_user)),
            (elt_password,),
        )
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dwh_db,))
    if cur.fetchone() is None:
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dwh_db)))
admin_conn.close()

dwh_conn = psycopg2.connect(
    host=host,
    port=port,
    user=admin_user,
    password=admin_password,
    database=dwh_db,
)
dwh_conn.autocommit = True
with dwh_conn.cursor() as cur:
    cur.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(dwh_db), sql.Identifier(elt_user)))
    cur.execute(sql.SQL("GRANT USAGE, CREATE ON SCHEMA public TO {}").format(sql.Identifier(elt_user)))
    cur.execute(
        """
        DO $$
        DECLARE
            obj record;
        BEGIN
            FOR obj IN
                SELECT p.oid::regprocedure AS signature
                FROM pg_proc p
                JOIN pg_namespace n ON n.oid = p.pronamespace
                WHERE n.nspname = 'public'
                  AND p.proname LIKE 'egisz_%%'
            LOOP
                EXECUTE format('ALTER FUNCTION %%s OWNER TO %%I', obj.signature, %s);
            END LOOP;
        END
        $$;
        """,
        (elt_user,),
    )
    cur.execute(
        "SELECT has_schema_privilege(%s, 'public', 'CREATE'), has_schema_privilege(%s, 'public', 'USAGE')",
        (elt_user, elt_user),
    )
    can_create, can_usage = cur.fetchone()
    if not (can_create and can_usage):
        raise RuntimeError(f"{elt_user} is still missing public schema privileges in {dwh_db}")
dwh_conn.close()

print(f"DWH privileges OK: {elt_user}@{host}:{port}/{dwh_db} can CREATE in public")
'@ | py -
}

function Ensure-SecretFiles {
    Write-Host "Preparing Kubernetes secrets from examples..."
    if (-not (Test-Path k8s/metabase/metabase-connections-secret.yaml)) {
        Copy-Item k8s/metabase/metabase-connections-secret.example.yaml k8s/metabase/metabase-connections-secret.yaml
    }
}

function Ensure-AirflowInternalMetadataDatabase {
    Write-Host "Ensuring Airflow internal metadata database airflow_db exists when PostgreSQL already has a PVC..."
    $postgresPod = kubectl get pod airflow-postgresql-0 --ignore-not-found -o name
    if ([string]::IsNullOrWhiteSpace($postgresPod)) {
        Write-Host "Airflow PostgreSQL pod is not present yet; Helm will initialize airflow_db on first startup."
        return
    }

    Invoke-Checked "Ensure airflow_db in internal Airflow PostgreSQL" {
        kubectl exec airflow-postgresql-0 -- bash -lc "export PGPASSWORD=postgres; psql -U postgres -d postgres -tc `"SELECT 1 FROM pg_database WHERE datname = 'airflow_db'`" | grep -q 1 || createdb -U postgres airflow_db"
    }
}

function Install-Airflow {
    Ensure-SecretFiles
    Test-KubernetesConnection

    Write-Host "Ensuring external PostgreSQL DWH privileges for Airflow ELT user..."
    Ensure-DwhDatabasePrivileges

    Write-Host "Applying Airflow connection secrets..."
    Invoke-Checked "Apply Airflow connection secrets" {
        kubectl apply -f k8s/airflow/airflow-connections-configmap.yaml
    }

    Write-Host "Building Airflow image with current DAG and egisz_elt package..."
    Invoke-Checked "Build Airflow image" {
        docker build -t $AirflowImage -t egisz-airflow-worker:latest -f k8s/airflow/Dockerfile .
    }

    Ensure-AirflowInternalMetadataDatabase

    Write-Host "Installing Airflow..."
    Invoke-Checked "Install Airflow Helm release" {
        helm upgrade --install airflow apache-airflow/airflow -f k8s/airflow/values.yaml --timeout 15m --set-string images.airflow.tag=$ImageTag
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

    Write-Host "Bootstrapping external DWH schema through Airflow connection dwh_egisz_pg..."
    Invoke-Checked "Bootstrap DWH through Airflow" {
        kubectl exec deploy/airflow-scheduler -- airflow tasks test egisz_elt_dag bootstrap_dwh 2026-01-01
    }
}

function Install-Metabase {
    Ensure-SecretFiles
    Test-KubernetesConnection

    Write-Host "Applying Metabase connection secrets..."
    Invoke-Checked "Apply Metabase connection secrets" {
        kubectl apply -f k8s/metabase/metabase-connections-secret.yaml
    }

    Write-Host "Building Metabase image with current dashboard provisioning scripts..."
    Invoke-Checked "Build Metabase image" {
        docker build -t $MetabaseImage -t egisz-metabase:latest -f metabase/Dockerfile .
    }

    Write-Host "Starting Metabase..."
    Invoke-Checked "Apply Metabase deployment" {
        kubectl apply -f k8s/metabase/metabase.yaml
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

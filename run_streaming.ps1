<#
.SYNOPSIS
    Ejecuta el pipeline de streaming Olist completo.
    Configura Java 17, Hadoop y lanza productor + consumidor.

.USAGE
    .\run_streaming.ps1                 # Ejecuta todo
    .\run_streaming.ps1 -only producer  # Solo el productor
    .\run_streaming.ps1 -only consumer  # Solo el consumidor
#>

param(
    [string]$only = "all"
)

# ── Ruta base del proyecto (donde está este .ps1) ─────────────────────────────
$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

# ── Variables de entorno ──────────────────────────────────────────────────────
$env:JAVA_HOME = "C:\Users\jhphi\Downloads\olist-risk-pipeline\java17\jdk-17.0.11+9"
$env:HADOOP_HOME       = $ROOT
$env:PATH              = "$env:JAVA_HOME\bin;$env:PATH;$ROOT\bin"
$env:JAVA_TOOL_OPTIONS = ""

Write-Host "=================================================" -ForegroundColor Cyan
Write-Host " OLIST STREAMING PIPELINE" -ForegroundColor Cyan
Write-Host "=================================================" -ForegroundColor Cyan
Write-Host " Root:    $ROOT"
Write-Host " Java:    $(java -version 2>&1 | Select-String 'version')"
Write-Host " Kafka:   localhost:9092"
Write-Host " Spark:   local[2] + Java 17"
Write-Host "=================================================" -ForegroundColor Cyan

# ── Verificar Docker ──────────────────────────────────────────────────────────
$containers = docker ps --format "{{.Names}}" 2>$null
$required   = @("olist_kafka", "olist_zookeeper", "olist_postgres")

foreach ($c in $required) {
    if ($containers -notcontains $c) {
        Write-Host "[ERROR] Contenedor '$c' no está corriendo." -ForegroundColor Red
        Write-Host "        Ejecuta: docker compose up -d zookeeper kafka postgres" -ForegroundColor Yellow
        exit 1
    }
}
Write-Host "[OK] Contenedores Docker verificados." -ForegroundColor Green

# ── Activar venv ──────────────────────────────────────────────────────────────
$venvPath = "$ROOT\.venv\Scripts\Activate.ps1"
if (Test-Path $venvPath) {
    & $venvPath
    Write-Host "[OK] Entorno virtual activado." -ForegroundColor Green
} else {
    Write-Host "[ERROR] No se encontró el venv en: $venvPath" -ForegroundColor Red
    exit 1
}

# ── Ejecutar productor ────────────────────────────────────────────────────────
if ($only -eq "all" -or $only -eq "producer") {
    Write-Host "`n[1/2] Ejecutando productor Kafka..." -ForegroundColor Yellow
    python "$ROOT\streaming\kafka_producer.py"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "[ERROR] El productor falló (exit code $LASTEXITCODE)." -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK] Productor completado." -ForegroundColor Green
}

# ── Ejecutar consumidor ───────────────────────────────────────────────────────
if ($only -eq "all" -or $only -eq "consumer") {
    Write-Host "`n[2/2] Ejecutando consumidor Spark..." -ForegroundColor Yellow
    Write-Host "       Presiona Ctrl+C para detener el streaming." -ForegroundColor Gray
    python "$ROOT\streaming\spark_consumer.py"
}

Write-Host "`n[OK] Pipeline streaming finalizado." -ForegroundColor Green
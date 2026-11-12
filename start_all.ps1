# start_all.ps1 - starts the whole Weather Analytics system
param([switch]$KeepStream)   # -KeepStream keeps old live-stream results

$root = $PSScriptRoot
Set-Location $root
$py = Join-Path $root "venv\Scripts\python.exe"

# One-time: make 'datanode' resolve to this PC so Spark can read from HDFS
$hostsPath = "$env:windir\System32\drivers\etc\hosts"
$hasEntry = Select-String -Path $hostsPath -Pattern '^\s*127\.0\.0\.1\s+datanode(\s|$)' -Quiet
if (-not $hasEntry) {
    Write-Host "Adding 'datanode' to the hosts file (one-time, Windows will ask for admin approval)..." -ForegroundColor Yellow
    $cmd = "Add-Content -Path '$hostsPath' -Value ([Environment]::NewLine + '127.0.0.1 datanode'); ipconfig /flushdns"
    Start-Process powershell -Verb RunAs -Wait -ArgumentList "-NoProfile", "-Command", $cmd
}

function Step($msg) { Write-Host "`n==> $msg" -ForegroundColor Cyan }

if (-not (Test-Path $py)) {
    Write-Host "venv not found. Run: python -m venv venv ; pip install -r requirements.txt" -ForegroundColor Red
    exit 1
}

# 0. Stop leftovers from a previous run
Step "Cleaning up old processes"
& "$root\stop_all.ps1" -KeepDocker

# 1. Docker engine
Step "Checking Docker engine"
docker info *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Starting Docker Desktop (this can take 1-2 minutes)..."
    Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    $ok = $false
    for ($i = 0; $i -lt 60; $i++) {
        Start-Sleep 5
        docker info *> $null
        if ($LASTEXITCODE -eq 0) { $ok = $true; break }
    }
    if (-not $ok) { Write-Host "Docker did not start." -ForegroundColor Red; exit 1 }
}

# 2. Containers
Step "Starting Kafka and HDFS containers"
docker compose up -d

Step "Waiting for HDFS to be healthy"
for ($i = 0; $i -lt 40; $i++) {
    $h = docker inspect -f "{{.State.Health.Status}}" namenode 2>$null
    if ($h -eq "healthy") { break }
    Start-Sleep 5
}
docker exec namenode hdfs dfs -safemode wait *> $null

Step "Waiting for Kafka and ensuring the topic exists"
for ($i = 0; $i -lt 30; $i++) {
    docker exec kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092 *> $null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep 3
}
docker exec kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic weather-events --bootstrap-server localhost:9092 *> $null

# 3. Data (only does work on the first run)
Step "Checking data"
if (-not (Test-Path "data\raw\Delhi.csv")) {
    Write-Host "Downloading historical data..."
    & $py src\collect_historical.py
}
docker exec namenode hdfs dfs -test -e /weather/raw/Delhi.csv *> $null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Uploading CSVs to HDFS..."
    docker exec namenode hdfs dfs -mkdir -p /weather/raw *> $null
    docker exec namenode rm -rf /tmp/raw
    docker cp data\raw namenode:/tmp/raw
    docker exec namenode bash -c "hdfs dfs -put -f /tmp/raw/*.csv /weather/raw/" *> $null
}
if (-not (Test-Path "output\batch\monthly")) {
    Write-Host "Running Spark batch job..."
    & $py src\spark_batch.py
}

# 4. Reset the stream output so Spark's checkpoint and results stay consistent
if (-not $KeepStream) {
    Remove-Item -Recurse -Force "output\stream\_chk", "output\stream\data" -ErrorAction SilentlyContinue
}

# 5. Launch the long-running parts, each in its own window
function Launch($title, $cmd) {
    Start-Process powershell -ArgumentList "-NoExit", "-Command",
        "`$host.UI.RawUI.WindowTitle='$title'; Set-Location '$root'; $cmd"
}
Step "Launching producer, streaming job and dashboard"
Launch "Kafka Producer"  "& '$py' src\kafka_producer.py"
Start-Sleep 5
Launch "Spark Streaming" "& '$py' src\spark_streaming.py"
Start-Sleep 3
Launch "Dashboard"       "& '$py' -m streamlit run dashboard\app.py"

Write-Host "`nAll started. Dashboard: http://localhost:8501" -ForegroundColor Green
Write-Host "The Live tab fills in after about 1-2 minutes."
Write-Host "To stop everything, run: .\stop_all.ps1"
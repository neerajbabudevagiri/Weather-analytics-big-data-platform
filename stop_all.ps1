# stop_all.ps1 - stops the producer, streaming job, dashboard and Docker
param([switch]$KeepDocker)   # -KeepDocker leaves the containers running

Write-Host "Stopping project processes..."
Get-CimInstance Win32_Process |
    Where-Object {
        $_.ProcessId -ne $PID -and
        $_.Name -match "python|powershell|java" -and
        $_.CommandLine -match "kafka_producer|spark_streaming|streamlit"
    } |
    ForEach-Object { taskkill /PID $_.ProcessId /T /F *> $null }

if (-not $KeepDocker) {
    Write-Host "Stopping Docker containers (data is kept)..."
    docker compose stop
}
Write-Host "Done." -ForegroundColor Green
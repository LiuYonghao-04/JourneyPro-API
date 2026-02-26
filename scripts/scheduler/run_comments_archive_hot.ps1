param(
  [int]$RetainHotRows = 10000000,
  [int]$BatchSize = 30000,
  [int]$MaxBatches = 3,
  [int]$OlderThanDays = 0
)

$ErrorActionPreference = "Stop"

$apiRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$logDir = Join-Path $apiRoot "logs\maintenance"
$dateTag = Get-Date -Format "yyyyMMdd"
$logFile = Join-Path $logDir ("comments_archive_hot_" + $dateTag + ".log")
$lockFile = Join-Path $logDir "comments_archive_hot.lock"

function Write-Log {
  param([string]$Message)
  $line = "[" + (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + "] " + $Message
  $line | Tee-Object -FilePath $logFile -Append
}

if (-not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

if (Test-Path $lockFile) {
  $ageMinutes = ((Get-Date) - (Get-Item $lockFile).LastWriteTime).TotalMinutes
  if ($ageMinutes -lt 240) {
    Write-Log ("Skip: lock exists (" + [math]::Round($ageMinutes, 1) + " min old).")
    exit 0
  }
}

Set-Content -Path $lockFile -Value (Get-Date -Format "o")
$exitCode = 0

try {
  Push-Location $apiRoot
  Write-Log "Start hourly archive maintenance."
  Write-Log ("Args retain_hot_rows=" + $RetainHotRows + ", batch_size=" + $BatchSize + ", max_batches=" + $MaxBatches + ", older_than_days=" + $OlderThanDays)

  $npmCmd = (Get-Command npm.cmd -ErrorAction SilentlyContinue).Source
  if ([string]::IsNullOrWhiteSpace($npmCmd)) {
    $npmCmd = "npm"
  }

  $npmArgs = @(
    "run", "comments:archive:hot",
    "--",
    "--retain-hot-rows=$RetainHotRows",
    "--batch-size=$BatchSize",
    "--max-batches=$MaxBatches",
    "--older-than-days=$OlderThanDays"
  )

  & $npmCmd @npmArgs 2>&1 | Tee-Object -FilePath $logFile -Append
  $exitCode = $LASTEXITCODE
  Write-Log ("Finish hourly archive maintenance with exit_code=" + $exitCode)
}
catch {
  $exitCode = 1
  Write-Log ("Archive task failed: " + $_.Exception.Message)
}
finally {
  Pop-Location
  Remove-Item -Path $lockFile -Force -ErrorAction SilentlyContinue
}

exit $exitCode

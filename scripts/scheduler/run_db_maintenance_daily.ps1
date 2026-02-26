param(
  [bool]$ApplyIndexFix = $true
)

$ErrorActionPreference = "Stop"

$apiRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$logDir = Join-Path $apiRoot "logs\maintenance"
$dateTag = Get-Date -Format "yyyyMMdd"
$logFile = Join-Path $logDir ("db_maintenance_daily_" + $dateTag + ".log")
$lockFile = Join-Path $logDir "db_maintenance_daily.lock"

function Write-Log {
  param([string]$Message)
  $line = "[" + (Get-Date -Format "yyyy-MM-dd HH:mm:ss") + "] " + $Message
  $line | Tee-Object -FilePath $logFile -Append
}

function Invoke-Step {
  param(
    [string]$Title,
    [string[]]$NpmArgs
  )
  Write-Log ("Run step: " + $Title)
  & $script:npmCmd @NpmArgs 2>&1 | Tee-Object -FilePath $logFile -Append
  if ($LASTEXITCODE -ne 0) {
    throw ("Step failed: " + $Title + " (exit_code=" + $LASTEXITCODE + ")")
  }
}

if (-not (Test-Path $logDir)) {
  New-Item -ItemType Directory -Path $logDir -Force | Out-Null
}

if (Test-Path $lockFile) {
  $ageMinutes = ((Get-Date) - (Get-Item $lockFile).LastWriteTime).TotalMinutes
  if ($ageMinutes -lt 600) {
    Write-Log ("Skip: lock exists (" + [math]::Round($ageMinutes, 1) + " min old).")
    exit 0
  }
}

Set-Content -Path $lockFile -Value (Get-Date -Format "o")
$exitCode = 0

try {
  Push-Location $apiRoot
  Write-Log "Start daily DB maintenance."
  Write-Log ("ApplyIndexFix=" + $ApplyIndexFix)

  $script:npmCmd = (Get-Command npm.cmd -ErrorAction SilentlyContinue).Source
  if ([string]::IsNullOrWhiteSpace($script:npmCmd)) {
    $script:npmCmd = "npm"
  }

  Invoke-Step -Title "DB health check" -NpmArgs @("run", "db:health")
  Invoke-Step -Title "Index compaction dry-run" -NpmArgs @("run", "db:migrate:index-v2", "--", "--dry-run")
  if ($ApplyIndexFix) {
    Invoke-Step -Title "Index compaction apply" -NpmArgs @("run", "db:migrate:index-v2")
  }

  Write-Log "Finish daily DB maintenance."
}
catch {
  $exitCode = 1
  Write-Log ("Daily maintenance failed: " + $_.Exception.Message)
}
finally {
  Pop-Location
  Remove-Item -Path $lockFile -Force -ErrorAction SilentlyContinue
}

exit $exitCode

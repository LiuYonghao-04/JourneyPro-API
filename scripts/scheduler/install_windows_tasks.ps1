param(
  [string]$TaskPrefix = "JourneyPro",
  [string]$HourlyStart = "",
  [string]$DailyTime = "03:20",
  [int]$RetainHotRows = 10000000,
  [int]$ArchiveBatchSize = 30000,
  [int]$ArchiveMaxBatches = 3,
  [int]$OlderThanDays = 0,
  [bool]$ApplyIndexFix = $true
)

$ErrorActionPreference = "Stop"

function Run-Schtasks {
  param([string[]]$TaskArgs)
  & schtasks.exe @TaskArgs
  if ($LASTEXITCODE -ne 0) {
    throw ("schtasks failed: " + ($TaskArgs -join " "))
  }
}

$archiveRunner = Resolve-Path (Join-Path $PSScriptRoot "run_comments_archive_hot.ps1")
$dailyRunner = Resolve-Path (Join-Path $PSScriptRoot "run_db_maintenance_daily.ps1")

if ([string]::IsNullOrWhiteSpace($HourlyStart)) {
  $HourlyStart = (Get-Date).AddMinutes(5).ToString("HH:mm")
}

$archiveTaskName = "$TaskPrefix-CommentsArchive-Hourly"
$dailyTaskName = "$TaskPrefix-DBMaintenance-Daily"

$archiveCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$archiveRunner`" -RetainHotRows $RetainHotRows -BatchSize $ArchiveBatchSize -MaxBatches $ArchiveMaxBatches -OlderThanDays $OlderThanDays"
$dailyCmd = "powershell -NoProfile -ExecutionPolicy Bypass -File `"$dailyRunner`" -ApplyIndexFix:$ApplyIndexFix"

Run-Schtasks -TaskArgs @("/Create", "/TN", $archiveTaskName, "/TR", $archiveCmd, "/SC", "HOURLY", "/MO", "1", "/ST", $HourlyStart, "/RL", "LIMITED", "/F")
Run-Schtasks -TaskArgs @("/Create", "/TN", $dailyTaskName, "/TR", $dailyCmd, "/SC", "DAILY", "/ST", $DailyTime, "/RL", "LIMITED", "/F")

Write-Host ""
Write-Host "Installed tasks:"
Run-Schtasks -TaskArgs @("/Query", "/TN", $archiveTaskName, "/V", "/FO", "LIST")
Write-Host ""
Run-Schtasks -TaskArgs @("/Query", "/TN", $dailyTaskName, "/V", "/FO", "LIST")

Write-Host ""
Write-Host "Done."
Write-Host ("- " + $archiveTaskName + " hourly at " + $HourlyStart)
Write-Host ("- " + $dailyTaskName + " daily at " + $DailyTime)

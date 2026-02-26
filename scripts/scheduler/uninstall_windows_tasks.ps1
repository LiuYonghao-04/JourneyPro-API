param(
  [string]$TaskPrefix = "JourneyPro"
)

$ErrorActionPreference = "Stop"

$taskNames = @(
  "$TaskPrefix-CommentsArchive-Hourly",
  "$TaskPrefix-DBMaintenance-Daily"
)

foreach ($name in $taskNames) {
  & schtasks.exe /Query /TN $name *> $null
  if ($LASTEXITCODE -eq 0) {
    & schtasks.exe /Delete /TN $name /F
    if ($LASTEXITCODE -ne 0) {
      throw ("Failed to delete task: " + $name)
    }
    Write-Host ("Deleted: " + $name)
  } else {
    Write-Host ("Skip (not found): " + $name)
  }
}

Write-Host "Done."

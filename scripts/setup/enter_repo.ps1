[CmdletBinding()]
param(
    [switch]$ActivateVenv = $true
)

$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$repoParent = Split-Path -Parent $repoRoot

$env:LIFESPAN_REPO_ROOT = $repoRoot
$env:LIFESPAN_DATA_ROOT = Join-Path $repoParent "Lifespan-data"
$env:LIFESPAN_TEMP_ROOT = Join-Path $repoParent "Lifespan-temp"
$env:LIFESPAN_REPORT_ROOT = Join-Path $repoParent "Lifespan-report"
$env:LIFESPAN_VALIDATED_ROOT = Join-Path $repoParent "Lifespan-Validated"

Set-Location $repoRoot

if ($ActivateVenv) {
    $activateScript = Join-Path $repoRoot ".venv\Scripts\Activate.ps1"
    if (Test-Path -LiteralPath $activateScript) {
        & $activateScript
    }
    else {
        Write-Warning ".venv not found at $activateScript"
    }
}

$pythonCommand = Get-Command python -ErrorAction SilentlyContinue

Write-Host "Repo root      : $repoRoot"
Write-Host "Location       : $(Get-Location)"
Write-Host "Data root      : $env:LIFESPAN_DATA_ROOT"
Write-Host "Temp root      : $env:LIFESPAN_TEMP_ROOT"
Write-Host "Report root    : $env:LIFESPAN_REPORT_ROOT"
Write-Host "Validated root : $env:LIFESPAN_VALIDATED_ROOT"
if ($pythonCommand) {
    Write-Host "Python         : $($pythonCommand.Source)"
}

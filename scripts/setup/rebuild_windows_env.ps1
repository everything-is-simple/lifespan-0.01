[CmdletBinding()]
param(
    [string]$PythonExe = "D:\miniconda310\python.exe",
    [string]$GitExe = "C:\Program Files\Git\cmd\git.exe",
    [switch]$RecreateVenv,
    [switch]$SkipTests,
    [switch]$SkipGovernance,
    [switch]$SkipGitCheck
)

$ErrorActionPreference = "Stop"

function Invoke-NativeCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Exe,
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    Write-Host "==> $Label"
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed: $Exe $($Arguments -join ' ')"
    }
}

function Assert-ExecutablePath {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue,
        [Parameter(Mandatory = $true)]
        [string]$Label
    )

    if (-not (Test-Path -LiteralPath $PathValue)) {
        throw "$Label not found: $PathValue"
    }
}

function Ensure-Directory {
    param(
        [Parameter(Mandatory = $true)]
        [string]$PathValue
    )

    if (-not (Test-Path -LiteralPath $PathValue)) {
        New-Item -ItemType Directory -Force -Path $PathValue | Out-Null
    }
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$repoParent = Split-Path -Parent $repoRoot
Set-Location $repoRoot

$env:LIFESPAN_REPO_ROOT = $repoRoot
$env:LIFESPAN_DATA_ROOT = Join-Path $repoParent "Lifespan-data"
$env:LIFESPAN_TEMP_ROOT = Join-Path $repoParent "Lifespan-temp"
$env:LIFESPAN_REPORT_ROOT = Join-Path $repoParent "Lifespan-report"
$env:LIFESPAN_VALIDATED_ROOT = Join-Path $repoParent "Lifespan-Validated"

Ensure-Directory -PathValue $env:LIFESPAN_DATA_ROOT
Ensure-Directory -PathValue $env:LIFESPAN_TEMP_ROOT
Ensure-Directory -PathValue $env:LIFESPAN_REPORT_ROOT
Ensure-Directory -PathValue $env:LIFESPAN_VALIDATED_ROOT

Assert-ExecutablePath -PathValue $PythonExe -Label "Python"
if (-not $SkipGitCheck) {
    Assert-ExecutablePath -PathValue $GitExe -Label "Git"
    Invoke-NativeCommand -Exe $GitExe -Arguments @("--version") -Label "Check Git"
}

Invoke-NativeCommand -Exe $PythonExe -Arguments @("--version") -Label "Check Python"

try {
    & $PythonExe -m virtualenv --version | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "virtualenv not ready"
    }
}
catch {
    Invoke-NativeCommand -Exe $PythonExe -Arguments @("-m", "pip", "install", "virtualenv") -Label "Install virtualenv"
}

$venvRoot = Join-Path $repoRoot ".venv"
if ($RecreateVenv -and (Test-Path -LiteralPath $venvRoot)) {
    Write-Host "==> Remove existing .venv"
    Remove-Item -LiteralPath $venvRoot -Recurse -Force
}

$venvPython = Join-Path $venvRoot "Scripts\python.exe"
if (-not (Test-Path -LiteralPath $venvPython)) {
    Invoke-NativeCommand -Exe $PythonExe -Arguments @("-m", "virtualenv", "--python", $PythonExe, ".venv") -Label "Create .venv"
}

Invoke-NativeCommand -Exe $venvPython -Arguments @("-m", "pip", "install", "--upgrade", "pip") -Label "Upgrade pip"
Invoke-NativeCommand -Exe $venvPython -Arguments @("-m", "pip", "install", "--editable", ".[dev]") -Label "Install project and dev dependencies"

$importSmoke = "import duckdb, pandas, mlq; print('imports_ok')"
Invoke-NativeCommand -Exe $venvPython -Arguments @("-c", $importSmoke) -Label "Run import smoke"

if (-not $SkipGovernance) {
    Invoke-NativeCommand -Exe $venvPython -Arguments @("scripts/system/check_development_governance.py") -Label "Run development governance"
    Invoke-NativeCommand -Exe $venvPython -Arguments @(".codex/skills/lifespan-execution-discipline/scripts/check_execution_indexes.py", "--include-untracked") -Label "Run execution index governance"
}

if (-not $SkipTests) {
    Invoke-NativeCommand -Exe $venvPython -Arguments @("-m", "pytest", "tests/unit/core/test_paths.py", "-q") -Label "Run core path tests"
}

Write-Host "==> Environment ready"
Write-Host "Repo root      : $repoRoot"
Write-Host "Python         : $venvPython"
Write-Host "Data root      : $env:LIFESPAN_DATA_ROOT"
Write-Host "Temp root      : $env:LIFESPAN_TEMP_ROOT"
Write-Host "Report root    : $env:LIFESPAN_REPORT_ROOT"
Write-Host "Validated root : $env:LIFESPAN_VALIDATED_ROOT"

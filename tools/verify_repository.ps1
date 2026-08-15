[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [string]$PythonExecutable = "python"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

function Invoke-SecretlessPythonCheck {
    param([string[]]$CommandArguments)

    & $PythonExecutable @CommandArguments
    if ($LASTEXITCODE -ne 0) {
        throw "Secretless repository check failed: $PythonExecutable $($CommandArguments -join ' ')"
    }
}

$resolvedRepo = (Resolve-Path -LiteralPath $RepoRoot).Path
$toolchainPath = Join-Path $resolvedRepo "runtime/toolchain.lock.json"
if (-not (Test-Path -LiteralPath $toolchainPath -PathType Leaf)) {
    throw "Pinned toolchain lock is missing: $toolchainPath"
}

$toolchain = Get-Content -LiteralPath $toolchainPath -Raw | ConvertFrom-Json
if ($toolchain.schema_version -ne "weather-toolchain/v1") {
    throw "Unsupported toolchain schema: $($toolchain.schema_version)"
}

foreach ($toolName in "python", "airflow", "dbt_core", "dbt_adapter", "node") {
    if (-not $toolchain.tools.$toolName -or -not $toolchain.tools.$toolName.version) {
        throw "Pinned toolchain is missing a version for $toolName"
    }
}

$expectedPythonMinor = [regex]::Match($toolchain.tools.python.version, "^\d+\.\d+").Value
if (-not $expectedPythonMinor) {
    throw "Pinned Python version is invalid: $($toolchain.tools.python.version)"
}
$actualPythonMinor = & $PythonExecutable -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')"
if ($LASTEXITCODE -ne 0) {
    throw "Cannot query Python executable: $PythonExecutable"
}
$actualPythonMinor = ($actualPythonMinor | Select-Object -Last 1).Trim()
if ($actualPythonMinor -ne $expectedPythonMinor) {
    throw "Python minor version mismatch: expected $expectedPythonMinor, got $actualPythonMinor"
}

Write-Output "Pinned toolchain: Python $($toolchain.tools.python.version); Airflow $($toolchain.tools.airflow.version); dbt-core $($toolchain.tools.dbt_core.version); dbt-trino $($toolchain.tools.dbt_adapter.version); Node $($toolchain.tools.node.version)."
Write-Output "Secretless repository checks: policy, provenance integrity/coverage, tests/repository and tests/deploy."

# This script intentionally invokes only local Python policy/provenance/tests.
# It never invokes Airflow, Docker, or pipeline-control commands.
Push-Location $resolvedRepo
try {
    Invoke-SecretlessPythonCheck @("-m", "tools.repository_policy", "--repo-root", $resolvedRepo)
    Invoke-SecretlessPythonCheck @("-m", "tools.verify_provenance", "--repo-root", $resolvedRepo)
    Invoke-SecretlessPythonCheck @("-m", "tools.refresh_provenance", "--repo-root", $resolvedRepo, "--check")
    Invoke-SecretlessPythonCheck @("-m", "tools.workflow_policy", "--repo-root", $resolvedRepo)
    Invoke-SecretlessPythonCheck @("-m", "pytest", "tests/repository", "tests/deploy")
}
finally {
    Pop-Location
}

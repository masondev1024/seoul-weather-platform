[CmdletBinding()]
param(
    [string]$RepoRoot = "",
    [string]$PythonExecutable = "python",
    [switch]$PrintCommand
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ([string]::IsNullOrWhiteSpace($RepoRoot)) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}

$resolvedRepo = (Resolve-Path -LiteralPath $RepoRoot).Path
$commandArguments = @("-m", "tools.dagbag_check", "--repo-root", $resolvedRepo)
if ($PrintCommand) {
    $commandArguments += "--print-command"
}

Push-Location $resolvedRepo
try {
    & $PythonExecutable @commandArguments
    $commandExitCode = $LASTEXITCODE
}
finally {
    Pop-Location
}
if ($commandExitCode -ne 0) {
    throw "Isolated DagBag verification failed."
}

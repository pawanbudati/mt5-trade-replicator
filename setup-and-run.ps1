[CmdletBinding()]
param(
    [switch]$Reinstall,
    [switch]$RecreateVenv
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venvDir = Join-Path $root '.venv'
$venvPython = Join-Path $venvDir 'Scripts\python.exe'
$configPath = Join-Path $root 'config.json'
$configExample = Join-Path $root 'config.example.json'

function Test-CompatiblePython([string]$Executable, [string[]]$CommandArguments) {
    & $Executable @CommandArguments -c "import struct, sys; sys.exit(0 if (3, 10) -le sys.version_info[:2] -and sys.version_info[:2] -le (3, 14) -and struct.calcsize('P') == 8 else 1)" 2>$null
    return $LASTEXITCODE -eq 0
}

function Get-CompatiblePythonCommand {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        foreach ($version in '3.14', '3.13', '3.12', '3.11', '3.10') {
            if (Test-CompatiblePython 'py' @("-$version")) {
                return @('py', "-$version")
            }
        }
    }
    if (Get-Command python -ErrorAction SilentlyContinue) {
        if (Test-CompatiblePython 'python' @()) {
            return @('python')
        }
    }
    throw 'Install 64-bit CPython 3.10 through 3.14, then run this script again. MetaTrader5 only publishes Windows x86-64 wheels.'
}

if ($RecreateVenv -and (Test-Path $venvDir)) {
    Write-Host 'Removing the existing virtual environment...'
    Remove-Item -LiteralPath $venvDir -Recurse -Force
}

if (-not (Test-Path $venvPython)) {
    $pythonCommand = Get-CompatiblePythonCommand
    Write-Host 'Creating virtual environment...'
    if ($pythonCommand.Length -gt 1) {
        & $pythonCommand[0] $pythonCommand[1] -m venv .venv
    } else {
        & $pythonCommand[0] -m venv .venv
    }
    if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed.' }
} elseif (-not (Test-CompatiblePython $venvPython @())) {
    throw 'The existing .venv does not use supported 64-bit Python 3.10-3.14. Install a supported Python version, then run: .\setup-and-run.ps1 -RecreateVenv'
}

if ($Reinstall -or -not (Test-Path (Join-Path $root '.venv\.installed'))) {
    Write-Host 'Installing Python dependencies...'
    & $venvPython -m pip install --upgrade pip
    if ($LASTEXITCODE -ne 0) { throw 'Could not upgrade pip.' }
    & $venvPython -m pip install -r requirements.txt
    if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
    New-Item -ItemType File -Path (Join-Path $root '.venv\.installed') -Force | Out-Null
}

if (-not (Test-Path $configPath)) {
    Copy-Item -LiteralPath $configExample -Destination $configPath
    Write-Host ''
    Write-Host 'Created config.json from config.example.json.' -ForegroundColor Yellow
    Write-Host 'Add the two MT5 account logins, passwords, servers, and terminal paths, then run this script again.' -ForegroundColor Yellow
    exit 0
}

Write-Host 'Starting MT5 Trade Copier v2. Press Ctrl+C to stop.'
& $venvPython main.py
exit $LASTEXITCODE

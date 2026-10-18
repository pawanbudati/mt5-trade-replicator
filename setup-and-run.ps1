[CmdletBinding()]
param(
    [switch]$Reinstall
)

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $root

$venvPython = Join-Path $root '.venv\Scripts\python.exe'
$configPath = Join-Path $root 'config.json'
$configExample = Join-Path $root 'config.example.json'

function Get-PythonCommand {
    $launcher = Get-Command py -ErrorAction SilentlyContinue
    if ($launcher) {
        return @('py', '-3')
    }
    $python = Get-Command python -ErrorAction SilentlyContinue
    if ($python) {
        return @('python')
    }
    throw 'Python 3 was not found. Install Python 3.10+ from python.org, then run this script again.'
}

if (-not (Test-Path $venvPython)) {
    $pythonCommand = Get-PythonCommand
    Write-Host 'Creating virtual environment...'
    if ($pythonCommand.Length -gt 1) {
        & $pythonCommand[0] $pythonCommand[1] -m venv .venv
    } else {
        & $pythonCommand[0] -m venv .venv
    }
    if ($LASTEXITCODE -ne 0) { throw 'Virtual environment creation failed.' }
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

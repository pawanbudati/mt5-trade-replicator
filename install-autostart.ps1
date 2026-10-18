$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$python = Join-Path $root '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) { throw 'Run install.bat first.' }
$action = New-ScheduledTaskAction -Execute $python -Argument "`"$(Join-Path $root 'main.py')`"" -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -AtStartup
$settings = New-ScheduledTaskSettingsSet -RestartCount 999 -RestartInterval (New-TimeSpan -Minutes 1) -ExecutionTimeLimit ([TimeSpan]::Zero)
Register-ScheduledTask -TaskName 'MT5 Account Copier' -Action $action -Trigger $trigger -Settings $settings -Description 'Runs the MT5 copier v2 supervisor, watcher, and executor' -RunLevel Limited -Force
Write-Host 'Created startup task. It runs as the current Windows user; ensure that user is logged in and MT5 terminals are available.'

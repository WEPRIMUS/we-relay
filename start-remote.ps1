<#
.SYNOPSIS
    Starts WE Relay v0.4 (bound to 0.0.0.0:8788 for local + LAN access) and activates
    the Cloudflare HTTPS tunnel so you can access the dashboard and bridge agents from
    your laptop or phone anywhere.
#>
param(
    [int]$Port = 8788,
    [switch]$NoTunnel
)

$ErrorActionPreference = "Stop"
$RelayDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ServerPy = Join-Path $RelayDir "server.py"
$CliPy    = Join-Path $RelayDir "cli.py"
$LocalUrl = "http://127.0.0.1:$Port"

function Test-RelayHealth {
    try {
        $r = Invoke-RestMethod -Uri "$LocalUrl/v1/health" -TimeoutSec 2
        return [bool]$r.ok
    } catch {
        return $false
    }
}

if (-not (Test-RelayHealth)) {
    Write-Host "[we-relay] Starting server on 0.0.0.0:$Port ..." -ForegroundColor Cyan
    $LogDir = if (Test-Path "D:\agent-workspace\scratch") { "D:\agent-workspace\scratch" } else { $RelayDir }
    $OutLog = Join-Path $LogDir "we-relay-$Port.out.log"
    $ErrLog = Join-Path $LogDir "we-relay-$Port.err.log"
    Start-Process -FilePath "python" `
        -ArgumentList @("`"$ServerPy`"", "--host", "0.0.0.0", "--port", "$Port") `
        -WorkingDirectory $RelayDir `
        -WindowStyle Hidden `
        -RedirectStandardOutput $OutLog `
        -RedirectStandardError $ErrLog | Out-Null

    $deadline = (Get-Date).AddSeconds(8)
    while (-not (Test-RelayHealth) -and (Get-Date) -lt $deadline) {
        Start-Sleep -Milliseconds 300
    }
}

if (-not (Test-RelayHealth)) {
    throw "WE Relay failed to start on $LocalUrl"
}

if (-not $NoTunnel) {
    Write-Host "[we-relay] Activating HTTPS tunnel ..." -ForegroundColor Cyan
    python $CliPy --url $LocalUrl remote --start
} else {
    python $CliPy --url $LocalUrl remote
}

Write-Host ""
Write-Host "Tip: Open $LocalUrl and click [Remote] in the top bar for your QR code, 1-Click Magic Link, and Laptop Agent Bridge prompt." -ForegroundColor Green

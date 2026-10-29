$ErrorActionPreference = 'Continue'
Set-StrictMode -Version Latest

function Invoke-Compose {
    param([string[]]$ComposeArgs, [switch]$ShowOutput)
    $stderrPath = [System.IO.Path]::GetTempFileName()
    try {
        $previousPreference = $ErrorActionPreference
        $ErrorActionPreference = 'SilentlyContinue'
        try {
            $output = & docker compose @ComposeArgs 2> $stderrPath
            $exitCode = $LASTEXITCODE
        } finally {
            $ErrorActionPreference = $previousPreference
        }
        if ($exitCode -ne 0) {
            Get-Content -LiteralPath $stderrPath | ForEach-Object { Write-Output $_ }
            $output | ForEach-Object { Write-Output $_ }
            throw "docker compose $($ComposeArgs -join ' ') failed"
        }
        if ($ShowOutput) {
            Get-Content -LiteralPath $stderrPath | ForEach-Object { Write-Output $_ }
            $output | ForEach-Object { Write-Output $_ }
        }
        return $output
    } finally {
        Remove-Item -LiteralPath $stderrPath -Force -ErrorAction SilentlyContinue
    }
}

function Wait-Healthy {
    param([string]$Name, [int]$TimeoutSeconds = 180)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $state = (& docker inspect $Name --format '{{.State.Status}}/{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' 2>$null)
        if ($LASTEXITCODE -eq 0 -and $state.Trim() -eq 'running/healthy') {
            return
        }
        Start-Sleep -Seconds 3
    }
    throw "$Name did not become healthy within $TimeoutSeconds seconds"
}

if (-not (Test-Path -LiteralPath '.env')) {
    Copy-Item -LiteralPath '.env.example' -Destination '.env'
    $password = -join ((1..48) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
    $apiKey = -join ((1..48) | ForEach-Object { '{0:x}' -f (Get-Random -Maximum 16) })
    (Get-Content -LiteralPath '.env') `
        -replace '^POSTGRES_PASSWORD=.*', "POSTGRES_PASSWORD=$password" `
        -replace '^MAO_API_KEY=.*', "MAO_API_KEY=$apiKey" |
        Set-Content -LiteralPath '.env'
}

$apiKey = (Select-String -LiteralPath '.env' -Pattern '^MAO_API_KEY=').Line.Split('=', 2)[1]

Invoke-Compose -ComposeArgs @('up', '-d', '--build') | Out-Null

foreach ($name in @(
    'mao-db',
    'mao-redis',
    'mao-engine-1',
    'mao-engine-2',
    'mao-gateway',
    'mao-task-executor',
    'mao-autoheal'
)) {
    Wait-Healthy -Name $name
}

$certState = (& docker inspect mao-cert-generator --format '{{.State.Status}}/{{.State.ExitCode}}' 2>$null).Trim()
if ($certState -ne 'exited/0') {
    throw "certificate generator did not exit cleanly: $certState"
}

$unauthorized = & curl.exe -s -o NUL -w '%{http_code}' 'http://127.0.0.1:8090/events/cursor?event_type=TASK_CREATED'
if ($unauthorized -ne '401') {
    throw "expected HTTP 401 without API key, got $unauthorized"
}

& curl.exe -fsS -H "X-API-Key: $apiKey" 'http://127.0.0.1:8090/events/cursor?event_type=TASK_CREATED' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'authorized HTTP request failed'
}

& curl.exe -kfsS 'https://127.0.0.1:8443/health' | Out-Null
if ($LASTEXITCODE -ne 0) {
    throw 'TLS health check failed'
}

$metrics = Invoke-Compose -ComposeArgs @('exec', '-T', 'engine-1', 'wget', '-qO-', 'http://127.0.0.1:8090/metrics')
if (($metrics -join "`n") -notmatch 'mao_http_requests_total') {
    throw 'metrics endpoint does not expose mao_http_requests_total'
}

Invoke-Compose -ComposeArgs @('exec', '-T', 'task-executor', 'python', 'healthcheck.py') -ShowOutput
Invoke-Compose -ComposeArgs @('exec', '-T', 'task-executor', 'sh', '-c', 'python -m unittest discover -s tests -v 2>&1') -ShowOutput
Invoke-Compose -ComposeArgs @('exec', '-T', 'task-executor', 'python', '-u', 'trigger_task.py') -ShowOutput

$groups = Invoke-Compose -ComposeArgs @('exec', '-T', 'redis', 'redis-cli', 'XINFO', 'GROUPS', 'mao:tasks')
if (($groups -join "`n") -notmatch 'mao-agents') {
    throw 'Redis consumer group mao-agents is missing'
}

$pending = Invoke-Compose -ComposeArgs @('exec', '-T', 'redis', 'redis-cli', 'XPENDING', 'mao:tasks', 'mao-agents')
$pendingCount = ($pending | Select-Object -First 1).Trim()
if ($pendingCount -ne '0') {
    throw "task stream has $pendingCount pending entries"
}

Write-Output 'STACK_VERIFICATION_OK'

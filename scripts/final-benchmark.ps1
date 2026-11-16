#Requires -Version 5.1
<#
.SYNOPSIS
    最终性能测试（Windows 版）：独立 TCP 服务端 + 外部纯 H2 客户端并发压测。

.DESCRIPTION
    与 final-benchmark.sh 等价：构建、启动独立 TCP 服务端、等待端口就绪、跑
    ConcurrentQueryBench、退出时清理服务端进程。每次调用使用全新的服务端进程与
    空数据库，因此不同写负载需要分别调用。

    默认压测内存库（jdbc:h2:mem:），OMAP 持久化不启用。-Persist 改为文件模式
    （jdbc:h2:<DataDir>\h2omapdb），OMAP 持久化随之启用；压测结束后脚本执行
    SHUTDOWN、停服务端、用新进程重新打开数据库并校验行数，校验通过才退出 0。

    -Rows 不得小于 1000000：ConcurrentQueryBench 会校验每条查询命中超过 50% 的行，
    行数更少时它在预热阶段直接抛异常退出。

.EXAMPLE
    ./scripts/final-benchmark.ps1
    内存库，1000 万行，16 读 + 1 写，写线程不限速。

.EXAMPLE
    ./scripts/final-benchmark.ps1 -WriterRps 50
    同上，写线程每秒执行 50 轮。

.EXAMPLE
    ./scripts/final-benchmark.ps1 -WriterRps 50 -Persist
    文件模式，写线程每秒执行 50 轮，结束后重启校验行数。

.EXAMPLE
    ./scripts/final-benchmark.ps1 -Rows 1000000 -ServerXmx 3g -ClientXmx 1g -Persist
    100 万行文件模式冒烟。
#>
[CmdletBinding()]
param(
    [int]$Rows = 10000000,
    [int]$Readers = 16,
    [int]$Rounds = 50,
    [int]$WriterRps = 0,
    [switch]$NoWriter,
    [switch]$Persist,
    [string]$DataDir = 'target\bench-db',
    [switch]$KeepData,
    [string]$ServerXmx = '8g',
    [string]$ClientXmx = '2g',
    [int]$Port = 19092,
    [ValidateSet('omap', 'mv')][string]$Engine = 'omap',
    [switch]$SkipBuild
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$H2Version = '2.5.252'
$MainJar = 'target/h2omap-0.1.0-SNAPSHOT.jar'
$BenchClass = 'com.hyd.h2omap.bench.ConcurrentQueryBench'

$root = Split-Path -Parent $PSScriptRoot
Set-Location $root

function Resolve-H2Jar {
    $candidates = @(
        (Join-Path $env:USERPROFILE ".m2\repository\com\h2database\h2\$H2Version\h2-$H2Version.jar")
    )
    try {
        $repo = (& mvn -B -q help:evaluate -Dexpression=settings.localRepository -DforceStdout 2>$null)
        if ($LASTEXITCODE -eq 0 -and $repo) {
            $candidates += (Join-Path $repo.Trim() "com\h2database\h2\$H2Version\h2-$H2Version.jar")
        }
    } catch {
        # help:evaluate 不可用时只依赖默认路径
    }
    foreach ($path in $candidates) {
        if (Test-Path $path) { return (Resolve-Path $path).Path }
    }
    throw "ERROR: h2-$H2Version.jar not found in local Maven repository."
}

function Test-TcpPort {
    param([int]$TargetPort)
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $client.Connect('127.0.0.1', $TargetPort)
        return $true
    } catch {
        return $false
    } finally {
        $client.Close()
    }
}

function Invoke-ShellSql {
    param([string]$Jar, [string]$Url, [string]$Sql)
    return (& java -cp $Jar org.h2.tools.Shell -url $Url -user sa -sql $Sql 2>&1)
}

function Get-PersistedRowCount {
    param([string]$Jar, [string]$LocalUrl)
    $output = Invoke-ShellSql -Jar $Jar -Url $LocalUrl -Sql 'SELECT COUNT(*) FROM T'
    foreach ($line in $output) {
        if ($line -match '^\s*(\d+)\s*$') { return [long]$Matches[1] }
    }
    throw "ERROR: cannot parse COUNT(*) from Shell output:`n$($output -join "`n")"
}

function Get-DirectorySize {
    param([string]$Base)
    $sum = 0
    foreach ($item in Get-ChildItem -Path "$Base*" -Force -ErrorAction SilentlyContinue) {
        if ($item.PSIsContainer) {
            $inner = Get-ChildItem -LiteralPath $item.FullName -Recurse -File -Force -ErrorAction SilentlyContinue |
                Measure-Object -Property Length -Sum
            if ($inner.Sum) { $sum += $inner.Sum }
        } else {
            $sum += $item.Length
        }
    }
    return $sum
}

function Remove-DbFiles {
    param([string]$Base)
    foreach ($item in Get-ChildItem -Path "$Base*" -Force -ErrorAction SilentlyContinue) {
        Remove-Item -LiteralPath $item.FullName -Recurse -Force -ErrorAction SilentlyContinue
    }
}

$withWriter = if ($NoWriter) { 'false' } else { 'true' }
$dbRoot = Join-Path $root $DataDir
$dataPath = $null
if ($Persist) {
    $dataPath = $dbRoot -replace '\\', '/'
    $url = "jdbc:h2:tcp://127.0.0.1:$Port/$dataPath;QUERY_CACHE_SIZE=0"
} else {
    $url = "jdbc:h2:tcp://127.0.0.1:$Port/mem:omap$Rows;DB_CLOSE_DELAY=-1;QUERY_CACHE_SIZE=0"
}
$storage = if ($Persist) { "file ($dataPath)" } else { 'in-memory' }
$totalSteps = if ($Persist) { 5 } else { 4 }

$logDir = Join-Path $env:TEMP ("h2omap_bench_" + [Guid]::NewGuid().ToString('N'))
$serverOut = Join-Path $logDir 'server.out'
$serverErr = Join-Path $logDir 'server.err'
$server = $null

if ($Persist -and (Get-ChildItem -Path "$dbRoot*" -Force -ErrorAction SilentlyContinue)) {
    Remove-DbFiles -Base $dbRoot
}

try {
    if (-not $SkipBuild) {
        Write-Host "[1/$totalSteps] Building h2omap..."
        & mvn -B -q package
        if ($LASTEXITCODE -ne 0) { throw "ERROR: mvn package failed ($LASTEXITCODE)." }
    } else {
        Write-Host "[1/$totalSteps] Build skipped."
    }

    if (-not (Test-Path $MainJar)) { throw "ERROR: $MainJar not found, run without -SkipBuild." }
    if (-not (Test-Path 'target/test-classes')) { throw 'ERROR: target/test-classes not found, run without -SkipBuild.' }

    $h2Jar = Resolve-H2Jar
    New-Item -ItemType Directory -Path $logDir -Force | Out-Null

    Write-Host "[2/$totalSteps] Starting standalone $Engine TCP server (-Xmx$ServerXmx, port=$Port, storage=$storage)..."
    $server = Start-Process -FilePath 'java' `
        -ArgumentList @("-Xmx$ServerXmx", '-cp', $MainJar, 'org.h2.tools.Server',
                        '-tcp', '-tcpPort', $Port, '-ifNotExists') `
        -RedirectStandardOutput $serverOut -RedirectStandardError $serverErr `
        -WorkingDirectory $root -NoNewWindow -PassThru

    Write-Host "[3/$totalSteps] Waiting for TCP port $Port..."
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        if ($server.HasExited) {
            throw "ERROR: TCP server exited (code $($server.ExitCode)):`n$(Get-Content $serverOut, $serverErr -Raw -ErrorAction SilentlyContinue)"
        }
        if (Test-TcpPort -TargetPort $Port) { $ready = $true; break }
        Start-Sleep -Milliseconds 200
    }
    if (-not $ready) {
        throw "ERROR: Timed out waiting for TCP port $Port.`n$(Get-Content $serverOut, $serverErr -Raw -ErrorAction SilentlyContinue)"
    }

    Write-Host "[4/$totalSteps] Running benchmark (rows=$Rows, engine=$Engine, storage=$storage, readers=$Readers, rounds=$Rounds, writer=$withWriter, writer_rps=$WriterRps)..."
    $watch = [System.Diagnostics.Stopwatch]::StartNew()
    & java "-Xmx$ClientXmx" -cp "$h2Jar;target\test-classes" $BenchClass `
        $Readers $Rows $Rounds $withWriter $Engine $url $WriterRps
    $exit = $LASTEXITCODE
    $watch.Stop()
    Write-Host ("Benchmark finished in {0:N1}s (exit code {1})." -f $watch.Elapsed.TotalSeconds, $exit)
    if ($exit -ne 0) { throw "ERROR: benchmark client failed ($exit)." }

    if ($Persist) {
        Write-Host "[5/$totalSteps] Verifying persisted rows after restart..."
        Invoke-ShellSql -Jar $h2Jar -Url $url -Sql 'SHUTDOWN' | Out-Null
        if ($server -and -not $server.HasExited) {
            Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
            $server.WaitForExit(5000) | Out-Null
        }
        $server = $null

        $localUrl = "jdbc:h2:$dataPath"
        $count = Get-PersistedRowCount -Jar $MainJar -LocalUrl $localUrl
        $sizeMb = [math]::Round((Get-DirectorySize -Base $dbRoot) / 1MB, 1)
        if ($count -ne $Rows) {
            throw "ERROR: recovered row count $count != expected $Rows (data dir $sizeMb MB kept at $($dataPath))."
        }
        Write-Host "Recovered rows=$count (expected $Rows), data dir $sizeMb MB."
    }
} catch {
    Write-Host "SERVER STDOUT:`n$(Get-Content $serverOut -Raw -ErrorAction SilentlyContinue)"
    Write-Host "SERVER STDERR:`n$(Get-Content $serverErr -Raw -ErrorAction SilentlyContinue)"
    throw
} finally {
    if ($server -and -not $server.HasExited) {
        Stop-Process -Id $server.Id -Force -ErrorAction SilentlyContinue
        $server.WaitForExit(5000) | Out-Null
    }
    if ($Persist -and -not $KeepData -and (Get-ChildItem -Path "$dbRoot*" -Force -ErrorAction SilentlyContinue)) {
        Remove-DbFiles -Base $dbRoot
    }
    Remove-Item -Recurse -Force $logDir -ErrorAction SilentlyContinue
}

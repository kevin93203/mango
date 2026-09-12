param(
    [Parameter(Mandatory = $true)]
    [string]$ArchivePath
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

$archive = [System.IO.Path]::GetFullPath($ArchivePath)
if (-not (Test-Path -LiteralPath $archive -PathType Leaf)) {
    throw "Release archive is missing: $archive"
}
$archiveName = [System.IO.Path]::GetFileName($archive)
$archiveBase = if ($archiveName.EndsWith(".tar.gz", [System.StringComparison]::OrdinalIgnoreCase)) { $archiveName.Substring(0, $archiveName.Length - 7) } else { [System.IO.Path]::GetFileNameWithoutExtension($archiveName) }
$checksumPath = Join-Path ([System.IO.Path]::GetDirectoryName($archive)) "$archiveBase.sha256"
if (-not (Test-Path -LiteralPath $checksumPath -PathType Leaf)) {
    throw "Release archive checksum is missing: $checksumPath"
}
$expectedArchiveHash = ((Get-Content -LiteralPath $checksumPath -Raw).Trim() -split "\s+")[0].ToLowerInvariant()
$actualArchiveHash = (Get-FileHash -LiteralPath $archive -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actualArchiveHash -ne $expectedArchiveHash) {
    throw "release archive checksum mismatch"
}

$root = Join-Path ([System.IO.Path]::GetTempPath()) ("mango-release-smoke-" + [Guid]::NewGuid().ToString("N"))
$extractRoot = Join-Path $root "package"
$homeRoot = Join-Path $root "home"
$probeRoot = Join-Path $root "outside-cwd"
New-Item -ItemType Directory -Force -Path $extractRoot, $homeRoot, $probeRoot | Out-Null
$previousHome = $env:MANGO_HOME
$daemonStarted = $false

try {
    if ($archive.EndsWith(".zip", [System.StringComparison]::OrdinalIgnoreCase)) {
        Expand-Archive -LiteralPath $archive -DestinationPath $extractRoot
    } else {
        tar -xzf $archive -C $extractRoot
    }

    $extension = if ($IsWindows) { ".exe" } else { "" }
    $required = @("mango$extension", "mangod$extension", "mango-shim$extension", "manifest.json", "README.md", "mango.example.yaml")
    foreach ($name in $required) {
        $path = Join-Path $extractRoot $name
        if (-not (Test-Path -LiteralPath $path -PathType Leaf)) {
            throw "Packaged file is missing: $name"
        }
    }

    $manifestText = Get-Content -LiteralPath (Join-Path $extractRoot "manifest.json") -Raw
    $manifest = $manifestText | ConvertFrom-Json
    $buildDateMatch = [regex]::Match($manifestText, '"build_date"\s*:\s*"([^"]+)"')
    if (-not $buildDateMatch.Success) {
        throw "manifest build date is missing"
    }
    $manifestBuildDate = $buildDateMatch.Groups[1].Value
    if ($manifest.compatibility.ipc_version -ne 3 -or $manifest.compatibility.shim_protocol_version -ne 3 -or $manifest.compatibility.bootstrap_schema_version -ne 2 -or $manifest.compatibility.metadata_schema_version -ne 11 -or $manifest.compatibility.yaml_schema_version -ne 4) {
        throw "manifest compatibility metadata does not match the supported release contract"
    }
    foreach ($binary in $manifest.binaries) {
        $binaryPath = Join-Path $extractRoot $binary.name
        $actualHash = (Get-FileHash -LiteralPath $binaryPath -Algorithm SHA256).Hash.ToLowerInvariant()
        if ($actualHash -ne $binary.sha256.ToLowerInvariant()) {
            throw "binary checksum mismatch: $($binary.name)"
        }
    }

    if ($manifest.binaries.Count -ne 3) {
        throw "manifest must contain exactly the CLI, daemon, and shim binaries"
    }

    $cli = Join-Path $extractRoot "mango$extension"
    $daemon = Join-Path $extractRoot "mangod$extension"
    $shim = Join-Path $extractRoot "mango-shim$extension"
    foreach ($binary in @($cli, $daemon, $shim)) {
        $output = & $binary --version 2>&1
        $versionOutput = $output -join "`n"
        if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace($versionOutput)) {
            throw "binary --version failed: $binary"
        }
        foreach ($metadata in @("$($manifest.build.version)", "commit=$($manifest.build.commit)", "build_date=$manifestBuildDate")) {
            if (-not $versionOutput.Contains($metadata)) {
                throw "binary metadata mismatch in ${binary}: expected $metadata"
            }
        }
    }

    $helpOutput = & $cli --help 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "packaged CLI help failed: $($helpOutput -join "`n")"
    }
    $helpText = $helpOutput -join "`n"
    foreach ($removedCommand in @("ps", "ls", "execution", "history", "enable", "disable")) {
        $pattern = '(?m)^\s+' + [regex]::Escape($removedCommand) + '(?:\s|$)'
        if ([regex]::IsMatch($helpText, $pattern)) {
            throw "packaged root help exposes removed command: $removedCommand"
        }
    }
    $completionOutput = & $cli completion powershell 2>&1
    if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($completionOutput -join "`n"))) {
        throw "packaged PowerShell completion failed"
    }

    $env:MANGO_HOME = $homeRoot
    Push-Location $probeRoot
    try {
        $doctorOutput = & $cli doctor --json 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "mango doctor --json failed: $($doctorOutput -join "`n")"
        }
        $doctor = ($doctorOutput -join "`n") | ConvertFrom-Json
        $resolvedDaemon = [string]$doctor.daemon_executable
        if ([System.IO.Path]::GetFullPath($resolvedDaemon) -ne [System.IO.Path]::GetFullPath($daemon)) {
            throw "CLI did not resolve the sibling daemon from the package directory: $resolvedDaemon"
        }
        & $cli config validate (Join-Path $extractRoot "mango.example.yaml") 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "packaged YAML v4 example failed validation"
        }

        & $cli daemon start 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "packaged daemon failed to start"
        }
        $daemonStarted = $true
        $healthOutput = & $cli doctor --json 2>&1
        if ($LASTEXITCODE -ne 0) {
            throw "packaged daemon health check failed: $($healthOutput -join "`n")"
        }
        $health = ($healthOutput -join "`n") | ConvertFrom-Json
        if ([string]$health.daemon.status -notin @("ok", "degraded")) {
            throw "packaged daemon did not report a usable health state"
        }
        if ([string]$health.daemon.build.version -eq "") {
            throw "daemon health did not expose build metadata"
        }
        if ([string]$health.database.migration.status -notin @("completed", "complete")) {
            throw "daemon health did not report a completed metadata migration"
        }
        & $cli service list --json 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "packaged canonical service list failed"
        }
        & $cli runs list --json 2>&1 | Out-Null
        if ($LASTEXITCODE -ne 0) {
            throw "packaged canonical runs list failed"
        }
    } finally {
        Pop-Location
    }
} finally {
    if ($daemonStarted) {
        try { & $cli daemon stop 2>&1 | Out-Null } catch { }
    }
    if ($null -eq $previousHome) {
        Remove-Item Env:MANGO_HOME -ErrorAction SilentlyContinue
    } else {
        $env:MANGO_HOME = $previousHome
    }
    if (Test-Path -LiteralPath $root) {
        Remove-Item -LiteralPath $root -Recurse -Force
    }
}

Write-Output "release smoke test passed: $archive"

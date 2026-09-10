param(
    [Parameter(Mandatory = $true)]
    [string]$BinaryDirectory,

    [Parameter(Mandatory = $true)]
    [string]$OutputDirectory,

    [Parameter(Mandatory = $true)]
    [string]$Version,

    [Parameter(Mandatory = $true)]
    [string]$Commit,

    [Parameter(Mandatory = $true)]
    [string]$BuildDate,

    [string]$RepositoryRoot = (Resolve-Path (Join-Path $PSScriptRoot ".."))
)

$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

if ($IsWindows) {
    $osName = "windows"
} elseif ($IsMacOS) {
    $osName = "macos"
} elseif ($IsLinux) {
    $osName = "linux"
} else {
    throw "Unsupported release operating system"
}

$runnerArchitecture = if ($env:RUNNER_ARCH) { $env:RUNNER_ARCH } else { [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() }
$architecture = switch ($runnerArchitecture.ToUpperInvariant()) {
    "X64" { "amd64"; break }
    "AMD64" { "amd64"; break }
    "ARM64" { "arm64"; break }
    default { throw "Unsupported release architecture: $runnerArchitecture" }
}

$extension = if ($osName -eq "windows") { ".exe" } else { "" }
$binaryNames = @("mango$extension", "mangod$extension", "mango-shim$extension")
$binaryRoot = [System.IO.Path]::GetFullPath($BinaryDirectory)
$outputRoot = [System.IO.Path]::GetFullPath($OutputDirectory)
$repoRoot = [System.IO.Path]::GetFullPath($RepositoryRoot)

foreach ($name in $binaryNames) {
    $source = Join-Path $binaryRoot $name
    if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
        throw "Release binary is missing: $source"
    }
}
foreach ($required in @("README.md", "mango.example.yaml")) {
    if (-not (Test-Path -LiteralPath (Join-Path $repoRoot $required) -PathType Leaf)) {
        throw "Release package input is missing: $required"
    }
}

New-Item -ItemType Directory -Force -Path $outputRoot | Out-Null
$stageRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mango-release-" + [Guid]::NewGuid().ToString("N"))
$packageRoot = Join-Path $stageRoot "package"
New-Item -ItemType Directory -Force -Path $packageRoot | Out-Null

try {
    foreach ($name in $binaryNames) {
        Copy-Item -LiteralPath (Join-Path $binaryRoot $name) -Destination (Join-Path $packageRoot $name)
    }
    Copy-Item -LiteralPath (Join-Path $repoRoot "README.md") -Destination (Join-Path $packageRoot "README.md")
    Copy-Item -LiteralPath (Join-Path $repoRoot "mango.example.yaml") -Destination (Join-Path $packageRoot "mango.example.yaml")

    $binaryManifest = @(
        foreach ($name in $binaryNames) {
            $hash = (Get-FileHash -LiteralPath (Join-Path $packageRoot $name) -Algorithm SHA256).Hash.ToLowerInvariant()
            [ordered]@{ name = $name; sha256 = $hash }
        }
    )
    $manifest = [ordered]@{
        format_version = 1
        build = [ordered]@{ version = $Version; commit = $Commit; build_date = $BuildDate }
        target = [ordered]@{ os = $osName; architecture = $architecture }
        compatibility = [ordered]@{ ipc_version = 3; shim_protocol_version = 3; bootstrap_schema_version = 2; metadata_schema_version = 11; yaml_schema_version = 4 }
        binaries = $binaryManifest
    }
    $json = $manifest | ConvertTo-Json -Depth 8
    [System.IO.File]::WriteAllText((Join-Path $packageRoot "manifest.json"), $json + [Environment]::NewLine, [System.Text.UTF8Encoding]::new($false))

    $archiveBase = "mango-$osName-$architecture"
    if ($osName -eq "windows") {
        $archivePath = Join-Path $outputRoot "$archiveBase.zip"
        Compress-Archive -Path (Join-Path $packageRoot "*") -DestinationPath $archivePath -CompressionLevel Optimal -Force
    } else {
        $archivePath = Join-Path $outputRoot "$archiveBase.tar.gz"
        tar -czf $archivePath -C $packageRoot .
    }
    $archiveHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
    [System.IO.File]::WriteAllText((Join-Path $outputRoot "$archiveBase.sha256"), "$archiveHash  $([System.IO.Path]::GetFileName($archivePath))$([Environment]::NewLine)", [System.Text.UTF8Encoding]::new($false))
    Write-Output $archivePath
} finally {
    if (Test-Path -LiteralPath $stageRoot) {
        Remove-Item -LiteralPath $stageRoot -Recurse -Force
    }
}

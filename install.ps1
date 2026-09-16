# Install the Mango CLI, daemon, and shim from the latest GitHub release.
#
# Usage from PowerShell:
#   irm https://raw.githubusercontent.com/kevin93203/mango/main/install.ps1 | iex
#
# Optional environment variables:
#   $env:MANGO_VERSION = "v0.1.0"
#   $env:MANGO_REPO = "kevin93203/mango"
#   $env:MANGO_INSTALL_DIR = "$HOME\.local\bin"
#   $env:MANGO_NO_MODIFY_PATH = "1"

$ErrorActionPreference = "Stop"

function Test-MangoTruthy([string]$Value) {
    return $Value -match '^(1|true|yes|on)$'
}

function Get-MangoHash([string]$Path) {
    return (Get-FileHash -LiteralPath $Path -Algorithm SHA256).Hash.ToLowerInvariant()
}

if ($env:OS -ne "Windows_NT") {
    throw "this installer is for Windows; use install.sh on Linux or macOS"
}

$repository = if ([string]::IsNullOrWhiteSpace($env:MANGO_REPO)) {
    "kevin93203/mango"
} else {
    $env:MANGO_REPO
}
$version = if ([string]::IsNullOrWhiteSpace($env:MANGO_VERSION)) {
    "latest"
} else {
    $env:MANGO_VERSION
}
if ($version -ne "latest") {
    if ($version.StartsWith("V", [System.StringComparison]::Ordinal)) {
        $version = "v" + $version.Substring(1)
    } elseif (-not $version.StartsWith("v", [System.StringComparison]::Ordinal)) {
        $version = "v$version"
    }
}

$processor = if (-not [string]::IsNullOrWhiteSpace($env:PROCESSOR_ARCHITEW6432)) {
    $env:PROCESSOR_ARCHITEW6432
} else {
    $env:PROCESSOR_ARCHITECTURE
}
$architecture = switch ($processor.ToUpperInvariant()) {
    "AMD64" { "amd64"; break }
    "ARM64" { "arm64"; break }
    default { throw "unsupported Windows architecture: $processor" }
}

$releaseBase = if ($version -eq "latest") {
    "https://github.com/$repository/releases/latest/download"
} else {
    "https://github.com/$repository/releases/download/$version"
}
$archiveBase = "mango-windows-$architecture"
$archive = "$archiveBase.zip"
$temporaryRoot = Join-Path ([System.IO.Path]::GetTempPath()) ("mango-install-" + [Guid]::NewGuid().ToString("N"))
$archivePath = Join-Path $temporaryRoot $archive
$checksumPath = Join-Path $temporaryRoot "$archiveBase.sha256"
$packagePath = Join-Path $temporaryRoot "package"

New-Item -ItemType Directory -Force -Path $packagePath | Out-Null
try {
    Write-Host "mango installer: downloading $archive ($version)"
    Invoke-WebRequest -Uri "$releaseBase/$archive" -OutFile $archivePath -UseBasicParsing
    Invoke-WebRequest -Uri "$releaseBase/$archiveBase.sha256" -OutFile $checksumPath -UseBasicParsing

    $expectedHash = ((Get-Content -LiteralPath $checksumPath -Raw).Trim() -split "\s+")[0].ToLowerInvariant()
    if ($expectedHash -notmatch '^[0-9a-f]{64}$') {
        throw "release checksum is invalid"
    }
    $actualHash = Get-MangoHash $archivePath
    if ($actualHash -ne $expectedHash) {
        throw "release checksum mismatch"
    }
    Write-Host "mango installer: release checksum verified"

    Expand-Archive -LiteralPath $archivePath -DestinationPath $packagePath -Force
    foreach ($binary in @("mango.exe", "mangod.exe", "mango-shim.exe")) {
        $binaryPath = Join-Path $packagePath $binary
        if (-not (Test-Path -LiteralPath $binaryPath -PathType Leaf)) {
            throw "release package is missing $binary"
        }
    }
    if (-not (Test-Path -LiteralPath (Join-Path $packagePath "manifest.json") -PathType Leaf)) {
        throw "release package is missing manifest.json"
    }

    $installDirectory = if ([string]::IsNullOrWhiteSpace($env:MANGO_INSTALL_DIR)) {
        Join-Path $HOME ".local\bin"
    } else {
        $env:MANGO_INSTALL_DIR
    }
    $installDirectory = [System.IO.Path]::GetFullPath($installDirectory)
    New-Item -ItemType Directory -Force -Path $installDirectory | Out-Null

    foreach ($binary in @("mango.exe", "mangod.exe", "mango-shim.exe")) {
        $source = Join-Path $packagePath $binary
        $destination = Join-Path $installDirectory $binary
        $staged = Join-Path $installDirectory (".$binary." + [Guid]::NewGuid().ToString("N"))
        Copy-Item -LiteralPath $source -Destination $staged -Force
        Move-Item -LiteralPath $staged -Destination $destination -Force
    }

    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    $pathEntries = if ([string]::IsNullOrWhiteSpace($userPath)) {
        @()
    } else {
        @($userPath -split ';' | Where-Object { $_ -ne "" })
    }
    $pathAlreadyContainsMango = $false
    foreach ($entry in $pathEntries) {
        try {
            $entryFull = [System.IO.Path]::GetFullPath([Environment]::ExpandEnvironmentVariables($entry)).TrimEnd('\')
            $installFull = $installDirectory.TrimEnd('\')
            if ($entryFull -ieq $installFull) {
                $pathAlreadyContainsMango = $true
                break
            }
        } catch {
            if ($entry.TrimEnd('\') -ieq $installDirectory.TrimEnd('\')) {
                $pathAlreadyContainsMango = $true
                break
            }
        }
    }

    $noModifyPath = Test-MangoTruthy $env:MANGO_NO_MODIFY_PATH
    if (-not $pathAlreadyContainsMango) {
        if ($noModifyPath) {
            Write-Host "mango installer: PATH was not modified (MANGO_NO_MODIFY_PATH is set)"
        } else {
            $newUserPath = if ([string]::IsNullOrWhiteSpace($userPath)) {
                $installDirectory
            } else {
                "$userPath;$installDirectory"
            }
            [Environment]::SetEnvironmentVariable("Path", $newUserPath, "User")
            Write-Host "mango installer: added $installDirectory to the user PATH"
        }
    }

    # Make the command available to this PowerShell process as well. New
    # terminals pick up the persisted user PATH automatically.
    if (-not (($env:Path -split ';') -contains $installDirectory)) {
        $env:Path = "$installDirectory;$env:Path"
    }

    Write-Host "mango installer: installed Mango in $installDirectory"
    & (Join-Path $installDirectory "mango.exe") --version
    Write-Host "mango installer: run 'mango init' to create a configuration, or 'mango --help' to get started"
} finally {
    if (Test-Path -LiteralPath $temporaryRoot) {
        Remove-Item -LiteralPath $temporaryRoot -Recurse -Force
    }
}

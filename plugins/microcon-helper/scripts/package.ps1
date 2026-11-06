# SPDX-License-Identifier: MIT

$ErrorActionPreference = "Stop"

$PluginRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$ManifestPath = Join-Path $PluginRoot "plugin.json"
$Manifest = Get-Content -Raw $ManifestPath | ConvertFrom-Json

if ($Manifest.id -notmatch "^[a-z0-9][a-z0-9.-]*$") {
    throw "Invalid plugin id"
}
if ($Manifest.version -notmatch "^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$") {
    throw "Invalid plugin version"
}

$BinaryName = "oxideterm-microcon-helper"
$BinaryPath = Join-Path $PluginRoot "bin/$BinaryName.exe"
if (-not (Test-Path $BinaryPath)) {
    throw "Missing compiled binary $BinaryPath; run: cargo build --release --locked"
}

$PackageTarget = if ($env:PACKAGE_TARGET) { $env:PACKAGE_TARGET } else { "x86_64-pc-windows-msvc" }
$PackageIdentity = "$($Manifest.id)-$($Manifest.version)-$PackageTarget"
$DistDirectory = Join-Path $PluginRoot "dist"
$ArtifactPath = Join-Path $DistDirectory "$PackageIdentity.zip"
$StagingDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ([System.IO.Path]::GetRandomFileName())

New-Item -ItemType Directory -Force $DistDirectory | Out-Null
New-Item -ItemType Directory -Force $StagingDirectory | Out-Null
New-Item -ItemType Directory -Force (Join-Path $StagingDirectory "bin") | Out-Null

try {
    foreach ($name in @("plugin", "plugin.cmd", "$BinaryName.exe")) {
        Copy-Item (Join-Path $PluginRoot "bin/$name") (Join-Path $StagingDirectory "bin")
    }
    Copy-Item (Join-Path $PluginRoot "locales") $StagingDirectory -Recurse

    # Windows packages point the manifest entry at the cmd.exe launcher.
    $Manifest.runtime.entry = "bin/plugin.cmd"
    $ManifestJson = $Manifest | ConvertTo-Json -Depth 100
    $Utf8WithoutBom = New-Object System.Text.UTF8Encoding($false)
    [System.IO.File]::WriteAllText(
        (Join-Path $StagingDirectory "plugin.json"),
        $ManifestJson,
        $Utf8WithoutBom
    )

    if (Test-Path $ArtifactPath) {
        Remove-Item $ArtifactPath
    }
    Compress-Archive -Path (Join-Path $StagingDirectory "*") -DestinationPath $ArtifactPath

    $PackageDigest = (Get-FileHash -Algorithm SHA256 $ArtifactPath).Hash.ToLowerInvariant()
    $PackageSize = (Get-Item $ArtifactPath).Length
    Write-Output "Artifact: $ArtifactPath"
    Write-Output "SHA-256: $PackageDigest"
    Write-Output "Size: $PackageSize bytes"
}
finally {
    Remove-Item $StagingDirectory -Recurse -Force
}

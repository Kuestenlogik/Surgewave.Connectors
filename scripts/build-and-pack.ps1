#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Builds all Surgewave Connectors and creates NuGet packages.

.PARAMETER Configuration
    Build configuration (default: Release)

.PARAMETER Clean
    Clean before building

.PARAMETER SkipTests
    Skip running tests

.EXAMPLE
    .\scripts\build-and-pack.ps1

.EXAMPLE
    .\scripts\build-and-pack.ps1 -Clean -SkipTests
#>

[CmdletBinding()]
param(
    [string]$Configuration = "Release",
    [switch]$Clean,
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$ProjectRoot = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$Solution = Join-Path $ProjectRoot "Kuestenlogik.Surgewave.Connectors.slnx"
$PackagesDir = Join-Path $ProjectRoot "artifacts\packages"

Write-Host ""
Write-Host "  Surgewave Connectors — Build & Pack" -ForegroundColor Cyan
Write-Host ""

# Clean
if ($Clean) {
    Write-Host "[1/4] Cleaning..." -ForegroundColor Yellow
    dotnet clean $Solution -c $Configuration --nologo -v quiet 2>&1 | Out-Null
    if (Test-Path $PackagesDir) { Remove-Item $PackagesDir -Recurse -Force }
    Write-Host "  Clean" -ForegroundColor Green
} else {
    Write-Host "[1/4] Skipping clean" -ForegroundColor Gray
}

# Restore
Write-Host "[2/4] Restoring..." -ForegroundColor Yellow
dotnet restore $Solution --nologo -v quiet
if ($LASTEXITCODE -ne 0) { Write-Host "  Restore failed!" -ForegroundColor Red; exit 1 }
Write-Host "  Restored" -ForegroundColor Green

# Build
Write-Host "[3/4] Building ($Configuration)..." -ForegroundColor Yellow
dotnet build $Solution -c $Configuration --no-restore --nologo
if ($LASTEXITCODE -ne 0) { Write-Host "  Build failed!" -ForegroundColor Red; exit 1 }
Write-Host "  Built" -ForegroundColor Green

# Test
if (!$SkipTests) {
    Write-Host "[3b]  Running tests..." -ForegroundColor Yellow
    dotnet test $Solution -c $Configuration --no-build --nologo -v quiet
    if ($LASTEXITCODE -ne 0) { Write-Host "  Tests failed!" -ForegroundColor Red; exit 1 }
    Write-Host "  Tests passed" -ForegroundColor Green
}

# Pack NuGet packages
Write-Host "[4/5] Packing NuGet packages..." -ForegroundColor Yellow
dotnet pack $Solution -c $Configuration --no-build --nologo
if ($LASTEXITCODE -ne 0) { Write-Host "  Pack failed!" -ForegroundColor Red; exit 1 }

$count = (Get-ChildItem $PackagesDir -Filter "*.nupkg" -ErrorAction SilentlyContinue).Count
Write-Host "  $count packages created in artifacts/packages/" -ForegroundColor Green

# Pack .swpkg plugins via the SurgewavePackPlugin MSBuild target. Each connector csproj
# inherits PackageReference Kuestenlogik.Surgewave.Build (PrivateAssets=all) plus Content includes
# for plugin.json + pluginsettings.json from Directory.Build.props, so a single
# 'dotnet publish -p:SurgewavePackPlugin=true' run packages every connector that has a
# plugin.json next to its csproj.
Write-Host "[5/5] Packing .swpkg plugins..." -ForegroundColor Yellow
$PluginPackageDir = Join-Path $ProjectRoot "artifacts\pluginPackage"
if (Test-Path $PluginPackageDir) { Remove-Item $PluginPackageDir -Recurse -Force }
$connectorProjects = Get-ChildItem -Path (Join-Path $ProjectRoot "src") -Recurse -Filter "Kuestenlogik.Surgewave.Connector.*.csproj"
$packed = 0
foreach ($proj in $connectorProjects) {
    if (!(Test-Path (Join-Path $proj.Directory.FullName "plugin.json"))) { continue }
    dotnet publish $proj.FullName -c $Configuration --no-build -p:SurgewavePackPlugin=true --nologo -v quiet 2>&1 | Out-Null
    if ($LASTEXITCODE -eq 0) { $packed++ }
}
$pluginPackageCount = (Get-ChildItem $PluginPackageDir -Filter "*.swpkg" -ErrorAction SilentlyContinue).Count
Write-Host "  $pluginPackageCount .swpkg packages created in artifacts/pluginPackage/" -ForegroundColor Green

Write-Host ""
Write-Host "  Done!" -ForegroundColor Green
Write-Host "  NuGet packages: $PackagesDir" -ForegroundColor Gray
Write-Host "  Plugin packages: $PluginPackageDir" -ForegroundColor Gray
Write-Host ""

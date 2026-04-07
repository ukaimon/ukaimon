param(
    [Parameter(Mandatory = $true)]
    [string]$DestinationPath,
    [switch]$BuildWheelhouse
)

$ErrorActionPreference = "Stop"
$root = $PSScriptRoot
$projectName = Split-Path -Leaf $root
if (-not (Test-Path $DestinationPath)) {
    New-Item -ItemType Directory -Force -Path $DestinationPath | Out-Null
}
$resolvedDestination = Resolve-Path -Path $DestinationPath
$targetRoot = Join-Path $resolvedDestination $projectName

if ($BuildWheelhouse) {
    $venvPython = Join-Path $root ".venv\Scripts\python.exe"
    if (-not (Test-Path $venvPython)) {
        throw ".venv が見つかりません。先にこの PC で一度セットアップしてください。"
    }
    $wheelhouse = Join-Path $root "vendor\wheels"
    New-Item -ItemType Directory -Force -Path $wheelhouse | Out-Null
    & $venvPython -m pip download -r (Join-Path $root "requirements.txt") -d $wheelhouse
}

New-Item -ItemType Directory -Force -Path $targetRoot | Out-Null

$excludeDirectories = @(
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    "logs"
)
$excludeFiles = @(
    "*.pyc",
    "*.pyo",
    "local_config.json"
)

$robocopyArgs = @(
    $root,
    $targetRoot,
    "/E",
    "/R:1",
    "/W:1",
    "/NFL",
    "/NDL",
    "/NJH",
    "/NJS",
    "/NP"
)

if ($excludeDirectories.Count -gt 0) {
    $robocopyArgs += "/XD"
    $robocopyArgs += $excludeDirectories
}
if ($excludeFiles.Count -gt 0) {
    $robocopyArgs += "/XF"
    $robocopyArgs += $excludeFiles
}

& robocopy @robocopyArgs | Out-Null
if ($LASTEXITCODE -gt 7) {
    throw "USB 配布用コピーに失敗しました。robocopy code: $LASTEXITCODE"
}

Write-Host "USB 配布用コピーを作成しました: $targetRoot"
Write-Host "別 PC では start_app.bat を実行してください。"
if (-not (Test-Path (Join-Path $root "vendor\wheels"))) {
    Write-Host "オフラインで使う場合は -BuildWheelhouse を付けて再実行してください。"
}

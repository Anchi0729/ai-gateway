param(
    [string]$Version = "0.3.10"
)

$projectRoot = Split-Path -Parent $PSScriptRoot
$releaseRoot = Join-Path $projectRoot "release"
$portableName = "AI Gateway_${Version}_portable_x64"
$portableDirectory = Join-Path $releaseRoot $portableName
$portableZip = Join-Path $releaseRoot "$portableName.zip"
# 便携版必须与 NSIS 使用同一次显式 GNU/MSVCRT Release 构建，避免误装宿主 target\release 的不同制式二进制。
$tauriRelease = Join-Path $projectRoot "src-tauri\target\msvcrt-build\x86_64-pc-windows-gnu\release"
$application = Join-Path $tauriRelease "ai-gateway.exe"
$webViewLoader = Join-Path $tauriRelease "WebView2Loader.dll"
$portableGuide = Join-Path $PSScriptRoot "便携版说明.txt"

foreach ($requiredFile in @($application, $webViewLoader, $portableGuide)) {
    if (-not (Test-Path -LiteralPath $requiredFile)) {
        throw "缺少便携版构建文件：$requiredFile"
    }
}

New-Item -ItemType Directory -Path $releaseRoot -Force | Out-Null
if (Test-Path -LiteralPath $portableDirectory) {
    Remove-Item -LiteralPath $portableDirectory -Recurse -Force
}
if (Test-Path -LiteralPath $portableZip) {
    Remove-Item -LiteralPath $portableZip -Force
}
New-Item -ItemType Directory -Path $portableDirectory | Out-Null

# 便携包只放运行所需二进制和说明，不携带 AppData、数据库或任何凭据。
Copy-Item -LiteralPath $application -Destination (Join-Path $portableDirectory "AI Gateway.exe")
Copy-Item -LiteralPath $webViewLoader -Destination $portableDirectory
Copy-Item -LiteralPath $portableGuide -Destination $portableDirectory
Compress-Archive -Path (Join-Path $portableDirectory "*") -DestinationPath $portableZip -CompressionLevel Optimal

Get-FileHash -Algorithm SHA256 -LiteralPath $portableZip

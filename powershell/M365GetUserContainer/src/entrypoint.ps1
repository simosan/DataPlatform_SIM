param (
    [string]$S3_BUCKET = "m365-dwh",
    [string]$SCRIPT_KEY = "scripts/M365GetUserDocker.ps1",
    [string]$CommandInput  # aws run-taskから渡される引数
)

Write-Host "=== Downloading script from S3 ==="
$localPath = "/tmp/M365GetUserDocker.ps1"

try {
    Read-S3Object -BucketName $S3_BUCKET -Key $SCRIPT_KEY -File $localPath -ErrorAction Stop
    Write-Host "Downloaded: $S3_BUCKET/$SCRIPT_KEY"
}
catch {
    Write-Error "Failed to download script from S3: $_"
    exit 1
}

Write-Host "=== Executing script ==="
if ($CommandInput) {
    Write-Host "Passing argument to script: $CommandInput"
    & $localPath -CommandInput $CommandInput
} else {
    & $localPath
}

param (
    [string]$S3_BUCKET = "m365-dwh",
    [string]$SCRIPT_KEY = "scripts/M365GetUserDocker.ps1",
    [string]$CommandInput  # aws run-taskから渡される引数
)

# CloudWatch Logs で ANSI エスケープコードが文字化け表示されるのを防ぐ
# (PowerShell 7+ のみ有効。古い場合は単に無視される)
try {
    if ($PSStyle -and $PSStyle.OutputRendering) {
        $PSStyle.OutputRendering = 'PlainText'
    }
} catch {
    # ignore
}

# 進捗バー等の制御文字出力を抑止
$ProgressPreference = 'SilentlyContinue'

Write-Host "=== Downloading script from S3 ==="
$localPath = "/tmp/M365GetUserDocker.ps1"

try {
    # Read-S3Object の出力が CloudWatch で表形式+ANSI色付きになって見づらいので、全ストリーム出力を抑止
    Read-S3Object -BucketName $S3_BUCKET -Key $SCRIPT_KEY -File $localPath -ErrorAction Stop *> $null
    Write-Host "Downloaded: $S3_BUCKET/$SCRIPT_KEY"
}
catch {
    Write-Error "Failed to download script from S3: $_"
    exit 1
}

Write-Host "=== Executing script ==="
try {
    # Step Functions からは環境変数 GROUP が渡される前提。
    # 引数(CommandInput)は使用せず、常に環境変数から引数JSONを作って渡す。
    if ([string]::IsNullOrWhiteSpace($env:GROUP)) {
        Write-Error "GROUP environment variable is empty."
        exit 1
    }
    $inputpath = [PSCustomObject]@{
        group = $env:GROUP
    }
    $inputJson = $inputpath | ConvertTo-Json -Compress
    Write-Host "[Debug]-[entrypoint] GROUP='$($env:GROUP)'"
    Write-Host "[Debug]-[entrypoint] CommandInput=$inputJson"
    $rtnmsg = & $localPath -CommandInput $inputJson
} catch {
    Write-Error "Script threw an exception: $_"
    exit 1
}

# 呼び出し先スクリプトは {"status":"success"|"failed"} をJSON文字列で返す想定
try {
    $result = $rtnmsg | ConvertFrom-Json
    $status = $result.status
} catch {
    Write-Error "Failed to parse script result as JSON. Raw result: $rtnmsg"
    exit 1
}

if ($status -eq "failed") {
    Write-Host "Script execution failed with status: $status"
    exit 1
}

exit 0

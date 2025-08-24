param(
    [string]$LambdaInput
)

Set-Location -Path "/var/task"
$PSStyle.OutputRendering = "PlainText"

Write-Output "Lambda started with input: $LambdaInput"

# 本体スクリプト呼び出し
. /var/task/M365GetUserDocker.ps1

# LambdaInput を渡して関数を実行
M365GetUserDocker -LambdaInput $LambdaInput -LambdaContext $null
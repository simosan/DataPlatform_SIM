<#
Step Functions（vpc配下）反映用コマンド - PowerShell版

実行例:
  pwsh ./stepfunctions.ps1

必要:
  - AWS CLI v2
  - プロファイル/権限（PROFILEのIAM権限）
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$Profile = ""
$Region  = 'ap-northeast-1'

$DefaultType              = 'STANDARD'
$DefaultRoleArn           = 'arn:aws:iam::XXXXXXXXXX:role/m365-stepfunctions-common-role'
$DefaultLogGroupArn       = 'arn:aws:logs:ap-northeast-1:XXXXXXXXXX:log-group:/aws/states/m365/m365StateMachine-Logs:*'
$DefaultLogLevel          = 'ALL'
$DefaultIncludeExecData   = 'false'
$DefaultTracingEnabled    = 'false'

function Update-StateMachine {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][string]$DefinitionFile
  )

  $query = "stateMachines[?name=='$Name'].stateMachineArn | [0]"

  $arn = (aws stepfunctions list-state-machines `
      --profile $Profile `
      --region $Region `
      --query $query `
      --output text).Trim()

  if ([string]::IsNullOrWhiteSpace($arn) -or $arn -eq 'None') {
    Write-Host "[CREATE] $Name ($DefinitionFile)"

    $loggingConfiguration = "level=$DefaultLogLevel,includeExecutionData=$DefaultIncludeExecData,destinations=[{cloudWatchLogsLogGroup={logGroupArn=$DefaultLogGroupArn}}]"
    $tracingConfiguration = "enabled=$DefaultTracingEnabled"

    aws stepfunctions create-state-machine `
      --profile $Profile `
      --region $Region `
      --name $Name `
      --type $DefaultType `
      --role-arn $DefaultRoleArn `
      --definition "file://$DefinitionFile" `
      --logging-configuration $loggingConfiguration `
      --tracing-configuration $tracingConfiguration |
      Out-Null

    return
  }

  Write-Host "[UPDATE] $Name -> $arn ($DefinitionFile)"

  aws stepfunctions update-state-machine `
    --profile $Profile `
    --region $Region `
    --state-machine-arn $arn `
    --definition "file://$DefinitionFile" |
    Out-Null
}

# ---- 反映対象（ファイル名＝ステートマシン名の想定） ----
Update-StateMachine -Name 'm365-1-daily' -DefinitionFile 'm365-1-daily.json'
Update-StateMachine -Name 'm365-2-1-UpdateBasedatetime' -DefinitionFile 'm365-2-1-UpdateBasedatetime.json'
Update-StateMachine -Name 'm365-2-2-group1' -DefinitionFile 'm365-2-2-group1.json'
Update-StateMachine -Name 'm365-2-3-UpdateDitionary' -DefinitionFile 'm365-2-3-UpdateDitionary.json'
Update-StateMachine -Name 'm365-3-1-ColConvDomain-group1' -DefinitionFile 'm365-3-1-ColConvDomain-group1.json'
Update-StateMachine -Name 'm365-3-2-UpdateCatalog' -DefinitionFile 'm365-3-2-UpdateCatalog.json'
Update-StateMachine -Name 'm365-3-3-CheckDataQuality' -DefinitionFile 'm365-3-3-CheckDataQuality.json'
Update-StateMachine -Name 'm365-3-3-UpdateType' -DefinitionFile 'm365-3-3-UpdateType.json'
Update-StateMachine -Name 'm365-3-3-DataGarbage' -DefinitionFile 'm365-3-3-DataGarbage.json'
Update-StateMachine -Name 'm365-4-m365getuser' -DefinitionFile 'm365-4-m365getuser.json'
Update-StateMachine -Name 'm365-4-m365getgroup' -DefinitionFile 'm365-4-m365getgroup.json'
Update-StateMachine -Name 'm365-4-athenabillingmetrics' -DefinitionFile 'm365-4-athenabillingmetrics.json'
Update-StateMachine -Name 'm365-4-athenaqueryhistory' -DefinitionFile 'm365-4-athenaqueryhistory.json'

Write-Host '[OK] done'

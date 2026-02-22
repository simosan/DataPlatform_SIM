<#
.SYNOPSIS
Step Functions のステートマシンごとに、実行の「最初の開始時刻」と「最後の終了時刻」だけを集計して出力します。

.DESCRIPTION
- 入力: target_drivehistory.csv（1行に1ステートマシン。名前 or ARN）
- 出力: target_drivehistory_times.csv（FirstStart/LastStop は TimeZoneId で表示）
- 途中の再リラン（中間実行）はスキップし、最初と最後のみを記録します（min/max）。

.EXAMPLE
pwsh -NoProfile -File ./get-stepfunctions-drivehistory-times.ps1 -AllTime -Region ap-northeast-1

.EXAMPLE
pwsh -NoProfile -File ./get-stepfunctions-drivehistory-times.ps1 -Day '2026-02-15' -Region ap-northeast-1 -OutputFile target_drivehistory_times_2026-02-15.csv

.EXAMPLE
pwsh -NoProfile -File ./get-stepfunctions-drivehistory-times.ps1 -From '2026-02-15 00:00:00' -To '2026-02-15 23:59:59' -Region ap-northeast-1

.EXAMPLE
pwsh -NoProfile -File ./get-stepfunctions-drivehistory-times.ps1 -Day (Get-Date).AddDays(-1) -Region ap-northeast-1

.NOTES
必要に応じて -Profile を指定してください（例: -Profile lfadmin）。
#>

[CmdletBinding()]
param(
    [Parameter()]
    [string]$InputFile = "target_drivehistory.csv",

    [Parameter()]
    [string]$OutputFile = "target_drivehistory_times.csv",

    [Parameter()]
    [string]$Profile = "",

    [Parameter()]
    [string]$Region,

    [Parameter()]
    [switch]$AllTime,

    [Parameter()]
    [datetime]$Day,

    [Parameter()]
    [datetime]$From,

    [Parameter()]
    [datetime]$To,

    [Parameter()]
    [string]$TimeZoneId = "Asia/Tokyo"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Resolve-PathIfRelative([string]$Path) {
    if ([string]::IsNullOrWhiteSpace($Path)) { return $Path }
    if ([System.IO.Path]::IsPathRooted($Path)) { return $Path }
    return (Join-Path -Path $PSScriptRoot -ChildPath $Path)
}

function Get-TimeZoneSafe([string]$Id) {
    try {
        return [TimeZoneInfo]::FindSystemTimeZoneById($Id)
    }
    catch {
        Write-Warning "TimeZoneId '$Id' が見つからないためローカルタイムゾーンを使用します。"
        return [TimeZoneInfo]::Local
    }
}

function Convert-AwsTimeToTzString([string]$AwsTime, [TimeZoneInfo]$Tz) {
    if ([string]::IsNullOrWhiteSpace($AwsTime)) { return $null }
    $dto = [DateTimeOffset]::Parse($AwsTime, [System.Globalization.CultureInfo]::InvariantCulture)
    $converted = [TimeZoneInfo]::ConvertTime($dto, $Tz)
    return $converted.ToString("yyyy-MM-dd HH:mm:ss zzz")
}

function Invoke-AwsCli([string[]]$AwsCliArgs) {
    $env:AWS_PAGER = ""
    $output = & aws @AwsCliArgs 2>&1
    if ($LASTEXITCODE -ne 0) {
        throw "aws CLI failed (exit=$LASTEXITCODE): $($output | Out-String)"
    }
    return ($output | Out-String)
}

function Get-StateMachineArnByName([string]$Name, [string]$Profile, [string]$Region) {
    $args = @('stepfunctions', 'list-state-machines', '--output', 'json')
    if ($Profile) { $args += @('--profile', $Profile) }
    if ($Region) { $args += @('--region', $Region) }

    $json = (Invoke-AwsCli -AwsCliArgs $args) | ConvertFrom-Json
    $matches = @($json.stateMachines | Where-Object { $_.name -eq $Name })

    if ($matches.Count -eq 0) {
        return $null
    }
    if ($matches.Count -gt 1) {
        Write-Warning "state machine name '$Name' が複数見つかりました。最初の1件を使用します。"
    }
    return $matches[0].stateMachineArn
}

function List-AllExecutions([string]$StateMachineArn, [string]$Profile, [string]$Region) {
    $all = New-Object System.Collections.Generic.List[object]
    $token = $null

    do {
        $args = @(
            'stepfunctions', 'list-executions',
            '--state-machine-arn', $StateMachineArn,
            '--max-results', '1000',
            '--output', 'json'
        )
        if ($Profile) { $args += @('--profile', $Profile) }
        if ($Region) { $args += @('--region', $Region) }
        if ($token) { $args += @('--next-token', $token) }

        $json = (Invoke-AwsCli -AwsCliArgs $args) | ConvertFrom-Json
        foreach ($e in @($json.executions)) {
            $all.Add($e)
        }
        $nextTokenProp = $json.PSObject.Properties['nextToken']
        $token = if ($null -ne $nextTokenProp) { [string]$nextTokenProp.Value } else { $null }
    } while ($token)

    return $all
}

$inputPath = Resolve-PathIfRelative $InputFile
$outputPath = Resolve-PathIfRelative $OutputFile

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "aws CLI が見つかりません。AWS CLI v2 をインストールして 'aws' が実行できる状態にしてください。"
}
if (-not (Test-Path -LiteralPath $inputPath)) {
    throw "InputFile が見つかりません: $inputPath"
}

$tz = Get-TimeZoneSafe -Id $TimeZoneId

if (-not $AllTime) {
    if ($PSBoundParameters.ContainsKey('Day')) {
        if ($PSBoundParameters.ContainsKey('From') -or $PSBoundParameters.ContainsKey('To')) {
            throw "Day と From/To は同時に指定できません。どちらか一方を使用してください。"
        }
        $From = $Day.Date
        $To = $Day.Date.AddDays(1).AddTicks(-1)
    }
    if (-not $PSBoundParameters.ContainsKey('From')) {
        $From = (Get-Date).Date
    }
    if (-not $PSBoundParameters.ContainsKey('To')) {
        $To = Get-Date
    }
    if ($To -lt $From) {
        throw "To は From 以降を指定してください。 From=$From To=$To"
    }
}

$names = Get-Content -LiteralPath $inputPath | ForEach-Object { $_.Trim() } | Where-Object { $_ -and -not $_.StartsWith('#') }

$results = New-Object System.Collections.Generic.List[object]

foreach ($id in $names) {
    $stateMachineArn = $null
    $stateMachineName = $id

    if ($id -match '^arn:aws:states:') {
        $stateMachineArn = $id
    }
    else {
        $stateMachineArn = Get-StateMachineArnByName -Name $id -Profile $Profile -Region $Region
    }

    if (-not $stateMachineArn) {
        Write-Warning "state machine が見つかりませんでした: $id"
        $results.Add([pscustomobject]@{
            StateMachine = $stateMachineName
            StateMachineArn = $null
            FirstStart = $null
            LastStop = $null
            ExecutionCount = 0
        })
        continue
    }

    $executions = List-AllExecutions -StateMachineArn $stateMachineArn -Profile $Profile -Region $Region

    $completed = @($executions | Where-Object {
        $_.status -ne 'RUNNING' -and $_.PSObject.Properties.Name -contains 'stopDate' -and $_.stopDate
    })

    if (-not $AllTime) {
        $fromUnspecified = [DateTime]::SpecifyKind($From, [DateTimeKind]::Unspecified)
        $toUnspecified = [DateTime]::SpecifyKind($To, [DateTimeKind]::Unspecified)
        $fromUtc = [TimeZoneInfo]::ConvertTimeToUtc($fromUnspecified, $tz)
        $toUtc = [TimeZoneInfo]::ConvertTimeToUtc($toUnspecified, $tz)
        $fromDto = [DateTimeOffset]::new($fromUtc)
        $toDto = [DateTimeOffset]::new($toUtc)
        $completed = @($completed | Where-Object {
            $sd = [DateTimeOffset]::Parse($_.startDate, [System.Globalization.CultureInfo]::InvariantCulture)
            $sd -ge $fromDto -and $sd -le $toDto
        })
    }

    if ($completed.Count -eq 0) {
        $results.Add([pscustomobject]@{
            StateMachine = $stateMachineName
            StateMachineArn = $stateMachineArn
            FirstStart = $null
            LastStop = $null
            ExecutionCount = 0
        })
        continue
    }

    $first = $completed | Sort-Object { [DateTimeOffset]::Parse($_.startDate, [System.Globalization.CultureInfo]::InvariantCulture) } | Select-Object -First 1
    $last = $completed | Sort-Object { [DateTimeOffset]::Parse($_.stopDate, [System.Globalization.CultureInfo]::InvariantCulture) } | Select-Object -Last 1

    $results.Add([pscustomobject]@{
        StateMachine = $stateMachineName
        StateMachineArn = $stateMachineArn
        FirstStart = (Convert-AwsTimeToTzString -AwsTime $first.startDate -Tz $tz)
        LastStop = (Convert-AwsTimeToTzString -AwsTime $last.stopDate -Tz $tz)
        ExecutionCount = $completed.Count
    })
}

$results | Export-Csv -LiteralPath $outputPath -NoTypeInformation -Encoding utf8
Write-Host "Wrote: $outputPath"
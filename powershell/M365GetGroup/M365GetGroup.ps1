# EntraIDからMicrosoft Graph APIを使用してグループ情報を取得するLambda関数
# EntraIDのグループ情報を取得し、S3に保存する。
# このスクリプトは、単一のグループ情報を取得することを目的としており、ページネーションは考慮していない。
# 事前にAWS Systems Manager Parameter Storeに必要なパラメータを設定しておく必要あり。
# - /m365/auth/tenantId: EntraID テナント ID
# - /m365/auth/clientId: EntraID アプリケーションのクライアント ID
# - /m365/auth/clientSecret: EntraID アプリケーションのクライアントシークレット
# - /m365/auth/scope: Microsoft Graph APIのスコープ (例: "https://graph.microsoft.com/.default")
try {
    Import-Module AWS.Tools.Lambda -ErrorAction Stop
    Import-Module AWS.Tools.SimpleSystemsManagement -ErrorAction Stop

} catch {
    Write-Host "[Func-Error]-[M365GetGroup]-[Import-Module] $($_.Exception.Message)"
    Write-Host "詳細Error $($_.Exception | Out-String)"
    throw
}


## 認証ヘッダ取得関数
function M365Auth {
    try {
        $invokeParams = @{
            FunctionName    = "M365Auth"
            InvocationType  = "RequestResponse"
        }
        $response = Invoke-LMFunction @invokeParams

        # json → PSObject → hashtableに変換
        $reader = New-Object System.IO.StreamReader($response.Payload)
        $jsonString = $reader.ReadToEnd()
        $authObject = $jsonString | ConvertFrom-Json
        $headers = @{}
        $authObject.PSObject.Properties | ForEach-Object {
            $headers[$_.Name] = $_.Value
        }
    } catch {
        Write-Host "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }

    return $headers
}


## 既存S3キー削除関数（M365CollectS3KeyDelete） 呼び出し
function CallM365CollectS3KeyDelete {
    param(
        [Parameter(Mandatory = $true)]
        $targetdataname,    # S3のターゲットデータ名
        [Parameter(Mandatory = $true)]
        $group,     # S3のグループキー名
        [Parameter(Mandatory = $true)]
        $basedate   # リカバリ用に関数入力パラメータから基準日を取得（na or yyyy-mm-dd形式）
    )

    $payloadObj = @{
       targetdataname = $targetdataname
       group          = $group
       basedate       = $basedate
    }
    $payloadJson = $payloadObj | ConvertTo-Json -Depth 4 -Compress
    $invokeParams = @{
        FunctionName    = "M365CollectS3KeyDelete"
        InvocationType  = "RequestResponse"
        Payload         = $payloadJson
    }

    try {
        $response = Invoke-LMFunction @invokeParams
        $reader = New-Object System.IO.StreamReader($response.Payload)
        $resultJson = $reader.ReadToEnd()

        if ($response.FunctionError) {
            $errorPayload = $resultJson | ConvertFrom-Json
            Write-Host "[Func-Error]-[M365GetUser]-[CallM365CollectS3KeyDelete:LambdaResponseError] `
                $($errorPayload.errorMessage)"
            throw $errorPayload
        } else {
            $result = $resultJson | ConvertFrom-Json
            Write-Host "[Func-Success] Batch ${key}: $($result | Out-String)"
        }
    } catch {
        Write-Host "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
        throw
    }
}

## main
function M365GetGroup {
    [cmdletbinding()]
    param(
        [parameter()]
        $LambdaInput,
        [parameter()]
        $LambdaContext
    )

    # リカバリ用に関数入力パラメータから基準日を取得（未使用：na, リカバリ用：yyyy-mm-dd形式）
    if ($null -eq $LambdaInput.basedate) {
        $basedate = "na"
        $fromtimestamp = $null
        $totimestamp   = $null
    }else{
        if ($LambdaInput.basedate -notmatch '^\d{4}-\d{2}-\d{2}$') {
            throw "[Func-Error]-[M365GetGroup]-[InvalidInput] basedateの形式が不正です。'yyyy-mm-dd'の形式で指定してください。"
        }
        $basedate = $LambdaInput.basedate
        $fromtimestamp = $LambdaInput.fromtimestamp
        $totimestamp   = $LambdaInput.totimestamp
    }

    ## M365Auth関数呼び出し-認証ヘッダ取得
    $headers = M365Auth

    # 冪等性確保のため、ファイル上書きではなく、上位キーを削除する　
    $targetdataname = "m365getgroup"
    $group          = "group1"
    CallM365CollectS3KeyDelete -targetdataname $targetdataname `
                                -group $group `
                                -basedate $basedate

    ## グループ一覧取得
    # エンドポイント
    $targeturi = (Get-SSMParameter -Name "/m365/getgroup/targeturi").Value
    try {
        $response = Invoke-RestMethod -Method Get -Uri $targeturi -Headers $headers
    } catch {
        Write-Host "[Func-Error]-[M365GetGroup]-[Invoke-RestMethod failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }

    # PSCustomObject配列オブジェクトにデータ格納
    $groups = @()
    foreach ($user in $response.value){
        $groups += [PSCustomObject]@{
            id                = $user.id
            description       = $user.description
            displayName       = $user.displayName
            securityIdentifier = $user.securityIdentifier
        }
    }

    # データはbodyに、テーブル名はtargetdatanameに、データパイプラインのグループはgroupにそれぞれ格納
    # fromtimestamp, totimestampはリカバリ用に基準日とともに渡す（未使用時は空文字）
    $payloadObj = @{
        body = $groups
        is_gzip = $false
        targetdataname = "m365getgroup"
        group = "group1"
        basedate = $basedate
        fromtimestamp = $fromtimestamp
        totimestamp   = $totimestamp
    }
    # Lambda関数への引数のためJSON形式に変換
    $payloadJson = $payloadObj | ConvertTo-Json -Depth 10 -Compress

    $invokeParams = @{
        FunctionName    = "M365CollectS3Export"
        InvocationType  = "RequestResponse"
    }

    try {
        <#Write-Host "[Debug] payloadjson: $($payloadJson | Out-String)"
        Write-Host "[Debug] invokeParams: $($invokeParams | Out-String)"#>

        $response = Invoke-LMFunction @invokeParams -Payload $payloadJson
        $reader = New-Object System.IO.StreamReader($response.Payload)
        $resultJson = $reader.ReadToEnd()

        if ($response.FunctionError) {
            $errorPayload = $resultJson | ConvertFrom-Json
            Write-Host "[Func-Error]-[M365GetGroup]-[Invoke-LMFunction LambdaResponseError] $($errorPayload.errorMessage)"
            throw $errorPayload
        } else {
            $result = $resultJson | ConvertFrom-Json
            Write-Host "[Func-Success] $($result | Out-String)"
        }

    } catch {
        Write-Host "[Func-Error]-[M365GetGroup]-[Invoke-LMLambdaFunction failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }

    return @{ status = "success" } | ConvertTo-Json -Compress
}
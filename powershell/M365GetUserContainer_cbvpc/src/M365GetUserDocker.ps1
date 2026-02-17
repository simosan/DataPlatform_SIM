# EntraIDからMicrosoft Graph APIを使用してユーザ情報を取得するLambda関数
# EntraIDのユーザ情報を取得し、S3に保存する。
# EntraIDユーザ情報は、大量に存在する可能性があるため、ページネーションを考慮して1000件ずつ取得し、S3に保存する。
# 事前にAWS Systems Manager Parameter Storeに必要なパラメータを設定しておく必要あり。
# - /m365/auth/tenantId: EntraID テナント ID
# - /m365/auth/clientId: EntraID アプリケーションのクライアント ID
# - /m365/auth/clientSecret: EntraID アプリケーションのクライアントシークレット
# - /m365/auth/scope: Microsoft Graph APIのスコープ (例: "https://graph.microsoft.com/.default")
# - /m365/getuser/targeturi: ユーザ情報を取得するためのエンドポイント (例: "https://graph.microsoft.com/v1.0/users")
# PowerShell スクリプト引数を受け取り、関数に渡す
param(
    [string]$CommandInput
)

try {
    Import-Module AWS.Tools.Lambda -ErrorAction Stop
    Import-Module AWS.Tools.SimpleSystemsManagement -ErrorAction Stop

} catch {
    Write-Host "[Func-Error]-[M365GetUser]-[Import-Module] $($_.Exception.Message)"
    Write-Host "詳細Error $($_.Exception | Out-String)"
    throw
}


## 認証ヘッダ取得関数
function M365Auth {
    try {
        $invokeParams = @{
            FunctionName    = "M365AuthVpc"
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
        FunctionName    = "M365CollectS3KeyDeleteVpc"
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
function M365GetUserDocker {
    [cmdletbinding()]
    param(
        [parameter()]
        $CommandInput
    )

    # entrypoint から引数が渡らないケースを許容し、環境変数 GROUP で補完する
    if ($null -eq $CommandInput -or [string]::IsNullOrWhiteSpace([string]$CommandInput)) {
        if ([string]::IsNullOrWhiteSpace($env:GROUP)) {
            Write-Host "[Func-Error]-[M365GetUser]-[InvalidInput] CommandInputが空で、GROUP環境変数も未設定です。"
            return @{ status = "failed"} | ConvertTo-Json -Compress
        }
        $CommandInput = [PSCustomObject]@{ group = $env:GROUP }
    } else {
        # JSON文字列を想定
        $CommandInput = $CommandInput | ConvertFrom-Json
    }

    # リカバリ用に関数入力パラメータから基準日を取得（未使用：na, リカバリ用：yyyy-mm-dd形式）
    if ($null -eq $CommandInput.basedate) {
        $basedate = "na"
        $fromtimestamp = $null
        $totimestamp   = $null
    }else{
        if ($CommandInput.basedate -notmatch '^\d{4}-\d{2}-\d{2}$') {
            throw "[Func-Error]-[M365GetUser]-[InvalidInput] basedateの形式が不正です。'yyyy-mm-dd'の形式で指定してください。"
        }
        $basedate = $CommandInput.basedate
        $fromtimestamp = $CommandInput.fromtimestamp
        $totimestamp   = $CommandInput.totimestamp
    }

    Write-Host "[Func-Info] basedate: $basedate"

    ## M365Auth関数呼び出し-認証ヘッダ取得
    try {
        $headers = M365Auth
    } catch {
        Write-Host "[Func-Error]-[M365GetUser]-[M365Auth failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        return @{ status = "failed"} | ConvertTo-Json -Compress
    }

    # 冪等性確保のため、ファイル上書きではなく、上位キーを削除する　
    $targetdataname = "m365getuser"
    $group = $CommandInput.group
    if ($null -eq $group) {
        Write-Host "[Func-Error]-[M365GetUser]-[InvalidInput] groupが指定されていません。"
        return @{ status = "failed"} | ConvertTo-Json -Compress
    }
    CallM365CollectS3KeyDelete -targetdataname $targetdataname `
                               -group $group `
                               -basedate $basedate

    ## ユーザ一覧取得（1000件ずつ取得（ページネーション対応））
    # エンドポイント
    $targeturi = (Get-SSMParameter -Name "/m365/getuser/targeturi").Value
    $users = @()
    $nextLink = $targeturi

    do {
        try {
            $response = Invoke-RestMethod -Method Get -Uri $nextLink -Headers $headers
        } catch {
            Write-Host "[Func-Error]-[M365GetUser]-[Invoke-RestMethod failed] $_"
            return @{ status = "failed"} | ConvertTo-Json -Compress
        }

        foreach ($user in $response.value) {
            $users += [PSCustomObject]@{
                id                = $user.id
                userPrincipalName = $user.userPrincipalName
                surname           = $user.surname
                givenName         = $user.givenName
                displayName       = $user.displayName
            }
        }

        $nextLink = $response.'@odata.nextLink'
    } while ($nextLink)

    Write-Host "[Func-Info] Total users fetched: $($users.Count)"

    ## S3 送信用に分割（1000件ごと）
    $batchSize = 1000
    $batches = @()
    # 10000件のユーザを1000件ずつのバッチに分割
    for ($i = 0; $i -lt $users.Count; $i += $batchSize) {
        $end = [math]::Min($i + $batchSize, $users.Count)
        # ,を付けて単一要素として配列に追加。
        # $batchesは1000件ごとの配列の配列になる。
        $batches += ,($users[$i..($end - 1)])
    }
    # 1000件ごとにpayLoadを作成
    for ($key = 0; $key -lt $batches.Count; $key++) {
        Write-Host "[Debug] Batch $key sending $($batches[$key].Count) users"

        $payloadObj = $batches[$key]
        # --- ペイロードをgzip圧縮 ---
        Write-Host "[Info] Batch $key payload (before gzip): $($payloadJson.Length) bytes"
        $payloadJson = $payloadObj | ConvertTo-Json -Depth 4 -Compress
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($payloadJson)
        $ms = New-Object System.IO.MemoryStream
        $gzip = New-Object System.IO.Compression.GzipStream($ms, [System.IO.Compression.CompressionMode]::Compress)
        $gzip.Write($bytes, 0, $bytes.Length)
        $gzip.Close()
        $compressedPayload = $ms.ToArray()
        $ms.Close()
        Write-Host "[Info] Batch $key payload (after gzip): $($compressedPayload.Length) bytes"
        # Base64エンコード（LambdaのInvoke-LMFunctionはバイナリ非対応で、System.Stringに変換する必要があるため）
        $base64Payload = [Convert]::ToBase64String($compressedPayload)

        $invokePayload = @{
            body           = $base64Payload
            is_gzip        = $true
            targetdataname = "m365getuser"
            group          = $group
            batch          = [string]$key
            basedate       = $basedate
            fromtimestamp  = $fromtimestamp
            totimestamp    = $totimestamp
        }
        $invokePayloadJson = $invokePayload | ConvertTo-Json -Depth 4 -Compress
        $invokeParams = @{
            FunctionName    = "M365CollectS3ExportVpc"
            InvocationType  = "RequestResponse"
            Payload         = $invokePayloadJson
        }

        try {
            $response = Invoke-LMFunction @invokeParams
            $reader = New-Object System.IO.StreamReader($response.Payload)
            $resultJson = $reader.ReadToEnd()

            if ($response.FunctionError) {
                $errorPayload = $resultJson | ConvertFrom-Json
                Write-Host "[Func-Error]-[M365GetUser]-[M365CollectS3ExportVpc:LambdaResponseError] `
                    $($errorPayload.errorMessage)"
                return @{ status = "failed"} | ConvertTo-Json -Compress
            } else {
                $result = $resultJson | ConvertFrom-Json
                Write-Host "[Func-Success] Batch ${key}: $($result | Out-String)"
            }
        } catch {
            Write-Host "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
            Write-Host "ErrorDetails: $($_.Exception | Out-String)"
            return @{ status = "failed"} | ConvertTo-Json -Compress
        }
    }

    return @{ status = "success" } | ConvertTo-Json -Compress
}

## Entry Point
M365GetUserDocker -CommandInput $CommandInput
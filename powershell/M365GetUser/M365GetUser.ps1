# EntraIDからMicrosoft Graph APIを使用してユーザ情報を取得するLambda関数
# EntraIDのユーザ情報を取得し、S3に保存する。
# EntraIDユーザ情報は、大量に存在する可能性があるため、ページネーションを考慮して1000件ずつ取得し、S3に保存する。
# 事前にAWS Systems Manager Parameter Storeに必要なパラメータを設定しておく必要あり。
# - /m365/auth/tenantId: EntraID テナント ID
# - /m365/auth/clientId: EntraID アプリケーションのクライアント ID
# - /m365/auth/clientSecret: EntraID アプリケーションのクライアントシークレット
# - /m365/auth/scope: Microsoft Graph APIのスコープ (例: "https://graph.microsoft.com/.default")
# - /m365/getuser/targeturi: ユーザ情報を取得するためのエンドポイント (例: "https://graph.microsoft.com/v1.0/users")
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
function M365GetUser {
    [cmdletbinding()]
    param(
        [parameter()]
        $LambdaInput,
        [parameter()]
        $LambdaContext
    )
    $LambdaInput = $LambdaInput | ConvertFrom-Json
    # リカバリ用に関数入力パラメータから基準日を取得（未使用：na, リカバリ用：yyyy-mm-dd形式）
    if ($null -eq $LambdaInput.basedate) {
        $basedate = "na"
        $fromtimestamp = $null
        $totimestamp   = $null
    }else{
        if ($LambdaInput.basedate -notmatch '^\d{4}-\d{2}-\d{2}$') {
            throw "[Func-Error]-[M365GetUser]-[InvalidInput] basedateの形式が不正です。'yyyy-mm-dd'の形式で指定してください。"
        }
        $basedate = $LambdaInput.basedate
        $fromtimestamp = $LambdaInput.fromtimestamp
        $totimestamp   = $LambdaInput.totimestamp
    }

    ## M365Auth関数呼び出し-認証ヘッダ取得
    $headers = M365Auth

    # 冪等性確保のため、ファイル上書きではなく、上位キーを削除する　
    $targetdataname = "m365getuser"
    $group          = "group1"
    CallM365CollectS3KeyDelete -targetdataname $targetdataname -group $group -basedate $basedate

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
            throw
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
            group          = "group1"
            batch          = [string]$key
            basedate       = $basedate
            fromtimestamp  = $fromtimestamp
            totimestamp    = $totimestamp
        }
        $invokePayloadJson = $invokePayload | ConvertTo-Json -Depth 4 -Compress
        $invokeParams = @{
            FunctionName    = "M365CollectS3Export"
            InvocationType  = "RequestResponse"
            Payload         = $invokePayloadJson
        }

        try {
            $response = Invoke-LMFunction @invokeParams
            $reader = New-Object System.IO.StreamReader($response.Payload)
            $resultJson = $reader.ReadToEnd()

            if ($response.FunctionError) {
                $errorPayload = $resultJson | ConvertFrom-Json
                Write-Host "[Func-Error]-[M365GetUser]-[M365CollectS3Export:LambdaResponseError] `
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

    return @{ status = "success" } | ConvertTo-Json -Compress
}
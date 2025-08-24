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
    Write-Output "[Func-Error]-[M365GetUser]-[Import-Module] $($_.Exception.Message)"
    Write-Output "詳細Error $($_.Exception | Out-String)"
    throw
}

function M365GetUserDocker {
    [cmdletbinding()]
    param(
        [parameter()]
        $LambdaInput,
        [parameter()]
        $LambdaContext
    )

    ## M365Auth関数呼び出し-認証ヘッダ取得
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
        Write-Output "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
        Write-Output "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }

    # 冪等性確保のため、ファイル上書きではなく、上位キーを削除する　
    $payloadObj = @{
       targetdataname = "m365getuser"
       group          = "group1"
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
            Write-Output "[Func-Error]-[M365GetUser]-[M365CollectS3KeyDelete:LambdaResponseError] `
                $($errorPayload.errorMessage)"
            throw $errorPayload
        } else {
            $result = $resultJson | ConvertFrom-Json
            Write-Output "[Func-Success] Batch ${key}: $($result | Out-String)"
        }
    } catch {
        Write-Output "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
        throw
    }


    ## ユーザ一覧取得（1000件ずつ取得（ページネーション対応））
    # エンドポイント
    $targeturi = (Get-SSMParameter -Name "/m365/getuser/targeturi").Value
    $users = @()
    $nextLink = $targeturi

    do {
        try {
            $response = Invoke-RestMethod -Method Get -Uri $nextLink -Headers $headers
        } catch {
            Write-Output "[Func-Error]-[M365GetUser]-[Invoke-RestMethod failed] $_"
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
  
    Write-Output "[Func-Info] Total users fetched: $($users.Count)"

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
        Write-Output "[Debug] Batch $key sending $($batches[$key].Count) users"

        $payloadObj = $batches[$key]
        # --- ペイロードをgzip圧縮 ---
        Write-Output "[Info] Batch $key payload (before gzip): $($payloadJson.Length) bytes"
        $payloadJson = $payloadObj | ConvertTo-Json -Depth 4 -Compress
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($payloadJson)
        $ms = New-Object System.IO.MemoryStream
        $gzip = New-Object System.IO.Compression.GzipStream($ms, [System.IO.Compression.CompressionMode]::Compress)
        $gzip.Write($bytes, 0, $bytes.Length)
        $gzip.Close()
        $compressedPayload = $ms.ToArray()
        $ms.Close()
        Write-Output "[Info] Batch $key payload (after gzip): $($compressedPayload.Length) bytes"
        # Base64エンコード（LambdaのInvoke-LMFunctionはバイナリ非対応で、System.Stringに変換する必要があるため）
        $base64Payload = [Convert]::ToBase64String($compressedPayload)
    
        $invokePayload = @{
            body           = $base64Payload
            is_gzip        = $true
            targetdataname = "m365getuser"
            group          = "group1"
            batch          = [string]$key
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
                Write-Output "[Func-Error]-[M365GetUser]-[M365CollectS3Export:LambdaResponseError] `
                    $($errorPayload.errorMessage)"
                throw $errorPayload
            } else {
                $result = $resultJson | ConvertFrom-Json
                Write-Output "[Func-Success] Batch ${key}: $($result | Out-String)"
            }
        } catch {
            Write-Output "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
            throw
        }
    }

    return @{ status = "success" } | ConvertTo-Json -Compress
}
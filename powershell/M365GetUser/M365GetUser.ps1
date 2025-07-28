try {
    Import-Module AWS.Tools.Lambda -ErrorAction Stop
    Import-Module AWS.Tools.SimpleSystemsManagement -ErrorAction Stop

} catch {
    Write-Host "[Func-Error]-[M365GetUser]-[Import-Module] $($_.Exception.Message)"
    Write-Host "詳細Error $($_.Exception | Out-String)"
    throw
}

function M365GetUser {
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
        Write-Host "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }

    ## ユーザ一覧取得
    # エンドポイント
    $targeturi = (Get-SSMParameter -Name "/m365/getuser/targeturi").Value
    try {
        $response = Invoke-RestMethod -Method Get -Uri $targeturi -Headers $headers 
    } catch {
        Write-Host "[Func-Error]-[M365GetUser]-[Invoke-RestMethod failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }
   
    # PSCustomObject配列オブジェクトにデータ格納
    $users = @()
    foreach ($user in $response.value){
        $users += [PSCustomObject]@{
            id                = $user.id
            userPrincipalName = $user.userPrincipalName
            surname           = $user.surname
            givenName         = $user.givenName
            displayName       = $user.displayName
        }
    }

    # データはbodyに、テーブル名はtargetdatanameに、データパイプラインのグループはgroupにそれぞれ格納
    $payloadObj = @{
        body = $users
        targetdataname = "m365getuser"
        group = "group1"
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
            Write-Host "[Func-Error]-[M365GetUser]-[Invoke-LMFunction LambdaResponseError] $($errorPayload.errorMessage)"
            throw $errorPayload
        } else {
            $result = $resultJson | ConvertFrom-Json
            Write-Host "[Func-Success] $($result | Out-String)"
        }
    
    } catch {
        Write-Host "[Func-Error]-[M365GetUser]-[Invoke-LMLambdaFunction failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw
    }

    return @{ status = "success" } | ConvertTo-Json -Compress
}
try{
    Import-Module AWS.Tools.SimpleSystemsManagement -ErrorAction Stop
} catch {
    Write-Host "[Func-Error]-[M365Auth]-[Import-Module] $($_.Exception.Message)"
    Write-Host "詳細Error $($_.Exception | Out-String)"
    throw
}


function M365Auth {
   
   $private:tenantId     = (Get-SSMParameter -Name "/m365/auth/tenantId" -WithDecryption $true).Value
   $private:clientId     = (Get-SSMParameter -Name "/m365/auth/clientId" -WithDecryption $true).Value
   $private:clientSecret = (Get-SSMParameter -Name "/m365/auth/clientSecret" -WithDecryption $true).Value
   $private:scope        = (Get-SSMParameter -Name "/m365/auth/scope" -WithDecryption $true).Value
   $private:grantType    = "client_credentials"

   # トークン取得用のエンドポイント
   $tokenEndpoint = "https://login.microsoftonline.com/$tenantId/oauth2/v2.0/token"
   # リクエストボディを作成
   $body = @{                                                                          
       client_id     = $clientId                                                       
       scope         = $scope
       client_secret = $clientSecret                                                   
       grant_type    = $grantType                                                      
   }

   try 
   {
       $response = Invoke-RestMethod `
                   -Method Post `
                   -Uri $tokenEndpoint `
                   -ContentType "application/x-www-form-urlencoded" `
                   -Body $body                                                         
       $accessToken = $response.access_token                                               
   } catch {
       Write-Host "[FuncError]-[M365Auth]-[Invoke-RestMethod failed] $_"
       Write-Host "ErrorDetails: $($_.Exception | Out-String)"
       throw "[FuncError]-[M365Auth]-[Invoke-RestMethod failed]"
   }
                                                                               
   # ヘッダー作成                                                                            
   $headers = @{                                                                       
      Authorization = "Bearer $accessToken"                                           
   }
   
   return $headers
}
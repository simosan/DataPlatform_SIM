Describe M365Auth-Tests {
    Write-Host "M365Auth-Tests 開始`n"
    BeforeAll {
        <#$env:AWS_DEFAULT_REGION = "ap-northeast-1"
        $env:AWS_ENDPOINT_URL = "http://localhost:4566"
        $env:AWSProfileName = "default"
        $env:AWS_PROFILE = "default"#>
        . "$PSScriptRoot/M365Auth.ps1"
        
        # Get-SSMParameter のモック
        Mock -CommandName Get-SSMParameter -MockWith {
            param($Name, $WithDecryption)
            switch ($Name) {
                "/m365/auth/tenantId"     { return @{ Value = "dummy-tenant" } }
                "/m365/auth/clientId"     { return @{ Value = "dummy-client" } }
                "/m365/auth/clientSecret" { return @{ Value = "dummy-secret" } }
                "/m365/auth/scope"        { return @{ Value = "dummy-scope" } }
                default                   { return @{ Value = "dummy-value" } }
            }
        }

        # M365 Web通信用のレスポンスをモック
        Mock -CommandName Invoke-RestMethod -MockWith {
            [PSCustomObject]@{ access_token = "test_token" }
        }
    }

    It "正常系：アクセストークンを含むヘッダーを返す" {
        $result = M365Auth
        $result.Authorization | Should -Be "Bearer test_token"
    }

    It "異常系：Invoke-RestMethod失敗エラー例外を投げるテスト" {
        # 失敗ケースのためのレスポンスを上書きモック
        Mock -CommandName Invoke-RestMethod -MockWith {
            throw "Throw-Invoke-RestMethod失敗エラー例外を投げるテスト"
        }

        { M365Auth } | Should -Throw "*Invoke-RestMethod failed*"
    }
}

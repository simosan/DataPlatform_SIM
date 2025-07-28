# out-fileの-encodingの型変換チェックが厳しく、UTF8文字列を与えると、System.Text.Encodingエラーになる
# なので、mockではなｋ、out-fileの関数再定義で回避 
function Out-File { param($InputObject, $FilePath, $Encoding) }

Describe M365CollectS3Export-Tests {
    Write-Host "M365CollectS3Export-Tests 開始`n"
    BeforeAll {
        
        . "$PSScriptRoot/M365CollectS3Export.ps1"

        # SSMパラメータモック
        Mock -CommandName Get-SSMParameter -MockWith {
            param($Name)
            switch ($Name) {
                "/m365/common/s3bucket" { return @{ Value = "dummy-bucket" } }
                "/m365/common/pipelinecol" { return @{ Value = "dummy-pipeline" } }
                default { return @{ Value = "dummy-value" } }
            }
        }
        # S3オブジェクト取得のモック
        Mock -CommandName Read-S3Object -MockWith {}
        # CSVインポートのモック
        Mock -CommandName Import-Csv -MockWith {
            [PSCustomObject]@{
                base = "2025-07-25"
                from = "2025-07-25T00:00:00"
                to   = "2025-07-25T23:59:59"
            }
        }
        # S3書き込みのモック
        Mock -CommandName Write-S3Object -MockWith {}
        # ファイル削除のモック
        Mock -CommandName Remove-Item -MockWith {}
    }

    # パラメータ変数inputは各テストごとでをを変数を変えておく（妙な動きを防ぐため）
    It "正常系：status=successを返す" {
        Write-Host "正常系：status=successを返す" 
        $input1 = @{
            body = @(
                @{
                    id = "11111111-0000-aaaa-bbbb-111111111111"
                    userPrincipalName = "test@test.com"
                    surname = "test"
                    givenName = "test"
                    displayName = "test test"
                }
            )
            group = "group1"
            targetdataname = "M365GetUser"
        }
        $result = M365CollectS3Export -LambdaInput $input1
        $result | Should -Be '{"status":"success"}'
    }
   
    It "異常系：LambdaInput.bodyが不正な形式" {
        Write-Host "異常系：LambdaInput.bodyが不正な形式" 
        $input2 = @{
            body = "invalid-format"
            group = "group1"
            targetdataname = "m365GetUser"
        }
        { M365CollectS3Export -LambdaInput $input2 } | 
            Should -Throw "*bodyが配列型ではありません*"
    }

    It "異常系：LambdaInput.groupが不正な形式" {
        Write-Host "異常系：LambdaInput.groupが不正な形式" 
        $input3 = @{
            body = @(
                @{
                    id = "11111111-0000-aaaa-bbbb-111111111111"
                    userPrincipalName = "test@test.com"
                    surname = "test"
                    givenName = "test"
                    displayName = "test test"
                }
            )
            group = "groupp"
            targetdataname = "M365GetUser"
        }
        { M365CollectS3Export -LambdaInput $input3 } | 
            Should -Throw "*groupの形式が不正です。*"
    }

    It "S3取得失敗時に例外を投げる" {
        Write-Host "異常系：取得失敗時に例外を投げる" 
        Mock -CommandName Read-S3Object -MockWith { throw "S3Error" }
        $input4 = @{
            body = @(
                @{
                    id = "11111111-0000-aaaa-bbbb-111111111111"
                    userPrincipalName = "test@test.com"
                    surname = "test"
                    givenName = "test"
                    displayName = "test test"
                }
            )
            group = "group1"
            targetdataname = "M365GetUser"
        }
        { M365CollectS3Export -LambdaInput $input4 } | 
            Should -Throw "*Read-basedatetime.csv failed*"
    }

    It "S3書き込み失敗時に例外を投げる" {
        Write-Host "異常系：S3書き込み失敗時に例外を投げる" 
        Mock -CommandName Write-S3Object -MockWith { throw "WriteError" }
        $input5 = @{
            body = @(
                @{
                    id = "11111111-0000-aaaa-bbbb-111111111111"
                    userPrincipalName = "test@test.com"
                    surname = "test"
                    givenName = "test"
                    displayName = "test test"
                }
            )
            group = "group1"
            targetdataname = "M365GetUser"
        }
        { M365CollectS3Export -LambdaInput $input5 } | 
            Should -Throw "*Write-S3Object failed*"
    }
}
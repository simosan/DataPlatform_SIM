# out-fileの-encodingの型変換チェックが厳しく、UTF8文字列を与えると、System.Text.Encodingエラーになる
# なので、mockではなｋ、out-fileの関数再定義で回避 
function Out-File { param($InputObject, $FilePath, $Encoding) }

Describe M365CollectS3keyDelete-Tests {
    Write-Host "M365CollectS3KeyDelete-Tests 開始`n"
    BeforeAll {
        
        . "$PSScriptRoot/M365CollectS3KeyDelete.ps1"

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
        # S3ファイルリストのモック
        Mock -CommandName Get-S3Object -MockWith {
            [PSCustomObject]@{
                ETag         = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                BucketName   = "dummy-bucket"
                Key          = "group1/collect/dummy-pipeline/year=2025/month=07/day=25/dummyfile.json"
                LastModified = "2017/01/01 00:00:00"
                Owner        = "Amazon.S3.Model.Owner"
                Size         = "100"
                StorageClass = "STANDARD"
            }
        }
        # S3キー削除のモック
        Mock -CommandName Remove-S3Object -MockWith {}
        # ファイル削除のモック
        Mock -CommandName Remove-Item -MockWith {}
    }

    # パラメータ変数inputは各テストごとでをを変数を変えておく（妙な動きを防ぐため）
    It "正常系：status=successを返す" {
        Write-Host "正常系：status=successを返す" 
        $input1 = @{
            group = "group1"
            targetdataname = "M365GetUser"
        }
        $result = M365CollectS3KeyDelete -LambdaInput $input1
        $result | Should -Be '{"status":"success"}'
    }
   
    It "異常系：LambdaInput.group不正な形式" {
        Write-Host "異常系：LambdaInput.groupが不正な形式" 
        $input2 = @{
            group = "groups"
            targetdataname = "m365GetUser"
        }
        { M365CollectS3KeyDelete -LambdaInput $input2 } | 
            Should -Throw "*groupの形式が不正です。*"
    }

    It "異常系：LambdaInput.targetdatanem不正な形式" {
        Write-Host "異常系：LambdaInput.targetdatanameが不正な形式" 
        $input2 = @{
            group = "group1"
            targetdataname = ""
        }
        { M365CollectS3KeyDelete -LambdaInput $input2 } | 
            Should -Throw "*targetdatanameがnullまたは空で*"
    }

    It "Cleanup失敗時に例外を投げる" {
        Write-Host "異常系：Cleanup失敗時に例外を投げる" 
        Mock -CommandName Get-S3Object -MockWith {
            return @(@{ Key = "dummyfile.json" })
        }
        Mock -CommandName Remove-S3Object -MockWith {
            throw "RemoveError"
        }
        $input6 = @{
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
        { M365CollectS3KeyDelete -LambdaInput $input6 } |
            Should -Throw "*Cleanup failed*"
    }
}
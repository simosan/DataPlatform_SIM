#### 再実行にかかる冪等性確保のため、指定S3キーを削除する。
try {
    Import-Module AWS.Tools.S3 -ErrorAction Stop
    Import-Module AWS.Tools.SimpleSystemsManagement -ErrorAction Stop

} catch {
    Write-Host "[Func-Error]-[S3Export]-[Import-Module] $($_.Exception.Message)"
    Write-Host "詳細Error $($_.Exception | Out-String)"
    throw
}

function M365CollectS3KeyDelete {
    param(
        [Parameter(Mandatory = $true)]
        $LambdaInput,

        [Parameter(Mandatory = $false)]
        $LambdaContext
    )

    $group              = $LambdaInput.group
    $targetdatatable    = $LambdaInput.targetdataname
   
    # $group が 'group' に続く数値の形式であることをチェック
    if ($group -notmatch '^group\d+$') {
        Write-Host "[Func-Error]-[S3Export]-[InvalidInput] groupの形式が不正です。'group<number>'の形式で指定してください。"
        throw "[Func-Error]-[S3Export]-[InvalidInput] groupの形式が不正です。'group<number>'の形式で指定してください。"
    }
   
    # $targetdataname が空やnullでないことをチェック
    if ([string]::IsNullOrWhiteSpace($targetdatatable)) {
        throw "[Func-Error]-[S3Export]-[InvalidInput] targetdatanameがnullまたは空です。"
    }

    ## 基準日日付をS3から取得。
    # 一時ファイル名（Lambdaの/tmpディレクトリに保存。他関数との競合回避のためGUID付加）
    $bucketname = (Get-SSMParameter -Name "/m365/common/s3bucket").Value
    try {
        $tmpname = $([guid]::NewGuid().ToString()) + "_basedatetime.csv"
        Read-S3Object -BucketName $bucketname -Key "basedatetime/basedatetime.csv" -File "/tmp/$tmpname"
        $psdttm = Import-Csv -Path "/tmp/$tmpname"
    } catch {
        Write-Host "[Func-Error]-[S3Export]-[Read-basedatetime.csv failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw "[Func-Error]-[S3Export]-[Read-basedatetime.csv failed] $_"
    } finally {
        Remove-Item -Path "/tmp/$tmpname" -Force -ErrorAction SilentlyContinue
    }
    
    # 指定キー指定 m365-dwh/groupx/collect/$targetdataname/year=yyyy/month=MM/day=dd/
    $twotier    = $group + '/'
    $threetier  = (Get-SSMParameter -Name "/m365/common/pipelinecol").Value
    $fourtier   = $targetdatatable + '/'
    $fivetier   = 'year=' + $psdttm.base.split('-')[0] + '/'
    $sixtier    = 'month=' + $psdttm.base.split('-')[1] + '/'
    $seventier  = 'day=' + $psdttm.base.split('-')[2] + '/'

    # S3キーフルパス
    $writekeys = $twotier + $threetier + $fourtier + $fivetier + $sixtier + $seventier
    # 冪等性のため、上書き対象のキーを削除
    try {
        $existingObjects = Get-S3Object -BucketName $bucketname -KeyPrefix $writekeys -ErrorAction Stop
        if ($existingObjects) {
            $existingObjects | ForEach-Object {
                Remove-S3Object -BucketName $bucketname -Key $_.Key -Force -ErrorAction Stop
                Write-Host "[Func-Info]-[S3Export] Deleted existing object: $($_.Key)"
            }
        }
    } catch {
        # 存在しないバケットを指定や権限の場合にThrow。単にキーが存在しない場合はエラーにならない
        Write-Host "[Func-Error]-[S3Export]-[Cleanup failed] $_"
        throw "[Func-Error]-[S3Export]-[Cleanup failed] $($_.Exception.Message)"
    }

    return @{ status = "success" } | ConvertTo-Json -Compress
}
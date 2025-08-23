# 呼び出し元からの引数を受け取り、M365データをS3に出力するLambda関数
# 単一ファイル、複数ファイル（batch番号をプレフィックスとして付与）での出力をサポート
try {
    Import-Module AWS.Tools.S3 -ErrorAction Stop
    Import-Module AWS.Tools.SimpleSystemsManagement -ErrorAction Stop

} catch {
    Write-Host "[Func-Error]-[S3Export]-[Import-Module] $($_.Exception.Message)"
    Write-Host "詳細Error $($_.Exception | Out-String)"
    throw
}

function M365CollectS3Export {
    param(
        [Parameter(Mandatory = $true)]
        $LambdaInput,

        [Parameter(Mandatory = $false)]
        $LambdaContext
    )

    # --- bodyがgzip圧縮されたbyte配列の場合、解凍してJSONに変換 ---
    if ($LambdaInput.is_gzip -eq $true) {
        Write-Host "[Info] Received compressed payload: $($LambdaInput.body.Length) bytes"
        $compressedBytes = [Convert]::FromBase64String($LambdaInput.body)
        $ms = New-Object System.IO.MemoryStream(,$compressedBytes)
        $gzip = New-Object System.IO.Compression.GzipStream($ms, [System.IO.Compression.CompressionMode]::Decompress)
        $sr = New-Object System.IO.StreamReader($gzip, [System.Text.Encoding]::UTF8)
        $decompressedJson = $sr.ReadToEnd()
        $sr.Close()
        $gzip.Close()
        $ms.Close()
        Write-Host "[Info] Decompressed payload: $($decompressedJson.Length) bytes"
        $body = $decompressedJson | ConvertFrom-Json
    } else {
        $body = $LambdaInput.body
        Write-Host "[Debug] M365GetGroup ${body}"
    }

    $group              = $LambdaInput.group
    $targetdatatable    = $LambdaInput.targetdataname
    # ファイル単一出力 or ファイル複数出力判定
    if (![System.String]::IsNullOrEmpty($LambdaInput.batch)) {
        $batchkey = $LambdaInput.batch
        $targetdataname = $batchkey + '_' + $targetdatatable 
    } else {
        $targetdataname = $targetdatatable
    }

    Write-Host "[Info] Received $($body.Count) users in batch $batchkey"

    # 1件だけの場合は配列に変換（ConbertFrom-Jsonの仕様で、単一オブジェクトの場合配列にならないため）
    if ($body -isnot [System.Object[]] -and $body -isnot [System.Collections.IEnumerable]) {
        $body = @($body)
    }

    # $body がリスト型（配列）であることをチェック
    if ($body -isnot [System.Collections.IEnumerable] -or $body -is [string]) {
        Write-Host "[FuncError]-[S3Export]-[InvalidInput] bodyが配列型ではありません" 
        throw "[FuncError]-[S3Export]-[InvalidInput] bodyが配列型ではありません"
    }
   
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
    
    # ファイル格納先のS3パーティション作成 - m365-dwh/groupx/collect/$targetdataname/year=yyyy/month=MM/day=dd/
    # パーティションキーはそれぞれの階層で作成すること！
    # すでに存在したとしても、都度作成で問題なし（冪等性）
    $twotier    = $group + '/'
    $threetier  = (Get-SSMParameter -Name "/m365/common/pipelinecol").Value
    $fourtier   = $targetdatatable + '/'
    $fivetier   = 'year=' + $psdttm.base.split('-')[0] + '/'
    $sixtier    = 'month=' + $psdttm.base.split('-')[1] + '/'
    $seventier  = 'day=' + $psdttm.base.split('-')[2] + '/'

    # S3キーフルパス
    $writekeys = $twotier + $threetier + $fourtier + $fivetier + $sixtier + $seventier
    # 一時ファイル名（Lambda用なら /tmp）
    $tmpJsonPath = "/tmp/$targetdatatable.json"
    # データ作成元のjsonデータに項目を追加。basedatetime.csvの内容を追加.
    $wrapObject = [PSCustomObject]@{
        m365_base = $psdttm.base
        m365_from = $psdttm.from
        m365_to   = $psdttm.to
        acquired_date = (Get-Date -Format "yyyy-MM-dd")
        data      = $body
    }
    # Jsonファイルで/tmpに保存
    $wrapObject | ConvertTo-Json -Depth 10 | Out-File -FilePath $tmpJsonPath -Encoding UTF8
    # S3にファイルをアップロード
    try {
        Write-S3Object -BucketName $bucketname -Key "$writekeys$targetdataname.json" -File $tmpJsonPath 
    } catch {
        Write-Host "[Func-Error]-[S3Export]-[Write-S3Object failed] $_"
        Write-Host "ErrorDetails: $($_.Exception | Out-String)"
        throw "[Func-Error]-[S3Export]-[Write-S3Object failed] $_"
    } finally {
        Remove-Item -Path $tmpJsonPath -Force -ErrorAction SilentlyContinue
    }

    return @{ status = "success" } | ConvertTo-Json -Compress
}
# S3のcollectディレクトリ（生データ取得用）にあるJSONファイルのリストを取得する関数
# S3キーは削除し、ファイル名のみを配列に格納
import boto3
import pandas as pd
from io import StringIO


def m365cols3list(event, context):

    bucket_name = event.get('bucket_name')
    collect_key = event.get('collect_key')
    group = event.get('group')
    targetdataname = event.get('targetdataname')

    ## 基準日(yyyy-mm-dd)をS3から取得。またはリカバリ用に関数入力パラメータから基準日(yyyy-mm-dd)を取得。
    s3_client = boto3.client('s3')
    try:
        # 通常処理の場合、S3から基準日を取得
        if event.get('basedate') == 'na':
            csv_file = s3_client.get_object(
                Bucket=bucket_name,
                Key="basedatetime/basedatetime.csv"
            )
            csv_file_body = csv_file['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_file_body), usecols=['base'])
            base_date = df['base'].iloc[0]
        # リカバリ用基準日が指定されている場合、その日付を使用
        else:
            base_date = event.get('basedate')
    except Exception as e:
        print(f"[func-error]-[m365cols3list]-[reading-error] \
            basedatetime.csv: {e}")
        return {
            "statusCode": 500,
            "message": f"m365cols3list Error : {str(e)}"
        }

    keys = []
    files = []
    try:
        dtstr = base_date.replace("-", "")
        targetkey = f"{group}/{collect_key}{targetdataname}/date={dtstr}/"
        # 返却リストは1000件まで
        keys = s3_client.list_objects(Bucket=bucket_name, Prefix=targetkey)
        for key in keys.get('Contents', []):
            # ファイル名を抽出してリストに追加
            files.append(key['Key'].split('/')[-1])

    except Exception as e:
        print(f"[func-error]-[m365cols3list]-[reading-error] \
            s3_client.list_objects: {e}")
        return {
            "statusCode": 500,
            "message": f"m365cols3list Error : {str(e)}"
        }

    return { "statusCode": 200, "files": files }


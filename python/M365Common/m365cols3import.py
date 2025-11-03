# S3のcollectディレクトリ（生データ取得用）からJSONファイルを取得し、DataFrameに変換する関数
import boto3
import pandas as pd
from io import StringIO

def m365cols3import(event, context):

    bucket_name = event.get('bucket_name')
    collect_key = event.get('collect_key')
    group = event.get('group')
    targetdataname = event.get('targetdataname')
    filename = event.get('filename')

    ## 基準日(yyyy-mm-dd)をS3から取得。またはリカバリ用に関数入力パラメータから基準日(yyyy-mm-dd)を取得。
    s3_client = boto3.client('s3')
    print(f"[Debug]-[m365colimport]-bucket_name:{bucket_name}, collect_key:{collect_key}, group:{group}, targetdataname:{targetdataname}, filename:{filename}")
    print(f"[Debug]-[m365colimport]-event:{event.get('basedate')}")
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
        print(f"[func-error]-[m365colimport]-[reading-error] \
            basedatetime.csv: {e}")
        return {
            "statusCode": 500,
            "message": f"m365cols3import Error : {str(e)}"
        }

    # 各収集データの取得
    try:
        dtstr = base_date.replace("-", "")
        targetkey = f"{group}/{collect_key}{targetdataname}/date={dtstr}/"
        json_file = s3_client.get_object(
            Bucket=bucket_name,
            Key=targetkey + filename
        )
        json_file_body = json_file['Body'].read().decode('utf-8')
        df = pd.read_json(StringIO(json_file_body))
    except s3_client.exceptions.NoSuchKey as e:
        print(f"[func-error]-[m365colimport]-NoSuchKey: {targetkey + filename}")
        return {"statusCode": 500, "message": f"File not found: {str(e)}"}
    except Exception as e:
        print(f"[func-error]-[m365colimport]-reading JSON: {targetkey + filename} : {e}")
        return {"statusCode": 500, "message": f"Error reading JSON: {str(e)}"}

    return {"data": df.get('data', []).tolist(),
            "m365_base": df['m365_base'].iloc[0],
            "m365_from": df['m365_from'].iloc[0],
            "m365_to": df['m365_to'].iloc[0],
            "acquired_date": df['acquired_date'].iloc[0]}


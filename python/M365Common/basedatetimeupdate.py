import boto3
import pandas as pd
import datetime
from io import StringIO

def basedatetimeupdate(event, context):
    # 変数設定
    try:
        ssm = boto3.client('ssm')
        bucket_name = ssm.get_parameter(Name='/m365/common/s3bucket',
                                WithDecryption=False)['Parameter']['Value']

    except Exception as e:
        print(f"[func-error]-[basedatetimeupdate]-[ssm-error] {e}")
        return {
            "statusCode": 500,
            "message": f"false! {e}"}    
    basedt_key = 'basedatetime/basedatetime.csv'

    # 基準日ファイルから基準日を取得
    s3_client = boto3.client('s3')
    try:
        csv_file = s3_client.get_object(
            Bucket=bucket_name,
            Key=basedt_key
        )
        csv_file_body = csv_file['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_file_body), usecols=['base'])
    except Exception as e:
        print(f"[func-error]-[basedatetimeupdate]-[reading-error] \
            basedatetime.csv: {e}")
        return {
            "statusCode": 500,
            "message": f"false! {e}"}

    try:
        # 日付を更新
        dt = datetime.date.today()
        basedate = dt - datetime.timedelta(days=1)
        basedate = basedate.strftime("%Y-%m-%d")
        df['base'] = basedate
        df['from'] = basedate + 'T00:00'
        df['to'] = basedate + 'T23:59'

         # S3へ書き戻し
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=basedt_key,
            Body=csv_buffer.getvalue().encode('utf-8')
        )
    except Exception as e:
        print(f"[func-error]-[basedatetimeupdate]-[writeing-error] basedatetime.csv: {e}")
        return {
            "statusCode": 500,
            "message": f"false! {e}"}

    return {
            "statusCode": 200,
            "message": "success"}
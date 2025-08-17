import boto3
import pandas as pd
from io import StringIO

def m365cols3import(event, context):
    
    bucket_name = event.get('bucket_name')
    collect_key = event.get('collect_key')
    group = event.get('group')
    targetdataname = event.get('targetdataname')
    filename = event.get('filename')
    
    # 基準日ファイルから基準日を取得
    s3_client = boto3.client('s3')
    try:
        csv_file = s3_client.get_object(
            Bucket=bucket_name,
            Key="basedatetime/basedatetime.csv"
        )
        csv_file_body = csv_file['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_file_body), usecols=['base'])
        base_date = df['base'].iloc[0]
    except Exception as e:
        print(f"[func-error]-[m365colimport]-[reading-error] \
            basedatetime.csv: {e}")
        return {
            "statusCode": 500,
            "message": f"m365cols3import Error : {str(e)}"
        }
    
    # 各収集データの取得
    try:
        targetkey = group + "/" + collect_key + targetdataname + "/" + "year=" \
                    + base_date[:4] + "/month=" + base_date[5:7]  \
                    + "/day=" + base_date[8:10] + "/"
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

    
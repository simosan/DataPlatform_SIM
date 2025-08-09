import boto3
import json
import pandas as pd


# M365ColS3Importから基準日データと収集データを取得
def imp_s3_collect_data(bucket_name, collect_key, group, targetdataname):
    lambda_client = boto3.client('lambda')
    payload = {
        "bucket_name": bucket_name,
        "collect_key": collect_key,
        "group": group,
        "targetdataname": targetdataname
    }
    response = lambda_client.invoke(
        FunctionName='m365cols3import',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = response['Payload'].read().decode('utf-8')
    result = json.loads(response_payload)

    # data空チェック
    if not result['data']:
        print(f"[func-error]-[m365convgroup]-[imp_s3_collect_data] {result}")
        return None
    return result

# S3のconvertに出力
def exp_s3_conv_data(bucket_name,
                     target_key,
                     group,
                     targetdataname,
                     base_date,
                     df):

    lambda_client = boto3.client('lambda')
    payload = {
        "bucket_name": bucket_name,
        "target_key": target_key,
        "group": group,
        "targetdataname": targetdataname,
        "base_date": base_date,
        "data": df.to_dict(orient="records")
    }
    response = lambda_client.invoke(
        FunctionName='m365convs3export_parquet',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = response['Payload'].read().decode('utf-8')
    result = json.loads(response_payload)

    return result

# main関数
def m365convgroup(event, context):
    # parameterストアから必要な値を取得 
    ssm = boto3.client('ssm')
    bucket_name = ssm.get_parameter(Name='/m365/common/s3bucket',
                                    WithDecryption=False)['Parameter']['Value']
    collect_key = ssm.get_parameter(Name='/m365/common/pipelinecol',
                                    WithDecryption=False)['Parameter']['Value']
    target_key = ssm.get_parameter(Name='/m365/common/pipelineconv', 
                                    WithDecryption=False)['Parameter']['Value']
    # データ取得対象グループ,テーブル
    group = "group1"
    targetdataname = "m365getgroup"  

    # S3から基準日・収集データを取得
    result = imp_s3_collect_data(bucket_name, collect_key, group, targetdataname) 
    if result is None:
        return {
            "statusCode": 500,
            "message": "m365convgroup_imp_s3_collect_dataでエラー"
        } 
    base_date = result.get('m365_base')
    from_datetime = result.get('m365_from')
    to_datetime = result.get('m365_to')
    acquired_date = result.get('acquired_date')
    data = result.get('data')

    # 加工、CSV出力前にpandas形式に変換
    try:
        df = pd.json_normalize(data)
    except Exception as e:
        print(f"[func-error]-[m365convgroup] \
            DataFrame変換失敗: {repr(e)}")
        return {
            "statusCode": 500,
            "message": f"Pandas DataFrame変換失敗: {repr(e)}"
        } 

    # 適当な加工（指定列のNaNを'dummy'に置換）
    df['description'] = df['description'].fillna("dummy")
    # 出力データに出力データの列に取得開始日、取得終了日、取得日を追加 
    df['from_datetime'] = from_datetime
    df['to_datetime'] = to_datetime
    df['base_date'] = base_date
    df['acquired_date'] = acquired_date

    # S3に出力
    result = exp_s3_conv_data(bucket_name, 
                                 target_key, 
                                 group,
                                 targetdataname, 
                                 base_date, 
                                 df)
    if result.get("statusCode") != 200:
        return {
            "statusCode": 500,
            "message": result["message"] if result else "exp_s3_conv_dataでエラー"
    }
    
    return {
            "statusCode": 200,
            "message": "success"}
    
    
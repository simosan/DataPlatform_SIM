# S3のcollectからデータを取得し、加工してS3のconvertに出力するLambda関数
# 出力形式は、parquet形式
# 対象のデータはEntra IDのユーザ情報
import boto3
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import re


# M365ColS3Importから収集データを取得
def imp_s3_collect_data(bucket_name, collect_key, group, targetdataname, filename, basedate):
    lambda_client = boto3.client('lambda')
    payload = {
        "bucket_name": bucket_name,
        "collect_key": collect_key,
        "group": group,
        "targetdataname": targetdataname,
        "filename": filename,
        "basedate": basedate
    }
    response = lambda_client.invoke(
        FunctionName='m365cols3import',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = response['Payload'].read().decode('utf-8')
    result = json.loads(response_payload)

    # data空チェック
    if not result.get('data'):
        print(f"[func-warn]-[imp_s3_collect_data] データが空です。result: {result}")
        return None
    return result

# 取得対象のS3キー一覧を取得
def list_s3_collect_data(bucket_name, collect_key, group, targetdataname, basedate):
    lambda_client = boto3.client('lambda')
    payload = {
        "bucket_name": bucket_name,
        "collect_key": collect_key,
        "group": group,
        "targetdataname": targetdataname,
        "basedate": basedate
    }
    response = lambda_client.invoke(
        FunctionName='m365cols3list',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = response['Payload'].read().decode('utf-8')
    result = json.loads(response_payload)

    if result.get("statusCode") != 200:
        print(f"[func-error]-[list_s3_collect_data] {result}")
        return None

    print(f"[Debug-result {result}]")

    return result.get('files', [])

# S3のconvertに出力
def exp_s3_conv_data(bucket_name,
                     target_key,
                     group,
                     targetdataname,
                     base_date,
                     df):

    # S3出力先のキーを組み立て
    dtstr=base_date.replace("-", "")
    target_key = (f"{group}/{target_key}"
                 f"{targetdataname}/"
                 f"date={dtstr}/")
    file_name = f"{targetdataname}.parquet"
    s3_key = f"{target_key}{file_name}"

    try:
        # DataFrameをParquetに変換
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False,
                      engine='pyarrow', compression='snappy')
        # S3にアップロード
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())
    except Exception as e:
        print(f"[func-error]-[m365convuser]-[s3-Export-Error] {str(e)}")
        return {
            "statusCode": 500,
            "message": f"Error uploading to S3: {str(e)}"
        }

    return {"statusCode": 200, "message": "success", "s3_key": s3_key }


# main関数
def m365convuser(event, context):
    # リカバリ用に関数入力パラメータから基準日を取得（未使用：na, リカバリ用：yyyy-mm-dd形式）
    if event.get('basedate') is None:
        basedate = "na"
    else:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', event['basedate']):
            raise ValueError("[Func-Error]-[m365convuser]-[InvalidInput]"
                             "basedateの形式が不正です。'yyyy-mm-dd'の形式で指定してください。")
        basedate = event['basedate']

    # parameterストアから必要な値を取得
    ssm = boto3.client('ssm')
    bucket_name = ssm.get_parameter(Name='/m365/common/s3bucket',
                                    WithDecryption=False)['Parameter']['Value']
    collect_key = ssm.get_parameter(Name='/m365/common/pipelinecol',
                                    WithDecryption=False)['Parameter']['Value']
    target_key = ssm.get_parameter(Name='/m365/common/pipelineconv',
                                    WithDecryption=False)['Parameter']['Value']
    # データ取得対象グループ,テーブル
    group = event.get("group")
    if not group:
        print(f"[Func-ERROR]-[m365convuser]-[InvalidInput] group is required.")
        return json.dumps({ "status": "failed" })
    targetdataname = "m365getuser"

    # 取得対象キーに格納された一覧を取得（単一キー、複数キーの違いはなし）
    filelist = list_s3_collect_data(bucket_name,
                                    collect_key,
                                    group,
                                    targetdataname,
                                    basedate)
    if filelist is None:
        print(f"[func-error]-[m365convuser] list_s3_collect_dataでエラー: filelist None")
        return json.dumps({"status":"failed"})

    # DataFrameを格納するリスト
    dfs = []
    for file in filelist:

        try:
            result = imp_s3_collect_data(bucket_name,
                                         collect_key,
                                         group,
                                         targetdataname,
                                         file,
                                         basedate)
        except Exception as e:
            print(f"[Func-ERROR]-[m365convuser]-[imp_s3_collect_data] {e}")
            return json.dumps({ "status": "failed" })

        if result is None:
            print(f"[Func-WARN]-[m365convuser]"
                  "imp_s3_collect_dataで警告が発生しました。データが空です。")
            # データが空の場合もあるため、後続処理は行わずに次のファイルへ（ループ継続）
            continue

        data = result.get('data')
        if not data:
            print(f"[Func-WARN]-[m365convuser] dataが空のためスキップします。file: {file}")
            continue

        print(data)
        # 加工、S3出力前に複数ファイル（単一でも）をまとめる
        try:
            df = pd.json_normalize(data)
            # 出力データの列に取得開始日、取得終了日、取得日を追加
            df['base_date'] = result.get('m365_base')
            df['from_datetime'] = result.get('m365_from')
            df['to_datetime'] = result.get('m365_to')
            df['acquired_date'] = result.get('acquired_date')
            dfs.append(df)
        except Exception as e:
            print(f"[func-error]-[m365convuser] \
                DataFrame変換失敗: {str(e)}")
            return json.dumps({"status": "failed"})

    # dfsを結合
    # 全て空だと pd.concat が例外になるので、事前にチェックして空の場合は成功で終了する
    if not dfs:
        print(f"[Func-WARN]-[m365convuser] 取得したデータは全て空でした。dfs is empty.")
        # データが空の場合もあるため、後続処理は行わずに成功で終了
        return json.dumps({ "status": "success" })
    # 0行のDataFrameが混在している可能性があるため、その場合も除外目的で成功終了
    merged_df = pd.concat(dfs, ignore_index=True)
    if merged_df.empty:
        print(f"[Func-WARN]-[m365convuser] 取得したデータは全て空でした。merged_df is empty.")
        # データが空の場合もあるため、後続処理は行わずにS3出力もせずに成功で終了
        return json.dumps({ "status": "success" })

    # 適当な加工（指定列のNaNを'dummy'に置換）
    merged_df[['surname', 'givenName']] = merged_df[['surname', 'givenName']].fillna("dummy")
    print(f"[Debug-merged_df] {len(merged_df)} 件のデータを取得しました。")
    # S3に出力
    result = exp_s3_conv_data(bucket_name,
                                 target_key,
                                 group,
                                 targetdataname,
                                 df['base_date'].iloc[0],
                                 merged_df)
    if result.get("statusCode") != 200:
        print(f"[func-error]-[m365convuser] exp_s3_conv_dataでエラーが発生しました。result: {result}")
        return json.dumps({"status":"failed"})

    return json.dumps({
            "status": "success"})

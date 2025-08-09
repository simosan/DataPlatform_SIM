import boto3
import pandas as pd
import io

# S3にParquetファイルを出力するLambda関数
def m365convs3export_parquet(event, context):
    bucket_name = event.get('bucket_name')
    target_key = event.get('target_key')
    group = event.get('group')
    targetdataname = event.get('targetdataname')
    base_date = event.get('base_date')
    json_data = event.get('data')

    # S3出力先のキーを組み立て
    target_key = (f"{group}/{target_key}"
                 f"{targetdataname}/"
                 f"year={base_date[:4]}/"
                 f"month={base_date[5:7]}/"
                 f"day={base_date[8:10]}/")
    file_name = f"{targetdataname}.parquet" 
    s3_key = f"{target_key}{file_name}"

    # JSON → DataFrame
    df = pd.DataFrame(json_data)

    try:
        # DataFrameをParquetに変換
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine='pyarrow', compression='snappy')
        # S3にアップロード
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=parquet_buffer.getvalue())
    except Exception as e:
        print(f"[func-error]-[m365convs3export_parguqet] {str(e)}")
        return {
            "statusCode": 500,
            "message": f"Error uploading to S3: {str(e)}"
        }

    return {
        "statusCode": 200,
        "message": f"S3({s3_key})に出力しました"
    }
    
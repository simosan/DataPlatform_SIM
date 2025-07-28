import psycopg2
import pandas as pd
import boto3
from io import StringIO

# groupx/convet/直下のテーブル名のみを抽出し配列化
def s3_target_list(bucket, tier1and2prefix):
    # 指定prefix直下のキー（サブディレクトリやファイル）名をリストで返す
    s3 = boto3.client('s3')
    response = s3.list_objects_v2(
        Bucket=bucket,
        Prefix=tier1and2prefix,
        Delimiter='/'
    )
    # サブディレクトリ名
    tables = [key['Prefix'].split('/')[2] for key in response.get('CommonPrefixes', [])]

    return tables

# groupx/convert/table名/直下の基準日指定の各データを取得
# 戻りはDataframe型
def getconvdata(bucket_name, tier1and2_prefix, table, base_date):
    s3_client = boto3.client('s3')
    targetkey = tier1and2_prefix + table + "/" + "year=" \
                + base_date[:4] + "/month=" + base_date[5:7]  \
                + "/day=" + base_date[8:10] + "/"
    try:
        csv_file = s3_client.get_object(
            Bucket=bucket_name,
            Key=targetkey + table + ".csv"
        )
        csv_file_body = csv_file['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_file_body)) 
    except Exception as e:
            print(f"[func-error]-[s3convtopg]-[getconvdata]-[reading-error] \
               {table}.csv: {e}")
            return {
                    "statusCode": 500,
                    "message": f"s3convtopg-getconvdata-{table}.csv Error: {str(e)}"
            }

    return {
        "statusCode": 200,
        "df": df
    }

    
# 基準日データ有無確認→存在していたら重複データのためTrue、存在してなければFalseで返却
def check_target_duplicate(table,
                           basedate,
                           pguser,
                           pgpasswd,
                           pghost,
                           dbname,
                           schema):

    try:
        conn = psycopg2.connect(
            user=pguser,
            password=pgpasswd,
            host=pghost,
            dbname=dbname,
            port='5432'
        )
        cur = conn.cursor()
        query = f"SELECT COUNT(base_date) FROM {schema}.{table} WHERE base_date = %s"
        cur.execute(query, (basedate,))
        count = cur.fetchone()[0]
        print(basedate + ":" + table + ":count:" + str(count))
    except Exception as e:
        print(f"[func-error]-[s3convtopg]-[check_target_duplicate]-[pg_error] {e}")
        return { "statusCode": 500, "message": str(e) }
    finally:
        if conn:
           conn.close()
    
    return { "statusCode": 200, "count": count }


def insert_targetdata(table,
                      pguser,
                      pgpasswd,
                      pghost,
                      dbname,
                      schema,
                      df):
    try:
        conn = psycopg2.connect(
            user=pguser,
            password=pgpasswd,
            host=pghost,
            dbname=dbname,
            port='5432'
        ) 
        cur = conn.cursor()   
        # DataFrameをCSV形式に変換（ヘッダーなし）
        buffer = StringIO()
        df.to_csv(buffer, header=False, index=False)
        buffer.seek(0)
        # テーブル名は文字列結合で注意
        sql = f"COPY {schema}.{table} FROM STDIN WITH CSV"
        cur.copy_expert(sql, buffer)
        conn.commit()
        print(f"[Info] Insert to {schema}.{table} success. Rows: {len(df)}")
    except Exception as e:
        print(f"[func-error]-[s3convtopg]-[insert_targetdata]-[pg_error] {e}")
        conn.rollback()
        return { "statusCode": 500, "message": str(e) }
    finally:
        if conn:
            conn.close()
   
    return { "statusCode": 200, "message": "Insert success" }


# main
def s3convtopg(event, context):
    # eventから引数を取得（グループ名取得）
    if 'group' not in event:
        raise ValueError(f"[func-error]-[s3convtopg]-[argument-error] \
            event[group]: {e}")
    group = event['group']

    # parameterストアから必要な値を取得 
    ssm = boto3.client('ssm')
    bucket_name = ssm.get_parameter(Name='/m365/common/s3bucket',
                                    WithDecryption=False)['Parameter']['Value']
   
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
        print(f"[func-error]-[s3convtopg]-[basedatetime.csv-reading-error] \
            basedatetime.csv: {e}")
        return {
            "statusCode": 500,
             "message": f"[func-error]-[s3convtopg]-[basedatetime.csv-reading-error] \
                   convert: {e}"}
    
    # groupx/convert/配下のテーブルキー取得のためのプレフィックス 
    tier1and2_prefix = group + '/convert/'
    try:
        tables = s3_target_list(bucket_name, tier1and2_prefix)
        print("Debug-Tables: " + ', '.join(tables))
    except Exception as e:
        print(f"[func-error]-[s3convtopg]-[s3listobject-error] \
            convert: {e}")
        return {
            "statusCode": 500,
            "message": f"[func-error]-[s3convtopg]-[s3listobject-error] \
                convert: {e}"}
            
    # parameterストアからデータストア（Postgres）の情報を取得
    ssm = boto3.client('ssm')
    pguser = ssm.get_parameter(Name='/m365/common/pg/' + group + '/pguser',
                                WithDecryption=True)['Parameter']['Value']
    pgpasswd = ssm.get_parameter(Name='/m365/common/pg/' + group + '/pgpasswd',
                                WithDecryption=True)['Parameter']['Value']
    pghost = ssm.get_parameter(Name='/m365/common/pg/' + group + '/pghost',
                                WithDecryption=False)['Parameter']['Value']
    dbname = ssm.get_parameter(Name='/m365/common/pg/' + group + '/dbname',
                                WithDecryption=False)['Parameter']['Value']    
    schema = ssm.get_parameter(Name='/m365/common/pg/' + group + '/schema',
                                WithDecryption=False)['Parameter']['Value']
   # テーブル単位で、基準日付データ有無確認、Postgresへのデータインサート 
    for table in tables:
        # Postgresへ接続し、テーブルに基準日のデータがすでに存在していないかをチェック（重複データ有無）
        # 存在していたら後続処理はスキップ
        result = check_target_duplicate(table,
                                        base_date,
                                        pguser,
                                        pgpasswd,
                                        pghost,
                                        dbname,
                                        schema)
        if result.get('statusCode') != 200:
            print(f"[func-error]-[s3convtopg]-[check_target_duplicate]-[pg_error] \
                {table}.csv: {result.get('message')}")
            continue
        if result.get('count', 0) > 0:
            print(f"[func-info]-[s3convtopg]-[check_target_duplicate]-{table}の基準日データに重複あり")
            continue

        # S3からテーブルデータを読み込む
        tier1and2_prefix = group + '/convert/'
        conv_data = getconvdata(bucket_name, tier1and2_prefix, table, base_date)
        #print("Debug: " + conv_data.get("df"))
        if conv_data.get("statusCode") != 200:
            # csvファイルが読み込めなくても次に進める（スキップ）
            continue

        # 読み込んだデータをPostgresにインサート
        result = insert_targetdata(table,
                                   pguser,
                                   pgpasswd,
                                   pghost,
                                   dbname,
                                   schema,
                                   conv_data.get("df"))
        if result.get('statusCode') != 200:
            print(f"[func-error]-[s3convtopg]-[insert_targetdata]-[pg_error] \
                {table}.csv: {result.get('message')}")
            continue
    return {
        "statusCode": 200,
        "message": "success"}    
import boto3
import os
import sys
import pandas as pd
import pyarrow.parquet as pq
from io import StringIO
import time
import json

### クローラ実行
def catalog_scan(table: str, full_scan: bool = False):
    """指定テーブル対応の Glue Crawler を取得し、ポリシー更新後に起動して起動クローラ名を返却。

    SSM パラメータ命名規則: /m365/updatecatalog/crawler/<table>
    適用ポリシー:
        増分走査
            SchemaChangePolicy: UpdateBehavior=LOG, DeleteBehavior=LOG
            RecrawlPolicy: RecrawlBehavior=CRAWL_NEW_FOLDERS_ONLY
        全走査
            SchemaChangePolicy: UpdateBehavior=UPDATE_IN_DATABASE, DeleteBehavior=DEPRECATE_IN_DATABASE
            RecrawlPolicy: RecrawlBehavior=CRAWL_EVERYTHING

    戻り値:
      {
        'crawler_name': crawler_name
      }
    """
    ssm = boto3.client('ssm')
    glue = boto3.client('glue')
    param_name = f'/m365/updatecatalog/crawler/{table}'
    try:
        crawler_name = ssm.get_parameter(Name=param_name, WithDecryption=False)['Parameter']['Value']
    except ssm.exceptions.ParameterNotFound:
        print(f"[Error]-[updatecatalog]-[full_catalog_scan] クローラ名パラメータ未存在: {param_name}")
        raise

    # ポリシー設定
    if full_scan:
        schema_change_policy = {
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        }
        recrawl_policy = {
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        }
    else:
        schema_change_policy = {
            'UpdateBehavior': 'LOG',
            'DeleteBehavior': 'LOG'
        }
        recrawl_policy = {
            'RecrawlBehavior': 'CRAWL_NEW_FOLDERS_ONLY'
        }

    # ポリシー更新（クローラは事前作成済み前提）
    try:
        glue.update_crawler(
            Name=crawler_name,
            SchemaChangePolicy=schema_change_policy,
            RecrawlPolicy=recrawl_policy
        )
        print(
            f"[Info]-[updatecatalog]-[full_catalog_scan] ポリシー更新完了 "
            f"crawler={crawler_name} schema_change={schema_change_policy} "
            f"recrawl={recrawl_policy}"
        )
    except glue.exceptions.EntityNotFoundException:
        print(f"[Error]-[updatecatalog]-[full_catalog_scan] Crawler未存在: {crawler_name}")
        raise
    except Exception as e:
        print(f"[Error]-[updatecatalog]-[full_catalog_scan] ポリシー更新失敗 (継続して起動): {e}")
        raise

    print(f"[Info]-[updatecatalog]-[full_catalog_scan] クローラ起動: {crawler_name}")
    glue.start_crawler(Name=crawler_name)
    print(f"[Info]-[updatecatalog]-[full_catalog_scan] 起動完了 {crawler_name}")
    return {
        'crawler_name': crawler_name
    }

### 複数クローラの完了確認
def wait_crawler_completion(crawler_list: list,
                            timeout_seconds: int = 10800,
                            poll_interval: int = 30):
    """複数クローラの完了 (State=READY) を待機する。
      - 全クローラがREADY になるまでループ継続。
      - 個別に取得失敗 / 未存在でも他の クローラ は継続監視。
      - いずれか未完了のまま timeout 到達で打ち切り。
      - 監視間隔はデフォルト30秒。
      - タイムアウトはデフォルト3時間(10800秒)。

    戻り値:
      {
        'crawlers': [
            {
              'crawler_name': str,
              'state': str,          # READY / RUNNING / ERROR / NOT_FOUND / UNKNOWN
              'last_status': str,    # 最終クロール結果 SUCCEEDED/FAILED/CANCELLED/UNKNOWN
            }, ...
        ],
        'all_ready': bool,
        'duration_seconds': int,
        'polls': int,
      }
    """
    glue = boto3.client('glue')
    start = time.time()
    polls = 0 # 監視回数
    final_snapshot = [] # 最終状態スナップショット
    while True:
        elapsed = time.time() - start
        if elapsed >= timeout_seconds:
            print(f"[Warn]-[updatecatalog]-[wait_crawler_completion] "
                  f"タイムアウト (未完のクローラあり) elapsed={int(elapsed)}s")
            break

        snapshot = [] # 現時点の状態スナップショット
        all_ready_flag = True
        for crawler_name in crawler_list:
            state = 'UNKNOWN'
            last_status = 'UNKNOWN'
            try:
                crawler = glue.get_crawler(Name=crawler_name)['Crawler']
                state = crawler.get('State') or 'UNKNOWN'
                last_crawl = crawler.get('LastCrawl') or {}
                last_status = last_crawl.get('Status') or 'UNKNOWN'
            except glue.exceptions.EntityNotFoundException:
                state = 'NOT_FOUND'
                last_status = 'UNKNOWN'
                print(f"[Error]-[updatecatalog]-[wait_crawler_completion] "
                      f"Crawler未存在: {crawler_name}")
            except Exception as e:
                state = 'ERROR'
                last_status = 'UNKNOWN'
                print(f"[Error]-[updatecatalog]-[wait_crawler_completion] "
                      f"取得失敗 crawler={crawler_name} error={e}")

            snapshot.append({
                'crawler_name': crawler_name,
                'state': state,
                'last_status': last_status,
            })
            if state != 'READY':
                all_ready_flag = False

        print(f"[Info]-[updatecatalog]-[wait_crawler_completion] "
              f"poll={polls} details={snapshot}")
        polls += 1
        if all_ready_flag:
            final_snapshot = snapshot
            break
        time.sleep(poll_interval)
        final_snapshot = snapshot  # 途中経過を保持

    duration = int(time.time() - start)
    return {
        'crawlers': final_snapshot,
        'all_ready': all_ready_flag if 'all_ready_flag' in locals() else False,
        'duration_seconds': duration,
        'polls': polls,
    }


### 指定テーブルの指定日のスキーマ差分比較
def tablecolumns_diff_verify(
    table: str,
    base_s3_path: str,
    base_day: str,
    target_day: str,
    base_cols_override=None,
    target_cols_override=None,
):
    """
    指定テーブルの Parquet スキーマ(カラム名)差分を比較する。

    期待S3配置: s3://<bucket>/<group>/<convert_key>/<table>/date=YYYYMMDD/<table>.parquet
    base_s3_path は "s3://<bucket>/<group>/<convert_key>/" で終端スラッシュ付き想定。

        戻り値(dict):
            {
                'diff': bool,                     # 差分有無（追加/削除いずれか）
                'newly_added_columns': [..],      # 基準日に存在し指定日側に存在しないカラム
                'removed_columns': [..],          # 指定日側に存在し基準日に存在しないカラム
                'base_columns': [..],             # 基準日カラム一覧
                'target_columns': [..],           # 指定日カラム一覧
            }

    base_cols_override / target_cols_override が与えられた場合はS3/Parquet読込をスキップする。
    テスト用で利用する。
    """
    # Parquet欠損フラグ（欠損時は差分あり＝新規扱いで full scan に寄せる）
    missing_base = False
    missing_target = False
    # 差分フラグ初期化（False=差分無し）
    diff_flag = False
    # オーバーライドが無ければS3からParquetを読み込む
    if base_cols_override is None or target_cols_override is None:
        # path解析 + 余分なスラッシュ除去
        parts = base_s3_path.replace("s3://", "").split('/')
        bucket = parts[0]
        prefix_raw = '/'.join(parts[1:])  # group/convert_key/ など
        # 空要素や余分なスラッシュを除去
        prefix_parts = [p for p in prefix_raw.split('/') if p]
        prefix = '/'.join(prefix_parts)
        if prefix:
            prefix += '/'
        # S3キー組み立て（複数スラッシュを単一化）
        base_key = f"{prefix}{table}/date={base_day}/{table}.parquet".replace('//', '/')
        target_key = f"{prefix}{table}/date={target_day}/{table}.parquet".replace('//', '/')
        print(f"[Debug]-[updatecatalog]-[tablecolumns_diff_verify] "
              f"bucket={bucket} base_key={base_key} target_key={target_key}")

        s3_client = boto3.client("s3")
        base_local_path = f"/tmp/{table}_base.parquet"
        target_local_path = f"/tmp/{table}_target.parquet"

        print(f"[Info]-[updatecatalog]-[tablecolumns_diff_verify] "
              f"Downloading s3://{bucket}/{base_key}")
        try:
            s3_client.download_file(bucket, base_key, base_local_path)
        except Exception as e:
            # 基準日にデータがないケースがあり、その場合はファイルが存在しないエラーになるため、差分あり扱い（全カラム新規）で続行する
            err_code = getattr(e, 'response', {}).get('Error', {}).get('Code') if hasattr(e, 'response') else None
            if err_code in ('404', 'NoSuchKey') or 'Not Found' in str(e) or '404' in str(e):
                print(f"[Warn]-[updatecatalog]-[tablecolumns_diff_verify] "
                      f"基準日ファイルなし (データ0件) key={base_key} err={e} => 全カラム新規扱いで進行")
                missing_base = True
            else:
                print(f"[Error]-[updatecatalog]-[tablecolumns_diff_verify] "
                      f"基準日ファイル取得失敗 key={base_key} err={e}")
                raise
        if not missing_base:
            base_table = pq.read_table(base_local_path)
            base_cols = list(base_table.schema.names)
            os.remove(base_local_path)
        else:
            base_cols = []  # 全て新規として扱う

        print(f"[Info]-[updatecatalog]-[tablecolumns_diff_verify] "
              f"Downloading s3://{bucket}/{target_key}")
        try:
            s3_client.download_file(bucket, target_key, target_local_path)
        except Exception as e:
            # 初回実行などで前日(ターゲット)が存在しない場合は差分あり扱い (全カラム新規)
            err_code = getattr(e, 'response', {}).get('Error', {}).get('Code') if hasattr(e, 'response') else None
            if err_code in ('404', 'NoSuchKey') or 'Not Found' in str(e) or '404' in str(e):
                print(f"[Warn]-[updatecatalog]-[tablecolumns_diff_verify] "
                      f"対象日ファイルなし (初回想定) key={target_key} err={e} => 全カラム新規扱いで進行")
                missing_target = True
            else:
                print(f"[Error]-[updatecatalog]-[tablecolumns_diff_verify] "
                      f"対象日ファイル取得失敗 key={target_key} err={e}")
                raise
        if not missing_target:
            target_table = pq.read_table(target_local_path)
            target_cols = list(target_table.schema.names)
            os.remove(target_local_path)
        else:
            target_cols = []  # 全て新規として扱う
    else:
        base_cols = list(base_cols_override)
        target_cols = list(target_cols_override)

    # 差分（追加/削除）検出
    # 新規追加されたカラム: 基準日(base)にあり指定日(target)にない
    newly_added_columns = [c for c in base_cols if c not in target_cols]
    # 削除されたカラム: 指定日(target)にあり基準日(base)にない
    removed_columns = [c for c in target_cols if c not in base_cols]
    # どちらかのParquetが欠損なら、新規扱いとして差分ありにする
    diff_flag = (missing_base or missing_target) or bool(newly_added_columns or removed_columns)

    if diff_flag:
        print(
            f"[Warn]-[updatecatalog]-[tablecolumns_diff_verify] "
            f"カラム差分検出 table={table} "
            f"newly_added={newly_added_columns} removed={removed_columns}"
        )
    else:
        print(f"[Info]-[updatecatalog]-[tablecolumns_diff_verify] 差分なし table={table}")

    return {
        'diff': diff_flag,
        'newly_added_columns': newly_added_columns,
        'removed_columns': removed_columns,
        'base_columns': base_cols,
        'target_columns': target_cols,
    }

### 特定のテーブルに対して指定日（target_day)を指定してのスキーマ差分比較の場合
### 差分有の場合：クローラー全走査
### 差分無の場合：増分走査
def specifiedday_diff_verify_and_runcrawler(table: str, base_s3_path: str, base_day: str, spec_day: str):
    """指定日のスキーマ差分を検証し走査種別を決定する簡易版。
    差分あり -> full_scan=True / 差分なし -> full_scan=False

    戻り値(list[str]): 起動したクローラ名のリスト
    """
    norm_base_day = base_day.replace('-', '')
    print(
        f"[Info]-[updatecatalog]-[specifiedday_diff_verify_and_runcrawler] "
        f"テーブル: {table} 指定日差分検証 base_day={norm_base_day} spec_day={spec_day}"
    )
    diff_result = tablecolumns_diff_verify(table, base_s3_path, norm_base_day, spec_day)
    diff_found = diff_result['diff']
    if diff_found:
        print(
            f"[Info]-[updatecatalog]-[specifiedday_diff_verify_and_runcrawler] "
            f"テーブル: {table} 差分あり "
            f"newly_added={diff_result['newly_added_columns']} "
            f"removed={diff_result['removed_columns']} => クローラー全走査"
        )
        run_result = catalog_scan(table, full_scan=True)
    else:
        print(
            f"[Info]-[updatecatalog]-[specifiedday_diff_verify_and_runcrawler] "
            f"テーブル: {table} 差分なし => 増分走査"
        )
        run_result = catalog_scan(table, full_scan=False)
    print(
        f"[Info]-[updatecatalog]-[specifiedday_diff_verify_and_runcrawler] "
        f"クローラ起動完了 crawler={run_result['crawler_name']}"
    )
    return [run_result['crawler_name']]

### 各テーブルの前日差分比較の場合
### 差分有の場合：クローラー全走査
### 差分無の場合：増分走査
def prevday_diff_verify_and_runcrawler(tablelist: str, base_s3_path: str, base_date: str):
    """前日との差分を各テーブルで検証し、差分有無に応じて走査種別を切替。

        戻り値: list[str]
            起動したクローラ名のリスト
    """
    # テーブルに紐づくクローラー名リスト
    run_crawler_list = []
    # 各テーブルで前日差分検証とクローラー起動を実施
    for table in tablelist.split(','):
        print(
            f"[Info]-[updatecatalog]-[prevday_diff_verify_and_runcrawler] "
            f"テーブル: {table} の前日差分検証を実施します。"
        )
        base_day = base_date.replace("-", "")
        target_day = (pd.to_datetime(base_date) - pd.Timedelta(days=1)).strftime('%Y%m%d')
        diff_result = tablecolumns_diff_verify(table, base_s3_path, base_day, target_day)
        diff_found = diff_result['diff']
        if diff_found:
            print(
                f"[Info]-[updatecatalog]-[prevday_diff_verify_and_runcrawler] "
                f"テーブル: {table} 差分あり "
                f"newly_added={diff_result['newly_added_columns']} "
                f"removed={diff_result['removed_columns']} => クローラー全走査"
            )
            run_result = catalog_scan(table, full_scan=True)
        else:
            print(f"[Info]-[updatecatalog]-[prevday_diff_verify]テーブル: {table} 差分なし => 増分走査")
            run_result = catalog_scan(table, full_scan=False)
        print(
            f"[Info]-[updatecatalog]-[prevday_diff_verify] "
            f"クローラ起動完了 crawler={run_result['crawler_name']}"
        )
        run_crawler_list.append(run_result['crawler_name'])

    print(f"[Info]-[updatecatalog]-[prevday_diff_verify] crawler_list={run_crawler_list}")

    return run_crawler_list

### カタログ更新処理メイン
## 引数
# event: イベントデータ（辞書形式）
# 例）　{"exec_type": "prevdif"|"specdif"|"fulscan",
#          "targettable": "テーブル名",
#          "specdif_targetday": "yyyymmdd"（specdif時のみ必須）}
# context: コンテキスト情報（未使用）※基本ECSでの実行だが、Lambda互換の引数形式を想定
## 必須環境変数
# GROUP: カタログ化対象グループ名
def updatecatalog(event, context):
    # 実行種別を判定
    exectype = event.get('exec_type')
    if exectype is None:
        print("[Error]-[updatecatalog]-[InvalidInput]"
              "exec_type パラメータが未設定")
        return json.dumps({'status': 'failed'})

    # prevdivの場合、グループ内の全テーブルのスキーマ変更有無について前日差分検証を実施のうえ増分走査
    # specdifの場合、指定テーブルのスキーマ変更有無について指定日の差分検証を実施のうえ増分走査
    # fulscanの場合、指定テーブルを全走査
    if exectype not in ['prevdif', 'specdif', 'fulscan']:
        print("[Error]-[updatecatalog]-[InvalidInput]"
              "exec_type パラメータが不正です。'prevdif', 'specdif', 'fulscan' を指定してください。")
        return json.dumps({'status': 'failed'})

    # sepcdifの場合、基準日と比較する日付が必要
    if exectype == 'specdif':
        spectd = event.get('specdif_targetday')
        targettable = event.get('targettable')
        if spectd is None:
            print("[Error]-[updatecatalog]-[InvalidInput]"
                  "specdif_targetday パラメータが未設定")
            return json.dumps({'status': 'failed'})
        # 日付形式チェック（yyyymmdd）
        if not isinstance(spectd, str) or len(spectd) != 8 or not spectd.isdigit():
            print("[Error]-[updatecatalog]-[InvalidInput]"
                  "specdif_targetday パラメータの形式が不正です。'yyyymmdd' の形式で指定してください。")
            return json.dumps({'status': 'failed'})
        # 正しい日付かどうかのチェック
        dtcheck = pd.to_datetime(spectd, format='%Y%m%d', errors='coerce')
        if pd.isna(dtcheck):
            print("[Error]-[updatecatalog]-[InvalidInput]"
                  "specdif_targetday パラメータの値が不正な日付です。正しい日付を指定してください。")
            return json.dumps({'status': 'failed'})
        # 指定テーブル有無チェック
        if targettable is None:
            print("[Error]-[updatecatalog]-[InvalidInput]"
                  "targettable パラメータが未設定")
            return json.dumps({'status': 'failed'})

    # fulscanの場合、指定テーブルが必要
    if exectype == 'fulscan':
        targettable = event.get('targettable')
        if targettable is None:
            print("[Error]-[updatecatalog]-[InvalidInput]"
                  "targettable パラメータが未設定")
            return json.dumps({'status': 'failed'})

    # カタログ化対象グループの環境変数取得
    group = os.getenv('GROUP')
    if group is None:
        print("[Error]-[updatecatalog]-[GROUP-NotFound]"
              "GROUP 環境変数が未設定")
        return json.dumps({'status': 'failed'})

    # parameterストアから必要な値を取得
    ssm = boto3.client('ssm')
    # 取得対象テーブル（カタログ化対象テーブル一覧, カンマ区切り）
    tablelist = ssm.get_parameter(Name=f'/m365/common/{group}/targettable',
                                  WithDecryption=False)['Parameter']['Value']
    bucket_name = ssm.get_parameter(Name='/m365/common/s3bucket',
                                    WithDecryption=False)['Parameter']['Value']
    convert_key = ssm.get_parameter(Name='/m365/common/pipelineconv',
                                    WithDecryption=False)['Parameter']['Value']
    # S3バケットとキーのベース部分を組み立て
    base_s3_path = f"s3://{bucket_name}/{group}/{convert_key}/"
    # 基準日取得(yyyy-mm-dd)
    s3_client = boto3.client('s3')
    csv_file = s3_client.get_object(
        Bucket=bucket_name,
        Key="basedatetime/basedatetime.csv"
        )
    csv_file_body = csv_file['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(csv_file_body), usecols=['base'])
    base_date = df['base'].iloc[0]

    # カタログ更新処理呼び出し
    if exectype == 'prevdif':
        run_crawler_list = prevday_diff_verify_and_runcrawler(tablelist, base_s3_path, base_date)
        complite_results = wait_crawler_completion(run_crawler_list)
        print(f"[Info]-[updatecatalog] カタログ更新処理完了結果: {complite_results}")
    elif exectype == 'specdif':
        run_crawler_list = specifiedday_diff_verify_and_runcrawler(targettable, base_s3_path, base_date, spectd)
        complite_result = wait_crawler_completion(run_crawler_list)
        print(
            f"[Info]-[updatecatalog] "
            f"指定テーブル・指定日によるカタログ更新処理完了結果: {complite_result}"
        )
    elif exectype == 'fulscan':
        run_result = catalog_scan(targettable, full_scan=True)
        complite_result = wait_crawler_completion([run_result['crawler_name']])
        print(
            f"[Info]-[updatecatalog] "
            f"指定テーブルによるカタログ全更新処理完了結果: {complite_result}"
        )
    return json.dumps({'status': 'success'})
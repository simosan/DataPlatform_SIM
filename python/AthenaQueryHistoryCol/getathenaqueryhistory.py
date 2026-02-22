# AthenaからAthenaクエリ履歴を取得する。
# Athenaワークグループ単位で取得する。
# 取得Athenaワークグループは、/m365/athenabillingmetrics/workgroups から取得する。
# 基準日はJSTで指定するが、AthenaはUTCで指定する必要があるので変換する。
# S3の出力はM365CollectS3Export関数（Lambda）を呼び出して行う。
# 出力形式は以下の通り。
'''
{
    "QueryExecutionId": "クエリ実行ID",
    "Query": "クエリ内容",
    "Status": "クエリ実行状態",
    "SubmissionTime": "クエリ開始時刻 2026-02-13T22:04:01.217000+09:00（JST表記）",
    "CompletionTime": "クエリ終了時刻 2026-02-13T22:04:02.584000+09:00（JST表記）",
    "WorkGroup": "Athenaワークグループ名",
    "Database": "Glue Data Catalogのデータベース名",
    "Catalog": "Glue Data Catalogのカタログ名",
    "BytesScanned": "スキャンしたバイト数",
    "ExecutionTimeMillis": "クエリ実行時間（ミリ秒）",
    "EngineExecutionTimeMillis": "クエリエンジン実行時間（ミリ秒）",
    "EngineVersion": "Athenaエンジンバージョン"
},
{...}
'''
import boto3
import json
import re
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo
import pandas as pd
from io import StringIO

JST = ZoneInfo("Asia/Tokyo")

# Athenaのクエリ実行時間はJSTで指定された基準日（basedate）に基づいて、UTCの開始日時と終了日時を計算する必要がある。
# naの場合は基準日ファイルをS3から取得する。ここで取得する基準日とS3に書き込む際に使用する基準日は用途が異なることに注意。
# S3に出力する基準日は、M365CollectS3Export関数でファイルの保存先を決めるためのもので、Athenaクエリ履歴の取得期間を決めるものではない。
def _resolve_base_date(basedate: str, session: boto3.Session) -> date:
    if basedate == "na":
        # 基準日ファイルから基準日を取得する
        ssm = session.client('ssm')
        bucket_name = ssm.get_parameter(Name='/m365/common/s3bucket',
                                    WithDecryption=False)['Parameter']['Value']
        s3_client = session.client('s3')
        try:
            csv_file = s3_client.get_object(
                Bucket=bucket_name,
                Key="basedatetime/basedatetime.csv"
            )
            csv_file_body = csv_file['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_file_body), usecols=['base'])
            base_date_str = df['base'].iloc[0]
            return datetime.strptime(base_date_str, "%Y-%m-%d").date()
        except Exception as e:
            print(f"[Func-ERROR]-[_resolve_base_date]-[S3ReadError] 基準日ファイル取得エラー: {e}")
            raise
    else:
        return datetime.strptime(basedate, "%Y-%m-%d").date()


def _resolve_time_range(basedate: str, session: boto3.Session) -> tuple[datetime, datetime, date]:
    # basedate を基準に JST の 00:00 -> 翌 00:00 の期間を UTC に変換して返す
    # UTC変換後の日時はAthena呼び出しに使用する
    base_date_jst = _resolve_base_date(basedate, session)
    start_jst = datetime(base_date_jst.year, base_date_jst.month, base_date_jst.day, \
                        0, 0, 0, tzinfo=JST)
    end_jst = start_jst + timedelta(days=1)
    return (
        start_jst.astimezone(timezone.utc),
        end_jst.astimezone(timezone.utc),
        base_date_jst,
    )

# SSMから取得したワークグループ文字列（CSV）をリストに変換する関数
def _parse_workgroups(value: str) -> list[str]:
    raw = (value or "").strip()
    if not raw:
        return []

    # SSM StringList は "a,b,c" の形式が多い。JSON配列も許容。
    if raw.startswith("["):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return [str(x).strip() for x in parsed if str(x).strip()]
        except Exception:
            pass

    return [x.strip() for x in raw.split(",") if x.strip()]

## 既存S3キー削除関数（M365CollectS3KeyDelete） 呼び出し
def call_collect_S3KeyDelete(targetdataname, group, basedate):
    """
    S3上の指定キーを削除する関数
    Args:
        targetdataname (str): テーブル名
        group (str): グループ名
        basedate (str): 基準日（na or yyyy-mm-dd形式）
    Returns:
        list: 削除したファイルのリスト
    """

    lambda_client = boto3.client('lambda')

    payload = {
        "targetdataname": targetdataname,
        "group": group,
        "basedate": basedate
    }
    response = lambda_client.invoke(
        FunctionName='M365CollectS3KeyDeleteVpc',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = response['Payload'].read().decode('utf-8')
    if response.get('FunctionError'):
        print(f"[Func-ERROR]-[call_collect_S3KeyDelete]-[LambdaResponseError] {response_payload}")
        raise

    try:
        result = json.loads(response_payload) if response_payload else None
    except Exception:
        print(f"[Func-ERROR]-[call_collect_S3KeyDelete]-[InvalidJSON] {response_payload}")
        raise

    # エラー処理
    is_success = False
    if isinstance(result, dict):
        is_success = (result.get('status') == "success") or (result.get('statusCode') == 200)
    elif isinstance(result, list):
        is_success = any(
            (isinstance(x, dict) and x.get('status') == "success")
            or (isinstance(x, str) and '"status"' in x and 'success' in x)
            for x in result
        )
    else:
        is_success = bool(response_payload and '"status"' in response_payload and 'success' in response_payload)

    if not is_success:
        print(f"[Func-ERROR]-[call_collect_S3KeyDelete]-[UnexpectedPayload] {response_payload}")
        raise


# main関数
def get_athena_query_history(event, context):
    # リカバリ用に関数入力パラメータから基準日を取得（未使用：na, リカバリ用：yyyy-mm-dd形式）
    if event.get('basedate') is None or event.get('basedate') == "na":
        basedate = "na"
    else:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', event['basedate']):
            print(f"[Func-ERROR]-[get_athena_query_history]-[InvalidInput]"
                  "basedate: {event['basedate']} basedateの形式が不正です。'yyyy-mm-dd'の形式で指定してください。")
            return json.dumps({ "status": "failed" })

        basedate = event['basedate']

    session = boto3.Session()
    SSM_WORKGROUPS_PARAM = "/m365/athenabillingmetrics/workgroups"

    # parameterストアからAthenaワークグループ一覧を取得
    ssm = session.client('ssm')
    workgroups_raw = ssm.get_parameter(Name=SSM_WORKGROUPS_PARAM,
                                       WithDecryption=False)['Parameter']['Value']
    workgroups = _parse_workgroups(workgroups_raw)
    if not workgroups:
        print(f"[Func-ERROR]-[get_athena_query_history]- \
                        [InvalidConfig] {SSM_WORKGROUPS_PARAM} is empty")
        return json.dumps({ "status": "failed" })

    # データ取得対象グループ,テーブル指定
    group = event.get("group")
    if not group:
        print(f"[Func-ERROR]-[get_athena_query_history]-[InvalidInput] group is required.")
        return json.dumps({ "status": "failed" })
    targetdataname = "athenaqueryhistory"

    # 冪等性確保のため、ファイル上書きではなく、上位キーを削除する　
    try:
        call_collect_S3KeyDelete(
            targetdataname,
            group,
            basedate,
        )
    except Exception as e:
        print(f"[Func-ERROR]-[get_athena_query_history]-[S3KeyDelete] {e}")
        return json.dumps({ "status": "failed" })

    # 基準日からUTCの開始日時、終了日時を計算する
    try:
        start_utc, end_utc, base_date_jst = _resolve_time_range(basedate, session)
    except Exception as e:
        print(f"[Func-ERROR]-[get_athena_query_history] 基準日変換処理エラー: {e}")
        return json.dumps({ "status": "failed" })

    # na の場合は欠落させる仕様（出力先Lambda側でチェック,設定）
    fromtimestamp = None
    totimestamp = None
    if basedate != "na":
        fromtimestamp = f"{base_date_jst.strftime('%Y-%m-%d')} 00:00"
        totimestamp = f"{base_date_jst.strftime('%Y-%m-%d')} 23:59"

    export_payload = {
        "is_gzip": False,
        "targetdataname": targetdataname,
        "group": group,
        "basedate": basedate,
    }
    if fromtimestamp is not None and totimestamp is not None:
        export_payload["fromtimestamp"] = fromtimestamp
        export_payload["totimestamp"] = totimestamp

    # Athenaクエリ履歴の取得とS3出力（M365CollectS3Export関数呼び出し）を実行
    try:
        athena = session.client("athena")
        lambda_client = session.client('lambda')

        query_results: list[dict] = []

        for wg in workgroups:
            next_token = None
            stop_paging = False

            while True:
                params: dict = {
                    "WorkGroup": wg,
                    "MaxResults": 50,
                }
                if next_token:
                    params["NextToken"] = next_token

                resp = athena.list_query_executions(**params)

                for qid in resp.get("QueryExecutionIds", []):
                    qexec = athena.get_query_execution(QueryExecutionId=qid).get("QueryExecution", {})
                    status = qexec.get("Status", {})
                    submission_time = status.get("SubmissionDateTime")
                    if not submission_time:
                        continue

                    # list_query_executions は新しい順で返る前提で、開始日時より古くなったら打ち切る
                    if submission_time < start_utc:
                        stop_paging = True
                        break
                    if submission_time >= end_utc:
                        continue

                    completion_time = status.get("CompletionDateTime")
                    context_info = qexec.get("QueryExecutionContext", {})
                    stats = qexec.get("Statistics", {})

                    record = {
                        "QueryExecutionId": qexec.get("QueryExecutionId", qid),
                        "Query": qexec.get("Query", ""),
                        "Status": status.get("State", ""),
                        "SubmissionTime": submission_time.astimezone(JST).isoformat(),
                        "CompletionTime": completion_time.astimezone(JST).isoformat() if completion_time else None,
                        "WorkGroup": qexec.get("WorkGroup", wg),
                        "Database": context_info.get("Database", ""),
                        "Catalog": context_info.get("Catalog", ""),
                        "BytesScanned": stats.get("DataScannedInBytes", 0),
                        "ExecutionTimeMillis": stats.get("TotalExecutionTimeInMillis", 0),
                        "EngineExecutionTimeMillis": stats.get("EngineExecutionTimeInMillis", 0),
                        "EngineVersion": qexec.get("EngineVersion", {}).get("SelectedEngineVersion", ""),
                    }
                    query_results.append(record)

                    # stdout: 1件ずつ標準出力（CloudWatch Logsに保存される）
                    print(json.dumps(record, ensure_ascii=False, default=str))

                if stop_paging:
                    break

                next_token = resp.get("NextToken")
                if not next_token:
                    break

        payload = dict(export_payload)
        payload["body"] = query_results

        response = lambda_client.invoke(
            FunctionName='M365CollectS3ExportVpc',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload, ensure_ascii=False, default=str),
        )
        response_payload = response['Payload'].read().decode('utf-8')
        if response.get('FunctionError'):
            print(f"[Func-ERROR]-[get_athena_query_history]-[M365CollectS3ExportVpc] {response_payload}")
            return json.dumps({ "status": "failed" })

        _ = json.loads(response_payload) if response_payload else {"status": "unknown"}

    except Exception as e:
        print(f"[Func-ERROR]-[get_athena_query_history]-[export-invoke] {e}")
        return { "status": "failed" }

    return json.dumps({ "status": "success" })

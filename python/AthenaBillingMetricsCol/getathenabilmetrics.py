# CloudwatchからAthenaメトリクス（ProcessedBytes）を取得する。
# Athenaワークグループ単位で取得する。
# 基準日はJSTで指定するが、CloudwatchはUTCで指定する必要があるので変換する。
# S3の出力はM365CollectS3Export関数（Lambda）を呼び出して行う。
# 出力形式は以下の通り。
'''
  {
    "workgroup": "AthenaWorkGroup名",
    "basedate_jst": "yyyy-mm-dd（基準日)",
    "period_seconds": 86400（1日分）,
    "start_utc": "2025-09-04T15:00:00Z（集計対象開始日時：UTC）",
    "end_utc": "2025-09-05T15:00:00Z"(集計対象終了日時：UTC）",
    "consumed_bytes_per_workgroup": "Athenaワークグループで消費したバイト数",
    "consumed_tb_per_workgroup": "Athenaワークグループで消費したテラバイト数",
    "usd_per_tb": "1TBあたりのUSD単価",
    "usd": "Athenaワークグループで消費したUSD金額"
  }
'''

import boto3
import json
import re
from datetime import datetime, timedelta, timezone, date
from decimal import Decimal, ROUND_HALF_UP
from zoneinfo import ZoneInfo


JST = ZoneInfo("Asia/Tokyo")
TB_IN_BYTES = Decimal(1024) ** 4  # 1024^4 = 1099511627776
USD_QUANTIZE_10DP = Decimal("0.0000000000")

def _resolve_base_date(basedate: str) -> date:
    if basedate == "na":
        return datetime.now(JST).date() - timedelta(days=1)
    return datetime.strptime(basedate, "%Y-%m-%d").date()


def _resolve_time_range(basedate: str) -> tuple[datetime, datetime, date]:
    # basedate を基準に JST の 00:00 -> 翌 00:00 の期間を UTC に変換して返す
    # UTC変換後の日時はcloudwatch呼び出しに使用する
    base_date_jst = _resolve_base_date(basedate)
    start_jst = datetime(base_date_jst.year, base_date_jst.month, base_date_jst.day, \
                        0, 0, 0, tzinfo=JST)
    end_jst = start_jst + timedelta(days=1)
    return (
        start_jst.astimezone(timezone.utc),
        end_jst.astimezone(timezone.utc),
        base_date_jst,
    )


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


def _sum_processed_bytes(datapoints: list[dict]) -> Decimal:
    total = Decimal(0)
    # 通常はワークグループに対してdatapointsは1件のみだが、
    # get_metric_statisticsの仕様上複数件返る可能性があるため合算する
    for dp in datapoints or []:
        v = dp.get("Sum")
        if v is None:
            continue
        total += Decimal(str(v))
    return total

# bytes to terabytes変換関数
def _bytes_to_tb(bytes_value: Decimal) -> Decimal:
    return (bytes_value / TB_IN_BYTES) if bytes_value else Decimal(0)

# USD計算関数
def _calc_usd(tb_value: Decimal, usd_per_tb: Decimal) -> Decimal:
    # 金額（USD）のみを返す。小数10桁に丸める。
    usd = (tb_value * usd_per_tb) if tb_value else Decimal(0)
    return usd.quantize(USD_QUANTIZE_10DP, rounding=ROUND_HALF_UP)


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
        FunctionName='M365CollectS3KeyDelete',
        InvocationType='RequestResponse',
        Payload=json.dumps(payload)
    )
    response_payload = response['Payload'].read().decode('utf-8')
    if response.get('FunctionError'):
        print(f"[func-error]-[call_collect_S3KeyDelete]-[LambdaResponseError] {response_payload}")
        raise Exception("[func-error]-[call_collect_S3KeyDelete] S3キー削除Lambdaがエラー応答しました。")

    try:
        result = json.loads(response_payload) if response_payload else None
    except Exception:
        result = None

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
        print(f"[func-error]-[call_collect_S3KeyDelete]-[UnexpectedPayload] {response_payload}")
        raise Exception("[func-error]-[call_collect_S3KeyDelete] S3キー削除に失敗しました。")


# main関数
def get_athena_billing_metrics(event, context):
    # リカバリ用に関数入力パラメータから基準日を取得（未使用：na, リカバリ用：yyyy-mm-dd形式）
    if event.get('basedate') is None or event.get('basedate') == "na":
        basedate = "na"
    else:
        if not re.match(r'^\d{4}-\d{2}-\d{2}$', event['basedate']):
            raise ValueError("[Func-Error]-[get_athena_billing_metrics]-[InvalidInput]"
                             "basedateの形式が不正です。'yyyy-mm-dd'の形式で指定してください。")
        basedate = event['basedate']

    session = boto3.Session()
    SSM_WORKGROUPS_PARAM = "/m365/athenabillingmetrics/workgroups"

    # parameterストアからAthenaワークグループ一覧を取得
    ssm = session.client('ssm')
    workgroups_raw = ssm.get_parameter(Name=SSM_WORKGROUPS_PARAM,
                                       WithDecryption=False)['Parameter']['Value']
    workgroups = _parse_workgroups(workgroups_raw)
    if not workgroups:
        raise ValueError(f"[Func-Error]-[get_athena_billing_metrics]- \
                        [InvalidConfig] {SSM_WORKGROUPS_PARAM} is empty")

    # データ取得対象グループ,テーブル
    group = "group1"
    targetdataname = "athenabillingmetrics"

    # 冪等性確保のため、ファイル上書きではなく、上位キーを削除する　
    call_collect_S3KeyDelete(
        targetdataname,
        group,
        basedate,
    )

    # CloudWatchからAthenaメトリクスを取得
    start_utc, end_utc, base_date_jst = _resolve_time_range(basedate)

    # na の場合は欠落させる仕様（出力先Lambda側でチェック,設定）
    fromtimestamp = None
    totimestamp = None
    if basedate != "na":
        fromtimestamp = f"{base_date_jst.strftime('%Y-%m-%d')} 00:00"
        totimestamp = f"{base_date_jst.strftime('%Y-%m-%d')} 23:59"

    cloudwatch = session.client('cloudwatch')
    per_workgroup_results: list[dict] = []
    total_bytes_all_workgroups = Decimal(0)

    export_payload = {
        "is_gzip": False,
        "targetdataname": targetdataname,
        "group": group,
        "basedate": basedate,
    }
    if fromtimestamp is not None and totimestamp is not None:
        export_payload["fromtimestamp"] = fromtimestamp
        export_payload["totimestamp"] = totimestamp

    # 固定値
    ATHENA_NAMESPACE = "AWS/Athena"
    ATHENA_METRIC_NAME = "ProcessedBytes"
    PERIOD_SECONDS = 86400  # 1日（24時間）
    USD_PER_TB_PARAM = "/m365/athenabillingmetrics/usd_per_tb"
    USD_PER_TB = Decimal(ssm.get_parameter(Name=USD_PER_TB_PARAM,
                                WithDecryption=False)['Parameter']['Value'])

    try:
        lambda_client = session.client('lambda')

        for wg in workgroups:
            resp = cloudwatch.get_metric_statistics(
                Namespace=ATHENA_NAMESPACE,
                MetricName=ATHENA_METRIC_NAME,
                StartTime=start_utc,
                EndTime=end_utc,
                Period=PERIOD_SECONDS,
                Statistics=['Sum'],
                Dimensions=[{'Name': 'WorkGroup', 'Value': wg}],
            )
            # respの形式は以下
            # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/cloudwatch/client/get_metric_statistics.html#
            bytes_per_workgroup = _sum_processed_bytes(resp.get('Datapoints', []))
            total_bytes_all_workgroups += bytes_per_workgroup
            tb_per_workgroup = _bytes_to_tb(bytes_per_workgroup)
            usd_per_workgroup = _calc_usd(tb_per_workgroup, USD_PER_TB)

            record = {
                "workgroup": wg,
                "basedate_jst": base_date_jst.strftime("%Y-%m-%d"),
                "period_seconds": PERIOD_SECONDS,
                "start_utc": start_utc.isoformat().replace("+00:00", "Z"),
                "end_utc": end_utc.isoformat().replace("+00:00", "Z"),
                "consumed_bytes_per_workgroup": int(bytes_per_workgroup.to_integral_value(rounding=ROUND_HALF_UP)),
                "consumed_tb_per_workgroup": float(tb_per_workgroup),
                "usd_per_tb": float(USD_PER_TB),
                "usd": format(usd_per_workgroup, "f"),
            }
            per_workgroup_results.append(record)

            # stdout: ワークグループ単位で標準出力（CloudWatch Logsに保存される）
            print(json.dumps(record, ensure_ascii=False, default=str))

        # S3 export: 複数WorkGroupを1つのJSON（配列）として出力
        payload = dict(export_payload)
        payload["body"] = per_workgroup_results

        response = lambda_client.invoke(
            FunctionName='M365CollectS3Export',
            InvocationType='RequestResponse',
            Payload=json.dumps(payload, ensure_ascii=False, default=str),
        )
        response_payload = response['Payload'].read().decode('utf-8')
        if response.get('FunctionError'):
            print(f"[func-error]-[get_athena_billing_metrics]-[M365CollectS3Export] {response_payload}")
            return {
                "statusCode": 500,
                "message": "M365CollectS3Export failed",
                "detail": response_payload,
            }
        export_result = json.loads(response_payload) if response_payload else {"status": "unknown"}
    except Exception as e:
        print(f"[func-error]-[get_athena_billing_metrics]-[export-invoke] {e}")
        return {
            "statusCode": 500,
            "message": f"Error invoking M365CollectS3Export: {str(e)}",
        }

    return {
        "statusCode": 200,
        "status": "success",
    }
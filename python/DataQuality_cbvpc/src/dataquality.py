import json
import os
import boto3
import time
import sys


def load_ruleset_from_s3(tablelist):
    """
    S3に格納された対象テーブルの全ルールセット(JSON)を読み込み、
    テーブル名をキーとした辞書で返却する。

    Parameters
    - tablelist: 例 "m365getgroup,m365getuser" のようなカンマ区切り文字列

        Returns
        - {
                "rulesets": [
                    {
                        "database": str,
                        "table": str,
                        "ruleset_name": str,
                        "rules": list[str]
                    }, ...
                ]
            }
    """
    ssm = boto3.client('ssm')
    bucket = ssm.get_parameter(Name=f'/m365/common/s3bucket',
                               WithDecryption=False)['Parameter']['Value']

    s3 = boto3.client("s3")
    combined = {"rulesets": []}
    tables = [t.strip() for t in tablelist.split(',') if t.strip()]

    for table in tables:
        key = f"rulesets/{table}_ruleset.json"
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
        except Exception as e:
            # 指定キー（テーブルに紐づくルールセット）が存在しない場合はスキップ
            response = getattr(e, "response", None)
            err = response.get("Error", {})
            code = err.get("Code")
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if code in {"NoSuchKey", "NotFound", "404"} or status == 404:
                print(
                    f"[Info]-[dataquality]-[load_ruleset_from_s3] ルール定義が存在しないためスキップ: table={table} key={key}"
                )
                continue
            raise
        ruleset = json.loads(obj["Body"].read().decode("utf-8"))

        # スキーマ正規化
        normalized = {
            "database": ruleset.get("database"),
            "table": ruleset.get("table"),
            "ruleset_name": ruleset.get("ruleset_name"),
            "rules": ruleset.get("rules"),
        }

        combined["rulesets"].append(normalized)

    return combined


def upsert_ruleset(ruleset_json):
    """
    Glue Data Quality RuleSet を常に再作成する。
    - `ruleset_json` に以下のキーがある前提:
        - `ruleset_name`: ルールセット名
        - `rules`: list[str] もしくは
        - `database`
        - `table`
    """
    glue = boto3.client('glue')
    name = ruleset_json["ruleset_name"]
    rules = ruleset_json["rules"]
    # ルールセット文字列をそのまま利用し、Rulesにセット
    trimmed = [str(r).strip() for r in rules if r is not None and str(r).strip()]
    body = ", ".join(trimmed)
    ruleset_text = f"Rules = [ {body} ]" if trimmed else "Rules = []"
    # ログ出力（簡易）: ルールセット名のみ
    print(f"[Info]-[dataquality]-[upsert_ruleset] ルールセットを作成: {name}")

    # 作成（既存ルールが存在する場合は失敗。既存削除→再作成）
    try:
        table_name = ruleset_json.get('table')
        db_name = ruleset_json.get('database')

        payload = {
            'Name': name,
            'Description': f"Auto-managed ruleset for {ruleset_json.get('table','unknown')}",
            'Ruleset': ruleset_text,
        }
        if table_name and db_name:
            payload['TargetTable'] = {
                'TableName': table_name,
                'DatabaseName': db_name
            }

        glue.create_data_quality_ruleset(**payload)
        print(f"[Info]-[dataquality]-[upsert_ruleset] ルールセットを作成: {name}")
    except glue.exceptions.AlreadyExistsException:
        try:
            glue.delete_data_quality_ruleset(Name=name)
            print(f"[Info]-[dataquality]-[upsert_ruleset] 既存のルールセットを削除: {name}")
        except Exception as de:
            print(f"[Warn]-[dataquality]-[upsert_ruleset] 既存のルールセット削除でエラー発生: err={de}")
        # 再作成
        table_name = ruleset_json.get('table')
        db_name = ruleset_json.get('database')
        payload = {
            'Name': name,
            'Description': f"Auto-managed ruleset for {ruleset_json.get('table','unknown')}",
            'Ruleset': ruleset_text,
        }
        if table_name and db_name:
            payload['TargetTable'] = {
                'TableName': table_name,
                'DatabaseName': db_name
            }
        glue.create_data_quality_ruleset(**payload)
        print(f"[Info]-[dataquality]-[upsert_ruleset] ルールセットを再作成: {name}")

    # ここまでで作成/再作成を完了
    return name


def run_dq(database, table, ruleset_name):
    """指定のテーブル・ルールセット名でDQを1件起動し、RunIdを返す"""
    glue = boto3.client('glue')
    # Glueサービスロールを活用して実行するため、当該RoleArnを取得
    ssm = boto3.client("ssm")
    param = ssm.get_parameter(Name="/m365/common/glue/dq_role_arn", WithDecryption=False)
    if param is None:
        print(f"[Error]-[dataquality]-[run_dq] Glue DQ用のRoleArnが取得できませんでした")
        sys.exit(255)

    role_arn = param["Parameter"]["Value"]
    payload = {
        "DataSource": {
            "GlueTable": {
                "DatabaseName": database,
                "TableName": table,
            }
        },
        "RulesetNames": [ruleset_name],
        "Role": role_arn,
    }

    response = glue.start_data_quality_ruleset_evaluation_run(**payload)
    run_id = response["RunId"]
    print(f"[Info]-[dataquality]-[run_dq] "
          f"Started DQ run: {run_id} for {database}.{table} using {ruleset_name}")
    return run_id


def get_dq_run_status(run_id):
    """DQ Runの現在ステータスを取得して返す"""
    glue = boto3.client('glue')
    result = glue.get_data_quality_ruleset_evaluation_run(RunId=run_id)
    return {
        "run_id": run_id,
        "status": result["Status"],
        "executiontime": result.get("ExecutionTime"),
        "result_ids": result.get("ResultIds"),
    }


def wait_for_dq_runs(run_ids, poll_interval=30, timeout=10800):
    """
    複数RunIdの完了を待機する。各Runの最終ステータスと要約を返す。
    timeout秒で打ち切り。
    """
    start = time.time()
    final = {}
    remaining = set(run_ids)
    while remaining:
        if time.time() - start > timeout:
            # タイムアウト扱い
            for rid in list(remaining):
                final[rid] = {"run_id": rid, "status": "TIMEOUT", "result": None}
                remaining.remove(rid)
            break

        done = []
        for rid in remaining:
            info = get_dq_run_status(rid)
            status = info["status"]
            # Glue DQ のステータスは 'STARTING'|'RUNNING'|'STOPPING'|'STOPPED'|'SUCCEEDED'|'FAILED'|'TIMEOUT'
            # 完了状態の場合は結果格納
            if status in ["SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED"]:
                final[rid] = info
                done.append(rid)
        for rid in done:
            remaining.remove(rid)
        if remaining:
            time.sleep(poll_interval)

    return final

def get_dq_run_result(result_id):
    """DQ Runの結果を取得し、必要項目を抽出して返す

    戻り値: List[Dict] 形式で、各ルールの以下項目を含む
      - RulesetName
      - Name
      - Description
      - Result
      - EvaluationMessage (存在する場合)
    """
    glue = boto3.client('glue')
    result = glue.get_data_quality_result(ResultId=result_id)
    ruleset_name = result.get("RulesetName")
    rule_results = result.get("RuleResults") or []
    extracted = []
    for rr in rule_results:
        extracted.append({
            "RulesetName": ruleset_name,
            "Name": rr.get("Name"),
            "Description": rr.get("Description"),
            "Result": rr.get("Result"),
            "EvaluationMessage": rr.get("EvaluationMessage"),
        })
    return extracted



def dataquality(input_event):
    # GROUP取得
    group = input_event.get("group")
    if not group:
        print(f"[Error]-[dataquality]-[dataquality] GROUP パラメータが未設定")
        return json.dumps({"status": "failed"})

    # GROUP内のテーブル情報取得
    ssm = boto3.client('ssm')
    tablelist = ssm.get_parameter(Name=f'/m365/common/{group}/targettable',
                                  WithDecryption=False)['Parameter']['Value']
    if not tablelist:
        print(f"[Error]-[dataquality]-[dataquality] GROUP={group} の対象テーブル情報が取得できません")
        return json.dumps({"status": "failed"})

    # ルールセット群をS3から取得し、結合
    combined_rulesets = load_ruleset_from_s3(tablelist)

    # GlueのルールセットをS3上の定義を正として都度作成/更新
    for entry in combined_rulesets["rulesets"]:
        upsert_ruleset(entry)

    # 各テーブルのDQを起動
    run_ids = []
    for entry in combined_rulesets["rulesets"]:
        database = entry["database"]
        table = entry["table"]
        ruleset_name = entry["ruleset_name"]
        rid = run_dq(database, table, ruleset_name)
        run_ids.append({
            "run_id": rid,
            "database": database,
            "table": table,
            "ruleset_name": ruleset_name
        })

    # ステータス待機
    final_status_map = wait_for_dq_runs([r["run_id"] for r in run_ids])

    # 結果整形
    results = []
    for meta in run_ids:
        # 各RunIdの最終ステータス・結果取得
        info = final_status_map.get(meta["run_id"])
        if info.get("status") == "SUCCEEDED":
            print(f"[Info]-[dataquality]-[dataquality] DQ Run SUCCEEDED: "
                  f"runid: {info.get('run_id')},status: {info.get('status')},"
                  f"executiontime: {info.get('executiontime')}")
        else:
            print(f"[Error]-[dataquality]-[dataquality] DQ Run Failed/Timeout..etc: "
                  f"runid: {info.get('run_id')},status: {info.get('status')},"
                  f"executiontime: {info.get('executiontime')}")

        # RunDQのレポート出力
        # RunDQの結果は複数ResultId（配列）になる可能性があるため、全て標準出力に整形して出力する
        for result_id in info.get("result_ids", []):
            extracted_rules = get_dq_run_result(result_id)
            for er in extracted_rules:
                level = "Info" if er.get("Result") == "PASS" else "Error"
                print(
                    f"[{level}]-[dataquality]-[rule] "
                    f"Ruleset={er.get('RulesetName')} "
                    f"Name={er.get('Name')} "
                    f"Description={er.get('Description')} "
                    f"Result={er.get('Result')} "
                    f"EvaluationMessage={er.get('EvaluationMessage')}"
                )
    print(f"[Info]-[dataquality]-[dataquality] 全てのDQ Runが完了しました {results}")

    return json.dumps({"status": "success"})

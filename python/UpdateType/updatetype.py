import boto3
import json


def load_typejson_from_s3(bucket_name, key):
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
    except Exception as e:
        # 指定キーが存在しない場合はエラーを返す
        response = getattr(e, "response", None) or {}
        err = response.get("Error", {}) if isinstance(response, dict) else {}
        code = err.get("Code")
        status = (
            response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if isinstance(response, dict)
            else None
        )
        if code in {"NoSuchKey", "NotFound", "404"} or status == 404:
            print(
                f"[Error]-[updatetype]-[load_typejson_from_s3] 型定義ファイルが存在しない: key={key}"
            )
            raise
        raise
    type_json = json.loads(obj["Body"].read().decode("utf-8"))
    return type_json


def updatetype(event, context):

    ssm = boto3.client("ssm")
    bucket_name = ssm.get_parameter(
        Name="/m365/common/s3bucket", WithDecryption=False
    )["Parameter"]["Value"]
    json_key = "datatype/updatetype.json"
    # 型更新ファイル読み込み
    try:
        type_json = load_typejson_from_s3(bucket_name, json_key)
    except Exception as e:
        print(
            f"[Error]-[updatetype] 型定義ファイルの読み込みに失敗: key={json_key} err={e}"
        )
        return json.dumps({"status": "failed"})

    if not isinstance(type_json, list):
        print(
            f"[Error]-[updatetype] 型定義JSONの形式が不正: expected=list actual={type(type_json)}"
        )
        return json.dumps({"status": "failed"})

    results = []
    glue = boto3.client("glue")
    for config in type_json:
        if not isinstance(config, dict):
            print(f"[Warn]-[updatetype] 定義がdictではないためスキップ: {config}")
            continue

        database_name = config.get("database")
        table_name = config.get("table")
        new_column_types = config.get("columns")

        if not database_name or not table_name or not isinstance(new_column_types, dict):
            print(
                f"[Warn]-[updatetype] 定義が不正のためスキップ: database={database_name} table={table_name} columns_type={type(new_column_types)}"
            )
            continue

        try:
            response = glue.get_table(DatabaseName=database_name, Name=table_name)
        except Exception as e:
            print(
                f"[Warn]-[updatetype] {database_name}.{table_name} - Failed to get table: {e}"
            )
            results.append(f"{database_name}.{table_name} failed(get_table)")
            continue

        # テーブル定義から更新対象のカラムを直接参照し、更新可能な状態にする
        table = response["Table"]
        storage_descriptor = table["StorageDescriptor"]
        former_columns = storage_descriptor.get("Columns", [])
        former_partition_keys = table.get("PartitionKeys", [])

        # パーティションキーとカラムの両方から名前を収集（パーティーションキーも型変更対象可能とするため）
        former_partition_key_names = {
            c.get("Name") for c in former_partition_keys if isinstance(c, dict)
        }
        former_column_names = {c.get("Name") for c in former_columns if isinstance(c, dict)}

        # 変更前のカラム有無チェック
        for col_name in new_column_types:
            if col_name not in former_column_names and col_name not in former_partition_key_names:
                print(
                    f"[Warn]-[updatetype] {database_name}.{table_name} - Column '{col_name}' テーブル内に存在しないためスキップ"
                )

        # カラム型変更
        for former_col in former_columns:
            if not isinstance(former_col, dict):
                continue

            col_name = former_col.get("Name")
            if col_name in new_column_types:
                former_col["Type"] = new_column_types[col_name]
                print(
                    f"[Info]-[updatetype] {database_name}.{table_name} - Column '{col_name}' type updated to {new_column_types[col_name]}"
                )

        # PartitionKeys の型も更新対象に含める（必要な場合）
        for former_col in former_partition_keys:
            if not isinstance(former_col, dict):
                continue

            col_name = former_col.get("Name")
            if col_name in new_column_types:
                former_col["Type"] = new_column_types[col_name]
                print(
                    f"[Info]-[updatetype] {database_name}.{table_name} - PartitionKey '{col_name}' type updated to {new_column_types[col_name]}"
                )

        table_input = {
            "Name": table["Name"],
            "Description": table.get("Description", ""),
            "Owner": table.get("Owner", ""),
            "Retention": table.get("Retention", 0),
            "StorageDescriptor": storage_descriptor,
            "PartitionKeys": former_partition_keys,
            "TableType": table.get("TableType"),
            "Parameters": table.get("Parameters", {})
        }

        # ビューの場合はViewOriginalTextとViewExpandedTextもtable_inputに追加
        view_original_text = table.get("ViewOriginalText")
        if view_original_text is not None:
            table_input["ViewOriginalText"] = view_original_text
        view_expanded_text = table.get("ViewExpandedText")
        if view_expanded_text is not None:
            table_input["ViewExpandedText"] = view_expanded_text
        target_table = table.get("TargetTable")
        if target_table is not None:
            table_input["TargetTable"] = target_table

        try:
            glue.update_table(
                DatabaseName=database_name,
                TableInput=table_input
            )
        except Exception as e:
            print(
                f"[Warn]-[updatetype] {database_name}.{table_name} - Failed to update table: {e}"
            )
            results.append(f"{database_name}.{table_name} failed(update_table)")
            continue

        results.append(f"{database_name}.{table_name} updated")

    print(f"[Info]-[updatetype] Update completed for tables: {results}")

    return json.dumps({"status": "success"})
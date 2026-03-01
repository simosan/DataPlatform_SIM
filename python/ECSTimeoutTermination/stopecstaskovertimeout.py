# ECSタスクにタイムアウト(秒）を設定し、タイムアウトしたタスクを停止するLambda関数
# 本Lambda関数はEventBridgeで定期的に実行されることを想定している
# 複数のECSタスクの状態を監視し、停止対象のタスクが複数ある場合は、まとめて停止する
# 停止処理は非同期で行い、すべての停止処理が完了するまで待機する
# タイムアウトの設定ファイルはS3（JSON形式）から取得する
# １関数あたり１クラスタのみ監視することを想定しているが、複数のタスクを監視することができる
# クラスタがに複数ある場合は、クラスタごとにLambda関数を作成することを想定している
'''
{
    "対象クラスタ名": [
        {
            "mon_task_name": "監視対象タスク名",
            "timeout_seconds": タイムアウト時間（秒）
        },
        {...}
    ]
}

'''
import boto3
import json
import re
import time
from datetime import datetime, timezone


# S3 URIをパースしてバケット名とキーを取得する
def _parse_s3_uri(s3_uri: str) -> tuple[str, str]:
    match = re.match(r"^s3://([^/]+)/(.+)$", s3_uri)
    if not match:
        print(f"[Error]-[_parse_s3_uri] Invalid S3 URI: {s3_uri}")
        raise
    return match.group(1), match.group(2)

# taskDefinitionArnからfamilyを抽出する
def _extract_taskdef_family(task_definition_arn: str) -> str | None:
    if not task_definition_arn:
        return None
    # ARN形式: arn:aws:ecs:region:account_id:task-definition/family:revision
    _TASKDEF_FAMILY_RE = re.compile(r"task-definition/(?P<family>[^:]+):(?P<rev>\d+)$")
    match = _TASKDEF_FAMILY_RE.search(task_definition_arn)
    if match:
        return match.group("family")

# タスクのタイムアウト判定
def _is_task_over_timeout(task: dict, timeout_seconds: int) -> bool:
    # タスクの起動時間を取得
    started_at = task.get('startedAt')
    if started_at is None:
        return False

    # 現在のUTC時間を取得
    now = datetime.now(timezone.utc)

    # タスクの起動時間からの経過時間を計算
    elapsed_time = (now - started_at).total_seconds()

    # 経過時間がタイムアウト時間を超えているか判定
    if elapsed_time > timeout_seconds:
        print(f"[Info]-[is_task_over_timeout]"
              f" Task {task['taskArn']} is over timeout: elapsed_time={elapsed_time} seconds")
        return True

    return False

# クラスタ内のすべてのRUNNINGタスクのARNを取得する
def _list_running_task_arns(ecs_client, cluster_name: str) -> list[str]:
    task_arns: list[str] = []
    paginator = ecs_client.get_paginator("list_tasks")
    for page in paginator.paginate(cluster=cluster_name, desiredStatus="RUNNING"):
        task_arns.extend(page.get("taskArns", []))
    return task_arns

# タスクARNのリストを受け取り、describe_tasks APIを呼び出してタスクの詳細情報を取得する
def _describe_tasks(ecs_client, cluster_name: str, task_arns: list[str]) -> list[dict]:
    tasks: list[dict] = []
    # describe_tasks は最大100件
    for i in range(0, len(task_arns), 100):
        chunk = task_arns[i : i + 100]
        resp = ecs_client.describe_tasks(cluster=cluster_name, tasks=chunk)
        tasks.extend(resp.get("tasks", []))
    return tasks

# タスクが監視対象かどうかを判定する
def _should_monitor_task(task: dict, mon_task_name: str) -> bool:
    family = _extract_taskdef_family(task.get("taskDefinitionArn", ""))
    group = task.get("group")

    if family and family == mon_task_name:
        return True
    if group and group == mon_task_name:
        return True
    if group and group == f"service:{mon_task_name}":
        return True
    return False


# タスクのタイムアウト判定と停止対象の収集
def _collect_tasks_over_timeout(ecs_client,
                                cluster_name: str,
                                monitor_items: list[dict],) -> list[str]:

    # クラスタ内のすべてのRUNNINGタスクを取得する
    # RUNNINGタスクがなければ呼出元に空リストを返す
    running_task_arns = _list_running_task_arns(ecs_client, cluster_name)
    if not running_task_arns:
        return []

    running_tasks = _describe_tasks(ecs_client, cluster_name, running_task_arns)
    to_stop: set[str] = set()

    for item in monitor_items:
        mon_task_name = item.get("mon_task_name")
        timeout_seconds = item.get("timeout_seconds")
        if not mon_task_name or timeout_seconds is None:
            print(f"[Error]-[_collect_tasks_over_timeout]"
                  f" 無効なモニター項目（フィールド不足）: {item}")
            raise
        try:
            timeout_seconds_int = int(timeout_seconds)
        except Exception as exc:
            print(f"[Error]-[_collect_tasks_over_timeout]"
                  f" 無効なtimeout_seconds（整数ではない）: {item}")
            raise

        # タスクごとに、監視対象かどうかを判定し、監視対象であればタイムアウト超過しているかを判定する
        for task in running_tasks:
            print(f"[Debug]-[_collect_tasks_over_timeout] Checking task: {task['taskArn']} "
                f"mon_task_name={mon_task_name} timeout_seconds={timeout_seconds_int}")
            if not _should_monitor_task(task, mon_task_name):
                continue
            if _is_task_over_timeout(task, timeout_seconds_int):
                print(f"[Info]-[_collect_tasks_over_timeout] タスクがタイムアウト超過で停止対象に追加: "
                    f"taskArn={task['taskArn']} mon_task_name={mon_task_name} timeout_seconds={timeout_seconds_int}")
                to_stop.add(task["taskArn"])

    return sorted(to_stop)


# タスク停止待ち（複数）
def _wait_for_tasks_stopped(ecs_client,
                            cluster_name: str,
                            target_list: list[str],
                            timeout_seconds: int,
                            poll_interval_seconds: int) -> bool:

    deadline = time.time() + timeout_seconds
    remaining: set[str] = set(target_list)

    while remaining:
        if time.time() >= deadline:
            print(
                "[Error]-[wait_for_tasks_stopped] タスク停止待ちのタイムアウトが発生 "
                f"remaining={len(remaining)}"
            )
            return False

        described = ecs_client.describe_tasks(cluster=cluster_name, tasks=list(remaining))
        tasks = described.get("tasks", [])
        failures = described.get("failures", [])

        stopped_now: set[str] = set()

        for t in tasks:
            if t.get("lastStatus") == "STOPPED":
                stopped_now.add(t.get("taskArn"))

        # describeで失敗(=見つからない)は停止済み扱い
        for f in failures:
            arn = f.get("arn")
            if arn:
                stopped_now.add(arn)

        remaining -= {a for a in stopped_now if a}

        if remaining:
            time.sleep(poll_interval_seconds)

    return True


# タスク停止処理（複数を順次）
def _stop_tasks_sequential(ecs_client,
                           cluster_name: str,
                           task_arns: list[str],
                          ) -> tuple[list[str], list[tuple[str, str]]]:
    if not task_arns:
        return [], []

    # 停止処理の結果を記録するリスト
    # succeeded: 停止処理が成功したタスクARNのリスト
    # failed: 停止処理が失敗したタスクARNとエラーメッセージのタプルのリスト
    succeeded: list[str] = []
    failed: list[tuple[str, str]] = []
    # タスクARNごとに順次停止処理を実行
    for arn in task_arns:
        try:
            ecs_client.stop_task(
                cluster=cluster_name,
                task=arn,
                reason="Task stopped by Lambda function due to timeout",
            )
            succeeded.append(arn)
        except Exception as exc:
            print(f"[Error]-[_stop_tasks_sequential] Failed to stop task {arn}: {exc}")
            failed.append((arn, str(exc)))

    return succeeded, failed


### メイン関数
## 引数
# event: イベントデータ（辞書形式）
# 例）　{"target_cluster_json": "s3://bucket-name/path/to/ecs_monitoring_timeout_[clustername].json"}
# context: コンテキスト情報（未使用）
def stop_ecstask_over_timeout(event, context):

    # 必須引数（S3のJSONファイルパス）の取得)の確認
    s3path = event.get('target_cluster_json')
    if s3path is None:
        print("[Error]-[stop_ecstask_over_timeout]-[InvalidInput]"
              "target_cluster_json パラメータが未設定")
        return json.dumps({'status': 'failed'})

    # S3からJSONファイルの取得
    s3_client = boto3.client('s3')
    try:
        bucket_name, key = _parse_s3_uri(s3path)
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        cluster_config = json.loads(content)
    except Exception as e:
        print(f"[Error]-[stop_ecstask_over_timeout]-[S3AccessError] S3ファイルの取得に失敗: {e}")
        return json.dumps({'status': 'failed'})

    if len(cluster_config) == 0 or len(cluster_config) > 1:
        print("[Error]-[stop_ecstask_over_timeout] 監視対象のクラスタは1つのみ設定してください")
        return json.dumps({'status': 'failed'})

    cluster_name = list(cluster_config.keys())[0]
    monitor_items = cluster_config[cluster_name]
    if not isinstance(monitor_items, list):
        print("[Error]-[stop_ecstask_over_timeout]"
              f" JSON format error: 監視対象のタスク設定はリスト形式である必要があります")
        return json.dumps({'status': 'failed'})

    ecs_client = boto3.client("ecs")
    # タスクのタイムアウト判定と停止対象の収集
    try:
        target_stop_task_arns = _collect_tasks_over_timeout(ecs_client, cluster_name, monitor_items)
    except Exception as e:
        print(f"[Error]-[stop_ecstask_over_timeout] タスクのタイムアウト判定と停止対象の収集に失敗 {e}")
        return json.dumps({'status': 'failed'})

    if not target_stop_task_arns:
        print(f"[Info]-[stop_ecstask_over_timeout] クラスタ内に処理中のタスクはありません {cluster_name}")
        return json.dumps({'status': 'success'})

    print(
        "[Info]-[stop_ecstask_over_timeout] "
        f"cluster={cluster_name} stop_targets={len(target_stop_task_arns)}"
    )

    # タスクの停止処理（複数タスクを順次処理）
    succeeded, failed = _stop_tasks_sequential(ecs_client, cluster_name, target_stop_task_arns)
    if failed:
        print(f"[Error]-[stop_ecstask_over_timeout] stop_task failed: {failed}")

    # contextからLambdaの残り実行時間を取得し、停止完了待機のタイムアウト値を調整する
    remaining_ms = None
    if context is not None and hasattr(context, "get_remaining_time_in_millis"):
        remaining_ms = context.get_remaining_time_in_millis()

    # デフォルト900秒（15分）
    wait_timeout = 900
    if remaining_ms is not None:
        wait_timeout = max(1, min(wait_timeout, int((remaining_ms / 1000) - 1)))

    # 停止完了待機（Lambda残り時間も考慮）
    all_stopped = _wait_for_tasks_stopped(
        ecs_client,
        cluster_name,
        succeeded,
        timeout_seconds=wait_timeout,
        poll_interval_seconds=5,
    )
    if all_stopped is False:
        print(f"[Error]-[stop_ecstask_over_timeout] Waitタイムアウト超過")

    # 結果の出力
    status = "success" if all_stopped else "partial"
    print(json.dumps(
        {
            "status": status,
            "cluster": cluster_name,
            "stopTargets": len(target_stop_task_arns),
            "stopRequested": len(succeeded),
            "stopFailed": [{"taskArn": a, "error": err} for a, err in failed],
            "allStopped": all_stopped,
        }
    ))
    return json.dumps({'status': 'success'})
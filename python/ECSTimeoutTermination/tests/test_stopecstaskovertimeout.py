import os
import sys
import json
import pytest
from datetime import datetime, timezone, timedelta

# ECSTimeoutTermination ディレクトリを import パスに追加
CURRENT_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.dirname(CURRENT_DIR)
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

import stopecstaskovertimeout as target  # noqa: E402


def test_parse_s3_uri_ok():
    bucket, key = target._parse_s3_uri("s3://my-bucket/path/to/file.json")
    assert bucket == "my-bucket"
    assert key == "path/to/file.json"


def test_parse_s3_uri_invalid_raises():
    with pytest.raises(Exception):
        target._parse_s3_uri("https://example.com/not-s3")


def test_extract_taskdef_family_ok():
    arn = "arn:aws:ecs:ap-northeast-1:123456789012:task-definition/myfamily:42"
    assert target._extract_taskdef_family(arn) == "myfamily"


def test_extract_taskdef_family_none_or_unmatched():
    assert target._extract_taskdef_family("") is None
    assert target._extract_taskdef_family("not-an-arn") is None


def test_should_monitor_task_matches_family():
    task = {
        "taskDefinitionArn": "arn:aws:ecs:ap-northeast-1:1:task-definition/hello:1",
        "group": "service:other",
    }
    assert target._should_monitor_task(task, "hello") is True


def test_should_monitor_task_matches_group_variants():
    task = {
        "taskDefinitionArn": "arn:aws:ecs:ap-northeast-1:1:task-definition/other:1",
        "group": "service:my-service",
    }
    assert target._should_monitor_task(task, "service:my-service") is True
    assert target._should_monitor_task(task, "my-service") is True


def test_is_task_over_timeout_true(monkeypatch):
    fixed_now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(target, "datetime", FixedDatetime)

    task = {"taskArn": "t1", "startedAt": fixed_now - timedelta(seconds=11)}
    assert target._is_task_over_timeout(task, timeout_seconds=10) is True


def test_is_task_over_timeout_false_when_missing_startedat():
    assert target._is_task_over_timeout({"taskArn": "t1"}, timeout_seconds=10) is False


class _DummyPaginator:
    def __init__(self, task_arns):
        self._task_arns = task_arns

    def paginate(self, cluster, desiredStatus):
        assert desiredStatus == "RUNNING"
        yield {"taskArns": list(self._task_arns)}


class _DummyECS:
    def __init__(self, tasks_by_arn):
        self._tasks_by_arn = dict(tasks_by_arn)
        self.stop_calls = []
        self._describe_call_count = 0
        self.fail_stop_for = set()

    def get_paginator(self, name):
        assert name == "list_tasks"
        return _DummyPaginator(self._tasks_by_arn.keys())

    def describe_tasks(self, cluster, tasks):
        self._describe_call_count += 1
        described = []
        for arn in tasks:
            t = self._tasks_by_arn.get(arn)
            if t is not None:
                described.append(t)
        return {"tasks": described, "failures": []}

    def stop_task(self, cluster, task, reason):
        if task in self.fail_stop_for:
            raise RuntimeError("boom")
        self.stop_calls.append((cluster, task, reason))
        return {}


def test_collect_tasks_over_timeout_filters_and_sorts(monkeypatch):
    fixed_now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(target, "datetime", FixedDatetime)

    tasks = {
        "arn1": {
            "taskArn": "arn1",
            "taskDefinitionArn": "arn:aws:ecs:xx:1:task-definition/famA:1",
            "group": "service:svcA",
            "startedAt": fixed_now - timedelta(seconds=100),
            "lastStatus": "RUNNING",
        },
        "arn2": {
            "taskArn": "arn2",
            "taskDefinitionArn": "arn:aws:ecs:xx:1:task-definition/famB:1",
            "group": "service:svcB",
            "startedAt": fixed_now - timedelta(seconds=5),
            "lastStatus": "RUNNING",
        },
        "arn3": {
            "taskArn": "arn3",
            "taskDefinitionArn": "arn:aws:ecs:xx:1:task-definition/famA:2",
            "group": "service:svcA",
            "startedAt": fixed_now - timedelta(seconds=100),
            "lastStatus": "RUNNING",
        },
    }
    ecs = _DummyECS(tasks)

    monitor_items = [
        {"mon_task_name": "famA", "timeout_seconds": 10},
        {"mon_task_name": "svcB", "timeout_seconds": 10},
    ]

    to_stop = target._collect_tasks_over_timeout(ecs, "cluster", monitor_items)
    assert to_stop == ["arn1", "arn3"]


def test_stop_tasks_sequential_records_failures():
    ecs = _DummyECS({})
    ecs.fail_stop_for.add("arn2")

    succeeded, failed = target._stop_tasks_sequential(ecs, "cluster", ["arn1", "arn2"])
    assert succeeded == ["arn1"]
    assert failed == [("arn2", "boom")]


def test_wait_for_tasks_stopped_returns_true(monkeypatch):
    # sleep を無効化して高速化
    monkeypatch.setattr(target.time, "sleep", lambda *_args, **_kwargs: None)

    class ECSForWait:
        def __init__(self):
            self._calls = 0

        def describe_tasks(self, cluster, tasks):
            self._calls += 1
            if self._calls == 1:
                return {
                    "tasks": [{"taskArn": t, "lastStatus": "RUNNING"} for t in tasks],
                    "failures": [],
                }
            return {
                "tasks": [{"taskArn": t, "lastStatus": "STOPPED"} for t in tasks],
                "failures": [],
            }

    ecs = ECSForWait()
    ok = target._wait_for_tasks_stopped(
        ecs,
        "cluster",
        ["arn1", "arn2"],
        timeout_seconds=30,
        poll_interval_seconds=1,
    )
    assert ok is True


def test_stop_ecstask_over_timeout_missing_param_failed():
    resp = json.loads(target.stop_ecstask_over_timeout({}, None))
    assert resp["status"] == "failed"


def test_stop_ecstask_over_timeout_happy_path(monkeypatch):
    fixed_now = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return fixed_now

    monkeypatch.setattr(target, "datetime", FixedDatetime)
    monkeypatch.setattr(target.time, "sleep", lambda *_args, **_kwargs: None)

    cluster_config = {
        "myCluster": [
            {"mon_task_name": "famA", "timeout_seconds": 10},
        ]
    }

    class DummyBody:
        def __init__(self, b):
            self._b = b

        def read(self):
            return self._b

    class DummyS3:
        def get_object(self, Bucket, Key):
            assert Bucket == "bucket"
            assert Key == "cfg.json"
            return {"Body": DummyBody(json.dumps(cluster_config).encode("utf-8"))}

    tasks = {
        "arn1": {
            "taskArn": "arn1",
            "taskDefinitionArn": "arn:aws:ecs:xx:1:task-definition/famA:1",
            "group": "service:svcA",
            "startedAt": fixed_now - timedelta(seconds=100),
            "lastStatus": "RUNNING",
        }
    }
    ecs = _DummyECS(tasks)

    import boto3

    def _client(service_name):
        if service_name == "s3":
            return DummyS3()
        if service_name == "ecs":
            return ecs
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, "client", _client)

    class DummyContext:
        def get_remaining_time_in_millis(self):
            return 30_000

    resp = json.loads(
        target.stop_ecstask_over_timeout(
            {"target_cluster_json": "s3://bucket/cfg.json"},
            DummyContext(),
        )
    )
    assert resp["status"] == "success"
    assert ecs.stop_calls, "stop_task should be called"

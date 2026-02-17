import os
import sys
import pytest

# srcディレクトリをパスに追加（pytest実行位置に依存しないように）
CURRENT_DIR = os.path.dirname(__file__)
SRC_DIR = os.path.join(os.path.dirname(CURRENT_DIR), 'src')
if SRC_DIR not in sys.path:
    sys.path.append(SRC_DIR)

from updatecatalog import (
    updatecatalog,
    tablecolumns_diff_verify,
    catalog_scan,
    wait_crawler_completion,
    specifiedday_diff_verify_and_runcrawler,
    prevday_diff_verify_and_runcrawler,
)  # noqa: E402

@pytest.fixture(autouse=True)
def mock_boto3(monkeypatch, tmp_path):
    """boto3 SSM/S3 クライアントをモックし、prevdif経路での S3/Parquet アクセスを無害化"""
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    class DummySSM:
        def get_parameter(self, *args, **kwargs):
            # /m365/common/{group}/targettable 等に対してカンマ区切りで返す想定
            name = kwargs.get('Name') or (len(args) > 0 and args[0])
            if name and 'targettable' in name:
                value = 'table1'
            elif name == '/m365/common/s3bucket':
                value = 'dummy-bucket'
            elif name == '/m365/common/pipelineconv':
                value = 'convert'
            else:
                value = 'dummy'
            return {"Parameter": {"Value": value}}

    class DummyBody:
        def __init__(self, data: bytes):
            self._b = data
        def read(self):
            return self._b

    class DummyS3:
        def get_object(self, Bucket, Key):  # base 日付CSV
            return {"Body": DummyBody(b"base\n2025-01-21\n")}
        def download_file(self, Bucket, Key, Filename):
            # 最低限の 1 カラム Parquet を生成
            table = pa.Table.from_pydict({"id": [1]})
            pq.write_table(table, Filename)

    class DummyGlue:
        class exceptions:
            class EntityNotFoundException(Exception):
                pass

        def __init__(self):
            self.updated = {}
        def update_crawler(self, Name, SchemaChangePolicy=None, RecrawlPolicy=None, **kwargs):
            self.updated = {
                'Name': Name,
                'SchemaChangePolicy': SchemaChangePolicy,
                'RecrawlPolicy': RecrawlPolicy,
            }
            return {}
        def start_crawler(self, Name):
            return {
                'ResponseMetadata': {'RequestId': 'dummy-request-id'},
                'CrawlerRunId': f'run-{Name}-1'
            }
        def get_crawler(self, Name):
            # デフォルトで即座に READY を返す（wait_crawler_completion で即完了）
            return {
                'Crawler': {
                    'Name': Name,
                    'State': 'READY',
                    'LastCrawl': {'Status': 'SUCCEEDED'}
                }
            }

    def _client(service_name):
        if service_name == 'ssm':
            return DummySSM()
        if service_name == 's3':
            return DummyS3()
        if service_name == 'glue':
            return DummyGlue()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)

# GROUP環境変数を設定していない異常系テスト
def test_missing_exec_type_exits(monkeypatch):
    monkeypatch.delenv('GROUP', raising=False)  # GROUPは先に評価されるため削除
    with pytest.raises(SystemExit) as e:
        updatecatalog({}, None)
    assert e.value.code == 255

# 実行種別が不正な場合の異常系テスト
def test_invalid_exec_type_exits(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'WRONG'}, None)
    assert e.value.code == 255

# specdifで基準日が未設定,かつテーブルが未設定の場合の異常系テスト
def test_specdif_missing_targetday_exits(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'specdif'}, None)
    assert e.value.code == 255

# specdifで基準日の形式が不正な場合の異常系テスト（yyyymmdd形式でない）
def test_specdif_bad_format_exits(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'specdif', 'specdif_targetday': '2025/01/01'}, None)
    assert e.value.code == 255
    with pytest.raises(SystemExit) as e2:
        updatecatalog({'exec_type': 'specdif', 'specdif_targetday': '202501'}, None)
    assert e2.value.code == 255
    with pytest.raises(SystemExit) as e3:
        updatecatalog({'exec_type': 'specdif', 'specdif_targetday': 'abcdefgh'}, None)
    assert e3.value.code == 255

# specdifで基準日の値が不正な場合の異常系テスト（存在しない日付）
def test_specdif_invalid_date_exits(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'specdif', 'specdif_targetday': '20250230'}, None)
    assert e.value.code == 255

# specdifでテーブルが未設定の場合の異常系テスト
def test_specdif_missing_targettable_exits(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'specdif', 'specdif_targetday': '20250101'}, None)
    assert e.value.code == 255

# fulscanでテーブルが未設定の場合の異常系テスト
def test_fulscan_missing_targettable_exits(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'fulscan'}, None)
    assert e.value.code == 255

# GROUP環境変数が未設定の場合の異常系テスト（実行種別は存在）
def test_missing_group_env_exits(monkeypatch):
    monkeypatch.delenv('GROUP', raising=False)
    with pytest.raises(SystemExit) as e:
        updatecatalog({'exec_type': 'prevdif'}, None)
    assert e.value.code == 255

# 正常系テストケース(prevdif)
def test_valid_prevdif(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    updatecatalog({'exec_type': 'prevdif'}, None)

def test_catalog_scan_full(monkeypatch):
    """catalog_scan で full_scan=True の場合、全走査ポリシーが適用されること"""
    monkeypatch.setenv('GROUP', 'group1')
    result = catalog_scan('table1', full_scan=True)
    assert result['crawler_name'] == 'dummy'

def test_catalog_scan_incremental(monkeypatch):
    """catalog_scan で full_scan=False の場合、増分走査ポリシーが適用されること"""
    monkeypatch.setenv('GROUP', 'group1')
    result = catalog_scan('table1', full_scan=False)
    assert result['crawler_name'] == 'dummy'

# 正常系テストケース(specdif)
def test_valid_specdif(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    updatecatalog({'exec_type': 'specdif',
                   'specdif_targetday': '20250101',
                   'targettable': 'table1'}, None)

# 正常系テストケース(fulscan)
def test_valid_fulscan(monkeypatch):
    monkeypatch.setenv('GROUP', 'group1')
    updatecatalog({'exec_type': 'fulscan',
                   'targettable': 'table1'}, None)


## tablecolumns_diff_verify のテスト群
# 正常系テストケース（差分無し）
def test_tablecolumns_no_diff():
    result = tablecolumns_diff_verify(
        table="sample",
        base_s3_path="s3://m365-dwh/group1/convert/",
        base_day="20250121",
        target_day="20250120",
        base_cols_override=["id", "name", "value"],
        target_cols_override=["id", "name", "value"],
    )
    assert result['diff'] is False
    assert result['newly_added_columns'] == []
    assert result['removed_columns'] == []

# 正常系テストケース（カラム追加）
def test_tablecolumns_missing():
    result = tablecolumns_diff_verify(
        table="sample",
        base_s3_path="s3://m365-dwh/group1/convert/",
        base_day="20250121",
        target_day="20250120",
        base_cols_override=["id", "name", "value"],
        target_cols_override=["id", "value"],
    )
    assert result['diff'] is True
    # "name" は基準日にあって前日に無い -> 新規追加
    assert result['newly_added_columns'] == ["name"]
    assert result['removed_columns'] == []

# 正常系テストケース（カラム削除）
def test_tablecolumns_added():
    result = tablecolumns_diff_verify(
        table="sample",
        base_s3_path="s3://m365-dwh/group1/convert/",
        base_day="20250121",
        target_day="20250120",
        base_cols_override=["id", "name"],
        target_cols_override=["id", "name", "extra"],
    )
    assert result['diff'] is True
    # "extra" は前日にあり基準日に無い -> 削除されたカラム（基準日時点では存在しない）
    assert result['newly_added_columns'] == []
    assert result['removed_columns'] == ["extra"]

# 正常系テストケース（カラム追加と削除）
def test_tablecolumns_added_and_missing():
    result = tablecolumns_diff_verify(
        table="sample",
        base_s3_path="s3://m365-dwh/group1/convert/",
        base_day="20250121",
        target_day="20250120",
        base_cols_override=["id", "name", "value"],
        target_cols_override=["id", "value", "newcol"],
    )
    assert result['diff'] is True
    assert result['newly_added_columns'] == ["name"]
    assert result['removed_columns'] == ["newcol"]


# 初回実行などで前日(ターゲット)側 Parquet が存在しない場合の挙動
def test_tablecolumns_missing_target_file(monkeypatch, tmp_path):
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    # 既存 autouse フィクスチャの boto3.client を上書きしてターゲット日付のみ欠如を再現
    class DummyS3Missing:
        def get_object(self, Bucket, Key):
            return {"Body": type("B", (), {"read": lambda self: b"base\n2025-01-21\n"})()}
        def download_file(self, Bucket, Key, Filename):
            # target_day=20250120 を含むキーなら存在しない想定で例外
            if "date=20250120" in Key:
                raise Exception("NoSuchKey 404")
            # 基準日ファイルは 1 カラム Parquet を生成
            table = pa.Table.from_pydict({"id": [1]})
            pq.write_table(table, Filename)

    def _client(service_name):
        if service_name == 's3':
            return DummyS3Missing()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)

    result = tablecolumns_diff_verify(
        table="sample",
        base_s3_path="s3://m365-dwh/group1/convert/",
        base_day="20250121",
        target_day="20250120",
    )
    # ターゲット欠如時は全カラム新規扱い → diff True / newly_added=基準日全カラム / removedなし / target_columns=[]
    assert result['diff'] is True
    assert result['newly_added_columns'] == result['base_columns']
    assert result['removed_columns'] == []
    assert result['target_columns'] == []


## wait_crawler_completion のテスト群
def test_wait_crawler_completion_all_ready(monkeypatch):
    """全クローラが即座にREADYの場合、1回の監視周期(polls)で終了すること"""
    import boto3

    class GlueWaitMock:
        def __init__(self):
            self.call_count = 0
        def get_crawler(self, Name):
            self.call_count += 1
            return {
                'Crawler': {
                    'Name': Name,
                    'State': 'READY',
                    'LastCrawl': {'Status': 'SUCCEEDED'}
                }
            }

    def _client(service_name):
        if service_name == 'glue':
            return GlueWaitMock()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)
    result = wait_crawler_completion(['crawler1', 'crawler2'], timeout_seconds=60, poll_interval=1)

    assert result['all_ready'] is True
    assert len(result['crawlers']) == 2
    assert result['crawlers'][0]['state'] == 'READY'
    assert result['crawlers'][1]['state'] == 'READY'
    assert result['polls'] == 1


def test_wait_crawler_completion_running_to_ready(monkeypatch):
    """RUNNING → READY への遷移を検出すること"""
    import boto3

    class GlueStateMock:
        def __init__(self):
            self.poll_count = 0
        def get_crawler(self, Name):
            self.poll_count += 1
            # 最初2回は RUNNING、3回目で READY
            if self.poll_count <= 2:
                return {
                    'Crawler': {
                        'Name': Name,
                        'State': 'RUNNING',
                        'LastCrawl': {'Status': 'RUNNING'}
                    }
                }
            else:
                return {
                    'Crawler': {
                        'Name': Name,
                        'State': 'READY',
                        'LastCrawl': {'Status': 'SUCCEEDED'}
                    }
                }

    def _client(service_name):
        if service_name == 'glue':
            return GlueStateMock()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)
    result = wait_crawler_completion(['crawler1'], timeout_seconds=60, poll_interval=0.01)

    assert result['all_ready'] is True
    assert result['crawlers'][0]['state'] == 'READY'
    assert result['crawlers'][0]['last_status'] == 'SUCCEEDED'
    assert result['polls'] >= 2


def test_wait_crawler_completion_multiple_mixed_state(monkeypatch):
    """複数クローラで一部 RUNNING, 一部 READY の状態から全て READY になるまで待機すること"""
    import boto3

    class GlueMixedMock:
        def __init__(self):
            self.poll_count = 0
        def get_crawler(self, Name):
            self.poll_count += 1
            # crawler1 は最初から READY、crawler2 は2回目から READY
            if Name == 'crawler1':
                return {
                    'Crawler': {
                        'Name': Name,
                        'State': 'READY',
                        'LastCrawl': {'Status': 'SUCCEEDED'}
                    }
                }
            else:  # crawler2
                if self.poll_count <= 2:
                    return {
                        'Crawler': {
                            'Name': Name,
                            'State': 'RUNNING',
                            'LastCrawl': {'Status': 'RUNNING'}
                        }
                    }
                else:
                    return {
                        'Crawler': {
                            'Name': Name,
                            'State': 'READY',
                            'LastCrawl': {'Status': 'SUCCEEDED'}
                        }
                    }

    def _client(service_name):
        if service_name == 'glue':
            return GlueMixedMock()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)
    result = wait_crawler_completion(['crawler1', 'crawler2'], timeout_seconds=60, poll_interval=0.01)

    assert result['all_ready'] is True
    assert len(result['crawlers']) == 2
    assert all(c['state'] == 'READY' for c in result['crawlers'])


def test_wait_crawler_completion_timeout(monkeypatch):
    """タイムアウト時に all_ready=False を返すこと"""
    import boto3

    class GlueTimeoutMock:
        def get_crawler(self, Name):
            # 常に RUNNING を返す
            return {
                'Crawler': {
                    'Name': Name,
                    'State': 'RUNNING',
                    'LastCrawl': {'Status': 'RUNNING'}
                }
            }

    def _client(service_name):
        if service_name == 'glue':
            return GlueTimeoutMock()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)
    result = wait_crawler_completion(['crawler1'], timeout_seconds=0.1, poll_interval=0.05)

    assert result['all_ready'] is False
    assert result['crawlers'][0]['state'] == 'RUNNING'


def test_wait_crawler_completion_not_found(monkeypatch):
    """存在しないクローラは state=NOT_FOUND で記録されること"""
    import boto3

    class GlueNotFoundMock:
        def get_crawler(self, Name):
            from botocore.exceptions import ClientError
            raise ClientError(
                {'Error': {'Code': 'EntityNotFoundException', 'Message': 'Not found'}},
                'get_crawler'
            )

    # EntityNotFoundException を模擬するため例外属性を追加
    class MockGlueClient:
        class exceptions:
            class EntityNotFoundException(Exception):
                pass
        def get_crawler(self, Name):
            raise self.exceptions.EntityNotFoundException()

    def _client(service_name):
        if service_name == 'glue':
            return MockGlueClient()
        raise AssertionError(f"Unexpected service: {service_name}")

    monkeypatch.setattr(boto3, 'client', _client)
    result = wait_crawler_completion(['missing_crawler'], timeout_seconds=1, poll_interval=0.5)

    assert result['crawlers'][0]['state'] == 'NOT_FOUND'
    assert result['crawlers'][0]['last_status'] == 'UNKNOWN'


# specifiedday_diff_verify_and_runcrawler のテスト群
def test_specifiedday_diff_verify_and_runcrawler_full_scan(monkeypatch):
    """差分ありの場合 full_scan=True が catalog_scan に渡されること"""
    # diff True を強制
    def fake_diff(*args, **kwargs):
        return {'diff': True, 'newly_added_columns': [], 'removed_columns': [], 'base_columns': [], 'target_columns': []}
    monkeypatch.setattr('updatecatalog.tablecolumns_diff_verify', fake_diff)

    calls = []
    def fake_catalog_scan(table, full_scan=False):
        calls.append((table, full_scan))
        return {'crawler_name': f'crawler-{table}'}
    monkeypatch.setattr('updatecatalog.catalog_scan', fake_catalog_scan)

    result = specifiedday_diff_verify_and_runcrawler('t1', 's3://bucket/group/conv/', '20250121', '20250120')
    assert result == ['crawler-t1']
    assert calls == [('t1', True)]


def test_specifiedday_diff_verify_and_runcrawler_incremental(monkeypatch):
    """差分なしの場合 full_scan=False が catalog_scan に渡されること"""
    def fake_diff(*args, **kwargs):
        return {'diff': False, 'newly_added_columns': [], 'removed_columns': [], 'base_columns': [], 'target_columns': []}
    monkeypatch.setattr('updatecatalog.tablecolumns_diff_verify', fake_diff)

    calls = []
    def fake_catalog_scan(table, full_scan=False):
        calls.append((table, full_scan))
        return {'crawler_name': f'crawler-{table}'}
    monkeypatch.setattr('updatecatalog.catalog_scan', fake_catalog_scan)

    result = specifiedday_diff_verify_and_runcrawler('t2', 's3://bucket/group/conv/', '20250121', '20250120')
    assert result == ['crawler-t2']
    assert calls == [('t2', False)]


def test_specifiedday_diff_verify_and_runcrawler_normalize(monkeypatch):
    """base_day が yyyy-mm-dd 形式でも内部で yyyymmdd に正規化されることを検証"""
    captured = {}
    def fake_diff(table, base_s3_path, base_day, target_day, *args, **kwargs):
        captured['base_day'] = base_day
        captured['target_day'] = target_day
        return {'diff': False, 'newly_added_columns': [], 'removed_columns': [], 'base_columns': [], 'target_columns': []}
    monkeypatch.setattr('updatecatalog.tablecolumns_diff_verify', fake_diff)

    def fake_catalog_scan(table, full_scan=False):
        return {'crawler_name': f'crawler-{table}'}
    monkeypatch.setattr('updatecatalog.catalog_scan', fake_catalog_scan)

    result = specifiedday_diff_verify_and_runcrawler('t_norm', 's3://bucket/group/conv/', '2025-01-21', '20250120')
    assert result == ['crawler-t_norm']
    assert captured['base_day'] == '20250121'
    assert captured['target_day'] == '20250120'


# prevday_diff_verify_and_runcrawler のテスト群
def test_prevdif_multi_tables_mixed(monkeypatch):
    """複数テーブルで一部差分あり (full_scan=True) と差分なし (False) が混在する挙動を検証"""
    def fake_diff(*args, **kwargs):
        table = args[0]
        if table == 't_added':
            return {'diff': True, 'newly_added_columns': ['x'], 'removed_columns': [], 'base_columns': [], 'target_columns': []}
        else:
            return {'diff': False, 'newly_added_columns': [], 'removed_columns': [], 'base_columns': [], 'target_columns': []}
    monkeypatch.setattr('updatecatalog.tablecolumns_diff_verify', fake_diff)

    calls = []
    def fake_catalog_scan(table, full_scan=False):
        calls.append((table, full_scan))
        return {'crawler_name': f'crawler-{table}'}
    monkeypatch.setattr('updatecatalog.catalog_scan', fake_catalog_scan)

    result = prevday_diff_verify_and_runcrawler('t_added,t_same', 's3://bucket/group/conv/', '2025-01-21')
    assert set(result) == {'crawler-t_added', 'crawler-t_same'}
    # 呼び出し順と full_scan フラグ確認
    assert calls == [('t_added', True), ('t_same', False)]


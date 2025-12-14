"""DataQuality Container ENTRYPOINT
"""
import os
import sys
import logging
import boto3
import importlib.util
from pathlib import Path

# parameterストアから必要な値を取得
ssm = boto3.client('ssm')
# S3 上のスクリプト配置情報（固定）
SCRIPT_BUCKET = ssm.get_parameter(Name=f'/m365/common/s3bucket',
                                  WithDecryption=False)['Parameter']['Value']
SCRIPT_KEY = "scripts/dataquality.py"

# ダウンロード先 (カレントディレクトリ直下)
LOCAL_SCRIPT_PATH = Path("dataquality.py")

logger = logging.getLogger("dataquality-entrypoint")

def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def ensure_dataquality_download():
    """S3 から dataquality.py を毎回再取得する。
    既存ファイルがある場合は削除してから取得。取得失敗時は終了。
    """
    if LOCAL_SCRIPT_PATH.exists():
        try:
            LOCAL_SCRIPT_PATH.unlink()
            logger.info("既存ファイルを削除しました: %s", LOCAL_SCRIPT_PATH)
        except Exception as e:
            logger.warning("既存ファイル削除失敗 (続行): %s", e)
    logger.info("S3 ダウンロード開始 bucket=%s key=%s -> %s", SCRIPT_BUCKET, SCRIPT_KEY, LOCAL_SCRIPT_PATH)
    s3 = boto3.client("s3")
    try:
        LOCAL_SCRIPT_PATH.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(SCRIPT_BUCKET, SCRIPT_KEY, str(LOCAL_SCRIPT_PATH))
    except Exception as e:
        logger.exception("dataquality.py ダウンロード失敗: %s", e)
        sys.exit(10)
    logger.info("再ダウンロード完了: %s", LOCAL_SCRIPT_PATH)


def dynamic_import_dataquality():
    """ダウンロード済みの dataquality.py から dataquality 関数を動的インポート。"""
    spec = importlib.util.spec_from_file_location("dataquality", str(LOCAL_SCRIPT_PATH))
    if spec is None or spec.loader is None:
        logger.error("spec の取得に失敗しました")
        sys.exit(11)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore
    except Exception as e:
        logger.exception("dataquality.py のロード失敗: %s", e)
        sys.exit(12)
    if not hasattr(module, "dataquality"):
        logger.error("dataquality 関数が見つかりません")
        sys.exit(13)
    return module.dataquality


def main():
    setup_logging()

    # GROUP の設定（Step Functions からの環境変数想定）
    if not os.getenv("GROUP"):
        logger.error("環境変数 GROUP が未設定です。")
        sys.exit(2)

    group_val = os.getenv("GROUP")
    logger.info("開始 GROUP=%s", group_val)

    # S3 からスクリプト取得 + 動的インポート
    ensure_dataquality_download()
    dataquality = dynamic_import_dataquality()

    try:
        dataquality(input_event={"group": os.getenv("GROUP")})
    except SystemExit as e:  # dataquality 内での sys.exit をそのまま反映
        logger.error("dataquality が異常終了 code=%s", e.code)
        raise
    except Exception as e:
        logger.exception("dataquality 実行中に予期せぬ例外: %s", e)
        sys.exit(99)

    logger.info("完了 GROUP=%s", group_val)

if __name__ == "__main__":
    main()

"""Catalog Update Container ENTRYPOINT

必須環境変数:
  GROUP : カタログ対象グループ名 (/m365/common/<GROUP>/targettable 用)

必須引数 (--exec-type):
  prevdif  : 全ターゲットテーブルの前日差分検証→差分に応じて走査
  specdif  : 指定テーブルの指定日差分検証→差分に応じて走査 (追加引数 --targettable, --specdif-targetday 必須)
  fulscan  : 指定テーブルを常に全走査 (追加引数 --targettable 必須)

使用例:
  python ENTRYPOINT.py --exec-type prevdif
  python ENTRYPOINT.py --exec-type specdif --targettable mytable --specdif-targetday 20250115
  python ENTRYPOINT.py --exec-type fulscan --targettable mytable

環境変数 GROUP を CLI から指定したい場合:
  python ENTRYPOINT.py --exec-type prevdif --group mygroup
"""
import os
import sys
import argparse
import logging
import importlib.util
from pathlib import Path
import boto3

# S3 上のスクリプト配置情報（固定）
SCRIPT_BUCKET = "m365-dwh"
SCRIPT_KEY = "scripts/updatecatalog.py"

# ダウンロード先 (カレントディレクトリ直下)
LOCAL_SCRIPT_PATH = Path("updatecatalog.py")

logger = logging.getLogger("catalog-entrypoint")


def setup_logging():
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        stream=sys.stdout,
    )


def parse_args():
    p = argparse.ArgumentParser(description="Glue Catalog Update runner")
    p.add_argument("--exec-type", required=True, choices=["prevdif", "specdif", "fulscan"], help="実行モード")
    p.add_argument("--targettable", help="対象テーブル (specdif/fulscan で必須)")
    p.add_argument("--specdif-targetday", help="指定日差分検証用 yyyymmdd (specdif で必須)")
    p.add_argument("--group", help="GROUP 環境変数を上書き設定")
    return p.parse_args()


def build_event(args):
    event = {"exec_type": args.exec_type}
    if args.exec_type in ("specdif", "fulscan"):
        if not args.targettable:
            logger.error("--targettable は %s で必須です", args.exec_type)
            sys.exit(2)
        event["targettable"] = args.targettable
    if args.exec_type == "specdif":
        if not args.specdif_targetday:
            logger.error("--specdif-targetday は specdif で必須です")
            sys.exit(2)
        event["specdif_targetday"] = args.specdif_targetday
    return event


def ensure_updatecatalog_download():
    """S3 から updatecatalog.py を毎回再取得する。
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
        logger.exception("updatecatalog.py ダウンロード失敗: %s", e)
        sys.exit(10)
    logger.info("再ダウンロード完了: %s", LOCAL_SCRIPT_PATH)


def dynamic_import_updatecatalog():
    """ダウンロード済みの updatecatalog.py から updatecatalog 関数を動的インポート。"""
    spec = importlib.util.spec_from_file_location("updatecatalog", str(LOCAL_SCRIPT_PATH))
    if spec is None or spec.loader is None:
        logger.error("spec の取得に失敗しました")
        sys.exit(11)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)  # type: ignore
    except Exception as e:
        logger.exception("updatecatalog.py のロード失敗: %s", e)
        sys.exit(12)
    if not hasattr(module, "updatecatalog"):
        logger.error("updatecatalog 関数が見つかりません")
        sys.exit(13)
    return module.updatecatalog


def main():
    setup_logging()
    args = parse_args()

    # GROUP の設定 (CLI 優先)
    if args.group:
        os.environ["GROUP"] = args.group
    if not os.getenv("GROUP"):
        logger.error("環境変数 GROUP が未設定です。--group で指定するか事前に設定してください。")
        sys.exit(2)

    event = build_event(args)
    logger.info("開始 exec_type=%s event=%s GROUP=%s", args.exec_type, event, os.getenv("GROUP"))

    # S3 からスクリプト取得 + 動的インポート
    ensure_updatecatalog_download()
    updatecatalog = dynamic_import_updatecatalog()

    try:
        updatecatalog(event, None)
    except SystemExit as e:  # updatecatalog 内での sys.exit をそのまま反映
        logger.error("updatecatalog が異常終了 code=%s", e.code)
        raise
    except Exception as e:
        logger.exception("updatecatalog 実行中に予期せぬ例外: %s", e)
        sys.exit(99)

    logger.info("完了 exec_type=%s", args.exec_type)


if __name__ == "__main__":
    main()

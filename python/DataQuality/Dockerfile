FROM python:3.12-slim AS base

# ===== 必要パッケージ (最小構成) =====
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    curl wget jq ca-certificates \
    build-essential \
    libicu-dev libunwind-dev \
 && rm -rf /var/lib/apt/lists/*

# ===== Python ランタイム設定 =====
ENV PYTHON_VERSION=3.12 \
	PYTHONDONTWRITEBYTECODE=1 \
	PYTHONUNBUFFERED=1 \
	PIP_NO_CACHE_DIR=off \
	PIP_DISABLE_PIP_VERSION_CHECK=on \
	APP_HOME=/app \
	PYTHONPATH=""

WORKDIR $APP_HOME

# ===== 依存ライブラリインストール (requirements.txt) =====
# boto3 などの依存を固定管理
COPY requirements.txt ./
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt
# ===== ソース配置 =====
COPY src/ENTRYPOINT.py ./src/
ENV PYTHONPATH="${APP_HOME}/src"

# ===== 非 root 実行ユーザー作成 =====
RUN useradd -m appuser && chown -R appuser:appuser $APP_HOME
USER appuser

# ===== デフォルトコマンド =====
# S3 からコード取得後に任意処理を実行する ENTRYPOINT.py を起動
CMD ["python", "src/ENTRYPOINT.py"]

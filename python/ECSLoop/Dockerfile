ARG BASE_IMAGE=public.ecr.aws/amazonlinux/amazonlinux:2023
FROM ${BASE_IMAGE}

# curl-minimal が存在する場合のみ削除して依存関係をインストール
RUN (rpm -q curl-minimal >/dev/null 2>&1 && rpm -e --nodeps curl-minimal || true) \
 && dnf install -y curl wget tar gzip jq libicu libunwind \
 && dnf clean all

# 起動スクリプトを配置
COPY src/ecsloop.sh /ecsloop.sh
RUN chmod +x /ecsloop.sh

# ECSLoop実行
ENTRYPOINT ["/ecsloop.sh"]

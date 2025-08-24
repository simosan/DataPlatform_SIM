FROM public.ecr.aws/lambda/provided:al2

ENV NO_COLOR=true

# PowerShell (ARM64) のインストール
RUN yum install -y wget tar gzip curl jq && yum clean all && \
    curl -L https://github.com/PowerShell/PowerShell/releases/download/v7.4.4/powershell-7.4.4-linux-arm64.tar.gz -o /tmp/powershell.tar.gz && \
    mkdir -p /opt/microsoft/powershell/7 && \
    tar zxf /tmp/powershell.tar.gz -C /opt/microsoft/powershell/7 && \
    chmod +x /opt/microsoft/powershell/7/pwsh && \
    ln -s /opt/microsoft/powershell/7/pwsh /usr/bin/pwsh && \
    rm /tmp/powershell.tar.gz

# AWS Lambda Runtime Interface Client (RIC)
RUN curl -Lo /var/runtime/bootstrap \
      https://github.com/aws/aws-lambda-runtime-interface-emulator/releases/latest/download/aws-lambda-rie-arm64 \
    && chmod +x /var/runtime/bootstrap

# PowerShell モジュールのインストール
RUN pwsh -Command "& { \
    Set-PSRepository -Name 'PSGallery' -InstallationPolicy Trusted; \
    Install-Module -Name AWS.Tools.Common -Force -Scope AllUsers; \
    Install-Module -Name AWS.Tools.Lambda -Force -Scope AllUsers; \
    Install-Module -Name AWS.Tools.S3 -Force -Scope AllUsers; \
    Install-Module -Name AWS.Tools.SimpleSystemsManagement -Force -Scope AllUsers; \
    Install-Module -Name Microsoft.Graph -Force -Scope AllUsers \
}"

# Lambda ハンドラと bootstrap を配置
COPY src/ /var/task/
RUN chmod +x /var/task/bootstrap && cp /var/task/bootstrap /var/runtime/bootstrap

ENTRYPOINT ["/var/task/bootstrap"]
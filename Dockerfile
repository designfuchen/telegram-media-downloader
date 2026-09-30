FROM ghcr.io/sagernet/sing-box:v1.14.2 AS network-runtime
FROM python:3.11-slim
COPY --from=network-runtime /usr/local/bin/sing-box /usr/local/bin/sing-box
WORKDIR /app
RUN apt-get update \
    && apt-get install -y --no-install-recommends rclone \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt requirements-lock.txt ./
RUN pip install --no-cache-dir -r requirements-lock.txt
RUN mkdir -p /app/downloads /app/sessions /app/log /app/temp /app/dbdata /app/rclone \
    && ln -s /usr/bin/rclone /app/rclone/rclone
# Explicit source copies keep private configs, sessions and DBs out of the image.
COPY media_downloader.py ./
COPY module ./module
COPY utils ./utils
COPY scripts ./scripts
COPY LICENSE THIRD_PARTY_NOTICES.md ./
COPY docs/sing-box-LICENSE.txt /usr/share/licenses/sing-box/LICENSE
ENV PYTHONUNBUFFERED=1 \
    TMD_TASK_DB=/app/dbdata/download_history.db \
    TMD_HISTORY_DB=/app/dbdata/download_history.db
EXPOSE 5002
CMD ["python", "media_downloader.py"]

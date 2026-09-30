#!/bin/bash
# Run from any directory, including Finder. Keeps Telegram login in this terminal.
set -euo pipefail
cd "$(dirname "$0")"
umask 077
if ! command -v python3.11 >/dev/null 2>&1; then
  echo '需要 Python 3.11。请从 python.org 安装后，再运行 bash start.command。'
  exit 1
fi
if [ ! -x .venv/bin/python ]; then
  python3.11 -m venv .venv
fi
if ! cmp -s requirements-lock.txt .venv/installed-requirements.txt; then
  echo '首次启动或依赖已更新：正在安装依赖，请保持网络连接。'
  .venv/bin/python -m pip install -r requirements-lock.txt
  cp requirements-lock.txt .venv/installed-requirements.txt
fi
if [ ! -f config.yaml ]; then
  .venv/bin/python scripts/init_config.py
fi
echo '正在启动真实下载服务。默认地址：http://127.0.0.1:5002'
echo '页面将自动打开；所有设置和 Telegram 登录都在页面完成。'
exec .venv/bin/python scripts/launch.py

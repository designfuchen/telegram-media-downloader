"""Generate a private configuration without printing credentials."""
import argparse
import os
from pathlib import Path
import secrets
import sys
import json


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker", action="store_true")
    parser.add_argument("--directory", type=Path, help="私有配置目录；容器应挂载整个目录")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    private_directory = args.directory.resolve() if args.directory else root
    private_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    destination = private_directory / "config.yaml"
    if destination.exists():
        parser.exit(1, "config.yaml 已存在，未覆盖。\n")
    content = (root / "config.example.yaml").read_text(encoding="utf-8")
    content = content.replace('web_login_secret: "CHANGE_ME"',
                              f'web_login_secret: "{secrets.token_urlsafe(32)}"')
    if args.docker:
        content = content.replace("web_host: 127.0.0.1", "web_host: 0.0.0.0")
        content = content.replace('save_path: "./downloads"', 'save_path: "/app/downloads"')
    elif sys.platform == 'darwin':
        save_path = Path.home() / 'Downloads' / 'Telegram'
        content = content.replace('save_path: "./downloads"', 'save_path: ' + json.dumps(str(save_path), ensure_ascii=False))
        save_path.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(destination, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        parser.exit(1, "config.yaml 已存在，未覆盖。\n")
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        handle.write(content)
    for name in ("downloads", "sessions", "log", "temp", "dbdata", "output"):
        (root / name).mkdir(exist_ok=True)
    print("已创建私有 config.yaml。启动后可在设置页填写 Telegram 信息并选择文件夹。访问密码请在文件中查看。")


if __name__ == "__main__":
    main()

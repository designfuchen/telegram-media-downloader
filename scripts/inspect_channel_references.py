"""Inspect all link and attachment shapes in a Telegram channel."""

import argparse
import sys
import asyncio
import sqlite3
import tempfile
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from module import config_io
from pyrogram import Client
from ruamel import yaml

from extract_channel_chat_ids import backup_session, message_urls


async def main_async(args):
    loader = yaml.YAML(typ="safe")
    config = config_io.load(args.config, loader)
    with tempfile.TemporaryDirectory(prefix="tmd-reference-scan-") as temp_dir:
        workdir = Path(temp_dir)
        backup_session(Path(args.session), workdir / "reference_scanner.session")
        client = Client(
            "reference_scanner",
            api_id=int(config["api_id"]),
            api_hash=str(config["api_hash"]),
            proxy=config.get("proxy") or None,
            workdir=str(workdir),
            no_updates=True,
        )
        await client.start()
        try:
            rows = []
            hosts = Counter()
            async for message in client.get_chat_history(args.chat_id):
                urls = message_urls(message)
                body = "\n".join(filter(None, [message.text, message.caption]))
                for token in body.split():
                    if "t.me/" in token or "telegram.me/" in token:
                        urls.append(token.strip("()[]{}<>,，。'\""))
                document = getattr(message, "document", None)
                filename = getattr(document, "file_name", "") if document else ""
                if urls or filename:
                    for url in urls:
                        parts = [
                            part
                            for part in urlparse(
                                url if "://" in url else "https://" + url
                            ).path.split("/")
                            if part
                        ]
                        hosts[parts[0] if parts else "root"] += 1
                    rows.append(
                        {
                            "id": message.id,
                            "date": message.date.isoformat() if message.date else "",
                            "urls": list(dict.fromkeys(urls)),
                            "file": filename,
                            "text": body[:300].replace("\n", " "),
                        }
                    )
            print(f"messages_with_refs={len(rows)} path_types={dict(hosts)}")
            for row in reversed(rows):
                print(f"\n#{row['id']} {row['date']} file={row['file']}")
                print(f"text={row['text']}")
                for url in row["urls"]:
                    print(f"url={url}")
        finally:
            await client.stop()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("chat_id")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--session", default="sessions/media_downloader.session")
    args = parser.parse_args()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()

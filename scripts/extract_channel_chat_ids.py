"""Extract referenced Telegram chat IDs from one channel in message order."""

import argparse
import sys
import asyncio
import csv
import re
import shutil
import sqlite3
import tempfile
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from module import config_io
from pyrogram import Client
from ruamel import yaml


PRIVATE_LINK_RE = re.compile(r"(?:https?://)?t\.me/c/(\d+)(?:/\d+)?", re.I)
PUBLIC_LINK_RE = re.compile(
    r"(?:https?://)?(?:www\.)?t\.me/([A-Za-z][A-Za-z0-9_]{3,})(?:/\d+)?",
    re.I,
)
CHAT_ID_RE = re.compile(r"(?<!\d)(-100\d{6,})(?!\d)")
IGNORED_PATHS = {"c", "s", "joinchat", "addlist", "share", "proxy", "socks"}


def backup_session(source: Path, destination: Path) -> None:
    """Copy into encrypted storage; never write a plaintext session snapshot."""
    from module import config_io, secret_storage
    from module.secure_session import install
    from utils.crypto import PREFIX
    if not secret_storage.enabled():
        raise ValueError("请先配置 TMD_SECRET_KEY，或使用 Mac 应用的钥匙串组件。")
    install()
    data = source.read_bytes()
    if data.startswith(PREFIX):
        snapshot = secret_storage.unseal(data, source)
    else:
        with sqlite3.connect(source.resolve().as_uri()+"?mode=ro",uri=True) as source_db, sqlite3.connect(":memory:") as memory:
            source_db.backup(memory)
            snapshot = memory.serialize()
    config_io.atomic_write(destination, secret_storage.seal(snapshot, destination))


def message_urls(message) -> list[str]:
    """Collect URL entities and inline-keyboard URLs."""
    urls = []
    for body_name, entity_name in (("text", "entities"), ("caption", "caption_entities")):
        body = getattr(message, body_name, None) or ""
        for entity in getattr(message, entity_name, None) or []:
            if getattr(entity, "url", None):
                urls.append(entity.url)
            elif str(getattr(entity, "type", "")).endswith("TEXT_LINK"):
                urls.append(getattr(entity, "url", ""))
            elif str(getattr(entity, "type", "")).endswith("URL"):
                urls.append(body[entity.offset : entity.offset + entity.length])
    markup = getattr(message, "reply_markup", None)
    for row in getattr(markup, "inline_keyboard", None) or []:
        for button in row:
            if getattr(button, "url", None):
                urls.append(button.url)
    return [url for url in urls if url]


def references_from_text(text: str) -> tuple[set[str], set[str]]:
    """Return direct numeric IDs and public usernames from text or URLs."""
    numeric = set(CHAT_ID_RE.findall(text or ""))
    numeric.update(f"-100{match}" for match in PRIVATE_LINK_RE.findall(text or ""))
    usernames = {
        match
        for match in PUBLIC_LINK_RE.findall(text or "")
        if match.lower() not in IGNORED_PATHS
    }
    return numeric, usernames


async def extract(args) -> list[dict]:
    config_loader = yaml.YAML(typ="safe")
    config = config_io.load(args.config, config_loader)
    source_session = Path(args.session).expanduser().resolve()

    with tempfile.TemporaryDirectory(prefix="tmd-chat-scan-") as temp_dir:
        workdir = Path(temp_dir)
        backup_session(source_session, workdir / "chat_scanner.session")
        client = Client(
            "chat_scanner",
            api_id=int(config["api_id"]),
            api_hash=str(config["api_hash"]),
            proxy=config.get("proxy") or None,
            workdir=str(workdir),
            no_updates=True,
        )
        await client.start()
        try:
            source_chat = await client.get_chat(args.chat_id)
            messages = []
            async for message in client.get_chat_history(args.chat_id):
                messages.append(message)
            messages.reverse()

            records = {}

            async def add_reference(chat_id: str, message, source: str, reference: str):
                key = str(chat_id)
                if key == str(args.chat_id) or key in records:
                    return
                title = ""
                username = ""
                chat_type = "unknown"
                try:
                    chat = await client.get_chat(key)
                    chat_type = str(chat.type).split(".")[-1].lower()
                    if chat_type in {"private", "bot"}:
                        return
                    title = chat.title or chat.first_name or ""
                    username = chat.username or ""
                    key = str(chat.id)
                except Exception:  # Keep inaccessible/deleted references in the result.
                    pass
                if key == str(args.chat_id) or key in records:
                    return
                records[key] = {
                    "order": len(records) + 1,
                    "chat_id": key,
                    "title": title,
                    "username": username,
                    "chat_type": chat_type,
                    "first_message_id": message.id,
                    "message_date": message.date.isoformat() if message.date else "",
                    "source": source,
                    "reference": reference,
                }

            for index, message in enumerate(messages, start=1):
                content = "\n".join(filter(None, [message.text, message.caption, *message_urls(message)]))
                numeric_ids, usernames = references_from_text(content)
                for chat_id in sorted(numeric_ids):
                    await add_reference(chat_id, message, "链接或正文", chat_id)
                for username in sorted(usernames, key=str.lower):
                    try:
                        chat = await client.get_chat(username)
                    except Exception:
                        continue
                    await add_reference(str(chat.id), message, "公开频道链接", f"@{username}")

                forward_chat = getattr(message, "forward_from_chat", None)
                if forward_chat and str(forward_chat.id).startswith("-100"):
                    await add_reference(
                        str(forward_chat.id), message, "转发来源", forward_chat.title or ""
                    )

                if index % 500 == 0:
                    print(f"scanned={index}/{len(messages)} found={len(records)}", flush=True)

            print(
                f"source={source_chat.title or args.chat_id} messages={len(messages)} found={len(records)}",
                flush=True,
            )
            return list(records.values())
        finally:
            await client.stop()


def write_outputs(records: list[dict], output_prefix: Path) -> None:
    output_prefix.parent.mkdir(parents=True, exist_ok=True)
    csv_path = output_prefix.with_suffix(".csv")
    txt_path = output_prefix.with_suffix(".txt")
    fields = [
        "order", "chat_id", "title", "username", "chat_type", "first_message_id",
        "message_date", "source", "reference",
    ]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    txt_path.write_text(
        "\n".join(record["chat_id"] for record in records) + ("\n" if records else ""),
        encoding="utf-8",
    )
    print(f"csv={csv_path}\ntxt={txt_path}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("chat_id")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--session", default="sessions/media_downloader.session")
    parser.add_argument("--output", default="output/channel_chat_ids")
    args = parser.parse_args()
    records = asyncio.run(extract(args))
    write_outputs(records, Path(args.output))


if __name__ == "__main__":
    main()

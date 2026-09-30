"""Export private Telegram child-channel links in source message order."""

import argparse
import sys
import asyncio
import csv
import re
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from module import config_io
from pyrogram import Client
from ruamel import yaml

from extract_channel_chat_ids import backup_session


INVITE_RE = re.compile(
    r"(?:https?://)?(?:www\.)?t(?:elegram)?\.(?:me|org)/(?:joinchat/|\+)([A-Za-z0-9_-]+)",
    re.I,
)


def entity_links(message) -> list[tuple[str, str]]:
    """Return entity labels and URLs in their original order."""
    result = []
    for body_name, entity_name in (("text", "entities"), ("caption", "caption_entities")):
        body = getattr(message, body_name, None) or ""
        for entity in getattr(message, entity_name, None) or []:
            url = getattr(entity, "url", None)
            if url and INVITE_RE.fullmatch(url.strip()):
                label = body[entity.offset : entity.offset + entity.length].strip()
                result.append((label, url.strip()))
    markup = getattr(message, "reply_markup", None)
    for row in getattr(markup, "inline_keyboard", None) or []:
        for button in row:
            url = getattr(button, "url", None)
            if url and INVITE_RE.fullmatch(url.strip()):
                result.append((getattr(button, "text", "").strip(), url.strip()))
    return result


async def export(args) -> list[dict]:
    loader = yaml.YAML(typ="safe")
    config = config_io.load(args.config, loader)
    with tempfile.TemporaryDirectory(prefix="tmd-invite-export-") as temp_dir:
        workdir = Path(temp_dir)
        backup_session(Path(args.session), workdir / "invite_exporter.session")
        client = Client(
            "invite_exporter",
            api_id=int(config["api_id"]),
            api_hash=str(config["api_hash"]),
            proxy=config.get("proxy") or None,
            workdir=str(workdir),
            no_updates=True,
        )
        await client.start()
        try:
            messages = []
            async for message in client.get_chat_history(args.chat_id):
                messages.append(message)
            messages.reverse()
            seen = set()
            records = []
            for message in messages:
                for label, link in entity_links(message):
                    match = INVITE_RE.fullmatch(link)
                    canonical = f"https://t.me/+{match.group(1)}"
                    if canonical in seen:
                        continue
                    seen.add(canonical)
                    records.append(
                        {
                            "order": len(records) + 1,
                            "label": label,
                            "chat_id": "",
                            "invite_link": canonical,
                            "first_message_id": message.id,
                            "message_date": message.date.isoformat() if message.date else "",
                            "resolution_status": "需加入后获取 Chat ID",
                        }
                    )
            return records
        finally:
            await client.stop()


def write(records: list[dict], output: Path) -> None:
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "order", "label", "chat_id", "invite_link", "first_message_id",
        "message_date", "resolution_status",
    ]
    with output.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(records)
    output.with_suffix(".txt").write_text(
        "\n".join(record["invite_link"] for record in records) + "\n",
        encoding="utf-8",
    )
    print(f"records={len(records)} csv={output} txt={output.with_suffix('.txt')}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("chat_id")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--session", default="sessions/media_downloader.session")
    parser.add_argument("--output", default="output/channel_invites.csv")
    args = parser.parse_args()
    write(asyncio.run(export(args)), Path(args.output))


if __name__ == "__main__":
    main()

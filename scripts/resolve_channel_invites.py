"""Resolve numeric IDs for private invite links without joining chats."""

import argparse
import sys
import asyncio
import csv
import sqlite3
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from module import config_io
from pyrogram import Client, raw, utils
from pyrogram.errors import FloodWait, RPCError
from ruamel import yaml

from extract_channel_chat_ids import backup_session


def numeric_chat_id(chat) -> str:
    if isinstance(chat, raw.types.Channel):
        return str(utils.get_channel_id(chat.id))
    if isinstance(chat, raw.types.Chat):
        return str(-chat.id)
    return ""


async def resolve(args) -> None:
    output = Path(args.output)
    with Path(args.input).open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    if output.exists():
        with output.open(encoding="utf-8-sig", newline="") as handle:
            previous = {
                row["invite_link"]: row for row in csv.DictReader(handle)
            }
        rows = [previous.get(row["invite_link"], row) for row in rows]
    loader = yaml.YAML(typ="safe")
    config = config_io.load(args.config, loader)

    fields = list(rows[0]) if rows else []
    for field in ("title", "chat_type", "resolution_status", "error"):
        if field not in fields:
            fields.append(field)

    def checkpoint() -> None:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8-sig", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=fields)
            writer.writeheader()
            writer.writerows(rows)

    with tempfile.TemporaryDirectory(prefix="tmd-invite-resolve-") as temp_dir:
        workdir = Path(temp_dir)
        backup_session(Path(args.session), workdir / "invite_resolver.session")
        client = Client(
            "invite_resolver",
            api_id=int(config["api_id"]),
            api_hash=str(config["api_hash"]),
            proxy=config.get("proxy") or None,
            workdir=str(workdir),
            no_updates=True,
            sleep_threshold=30,
        )
        await client.start()
        try:
            attempted = 0
            for index, row in enumerate(rows, start=1):
                if row.get("resolution_status") in {
                    "已解析",
                    "未加入，Telegram 不返回 Chat ID",
                    "邀请链接失效或不可访问",
                }:
                    continue
                if args.max_items and attempted >= args.max_items:
                    break
                attempted += 1
                invite_hash = row["invite_link"].rsplit("+", 1)[-1]
                while True:
                    try:
                        result = await client.invoke(
                            raw.functions.messages.CheckChatInvite(hash=invite_hash)
                        )
                        break
                    except FloodWait as exc:
                        wait_seconds = int(exc.value) + 1
                        print(f"flood_wait={wait_seconds}s at={index}", flush=True)
                        checkpoint()
                        if wait_seconds > args.max_flood_wait:
                            print("stopped_to_protect_account=true", flush=True)
                            return
                        await asyncio.sleep(wait_seconds)
                try:
                    if isinstance(result, raw.types.ChatInvite):
                        row["title"] = result.title
                        row["chat_type"] = (
                            "channel" if result.broadcast else
                            "supergroup" if result.channel else "group"
                        )
                        row["resolution_status"] = "未加入，Telegram 不返回 Chat ID"
                    else:
                        row["chat_id"] = numeric_chat_id(result.chat)
                        row["title"] = getattr(result.chat, "title", "")
                        row["chat_type"] = (
                            "channel" if getattr(result.chat, "broadcast", False)
                            else "supergroup" if isinstance(result.chat, raw.types.Channel)
                            else "group"
                        )
                        row["resolution_status"] = "已解析"
                    row["error"] = ""
                except RPCError as exc:
                    row["resolution_status"] = "邀请链接失效或不可访问"
                    row["error"] = type(exc).__name__
                if index % 50 == 0:
                    resolved = sum(bool(item.get("chat_id")) for item in rows[:index])
                    print(f"checked={index}/{len(rows)} resolved={resolved}", flush=True)
                    checkpoint()
                await asyncio.sleep(args.delay)
        finally:
            checkpoint()
            await client.stop()

    resolved_rows = [row for row in rows if row.get("chat_id")]
    unresolved_rows = [row for row in rows if not row.get("chat_id")]
    output.with_name(output.stem + "_chat_ids.txt").write_text(
        "\n".join(row["chat_id"] for row in resolved_rows) + "\n",
        encoding="utf-8",
    )
    unresolved_path = output.with_name(output.stem + "_unresolved.csv")
    with unresolved_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(unresolved_rows)
    print(
        f"complete total={len(rows)} resolved={len(resolved_rows)} "
        f"unresolved={len(unresolved_rows)} output={output}",
        flush=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--session", default="sessions/media_downloader.session")
    parser.add_argument("--delay", type=float, default=0.12)
    parser.add_argument("--max-items", type=int, default=10)
    parser.add_argument("--max-flood-wait", type=int, default=30)
    asyncio.run(resolve(parser.parse_args()))


if __name__ == "__main__":
    main()

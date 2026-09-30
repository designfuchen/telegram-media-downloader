"""One-command helper for the "you join manually, I do the rest" workflow.

Run this any time after you've manually joined a fresh batch of channels
from a scanned CSV (produced by extract_channel_chat_ids.py /
resolve_channel_invites.py). It is fully safe to re-run as often as you
like -- it never joins anything itself, it only:

  1. Re-checks which invite links in the CSV your account can now see a
     real Chat ID for (this uses Telegram's CheckChatInvite preview call,
     which does NOT join anything -- it just reports what you've already
     joined by hand). Already-resolved / already-failed rows are skipped,
     so this step is cheap and incremental.
  2. Pushes every newly-resolved row into the downloader's import queue
     (the same "channel_import_batches" table the web console's "频道导入"
     page uses). It does NOT auto-confirm the batch -- you still review and
     click "确认导入" in the web UI, so nothing starts downloading without
     you seeing it first.

Run this INSIDE the running tmd container so it shares the exact same
session, config, proxy and database as the live app:

    sudo docker exec tmd python scripts/sync_and_import.py \\
        --input /app/downloads/channel_-1001234567890_resolved.csv

(Put the CSV under the mounted downloads/ folder first so the container
can see it, e.g. copy it to /path/to/downloader/downloads/ on the NAS.)
"""

import argparse
import csv
import io
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from module.download_tasks import create_import_batch  # noqa: E402


def _run_resolver(input_csv: Path, config: str, session: str, max_items: int) -> None:
    """Incrementally re-resolve invite links in place (safe, never joins)."""
    script = Path(__file__).resolve().parent / "resolve_channel_invites.py"
    command = [
        sys.executable,
        str(script),
        "--input",
        str(input_csv),
        "--output",
        str(input_csv),
        "--config",
        config,
        "--session",
        session,
        "--max-items",
        str(max_items),
    ]
    print(f"[1/2] 重新解析邀请链接（安全，不会加群）: {' '.join(command)}")
    result = subprocess.run(command, check=False)
    if result.returncode != 0:
        print("解析步骤返回非零退出码，继续尝试导入已有的解析结果", file=sys.stderr)


def _rows_ready_to_import(input_csv: Path) -> list:
    """Return CSV rows that now have a resolved chat_id."""
    with input_csv.open(encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    return [row for row in rows if str(row.get("chat_id") or "").strip()]


def _build_import_content(rows: list) -> str:
    """Format resolved rows as the plain chat_id,,group_name,priority lines
    the existing 频道导入 parser already understands."""
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    for row in rows:
        chat_id = str(row.get("chat_id") or "").strip()
        title = str(row.get("title") or row.get("label") or "").strip()
        if not chat_id:
            continue
        writer.writerow([chat_id, "", title, ""])
    return buffer.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="扫描得到的 CSV（会被增量更新）")
    parser.add_argument("--config", default="config.yaml")
    parser.add_argument("--session", default="sessions/media_downloader.session")
    parser.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="本次最多重新检查多少条未解析的邀请链接（0 = 全部检查一遍，安全，不涉及加群）",
    )
    parser.add_argument(
        "--skip-resolve",
        action="store_true",
        help="跳过重新解析，只用 CSV 当前内容直接导入",
    )
    args = parser.parse_args()

    input_csv = Path(args.input)
    if not input_csv.exists():
        print(f"找不到文件: {input_csv}", file=sys.stderr)
        sys.exit(1)

    if not args.skip_resolve:
        _run_resolver(input_csv, args.config, args.session, args.max_items)

    rows = _rows_ready_to_import(input_csv)
    print(f"[2/2] CSV 中当前已解析出 Chat ID 的频道: {len(rows)} 个")

    content = _build_import_content(rows)
    if not content.strip():
        print("没有可导入的频道（还没有新解析出 Chat ID 的频道）")
        return

    batch = create_import_batch(
        content, source_name=f"手动加群同步 - {input_csv.name}"
    )
    print(
        f"已创建导入批次 #{batch['id']}：共 {batch['total_count']} 条，"
        f"请到网页控制台「配置 -> 频道导入」查看校验结果并点击「确认导入」。"
    )


if __name__ == "__main__":
    main()

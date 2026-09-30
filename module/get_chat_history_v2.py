"""Rewrite pyrogram.get_chat_history"""

from datetime import datetime
from typing import AsyncGenerator, Optional, Union

import pyrogram

# pylint: disable = W0611
from pyrogram import raw, types, utils

from module import rate_governor
from module.async_utils import telegram_call


HISTORY_REQUEST_TIMEOUT = 45


async def get_chunk_v2(
    *,
    client: pyrogram.Client,
    chat_id: Union[int, str],
    limit: int = 0,
    offset: int = 0,
    max_id: int = 0,
    from_message_id: int = 0,
    from_date: datetime = utils.zero_datetime(),
    reverse: bool = False
):
    """get chunk"""
    from_message_id = from_message_id or (1 if reverse else 0)

    messages = await utils.parse_messages(
        client,
        await client.invoke(
            raw.functions.messages.GetHistory(
                peer=await client.resolve_peer(chat_id),
                offset_id=from_message_id,
                offset_date=utils.datetime_to_timestamp(from_date),
                add_offset=offset * (-1 if reverse else 1) - (limit if reverse else 0),
                limit=limit,
                max_id=max_id,
                min_id=0,
                hash=0,
            ),
            sleep_threshold=5,
        ),
        replies=0,
    )

    if reverse:
        messages.reverse()

    return messages


# pylint: disable = C0301
async def get_chat_history_v2(
    self: pyrogram.Client,
    chat_id: Union[int, str],
    limit: int = 0,
    max_id: int = 0,
    offset: int = 0,
    offset_id: int = 0,
    offset_date: datetime = utils.zero_datetime(),
    reverse: bool = False,
) -> Optional[AsyncGenerator["types.Message", None]]:
    """Get messages from a chat history."""
    current = 0
    total = limit or (1 << 31) - 1
    limit = min(100, total)

    while True:
        # GetHistory is an account-level metadata request.  Pace the request
        # itself (not the messages yielded afterwards) so multiple channel
        # scanners cannot burst against Telegram at the same time.
        await rate_governor.acquire()
        messages = await telegram_call(
            get_chunk_v2(
                client=self,
                chat_id=chat_id,
                limit=limit,
                offset=offset,
                max_id=max_id + 1 if max_id else 0,
                from_message_id=offset_id,
                from_date=offset_date,
                reverse=reverse,
            ),
            HISTORY_REQUEST_TIMEOUT,
            "GetHistory",
        )

        if not messages:
            # An empty GetHistory page is the end of this scan.  Falling back
            # to Client.get_chat_history here used to restart a second history
            # walk and could revisit thousands of messages after completion.
            return

        offset_id = messages[-1].id + (1 if reverse else 0)

        for message in messages:
            yield message

            current += 1

            if current >= total:
                return

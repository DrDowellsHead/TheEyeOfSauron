"""sender.py — отправка отчёта: всегда в Избранное, опционально в чат ответом на опрос."""

import os
from typing import Optional

from telethon import TelegramClient


async def send_report(
        client: TelegramClient,
        *,
        report: str,
        chat_entity,
        poll_msg_id: int,
        send_to_chat: bool,
        image_path: str = "TheEye.jpg",
        image_caption: str = "I see you, little hobbit!",
) -> None:
    async def send_bundle(target, reply_to: Optional[int] = None):
        if image_path and os.path.exists(image_path):
            await client.send_file(target, image_path, caption=image_caption, reply_to=reply_to)
        if reply_to:
            await client.send_message(target, report, reply_to=reply_to)
        else:
            await client.send_message(target, report)

    # 1) всегда в Избранное
    await send_bundle("me", reply_to=None)

    # 2) опционально в чат ответом на опрос
    if send_to_chat:
        await send_bundle(chat_entity, reply_to=poll_msg_id)

"""tg_chat.py — выбор чата (--pick-chat) и резолв chat_id/@username в entity."""

import re

from telethon import TelegramClient
from telethon.tl import types
from telethon.utils import get_peer_id

from .core_log import log


def entity_kind(ent) -> str:
    if isinstance(ent, types.User):
        return "user"
    if isinstance(ent, types.Chat):
        return "chat"
    if isinstance(ent, types.Channel):
        return "channel/supergroup"
    return type(ent).__name__


def parse_chat_ref(chat_ref):
    if isinstance(chat_ref, int):
        return chat_ref
    if not isinstance(chat_ref, str):
        return chat_ref
    s = chat_ref.strip()
    if re.fullmatch(r"-?\d+", s):
        n = int(s)
        if n < 0 and not s.startswith("-100"):
            return abs(n)
        return n
    return s


async def resolve_chat_entity(client: TelegramClient, chat_ref, scan_limit: int = 200):
    ref = parse_chat_ref(chat_ref)

    try:
        return await client.get_entity(ref)
    except Exception:
        pass

    target = ref if isinstance(ref, int) else None
    if target is None:
        raise ValueError(f"Cannot find any entity corresponding to {chat_ref!r}")

    i = 0
    async for d in client.iter_dialogs():
        ent = d.entity
        pid = get_peer_id(ent)
        if pid == target or pid == -target:
            return ent
        i += 1
        if i >= scan_limit:
            break

    raise ValueError(f"Cannot find any entity corresponding to {chat_ref!r} (scanned {scan_limit} dialogs)")


async def pick_chat_interactively(client: TelegramClient, limit: int = 30):
    dialogs = []
    i = 0
    async for d in client.iter_dialogs():
        dialogs.append(d)
        i += 1
        if i >= limit:
            break

    log("\n📚 Диалоги:")
    for idx, d in enumerate(dialogs, start=1):
        ent = d.entity
        log(f"{idx:>2}. {d.name} | id={d.id} | type={entity_kind(ent)}")

    raw = input("\nНомер диалога (Enter = 1): ").strip()
    n = 1 if raw == "" else int(raw)
    n = max(1, min(n, len(dialogs)))
    chosen = dialogs[n - 1].entity

    title = getattr(chosen, "title", getattr(chosen, "first_name", ""))
    cid = getattr(chosen, "id", None)
    log(f"✅ Выбран чат: {title} (id={cid})\n")
    return chosen

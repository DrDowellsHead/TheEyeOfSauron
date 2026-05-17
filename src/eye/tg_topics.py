"""tg_topics.py — работа с темами форума: list/поиск/выбор topic_id."""

from typing import Optional

from telethon import TelegramClient, functions

from .core_log import log


async def get_forum_topics(client: TelegramClient, chat_entity, query: Optional[str], limit: int = 100):
    q = query if query else None

    if hasattr(functions.channels, "GetForumTopicsRequest"):
        req = functions.channels.GetForumTopicsRequest(
            channel=chat_entity,
            q=q,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=limit,
        )
    elif hasattr(functions.messages, "GetForumTopicsRequest"):
        req = functions.messages.GetForumTopicsRequest(
            peer=chat_entity,
            q=q,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=limit,
        )
    else:
        raise RuntimeError(
            "В вашей версии Telethon нет getForumTopics.\n"
            "Обновите: python -m pip install -U telethon"
        )

    res = await client(req)
    return getattr(res, "topics", []) or []


async def choose_topic_id(client: TelegramClient, chat_entity, topic_title_query: str) -> int:
    topics = await get_forum_topics(client, chat_entity, query=topic_title_query, limit=200)
    if not topics:
        raise RuntimeError(f"Не нашёл темы по запросу: {topic_title_query}")

    if len(topics) == 1:
        t = topics[0]
        log(f"✅ Тема найдена: ID={t.id} | {t.title}")
        return int(t.id)

    log("\n📌 Нашлось несколько тем. Выбери:")
    for i, t in enumerate(topics, start=1):
        log(f"{i:>2}. ID={t.id} | {t.title}")

    raw = input("\nНомер темы (Enter = 1): ").strip()
    idx = 1 if raw == "" else int(raw)
    idx = max(1, min(idx, len(topics)))
    chosen = topics[idx - 1]
    log(f"✅ Выбрана тема: ID={chosen.id} | {chosen.title}")
    return int(chosen.id)

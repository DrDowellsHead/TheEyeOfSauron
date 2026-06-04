"""tg_polls.py — поиск опросов и сбор голосов (включая smart-sort и union по 'Смогу...')."""

import re
from typing import List, Optional, Set, Tuple

from telethon import TelegramClient, functions
from telethon.tl import types
from telethon.tl.types import MessageMediaPoll

from .core_log import log
from .text_utils import as_text

# Это учитывание вариантов по-умолчанию
DEFAULT_YES_KEYWORDS = ["✅", "приду", "смогу", "буду"]


async def find_polls_in_topic(client, chat, topic_id: int, limit: int):
    polls = []
    kwargs = {}
    if topic_id > 0:
        kwargs["reply_to"] = topic_id

    async for msg in client.iter_messages(chat, limit=limit, **kwargs):
        if isinstance(getattr(msg, "media", None), MessageMediaPoll):
            q = as_text(msg.media.poll.question)
            polls.append((msg, q))
    return polls


def pick_poll(polls, poll_query: Optional[str]):
    if not polls:
        return None

    if poll_query:
        pq = poll_query.casefold()
        matches = [(m, q) for (m, q) in polls if pq in (q or "").casefold()]
        if len(matches) == 1:
            return matches[0][0]

        if len(matches) > 1:
            log("🗳️ Нашлось несколько опросов по запросу. Выбери нужный:")
            for i, (m, q) in enumerate(matches, start=1):
                d = m.date.strftime("%Y-%m-%d %H:%M") if m.date else "?"
                log(f"{i:>2}. [{d}] id={m.id} | {q[:90]}")
            raw = input("\nНомер опроса (Enter = 1): ").strip()
            idx = 1 if raw == "" else int(raw)
            idx = max(1, min(idx, len(matches)))
            return matches[idx - 1][0]

        log("⚠️ По --poll ничего не найдено, беру самый последний опрос в теме.")

    return polls[0][0]


def is_yes_option_text(txt: str, positive_keywords: List[str]) -> bool:
    t = (txt or "").strip().casefold()
    t = " ".join(t.split())

    if t.startswith("не "):
        for kw in positive_keywords:
            k = (kw or "").strip().casefold()
            if k and k in t:
                return False

    if "не смогу" in t:
        return False
    if "не приду" in t:
        return False
    if re.search(r"\bне\s+буду\b", t):
        return False

    for kw in positive_keywords:
        k = (kw or "").strip().casefold()
        if not k:
            continue
        if k in t:
            return True

    return False


def extract_time_minutes(txt: str) -> Optional[int]:
    t = (txt or "").strip().casefold()
    t = " ".join(t.split())
    m = re.search(r"(?:\bв\b|\bк\b)\s*(\d{1,2})(?::(\d{2}))?\b", t)
    if not m:
        return None
    hh = int(m.group(1))
    mm = int(m.group(2) or "0")
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        return None
    return hh * 60 + mm


def kw_rank(txt: str) -> int:
    t = (txt or "").strip().casefold()
    t = " ".join(t.split())

    if "саунд" in t or "чек" in t:
        return 0
    if "репет" in t:
        return 1
    if "концерт" in t:
        return 2
    return 3


async def fetch_poll_voters_yes_union(
        client: TelegramClient,
        chat_peer,
        poll_msg,
        votes_page_size: int,
        smart_sort: bool,
        positive_keywords: Optional[List[str]] = None,
) -> Tuple[Set[int], List[str]]:
    poll = poll_msg.media.poll

    if not positive_keywords:
        positive_keywords = DEFAULT_YES_KEYWORDS

    targets = []
    for ans in poll.answers:
        txt = as_text(ans.text)
        if is_yes_option_text(txt, positive_keywords):
            targets.append(ans)

    if not targets:
        answers_debug = "\n".join([f"- {as_text(a.text)}" for a in poll.answers])
        raise RuntimeError("В опросе нет позитивных вариантов (✅/приду/смогу).\n" + answers_debug)

    if smart_sort:
        index_map = {id(a): i for i, a in enumerate(targets)}

        def sort_key(a):
            txt = as_text(a.text)
            tmin = extract_time_minutes(txt)
            if tmin is not None:
                return (0, tmin, kw_rank(txt), index_map[id(a)])
            return (1, kw_rank(txt), 10_000, index_map[id(a)])

        targets = sorted(targets, key=sort_key)

    if not getattr(poll, "public_voters", False):
        raise RuntimeError("Опрос анонимный — Telegram не отдаёт список проголосовавших.")

    voter_ids: Set[int] = set()
    option_texts: List[str] = []

    for target in targets:
        option_text = as_text(target.text)
        option_texts.append(option_text)
        log(f"⬇️  Загружаю голоса за: {option_text}")

        offset = None
        while True:
            res = await client(functions.messages.GetPollVotesRequest(
                peer=chat_peer,
                id=poll_msg.id,
                option=target.option,
                offset=offset,
                limit=votes_page_size
            ))

            for v in getattr(res, "votes", []) or []:
                peer = getattr(v, "peer", None)
                if isinstance(peer, types.PeerUser):
                    voter_ids.add(int(peer.user_id))

            for u in getattr(res, "users", []) or []:
                if getattr(u, "id", None):
                    voter_ids.add(int(u.id))

            next_offset = getattr(res, "next_offset", None)
            if not next_offset:
                break
            offset = next_offset

    return voter_ids, option_texts

import asyncio
import os
import argparse
from datetime import datetime
import configparser
import re

from telethon import TelegramClient, functions, errors
from telethon.tl import types
from telethon.tl.types import MessageMediaPoll


import configparser
import os

def load_config(path="config.ini"):
    cfg = configparser.ConfigParser()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Не найден {path}. Создай его из config.example.ini и заполни значения."
        )
    cfg.read(path, encoding="utf-8")

    # telegram
    api_id = int(cfg["telegram"]["api_id"])
    api_hash = cfg["telegram"]["api_hash"].strip()
    session_name = cfg["telegram"].get("session_name", "orchestra_parser").strip()

    chat_id = int(cfg["telegram"]["chat_id"])
    default_topic_id = int(cfg["telegram"].get("default_topic_id", "0"))

    # files
    musicians_csv = cfg["files"].get("musicians_csv", "Музыканты.csv").strip()

    # search
    search_limit = int(cfg["search"].get("search_limit", "300"))
    votes_page_size = int(cfg["search"].get("votes_page_size", "100"))

    return {
        "API_ID": api_id,
        "API_HASH": api_hash,
        "SESSION_NAME": session_name,
        "CHAT_ID": chat_id,
        "DEFAULT_TOPIC_ID": default_topic_id,
        "MUSICIANS_CSV": musicians_csv,
        "SEARCH_LIMIT": search_limit,
        "VOTES_PAGE_SIZE": votes_page_size,
    }


# ====== ТВОИ НАСТРОЙКИ ======
CONF = load_config()

API_ID = CONF["API_ID"]
API_HASH = CONF["API_HASH"]
SESSION_NAME = CONF["SESSION_NAME"]

CHAT_ID = CONF["CHAT_ID"]
DEFAULT_TOPIC_ID = CONF["DEFAULT_TOPIC_ID"]  # тема по умолчанию (если не передали --topic-id/--topic)
MUSICIANS_CSV = CONF["MUSICIANS_CSV"]

SEARCH_LIMIT = CONF["SEARCH_LIMIT"]  # сколько сообщений в теме смотреть при поиске опросов
VOTES_PAGE_SIZE = CONF["VOTES_PAGE_SIZE"]


# ====== ВСПОМОГАТЕЛЬНОЕ ======
def as_text(x) -> str:
    if x is None:
        return ""
    return x.text if hasattr(x, "text") else str(x)


def plural_ru(n: int, form1: str, form2: str, form5: str) -> str:
    n = abs(int(n))
    n10 = n % 10
    n100 = n % 100
    if 11 <= n100 <= 14:
        return form5
    if n10 == 1:
        return form1
    if 2 <= n10 <= 4:
        return form2
    return form5


INSTR_FORMS = {
    "первые скрипки": ("первая скрипка", "первые скрипки", "первых скрипок"),
    "вторые скрипки": ("вторая скрипка", "вторые скрипки", "вторых скрипок"),
    "альт": ("альт", "альта", "альтов"),
    "виолончель": ("виолончель", "виолончели", "виолончелей"),
    "контрабас": ("контрабас", "контрабаса", "контрабасов"),
    "флейта": ("флейта", "флейты", "флейт"),
    "гобой": ("гобой", "гобоя", "гобоев"),
    "кларнет": ("кларнет", "кларнета", "кларнетов"),
    "фагот": ("фагот", "фагота", "фаготов"),
    "саксофон": ("саксофон", "саксофона", "саксофонов"),
    "валторна": ("валторна", "валторны", "валторн"),
    "труба": ("труба", "трубы", "труб"),
    "тромбон": ("тромбон", "тромбона", "тромбонов"),
    "туба": ("туба", "тубы", "туб"),
    "ударные": ("ударный", "ударных", "ударных"),
    "фортепиано": ("фортепиано", "фортепиано", "фортепиано"),
    "арфа": ("арфа", "арфы", "арф"),
    "дирижёр": ("дирижёр", "дирижёра", "дирижёров"),
}

ICON = {
    "первые скрипки": "🎻",
    "вторые скрипки": "🎻",
    "альт": "🎻",
    "виолончель": "🎻",
    "контрабас": "🎻",
    "флейта": "🎵",
    "гобой": "🎵",
    "кларнет": "🎵",
    "фагот": "🎵",
    "саксофон": "🎷",
    "валторна": "🎺",
    "труба": "🎺",
    "тромбон": "🎺",
    "туба": "🎺",
    "ударные": "🥁",
    "фортепиано": "🎹",
    "арфа": "🎶",
    "дирижёр": "👨‍🏫",
}


def normalize_instrument(raw: str) -> str:
    s = (raw or "").strip().lower().replace("ё", "е")

    if "скрип" in s:
        if "1" in s:
            return "первые скрипки"
        if "2" in s:
            return "вторые скрипки"
        return "первые скрипки"  # если в базе просто "скрипки" — лучше уточнить, но пусть так

    if "альт" in s:
        return "альт"
    if "виолонч" in s:
        return "виолончель"
    if "контрабас" in s:
        return "контрабас"

    if "флейт" in s:
        return "флейта"
    if "гобо" in s:
        return "гобой"
    if "кларнет" in s:
        return "кларнет"
    if "фагот" in s:
        return "фагот"
    if "сакс" in s:
        return "саксофон"

    if "валторн" in s:
        return "валторна"
    if "труба" in s:
        return "труба"
    if "тромбон" in s:
        return "тромбон"
    if "туба" in s:
        return "туба"

    if "удар" in s or "перкус" in s:
        return "ударные"
    if "фортеп" in s or "пианино" in s:
        return "фортепиано"
    if "арфа" in s:
        return "арфа"
    if "дириж" in s:
        return "дирижёр"

    return s or "неизвестно"


def load_musicians(path: str) -> dict[int, str]:
    """
    Пытаемся через pandas (если есть), иначе fallback на встроенный csv.
    CSV у тебя с разделителем ; и колонками user_id, Инструмент.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Не найден файл: {path}")

    try:
        import pandas as pd
        df = pd.read_csv(path, delimiter=";", encoding="utf-8-sig")
        musicians: dict[int, str] = {}
        for _, row in df.iterrows():
            uid = row.get("user_id")
            instr = row.get("Инструмент")
            if uid is None or instr is None:
                continue
            try:
                musicians[int(uid)] = str(instr).strip()
            except Exception:
                pass
        return musicians
    except ImportError:
        import csv
        musicians: dict[int, str] = {}
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f, delimiter=";")
            for row in r:
                uid = (row.get("user_id") or "").strip()
                instr = (row.get("Инструмент") or "").strip()
                if not uid or not instr:
                    continue
                try:
                    musicians[int(uid)] = instr
                except ValueError:
                    pass
        return musicians


async def get_forum_topics(client, chat_entity, query: str | None, limit: int = 100):
    q = query if query else None

    if hasattr(functions.channels, "GetForumTopicsRequest"):
        req = functions.channels.GetForumTopicsRequest(
            channel=chat_entity,
            q=q,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=limit
        )
    elif hasattr(functions.messages, "GetForumTopicsRequest"):
        req = functions.messages.GetForumTopicsRequest(
            peer=chat_entity,
            q=q,
            offset_date=None,
            offset_id=0,
            offset_topic=0,
            limit=limit
        )
    else:
        raise RuntimeError(
            "В вашей версии Telethon нет getForumTopics. Обновите telethon: python -m pip install -U telethon"
        )

    res = await client(req)
    return getattr(res, "topics", []) or []


async def choose_topic_id(client: TelegramClient, chat_entity, topic_title_query: str | None) -> int:
    topics = await get_forum_topics(client, chat_entity, query=topic_title_query, limit=100)

    if not topics:
        # если поиск ничего не дал — покажем все темы
        topics = await get_forum_topics(client, chat_entity, query=None, limit=100)

    print("\n📌 Темы форума:")
    for i, t in enumerate(topics, start=1):
        # t.id — это topic_id (его обычно и используют как reply_to для iter_messages)
        print(f"{i:>2}. ID={t.id} | {t.title}")

    raw = input("\nВыбери номер темы (Enter = 1): ").strip()
    idx = 1 if raw == "" else int(raw)
    idx = max(1, min(idx, len(topics)))
    chosen = topics[idx - 1]
    print(f"✅ Выбрана тема: ID={chosen.id} | {chosen.title}\n")
    return int(chosen.id)


async def find_polls_in_topic(client: TelegramClient, chat_id: int, topic_id: int, limit: int) -> list:
    polls = []
    async for msg in client.iter_messages(chat_id, limit=limit, reply_to=topic_id):
        if isinstance(getattr(msg, "media", None), MessageMediaPoll):
            poll = msg.media.poll
            q = as_text(poll.question)
            polls.append((msg, q))
    return polls  # уже в порядке от нового к старому


def pick_poll(polls: list, poll_query: str | None):
    if not polls:
        return None

    if poll_query:
        pq = poll_query.casefold()
        matches = [(m, q) for (m, q) in polls if pq in (q or "").casefold()]
        if len(matches) == 1:
            return matches[0][0]
        if len(matches) > 1:
            print("🗳️ Нашлось несколько опросов по запросу. Выбери нужный:")
            for i, (m, q) in enumerate(matches, start=1):
                d = m.date.strftime("%Y-%m-%d %H:%M") if m.date else "?"
                print(f"{i:>2}. [{d}] id={m.id} | {q[:90]}")
            raw = input("\nНомер опроса (Enter = 1): ").strip()
            idx = 1 if raw == "" else int(raw)
            idx = max(1, min(idx, len(matches)))
            return matches[idx - 1][0]

        # если по query не нашли — упадём на самый последний
        print("⚠️ По --poll ничего не найдено, беру самый последний опрос в теме.")

    # по умолчанию — самый последний (самый новый)
    return polls[0][0]


async def fetch_poll_voters_for_checkmark(client: TelegramClient, chat_peer, poll_msg, smart_sort: bool = False):
    poll = poll_msg.media.poll

    def norm(txt: str) -> str:
        t = (as_text(txt) or "").strip().casefold()
        return " ".join(t.split())

    def is_yes_option(txt) -> bool:
        t = norm(txt)

        # Явные "нет"
        if "не смогу" in t or (t.startswith("не") and "смогу" in t):
            return False
        if "не приду" in t or (t.startswith("не") and "приду" in t):
            return False

        # Репетиции
        if "✅" in t:
            return True
        if "приду" in t:
            return True

        # Концерты: все варианты "смогу ..."
        if "смогу" in t:
            return True

        return False

    def extract_time_minutes(txt: str) -> int | None:
        """
        Ищем время в варианте ответа.
        Считаем только то, что похоже на "в 13:00", "к 10", "в 9", "к 8:30".
        """
        t = norm(txt)

        # матч "в 13:00" / "к 10" / "в 9"
        m = re.search(r"(?:\bв\b|\bк\b)\s*(\d{1,2})(?::(\d{2}))?\b", t)
        if not m:
            return None

        hh = int(m.group(1))
        mm = int(m.group(2) or "0")
        if not (0 <= hh <= 23 and 0 <= mm <= 59):
            return None
        return hh * 60 + mm

    def kw_rank(txt: str) -> int:
        """
        Порядок 'смысловых' вариантов, когда времени нет.
        Меньше = раньше в списке.
        """
        t = norm(txt)
        if "саунд" in t or "чек" in t:
            return 0
        if "репет" in t:
            return 1
        if "концерт" in t:
            return 2
        return 3

    # 1) Находим ВСЕ подходящие опции (✅ / приду / смогу...)
    targets = [ans for ans in poll.answers if is_yes_option(ans.text)]

    if not targets:
        answers_debug = "\n".join([f"- {as_text(a.text)}" for a in poll.answers])
        raise RuntimeError(
            "В опросе нет подходящих вариантов (✅/приду/смогу...).\n"
            f"Варианты:\n{answers_debug}"
        )

    # 1.1) Умная сортировка (по флагу)
    if smart_sort:
        # сохраняем исходный порядок как последний критерий (стабильность)
        index_map = {id(ans): i for i, ans in enumerate(targets)}

        def sort_key(ans) -> tuple:
            txt = as_text(ans.text)
            tmin = extract_time_minutes(txt)
            # timed -> раньше, потом без времени по смыслу
            if tmin is not None:
                return (0, tmin, kw_rank(txt), index_map[id(ans)])
            return (1, kw_rank(txt), 10_000, index_map[id(ans)])

        targets = sorted(targets, key=sort_key)

    # 2) Проверяем, что опрос не анонимный
    if not getattr(poll, "public_voters", False):
        raise RuntimeError("Опрос анонимный — Telegram не отдаёт список проголосовавших.")

    # 3) Выгружаем проголосовавших по каждой опции и объединяем
    voter_ids = set()

    for target in targets:
        offset = None
        while True:
            res = await client(functions.messages.GetPollVotesRequest(
                peer=chat_peer,
                id=poll_msg.id,
                option=target.option,   # bytes
                offset=offset,
                limit=VOTES_PAGE_SIZE
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

    option_text = " / ".join(as_text(t.text) for t in targets)
    return voter_ids, option_text


def build_report(voter_ids: set[int], musicians: dict[int, str], header: str) -> str:
    counts: dict[str, int] = {}
    found = 0

    for uid in voter_ids:
        if uid not in musicians:
            continue
        found += 1
        key = normalize_instrument(musicians[uid])
        counts[key] = counts.get(key, 0) + 1

    order = [
        "первые скрипки", "вторые скрипки",
        "альт", "виолончель", "контрабас",
        "флейта", "гобой", "кларнет", "фагот", "саксофон",
        "валторна", "труба", "тромбон", "туба",
        "ударные", "фортепиано", "арфа", "дирижёр",
        "неизвестно",
    ]

    lines = [header, ""]
    total = 0

    for k in order:
        if k in counts:
            c = counts[k]
            total += c
            f1, f2, f5 = INSTR_FORMS.get(k, (k, k, k))
            name = plural_ru(c, f1, f2, f5)
            lines.append(f"{ICON.get(k, '🎵')} {c} {name}")

    lines.append("")
    lines.append(f"📊 ВСЕГО: {total} человек")

    not_found = len(voter_ids) - found
    if not_found > 0:
        lines.append(f"⚠️ Не найдено в базе: {not_found}")

    lines.append("")
    lines.append("ℹ️ Данные собраны из голосов опроса (✅)")

    return "\n".join(lines)


# ====== MAIN ======
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--list-topics", action="store_true", help="Показать темы и выйти")
    parser.add_argument("--topic-id", type=int, default=0, help="ID темы (topic_id)")
    parser.add_argument("--topic", type=str, default="", help="Найти тему по части названия")
    parser.add_argument("--poll", type=str, default="", help="Найти опрос по подстроке в вопросе")
    parser.add_argument("--smart-sort",action="store_true",help="Умно сортировать позитивные варианты (Смогу...) по времени/смыслу"
)

    args = parser.parse_args()

    print("🎻 Запуск парсера оркестра...")

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    print("✅ Подключено к Telegram")

    try:
        chat_entity = await client.get_entity(CHAT_ID)
        chat_peer = await client.get_input_entity(CHAT_ID)

        if args.list_topics:
            topics = await get_forum_topics(client, chat_entity, query=None, limit=200)
            print("\n📌 Темы форума:")
            for t in topics:
                print(f"ID={t.id} | {t.title} | top_message={t.top_message}")
            return

        # 1) Выбор темы
        topic_id = args.topic_id if args.topic_id else 0
        if not topic_id and args.topic.strip():
            topic_id = await choose_topic_id(client, chat_entity, args.topic.strip())

        if not topic_id:
            topic_id = DEFAULT_TOPIC_ID

        print(f"🧵 Тема: {topic_id}")

        # 2) Поиск опроса в теме
        polls = await find_polls_in_topic(client, CHAT_ID, topic_id, SEARCH_LIMIT)
        if not polls:
            await client.send_message("me", f"❌ В теме {topic_id} не найдено опросов.")
            print("❌ Опросов не найдено")
            return

        poll_msg = pick_poll(polls, args.poll.strip() if args.poll else None)
        if not poll_msg:
            await client.send_message("me", "❌ Не удалось выбрать опрос.")
            return

        poll_question = as_text(poll_msg.media.poll.question)
        print(f"✅ Выбран опрос id={poll_msg.id}: {poll_question[:80]}")

        # 3) Получаем проголосовавших за ✅
        try:
            voter_ids, option_text = await fetch_poll_voters_for_checkmark(
    client, chat_peer, poll_msg, smart_sort=args.smart_sort)
        except errors.PollVoteRequiredError:
            await client.send_message(
                "me",
                "❌ Telegram требует, чтобы этот аккаунт проголосовал в опросе, прежде чем смотреть голоса.\n"
                "Проголосуй (любой вариант) и запусти снова."
            )
            print("❌ POLL_VOTE_REQUIRED")
            return
        except RuntimeError as e:
            await client.send_message("me", f"❌ {e}")
            print(f"❌ {e}")
            return

        print(f"👥 Голосов за '{option_text}': {len(voter_ids)}")

        # 4) Грузим базу и делаем отчёт
        musicians = load_musicians(MUSICIANS_CSV)
        header = f"🎵 СТАТИСТИКА\n\nОпрос: {poll_question}\nОпция: {option_text}\n"
        report = build_report(voter_ids, musicians, header)

        await client.send_message("me", report)
        print("✅ Отчет отправлен в Избранное")

    finally:
        await client.disconnect()
        print("👋 Завершено")


if __name__ == "__main__":
    asyncio.run(main())

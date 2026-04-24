import asyncio
import argparse
import configparser
import csv
import os
import re
from typing import Dict, List, Optional, Set, Tuple

from telethon import TelegramClient, functions, errors
from telethon.tl import types
from telethon.tl.types import MessageMediaPoll


# =========================
# ЛОГГЕР
# =========================
def log(msg: str) -> None:
    print(msg, flush=True)


# =========================
# CONFIG
# =========================
def load_config(path: str = "config.ini") -> dict:
    cfg = configparser.ConfigParser()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Не найден {path}.\n"
            f"Создай его из config.example.ini и заполни свои значения."
        )

    cfg.read(path, encoding="utf-8")

    def get(section: str, key: str, default: Optional[str] = None) -> str:
        if section not in cfg or key not in cfg[section]:
            if default is None:
                raise KeyError(f"В конфиге нет [{section}] {key}")
            return default
        return cfg[section][key].strip()

    return {
        "API_ID": int(get("telegram", "api_id")),
        "API_HASH": get("telegram", "api_hash"),
        "SESSION_NAME": get("telegram", "session_name", "orchestra_parser"),
        "CHAT_ID": int(get("telegram", "chat_id")),
        "DEFAULT_TOPIC_ID": int(get("telegram", "default_topic_id", "0")),
        "MUSICIANS_CSV": get("files", "musicians_csv", "Музыканты.csv"),
        "SEARCH_LIMIT": int(get("search", "search_limit", "300")),
        "VOTES_PAGE_SIZE": int(get("search", "votes_page_size", "100")),
    }


# =========================
# РУССКИЕ ОКОНЧАНИЯ
# =========================
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
    "неизвестно": ("неизвестный", "неизвестных", "неизвестных"),
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
    "неизвестно": "❓",
}


# =========================
# ВСПОМОГАТЕЛЬНОЕ
# =========================
def as_text(x) -> str:
    if x is None:
        return ""
    return x.text if hasattr(x, "text") else str(x)


def normalize_instrument(raw: str) -> str:
    s = (raw or "").strip().lower().replace("ё", "е")

    if "скрип" in s:
        if "1" in s:
            return "первые скрипки"
        if "2" in s:
            return "вторые скрипки"
        return "первые скрипки"  # если база хранит просто "скрипки" — выбери, что удобнее

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


def load_musicians_csv(path: str) -> Tuple[Dict[int, str], int]:
    """
    CSV: delimiter ';', columns: user_id, Инструмент
    Возвращает: (user_id -> instrument, total_rows)
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")

    musicians: Dict[int, str] = {}
    total_rows = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            total_rows += 1
            uid = (row.get("user_id") or "").strip()
            instr = (row.get("Инструмент") or "").strip()
            if not uid or not instr:
                continue
            try:
                musicians[int(uid)] = instr
            except ValueError:
                continue

    return musicians, total_rows


# =========================
# TOPICS (совместимость Telethon)
# =========================
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


# =========================
# POLLS
# =========================
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


def is_yes_option_text(txt: str) -> bool:
    """
    Позитивные варианты:
      - репетиции: ✅, "приду" (но не "не приду")
      - концерты: "смогу ..." (но не "не смогу")
    """
    t = (txt or "").strip().casefold()
    t = " ".join(t.split())

    # явное "нет"
    if "не смогу" in t or (t.startswith("не") and "смогу" in t):
        return False
    if "не приду" in t or (t.startswith("не") and "приду" in t):
        return False

    if "✅" in t:
        return True
    if "приду" in t:
        return True
    if "смогу" in t:
        return True

    return False


def extract_time_minutes(txt: str) -> Optional[int]:
    """
    Ищем время в варианте ответа: "в 13:00", "к 10", "в 9", "к 8:30".
    """
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
    """
    Смысловой порядок вариантов без времени.
    """
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
) -> Tuple[Set[int], List[str]]:
    """
    Собирает ВСЕ "позитивные" варианты и объединяет проголосовавших.
    Возвращает (set(user_id), list(option_texts_sorted))
    """
    poll = poll_msg.media.poll

    # 1) Найти все позитивные варианты
    targets = []
    for ans in poll.answers:
        txt = as_text(ans.text)
        if is_yes_option_text(txt):
            targets.append(ans)

    if not targets:
        answers_debug = "\n".join([f"- {as_text(a.text)}" for a in poll.answers])
        raise RuntimeError("В опросе нет позитивных вариантов (✅/приду/смогу).\n" + answers_debug)

    # 2) Умная сортировка по флагу
    if smart_sort:
        index_map = {id(a): i for i, a in enumerate(targets)}

        def sort_key(a):
            txt = as_text(a.text)
            tmin = extract_time_minutes(txt)
            if tmin is not None:
                return (0, tmin, kw_rank(txt), index_map[id(a)])
            return (1, kw_rank(txt), 10_000, index_map[id(a)])

        targets = sorted(targets, key=sort_key)

    # 3) Опрос должен быть неанонимный
    if not getattr(poll, "public_voters", False):
        raise RuntimeError("Опрос анонимный — Telegram не отдаёт список проголосовавших.")

    # 4) Выгрузить голоса по каждой позитивной опции и объединить
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
                option=target.option,  # bytes
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


def build_report(poll_question: str, option_texts: List[str], voter_ids: Set[int], musicians: Dict[int, str]) -> str:
    counts: Dict[str, int] = {}
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

    lines: List[str] = []
    lines.append("🎵 СТАТИСТИКА")
    lines.append("")
    lines.append(f"Опрос: {poll_question}")
    lines.append(f"Учитываю варианты: {' / '.join(option_texts)}")
    lines.append("")

    total = 0
    for k in order:
        if k in counts:
            c = counts[k]
            total += c
            f1, f2, f5 = INSTR_FORMS.get(k, (k, k, k))
            name = plural_ru(c, f1, f2, f5)
            lines.append(f"{ICON.get(k, '🎵')} {c} {name}")

    lines.append("")

    paired = {"первые скрипки", "вторые скрипки", "альт", "виолончель"}

    pupitre = 0
    strings_pupitre = 0
    for instr, n in counts.items():
        if instr in paired:
            strings_pupitre += (n + 1) // 2
        else:
            pupitre += n

    lines.append(f"📊 Всего: {total} человек")
    lines.append(f"🎼 Нужно Пультов: {pupitre + strings_pupitre}")
    lines.append(f"❤️ Из них для струнников: {strings_pupitre}, 💔 для остальных: {pupitre}")

    not_found = len(voter_ids) - found
    if not_found > 0:
        lines.append(f"⚠️ Не найдено в базе: {not_found}")

    lines.append("")
    return "\n".join(lines)


# Функция выбора чата по ID
def entity_kind(ent) -> str:
    if isinstance(ent, types.User):
        return "user"
    if isinstance(ent, types.Chat):
        return "chat"
    if isinstance(ent, types.Channel):
        return "channel/supergroup"
    return type(ent).__name__


async def pick_chat_interactively(client: TelegramClient, limit: int = 30):
    """
    Показывает первые N диалогов и даёт выбрать.
    Возвращает entity выбранного диалога.
    """
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


# =========================
# MAIN
# =========================
async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="config.ini", help="Путь к config.ini")
    parser.add_argument("--list-topics", action="store_true", help="Показать темы и выйти")
    parser.add_argument("--topic-id", type=int, default=0, help="ID темы (как ты обычно используешь в reply_to)")
    parser.add_argument("--topic", type=str, default="", help="Найти тему по части названия")
    parser.add_argument("--poll", type=str, default="", help="Найти опрос по подстроке в вопросе")
    parser.add_argument("--smart-sort", action="store_true",
                        help="Умно сортировать варианты 'Смогу...' по времени/смыслу")
    parser.add_argument("--chat", type=str, default="",
                        help="Чат: id / @username / ссылка. Перезаписывает chat_id из config.ini")
    parser.add_argument("--pick-chat", action="store_true", help="Выбрать чат из списка диалогов (интерактивно)")
    parser.add_argument("--pick-chat-limit", type=int, default=30,
                        help="Сколько диалогов показать при --pick-chat (по умолчанию 30)")
    args = parser.parse_args()

    conf = load_config(args.config)

    API_ID = conf["API_ID"]
    API_HASH = conf["API_HASH"]
    SESSION_NAME = conf["SESSION_NAME"]
    CHAT_ID = conf["CHAT_ID"]
    DEFAULT_TOPIC_ID = conf["DEFAULT_TOPIC_ID"]
    MUSICIANS_CSV = conf["MUSICIANS_CSV"]
    SEARCH_LIMIT = conf["SEARCH_LIMIT"]
    VOTES_PAGE_SIZE = conf["VOTES_PAGE_SIZE"]

    log("🎻 Запуск парсера оркестра...")

    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()
    log("✅ Подключено к Telegram")

    try:
        # 0) Выбор чата: config -> --chat -> --pick-chat
        chat_ref = None

        if args.pick_chat:
            chat_entity = await pick_chat_interactively(client, limit=args.pick_chat_limit)
        else:
            # если указали --chat, используем его, иначе берём из конфига
            chat_ref = args.chat.strip() if args.chat.strip() else str(CHAT_ID)
            chat_entity = await client.get_entity(chat_ref)

        chat_peer = await client.get_input_entity(chat_entity)

        # для логов
        chat_title = getattr(chat_entity, "title",
                             getattr(chat_entity, "first_name", str(getattr(chat_entity, "id", ""))))
        log(f"📌 Чат: {chat_title} (id={getattr(chat_entity, 'id', '')})")

        # list topics
        if args.list_topics:
            topics = await get_forum_topics(client, chat_entity, query=None, limit=200)
            log("\n📌 Темы форума:")
            for t in topics:
                # покажем и id, и top_message на всякий случай
                log(f"ID={t.id} | top_message={t.top_message} | {t.title}")
            log("\n👋 Завершено")
            return

        # choose topic id
        topic_id = args.topic_id if args.topic_id else 0
        if not topic_id and args.topic.strip():
            topic_id = await choose_topic_id(client, chat_entity, args.topic.strip())
        if not topic_id:
            topic_id = DEFAULT_TOPIC_ID

        log(f"🔍 Ищу опрос в теме ID {topic_id}...")

        polls = await find_polls_in_topic(client, chat_entity, topic_id, SEARCH_LIMIT)

        # Авто-фоллбек: если тема не форумная/не та — пробуем искать опросы по всему чату
        if not polls and topic_id > 0:
            log("⚠️ В этой теме опросов нет. Пробую искать по всему чату (без topic_id)...")
            polls = await find_polls_in_topic(client, chat_entity, 0, SEARCH_LIMIT)

        if not polls:
            msg = f"❌ Не найдено опросов (topic_id={topic_id}, fallback=0 тоже пусто)."
            log(msg)
            await client.send_message("me", msg)
            return

        poll_msg = pick_poll(polls, args.poll.strip() if args.poll else None)
        if not poll_msg:
            msg = "❌ Не удалось выбрать опрос."
            log(msg)
            await client.send_message("me", msg)
            return

        poll_question = as_text(poll_msg.media.poll.question)
        log(f"✅ Найден опрос: {poll_question[:60]}...")

        # print answers like раньше
        poll = poll_msg.media.poll
        for i, ans in enumerate(poll.answers):
            log(f"Ответ {i}: {as_text(ans.text)}")

        if args.smart_sort:
            log("🧠 Smart sort: включён (сортирую 'Смогу...' по времени/смыслу)")

        # fetch voters
        try:
            voter_ids, option_texts = await fetch_poll_voters_yes_union(
                client=client,
                chat_peer=chat_peer,
                poll_msg=poll_msg,
                votes_page_size=VOTES_PAGE_SIZE,
                smart_sort=args.smart_sort,
            )
        except errors.PollVoteRequiredError:
            msg = (
                "❌ Telegram требует, чтобы этот аккаунт проголосовал в опросе, прежде чем смотреть голоса.\n"
                "Проголосуй (любой вариант) и запусти скрипт снова."
            )
            log(msg)
            await client.send_message("me", msg)
            return
        except RuntimeError as e:
            log(f"❌ {e}")
            await client.send_message("me", f"❌ {e}")
            return

        log(f"📊 На мероприятие идут: {len(voter_ids)} человек")

        # load musicians
        musicians, total_rows = load_musicians_csv(MUSICIANS_CSV)
        log(f"📁 Загружено {total_rows} записей")
        log(f"✅ В базе {len(musicians)} музыкантов с инструментами")

        # report
        report = build_report(poll_question, option_texts, voter_ids, musicians)

        await client.send_message("me", report)
        log("✅ Отчет отправлен!")
        log(report)
        log("👋 Завершено")

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

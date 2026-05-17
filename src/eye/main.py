"""main.py — точка входа приложения: парсинг флагов -> поиск опроса -> отчёт -> отправка."""

import asyncio
import argparse

from telethon import TelegramClient, errors

from core_log import log
from config_utils import load_config
from tg_chat import pick_chat_interactively, resolve_chat_entity
from tg_topics import get_forum_topics, choose_topic_id
from tg_polls import find_polls_in_topic, pick_poll, fetch_poll_voters_yes_union
from musicians_db import load_musicians_csv
from report_builder import build_report
from sender import send_report
from text_utils import as_text


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
    parser.add_argument("--send-to-chat", action="store_true",
                        help="Дополнительно отправить отчёт в чат ответом на сообщение опроса")
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
        if args.pick_chat:
            chat_entity = await pick_chat_interactively(client, limit=args.pick_chat_limit)
        else:
            chat_ref = args.chat.strip() if args.chat.strip() else CHAT_ID
            chat_entity = await resolve_chat_entity(client, chat_ref, scan_limit=max(args.pick_chat_limit, 200))

        chat_peer = await client.get_input_entity(chat_entity)

        chat_title = getattr(chat_entity, "title",
                             getattr(chat_entity, "first_name", str(getattr(chat_entity, "id", ""))))
        log(f"📌 Чат: {chat_title} (id={getattr(chat_entity, 'id', '')})")

        # list topics
        if args.list_topics:
            topics = await get_forum_topics(client, chat_entity, query=None, limit=200)
            log("\n📌 Темы форума:")
            for t in topics:
                log(f"ID={t.id} | top_message={t.top_message} | {t.title}")
            log("\n👋 Завершено")
            return

        # topic id
        topic_id = args.topic_id if args.topic_id else 0
        if not topic_id and args.topic.strip():
            topic_id = await choose_topic_id(client, chat_entity, args.topic.strip())
        if not topic_id:
            topic_id = DEFAULT_TOPIC_ID

        log(f"🔍 Ищу опрос (topic_id={topic_id})...")

        try:
            polls = await find_polls_in_topic(client, chat_entity, topic_id, SEARCH_LIMIT)
        except errors.rpcerrorlist.PeerIdInvalidError:
            log("⚠️ Этот чат не поддерживает темы/reply_to. Ищу опрос по всему чату (без topic_id)...")
            topic_id = 0
            polls = await find_polls_in_topic(client, chat_entity, 0, SEARCH_LIMIT)

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

        poll = poll_msg.media.poll
        for i, ans in enumerate(poll.answers):
            log(f"Ответ {i}: {as_text(ans.text)}")

        if args.smart_sort:
            log("🧠 Smart sort: включён (сортирую 'Смогу...' по времени/смыслу)")

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

        musicians, total_rows = load_musicians_csv(MUSICIANS_CSV)
        log(f"📁 Загружено {total_rows} записей")
        log(f"✅ В базе {len(musicians)} музыкантов с инструментами")

        report = build_report(poll_question, option_texts, voter_ids, musicians)

        await send_report(
            client,
            report=report,
            chat_entity=chat_entity,
            poll_msg_id=poll_msg.id,
            send_to_chat=args.send_to_chat,
            image_path="assets/TheEye.jpg",
            image_caption="I see you, little hobbit!",
        )

        log("✅ Отчет отправлен в Избранное!")
        if args.send_to_chat:
            log("✅ (Дополнительно) Отчет отправлен в чат (ответом на опрос)!")
        log(report)
        log("👋 Завершено")

    finally:
        await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

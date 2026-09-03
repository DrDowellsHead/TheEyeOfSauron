import asyncio
import configparser
import csv
import os
from typing import Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError

from src.eye.google_sheets import connect_to_google_sheet, sync_musicians_to_sheet


def load_config(path: str = "config.ini") -> dict:
    cfg = configparser.ConfigParser()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Не найден {path}. Создай его из config.example.ini и заполни значения."
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
    }


async def main():
    conf = load_config("config.ini")

    client = TelegramClient(conf["SESSION_NAME"], conf["API_ID"], conf["API_HASH"])

    print("🆔 Запуск сборщика участников...")
    await client.start()
    print("✅ Подключено к Telegram")

    worksheet = connect_to_google_sheet(
        credentials_file=settings["GOOGLE_CREDENTIALS"],
        spreadsheet_id=settings["GOOGLE_SPREADSHEET_ID"],
        worksheet_name=settings["GOOGLE_WORKSHEET_NAME"],
    )

    chat_id = conf["CHAT_ID"]

    try:
        chat = await client.get_entity(chat_id)
        title = getattr(chat, "title", str(chat_id))
        print(f"👥 Собираю участников чата: {title} ({chat_id})")

        rows = []
        count = 0

        async for user in client.iter_participants(chat):
            try:
                # user может быть deleted — тогда имена/юзернейм могут быть пустыми
                uid = int(user.id)
                first_name = (user.first_name or "").strip()
                last_name = (user.last_name or "").strip()
                username = (user.username or "").strip()

                rows.append([uid, first_name, last_name, username])
                count += 1

                if count % 200 == 0:
                    print(f"  ... {count} участников")

            except FloodWaitError as e:
                # если Telegram просит подождать
                print(f"⏳ FloodWait: жду {e.seconds} сек...")
                await asyncio.sleep(e.seconds)

        print(f"✅ Собрано: {count} участников")

        telegram_users = {}

        async for user in client.iter_participants(chat_entity):
            telegram_users[user.id] = {
                "first_name": user.first_name or "",
                "last_name": user.last_name or "",
                "username": user.username or "",
            }

            print("Импорт выполнен успешно!")

            sync_musicians_to_sheet(
                worksheet,
                telegram_users,
            )

    finally:
        await client.disconnect()
        print("👋 Завершено")


if __name__ == "__main__":
    asyncio.run(main())

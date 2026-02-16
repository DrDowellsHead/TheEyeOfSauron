import asyncio
import configparser
import csv
import os
from typing import Optional

from telethon import TelegramClient
from telethon.errors import FloodWaitError


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

    out_file = "Участники.csv"
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

        # Пишем CSV (UTF-8 with BOM, чтобы нормально открывалось в Excel)
        with open(out_file, "w", encoding="utf-8-sig", newline="") as f:
            w = csv.writer(f, delimiter=";")
            w.writerow(["user_id", "first_name", "last_name", "username"])
            w.writerows(rows)

        print(f"💾 Сохранено в файл: {out_file}")

    finally:
        await client.disconnect()
        print("👋 Завершено")


if __name__ == "__main__":
    asyncio.run(main())

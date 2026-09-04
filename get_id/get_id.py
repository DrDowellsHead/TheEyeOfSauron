"""Синхронизация участников Telegram с Google Sheets."""

import asyncio

from telethon import TelegramClient

from eye.config_utils import load_config
from eye.google_client import connect_to_google_sheet
from eye.google_sheets import sync_musicians_to_sheet


async def main() -> None:
    settings = load_config("config.ini")

    client = TelegramClient(
        settings["SESSION_NAME"],
        settings["API_ID"],
        settings["API_HASH"],
    )

    print("🆔 Запуск синхронизации участников...")

    await client.start()

    print("✅ Подключено к Telegram")

    try:
        chat = await client.get_entity(
            settings["CHAT_ID"]
        )

        title = getattr(
            chat,
            "title",
            str(settings["CHAT_ID"]),
        )

        print(f"👥 Собираю участников чата: {title}")

        telegram_users = {}

        async for user in client.iter_participants(chat):
            telegram_users[int(user.id)] = {
                "first_name": (
                        user.first_name or ""
                ).strip(),
                "last_name": (
                        user.last_name or ""
                ).strip(),
                "username": (
                        user.username or ""
                ).strip(),
            }

            if len(telegram_users) % 200 == 0:
                print(
                    f"  ... {len(telegram_users)} участников"
                )

        print(
            f"✅ Собрано: {len(telegram_users)} участников"
        )

        worksheet = connect_to_google_sheet(
            credentials_file=settings[
                "GOOGLE_CREDENTIALS"
            ],
            spreadsheet_id=settings[
                "GOOGLE_SPREADSHEET_ID"
            ],
            worksheet_name=settings[
                "GOOGLE_WORKSHEET_NAME"
            ],
        )

        sync_musicians_to_sheet(
            worksheet,
            telegram_users,
        )

        print("✅ Google Sheets синхронизирована")

    finally:
        await client.disconnect()
        print("👋 Завершено")


if __name__ == "__main__":
    asyncio.run(main())

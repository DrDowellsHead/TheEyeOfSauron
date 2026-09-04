"""Синхронизация участников Telegram с Google Sheets."""

import asyncio

from telethon import TelegramClient

from .config_utils import load_config
from .google_client import GoogleSheetsClient
from .google_sheets import sync_musicians_to_sheet


async def main() -> None:
    settings = load_config("config.ini")
    telegram = TelegramClient(
        settings["SESSION_NAME"],
        settings["API_ID"],
        settings["API_HASH"],
    )

    print("🆔 Запуск синхронизации участников...")
    await telegram.start()
    print("✅ Подключено к Telegram")

    try:
        chat = await telegram.get_entity(settings["CHAT_ID"])
        title = getattr(chat, "title", str(settings["CHAT_ID"]))
        print(f"👥 Собираю участников чата: {title}")

        telegram_users = {}
        async for user in telegram.iter_participants(chat):
            telegram_users[int(user.id)] = {
                "first_name": (user.first_name or "").strip(),
                "last_name": (user.last_name or "").strip(),
                "username": (user.username or "").strip(),
            }
            if len(telegram_users) % 200 == 0:
                print(f"  ... {len(telegram_users)} участников")

        print(f"✅ Собрано: {len(telegram_users)} участников")

        sheets = GoogleSheetsClient.from_service_account(
            credentials_file=settings["GOOGLE_CREDENTIALS"],
            spreadsheet_id=settings["GOOGLE_SPREADSHEET_ID"],
            worksheet_name=settings["GOOGLE_WORKSHEET_NAME"],
        )
        sync_musicians_to_sheet(sheets, telegram_users)
        print("✅ Google Sheets синхронизирована")
    finally:
        await telegram.disconnect()
        print("👋 Завершено")


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()

import asyncio
from telethon import TelegramClient
import csv

# === НАСТРОЙКИ ===
API_ID = 123456789  # Получить на my.telegram.org
API_HASH = '123456789'  # Получить на my.telegram.org
SESSION_NAME = 'collect_ids'

# ID вашего чата
CHAT_ID = -123456789  # Замените на ваш


async def main():
    """Собираем ID участников чата"""
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    await client.start()

    print("🔍 Собираю участников чата...")

    participants = []
    async for user in client.iter_participants(CHAT_ID):
        participants.append({
            'user_id': user.id,
            'first_name': user.first_name or '',
            'last_name': user.last_name or '',
            'username': user.username or ''
        })
        print(f"👤 {user.first_name} {user.last_name} (@{user.username}) - ID: {user.id}")

    # Сохраняем в CSV с правильным разделителем
    with open('Участники.csv', 'w', newline='', encoding='utf-8-sig') as f:
        # Используем delimiter=';' для Excel
        writer = csv.DictWriter(f,
                                fieldnames=['user_id', 'first_name', 'last_name', 'username'],
                                delimiter=';')
        writer.writeheader()
        writer.writerows(participants)

    print(f"\n✅ Собрано {len(participants)} участников")
    print("📁 Сохранено в файл: участники.csv")
    print("\n📝 Теперь откройте файл в Excel и добавьте колонку 'инструмент'")

    await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())

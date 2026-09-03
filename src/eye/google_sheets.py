"""Работа с Google Sheets."""

import os
from datetime import datetime

import gspread
from google.oauth2.service_account import Credentials

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


def connect_to_google_sheet(
        credentials_file: str,
        spreadsheet_id: str,
        worksheet_name: str,
):
    """
    Подключается к Google Sheets по ID таблицы
    и возвращает нужный лист.
    """

    if not os.path.exists(credentials_file):
        raise FileNotFoundError(
            f"Не найден файл ключа Google Service Account: "
            f"{credentials_file}"
        )

    credentials = Credentials.from_service_account_file(
        credentials_file,
        scopes=SCOPES,
    )

    client = gspread.authorize(credentials)

    spreadsheet = client.open_by_key(spreadsheet_id)

    worksheet = spreadsheet.worksheet(worksheet_name)

    return worksheet


def load_musicians_from_sheet(worksheet):
    """
    Загружает музыкантов из Google Sheets.

    Возвращает:
        dict[user_id] = instrument
        количество строк
    """

    records = worksheet.get_all_records()

    musicians = {}
    total_rows = len(records)

    for row in records:
        user_id = str(row.get("user_id", "")).strip()
        instrument = str(row.get("Инструмент", "")).strip()

        if not user_id:
            continue

        try:
            user_id = int(user_id)
        except ValueError:
            continue

        # Важно:
        # если инструмента нет — тоже сохраняем человека.
        # Потом get_id.py будет использовать это.
        musicians[user_id] = instrument

    return musicians, total_rows


def sync_musicians_to_sheet(worksheet, telegram_users):
    """
    Синхронизация Telegram пользователей с Google Sheets.

    Правила:
    - user_id является ключом;
    - имя, фамилия, username обновляются;
    - инструмент никогда не изменяется автоматически;
    - новые пользователи получают пустой инструмент;
    - обновление выполняется одним batch_update.
    """

    now = datetime.now().strftime("%Y-%m-%d %H:%M")

    records = worksheet.get_all_records()

    existing = {}

    for index, row in enumerate(records, start=2):
        uid = str(row.get("user_id", "")).strip()

        if uid:
            existing[uid] = {
                "row": index,
                "instrument": row.get("Инструмент", ""),
            }

    updates = []

    # Обновляем существующих
    for uid, user in telegram_users.items():

        uid = str(uid)

        if uid in existing:
            row = existing[uid]["row"]

            instrument = existing[uid]["instrument"]

            updates.append(
                {
                    "range": f"A{row}:F{row}",
                    "values": [[
                        uid,
                        user["first_name"],
                        user["last_name"],
                        user["username"],
                        instrument,
                        now,
                    ]]
                }
            )

    # Добавляем новых пользователей
    new_rows = []

    for uid, user in telegram_users.items():

        uid = str(uid)

        if uid not in existing:
            new_rows.append(
                [
                    uid,
                    user["first_name"],
                    user["last_name"],
                    user["username"],
                    "",
                    now,
                ]
            )

    # Массовое обновление
    if updates:
        worksheet.batch_update(updates)

    # Массовое добавление
    if new_rows:
        start_row = len(records) + 2

        end_row = start_row + len(new_rows) - 1

        worksheet.update(
            f"A{start_row}:F{end_row}",
            new_rows,
        )

"""Работа с Google Sheets."""

import os

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
    Синхронизация участников Telegram с Google Sheets.

    telegram_users:
        {
            user_id: {
                "first_name": "...",
                "last_name": "...",
                "username": "..."
            }
        }

    Инструменты существующих пользователей сохраняются.
    """

    existing = worksheet.get_all_records()

    rows_by_id = {}

    for index, row in enumerate(existing, start=2):
        uid = str(row.get("user_id", "")).strip()

        if uid:
            rows_by_id[uid] = {
                "row": index,
                "instrument": row.get("Инструмент", ""),
            }

    # если таблица пустая — создаём заголовки
    if not existing:
        worksheet.append_row(
            [
                "user_id",
                "first_name",
                "last_name",
                "username",
                "Инструмент",
            ]
        )

    updates = []

    for uid, user in telegram_users.items():

        uid_str = str(uid)

        if uid_str in rows_by_id:
            row = rows_by_id[uid_str]["row"]

            instrument = rows_by_id[uid_str]["instrument"]

            updates.append(
                {
                    "range": f"A{row}:E{row}",
                    "values": [[
                        uid,
                        user["first_name"],
                        user["last_name"],
                        user["username"],
                        instrument,
                    ]]
                }
            )

        else:
            worksheet.append_row(
                [
                    uid,
                    user["first_name"],
                    user["last_name"],
                    user["username"],
                    "",
                ]
            )

    if updates:
        worksheet.batch_update(updates)

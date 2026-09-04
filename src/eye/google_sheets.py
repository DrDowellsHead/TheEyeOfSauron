"""Чтение и обновление базы музыкантов в Google Sheets."""

from datetime import datetime
from typing import Dict, Mapping, Tuple


def load_musicians_from_sheet(
        worksheet,
) -> Tuple[Dict[int, str], int]:
    """
    Загружает музыкантов из Google Sheets.

    Возвращает:

        словарь user_id -> инструмент;
        общее количество строк.
    """

    records = worksheet.get_all_records()

    musicians = {}

    for row in records:
        user_id = str(
            row.get("user_id", "")
        ).strip()

        instrument = str(
            row.get("Инструмент", "")
        ).strip()

        if not user_id:
            continue

        try:
            user_id = int(user_id)
        except ValueError:
            continue

        musicians[user_id] = instrument

    return musicians, len(records)


def sync_musicians_to_sheet(
        worksheet,
        telegram_users: Mapping[int, Mapping[str, str]],
) -> None:
    """
    Синхронизирует пользователей Telegram с таблицей.

    Существующие пользователи:
    - получают актуальные имя, фамилию и username;
    - сохраняют назначенный вручную инструмент.

    Новые пользователи:
    - добавляются в конец таблицы;
    - получают пустое поле инструмента.
    """

    records = worksheet.get_all_records()

    updated_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M"
    )

    existing_users = {}

    for row_number, row in enumerate(
            records,
            start=2,
    ):
        user_id = str(
            row.get("user_id", "")
        ).strip()

        if not user_id:
            continue

        existing_users[user_id] = {
            "row_number": row_number,
            "instrument": row.get(
                "Инструмент",
                "",
            ),
        }

    existing_updates = []
    new_rows = []

    for user_id, user in telegram_users.items():
        user_id = str(user_id)

        telegram_data = [
            user_id,
            user.get("first_name", ""),
            user.get("last_name", ""),
            user.get("username", ""),
        ]

        existing_user = existing_users.get(user_id)

        if existing_user:
            row_number = existing_user["row_number"]
            instrument = existing_user["instrument"]

            existing_updates.append(
                {
                    "range": (
                        f"A{row_number}:F{row_number}"
                    ),
                    "values": [
                        [
                            *telegram_data,
                            instrument,
                            updated_at,
                        ]
                    ],
                }
            )
        else:
            new_rows.append(
                [
                    *telegram_data,
                    "",
                    updated_at,
                ]
            )

    if existing_updates:
        worksheet.batch_update(
            existing_updates
        )

    if new_rows:
        start_row = len(records) + 2
        end_row = start_row + len(new_rows) - 1

        # В gspread 6 сначала передаются значения,
        # затем диапазон.
        worksheet.update(
            new_rows,
            f"A{start_row}:F{end_row}",
        )

"""Преобразование данных музыкантов и синхронизация с Google Sheets."""

from datetime import datetime
from typing import Any, Mapping

from .google_client import GoogleSheetsApiError, GoogleSheetsClient

HEADERS = (
    "user_id",
    "first_name",
    "last_name",
    "username",
    "Инструмент",
    "Обновлено",
)
REQUIRED_HEADERS = frozenset(HEADERS)


def load_musicians_from_sheet(
        client: GoogleSheetsClient,
) -> tuple[dict[int, str], int]:
    """Возвращает инструменты музыкантов и количество записей в таблице."""

    records = _load_records(client)
    musicians = {}

    for record in records:
        user_id = str(record.get("user_id", "")).strip()
        instrument = str(record.get("Инструмент", "")).strip()
        if not user_id:
            continue

        try:
            musicians[int(user_id)] = instrument
        except ValueError:
            continue

    return musicians, len(records)


def sync_musicians_to_sheet(
        client: GoogleSheetsClient,
        telegram_users: Mapping[int, Mapping[str, str]],
) -> None:
    """Обновляет Telegram-данные, сохраняя назначенные инструменты."""

    records = _load_records(client, create_headers=True)
    updated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    existing_users = {
        str(record.get("user_id", "")).strip(): {
            "row_number": row_number,
            "instrument": record.get("Инструмент", ""),
        }
        for row_number, record in enumerate(records, start=2)
        if str(record.get("user_id", "")).strip()
    }

    updates = []
    new_rows = []

    for user_id, user in telegram_users.items():
        uid = str(user_id)
        telegram_data = [
            uid,
            user.get("first_name", ""),
            user.get("last_name", ""),
            user.get("username", ""),
        ]
        existing_user = existing_users.get(uid)

        if existing_user:
            row_number = existing_user["row_number"]
            row = [
                *telegram_data,
                existing_user["instrument"],
                updated_at,
            ]
            updates.append((f"A{row_number}:F{row_number}", [row]))
        else:
            new_rows.append([*telegram_data, "", updated_at])

    client.batch_update_rows(updates)
    client.append_rows(new_rows)


def _load_records(
        client: GoogleSheetsClient,
        *,
        create_headers: bool = False,
) -> list[dict[str, Any]]:
    rows = client.get_rows("A:F")
    if not rows:
        if create_headers:
            client.batch_update_rows([("A1:F1", [list(HEADERS)])])
        return []

    headers = [str(value).strip() for value in rows[0]]
    missing_headers = REQUIRED_HEADERS.difference(headers)
    if missing_headers:
        missing = ", ".join(sorted(missing_headers))
        raise GoogleSheetsApiError(
            f"В первой строке таблицы отсутствуют столбцы: {missing}"
        )

    records = []
    for row in rows[1:]:
        values = list(row[:len(headers)])
        values.extend([""] * (len(headers) - len(values)))
        records.append(dict(zip(headers, values)))
    return records

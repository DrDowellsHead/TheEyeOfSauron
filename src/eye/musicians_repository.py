"""
Работа с музыкантами.
Источник данных — Google Sheets.
"""

from typing import Dict, Tuple

from .google_sheets import (
    connect_to_google_sheet,
    load_musicians_from_sheet,
)


def load_musicians(
        credentials_file: str,
        spreadsheet_id: str,
        worksheet_name: str,
) -> Tuple[Dict[int, str], int]:
    """
    Загружает список музыкантов.

    Возвращает:
        {
            user_id: instrument
        }

    и количество записей.
    """

    worksheet = connect_to_google_sheet(
        credentials_file=credentials_file,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
    )

    return load_musicians_from_sheet(worksheet)

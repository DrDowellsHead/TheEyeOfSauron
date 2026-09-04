"""Репозиторий базы музыкантов."""

from typing import Dict, Tuple

from .google_client import connect_to_google_sheet
from .google_sheets import load_musicians_from_sheet


def load_musicians(
        credentials_file: str,
        spreadsheet_id: str,
        worksheet_name: str,
) -> Tuple[Dict[int, str], int]:
    """Загружает музыкантов из Google Sheets."""

    worksheet = connect_to_google_sheet(
        credentials_file=credentials_file,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
    )

    return load_musicians_from_sheet(
        worksheet
    )

"""Загрузка базы музыкантов."""

from .google_client import GoogleSheetsClient
from .google_sheets import load_musicians_from_sheet


def load_musicians(
        credentials_file: str,
        spreadsheet_id: str,
        worksheet_name: str,
) -> tuple[dict[int, str], int]:
    """Загружает музыкантов из настроенной Google-таблицы."""

    client = GoogleSheetsClient.from_service_account(
        credentials_file=credentials_file,
        spreadsheet_id=spreadsheet_id,
        worksheet_name=worksheet_name,
    )
    return load_musicians_from_sheet(client)

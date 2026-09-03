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

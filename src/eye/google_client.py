"""Подключение к Google Sheets."""

import os

import gspread

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
]


def connect_to_google_sheet(
        credentials_file: str,
        spreadsheet_id: str,
        worksheet_name: str,
):
    """Подключается к указанному листу Google Sheets."""

    if not os.path.isfile(credentials_file):
        raise FileNotFoundError(
            "Не найден файл ключа Google Service Account: "
            f"{credentials_file}"
        )

    client = gspread.service_account(
        filename=credentials_file,
        scopes=SCOPES,
    )

    spreadsheet = client.open_by_key(spreadsheet_id)

    return spreadsheet.worksheet(worksheet_name)

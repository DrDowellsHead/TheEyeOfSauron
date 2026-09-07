"""Ручная проверка чтения базы музыкантов из Google Sheets."""

from eye.config_utils import load_config
from eye.google_client import GoogleSheetsClient
from eye.google_sheets import load_musicians_from_sheet


def main():
    """Подключается к таблице и читает текущую базу без изменений."""

    settings = load_config()
    client = GoogleSheetsClient.from_service_account(
        credentials_file=settings["GOOGLE_CREDENTIALS"],
        spreadsheet_id=settings["GOOGLE_SPREADSHEET_ID"],
        worksheet_name=settings["GOOGLE_WORKSHEET_NAME"],
    )
    musicians, total_rows = load_musicians_from_sheet(client)

    print("Успешное подключение!")
    print(f"Таблица: {settings['GOOGLE_SPREADSHEET_ID']}")
    print(f"Лист: {settings['GOOGLE_WORKSHEET_NAME']}")
    print(f"Количество строк: {total_rows}")
    print(f"Музыкантов с корректным user_id: {len(musicians)}")


if __name__ == "__main__":
    main()

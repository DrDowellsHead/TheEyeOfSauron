import configparser

from eye.google_sheets import connect_to_google_sheet


def load_google_sheets_config():
    """
    Загружает настройки Google Sheets из config.ini.
    """

    config = configparser.ConfigParser()
    config.read("config.ini", encoding="utf-8")

    return {
        "credentials_file": config["google_sheets"]["credentials_file"],
        "spreadsheet_id": config["google_sheets"]["spreadsheet_id"],
        "worksheet_name": config["google_sheets"]["worksheet_name"],
    }


def main():
    """
    Проверяет подключение к Google Sheets.
    """

    settings = load_google_sheets_config()

    worksheet = connect_to_google_sheet(
        credentials_file=settings["credentials_file"],
        spreadsheet_id=settings["spreadsheet_id"],
        worksheet_name=settings["worksheet_name"],
    )

    print("Успешное подключение!")

    print(f"Таблица: {settings['spreadsheet_id']}")
    print(f"Лист: {settings['worksheet_name']}")

    records = worksheet.get_all_records()

    print(f"Количество записей: {len(records)}")


if __name__ == "__main__":
    main()

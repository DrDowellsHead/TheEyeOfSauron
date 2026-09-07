import unittest

from eye.google_client import GoogleSheetsApiError
from eye.google_sheets import load_musicians_from_sheet, sync_musicians_to_sheet


HEADERS = [
    "user_id",
    "first_name",
    "last_name",
    "username",
    "Инструмент",
    "Обновлено",
]


class FakeClient:
    def __init__(self, rows):
        self.rows = rows
        self.updates = []
        self.appended = []

    def get_rows(self, _range):
        return self.rows

    def batch_update_rows(self, updates):
        self.updates.extend(updates)

    def append_rows(self, rows):
        self.appended.extend(rows)


class GoogleSheetsTests(unittest.TestCase):
    def test_load_musicians_keeps_saxophone_value(self):
        client = FakeClient(
            [HEADERS, [571345749, "Елена", "", "Elena", "саксофон", ""]]
        )

        musicians, total = load_musicians_from_sheet(client)

        self.assertEqual(musicians, {571345749: "саксофон"})
        self.assertEqual(total, 1)

    def test_sync_preserves_instrument(self):
        client = FakeClient([HEADERS, [123, "Старое", "", "", "альт", ""]])

        sync_musicians_to_sheet(
            client,
            {
                123: {"first_name": "Иван", "last_name": "", "username": "ivan"},
                456: {"first_name": "Анна", "last_name": "", "username": "anna"},
            },
        )

        self.assertEqual(client.updates[0][1][0][4], "альт")
        self.assertEqual(client.appended[0][:4], ["456", "Анна", "", "anna"])

    def test_missing_headers_are_reported(self):
        client = FakeClient([["user_id"], [123]])

        with self.assertRaises(GoogleSheetsApiError):
            load_musicians_from_sheet(client)

    def test_sync_creates_headers_for_empty_sheet(self):
        client = FakeClient([])

        sync_musicians_to_sheet(
            client,
            {123: {"first_name": "Иван", "last_name": "", "username": ""}},
        )

        self.assertEqual(client.updates[0][0], "A1:F1")
        self.assertEqual(client.updates[0][1][0], HEADERS)
        self.assertEqual(client.appended[0][0], "123")


if __name__ == "__main__":
    unittest.main()

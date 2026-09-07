import unittest

from eye.google_client import GoogleSheetsClient


class FakeResponse:
    def __init__(self, payload):
        self.ok = True
        self._payload = payload

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        return FakeResponse(self.payload)


class GoogleSheetsClientTests(unittest.TestCase):
    def test_get_rows_uses_encoded_sheet_range(self):
        session = FakeSession({"values": [["user_id"], [123]]})
        client = GoogleSheetsClient("sheet-id", "Musicians", session)

        rows = client.get_rows()

        self.assertEqual(rows, [["user_id"], [123]])
        self.assertIn("%27Musicians%27%21A%3AF", session.calls[0][1])

    def test_append_rows_uses_insert_rows(self):
        session = FakeSession({"updates": {"updatedRows": 1}})
        client = GoogleSheetsClient("sheet-id", "Musicians", session)

        client.append_rows([[123, "Иван", "", "", "", "2026-09-04"]])

        _, url, kwargs = session.calls[0]
        self.assertTrue(url.endswith(":append"))
        self.assertEqual(kwargs["params"]["insertDataOption"], "INSERT_ROWS")


if __name__ == "__main__":
    unittest.main()

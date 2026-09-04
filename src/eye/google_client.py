"""Небольшой REST-клиент Google Sheets API."""

from typing import Any, Iterable, Mapping
from urllib.parse import quote

import requests
from google.auth.exceptions import GoogleAuthError

from .google_auth import create_authorized_session

SHEETS_API_URL = "https://sheets.googleapis.com/v4/spreadsheets"
HTTP_TIMEOUT_SECONDS = 30


class GoogleSheetsApiError(RuntimeError):
    """Ошибка запроса к Google Sheets API."""


class GoogleSheetsClient:
    """Читает и обновляет один лист Google Sheets через REST API."""

    def __init__(self, spreadsheet_id: str, worksheet_name: str, session) -> None:
        self.spreadsheet_id = spreadsheet_id
        self.worksheet_name = worksheet_name
        self._session = session

    @classmethod
    def from_service_account(
            cls,
            credentials_file: str,
            spreadsheet_id: str,
            worksheet_name: str,
    ) -> "GoogleSheetsClient":
        session = create_authorized_session(credentials_file)
        return cls(spreadsheet_id, worksheet_name, session)

    def get_rows(self, a1_range: str = "A:F") -> list[list[Any]]:
        """Возвращает значения указанного диапазона построчно."""

        response = self._request(
            "GET",
            self._values_url(a1_range),
            params={
                "majorDimension": "ROWS",
                "valueRenderOption": "UNFORMATTED_VALUE",
            },
        )
        return response.get("values", [])

    def batch_update_rows(
            self,
            updates: Iterable[tuple[str, list[list[Any]]]],
    ) -> None:
        """Обновляет несколько диапазонов одним запросом."""

        data = [
            {
                "range": self._qualified_range(a1_range),
                "values": values,
            }
            for a1_range, values in updates
        ]
        if not data:
            return

        self._request(
            "POST",
            f"{self._spreadsheet_url()}/values:batchUpdate",
            json={"valueInputOption": "RAW", "data": data},
        )

    def append_rows(self, rows: list[list[Any]]) -> None:
        """Добавляет новые строки после последней заполненной строки."""

        if not rows:
            return

        self._request(
            "POST",
            f"{self._values_url('A:F')}:append",
            params={
                "valueInputOption": "RAW",
                "insertDataOption": "INSERT_ROWS",
            },
            json={"values": rows},
        )

    def _spreadsheet_url(self) -> str:
        spreadsheet_id = quote(self.spreadsheet_id, safe="")
        return f"{SHEETS_API_URL}/{spreadsheet_id}"

    def _values_url(self, a1_range: str) -> str:
        encoded_range = quote(self._qualified_range(a1_range), safe="")
        return f"{self._spreadsheet_url()}/values/{encoded_range}"

    def _qualified_range(self, a1_range: str) -> str:
        worksheet_name = self.worksheet_name.replace("'", "''")
        return f"'{worksheet_name}'!{a1_range}"

    def _request(
            self,
            method: str,
            url: str,
            **kwargs: Any,
    ) -> Mapping[str, Any]:
        try:
            response = self._session.request(
                method,
                url,
                timeout=HTTP_TIMEOUT_SECONDS,
                **kwargs,
            )
        except (requests.RequestException, GoogleAuthError) as error:
            raise GoogleSheetsApiError(
                f"Не удалось подключиться к Google Sheets: {error}"
            ) from error

        if not response.ok:
            raise GoogleSheetsApiError(self._get_error_message(response))

        try:
            payload = response.json()
        except ValueError as error:
            raise GoogleSheetsApiError(
                "Google Sheets API вернул некорректный JSON"
            ) from error

        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _get_error_message(response) -> str:
        try:
            payload = response.json()
        except ValueError:
            message = response.text.strip() or f"HTTP {response.status_code}"
            return f"Google Sheets API: {message}"

        error = payload.get("error", {}) if isinstance(payload, dict) else {}
        if isinstance(error, dict):
            message = error.get("message") or error.get("status") or error
        else:
            message = error or payload
        return f"Google Sheets API: {message}"

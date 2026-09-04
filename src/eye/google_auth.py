"""Авторизация Google Service Account."""

import os

from google.auth.transport.requests import AuthorizedSession
from google.oauth2.service_account import Credentials

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def create_authorized_session(credentials_file: str) -> AuthorizedSession:
    """Создаёт HTTP-сессию с автоматическим обновлением OAuth-токена."""

    if not os.path.isfile(credentials_file):
        raise FileNotFoundError(
            "Не найден файл ключа Google Service Account: "
            f"{credentials_file}"
        )

    credentials = Credentials.from_service_account_file(
        credentials_file,
        scopes=SCOPES,
    )

    return AuthorizedSession(credentials)

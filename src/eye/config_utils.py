"""config_utils.py — загрузка config.ini и базовая валидация настроек."""

import configparser
import os
from typing import Optional


def load_config(path: str = "config.ini") -> dict:
    cfg = configparser.ConfigParser()
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Не найден {path}.\n"
            f"Создай его из config.example.ini и заполни свои значения."
        )

    cfg.read(path, encoding="utf-8")

    def get(section: str, key: str, default: Optional[str] = None) -> str:
        if section not in cfg or key not in cfg[section]:
            if default is None:
                raise KeyError(f"В конфиге нет [{section}] {key}")
            return default
        return cfg[section][key].strip()

    return {
        "API_ID": int(get("telegram", "api_id")),
        "API_HASH": get("telegram", "api_hash"),
        "SESSION_NAME": get("telegram", "session_name", "orchestra_parser"),
        "CHAT_ID": int(get("telegram", "chat_id")),
        "DEFAULT_TOPIC_ID": int(get("telegram", "default_topic_id", "0")),
        "MUSICIANS_CSV": get("files", "musicians_csv", "Музыканты.csv"),
        "SEARCH_LIMIT": int(get("search", "search_limit", "300")),
        "VOTES_PAGE_SIZE": int(get("search", "votes_page_size", "100")),
    }

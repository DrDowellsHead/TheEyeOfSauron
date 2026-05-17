"""musicians_db.py — чтение Музыканты.csv (user_id -> instrument)."""

import csv
import os
from typing import Dict, Tuple


def load_musicians_csv(path: str) -> Tuple[Dict[int, str], int]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл не найден: {path}")

    musicians: Dict[int, str] = {}
    total_rows = 0

    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            total_rows += 1
            uid = (row.get("user_id") or "").strip()
            instr = (row.get("Инструмент") or "").strip()
            if not uid or not instr:
                continue
            try:
                musicians[int(uid)] = instr
            except ValueError:
                continue

    return musicians, total_rows

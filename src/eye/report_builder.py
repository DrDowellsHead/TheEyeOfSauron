"""report_builder.py — сборка финального текста отчёта + подсчёт пультов."""

from typing import Dict, List, Set

from .instruments import ICON, INSTR_FORMS, INSTRUMENT_ORDER, normalize_instrument
from .text_utils import plural_ru


def build_report(
    poll_question: str,
    option_texts: List[str],
    voter_ids: Set[int],
    musicians: Dict[int, str],
) -> str:
    counts: Dict[str, int] = {}
    known_voter_ids = voter_ids.intersection(musicians)
    missing_voter_ids = sorted(voter_ids.difference(musicians))

    for uid in known_voter_ids:
        key = normalize_instrument(musicians[uid])
        counts[key] = counts.get(key, 0) + 1

    # Не теряем нестандартные значения из таблицы: стандартные группы идут в
    # заданном порядке, остальные — после них по алфавиту.
    extra_instruments = sorted(set(counts).difference(INSTRUMENT_ORDER))
    order = (*INSTRUMENT_ORDER, *extra_instruments)

    lines: List[str] = []
    lines.append("††† The Eye Of Sauron †††")
    lines.append("")
    lines.append("🎵 СТАТИСТИКА")
    lines.append("")
    lines.append(f"Опрос: {poll_question}")
    lines.append(f"Учитываю варианты: {' / '.join(option_texts)}")
    lines.append("")

    for k in order:
        if k in counts:
            c = counts[k]
            f1, f2, f5 = INSTR_FORMS.get(k, (k, k, k))
            name = plural_ru(c, f1, f2, f5)
            lines.append(f"{ICON.get(k, '🎵')} {c} {name}")

    lines.append("")

    paired = {"первые скрипки", "вторые скрипки", "альт", "виолончель"}

    pupitre = 0
    strings_pupitre = 0
    for instr, n in counts.items():
        if instr in paired:
            strings_pupitre += (n + 1) // 2
        else:
            pupitre += n

    voters_total = len(voter_ids)
    known_total = len(known_voter_ids)
    missing_total = len(missing_voter_ids)
    voters_word = plural_ru(voters_total, "человек", "человека", "человек")
    known_word = plural_ru(known_total, "человек", "человека", "человек")
    missing_word = plural_ru(missing_total, "человек", "человека", "человек")

    lines.append(f"📊 Всего проголосовало: {voters_total} {voters_word}")
    lines.append(f"✅ Учтено по инструментам: {known_total} {known_word}")
    lines.append(f"⚠️ Не найдено в базе: {missing_total} {missing_word}")
    if missing_voter_ids:
        missing_ids = ", ".join(str(uid) for uid in missing_voter_ids)
        lines.append(f"🔎 Telegram ID вне базы: {missing_ids}")

    lines.append(f"🎼 Нужно Пультов: {pupitre + strings_pupitre}")
    lines.append(f"❤️ Из них для струнников: {strings_pupitre}, 💔 для остальных: {pupitre}")

    lines.append("")
    lines.append("By: https://github.com/DrDowellsHead")
    lines.append("")
    return "\n".join(lines)

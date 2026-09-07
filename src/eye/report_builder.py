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
    found = 0

    for uid in voter_ids:
        if uid not in musicians:
            continue
        found += 1
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

    total = sum(counts.values())
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

    people_word = plural_ru(total, "человек", "человека", "человек")
    lines.append(f"📊 Всего: {total} {people_word}")
    lines.append(f"🎼 Нужно Пультов: {pupitre + strings_pupitre}")
    lines.append(f"❤️ Из них для струнников: {strings_pupitre}, 💔 для остальных: {pupitre}")

    not_found = len(voter_ids) - found
    if not_found > 0:
        lines.append(f"⚠️ Не найдено в базе: {not_found}")

    lines.append("")
    lines.append("By: https://github.com/DrDowellsHead")
    lines.append("")
    return "\n".join(lines)

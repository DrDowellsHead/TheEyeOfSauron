"""text_utils.py — мелкие утилиты: as_text(), plural_ru()."""


def as_text(x) -> str:
    if x is None:
        return ""
    return x.text if hasattr(x, "text") else str(x)


def plural_ru(n: int, form1: str, form2: str, form5: str) -> str:
    n = abs(int(n))
    n10 = n % 10
    n100 = n % 100
    if 11 <= n100 <= 14:
        return form5
    if n10 == 1:
        return form1
    if 2 <= n10 <= 4:
        return form2
    return form5

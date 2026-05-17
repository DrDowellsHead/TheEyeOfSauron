"""Minimal logging utilities.

We deliberately keep logging simple: stdout with flush=True.
"""


def log(msg: str) -> None:
    print(msg, flush=True)

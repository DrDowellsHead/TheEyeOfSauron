"""Command-line entry point for python -m eye."""

import asyncio

from .main import main


def run() -> None:
    asyncio.run(main())


if __name__ == "__main__":
    run()

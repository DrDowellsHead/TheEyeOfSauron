#!/data/data/com.termux/files/usr/bin/bash

set -eu

PROJECT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
cd "$PROJECT_DIR"

if ! command -v python >/dev/null 2>&1; then
    echo "Python не найден. Сначала выполните:"
    echo "  pkg update && pkg upgrade"
    echo "  pkg install python python-pip python-ensurepip-wheels"
    exit 1
fi

if [ -d .venv ] && [ ! -x .venv/bin/python ]; then
    echo "Найдена несовместимая .venv."
    echo "Вероятно, она была скопирована с Windows."
    echo
    echo "Удалите только папку .venv внутри проекта:"
    echo "  rm -rf \"$PROJECT_DIR/.venv\""
    echo
    echo "После этого снова запустите установщик."
    exit 1
fi

python -m venv .venv

# Termux управляет pip через пакетный менеджер и запрещает его самообновление.
.venv/bin/python -m pip install \
    --upgrade \
    setuptools

.venv/bin/python -m pip install \
    --no-build-isolation \
    -e .

.venv/bin/python -c "import eye, google.auth, requests, telethon"

echo
echo "✅ Установка завершена."
echo
echo "Для запуска выполните:"
echo "  cd \"$PROJECT_DIR\""
echo "  source .venv/bin/activate"
echo "  python -m eye --help"

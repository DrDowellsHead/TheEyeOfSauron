# The Eye of Sauron (Telegram Orchestra Parser)

Скрипт находит опрос (poll) в чате/теме Telegram-форума, собирает список проголосовавших за “позитивные” варианты и отправляет статистику по инструментам.

Поддерживает:
- Репетиции: варианты с `✅` и/или текстом `приду` / `буду` (но не `не приду`)
- Концерты: все варианты `Смогу ...` (например: “Смогу к 10”, “Смогу в 13:00”, “Смогу к концерту”), объединяет участников без дублей
- Умную сортировку вариантов “Смогу ...” по времени/смыслу (`--smart-sort`)
- Выбор чата (`--chat` / `--pick-chat`)
- Отправку отчёта в Избранное всегда, и дополнительно в чат ответом на сообщение опроса (`--send-to-chat`)
- Картинку в начале отчёта (если файл существует)

> Важно: Telegram отдаёт список проголосовавших только для **неанонимных** опросов (public voters).  
> Если опрос анонимный — получить `user_id` голосовавших нельзя.

---

## Структура проекта

```
TheEyeOfSauron/
  assets/
    TheEye.jpg
  data/
    Музыканты.csv
  get_id/
    get_id.py
  src/
    eye/
      __main__.py
      main.py
      core_log.py
      config_utils.py
      text_utils.py
      instruments.py
      musicians_db.py
      report_builder.py
      sender.py
      tg_chat.py
      tg_topics.py
      tg_polls.py
  README.md
  requirements.txt
  config.example.ini
  config.ini          (локально, НЕ коммитить)
  pyproject.toml      (если хочешь запуск/установку как пакет)
  .gitignore
```

---

## Установка (Windows / Linux)

```bash
python -m venv .venv

# Windows:
.venv\Scripts\activate

# Linux/macOS:
source .venv/bin/activate

python -m pip install -U pip
python -m pip install -r requirements.txt
```

---

## Установка (Android / Termux)

```bash
pkg update && pkg upgrade -y
pkg install -y python
python -m pip install -r requirements.txt
```

> В Termux не обновляй pip через `pip install -U pip` — обновляй пакеты через `pkg update && pkg upgrade`.

---

## Конфиг

Секреты хранятся в `config.ini` (локально, не в git). В репозитории лежит только `config.example.ini`.

### config.example.ini (пример)

```ini
[telegram]
api_id = 123456
api_hash = put_hash_here
session_name = orchestra_parser
chat_id = -1002291481872
default_topic_id = 4

[files]
# ВАЖНО: укажи путь к CSV в папке data
musicians_csv = data/Музыканты.csv

[search]
search_limit = 300
votes_page_size = 100
```

---

## Данные

- Картинка: `assets/TheEye.jpg`
- База музыкантов: `data/Музыканты.csv` (разделитель `;`, колонки `user_id` и `Инструмент`)

---

## Запуск

Есть два варианта.

### Вариант A (без установки пакета): через PYTHONPATH
Запуск из корня проекта:

```bash
PYTHONPATH=src python -m eye --help
PYTHONPATH=src python -m eye
```

Примеры:

```bash
PYTHONPATH=src python -m eye --pick-chat
PYTHONPATH=src python -m eye --pick-chat --send-to-chat
PYTHONPATH=src python -m eye --poll "Бал в Атриуме" --smart-sort
```

### Вариант B (как пакет): через `pip install -e .`
Требует наличия `pyproject.toml` в корне.

Один раз:

```bash
python -m pip install -e .
```

Дальше:

```bash
python -m eye --help
python -m eye --pick-chat --send-to-chat
```

> После `git pull` переустанавливать не нужно, пока ты не меняешь зависимости/метаданные пакета.

---

## Все флаги командной строки

### `--config <path>`
Путь к `config.ini` (по умолчанию `config.ini` в текущей директории запуска).

### `--list-topics`
Печатает список тем форума в выбранном чате и выходит.

### `--topic-id <id>`
Явно задаёт `topic_id` (reply_to), в которой искать опрос.

### `--topic "<часть названия>"`
Ищет тему по части названия (если совпадений несколько — предложит выбрать).

### `--poll "<подстрока в вопросе>"`
Выбирает опрос по подстроке в тексте вопроса. Если не найдено — берётся самый последний опрос.

### `--smart-sort`
Сортирует позитивные варианты “Смогу ...” по времени/смыслу:
- сначала варианты с распознанным временем (`к 10`, `в 13:00`...) по возрастанию
- затем без времени: чек/саундчек → репетиция → концерт → прочее

### `--chat <ref>`
Выбрать чат вручную: id / @username / ссылка.

### `--pick-chat`
Интерактивный выбор чата из списка диалогов.

### `--pick-chat-limit <N>`
Сколько диалогов показать при `--pick-chat` (по умолчанию 30).

### `--send-to-chat`
Если включён — отчёт уйдёт:
1) в Избранное (всегда)
2) дополнительно в выбранный чат ответом на сообщение опроса (`reply_to=poll_msg.id`)

---

## get_id (сбор участников)

Запуск:
```bash
python get_id/get_id.py
```

---

## Типовые проблемы

### Termux: `sqlite3.OperationalError: database is locked`
Ты остановил процесс через `Ctrl+Z` (он не завершился и держит `.session`).
Решение:
- `pgrep -a python`
- `kill <pid>` или `kill -9 <pid>`
- удалить хвосты: `rm -f *.session-journal *.session-wal *.session-shm`

---

## Безопасность

Никогда не коммить:
- `config.ini`
- `*.session*`

Если случайно запушил `.session`:
1) Telegram → Settings → Devices → Terminate sessions
2) очистить историю git (git-filter-repo) или удалить репозиторий

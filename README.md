# The Eye of Sauron (Telegram Orchestra Parser)

Скрипт находит опрос (poll) в чате/теме Telegram-форума, собирает **список проголосовавших** за “позитивные” варианты и отправляет статистику по инструментам.

Поддерживает:
- Репетиции: варианты с `✅` и/или текстом `приду` / `буду` (но не `не приду`)
- Концерты: все варианты `Смогу ...` (например: “Смогу к 10”, “Смогу в 13:00”, “Смогу к концерту”), объединяет участников без дублей
- Умную сортировку вариантов “Смогу ...” по времени/смыслу (`--smart-sort`)
- Выбор чата (`--chat` / `--pick-chat`)
- Опциональную отправку отчёта в чат ответом на сообщение опроса (`--send-to-chat`)
- Картинку в начале (если рядом лежит `TheEye.jpg`)

> Важно: Telegram отдаёт список проголосовавших только для **неанонимных** опросов (public voters).  
> Если опрос анонимный — получить `user_id` голосовавших нельзя.

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

## Конфиг (секреты не хранятся в коде)

Создай файл `config.ini` (он **не должен коммититься**). В репозитории держи только `config.example.ini`.

### config.example.ini (пример)

```ini
[telegram]
api_id = 123456
api_hash = put_hash_here
session_name = orchestra_parser

chat_id = -1002291481872
default_topic_id = 4

[files]
musicians_csv = Музыканты.csv

[search]
search_limit = 300
votes_page_size = 100
```

---

## .gitignore (обязательно)

Добавь:

```gitignore
config.ini

*.session
*.session-journal
*.session-wal
*.session-shm
```

> `.session` — ключ авторизации Telethon. Никогда не пушь его в GitHub.

---

## Файл базы музыкантов (CSV)

Файл `Музыканты.csv` должен быть с разделителем `;` и минимум с колонками:
- `user_id`
- `Инструмент`

Пример:

```csv
user_id;Инструмент
123456789;Кларнет
987654321;Скрипка 1
```

---

## Картинка в начале отчёта

Если рядом с `main.py` лежит файл `TheEye.jpg`, скрипт отправит её перед отчётом (в Избранное, и в чат при `--send-to-chat`).

---

## Использование (быстрый старт)

### Только в Избранное (как базовый режим)
Берёт чат из `config.ini` и ищет опрос в теме `default_topic_id`.

```bash
python main.py
```

---

## Все флаги командной строки (main.py)

### `--config <path>`
Путь к конфигу (по умолчанию `config.ini`).
```bash
python main.py --config config.ini
```

### `--list-topics`
Печатает список тем форума (topic) в выбранном чате и выходит.
```bash
python main.py --list-topics
```

### `--topic-id <id>`
Явно задаёт ID темы (topic), в которой искать опрос.
```bash
python main.py --topic-id 4
```

### `--topic "<часть названия>"`
Ищет тему по части названия (если совпадений несколько — предложит выбрать).
```bash
python main.py --topic "концерт"
```

### `--poll "<подстрока в вопросе>"`
Выбирает опрос по подстроке в тексте вопроса.
- если совпадений несколько — предложит выбрать
- если не найдено — берётся самый последний опрос в теме/чате
```bash
python main.py --poll "Бал в Атриуме"
```

### `--smart-sort`
Умно сортирует позитивные варианты “Смогу ...”:
- сначала варианты с распознанным временем (`к 10`, `в 13:00`...) по возрастанию
- затем без времени по смыслу: чек/саундчек → репетиция → концерт → прочее
```bash
python main.py --smart-sort
```

---

## Выбор чата

### `--chat <ref>`
Выбрать чат вручную: `id` / `@username` / ссылка (если Telethon может распарсить).
```bash
python main.py --chat -1002291481872
python main.py --chat @my_supergroup
```

### `--pick-chat`
Интерактивный выбор чата из списка диалогов.
```bash
python main.py --pick-chat
```

### `--pick-chat-limit <N>`
Сколько диалогов показать при `--pick-chat` (по умолчанию 30).
```bash
python main.py --pick-chat --pick-chat-limit 80
```

---

## Отправка в чат

### `--send-to-chat`
Если включён, скрипт:
1) отправит отчёт в **Избранное** (всегда)
2) дополнительно отправит отчёт в выбранный чат **ответом на сообщение опроса** (`reply_to=poll_msg.id`)

```bash
python main.py --send-to-chat
python main.py --pick-chat --send-to-chat
```

---

## Типовые проблемы

### `Опрос анонимный — Telegram не отдаёт список проголосовавших`
Опрос должен быть **неанонимным**.

### `POLL_VOTE_REQUIRED`
Иногда Telegram требует, чтобы аккаунт, которым запускается скрипт, сам проголосовал в опросе.
Проголосуй любым вариантом и запусти снова.

### Termux: `sqlite3.OperationalError: database is locked`
Ты остановил процесс через `Ctrl+Z` (он не завершился, а “заморозился”) и он держит `.session`.
Решение:
- Найди PID: `pgrep -a python`
- Убей: `kill <pid>` или `kill -9 <pid>`
- Удали хвосты: `rm -f *.session-journal *.session-wal *.session-shm`

### Termux: доступ к Downloads/Shared storage
Сделай один раз:
```bash
termux-setup-storage
```
Пути:
- `~/storage/shared` → корень внутренней памяти телефона
- `~/storage/downloads` → Download

---

## Безопасность
Если случайно запушил `.session` в GitHub:
1) Telegram → Settings → Devices → завершить сессии (Terminate)
2) Удалить `.session` из истории (git-filter-repo) или удалить репозиторий целиком

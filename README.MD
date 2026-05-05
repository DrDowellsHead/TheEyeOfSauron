# The Eye of Sauron (Telegram Orchestra Parser)

Скрипт находит опрос в теме Telegram-форума, собирает **список проголосовавших** за “позитивные” варианты и отправляет статистику по инструментам в **Избранное**.

Поддерживает:
- Репетиции: варианты с `✅` и/или текстом `приду`
- Концерты: все варианты `Смогу ...` (например: “Смогу к 10”, “Смогу в 13:00”, “Смогу к концерту”)
- Умную сортировку вариантов “Смогу ...” по времени/смыслу (`--smart-sort`)
- Выбор чата интерактивно (`--pick-chat`) или вручную (`--chat`)

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

## Установка (Android / Termux)

```bash
pkg update && pkg upgrade -y
pkg install -y python

python -m pip install -r requirements.txt
```

---

# CLI flags (main.py)

## --config <path>
Путь к конфигу (по умолчанию `config.ini`).

```bash
python main.py --config config.ini
```

---

## Выбор чата

По умолчанию скрипт использует чат из `config.ini` (`chat_id`).

### --chat <ref>
Запустить скрипт для другого чата.
`<ref>` может быть:
- `-100...` (супергруппа/канал)
- положительный `id` (обычная группа/чат)
- `@username`

```bash
python main.py --chat -1002291481872
python main.py --chat @my_supergroup
```

### --pick-chat
Показать список диалогов и выбрать чат номером.

```bash
python main.py --pick-chat
```

### --pick-chat-limit <N>
Сколько диалогов показывать в меню при `--pick-chat` (по умолчанию 30).

```bash
python main.py --pick-chat --pick-chat-limit 80
```

---

## --list-topics
Печатает список тем форума (topic) в текущем чате и выходит.

```bash
python main.py --list-topics
python main.py --chat -1002291481872 --list-topics
python main.py --pick-chat --list-topics
```

> Если выбранный чат не форумный (topics выключены), список будет пустым.

---

## --topic-id <id>
Явно задаёт ID темы (topic), в которой искать опрос.

```bash
python main.py --topic-id 4
```

> Если в выбранном чате нет тем/форумов, скрипт автоматически ищет опросы по всему чату (без topic_id).

---

## --topic "<часть названия>"
Ищет тему по части её названия. Если совпадений несколько — предложит выбрать.

```bash
python main.py --topic "концерт"
```

---

## --poll "<подстрока в вопросе>"
Выбирает опрос по подстроке в тексте вопроса. Если несколько — предложит выбрать.
Если не найдено — берётся самый последний опрос.

```bash
python main.py --topic-id 123 --poll "Бал в Атриуме"
```

---

## --smart-sort
Умная сортировка позитивных вариантов (в основном для концертов с несколькими “Смогу ...”):
- сначала варианты с распознанным временем (к 10, в 13:00...) по возрастанию
- затем без времени по смыслу: чек/саундчек → репетиция → концерт → прочее

```bash
python main.py --topic "концерт" --poll "Бал в Атриуме" --smart-sort
```

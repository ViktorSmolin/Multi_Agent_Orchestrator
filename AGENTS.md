# MAO — правила работы агента

Event-driven оркестратор: `core-engine` (Node 20) + PostgreSQL 16 + Redis 7 +
gateway (nginx, TLS) + агенты (Python 3.12) в Docker Compose.

## С чего начинать

1. Роли и полномочия: `Layer_1.md`, `Layer_2.md`, `Layer_3.md`, `Триада Конституций.md`.
2. Дисциплины работы: `.mao/rules/disciplines.md`.
3. Передача между уровнями: `.mao/memory/README.md`.
4. Устройство системы: `PROJECT_GUIDE.md`. Контракт: `protocol-spec.md`, `contracts/*.json`.

## Жёсткие правила

- Код — L1. Архитектура и схемы — L2. Санкция и вектор — L3/человек.
- Новая работа идёт цепочкой: `DIRECTION` → `DECISION` → `RESULT`. Минуя шаг — нельзя.
- Расхождение кода и контракта: сначала контракт, потом код.
- Закрытое не переписываем: изменение закрытого — новая фаза.
- Незакрытое замечание не теряем: чиним или регистрируем долгом.
- Секреты только в `.env` (в `.gitignore`). В код, тесты и документацию — никогда.

## Точки правды

| Что | Где |
|---|---|
| Поведение протокола | `protocol-spec.md` + `contracts/*.json` |
| Схема данных | `db/init/*.sql`, `db/migrations/*.sql` |
| Топология и сервисы | `docker-compose.yml` |
| Метрики и алерты | `observability/` |
| Реестр техдолга | `.mao/memory` (kind `DEBT`), обзор: `cli.js debt` |
| История решений | `.mao/memory/entries.jsonl` |

## Проверка (от узкого к широкому)

```bash
node --check core-engine/src/server.js        # синтаксис
node --test .mao/memory/tests/store.test.js   # тесты памяти
docker compose config --quiet                 # валидность стека
./scripts/verify_stack.sh                     # полный E2E, долго
```

Подробнее — `.mao/rules/disciplines.md` § Проверка.

## Границы

- `MAO_API_KEY` — общий ключ без ролей. Не предполагай per-user авторизацию.
- `autoheal` имеет доступ к Docker socket. Не публикуй его наружу.
- CI на чистой машине ни разу не прогонялся. Не ссылайся на «зелёный CI» как на доказательство.

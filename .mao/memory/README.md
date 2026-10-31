

# Промежуточная память триады

Общая шина общения L1 ↔ L2 ↔ L3. Не заменяет `Layer_1.md`/`Layer_2.md`/`Layer_3.md`
(это конституции), не заменяет `PROJECT_GUIDE.md` (это учебник) и не заменяет
`protocol-spec.md` (это контракт MAO). Это оперативная память: факты, эскалации,
решения, санкции, реестр техдолга.

## Где лежит

```text
.mao/memory/
  entries.jsonl   — источник истины, append-only, коммитится в git
  store.js        — библиотека (валидация, запись, чтение), ноль зависимостей
  cli.js          — интерфейс для уровней
  tests/          — node:test
```

Файл `.mao/memory/entries.jsonl` умышленно в git: история решений должна быть
воспроизводимой, а не жить в переписке.

## Модель

- Запись никогда не изменяется и не удаляется.
- Статус записи изменяется только через `ACK` или `CLOSE` со ссылкой `reply_to`.
- Итоговый статус выводится: `open` → `acknowledged` → `closed`.
- Повторная запись с тем же `id` — идемпотентный no-op (`duplicate: true`).

`DEBT` — реестр техдолга, аналог `R*` из Extra Powers; правила записи долга —
`.mao/rules/disciplines.md` § 5. `CLOSE` означает «решение исполнено» и **не**
является приёмкой: приёмку ставит человек.

## Матрица полномочий

| Уровень | Может писать |
|---|---|
| L1 | `FACT`, `QUESTION`, `ESCALATION`, `DEBT`, `RESULT`, `ACK`, `CLOSE` |
| L2 | `FACT`, `DECISION`, `ANSWER`, `ESCALATION`, `DEBT`, `ACK`, `CLOSE` |
| L3 | `DIRECTION`, `SANCTION`, `ANSWER`, `ACK`, `CLOSE` |

Правила:

- `FACT`, `ESCALATION`, `DEBT`, `DECISION`, `SANCTION`, `RESULT` — обязательны `evidence`.
- `DEBT` — обязателен `severity`: `CRITICAL` | `HIGH` | `MEDIUM` | `LOW`.
- `ANSWER`, `DECISION`, `SANCTION`, `RESULT`, `ACK`, `CLOSE` — обязателен `reply_to` на существующую запись.
- `ACK`/`CLOSE` разрешён адресату записи, её автору или кому угодно для `to: ALL`.
- `DECISION`/`SANCTION` от L1 невозможны — это отказ записи, а не предупреждение.

## Конвейер передачи

Обсуждение стратегии не заканчивается словами — оно заканчивается записью в память
по цепочке. Каждый шаг обязан ссылаться на предыдущий.

```text
L3  DIRECTION   → L2     (идея, вектор, что строим)
      ↓ reply_to
L2  DECISION    → L1     (проект: ARD, схема, контракт, порядок работ)
      ↓ reply_to
L1  RESULT      → L2     (что сделано, чем проверено)
      ↓ reply_to
L2  ACK / CLOSE          (приёмка или возврат на доработку)
```

Источником проекта может быть `DIRECTION` (новая работа от L3), `ESCALATION`
(проблема от L1) или `DEBT` (зарегистрированный долг, владелец реестра — L2).

| Шлюз | Правило | Что отклоняется |
|---|---|---|
| G1 | `DIRECTION` адресуется только `L2` | L3, командующий реализацией напрямую |
| G2 | `DECISION` адресуется `L1` и отвечает на `DIRECTION`, `ESCALATION` или `DEBT` | проект «из воздуха», без источника |
| G3 | `RESULT` отвечает на `DECISION` | реализация без утверждённого проекта |
| G4 | `SANCTION` отвечает на `DECISION` | санкция на идею, минуя проект |

Это не бюрократия, а защита от двух конкретных сбоев: L1 делает то, что никто не
проектировал, и L3 правит код через голову L2 (L3 п.6, L2 п.5).

**Исключение — эскалация, а не разработка.** `ESCALATION` от L1 идёт в L2 напрямую
(L1 п.6), `DEBT` регистрируется без проекта (L2 п.4), L2 сам решает, нужна ли
санкция L3 (L2 п.6). Шлюзы G1–G4 касаются только *новой работы*.

Посмотреть воронку целиком:

```bash
node .mao/memory/cli.js flow
```

```text
DIRECTION 1a2b3c4d L3→L2 (open) Открыть фазу целостности
  └ DECISION 5e6f7a8b L2→L1 (acknowledged) ARD-001: backpressure вместо trim
      └ RESULT 9c0d1e2f L1→L2 (open) Реализовано, тесты 19/19
```

## Схема записи

```json
{
  "id": "uuid",
  "ts": "2026-10-30T19:25:00.000Z",
  "from": "L1",
  "to": "L2",
  "kind": "ESCALATION",
  "severity": "HIGH",
  "body": "Текст в свободной форме",
  "evidence": ["core-engine/src/taskStream.js:8"],
  "reply_to": null,
  "correlation_id": "uuid или строка",
  "ref": { "files": ["path"], "ard": "ARD-001", "task": "TASK-12" }
}
```

`severity` допустим только у `DEBT`. `evidence` — до 20 строк по 200 символов.

## CLI

```bash
# L1 сообщает L2 о проблеме
node .mao/memory/cli.js post --from L1 --to L2 --kind ESCALATION \
  --body "XADD MAXLEN ~10000 теряет задачи" \
  --evidence core-engine/src/taskStream.js:8

# L2 отвечает решением
node .mao/memory/cli.js post --from L2 --to L1 --kind DECISION \
  --body "Backpressure вместо trim" --evidence docs/ard/ARD-001.md

# L1 регистрирует долг
node .mao/memory/cli.js post --from L1 --to L2 --kind DEBT --severity HIGH \
  --body "Нет сверки БД и stream" --evidence core-engine/src/server.js:775

# Что ждёт меня
node .mao/memory/cli.js list --to L2 --status open

# Реестр техдолга
node .mao/memory/cli.js debt

# Подтвердить и закрыть
node .mao/memory/cli.js ack <id> --from L2
node .mao/memory/cli.js close <id> --from L2
```

Многострочный текст: `--body-file отчёт.md` или `--body -` (stdin).

Повторная отправка: укажи `--id <uuid>` — тот же `id` даст ответ `duplicate` и не
создаст вторую строку. Без `--id` генерируется новый UUID, и это осознанно:
разные сообщения с одинаковым текстом — разные сообщения.

## Ограничения, зафиксированные явно

- Однопроцессная запись: файловая блокировка `entries.lock`, таймаут 5 с,
  признание блокировки протухшей через 30 с. Это защита от параллельных CLI,
  не распределённый lock.
- Нет сетевого доступа и нет интеграции с Redis/Postgres: сознательно, чтобы
  память уровней не зависела от рантайма MAO. Если понадобится runtime-обмен —
  это отдельный ARD на L2.
- Объём не ограничен; при росте > 10 МБ нужен архив по фазам (тоже ARD).

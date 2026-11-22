# Design

## Context

mpris-server 0.11 даёт `connect_next`/`connect_previous` (player.rs:538/:542) — сервис сейчас заявляет `can_go_next(false)`/`can_go_previous(false)` и не подключает колбэки. Политика переключения целиком живёт в `command_switch_station` + чистой `switch_plan` (change add-station-switcher) — MPRIS-путь должен её переиспользовать, а не дублировать.

## Goals / Non-Goals

**Goals:**
- Кнопки Next/Previous аплета переключают эфиры по кольцу с теми же правилами, что сегмент окна.

**Non-Goals:**
- TrackList-интерфейс, очередь треков — не существует.
- Позиционирование внутри потока (CanSeek) — осталось false.

## Decisions

- **Реuse `command_switch_station`**: Next/Previous вычисляют целевой индекс кольцом и зовут тот же хендлер — одна политика на оба источника (окно, MPRIS). Никакой новой семантики.
- **Кольцо — чистая `cycle_station(current, dir, len)` в ui.rs**: от `-1` (пользовательский URL) next ведёт в первый эфир, previous — в последний; `rem_euclid` страхует отрицательные шаги. Тестируется без окна.
- **Доставка — существующий `UiCommand`-конвейер**: MPRIS-поток → `spawn_ui_command` → event loop → `UiCommand::NextStation/PreviousStation` → `command_cycle_station` → `command_switch_station`. Тот же путь, что Play/Stop — отравленный замок и гонки уже закрыты там.

## Risks / Trade-offs

- [Аплет показывает кнопки, но трактовка «следующий трек» ≠ «следующий эфир»] → осознанно: треков в очереди нет, переключение эфира — единственный осмысленный смысл кнопок (заказ пользователя).
- [Два эфира: Next и Previous делают одно и то же] → следствие кольца из двух элементов; при добавлении станций семантика останется верной.

## Migration Plan

Три файла (mpris_service, main, ui) + спека; откат — revert.

## Open Questions

(нет)

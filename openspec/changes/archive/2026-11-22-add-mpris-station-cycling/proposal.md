# Change: Переключение эфиров кнопками Next/Previous в MPRIS

## Why

Аплет MPRIS (KDE/GNOME) показывает кнопки «следующий»/«предыдущий трек», но сервис заявляет CanGoNext=false/CanGoPrevious=false — кнопки мертвы. Переключатель эфиров уже есть в окне (change add-station-switcher); пользователь заказал тот же обход по кольцу из аплета.

## What Changes

- Сервис заявляет CanGoNext=true / CanGoPrevious=true; Next и Previous переключают эфир по кольцу (у двух эфиров — на другой; от пользовательского URL запуска next ведёт в первый эфир, previous — в последний) с той же семантикой, что у сегмента окна: при живой сессии — остановка и автозапуск выбранного, при остановленном плеере — только выбор.

## Capabilities

### New Capabilities

(пусто)

### Modified Capabilities

- `mpris`: «Команды воспроизведения» — CanGoNext/CanGoPrevious=true, Next/Previous переключают эфир по кольцу.

## Impact

- `src/mpris_service.rs` — команды NextStation/PreviousStation, флаги can_go_next/previous=true, connect_next/previous.
- `src/main.rs` — UiCommand, кольцевой шаг и вызов общего `command_switch_station`.
- `src/ui.rs` — чистая `cycle_station` (кольцо от активного индекса).

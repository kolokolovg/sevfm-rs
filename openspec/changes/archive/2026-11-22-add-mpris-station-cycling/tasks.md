# Tasks

## 1. Сервис и команды

- [x] 1.1 `src/ui.rs`: чистая `cycle_station(current, dir, len)` (кольцо, `-1` — пользовательский URL: next → первый, previous → последний) + тесты кольца — критерий: тесты зелёные
- [x] 1.2 `src/mpris_service.rs`: варианты `NextStation`/`PreviousStation` в `MprisCommand`, `can_go_next(true)`/`can_go_previous(true)`, `connect_next`/`connect_previous` — критерий: `cargo build`
- [x] 1.3 `src/main.rs`: `UiCommand::NextStation/PreviousStation`, маппинг в MPRIS-closure, `command_cycle_station` → общий `command_switch_station` — критерий: `cargo build`

## 2. Вороты

- [x] 2.1 Полный набор локальных ворот: `cargo fmt --check`, `cargo clippy --all-targets`, `cargo nextest run`, doctests, `openspec validate --all` — критерий: все зелёные

## 3. Ручная приёмка (пользователь)

- [x] 3.1 Аплет MPRIS: Next при воспроизведении — эфир сменился с автоподключением; Previous при остановленном — выделение сместилось без звука

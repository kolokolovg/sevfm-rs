# Tasks

## 1. Командная шина и close-to-tray (платформонезависимое ядро)

- [x] 1.1 Семантика Slint 1.18 по исходникам: `run_event_loop_until_quit` (slint lib.rs:265 = `set_event_loop_quit_on_last_window_closed(false)`) не завершает цикл при скрытии последнего окна; `on_close_requested` + `CloseRequestResponse::HideWindow` (i-slint-core api.rs:458–530); `Weak<T>: Send` (api.rs:1247); `WindowItem.icon` (items.rs:1274). Выводы зафиксированы в design.md D3/D5/D6.
- [x] 1.2 Выделить общие хендлеры команд (play / stop / show window / hide window / quit) из замыканий `on_play`/`on_stop` в `src/main.rs` (design D2): кнопки окна, меню трея и MPRIS зовут один код. Проверка: `cargo test --lib` зелёный, кнопки окна работают как раньше при ручном запуске.
- [x] 1.3 Перевести `run_ui` на `slint::run_event_loop_until_quit()` + `window.on_close_requested`: трей жив ⇒ только `HideWindow`, трея нет ⇒ `quit_event_loop()`; teardown после цикла (`main.rs:443–470`) не трогать. Проверка: существующие `tests/sigint_*` зелёные; ручной запуск (до трея) — закрытие окна и сигнал завершают процесс с кодом 0.

## 2. Трей: встроенный SystemTrayIcon (.slint-компонент и wiring)

- [x] 2.1 Создать lib-модель `src/tray.rs` (design D2): `TraySnapshot` (UiState, станция, window_visible) с производными — tooltip-строка, label пункта «Показать/Скрыть окно», can_play/can_stop; чистая сборка из `SessionTick`. Проверка: unit-тесты в `src/tray.rs` зелёные — пять статусов, границы гейтов, станция без icy-name, tooltip-строки по правилу «трек или станция» (уточнено пользователем при ручном тестировании) и label переключателя.
- [x] 2.2 `ui/player-window.slint`: экспортируемый `PlayerTray inherits SystemTrayIcon` — `icon: @image-url("app-icon.png")`, `tooltip`, свойства `can-play`/`can-stop`/`window-visible`, меню «Играть»/«Стоп»/«Показать-Скрыть окно»/«Выход» с enabled-гейтами, callback'и `play`/`stop`/`toggle-window`/`quit` и `clicked`; окну `PlayerWindow` добавить `icon: @image-url("app-icon.png")`. Проверка: `cargo build` — сгенерирован `PlayerTray` с `new`/`show`/`hide`/set-терами, окно получает иконку.
- [x] 2.3 Wiring в `run_ui`: `PlayerTray::new()` (ошибка ⇒ `None` + лог, design D3), колбэки трея и `clicked` (ЛКМ = toggle окна) → общие хендлеры 1.2, push свойств (`tooltip`/`can-play`/`can-stop`/`window-visible`) из существующего 100-мс тика, `on_close_requested` учитывает живость трея. Проверка: D-Bus-прогон трея 10/10; крестик (шорткат Window Close) при живом трее — процесс жив, окно скрыто, ЛКМ возвращает; без трея крестик завершает с кодом 0.
- [x] 2.4 Деградация без D-Bus: запуск с недоступной шиной (`DBUS_SESSION_BUS_ADDRESS` на несуществующий путь) — отказ трея в логе, окно работает, закрытие окна завершает процесс с кодом 0. Проверка: живой запуск с битой шиной — «трей недоступен: нет шины D-Bus сессии» в логе, процесс жив, SIGINT → код 0, значка на шине нет; для детекта отказа пробуем шину сами (ksni-спавн асинхронен, `new()` ошибки не даёт).

## 3. Иконка приложения

- [x] 3.1 Иконка `ui/app-icon.png` — герб Севастополя, вырезанный по альфа-bbox из `ui/cover-placeholder.png` (256×256 RGBA, поля 14 px, прозрачный фон; визуальная проверка пройдена: щит целиком, края не задеты, альфа внутри щита непрозрачна). Подключение — в 2.2.

## 4. MPRIS (mpris-server, Linux)

- [x] 4.1 Добавить `mpris-server` 0.11 в `[target.'cfg(target_os = "linux")'.dependencies]`; выделенный `std::thread` с `mpris_server::zbus::block_on`, реализация `Player`: Play/Stop/PlayPause/Raise/Quit через `invoke_from_event_loop` → общие хендлеры 1.2; bus name `org.mpris.MediaPlayer2.sevfm`, Identity «Говорит Севастополь» (design D4). Проверка: `cargo build`; `busctl --user list | grep sevfm` видит сервис при живом запуске.
- [x] 4.2 PlaybackStatus (активная сессия ⇒ Playing, «Остановлено» ⇒ Stopped), Can*-флаги (CanPlay/CanControl=true, CanPause/CanSeek/CanGoNext/CanGoPrevious=false), метаданные: xesam:artist/title с заглушкой-станцией, xesam:url = адрес потока, `mpris:artUrl` = `file://` на файл кеша обложек; diff-кеш последнего отправленного + эмитеры из 100-мс тика. Проверка: unit-тесты маппинга статусов и метаданных; живой прогон — аплет KDE показывает плеера, статус и трек.
- [x] 4.3 Raise (показать окно) и Quit (полный выход с кодом 0); деградация: ошибка шины ⇒ поток MPRIS логирует и завершается, приложение работает. Проверка: `busctl --user call` для Raise/Quit вручную; запуск без шины — приложение живо, в логе отказ.
- [x] 4.4 Живой интеграционный тест MPRIS против реальной шины сессии (пропуск без `DBUS_SESSION_BUS_ADDRESS`): Identity/PlaybackStatus/Metadata до и после Play/Stop на мок-сессии. Проверка: тест зелёный в среде с шиной, корректно пропускается без неё.

## 5. Гейты качества

- [x] 5.1 Полный живой прогон в KDE по чек-листу спек: close-to-tray с играющим потоком, возврат окна из трея, «Выход» из трея (код 0, без зависающих потоков), SIGINT при скрытом окне (код 0), MPRIS в аплете (статус/трек/Play/Stop/Raise/Quit), деградация без шины. Проверка: чек-лист пройден пользователем вручную (раунды 1–19 ок + виджет KDE: плей/пауза/метаданные), деградация без шины — сценарно.
- [x] 5.2 Гейты: `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, `cargo test`, `cargo check --target x86_64-pc-windows-msvc` (rustup target add); сборка бинарника перед ручными запусками (не полагаться на stale `target/debug`). Проверка: fmt/clippy `-D warnings`/test зелёные; mingw-w64 установлен пользователем; `cargo check --target x86_64-pc-windows-gnu` зелёный по коду выхода. Находка ревью oracle: первый «зелёный» прогон был ложным (grep по warnings при падённой сборке, impl UiCommand без cfg — 7 ошибок) — после правки перепроверено по exit-коду. `cargo tree -d`: zbus 5 + 6 — ожидаемое дубление.
- [x] 5.3 Headless-регресс: `--headless` — значка и MPRIS-сервиса нет, коды выхода 0/2 сохранены. Проверка: `cargo test` зелёный (88), `sigint_live` ок; живой headless под KDE — ни SNI-значка, ни MPRIS-имени (единственное bus-соединение процесса — PipeWire аудио), SIGINT — код 0.

## Workflow follow-up

- Ревью реализации субагентами (hunter + oracle) после закрытия задач; устранение находок.
- `openspec sync` дельт в main-спеки после приёмки.
- Архив change (`openspec archive`), коммит и пуш по готовности.

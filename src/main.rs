// Copyright 2026 Gleb Kolokolov
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! sevfm — настольный плеер онлайн-радио «Говорит Севастополь»
//! на Rust + Slint.
//!
//! По умолчанию открывается окно (кнопки «Играть»/«Стоп», станция,
//! статус, трек); флаг `--headless` сохраняет CLI-поведение: тракт
//! сразу, статусы и эфир в stderr. Позиционный аргумент — URL потока
//! (без него — станция по умолчанию). Остановка: «Стоп»; полный выход
//! — «Выход» из трея, закрытие окна (без трея), Ctrl+C (SIGINT) или
//! SIGTERM — код 0 в течение секунды. При живом трее закрытие окна
//! скрывает его, не останавливая воспроизведение.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sevfm::cover::{CoverImage, CoverReply, Covers};
use sevfm::icy::TrackInfo;
use sevfm::net::{PlaybackStatus, playback_status};
use sevfm::session::{PlayerSession, SessionHandle, SessionOutcome};
use sevfm::tray::TraySnapshot;
use sevfm::ui::{PlayerView, StatusKind as UiStatusKind, TrackSlot, UiState, parse_args};
use sevfm::{redact_url, say};

slint::include_modules!();

// MPRIS — только Linux (design D4 add-tray): D-Bus-поток с командами
// в общий event loop; прочие платформы собираются без него.
#[cfg(target_os = "linux")]
mod mpris_service;

/// Интервал опроса сессии (poll-таймер окна и цикл статусов headless) —
/// тот же ритм, что такт воркера сессии.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Код выхода при ошибке запуска тракта или сбое живого цикла
/// (спека: 0 — штатная остановка, 2 — неуспех).
const EXIT_FAILURE: i32 = 2;
/// Сторожевая граница join воркера после выхода из цикла событий:
/// закрытие окна и сигнал обязаны давать код 0 ≤ 1 с (спека player-ui).
const JOIN_DEADLINE: Duration = Duration::from_secs(1);
/// Ожидание пробы среды трея (находка ревью hunter): у zbus нет
/// таймаута handshake — «чёрная дыра» в адресе шины не должна вешать
/// старт UI дольше этого срока.
const TRAY_PROBE_TIMEOUT: Duration = Duration::from_secs(3);

/// Индикатор состояния окна (design D3 redesign-hero-window): lib не
/// зависит от Slint, поэтому вид живёт в `sevfm::ui`, а здесь —
/// отображение на сгенерированный enum окна.
fn window_status_kind(kind: UiStatusKind) -> StatusKind {
    match kind {
        UiStatusKind::Playing => StatusKind::Playing,
        UiStatusKind::Waiting => StatusKind::Waiting,
        UiStatusKind::Stopped => StatusKind::Stopped,
    }
}

/// Печать эфира с дедупликацией (design D5 фазы ICY, headless-режим):
/// станция — один раз при первом появлении, трек — при каждой смене.
struct NowPlayingPrinter {
    station_shown: bool,
    last_track: Option<TrackInfo>,
}

impl NowPlayingPrinter {
    fn new() -> Self {
        Self {
            station_shown: false,
            last_track: None,
        }
    }

    fn poll(&mut self, station: &Option<String>, track: &Option<TrackInfo>) {
        if !self.station_shown
            && let Some(station) = station
        {
            say(&format!("станция: {station}"));
            self.station_shown = true;
        }
        // None-трек — «нет данных», не переход: печать только на смену
        // реального трека.
        if track != &self.last_track
            && let Some(track) = track
        {
            say(&format!("сейчас в эфире: {}", track.display()));
            self.last_track = Some(track.clone());
        }
    }
}

/// Связка окна и сессий (design D3/D4 add-player-ui): живёт под
/// `Arc<Mutex>` — её трогают poll-таймер, callback'и окна и обработчик
/// сигнала, все секции короткие.
struct UiBinding {
    url: String,
    process_stop: Arc<AtomicBool>,
    session: Option<SessionHandle>,
    view: PlayerView,
    /// Сервис обложек (design D5/D6 add-album-art): подписка на смены
    /// трека и сбор ответов — из poll-таймера, инвалидация — на «Играть».
    covers: Covers,
}

impl UiBinding {
    fn new(url: String, process_stop: Arc<AtomicBool>) -> Self {
        Self {
            url,
            process_stop,
            session: None,
            view: PlayerView::new(),
            covers: Covers::start(),
        }
    }
}

/// Сборка изображения Slint из декодированной обложки (design D7):
/// RGB-буфер из lib → `SharedPixelBuffer` → `Image`. Живёт в bin —
/// lib от Slint не зависит.
fn slint_cover(image: &CoverImage) -> slint::Image {
    let mut buffer = slint::SharedPixelBuffer::<slint::Rgb8Pixel>::new(image.width, image.height);
    buffer.make_mut_bytes().copy_from_slice(&image.pixels);
    slint::Image::from_rgb8(buffer)
}

/// Разделяемая связка для таймера и обработчиков.
type SharedUiBinding = Arc<Mutex<UiBinding>>;

/// Команда «Играть» (design D2 add-tray): общий код кнопки окна, меню
/// трея и MPRIS — все источники зовут одну функцию. Новая сессия —
/// только из «Остановлено» (гейт can-play отпускает кнопку по факту
/// завершения воркера, teardown ~100–300 мс: следующая сессия стартует
/// только на мёртвом воркере — без гонки за аудиоустройство); здесь же
/// защита от команды, опередившей таймер.
fn command_play(binding: &SharedUiBinding, window: &PlayerWindow) {
    // Отравленный замок не глушит команду (находка ревью hunter):
    // паника под замком уже напечатана хуком, данные связки пригодны
    // и из отравленного состояния — как в teardown.
    let mut ui = binding
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if ui
        .session
        .as_ref()
        .is_some_and(|session| session.is_running())
    {
        return;
    }
    // Мёртвый воркер прошлой сессии: подбираем поток (мгновенно).
    if let Some(mut old) = ui.session.take() {
        old.join();
    }
    // Заглушка берётся у окна в момент команды: держать Image в
    // межпоточных замыканиях нельзя (не Send), а окно живёт, пока
    // жив цикл событий.
    let placeholder_cover = window.get_placeholder_cover();
    match PlayerSession::start(ui.url.clone(), ui.process_stop.clone()) {
        Ok(session) => {
            say("играть: запуск сессии");
            ui.session = Some(session);
            ui.view.reset();
            // Поля прошлой сессии не имеют отношения к новой:
            // статус и гейты обновит ближайший poll-такт.
            window.set_station("".into());
            window.set_artist("".into());
            window.set_track_title("".into());
            window.set_error("".into());
            // Обложка тоже: гасим незавершённый поиск прошлой
            // сессии и показываем заглушку до первого трека.
            ui.covers.invalidate();
            window.set_cover(placeholder_cover);
        }
        Err(e) => {
            // Поля прошлой сессии не переживают неудачный старт:
            // трек и обложка погашены, плашка ошибки — не поверх чужого
            // трека (находка ревью hunter).
            window.set_station("".into());
            window.set_artist("".into());
            window.set_track_title("".into());
            window.set_error(format!("не удалось запустить поток чтения: {e}").into());
            ui.covers.invalidate();
            window.set_cover(placeholder_cover);
        }
    }
}

/// Команда «Стоп» (design D2 add-tray): флаг остановки сессии — воркер
/// глушит вывод и освобождает устройство за такт опроса, статус уходит
/// в «Остановлено» по факту завершения воркера (ближайший poll-такт).
fn command_stop(binding: &SharedUiBinding) {
    let ui = binding
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(session) = ui.session.as_ref() {
        say("стоп: остановка сессии");
        session.stop();
    }
}

/// Переключение видимости окна (design D2 add-tray): пункт меню и ЛКМ
/// по значку; воспроизведение не трогают.
fn command_toggle_window(window: &PlayerWindow) {
    if window.window().is_visible() {
        let _ = window.window().hide();
    } else {
        let _ = window.show();
    }
}

/// PlayPause MPRIS (design D2): живой поток не паузится — активная
/// сессия останавливается, остановленная запускается. Только Linux:
/// единственный источник PlayPause — MPRIS-поток.
#[cfg(target_os = "linux")]
fn command_play_pause(binding: &SharedUiBinding, window: &PlayerWindow) {
    // Отравленный замок не глушит команду (как в command_play/stop).
    let running = binding
        .lock()
        .map(|ui| {
            ui.session
                .as_ref()
                .is_some_and(|session| session.is_running())
        })
        .unwrap_or_else(|poisoned| {
            poisoned
                .into_inner()
                .session
                .as_ref()
                .is_some_and(|session| session.is_running())
        });
    if running {
        command_stop(binding);
    } else {
        command_play(binding, window);
    }
}

/// Полный выход (design D5): «Выход» из трея и MPRIS Quit идут тем же
/// путём, что сигнал, — teardown после цикла событий.
fn command_quit() {
    let _ = slint::quit_event_loop();
}

/// Команды внешних источников в event loop (design D2): кнопки окна,
/// меню трея и MPRIS исполняют одни хендлеры; вариант нужен только
/// MPRIS (его поток — не поток цикла событий) — платформа с MPRIS.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
enum UiCommand {
    Play,
    Stop,
    PlayPause,
    Raise,
    Quit,
}

#[cfg(target_os = "linux")]
impl UiCommand {
    /// Исполнение в главном потоке (внутри цикла событий).
    fn run(self, binding: &SharedUiBinding, window: &PlayerWindow) {
        match self {
            UiCommand::Play => command_play(binding, window),
            UiCommand::Stop => command_stop(binding),
            UiCommand::PlayPause => command_play_pause(binding, window),
            UiCommand::Raise => {
                let _ = window.show();
            }
            UiCommand::Quit => command_quit(),
        }
    }
}

/// Планирование команды в цикл событий из любого потока (design D2;
/// паттерн ctrlc-обработчика). `Weak` и связка — Send, окно живёт,
/// пока жив цикл. Ошибка доставки (находка ревью) — в лог: молчать о
/// потерянной команде хуже шума teardown.
#[cfg(target_os = "linux")]
fn spawn_ui_command(
    command: UiCommand,
    binding: &SharedUiBinding,
    weak: slint::Weak<PlayerWindow>,
) {
    let binding = binding.clone();
    if let Err(e) = slint::invoke_from_event_loop(move || {
        if let Some(window) = weak.upgrade() {
            command.run(&binding, &window);
        }
    }) {
        say(&format!("mpris: команда не доставлена в цикл событий: {e}"));
    }
}

/// Проба среды трея (design D3, находки ревью): недостаточно живой
/// шины — нужен и SNI-хост (`org.kde.StatusNotifierWatcher`), иначе
/// значок некому показать (GNOME без расширения), а close-to-tray
/// прячет окно «в никуда» (спека system-tray «Деградация»). Проба —
/// ранняя диагностика с причиной в логе; ошибки `PlayerTray::new()`
/// остаются второй линией защиты. Трейд-офф: автозапуск раньше панели
/// получает отказ на всю сессию — принято (design). Сама проверка идёт
/// в отдельном потоке с таймаутом: у zbus нет таймаута handshake, и
/// «чёрная дыра» в адресе шины не должна вешать старт UI — SIGINT её
/// не прерывает (находка ревью hunter).
#[cfg(target_os = "linux")]
fn tray_environment() -> Option<&'static str> {
    let (probe_tx, probe_rx) = std::sync::mpsc::channel::<Option<&'static str>>();
    let probe = std::thread::Builder::new()
        .name("tray-probe".into())
        .spawn(move || {
            let result = mpris_server::zbus::block_on(async {
                let connection = match mpris_server::zbus::Connection::session().await {
                    Ok(connection) => connection,
                    Err(_) => return Some("нет шины D-Bus сессии"),
                };
                let watcher = connection
                    .call_method(
                        Some("org.freedesktop.DBus"),
                        "/org/freedesktop/DBus",
                        Some("org.freedesktop.DBus"),
                        "NameHasOwner",
                        &"org.kde.StatusNotifierWatcher",
                    )
                    .await;
                let hosted = watcher
                    .ok()
                    .and_then(|reply| reply.body().deserialize::<bool>().ok())
                    .unwrap_or(false);
                if hosted {
                    None
                } else {
                    Some("нет SNI-хоста (org.kde.StatusNotifierWatcher)")
                }
            });
            let _ = probe_tx.send(result);
        });
    let Ok(probe) = probe else {
        return Some("поток пробы трея не запущен");
    };
    // Поток отвязан: зависшая проба приберёт выход процесса.
    drop(probe);
    match probe_rx.recv_timeout(TRAY_PROBE_TIMEOUT) {
        Ok(result) => result,
        Err(_) => Some("среда трея не ответила вовремя"),
    }
}

#[cfg(not(target_os = "linux"))]
fn tray_environment() -> Option<&'static str> {
    None
}

fn main() {
    std::process::exit(run());
}

/// Коды выхода: 0 — штатная остановка (сигнал, закрытие окна или конец
/// потока), 2 — ошибка аргументов, запуска тракта (нет аудиоустройства,
/// поток не MP3) или сбой живого цикла.
fn run() -> i32 {
    let launch = match parse_args(std::env::args_os().skip(1)) {
        Ok(launch) => launch,
        Err(message) => {
            say(&message);
            return EXIT_FAILURE;
        }
    };

    // Флаг завершения процесса — головной стоп для любой сессии:
    // в headless его ставит сигнал (плюс сторож выхода), в UI — сигнал
    // дублирует им прямой stop активной сессии.
    let process_stop = Arc::new(AtomicBool::new(false));

    // Связка окна — только в UI-режиме: её конструктор поднимает сервис
    // обложек, headless создаёт свой в run_headless (находка ревью:
    // второй воркер обложек в headless был бы осиротевшим).
    let ui_mode = !launch.headless;
    let binding: Option<SharedUiBinding> = ui_mode.then(|| {
        Arc::new(Mutex::new(UiBinding::new(
            launch.url.clone(),
            process_stop.clone(),
        )))
    });

    // Обработчик сигнала — один на процесс (ограничение крейта), ставится
    // до ветвления режимов (design D5). Действие выбирается по режиму:
    // headless — только флаг (дальше сторож), UI — стоп активной сессии
    // и выход из цикла событий.
    if ctrlc::set_handler({
        let process_stop = process_stop.clone();
        let binding = binding.clone();
        move || {
            process_stop.store(true, Ordering::SeqCst);
            if let Some(binding) = binding.as_ref() {
                // Паника в обработчике сигнала недопустима: отравленный
                // замок не должен мешать главному действию — выходу из
                // цикла событий.
                if let Ok(ui) = binding.lock()
                    && let Some(session) = ui.session.as_ref()
                {
                    session.stop();
                }
                // Разрешён из любого потока; ошибка «цикл не запущен или
                // уже завершён» не важна — окно закрывается и само.
                let _ = slint::quit_event_loop();
            }
        }
    })
    .is_err()
    {
        say("не удалось установить обработчик сигнала");
        return EXIT_FAILURE;
    }

    if launch.headless {
        run_headless(&launch.url, &process_stop)
    } else {
        run_ui(
            &binding.expect("связка окна создана только в UI-режиме"),
            &process_stop,
        )
    }
}

/// CLI-режим (`--headless`): тракт сразу, статусы и эфир в stderr.
///
/// Коды выхода: 0 — штатная остановка (сигнал или конец потока),
/// 2 — ошибка запуска тракта или сбой живого цикла.
fn run_headless(url: &str, process_stop: &Arc<AtomicBool>) -> i32 {
    // Сторож выхода: сигнал может прийти, когда главный поток заблокирован
    // (ожидание данных, декодирование) — выход делаем сразу, без
    // drop/join аудио-потоков (design D5: ресурсы освобождает процесс).
    {
        let stop = process_stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                std::thread::sleep(POLL_INTERVAL);
            }
            say("остановка по сигналу…");
            std::process::exit(0);
        });
    }

    say(&format!("подключение к {}…", redact_url(url)));
    let mut session = match PlayerSession::start(url.to_owned(), process_stop.clone()) {
        Ok(session) => session,
        Err(e) => {
            say(&format!("не удалось запустить поток чтения: {e}"));
            return EXIT_FAILURE;
        }
    };

    // Статус — по переходам: «Подключение» предсеяно (строка подключения
    // уже напечатана); станция и первый трек видны уже в фазе предбуфера —
    // метаданные приходят с первыми байтами.
    let mut ether_printer = NowPlayingPrinter::new();
    let mut last_status = Some(PlaybackStatus::Connecting);
    // Обложки в headless (спека album-art): тот же сервис, исходы поиска
    // пишет в лог сам воркер — здесь только подписка на смену трека
    // (дедупликация по значению, как у эфирной печати).
    let covers = Covers::start();
    let mut cover_target: Option<TrackInfo> = None;
    while session.is_running() {
        let tick = session.tick();
        ether_printer.poll(&tick.station, &tick.track);
        if tick.track != cover_target {
            cover_target = tick.track.clone();
            covers.track_changed(match &tick.track {
                Some(TrackInfo::Track { artist, title }) => Some((artist, title)),
                _ => None,
            });
        }
        // Ответы только вычитываем: находки и промахи уже в логе воркера.
        while covers.poll_reply().is_some() {}
        // Живой цикл: пауза — точный флаг underrun декодера, а не «буфер
        // пуст» (звук идёт и на низком уровне заполнения).
        let status = playback_status(tick.net, tick.playback_started, tick.underrun);
        if last_status != Some(status) {
            say(&format!("состояние: {}", status.label()));
            last_status = Some(status);
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    // Классификация исхода — в сессии (порядок и сообщения прежнего
    // `run()`): Fatal → диагностика и код 2, Stopped → 0.
    match session.join() {
        SessionOutcome::Stopped => 0,
        SessionOutcome::Fatal(message) => {
            say(&message);
            EXIT_FAILURE
        }
    }
}

/// Режим окна по умолчанию: окно в «Остановлено», тракт — по «Играть».
fn run_ui(binding: &SharedUiBinding, process_stop: &Arc<AtomicBool>) -> i32 {
    // Жив ли трей (design D5): закрывает ли окно «в трей» или завершает
    // процесс. Взводится созданием tray-компонента; без трея закрытие
    // окна — полный выход, как прежде.
    let tray_alive = Arc::new(AtomicBool::new(false));
    let window = match PlayerWindow::new() {
        Ok(window) => window,
        Err(e) => {
            say(&format!("не удалось открыть окно: {e}"));
            return EXIT_FAILURE;
        }
    };
    // Идентификатор окна (app_id на Wayland, WM_CLASS на X11): панель задач
    // резолвит его в sevfm.desktop и показывает герб. Без app_id Wayland
    // связать окно с приложением не может — на таскбаре заглушка.
    // Читается адаптером при создании winit-окна (в event loop), поэтому
    // успевает и после new(), главное — до run_event_loop.
    slint::set_xdg_app_id("sevfm").expect("app_id задаётся при живой платформе");
    window.set_status(UiState::Stopped.label().into());
    window.set_status_kind(window_status_kind(UiState::Stopped.kind()));
    window.set_can_play(true);
    window.set_can_stop(false);
    // Заглушка обложки — отдельное свойство окна (design D2 add-tray):
    // Rust читает его и для poll-таймера, и для хендлеров команд.
    // Старт в «Остановлено» — уже с заглушкой (спека player-ui): у
    // свойства `cover` дефолта больше нет.
    let placeholder_cover = window.get_placeholder_cover();
    window.set_cover(placeholder_cover.clone());

    // Трей (design D1/D3 add-tray): единая иконка приложения, меню —
    // декларативно в .slint, бэкенд строит Slint. Отказ — только лог:
    // окно работает, close-to-tray выключен, закрытие окна возвращает
    // полный выход. Встроенный трей создаётся асинхронно и его отказ
    // не виден из `new()` (ksni-спавн в цикле событий) — среду (шина +
    // SNI-хост) пробуем сами, иначе close-to-tray включился бы при
    // мёртвом/невидимом трее (спека system-tray «Деградация»).
    let tray = match tray_environment() {
        None => PlayerTray::new()
            .inspect_err(|e| say(&format!("трей недоступен: {e}")))
            .ok(),
        Some(reason) => {
            say(&format!("трей недоступен: {reason}"));
            None
        }
    };
    if tray.is_some() {
        tray_alive.store(true, Ordering::SeqCst);
    }
    if let Some(tray) = tray.as_ref() {
        // Команды меню — общие хендлеры (design D2); «Выход» — тот же
        // путь, что сигнал.
        {
            let binding = binding.clone();
            let weak = window.as_weak();
            tray.on_play(move || {
                if let Some(window) = weak.upgrade() {
                    command_play(&binding, &window);
                }
            });
        }
        {
            let binding = binding.clone();
            tray.on_stop(move || command_stop(&binding));
        }
        {
            let weak = window.as_weak();
            tray.on_toggle_window(move || {
                if let Some(window) = weak.upgrade() {
                    command_toggle_window(&window);
                }
            });
        }
        tray.on_quit(command_quit);
    }

    // MPRIS (design D4 add-tray): сервис в отдельном потоке, команды —
    // в общий event loop; отказ шины — только лог, трей не нужен.
    #[cfg(target_os = "linux")]
    let mpris = {
        use mpris_service::{Mpris, MprisCommand};
        let binding = binding.clone();
        let weak = window.as_weak();
        Mpris::start(Arc::new(move |command| {
            let command = match command {
                MprisCommand::Play => UiCommand::Play,
                MprisCommand::Stop => UiCommand::Stop,
                MprisCommand::PlayPause => UiCommand::PlayPause,
                MprisCommand::Raise => UiCommand::Raise,
                MprisCommand::Quit => UiCommand::Quit,
            };
            spawn_ui_command(command, &binding, weak.clone());
        }))
    };

    // «Играть» и «Стоп» — общие хендлеры команд (design D2 add-tray):
    // тот же код зовут меню трея и MPRIS; кнопки лишь доставляют
    // команду в главный поток, где живёт цикл событий.
    {
        let binding = binding.clone();
        let weak = window.as_weak();
        window.on_play(move || {
            let Some(window) = weak.upgrade() else { return };
            command_play(&binding, &window);
        });
    }
    {
        let binding = binding.clone();
        window.on_stop(move || command_stop(&binding));
    }

    // Poll-таймер (design D3): создаётся в главном потоке (ограничение
    // Slint — поток, не факт запуска цикла) и живёт до конца run_ui —
    // drop таймера глушит его. Читает слоты сессии, прогоняет модель
    // окна с дедупликацией и пишет только изменения.
    let timer = slint::Timer::default();
    {
        let binding = binding.clone();
        let weak = window.as_weak();
        let process_stop = process_stop.clone();
        // Хендл трея для poll-такта: Timer живёт на потоке Slint, VRc
        // переносить между потоками не нужно.
        let tray = tray.as_ref().map(|tray| tray.clone_strong());
        timer.start(slint::TimerMode::Repeated, POLL_INTERVAL, move || {
            // Сигнал до старта цикла терял quit_event_loop (Slint не
            // латчит запрос без цикла — NoEventLoopProvider): повторяем
            // выход по флагу из первого же тика.
            if process_stop.load(Ordering::SeqCst) {
                let _ = slint::quit_event_loop();
            }
            let Some(window) = weak.upgrade() else { return };
            // Отравленный замок не глушит poll-такт: паника под замком
            // уже напечатана хуком, а глушение здесь заморозило бы
            // окно, трей и MPRIS молча и навечно при живых командах
            // (находка ревью hunter).
            let mut ui = binding
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let tick = ui.session.as_ref().map(|session| session.tick());
            // Снапшот внешних поверхностей — один на такт (design D2):
            // трей обновляет свойства, MPRIS шлёт статус и метаданные;
            // дедупликацию делают сами потребители (ChangeTracker у
            // свойств, Player у PlaybackStatus/Metadata).
            let snapshot = TraySnapshot::from_tick(tick.as_ref(), window.window().is_visible());
            if let Some(tray) = tray.as_ref() {
                tray.set_tray_tooltip(snapshot.tooltip().into());
                tray.set_can_play(snapshot.can_play());
                tray.set_can_stop(snapshot.can_stop());
                tray.set_window_visible(snapshot.window_visible);
            }
            #[cfg(target_os = "linux")]
            if let Some(mpris) = mpris.as_ref() {
                // artUrl — только у музыкального трека с обложкой
                // в кеше (спека mpris); имя файла — sha256, но базовый
                // каталог из XDG/HOME требует экранирования (находка
                // аудита — sevfm::mpris::file_uri).
                let art_url = match &snapshot.track {
                    TrackSlot::Track { artist, title } => {
                        sevfm::cover::cached_cover_path(artist, title)
                            .map(|path| sevfm::mpris::file_uri(&path))
                    }
                    _ => None,
                };
                mpris.push(mpris_service::MprisUpdate {
                    playback: sevfm::mpris::MprisPlayback::of(&snapshot),
                    track: sevfm::mpris::MprisTrack::of(&snapshot),
                    art_url,
                    stream_url: ui.url.clone(),
                });
            }
            let Some(tick) = tick else { return };
            let update = ui.view.poll(&tick);
            if let Some(station) = update.station {
                window.set_station(station.unwrap_or_default().into());
            }
            if let Some(slot) = update.track {
                // Обложка следует правилу видимости трек-строки (спека
                // player-ui/album-art): трек — асинхронный поиск, сырое
                // название, вставка и остановка — заглушка без сети.
                match &slot {
                    TrackSlot::Track { artist, title } => {
                        ui.covers.track_changed(Some((artist, title)));
                    }
                    _ => {
                        ui.covers.track_changed(None);
                        window.set_cover(placeholder_cover.clone());
                    }
                }
                // Трек-строка (правило чек-листа 4.3): трек — исполнитель
                // и название; сырой StreamTitle — только название;
                // заглушка — название станции (без станции строка пуста
                // и скрыта разметкой).
                let (artist, title) = match slot {
                    TrackSlot::Track { artist, title } => (artist, title),
                    TrackSlot::Title { title } => (String::new(), title),
                    TrackSlot::Placeholder { station } => {
                        (String::new(), station.unwrap_or_default())
                    }
                };
                // Артист капсом (design D5): у Slint нет text-transform,
                // преобразование делает связка при записи в окно.
                window.set_artist(artist.to_uppercase().into());
                window.set_track_title(title.into());
            }
            // Ответы воркера обложек: устаревшие гасятся генерацией
            // внутри сервиса, поэтому каждый доставленный применим;
            // промах — заглушка (спека album-art).
            while let Some(reply) = ui.covers.poll_reply() {
                window.set_cover(match reply {
                    CoverReply::Found(image) => slint_cover(&image),
                    CoverReply::Miss => placeholder_cover.clone(),
                });
            }
            if let Some(status) = update.status {
                window.set_status(status.label().into());
                window.set_status_kind(window_status_kind(status.kind()));
                window.set_can_play(status == UiState::Stopped);
                window.set_can_stop(status != UiState::Stopped);
                // Исход сессии — в stderr: ошибки в окне мимолётны, а
                // таймлайн Играть/Стоп/ошибка нужен и в логе.
                if status == UiState::Stopped
                    && let Some(outcome) = &tick.outcome
                {
                    match outcome {
                        SessionOutcome::Stopped => say("сессия завершена: остановлено"),
                        SessionOutcome::Fatal(message) => {
                            say(&format!("сессия завершена: ошибка — {message}"));
                        }
                    }
                }
            }
            if let Some(error) = update.error {
                window.set_error(error.into());
            }
            // Смена эфира из потока — в stderr: вставка в окне видна
            // заглушкой, но в логе смена метаданных обязана быть видна.
            if let Some(ether) = update.ether {
                match ether {
                    TrackInfo::Track { artist, title } => {
                        say(&format!("эфир: {artist} — {title}"));
                    }
                    TrackInfo::Insert { title } => {
                        say(&format!("эфир: вставка ({title}) — в окне заглушка"));
                    }
                    TrackInfo::Raw { title } => {
                        say(&format!("эфир: {title}"));
                    }
                }
            }
        });
    }

    // Close-to-tray (design D5): закрытие окна при живом трее лишь
    // скрывает окно — процесс и воспроизведение живут, полный выход
    // идёт через quit_event_loop («Выход» из трея, сигнал, MPRIS Quit)
    // и разбирается одинаковым teardown ниже. Хендлер обязателен и без
    // трея: под until_quit закрытие по умолчанию не завершает цикл.
    {
        let tray_alive = tray_alive.clone();
        window.window().on_close_requested(move || {
            say(&format!(
                "закрытие окна: трей жив = {}",
                tray_alive.load(Ordering::SeqCst)
            ));
            if !tray_alive.load(Ordering::SeqCst) {
                let _ = slint::quit_event_loop();
            }
            slint::CloseRequestResponse::HideWindow
        });
    }

    // Цикл — до явного quit_event_loop (design D5): скрытие последнего
    // окна (в том числе в close-to-tray) цикл не завершает — семантика
    // run_event_loop_until_quit (slint: set_event_loop_quit_on_last_
    // window_closed(false)). Показ окна явный: until_quit не делает
    // этого за нас, в отличие от window.run().
    if let Err(e) = window.show() {
        say(&format!("не удалось показать окно: {e}"));
        return EXIT_FAILURE;
    }
    let event_loop = slint::run_event_loop_until_quit();
    drop(timer);

    // Разбор после цикла событий (закрытие окна или сигнал): остановка
    // сессии и join воркера со сторожем-страховкой — гарантия «код 0
    // ≤ 1 с» (design D5), как сторож headless. Отравленный замок не
    // отменяет разбор: данных связки достаточно и из отравленного
    // состояния (паника в коллбеке уже напечатана хуком), а выходить
    // здесь надо всегда — иначе сессия переживёт закрытие окна.
    let mut ui = binding
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(session) = ui.session.as_mut() {
        session.stop();
        let joined = Arc::new(AtomicBool::new(false));
        let watchdog = {
            let joined = joined.clone();
            std::thread::spawn(move || {
                std::thread::sleep(JOIN_DEADLINE);
                // Join завис — выходим напрямую, ресурсы освободит
                // процесс; код 0 даже в этой аномалии (спека).
                if !joined.load(Ordering::SeqCst) {
                    std::process::exit(0);
                }
            })
        };
        session.join();
        joined.store(true, Ordering::SeqCst);
        drop(watchdog); // поток отвязан: главный выход завершит его
    }

    match event_loop {
        Ok(()) => 0,
        Err(e) => {
            say(&format!("цикл событий завершён с ошибкой: {e}"));
            EXIT_FAILURE
        }
    }
}

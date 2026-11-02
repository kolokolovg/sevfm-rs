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
//! (без него — станция по умолчанию). Остановка: «Стоп», закрытие
//! окна, Ctrl+C (SIGINT) или SIGTERM — код 0 в течение секунды.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sevfm::cover::{CoverImage, CoverReply, Covers};
use sevfm::icy::TrackInfo;
use sevfm::net::{PlaybackStatus, playback_status};
use sevfm::session::{PlayerSession, SessionHandle, SessionOutcome};
use sevfm::ui::{PlayerView, TrackSlot, UiState, parse_args};
use sevfm::{redact_url, say};

slint::include_modules!();

/// Интервал опроса сессии (poll-таймер окна и цикл статусов headless) —
/// тот же ритм, что такт воркера сессии.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Код выхода при ошибке запуска тракта или сбое живого цикла
/// (спека: 0 — штатная остановка, 2 — неуспех).
const EXIT_FAILURE: i32 = 2;
/// Сторожевая граница join воркера после выхода из цикла событий:
/// закрытие окна и сигнал обязаны давать код 0 ≤ 1 с (спека player-ui).
const JOIN_DEADLINE: Duration = Duration::from_secs(1);

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
    let window = match PlayerWindow::new() {
        Ok(window) => window,
        Err(e) => {
            say(&format!("не удалось открыть окно: {e}"));
            return EXIT_FAILURE;
        }
    };
    window.set_status(UiState::Stopped.label().into());
    window.set_can_play(true);
    window.set_can_stop(false);
    // Заглушка обложки — дефолт свойства окна, прочитан до первого set
    // (design D7): переиспользуется как значение сброса для не-музыки,
    // промаха поиска и новой сессии.
    let placeholder_cover = window.get_cover();

    // «Играть»: новая сессия только из «Остановлено» — гейт can-play
    // отпускает кнопку по факту завершения воркера (design: teardown
    // ~100–300 мс, следующая сессия стартует только на мёртвом воркере —
    // без гонки за аудиоустройство); здесь защита от клика, опередившего
    // таймер.
    {
        let binding = binding.clone();
        let weak = window.as_weak();
        let placeholder_cover = placeholder_cover.clone();
        window.on_play(move || {
            let Some(window) = weak.upgrade() else { return };
            let Ok(mut ui) = binding.lock() else { return };
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
                    window.set_cover(placeholder_cover.clone());
                }
                Err(e) => {
                    window.set_error(format!("не удалось запустить поток чтения: {e}").into());
                }
            }
        });
    }

    // «Стоп»: флаг остановки сессии — воркер глушит вывод и освобождает
    // устройство за такт опроса, статус уходит в «Остановлено» по факту
    // завершения воркера (ближайший poll-такт).
    {
        let binding = binding.clone();
        window.on_stop(move || {
            if let Ok(ui) = binding.lock()
                && let Some(session) = ui.session.as_ref()
            {
                say("стоп: остановка сессии");
                session.stop();
            }
        });
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
        timer.start(slint::TimerMode::Repeated, POLL_INTERVAL, move || {
            // Сигнал до старта цикла терял quit_event_loop (Slint не
            // латчит запрос без цикла — NoEventLoopProvider): повторяем
            // выход по флагу из первого же тика.
            if process_stop.load(Ordering::SeqCst) {
                let _ = slint::quit_event_loop();
            }
            let Some(window) = weak.upgrade() else { return };
            let Ok(mut ui) = binding.lock() else { return };
            let Some(session) = ui.session.as_ref() else {
                return;
            };
            let tick = session.tick();
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
                window.set_artist(artist.into());
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

    let event_loop = window.run();
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

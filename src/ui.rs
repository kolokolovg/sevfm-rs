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

//! UI-логика без Slint (design D2 add-player-ui): маппинг состояний,
//! модель окна с дедупликацией по переходам и разбор аргументов —
//! обычные функции lib, тестируемые `cargo test` без окна. Слой
//! связки с окном живёт в main.rs; lib от Slint не зависит.

use crate::icy::TrackInfo;
use crate::net::{NetStatus, playback_status};
use crate::pipeline::DEFAULT_STREAM_URL;
use crate::session::{SessionOutcome, SessionTick};

/// Состояние плеера для окна (спека player-ui): «Остановлено» — нет
/// живой сессии; остальные состояния — метки [`crate::net::PlaybackStatus`]
/// живого тракта. `PlaybackStatus` не расширяется: «Остановлено» — не
/// состояние живого тракта (design D2).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum UiState {
    /// Нет живой сессии (до «Играть», после «Стопа», конца или ошибки).
    #[default]
    Stopped,
    Connecting,
    Playing,
    Buffering,
    Reconnecting,
}

impl UiState {
    /// Метка статуса для окна.
    pub fn label(self) -> &'static str {
        match self {
            UiState::Stopped => "Остановлено",
            UiState::Connecting => "Подключение",
            UiState::Playing => "Воспроизведение",
            UiState::Buffering => "Буферизация",
            UiState::Reconnecting => "Переподключение",
        }
    }

    /// Цветовой вид индикатора состояния окна (design D3
    /// redesign-hero-window): производная статуса без новой информации.
    /// Красного вида нет — окно выводит его из видимой ошибки.
    pub fn kind(self) -> StatusKind {
        match self {
            UiState::Playing => StatusKind::Playing,
            UiState::Connecting | UiState::Buffering | UiState::Reconnecting => StatusKind::Waiting,
            UiState::Stopped => StatusKind::Stopped,
        }
    }
}

/// Цвет точки индикатора состояния в окне (design D3
/// redesign-hero-window): lib не зависит от Slint, поэтому вид — свой
/// enum, main.rs отображает его на сгенерированный enum окна.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum StatusKind {
    /// «Воспроизведение» — зелёная точка.
    Playing,
    /// «Подключение», «Буферизация», «Переподключение» — янтарная.
    Waiting,
    /// «Остановлено» — серая.
    Stopped,
}

/// Чистый маппинг состояния окна (задача 2.1): мёртвый воркер —
/// «Остановлено» независимо от слотов (после ошибки и после конца
/// потока тракт «замирает» в последнем статусе); живой — сводка
/// сетевого статуса и буфера поверх `playback_status`.
pub fn ui_state(
    worker_alive: bool,
    net: NetStatus,
    playback_started: bool,
    underrun: bool,
) -> UiState {
    if worker_alive {
        playback_status(net, playback_started, underrun).into()
    } else {
        UiState::Stopped
    }
}

impl From<crate::net::PlaybackStatus> for UiState {
    fn from(status: crate::net::PlaybackStatus) -> Self {
        match status {
            crate::net::PlaybackStatus::Connecting => UiState::Connecting,
            crate::net::PlaybackStatus::Playing => UiState::Playing,
            crate::net::PlaybackStatus::Reconnecting => UiState::Reconnecting,
            crate::net::PlaybackStatus::Buffering => UiState::Buffering,
        }
    }
}

/// Изменения представления после poll: `Some` — поле изменилось и его
/// нужно записать в окно. Для станции `None` внутри `Some` — значение
/// стало пустым (станция не пришла или сброс на повторном старте).
#[derive(Default, PartialEq, Eq, Debug)]
pub struct ViewUpdate {
    pub station: Option<Option<String>>,
    pub track: Option<TrackSlot>,
    pub status: Option<UiState>,
    /// Сообщение фатальной ошибки сессии (однократно).
    pub error: Option<String>,
    /// Сырая смена эфира из потока (трек или вставка) — для диагностики
    /// в stderr, независимо от того, что показывается в окне (вставка в
    /// окне — заглушка, но в логе обязана быть видна).
    pub ether: Option<TrackInfo>,
}

/// Содержимое трек-строки окна после poll (правило чек-листа 4.3:
/// трек виден только при живой сессии): трек — исполнитель и название,
/// сырой `StreamTitle` — только название, во всех остальных случаях
/// (остановка, до первого метаблока, рекламная вставка) — заглушка:
/// название станции, без него — пустая строка. Будущая обложка следует
/// тому же правилу видимости (design D2 add-player-ui, future change).
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TrackSlot {
    Track { artist: String, title: String },
    Title { title: String },
    Placeholder { station: Option<String> },
}

/// Модель окна с дедупликацией по переходам (задача 2.2, эволюция
/// `NowPlayingPrinter`): poll снимка сессии отдаёт только изменения
/// станции, трека, статуса и ошибки относительно прошлого опроса.
/// При `Fatal` — строка ошибки, статус при этом — «Остановлено»
/// (мёртвый воркер, спека player-ui).
pub struct PlayerView {
    station: Option<String>,
    track: TrackSlot,
    /// Сырой эфир для отдельной дедупликации диагностики: вставка и
    /// заглушка «Остановлено» дают один слот, а в логе различимы.
    ether: Option<TrackInfo>,
    status: UiState,
    error_shown: bool,
}

impl Default for PlayerView {
    fn default() -> Self {
        Self::new()
    }
}

impl PlayerView {
    /// Модель с пустой предысторией: первый poll переиздаст всё.
    pub fn new() -> Self {
        Self {
            station: None,
            track: TrackSlot::Placeholder { station: None },
            ether: None,
            status: UiState::Stopped,
            error_shown: false,
        }
    }

    /// Сброс перед новой сессией: следующий poll переиздаст станцию,
    /// трек и статус заново (дедупликация не должна глотать первое
    /// появление значений новой сессии).
    pub fn reset(&mut self) {
        *self = Self::new();
    }

    /// Сравнивает снимок с предысторией и отдаёт только изменения.
    pub fn poll(&mut self, tick: &SessionTick) -> ViewUpdate {
        let status = ui_state(tick.alive, tick.net, tick.playback_started, tick.underrun);
        let track = track_slot(tick);
        let mut update = ViewUpdate::default();
        if tick.station != self.station {
            update.station = Some(tick.station.clone());
            self.station = tick.station.clone();
        }
        if track != self.track {
            update.track = Some(track.clone());
            self.track = track;
        }
        if tick.track != self.ether {
            update.ether = tick.track.clone();
            self.ether = tick.track.clone();
        }
        if status != self.status {
            update.status = Some(status);
            self.status = status;
        }
        // Ошибка — однократно на исход: исход остаётся в каждом снимке
        // мёртвой сессии, повторная выдача не нужна.
        if let Some(SessionOutcome::Fatal(message)) = &tick.outcome
            && !self.error_shown
        {
            update.error = Some(message.clone());
            self.error_shown = true;
        }
        update
    }
}

/// Содержимое трек-строки по снимку: мёртвая сессия, отсутствие трека
/// и рекламная вставка дают заглушку с названием станции (смена станции
/// обновляет и заглушку — сравнение слота по значению). Публично для
/// тултипа трея (design D2 add-tray): одна трек-строка для окна и трея.
pub fn track_slot(tick: &SessionTick) -> TrackSlot {
    match (&tick.track, tick.alive) {
        (Some(TrackInfo::Track { artist, title }), true) => TrackSlot::Track {
            artist: artist.clone(),
            title: title.clone(),
        },
        (Some(TrackInfo::Raw { title }), true) => TrackSlot::Title {
            title: title.clone(),
        },
        _ => TrackSlot::Placeholder {
            station: tick.station.clone(),
        },
    }
}

/// Режим запуска приложения (задача 2.3).
#[derive(Debug)]
pub struct Launch {
    /// Окно не открывается, воспроизведение сразу, вывод в stderr.
    pub headless: bool,
    /// Очистить кеш обложек и завершиться (флаг `--clear-cache`):
    /// утилитный запуск без окна и воспроизведения (design D5
    /// cover-cache-cleanup).
    pub clear_cache: bool,
    /// Адрес потока (по умолчанию — станция).
    pub url: String,
}

/// Разбор аргументов командной строки (design D6, без clap): флаг
/// `--headless` в любой позиции, первый позиционный аргумент — URL.
/// Ошибки — неизвестный флаг, повторный `--headless`, лишний
/// позиционный аргумент, некорректный URL: сообщение для stderr и код 2
/// до открытия окна. `args_os`-семантика сохранена: невалидный UTF-8 не
/// паникует, lossy-конверсии достаточно (URL с U+FFFD не парсится и
/// отклоняется здесь же).
pub fn parse_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> Result<Launch, String> {
    let mut headless = false;
    let mut clear_cache = false;
    let mut url = None;
    for arg in args {
        let text = arg.to_string_lossy();
        if text == "--headless" {
            if headless {
                return Err("флаг --headless указан дважды".into());
            }
            headless = true;
        } else if text == "--clear-cache" {
            if clear_cache {
                return Err("флаг --clear-cache указан дважды".into());
            }
            clear_cache = true;
        } else if text.starts_with('-') && text.len() > 1 {
            return Err(format!("неизвестный флаг: {text}"));
        } else if url.is_none() {
            url = Some(text.into_owned());
        } else {
            return Err("лишний позиционный аргумент — ожидается один URL".into());
        }
    }
    // `--clear-cache` — самостоятельный утилитный запуск (design D5
    // cover-cache-cleanup): сочетание молча игнорировало бы часть
    // аргументов — отклоняется целиком, кеш не трогается.
    if clear_cache && (headless || url.is_some()) {
        return Err("флаг --clear-cache не сочетается с другими аргументами".into());
    }
    let url = url.unwrap_or_else(|| DEFAULT_STREAM_URL.to_owned());
    validate_url(&url)?;
    Ok(Launch {
        headless,
        clear_cache,
        url,
    })
}

/// URL запуска обязан быть http/https-адресом с хостом (design D5
/// harden-network-resilience): синтаксически битый адрес сегодня был бы
/// сетевой ошибкой без классификации — вечные переподключения без выхода.
/// Парсер — `http::Uri`, тот же, которым ureq разберёт URL запроса:
/// отказ переносится с первой попытки соединения на старт. Дефолтный URL
/// станции проходит ту же проверку (самопроверка константы).
fn validate_url(url: &str) -> Result<(), String> {
    let parsed = url
        .parse::<http::Uri>()
        .map_err(|e| format!("некорректный URL {url}: {e}"))?;
    if parsed.scheme_str() != Some("http") && parsed.scheme_str() != Some("https") {
        return Err(format!(
            "некорректный URL {url}: нужна схема http:// или https://"
        ));
    }
    if parsed.host().is_none_or(str::is_empty) {
        return Err(format!("некорректный URL {url}: нет хоста"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Таблица всех комбинаций входов маппинга (задача 2.1): мёртвый
    /// воркер — всегда «Остановлено»; живой — сводка по playback_status,
    /// включая underrun при переподключении (приоритет Буферизации).
    #[test]
    fn ui_state_covers_all_input_combinations() {
        use NetStatus::{Connecting, Reconnecting, Streaming};

        // Отсутствие сессии (воркер мёртв) — «Остановлено» при любых
        // остаточных значениях слотов.
        for net in [Connecting, Streaming, Reconnecting] {
            for playback_started in [false, true] {
                for underrun in [false, true] {
                    assert_eq!(
                        ui_state(false, net, playback_started, underrun),
                        UiState::Stopped,
                        "мёртвый воркер: {net:?}/{playback_started}/{underrun}"
                    );
                }
            }
        }

        // Живой воркер: старт без звука — Подключение (ретраи первого
        // соединения — всё ещё подключение).
        assert_eq!(
            ui_state(true, Connecting, false, false),
            UiState::Connecting
        );
        assert_eq!(
            ui_state(true, Reconnecting, false, false),
            UiState::Connecting
        );
        assert_eq!(ui_state(true, Streaming, false, false), UiState::Connecting);
        // Underrun до старта звука невозможен, но маппинг детерминирован.
        assert_eq!(ui_state(true, Streaming, false, true), UiState::Connecting);

        // Игра: данные есть, соединение живо.
        assert_eq!(ui_state(true, Streaming, true, false), UiState::Playing);

        // Кратковременный обрыв: звук идёт из буфера.
        assert_eq!(
            ui_state(true, Reconnecting, true, false),
            UiState::Reconnecting
        );

        // Долгий обрыв: пауза приоритетна даже при вернувшейся сети;
        // underrun при переподключении — Буферизация.
        assert_eq!(ui_state(true, Reconnecting, true, true), UiState::Buffering);
        assert_eq!(ui_state(true, Streaming, true, true), UiState::Buffering);

        // Метки — строки спеки player-ui.
        assert_eq!(UiState::Stopped.label(), "Остановлено");
        assert_eq!(UiState::Connecting.label(), "Подключение");
        assert_eq!(UiState::Playing.label(), "Воспроизведение");
        assert_eq!(UiState::Buffering.label(), "Буферизация");
        assert_eq!(UiState::Reconnecting.label(), "Переподключение");
    }

    /// Конструктор снимка сессии для табличных тестов модели окна.
    fn tick(
        alive: bool,
        net: NetStatus,
        station: Option<&str>,
        track: Option<TrackInfo>,
        outcome: Option<SessionOutcome>,
    ) -> SessionTick {
        SessionTick {
            alive,
            net,
            playback_started: alive && net == NetStatus::Streaming,
            underrun: false,
            station: station.map(str::to_owned),
            track,
            outcome,
        }
    }

    fn track(artist: &str, title: &str) -> TrackInfo {
        TrackInfo::Track {
            artist: artist.to_owned(),
            title: title.to_owned(),
        }
    }

    fn insert(title: &str) -> TrackInfo {
        TrackInfo::Insert {
            title: title.to_owned(),
        }
    }

    fn raw(title: &str) -> TrackInfo {
        TrackInfo::Raw {
            title: title.to_owned(),
        }
    }

    fn placeholder(station: Option<&str>) -> TrackSlot {
        TrackSlot::Placeholder {
            station: station.map(str::to_owned),
        }
    }

    /// Индикатор состояния (redesign-hero-window D3): таблично все пять
    /// статусов — зелёный только у «Воспроизведения», янтарный у трёх
    /// «живых, но без гарантии звука», серый у «Остановлено».
    #[test]
    fn ui_state_kind_maps_all_statuses() {
        assert_eq!(UiState::Playing.kind(), StatusKind::Playing);
        assert_eq!(UiState::Connecting.kind(), StatusKind::Waiting);
        assert_eq!(UiState::Buffering.kind(), StatusKind::Waiting);
        assert_eq!(UiState::Reconnecting.kind(), StatusKind::Waiting);
        assert_eq!(UiState::Stopped.kind(), StatusKind::Stopped);
    }

    /// Дедупликация по переходам (задача 2.2 + правило заглушек 4.3):
    /// первый poll выдаёт всё, повторный на неизменном снимке — пуст,
    /// изменения — точечно. До первого метаблока трек-строка — заглушка
    /// с названием станции.
    #[test]
    fn player_view_emits_only_transitions() {
        let mut view = PlayerView::new();

        // Станция впервые + старт статуса: трека ещё нет — заглушка.
        let first = tick(
            true,
            NetStatus::Connecting,
            Some("Говорит Севастополь"),
            None,
            None,
        );
        let update = view.poll(&first);
        assert_eq!(update.station, Some(Some("Говорит Севастополь".into())));
        assert_eq!(update.track, Some(placeholder(Some("Говорит Севастополь"))));
        assert_eq!(update.status, Some(UiState::Connecting));
        assert_eq!(update.error, None);

        // Тот же снимок — без изменений.
        assert_eq!(view.poll(&first), ViewUpdate::default());

        // Первый трек — только трек-строка; статус — только статус.
        let playing = tick(
            true,
            NetStatus::Streaming,
            Some("Говорит Севастополь"),
            Some(track("ВЛАДИМИР ТРОШИН", "ЮНОСТЬ МОЯ")),
            None,
        );
        let update = view.poll(&playing);
        assert_eq!(update.station, None);
        assert_eq!(
            update.track,
            Some(TrackSlot::Track {
                artist: "ВЛАДИМИР ТРОШИН".into(),
                title: "ЮНОСТЬ МОЯ".into(),
            })
        );
        assert_eq!(update.status, Some(UiState::Playing));

        // Рекламная вставка — заглушка с названием станции, не название
        // вставки (правило чек-листа 4.3).
        let ad_break = tick(
            true,
            NetStatus::Streaming,
            Some("Говорит Севастополь"),
            Some(insert("Реклама")),
            None,
        );
        let update = view.poll(&ad_break);
        assert_eq!(update.track, Some(placeholder(Some("Говорит Севастополь"))));
        assert_eq!(update.status, None);

        // Сырой StreamTitle без JSON — название без исполнителя.
        let raw_tick = tick(
            true,
            NetStatus::Streaming,
            Some("Говорит Севастополь"),
            Some(raw("ВАЛАДАР — Простые слова")),
            None,
        );
        let update = view.poll(&raw_tick);
        assert_eq!(
            update.track,
            Some(TrackSlot::Title {
                title: "ВАЛАДАР — Простые слова".into()
            })
        );
    }

    /// Сырая смена эфира — отдельный канал диагностики (задача 5.2):
    /// вставка видна в `ether` даже тогда, когда слот трек-строки
    /// показывает заглушку; неизменный эфир не переиздаётся.
    #[test]
    fn player_view_reports_raw_ether_changes() {
        let mut view = PlayerView::new();
        let playing = tick(
            true,
            NetStatus::Streaming,
            Some("станция"),
            Some(track("А", "Б")),
            None,
        );
        assert_eq!(view.poll(&playing).ether, Some(track("А", "Б")));

        // Тот же трек — эфир молчит.
        assert_eq!(view.poll(&playing).ether, None);

        // Вставка: слот трек-строки — заглушка, эфир — сама вставка.
        let ad_break = tick(
            true,
            NetStatus::Streaming,
            Some("станция"),
            Some(insert("Реклама")),
            None,
        );
        let update = view.poll(&ad_break);
        assert_eq!(update.track, Some(placeholder(Some("станция"))));
        assert_eq!(update.ether, Some(insert("Реклама")));

        // Остановка: эфир мёртвой сессии не меняется — переиздания нет.
        let dead = tick(
            false,
            NetStatus::Streaming,
            Some("станция"),
            Some(insert("Реклама")),
            None,
        );
        assert_eq!(view.poll(&dead).ether, None);
    }

    /// Фатальная ошибка сессии: строка ошибки однократно, статус —
    /// «Остановлено» (мёртвый воркер), трек-строка — заглушка; ошибку
    /// не повторять в каждом опросе мёртвой сессии.
    #[test]
    fn player_view_reports_fatal_error_once_as_stopped() {
        let mut view = PlayerView::new();
        let playing = tick(
            true,
            NetStatus::Streaming,
            Some("станция"),
            Some(track("А", "Б")),
            None,
        );
        view.poll(&playing);

        let dead = tick(
            false,
            NetStatus::Streaming,
            Some("станция"),
            Some(track("А", "Б")),
            Some(SessionOutcome::Fatal(
                "поток не распознан как MP3: мусор".into(),
            )),
        );
        let update = view.poll(&dead);
        assert_eq!(update.status, Some(UiState::Stopped));
        // Последний трек не переживает остановку — заглушка-станция.
        assert_eq!(update.track, Some(placeholder(Some("станция"))));
        assert_eq!(
            update.error,
            Some("поток не распознан как MP3: мусор".into())
        );

        // Снимок мёртвой сессии не меняется — повторной выдачи ошибки нет.
        assert_eq!(view.poll(&dead), ViewUpdate::default());
    }

    /// Сброс на повторном старте: после reset первый poll переиздаёт
    /// станцию и трек-строку заново, дедупликация не глотает значения
    /// новой сессии.
    #[test]
    fn player_view_reset_reemits_everything_on_next_session() {
        let mut view = PlayerView::new();
        let playing = tick(
            true,
            NetStatus::Streaming,
            Some("станция"),
            Some(track("А", "Б")),
            None,
        );
        view.poll(&playing);

        view.reset();
        // Reset уже обнулил предысторию: свежая сессия без станции —
        // переходов нет (окно чистит связка при старте новой сессии),
        // статус уходит из «Остановлено».
        let fresh = tick(true, NetStatus::Connecting, None, None, None);
        let update = view.poll(&fresh);
        assert_eq!(update.station, None);
        assert_eq!(update.track, None);
        assert_eq!(update.status, Some(UiState::Connecting));

        // Станция той же строки новой сессии — выдана заново, в том
        // числе в заглушку трек-строки.
        let connected = tick(true, NetStatus::Streaming, Some("станция"), None, None);
        let update = view.poll(&connected);
        assert_eq!(update.station, Some(Some("станция".into())));
        assert_eq!(update.track, Some(placeholder(Some("станция"))));
    }

    /// Разбор аргументов (задача 2.3): все ветки — позиционный URL,
    /// `--headless` до и после URL, дефолт станции, неизвестный флаг,
    /// повторный `--headless`, лишний позиционный.
    #[test]
    fn parse_args_supports_headless_and_positional_url() {
        use std::ffi::OsString;

        let args = |parts: &[&str]| parts.iter().map(OsString::from).collect::<Vec<_>>();

        // Без аргументов — станция по умолчанию, окно.
        let launch = parse_args(args(&[])).expect("пустые аргументы валидны");
        assert!(!launch.headless);
        assert_eq!(launch.url, DEFAULT_STREAM_URL);

        // Только URL.
        let launch = parse_args(args(&["http://example.com/stream"])).expect("URL валиден");
        assert!(!launch.headless);
        assert_eq!(launch.url, "http://example.com/stream");

        // Только --headless — станция по умолчанию.
        let launch = parse_args(args(&["--headless"])).expect("флаг валиден");
        assert!(launch.headless);
        assert_eq!(launch.url, DEFAULT_STREAM_URL);

        // --headless до и после URL.
        for parts in [
            vec!["--headless", "http://example.com/stream"],
            vec!["http://example.com/stream", "--headless"],
        ] {
            let launch = parse_args(args(&parts)).expect("комбинация валидна");
            assert!(launch.headless, "{parts:?}");
            assert_eq!(launch.url, "http://example.com/stream");
        }

        // Неизвестный флаг — ошибка с сообщением.
        let err = parse_args(args(&["--verbose"])).expect_err("неизвестный флаг");
        assert!(err.contains("неизвестный флаг"), "{err}");
        assert!(err.contains("--verbose"), "{err}");

        // Повторный --headless и лишний позиционный — тоже ошибки.
        assert!(
            parse_args(args(&["--headless", "--headless"]))
                .expect_err("повторный флаг")
                .contains("дважды")
        );
        assert!(
            parse_args(args(&["a", "b"]))
                .expect_err("лишний аргумент")
                .contains("URL")
        );
    }

    /// `--clear-cache` (design D5 cover-cache-cleanup): самостоятельный
    /// утилитный запуск — флаг один проходит, повтор и сочетание с URL
    /// или `--headless` отклоняются; дефолтный URL проходит ту же
    /// самопроверку константы, но запуск до сети не доходит.
    #[test]
    fn clear_cache_flag_is_exclusive() {
        use std::ffi::OsString;

        let args = |parts: &[&str]| parts.iter().map(OsString::from).collect::<Vec<_>>();

        let launch = parse_args(args(&["--clear-cache"])).expect("флаг один валиден");
        assert!(launch.clear_cache);
        assert!(!launch.headless);

        // Обычный запуск флага не несёт.
        let plain = parse_args(args(&[])).expect("без флагов — обычный запуск");
        assert!(!plain.clear_cache);

        // Повтор и сочетания — ошибки с понятным сообщением.
        for parts in [
            vec!["--clear-cache", "--clear-cache"],
            vec!["--clear-cache", "http://example.com/stream"],
            vec!["http://example.com/stream", "--clear-cache"],
            vec!["--clear-cache", "--headless"],
            vec!["--headless", "--clear-cache"],
        ] {
            let err = parse_args(args(&parts)).expect_err("сочетание отклонено");
            assert!(err.contains("--clear-cache"), "{parts:?}: {err}");
        }
    }

    /// Валидация URL запуска (design D5 harden-network-resilience):
    /// таблица валидных и битых адресов + самопроверка дефолтного URL
    /// станции (включая `--headless` без позиционного).
    #[test]
    fn parse_args_validates_stream_url() {
        use std::ffi::OsString;

        let args = |parts: &[&str]| parts.iter().map(OsString::from).collect::<Vec<_>>();

        // Валидные: обе схемы, порт, путь, IPv6-хост.
        for url in [
            "http://example.com/stream",
            "https://sevfm.ru:9006/govsev",
            "http://127.0.0.1:8080",
            "http://[::1]:9006/stream",
        ] {
            let launch = parse_args(args(&[url])).unwrap_or_else(|e| panic!("{url}: {e}"));
            assert_eq!(launch.url, url);
        }

        // Битые: без схемы, чужая схема, без хоста, пусто, пробел,
        // «host:port» без // (схема не http/https).
        for url in [
            "example.com/stream",
            "ftp://example.com/x",
            "http://",
            "http://:9006/",
            "",
            "http://ho st/x",
            "sevfm.ru:9006/govsev",
        ] {
            let err = parse_args(args(&[url])).expect_err(&format!("битый URL принят: {url:?}"));
            assert!(err.contains("некорректный URL"), "{url:?}: {err}");
        }

        // Дефолтный URL станции валиден (в обоих режимах запуска).
        parse_args(args(&[])).expect("дефолтный URL валиден");
        parse_args(args(&["--headless"])).expect("дефолтный URL валиден в headless");
    }
}

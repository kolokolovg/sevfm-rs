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

//! Модель трея (design D2 add-tray): чистая сборка снапшота из такта
//! сессии — тултип значка, гейты пунктов меню и label переключателя
//! окна. Без Slint и D-Bus: unit-тесты не требуют ни окна, ни шины.
//! Поверхности-потребители (tray-компонент в main.rs, MPRIS-поток)
//! читают один и тот же снапшот из единственного poll-такта.

use crate::session::SessionTick;
use crate::ui::{TrackSlot, UiState, track_slot, ui_state};

/// Название приложения: Identity MPRIS и база тултипа трея.
pub const APP_NAME: &str = "Говорит Севастополь";

/// Снимок состояния для поверхностей вне окна: значок трея и MPRIS
/// (design D2). Видимость окна — часть снапшота: от неё зависит label
/// пункта «Показать/Скрыть окно», а источник правды о ней — само окно.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct TraySnapshot {
    /// Состояние плеера — те же метки, что в окне.
    pub state: UiState,
    /// Название станции из `icy-name`, начиная с первого соединения.
    pub station: Option<String>,
    /// Текущая трек-строка по правилу окна (трек/сырая строка/заглушка).
    pub track: TrackSlot,
    /// Окно видимо (ложь — скрыто в трей или ещё не показано).
    pub window_visible: bool,
}

impl TraySnapshot {
    /// Сборка из такта сессии: состояние и трек-строка переиспользуют
    /// маппинг окна (`ui_state`, `track_slot`), станция — как есть.
    /// `None` — сессии нет (до первого «Играть» или после «Стопа»):
    /// «Остановлено» без станции.
    pub fn from_tick(tick: Option<&SessionTick>, window_visible: bool) -> Self {
        match tick {
            Some(tick) => Self {
                state: ui_state(tick.alive, tick.net, tick.playback_started, tick.underrun),
                station: tick.station.clone(),
                track: track_slot(tick),
                window_visible,
            },
            None => Self {
                state: UiState::Stopped,
                station: None,
                track: TrackSlot::Placeholder { station: None },
                window_visible,
            },
        }
    }

    /// Доступность «Играть» — только из «Остановлено» (гейты кнопок
    /// окна, спека system-tray: трей повторяет управление окна).
    pub fn can_play(&self) -> bool {
        self.state == UiState::Stopped
    }

    /// Доступность «Стоп» — во всех состояниях активной сессии.
    pub fn can_stop(&self) -> bool {
        self.state != UiState::Stopped
    }

    /// Label переключателя окна в меню трея.
    pub fn window_label(&self) -> &'static str {
        if self.window_visible {
            "Скрыть окно"
        } else {
            "Показать окно"
        }
    }

    /// Тултип значка — ровно одна строка, без смешивания: играющий
    /// трек — «исполнитель — название» (сырой StreamTitle — только
    /// название); «Остановлено», вставка и ожидание первого метаблока —
    /// название станции. До первого соединения станции нет — тултип
    /// показывает название приложения, чтобы не быть пустым.
    /// Тултип значка — ровно одна строка, без смешивания: играющий
    /// трек — «исполнитель — название» (сырой StreamTitle — только
    /// название); «Остановлено», вставка и ожидание первого метаблока —
    /// название станции. До первого соединения станции нет — тултип
    /// показывает название приложения, чтобы не быть пустым. Строки эфира
    /// недоверенные — очищаются от управляющих/bidi-символов (находка
    /// аудита безопасности).
    pub fn tooltip(&self) -> String {
        match &self.track {
            TrackSlot::Track { artist, title } => format!(
                "{} — {}",
                crate::sanitize_display(artist),
                crate::sanitize_display(title)
            ),
            TrackSlot::Title { title } => crate::sanitize_display(title).into_owned(),
            TrackSlot::Placeholder { station } => station
                .as_deref()
                .map(crate::sanitize_display)
                .map(std::borrow::Cow::into_owned)
                .unwrap_or_else(|| APP_NAME.to_owned()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::icy::TrackInfo;
    use crate::net::NetStatus;

    /// Минимальный такт сессии для табличных тестов (как в ui.rs).
    fn tick(alive: bool, net: NetStatus, playback_started: bool) -> SessionTick {
        SessionTick {
            alive,
            net,
            playback_started,
            underrun: false,
            station: None,
            track: None,
            outcome: None,
        }
    }

    /// Маппинг такта покрывает все пять состояний (границы гейтов —
    /// то же, что у кнопок окна).
    #[test]
    fn from_tick_covers_all_states_and_gates() {
        let cases = [
            (false, NetStatus::Connecting, false, UiState::Stopped),
            (true, NetStatus::Connecting, false, UiState::Connecting),
            (true, NetStatus::Reconnecting, false, UiState::Connecting),
            (true, NetStatus::Streaming, false, UiState::Connecting),
            (true, NetStatus::Streaming, true, UiState::Playing),
            (true, NetStatus::Reconnecting, true, UiState::Reconnecting),
        ];
        for (alive, net, started, expected) in cases {
            let snap = TraySnapshot::from_tick(Some(&tick(alive, net, started)), true);
            assert_eq!(snap.state, expected, "{alive:?}/{net:?}/{started}");
            // Гейты: «Играть» только из «Остановлено», «Стоп» — из всех
            // активных состояний.
            assert_eq!(snap.can_play(), expected == UiState::Stopped);
            assert_eq!(snap.can_stop(), expected != UiState::Stopped);
        }

        // Без сессии — «Остановлено» без станции (до «Играть», после
        // «Стопа»).
        let snap = TraySnapshot::from_tick(None, false);
        assert_eq!(snap.state, UiState::Stopped);
        assert_eq!(snap.station, None);
        assert!(snap.can_play() && !snap.can_stop());
    }

    /// Тултип — одна строка без смешивания (правило ручного
    /// тестирования): трек — только трек, вставка и «Остановлено» —
    /// только станция, до соединения — название приложения.
    #[test]
    fn tooltip_is_track_or_station_never_both() {
        // До соединения станции нет — название приложения.
        let mut t = tick(false, NetStatus::Connecting, false);
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), true).tooltip(),
            "Говорит Севастополь"
        );

        // Живой трек — только исполнитель и название, станция рядом
        // не ставится (даже когда известна).
        t.alive = true;
        t.net = NetStatus::Streaming;
        t.playback_started = true;
        t.station = Some("Говорит Севастополь".into());
        t.track = Some(TrackInfo::Track {
            artist: "ПЕЛАГЕЯ".into(),
            title: "НЮРКИНА ПЕСНЯ".into(),
        });
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), true).tooltip(),
            "ПЕЛАГЕЯ — НЮРКИНА ПЕСНЯ"
        );

        // Сырой StreamTitle — только название.
        t.track = Some(TrackInfo::Raw {
            title: "Эфир".into(),
        });
        assert_eq!(TraySnapshot::from_tick(Some(&t), true).tooltip(), "Эфир");

        // Вставка — название станции, не название вставки.
        t.track = Some(TrackInfo::Insert {
            title: "Реклама".into(),
        });
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), true).tooltip(),
            "Говорит Севастополь"
        );

        // Чужая станция видна и в заглушке.
        t.station = Some("Радио Крыма".into());
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), true).tooltip(),
            "Радио Крыма"
        );

        // «Остановлено» — тоже станция.
        t.station = Some("Говорит Севастополь".into());
        t.alive = false;
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), true).tooltip(),
            "Говорит Севастополь"
        );
    }

    /// Переключатель окна: label зависит от видимости, станция ни при чём.
    #[test]
    fn window_label_follows_visibility() {
        let t = tick(false, NetStatus::Connecting, false);
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), true).window_label(),
            "Скрыть окно"
        );
        assert_eq!(
            TraySnapshot::from_tick(Some(&t), false).window_label(),
            "Показать окно"
        );
    }
}

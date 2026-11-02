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

//! Маппинг состояния плеера в MPRIS (design D4 add-tray): чистые
//! значения для D-Bus-сервиса — PlaybackStatus и трек-строка
//! метаданных из того же снапшота трея. Без zbus: unit-тесты не
//! требуют ни шины, ни окна.

use crate::tray::{APP_NAME, TraySnapshot};
use crate::ui::UiState;

/// PlaybackStatus MPRIS (спека mpris): живой поток не паузится и не
/// перематывается — любая активная сессия (подключение, эфир,
/// буферизация, переподключение) отдаётся Playing, «Остановлено» —
/// Stopped.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum MprisPlayback {
    Playing,
    Stopped,
}

impl MprisPlayback {
    /// Метка значения свойства PlaybackStatus.
    pub fn label(self) -> &'static str {
        match self {
            MprisPlayback::Playing => "Playing",
            MprisPlayback::Stopped => "Stopped",
        }
    }

    /// Из снапшота: активная сессия — Playing, «Остановлено» — Stopped.
    pub fn of(snapshot: &TraySnapshot) -> Self {
        if snapshot.state == UiState::Stopped {
            MprisPlayback::Stopped
        } else {
            MprisPlayback::Playing
        }
    }
}

/// Трек-строка метаданных MPRIS: живой музыкальный трек — исполнитель
/// и название; сырой `StreamTitle` — только название; вставка,
/// «Остановлено» и ожидание метаблока — заглушка-станция (без станции —
/// имя приложения), без исполнителя. Правило то же, что у трек-строки
/// окна (спека mpris: метаданные согласованы с окном).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct MprisTrack {
    pub artist: Option<String>,
    pub title: String,
}

impl MprisTrack {
    /// Из снапшота (трек-строка уже собрана по правилу окна). Строки
    /// эфира недоверенные — очищаются от управляющих/bidi-символов
    /// (находка аудита безопасности).
    pub fn of(snapshot: &TraySnapshot) -> Self {
        match &snapshot.track {
            crate::ui::TrackSlot::Track { artist, title } => Self {
                artist: Some(crate::sanitize_display(artist).into_owned()),
                title: crate::sanitize_display(title).into_owned(),
            },
            crate::ui::TrackSlot::Title { title } => Self {
                artist: None,
                title: crate::sanitize_display(title).into_owned(),
            },
            crate::ui::TrackSlot::Placeholder { station } => Self {
                artist: None,
                title: station
                    .as_deref()
                    .map(crate::sanitize_display)
                    .map(std::borrow::Cow::into_owned)
                    .unwrap_or_else(|| APP_NAME.to_owned()),
            },
        }
    }
}

/// `file://` URI для `mpris:artUrl` (находка аудита): имя файла — hex-ключ,
/// но базовый каталог из XDG/HOME может содержать пробелы, `%`, `#` и
/// не-ASCII — экранируется каждый байт пути вне незарезервированных
/// (RFC 3986: unreserved + разделитель сегментов `/`).
pub fn file_uri(path: &std::path::Path) -> String {
    let mut uri = String::from("file://");
    for byte in path.as_os_str().as_encoded_bytes() {
        let c = *byte as char;
        if c.is_ascii_alphanumeric() || matches!(c, '/' | '-' | '.' | '_' | '~') {
            uri.push(c);
        } else {
            uri.push_str(&format!("%{byte:02X}"));
        }
    }
    uri
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::icy::TrackInfo;
    use crate::net::NetStatus;
    use crate::session::SessionTick;

    fn snapshot(alive: bool, station: Option<&str>, track: Option<TrackInfo>) -> TraySnapshot {
        let tick = SessionTick {
            alive,
            net: if alive {
                NetStatus::Streaming
            } else {
                NetStatus::Connecting
            },
            playback_started: alive,
            underrun: false,
            station: station.map(str::to_owned),
            track,
            outcome: None,
        };
        TraySnapshot::from_tick(Some(&tick), true)
    }

    /// Активная сессия — Playing (включая буферизацию и переподключение),
    /// «Остановлено» — Stopped; метки — строки MPRIS.
    #[test]
    fn playback_status_maps_states() {
        assert_eq!(
            MprisPlayback::of(&snapshot(false, None, None)),
            MprisPlayback::Stopped
        );
        assert_eq!(
            MprisPlayback::of(&snapshot(true, Some("станция"), None)),
            MprisPlayback::Playing
        );
        assert_eq!(MprisPlayback::Stopped.label(), "Stopped");
        assert_eq!(MprisPlayback::Playing.label(), "Playing");
    }

    /// Метаданные: трек — исполнитель и название; сырой StreamTitle —
    /// название; вставка и «Остановлено» — заглушка-станция, без
    /// станции — имя приложения; исполнитель в заглушках опущен.
    #[test]
    fn track_follows_window_rule() {
        let track = snapshot(
            true,
            Some("Говорит Севастополь"),
            Some(TrackInfo::Track {
                artist: "ПЕЛАГЕЯ".into(),
                title: "НЮРКИНА ПЕСНЯ".into(),
            }),
        );
        assert_eq!(
            MprisTrack::of(&track),
            MprisTrack {
                artist: Some("ПЕЛАГЕЯ".into()),
                title: "НЮРКИНА ПЕСНЯ".into(),
            }
        );

        let raw = snapshot(
            true,
            Some("станция"),
            Some(TrackInfo::Raw {
                title: "Эфир".into(),
            }),
        );
        assert_eq!(
            MprisTrack::of(&raw),
            MprisTrack {
                artist: None,
                title: "Эфир".into(),
            }
        );

        let insert = snapshot(true, Some("Говорит Севастополь"), None);
        assert_eq!(
            MprisTrack::of(&insert),
            MprisTrack {
                artist: None,
                title: "Говорит Севастополь".into(),
            }
        );

        let no_station = snapshot(false, None, None);
        assert_eq!(MprisTrack::of(&no_station).title, "Говорит Севастополь");
    }

    /// file:// URI: hex-ключи идут как есть, пробелы, `%`, `#` и
    /// не-ASCII из базового каталога экранируются (находка аудита).
    #[test]
    fn file_uri_escapes_reserved_bytes() {
        use std::path::Path;
        assert_eq!(
            file_uri(Path::new("/home/u/.cache/sevfm/covers/a12f.jpg")),
            "file:///home/u/.cache/sevfm/covers/a12f.jpg"
        );
        assert_eq!(
            file_uri(Path::new("/home/my user/кеш/covers/a b.jpg")),
            "file:///home/my%20user/%D0%BA%D0%B5%D1%88/covers/a%20b.jpg"
        );
        assert_eq!(
            file_uri(Path::new("/tmp/100%#x/cover.jpg")),
            "file:///tmp/100%25%23x/cover.jpg"
        );
    }
}

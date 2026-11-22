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

//! Живой тест источника текстов (задача 5.1 add-lyrics-panel): реальный
//! LrcLib — известный трек, кириллический запрос и гарантированный
//! промах базы. Правило живых тестов проекта: каждый источник
//! прогоняется живьём до ревью; живая проба доступности из сети
//! пользователя выполнена 2026-11-20 (17/19, русский репертуар).
//!
//! Запуск: `SEVFM_LYRICS_LIVE=1 cargo test --test lyrics_live`.
//! Требует доступ к lrclib.net (без ключей); записи кеша не пишутся —
//! воркер собирается без каталога кеша.

use std::time::{Duration, Instant};

use sevfm::lyrics::{Lyrics, LyricsReply};

/// Живой режим включён переменной окружения.
fn live() -> bool {
    match std::env::var_os("SEVFM_LYRICS_LIVE") {
        Some(value) => value != "0" && !value.is_empty(),
        None => false,
    }
}

/// Ждёт ответ воркера с таймаутом — ответы асинхронные.
fn await_reply(lyrics: &Lyrics, what: &str) -> LyricsReply {
    let deadline = Instant::now() + Duration::from_secs(20);
    while Instant::now() < deadline {
        if let Some(reply) = lyrics.poll_reply() {
            return reply;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("ответ воркера не пришёл: {what}");
}

/// Известный трек с живой пробы (Кино — «Группа крови», 2026-11-20):
/// полный конвейер — запрос с URL-кодированием кириллицы, разбор
/// plainLyrics, источник LrcLib.
#[test]
fn live_find_known_track() {
    if !live() {
        eprintln!("пропущено: задайте SEVFM_LYRICS_LIVE=1");
        return;
    }
    let lyrics = Lyrics::without_cache();
    lyrics.track_changed(Some(("Кино", "Группа крови")));
    assert!(
        matches!(
            await_reply(&lyrics, "Группа крови"),
            LyricsReply::Found {
                source: Some(_),
                ..
            }
        ),
        "известный трек обязан найтись"
    );
}

/// Кириллический запрос с живой пробы (Сплин — «Выхода нет»): параметры
/// кодируются, текст возвращается.
#[test]
fn live_find_cyrillic_track() {
    if !live() {
        eprintln!("пропущено: задайте SEVFM_LYRICS_LIVE=1");
        return;
    }
    let lyrics = Lyrics::without_cache();
    lyrics.track_changed(Some(("Сплин", "Выхода нет")));
    assert!(
        matches!(
            await_reply(&lyrics, "Выхода нет"),
            LyricsReply::Found { .. }
        ),
        "кириллический запрос обязан найтись"
    );
}

/// Гарантированный промах базы с живой пробы (Баста — «Сансара»,
/// 2026-11-20: отсутствует целиком, search тоже пуст): штатный
/// Missing, не авария (спека lyrics «Промах источника»).
#[test]
fn live_miss_confirmed_absent_track() {
    if !live() {
        eprintln!("пропущено: задайте SEVFM_LYRICS_LIVE=1");
        return;
    }
    let lyrics = Lyrics::without_cache();
    lyrics.track_changed(Some(("Баста", "Сансара")));
    assert_eq!(
        await_reply(&lyrics, "Сансара"),
        LyricsReply::Missing,
        "отсутствующий в базе трек — штатный промах"
    );
}

/// Дуэт с приёмки 5.3 (2026-11-22): точный запрос промахнулся бы —
/// запись в базе под другим написанием артистов («Олег Анофриев /
/// Анатолий Горохов») и с чистым названием; спасает fallback поиска
/// (спека lyrics «Дуэт с другим написанием в базе»).
#[test]
fn live_fallback_finds_duet_under_other_spelling() {
    if !live() {
        eprintln!("пропущено: задайте SEVFM_LYRICS_LIVE=1");
        return;
    }
    let lyrics = Lyrics::without_cache();
    lyrics.track_changed(Some((
        "АНАТОЛИЙ ГОРОХОВ;ОЛЕГ АНОФРИЕВ",
        "Песня друзей (м,ф Бременские музыканты)",
    )));
    assert!(
        matches!(
            await_reply(&lyrics, "Песня друзей через fallback"),
            LyricsReply::Found { .. }
        ),
        "дуэт с иным написанием в базе обязан найтись fallback'ом"
    );
}

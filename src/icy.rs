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

//! ICY-метаданные радиопотока: кадрирование по `icy-metaint` (вырезание
//! метаблоков из байтового потока до буфера тракта — декодер получает
//! чистый MP3) и разбор поля `StreamTitle` по правилам спеки icy-metadata
//! (JSON с исполнителем/названием, рекламные вставки, сырая строка).

use std::borrow::Cow;
use std::sync::{Arc, Mutex};

/// Вес одного шага размерного байта: блок растёт по 16 байтов (спека
/// icy-metadata).
const META_UNIT_BYTES: usize = 16;
/// Максимум содержимого метаблока: размерный байт 255 × 16 байтов.
/// Нужен только тестам: автомат честно читает размерный байт, `min`
/// на горячем пути избыточен (255 × 16 — потолок типа и так).
#[cfg(test)]
const META_MAX_BYTES: usize = 255 * META_UNIT_BYTES;

/// Кадр на выходе кадрировщика: участок аудио или целый метаблок.
pub(crate) enum Frame<'a> {
    /// Аудиобайты — срез входного чанка, идут в буфер тракта.
    Audio(&'a [u8]),
    /// Метаблок целиком (без размерного байта, с паддингом) — в парсер.
    Meta(Vec<u8>),
}

/// Состояние автомата кадрирования.
#[derive(Clone, Copy)]
enum FramerState {
    /// Осталось байтов аудио до следующего размерного байта.
    Audio(usize),
    /// Ждём размерный байт метаблока.
    LenByte,
    /// Читаем содержимое метаблока: осталось байтов до конца блока.
    Meta(usize),
}

/// Кадрировщик ICY-потока одного соединения (design D2): делит входные
/// байты на аудиоучастки длиной `metaint` и метаблоки между ними.
/// `metaint == None` — сервер без ICY: проход тождественный, всякий чанк
/// целиком аудио.
pub(crate) struct IcyFramer {
    /// Интервал из `icy-metaint`; None — договорённости нет.
    metaint: Option<usize>,
    state: FramerState,
    /// Накопитель неполного метаблока (блок на границе чанков чтения).
    /// Аллокация на каждый непустой метаблок (`mem::take` отдаёт capacity
    /// вместе с блоком; ≈ раз в секунду на реальном радио); на аудиопути
    /// аллокаций нет вовсе.
    meta_buf: Vec<u8>,
}

impl IcyFramer {
    /// Кадрировщик установленного соединения.
    pub(crate) fn new(metaint: Option<usize>) -> Self {
        Self {
            metaint,
            state: FramerState::Audio(metaint.filter(|&m| m > 0).unwrap_or(usize::MAX)),
            meta_buf: Vec::new(),
        }
    }

    /// Прогоняет чанк через автомат: аудиоучастки и целые метаблоки
    /// передаются в `emit` по мере разбора. Блок, разрезанный границей
    /// чанков, накапливается и выдаётся целиком; обрыв посреди блока
    /// теряется вместе с соединением (кадрировщик живёт одно соединение,
    /// новое начинается с заголовков заново).
    pub(crate) fn push<'a>(&mut self, chunk: &'a [u8], emit: &mut dyn FnMut(Frame<'a>)) {
        let Some(metaint) = self.metaint.filter(|&m| m > 0) else {
            // Сервер без ICY: весь чанк — аудио, автомат не работает.
            emit(Frame::Audio(chunk));
            return;
        };
        let mut rest = chunk;
        while !rest.is_empty() {
            match self.state {
                FramerState::Audio(mut remaining) => {
                    let take = rest.len().min(remaining);
                    let (audio, tail) = rest.split_at(take);
                    emit(Frame::Audio(audio));
                    rest = tail;
                    remaining -= take;
                    self.state = if remaining == 0 {
                        FramerState::LenByte
                    } else {
                        FramerState::Audio(remaining)
                    };
                }
                FramerState::LenByte => {
                    let block_len = usize::from(rest[0]) * META_UNIT_BYTES;
                    rest = &rest[1..];
                    self.state = if block_len == 0 {
                        // Пустой метаблок: сразу следующий аудиоучасток.
                        FramerState::Audio(metaint)
                    } else {
                        self.meta_buf.reserve(block_len);
                        FramerState::Meta(block_len)
                    };
                }
                FramerState::Meta(mut remaining) => {
                    let take = rest.len().min(remaining);
                    let (part, tail) = rest.split_at(take);
                    self.meta_buf.extend_from_slice(part);
                    rest = tail;
                    remaining -= take;
                    if remaining == 0 {
                        let block = std::mem::take(&mut self.meta_buf);
                        self.state = FramerState::Audio(metaint);
                        emit(Frame::Meta(block));
                    } else {
                        self.state = FramerState::Meta(remaining);
                    }
                }
            }
        }
    }
}

/// Разобранный трек из `StreamTitle`. Вставка (реклама/новости)
/// отличается от трека: окно показывает её заглушкой, а не названием
/// (спека player-ui — правило «трек виден только при воспроизведении»);
/// stderr-представление у обоих — название.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum TrackInfo {
    /// Трек: исполнитель и название.
    Track { artist: String, title: String },
    /// Вставка (реклама/новости): пустой исполнитель или тип ≠ «М».
    Insert { title: String },
    /// `StreamTitle` без распознанного JSON: вся строка — название,
    /// показывается как трек без исполнителя.
    Raw { title: String },
}

impl TrackInfo {
    /// Название (у всех видов трека).
    pub fn title(&self) -> &str {
        match self {
            TrackInfo::Track { title, .. }
            | TrackInfo::Insert { title }
            | TrackInfo::Raw { title } => title,
        }
    }

    /// Строка для пользователя (stderr): «АРТИСТ — НАЗВАНИЕ» или просто
    /// название.
    pub fn display(&self) -> String {
        match self {
            TrackInfo::Track { artist, title } => format!("{artist} — {title}"),
            TrackInfo::Insert { title } | TrackInfo::Raw { title } => title.clone(),
        }
    }
}

/// Станция и текущий трек — разделяемое представление эфира. Пишет
/// только reader-поток, читает потребитель (CLI в этой фазе, UI позже).
#[derive(Default)]
pub struct NowPlaying {
    /// Название станции (`icy-name`), начиная с первого соединения, где
    /// оно пришло.
    pub station: Option<String>,
    /// Текущий трек из последнего распознанного метаблока.
    pub track: Option<TrackInfo>,
}

/// Разделяемый слот эфира (паттерн `SharedNetStatus`).
pub type SharedNowPlaying = Arc<Mutex<NowPlaying>>;

/// Разбирает содержимое метаблока (без размерного байта) по правилам
/// спеки icy-metadata: JSON с `artist`/`name` — трек; пустой `artist`
/// или `type` ≠ «М» — вставка (только название); невалидный JSON или
/// валидный JSON-не-объект (скаляр/массив) — вся строка как название.
/// Нет `StreamTitle`, нет закрывающей кавычки, нет названия — None,
/// блок игнорируется.
pub(crate) fn parse_meta(block: &[u8]) -> Option<TrackInfo> {
    let value = stream_title_value(block)?;
    let unescaped = unescape_doubled_quotes(value);
    let raw = String::from_utf8_lossy(&unescaped);
    if raw.is_empty() {
        return None;
    }
    let track = serde_json::from_str::<serde_json::Value>(&raw).ok();
    // JSON-не-объект (число, булево, массив) — по спеке сырая строка
    // как название, как при провале разбора.
    match track.filter(|json| json.is_object()) {
        Some(json) => track_from_json(&json),
        None => Some(TrackInfo::Raw {
            title: raw.into_owned(),
        }),
    }
}

/// Трек из распознанного JSON: правила вставок спеки icy-metadata.
/// Нет непустого `name` — блок игнорируется (нет названия — нечего
/// показывать).
fn track_from_json(json: &serde_json::Value) -> Option<TrackInfo> {
    let name = json
        .get("name")
        .and_then(serde_json::Value::as_str)
        .filter(|name| !name.is_empty())?;
    let artist = json
        .get("artist")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default();
    let kind = json.get("type").and_then(serde_json::Value::as_str);
    // Вставка (реклама/новости): пустой исполнитель или тип ≠ «М» —
    // показывается только название, треком не считается. Кириллическая
    // «М» — по спеке icy-metadata.
    const TRACK_TYPE_MUSIC: &str = "М";
    let is_track = !artist.is_empty() && kind.is_none_or(|k| k == TRACK_TYPE_MUSIC);
    Some(if is_track {
        TrackInfo::Track {
            artist: artist.to_owned(),
            title: name.to_owned(),
        }
    } else {
        TrackInfo::Insert {
            title: name.to_owned(),
        }
    })
}

/// Значение `StreamTitle='…'` из метаблока: байтовый скан до закрывающей
/// кавычки; `''` внутри — экранированная кавычка, скан перескакивает её.
/// Поля или закрывающей кавычки нет (обрезанный блок) — None.
fn stream_title_value(block: &[u8]) -> Option<&[u8]> {
    const PREFIX: &[u8] = b"StreamTitle='";
    let start = block
        .windows(PREFIX.len())
        .position(|window| window == PREFIX)?
        + PREFIX.len();
    let mut i = start;
    while i < block.len() {
        if block[i] == b'\'' {
            if block.get(i + 1) == Some(&b'\'') {
                i += 2;
            } else {
                return Some(&block[start..i]);
            }
        } else {
            i += 1;
        }
    }
    None
}

/// Заменяет экранированные кавычки `''` на одну `'`.
fn unescape_doubled_quotes(value: &[u8]) -> Cow<'_, [u8]> {
    if !value.contains(&b'\'') {
        return Cow::Borrowed(value);
    }
    let mut out = Vec::with_capacity(value.len());
    let mut i = 0;
    while i < value.len() {
        if value[i] == b'\'' && value.get(i + 1) == Some(&b'\'') {
            out.push(b'\'');
            i += 2;
        } else {
            out.push(value[i]);
            i += 1;
        }
    }
    Cow::Owned(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Сборка кадрированных выходов для таблиц фреймера.
    fn collect(framer: &mut IcyFramer, chunks: &[&[u8]]) -> (Vec<u8>, Vec<Vec<u8>>) {
        let mut audio = Vec::new();
        let mut metas = Vec::new();
        for chunk in chunks {
            framer.push(chunk, &mut |frame| match frame {
                Frame::Audio(seg) => audio.extend_from_slice(seg),
                Frame::Meta(block) => metas.push(block),
            });
        }
        (audio, metas)
    }

    /// Метаблок с StreamTitle (содержимое выровнено кратно 16 байтов,
    /// как шлёт сервер).
    fn meta_block(title: &str) -> Vec<u8> {
        let mut content = format!("StreamTitle='{title}';").into_bytes();
        while content.len() % 16 != 0 {
            content.push(0);
        }
        let mut block = vec![u8::try_from(content.len() / 16).expect(" ≤ 4080 байтов")];
        block.extend_from_slice(&content);
        block
    }

    /// Аудио и метаблоки чередуются по metaint; блок внутри одного чанка.
    #[test]
    fn framer_splits_audio_and_meta_blocks() {
        let metaint = 4;
        let block = meta_block("Реклама");
        let mut stream = Vec::new();
        for i in 0..3u8 {
            stream.extend_from_slice(&[i; 4]);
            stream.extend_from_slice(&block);
        }
        let mut framer = IcyFramer::new(Some(metaint));
        let (audio, metas) = collect(&mut framer, &[&stream]);
        let mut expected_audio = Vec::new();
        for i in 0..3u8 {
            expected_audio.extend_from_slice(&[i; 4]);
        }
        assert_eq!(audio, expected_audio);
        assert_eq!(metas.len(), 3);
        assert_eq!(metas[0], block[1..]);
    }

    /// Блок разрезан границей чанков чтения — собирается из частей.
    #[test]
    fn meta_block_across_chunk_boundary() {
        let metaint = 4;
        let block = meta_block("ЮНОСТЬ МОЯ");
        let mut stream = vec![7u8; 4];
        stream.extend_from_slice(&block);
        // Разрез на 2 байта: блок дробится на несколько чанков.
        let chunks: Vec<&[u8]> = stream.chunks(2).collect();
        let mut framer = IcyFramer::new(Some(metaint));
        let (audio, metas) = collect(&mut framer, &chunks);
        assert_eq!(audio, vec![7u8; 4]);
        assert_eq!(metas, vec![block[1..].to_vec()]);
    }

    /// Размерный байт 0 — пустой метаблок (сам байт — не аудио),
    /// кадрирование продолжается. Хвост аудио короче интервала: после
    /// полного интервала следующий байт трактуется как размерный.
    #[test]
    fn empty_meta_block_is_skipped() {
        let stream = [1u8, 2, 3, 4, 0, 5, 6, 7];
        let mut framer = IcyFramer::new(Some(4));
        let (audio, metas) = collect(&mut framer, &[&stream]);
        assert_eq!(audio, [1u8, 2, 3, 4, 5, 6, 7]);
        assert!(metas.is_empty());
    }

    /// Без договорённости (сервер без ICY) — тождественный проход.
    #[test]
    fn passthrough_without_metaint() {
        let stream = [9u8, 8, 7, 6];
        let mut framer = IcyFramer::new(None);
        let (audio, metas) = collect(&mut framer, &[&stream]);
        assert_eq!(audio, stream);
        assert!(metas.is_empty());
    }

    /// Обрыв посреди метаблока: аудио до блока отдано, блок не выдан
    /// (умирает вместе с соединением).
    #[test]
    fn truncated_meta_block_is_dropped() {
        let block = meta_block("обрезанный");
        let mut stream = vec![3u8; 4];
        stream.extend_from_slice(&block[..block.len() / 2]);
        let mut framer = IcyFramer::new(Some(4));
        let (audio, metas) = collect(&mut framer, &[&stream]);
        assert_eq!(audio, vec![3u8; 4]);
        assert!(metas.is_empty());
    }

    /// Таблица StreamTitle: JSON-трек, строка, обрезанный JSON, вставки,
    /// экранирование, мусор (design D3, задачи 1.2; правило заглушек
    /// player-ui — вставка различается с треком и сырой строкой).
    #[test]
    fn parse_meta_table() {
        let track = |artist: &str, title: &str| TrackInfo::Track {
            artist: artist.to_owned(),
            title: title.to_owned(),
        };
        let insert = |title: &str| TrackInfo::Insert {
            title: title.to_owned(),
        };
        let raw = |title: &str| TrackInfo::Raw {
            title: title.to_owned(),
        };

        // JSON с исполнителем и названием.
        assert_eq!(
            parse_meta(&meta_block(
                "{\"artist\":\"ВЛАДИМИР ТРОШИН\",\"name\":\"ЮНОСТЬ МОЯ\"}"
            )),
            Some(track("ВЛАДИМИР ТРОШИН", "ЮНОСТЬ МОЯ"))
        );
        // JSON с type «М» — музыкальный трек.
        assert_eq!(
            parse_meta(&meta_block(
                "{\"artist\":\"ИСПОЛНИТЕЛЬ\",\"name\":\"ТРЕК\",\"type\":\"М\"}"
            )),
            Some(track("ИСПОЛНИТЕЛЬ", "ТРЕК"))
        );
        // Вставка: пустой artist, type «Д».
        assert_eq!(
            parse_meta(&meta_block(
                "{\"artist\":\"\",\"duration\":\"\",\"name\":\"Говорит Севастополь\",\"type\":\"Д\"}"
            )),
            Some(insert("Говорит Севастополь"))
        );
        // Вставка по одному type ≠ «М» (artist непустой).
        assert_eq!(
            parse_meta(&meta_block(
                "{\"artist\":\"Х\",\"name\":\"НОВОСТИ\",\"type\":\"Н\"}"
            )),
            Some(insert("НОВОСТИ"))
        );
        // Без JSON — вся строка как название.
        assert_eq!(parse_meta(&meta_block("Реклама")), Some(raw("Реклама")));
        // Обрезанный JSON — сырая строка, без краха.
        assert_eq!(
            parse_meta(&meta_block("{\"artist\":\"ВЛАДИМИР")),
            Some(raw("{\"artist\":\"ВЛАДИМИР"))
        );
        // Экранированная кавычка '' внутри значения.
        assert_eq!(
            parse_meta(&meta_block("It''s fine")),
            Some(raw("It's fine"))
        );
        // Валидный JSON-скаляр — не объект: сырая строка как название
        // (спека icy-metadata).
        assert_eq!(parse_meta(&meta_block("42")), Some(raw("42")));
        assert_eq!(parse_meta(&meta_block("true")), Some(raw("true")));
        // Пустой StreamTitle — нечего показывать.
        assert_eq!(parse_meta(&meta_block("")), None);
        // Мусорный блок без StreamTitle и JSON без name — игнор.
        assert_eq!(parse_meta(&[0u8; 32]), None);
        assert_eq!(
            parse_meta(&meta_block("{\"artist\":\"БЕЗ НАЗВАНИЯ\"}")),
            None
        );
    }
}

#[cfg(test)]
mod proptests {
    use proptest::prelude::*;

    use super::{Frame, IcyFramer, META_MAX_BYTES, parse_meta};

    /// Инвариант кадрировщика (design D2): при произвольных аудиобайтах,
    /// метаблоках и разрезах потока на чанки чтения аудио восстанавливается
    /// байт-в-байт, метаблоки выдаются целиком.
    #[test]
    fn framer_roundtrip_any_chunking() {
        proptest!(|(
            audio in prop::collection::vec(any::<u8>(), 0..1024),
            metaint in 1usize..64,
            // Блоки непустые: после полного интервала размерный байт
            // обязан быть, а нулевой (пустой блок 0×16) не эмитит
            // ничего — его покрывает юнит-тест.
            payloads in prop::collection::vec(prop::collection::vec(any::<u8>(), 1..48), 1..8),
            split in 1usize..7,
        )| {
            // Поток: аудио кусками metaint, после каждого ПОЛНОГО куска —
            // метаблок (содержимое, выровненное кратно 16, с размерным
            // байтом). Неполный последний кусок блока не получает: блок
            // следует строго после metaint аудиобайтов.
            let mut stream = Vec::new();
            let mut expected_metas = Vec::new();
            let mut block_index = 0usize;
            for piece in audio.chunks(metaint) {
                stream.extend_from_slice(piece);
                if piece.len() < metaint {
                    continue;
                }
                let payload = &payloads[block_index % payloads.len()];
                block_index += 1;
                let mut content = payload.clone();
                while content.len() % 16 != 0 {
                    content.push(0);
                }
                content.truncate(META_MAX_BYTES);
                stream.push(u8::try_from(content.len() / 16).unwrap());
                stream.extend_from_slice(&content);
                expected_metas.push(content);
            }

            let mut framer = IcyFramer::new(Some(metaint));
            let mut got_audio = Vec::new();
            let mut got_metas = Vec::new();
            for chunk in stream.chunks(split) {
                framer.push(chunk, &mut |frame| match frame {
                    Frame::Audio(seg) => got_audio.extend_from_slice(seg),
                    Frame::Meta(block) => got_metas.push(block),
                });
            }
            prop_assert_eq!(got_audio, audio);
            prop_assert_eq!(got_metas, expected_metas);
        });
    }

    /// Парсер не паникует на произвольных байтах блока (недоверенные
    /// данные из сети).
    #[test]
    fn parse_meta_never_panics() {
        proptest!(|(bytes in prop::collection::vec(any::<u8>(), 0..META_MAX_BYTES))| {
            let _ = parse_meta(&bytes);
        });
    }
}

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

//! Тексты песен (change add-lyrics-panel): единственный источник LrcLib —
//! открытый API без ключей, покрывший 17 из 19 песен живой пробы
//! русского репертуара (2026-11-20); дисковый кеш повторных треков и
//! фоновый воркер-поток, не мешающий тракту и окну.
//!
//! Только plain-текст: синхронизация строки по времени эфира не делается
//! (решение пользователя). Промах источника (404, пустой plainLyrics,
//! инструментальная композиция) — штатный исход «текста нет», не ошибка;
//! сбой сети или мусорный ответ — запись в лог и тот же исход. Не-музыка
//! (вставка, сырой StreamTitle, остановка) в сеть не ходит вовсе.
//! Паттерны транспорта, кеша и воркера перенесены из `cover.rs` — там
//! они прошли ревью; отличия: один источник без цепочки и без токенов,
//! лимит кеша в десять раз меньше (текст на два порядка легче картинки).

use std::io::Read;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, SystemTime};

use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::say;

/// Таймауты HTTP источника текстов (design D2): как у сервисов обложек —
/// соединение, заголовки и каждое чтение тела 5 с, общий бюджет тела
/// 15 с, резолв ограничен бюджетом.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const BODY_TIMEOUT: Duration = Duration::from_secs(15);
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);
/// Лимит тела точного ответа (design D2): живые ответы 13–17 КиБ (замер
/// 2026-11-20); 1 МиБ — запас на два порядка. Гигантское или враждебное
/// тело обрывается лимитом, а не скоростью канала.
const EXACT_BODY_LIMIT_BYTES: u64 = 1024 * 1024;
/// Лимит тела поискового ответа (design D13; находка hunter-ревью):
/// массив до ~20 записей несёт вместе с plainLyrics и неиспользуемые
/// syncedLyrics — живые пробы ~50 КиБ, длинные треки сотни КиБ. Единый
/// с точным запросом лимит называл большой честный ответ «мусором» и
/// молча терял тексты длинных песен; 4 МиБ — запас свыше десятка раз.
const SEARCH_BODY_LIMIT_BYTES: u64 = 4 * 1024 * 1024;
/// User-Agent запросов к источнику текстов (design D2).
const USER_AGENT: &str = concat!("sevfm/", env!("CARGO_PKG_VERSION"));
/// Базовый URL API LrcLib (design D1): один источник, эндпоинт точного
/// запроса. Ответ ссылок не содержит — допуска URL-хостов не требуется.
const LRCLIB_API_URL: &str = "https://lrclib.net/api/get";
/// URL поиска LrcLib (design D13): fallback точного запроса для дуэтов
/// и саундтреков с иным написанием артистов и пометками в титуле.
const LRCLIB_SEARCH_URL: &str = "https://lrclib.net/api/search";
/// Возраст осиротевшего временного файла кеша (симметрия cover.rs).
const STALE_TEMP_AGE: Duration = Duration::from_secs(3600);
/// Лимит суммарного размера кеша текстов (design D3): медиана записи
/// живой пробы ~15 КиБ; 10 МиБ ≈ сотни уникальных треков ротации.
const CACHE_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
/// Порог прироста записанных текстов для повторной проверки лимита
/// (design D3): масштаб 1/10 от обложечных 8 МиБ — скан каталога
/// не чаще раза в день прослушивания.
const CACHE_RECHECK_DELTA_BYTES: u64 = 512 * 1024;
/// Предел размера sidecar источника (design D3): машинное имя одного
/// источника; больший файл — не наша запись (симметрия cover.rs).
const SIDECAR_LIMIT_BYTES: u64 = 32;

/// Ошибка обращения к источнику текстов: та же таксономия, что у
/// обложек — «недоступен» и «вернул мусор» различаются в логе.
#[derive(Debug, thiserror::Error)]
enum LyricsError {
    /// Сеть, TLS, HTTP-статус.
    #[error("{0}")]
    Network(String),
    /// Ответ не разобран.
    #[error("{0}")]
    BadResponse(String),
}

impl LyricsError {
    /// Шаг конвейера в сообщении: сетевых шагов два (точный и поисковый),
    /// без контекста их сбои в логе неотличимы (симметрия cover.rs
    /// with_context — находка hunter-ревью).
    fn context(self, step: &str) -> Self {
        match self {
            Self::Network(message) => Self::Network(format!("{step}: {message}")),
            Self::BadResponse(message) => Self::BadResponse(format!("{step}: {message}")),
        }
    }
}

/// Транспорт HTTP источника текстов (design D2): один запрос на трек,
/// ключей авторизации нет. Трейт существует ради fake-транспорта в
/// юнит-тестах; продакшн — [`UreqLyrics`]. Ответ несёт статус: 404 —
/// штатный промах, не ошибка (спека lyrics «Поиск текста по треку»).
trait LyricsHttp: Send {
    /// Ответ источника: статус и тело с переданным лимитом чтения.
    fn get(&self, url: &str, limit_bytes: u64) -> Result<LyricsResponse, LyricsError>;
}

/// Ответ транспорта: статус и тело (дизайн D1 — статус различает промах
/// и сбой: 404 у единственного источника — «текста нет», не авария).
struct LyricsResponse {
    status: u16,
    body: String,
}

/// Источник текста (design D4): сейчас единственный LrcLib. Машинное
/// имя живёт в sidecar-файле кеша и переживает смену формулировок лога.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum LyricsSource {
    LrcLib,
}

impl LyricsSource {
    /// Человекочитаемое имя: в лог и в плашку панели текста.
    pub fn name(self) -> &'static str {
        match self {
            LyricsSource::LrcLib => "LrcLib",
        }
    }

    /// Содержимое sidecar-файла кеша `<key>.src`.
    fn machine_name(self) -> &'static str {
        match self {
            LyricsSource::LrcLib => "lrclib",
        }
    }

    /// Обратное к [`LyricsSource::machine_name`]: неизвестное имя —
    /// `None`, текст без источника легален (деградация, спека lyrics).
    fn from_machine_name(name: &str) -> Option<Self> {
        match name.trim() {
            "lrclib" => Some(LyricsSource::LrcLib),
            _ => None,
        }
    }
}

/// Первичный исполнитель из метаданных эфира (предложение приёмки 5.3,
/// 2026-11-22): ICY несёт нескольких артистов через «;»
/// («СЕРГЕЙ ТАЮШЕВ;ТАТЬЯНА РУЗАВИНА»). Живая проверка: LrcLib обычно
/// прощает полную строку («Кино;Виктор Цой» — 200 с текстом), поэтому
/// нормализация — предосторожность и унификация ключа кеша, а не
/// исправление промахов: чистый запрос по «Станции минутки» тоже 404
/// (записи в базе нет). Берётся часть до первой «;». Единая точка: URL
/// запроса и ключ кеша видят одно значение — запись не плодится на
/// каждый вариант.
fn primary_artist(artist: &str) -> &str {
    artist.split(';').next().unwrap_or(artist).trim()
}

/// Срез пометок в скобках из названия (design D13): ICY несёт
/// «Песня друзей (м,ф Бременские музыканты)», в базе — «Песня друзей».
/// Вложенные скобки учитываются счётчиком глубины.
fn strip_parens(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut depth = 0usize;
    for ch in s.chars() {
        match ch {
            '(' | '[' => depth += 1,
            ')' | ']' => depth = depth.saturating_sub(1),
            _ if depth == 0 => out.push(ch),
            _ => {}
        }
    }
    out
}

/// Название для запроса (design D13): скобки срезаны — пометки
/// источника («м,ф …», «remaster») не обязаны совпадать с базой.
fn query_title(title: &str) -> String {
    strip_parens(title).trim().to_owned()
}

/// Строка для сравнения названий (design D13): скобки срезаны, регистр
/// снят, небуквенно-цифровые знаки заменены пробелами, пробелы
/// схлопнуты — «ПЕСНЯ ДРУЗЕЙ (м,ф …)» и «Песня друзей» равны.
fn normalize_match(s: &str) -> String {
    let lowered: String = strip_parens(s)
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .to_lowercase();
    lowered.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Строка артиста для сравнения (design D13): регистр снят,
/// небуквенно-цифровые знаки («;», «/», «,») заменены пробелами.
/// Скобки в артисте сохраняются как слова: «Кино (Виктор Цой)» — это
/// второй артист, а не пометка.
fn normalize_artist(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// Множество токенов артиста (design D13) — сравнение дуэтов без учёта
/// порядка и разделителей: «ГОРОХОВ;АНОФРИЕВ» ≡ «Анофриев / Горохов».
fn artist_tokens(s: &str) -> std::collections::BTreeSet<String> {
    normalize_artist(s)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// URL запроса текста (design D1): значения параметров кодируются
/// целиком — кириллица, пробелы и служебные символы названий («&»,
/// «#») не должны ломать query. Trim, разделение артистов по «;»
/// ([`primary_artist`]) и срез скобок из названия ([`query_title`]) —
/// единая нормализация входа от ICY-метаданных.
fn lyrics_url(artist: &str, title: &str) -> String {
    let artist = utf8_percent_encode(primary_artist(artist), NON_ALPHANUMERIC);
    let title = query_title(title);
    let title = utf8_percent_encode(title.as_str(), NON_ALPHANUMERIC);
    format!("{LRCLIB_API_URL}?artist_name={artist}&track_name={title}")
}

/// URL поискового запроса (design D13): только название — живая
/// проверка 2026-11-22 показала, что артист в query сужает поиск мимо
/// записей с другим написанием артистов.
fn search_url(title: &str) -> String {
    let title = query_title(title);
    let title = utf8_percent_encode(title.as_str(), NON_ALPHANUMERIC);
    format!("{LRCLIB_SEARCH_URL}?track_name={title}")
}

/// Запрос текста у источника (спека lyrics «Поиск текста по треку»):
/// точный запрос — 2xx разбор, 404 — переход к поиску, прочий статус —
/// сетевая ошибка; поиск — тот же разбор статусов, из массива
/// кандидатов выбирается лучший (design D13).
fn fetch_lyrics(
    http: &dyn LyricsHttp,
    artist: &str,
    title: &str,
) -> Result<Option<String>, LyricsError> {
    let response = http
        .get(&lyrics_url(artist, title), EXACT_BODY_LIMIT_BYTES)
        .map_err(|e| e.context("точный запрос"))?;
    let exact = match response.status {
        404 => None,
        status if (200..300).contains(&status) => {
            extract_lyrics(&response.body).map_err(|e| e.context("точный запрос"))?
        }
        status => {
            return Err(LyricsError::Network(format!(
                "точный запрос: HTTP {status}"
            )));
        }
    };
    if exact.is_some() {
        return Ok(exact);
    }
    // Fallback поиска (design D13): точный запрос строг к написаниям —
    // дуэты и саундтреки с пометками в титуле он теряет даже при живой
    // записи в базе. Сбой поиска — та же деградация «текста нет», что и
    // сбой точного запроса (спека «Отказы текста не влияют...»).
    let response = http
        .get(&search_url(title), SEARCH_BODY_LIMIT_BYTES)
        .map_err(|e| e.context("поиск"))?;
    if response.status == 404 {
        return Ok(None);
    }
    if !(200..300).contains(&response.status) {
        return Err(LyricsError::Network(format!(
            "поиск: HTTP {}",
            response.status
        )));
    }
    Ok(pick_candidate(
        &parse_candidates(&response.body).map_err(|e| e.context("поиск"))?,
        artist,
        title,
    ))
}

/// Кандидат поискового ответа (design D13): запись LrcLib с непустым
/// не-инструментальным текстом.
struct Candidate {
    artist: String,
    track: String,
    plain: String,
}

/// Кандидаты из ответа `/api/search`: пустые тексты, инструменталы и
/// записи без артиста отбрасываются ещё при разборе; мусор вместо JSON —
/// ошибка. Пустой артист вырождает сравнение токенов (пустое множество —
/// подмножество любого): кандидат выбирался бы без проверки артиста
/// вовсе (находка security-ревью).
fn parse_candidates(body: &str) -> Result<Vec<Candidate>, LyricsError> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| LyricsError::BadResponse(format!("ответ не JSON: {e}")))?;
    let Some(records) = json.as_array() else {
        return Err(LyricsError::BadResponse("поиск вернул не массив".into()));
    };
    Ok(records
        .iter()
        .filter_map(|record| {
            let plain = record.get("plainLyrics").and_then(Value::as_str)?;
            if plain.trim().is_empty()
                || record.get("instrumental").and_then(Value::as_bool) == Some(true)
            {
                return None;
            }
            let artist = record
                .get("artistName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if artist.trim().is_empty() {
                return None;
            }
            Some(Candidate {
                artist: artist.to_owned(),
                track: record
                    .get("trackName")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                plain: plain.to_owned(),
            })
        })
        .collect())
}

/// Выбор кандидата (design D13, находки hunter-ревью): двухпроходный —
/// сначала точное равенство множеств токенов артистов (оригинал), затем
/// вложение в любую сторону (дуэты и «Кино (Виктор Цой)»: порядок и
/// разделители не важны). Двухпроходность не даёт кавер-версии с
/// неполным множеством, стоящей выше оригинала в ранжировании API,
/// перехватить текст; пустое множество токенов эфира (мусорный артист
/// ICY вроде «;;;») означает «артист неизвестен» — совпадение по одному
/// названию было бы вакуумным, кандидат не выбирается вовсе.
fn pick_candidate(candidates: &[Candidate], artist: &str, title: &str) -> Option<String> {
    let icy = artist_tokens(primary_artist(artist));
    if icy.is_empty() {
        return None;
    }
    let want = normalize_match(&query_title(title));
    let title_match = |candidate: &Candidate| normalize_match(&candidate.track) == want;
    candidates
        .iter()
        .find(|candidate| title_match(candidate) && artist_tokens(&candidate.artist) == icy)
        .or_else(|| {
            candidates.iter().find(|candidate| {
                title_match(candidate) && {
                    let db = artist_tokens(&candidate.artist);
                    icy.is_subset(&db) || db.is_subset(&icy)
                }
            })
        })
        .map(|candidate| candidate.plain.clone())
}

/// Текст из ответа LrcLib: поле `plainLyrics`; отсутствие записи или
/// поля, пустой текст и признак инструментальной композиции — промах
/// (`None`), мусор вместо JSON — ошибка.
fn extract_lyrics(body: &str) -> Result<Option<String>, LyricsError> {
    let json: Value = serde_json::from_str(body)
        .map_err(|e| LyricsError::BadResponse(format!("ответ не JSON: {e}")))?;
    if json.get("instrumental").and_then(Value::as_bool) == Some(true) {
        return Ok(None);
    }
    let lyrics = json
        .get("plainLyrics")
        .and_then(Value::as_str)
        .unwrap_or_default();
    Ok((!lyrics.trim().is_empty()).then(|| lyrics.to_owned()))
}

/// Продакшн-транспорт поверх ureq: агент с таймаутами источника текстов
/// (design D2). Конфигурация радио-агента `net.rs` не трогается.
struct UreqLyrics {
    agent: ureq::Agent,
}

impl UreqLyrics {
    fn new() -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_recv_response(Some(RESPONSE_TIMEOUT))
            .timeout_per_read(Some(READ_TIMEOUT))
            .timeout_recv_body(Some(BODY_TIMEOUT))
            // Дисциплина обложечного агента: редиректы запрещены, не-https
            // отклоняется транспортом, резолв ограничен бюджетом.
            .max_redirects(0)
            .https_only(true)
            .timeout_resolve(Some(RESOLVE_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Self { agent }
    }
}

/// Чтение тела с лимитом: `take(limit + 1)` различает «ровно лимит» и
/// «сверх лимита» (симметрия cover.rs — design D1 harden-network-
/// resilience); превышение — вина ответа, не сети (BadResponse).
fn read_limited(reader: impl Read, limit_bytes: u64) -> Result<Vec<u8>, LyricsError> {
    let mut bytes = Vec::new();
    reader
        .take(limit_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| LyricsError::Network(format!("чтение тела: {e}")))?;
    if bytes.len() as u64 > limit_bytes {
        return Err(LyricsError::BadResponse(format!(
            "превышен лимит тела {limit_bytes} байт"
        )));
    }
    Ok(bytes)
}

impl LyricsHttp for UreqLyrics {
    fn get(&self, url: &str, limit_bytes: u64) -> Result<LyricsResponse, LyricsError> {
        let response = self
            .agent
            .get(url)
            .header("User-Agent", USER_AGENT)
            .call()
            .map_err(|e| LyricsError::Network(e.to_string()))?;
        let status = response.status().as_u16();
        let body = if (200..300).contains(&status) || status == 404 {
            String::from_utf8_lossy(&read_limited(
                response.into_body().into_reader(),
                limit_bytes,
            )?)
            .into_owned()
        } else {
            // Чужой статус: тело не читается — не тратим лимит.
            String::new()
        };
        Ok(LyricsResponse { status, body })
    }
}

/// Каталог кеша текстов (design D3): `<XDG_CACHE_HOME|~/.cache>/sevfm/
/// lyrics` — симметрия обложечного каталога с теми же правилами XDG
/// (пустые/относительные значения игнорируются; без базового каталога
/// кеш отключён, поиск работает).
fn cache_dir() -> Option<PathBuf> {
    let base = match std::env::var_os("XDG_CACHE_HOME") {
        Some(dir) if !dir.is_empty() && Path::new(&dir).is_absolute() => PathBuf::from(dir),
        _ => {
            let home = std::env::var_os("HOME").filter(|home| !home.is_empty())?;
            PathBuf::from(home).join(".cache")
        }
    };
    Some(base.join("sevfm").join("lyrics"))
}

/// Имя файла кеша: sha256 от `artist\ntitle` (design D3; trim, снятие
/// регистра и пометок в скобках титула ([`query_title`]) и
/// [`primary_artist`] — единая нормализация входа с URL запроса:
/// «А;Б» и «А», «КИНО» и «Кино», «Трек (пометка)» и «Трек» — одна
/// запись. Иначе вариации написания ICY между звучаниями плодят сетевые
/// запросы и записи кеша — находки hunter/oracle-ревью).
/// Файлобезопасно, без коллизий и лимитов файловой системы.
fn cache_key(artist: &str, title: &str) -> String {
    let digest = Sha256::digest(
        format!(
            "{}\n{}",
            primary_artist(artist).to_lowercase(),
            query_title(title).to_lowercase()
        )
        .as_bytes(),
    );
    format!("{digest:x}")
}

/// Источник из sidecar кеша (design D3): только обычный файл не больше
/// [`SIDECAR_LIMIT_BYTES`] — `symlink_metadata` не следует по ссылке;
/// всё прочее — тихий `None`, текст без источника легален.
fn sidecar_source(sidecar: &Path) -> Option<LyricsSource> {
    let meta = std::fs::symlink_metadata(sidecar).ok()?;
    if !meta.is_file() || meta.len() > SIDECAR_LIMIT_BYTES {
        return None;
    }
    std::fs::read_to_string(sidecar)
        .ok()
        .and_then(|name| LyricsSource::from_machine_name(&name))
}

/// Текст из кеша; попадание обновляет время последнего использования
/// записи (design D3) — вытеснение удаляет давно неиспользованные
/// первыми. Вместе с текстом читается sidecar `<key>.src` с источником:
/// нет файла, не обычный файл, больше лимита или неизвестное имя — текст
/// без источника, не ошибка. Повреждённая запись (нечитаемый файл или
/// пустой текст — пустые записи источник не возвращает) удаляется вместе
/// со sidecar и считается промахом (спека lyrics «Локальный кеш
/// текстов»): при следующей смене трек поищется заново.
fn read_cached(dir: &Path, artist: &str, title: &str) -> Option<(String, Option<LyricsSource>)> {
    let key = cache_key(artist, title);
    let file = dir.join(key.clone() + ".txt");
    let sidecar = dir.join(key + ".src");
    let read = std::fs::read(&file);
    let damaged = |file: &Path, sidecar: &Path, reason: &str| {
        say(&format!(
            "файл кеша текста повреждён ({reason}), удаляю: {}",
            file.display()
        ));
        let _ = std::fs::remove_file(file);
        let _ = std::fs::remove_file(sidecar);
    };
    match read {
        // Отсутствие записи — штатный промах кеша, не повреждение.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => None,
        Err(e) => {
            damaged(&file, &sidecar, &e.to_string());
            None
        }
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) if !text.trim().is_empty() => {
                touch_last_used(&file);
                Some((text, sidecar_source(&sidecar)))
            }
            Ok(_) => {
                damaged(&file, &sidecar, "пустая запись");
                None
            }
            Err(e) => {
                damaged(&file, &sidecar, &e.to_string());
                None
            }
        },
    }
}

/// Продление жизни записи кеша: время последнего использования — mtime
/// (симметрия cover.rs); симлинки не открываются, отказ — тихий пропуск.
fn touch_last_used(file: &Path) {
    let not_symlink =
        |file: &Path| std::fs::symlink_metadata(file).is_ok_and(|meta| meta.is_file());
    if !not_symlink(file) {
        return;
    }
    let _ = std::fs::File::options()
        .write(true)
        .open(file)
        .and_then(|file| file.set_modified(SystemTime::now()));
}

/// Запись в кеш — best effort (design D3): неудача не отменяет выдачу
/// текста, только запись в лог. Временный файл + rename — атомарно;
/// суффикс pid — параллельный экземпляр пишет свой temp. Рядом пишется
/// sidecar `<key>.src` с машинным именем источника; его неудача снимает
/// и прежний sidecar — текст без источника честнее неверного имени.
/// Возвращает размер записанного текста: воркер копит прирост для
/// повторной проверки лимита, sidecar в учёт не входит; неудача — None.
fn write_cached(
    dir: &Path,
    artist: &str,
    title: &str,
    source: LyricsSource,
    text: &str,
) -> Option<u64> {
    let key = cache_key(artist, title);
    let write = || -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let temp = dir.join(format!(".{key}.{}.tmp", std::process::id()));
        std::fs::write(&temp, text)?;
        std::fs::rename(temp, dir.join(format!("{key}.txt")))
    };
    match write() {
        Ok(()) => {
            let sidecar_write = || -> std::io::Result<()> {
                let temp = dir.join(format!(".{key}.{}.src.tmp", std::process::id()));
                std::fs::write(&temp, source.machine_name())?;
                std::fs::rename(temp, dir.join(format!("{key}.src")))
            };
            if sidecar_write().is_err() {
                let _ = std::fs::remove_file(dir.join(format!("{key}.src")));
            }
            Some(text.len() as u64)
        }
        Err(e) => {
            say(&format!("не удалось записать кеш текста: {e}"));
            None
        }
    }
}

/// Подчистка осиротевших временных файлов кеша при старте воркера
/// (симметрия cover.rs): старше [`STALE_TEMP_AGE`] — точно мусор.
fn sweep_stale_temp_files(dir: &Path) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let stale = entry
            .file_name()
            .to_str()
            .is_some_and(|name| name.starts_with('.') && name.ends_with(".tmp"))
            && entry
                .metadata()
                .and_then(|meta| meta.modified())
                .ok()
                .and_then(|modified| modified.elapsed().ok())
                .is_some_and(|age| age > STALE_TEMP_AGE);
        if stale {
            let _ = std::fs::remove_file(entry.path());
        }
    }
}

/// Удаление записи кеша целиком: текст и sidecar — пара; исход
/// txt-удаления возвращается, sidecar снят при любом исходе.
fn remove_entry_files(txt: &Path) -> std::io::Result<()> {
    let txt_removed = std::fs::remove_file(txt);
    let _ = std::fs::remove_file(txt.with_extension("src"));
    txt_removed
}

/// Вытеснение кеша по лимиту (design D3, спека lyrics «Ограничение кеша
/// текстов»): сверх лимита удаляются самые давно использовавшиеся записи
/// `*.txt` — mtime обновляется попаданием, — пока размер не войдёт в
/// лимит; sidecar уходит вместе с жертвой. Отказ удаления отдельного
/// файла — лог и следующая жертва; `*.tmp` не трогаем. `limit` —
/// параметр: продакшн передаёт [`CACHE_LIMIT_BYTES`], тесты — малые числа.
fn enforce_cache_limit(dir: &Path, limit: u64) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            say(&format!(
                "не удалось прочитать каталог кеша текстов {}: {e}",
                dir.display()
            ));
            return;
        }
    };
    // lstat-семантика `DirEntry::metadata`: симлинки не собираются и не
    // следуют по ссылке (симметрия security-ревью обложек).
    let mut records: Vec<(SystemTime, u64, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_file() || !entry.file_name().to_string_lossy().ends_with(".txt") {
            continue;
        }
        let Ok(used) = meta.modified() else { continue };
        records.push((used, meta.len(), entry.path()));
    }
    let total: u64 = records.iter().map(|(_, len, _)| *len).sum();
    let Some(mut excess) = total.checked_sub(limit) else {
        return;
    };
    records.sort_by_key(|(used, _, _)| *used);
    let (mut removed_files, mut removed_bytes) = (0u64, 0u64);
    for (_, len, path) in &records {
        if excess == 0 {
            break;
        }
        match remove_entry_files(path) {
            Ok(()) => {
                excess = excess.saturating_sub(*len);
                removed_files += 1;
                removed_bytes += *len;
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                excess = excess.saturating_sub(*len);
                removed_files += 1;
            }
            Err(e) => say(&format!(
                "не удалось удалить запись кеша текста {}: {e}",
                path.display()
            )),
        }
    }
    say(&format!(
        "кеш текстов: превышен лимит {limit} Б, удалено записей: {removed_files} ({removed_bytes} Б)"
    ));
}

/// Счётчик прироста кеша с порогом повторной проверки лимита
/// (симметрия cover.rs design D3).
struct CacheGrowth {
    since_check: u64,
    delta: u64,
}

impl CacheGrowth {
    fn production() -> Self {
        Self {
            since_check: 0,
            delta: CACHE_RECHECK_DELTA_BYTES,
        }
    }

    /// Учтена запись в `bytes` байтов; `true` — прирост достиг порога,
    /// пора проверить лимит (счётчик сброшен этим вызовом).
    fn record(&mut self, bytes: u64) -> bool {
        self.since_check += bytes;
        if self.since_check >= self.delta {
            self.since_check = 0;
            true
        } else {
            false
        }
    }
}

/// Очистка одного каталога кеша текстов — слой [`clear_lyrics_cache`]
/// ради тестов: удаляет `*.txt`, `*.src` и осиротевшие `*.tmp`,
/// каталог остаётся. Возвращает (удалено файлов, освобождённые байты,
/// не удалось удалить) — печать и код выхода у бинарника.
fn clear_lyrics_cache_in(dir: &Path) -> (u32, u64, u32) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, 0, 0),
        Err(e) => {
            say(&format!(
                "не удалось прочитать каталог кеша текстов {}: {e}",
                dir.display()
            ));
            return (0, 0, 0);
        }
    };
    let (mut files, mut bytes, mut failed) = (0u32, 0u64, 0u32);
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let cached = meta.is_file()
            && (name.ends_with(".txt")
                || name.ends_with(".src")
                || (name.starts_with('.') && name.ends_with(".tmp")));
        if !cached {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                files += 1;
                bytes += meta.len();
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                files += 1;
            }
            Err(e) => {
                failed += 1;
                say(&format!(
                    "не удалось удалить файл кеша текста {}: {e}",
                    entry.path().display()
                ));
            }
        }
    }
    (files, bytes, failed)
}

/// Полная очистка кеша текстов — флаг `--clear-cache` (спека album-art
/// «Ручная очистка кеша обложек» в редакции add-lyrics-panel: флаг
/// чистит оба кеша). Возвращает (удалено файлов, освобождённые байты,
/// не удалось удалить). Кеш отключён — успешная пустая очистка.
pub fn clear_lyrics_cache() -> (u32, u64, u32) {
    cache_dir()
        .map(|dir| clear_lyrics_cache_in(&dir))
        .unwrap_or((0, 0, 0))
}

/// Ответ воркера потребителю: найденный текст с источником или «текста
/// нет». `source` — `None` у текста без источника (старый/повреждённый
/// sidecar кеша): легальный исход, плашка просто не показывается.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum LyricsReply {
    Found {
        text: String,
        source: Option<LyricsSource>,
    },
    Missing,
}

/// Одна загрузка текста трека: кеш → источник → кеш. Все исходы
/// логируются; внешне — `Found` или `Missing`.
fn load_lyrics(
    cache: Option<&Path>,
    http: &dyn LyricsHttp,
    artist: &str,
    title: &str,
    growth: &mut CacheGrowth,
) -> LyricsReply {
    if let Some(dir) = cache
        && let Some((text, source)) = read_cached(dir, artist, title)
    {
        say(&format!("текст: кеш — {artist} — {title}"));
        return LyricsReply::Found { text, source };
    }
    let text = match fetch_lyrics(http, artist, title) {
        Ok(Some(text)) => text,
        Ok(None) => {
            // В логе — нормализованный запрос (первый артист): сырая
            // строка эфира и так видна в записи «эфир:» того же тика.
            say(&format!(
                "текст не найден (запрос: {}): {artist} — {title}",
                primary_artist(artist)
            ));
            return LyricsReply::Missing;
        }
        Err(e @ LyricsError::Network(_)) => {
            say(&format!("источник текстов недоступен: {e}"));
            return LyricsReply::Missing;
        }
        Err(e @ LyricsError::BadResponse(_)) => {
            say(&format!("источник текстов вернул мусор: {e}"));
            return LyricsReply::Missing;
        }
    };
    // Источник у свежего фетча всегда известен — единственный LrcLib.
    if let Some(dir) = cache
        && let Some(written) = write_cached(dir, artist, title, LyricsSource::LrcLib, &text)
        && growth.record(written)
    {
        enforce_cache_limit(dir, CACHE_LIMIT_BYTES);
    }
    say(&format!(
        "текст: {} — {artist} — {title}",
        LyricsSource::LrcLib.name()
    ));
    LyricsReply::Found {
        text,
        source: Some(LyricsSource::LrcLib),
    }
}

/// Ответ воркера с генерацией запроса: свежесть проверяет потребитель
/// при дренировании ([`Lyrics::poll_reply`]).
struct Reply {
    generation: u64,
    outcome: LyricsReply,
}

/// Запрос воркеру с генерацией запроса.
struct Request {
    generation: u64,
    artist: String,
    title: String,
}

/// Сервис текстов (design D4): фоновый воркер с каналом — запросы
/// неблокирующие, тракт и окно сеть не ждут. Каркас — перенос воркера
/// обложек. Запросы подаёт [`Lyrics::track_changed`] из poll-потока,
/// ответы забираются опросом [`Lyrics::poll_reply`].
pub struct Lyrics {
    requests: Sender<Request>,
    replies: Receiver<Reply>,
    generation: Arc<AtomicU64>,
    /// Мёртв ли воркер (паника): поднявшись, глушит новые запросы —
    /// тексты не критичны, но их исчезновение объясняется записью в логе.
    dead: Arc<AtomicBool>,
}

impl Lyrics {
    /// Продакшн-сервис: ureq-транспорт и кеш в пользовательском каталоге.
    pub fn start() -> Self {
        Self::with_backend(cache_dir(), Box::new(UreqLyrics::new()))
    }

    /// Продакшн-транспорт без кеша — живой тест источника
    /// (`tests/lyrics_live.rs`): записи кеша пользователя при прогоне
    /// не пишутся.
    pub fn without_cache() -> Self {
        Self::with_backend(None, Box::new(UreqLyrics::new()))
    }

    /// Сборка с заданным транспортом и каталогом кеша (`None` — без
    /// кеша): для fake-транспорта юнит-тестов.
    fn with_backend(cache: Option<PathBuf>, http: Box<dyn LyricsHttp>) -> Self {
        let (request_tx, request_rx) = channel::<Request>();
        let (reply_tx, reply_rx) = channel::<Reply>();
        let generation = Arc::new(AtomicU64::new(0));
        let dead = Arc::new(AtomicBool::new(false));
        let worker_dead = dead.clone();
        // Отказ потока — деградация без текстов, не крах плеера (спека
        // lyrics «Отказы текста не влияют на воспроизведение»). Паника
        // воркера ловится: одна запись в лог и флаг «мёртв»; respawn не
        // делаем — паника может быть систематической, перезапуск дал бы
        // петлю.
        match std::thread::Builder::new()
            .name("lyrics-service".into())
            .spawn(move || {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    worker_loop(cache, http, request_rx, reply_tx)
                })) {
                    let message = payload
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_owned())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "причина неизвестна".into());
                    say(&format!("сервис текстов аварийно завершился: {message}"));
                    worker_dead.store(true, Ordering::SeqCst);
                }
            }) {
            Ok(_) => {}
            Err(e) => {
                say(&format!("не удалось запустить сервис текстов: {e}"));
                dead.store(true, Ordering::SeqCst);
            }
        }
        Self {
            requests: request_tx,
            replies: reply_rx,
            generation,
            dead,
        }
    }

    /// Смена трека: музыкальный трек — поиск текста; не-музыка (вставка,
    /// сырой StreamTitle, остановка) — гашение поиска без сети (спека
    /// lyrics «Не-музыка не ищется»).
    pub fn track_changed(&self, track: Option<(&str, &str)>) {
        match track {
            Some((artist, title)) => self.request(artist, title),
            None => self.invalidate(),
        }
    }

    fn request(&self, artist: &str, title: &str) {
        // Мёртвый воркер: авария уже залогирована, повторы тихо
        // пропускаются до перезапуска приложения.
        if self.dead.load(Ordering::SeqCst) {
            return;
        }
        // Снимок генерации после инкремента: любой последующий bump
        // делает снимок устаревшим.
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = self.requests.send(Request {
            generation,
            artist: artist.to_owned(),
            title: title.to_owned(),
        });
    }

    /// Гасит незавершённый поиск: ответ доставлен не будет (смена слота
    /// на не-музыку, новая сессия, закрытие окна).
    pub fn invalidate(&self) {
        self.generation.fetch_add(1, Ordering::SeqCst);
    }

    /// Мёртв ли воркер (паника или отказ спавна): потребитель гасит
    /// кнопку текста сразу, не ожидая ответа, который не придёт.
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// Очередной ответ воркера без ожидания; `None` — ответов нет.
    ///
    /// Свежесть проверяется здесь, а не в воркере (симметрия cover.rs):
    /// все bump'ы генерации идут из потока потребителя — и этот же поток
    /// дренирует канал, поэтому сверка на его стороне атомарна и закрывает
    /// ответы, уже лежащие в канале.
    pub fn poll_reply(&self) -> Option<LyricsReply> {
        while let Ok(reply) = self.replies.try_recv() {
            if reply.generation == self.generation.load(Ordering::SeqCst) {
                return Some(reply.outcome);
            }
        }
        None
    }
}

/// Воркер: обрабатывает самый свежий запрос (дренирует backlog — смена
/// треков быстрее ответов не копит очередь) и шлёт ответ с генерацией;
/// свежесть фильтрует потребитель. Выход — закрытие канала запросов.
fn worker_loop(
    cache: Option<PathBuf>,
    http: Box<dyn LyricsHttp>,
    requests: Receiver<Request>,
    replies: Sender<Reply>,
) {
    if let Some(dir) = cache.as_deref() {
        sweep_stale_temp_files(dir);
        // Стартовая проверка лимита: перезапуски начинают с лимита,
        // долгие сессии — порог прироста в load_lyrics.
        enforce_cache_limit(dir, CACHE_LIMIT_BYTES);
    }
    let mut growth = CacheGrowth::production();
    while let Ok(mut current) = requests.recv() {
        while let Ok(next) = requests.try_recv() {
            current = next;
        }
        let outcome = load_lyrics(
            cache.as_deref(),
            http.as_ref(),
            &current.artist,
            &current.title,
            &mut growth,
        );
        let _ = replies.send(Reply {
            generation: current.generation,
            outcome,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::time::Instant;

    /// Исход fake-ответа: статус + тело, либо сетевой сбой.
    type FakeOutcome = Result<(u16, String), String>;

    /// Fake-транспорт: ответы (статус + тело) или сетевые сбои
    /// раздаются по первому вхождению подстроки в URL, каждое обращение
    /// записывается — тесты сверяют лимит тела и число обращений.
    #[derive(Default)]
    struct FakeHttp {
        responses: Vec<(&'static str, FakeOutcome)>,
        calls: Arc<Mutex<Vec<(String, u64)>>>,
    }

    impl FakeHttp {
        fn text(&mut self, needle: &'static str, body: &str) {
            self.responses.push((needle, Ok((200, body.to_owned()))));
        }

        fn status(&mut self, needle: &'static str, status: u16, body: &str) {
            self.responses.push((needle, Ok((status, body.to_owned()))));
        }

        fn network_err(&mut self, needle: &'static str, message: &str) {
            self.responses.push((needle, Err(message.to_owned())));
        }

        fn calls_total(&self) -> usize {
            self.calls.lock().expect("замок вызовов не отравлен").len()
        }

        fn limits_snapshot(&self) -> Vec<u64> {
            self.calls
                .lock()
                .expect("замок вызовов не отравлен")
                .iter()
                .map(|(_, limit)| *limit)
                .collect()
        }
    }

    impl LyricsHttp for FakeHttp {
        fn get(&self, url: &str, limit_bytes: u64) -> Result<LyricsResponse, LyricsError> {
            self.calls
                .lock()
                .expect("замок вызовов не отравлен")
                .push((url.to_owned(), limit_bytes));
            self.responses
                .iter()
                .find(|(needle, _)| url.contains(needle))
                .map_or_else(
                    || {
                        Err(LyricsError::Network(format!(
                            "у fake-транспорта нет ответа для {url}"
                        )))
                    },
                    |(_, outcome)| {
                        outcome
                            .clone()
                            .map(|(status, body)| LyricsResponse { status, body })
                            .map_err(LyricsError::Network)
                    },
                )
        }
    }

    /// Ждёт ответ воркера с таймаутом — ответы асинхронные.
    fn await_reply(lyrics: &Lyrics, what: &str) -> LyricsReply {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(reply) = lyrics.poll_reply() {
                return reply;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("ответ воркера не пришёл: {what}");
    }

    /// Каталог кеша для теста с уникальным суффиксом (параллельные
    /// тесты одного процесса не делят каталог).
    fn test_cache_dir(label: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("sevfm-lyrics-test-{}-{label}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("каталог кеша теста создаётся");
        dir
    }

    /// Ответ LrcLib с текстом — заготовка для таблицы разборов.
    fn api_body(plain: &str, instrumental: bool) -> String {
        format!(r#"{{"plainLyrics":"{plain}","instrumental":{instrumental}}}"#)
    }

    /// Массив кандидатов поискового ответа (design D13): пары
    /// (артист, название), текст и инструментал — общие.
    fn search_body(records: &[(&str, &str)]) -> String {
        let items: Vec<String> = records
            .iter()
            .map(|(artist, track)| {
                format!(
                    r#"{{"artistName":"{artist}","trackName":"{track}","plainLyrics":"текст {track}","instrumental":false}}"#
                )
            })
            .collect();
        format!("[{}]", items.join(","))
    }

    /// Разбор ответа (спека «Поиск текста по треку»): найден, промах без
    /// записи, пустой текст, инструментал, мусор вместо JSON.
    #[test]
    fn extract_lyrics_covers_all_outcomes() {
        assert_eq!(
            extract_lyrics(&api_body("Группа крови на рукаве", false)).expect("текст разбирается"),
            Some("Группа крови на рукаве".to_owned())
        );
        // Промахи: без поля, пустой текст, инструментал, нет записи.
        for body in [
            r#"{"instrumental":false}"#.to_owned(),
            api_body("", false),
            api_body("   ", false),
            api_body("", true),
            r#"{"name":"Creep"}"#.to_owned(),
        ] {
            assert_eq!(
                extract_lyrics(&body).expect("промах — не ошибка"),
                None,
                "{body}"
            );
        }
        let err = extract_lyrics("<html>nginx</html>").expect_err("мусор — ошибка");
        assert!(matches!(err, LyricsError::BadResponse(_)), "{err}");
    }

    /// URL запроса (design D1): кириллица кодируется, пробел не остаётся
    /// сырым, служебный «&» в названии не ломает query; trim применяется.
    #[test]
    fn lyrics_url_encodes_values() {
        let url = lyrics_url("Кино", "Группа крови");
        assert!(url.starts_with(LRCLIB_API_URL), "{url}");
        assert!(url.contains("artist_name="), "{url}");
        assert!(!url.contains(' '), "{url}");
        assert!(!url.contains("Кино"), "кириллица должна быть закодирована");

        // «&» и «#» в значении не разрывают параметры и фрагмент.
        let url = lyrics_url("AC/DC", "A&B #1");
        let query = url.split('?').nth(1).expect("query есть");
        let pairs: Vec<&str> = query.split('&').collect();
        assert_eq!(pairs.len(), 2, "ровно два параметра: {query}");
        assert!(pairs[0].starts_with("artist_name="), "{query}");
        assert!(pairs[1].starts_with("track_name="), "{query}");

        // Trim — единая нормализация входа.
        assert_eq!(lyrics_url(" Кино ", " Трек "), lyrics_url("Кино", "Трек"));

        // Название для запроса срезает пометки в скобках (design D13).
        assert_eq!(
            lyrics_url("Кино", "Песня друзей (м,ф Бременские музыканты)"),
            lyrics_url("Кино", "Песня друзей")
        );
        assert_eq!(
            search_url("Песня друзей (м,ф Бременские музыканты)"),
            search_url("Песня друзей")
        );

        // Несколько артистов через «;» (находка приёмки 5.3, живой
        // пример «СЕРГЕЙ ТАЮШЕВ;ТАТЬЯНА РУЗАВИНА»): запрос и ключ кеша —
        // по первому исполнителю, запись кеша одна.
        assert_eq!(
            lyrics_url("СЕРГЕЙ ТАЮШЕВ;ТАТЬЯНА РУЗАВИНА", "Трек"),
            lyrics_url("СЕРГЕЙ ТАЮШЕВ", "Трек")
        );
        assert_eq!(
            cache_key("СЕРГЕЙ ТАЮШЕВ;ТАТЬЯНА РУЗАВИНА", "Трек"),
            cache_key("СЕРГЕЙ ТАЮШЕВ", "Трек")
        );
        // Регистр снят (находка hunter-ревью): капс эфира — та же запись.
        assert_eq!(
            cache_key("КИНО", "ГРУППА КРОВИ"),
            cache_key("Кино", "Группа крови")
        );
        // Пометки в скобках срезаны (находка oracle-ревью): ключ паритетен
        // URL запроса, вариации написания — одна запись.
        assert_eq!(
            cache_key("Кино", "Песня друзей (м,ф Бременские музыканты)"),
            cache_key("Кино", "Песня друзей")
        );
    }

    /// Классификация статусов (design D1): 404 — промах, прочий не-2xx —
    /// сетевая ошибка, 2xx — разбор.
    #[test]
    fn fetch_lyrics_maps_statuses() {
        let mut http = FakeHttp::default();
        http.status("lrclib", 404, "not found");
        assert_eq!(
            fetch_lyrics(&http, "Баста", "Сансара").expect("404 — промах"),
            None
        );

        let mut http = FakeHttp::default();
        http.status("lrclib", 500, "boom");
        let err = fetch_lyrics(&http, "А", "Б").expect_err("500 — ошибка");
        assert!(matches!(err, LyricsError::Network(_)), "{err}");
        assert!(err.to_string().contains("HTTP 500"), "{err}");
    }

    /// Лимит тела (design D2): `take(limit+1)` различает «ровно лимит»
    /// и «сверх лимита»; транспорт передаёт константу [`EXACT_BODY_LIMIT_BYTES`].
    #[test]
    fn body_limit_is_enforced_and_passed() {
        let body = |len: usize| vec![0u8; len];
        let exact =
            read_limited(std::io::Cursor::new(body(100)), 100).expect("ровно лимит проходит");
        assert_eq!(exact.len(), 100);
        let err = read_limited(std::io::Cursor::new(body(101)), 100)
            .expect_err("лимит + 1 байт — ошибка");
        assert!(matches!(err, LyricsError::BadResponse(_)), "{err}");
        assert!(err.to_string().contains("лимит"), "{err}");

        let mut http = FakeHttp::default();
        http.text("lrclib", &api_body("текст", false));
        fetch_lyrics(&http, "Кино", "Группа крови").expect("живой ответ");
        assert_eq!(
            http.limits_snapshot(),
            vec![EXACT_BODY_LIMIT_BYTES],
            "запрос несёт лимит тела транспорта"
        );
    }

    /// Кеш (спека «Локальный кеш текстов»): запись → чтение возвращает
    /// текст и источник; отсутствие записи — промах без удалений.
    #[test]
    fn cache_roundtrip_preserves_text_and_source() {
        let dir = test_cache_dir("roundtrip");
        assert_eq!(read_cached(&dir, "Кино", "Группа крови"), None);
        let written = write_cached(
            &dir,
            "Кино",
            "Группа крови",
            LyricsSource::LrcLib,
            "Группа крови на рукаве\nМой порядковый номер на рукаве",
        )
        .expect("запись удачна");
        assert!(written > 0);
        let (text, source) = read_cached(&dir, "Кино", "Группа крови").expect("запись читается");
        assert_eq!(
            text,
            "Группа крови на рукаве\nМой порядковый номер на рукаве"
        );
        assert_eq!(source, Some(LyricsSource::LrcLib));
        // Трим входа не должен плодить вторую запись.
        assert_eq!(
            read_cached(&dir, " Кино ", " Группа крови "),
            Some((text, Some(LyricsSource::LrcLib)))
        );
    }

    /// Повреждённая запись (спека «Локальный кеш текстов»): нечитаемый
    /// файл и пустой текст — промах, файлы удалены; повторный поиск
    /// перезаписывает запись.
    #[test]
    fn damaged_cache_entry_is_removed_and_refetched() {
        let dir = test_cache_dir("damaged");
        let key = cache_key("Кино", "Пачка сигарет");
        let file = dir.join(format!("{key}.txt"));
        let sidecar = dir.join(format!("{key}.src"));

        // Нечитаемый как UTF-8.
        std::fs::create_dir_all(&dir).expect("каталог создаётся");
        std::fs::write(&file, [0xFF, 0xFE, 0x00]).expect("мусор пишется");
        std::fs::write(&sidecar, "lrclib").expect("sidecar пишется");
        assert_eq!(read_cached(&dir, "Кино", "Пачка сигарет"), None);
        assert!(!file.exists(), "повреждённая txt удалена");
        assert!(!sidecar.exists(), "sidecar снят вместе с записью");

        // Пустой файл — тоже повреждение.
        std::fs::write(&file, "").expect("пустой файл пишется");
        assert_eq!(read_cached(&dir, "Кино", "Пачка сигарет"), None);
        assert!(!file.exists(), "пустая запись удалена");

        // Повторный поиск перезаписывает запись.
        write_cached(&dir, "Кино", "Пачка сигарет", LyricsSource::LrcLib, "текст")
            .expect("перезапись удачна");
        assert_eq!(
            read_cached(&dir, "Кино", "Пачка сигарет"),
            Some(("текст".to_owned(), Some(LyricsSource::LrcLib)))
        );
    }

    /// Sidecar (design D3): отсутствие, неизвестное имя и гигантский
    /// файл дают текст без источника, не ошибку.
    #[test]
    fn sidecar_failures_degrade_to_no_source() {
        let dir = test_cache_dir("sidecar");
        write_cached(&dir, "А", "Б", LyricsSource::LrcLib, "текст").expect("запись удачна");
        let key = cache_key("А", "Б");
        let sidecar = dir.join(format!("{key}.src"));

        // Нет sidecar (кеш старого формата).
        std::fs::remove_file(&sidecar).expect("sidecar снимается");
        assert_eq!(
            read_cached(&dir, "А", "Б"),
            Some(("текст".to_owned(), None))
        );

        // Гигантский или подменённый файл не читается.
        std::fs::write(&sidecar, [b'x'; 40]).expect("гигантский sidecar пишется");
        assert_eq!(
            read_cached(&dir, "А", "Б"),
            Some(("текст".to_owned(), None))
        );

        // Неизвестное машинное имя — то же.
        std::fs::write(&sidecar, "unknown").expect("чужое имя пишется");
        assert_eq!(
            read_cached(&dir, "А", "Б"),
            Some(("текст".to_owned(), None))
        );
    }

    /// Вытеснение по лимиту (спека «Ограничение кеша текстов»): сверх
    /// лимита удаляются самые давно использованные; попадание обновляет
    /// mtime и продлевает жизнь записи.
    #[test]
    fn cache_limit_evicts_least_recently_used() {
        let dir = test_cache_dir("lru");
        let entry = |text: &str| format!("текст {text}");
        for label in ["1", "2", "3"] {
            std::thread::sleep(Duration::from_millis(15));
            write_cached(&dir, "А", label, LyricsSource::LrcLib, &entry(label))
                .expect("запись удачна");
        }
        // Попадание по первой записи продлевает её жизнь.
        std::thread::sleep(Duration::from_millis(15));
        read_cached(&dir, "А", "1").expect("первая запись читается");

        // Лимит влезает ровно две записи — старейшая невостребованная
        // (№2) вытесняется, №1 продлена попаданием, №3 свежая.
        let limit = entry("1").len() as u64 + entry("3").len() as u64;
        enforce_cache_limit(&dir, limit);
        assert!(read_cached(&dir, "А", "1").is_some(), "продлённая жива");
        assert!(read_cached(&dir, "А", "3").is_some(), "свежая жива");
        assert_eq!(read_cached(&dir, "А", "2"), None, "старейшая вытеснена");
        let sidecar = dir.join(format!("{}.src", cache_key("А", "2")));
        assert!(!sidecar.exists(), "sidecar вытесненной снят");
    }

    /// Порог прироста кеша (design D3): проверка запускается при
    /// достижении порога и счётчик сбрасывается.
    #[test]
    fn cache_growth_triggers_recheck_at_threshold() {
        let mut growth = CacheGrowth {
            since_check: 0,
            delta: 100,
        };
        assert!(!growth.record(60), "до порога проверки нет");
        assert!(growth.record(40), "порог достигнут");
        assert!(!growth.record(10), "после порога счётчик начат заново");
        assert!(growth.record(90), "новый порог достигнут");
    }

    /// Очистка кеша текстов (спека album-art в редакции change): все
    /// файлы записей удалены, объём посчитан, чужие файлы не тронуты;
    /// отсутствующий каталог — пустая очистка.
    #[test]
    fn clear_lyrics_cache_removes_entries_only() {
        let dir = test_cache_dir("clear");
        write_cached(&dir, "А", "Б", LyricsSource::LrcLib, "текст АБ").expect("запись удачна");
        write_cached(&dir, "В", "Г", LyricsSource::LrcLib, "текст ВГ").expect("запись удачна");
        let foreign = dir.join("foreign.bin");
        std::fs::write(&foreign, b"keep").expect("чужой файл пишется");

        let (files, bytes, failed) = clear_lyrics_cache_in(&dir);
        assert_eq!(failed, 0, "очистка без отказов");
        // Объём — все удалённые файлы: два текста и два sidecar «lrclib».
        assert_eq!(
            bytes,
            "текст АБ".len() as u64 + "текст ВГ".len() as u64 + 2 * "lrclib".len() as u64,
            "учтён объём записей и sidecar'ов"
        );
        assert!(
            files >= 2,
            "как минимум две записи (txt) удалены, sidecar и tmp — сверху: {files}"
        );
        assert!(foreign.exists(), "чужой файл не тронут");
        assert_eq!(read_cached(&dir, "А", "Б"), None);

        // Отсутствующий каталог — пустая очистка, не ошибка.
        let empty = test_cache_dir("clear-empty");
        assert_eq!(clear_lyrics_cache_in(&empty), (0, 0, 0));
    }

    /// Fallback поиска (design D13, спека «Дуэт с другим написанием в
    /// базе»): точный промах → поиск → кандидат с другим написанием
    /// артистов и порядком дуэта берётся; чужой артист при том же
    /// названии — нет; сетевой сбой поиска — та же деградация Missing.
    #[test]
    fn fetch_lyrics_falls_back_to_search() {
        // Дуэт: ICY «ГОРОХОВ;АНОФРИЕВ», в базе порядок и разделители
        // другие — кандидат берётся (живой случай приёмки 5.3).
        let mut http = FakeHttp::default();
        http.status("api/get", 404, "");
        http.text(
            "api/search",
            &search_body(&[
                (
                    "Инструментальный ансамбль, Олег Анофриев, Анатолий Горохов",
                    "Песня друзей и приезд в город",
                ),
                ("Олег Анофриев / Анатолий Горохов", "Песня друзей"),
            ]),
        );
        let text = fetch_lyrics(
            &http,
            "АНАТОЛИЙ ГОРОХОВ;ОЛЕГ АНОФРИЕВ",
            "Песня друзей (м,ф Бременские музыканты)",
        )
        .expect("fallback находит");
        assert_eq!(
            text.as_deref(),
            Some("текст Песня друзей"),
            "кандидат с совпавшим названием"
        );
        assert_eq!(http.calls_total(), 2, "точный + поиск");
        assert_eq!(
            http.limits_snapshot(),
            vec![EXACT_BODY_LIMIT_BYTES, SEARCH_BODY_LIMIT_BYTES],
            "у шагов разные лимиты тела (находка hunter-ревью)"
        );

        // Точный ответ закрывает дело: поиск не запускается.
        let mut http = FakeHttp::default();
        http.text("api/get", &api_body("текст", false));
        http.text("api/search", &search_body(&[("Кино", "Группа крови")]));
        fetch_lyrics(&http, "Кино", "Группа крови").expect("точный ответ");
        assert_eq!(http.calls_total(), 1, "fallback не нужен");

        // Кандидаты есть, но название/артист не совпали — промах.
        // Запись без артиста с совпавшим названием тоже: пустое множество
        // токенов — подмножество любого, отбрасывается при разборе
        // (находка security-ревью).
        let mut http = FakeHttp::default();
        http.status("api/get", 404, "");
        http.text(
            "api/search",
            &search_body(&[
                ("Кино", "Группа крови"),
                ("Песня друзей", "Совсем другая группа"),
                ("", "Песня друзей"),
            ]),
        );
        assert_eq!(
            fetch_lyrics(&http, "АНАТОЛИЙ ГОРОХОВ;ОЛЕГ АНОФРИЕВ", "Песня друзей")
                .expect("промах без ошибки"),
            None
        );

        // Сбой поиска — сетевая ошибка, не паника (спека «Отказы...»).
        let mut http = FakeHttp::default();
        http.status("api/get", 404, "");
        http.network_err("api/search", "connection refused");
        let err = fetch_lyrics(&http, "А", "Б").expect_err("сбой поиска — ошибка");
        assert!(matches!(err, LyricsError::Network(_)), "{err}");

        // Подмножество токенов: «кино» против «Кино (Виктор Цой)».
        let candidates = parse_candidates(&search_body(&[("Кино (Виктор Цой)", "Пачка сигарет")]))
            .expect("кандидаты разбираются");
        assert_eq!(
            pick_candidate(&candidates, "Кино", "Пачка сигарет").as_deref(),
            Some("текст Пачка сигарет"),
            "токены первого артиста — подмножество записи базы"
        );

        // Двухпроходный выбор (находка hunter-ревью): кавер с неполным
        // множеством токенов («олег» ⊂ полного дуэта) стоит выше оригинала
        // в ранжировании API — побеждает точное равенство, не первый
        // subset.
        let candidates = parse_candidates(&search_body(&[
            ("Олег", "Песня друзей (live)"),
            ("Олег Анофриев / Анатолий Горохов", "Песня друзей"),
        ]))
        .expect("кандидаты разбираются");
        assert_eq!(
            pick_candidate(
                &candidates,
                "АНАТОЛИЙ ГОРОХОВ;ОЛЕГ АНОФРИЕВ",
                "Песня друзей"
            )
            .as_deref(),
            Some("текст Песня друзей"),
            "точное равенство множеств предпочтительнее вложения"
        );

        // Пустое множество токенов эфира (мусорный артист «;;;»):
        // совпадение по одному названию было бы вакуумным — промах.
        assert_eq!(
            pick_candidate(&candidates, ";;;", "Песня друзей"),
            None,
            "неизвестный артист эфира не выбирает кандидата"
        );

        // Мусор вместо массива — ошибка разбора.
        let mut http = FakeHttp::default();
        http.status("api/get", 404, "");
        http.text("api/search", "<html>boom</html>");
        let err = fetch_lyrics(&http, "А", "Б").expect_err("мусор поиска");
        assert!(matches!(err, LyricsError::BadResponse(_)), "{err}");
    }

    /// Полная загрузка (спека «Поиск текста по треку»): находка пишется
    /// в кеш и возвращает источник; повторный трек берётся из кеша без
    /// сетевого запроса; промах — Missing без записи в кеш.
    #[test]
    fn load_lyrics_caches_hits_and_leaves_misses_uncached() {
        let dir = test_cache_dir("load");
        let mut http = FakeHttp::default();
        http.text("lrclib", &api_body("текст трека", false));

        let reply = load_lyrics(
            Some(&dir),
            &http,
            "Кино",
            "Группа крови",
            &mut CacheGrowth::production(),
        );
        assert_eq!(
            reply,
            LyricsReply::Found {
                text: "текст трека".to_owned(),
                source: Some(LyricsSource::LrcLib),
            }
        );

        // Повторный трек — из кеша: число обращений не растёт.
        let calls_after_fetch = http.calls_total();
        let reply = load_lyrics(
            Some(&dir),
            &http,
            "Кино",
            "Группа крови",
            &mut CacheGrowth::production(),
        );
        assert!(matches!(reply, LyricsReply::Found { .. }), "{reply:?}");
        assert_eq!(http.calls_total(), calls_after_fetch, "сеть не тронута");

        // Промах — Missing и без записи в кеш: следующий заход снова
        // пойдёт в сеть (спека: промах не кешируется).
        let mut http = FakeHttp::default();
        http.status("lrclib", 404, "");
        let reply = load_lyrics(
            Some(&dir),
            &http,
            "Баста",
            "Сансара",
            &mut CacheGrowth::production(),
        );
        assert_eq!(reply, LyricsReply::Missing);
        let missing_key = dir.join(format!("{}.txt", cache_key("Баста", "Сансара")));
        assert!(!missing_key.exists(), "промах не пишет запись");

        // Сбой сети — тоже Missing (спека «Отказы текста не влияют...»).
        let mut http = FakeHttp::default();
        http.network_err("lrclib", "connection refused");
        let reply = load_lyrics(
            Some(&dir),
            &http,
            "ДДТ",
            "Что такое осень",
            &mut CacheGrowth::production(),
        );
        assert_eq!(reply, LyricsReply::Missing);
    }

    /// Воркер (спека «Смена трека отменяет прошлый поиск»): ответ
    /// доставляется, опоздавший ответ прошлого трека после инвалидации
    /// отбрасывается, мёртвый воркер тихо глушит запросы. Названия —
    /// ASCII: fake-транспорт ищет подстроку в URL, где кириллица
    /// процент-кодирована (как ASCII-needle'ы у обложечных тестов).
    #[test]
    fn worker_delivers_fresh_and_drops_stale_replies() {
        let mut http = FakeHttp::default();
        http.text("first", &api_body("текст первого", false));
        let lyrics = Lyrics::with_backend(None, Box::new(http));

        lyrics.track_changed(Some(("Исполнитель", "first")));
        assert_eq!(
            await_reply(&lyrics, "первый ответ"),
            LyricsReply::Found {
                text: "текст первого".to_owned(),
                source: Some(LyricsSource::LrcLib),
            }
        );

        // Смена трека до ответа: инвалидация глушит предыдущий запрос,
        // новый — доставляется.
        lyrics.track_changed(Some(("Исполнитель", "second")));
        lyrics.track_changed(Some(("Исполнитель", "first-again")));
        // Прошлый (second) ответ, если пришёл раньше, гасится
        // генерацией; свежий — доставлен.
        let reply = await_reply(&lyrics, "ответ по последнему треку");
        assert!(
            matches!(
                reply,
                LyricsReply::Found {
                    source: Some(LyricsSource::LrcLib),
                    ..
                } | LyricsReply::Missing
            ),
            "{reply:?}"
        );

        // Не-музыка: гашение без сети, ответов нет.
        lyrics.track_changed(None);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(lyrics.poll_reply(), None, "после инвалидации ответов нет");

        // Мёртвый воркер: запросы тихо игнорируются (флаг ставится
        // вручную — панику потока не эмулируем).
        lyrics.dead.store(true, Ordering::SeqCst);
        lyrics.track_changed(Some(("Исполнитель", "first")));
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(lyrics.poll_reply(), None, "мёртвый воркер молчит");
    }

    /// Двойной ответ при гонке «запрос-до-дренирования» (симметрия
    /// cover.rs: воркер обрабатывает самый свежий запрос backlog'а).
    #[test]
    fn worker_handles_only_freshest_backlog() {
        let mut http = FakeHttp::default();
        http.text("lrclib", &api_body("текст", false));
        let lyrics = Lyrics::with_backend(None, Box::new(http));
        // Три запроса подряд: воркер обязан обработать только последний
        // и вернуть один ответ.
        lyrics.track_changed(Some(("А", "1")));
        lyrics.track_changed(Some(("А", "2")));
        lyrics.track_changed(Some(("А", "3")));
        let reply = await_reply(&lyrics, "ответ по свежайшему запросу");
        assert!(
            matches!(reply, LyricsReply::Found { .. }),
            "свежайший запрос ищется и находится: {reply:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(lyrics.poll_reply(), None, "второго ответа нет");
    }
}

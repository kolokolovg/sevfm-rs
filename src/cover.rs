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

//! Обложки трека (фаза add-album-art): поиск по цепочке публичных
//! сервисов без ключа — iTunes → Deezer → Яндекс.Музыка (схема и
//! эндпоинты — Java-референс, design D1), дисковый кеш повторных
//! треков и фоновый воркер-поток, не мешающий тракту и окну.
//!
//! Сервисы возвращают списки кандидатов — берётся первый с непустым
//! ключом обложки (верх ранжирования сервиса); поиск Яндекса нестрогий
//! (fuzzy), поэтому он замыкает цепочку. Сбой сервиса — запись в лог и
//! следующий; полный промах — заглушка у потребителя. Не-музыка
//! (вставка, трек без исполнителя, остановка) в сеть не ходит вовсе.

use std::io::Read;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::{Duration, SystemTime};

use http::Uri;
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::say;

/// Таймауты HTTP сервисов обложек (design D3): соединение, заголовки и
/// каждое чтение тела — 5 с; общий бюджет тела — 15 с (`per_read`
/// перезапускается на каждом чтении и один не ограничивает медленный
/// trickle-CDN — находка ревью).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const BODY_TIMEOUT: Duration = Duration::from_secs(15);
/// Таймаут DNS-резолва (design D2 harden-network-resilience):
/// симметрия со стрим-агентом `net.rs` — зависший резолв (битый NSS,
/// отвалившийся DNS) обрывается бюджетом и трактуется как сбой сервиса,
/// а не подвешивает единственного воркера обложек навсегда.
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);
/// Лимит тела поискового ответа (design D1 harden-network-resilience):
/// живые ответы трёх сервисов с limit=5 — единицы–десятки КиБ (замер
/// 2026-11-02); 1 МиБ — запас на два порядка. Раньше `read_to_end` был
/// без ограничения: гигантское или враждебное тело раздувало память.
const SEARCH_BODY_LIMIT_BYTES: u64 = 1024 * 1024;
/// Лимит тела картинки обложки: `cover_big`/1000x1000 — сотни КиБ;
/// 10 МиБ покрывает любой честный ответ с запасом, декодер всё равно
/// ограничен 4096x4096 (см. `decode_jpeg`).
const IMAGE_BODY_LIMIT_BYTES: u64 = 10 * 1024 * 1024;
/// User-Agent запросов к сервисам обложек (design D3).
const USER_AGENT: &str = concat!("sevfm/", env!("CARGO_PKG_VERSION"));
/// Возраст осиротевшего временного файла кеша, после которого он
/// точно мусор (подчистка на старте воркера — находка ревью).
const STALE_TEMP_AGE: Duration = Duration::from_secs(3600);
/// Лимит суммарного размера кеша обложек (design D2 cover-cache-cleanup):
/// замер 2026-11-04 — приток ~8 МиБ/день при медиане записи 108 КиБ;
/// 100 МиБ ≈ две недели уникальных треков ротации, повторные звучания
/// остаются в кеше.
const CACHE_LIMIT_BYTES: u64 = 100 * 1024 * 1024;
/// Порог прироста записанных обложек для повторной проверки лимита в
/// работающем воркере (design D3 cover-cache-cleanup): ≈ дневной приток —
/// скан каталога не чаще раза в день прослушивания, миллисекунды в
/// потоке воркера, без таймеров и дополнительных потоков.
const CACHE_RECHECK_DELTA_BYTES: u64 = 8 * 1024 * 1024;

/// Ошибка обращения к сервису обложек. Варианты разделяют формулировки
/// лога (находка ревью): «недоступен» уводил бы диагностику мусорного
/// ответа в проверку сети.
#[derive(Debug, thiserror::Error)]
enum CoverError {
    /// Сеть, TLS, HTTP-статус — сервис не ответил полезным образом.
    #[error("{0}")]
    Network(String),
    /// Живой ответ не разобран (мусор/HTML вместо JSON) или картинка
    /// не декодируется.
    #[error("{0}")]
    BadResponse(String),
}

/// Транспорт HTTP сервисов обложек (design D3): `get_text` — поисковые
/// ответы, `get_bytes` — картинка. Лимит тела передаёт вызывающий
/// (design D1 harden-network-resilience): политика «сколько готовы
/// прочитать» живёт у потребителя, транспорт только исполняет.
/// Трейт существует ради fake-транспорта в юнит-тестах; продакшн —
/// [`UreqCovers`].
trait CoverHttp: Send {
    /// Поисковый ответ как текст; тело читается с тем же лимитом.
    fn get_text(&self, url: &str, limit_bytes: u64) -> Result<String, CoverError> {
        let bytes = self.get_bytes(url, limit_bytes)?;
        // Ответы каталогов — UTF-8; не-UTF-8 байты не роняют поиск.
        Ok(String::from_utf8_lossy(&bytes).into_owned())
    }
    fn get_bytes(&self, url: &str, limit_bytes: u64) -> Result<Vec<u8>, CoverError>;
}

/// Сервис цепочки поиска (design D1): эндпоинт, путь к обложке в ответе
/// и апгрейд размера — всё про один каталог.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Service {
    Itunes,
    Deezer,
    Yandex,
}

/// Порядок цепочки (спека album-art): iTunes — основной каталог,
/// Deezer — второй эшелон, Яндекс с fuzzy-поиском — последний для
/// отличий в написании (Deezer часть русскоязычного каталога скрывает
/// гео-фильтром — живой квирк 2026-11-02, работу для этой станции
/// делает Яндекс).
const CHAIN: [Service; 3] = [Service::Itunes, Service::Deezer, Service::Yandex];

impl Service {
    /// Имя для лога.
    fn name(self) -> &'static str {
        match self {
            Service::Itunes => "iTunes",
            Service::Deezer => "Deezer",
            Service::Yandex => "Яндекс.Музыка",
        }
    }

    /// URL поиска по паре исполнитель + название (design D1). Пробел —
    /// `+` у iTunes и `%20` у Deezer/Яндекса: эти двое надёжнее понимают
    /// `%20`; `page=0` у Яндекса обязателен — без него HTTP 400.
    fn search_url(self, artist: &str, title: &str) -> String {
        let term = utf8_percent_encode(&format!("{artist} {title}"), NON_ALPHANUMERIC).to_string();
        match self {
            Service::Itunes => format!(
                "https://itunes.apple.com/search?term={}&entity=song&limit=5",
                term.replace("%20", "+")
            ),
            Service::Deezer => format!("https://api.deezer.com/search?q={term}&limit=5"),
            Service::Yandex => {
                format!("https://api.music.yandex.net/search?text={term}&type=track&page=0")
            }
        }
    }

    /// Обложка из ответа поиска: первый кандидат списка с непустым
    /// ключом обложки — верх ранжирования сервиса. Пустой список или
    /// отсутствующая ветка — промах (`None`), не ошибка; мусор вместо
    /// JSON — ошибка сервиса.
    fn extract_cover(self, body: &str) -> Result<Option<String>, CoverError> {
        let json: Value = serde_json::from_str(body)
            .map_err(|e| CoverError::BadResponse(format!("ответ не JSON: {e}")))?;
        // Яндекс (живая проверка 2026-11-02): список кандидатов живёт в
        // `result.tracks.results` — схема неофициального API менялась
        // (`tracks.items` более нет), сам `coverUri` не изменился.
        let candidates = match self {
            Service::Itunes => json.get("results"),
            Service::Deezer => json.get("data"),
            Service::Yandex => json
                .get("result")
                .and_then(|result| result.get("tracks"))
                .and_then(|tracks| tracks.get("results")),
        };
        let Some(candidates) = candidates.and_then(Value::as_array) else {
            return Ok(None);
        };
        for item in candidates {
            // Deezer прячет обложку в album.cover_big; iTunes и Яндекс
            // держат ключ на самом кандидате.
            let holder = match self {
                Service::Deezer => item.get("album"),
                _ => Some(item),
            };
            let key = match self {
                Service::Itunes => "artworkUrl100",
                Service::Deezer => "cover_big",
                Service::Yandex => "coverUri",
            };
            let url = holder
                .and_then(|holder| holder.get(key))
                .and_then(Value::as_str)
                .filter(|url| !url.is_empty());
            if let Some(url) = url {
                return Ok(Some(self.upgrade(url)));
            }
        }
        Ok(None)
    }

    /// Апгрейд размера картинки в URL (design D1): сервисы отдают
    /// поисковые миниатюры, заменой сегмента получается большой размер.
    fn upgrade(self, url: &str) -> String {
        match self {
            Service::Itunes => url.replace("100x100bb", "600x600bb"),
            Service::Deezer => url.replace("/500x500-", "/1000x1000-"),
            // coverUri приходит без схемы, с плейсхолдером размера %%.
            Service::Yandex => format!("https://{}", url.replace("%%", "1000x1000")),
        }
    }

    /// CDN-хосты картинок сервиса (design D3 harden-network-resilience):
    /// URL картинки приходит из недоверенного ответа поиска — загружать
    /// можно только с родных CDN сервиса; чужой хост — промах сервиса.
    fn image_host_suffixes(self) -> &'static [&'static str] {
        match self {
            Service::Itunes => &["mzstatic.com"],
            Service::Deezer => &["dzcdn.net"],
            Service::Yandex => &["avatars.yandex.net"],
        }
    }

    /// Доверенный ли URL картинки (design D3): схема https и хост
    /// сервиса — совпадение по сегментам (`evil-dzcdn.net` и
    /// `mzstatic.com.evil.ru` не проходят); двойная защита к
    /// `https_only` агента.
    fn trusts_image_url(self, url: &str) -> bool {
        let Ok(uri) = url.parse::<Uri>() else {
            return false;
        };
        uri.scheme_str() == Some("https")
            && uri.host().is_some_and(|host| {
                // DNS нечувствителен к регистру и хвостовой FQDN-точке —
                // сравнение тоже, иначе честный ответ CDN в верхнем
                // регистре молча терял бы обложку (находка ревью).
                let host = host.trim_end_matches('.').to_ascii_lowercase();
                self.image_host_suffixes()
                    .iter()
                    .any(|suffix| host == *suffix || host.ends_with(&format!(".{suffix}")))
            })
    }
}

/// Причина недоверия к URL картинки для лога (design D3, находка
/// ревью): хост или схема — оператору видно, что именно не прошло и
/// какую константу allowlist'а править при смене CDN.
fn image_reject_reason(url: &str) -> String {
    let Ok(uri) = url.parse::<Uri>() else {
        return "URL не разбирается".to_owned();
    };
    match uri.host() {
        // https уже требует trusts_image_url — сюда попадает чужой хост.
        Some(host) if uri.scheme_str() == Some("https") => {
            format!("чужой хост {host}")
        }
        Some(host) => format!("хост {host} не по https"),
        None => "нет хоста".to_owned(),
    }
}

/// Продакшн-транспорт поверх ureq: свой агент с таймаутами сервисов
/// обложек, не-2xx — ошибка (design D3). Конфигурация радио-агента
/// `net.rs` не трогается — у того другие задачи.
struct UreqCovers {
    agent: ureq::Agent,
}

impl UreqCovers {
    fn new() -> Self {
        let agent = ureq::Agent::config_builder()
            .timeout_connect(Some(CONNECT_TIMEOUT))
            .timeout_recv_response(Some(RESPONSE_TIMEOUT))
            .timeout_per_read(Some(READ_TIMEOUT))
            .timeout_recv_body(Some(BODY_TIMEOUT))
            // Дисциплина стрим-агента net.rs (design D2
            // harden-network-resilience): редиректы запрещены — ответ
            // поиска не уводит загрузку на произвольный хост; не-https
            // отклоняется транспортом; резолв ограничен бюджетом.
            .max_redirects(0)
            .https_only(true)
            .timeout_resolve(Some(RESOLVE_TIMEOUT))
            .http_status_as_error(false)
            .build()
            .new_agent();
        Self { agent }
    }
}

/// Чтение тела с лимитом (design D1 harden-network-resilience):
/// `take(limit + 1)` различает «ровно лимит» и «сверх лимита» — при
/// превышении прочитан лишний байт, но чтение останавливается и память
/// ограничена `limit + 1`, а не скоростью канала за `BODY_TIMEOUT`.
fn read_limited(reader: impl Read, limit_bytes: u64) -> Result<Vec<u8>, CoverError> {
    let mut bytes = Vec::new();
    reader
        .take(limit_bytes + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| CoverError::Network(format!("чтение тела: {e}")))?;
    if bytes.len() as u64 > limit_bytes {
        // Огромный или враждебный ответ — вина ответа, не сети:
        // BadResponse ведёт лог по ветке «вернул мусор», а не «недоступен»
        // (находка ревью о таксономии CoverError).
        return Err(CoverError::BadResponse(format!(
            "превышен лимит тела {limit_bytes} байт"
        )));
    }
    Ok(bytes)
}

impl CoverHttp for UreqCovers {
    fn get_bytes(&self, url: &str, limit_bytes: u64) -> Result<Vec<u8>, CoverError> {
        let response = self
            .agent
            .get(url)
            .header("User-Agent", USER_AGENT)
            .call()
            .map_err(|e| CoverError::Network(e.to_string()))?;
        let status = response.status().as_u16();
        if !(200..300).contains(&status) {
            return Err(CoverError::Network(format!("HTTP {status}")));
        }
        read_limited(response.into_body().into_reader(), limit_bytes)
    }
}

/// Декодированная обложка: RGB8, строка за строкой. Декодирует воркер,
/// потребляет окно (`SharedPixelBuffer` в main.rs) — lib от Slint не
/// зависит (design D7).
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct CoverImage {
    pub width: u32,
    pub height: u32,
    /// RGB-байты, `width * height * 3` штук.
    pub pixels: Vec<u8>,
}

/// Декод JPEG (design D4): не декодируется — ошибка; мусор не
/// кешируется и показывается заглушка. Лимиты размера — чужой или
/// битый JPEG с гигантской декларацией размеров не аллоцирует сотни
/// мегабайтов (находка ревью: дефолт image — 512 МБ).
fn decode_jpeg(bytes: &[u8]) -> Result<CoverImage, CoverError> {
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(4096);
    limits.max_image_height = Some(4096);
    let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes))
        .with_guessed_format()
        .map_err(|e| CoverError::BadResponse(format!("не декодируется: {e}")))?;
    reader.limits(limits);
    let image = reader
        .decode()
        .map_err(|e| CoverError::BadResponse(format!("не декодируется: {e}")))?;
    let rgb = image.to_rgb8();
    Ok(CoverImage {
        width: rgb.width(),
        height: rgb.height(),
        pixels: rgb.into_raw(),
    })
}

/// Каталог кеша (design D4): `<XDG_CACHE_HOME|~/.cache>/sevfm/covers`.
/// По XDG пустые и относительные значения переменных игнорируются;
/// без абсолютного базового каталога (`None`) кеш отключён, поиск
/// продолжает работать (находка ревью: `HOME=""` давал кеш в CWD).
fn cache_dir() -> Option<PathBuf> {
    let base = match std::env::var_os("XDG_CACHE_HOME") {
        Some(dir) if !dir.is_empty() && Path::new(&dir).is_absolute() => PathBuf::from(dir),
        _ => {
            let home = std::env::var_os("HOME").filter(|home| !home.is_empty())?;
            PathBuf::from(home).join(".cache")
        }
    };
    Some(base.join("sevfm").join("covers"))
}

/// Имя файла кеша: sha256 от `artist|title` — файлобезопасное, без
/// коллизий и лимитов файловой системы (design D4).
fn cache_key(artist: &str, title: &str) -> String {
    let digest = Sha256::digest(format!("{artist}|{title}").as_bytes());
    format!("{digest:x}")
}

/// Путь файла кеша обложки трека, если файл существует (design D4
/// add-tray: `mpris:artUrl` — file:// на файл кеша). Кеш отключён или
/// обложки ещё нет — None; имя файла — sha256-ключ, путь безопасен
/// для URI без экранирования.
pub fn cached_cover_path(artist: &str, title: &str) -> Option<std::path::PathBuf> {
    let file = cache_dir()?.join(cache_key(artist, title) + ".jpg");
    file.is_file().then_some(file)
}

/// Обложка из кеша; попадание обновляет время последнего использования
/// записи (design D1 cover-cache-cleanup) — вытеснение по лимиту удаляет
/// давно неиспользованные первыми. Повреждённый файл удаляется и
/// считается промахом (design D4) — при следующей смене трек поищется
/// заново.
fn read_cached(dir: &Path, artist: &str, title: &str) -> Option<CoverImage> {
    let file = dir.join(cache_key(artist, title) + ".jpg");
    let bytes = std::fs::read(&file).ok()?;
    match decode_jpeg(&bytes) {
        Ok(image) => {
            touch_last_used(&file);
            Some(image)
        }
        Err(e) => {
            say(&format!(
                "файл кеша обложки повреждён ({e}), удаляю: {}",
                file.display()
            ));
            let _ = std::fs::remove_file(&file);
            None
        }
    }
}

/// Продление жизни записи кеша (design D1 cover-cache-cleanup): время
/// последнего использования — mtime, вытеснение (`enforce_cache_limit`)
/// удаляет по нему. `set_modified` есть только у открытого файла —
/// std-путь без новой зависимости (крейт filetime не нужен). Симлинки
/// не открываются (находка security-ревью): подменённая запись не должна
/// трогать mtime чужого файла. Отказ (права, шеринг Windows) — тихий
/// пропуск: политика деградирует до порядка добавления, поведение
/// остаётся корректным.
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

/// Запись в кеш — best effort (design D4): неудача не отменяет показ
/// обложки, только запись в лог. Временный файл + rename в том же
/// каталоге — атомарно, обрыв не оставляет половину; суффикс pid —
/// параллельный экземпляр приложения пишет свой temp (находка ревью),
/// осироты подчистит старт воркера. Возвращает размер записанных байтов:
/// воркер копит прирост для повторной проверки лимита (design D3
/// cover-cache-cleanup); неудача — `None`.
fn write_cached(dir: &Path, artist: &str, title: &str, bytes: &[u8]) -> Option<u64> {
    let key = cache_key(artist, title);
    let write = || -> std::io::Result<()> {
        std::fs::create_dir_all(dir)?;
        let temp = dir.join(format!(".{key}.{}.tmp", std::process::id()));
        std::fs::write(&temp, bytes)?;
        std::fs::rename(temp, dir.join(format!("{key}.jpg")))
    };
    match write() {
        Ok(()) => Some(bytes.len() as u64),
        Err(e) => {
            say(&format!("не удалось записать кеш обложки: {e}"));
            None
        }
    }
}

/// Подчистка осиротевших временных файлов кеша при старте воркера:
/// обрыв между write и rename оставляет `.*.tmp`; старше `STALE_TEMP_AGE`
/// — точно мусор, свежие (возможно, живого параллельного экземпляра)
/// не трогаем.
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

/// Вытеснение кеша по лимиту (design D4, спека album-art «Ограничение
/// кеша обложек»): при суммарном размере записей `*.jpg` сверх лимита
/// удаляются самые давно использовавшиеся — mtime обновляется попаданием
/// (`read_cached`), — пока размер не войдёт в лимит. Отказ удаления
/// отдельного файла — лог и следующая жертва (best effort, как запись
/// кеша); `*.tmp` не трогаем: свежие могут принадлежать параллельному
/// экземпляру, осиротевшие убирает `sweep_stale_temp_files`. `limit` —
/// параметр: продакшн передаёт `CACHE_LIMIT_BYTES`, тесты — малые числа.
fn enforce_cache_limit(dir: &Path, limit: u64) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // Нет каталога — нечего вытеснять; прочий отказ (права, путь
        // занят файлом) — одна строка в лог: граница размера не должна
        // умирать молча (находка ревью).
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return,
        Err(e) => {
            say(&format!(
                "не удалось прочитать каталог кеша обложек {}: {e}",
                dir.display()
            ));
            return;
        }
    };
    // (время последнего использования, размер, путь). `DirEntry::
    // metadata` — lstat-семантика: симлинки не собираются и не следуют
    // по ссылке (security-ревью: подменённая запись не должна раздувать
    // сумму размером цели и провоцировать вытеснение честных записей).
    let mut records: Vec<(SystemTime, u64, PathBuf)> = Vec::new();
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if !meta.is_file() || !entry.file_name().to_string_lossy().ends_with(".jpg") {
            continue;
        }
        let Ok(used) = meta.modified() else { continue };
        records.push((used, meta.len(), entry.path()));
    }
    let total: u64 = records.iter().map(|(_, len, _)| *len).sum();
    // В лимите — тихий выход: чистка не событие, пока лимит не превышен.
    let Some(mut excess) = total.checked_sub(limit) else {
        return;
    };
    records.sort_by_key(|(used, _, _)| *used);
    let (mut removed_files, mut removed_bytes) = (0u64, 0u64);
    for (_, len, path) in &records {
        if excess == 0 {
            break;
        }
        match std::fs::remove_file(path) {
            Ok(()) => {
                excess = excess.saturating_sub(*len);
                removed_files += 1;
                removed_bytes += *len;
            }
            // Уже нет — параллельный экземпляр или удаление повреждённой
            // записи: цель достигнута, не событие (находка ревью).
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                excess = excess.saturating_sub(*len);
                removed_files += 1;
            }
            Err(e) => say(&format!(
                "не удалось удалить запись кеша обложки {}: {e}",
                path.display()
            )),
        }
    }
    say(&format!(
        "кеш обложек: превышен лимит {limit} Б, удалено записей: {removed_files} ({removed_bytes} Б)"
    ));
}

/// Счётчик прироста кеша с порогом повторной проверки лимита
/// (design D3): воркер учитывает записанные байты, при пороге —
/// повторная проверка и сброс. Порог — поле: продакшн передаёт
/// `CACHE_RECHECK_DELTA_BYTES`, тесты — малые числа без 8 МиБ фикстур
/// (находка oracle-ревью: out-параметр `&mut u64` не выражал инвариант
/// сброса и не позволял протестировать границу в сборке).
struct CacheGrowth {
    since_check: u64,
    delta: u64,
}

impl CacheGrowth {
    /// Продакшн-счётчик с порогом из константы.
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

/// Очистка одного каталога кеша — слой [`clear_cover_cache`] ради
/// тестов без env-переменных: удаляет файлы записей `*.jpg` и
/// осиротевшие `*.tmp` (симлинки пропускаются — security-ревью), каталог
/// остаётся. Возвращает (удалено файлов, освобождённые байты, не
/// удалось удалить): частичная неудача обязана быть видна вызывающему —
/// тихий успех при несостоявшейся очистке вводит в заблуждение
/// (находка ревью).
fn clear_cover_cache_in(dir: &Path) -> (u32, u64, u32) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        // Нет каталога — пустая очистка, не ошибка (спека album-art);
        // прочий отказ — лог, нули.
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, 0, 0),
        Err(e) => {
            say(&format!(
                "не удалось прочитать каталог кеша обложек {}: {e}",
                dir.display()
            ));
            return (0, 0, 0);
        }
    };
    // `DirEntry::metadata` — lstat-семантика: симлинки пропускаются
    // (security-ревью), ни удаление, ни цель ссылки не затрагиваются.
    let (mut files, mut bytes, mut failed) = (0u32, 0u64, 0u32);
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let cached = meta.is_file()
            && (name.ends_with(".jpg") || (name.starts_with('.') && name.ends_with(".tmp")));
        if !cached {
            continue;
        }
        match std::fs::remove_file(entry.path()) {
            Ok(()) => {
                files += 1;
                bytes += meta.len();
            }
            // Уже нет — параллельный экземпляр: цель достигнута.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                files += 1;
            }
            Err(e) => {
                failed += 1;
                say(&format!(
                    "не удалось удалить файл кеша обложки {}: {e}",
                    entry.path().display()
                ));
            }
        }
    }
    (files, bytes, failed)
}

/// Полная очистка кеша обложек — флаг `--clear-cache` (спека album-art
/// «Ручная очистка кеша обложек», design D5). Возвращает (удалено
/// файлов, освобождённые байты, не удалось удалить) — печать и код
/// выхода выполняет бинарник, библиотека возвращает числа. Кеш отключён
/// (нет домашнего каталога) — успешная пустая очистка.
pub fn clear_cover_cache() -> (u32, u64, u32) {
    cache_dir()
        .map(|dir| clear_cover_cache_in(&dir))
        .unwrap_or((0, 0, 0))
}

/// Цепочка поиска (спека album-art): сервисы по порядку, первый
/// непустой URL побеждает; сбой сервиса — лог и следующий; полный
/// промах — `None`.
fn search_cover(http: &dyn CoverHttp, artist: &str, title: &str) -> Option<(Service, String)> {
    for service in CHAIN {
        let url = service.search_url(artist, title);
        let outcome = http
            .get_text(&url, SEARCH_BODY_LIMIT_BYTES)
            .and_then(|body| service.extract_cover(&body))
            .map(|cover| match cover {
                // URL картинки — из недоверенного ответа: загрузка только
                // с родного CDN сервиса (design D3), иначе промах сервиса
                // и цепочка идёт дальше (спека album-art); в логе — хост
                // и причина (находка ревью).
                Some(url) if !service.trusts_image_url(&url) => {
                    say(&format!(
                        "сервис обложек {}: URL картинки отклонён ({}) — пропущен",
                        service.name(),
                        image_reject_reason(&url)
                    ));
                    None
                }
                other => other,
            });
        match outcome {
            Ok(Some(cover)) => return Some((service, cover)),
            Ok(None) => {}
            // Формулировки различают диагностику: «недоступен» — смотреть
            // сеть, «вернул мусор» — смотреть формат ответа (находка
            // ревью: единая фраза уводила разбор схемы в проверку сети).
            Err(e @ CoverError::Network(_)) => {
                say(&format!(
                    "сервис обложек {} недоступен: {e}",
                    service.name()
                ));
            }
            Err(e @ CoverError::BadResponse(_)) => {
                say(&format!(
                    "сервис обложек {} вернул мусор: {e}",
                    service.name()
                ));
            }
        }
    }
    None
}

/// Одна загрузка обложки трека: кеш → цепочка → скачивание → декод →
/// кеш. Все исходы логируются (задача 4.2), внешне — `Found` или `Miss`.
/// `growth` — прирост кеша с прошлой проверки лимита: после записи
/// пороговое решение и сама проверка — здесь (design D3
/// cover-cache-cleanup), счётчик живёт в воркере.
fn load_cover(
    cache: Option<&Path>,
    http: &dyn CoverHttp,
    artist: &str,
    title: &str,
    growth: &mut CacheGrowth,
) -> CoverReply {
    if let Some(dir) = cache
        && let Some(image) = read_cached(dir, artist, title)
    {
        say(&format!("обложка: кеш — {artist} — {title}"));
        return CoverReply::Found(image);
    }
    let Some((service, cover_url)) = search_cover(http, artist, title) else {
        say(&format!(
            "обложка не найдена ({}): {artist} — {title}",
            chain_names()
        ));
        return CoverReply::Miss;
    };
    let bytes = match http.get_bytes(&cover_url, IMAGE_BODY_LIMIT_BYTES) {
        Ok(bytes) => bytes,
        Err(e) => {
            say(&format!(
                "не удалось скачать обложку {}: {e}",
                service.name()
            ));
            return CoverReply::Miss;
        }
    };
    let image = match decode_jpeg(&bytes) {
        Ok(image) => image,
        Err(e) => {
            say(&format!("обложка {} не декодируется: {e}", service.name()));
            return CoverReply::Miss;
        }
    };
    if let Some(dir) = cache
        && let Some(written) = write_cached(dir, artist, title, &bytes)
        && growth.record(written)
    {
        enforce_cache_limit(dir, CACHE_LIMIT_BYTES);
    }
    say(&format!("обложка: {} — {artist} — {title}", service.name()));
    CoverReply::Found(image)
}

/// Имена сервисов цепочки одной строкой — для записи о промахе.
fn chain_names() -> String {
    CHAIN
        .iter()
        .map(|service| service.name())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Ответ воркера потребителю: найденная обложка или заглушка.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CoverReply {
    Found(CoverImage),
    Miss,
}

/// Ответ воркера с генерацией запроса: свежесть проверяет потребитель
/// при дренировании ([`Covers::poll_reply`]).
struct Reply {
    generation: u64,
    outcome: CoverReply,
}

/// Запрос воркеру с генерацией запроса.
struct Request {
    generation: u64,
    artist: String,
    title: String,
}

/// Сервис обложек (design D5): фоновый воркер с каналом — запросы
/// неблокирующие, тракт и окно сеть не ждут. Запросы подаёт
/// [`Covers::track_changed`] из poll-потока потребителя, ответы
/// забираются опросом [`Covers::poll_reply`].
pub struct Covers {
    requests: Sender<Request>,
    replies: Receiver<Reply>,
    generation: Arc<AtomicU64>,
    /// Мёртв ли воркер (паника, design D6 harden-network-resilience):
    /// поднявшись, глушит новые запросы — обложки не критичны, но их
    /// исчезновение обязано быть объяснимым записью в логе, а не тишиной.
    dead: Arc<AtomicBool>,
}

impl Covers {
    /// Продакшн-сервис: ureq-транспорт и кеш в пользовательском каталоге.
    pub fn start() -> Self {
        Self::with_backend(cache_dir(), Box::new(UreqCovers::new()))
    }

    /// Сборка с заданным транспортом и каталогом кеша (`None` — без
    /// кеша): для fake-транспорта юнит-тестов.
    fn with_backend(cache: Option<PathBuf>, http: Box<dyn CoverHttp>) -> Self {
        let (request_tx, request_rx) = channel::<Request>();
        let (reply_tx, reply_rx) = channel::<Reply>();
        let generation = Arc::new(AtomicU64::new(0));
        let dead = Arc::new(AtomicBool::new(false));
        let worker_dead = dead.clone();
        // Отказ потока — деградация без обложек, не крах плеера: спека
        // требует независимость воспроизведения от обложек. Паника
        // воркера ловится (прецедент — symphonia в pipeline.rs): одна
        // запись в лог и флаг «мёртв»; respawn не делаем — паника может
        // быть систематической (баг декодера), перезапуск дал бы петлю.
        match std::thread::Builder::new()
            .name("cover-service".into())
            .spawn(move || {
                if let Err(payload) = catch_unwind(AssertUnwindSafe(|| {
                    worker_loop(cache, http, request_rx, reply_tx)
                })) {
                    let message = payload
                        .downcast_ref::<&str>()
                        .map(|s| (*s).to_owned())
                        .or_else(|| payload.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "причина неизвестна".into());
                    say(&format!("сервис обложек аварийно завершился: {message}"));
                    worker_dead.store(true, Ordering::SeqCst);
                }
            }) {
            Ok(_) => {}
            Err(e) => {
                say(&format!("не удалось запустить сервис обложек: {e}"));
                // Тот же контракт, что у паники (находка ревью): мёртвый
                // воркер — тихий пропуск запросов, а не накопление их в
                // осиротевшем канале на каждую смену трека.
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

    /// Смена трека: музыкальный трек — поиск обложки; не-музыка (вставка,
    /// трек без исполнителя, остановка) — заглушка без сети (спека
    /// album-art: не-музыка не ищется).
    pub fn track_changed(&self, track: Option<(&str, &str)>) {
        match track {
            Some((artist, title)) => self.request(artist, title),
            None => self.invalidate(),
        }
    }

    fn request(&self, artist: &str, title: &str) {
        // Мёртвый воркер (паника, design D6): авария уже залогирована
        // один раз, повторы тихо пропускаются до перезапуска.
        if self.dead.load(Ordering::SeqCst) {
            return;
        }
        // Снимок генерации после инкремента: любой последующий bump
        // (новый запрос или инвалидация) делает снимок устаревшим.
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        // Канал живёт, пока жив воркер; после его смерти запросы тихо
        // игнорируются — обложки не критичны для воспроизведения.
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

    /// Мёртв ли воркер (паника или отказ спавна): потребитель
    /// показывает заглушку сразу, не ожидая ответа, который не придёт
    /// (спека album-art «Диагностика аварии воркера»).
    pub fn is_dead(&self) -> bool {
        self.dead.load(Ordering::SeqCst)
    }

    /// Очередной ответ воркера без ожидания; `None` — ответов нет.
    ///
    /// Свежесть проверяется здесь, а не в воркере (находка ревью):
    /// все bump'ы генерации (`request`/`invalidate`) идут из одного
    /// потока потребителя — и этот же поток дренирует канал, поэтому
    /// сверка на его стороне атомарна и закрывает ответы, уже лежащие
    /// в канале, — TOCTOU «проверил-и-отправил» в воркере неустраним,
    /// а здесь его просто нет.
    pub fn poll_reply(&self) -> Option<CoverReply> {
        while let Ok(reply) = self.replies.try_recv() {
            if reply.generation == self.generation.load(Ordering::SeqCst) {
                return Some(reply.outcome);
            }
        }
        None
    }
}

/// Воркер (design D5): обрабатывает самый свежий запрос (дренирует
/// backlog — смена треков быстрее ответов не копит очередь) и шлёт
/// ответ с генерацией запроса; свежесть фильтрует потребитель.
/// Выход — закрытие канала запросов (потребитель dropped).
fn worker_loop(
    cache: Option<PathBuf>,
    http: Box<dyn CoverHttp>,
    requests: Receiver<Request>,
    replies: Sender<Reply>,
) {
    if let Some(dir) = cache.as_deref() {
        sweep_stale_temp_files(dir);
        // Стартовая проверка лимита (design D3 cover-cache-cleanup):
        // перезапуски начинают с лимита, долгие сессии — порог прироста
        // в load_cover.
        enforce_cache_limit(dir, CACHE_LIMIT_BYTES);
    }
    // Прирост кеша с прошлой проверки лимита (design D3).
    let mut growth = CacheGrowth::production();
    while let Ok(mut current) = requests.recv() {
        while let Ok(next) = requests.try_recv() {
            current = next;
        }
        let outcome = load_cover(
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

    /// Fake-транспорт: тексты и байты раздаются по первому вхождению
    /// подстроки в URL (URL детерминирован парой artist/title), каждый
    /// вызов записывается. `delay_ms` у текста — имитация медленного
    /// сервиса для тестов вытеснения.
    #[derive(Clone, Default)]
    struct FakeHttp {
        texts: Vec<(&'static str, Result<String, String>, u64)>,
        bytes: Vec<(&'static str, Result<Vec<u8>, String>)>,
        /// Каждое обращение — URL и переданный лимит тела: тесты политики
        /// лимитов (design D1) сверяют лимиты поиска и картинки.
        calls: Arc<Mutex<Vec<(String, u64)>>>,
    }

    impl FakeHttp {
        fn text(&mut self, needle: &'static str, body: &str) {
            self.texts.push((needle, Ok(body.to_owned()), 0));
        }

        fn text_err(&mut self, needle: &'static str, message: &str) {
            self.texts.push((needle, Err(message.to_owned()), 0));
        }

        /// Медленный успешный ответ — воркер задержится в get_text.
        fn text_slow(&mut self, needle: &'static str, body: &str, delay_ms: u64) {
            self.texts.push((needle, Ok(body.to_owned()), delay_ms));
        }

        fn jpeg(&mut self, needle: &'static str) {
            self.bytes.push((needle, Ok(tiny_jpeg())));
        }

        fn calls_of(&self, needle: &str) -> usize {
            self.calls
                .lock()
                .expect("замок вызовов не отравлен")
                .iter()
                .filter(|(url, _)| url.contains(needle))
                .count()
        }

        fn calls_total(&self) -> usize {
            self.calls.lock().expect("замок вызовов не отравлен").len()
        }

        /// Лимиты всех обращений по порядку — поиск/картинка.
        fn limits_snapshot(&self) -> Vec<u64> {
            self.calls
                .lock()
                .expect("замок вызовов не отравлен")
                .iter()
                .map(|(_, limit)| *limit)
                .collect()
        }
    }

    impl CoverHttp for FakeHttp {
        fn get_text(&self, url: &str, limit_bytes: u64) -> Result<String, CoverError> {
            self.calls
                .lock()
                .expect("замок вызовов не отравлен")
                .push((url.to_owned(), limit_bytes));
            let (_, outcome, delay_ms) = self
                .texts
                .iter()
                .find(|(needle, _, _)| url.contains(needle))
                .unwrap_or_else(|| panic!("у fake-транспорта нет текста для {url}"));
            if *delay_ms > 0 {
                std::thread::sleep(Duration::from_millis(*delay_ms));
            }
            outcome.clone().map_err(CoverError::Network)
        }

        fn get_bytes(&self, url: &str, limit_bytes: u64) -> Result<Vec<u8>, CoverError> {
            self.calls
                .lock()
                .expect("замок вызовов не отравлен")
                .push((url.to_owned(), limit_bytes));
            self.bytes
                .iter()
                .find(|(needle, _)| url.contains(needle))
                .map_or_else(
                    || {
                        Err(CoverError::Network(format!(
                            "у fake-транспорта нет байтов для {url}"
                        )))
                    },
                    |(_, outcome)| outcome.clone().map_err(CoverError::Network),
                )
        }
    }

    /// Мини-JPEG для тестов декода и кеша — собирается энкодером image.
    fn tiny_jpeg() -> Vec<u8> {
        let image = image::RgbImage::from_pixel(8, 6, image::Rgb([120, 40, 200]));
        let mut bytes = std::io::Cursor::new(Vec::new());
        image::codecs::jpeg::JpegEncoder::new(&mut bytes)
            .encode_image(&image)
            .expect("кодирование тестового JPEG неисправно");
        bytes.into_inner()
    }

    /// Ждёт ответ воркера с таймаутом — ответы асинхронные.
    fn await_reply(covers: &Covers, what: &str) -> CoverReply {
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if let Some(reply) = covers.poll_reply() {
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
            std::env::temp_dir().join(format!("sevfm-cover-test-{}-{label}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("каталог кеша теста создаётся");
        dir
    }

    /// Лимит тела (design D1 harden-network-resilience): `take(limit+1)`
    /// различает «ровно лимит» и «сверх лимита» — чтение останавливается
    /// на лимите + 1 байт, память не растёт со скоростью канала.
    #[test]
    fn read_limited_distinguishes_boundary() {
        let body = |len: usize| vec![0u8; len];
        let exact =
            read_limited(std::io::Cursor::new(body(100)), 100).expect("ровно лимит проходит");
        assert_eq!(exact.len(), 100);

        let err = read_limited(std::io::Cursor::new(body(101)), 100)
            .expect_err("лимит + 1 байт — ошибка");
        assert!(err.to_string().contains("лимит"), "{err}");

        assert!(
            read_limited(std::io::Cursor::new(Vec::new()), 100)
                .expect("пустое тело — валидный ответ")
                .is_empty()
        );
    }

    /// Поиск и картинка получают свои лимиты тела (design D1): fake
    /// фиксирует лимит каждого обращения.
    #[test]
    fn search_and_image_use_their_body_limits() {
        let mut http = FakeHttp::default();
        http.text(
            "itunes.apple.com/search",
            r#"{"results":[{"artworkUrl100":"https://is1-ssl.mzstatic.com/x/100x100bb.jpg"}]}"#,
        );
        http.jpeg("mzstatic");
        let reply = load_cover(
            None,
            &http,
            "Артист",
            "Трек",
            &mut CacheGrowth::production(),
        );
        assert!(matches!(reply, CoverReply::Found(_)), "{reply:?}");
        assert_eq!(
            http.limits_snapshot(),
            vec![SEARCH_BODY_LIMIT_BYTES, IMAGE_BODY_LIMIT_BYTES],
            "поиск — свой лимит, картинка — свой"
        );
    }

    /// Allowlist CDN (design D3 harden-network-resilience): родные хосты
    /// проходят, чужие и http — нет, независимо от вхождения суффикса в
    /// середину хоста (совпадение по сегментам).
    #[test]
    fn image_host_allowlist_is_segmental() {
        let cases = [
            (
                Service::Itunes,
                "https://is1-ssl.mzstatic.com/image/600x600bb.jpg",
                true,
            ),
            (Service::Itunes, "https://mzstatic.com.evil.ru/x.jpg", false),
            (
                Service::Deezer,
                "https://e-cdns-images.dzcdn.net/cover/1000x1000-x.jpg",
                true,
            ),
            (Service::Deezer, "https://evil-dzcdn.net/x.jpg", false),
            (
                Service::Yandex,
                "https://avatars.yandex.net/get-music-content/x/1000x1000",
                true,
            ),
            (Service::Yandex, "http://avatars.yandex.net/x", false),
            // Чужой CDN другого сервиса — тоже нет.
            (
                Service::Itunes,
                "https://e-cdns-images.dzcdn.net/x.jpg",
                false,
            ),
            // Регистр и хвостовая FQDN-точка — DNS нечувствительна к ним,
            // сравнение тоже (находка ревью: честный ответ в верхнем
            // регистре молча терял бы обложку).
            (
                Service::Itunes,
                "https://IS1-SSL.MZSTATIC.COM/image/600x600bb.jpg",
                true,
            ),
            (
                Service::Deezer,
                "https://e-cdns-images.dzcdn.net./cover/x.jpg",
                true,
            ),
        ];
        for (service, url, trusted) in cases {
            assert_eq!(service.trusts_image_url(url), trusted, "{url}");
        }
    }

    /// Чужой хост картинки — промах сервиса, цепочка идёт дальше (спека
    /// album-art «Ограниченные сетевые обращения»): iTunes возвращает URL
    /// с чужого хоста — загрузки нет, побеждает Deezer.
    #[test]
    fn untrusted_image_url_falls_through_to_next_service() {
        let mut http = FakeHttp::default();
        http.text(
            "itunes.apple.com/search",
            r#"{"results":[{"artworkUrl100":"https://evil.example.com/x/100x100bb.jpg"}]}"#,
        );
        http.text(
            "api.deezer.com/search",
            r#"{"data":[{"album":{"cover_big":"https://e-cdns-images.dzcdn.net/a/1000x1000-b.jpg"}}]}"#,
        );
        http.jpeg("dzcdn");
        let reply = load_cover(
            None,
            &http,
            "Артист",
            "Трек",
            &mut CacheGrowth::production(),
        );
        assert!(matches!(reply, CoverReply::Found(_)), "{reply:?}");
        assert_eq!(
            http.calls_of("evil.example.com"),
            0,
            "чужой хост не загружается"
        );
        assert_eq!(http.calls_of("itunes"), 1, "iTunes опрошен и отклонён");
        assert_eq!(http.calls_of("api.deezer.com"), 1, "поиск Deezer");
        assert_eq!(http.calls_of("dzcdn"), 1, "картинка Deezer с родного CDN");
    }

    /// URL поиска (задача 1.2): кириллица percent-кодирована, пробел —
    /// `+` у iTunes и `%20` у остальных, параметры по схеме design D1.
    #[test]
    fn search_urls_encode_cyrillic_terms() {
        let itunes = Service::Itunes.search_url("Владимир Трошин", "Юность моя");
        assert!(
            itunes.starts_with("https://itunes.apple.com/search?term="),
            "{itunes}"
        );
        assert!(itunes.ends_with("&entity=song&limit=5"), "{itunes}");
        assert!(itunes.contains('+'), "пробел — плюс: {itunes}");
        assert!(!itunes.contains("%20"), "{itunes}");

        let deezer = Service::Deezer.search_url("Владимир Трошин", "Юность моя");
        assert!(
            deezer.starts_with("https://api.deezer.com/search?q="),
            "{deezer}"
        );
        assert!(deezer.ends_with("&limit=5"), "{deezer}");
        assert!(deezer.contains("%20"), "пробел — %20: {deezer}");
        assert!(!deezer.contains('+'), "{deezer}");

        let yandex = Service::Yandex.search_url("Владимир Трошин", "Юность моя");
        assert!(
            yandex.starts_with("https://api.music.yandex.net/search?text="),
            "{yandex}"
        );
        assert!(yandex.ends_with("&type=track&page=0"), "{yandex}");
        assert!(!yandex.contains('+'), "{yandex}");

        // Кириллица нигде не осталась сырой.
        for url in [&itunes, &deezer, &yandex] {
            assert!(
                !url.chars().any(|c| ('\u{0400}'..='\u{04FF}').contains(&c)),
                "кириллица закодирована: {url}"
            );
        }
    }

    /// Разбор ответов (задача 1.3): первый кандидат с непустым ключом,
    /// апгрейд размеров, вложенность Deezer (album.cover_big), пустые
    /// и отсутствующие списки — промах, мусор — ошибка сервиса.
    #[test]
    fn extract_takes_first_candidate_and_upgrades_size() {
        // iTunes: кандидат без artworkUrl100 пропускается, размер
        // повышается.
        let itunes = r#"{"resultCount":2,"results":[{"artistId":1},{"artworkUrl100":"https://x/100x100bb.jpg"}]}"#;
        assert_eq!(
            Service::Itunes.extract_cover(itunes).unwrap(),
            Some("https://x/600x600bb.jpg".to_owned())
        );

        // Deezer: обложка в album, пустая обложка первого кандидата
        // пропускается.
        let deezer =
            r#"{"data":[{"album":{}},{"album":{"cover_big":"https://cdn/x/500x500-abc.jpg"}}]}"#;
        assert_eq!(
            Service::Deezer.extract_cover(deezer).unwrap(),
            Some("https://cdn/x/1000x1000-abc.jpg".to_owned())
        );

        // Яндекс: список кандидатов fuzzy-поиска под result.tracks.results
        // (схема живого API), coverUri без схемы с плейсхолдером %% —
        // берётся первый.
        let yandex = r#"{"result":{"tracks":{"results":[
            {"coverUri":"avatars.yandex.net/get-music-content/a%%"},
            {"coverUri":"avatars.yandex.net/get-music-content/b%%"}]}}}"#;
        assert_eq!(
            Service::Yandex.extract_cover(yandex).unwrap(),
            Some("https://avatars.yandex.net/get-music-content/a1000x1000".to_owned())
        );

        // Пустые списки и отсутствующие ветки — промах, не ошибка.
        for (service, body) in [
            (Service::Itunes, r#"{"resultCount":0,"results":[]}"#),
            (Service::Deezer, r#"{"data":[]}"#),
            (Service::Deezer, "{}"),
            (Service::Yandex, r#"{"result":{"tracks":{"results":[]}}}"#),
            (Service::Yandex, "{}"),
        ] {
            assert_eq!(
                service.extract_cover(body).unwrap(),
                None,
                "промах без ошибки: {service:?}"
            );
        }

        // Мусор вместо JSON — ошибка сервиса (цепочка продолжится).
        assert!(Service::Itunes.extract_cover("<html>").is_err());
    }

    /// Декод JPEG (задача 2.3): настоящий мини-JPEG декодируется с теми
    /// же размерами; мусор — ошибка.
    #[test]
    fn decode_jpeg_accepts_real_and_rejects_garbage() {
        let decoded = decode_jpeg(&tiny_jpeg()).expect("мини-JPEG декодируется");
        assert_eq!((decoded.width, decoded.height), (8, 6));
        assert_eq!(decoded.pixels.len(), 8 * 6 * 3);

        let garbage = decode_jpeg(b"<html>not a jpeg</html>");
        assert!(garbage.is_err(), "мусор не декодируется");
    }

    /// Цепочка (задача 2.2): первый непустой результат побеждает и
    /// остальные не опрашиваются; сбой и мусор сервиса не прерывают
    /// цепочку; промах всех — None.
    #[test]
    fn chain_first_hit_wins_and_failures_continue() {
        // Находка первым сервисом: Deezer и Яндекс не опрашиваются.
        // Хост картинки — родной CDN (design D3): allowlist пропускает.
        let mut http = FakeHttp::default();
        http.text(
            "itunes.apple.com",
            r#"{"results":[{"artworkUrl100":"https://is1-ssl.mzstatic.com/a/100x100bb.jpg"}]}"#,
        );
        http.jpeg("mzstatic.com/a/");
        let (service, url) = search_cover(&http, "Владимир", "Юность").expect("iTunes находит");
        assert_eq!(
            (service, url.as_str()),
            (
                Service::Itunes,
                "https://is1-ssl.mzstatic.com/a/600x600bb.jpg"
            )
        );
        assert_eq!(http.calls_of("deezer"), 0, "Deezer не опрашивается");
        assert_eq!(http.calls_of("yandex"), 0, "Яндекс не опрашивается");

        // Сбой сети iTunes → находка Deezer, Яндекс не тронут.
        let mut http = FakeHttp::default();
        http.text_err("itunes.apple.com", "сеть недоступна");
        http.text(
            "api.deezer.com",
            r#"{"data":[{"album":{"cover_big":"https://e-cdns-images.dzcdn.net/d/500x500-1.jpg"}}]}"#,
        );
        let (service, _) = search_cover(&http, "Владимир", "Юность").expect("Deezer добирает");
        assert_eq!(service, Service::Deezer);
        assert_eq!(http.calls_of("yandex"), 0, "Яндекс не опрашивается");

        // Мусорный JSON iTunes и промах Deezer → находка Яндексом.
        let mut http = FakeHttp::default();
        http.text("itunes.apple.com", "<html>");
        http.text("api.deezer.com", r#"{"data":[]}"#);
        http.text(
            "api.music.yandex.net",
            r#"{"result":{"tracks":{"results":[{"coverUri":"avatars.yandex.net/y%%"}]}}}"#,
        );
        let (service, url) = search_cover(&http, "Владимир", "Юность").expect("Яндекс добирает");
        assert_eq!(
            (service, url.as_str()),
            (Service::Yandex, "https://avatars.yandex.net/y1000x1000")
        );
        assert_eq!(http.calls_total(), 3, "опрошены все три сервиса");

        // Промах всех и сбой всех — None.
        let mut http = FakeHttp::default();
        for needle in ["itunes.apple.com", "api.deezer.com", "api.music.yandex.net"] {
            http.text(needle, "{}");
        }
        assert_eq!(search_cover(&http, "Владимир", "Юность"), None);
        let mut http = FakeHttp::default();
        for needle in ["itunes.apple.com", "api.deezer.com", "api.music.yandex.net"] {
            http.text_err(needle, "сеть недоступна");
        }
        assert_eq!(
            search_cover(&http, "Владимир", "Юность"),
            None,
            "сбой всех сервисов — промах"
        );
    }

    /// Кеш (задача 3.1): ключ стабилен и различает треки, запись
    /// читается и не оставляет временных файлов, повреждённый файл
    /// удаляется и считается промахом.
    #[test]
    fn cache_roundtrip_and_broken_file_cleanup() {
        let dir = test_cache_dir("roundtrip");

        assert_eq!(
            cache_key("Владимир", "Юность"),
            cache_key("Владимир", "Юность")
        );
        assert_ne!(
            cache_key("Владимир", "Юность"),
            cache_key("Владимир", "Другая")
        );

        write_cached(&dir, "Владимир", "Юность", &tiny_jpeg());
        let cached = read_cached(&dir, "Владимир", "Юность").expect("записанное читается");
        assert_eq!((cached.width, cached.height), (8, 6));

        // В каталоге только итоговый файл — временный переименовался.
        let entries = std::fs::read_dir(&dir)
            .expect("каталог кеша читается")
            .count();
        assert_eq!(entries, 1, "в кеше один файл");

        // Повреждённый файл — промах и удаление: следующий запрос трека
        // пойдёт в сеть заново.
        let file = dir.join(format!("{}.jpg", cache_key("Владимир", "Юность")));
        std::fs::write(&file, b"broken").expect("порча файла кеша");
        assert_eq!(read_cached(&dir, "Владимир", "Юность"), None);
        assert!(!file.exists(), "битый файл удалён");

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Установка времени последнего использования записи в тестах —
    /// тот же механизм, что `touch_last_used`, с явным моментом.
    fn set_last_used(path: &std::path::Path, when: SystemTime) {
        std::fs::File::options()
            .write(true)
            .open(path)
            .expect("запись кеша открывается")
            .set_modified(when)
            .expect("время использования обновляется");
    }

    /// Вытеснение по лимиту (design D4 cover-cache-cleanup): в лимите —
    /// тихий no-op; сверх лимита уходят самые давно использовавшиеся и
    /// ровно до лимита; `.tmp` и не-jpg не тронуты, каталог остаётся.
    #[test]
    fn enforce_cache_limit_evicts_oldest_until_limit() {
        let dir = test_cache_dir("evict");
        // Содержимое записей не важно — вытеснение не декодирует.
        let old = dir.join("old.jpg");
        let mid = dir.join("mid.jpg");
        let new = dir.join("new.jpg");
        for file in [&old, &mid, &new] {
            std::fs::write(file, vec![0u8; 1000]).expect("запись кеша");
        }
        let stray = dir.join("note.txt");
        let temp = dir.join(".pending.tmp");
        std::fs::write(&stray, b"stray").expect("посторонний файл");
        std::fs::write(&temp, b"tmp").expect("временный файл");
        // Симлинк-обманка на большой файл (security-ревью): если сбор
        // записей следует по ссылке, сумма раздувается до 13 000 и
        // «ровно в лимите» ниже превращается в вытеснение.
        #[cfg(unix)]
        {
            std::fs::write(dir.join("big.bin"), vec![0u8; 10_000]).expect("большой файл");
            std::os::unix::fs::symlink("big.bin", dir.join("fake.jpg")).expect("симлинк-обманка");
        }
        let now = SystemTime::now();
        set_last_used(&old, now - Duration::from_secs(3000));
        set_last_used(&mid, now - Duration::from_secs(2000));
        set_last_used(&new, now - Duration::from_secs(1000));

        // Ровно в лимите — ничего не тронуто.
        enforce_cache_limit(&dir, 3000);
        for file in [&old, &mid, &new, &stray, &temp] {
            assert!(file.is_file(), "в лимите файл жив: {}", file.display());
        }
        #[cfg(unix)]
        assert!(
            dir.join("fake.jpg").symlink_metadata().is_ok(),
            "симлинк-обманка не считается записью"
        );

        // 3000 → 2500: уходит самая старая, свежие и посторонние живы.
        enforce_cache_limit(&dir, 2500);
        assert!(!old.exists(), "самая старая запись вытеснена");
        assert!(mid.is_file() && new.is_file(), "свежие записи остались");
        assert!(
            stray.is_file() && temp.is_file(),
            "не-jpg и .tmp не тронуты"
        );

        // Лимит 0 — удаляются все записи, каталог остаётся.
        enforce_cache_limit(&dir, 0);
        assert!(!mid.exists() && !new.exists());
        assert!(dir.is_dir(), "каталог кеша остаётся");
        #[cfg(unix)]
        assert!(
            dir.join("fake.jpg").symlink_metadata().is_ok() && dir.join("big.bin").is_file(),
            "симлинк и его цель не тронуты"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Отказ удаления (спека album-art «Отказ удаления»): read-only
    /// каталог не даёт unlink — чистка не паникует, файл жив; после
    /// восстановления прав вытеснение работает. Unix--only (chmod).
    #[cfg(unix)]
    #[test]
    fn enforce_cache_limit_survives_unremovable_file() {
        use std::os::unix::fs::PermissionsExt;

        let dir = test_cache_dir("evict-ro");
        let file = dir.join("a.jpg");
        std::fs::write(&file, vec![0u8; 100]).expect("запись кеша");

        // unlink требует права записи на каталог: 0o555 — только чтение.
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
            .expect("каталог переведён в read-only");
        enforce_cache_limit(&dir, 0);
        assert!(file.is_file(), "unlink не удался — файл жив, паники нет");

        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))
            .expect("права восстановлены");
        enforce_cache_limit(&dir, 0);
        assert!(
            !file.exists(),
            "после восстановления прав вытеснение работает"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// LRU (design D1 cover-cache-cleanup): попадание обновляет время
    /// использования записи — «старая, но недавно звучавшая» живёт дольше
    /// более свежей нетронутой. Read-only запись читается (отказ touch
    /// молча — Unix).
    #[test]
    fn cache_hit_renews_last_used_and_saves_from_eviction() {
        let dir = test_cache_dir("lru");
        write_cached(&dir, "Владимир", "Юность", &tiny_jpeg());
        write_cached(&dir, "Анна", "Другая", &tiny_jpeg());
        let a = dir.join(format!("{}.jpg", cache_key("Владимир", "Юность")));
        let b = dir.join(format!("{}.jpg", cache_key("Анна", "Другая")));
        let now = SystemTime::now();
        set_last_used(&a, now - Duration::from_secs(3000));
        set_last_used(&b, now - Duration::from_secs(1000));

        assert!(
            read_cached(&dir, "Владимир", "Юность").is_some(),
            "запись читается"
        );
        let (used_a, used_b) = (
            std::fs::metadata(&a)
                .expect("метаданные живы")
                .modified()
                .unwrap(),
            std::fs::metadata(&b)
                .expect("метаданные живы")
                .modified()
                .unwrap(),
        );
        assert!(used_a > used_b, "hit обновил время использования записи");

        // Лимит на одну запись: вытесняется давно не звучавшая «Другая».
        let limit = std::fs::metadata(&a).expect("метаданные живы").len();
        enforce_cache_limit(&dir, limit);
        assert!(a.is_file(), "недавно использованная запись жива");
        assert!(!b.exists(), "нетронутая запись вытеснена");

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&a, std::fs::Permissions::from_mode(0o444))
                .expect("запись переведена в read-only");
            assert!(
                read_cached(&dir, "Владимир", "Юность").is_some(),
                "чтение read-only записи работает (touch молча пропущен)"
            );
            std::fs::set_permissions(&a, std::fs::Permissions::from_mode(0o644))
                .expect("права восстановлены");
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Порог повторной проверки лимита (design D3) и размер записи
    /// (задача 1.4): прирост копится и срабатывает на границе дельты,
    /// счётчик сбрасывается проверкой; запись возвращает размер,
    /// неудача — `None`.
    #[test]
    fn recheck_threshold_and_written_size() {
        // Граница порога: дельта минус один — ещё нет, дельта — пора;
        // срабатывание сбрасывает счётчик (после проверки прирост
        // начинается заново).
        let mut growth = CacheGrowth::production();
        assert!(!growth.record(CACHE_RECHECK_DELTA_BYTES - 1));
        assert!(growth.record(1), "ровно на дельте — проверка");
        assert!(!growth.record(0), "после проверки счётчик с нуля");

        // Малая дельта — накопление через несколько записей.
        let mut growth = CacheGrowth {
            since_check: 0,
            delta: 10,
        };
        assert!(!growth.record(4) && !growth.record(5));
        assert!(growth.record(1), "4+5+1 достигли дельты 10");

        let dir = test_cache_dir("write-size");
        let jpeg = tiny_jpeg();
        assert_eq!(
            write_cached(&dir, "Владимир", "Юность", &jpeg),
            Some(jpeg.len() as u64)
        );
        // Путь каталога занят файлом — create_dir_all падает: None.
        let occupied = std::env::temp_dir().join(format!(
            "sevfm-cover-test-{}-write-size-file",
            std::process::id()
        ));
        std::fs::write(&occupied, b"not a dir").expect("файл на месте каталога");
        assert_eq!(
            write_cached(&occupied, "Владимир", "Юность", &jpeg),
            None,
            "неудача записи — None"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_file(&occupied);
    }

    /// Полная очистка каталога кеша (design D5): записи и осиротевшие
    /// `.tmp` удаляются с верными счётчиками, каталог, посторонние файлы
    /// и симлинки остаются; отсутствующий каталог — пустая очистка;
    /// неудачи удаления видны в третьем элементе (находка ревью).
    #[test]
    fn clear_cover_cache_in_removes_only_cache_files() {
        let dir = test_cache_dir("clear");
        let jpeg_bytes = vec![0u8; 4096];
        std::fs::write(dir.join("a.jpg"), &jpeg_bytes).expect("запись кеша");
        std::fs::write(dir.join("b.jpg"), &jpeg_bytes).expect("запись кеша");
        std::fs::write(dir.join(".orphan.tmp"), b"tmp").expect("осиротевший tmp");
        std::fs::write(dir.join("note.txt"), b"x").expect("посторонний файл");
        std::fs::create_dir(dir.join("nested")).expect("подкаталог");
        std::fs::write(dir.join("nested/inner.jpg"), b"x").expect("файл подкаталога");
        #[cfg(unix)]
        std::os::unix::fs::symlink("../nested/inner.jpg", dir.join("link.jpg"))
            .expect("симлинк-обманка");

        let (files, bytes, failed) = clear_cover_cache_in(&dir);
        assert_eq!(files, 3, "две записи и один tmp");
        assert_eq!(bytes, jpeg_bytes.len() as u64 * 2 + 3);
        assert_eq!(failed, 0, "успешная очистка без неудач");
        assert!(dir.join("note.txt").is_file(), "посторонний файл жив");
        assert!(
            dir.join("nested/inner.jpg").is_file(),
            "подкаталог не обходится"
        );
        #[cfg(unix)]
        assert!(
            dir.join("link.jpg").symlink_metadata().is_ok(),
            "симлинк не трогается: ни удалением, ни целью"
        );
        assert!(dir.is_dir(), "каталог кеша остаётся");

        // Пустой и отсутствующий каталог — пустая очистка.
        assert_eq!(clear_cover_cache_in(&dir), (0, 0, 0));
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(clear_cover_cache_in(&dir), (0, 0, 0));
    }

    /// Неудачи удаления видны вызывающему (находка ревью): read-only
    /// каталог не даёт unlink — удалено 0, все отказы посчитаны,
    /// паники нет. Unix--only (chmod).
    #[cfg(unix)]
    #[test]
    fn clear_cover_cache_in_counts_failures() {
        use std::os::unix::fs::PermissionsExt;

        let dir = test_cache_dir("clear-fail");
        std::fs::write(dir.join("a.jpg"), vec![0u8; 16]).expect("запись кеша");
        std::fs::write(dir.join("b.jpg"), vec![0u8; 16]).expect("запись кеша");

        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555))
            .expect("каталог переведён в read-only");
        let (files, bytes, failed) = clear_cover_cache_in(&dir);
        assert_eq!((files, bytes, failed), (0, 0, 2), "обе записи не удалены");

        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755))
            .expect("права восстановлены");
        assert_eq!(clear_cover_cache_in(&dir).0, 2, "после прав — удалены");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Кеш в сервисе (задача 3.2): первый запрос ходит в сеть, повтор
    /// того же трека обслуживается кешем без вызовов транспорта.
    #[test]
    fn cache_hit_skips_network() {
        let dir = test_cache_dir("service");

        let mut http = FakeHttp::default();
        http.text(
            "itunes.apple.com",
            r#"{"results":[{"artworkUrl100":"https://is1-ssl.mzstatic.com/a/100x100bb.jpg"}]}"#,
        );
        http.jpeg("mzstatic.com/a/");
        let calls = http.calls.clone();
        let covers = Covers::with_backend(Some(dir.clone()), Box::new(http));

        covers.request("hit", "x");
        assert!(matches!(
            await_reply(&covers, "первый поиск"),
            CoverReply::Found(_)
        ));
        let after_first = calls.lock().expect("замок вызовов не отравлен").len();
        assert!(after_first >= 2, "поиск и скачивание сходили в сеть");

        covers.request("hit", "x");
        assert!(matches!(
            await_reply(&covers, "повтор из кеша"),
            CoverReply::Found(_)
        ));
        assert_eq!(
            calls.lock().expect("замок вызовов не отравлен").len(),
            after_first,
            "повтор обслужен кешем без сети"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Воркер (задача 4.1 + находка ревью P1): медленный запрос
    /// вытесняется свежим — доставляется только свежий; инвалидация
    /// гасит даже ответ, уже лежащий в канале; не-музыка в сеть
    /// не ходит.
    #[test]
    fn worker_delivers_only_latest_request() {
        // «hit» находит обложку, «miss» — нет: различаем ответы.
        let mut http = FakeHttp::default();
        http.text(
            "term=hit",
            r#"{"results":[{"artworkUrl100":"https://is1-ssl.mzstatic.com/a/100x100bb.jpg"}]}"#,
        );
        http.text("term=miss", r#"{"resultCount":0,"results":[]}"#);
        // Медленный iTunes для любого запроса «slow…»: воркер застрянет
        // в get_text, свежий запрос тем временем сменит генерацию.
        http.text_slow("term=slow", r#"{"results":[]}"#, 300);
        // Промах первого сервиса проходит цепочку дальше — хвостовые
        // сервисы отвечают пустыми результатами.
        http.text("api.deezer.com", r#"{"data":[]}"#);
        http.text("api.music.yandex.net", "{}");
        http.jpeg("mzstatic.com/a/");
        let covers = Covers::with_backend(None, Box::new(http));

        // Штатные запросы-ответы.
        covers.request("hit", "x");
        assert!(matches!(
            await_reply(&covers, "нахождение"),
            CoverReply::Found(_)
        ));
        covers.request("miss", "x");
        assert_eq!(await_reply(&covers, "промах"), CoverReply::Miss);

        // Медленный запрос вытесняется свежим: устаревший ответ воркер
        // отсылает, но дренирование его гасит по генерации.
        covers.request("slowartist", "x");
        std::thread::sleep(Duration::from_millis(50)); // воркер вошёл в get_text
        covers.request("miss", "x");
        assert_eq!(await_reply(&covers, "свежий запрос"), CoverReply::Miss);
        assert_eq!(covers.poll_reply(), None, "второго (медленного) ответа нет");

        // Инвалидация гасит незавершённый поиск.
        covers.request("slowartist", "x");
        std::thread::sleep(Duration::from_millis(50));
        covers.invalidate();
        std::thread::sleep(Duration::from_millis(400)); // медленный запрос завершился
        assert_eq!(covers.poll_reply(), None, "ответ погашен инвалидацией");

        // Регрессия находки ревью P1: ответ, уже лежащий в канале до
        // инвалидации (воркер успел завершиться), всё равно не
        // применяется — фильтр генерации на дренировании.
        covers.request("hit", "x");
        assert!(matches!(
            await_reply(&covers, "пополнение канала"),
            CoverReply::Found(_)
        ));
        std::thread::sleep(Duration::from_millis(50)); // ответ лежит в канале
        covers.invalidate();
        assert_eq!(
            covers.poll_reply(),
            None,
            "лежавший в канале ответ погашен инвалидацией"
        );
    }

    /// Паника воркера (design D6 harden-network-resilience, спека
    /// album-art «Диагностика аварии воркера»): авария фиксируется
    /// флагом, последующие запросы тихо пропускаются — обращений к
    /// транспорту нет, ответов нет.
    #[test]
    fn panicked_worker_disables_service_silently() {
        /// Транспорт, падающий на первом же обращении (модель бага
        /// декодера/библиотеки внутри воркера).
        struct PanicHttp {
            calls: Arc<Mutex<Vec<String>>>,
        }
        impl CoverHttp for PanicHttp {
            fn get_text(&self, url: &str, _limit_bytes: u64) -> Result<String, CoverError> {
                self.calls
                    .lock()
                    .expect("замок вызовов не отравлен")
                    .push(url.to_owned());
                panic!("транспорт сломан");
            }
            fn get_bytes(&self, url: &str, _limit_bytes: u64) -> Result<Vec<u8>, CoverError> {
                self.calls
                    .lock()
                    .expect("замок вызовов не отравлен")
                    .push(url.to_owned());
                panic!("транспорт сломан");
            }
        }

        let calls = Arc::new(Mutex::new(Vec::new()));
        let covers = Covers::with_backend(
            None,
            Box::new(PanicHttp {
                calls: calls.clone(),
            }),
        );

        covers.track_changed(Some(("Артист", "Трек")));
        // Смерть воркера фиксируется флагом — публичный контракт для
        // потребителя: заглушка вместо ожидания ответа.
        let deadline = Instant::now() + Duration::from_secs(5);
        while !covers.is_dead() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(covers.is_dead(), "паника поймана и зафиксирована");

        // После аварии запросы не доходят до транспорта и ответов нет.
        covers.track_changed(Some(("Другой", "Трек")));
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            calls.lock().expect("замок вызовов не отравлен").len(),
            1,
            "после аварии обращений к транспорту нет"
        );
        assert_eq!(covers.poll_reply(), None, "ответов мёртвого воркера нет");
    }

    /// Не-музыка не ходит в сеть (задача 4.1): `track_changed(None)` —
    /// только инвалидация, транспорт не вызывается, ответов нет.
    #[test]
    fn non_music_skips_network() {
        let mut http = FakeHttp::default();
        http.text("itunes.apple.com", "{}"); // ни один вызов не ожидается
        let calls = http.calls.clone();
        let covers = Covers::with_backend(None, Box::new(http));

        covers.track_changed(None);
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            calls.lock().expect("замок вызовов не отравлен").len(),
            0,
            "не-музыка не порождает сетевых вызовов"
        );
        assert_eq!(
            covers.poll_reply(),
            None,
            "ответа нет — заглушка у потребителя"
        );
    }

    /// Живые сервисы обложек — реальная сеть, запуск вручную:
    /// `cargo test --lib cover::tests::live -- --ignored --nocapture`.
    /// Для каждого сервиса: известный трек находится, обложка
    /// скачивается и декодируется (эндпоинт, кодирование, путь в JSON,
    /// апгрейд размера, CDN). Плюс цепочка на треках эфира станции —
    /// победитель или честный промах, включая опечатку в названии для
    /// fuzzy-добора хвостовыми сервисами.
    #[test]
    #[ignore = "живая сеть: cargo test --lib cover::tests::live -- --ignored --nocapture"]
    fn live_services_find_download_and_decode() {
        let http = UreqCovers::new();
        // Известный трек для каждого каталога: у Deezer «Машина
        // времени» скрыта гео-фильтром (total есть, data пуст —
        // живой квирк 2026-11-02), поэтому проверочные треки — из
        // эфира станции.
        let known = [
            (Service::Itunes, ("Владимир Трошин", "ЮНОСТЬ МОЯ")),
            (Service::Deezer, ("Владимир Трошин", "Юность моя")),
            (Service::Yandex, ("Эдуард Хиль", "Я очень рад")),
        ];
        for (service, (artist, title)) in known {
            let url = service.search_url(artist, title);
            let body = http
                .get_text(&url, SEARCH_BODY_LIMIT_BYTES)
                .expect("поисковый запрос прошёл");
            let cover_url = service
                .extract_cover(&body)
                .expect("ответ парсится")
                .unwrap_or_else(|| panic!("{} не нашёл известный трек", service.name()));
            let bytes = http
                .get_bytes(&cover_url, IMAGE_BODY_LIMIT_BYTES)
                .expect("обложка скачалась");
            let image = decode_jpeg(&bytes).expect("обложка декодируется");
            println!(
                "живой {}: {cover_url} ({}x{})",
                service.name(),
                image.width,
                image.height
            );
        }

        for (artist, title) in [
            ("ВЛАДИМИР ТРОШИН", "ЮНОСТЬ МОЯ"),
            ("ВЛАДИМИР ТРОШИН", "ЮНОСТ МОЯ"), // опечатка — смотрим, кто добирает
            (
                "БОЛЬШОЙ ДЕТСКИЙ ХОР ВГТРК П/У В.ПОПОВА;ТАНЯ МЕЛИХОВА",
                "КРЫЛАТЫЕ КАЧЕЛИ",
            ),
            ("ЭДУАРД ХИЛЬ", "ДЕВУШКА В САПОЖКАХ"),
        ] {
            match search_cover(&http, artist, title) {
                Some((service, url)) => {
                    println!("цепочка {artist} — {title}: {} ({url})", service.name());
                }
                None => println!("цепочка {artist} — {title}: промах всех"),
            }
        }
    }

    /// Живой добор Яндексом при недоступных iTunes и Deezer: обёртка над
    /// реальным транспортом возвращает ошибку первым двум сервисам —
    /// цепочка обязана дойти до настоящего API Яндекса, найти, скачать
    /// и декодировать обложку. Запуск:
    /// `cargo test --lib cover::tests::live -- --ignored --nocapture`.
    #[test]
    #[ignore = "живая сеть: cargo test --lib cover::tests::live -- --ignored --nocapture"]
    fn live_yandex_when_first_two_services_down() {
        /// Реальный транспорт, но запросы к заблокированным сервисам —
        /// ошибка, как при их недоступности.
        struct BlockedHttp {
            real: UreqCovers,
            blocked: [&'static str; 2],
        }
        impl CoverHttp for BlockedHttp {
            fn get_text(&self, url: &str, limit_bytes: u64) -> Result<String, CoverError> {
                if self.blocked.iter().any(|needle| url.contains(needle)) {
                    return Err(CoverError::Network("сервис отключён тестом".into()));
                }
                self.real.get_text(url, limit_bytes)
            }

            fn get_bytes(&self, url: &str, limit_bytes: u64) -> Result<Vec<u8>, CoverError> {
                if self.blocked.iter().any(|needle| url.contains(needle)) {
                    return Err(CoverError::Network("сервис отключён тестом".into()));
                }
                self.real.get_bytes(url, limit_bytes)
            }
        }

        let http = BlockedHttp {
            real: UreqCovers::new(),
            blocked: ["itunes.apple.com", "api.deezer.com"],
        };
        let reply = load_cover(
            None,
            &http,
            "Эдуард Хиль",
            "Я очень рад",
            &mut CacheGrowth::production(),
        );
        let CoverReply::Found(image) = reply else {
            panic!("Яндекс не добрал обложку при недоступных iTunes и Deezer");
        };
        println!(
            "добор Яндексом при лежащих iTunes и Deezer: {}x{}",
            image.width, image.height
        );
    }
}

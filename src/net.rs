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

//! Reader-поток тракта (design D4/D6): `open_stream` → непрерывная запись
//! байтов в кольцевой буфер. Обрыв соединения, «молчание» сервера,
//! «капающая» скорость или мусорный лимит декодера (запрос через
//! [`Link`]) — переподключение по лестнице задержек 1 → 2 → 5 → далее
//! 10 с; звук из буфера при сетевых обрывах не прерывается. Сетевые
//! ошибки первого соединения — бесконечные попытки с логом (выход —
//! только по флагу остановки).
//!
//! Остановка: флаг опрашивается между короткими чтениями сокета, поэтому
//! блокированное чтение не тормозит выход дольше одного интервала опроса;
//! соединение закрывается drop'ом тела ответа при выходе из потока.

use std::io::{ErrorKind, Read};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::buffer::RingBuffer;
use crate::icy;

/// Размер чанка чтения из сокета. Публична для интеграционных тестов
/// (`tests/` — внешний крейт): единственный источник знания о размере.
pub const READ_CHUNK: usize = 16 * 1024;
/// Таймаут подключения (design D4: детект мёртвого соединения).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Таймаут фазы DNS-резолва (подключение начинается после резолва).
const RESOLVE_TIMEOUT: Duration = Duration::from_secs(5);
/// Интервал одного чтения тела ответа: чтение либо даёт байты, либо
/// возвращает управление за это время. Короткий интервал даёт reader-потоку
/// опрашивать флаг остановки без расшаривания сокета; «молчащий» сервер
/// детектируется составным no-progress-таймаутом ниже (design D4: 30 с).
/// Тот же интервал использует `BufferReader` декодера (pipeline.rs).
pub(crate) const READ_POLL_INTERVAL: Duration = Duration::from_millis(500);
/// Сколько соединение может молчать без единого байта, прежде чем оно
/// считается мёртвым (design D4: «молчащий» сервер). Складывается из
/// коротких чтений `READ_POLL_INTERVAL` (конфигурация агента в
/// `open_stream` ниже).
const NO_PROGRESS_TIMEOUT: Duration = Duration::from_secs(30);
/// Лестница задержек переподключения: 1 → 2 → 5 → далее 10 с (design D4).
const BACKOFF_LADDER: [Duration; 4] = [
    Duration::from_secs(1),
    Duration::from_secs(2),
    Duration::from_secs(5),
    Duration::from_secs(10),
];
/// Шаг прерываемого сна: остановка замечается быстро, без потери
/// величины задержки backoff.
const SLEEP_SLICE: Duration = Duration::from_millis(25);
/// Мусорный бюджет: столько байтов без единого распознаваемого кадра
/// закрывает соединение (design D4; ≈ 16 с потока — на живом радио кадры
/// каждые ~24 мс, ложных срабатываний нет).
/// Публична для интеграционных тестов (`tests/` — внешний крейт):
/// единственный источник знания о размере бюджета.
pub const GARBAGE_BUDGET_BYTES: usize = 256 * 1024;
/// Минимальная скорость живого соединения (design D4): за окно
/// `MIN_RATE_WINDOW` должно приходить не меньше `MIN_RATE_BYTES`, иначе
/// соединение считается деградировавшим («капающий» сервер) и
/// переподключается — спека запрещает бесконечно обрабатывать соединение
/// без прогресса. 2 КиБ/с — ниже самого экономного голосового MP3
/// (32 кбит/с); чистое молчание (0 байтов в окне) остаётся за
/// `NO_PROGRESS_TIMEOUT`.
const MIN_RATE_WINDOW: Duration = Duration::from_secs(10);
const MIN_RATE_BYTES: usize = 20 * 1024;
/// Разумный диапазон `icy-metaint`: реальные icecast-серверы шлют
/// 8192–65536. Значение вне 512..64 КиБ — договорённость вне практики
/// реальных серверов, трактуется как «без ICY»: поток продолжает
/// играть без метаданных (мелкий metaint = метаблоки в аудиопути →
/// мусорный бюджет переподключит — штатная защита design D4).
const METAINT_MIN_BYTES: usize = 512;
const METAINT_MAX_BYTES: usize = 64 * 1024;
/// Лимит байтов метаданных за окно `MIN_RATE_WINDOW`: реальное радио
/// шлёт ≈ 100–300 байтов/с; 64 КиБ за 10 с на порядки выше любого
/// живого вещателя, но легко достижимы вредоносным сервером с мелким
/// metaint — защита от CPU-DoS на парсинге метаблоков.
const META_RATE_LIMIT: usize = 64 * 1024;

/// User-Agent стрима.
const USER_AGENT: &str = "sevfm-radio/1.0";

/// Первая ступень backoff — точка знания для тестов (окна ожидания ретраев).
pub const FIRST_BACKOFF_DELAY: Duration = BACKOFF_LADDER[0];

/// Ошибка открытия стрима.
#[derive(Debug, thiserror::Error)]
pub enum StreamError {
    /// Ошибка запроса: сеть, TLS, DNS.
    #[error("ошибка запроса стрима: {0}")]
    Request(String),
    /// HTTP-статус отличен от 200.
    #[error("стрим недоступен: HTTP {0}")]
    Status(u16),
}

/// Сообщения ureq содержат сырой URL (включая userinfo) — вхождения
/// заменяются на редактированный URL, учётные данные в лог не попадают.
fn redact_url_in(message: &str, url: &str) -> String {
    // Двойная защита: точная замена по известному URL + вырезание
    // userinfo везде — ureq мог нормализовать URL, и тогда замена
    // молча не сработала бы.
    redact_userinfo_everywhere(&message.replace(url, &crate::redact_url(url)))
}

/// Вырезает userinfo (`user:pass@`) из всех authority в тексте: после
/// каждой `://` берётся отрезок до следующего `/` (или конца строки);
/// если в нём есть `@`, userinfo до последней `@` заменяется (логика
/// как в `crate::redact_url`). Ловит URL, нормализованные ureq'ом.
fn redact_userinfo_everywhere(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(pos) = rest.find("://") {
        let after = &rest[pos + 3..];
        let authority_end = after.find('/').unwrap_or(after.len());
        let authority = &after[..authority_end];
        result.push_str(&rest[..pos + 3]);
        match authority.rfind('@') {
            Some(at) => {
                result.push_str(&authority[at + 1..]);
            }
            None => {
                result.push_str(authority);
            }
        }
        rest = &after[authority_end..];
    }
    result.push_str(rest);
    result
}

/// GET-запрос URL стрима с User-Agent и запросом ICY-метаданных
/// (`Icy-MetaData: 1`). Тело ответа — `Read`: байты читаются по мере
/// поступления, без скачивания целиком — живой поток, а не файл.
///
/// Возвращает тело вместе с ICY-договорённостью соединения (design D1
/// фазы ICY): интервал метаблоков и название станции. Отсутствует
/// `icy-metaint` (сервер без ICY), нечисловой, нулевой или вне разумного
/// диапазона (см. `METAINT_MIN_BYTES`) — `metaint`
/// None: легитимный режим чистого MP3 без метаданных, не ошибка.
/// Публичен для интеграционных тестов (`tests/` — внешний крейт):
/// сценарии мок-сервера проверяются реальным клиентом тракта.
pub struct OpenedStream {
    pub body: Box<dyn Read + Send + Sync>,
    /// Интервал `icy-metaint`: байтов аудио между метаблоками.
    pub(crate) metaint: Option<usize>,
    /// Название станции (`icy-name`), если прислано.
    pub(crate) icy_name: Option<String>,
}

pub fn open_stream(url: &str) -> Result<OpenedStream, StreamError> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_connect(Some(CONNECT_TIMEOUT))
        // `timeout_per_read` капирует КАЖДОЕ чтение сокета, включая
        // ожидание заголовков в `call()`. Значит одиночный тихий
        // интервал > 500 мс в фазе заголовков рвёт попытку → штатный
        // ретрай с backoff (осознанное ограничение, design D4/D5:
        // короткий per_read нужен для отзывчивой остановки, тот же
        // интервал использует `BufferReader` декодера).
        .timeout_per_read(Some(READ_POLL_INTERVAL))
        // Суммарный бюджет фазы заголовков: защита от «бесконечного
        // drip» — сервер, отдающий по байту чаще 500 мс.
        .timeout_recv_response(Some(CONNECT_TIMEOUT))
        // Ограничивает фазу DNS-резолва; connect-бюджет начинается
        // после резолва.
        .timeout_resolve(Some(RESOLVE_TIMEOUT))
        // Радио — один фиксированный URL: авто-редиректы на произвольные
        // хосты не нужны, 3xx трактуется как обрыв соединения.
        .max_redirects(0)
        .http_status_as_error(false)
        .build()
        .new_agent();

    let response = agent
        .get(url)
        .header("User-Agent", USER_AGENT)
        .header("Icy-MetaData", "1")
        .call()
        .map_err(|e| StreamError::Request(redact_url_in(&e.to_string(), url)))?;

    let status = response.status().as_u16();
    if status != 200 {
        return Err(StreamError::Status(status));
    }
    // Заголовки ICY регистронезависимы (http::HeaderMap::get); значения
    // могут быть с пробелами — тримим перед разбором числа. Вне разумного
    // диапазона — как «без ICY» (комментарий у METAINT_*: мусорный
    // бюджет переподключит, поток не теряется).
    let metaint = response
        .headers()
        .get("icy-metaint")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<usize>().ok())
        .filter(|&interval| (METAINT_MIN_BYTES..=METAINT_MAX_BYTES).contains(&interval));
    // Название станции может быть не-ASCII (русские станции шлют UTF-8):
    // to_str() в http-крейте отвергает такие байты, читаем сырыми с
    // lossy-конверсией (Latin-1 станции покажутся искажённо — принятый
    // риск в design.md фазы ICY).
    let icy_name = response
        .headers()
        .get("icy-name")
        .map(|value| String::from_utf8_lossy(value.as_bytes()).into_owned());
    Ok(OpenedStream {
        body: Box::new(response.into_body().into_reader()),
        metaint,
        icy_name,
    })
}

/// Счётчик байтов от последнего распознаваемого MP3-кадра — мусорный
/// бюджет (design D4). Reader наращивает после каждой записи в буфер;
/// декодер сбрасывает после каждого успешно декодированного кадра (этим
/// же первым сбросом бюджет активируется); reader обнуляет без активации
/// при новом соединении. Превышение бюджета → reader закрывает
/// соединение и переподключается: ограничивает неограниченные ресинк-циклы
/// декодера на вечном мусоре.
///
/// До первого сброса от декодера бюджет соединение не рвёт: на холодном
/// старте reader мог бы набрать бюджет раньше, чем декодер сделает первый
/// decode-Ok (быстрое соединение + опрос статусов в main), — стартовую
/// мусорность ограничивают probe symphonia (1 МиБ), лимит фазы первого
/// кадра (4 МиБ/30 с) и лимит ошибок декодера (50 подряд).
pub struct GarbageBudget {
    /// Байты без распознаваемого кадра.
    bytes: AtomicUsize,
    /// Бюджет активирован первым сбросом декодера (первым кадром).
    armed: AtomicBool,
}

impl GarbageBudget {
    /// Разделяемый счётчик для reader-потока и декодера.
    pub fn shared() -> Arc<Self> {
        Arc::new(Self {
            bytes: AtomicUsize::new(0),
            armed: AtomicBool::new(false),
        })
    }

    /// Наращивает счётчик прочитанными из сокета байтами.
    pub(crate) fn add(&self, bytes: usize) {
        self.bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Обнуляет счётчик БЕЗ активации: новое соединение reader'а
    /// (декодер мог ещё не декодировать ни одного кадра).
    pub(crate) fn clear(&self) {
        self.bytes.store(0, Ordering::Relaxed);
    }

    /// Обнуляет счётчик и активирует бюджет (успешно декодированный
    /// кадр декодера — первый сброс тоже активирует).
    pub fn reset(&self) {
        self.bytes.store(0, Ordering::Relaxed);
        self.armed.store(true, Ordering::Relaxed);
    }

    /// Превышен ли мусорный бюджет (до активации первым сбросом
    /// декодера — нет).
    pub(crate) fn exceeded(&self) -> bool {
        self.armed.load(Ordering::Relaxed)
            && self.bytes.load(Ordering::Relaxed) > GARBAGE_BUDGET_BYTES
    }
}

/// Связка reader ↔ декодер (design D4: мусорный лимит лечится
/// переподключением, не завершением). Декодер, исчерпав лимит подряд
/// ошибок, запрашивает закрытие соединения (`request_reconnect`),
/// штампуя запрос эпохой соединения; reader нумерует установленные
/// соединения (`epoch`) — счётчик ошибок «подряд» живёт в рамках одного
/// соединения, а «протухший» запрос (соединение уже умерло само и
/// сменилось) не закрывает невинное новое соединение.
///
/// Поля — только атомарные флаги/счётчики без данных через них:
/// `Relaxed` достаточен и одинаков во всех точках.
#[derive(Default)]
pub struct Link {
    /// Эпоха соединения, которую декодер просит закрыть, плюс один
    /// (0 — запроса нет): сдвиг различает «нет запроса» и запрос для
    /// эпохи 0 (тесты без reader'а).
    reconnect_epoch: AtomicU64,
    /// Номер установленного соединения; растёт с каждым подключением.
    epoch: AtomicU64,
    /// Reader-поток умер паникой (ставит catch_unwind в spawn_reader):
    /// CLI классифицирует выход по флагу, а не по liveness потока —
    /// между закрытием буфера и смертью потока есть окно.
    reader_panicked: AtomicBool,
    /// Сервер ответил перманентным отказом (HTTP 3xx/4xx): повторение
    /// бессмысленно, поток завершается; CLI классифицирует выход по флагу.
    fatal: AtomicBool,
}

/// Разделяемая связка reader ↔ декодер.
pub type SharedLink = Arc<Link>;

impl Link {
    /// Разделяемая связка для reader-потока и декодера.
    pub fn shared() -> SharedLink {
        Arc::new(Self::default())
    }

    /// Запрос декодера: закрыть соединение `connection` (мусорный лимит).
    /// Штамп эпохой: запрос переживает естественную смерть соединения
    /// только как «протухший» — reader его отбросит на новом соединении.
    pub fn request_reconnect(&self, connection: u64) {
        self.reconnect_epoch
            .store(connection + 1, Ordering::Relaxed);
    }

    /// Есть ли незабранный запрос переподключения (наблюдение для тестов;
    /// reader забирает запрос сам в `pump`).
    // Вне тестов метод не зовётся: без `allow` не-тестовая сборка
    // считает метод мёртвым кодом.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn reconnect_requested(&self) -> bool {
        self.reconnect_epoch.load(Ordering::Relaxed) != 0
    }

    /// Reader: забрать запрос, если он адресован текущему соединению
    /// `current`. Чужая (протухшая) эпоха молча отбрасывается — декодер
    /// к этому моменту уже видит смену эпохи и выходит из вычитки.
    fn take_reconnect_request(&self, current: u64) -> bool {
        if self.reconnect_epoch.load(Ordering::Relaxed) == current + 1 {
            self.reconnect_epoch.store(0, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Reader умер паникой (выставляет catch_unwind; pub(crate) — для
    /// юнит-тестов `pipeline`, моделирующих аварию reader'а).
    pub(crate) fn mark_reader_panicked(&self) {
        self.reader_panicked.store(true, Ordering::Relaxed);
    }

    /// Умер ли reader-поток паникой (для классификации кода выхода).
    pub fn reader_panicked(&self) -> bool {
        self.reader_panicked.load(Ordering::Relaxed)
    }

    /// Reader: сервер отклонил запрос перманентно (HTTP 3xx/4xx) —
    /// повторные попытки бессмысленны. pub(crate) — юнит-тесты сессии
    /// моделируют отказ сервера этим же флагом.
    pub(crate) fn mark_fatal(&self) {
        self.fatal.store(true, Ordering::Relaxed);
    }

    /// Перманентный отказ сервера (для классификации кода выхода).
    pub fn fatal(&self) -> bool {
        self.fatal.load(Ordering::Relaxed)
    }

    /// Номер текущего соединения.
    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Reader (и тесты, моделирующие reader): новое соединение установлено.
    pub(crate) fn next_epoch(&self) {
        self.epoch.fetch_add(1, Ordering::Relaxed);
    }

    /// Сменилось ли соединение с последнего виденного номера (декодер:
    /// счётчик ошибок «подряд» обнуляется на новом соединении).
    pub(crate) fn connection_changed(&self, seen: u64) -> bool {
        self.epoch() != seen
    }
}

/// Сетевое состояние reader'а — часть сводного статуса воспроизведения
/// (спека radio-player: Подключение / Переподключение).
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum NetStatus {
    /// Устанавливается первое соединение (данных ещё не было).
    #[default]
    Connecting,
    /// Соединение живо, данные читаются.
    Streaming,
    /// Соединение потеряно — переподключение с backoff.
    Reconnecting,
}

/// Разделяемый сетевой статус (reader пишет, CLI читает для статусов).
pub type SharedNetStatus = Arc<std::sync::Mutex<NetStatus>>;

/// Сводное состояние воспроизведения для stderr (спека radio-player:
/// Подключение / Воспроизведение / Переподключение / Буферизация).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PlaybackStatus {
    /// Старт: первое соединение ещё не дало предбуфер.
    Connecting,
    /// Звук идёт из буфера.
    Playing,
    /// Соединение потеряно, переподключение; в буфере есть данные —
    /// звук не прерывается.
    Reconnecting,
    /// Буфер исчерпан — воспроизведение на паузе до повторного
    /// накопления предбуфера.
    Buffering,
}

impl PlaybackStatus {
    /// Имя состояния для вывода в stderr.
    pub fn label(self) -> &'static str {
        match self {
            PlaybackStatus::Connecting => "Подключение",
            PlaybackStatus::Playing => "Воспроизведение",
            PlaybackStatus::Reconnecting => "Переподключение",
            PlaybackStatus::Buffering => "Буферизация",
        }
    }
}

/// Сводит сетевое состояние и состояние буфера в статус воспроизведения.
/// `playback_started` — звук уже запускался; `decoder_paused` — декодер
/// в underrun-паузе (буфер опустошался и ждёт повторного предбуфера —
/// точный признак паузы звука, а не «буфер почти пуст, но звук идёт»).
pub fn playback_status(
    net: NetStatus,
    playback_started: bool,
    decoder_paused: bool,
) -> PlaybackStatus {
    if !playback_started {
        // Старт: сетевые ретраи первого соединения — всё ещё подключение.
        PlaybackStatus::Connecting
    } else if decoder_paused {
        // Долгий обрыв: пауза имеет приоритет — звука нет независимо
        // от состояния сети.
        PlaybackStatus::Buffering
    } else if net == NetStatus::Reconnecting {
        // Кратковременный обрыв: звук идёт из буфера.
        PlaybackStatus::Reconnecting
    } else {
        PlaybackStatus::Playing
    }
}

/// Запускает reader-поток для `url`. Ошибка — отказ ОС создать поток
/// (ulimit/память): вызывающий обязан завершиться с диагностикой.
/// Остановка — внешним флагом `stop` (его же слушает обработчик сигналов
/// в main). `budget` — разделяемый с декодером мусорный счётчик;
/// `link` — связка с декодером (запросы переподключения, эпоха соединений);
/// `net_status` — разделяемый сетевой статус для сводного статуса.
///
/// Поток завершается по флагу остановки или закрытии буфера изнутри
/// (downstream ушёл); сетевые ошибки никогда не завершают поток.
/// Паника внутри потока ловится: буфер закрывается (декодер дочитывает и
/// завершается, а не ждёт данные вечно), диагностику печатает стандартный
/// panic-хук; финальную классификацию выхода CLI делает по смерти потока.
pub fn spawn_reader(
    url: String,
    buffer: RingBuffer,
    stop: Arc<AtomicBool>,
    budget: Arc<GarbageBudget>,
    link: SharedLink,
    net_status: SharedNetStatus,
    now_playing: icy::SharedNowPlaying,
) -> std::io::Result<JoinHandle<()>> {
    std::thread::Builder::new()
        .name("net-reader".into())
        .spawn(move || {
            // AssertUnwindSafe: замки/атомы внутри reader_loop не зависят
            // от паник-безопасности заимствований — после паники поток
            // завершается, состояние связки больше не читается.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                reader_loop(
                    &url,
                    &buffer,
                    &stop,
                    &budget,
                    &link,
                    &net_status,
                    &now_playing,
                )
            }));
            if result.is_err() {
                // Сначала разблокируем тракт: сбой печати (закрытый stderr,
                // переполненный пайп) не должен отменять закрытие буфера,
                // иначе декодер вечно ждёт данные.
                buffer.close();
                link.mark_reader_panicked();
                crate::say("поток чтения аварийно завершён — воспроизведение будет остановлено");
            }
        })
}

/// Состояние reader-цикла, живущее между соединениями.
struct ReaderState {
    /// Индекс лестницы backoff: растёт с каждой неудачей, сбрасывается
    /// первым прочитанным байтом данных.
    backoff_index: usize,
    /// Было ли хоть одно рабочее соединение: ретраи до него — стартовое
    /// подключение, после — переподключение.
    had_connection: bool,
}

/// Главный цикл: подключение → перекачка → backoff → повтор.
fn reader_loop(
    url: &str,
    buffer: &RingBuffer,
    stop: &AtomicBool,
    budget: &GarbageBudget,
    link: &Link,
    net_status: &SharedNetStatus,
    now_playing: &icy::SharedNowPlaying,
) {
    let mut state = ReaderState {
        backoff_index: 0,
        had_connection: false,
    };
    // Флаг остановки: запись SeqCst (обработчик сигнала/тесты), чтение
    // здесь и в тракте — тем же порядком, без смеси.
    while !stop.load(Ordering::SeqCst) {
        // Новое соединение — свежий мусорный бюджет (без активации:
        // активирует только первый сброс декодера).
        budget.clear();
        match open_stream(url) {
            Ok(mut opened) => {
                // Новая эпоха: декодер обнуляет счётчик ошибок «подряд»
                // и возобновляет декодирование после мусорного лимита.
                link.next_epoch();
                // Станция печатается один раз: слот хранит название из
                // первого соединения, где оно пришло (станция не меняется,
                // повторные соединения молчат).
                {
                    let mut ether = now_playing
                        .lock()
                        .expect("замок эфира не отравлен: присваивание не паникует");
                    if ether.station.is_none() {
                        ether.station = opened.icy_name;
                    }
                }
                // Кадрировщик живёт одно соединение: договорённость
                // каждого нового ответа переустанавливается заново.
                let mut framer = icy::IcyFramer::new(opened.metaint);
                match pump(
                    &mut opened.body,
                    &mut framer,
                    now_playing,
                    buffer,
                    stop,
                    link,
                    budget,
                    net_status,
                    &mut state,
                ) {
                    PumpOutcome::Stopped | PumpOutcome::BufferClosed => return,
                    PumpOutcome::GarbageLimit => {
                        // Мусор декодера не сшиваем со свежим потоком:
                        // буфер очищается, новое соединение начнётся с
                        // чистого выравнивания кадров.
                        buffer.clear();
                        crate::say("мусорный лимит декодера — соединение закрывается");
                        set_net_status(net_status, state.had_connection);
                    }
                    PumpOutcome::GarbageBudget(reason) => {
                        // Мусор мёртвого соединения не сшиваем со свежим
                        // потоком: очищается, как при мусорном лимите
                        // декодера.
                        buffer.clear();
                        crate::say(&reason);
                        set_net_status(net_status, state.had_connection);
                    }
                    PumpOutcome::Failed(reason) => {
                        // Попытки разнесены задержками ≥ 1 с — троттлинг
                        // лога естественный, без счётчика времени.
                        crate::say(&reason);
                        set_net_status(net_status, state.had_connection);
                    }
                }
            }
            Err(e) => {
                if let StreamError::Status(code) = &e
                    && (300..500).contains(code)
                {
                    // Перманентный ответ сервера: повторение бессмысленно,
                    // поток завершается, CLI классифицирует по флагу Link.
                    // Буфер закрывается (как при панике): декодер дочитывает
                    // и завершается, а не ждёт данные от мёртвого тракта.
                    crate::say(&format!(
                        "сервер отклонил запрос: HTTP {code} — повторение бессмысленно"
                    ));
                    buffer.close();
                    link.mark_fatal();
                    return;
                }
                crate::say(&format!("подключение не удалось: {e}"));
                set_net_status(net_status, state.had_connection);
            }
        }
        if stop.load(Ordering::SeqCst) {
            return;
        }
        let delay = BACKOFF_LADDER[state.backoff_index.min(BACKOFF_LADDER.len() - 1)];
        state.backoff_index += 1;
        // До первого рабочего соединения это подключение, после —
        // переподключение.
        let action = if state.had_connection {
            "переподключение"
        } else {
            "подключение"
        };
        crate::say(&format!(
            "{action} через {delay:?} (попытка {})",
            state.backoff_index
        ));
        sleep_interruptible(delay, stop);
    }
}

/// Чем закончилась перекачка одного соединения.
enum PumpOutcome {
    /// Флаг остановки выставлен — поток завершается.
    Stopped,
    /// Буфер закрыт изнутри — downstream ушёл, поток завершается.
    BufferClosed,
    /// Декодер запросил переподключение (мусорный лимит): соединение
    /// закрывается, буфер очищается от мусора.
    GarbageLimit,
    /// Превышен мусорный бюджет reader'а: соединение закрывается,
    /// буфер очищается (мусор мёртвого соединения не сшивается со
    /// свежим потоком).
    GarbageBudget(String),
    /// Соединение потеряно — повод для переподключения.
    Failed(String),
}

/// Сетевой статус после исхода соединения: ретраи до первого успеха —
/// стартовое подключение, после — переподключение.
fn set_net_status(net_status: &SharedNetStatus, had_connection: bool) {
    let next = if had_connection {
        NetStatus::Reconnecting
    } else {
        NetStatus::Connecting
    };
    *net_status
        .lock()
        .expect("замок статуса не отравлен: присваивание не паникует") = next;
}

/// Пауза чтения без данных (таймаут одного чтения) — не ошибка.
///
/// ureq 3.x оборачивает пер-рид-таймаут в `io::ErrorKind::Other` с
/// вложенным `ureq::Error::Timeout`: один kind не отличает его от
/// настоящих ошибок, поэтому сверяем и kind, и вложенную ошибку.
fn is_read_timeout(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        ErrorKind::WouldBlock | ErrorKind::TimedOut | ErrorKind::Interrupted
    ) || e
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<ureq::Error>())
        .is_some_and(|ureq_err| matches!(ureq_err, ureq::Error::Timeout(_)))
}

/// Читает сокет и пишет в буфер, пока соединение живо. Между сокетом и
/// буфером — ICY-кадрировщик (design D2 фазы ICY): декодер получает
/// только аудиобайты, метаблоки уходят в парсер трека.
#[allow(clippy::too_many_arguments)]
fn pump(
    body: &mut dyn Read,
    framer: &mut icy::IcyFramer,
    now_playing: &icy::SharedNowPlaying,
    buffer: &RingBuffer,
    stop: &AtomicBool,
    link: &Link,
    budget: &GarbageBudget,
    net_status: &SharedNetStatus,
    state: &mut ReaderState,
) -> PumpOutcome {
    let mut chunk = vec![0u8; READ_CHUNK];
    let mut last_progress = Instant::now();
    // Окно минимальной скорости: стартует первым байтом, оценивается
    // ТОЛЬКО на границе окна (истёк срок) — бёрст-серверы (много разом,
    // затем пауза) со средней скоростью выше минимума не наказываются
    // серединой окна. Ноль байтов за окно — чистое молчание, его судит
    // NO_PROGRESS_TIMEOUT; окно просто перезапускается.
    let mut rate_window_start: Option<Instant> = None;
    let mut rate_window_bytes = 0usize;
    // Метабайты того же окна: лимит защищает CPU от meta-flood (сервер
    // с мелким metaint гоняет парсинг на скорости сети — сырые байты
    // окна такой трафик не ловят, мусорный бюджет метабайты не видит).
    let mut meta_window_bytes = 0usize;
    // Эпоха этого соединения: только reader-поток инкрементирует её,
    // за время перекачки номер не меняется.
    let epoch = link.epoch();
    loop {
        if stop.load(Ordering::SeqCst) {
            return PumpOutcome::Stopped;
        }
        // Запрос декодера (мусорный лимит) проверяется на каждой итерации:
        // чтение квантовано таймаутом, задержка реакции — один такт.
        // Запрос чужой (протухшей) эпохи молча отбрасывается.
        if link.take_reconnect_request(epoch) {
            return PumpOutcome::GarbageLimit;
        }
        // Граница окна: «капающий» сервер (байты в окне есть, но меньше
        // минимума) — деградация, соединение закрывается.
        if let Some(start) = rate_window_start
            && start.elapsed() >= MIN_RATE_WINDOW
        {
            if 0 < rate_window_bytes && rate_window_bytes < MIN_RATE_BYTES {
                return PumpOutcome::Failed(format!(
                    "скорость потока ниже минимальной (меньше {MIN_RATE_BYTES} байтов \
                     за {MIN_RATE_WINDOW:?}) — соединение деградировало"
                ));
            }
            rate_window_start = Some(Instant::now());
            rate_window_bytes = 0;
            if meta_window_bytes > META_RATE_LIMIT {
                return PumpOutcome::Failed(format!(
                    "поток метаданных превышает разумный объём ({META_RATE_LIMIT} байтов \
                     за {MIN_RATE_WINDOW:?}) — соединение закрывается"
                ));
            }
            meta_window_bytes = 0;
        }
        match body.read(&mut chunk) {
            Ok(0) => {
                return PumpOutcome::Failed("соединение закрыто сервером".into());
            }
            Ok(n) => {
                state.backoff_index = 0;
                state.had_connection = true;
                rate_window_start.get_or_insert_with(Instant::now);
                // Окно скорости считает сырые байты сокета: метаблоки —
                // тоже прогресс сервера (асимметрия с мусорным бюджетом
                // ниже — design D4 фазы ICY).
                rate_window_bytes += n;
                {
                    let mut status = net_status
                        .lock()
                        .expect("замок статуса не отравлен: присваивание не паникует");
                    *status = NetStatus::Streaming;
                }
                // Кадрирование до буфера: аудиоучастков на одно чтение
                // может быть несколько (метаблоки делят чанк); запись
                // блокируется на полном буфере (backpressure) дольше окна —
                // это наш простой, окно переанкеруется после разблокировки.
                let write_started = Instant::now();
                let mut audio_bytes = 0usize;
                let mut buffer_closed = false;
                framer.push(&chunk[..n], &mut |frame| match frame {
                    icy::Frame::Audio(seg) => {
                        audio_bytes += seg.len();
                        if !buffer_closed && buffer.write(seg).is_err() {
                            buffer_closed = true;
                        }
                    }
                    icy::Frame::Meta(block) => {
                        meta_window_bytes += block.len();
                        if let Some(track) = icy::parse_meta(&block) {
                            let mut ether = now_playing
                                .lock()
                                .expect("замок эфира не отравлен: присваивание не паникует");
                            ether.track = Some(track);
                        }
                    }
                });
                if buffer_closed {
                    return PumpOutcome::BufferClosed;
                }
                if write_started.elapsed() > READ_POLL_INTERVAL {
                    rate_window_start = Some(Instant::now());
                    rate_window_bytes = 0;
                    meta_window_bytes = 0;
                }
                last_progress = Instant::now();
                // Мусорный бюджет считает только аудиобайты: метаданные
                // декодер не видит, они не мусор его тракта. Никто не
                // сбросил бюджет успешным кадром — соединение несёт
                // сплошной мусор, закрываем (design D4).
                budget.add(audio_bytes);
                if budget.exceeded() {
                    return PumpOutcome::GarbageBudget(format!(
                        "мусорный бюджет: {GARBAGE_BUDGET_BYTES} байтов без \
                         распознаваемого кадра — соединение закрывается"
                    ));
                }
            }
            // Пауза без данных: не ошибка, пока соединение не молчит
            // дольше NO_PROGRESS_TIMEOUT.
            Err(e) if is_read_timeout(&e) => {
                if stop.load(Ordering::SeqCst) {
                    return PumpOutcome::Stopped;
                }
                if last_progress.elapsed() >= NO_PROGRESS_TIMEOUT {
                    return PumpOutcome::Failed(format!(
                        "сервер молчит {NO_PROGRESS_TIMEOUT:?} — соединение считается мёртвым"
                    ));
                }
            }
            Err(e) => {
                return PumpOutcome::Failed(format!("ошибка чтения: {e}"));
            }
        }
    }
}

/// Сон, прерываемый флагом остановки: полная задержка при живом потоке,
/// немедленный выход при остановке.
fn sleep_interruptible(delay: Duration, stop: &AtomicBool) {
    let deadline = Instant::now() + delay;
    while Instant::now() < deadline {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        std::thread::sleep(SLEEP_SLICE.min(deadline - Instant::now()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Точная замена по известному URL и ловля нормализованного URL,
    /// который `replace` уже не находит (двойная защита redact_url_in).
    #[test]
    fn redact_url_in_covers_exact_and_normalized_urls() {
        // Замена по точному URL срабатывает.
        assert_eq!(
            redact_url_in(
                "ошибка запроса: http://user:pass@host:8080/stream — сеть",
                "http://user:pass@host:8080/stream"
            ),
            "ошибка запроса: http://host:8080/stream — сеть"
        );
        // ureq нормализовал URL (decode '@' и т.п.) — replace не сработал,
        // но паттерн вырезает userinfo из сообщения целиком.
        assert_eq!(
            redact_url_in(
                "ошибка запроса: http://user:pass@host:8080/stream%20x",
                "http://user:pass@host:8080/stream x"
            ),
            "ошибка запроса: http://host:8080/stream%20x"
        );
    }

    /// `@` в пути — не userinfo: красакция его не трогает.
    #[test]
    fn redact_userinfo_leaves_at_in_path() {
        assert_eq!(
            redact_userinfo_everywhere("http://example.com/@mention и ещё"),
            "http://example.com/@mention и ещё"
        );
        assert_eq!(
            redact_userinfo_everywhere("http://u:p@a/ и http://b:q@c/d до конца без слэша x@y"),
            "http://a/ и http://c/d до конца без слэша x@y"
        );
    }

    /// Таблица сведений статусов (задача 4.1): старт, игра, обрыв при
    /// живом буфере, долгий обрыв с паузой — приоритет Буферизации.
    #[test]
    fn playback_status_matches_spec_states() {
        use NetStatus::{Connecting, Reconnecting, Streaming};
        use PlaybackStatus as P;

        // Старт: независимо от сети, пока звук не запущен, — Подключение.
        assert_eq!(playback_status(Connecting, false, true), P::Connecting);
        assert_eq!(playback_status(Reconnecting, false, true), P::Connecting);
        assert_eq!(playback_status(Streaming, false, false), P::Connecting);

        // Игра: данные есть, соединение живо.
        assert_eq!(playback_status(Streaming, true, false), P::Playing);

        // Кратковременный обрыв: звук не прерывается, буфер кормит.
        assert_eq!(playback_status(Reconnecting, true, false), P::Reconnecting);

        // Долгий обрыв: буфер пуст — пауза, даже если сеть уже вернулась.
        assert_eq!(playback_status(Reconnecting, true, true), P::Buffering);
        assert_eq!(playback_status(Streaming, true, true), P::Buffering);

        // Метки для stderr — по состояниям главного спека.
        assert_eq!(P::Connecting.label(), "Подключение");
        assert_eq!(P::Playing.label(), "Воспроизведение");
        assert_eq!(P::Reconnecting.label(), "Переподключение");
        assert_eq!(P::Buffering.label(), "Буферизация");
    }
}

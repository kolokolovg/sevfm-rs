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

//! Тракт фазы 1: покадровое декодирование (symphonia) живого MP3-потока,
//! читаемого из кольцевого буфера (наполняет reader-поток `net.rs`), →
//! источник сэмплов для rodio.
//!
//! Правила устойчивости (см. спеку `audio-pipeline` и design.md):
//! лимит подряд ошибок декодирования, лог ошибок не чаще раза в секунду,
//! формат аудио — из первого декодированного кадра. Радиопоток начинается
//! посреди кадра — symphonia сама ищет первый sync-маркер при probe.
//! Мусорный лимит в живом цикле НЕ завершает воспроизведение (спека:
//! переподключение, не выход): декодер запрашивает закрытие соединения
//! через `net::Link` и вычитывает байты закрытого соединения вхолостую,
//! пока не установится новое (смена эпохи).
//!
//! Фаза до первого декодированного кадра ограничена: probe — собственным
//! пределом symphonia (max_probe_depth, 1 МиБ → «не распознан»), а поиск
//! ПОСЛЕ probe (лестница ошибок на «кадрах» с мусорными телами: 50 ошибок
//! → переподключение → счётчик обнулён → снова 50, до бесконечности) —
//! лимитом байтов и дедлайном: поток без единого валидного кадра — ошибка
//! старта «не распознан как MP3» (код 2), а не вечный цикл.

use std::io::Read;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rodio::{ChannelCount, Sample, SampleRate, Source};
use symphonia::core::codecs::CodecParameters;
use symphonia::core::codecs::audio::AudioCodecParameters;
use symphonia::core::codecs::audio::AudioDecoder;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::codecs::audio::well_known::CODEC_ID_MP3;
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::FormatReader;
use symphonia::core::formats::Track;
use symphonia::core::formats::probe::Hint;
use symphonia::core::io::MediaSourceStream;
use symphonia::core::io::ReadOnlySource;
use symphonia::core::meta::MetadataOptions;
use symphonia::default::get_codecs;
use symphonia::default::get_probe;

use crate::buffer::{ReadError, RingBuffer};
use crate::net::{GarbageBudget, READ_POLL_INTERVAL, SharedLink};
use crate::say;

/// Лимит подряд ошибок декодирования (design D2: контроль ошибок на уровне кадра).
const MAX_CONSECUTIVE_DECODE_ERRORS: u32 = 50;
/// Интервал лога ошибок декодирования — не чаще раза в секунду.
const DECODE_ERROR_LOG_INTERVAL: Duration = Duration::from_secs(1);
/// Лимит фазы до первого декодированного кадра — сколько байтов суммарно
/// читает тракт, пока первого кадра нет (спека устойчивости: соединение
/// без прогресса не обрабатывается бесконечно). Probe ограничен сам
/// (symphonia max_probe_depth = 1 МиБ → `Unsupported`); этот лимит
/// закрывает остаток фазы — лестницу ошибок декодирования на «кадрах» с
/// мусорными телами, где переподключения обнуляют счётчик ошибок, но
/// первый кадр так и не появляется. Счётчик переживает переподключения
/// reader'а. 16 мусорных бюджетов reader'а (`net::GARBAGE_BUDGET_BYTES`):
/// с запасом выше предбуфера и любого разумного мусорного хвоста живого
/// среза.
const FIRST_FRAME_SCAN_LIMIT_BYTES: usize = 4 * 1024 * 1024;
/// Дедлайн той же фазы: покрывает медленные и рвущиеся соединения, где
/// байтовый лимит наполняется слишком медленно (согласован с
/// no-progress-таймаутом reader'а, design D4).
const FIRST_FRAME_DEADLINE: Duration = Duration::from_secs(30);

/// Адрес станции по умолчанию.
pub const DEFAULT_STREAM_URL: &str = "https://sevfm.ru:9006/govsev";

/// Путь фикстуры живого среза потока (256 КиБ) — единая точка для
/// юнит-тестов модуля и интеграционных тестов `tests/` (конвенция:
/// tests не дублирует знание библиотеки).
pub const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/stream-sample.mp3"
);

/// Чем закончился источник сэмплов.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Воспроизведение ещё идёт.
    Running,
    /// Источник байтов исчерпан (буфер закрыт и пуст) — штатный конец.
    EndOfStream,
    /// Источник байтов исчерпан остановкой (флаг stop при закрытии
    /// буфера) — не конец данных и не ошибка.
    Stopped,
    /// Ошибка декодера или источника данных, продолжение невозможно.
    DecoderError,
}

/// Разделяемый итог источника: источник уезжает в rodio, CLI читает
/// финальное состояние отсюда (мусорный лимит сюда не попадает — лечится
/// переподключением, см. `Mp3Source::refill`).
pub type SharedOutcome = Arc<Mutex<Outcome>>;

/// Разделяемый сигнал «первый кадр декодирован»: пока не поднят, чтения
/// тракта ограничены лимитом фазы поиска первого кадра; поднимается первым
/// декодированным кадром — живой цикл объёмом и временем не ограничен.
pub(crate) type FirstFrameDecoded = Arc<AtomicBool>;

/// Ошибка построения тракта декодирования (до первого кадра).
#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    /// Поток не распознан как MP3 (sync-маркер не найден).
    #[error("поток не распознан как MP3: {0}")]
    Unsupported(String),
    /// До первого валидного кадра исчерпан лимит подряд ошибок.
    #[error(
        "поток мусорный: {MAX_CONSECUTIVE_DECODE_ERRORS} подряд \
         ошибок декодирования до первого валидного кадра"
    )]
    GarbageStream,
    /// Фаза поиска первого кадра превысила лимит (байты или время):
    /// бесконечный мусор без sync-маркеров — поток не распознан как MP3,
    /// диагностика и ненулевой код, а не вечный поиск.
    #[error(
        "первый MP3-кадр не найден за {FIRST_FRAME_SCAN_LIMIT_BYTES} байтов \
         или {FIRST_FRAME_DEADLINE:?} поиска — поток не распознан как MP3"
    )]
    FirstFrameLimit,
    /// Поток закончился раньше первого валидного кадра.
    #[error("поток закончился до первого MP3-кадра")]
    EmptyStream,
    /// Авария тракта чтения до первого кадра: reader-поток умер паникой
    /// или сервер отклонил соединение навсегда — причина не в данных,
    /// «поток закончился» ввела бы в заблуждение.
    #[error("авария потока чтения до первого MP3-кадра")]
    ReaderCrashed,
    /// Ошибка ввода-вывода.
    #[error("ошибка чтения потока: {0}")]
    Io(String),
}

/// Источник PCM-сэмплов для rodio: symphonia поверх кольцевого буфера,
/// который наполняет reader-поток (design D4: воспроизведение из буфера,
/// а не напрямую из сети).
///
/// Первый кадр декодируется в конструкторе — его формат становится
/// форматом источника, поэтому rodio
/// получает `channels`/`sample_rate` без дополнительной настройки.
/// Дальнейшие кадры тянутся лениво по мере вывода.
pub struct Mp3Source {
    reader: Box<dyn FormatReader>,
    decoder: Box<dyn AudioDecoder>,
    track_id: u32,
    buffer: Vec<Sample>,
    buffer_offset: usize,
    channels: ChannelCount,
    sample_rate: SampleRate,
    budget: Arc<GarbageBudget>,
    stop: Arc<AtomicBool>,
    link: SharedLink,
    /// Итог для CLI: источник уезжает в rodio, своё поле недоступно.
    shared_outcome: SharedOutcome,
    /// Снимает лимит фазы поиска первым декодированным кадром
    /// (см. [`BufferReader`]).
    first_frame: FirstFrameDecoded,
    /// Эпоха соединения, видимая при последнем кадре: смена — обнуление
    /// счётчика ошибок «подряд» (ошибки копятся в рамках соединения).
    seen_epoch: u64,
    consecutive_errors: u32,
    last_error_log: Option<Instant>,
    /// Был ли мусорный лимит (классификация ошибки конструктора:
    /// лимит до первого кадра — «поток мусорный», не «закончился»).
    hit_garbage_limit: bool,
    /// Превышен ли лимит фазы поиска первого кадра — байты или дедлайн
    /// (классификация ошибки конструктора: «не распознан как MP3»).
    hit_search_limit: bool,
    outcome: Outcome,
    packets: u64,
}

/// Читатель кольцевого буфера для symphonia (`MediaSourceStream`):
/// блокирующее чтение с underrun-паузой. Пока данные есть — отдаёт сколько
/// запрошено-доступно; пусто и буфер открыт — ждёт повторного накопления
/// предбуфера (пауза воспроизведения при долгом обрыве, design D4);
/// буфер закрыт и пуст — EOF (`Ok(0)`); флаг `stop` во время паузы —
/// тоже EOF (refill трактата классифицирует его как остановку).
pub(crate) struct BufferReader {
    buffer: RingBuffer,
    stop: Arc<AtomicBool>,
    /// Сигнал «первый кадр декодирован»: пока не поднят, действует лимит
    /// фазы поиска (см. [`FIRST_FRAME_SCAN_LIMIT_BYTES`]).
    first_frame: FirstFrameDecoded,
    /// Байты, прочитанные в фазе поиска (фаза одна — сброса не требует).
    scanned: usize,
    /// Старт фазы поиска — отсчёт дедлайна.
    search_started: Instant,
}

/// Интервал опроса накопления предбуфера в underrun-паузе.
const PREBUFFER_POLL_INTERVAL: Duration = Duration::from_millis(25);

impl BufferReader {
    /// Читатель разделяемого буфера; `stop` выводит из underrun-паузы
    /// без накопления предбуфера; `first_frame` снимает лимит фазы
    /// поиска первого кадра (поднимает декодер).
    pub(crate) fn new(
        buffer: RingBuffer,
        stop: Arc<AtomicBool>,
        first_frame: FirstFrameDecoded,
    ) -> Self {
        Self {
            buffer,
            stop,
            first_frame,
            scanned: 0,
            search_started: Instant::now(),
        }
    }

    /// Фаза поиска ещё идёт и дедлайн истёк («капающий» сервер никогда
    /// не наберёт байтовый лимит — время единственная граница).
    fn search_deadline_hit(&self) -> bool {
        !self.first_frame.load(Ordering::Relaxed)
            && self.search_started.elapsed() > FIRST_FRAME_DEADLINE
    }
}

impl Read for BufferReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        loop {
            // Дедлайн фазы поиска проверяется до чтения: ожидание данных
            // не должно продлевать фазу бесконечно.
            if self.search_deadline_hit() {
                return Err(first_frame_limit_error());
            }
            match self.buffer.read(out, READ_POLL_INTERVAL) {
                Ok(n) => {
                    // Байтовый лимит — по фактически прочитанному: probe
                    // сканирует всё, что ему дали, счётчик переживает
                    // переподключения reader'а (мусор не «сбрасывается»).
                    if !self.first_frame.load(Ordering::Relaxed) {
                        self.scanned += n;
                        if self.scanned > FIRST_FRAME_SCAN_LIMIT_BYTES {
                            return Err(first_frame_limit_error());
                        }
                    }
                    return Ok(n);
                }
                // Закрытый и пустой буфер — конец потока.
                Err(ReadError::Closed) => return Ok(0),
                // Underrun: пауза до повторного достижения предбуфера.
                // Воспроизведение возобновится только после накопления,
                // а не по первым подвернувшимся байтам. Пауза отмечается
                // на буфере — точный признак для статуса воспроизведения.
                Err(ReadError::Timeout) => {
                    self.buffer.set_underrun(true);
                    while !self.buffer.is_prebuffered() {
                        if self.buffer.is_closed() {
                            // Буфер закрыли во время паузы: следующий
                            // read вернёт EOF.
                            break;
                        }
                        if self.stop.load(Ordering::SeqCst) {
                            // Остановка во время паузы: EOF — refill по
                            // флагу классифицирует её как Stopped, а не
                            // «данные исчерпаны».
                            self.buffer.set_underrun(false);
                            return Ok(0);
                        }
                        if self.search_deadline_hit() {
                            // Пауза внутри фазы поиска не переживает
                            // дедлайн: данных для первого кадра нет.
                            self.buffer.set_underrun(false);
                            return Err(first_frame_limit_error());
                        }
                        std::thread::sleep(PREBUFFER_POLL_INTERVAL);
                    }
                    self.buffer.set_underrun(false);
                }
            }
        }
    }
}

/// Причина io-ошибки лимита фазы поиска: маркер опознаётся по цепочке
/// symphonia (`io::Error` сохраняет вложенную причину для downcast) в
/// любой точке тракта — probe, refill, вычитка мусора.
#[derive(Debug)]
struct FirstFrameLimitError;

impl std::fmt::Display for FirstFrameLimitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "лимит фазы поиска первого кадра")
    }
}

impl std::error::Error for FirstFrameLimitError {}

/// Ошибка лимита фазы поиска первого кадра.
fn first_frame_limit_error() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, FirstFrameLimitError)
}

/// Опознаёт ошибку лимита фазы поиска в io-ошибке (возможно, в обёртке
/// symphonia).
fn is_first_frame_limit(e: &std::io::Error) -> bool {
    e.get_ref()
        .is_some_and(|cause| cause.downcast_ref::<FirstFrameLimitError>().is_some())
}

impl Mp3Source {
    /// Строит источник поверх кольцевого буфера: probe (поиск sync-маркера,
    /// радиопоток может начинаться посреди кадра) → формат-ридер → декодер →
    /// первый кадр. `budget` сбрасывается после каждого успешно
    /// декодированного кадра (мусорный бюджет reader'а, контракт
    /// `net::GarbageBudget`); `stop` различает исчерпание данных
    /// остановкой (`Outcome::Stopped`) от штатного конца; `link` — связка
    /// с reader-потоком (запрос переподключения при мусорном лимите, эпоха
    /// соединений); `shared_outcome` — слот финального исхода для CLI
    /// (источник после `player.append` недоступен напрямую).
    pub fn new(
        buffer: RingBuffer,
        budget: Arc<GarbageBudget>,
        stop: Arc<AtomicBool>,
        link: SharedLink,
        shared_outcome: SharedOutcome,
    ) -> Result<Self, PipelineError> {
        // Probe и первый refill разбирают недоверенные данные: паника
        // symphonia возможна и здесь, не только в живом цикле (см.
        // catch_unwind в `Iterator::next`). Без ловли crafted-MP3 убивает
        // процесс с кодом 101 вместо проектного «поток не распознан»
        // (код 2). AssertUnwindSafe: после паники частично построенный
        // тракт отбрасывается целиком, состояние никем не читается.
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Self::new_inner(buffer, budget, stop, link, shared_outcome)
        }))
        .unwrap_or_else(|_| {
            // Диагностика до классификации: сбой печати не отменяет исход.
            say("декодер аварийно завершился при разборе — поток не распознан как MP3");
            Err(PipelineError::Unsupported(
                "паника декодера на недоверенных данных".into(),
            ))
        })
    }

    /// Тело [`Self::new`] без защиты от паники (см. обёртку там же).
    fn new_inner(
        buffer: RingBuffer,
        budget: Arc<GarbageBudget>,
        stop: Arc<AtomicBool>,
        link: SharedLink,
        shared_outcome: SharedOutcome,
    ) -> Result<Self, PipelineError> {
        // Фаза поиска первого кадра: читатель ограничен лимитом (байты/
        // дедлайн), сигнал снимается первым декодированным кадром в refill.
        let first_frame: FirstFrameDecoded = Arc::new(AtomicBool::new(false));
        let reader = BufferReader::new(buffer, stop.clone(), first_frame.clone());
        // Живой стрим не умеет Seek — оборачиваем в неseekable MediaSource.
        let mss = MediaSourceStream::new(Box::new(ReadOnlySource::new(reader)), Default::default());
        let reader = get_probe()
            .probe(
                &Hint::new(),
                mss,
                Default::default(),
                MetadataOptions::default(),
            )
            .map_err(|e| {
                // Авария reader'а — до классификации по данным: гонка
                // «reader умер паникой во время probe» (catch_unwind
                // reader'а закрывает буфер → probe видит UnexpectedEof)
                // иначе маскировалась бы под EmptyStream.
                if reader_dead(&link) {
                    PipelineError::ReaderCrashed
                } else {
                    map_probe_error(e)
                }
            })?;

        // Радиопоток — одиночная MP3-дорожка; ищем именно её. Дорожка
        // заимствуется только на время создания декодера, после чего
        // `reader` переезжает в источник — клонировать нечего.
        let (track_id, decoder) = {
            let (track_id, params) = reader
                .tracks()
                .iter()
                .find_map(mp3_track)
                .ok_or_else(|| PipelineError::Unsupported("MP3-дорожка не найдена".into()))?;
            let factory = get_codecs()
                .get_audio_decoder(CODEC_ID_MP3)
                .ok_or_else(|| PipelineError::Unsupported("MP3-декодер не зарегистрирован".into()))?
                .factory;
            let decoder = factory(params, &AudioDecoderOptions::default())
                .map_err(|e| PipelineError::Unsupported(e.to_string()))?;
            (track_id, decoder)
        };

        // Эпоха текущего соединения — до перемещения link в структуру.
        let seen_epoch = link.epoch();
        let mut source = Self {
            reader,
            decoder,
            track_id,
            buffer: Vec::new(), // заполняется первым кадром в refill()
            buffer_offset: 0,
            channels: ChannelCount::new(1).expect("1 — допустимое число каналов"),
            sample_rate: SampleRate::new(1).expect("1 — допустимая частота"),
            budget,
            stop,
            link,
            shared_outcome,
            first_frame,
            seen_epoch,
            consecutive_errors: 0,
            last_error_log: None,
            hit_garbage_limit: false,
            hit_search_limit: false,
            outcome: Outcome::Running,
            packets: 0,
        };
        // Первый кадр — по тем же правилам устойчивости, что и весь
        // цикл: мусор пропускается до первого валидного кадра.
        if !source.refill() {
            return Err(
                // Авария reader'а — до классификации по данным: гонка
                // «предбуфер накоплен → reader умер аварийно → построение
                // тракта» иначе маскировалась бы под EmptyStream, хотя
                // буфер был непуст и причина — гибель тракта чтения.
                if reader_dead(&source.link) {
                    PipelineError::ReaderCrashed
                } else if source.hit_search_limit {
                    // Бесконечный мусор без единого кадра: поиск ограничен
                    // лимитом — «не распознан как MP3», не вечное сканирование.
                    PipelineError::FirstFrameLimit
                } else if source.hit_garbage_limit {
                    // Лимит ошибок до первого кадра — поток мусорный (не
                    // «закончился»): в живом цикле лимит лечится
                    // переподключением, но до первого кадра смены соединения
                    // ещё не было.
                    PipelineError::GarbageStream
                } else {
                    match source.outcome() {
                        // Ошибка тракта до первого кадра — не «поток
                        // закончился» (задача 3.2: состояния различимы);
                        // детальная диагностика уже выведена в stderr.
                        Outcome::DecoderError => {
                            PipelineError::Io("ошибка тракта до первого кадра".into())
                        }
                        _ => PipelineError::EmptyStream,
                    }
                },
            );
        }
        Ok(source)
    }

    /// Формат потока: (частота, каналы).
    pub fn spec(&self) -> (u32, usize) {
        (self.sample_rate.get(), self.channels.get() as usize)
    }

    /// Итог источника: чем закончилось воспроизведение.
    pub(crate) fn outcome(&self) -> Outcome {
        self.outcome
    }

    /// Число декодированных кадров (для диагностики и тестов).
    // Снаружи крейта не зовётся (только юнит-тесты): без `allow` не-тестовая
    // сборка считает метод мёртвым кодом.
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn packets(&self) -> u64 {
        self.packets
    }

    /// Тянет и декодирует следующий кадр в буфер сэмплов.
    ///
    /// `true` — буфер наполнен; `false` — терминальное состояние
    /// (`outcome` установлен, диагностика выведена в stderr).
    fn refill(&mut self) -> bool {
        // Смена соединения (переподключение reader'а) обнуляет счётчик
        // ошибок «подряд»: ошибки копятся в рамках одного соединения.
        if self.link.connection_changed(self.seen_epoch) {
            self.seen_epoch = self.link.epoch();
            self.consecutive_errors = 0;
        }
        loop {
            let packet = match self.reader.next_packet() {
                Ok(Some(packet)) => {
                    // Сброс бюджета — только после успешного декодирования
                    // ниже; распознанный заголовок без валидного тела
                    // кадром не считается.
                    packet
                }
                // Лимит фазы поиска — ДО stop-гарда: гонка «stop поднят и
                // лимит истёк» обязана классифицироваться лимитом
                // («не распознан как MP3»), а не остановкой.
                Err(SymphoniaError::IoError(e)) if is_first_frame_limit(&e) => {
                    self.hit_search_limit = true;
                    self.finish(Outcome::DecoderError, "Лимит поиска первого кадра");
                    return false;
                }
                // MPEG-потоки не имеют маркера конца: EOF из буфера —
                // данные исчерпаны. Остановка (флаг поднят до/при
                // закрытии буфера) — не конец данных и не ошибка.
                Ok(None) | Err(SymphoniaError::IoError(_)) if self.stop.load(Ordering::SeqCst) => {
                    self.finish(Outcome::Stopped, "Остановка: источник данных закрыт");
                    return false;
                }
                Ok(None) => {
                    self.finish(Outcome::EndOfStream, "Поток данных исчерпан");
                    return false;
                }
                Err(SymphoniaError::IoError(e))
                    if e.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    self.finish(Outcome::EndOfStream, "Поток данных исчерпан");
                    return false;
                }
                Err(SymphoniaError::IoError(e)) => {
                    // Лимит фазы поиска распознан выше, до stop-гарда.
                    self.finish(
                        Outcome::DecoderError,
                        &format!("Ошибка источника данных: {e}"),
                    );
                    return false;
                }
                Err(SymphoniaError::ResetRequired) => {
                    // Для MP3-ридера недостижимо (symphonia-bundle-mp3 не
                    // выдаёт ResetRequired); переподключение при обрывах
                    // делает reader-поток — декодер его не видит.
                    self.finish(Outcome::DecoderError, "Требуется сброс декодера");
                    return false;
                }
                Err(e) => {
                    if !self.register_decode_failure(e.to_string()) {
                        return false;
                    }
                    continue;
                }
            };
            if packet.track_id != self.track_id {
                continue;
            }
            match self.decoder.decode(&packet) {
                Ok(decoded) => {
                    // Копируем сэмплы немедленно: `decoded` заимствует
                    // декодер, ссылки наружу не уходят. Буфер переиспользуем:
                    // copy_to_vec_interleaved делает resize на месте, а кадры
                    // MP3 постоянного размера — аллокаций в цикле нет.
                    let (rate, channels) =
                        (decoded.spec().rate(), decoded.spec().channels().count());
                    decoded.copy_to_vec_interleaved(&mut self.buffer);
                    self.buffer_offset = 0;
                    self.packets += 1;
                    // Кадр успешно декодирован — сброс мусорного бюджета
                    // reader'а (контракт `net::GarbageBudget`: счётчик
                    // байтов от последнего декодированного кадра); первый
                    // сброс заодно активирует бюджет.
                    self.budget.reset();
                    self.consecutive_errors = 0;
                    // Первый декодированный кадр завершает фазу поиска:
                    // лимиты читателя снимаются — живой цикл объёмом и
                    // временем не ограничен.
                    self.first_frame.store(true, Ordering::Relaxed);
                    // Формат источника задаётся первым кадром и дальше не
                    // меняется — как линия, открытая форматом первого
                    // кадра в LineAudioSink.
                    if self.packets == 1 {
                        // Значения берутся из кадра в сети: неверное — ошибка
                        // тракта, а не паника. Для MP3 каналов ≤ 2 и частота
                        // из таблицы — ветка страховочная.
                        match (ChannelCount::new(channels as u16), SampleRate::new(rate)) {
                            (Some(channels), Some(sample_rate)) => {
                                self.channels = channels;
                                self.sample_rate = sample_rate;
                            }
                            _ => {
                                self.finish(
                                Outcome::DecoderError,
                                &format!(
                                    "неверный формат первого кадра: {channels} каналов, {rate} Гц"
                                ),
                            );
                                return false;
                            }
                        }
                    }
                    return true;
                }
                // Повреждённый кадр: пропускаем и продолжаем поток.
                Err(SymphoniaError::DecodeError(e)) => {
                    if !self.register_decode_failure(e.to_string()) {
                        return false;
                    }
                }
                Err(e) => {
                    self.finish(Outcome::DecoderError, &format!("Ошибка декодера: {e}"));
                    return false;
                }
            }
        }
    }

    /// Логирует ошибку декодирования не чаще раза в секунду.
    ///
    /// При исчерпании лимита подряд ошибок запрашивает переподключение
    /// (спека устойчивости: прекратить декодирование этого соединения,
    /// reader закроет его и откроет новое) и переходит в вычитку мусора
    /// вхолостую до новой эпохи — см. [`Self::discard_until_reconnect`].
    fn register_decode_failure(&mut self, reason: String) -> bool {
        self.consecutive_errors += 1;
        let now = Instant::now();
        if self
            .last_error_log
            .is_none_or(|last| now.duration_since(last) >= DECODE_ERROR_LOG_INTERVAL)
        {
            self.last_error_log = Some(now);
            say(&format!(
                "ошибка декодирования кадра, подряд ошибок: {} ({reason})",
                self.consecutive_errors
            ));
        }
        if self.consecutive_errors >= MAX_CONSECUTIVE_DECODE_ERRORS {
            self.hit_garbage_limit = true;
            // Запрос штампуется эпохой текущего соединения: если оно умрёт
            // само раньше, «протухший» запрос не закроет следующее.
            self.link.request_reconnect(self.seen_epoch);
            return self.discard_until_reconnect();
        }
        true
    }

    /// Мусорный лимит: байты закрытого соединения вычитываются вхолостую
    /// (без декодирования), пока reader не закроет соединение и не
    /// откроет новое — смена эпохи. Вычитка обязательна: остановись декодер,
    /// буфер переполнится и reader заблокируется в записи (deadlock).
    /// На новом соединении счётчик «подряд» обнуляется.
    ///
    /// `false` — терминальное состояние (остановка или конец данных:
    /// переподключения не случится, например буфер закрыли в тесте).
    fn discard_until_reconnect(&mut self) -> bool {
        while !self.link.connection_changed(self.seen_epoch) {
            if self.stop.load(Ordering::SeqCst) {
                self.finish(Outcome::Stopped, "Остановка: источник данных закрыт");
                return false;
            }
            match self.reader.next_packet() {
                // Вычитка вхолостую: результат пакета не важен.
                Ok(Some(_)) => {}
                // Закрытый буфер опустошён: новых данных не будет —
                // переподключение в этом контексте невозможно.
                Ok(None) => {
                    self.finish(Outcome::EndOfStream, "Поток данных исчерпан");
                    return false;
                }
                // Ошибки распознавания — мусор этого соединения, то, что
                // вычитываем (каждая потребляет байты, прогресс есть).
                Err(SymphoniaError::DecodeError(_)) => {}
                // Лимит фазы поиска: мусор не кончился ни за объём, ни
                // за время — вычитка до смены эпохи бессмысленна.
                Err(SymphoniaError::IoError(e)) if is_first_frame_limit(&e) => {
                    self.hit_search_limit = true;
                    self.finish(Outcome::DecoderError, "Лимит поиска первого кадра");
                    return false;
                }
                // Остановка в окне между проверкой флага и read: не
                // классифицируется как ошибка (зеркально refill).
                Err(SymphoniaError::IoError(_)) if self.stop.load(Ordering::SeqCst) => {
                    self.finish(Outcome::Stopped, "Остановка: источник данных закрыт");
                    return false;
                }
                // Прочих IO-ошибок BufferReader не даёт (таймауты
                // обращаются в ожидание предбуфера, EOF — в Ok(None)):
                // всё остальное — терминал, а не молчаливый горячий
                // цикл до смены эпохи.
                Err(e) => {
                    self.finish(
                        Outcome::DecoderError,
                        &format!("Ошибка источника данных при пропуске мусора: {e}"),
                    );
                    return false;
                }
            }
        }
        self.seen_epoch = self.link.epoch();
        self.consecutive_errors = 0;
        true
    }

    /// Фиксирует терминальное состояние с диагностикой в stderr и слотом
    /// исхода для CLI.
    fn finish(&mut self, outcome: Outcome, message: &str) {
        self.outcome = outcome;
        *self
            .shared_outcome
            .lock()
            .expect("замок исхода не отравлен: присваивание не паникует") = outcome;
        say(message);
    }
}

/// MP3-дорожка ридера: id и параметры кодека (радиопоток — единственная
/// дорожка; не-MP3 дорожки не рассматриваются).
fn mp3_track(track: &Track) -> Option<(u32, &AudioCodecParameters)> {
    match &track.codec_params {
        Some(CodecParameters::Audio(params)) if params.codec == CODEC_ID_MP3 => {
            Some((track.id, params))
        }
        _ => None,
    }
}

impl Iterator for Mp3Source {
    type Item = Sample;

    fn next(&mut self) -> Option<Sample> {
        loop {
            if self.buffer_offset < self.buffer.len() {
                let sample = self.buffer[self.buffer_offset];
                self.buffer_offset += 1;
                return Some(sample);
            }
            // Кадр исчерпан — тянем следующий из буфера (сеть читает
            // reader-поток, здесь только декодирование). Паника тракта
            // (symphonia на недоверенных данных) ловится: погибший поток
            // вывода rodio перестаёт опрашивать источник, `Player::empty`
            // не опустеет и CLI завис бы с тишиной — вместо паники источник
            // завершается штатно, слот исхода даёт код 2 (зеркально
            // catch_unwind reader'а в `net::spawn_reader`).
            // AssertUnwindSafe: после паники источник отбрасывается —
            // частично изменённое состояние никем не читается.
            let frame = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| self.refill()));
            match frame {
                Ok(true) => {}
                Ok(false) => return None,
                Err(_) => {
                    // finish фиксирует исход до диагностики: сбой печати
                    // не отменяет классификацию (код 2).
                    self.finish(
                        Outcome::DecoderError,
                        "Паника тракта декодирования — воспроизведение будет остановлено",
                    );
                    return None;
                }
            }
        }
    }
}

impl Source for Mp3Source {
    /// Полный размер текущего кадра, а не остаток (design D3): `Some(0)`
    /// по контракту rodio означает «данных нет», и остаток давал бы
    /// `Some(0)` на каждой межкадровой границе.
    fn current_span_len(&self) -> Option<usize> {
        Some(self.buffer.len())
    }

    fn channels(&self) -> ChannelCount {
        self.channels
    }

    fn sample_rate(&self) -> SampleRate {
        self.sample_rate
    }

    fn total_duration(&self) -> Option<Duration> {
        None
    }
}

/// Авария reader'а (паника или фатальная ошибка тракта чтения) —
/// общий признак для refill- и probe-путей: проверяется до классификации
/// по данным, т.к. закрытый буфер отдаёт UnexpectedEof и ошибка чтения
/// иначе маскируется под EmptyStream.
fn reader_dead(link: &SharedLink) -> bool {
    link.reader_panicked() || link.fatal()
}

/// Ошибка probe до первого кадра: мусор без sync-маркера или пустой поток.
fn map_probe_error(e: SymphoniaError) -> PipelineError {
    match e {
        SymphoniaError::Unsupported(reason) => PipelineError::Unsupported(reason.to_string()),
        SymphoniaError::IoError(io) if is_first_frame_limit(&io) => PipelineError::FirstFrameLimit,
        SymphoniaError::IoError(io) if io.kind() == std::io::ErrorKind::UnexpectedEof => {
            PipelineError::EmptyStream
        }
        other => PipelineError::Io(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::Link;

    fn fixture_bytes() -> Vec<u8> {
        let bytes = std::fs::read(FIXTURE_PATH).expect("фикстура stream-sample.mp3");
        assert!(!bytes.is_empty(), "фикстура не должна быть пустой");
        bytes
    }

    /// Источник поверх буфера: все байты уже записаны, буфер закрыт
    /// (EOF после данных) — как вычитанный до конца живой срез.
    fn source_over(bytes: Vec<u8>) -> Mp3Source {
        source_over_with_stop(bytes, Arc::new(AtomicBool::new(false)))
    }

    /// То же с внешним флагом остановки (разделение терминальных
    /// состояний).
    fn source_over_with_stop(bytes: Vec<u8>, stop: Arc<AtomicBool>) -> Mp3Source {
        let buffer = RingBuffer::new(bytes.len().max(8 * 1024), 4 * 1024);
        buffer.write(&bytes).expect("ёмкости буфера хватает");
        buffer.close();
        let budget = GarbageBudget::shared();
        Mp3Source::new(
            buffer,
            budget,
            stop,
            Link::shared(),
            Arc::new(Mutex::new(Outcome::Running)),
        )
        .expect("фикстура декодируется")
    }

    /// Вычитывает источник до конца, возвращает суммарные сэмплы.
    fn drain(source: &mut Mp3Source) -> u64 {
        let mut samples = 0u64;
        while source.next().is_some() {
            samples += 1;
        }
        samples
    }

    /// 3.2: фикстура живого среза декодируется до конца, ≥100 кадров.
    #[test]
    fn fixture_decodes_to_end_with_at_least_100_packets() {
        let mut source = source_over(fixture_bytes());
        let (_, channels) = source.spec();
        let samples = drain(&mut source);
        assert_eq!(source.outcome(), Outcome::EndOfStream);
        assert!(
            source.packets() >= 100,
            "кадров {} — слишком мало для 256 КБ потока",
            source.packets()
        );
        // Сэмплы бьются по кадрам: кратно числу каналов.
        assert_eq!(samples % channels as u64, 0, "кадры бьются по каналам");
    }

    /// Старт посреди кадра: фикстура записана с живого среза и начинается
    /// мусорным хвостом кадра (первый sync — на смещении ~0x24) —
    /// конструктор обязан найти первый sync-маркер.
    #[test]
    fn stream_starting_mid_frame_finds_sync() {
        let bytes = fixture_bytes();
        assert!(
            source_over(bytes).packets() > 0,
            "поток, начинающийся посреди кадра, декодируется"
        );
    }

    /// 3.3: повреждённый кусок в середине фикстуры — декодер продолжает
    /// со следующего валидного кадра и завершается штатно (требование
    /// спеки: «повреждённые кадры и мусор пропускаются»).
    #[test]
    fn corrupted_chunk_mid_stream_is_skipped() {
        let mut bytes = fixture_bytes();
        let mid = bytes.len() / 2;
        // Псевдослучайный мусор без sync-маркеров вместо ~2 кадров.
        for (i, byte) in bytes.iter_mut().enumerate().skip(mid).take(768) {
            *byte = if i % 2 == 0 { 0x37 } else { 0xa1 };
        }
        let mut source = source_over(bytes);
        let samples = drain(&mut source);
        assert_eq!(source.outcome(), Outcome::EndOfStream);
        assert!(
            source.packets() >= 100,
            "после пропуска повреждённых кадров декодирование продолжается"
        );
        assert!(samples > 0);
    }

    /// 3.3: мусорный поток (случайные байты) — терминальная диагностика
    /// без паники и зависания.
    #[test]
    fn garbage_stream_terminates_with_diagnostic() {
        let garbage = pseudo_random_bytes(1024 * 1024);
        let buffer = RingBuffer::new(1024 * 1024, 4 * 1024);
        buffer.write(&garbage).expect("ёмкости хватает");
        buffer.close();
        let stop = Arc::new(AtomicBool::new(false));
        let err = match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            stop,
            Link::shared(),
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("мусор не должен декодироваться"),
        };
        // Диагностика вместо паники: либо sync не найден (probe),
        // либо исчерпан лимит ошибок до первого кадра.
        assert!(
            matches!(
                err,
                PipelineError::Unsupported(_) | PipelineError::GarbageStream
            ),
            "неожиданная ошибка: {err}"
        );
    }

    /// Мусор без sync-маркеров больше собственного предела probe symphonia
    /// (max_probe_depth = 1 МиБ) и больше лимита фазы (4 МиБ): построение
    /// тракта завершается ошибкой «не распознан как MP3», а не вечным
    /// сканированием — какая граница сработала, деталь реализации.
    #[test]
    fn no_sync_garbage_larger_than_any_limit_still_terminates() {
        let garbage = no_sync_garbage(5 * 1024 * 1024);
        let buffer = RingBuffer::new(garbage.len(), 64 * 1024);
        buffer.write(&garbage).expect("ёмкости хватает");
        buffer.close();
        let stop = Arc::new(AtomicBool::new(false));
        let err = match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            stop,
            Link::shared(),
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("поток без sync-маркеров не должен декодироваться"),
        };
        assert!(
            matches!(
                err,
                PipelineError::Unsupported(_) | PipelineError::FirstFrameLimit
            ),
            "неожиданная ошибка: {err}"
        );
    }

    /// Гонка «предбуфер накоплен → reader умер паникой → построение
    /// тракта»: классификация — авария потока чтения, а не «поток
    /// закончился до первого кадра» (буфер непуст, но валидного кадра
    /// в нём нет — refill заканчивается исчерпанием данных).
    #[test]
    fn reader_panic_during_construction_is_not_empty_stream() {
        let bytes = fixture_bytes();
        let header_at = find_frame_start(&bytes).expect("в фикстуре есть кадр");
        let header = bytes[header_at..header_at + 4].to_vec();
        let frame_size = layer3_frame_size(&header);
        // Данные для probe (фейковые кадры) + мусор без sync-маркеров:
        // буфер непуст, но ни одного декодируемого кадра — без флага
        // паники это классифицировалось бы как EmptyStream.
        let mut stream = Vec::new();
        for _ in 0..3 {
            stream.extend_from_slice(&header);
            stream.extend(pseudo_random_bytes(frame_size - 4));
        }
        stream.extend(no_sync_garbage(64 * 1024));
        let buffer = RingBuffer::new(stream.len(), 4 * 1024);
        buffer.write(&stream).expect("ёмкости буфера хватает");
        buffer.close();
        let link = Link::shared();
        link.mark_reader_panicked();
        let err = match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            Arc::new(AtomicBool::new(false)),
            link,
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("авария reader'а не должна давать источник"),
        };
        assert!(
            matches!(err, PipelineError::ReaderCrashed),
            "неожиданная ошибка: {err}"
        );
    }

    /// Гонка «stop поднят и лимит фазы поиска истёк»: лимит побеждает
    /// в классификации («не распознан как MP3»), stop-гард — только для
    /// остальных исходов.
    #[test]
    fn search_limit_beats_stop_flag_in_classification() {
        // Пробуемые заголовки (probe проходит) + мусор без 0xFF сверх
        // байтового лимита фазы: первый кадр не декодируется, лимит
        // срабатывает при чтении уже ПОСЛЕ probe.
        let bytes = fixture_bytes();
        let header_at = find_frame_start(&bytes).expect("в фикстуре есть кадр");
        let header = bytes[header_at..header_at + 4].to_vec();
        let frame_size = layer3_frame_size(&header);
        let mut stream = Vec::new();
        for _ in 0..3 {
            stream.extend_from_slice(&header);
            stream.extend(pseudo_random_bytes(frame_size - 4));
        }
        let garbage = no_sync_garbage(FIRST_FRAME_SCAN_LIMIT_BYTES);
        stream.extend(&garbage);

        let buffer = RingBuffer::new(stream.len(), 64 * 1024);
        buffer.write(&stream).expect("ёмкости буфера хватает");
        buffer.close();
        let err = match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            Arc::new(AtomicBool::new(true)),
            Link::shared(),
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("мусор без sync-маркеров не должен декодироваться"),
        };
        assert!(
            matches!(err, PipelineError::FirstFrameLimit),
            "неожиданная ошибка: {err}"
        );
    }

    /// Лестница фейковых кадров ДО первого декодированного: «кадры» с
    /// валидными заголовками и мусорными телами проходят probe и next_packet
    /// (сбрасывая мусорный бюджет reader'а), но не декодируются; 50 ошибок
    /// → переподключение → счётчик обнулён → снова 50 — без лимита фазы
    /// цикл бесконечен. «Reader» подкидывает данные по запросам
    /// переподключения; первый кадр так и не появляется — байтовый лимит
    /// фазы обязан остановить построение тракта (спека: без прогресса —
    /// не бесконечно).
    #[test]
    fn fake_frame_ladder_before_first_frame_hits_search_limit() {
        fn fake_frame(out: &mut Vec<u8>, header: &[u8], frame_size: usize) {
            out.extend_from_slice(header);
            out.extend(pseudo_random_bytes(frame_size - 4));
        }

        let bytes = fixture_bytes();
        let header_at = find_frame_start(&bytes).expect("в фикстуре есть кадр");
        let header = bytes[header_at..header_at + 4].to_vec();
        let frame_size = layer3_frame_size(&header);
        let portion_frames = MAX_CONSECUTIVE_DECODE_ERRORS + 10;

        // Стартовая порция: пары валидных заголовков probe'у достаточно.
        let mut first = Vec::new();
        for _ in 0..portion_frames {
            fake_frame(&mut first, &header, frame_size);
        }
        // Ёмкость с двойным запасом к остатку «недокорма» (лимит фазы +
        // 256 КиБ): write писателя не блокируется навсегда, если декодер
        // завершится раньше писателя.
        let buffer = RingBuffer::new(512 * 1024, 16 * 1024);
        buffer.write(&first).expect("стартовая порция влезает");

        // «Reader»: пока запрос переподключения активен — новая эпоха и
        // следующая порция фейковых кадров (суммарно больше лимита фазы),
        // затем закрытие буфера. Если лимит фазы не сработает, конструктор
        // завершится «данные исчерпаны» — ассерт упадёт с ясной ошибкой.
        let link = Link::shared();
        let writer = {
            let buffer = buffer.clone();
            let link = link.clone();
            std::thread::spawn(move || {
                let mut fed = first.len();
                let target = FIRST_FRAME_SCAN_LIMIT_BYTES + 256 * 1024;
                while fed < target {
                    if link.reconnect_requested() {
                        buffer.clear();
                        link.next_epoch();
                        let mut portion = Vec::new();
                        for _ in 0..portion_frames {
                            fake_frame(&mut portion, &header, frame_size);
                        }
                        buffer
                            .write(&portion)
                            .expect("порция влезает после очистки");
                        fed += portion.len();
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                buffer.close();
            })
        };

        let stop = Arc::new(AtomicBool::new(false));
        let err = match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            stop,
            link,
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("фейковые кадры не должны декодироваться"),
        };
        assert!(
            matches!(err, PipelineError::FirstFrameLimit),
            "неожиданная ошибка: {err}"
        );
        writer.join().expect("поток «reader'а» завершён");
    }

    /// 3.3 (лимит ошибок): кадры с настоящими заголовками, но мусорными
    /// телами — подряд ошибки декодирования до лимита
    /// MAX_CONSECUTIVE_DECODE_ERRORS: до первого кадра — ошибка
    /// «поток мусорный»; в живом цикле — запрос переподключения
    /// (см. тест ниже), без паники и зависания.
    #[test]
    fn planted_bad_frames_hit_consecutive_error_limit() {
        let bytes = fixture_bytes();
        let header_at = find_frame_start(&bytes).expect("в фикстуре есть кадр");
        let header = bytes[header_at..header_at + 4].to_vec();
        let frame_size = layer3_frame_size(&header);

        // «Кадры»: валидный заголовок + мусорное тело → decode error
        // (мусорная side info даёт невалидный main_data offset).
        let mut stream = Vec::new();
        for _ in 0..(MAX_CONSECUTIVE_DECODE_ERRORS + 10) {
            stream.extend_from_slice(&header);
            stream.extend(pseudo_random_bytes(frame_size - 4));
        }

        // Двух валидных заголовков подряд достаточно для probe; дальше
        // конструктор упирается в лимит ошибок до первого кадра…
        let buffer = RingBuffer::new(stream.len(), 4 * 1024);
        buffer.write(&stream).expect("ёмкости хватает");
        buffer.close();
        let stop = Arc::new(AtomicBool::new(false));
        match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            stop,
            Link::shared(),
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(PipelineError::GarbageStream) => {}
            Ok(mut source) => {
                // …либо первый кадр как-то декодировался — тогда в живом
                // цикле лимит запрашивает переподключение; буфер закрыт,
                // нового соединения не будет — источник завершается
                // штатно (данные исчерпаны), без паники.
                drain(&mut source);
                assert_eq!(source.outcome(), Outcome::EndOfStream);
            }
            Err(other) => panic!("неожиданная ошибка: {other}"),
        }
    }

    /// Спека устойчивости в живом цикле: мусорный лимит НЕ завершает
    /// воспроизведение — декодер запрашивает переподключение, байты
    /// закрытого соединения вычитываются вхолостую, после нового
    /// соединения (смена эпохи) декодирование продолжается.
    #[test]
    fn garbage_limit_mid_stream_requests_reconnect_and_continues() {
        let bytes = fixture_bytes();
        let header_at = find_frame_start(&bytes).expect("в фикстуре есть кадр");
        let header = bytes[header_at..header_at + 4].to_vec();
        let frame_size = layer3_frame_size(&header);
        let valid_head = bytes[..64 * 1024].to_vec();
        let valid_tail = bytes[64 * 1024..128 * 1024].to_vec();

        // Валидный старт, затем «кадры» с мусорными телами сверх лимита.
        let mut bad = Vec::new();
        for _ in 0..(MAX_CONSECUTIVE_DECODE_ERRORS + 10) {
            bad.extend_from_slice(&header);
            bad.extend(pseudo_random_bytes(frame_size - 4));
        }

        let buffer = RingBuffer::new(256 * 1024, 16 * 1024);
        buffer.write(&valid_head).expect("валидный старт влезает");
        buffer.write(&bad).expect("мусорный кусок влезает");

        // «Reader»: по запросу переподключения — очистка буфера, новая
        // эпоха, продолжение потока и его закрытие (EOF). Связка общая
        // с декодером — как reader-поток и Mp3Source в живом тракте.
        let link = Link::shared();
        let writer = {
            let buffer = buffer.clone();
            let link = link.clone();
            std::thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(10);
                while !link.reconnect_requested() {
                    assert!(
                        Instant::now() < deadline,
                        "мусорный лимит не запросил переподключение за 10 с"
                    );
                    std::thread::sleep(Duration::from_millis(10));
                }
                buffer.clear();
                link.next_epoch();
                buffer.write(&valid_tail).expect("продолжение влезает");
                buffer.close();
            })
        };

        let shared_outcome = Arc::new(Mutex::new(Outcome::Running));
        let stop = Arc::new(AtomicBool::new(false));
        let mut source = Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            stop,
            link,
            shared_outcome.clone(),
        )
        .expect("валидный старт декодируется");
        let samples = drain(&mut source);

        // Источник пережил мусорный лимит: итог — штатный конец, а не
        // ошибка; кадров — как минимум оба валидных куска.
        assert_eq!(source.outcome(), Outcome::EndOfStream);
        assert_eq!(
            *shared_outcome.lock().expect("замок исхода не отравлен"),
            Outcome::EndOfStream,
            "слот исхода синхронен с источником"
        );
        assert!(
            source.packets() >= 200,
            "после переподключения декодирование продолжилось: {} кадров",
            source.packets()
        );
        assert!(samples > 0);
        writer.join().expect("поток «reader'а» завершён");
    }

    /// 3.2 (терминальные состояния): буфер закрыт без флага остановки —
    /// штатный конец; с флагом — остановка. Состояния различимы, а не
    /// слиты в один «конец потока».
    #[test]
    fn terminal_states_distinguish_end_of_stream_from_stop() {
        let bytes = fixture_bytes();
        // Без флага: закрытие буфера = конец данных.
        let mut ended = source_over_with_stop(bytes.clone(), Arc::new(AtomicBool::new(false)));
        drain(&mut ended);
        assert_eq!(
            ended.outcome(),
            Outcome::EndOfStream,
            "закрытие без остановки — конец"
        );

        // С флагом, поднятым до исчерпания данных: та же исчерпанность
        // буфера классифицируется как остановка.
        let stop = Arc::new(AtomicBool::new(true));
        let mut stopped = source_over_with_stop(bytes, stop);
        drain(&mut stopped);
        assert_eq!(
            stopped.outcome(),
            Outcome::Stopped,
            "закрытие при остановке — не конец и не ошибка"
        );
        assert_ne!(
            stopped.outcome(),
            Outcome::DecoderError,
            "не ошибка декодера"
        );
    }

    /// 3.2 (старт): поток не MP3 — конструктор отличает это от сетевых
    /// проблем: диагностика указывает на содержимое, а не на соединение
    /// (для кода выхода 2 в CLI).
    #[test]
    fn non_mp3_stream_is_recognized_as_unsupported() {
        let html: &[u8] = b"<html><head><title>50x</title></head><body>oops</body></html>";
        let buffer = RingBuffer::new(8 * 1024, 4 * 1024);
        buffer.write(html).expect("ёмкости хватает");
        buffer.close();
        let stop = Arc::new(AtomicBool::new(false));
        let err = match Mp3Source::new(
            buffer,
            GarbageBudget::shared(),
            stop,
            Link::shared(),
            Arc::new(Mutex::new(Outcome::Running)),
        ) {
            Err(e) => e,
            Ok(_) => panic!("HTML не должен распознаваться как MP3"),
        };
        assert!(
            matches!(
                err,
                PipelineError::Unsupported(_) | PipelineError::GarbageStream
            ),
            "неожиданная ошибка: {err}"
        );
    }

    /// 3.1 (контракт rodio): `current_span_len` — полный размер текущего
    /// кадра, а не остаток: на межкадровой границе (кадр только что
    /// исчерпан) span не должен становиться `Some(0)` — `Some(0)` по
    /// контракту значит «данных нет» (design D3).
    #[test]
    fn span_len_is_full_frame_even_at_frame_boundary() {
        let mut source = source_over(fixture_bytes());
        assert!(source.next().is_some(), "первый сэмпл есть");
        let frame_len = source.current_span_len().expect("источник жив — span есть");
        assert!(frame_len > 0, "кадр не пуст");

        // Дошёвываем кадр до конца: на границе span обязан остаться
        // равным полному размеру кадра, а не упасть в ноль.
        for _ in 1..frame_len {
            assert!(
                source.next().is_some(),
                "сэмплы текущего кадра исчерпываются не раньше frame_len"
            );
        }
        assert_eq!(
            source.current_span_len(),
            Some(frame_len),
            "на межкадровой границе span — полный кадр, не Some(0)"
        );
        // Данные при этом есть: следующий кадр отдаётся.
        assert!(source.next().is_some(), "следующий кадр читается");
    }

    /// 3.1 (underrun): `BufferReader` при исчерпании данных ждёт
    /// повторного накопления предбуфера, а не возвращает пустоту;
    /// возобновление — после порога.
    #[test]
    fn buffer_reader_pauses_until_prebuffer_after_underrun() {
        let bytes = fixture_bytes();
        let first = bytes[..8 * 1024].to_vec();
        let refill = bytes[8 * 1024..24 * 1024].to_vec(); // 16 КиБ = предбуфер
        let buffer = RingBuffer::new(32 * 1024, 16 * 1024);
        buffer.write(&first).expect("первая порция влезает");

        // Писатель возобновляет подачу с задержкой: читатель успевает
        // уйти в underrun-паузу.
        let writer_refill = refill.clone();
        let writer_buffer = buffer.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            writer_buffer
                .write(&writer_refill)
                .expect("вторая порция влезает");
        });

        let mut reader = BufferReader::new(
            buffer,
            Arc::new(AtomicBool::new(false)),
            Arc::new(AtomicBool::new(false)),
        );
        let mut out = vec![0u8; 8 * 1024];
        let mut filled = 0usize;
        while filled < 8 * 1024 {
            filled += reader
                .read(&mut out[filled..])
                .expect("первая порция читается");
        }
        assert_eq!(&out[..], &first[..], "первая порция без потерь");

        // Вторая порция: чтение стоит в underrun-паузе не меньше задержки
        // писателя и приходит после накопления предбуфера.
        let started = Instant::now();
        let mut second = vec![0u8; 16 * 1024];
        let mut got = 0usize;
        while got < 16 * 1024 {
            got += reader
                .read(&mut second[got..])
                .expect("вторая порция читается");
        }
        assert_eq!(&second[..], &refill[..], "вторая порция без потерь");
        assert!(
            started.elapsed() >= Duration::from_millis(250),
            "чтение ждало предбуфер, а не среагировало на первые байты: {:?}",
            started.elapsed()
        );
    }

    /// Underrun-флаг: пустой открытый буфер вводит читателя в паузу
    /// (после таймаута чтения) — флаг поднят; накопление предбуфера
    /// выводит из паузы — флаг снят. Это точный признак «звука нет»
    /// для статуса воспроизведения.
    #[test]
    fn underrun_flag_marks_decoder_pause_exactly() {
        let buffer = RingBuffer::new(32 * 1024, 16 * 1024);
        let stop = Arc::new(AtomicBool::new(false));
        let reader = {
            let buffer = buffer.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut reader =
                    BufferReader::new(buffer.clone(), stop, Arc::new(AtomicBool::new(false)));
                let mut out = [0u8; 8 * 1024];
                let _ = reader.read(&mut out);
            })
        };

        // Таймаут чтения (READ_POLL_INTERVAL = 500 мс) вводит в паузу:
        // ждём с запасом и проверяем, что флаг поднят (буфер открыт,
        // но предбуфера нет — пауза длится).
        std::thread::sleep(READ_POLL_INTERVAL + Duration::from_millis(500));
        assert!(buffer.is_underrun(), "читатель в underrun-паузе");

        // Предбуфер накоплен — пауза снята, чтение вернулось.
        let fixture = fixture_bytes();
        buffer
            .write(&fixture[..16 * 1024])
            .expect("предбуфер влезает");
        reader
            .join()
            .expect("читатель вышел из паузы после предбуфера");
        assert!(!buffer.is_underrun(), "после предбуфера паузы нет");
    }

    /// Остановка во время underrun-паузы: чтение на открытом пустом
    /// буфере не висит до предбуфера — по флагу stop возвращается EOF
    /// (refill классифицирует его как Stopped), флаг паузы снят.
    #[test]
    fn stop_breaks_underrun_pause_with_eof() {
        let buffer = RingBuffer::new(32 * 1024, 16 * 1024);
        let stop = Arc::new(AtomicBool::new(false));
        let (tx, rx) = std::sync::mpsc::channel();
        let reader = {
            let buffer = buffer.clone();
            let stop = stop.clone();
            std::thread::spawn(move || {
                let mut reader =
                    BufferReader::new(buffer.clone(), stop, Arc::new(AtomicBool::new(false)));
                let mut out = [0u8; 8 * 1024];
                let n = reader.read(&mut out).expect("чтение без ошибки");
                tx.send(n).expect("канал читается");
            })
        };

        // Читатель вошёл в паузу (таймаут чтения + ожидание предбуфера).
        std::thread::sleep(READ_POLL_INTERVAL + Duration::from_millis(300));
        assert!(buffer.is_underrun(), "читатель в underrun-паузе");

        stop.store(true, Ordering::SeqCst);
        let n = rx
            .recv_timeout(Duration::from_secs(1))
            .expect("остановка выводит из паузы, а не висит до предбуфера");
        assert_eq!(n, 0, "остановка в паузе — EOF");
        reader.join().expect("поток читателя завершён");
        assert!(!buffer.is_underrun(), "флаг паузы снят");
    }

    /// Размер кадра MPEG Layer III по битам заголовка (битрейт/частота/
    /// паддинг). Расстояние между sync-маркерами для этого не годится:
    /// случайные 0xFFEx внутри тела кадра дают короткую длину.
    fn layer3_frame_size(header: &[u8]) -> usize {
        let version = (header[1] >> 3) & 0b11; // 11=MPEG1, 10=MPEG2, 00=MPEG2.5
        let bitrate_kbps = [
            0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 0,
        ][(header[2] >> 4) as usize];
        let sample_rate_hz = [44_100, 48_000, 32_000][(header[2] >> 2) as usize & 0b11]
            >> match version {
                3 => 0, // MPEG1
                2 => 1, // MPEG2
                _ => 2, // MPEG2.5
            };
        let padding = (header[2] >> 1) & 1;
        let coefficient = if version == 3 { 144 } else { 72 };
        coefficient * bitrate_kbps * 1000 / sample_rate_hz + padding as usize
    }

    /// 2.1 (живой): первые 64 КБ живого потока читаются без ошибки
    /// и таймаута. Запуск: `cargo test -- --ignored`.
    #[test]
    #[ignore = "живой тест: требует сеть и доступный поток"]
    fn live_stream_reads_first_64k() {
        let mut reader = crate::net::open_stream(DEFAULT_STREAM_URL).expect("подключение");
        let mut chunk = [0u8; 8192];
        let mut total = 0usize;
        while total < 64 * 1024 {
            let read = reader.read(&mut chunk).expect("чтение без ошибки");
            assert!(read > 0, "поток закрылся до 64 КБ");
            total += read;
        }
    }

    /// Первый sync-маркер MPEG-аудиокадра (0xFFEx).
    fn find_frame_start(bytes: &[u8]) -> Option<usize> {
        bytes
            .windows(2)
            .position(|w| w[0] == 0xff && w[1] & 0xe0 == 0xe0)
    }

    /// Детерминированный мусор без байта 0xFF: sync-маркер MPEG
    /// начинается с 0xFF — в таких данных probe не найдёт ни одного
    /// кандидата и упрётся только в лимит фазы поиска.
    fn no_sync_garbage(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| if i % 2 == 0 { 0x37 } else { 0xa1 })
            .collect()
    }

    /// Детерминированные псевдослучайные байты (xorshift).
    fn pseudo_random_bytes(len: usize) -> Vec<u8> {
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }
}

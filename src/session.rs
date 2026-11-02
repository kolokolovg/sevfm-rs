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

//! Сессии воспроизведения (design D1 add-player-ui): повторные запуски
//! тракта в одном процессе — слой над одноразовыми структурами фазы 1.
//! Каждая сессия создаёт свежий набор (буфер, бюджет мусора, `Link`,
//! слоты) и worker-поток с живым циклом из прежнего `run()` main.rs;
//! сам тракт не меняется. Остановка — флаг сессии («Стоп» в окне) или
//! флаг процесса (сигнал): воркер замечает его за такт опроса, глушит
//! вывод и освобождает аудиоустройство.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::buffer::RingBuffer;
use crate::icy::{SharedEtherGate, SharedNowPlaying, TrackInfo};
use crate::net::{GarbageBudget, Link, NetStatus, SharedLink, SharedNetStatus, spawn_reader};
use crate::pipeline::{Mp3Source, Outcome, SharedOutcome};
use crate::say;

/// Такт опроса флагов остановки и слотов живого цикла воркера (тот же
/// ритм, что POLL_INTERVAL в main.rs).
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Чем закончилась сессия воспроизведения. `Fatal` — не-сетевая авария
/// тракта (нет аудиоустройства, поток не MP3, отклонение сервером,
/// гибель reader'а или вывода, ошибка декодера): в headless это код
/// выхода 2 с сообщением, в UI — строка ошибки в окне (спека
/// player-ui). `Stopped` — остановка пользователем, сигнал или штатный
/// конец потока.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum SessionOutcome {
    /// Остановлено штатно: «Стоп», сигнал или конец потока.
    Stopped,
    /// Авария тракта с сообщением для пользователя.
    Fatal(String),
}

/// Снимок сессии для потребителя (headless-цикл статусов или poll-таймер
/// окна): живость воркера, входы статуса, эфир и исход.
pub struct SessionTick {
    /// Жив ли worker-поток сессии.
    pub alive: bool,
    /// Сетевой статус reader'а.
    pub net: NetStatus,
    /// Звук уже запускался (предбуфер пройден, устройство открыто).
    pub playback_started: bool,
    /// Декодер в underrun-паузе (пауза декодера: звук доигрывает
    /// сэмпл-FIFO ещё до ~2,4 с — design D8).
    pub underrun: bool,
    /// Название станции из `icy-name`, начиная с первого соединения.
    pub station: Option<String>,
    /// Текущий трек из последнего метаблока.
    pub track: Option<TrackInfo>,
    /// Исход сессии после завершения воркера.
    pub outcome: Option<SessionOutcome>,
}

/// Запущенная сессия воспроизведения. Владелец останавливает её флагом
/// (`stop`), наблюдает состояние (`tick`, `outcome`) и забирает исход
/// (`join`). Слоты сессии читаются без блокировок тракта — poll не
/// мешает reader'у и декодеру.
pub struct SessionHandle {
    /// Флаг остановки сессии («Стоп» в окне): его же слушает reader.
    stop: Arc<AtomicBool>,
    /// Worker-поток; после `join` — None.
    worker: Option<JoinHandle<()>>,
    now_playing: SharedNowPlaying,
    net_status: SharedNetStatus,
    /// Ворота эфира: дренаж в `tick` публикует трек, когда его аудио
    /// зазвучало (design add-ether-sync D4).
    ether_gate: SharedEtherGate,
    /// Позиция звучания в аудиобайтах: ставит потребитель сэмплов при
    /// исчерпании чанка FIFO (design D8) — не позиция декодера, который
    /// бежит впереди звука на заполненный FIFO.
    sound_pos: Arc<AtomicU64>,
    /// Буфер тракта: underrun-флаг для сводного статуса.
    buffer: RingBuffer,
    /// «Звук запускался» — ставит воркер после открытия устройства.
    playback_started: Arc<AtomicBool>,
    /// Исход, публикуемый воркером при завершении.
    outcome: Arc<Mutex<Option<SessionOutcome>>>,
}

impl SessionHandle {
    /// Просит сессию остановиться: воркер глушит вывод и освобождает
    /// устройство за такт опроса, reader прекращает сеть. Идемпотентно.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    /// Жив ли worker-поток (сессия ещё работает).
    pub fn is_running(&self) -> bool {
        self.worker
            .as_ref()
            .is_some_and(|worker| !worker.is_finished())
    }

    /// Опубликованный исход сессии, если воркер уже завершился.
    pub fn outcome(&self) -> Option<SessionOutcome> {
        self.outcome
            .lock()
            .expect("замок исхода не отравлен: присваивание не паникует")
            .clone()
    }

    /// Снимок состояния для poll-потребителей (статусы, эфир, исход).
    /// Перед снапшотом дренирует ворота эфира (design add-ether-sync D4):
    /// смена трека, чьё аудио зазвучало, видна потребителю в этом же тике.
    pub fn tick(&self) -> SessionTick {
        drain_ether(&self.ether_gate, &self.sound_pos, &self.now_playing);
        // Копии под короткими замками: потребитель не держит reader
        // в Meta-callback и не мешает воркеру.
        let (station, track) = {
            let ether = self
                .now_playing
                .lock()
                .expect("замок эфира не отравлен: присваивание не паникует");
            (ether.station.clone(), ether.track.clone())
        };
        let net = *self
            .net_status
            .lock()
            .expect("замок статуса не отравлен: присваивание не паникует");
        SessionTick {
            alive: self.is_running(),
            net,
            playback_started: self.playback_started.load(Ordering::SeqCst),
            underrun: self.buffer.is_underrun(),
            station,
            track,
            outcome: self.outcome(),
        }
    }

    /// Ждёт завершения воркера и возвращает исход. Паника воркера
    /// классифицируется как авария (исход не был опубликован).
    /// Повторный вызов возвращает опубликованный исход.
    pub fn join(&mut self) -> SessionOutcome {
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            return SessionOutcome::Fatal("воркер сессии завершился аварийно".into());
        }
        self.outcome()
            .unwrap_or_else(|| SessionOutcome::Fatal("воркер сессии завершился аварийно".into()))
    }
}

/// Дренаж ворот эфира (design D4): публикует в `now_playing` смены,
/// чьё аудио зазвучало — `sound_pos` ставит потребитель сэмплов (design
/// D8), не декодер. Отдельная функция — проводка «позиция звучания →
/// дренаж → слот эфира» проверяется юнит-тестом без аудиоустройства.
/// Порядок захвата: замок ворот, затем замок эфира; reader берёт их
/// порознь (push / станция), цикла не возникает.
fn drain_ether(
    ether_gate: &crate::icy::EtherGate,
    sound_pos: &std::sync::atomic::AtomicU64,
    now_playing: &SharedNowPlaying,
) {
    let played = sound_pos.load(Ordering::Relaxed);
    if let Some(track) = ether_gate.drain_until(played) {
        now_playing
            .lock()
            .expect("замок эфира не отравлен: присваивание не паникует")
            .track = Some(track);
    }
}

/// Сессия воспроизведения: пространство имён для [`PlayerSession::start`].
pub struct PlayerSession;

impl PlayerSession {
    /// Запускает сессию воспроизведения `url`: свежие структуры тракта,
    /// reader-поток и worker с живым циклом (предбуфер → устройство →
    /// воспроизведение → классификация исхода). `process_stop` — флаг
    /// завершения процесса (сигнал): воркер слушает его наравне с
    /// флагом сессии. Ошибка `Err` — отказ ОС создать поток.
    pub fn start(url: String, process_stop: Arc<AtomicBool>) -> std::io::Result<SessionHandle> {
        let buffer = RingBuffer::phase1();
        let budget = GarbageBudget::shared();
        let link = Link::shared();
        let net_status: SharedNetStatus = Arc::new(Mutex::new(NetStatus::Connecting));
        let now_playing: SharedNowPlaying = Default::default();
        let ether_gate: SharedEtherGate = Default::default();
        let sound_pos = Arc::new(AtomicU64::new(0));
        let session_stop = Arc::new(AtomicBool::new(false));
        let playback_started = Arc::new(AtomicBool::new(false));
        let outcome = Arc::new(Mutex::new(None));
        let pipeline_outcome: SharedOutcome = Arc::new(Mutex::new(Outcome::Running));

        // Reader слушает флаг сессии — «Стоп» прекращает сеть немедленно;
        // процессный флаг видит воркер и закрывает буфер (reader уходит).
        spawn_reader(
            url,
            buffer.clone(),
            session_stop.clone(),
            budget.clone(),
            link.clone(),
            net_status.clone(),
            now_playing.clone(),
            ether_gate.clone(),
        )?;

        let worker = match std::thread::Builder::new()
            .name("session-worker".into())
            .spawn({
                let buffer = buffer.clone();
                let link = link.clone();
                let pipeline_outcome = pipeline_outcome.clone();
                let session_stop = session_stop.clone();
                let playback_started = playback_started.clone();
                let outcome = outcome.clone();
                let sound_pos = sound_pos.clone();
                move || {
                    // Паника воркера обязана стать Fatal-исходом: UI читает
                    // только слот (join там недоступен), тихое «Остановлено»
                    // без ошибки нарушило бы спеку player-ui. Диагностику
                    // паники печатает panic-хук; паттерн — как в
                    // `spawn_reader` и `Mp3Source::new`.
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        live_loop(
                            &buffer,
                            &budget,
                            &link,
                            &pipeline_outcome,
                            &process_stop,
                            &session_stop,
                            &playback_started,
                            &sound_pos,
                        )
                    }))
                    .unwrap_or_else(|_| {
                        SessionOutcome::Fatal("воркер сессии завершился аварийно".into())
                    });
                    // Разбор сессии: буфер закрывается всегда — reader
                    // прекращает сеть, декодер дочитывает остаток; исход
                    // публикуется последним, после него воркер завершается.
                    buffer.close();
                    *outcome
                        .lock()
                        .expect("замок исхода не отравлен: присваивание не паникует") =
                        Some(result);
                }
            }) {
            Ok(worker) => worker,
            Err(e) => {
                // Reader уже запущен, а хэндла сессии не будет: глушим
                // флагом и буфером, иначе поток навсегда останется
                // перекачивать сеть в никуда.
                session_stop.store(true, Ordering::SeqCst);
                buffer.close();
                return Err(e);
            }
        };

        Ok(SessionHandle {
            stop: session_stop,
            worker: Some(worker),
            now_playing,
            net_status,
            ether_gate,
            sound_pos,
            buffer,
            playback_started,
            outcome,
        })
    }
}

/// Живой цикл сессии (перенос `run()` из main.rs фазы 1): предбуфер →
/// тракт декодирования → аудиоустройство → ожидание конца потока.
/// Отличие от CLI-цикла: выход по флагу сессии, а не только процесса.
/// Устройство открывается и освобождается внутри — сессии не делят его.
#[allow(clippy::too_many_arguments)]
fn live_loop(
    buffer: &RingBuffer,
    budget: &Arc<GarbageBudget>,
    link: &SharedLink,
    pipeline_outcome: &SharedOutcome,
    process_stop: &Arc<AtomicBool>,
    session_stop: &Arc<AtomicBool>,
    playback_started: &Arc<AtomicBool>,
    sound_pos: &Arc<AtomicU64>,
) -> SessionOutcome {
    let stopping = || process_stop.load(Ordering::SeqCst) || session_stop.load(Ordering::SeqCst);

    // Предбуферизация: старт декодирования после накопления начального
    // объёма данных. Сетевые ретраи первого соединения — внутри reader'а,
    // выход здесь только по флагам и авариям до данных.
    while !stopping() && !buffer.is_prebuffered() {
        if link.reader_panicked() {
            // Паника reader'а до предбуфера (catch_unwind уже закрыл
            // буфер и напечатал диагностику): данных больше не будет.
            return SessionOutcome::Fatal(
                "поток чтения завершился до накопления предбуфера".into(),
            );
        }
        if link.fatal() {
            // Сервер отклонил запрос перманентно (HTTP 3xx/4xx): reader
            // завершился, повторные попытки бессмысленны.
            return SessionOutcome::Fatal("подключение отклонено сервером".into());
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    if stopping() {
        return SessionOutcome::Stopped;
    }

    let source = match Mp3Source::new(
        buffer.clone(),
        budget.clone(),
        session_stop.clone(),
        link.clone(),
        pipeline_outcome.clone(),
        sound_pos.clone(),
    ) {
        Ok(source) => source,
        Err(e) => {
            // Сигнал во время построения тракта — остановка, а не ошибка.
            if stopping() {
                return SessionOutcome::Stopped;
            }
            return SessionOutcome::Fatal(e.to_string());
        }
    };
    let (rate, channels) = source.spec();
    say(&format!("поток: MP3, {rate} Гц, {channels} кан."));

    // Вывод: устройство по умолчанию; Player сам конвертирует формат
    // источника в формат устройства. Гибель потока вывода наблюдаем
    // error-callback'ом cpal: без наблюдателя Player::empty на мёртвом
    // потоке не опустеет — сессия зависла бы с тишиной.
    let audio_crashed = Arc::new(AtomicBool::new(false));
    // device_sink локален: любой выход из цикла ниже освобождает
    // аудиоустройство (повторные сессии не делят его).
    let mut device_sink = match open_output_device(&audio_crashed) {
        Ok(device_sink) => device_sink,
        Err(e) => return SessionOutcome::Fatal(format!("аудиоустройство недоступно: {e}")),
    };
    // Предупреждение rodio при drop устройства не нужно: штатный выход
    // сопровождается своими строками.
    device_sink.log_on_drop(false);
    let player = rodio::Player::connect_new(device_sink.mixer());
    player.append(source);
    playback_started.store(true, Ordering::SeqCst);

    // Ждём конца потока: пустой Player (штатный конец) либо аварии
    // (вывод, reader) либо остановка — флаги опрашиваются такт за тактом.
    while !player.empty() {
        if audio_crashed.load(Ordering::SeqCst) {
            // Поток вывода умер: источник больше никем не опрашивается.
            break;
        }
        if link.reader_panicked() {
            // Паника reader'а в живом цикле: буфер закрыт, тракт дочитает
            // и опустеет, но исход уже не штатный.
            break;
        }
        if stopping() {
            break;
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    player.stop();
    drop(device_sink); // освобождаем аудиоустройство

    classify(
        stopping(),
        link,
        audio_crashed.load(Ordering::SeqCst),
        pipeline_outcome,
    )
}

/// Классификация исхода по флагам тракта (порядок и сообщения — прежний
/// `run()` main.rs). Аварии при активной остановке — `Stopped`: спека
/// требует штатный код, даже если события совпали.
fn classify(
    stopping: bool,
    link: &Link,
    audio_crashed: bool,
    pipeline_outcome: &SharedOutcome,
) -> SessionOutcome {
    if link.fatal() && !stopping {
        return SessionOutcome::Fatal("подключение отклонено сервером".into());
    }
    if link.reader_panicked() && !stopping {
        return SessionOutcome::Fatal("поток чтения завершился аварийно".into());
    }
    if audio_crashed && !stopping {
        return SessionOutcome::Fatal("поток вывода аварийно завершился (аудиоустройство)".into());
    }
    // Running недостижим: источник отдаёт None только после фиксации
    // исхода. Сигнал/«Стоп» и штатный конец — Stopped.
    match *pipeline_outcome
        .lock()
        .expect("замок исхода не отравлен: присваивание не паникует")
    {
        Outcome::DecoderError if !stopping => {
            SessionOutcome::Fatal("воспроизведение прервано ошибкой тракта".into())
        }
        _ => SessionOutcome::Stopped,
    }
}

/// Открывает вывод на устройстве по умолчанию с наблюдателем гибели
/// аудио-потока: error-callback cpal поднимает `crashed` (устройство
/// отключили, ошибка драйвера) — сессия обязана завершиться аварией,
/// а не ждать `Player::empty`, который на мёртвом потоке не опустеет.
///
/// Основной путь не перебирает конфигурации и устройства; при неудаче
/// откатываемся на `open_default_sink` rodio (там наблюдателя поставить
/// нельзя) — авария вывода в запасном пути видна только как пропавший
/// звук, лучше чем отказ в запуске на нестандартных конфигурациях.
/// Обе неудачи — комбинированная диагностика: первичная ошибка не
/// глотается вторичной.
fn open_output_device(crashed: &Arc<AtomicBool>) -> Result<rodio::MixerDeviceSink, String> {
    let watched = rodio::DeviceSinkBuilder::from_default_device().and_then(|builder| {
        builder
            .with_error_callback({
                let crashed = crashed.clone();
                move |error| {
                    // Текст ошибки обязателен (design add-ether-sync D8):
                    // без него Fatal ложно винит «аудиоустройство»,
                    // а причина была в блокировке колбэка или смерти
                    // устройства — теперь различимо в stderr.
                    say(&format!("аудиопоток: ошибка вывода: {error}"));
                    crashed.store(true, Ordering::SeqCst);
                }
            })
            .open_stream()
    });
    match watched {
        Ok(sink) => Ok(sink),
        Err(primary) => {
            let fallback = rodio::DeviceSinkBuilder::open_default_sink().map_err(|fallback| {
                format!(
                    "первичная ошибка открытия вывода: {primary}; перебор устройств rodio: {fallback}"
                )
            });
            if fallback.is_ok() {
                // Деградация наблюдаемости: на запасном пути error-callback
                // поставить нельзя — гибель вывода будет видна только
                // тишиной. Пользователь обязан узнать об этом из лога.
                say("аудиовывод открыт запасным путём: наблюдение гибели потока недоступно");
            }
            fallback
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::icy::{EtherGate, TrackInfo};

    /// Проводка дренажа (design D4, ревью hunter#3): смена публикуется
    /// строго по позиции звучания — не раньше и не теряясь. Запись со
    /// смещением 0 легитимно публикуется нулевой позицией («ничего ещё
    /// не звучало» — это и есть момент до первого байта), шум одного
    /// чанка ~26 мс.
    #[test]
    fn drain_ether_publishes_by_sounding_position() {
        let gate = EtherGate::default();
        let now_playing: SharedNowPlaying = Default::default();
        let sound_pos = std::sync::atomic::AtomicU64::new(0);

        let first = TrackInfo::Raw {
            title: "первый".to_owned(),
        };
        let second = TrackInfo::Raw {
            title: "второй".to_owned(),
        };
        gate.push(100, first.clone());
        gate.push(200, second.clone());

        drain_ether(&gate, &sound_pos, &now_playing);
        assert!(
            now_playing
                .lock()
                .expect("замок эфира не отравлен: присваивание не паникует")
                .track
                .is_none(),
            "до звучания публикаций нет"
        );

        sound_pos.store(150, Ordering::Relaxed);
        drain_ether(&gate, &sound_pos, &now_playing);
        assert_eq!(
            now_playing
                .lock()
                .expect("замок эфира не отравлен: присваивание не паникует")
                .track,
            Some(first),
            "первая смена опубликована по позиции звучания"
        );

        sound_pos.store(300, Ordering::Relaxed);
        drain_ether(&gate, &sound_pos, &now_playing);
        assert_eq!(
            now_playing
                .lock()
                .expect("замок эфира не отравлен: присваивание не паникует")
                .track,
            Some(second),
            "вторая смена опубликована по позиции звучания"
        );
    }

    /// Link с поднятым флагом fatal (модель отказа сервера).
    fn fatal_link() -> SharedLink {
        let link = Link::shared();
        link.mark_fatal();
        link
    }

    /// Таблица классификации: аварии по флагам — Fatal с прежними
    /// сообщениями CLI; любая остановка глушит аварию — Stopped.
    #[test]
    fn classify_maps_tract_failures_and_stops() {
        let clean_outcome: SharedOutcome = Arc::new(Mutex::new(Outcome::EndOfStream));
        let link = Link::shared();

        // Штатный конец потока и остановка тракта — Stopped.
        assert_eq!(
            classify(false, &link, false, &clean_outcome),
            SessionOutcome::Stopped
        );
        let stopped_outcome: SharedOutcome = Arc::new(Mutex::new(Outcome::Stopped));
        assert_eq!(
            classify(false, &link, false, &stopped_outcome),
            SessionOutcome::Stopped
        );

        // Отказ сервера.
        assert_eq!(
            classify(false, &fatal_link(), false, &clean_outcome),
            SessionOutcome::Fatal("подключение отклонено сервером".into())
        );

        // Паника reader'а.
        let panicked = Link::shared();
        panicked.mark_reader_panicked();
        assert_eq!(
            classify(false, &panicked, false, &clean_outcome),
            SessionOutcome::Fatal("поток чтения завершился аварийно".into())
        );

        // Гибель вывода.
        assert_eq!(
            classify(false, &link, true, &clean_outcome),
            SessionOutcome::Fatal("поток вывода аварийно завершился (аудиоустройство)".into())
        );

        // Ошибка декодера.
        let decoder_error: SharedOutcome = Arc::new(Mutex::new(Outcome::DecoderError));
        assert_eq!(
            classify(false, &link, false, &decoder_error),
            SessionOutcome::Fatal("воспроизведение прервано ошибкой тракта".into())
        );

        // Любая авария при остановке — Stopped (спека: сигнал — код 0).
        assert_eq!(
            classify(true, &fatal_link(), true, &decoder_error),
            SessionOutcome::Stopped
        );
    }
}

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

//! sevfm — настольный плеер онлайн-радио «Говорит Севастополь»
//! на Rust + Slint.
//!
//! Фаза 1 — тракт живого MP3-стрима: reader-поток → кольцевой буфер →
//! покадровый декод → вывод PCM. Статусы и диагностика — в stderr.
//! Использование: `sevfm [URL]` (без аргументов — станция по умолчанию).
//! Остановка: Ctrl+C (SIGINT) или SIGTERM — код 0 в течение секунды.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use sevfm::buffer::RingBuffer;
use sevfm::net::{
    self, GarbageBudget, Link, NetStatus, PlaybackStatus, SharedNetStatus, playback_status,
};
use sevfm::pipeline::{DEFAULT_STREAM_URL, Mp3Source, Outcome, SharedOutcome};
use sevfm::{redact_url, say};

/// Интервал опроса флага остановки и статусов.
const POLL_INTERVAL: Duration = Duration::from_millis(100);
/// Код выхода при ошибке запуска тракта или сбое живого цикла
/// (спека: 0 — штатная остановка, 2 — неуспех).
const EXIT_FAILURE: i32 = 2;

fn main() {
    std::process::exit(run());
}

/// Коды выхода: 0 — штатная остановка (сигнал или конец потока),
/// 2 — ошибка запуска тракта (нет аудиоустройства, поток не MP3) или
/// сбой живого цикла (ошибка декодера, аварийное завершение reader'а).
fn run() -> i32 {
    // CLI: URL из аргумента, по умолчанию — адрес станции. args_os вместо
    // args: невалидный UTF-8 не паникует, lossy-конверсии достаточно
    // (URL с U+FFFD просто не подключится с внятной диагностикой).
    let url = std::env::args_os()
        .nth(1)
        .map(|a| a.to_string_lossy().into_owned())
        .unwrap_or_else(|| DEFAULT_STREAM_URL.to_owned());

    // SIGINT/SIGTERM → атомарный флаг. Порядок SeqCst во всех точках
    // (запись из обработчика сигнала, чтение из потоков тракта).
    let stop = Arc::new(AtomicBool::new(false));
    if ctrlc::set_handler({
        let stop = stop.clone();
        move || stop.store(true, Ordering::SeqCst)
    })
    .is_err()
    {
        say("не удалось установить обработчик сигнала");
        return EXIT_FAILURE;
    }
    // Сторож выхода: сигнал может прийти, когда главный поток заблокирован
    // (ожидание данных, декодирование) — выход делаем сразу, без
    // drop/join аудио-потоков (design D5: ресурсы освобождает процесс).
    {
        let stop = stop.clone();
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                std::thread::sleep(POLL_INTERVAL);
            }
            say("остановка по сигналу…");
            std::process::exit(0);
        });
    }

    // Тракт: reader-поток наполняет кольцевой буфер, декодер читает
    // из него — воспроизведение не из сети (design D4). Link — связка
    // reader ↔ декодер (мусорный лимит → переподключение, эпоха
    // соединений); слот исхода — финальная классификация кода выхода.
    say(&format!("подключение к {}…", redact_url(&url)));
    let buffer = RingBuffer::phase1();
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let outcome_slot: SharedOutcome = Arc::new(Mutex::new(Outcome::Running));
    let net_status: SharedNetStatus = Arc::new(Mutex::new(NetStatus::Connecting));
    // Хэндл потока не храним: паника reader'а видна через флаг в Link
    // (catch_unwind), нормальный выход потока без остановки в живом
    // тракте невозможен.
    if let Err(e) = net::spawn_reader(
        url,
        buffer.clone(),
        stop.clone(),
        budget.clone(),
        link.clone(),
        net_status.clone(),
    ) {
        say(&format!("не удалось запустить поток чтения: {e}"));
        return EXIT_FAILURE;
    }

    // Предбуферизация: старт декодирования после накопления начального
    // объёма данных (спека о непрерывности). Сетевые ретраи первого
    // соединения здесь же — выход только по сигналу.
    while !stop.load(Ordering::SeqCst) && !buffer.is_prebuffered() {
        if link.reader_panicked() {
            // Паника reader'а до предбуфера (catch_unwind уже закрыл буфер
            // и напечатал диагностику): данных больше не будет.
            say("поток чтения завершился до накопления предбуфера");
            return EXIT_FAILURE;
        }
        if link.fatal() {
            // Сервер отклонил запрос перманентно (HTTP 3xx/4xx): reader
            // завершился, повторные попытки бессмысленны.
            say("подключение отклонено сервером");
            return EXIT_FAILURE;
        }
        std::thread::sleep(POLL_INTERVAL);
    }
    if stop.load(Ordering::SeqCst) {
        return 0;
    }

    let source = match Mp3Source::new(
        buffer.clone(),
        budget,
        stop.clone(),
        link.clone(),
        outcome_slot.clone(),
    ) {
        Ok(source) => source,
        Err(e) => {
            // Сигнал во время построения тракта — остановка (код 0),
            // а не ошибка старта.
            if stop.load(Ordering::SeqCst) {
                return 0;
            }
            say(&e.to_string());
            return EXIT_FAILURE;
        }
    };
    let (rate, channels) = source.spec();
    say(&format!("поток: MP3, {rate} Гц, {channels} кан."));

    // Вывод: устройство по умолчанию; Player сам конвертирует формат
    // источника в формат устройства — «параметры из потока» (спека).
    // Гибель потока вывода (устройство отключили, ошибка драйвера)
    // наблюдаем error-callback'ом cpal: без наблюдателя Player::empty
    // на мёртвом потоке не опустеет — CLI завис бы с тишиной.
    let audio_crashed = Arc::new(AtomicBool::new(false));
    let mut device_sink = match open_output_device(&audio_crashed) {
        Ok(device_sink) => device_sink,
        Err(e) => {
            say(&format!("аудиоустройство недоступно: {e}"));
            return EXIT_FAILURE;
        }
    };
    // Штатный выход сопровождаем своими строками; предупреждение rodio
    // при drop устройства не нужно.
    device_sink.log_on_drop(false);
    let player = rodio::Player::connect_new(device_sink.mixer());
    player.append(source);

    // Ждём конца потока (сигнал обслуживает сторож): статус выводится
    // по переходам — Подключение уже напечатан, дальше живой цикл.
    let mut last_status = Some(PlaybackStatus::Connecting);
    while !player.empty() {
        if audio_crashed.load(Ordering::SeqCst) {
            // Поток вывода умер: источник больше никем не опрашивается,
            // empty() не опустеет — выходим и классифицируем как аварию.
            break;
        }
        if link.reader_panicked() {
            // Паника reader'а в живом цикле (catch_unwind закрыл буфер —
            // тракт дочитает и опустеет, но исход уже не штатный).
            break;
        }
        let net = *net_status
            .lock()
            .expect("замок статуса не отравлен: присваивание не паникует");
        // Живой цикл: воспроизведение уже запущено; пауза — точный флаг
        // underrun декодера, а не «буфер пуст» (звук идёт и на низком
        // уровне заполнения).
        let status = playback_status(net, true, buffer.is_underrun());
        if last_status != Some(status) {
            say(&format!("состояние: {}", status.label()));
            last_status = Some(status);
        }
        std::thread::sleep(POLL_INTERVAL);
    }

    player.stop();
    drop(device_sink); // освобождаем аудиоустройство

    // Классификация исхода: паника reader'а, гибель потока вывода или
    // ошибка декодера вне остановки — неуспех (код 2); сигнал и штатный
    // конец — 0. Running недостижим: источник отдаёт None только после
    // фиксации исхода. Сигнал проверяем после аварийных флагов — спека
    // требует код 0, даже если события совпали.
    if link.fatal() && !stop.load(Ordering::SeqCst) {
        say("подключение отклонено сервером");
        return EXIT_FAILURE;
    }
    if link.reader_panicked() && !stop.load(Ordering::SeqCst) {
        say("поток чтения завершился аварийно");
        return EXIT_FAILURE;
    }
    if audio_crashed.load(Ordering::SeqCst) && !stop.load(Ordering::SeqCst) {
        // Гибель потока вывода (error-callback cpal) — не штатный конец:
        // авария тракта, код 2, а не молчаливый зависший процесс.
        say("поток вывода аварийно завершился (аудиоустройство)");
        return EXIT_FAILURE;
    }
    match *outcome_slot
        .lock()
        .expect("замок исхода не отравлен: присваивание не паникует")
    {
        Outcome::DecoderError => {
            say("воспроизведение прервано ошибкой тракта");
            EXIT_FAILURE
        }
        _ => 0,
    }
}

/// Открывает вывод на устройстве по умолчанию с наблюдателем гибели
/// аудио-потока: error-callback cpal поднимает `crashed` (устройство
/// отключили, ошибка драйвера) — CLI обязан завершиться с кодом 2, а не
/// ждать `Player::empty`, который на мёртвом потоке не опустеет.
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
                move |_error| crashed.store(true, Ordering::SeqCst)
            })
            .open_stream()
    });
    match watched {
        Ok(sink) => Ok(sink),
        Err(primary) => rodio::DeviceSinkBuilder::open_default_sink().map_err(|fallback| {
            format!(
                "первичная ошибка открытия вывода: {primary}; перебор устройств rodio: {fallback}"
            )
        }),
    }
}

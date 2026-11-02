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

//! Мок-сервер живого MP3-потока для детерминированных тестов без сети
//! (design D4): локальный `TcpListener` со скриптуемыми сценариями —
//! валидные байты с обрывом соединения, выдача чанками с паузами,
//! бесконечный мусор, молчание, не-MP3 ответ, HTTP-статус.
//!
//! Каждому принятому соединению выдаётся следующий сценарий из списка;
//! когда сценарии исчерпаны — повторяется последний (переподключения
//! продолжают получать данные). Все потоки-обработчики завершаются по
//! `stop()`/`Drop`: молчащие — опросом флага остановки, пишущие — ошибкой
//! записи после ухода клиента.

// Каждый файл tests/*.rs — отдельный крейт и компилирует этот модуль
// независимо: сценарии, не задействованные данным крейтом, считались бы
// мёртвым кодом, хотя используются соседними тестами.
#![allow(dead_code)]

use std::collections::VecDeque;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Интервал опроса флага остановки (accept-цикл, «молчащие» сценарии).
const POLL_INTERVAL: Duration = Duration::from_millis(5);
/// Таймаут чтения заголовков запроса клиента.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);
/// Потолок чтения заголовков запроса.
const REQUEST_HEAD_LIMIT: usize = 8 * 1024;
/// Размер чанка бесконечного мусора.
const GARBAGE_CHUNK: usize = 8 * 1024;

/// Поведение соединения после HTTP-заголовков ответа.
#[derive(Clone)]
pub enum Body {
    /// Отправить байты и оборвать соединение — клиент увидит EOF.
    BytesThenClose(Vec<u8>),
    /// Отправить байты чанками с паузой между ними, затем молчать,
    /// держа соединение открытым.
    PacedThenHold {
        bytes: Vec<u8>,
        chunk: usize,
        pace: Duration,
    },
    /// Медленная выдача с обрывом: байты чанками с паузой, затем EOF —
    /// клиент успевает пронаблюдать чтение до потери соединения.
    PacedThenClose {
        bytes: Vec<u8>,
        chunk: usize,
        pace: Duration,
    },
    /// Отправить байты, затем бесконечный детерминированный мусор, пока
    /// клиент не отключится (ошибка записи завершает обработчик).
    BytesThenGarbage { bytes: Vec<u8> },
    /// Не отправлять ничего, держать соединение открытым (молчун).
    Silence,
}

/// ICY-договорённость в ответе мока: заголовки `icy-metaint`/`icy-name`.
#[derive(Clone)]
pub struct IcyHeaders {
    pub metaint: usize,
    pub name: Option<String>,
}

/// Сценарий одного соединения: HTTP-статус + тело (+ ICY-заголовки).
#[derive(Clone)]
pub struct Scenario {
    pub status: u16,
    pub body: Body,
    /// None — сервер без ICY (клиент получает чистый MP3).
    pub icy: Option<IcyHeaders>,
}

impl Scenario {
    /// Валидный поток: байты, затем обрыв соединения.
    pub fn stream(bytes: Vec<u8>) -> Self {
        Self {
            status: 200,
            body: Body::BytesThenClose(bytes),
            icy: None,
        }
    }

    /// Поток с ICY-договорённостью: кадрированные байты (см. `icy_frame`),
    /// ответ включает `icy-metaint`/`icy-name`, затем обрыв соединения.
    pub fn icy_stream(bytes: Vec<u8>, metaint: usize, station: &str) -> Self {
        Self {
            status: 200,
            body: Body::BytesThenClose(bytes),
            icy: Some(IcyHeaders {
                metaint,
                name: Some(station.to_owned()),
            }),
        }
    }

    /// Медленная выдача с ICY-договорённостью: соединение держится.
    pub fn icy_paced_then_hold(
        bytes: Vec<u8>,
        chunk: usize,
        pace: Duration,
        metaint: usize,
        station: &str,
    ) -> Self {
        Self {
            status: 200,
            body: Body::PacedThenHold { bytes, chunk, pace },
            icy: Some(IcyHeaders {
                metaint,
                name: Some(station.to_owned()),
            }),
        }
    }

    /// Медленная выдача: байты чанками с паузой, соединение держится.
    pub fn paced_then_hold(bytes: Vec<u8>, chunk: usize, pace: Duration) -> Self {
        Self {
            status: 200,
            body: Body::PacedThenHold { bytes, chunk, pace },
            icy: None,
        }
    }

    /// Медленная выдача с обрывом: байты чанками с паузой, затем EOF.
    pub fn paced_then_close(bytes: Vec<u8>, chunk: usize, pace: Duration) -> Self {
        Self {
            status: 200,
            body: Body::PacedThenClose { bytes, chunk, pace },
            icy: None,
        }
    }

    /// Валидный префикс, затем бесконечный мусор (мусорный бюджет →
    /// переподключение).
    pub fn garbage_forever(prefix: Vec<u8>) -> Self {
        Self {
            status: 200,
            body: Body::BytesThenGarbage { bytes: prefix },
            icy: None,
        }
    }

    /// Молчаливый сервер: соединение принимается, ответ не приходит
    /// (блокированное чтение клиента — сценарий остановки reader'а).
    pub fn silence() -> Self {
        Self {
            status: 200,
            body: Body::Silence,
            icy: None,
        }
    }

    /// Не-MP3 ответ: статус 200, большое HTML-тело (реалистичная страница
    /// вместо потока — достаточный объём для предбуфера и probe), обрыв.
    pub fn non_mp3() -> Self {
        let mut body = Vec::new();
        while body.len() < 128 * 1024 {
            body.extend_from_slice(b"<html><body>not a stream at all</body></html>\n");
        }
        Self {
            status: 200,
            body: Body::BytesThenClose(body),
            icy: None,
        }
    }

    /// HTTP-статус без тела (сервер недоступен по смыслу ответа).
    pub fn status(code: u16) -> Self {
        Self {
            status: code,
            body: Body::BytesThenClose(Vec::new()),
            icy: None,
        }
    }
}

/// Собирает ICY-кадрированный поток из аудиобайтов: аудио кусками
/// `metaint`, после каждого полного куска — метаблок с `StreamTitle`
/// из `titles` (по циклу, содержимое выровнено кратно 16 байтов).
/// Независимый от библиотечного фреймера построитель — оракул тестов.
pub fn icy_frame(audio: &[u8], metaint: usize, titles: &[&str]) -> Vec<u8> {
    assert!(!titles.is_empty(), "нужен хотя бы один StreamTitle");
    let mut stream = Vec::new();
    for (index, piece) in audio.chunks(metaint).enumerate() {
        stream.extend_from_slice(piece);
        if piece.len() < metaint {
            break;
        }
        let title = titles[index % titles.len()];
        let mut content = format!("StreamTitle='{title}';").into_bytes();
        while content.len() % 16 != 0 {
            content.push(0);
        }
        let len_byte = u8::try_from(content.len() / 16).expect("≤ 4080 байтов");
        stream.push(len_byte);
        stream.extend_from_slice(&content);
    }
    stream
}

/// Локальный мок-сервер. Останавливается в `Drop` (или явно `stop`):
/// слушатель закрывается, все обработчики соединений присоединяются.
pub struct MockServer {
    listener: TcpListener,
    connections: Arc<AtomicUsize>,
    /// Сколько запросов содержали `Icy-MetaData: 1` — договорённость
    /// видна моку, тесты ассертят переговоры на каждом соединении.
    icy_requests: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
    accept_thread: Option<JoinHandle<()>>,
    workers: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl MockServer {
    /// Поднимает сервер на `127.0.0.1` со свободным портом.
    ///
    /// Паникует при пустом сценарном списке: минимально один сценарий
    /// нужен как повторяемый хвост для переподключений.
    pub fn start(script: Vec<Scenario>) -> Self {
        assert!(!script.is_empty(), "нужен хотя бы один сценарий");
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind на 127.0.0.1:0");
        // Неблокирующий accept + опрос флага остановки: без трюков
        // с самоподключением для пробуждения blocked-accept.
        listener
            .set_nonblocking(true)
            .expect("неблокирующий accept");

        let script = Arc::new(Mutex::new(script.into()));
        let connections = Arc::new(AtomicUsize::new(0));
        let icy_requests = Arc::new(AtomicUsize::new(0));
        let shutdown = Arc::new(AtomicBool::new(false));
        let workers = Arc::new(Mutex::new(Vec::new()));

        let accept_thread = {
            let listener = listener
                .try_clone()
                .expect("клонирование слушателя для потока accept");
            let script = script.clone();
            let connections = connections.clone();
            let icy_requests = icy_requests.clone();
            let shutdown = shutdown.clone();
            let workers = workers.clone();
            thread::spawn(move || {
                accept_loop(
                    &listener,
                    &script,
                    &connections,
                    &icy_requests,
                    &shutdown,
                    &workers,
                )
            })
        };

        Self {
            listener,
            connections,
            icy_requests,
            shutdown,
            accept_thread: Some(accept_thread),
            workers,
        }
    }

    /// URL потока для HTTP-клиента (`open_stream`).
    pub fn url(&self) -> String {
        let addr = self.listener.local_addr().expect("адрес слушателя");
        format!("http://{addr}/stream")
    }

    /// Число принятых соединений (переподключения видны как рост).
    pub fn connections(&self) -> usize {
        self.connections.load(Ordering::SeqCst)
    }

    /// Число запросов с заголовком `Icy-MetaData: 1`.
    pub fn icy_requests(&self) -> usize {
        self.icy_requests.load(Ordering::SeqCst)
    }

    /// Останавливает сервер и присоединяет все потоки. Идемпотентно.
    pub fn stop(&mut self) {
        let Some(accept_thread) = self.accept_thread.take() else {
            return;
        };
        self.shutdown.store(true, Ordering::SeqCst);
        let _ = accept_thread.join();
        let mut workers = self.workers.lock().expect("замок воркеров не отравлен");
        for worker in workers.drain(..) {
            let _ = worker.join();
        }
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Цикл приёма соединений: каждому — следующий сценарий и свой поток.
fn accept_loop(
    listener: &TcpListener,
    script: &Mutex<VecDeque<Scenario>>,
    connections: &AtomicUsize,
    icy_requests: &Arc<AtomicUsize>,
    shutdown: &Arc<AtomicBool>,
    workers: &Mutex<Vec<JoinHandle<()>>>,
) {
    loop {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                connections.fetch_add(1, Ordering::SeqCst);
                let scenario = next_scenario(script);
                let icy_requests = icy_requests.clone();
                let shutdown = shutdown.clone();
                let worker = thread::spawn(move || {
                    handle_connection(stream, scenario, &icy_requests, &shutdown)
                });
                workers
                    .lock()
                    .expect("замок воркеров не отравлен")
                    .push(worker);
            }
            Err(_) => thread::sleep(POLL_INTERVAL),
        }
    }
}

/// Следующий сценарий: исчерпаны — повторяется последний.
fn next_scenario(script: &Mutex<VecDeque<Scenario>>) -> Scenario {
    let mut queue = script.lock().expect("замок сценарного списка не отравлен");
    if queue.len() > 1 {
        queue.pop_front().expect("длина больше единицы проверена")
    } else {
        queue.front().expect("список не пуст").clone()
    }
}

/// Обслуживает одно соединение по сценарию.
fn handle_connection(
    mut stream: TcpStream,
    scenario: Scenario,
    icy_requests: &AtomicUsize,
    shutdown: &AtomicBool,
) {
    let _ = stream.set_nodelay(true);
    // Запрос вычитываем до конца заголовков: HTTP-клиент ждёт ответа
    // только после отправки запроса. Попутно считаем договорённость о
    // метаданных — клиент обязан просить её на каждом соединении.
    if let Some(head) = read_request_head(&mut stream)
        && head.to_ascii_lowercase().contains("icy-metadata: 1")
    {
        icy_requests.fetch_add(1, Ordering::SeqCst);
    }
    if write_response_head(&mut stream, scenario.status, scenario.icy.as_ref()).is_err() {
        return;
    }
    match scenario.body {
        Body::BytesThenClose(bytes) => {
            let _ = stream.write_all(&bytes);
            let _ = stream.shutdown(Shutdown::Both);
        }
        Body::PacedThenHold { bytes, chunk, pace } => {
            send_paced(&mut stream, &bytes, chunk, pace, shutdown);
            hold_until_stop(shutdown);
        }
        Body::PacedThenClose { bytes, chunk, pace } => {
            send_paced(&mut stream, &bytes, chunk, pace, shutdown);
            let _ = stream.shutdown(Shutdown::Both);
        }
        Body::BytesThenGarbage { bytes } => {
            if stream.write_all(&bytes).is_err() {
                return;
            }
            let mut garbage = Garbage::default();
            let mut chunk = vec![0u8; GARBAGE_CHUNK];
            loop {
                garbage.fill(&mut chunk);
                // Отключение клиента = ошибка записи = конец обработчика.
                if stream.write_all(&chunk).is_err() {
                    return;
                }
            }
        }
        Body::Silence => hold_until_stop(shutdown),
    }
}

/// Молчание с удержанием соединения до остановки сервера.
/// Медленная выдача байтов чанками с паузами; остановка сервера или уход
/// клиента прерывают выдачу.
fn send_paced(
    stream: &mut TcpStream,
    bytes: &[u8],
    chunk: usize,
    pace: Duration,
    shutdown: &AtomicBool,
) {
    for piece in bytes.chunks(chunk.max(1)) {
        if shutdown.load(Ordering::SeqCst) {
            return;
        }
        if stream.write_all(piece).is_err() {
            return;
        }
        thread::sleep(pace);
    }
}

fn hold_until_stop(shutdown: &AtomicBool) {
    while !shutdown.load(Ordering::SeqCst) {
        thread::sleep(POLL_INTERVAL);
    }
}

/// Читает заголовки запроса до `\r\n\r\n` (потолок `REQUEST_HEAD_LIMIT`).
/// Ошибки и таймаут не фатальны: ответ отправляется всё равно.
fn read_request_head(stream: &mut TcpStream) -> Option<String> {
    let _ = stream.set_read_timeout(Some(REQUEST_TIMEOUT));
    let mut head = Vec::new();
    let mut chunk = [0u8; 512];
    while head.len() < REQUEST_HEAD_LIMIT {
        match stream.read(&mut chunk) {
            Ok(0) | Err(_) => return None,
            Ok(n) => {
                head.extend_from_slice(&chunk[..n]);
                if head.ends_with(b"\r\n\r\n") {
                    return Some(String::from_utf8_lossy(&head).into_owned());
                }
            }
        }
    }
    None
}

/// Пишет строку статуса и заголовки ответа (включая ICY-договорённость
/// сценария, если есть).
fn write_response_head(
    stream: &mut TcpStream,
    status: u16,
    icy: Option<&IcyHeaders>,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        403 => "Forbidden",
        404 => "Not Found",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let content_type = if status == 200 {
        "audio/mpeg"
    } else {
        "text/plain"
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nConnection: close\r\n"
    )?;
    if let Some(icy) = icy {
        write!(stream, "icy-metaint: {}\r\n", icy.metaint)?;
        if let Some(name) = &icy.name {
            write!(stream, "icy-name: {name}\r\n")?;
        }
    }
    write!(stream, "\r\n")
}

/// Детерминированный мусор (xorshift) — одинаковый между запусками.
struct Garbage(u64);

impl Default for Garbage {
    fn default() -> Self {
        Self(0x9E37_79B9_7F4A_7C15)
    }
}

impl Garbage {
    fn fill(&mut self, buf: &mut [u8]) {
        for byte in buf {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            *byte = self.0 as u8;
        }
    }
}

/// Путь фикстуры живого среза потока (256 КиБ) — единственная точка
/// определения в библиотеке (`pipeline::FIXTURE_PATH`), тесты не дублируют.
pub fn fixture_path() -> std::path::PathBuf {
    std::path::PathBuf::from(sevfm::pipeline::FIXTURE_PATH)
}

/// Байты фикстуры живого среза.
pub fn fixture_bytes() -> Vec<u8> {
    let bytes = std::fs::read(fixture_path()).expect("фикстура stream-sample.mp3 читается");
    assert!(!bytes.is_empty(), "фикстура не пуста");
    bytes
}

/// «Проигрывает» порцию аудио для ворот эфира (design add-ether-sync D7):
/// читает из буфера до `bytes` байтов порциями (доступное без ожидания
/// накопления, как декодер у живого потока; сухое чтение завершает порцию)
/// и дренирует ворота по позиции чтения — те же шаги, что `tick()` сессии
/// делает для потребителя. Возвращает прочитанные байты.
pub fn play_chunk(
    buffer: &sevfm::buffer::RingBuffer,
    ether_gate: &sevfm::icy::EtherGate,
    now_playing: &sevfm::icy::SharedNowPlaying,
    bytes: usize,
) -> Vec<u8> {
    let mut played = Vec::with_capacity(bytes);
    let mut sink = vec![0u8; 4 * 1024];
    while played.len() < bytes {
        let want = (bytes - played.len()).min(sink.len());
        match buffer.read(&mut sink[..want], Duration::from_millis(100)) {
            Ok(n) => played.extend_from_slice(&sink[..n]),
            // Сухое чтение: порция воспроизведения закончена — дренируем
            // то, что прочитано (пустое чтение позицию звука не двигает).
            Err(sevfm::buffer::ReadError::Timeout) => break,
            Err(sevfm::buffer::ReadError::Closed) => {
                panic!("буфер закрыт раньше конца воспроизведения")
            }
        }
    }
    if let Some(track) = ether_gate.drain_until(buffer.total_read()) {
        now_playing
            .lock()
            .expect("замок эфира не отравлен: присваивание не паникует")
            .track = Some(track);
    }
    played
}

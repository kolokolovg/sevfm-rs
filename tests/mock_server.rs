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

//! Проверки мок-сервера (задача 1.2): каждый сценарий детерминированно
//! ведёт себя и с сырым `TcpStream`, и с реальным HTTP-клиентом тракта
//! (`open_stream`) — фундамент для тестов 2.x–4.x без сети.

mod common;

use std::io::{ErrorKind, Read, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant};

use common::{MockServer, Scenario};
use sevfm::net::{self, StreamError};

/// Читает из потока `total` байтов с общим дедлайном.
fn read_exact_deadline(reader: &mut impl Read, total: usize, deadline: Duration) -> Vec<u8> {
    let mut out = vec![0u8; total];
    let mut filled = 0;
    let deadline = Instant::now() + deadline;
    while filled < total {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(!remaining.is_zero(), "дедлайн чтения {total} байтов истёк");
        let n = reader
            .read(&mut out[filled..])
            .expect("чтение без ошибки соединения");
        assert!(n > 0, "сервер закрыл соединение до {total} байтов");
        filled += n;
    }
    out
}

/// Открывает соединение с GET-запросом (сырой клиент).
fn raw_get(url: &str) -> TcpStream {
    let authority = url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .expect("в URL есть хост с портом");
    let stream = TcpStream::connect(authority).expect("соединение с моком");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut stream = stream;
    stream
        .write_all(b"GET /stream HTTP/1.1\r\nHost: mock\r\n\r\n")
        .expect("запрос отправлен");
    stream
}

/// Читает заголовки ответа до пустой строки, возвращает остаток буфера.
fn read_head(stream: &mut TcpStream) -> Vec<u8> {
    let mut head = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        stream.read_exact(&mut byte).expect("заголовок читается");
        head.push(byte[0]);
        if head.ends_with(b"\r\n\r\n") {
            return head;
        }
    }
}

/// Сценарий «поток»: байты фикстуры доходят целиком, затем соединение
/// оборвано (EOF) — клиент видит конец данных.
#[test]
fn stream_scenario_sends_bytes_then_eof() {
    let fixture = common::fixture_bytes();
    let payload = fixture[..32 * 1024].to_vec();
    let mut server = MockServer::start(vec![Scenario::stream(payload.clone())]);

    let mut client = raw_get(&server.url());
    let head = read_head(&mut client);
    assert!(
        head.starts_with(b"HTTP/1.1 200"),
        "статус 200: {:?}",
        String::from_utf8_lossy(&head[..12])
    );

    let mut body = Vec::new();
    client.read_to_end(&mut body).expect("тело читается до EOF");
    assert_eq!(body, payload, "тело совпадает с фикстурой, затем EOF");

    server.stop();
}

/// Мок совместим с реальным клиентом тракта: `open_stream` проходит,
/// тело читается по мере поступления.
#[test]
fn stream_scenario_works_with_open_stream_client() {
    let fixture = common::fixture_bytes();
    let payload = fixture[..64 * 1024].to_vec();
    let mut server = MockServer::start(vec![Scenario::stream(payload.clone())]);

    let mut reader = net::open_stream(&server.url()).expect("open_stream против мока");
    let body = read_exact_deadline(&mut reader, payload.len(), Duration::from_secs(10));
    assert_eq!(body, payload);

    // Обрыв: EOF после тела.
    let mut tail = [0u8; 16];
    assert_eq!(reader.read(&mut tail).expect("EOF читается"), 0);

    server.stop();
}

/// Сценарий «молчание»: соединение принято, ответ не приходит — чтение
/// клиента блокируется (не EOF), пока сервер жив.
#[test]
fn silence_scenario_holds_connection_without_data() {
    let mut server = MockServer::start(vec![Scenario::silence()]);

    let mut client = raw_get(&server.url());
    // Заголовки приходят, тело — никогда: чтение клиента блокируется
    // (не EOF), пока сервер жив.
    read_head(&mut client);
    client
        .set_read_timeout(Some(Duration::from_millis(300)))
        .expect("таймаут чтения клиента");
    let err = client
        .read(&mut [0u8; 8])
        .expect_err("данных нет — таймаут");
    assert_eq!(
        err.kind(),
        ErrorKind::WouldBlock,
        "чтение стоит, соединение живо"
    );

    server.stop();
}

/// Сценарий «мусор»: валидный префикс доходит, дальше бесконечный
/// детерминированный мусор; отключение клиента завершает обработчик
/// (сервер останавливается без зависания).
#[test]
fn garbage_scenario_streams_prefix_then_junk_forever() {
    let fixture = common::fixture_bytes();
    let prefix = fixture[..4096].to_vec();
    let mut server = MockServer::start(vec![Scenario::garbage_forever(prefix.clone())]);

    let mut client = raw_get(&server.url());
    read_head(&mut client);
    let head_of_body = read_exact_deadline(&mut client, 4096, Duration::from_secs(5));
    assert_eq!(head_of_body, prefix, "валидный префикс доходит до мусора");

    // Мусор течёт без конца: читаем ещё 64 КиБ — данных хватает.
    let junk = read_exact_deadline(&mut client, 64 * 1024, Duration::from_secs(5));
    assert_ne!(
        junk,
        fixture[..64 * 1024],
        "после префикса идёт не фикстура"
    );
    assert_eq!(server.connections(), 1, "один клиент, без переподключений");

    drop(client); // клиент ушёл — обработчик завершается по ошибке записи
    server.stop(); // Drop присоединяет воркеров: зависший тест = провал
}

/// Сценарий «не-MP3»: статус 200, HTML-тело — клиент тракта получает
/// байты (распознавание формата — задача декодера, не клиента).
#[test]
fn non_mp3_scenario_serves_html_body() {
    let mut server = MockServer::start(vec![Scenario::non_mp3()]);

    let mut reader = net::open_stream(&server.url()).expect("статус 200 — тело читается");
    let mut body = Vec::new();
    reader
        .read_to_end(&mut body)
        .expect("HTML-тело читается до EOF");
    assert!(
        body.starts_with(b"<html>"),
        "тело — HTML: {:?}",
        &body[..16.min(body.len())]
    );

    server.stop();
}

/// Сценарий «статус»: `open_stream` возвращает ошибку статуса —
/// сервер недоступен по смыслу ответа.
#[test]
fn status_scenario_maps_to_stream_error() {
    let mut server = MockServer::start(vec![Scenario::status(503)]);

    let result = net::open_stream(&server.url());
    assert!(
        matches!(result, Err(StreamError::Status(503))),
        "ожидалась ошибка статуса 503"
    );

    server.stop();
}

/// Сценарии выдаются по одному на соединение; исчерпаны — повторяется
/// последний. Счётчик соединений видит переподключения.
#[test]
fn scenarios_advance_per_connection_and_repeat_last() {
    let first = vec![1u8; 1024];
    let second = vec![2u8; 1024];
    let mut server = MockServer::start(vec![
        Scenario::stream(first.clone()),
        Scenario::stream(second.clone()),
    ]);

    let read_body = |url: &str| -> Vec<u8> {
        let mut client = raw_get(url);
        read_head(&mut client);
        let mut body = Vec::new();
        client.read_to_end(&mut body).expect("тело читается");
        body
    };

    assert_eq!(
        read_body(&server.url()),
        first,
        "первое соединение — первый сценарий"
    );
    assert_eq!(
        read_body(&server.url()),
        second,
        "второе соединение — второй сценарий"
    );
    assert_eq!(
        read_body(&server.url()),
        second,
        "исчерпаны — повторяется последний"
    );
    assert_eq!(server.connections(), 3, "переподключения считаются");

    server.stop();
}

/// Сценарий «чанками с паузами»: байты доходят все, выдача занимает
/// не меньше суммы пауз (медленный поток для тестов буферизации).
#[test]
fn paced_scenario_feeds_chunks_with_pauses() {
    let payload = vec![7u8; 4 * 1024];
    let started = Instant::now();
    let mut server = MockServer::start(vec![Scenario::paced_then_hold(
        payload.clone(),
        1024,
        Duration::from_millis(30),
    )]);

    let mut reader = net::open_stream(&server.url()).expect("медленный поток открывается");
    let body = read_exact_deadline(&mut reader, payload.len(), Duration::from_secs(10));
    assert_eq!(body, payload);
    assert!(
        started.elapsed() >= Duration::from_millis(90),
        "три паузы по 30 мс соблюдены: прошло {:?}",
        started.elapsed()
    );

    server.stop();
}

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

//! Коды выхода бинарника на мок-сервере (задача 4.3): поток не MP3 —
//! код 2 с диагностикой; недоступный сервер — бесконечные ретраи с логом
//! в stderr и выход только по сигналу с кодом 0. Запуск с `--headless`
//! (задача 4.1 add-player-ui): без флага бинарник открывает окно.

mod common;

use std::process::{Command, Stdio};
use std::time::Duration;

use common::{MockServer, Scenario};
use std::time::Instant;

/// Не-MP3 ответ (HTTP 200, большой HTML): процесс завершается кодом 2
/// с диагностикой «не распознан как MP3», без паники. Аудиоустройство
/// не нужно — ошибка старта происходит до его открытия.
#[test]
fn non_mp3_stream_exits_with_code_2() {
    let mut server = MockServer::start(vec![Scenario::non_mp3()]);

    let output = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--headless")
        .arg(server.url())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .expect("бинарник sevfm собран");

    assert_eq!(
        output.status.code(),
        Some(2),
        "не-MP3 поток — ненулевой код выхода"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("не распознан") || stderr.contains("мусорный"),
        "в stderr есть диагностика: {stderr}"
    );

    server.stop();
}

/// Бесконечный мусорный поток (HTTP 200, данные без конца и без единого
/// валидного кадра): фаза поиска первого кадра обязана завершиться
/// лимитом — процесс выходит кодом 2 с диагностикой, а не остаётся в
/// вечном цикле «probe ↔ переподключение» (спека устойчивости: без
/// прогресса — не бесконечно). Локальная сеть быстрая, узкое место —
/// backoff переподключений reader'а (по 1 с на соединение), поэтому
/// запас ожидания — 60 с.
#[test]
fn endless_garbage_stream_exits_with_code_2() {
    let mut server = MockServer::start(vec![Scenario::garbage_forever(Vec::new())]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--headless")
        .arg(server.url())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("бинарник sevfm собран");

    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        match child.try_wait().expect("процесс существует") {
            Some(status) => {
                assert_eq!(
                    status.code(),
                    Some(2),
                    "бесконечный мусор — код 2, не вечный поиск первого кадра"
                );
                break;
            }
            None if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(250));
            }
            None => {
                child.kill().expect("убираем застрявший процесс");
                panic!("процесс не завершился за 60 с — вечный поиск первого кадра вернулся");
            }
        }
    }
    let output = child.wait_with_output().expect("выход вычитан");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("не распознан") || stderr.contains("мусорный"),
        "в stderr есть диагностика: {stderr}"
    );

    server.stop();
}

/// Недоступный сервер: ретраи с задержкой и логом в stderr, процесс
/// не завершается сам; SIGINT — код 0.
#[cfg(unix)] // сигнал доставляется утилитой kill(1)
#[test]
fn unreachable_server_retries_forever_then_signal_exits_zero() {
    // Свободный порт без слушателя: соединение гарантированно refused.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind пробы");
    let dead_url = format!("http://{}/stream", probe.local_addr().expect("адрес пробы"));
    drop(probe);

    let mut child = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--headless")
        .arg(&dead_url)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("бинарник sevfm собран");

    // Первый backoff успел залогироваться (привязан к первой ступени
    // лестницы).
    std::thread::sleep(sevfm::net::FIRST_BACKOFF_DELAY + Duration::from_millis(500));
    assert!(
        child.try_wait().expect("процесс существует").is_none(),
        "сетевые ошибки первого соединения не завершают процесс"
    );

    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("утилита kill доступна");
    assert!(signalled.success(), "SIGINT доставлен");

    let output = child.wait_with_output().expect("процесс завершается");
    assert_eq!(output.status.code(), Some(0), "выход по сигналу — код 0");

    // Ретраи залогированы в stderr (wait_with_output уже вычитал пайп).
    // Рабочего соединения не было — это попытки подключения, не
    // переподключения.
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("подключение через 1s"),
        "лог ретраев в stderr: {stderr}"
    );
    assert!(
        !stderr.contains("переподключение"),
        "до рабочего соединения переподключений нет: {stderr}"
    );
}

/// Перманентный отказ сервера (HTTP 404): процесс завершается кодом 2
/// с диагностикой, без вечных ретраев бессмысленного запроса.
#[test]
fn permanent_status_exits_with_code_2() {
    let mut server = MockServer::start(vec![Scenario::status(404)]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--headless")
        .arg(server.url())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("бинарник sevfm собран");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match child.try_wait().expect("процесс существует") {
            Some(status) => {
                assert_eq!(status.code(), Some(2), "HTTP 404 — код 2, не вечные ретраи");
                break;
            }
            None if Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(100));
            }
            None => {
                child.kill().expect("убираем застрявший процесс");
                panic!("процесс не завершился за 10 с после HTTP 404");
            }
        }
    }
    let output = child.wait_with_output().expect("выход вычитан");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("отклонил запрос"),
        "в stderr есть диагностика: {stderr}"
    );

    server.stop();
}

/// Метаданные ICY доходят до пользователя (задача 3.1): станция и трек
/// печатаются в stderr при подключении и первой смене. Строки видны ещё
/// в стартовом цикле (до открытия аудиоустройства), поэтому тест честен и
/// на машине без звука; SIGINT останавливает процесс с кодом 0.
#[cfg(unix)] // сигнал доставляется утилитой kill(1)
#[test]
fn icy_metadata_printed_to_stderr() {
    use std::io::BufRead;

    let fixture = common::fixture_bytes();
    let metaint = 16 * 1024;
    // Первый блок — трек, дальше вставка: обе строки обязаны появиться.
    let framed = common::icy_frame(
        &fixture[..128 * 1024],
        metaint,
        &[
            "{\"artist\":\"ВЛАДИМИР ТРОШИН\",\"name\":\"ЮНОСТЬ МОЯ\"}",
            "Реклама",
        ],
    );
    // Медленная выдача держит соединение: процесс живёт до SIGINT.
    let mut server = MockServer::start(vec![Scenario::icy_paced_then_hold(
        framed,
        8 * 1024,
        Duration::from_millis(20),
        metaint,
        "Говорит Севастополь",
    )]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--headless")
        .arg(server.url())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("бинарник sevfm собран");
    let stderr = child.stderr.take().expect("stderr подключён");
    let reader = std::io::BufReader::new(stderr);

    let mut seen_station = false;
    let mut seen_track = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    for line in reader.lines() {
        let Ok(line) = line else { break };
        seen_station |= line.contains("станция: Говорит Севастополь");
        seen_track |= line.contains("сейчас в эфире: ВЛАДИМИР ТРОШИН — ЮНОСТЬ МОЯ");
        if (seen_station && seen_track) || Instant::now() > deadline {
            break;
        }
    }
    assert!(
        seen_station,
        "строка станции не появилась за 30 с (track: {seen_track})"
    );
    assert!(seen_track, "строка трека не появилась за 30 с");

    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("утилита kill доступна");
    assert!(signalled.success(), "SIGINT доставлен");
    let output = child.wait_with_output().expect("процесс завершается");
    assert_eq!(output.status.code(), Some(0), "выход по сигналу — код 0");

    server.stop();
}

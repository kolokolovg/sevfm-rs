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

/// `--clear-cache` (design D5 cover-cache-cleanup): файлы кеша обложек
/// удаляются, объём выводится в stdout, процесс завершается кодом 0, не
/// открывая окна и не начиная воспроизведение. `XDG_CACHE_HOME` указывает
/// на каталог теста — реальный кеш пользователя не трогается. Пустой кеш —
/// код 0 с нулевым объёмом; сочетание с URL — ошибка аргументов, кеш
/// не тронут (спека album-art «Ручная очистка кеша обложек»).
#[test]
fn clear_cache_removes_covers_and_exits_zero() {
    let base = std::env::temp_dir().join(format!("sevfm-clearcache-test-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let covers = base.join("sevfm").join("covers");
    std::fs::create_dir_all(&covers).expect("каталог кеша теста создаётся");
    std::fs::write(covers.join("aaa.jpg"), vec![0u8; 4096]).expect("запись обложки");
    std::fs::write(covers.join("bbb.jpg"), vec![0u8; 8192]).expect("запись обложки");
    std::fs::write(covers.join(".orphan.tmp"), vec![0u8; 10]).expect("осиротевший tmp");

    // Живой кеш: код 0, файлы удалены, каталог остался, объём в stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--clear-cache")
        .env("XDG_CACHE_HOME", &base)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("бинарник sevfm собран");
    assert_eq!(output.status.code(), Some(0), "очистка — код 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("Кеш обложек очищен") && stdout.contains("освобождено"),
        "результат в stdout: {stdout}"
    );
    assert!(
        stdout.contains("3 файла"),
        "счётчик файлов с формой множественного: {stdout}"
    );
    let left = std::fs::read_dir(&covers)
        .expect("каталог кеша читается")
        .count();
    assert_eq!(left, 0, "файлы кеша удалены");

    // Пустой кеш: код 0, нулевой объём.
    let output = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--clear-cache")
        .env("XDG_CACHE_HOME", &base)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("бинарник sevfm собран");
    assert_eq!(output.status.code(), Some(0), "пустой кеш — код 0");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("0 файлов") && stdout.contains("0 Б"),
        "нулевая очистка в stdout: {stdout}"
    );

    // Сочетание с URL — ошибка аргументов (код 2), кеш не тронут.
    std::fs::write(covers.join("ccc.jpg"), vec![0u8; 16]).expect("запись обложки");
    let output = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .args(["--clear-cache", "http://example.com/stream"])
        .env("XDG_CACHE_HOME", &base)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("бинарник sevfm собран");
    assert_eq!(output.status.code(), Some(2), "сочетание флагов — код 2");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("не сочетается"),
        "диагностика в stderr: {stderr}"
    );
    assert!(
        covers.join("ccc.jpg").is_file(),
        "при ошибке аргументов кеш не удалён"
    );

    let _ = std::fs::remove_dir_all(&base);
}

/// Частичная неудача `--clear-cache` (находка ревью): read-only каталог
/// кеша не даёт unlink — процесс выходит кодом 2 с предупреждением в
/// stderr, а не рапортует об успешной пустой очистке. Unix--only (chmod).
#[cfg(unix)]
#[test]
fn clear_cache_partial_failure_exits_with_code_2() {
    use std::os::unix::fs::PermissionsExt;

    let base = std::env::temp_dir().join(format!("sevfm-clearcache-ro-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let covers = base.join("sevfm").join("covers");
    std::fs::create_dir_all(&covers).expect("каталог кеша теста создаётся");
    std::fs::write(covers.join("aaa.jpg"), vec![0u8; 64]).expect("запись обложки");

    // unlink требует права записи на каталог: 0o555 — только чтение.
    std::fs::set_permissions(&covers, std::fs::Permissions::from_mode(0o555))
        .expect("каталог кеша переведён в read-only");
    let output = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg("--clear-cache")
        .env("XDG_CACHE_HOME", &base)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("бинарник sevfm собран");
    assert_eq!(
        output.status.code(),
        Some(2),
        "несостоявшаяся очистка — неуспех, а не пустой успех"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("частично"),
        "предупреждение в stderr: {stderr}"
    );
    assert!(
        covers.join("aaa.jpg").is_file(),
        "файл не удалён (права не давали)"
    );

    std::fs::set_permissions(&covers, std::fs::Permissions::from_mode(0o755))
        .expect("права восстановлены");
    let _ = std::fs::remove_dir_all(&base);
}

/// Битый URL запуска (design D5 harden-network-resilience): адрес без
/// схемы/хоста отклоняется при разборе аргументов — код 2 с
/// диагностикой в stderr до сети и окна, в обоих режимах; сегодня такой
/// адрес ретраился бы вечно как неклассифицированная сетевая ошибка.
#[test]
fn invalid_url_exits_with_code_2_in_both_modes() {
    for headless in [true, false] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_sevfm"));
        if headless {
            command.arg("--headless");
        }
        let output = command
            .arg("sevfm.ru:9006/govsev")
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .output()
            .expect("бинарник sevfm собран");

        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(
            output.status.code(),
            Some(2),
            "битый URL, headless={headless}: код 2, stderr: {stderr}"
        );
        assert!(
            stderr.contains("некорректный URL"),
            "headless={headless}, в stderr есть диагностика: {stderr}"
        );
    }
}

/// Метаданные ICY доходят до пользователя, синхронно со звуком
/// (design add-ether-sync): станция печатается сразу по соединению,
/// трек — когда декодер прочитал аудио до смещения метаблока (после
/// старта воспроизведения), поэтому строка трека всегда позже строки
/// станции. Тест требует рабочего аудиопути (как session-тесты);
/// SIGINT останавливает процесс с кодом 0.
#[cfg(unix)] // сигнал доставляется утилитой kill(1)
#[test]
fn icy_metadata_printed_to_stderr() {
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

    // Чтение stderr — в отдельном потоке: блокирующий lines() оставлял бы
    // дедлайн беспомощным против молчащего живого ребёнка (заклинивший
    // пайплайн — ни новых строк, ни EOF). Канал с recv_timeout(дедлайн)
    // закрывает и этот класс: тайм-аут убивает ребёнка и паникует с дампом
    // всего напечатанного. Строки собираются и на успехе: паники станции и
    // трека обязаны показать, что процесс печатал (ретраи, состояние
    // пайплайна) — иначе диагностика глухая (флейк CI GitVerse 2026-11-03
    // разбирали именно по такому логу).
    let (line_tx, line_rx) = std::sync::mpsc::channel::<String>();
    let reader = std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stderr).lines() {
            match line {
                Ok(l) => {
                    if line_tx.send(l).is_err() {
                        break;
                    }
                }
                // EOF или разрыв пайпа — поток завершается, канал
                // опустошается и отключается.
                Err(_) => break,
            }
        }
    });

    // Позиции первых вхождений: станция — сразу, трек — по звуку.
    let mut lines_seen: Vec<String> = Vec::new();
    let mut station_at: Option<usize> = None;
    let mut track_at: Option<usize> = None;
    // Запас 60 с — не для сети (мок локален), а для перегруженного
    // шаренного раннера: трек появляется после ~1 с звучания через ALSA
    // null, и на голодном по CPU раннере путь до метаблока растягивается.
    // На зелёном прогоне стоимость нулевая — выход по успеху.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let line = match line_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(l) => l,
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                let _ = child.kill();
                panic!(
                    "строки не приходят 60 с (процесс убит), stderr:\n{}",
                    lines_seen.join("\n")
                );
            }
            // Канал опустел и закрылся: процесс умер, EOF.
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
        };
        let index = lines_seen.len();
        if station_at.is_none() && line.contains("станция: Говорит Севастополь")
        {
            station_at = Some(index);
        }
        if track_at.is_none() && line.contains("сейчас в эфире: ВЛАДИМИР ТРОШИН — ЮНОСТЬ МОЯ")
        {
            track_at = Some(index);
        }
        lines_seen.push(line);
        if station_at.is_some() && track_at.is_some() {
            break;
        }
    }
    let dump = || lines_seen.join("\n");
    let (station_at, track_at) = (
        station_at
            .unwrap_or_else(|| panic!("строка станции не появилась за 60 с, stderr:\n{}", dump())),
        track_at
            .unwrap_or_else(|| panic!("строка трека не появилась за 60 с, stderr:\n{}", dump())),
    );
    assert!(
        station_at < track_at,
        "станция печатается до трека: сразу по соединению, трек — по звуку \
         (станция: {station_at}, трек: {track_at})"
    );

    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("утилита kill доступна");
    assert!(signalled.success(), "SIGINT доставлен");
    let output = child.wait_with_output().expect("процесс завершается");
    assert_eq!(output.status.code(), Some(0), "выход по сигналу — код 0");
    // Ребёнок вышел — stderr в EOF, поток-читатель завершается сам;
    // join подтверждает, что ничего не осталось за кадром.
    reader
        .join()
        .expect("поток чтения stderr завершается без паники");

    server.stop();
}

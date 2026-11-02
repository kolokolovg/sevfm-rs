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

//! Сессии воспроизведения на мок-сервере (задача 1.2): старт → воркер
//! жив → «Стоп» → воркер завершился со `Stopped` за секунду; повторный
//! старт в том же процессе работает. Молчаливый сервер держит сессию в
//! фазе предбуфера — аудиоустройство не нужно, тест честен на машине
//! без звука.

mod common;

use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use common::{MockServer, Scenario};
use sevfm::session::{PlayerSession, SessionHandle, SessionOutcome};

/// Норма спеки на остановку звука и воркера.
const STOP_DEADLINE: Duration = Duration::from_secs(1);

/// Ждёт исход сессии не дольше `STOP_DEADLINE` (poll слота: join без
/// таймаута завис бы вместе с воркером при регрессии).
fn wait_outcome(session: &SessionHandle) -> SessionOutcome {
    let started = Instant::now();
    loop {
        if let Some(outcome) = session.outcome() {
            return outcome;
        }
        assert!(
            started.elapsed() < STOP_DEADLINE,
            "воркер не завершился за 1 с после остановки"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Стоп живой сессии завершает воркер исходом `Stopped` за секунду;
/// повторный старт в том же процессе поднимает новую живую сессию.
#[test]
fn stop_ends_worker_as_stopped_and_restart_works() {
    let mut server = MockServer::start(vec![Scenario::silence()]);
    let process_stop = Arc::new(AtomicBool::new(false));

    let mut first =
        PlayerSession::start(server.url(), process_stop.clone()).expect("старт первой сессии");
    // Сессия жива: ждёт предбуфер от молчуна.
    assert!(first.is_running(), "воркер жив до остановки");
    first.stop();
    assert_eq!(
        wait_outcome(&first),
        SessionOutcome::Stopped,
        "остановка живой сессии — Stopped"
    );
    first.join();

    // Повторный старт в том же процессе: новая сессия жива и останавливается.
    let mut second =
        PlayerSession::start(server.url(), process_stop).expect("повторный старт сессии");
    assert!(second.is_running(), "воркер повторной сессии жив");
    second.stop();
    assert_eq!(
        wait_outcome(&second),
        SessionOutcome::Stopped,
        "остановка повторной сессии — Stopped"
    );
    second.join();

    server.stop();
}

/// Фатальный путь сессии end-to-end (ревью add-player-ui): перманентный
/// отказ сервера (HTTP 404) → reader помечает `link.fatal` → воркер
/// классифицирует `Fatal`; `join` идемпотентен — повторный вызов
/// возвращает опубликованный исход, а не аварийный запасной.
#[test]
fn permanent_refusal_ends_session_as_fatal_and_join_is_idempotent() {
    let mut server = MockServer::start(vec![Scenario::status(404)]);
    let process_stop = Arc::new(AtomicBool::new(false));

    let mut session = PlayerSession::start(server.url(), process_stop)
        .expect("старт сессии на отказывающем сервере");
    let fatal = SessionOutcome::Fatal("подключение отклонено сервером".into());
    assert_eq!(
        wait_outcome(&session),
        fatal,
        "HTTP 404 — Fatal с сообщением"
    );
    assert_eq!(session.join(), fatal, "первый join — опубликованный исход");
    assert_eq!(session.join(), fatal, "повторный join — тот же исход");

    server.stop();
}

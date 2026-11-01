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

//! Остановка по сигналу на молчащем сервере без сети и аудиоустройства
//! (задача 4.2): процесс ждёт данные, SIGINT завершает его кодом 0
//! за секунду — сторож выходит напрямую, без drop/join аудио-потоков.

mod common;

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use common::{MockServer, Scenario};

#[cfg(unix)] // сигнал доставляется утилитой kill(1)
#[test]
fn sigint_on_silent_server_exits_zero_within_second() {
    let mut server = MockServer::start(vec![Scenario::silence()]);

    let mut child = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg(server.url())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("бинарник sevfm собран");

    // Процесс подключён к молчуну и ждёт предбуфер.
    let connected = Instant::now() + Duration::from_secs(5);
    while server.connections() < 1 {
        assert!(
            Instant::now() < connected,
            "процесс не подключился к молчуну за 5 с"
        );
        std::thread::sleep(Duration::from_millis(10));
    }

    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("утилита kill доступна");
    assert!(signalled.success(), "SIGINT доставлен процессу");

    let started = Instant::now();
    let deadline = started + Duration::from_secs(1);
    loop {
        match child.try_wait().expect("процесс ещё существует") {
            Some(exit) => {
                assert_eq!(
                    exit.code(),
                    Some(0),
                    "сигнал на молчащем сервере — код 0 за секунду"
                );
                assert!(
                    started.elapsed() <= Duration::from_secs(1),
                    "выход занял {:?}",
                    started.elapsed()
                );
                break;
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "процесс не завершился в течение 1 с после SIGINT на молчуне"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }

    server.stop();
}

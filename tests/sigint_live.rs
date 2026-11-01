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

//! Живой тест остановки по сигналу (задача 4.2): запуск бинарника
//! на живом потоке, SIGINT — процесс завершается кодом 0 не дольше
//! чем за 1 с (требование спеки об остановке).
//!
//! Запуск: `cargo test --test sigint_live -- --ignored`.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use sevfm::pipeline::DEFAULT_STREAM_URL;

#[cfg(unix)] // сигнал доставляется утилитой kill(1)
#[test]
#[ignore = "живой тест: требует сеть и аудиоустройство"]
fn sigint_stops_playback_with_zero_exit_code() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_sevfm"))
        .arg(DEFAULT_STREAM_URL)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("бинарник sevfm собран");

    std::thread::sleep(Duration::from_secs(5));

    // SIGINT — как Ctrl+C в терминале.
    let signalled = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status()
        .expect("утилита kill доступна");
    assert!(signalled.success(), "SIGINT доставлен процессу");

    // Процесс завершается кодом 0 не дольше чем за 1 с после сигнала
    // (требование спеки; drop/join аудио-потока не выполняется — exit(0)).
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match child.try_wait().expect("процесс ещё существует") {
            Some(exit) => {
                assert_eq!(
                    exit.code(),
                    Some(0),
                    "после SIGINT процесс выходит с кодом 0"
                );
                return;
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "процесс не завершился в течение 1 с после SIGINT"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
}

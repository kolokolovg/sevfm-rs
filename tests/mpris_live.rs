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

//! Живой интеграционный тест MPRIS (задача 4.4 add-tray): бинарник
//! UI-режима поднимает сервис `org.mpris.MediaPlayer2.sevfm` на шине
//! сессии; тест читает Identity/Can*/PlaybackStatus/Metadata, дёргает
//! Play/PlayPause/Stop/Raise/Quit и проверяет код выхода 0.
//!
//! Запуск: `cargo test --test mpris_live -- --ignored`.
//! Требует шину D-Bus сессии, графическую сессию (окно), утилиты
//! busctl/gdbus и живой поток (Play выходит в сеть).

use std::net::TcpStream;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const DEST: &str = "org.mpris.MediaPlayer2.sevfm";
/// Предельное ожидание сетевого подключения после Play (спека
/// radio-player: попытки с задержкой, не мгновенно).
const PLAY_DEADLINE: Duration = Duration::from_secs(20);

/// Окружение для живого MPRIS-теста; None — пропустить тест.
fn live_env() -> Option<()> {
    std::env::var_os("DBUS_SESSION_BUS_ADDRESS")?;
    std::env::var_os("WAYLAND_DISPLAY").or_else(|| std::env::var_os("DISPLAY"))?;
    // Утилита D-Bus для запросов и вызовов.
    which("busctl")?;
    // Сеть доступна? Play выходит на живой поток.
    TcpStream::connect("sevfm.ru:443").ok()?;
    // Чужой экземпляр плеера займёт имя сервиса — тест недостоверен.
    if name_taken() {
        return None;
    }
    Some(())
}

/// Поиск утилиты в PATH.
fn which(bin: &str) -> Option<()> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|dir| dir.join(bin))
        .find(|file| file.is_file())
        .map(|_| ())
}

/// Занято ли имя сервиса другим процессом.
fn name_taken() -> bool {
    Command::new("busctl")
        .args(["--user", "list"])
        .output()
        .is_ok_and(|out| String::from_utf8_lossy(&out.stdout).contains(DEST))
}

/// Значение свойства сервиса как текст: строки — декодированными из
/// октальных эскейпов busctl, bool — как `true`/`false`. None — свойство
/// недоступно.
fn property(interface: &str, name: &str) -> Option<String> {
    let out = Command::new("busctl")
        .args([
            "--user",
            "get-property",
            DEST,
            "/org/mpris/MediaPlayer2",
            interface,
            name,
        ])
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    // Формат busctl: `<сигнатура> <значение>` — у строки значение в
    // кавычках с октальными эскейпами, у bool — голое true/false.
    let (_, value) = text.trim().split_once(' ')?;
    if let Some(quoted) = value.strip_prefix('"') {
        decode_busctl_string(quoted.strip_suffix('"')?)
    } else {
        Some(value.to_owned())
    }
}

/// Строка из октальных эскейпов busctl: `"\320\223..."` — байты UTF-8.
fn decode_busctl_string(escaped: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(escaped.len());
    let mut rest = escaped;
    while !rest.is_empty() {
        if rest.starts_with('\\') && rest.len() >= 4 {
            bytes.push(u8::from_str_radix(&rest[1..4], 8).ok()?);
            rest = &rest[4..];
        } else {
            let mut chars = rest.chars();
            let c = chars.next()?;
            bytes.extend_from_slice(c.to_string().as_bytes());
            rest = chars.as_str();
        }
    }
    String::from_utf8(bytes).ok()
}

/// Вызов метода MPRIS без аргументов.
fn call(interface: &str, method: &str) {
    let ok = Command::new("busctl")
        .args([
            "--user",
            "call",
            DEST,
            "/org/mpris/MediaPlayer2",
            interface,
            method,
        ])
        .status()
        .is_ok_and(|status| status.success());
    assert!(ok, "метод {interface}.{method} выполнен");
}

/// Ожидание значения PlaybackStatus.
fn wait_status(want: &str) {
    let deadline = Instant::now() + PLAY_DEADLINE;
    loop {
        if property("org.mpris.MediaPlayer2.Player", "PlaybackStatus").as_deref() == Some(want) {
            return;
        }
        assert!(Instant::now() < deadline, "PlaybackStatus не стал {want}");
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// Ждать появления имени на шине после старта приложения.
fn wait_name() {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !name_taken() {
        if Instant::now() > deadline {
            panic!("имя {DEST} не появилось на шине");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Плеер под тестом: паника в любом ассерте не должна оставлять процесс
/// висеть с занятым именем на шине (утёкшие экземпляры — находка
/// отладки живого теста).
struct PlayerUnderTest(Child);

impl Drop for PlayerUnderTest {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[cfg(target_os = "linux")]
#[test]
#[ignore = "живой тест: требует шину D-Bus, графическую сессию и сеть"]
fn mpris_service_reflects_player_and_takes_commands() {
    let Some(_) = live_env() else {
        eprintln!("пропуск: нет шины/графики/утилит/сети или имя занято");
        return;
    };

    let mut player = PlayerUnderTest(
        Command::new(env!("CARGO_BIN_EXE_sevfm"))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("бинарник sevfm собран"),
    );
    wait_name();

    // Идентичность и возможности (спека mpris; находка ревью hunter:
    // полный набор Can*, иначе регрессия проходит зелёной).
    assert_eq!(
        property("org.mpris.MediaPlayer2", "Identity").as_deref(),
        Some("Говорит Севастополь")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2", "CanRaise").as_deref(),
        Some("true")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2", "CanQuit").as_deref(),
        Some("true")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "CanControl").as_deref(),
        Some("true")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "CanPause").as_deref(),
        Some("true")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "CanSeek").as_deref(),
        Some("false")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "CanGoNext").as_deref(),
        Some("false")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "CanGoPrevious").as_deref(),
        Some("false")
    );
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "CanPlay").as_deref(),
        Some("true")
    );
    // Стартовое состояние до команд (спека mpris).
    assert_eq!(
        property("org.mpris.MediaPlayer2.Player", "PlaybackStatus").as_deref(),
        Some("Stopped")
    );

    // Play/PlayPause/Stop (спека mpris: команды управления).
    call("org.mpris.MediaPlayer2.Player", "Play");
    wait_status("Playing");
    let metadata = || {
        Command::new("busctl")
            .args([
                "--user",
                "get-property",
                DEST,
                "/org/mpris/MediaPlayer2",
                "org.mpris.MediaPlayer2.Player",
                "Metadata",
            ])
            .output()
            .map(|out| String::from_utf8_lossy(&out.stdout).into_owned())
            .unwrap_or_default()
    };
    let deadline = Instant::now() + PLAY_DEADLINE;
    while !metadata().contains("xesam:title") {
        assert!(Instant::now() < deadline, "xesam:title не появился");
        std::thread::sleep(Duration::from_millis(200));
    }
    let metadata = metadata();
    assert!(
        metadata.contains("xesam:url"),
        "xesam:url — адрес потока: {metadata}"
    );

    // Pause — метод, который шлёт кнопка «пауза» виджета KDE.
    call("org.mpris.MediaPlayer2.Player", "Pause");
    wait_status("Stopped");
    call("org.mpris.MediaPlayer2.Player", "Play");
    wait_status("Playing");
    call("org.mpris.MediaPlayer2.Player", "PlayPause");
    wait_status("Stopped");
    call("org.mpris.MediaPlayer2.Player", "PlayPause");
    wait_status("Playing");
    call("org.mpris.MediaPlayer2.Player", "Stop");
    wait_status("Stopped");

    // Raise не падает; Quit — полный выход с кодом 0 (спека mpris).
    call("org.mpris.MediaPlayer2", "Raise");
    call("org.mpris.MediaPlayer2", "Quit");
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        match player.0.try_wait().expect("процесс существует") {
            Some(exit) => {
                assert_eq!(exit.code(), Some(0), "Quit — код 0");
                break;
            }
            None => {
                assert!(
                    Instant::now() < deadline,
                    "процесс не завершился в течение 1 с после Quit"
                );
                std::thread::sleep(Duration::from_millis(20));
            }
        }
    }
    // Имя сервиса исчезает с шины (спека mpris; находка ревью hunter):
    // снятие держится на очистке демона при закрытии сокета — даём
    // момент и проверяем.
    let deadline = Instant::now() + Duration::from_secs(2);
    while name_taken() {
        assert!(
            Instant::now() < deadline,
            "имя {DEST} не исчезло с шины после Quit"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

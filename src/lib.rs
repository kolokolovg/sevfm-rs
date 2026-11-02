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

//! Переносимая логика sevfm. `main.rs` — только CLI-обвязка (аргументы,
//! сигналы, коды выхода); интеграционные тесты из `tests/` импортируют
//! модули отсюда — модули bin-крейта снаружи не видны.

pub mod buffer;
pub mod cover;
pub mod icy;
pub mod net;
pub mod pipeline;
pub mod session;
pub mod ui;

/// Пользовательское сообщение в stderr: по-русски, с префиксом `sevfm: `
/// (конвенция проекта). Весь вывод для пользователя идёт через него —
/// префикс не может потеряться в новой точке вывода, а недоверенные
/// фрагменты (URL, строки сетевых ошибок) не несут управляющих байтов.
///
/// Ошибка записи проглатывается: `eprintln!` паникует на закрытом пайпе
/// (`sevfm 2>&1 | head -1`), а диагностика не должна ронять сторожа
/// сигнала, audio-поток или reader — классификация исхода важнее лога.
pub fn say(message: &str) {
    use std::io::Write;
    // Запись вручную с проглатыванием ошибки: диагностика не должна
    // ронять поток (закрытый пайп) — классификация исхода важнее лога.
    // Блокировка на переполненном пайпе остаётся — stderr иначе не пишется.
    let mut stderr = std::io::stderr().lock();
    let _ = writeln!(stderr, "sevfm: {}", sanitize_for_terminal(message));
}

/// Управляющие байты (кроме табуляции) и невидимые bidi/формат-символы
/// Unicode заменяются на U+FFFD: ESC-последовательности из скопированного
/// URL не перерисовывают терминал, CRLF не подделывает строки лога, а
/// bidi-символы (U+202A–202E, U+2066–2069, U+200B–200F, U+FEFF — это Cf,
/// не Cc, `is_control()` их не ловит) не спуфят отображение названия трека.
fn sanitize_for_terminal(text: &str) -> std::borrow::Cow<'_, str> {
    let spoofing = |c: char| matches!(c, '\u{202A}'..='\u{202E}' | '\u{2066}'..='\u{2069}' | '\u{200B}'..='\u{200F}' | '\u{FEFF}');
    if text
        .chars()
        .all(|c| c == '\t' || (!c.is_control() && !spoofing(c)))
    {
        return std::borrow::Cow::Borrowed(text);
    }
    std::borrow::Cow::Owned(
        text.chars()
            .map(|c| {
                if c == '\t' || (!c.is_control() && !spoofing(c)) {
                    c
                } else {
                    '\u{FFFD}'
                }
            })
            .collect(),
    )
}

/// URL для вывода в терминал: userinfo (`scheme://user:pass@…`) вырезается —
/// учётные данные из командной строки не попадают в лог. Строка без схемы
/// (`user:pass@host/path`) тоже редактируется: именно так ureq печатает
/// URL без схемы в сообщениях об ошибках.
pub fn redact_url(url: &str) -> std::borrow::Cow<'_, str> {
    // Часть после схемы (или вся строка, если схемы нет).
    let after_scheme = match url.find("://") {
        Some(i) => &url[i + 3..],
        None => url,
    };
    // Userinfo — только до первого `/` (собачка в пути — не credential).
    // У валидного URL userinfo один; в malformed-строках последняя `@`
    // в authority отделяет хост надёжнее первой.
    let authority_end = after_scheme.find('/').unwrap_or(after_scheme.len());
    match after_scheme[..authority_end].rfind('@') {
        Some(at) => {
            let scheme_len = url.len() - after_scheme.len();
            let mut redacted = String::with_capacity(url.len());
            redacted.push_str(&url[..scheme_len]);
            redacted.push_str(&after_scheme[at + 1..]);
            std::borrow::Cow::Owned(redacted)
        }
        None => std::borrow::Cow::Borrowed(url),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Управляющие байты заменяются, табуляция и обычный текст — нет.
    #[test]
    fn sanitize_replaces_control_bytes_but_keeps_tabs() {
        assert_eq!(sanitize_for_terminal("чисто"), "чисто");
        assert_eq!(sanitize_for_terminal("та\tб"), "та\tб");
        // ESC-эскейп и CRLF из недоверенного URL — на замену.
        assert_eq!(
            sanitize_for_terminal("\x1b]2;подделка\x07\r\nстрока"),
            "\u{FFFD}]2;подделка\u{FFFD}\u{FFFD}\u{FFFD}строка"
        );
        // Bidi/формат-символы Unicode (Cf, не Cc) — тоже на замену.
        assert_eq!(
            sanitize_for_terminal("\u{202E}подделка\u{2066}"),
            "\u{FFFD}подделка\u{FFFD}"
        );
    }

    /// Userinfo вырезается; `@` в пути и URL без userinfo — без изменений.
    #[test]
    fn redact_url_strips_userinfo_only_in_authority() {
        assert_eq!(
            redact_url("http://user:pass@example.com:8080/stream"),
            "http://example.com:8080/stream"
        );
        assert_eq!(
            redact_url("https://example.com/stream"),
            "https://example.com/stream"
        );
        // Собачка после первого `/` — часть пути, не userinfo.
        assert_eq!(
            redact_url("http://example.com/@mention"),
            "http://example.com/@mention"
        );
        assert_eq!(redact_url("example.com/stream"), "example.com/stream");
        // URL без схемы (так ureq печатает его в ошибках) и пароль
        // с `@` внутри — последняя собачка в authority.
        assert_eq!(redact_url("user:pass@host/stream"), "host/stream");
        assert_eq!(
            redact_url("http://u:p@ss@host/stream"),
            "http://host/stream"
        );
    }
}

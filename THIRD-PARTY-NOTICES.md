# Сведения о сторонних компонентах

Собственный код проекта распространяется по лицензии
[Apache License 2.0](LICENSE). Проект использует сторонние библиотеки
на следующих условиях:

| Компонент | Назначение | Лицензия |
|---|---|---|
| [ureq](https://crates.io/crates/ureq) | HTTP/TLS-клиент: подключение к стриму радиостанции, стриминговое чтение тела ответа | MIT OR Apache-2.0 |
| [symphonia](https://crates.io/crates/symphonia) (+`symphonia-bundle-mp3`) | Покадровый MP3-декодер: probe → формат-ридер → decode | MPL-2.0 |
| [rodio](https://crates.io/crates/rodio) | Аудиовыход: `OutputStream`/`Sink`, конвертация формата в устройство | MIT OR Apache-2.0 |
| [cpal](https://crates.io/crates/cpal) (транзитивно через rodio) | Кроссплатформенный доступ к аудиоустройству (ALSA и др.) | Apache-2.0 |
| [thiserror](https://crates.io/crates/thiserror) | Derive-макросы `Display`/`Error` для enum'ов ошибок (только compile-time) | MIT OR Apache-2.0 |
| [ctrlc](https://crates.io/crates/ctrlc) 3.5.2 | Обработчик SIGINT/SIGTERM: атомарный флаг остановки | MIT |

Перечисленные лицензии допускают использование библиотек в составе
распространяемого приложения; исходные тексты самих библиотек
в поставку проекта не входят и по своим лицензиям не распространяются.

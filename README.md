# sevfm (sevfm-rs)

Настольный плеер онлайн-радио **«Говорит Севастополь»** — порт
[Java-версии](../../java/sevfm) на Rust + Slint.

Цель порта: нативный бинарник с малым потреблением ОЗУ и процессора
(JavaFX-версия поставляется с jlink-рантаймом ~87 МБ и потребляет
~150–250 МБ RSS; целевой ориентир порта — десятки МБ).

## Стек

- **Rust** (edition 2024)
- **Slint** — UI (render-on-demand) + свёртывание в трей через `tray-icon`
- Аудио-тракт и стриминг — по фазам реализации (см. `openspec/`)

## Статус

Фаза 0 — скелет проекта. Спецификация поведения перенесена из Java-версии
без изменений: [`openspec/specs/radio-player/spec.md`](openspec/specs/radio-player/spec.md).

## Сборка

```bash
cargo build --release
```

## Лицензия

Apache License 2.0 — см. [LICENSE](LICENSE) и [NOTICE](NOTICE).
Сторонние компоненты: [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md).

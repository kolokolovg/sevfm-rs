# Tasks

## 1. Сборка

- [x] 1.1 Cargo.toml: slint на явный набор фич (default-features = false;
      std, backend-winit, renderer-software, accessibility, compat-1-2,
      system-tray) с комментарием-обоснованием (замеры design.md)
- [x] 1.2 Cargo.lock обновлён; `cargo tree -i femtovg` подтверждает
      выпадение femtovg-ветки из дерева

## 2. Живая проверка (по design D3; критерий пересмотрен — см. design)

- [x] 2.1 Локальные гейты зелёные: `cargo fmt --all --check`,
      `cargo clippy --all-targets -- -D warnings`, `cargo test` (100 passed)
- [x] 2.2 Живой запуск окна: PSS 32 МБ покой / 46 МБ игра (критерий ≤ 50 МБ
      по исправленному design); libLLVM/gallium в /proc/…/maps — 0 вхождений
- [x] 2.3 Вид окна глазами пользователя: обложка/заглушка, статусы,
      индикатор-точка, кнопки — соответствуют прежним (подтверждено:
      «как прежде, всё на месте», окно в воспроизведении)

## 3. Закрытие

- [x] 3.1 Ревью субагентом: P2 (устаревшая цифра «3.0 МБ» в комментарии
      Cargo.toml) и P3 (комментарии ci.yml/deb про femtovg/GL) устранены;
      потери фич и рисков для Windows/релиза не найдено
- [x] 3.2 Версия 0.1.2, CHANGELOG entry, коммит; релиз — по решению
      пользователя

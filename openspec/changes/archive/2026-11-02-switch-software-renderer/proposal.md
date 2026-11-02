# Proposal

## Why

Установленный из deb плеер занимает ~93 МБ PSS (135 МБ RSS) — против ориентира
проекта «десятки МБ». Живой замер (`/proc/<pid>/smaps_rollup`, Wayland/KDE,
Intel Raptor Lake) показал: ~50 МБ файловых страниц — это софт-стек Mesa
(libLLVM ~41 МБ + libgallium ~9 МБ), который тянет femtovg-рендерер, не
поднимаясь на аппаратный EGL; остальное — связанная с GL анонимная память.
Software-рендерер в той же сессии: PSS ~32 МБ в покое / ~46 МБ при игре —
экономия ~60 МБ и полный уход GL-стека. Окно плеера статично
(render-on-demand), GPU-конвейер ему не нужен.

(Ранний замер «software = 3 МБ» был артефактом измерения — PID процесса
`timeout` вместо sevfm — и здесь исправлен; см. design.md.)

## What Changes

- Сборка переключается с дефолтного набора фич slint на явный:
  `default-features = false` + `std, backend-winit, renderer-software,
  accessibility, compat-1-2, system-tray`. Femtovg и GL-стек выпадают из
  дерева зависимостей (бинарник худеет), software-рендерер становится
  единственным и не требующим SLINT_BACKEND.
- Видимое поведение окна не меняется: та же разметка
  (`ui/player-window.slint`), те же статусы/обложка/анимации индикатора —
  software-рендерер отрисовывает ту же сцену.
- Вне области изменения: аудио-тракт, ICY, трей/MPRIS, упаковка.

## Capabilities

### New Capabilities

_(нет — видимое поведение и требования спек не меняются; смена реализации
рендера, `skip_specs: true` в `.openspec.yaml`)_

### Modified Capabilities

_(нет)_

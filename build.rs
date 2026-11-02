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

//! Компиляция разметки окна (design D4 add-player-ui): сгенерированный
//! компонент доступен в main.rs через `slint::include_modules!()`.

fn main() {
    slint_build::compile("ui/player-window.slint").expect("разметка окна компилируется");

    // Иконка exe-файла (фаза add-ci-release): Windows-ресурс из packaging/
    // sevfm.ico — проводник и таскбар показывают герб, а не дефолтную иконку
    // rust-бинарника. На Linux ветка не компилируется: иконки окна и трея
    // уже вшиты в бинарник Slint-компилятором из ui/app-icon.png, а
    // «иконку пакета» видит файловая система — её кладёт cargo-deb (desktop +
    // hicolor, Cargo.toml [package.metadata.deb]).
    #[cfg(target_os = "windows")]
    {
        let mut resource = winresource::WindowsResource::new();
        resource.set_icon("packaging/sevfm.ico");
        resource.compile().expect("иконка exe вшивается ресурсом");
    }
}

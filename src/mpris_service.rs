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

//! MPRIS-сервис (design D4 add-tray): готовый `mpris_server::Player`
//! в выделенном `std::thread` с `zbus::block_on` — рантайм zbus
//! обслуживает соединение сам, tokio не нужен. `Player` построен на
//! `Rc`: весь живёт в этом потоке, наружу — только канал обновлений и
//! колбэки команд в event loop Slint. Повторная запись тех же
//! PlaybackStatus/Metadata дедуплицируется самим `Player` — собственный
//! diff-кеш не нужен (уточнение design D4).

use std::future::Future;
use std::pin::{Pin, pin};
use std::sync::Arc;
use std::task::{Context, Poll};

use std::sync::mpsc::RecvTimeoutError;
use std::time::Duration;

use mpris_server::{Metadata, PlaybackStatus, Player as MprisPlayer, TrackId, zbus};
use sevfm::mpris::{MprisPlayback, MprisTrack};
use sevfm::say;
use sevfm::tray::APP_NAME;

/// Ожидание готовности сервиса при старте (находка ревью oracle):
/// зависший handshake шины не должен блокировать запуск UI дольше
/// этого срока.
const READY_TIMEOUT: Duration = Duration::from_secs(5);
/// Ёмкость канала обновлений (находка аудита): bounded — после смерти
/// потока очередь не растёт от poll-такта; состояний больше двух не
/// бывает (статус + метаданные), 8 слотов хватает с запасом.
const UPDATES_CAPACITY: usize = 8;

/// Обновление из poll-такта: статус, трек-строка, обложка и адрес
/// потока. Отправляется без дедупликации — фильтрует `Player`.
pub struct MprisUpdate {
    pub playback: MprisPlayback,
    pub track: MprisTrack,
    /// `mpris:artUrl` — file:// на файл кеша обложки, None без обложки.
    pub art_url: Option<String>,
    /// `xesam:url` — адрес потока.
    pub stream_url: String,
}

/// Команды MPRIS в event loop приложения (те же хендлеры, что у окна
/// и трея).
pub enum MprisCommand {
    Play,
    Stop,
    PlayPause,
    Raise,
    Quit,
}

/// Хендл MPRIS-потока: `Send` — хранится в `run_ui`, push идёт из
/// poll-такта.
pub struct Mpris {
    updates: async_channel::Sender<MprisUpdate>,
    /// Отказ канала уже напечатан (Closed — навсегда, повторять каждый
    /// такт не нужно; находка ревью hunter).
    closed_reported: std::sync::atomic::AtomicBool,
}

impl Mpris {
    /// Поднимает сервис `org.mpris.MediaPlayer2.sevfm` в выделенном
    /// потоке. Отказ (нет шины) — `None` и строка в лог: приложение
    /// продолжает работать без MPRIS (спека mpris).
    pub fn start(commands: Arc<dyn Fn(MprisCommand) + Send + Sync>) -> Option<Self> {
        let (updates_tx, updates_rx) = async_channel::bounded::<MprisUpdate>(UPDATES_CAPACITY);
        let (ready_tx, ready_rx) = std::sync::mpsc::channel::<()>();
        let spawn = std::thread::Builder::new()
            .name("mpris".into())
            .spawn(move || {
                zbus::block_on(async move {
                    let player = match MprisPlayer::builder("sevfm")
                        .identity(APP_NAME)
                        .can_quit(true)
                        .can_raise(true)
                        .can_play(true)
                        // Пауза живого эфира — это остановка (у виджета
                        // KDE кнопки «стоп» нет вообще: плей-пауза
                        // единственная точка остановки, находка
                        // ручного тестирования).
                        .can_pause(true)
                        .can_seek(false)
                        .can_go_next(false)
                        .can_go_previous(false)
                        .playback_status(PlaybackStatus::Stopped)
                        .build()
                        .await
                    {
                        Ok(player) => player,
                        Err(e) => {
                            say(&format!("mpris: сервис недоступен: {e}"));
                            return;
                        }
                    };
                    // Колбэки — команды в event loop; живой поток не
                    // паузится: Pause и PlayPause останавливают эфир
                    // (кнопка «паузы» виджета KDE шлёт именно Pause —
                    // находка ручного тестирования), PlayPause из
                    // «Остановлено» решает главное окно по факту
                    // состояния сессии.
                    {
                        let commands = commands.clone();
                        player.connect_play(move |_| commands(MprisCommand::Play));
                    }
                    {
                        let commands = commands.clone();
                        player.connect_stop(move |_| commands(MprisCommand::Stop));
                    }
                    {
                        let commands = commands.clone();
                        player.connect_pause(move |_| commands(MprisCommand::Stop));
                    }
                    {
                        let commands = commands.clone();
                        player.connect_play_pause(move |_| commands(MprisCommand::PlayPause));
                    }
                    {
                        let commands = commands.clone();
                        player.connect_raise(move |_| commands(MprisCommand::Raise));
                    }
                    player.connect_quit(move |_| commands(MprisCommand::Quit));

                    let _ = ready_tx.send(());
                    // Гонка обработки вызовов (run) и обновлений (канал):
                    // селект из двух futures без зависимости futures.
                    let mut run = pin!(player.run());
                    loop {
                        let mut updates = pin!(updates_rx.recv());
                        match Race::new(run.as_mut(), updates.as_mut()).await {
                            // run завершён — соединение с шиной закрыто;
                            // молчаливая смерть сервиса нарушала бы спеку
                            // («сервис существует, пока работает
                            // приложение») без диагностики (находка
                            // ревью hunter).
                            Either::Left(()) => {
                                say("mpris: соединение закрыто, сервис остановлен");
                                return;
                            }
                            Either::Right(Ok(update)) => {
                                let status = match update.playback {
                                    MprisPlayback::Playing => PlaybackStatus::Playing,
                                    MprisPlayback::Stopped => PlaybackStatus::Stopped,
                                };
                                if let Err(e) = player.set_playback_status(status).await {
                                    say(&format!("mpris: шина потеряна ({e}), сервис остановлен"));
                                    return;
                                }
                                if let Err(e) = player.set_metadata(metadata(&update)).await {
                                    say(&format!("mpris: шина потеряна ({e}), сервис остановлен"));
                                    return;
                                }
                            }
                            // Канал закрыт — приложение завершается.
                            Either::Right(Err(_)) => return,
                        }
                    }
                })
            });
        match spawn {
            Ok(_) => match ready_rx.recv_timeout(READY_TIMEOUT) {
                Ok(()) => Some(Self {
                    updates: updates_tx,
                    closed_reported: std::sync::atomic::AtomicBool::new(false),
                }),
                // Поток умер сразу и уже напечатал причину отказа.
                Err(RecvTimeoutError::Disconnected) => None,
                // Зависший handshake (находка ревью oracle) не должен
                // блокировать старт UI: хендла нет, поток добьёт своё сам.
                Err(RecvTimeoutError::Timeout) => {
                    say("mpris: сервис не поднялся за 5 с — работаем без него");
                    None
                }
            },
            Err(e) => {
                say(&format!("mpris: поток не запущен: {e}"));
                None
            }
        }
    }

    /// Push состояния из poll-такта без блокировки. Full — очередь
    /// заполнена, мимо: следующий такт повторит. Closed — поток-владелец
    /// умер, доставка невозможна навсегда: печатаем один раз (находка
    /// ревью hunter — замаскированная потеря не должна быть молчаливой).
    pub fn push(&self, update: MprisUpdate) {
        use async_channel::TrySendError;
        use std::sync::atomic::Ordering;
        match self.updates.try_send(update) {
            Ok(()) | Err(TrySendError::Full(_)) => {}
            Err(TrySendError::Closed(_)) => {
                if !self.closed_reported.swap(true, Ordering::SeqCst) {
                    say("mpris: сервис остановлен — обновления не доставляются");
                }
            }
        }
    }
}

/// Сборка метаданных MPRIS (спека mpris): artist только у живого трека,
/// title — трек-строка по правилу окна, url — адрес потока, artUrl —
/// файл кеша при наличии. Треклиста нет — trackid фиксированный.
fn metadata(update: &MprisUpdate) -> Metadata {
    let mut builder = Metadata::builder().trackid(TrackId::NO_TRACK);
    builder = builder.title(update.track.title.clone());
    if let Some(artist) = &update.track.artist {
        builder = builder.artist([artist.clone()]);
    }
    builder = builder.url(update.stream_url.clone());
    if let Some(art) = &update.art_url {
        builder = builder.art_url(art.clone());
    }
    builder.build()
}

/// Победитель гонки двух futures.
enum Either<A, B> {
    Left(A),
    Right(B),
}

/// Селект из двух заимствованных futures: `poll` обоих с общим waker'ом
/// (каждый Pending-опрос регистрирует его сам), первый готовый
/// выигрывает; проигравший опрашивается в следующей итерации гонки.
struct Race<'a, A, B> {
    a: Pin<&'a mut A>,
    b: Pin<&'a mut B>,
}

impl<'a, A, B> Race<'a, A, B> {
    fn new(a: Pin<&'a mut A>, b: Pin<&'a mut B>) -> Self {
        Self { a, b }
    }
}

impl<A: Future, B: Future> Future for Race<'_, A, B> {
    type Output = Either<A::Output, B::Output>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        if let Poll::Ready(value) = this.a.as_mut().poll(cx) {
            return Poll::Ready(Either::Left(value));
        }
        if let Poll::Ready(value) = this.b.as_mut().poll(cx) {
            return Poll::Ready(Either::Right(value));
        }
        Poll::Pending
    }
}

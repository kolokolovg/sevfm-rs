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

//! Reader-поток на мок-сервере: переподключение после обрыва (задача 2.1),
//! мусорный бюджет с переподключением без зависания (задача 2.2),
//! остановка по флагу на молчащем сервере (задача 2.3) и закрытие
//! соединения по запросу декодера — мусорный лимит живого цикла.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

use common::{MockServer, Scenario};
use sevfm::buffer::{ReadError, RingBuffer};
use sevfm::net::{self, GARBAGE_BUDGET_BYTES, GarbageBudget, Link, READ_CHUNK};

/// Обрыв после 32 КиБ валидных данных; второе соединение отдаёт
/// продолжение фикстуры и держится. Reader обязан сшить оба куска.
#[test]
fn reconnect_after_drop_continues_stream() {
    let fixture = common::fixture_bytes();
    let first = fixture[..32 * 1024].to_vec();
    let rest = fixture[32 * 1024..64 * 1024].to_vec();
    let mut server = MockServer::start(vec![
        Scenario::stream(first),
        Scenario::paced_then_hold(rest, 8 * 1024, Duration::from_millis(5)),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    // Первый backoff — 1 с: укладываемся с запасом в 15 с.
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = vec![0u8; 64 * 1024];
    let mut filled = 0usize;
    while filled < got.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "поток не дошёл за 15 с: {filled} байтов"
        );
        match buffer.read(&mut got[filled..], remaining) {
            Ok(n) => filled += n,
            Err(ReadError::Timeout) => {}
            Err(ReadError::Closed) => panic!("буфер закрыт раньше конца потока"),
        }
    }
    assert_eq!(got, fixture[..64 * 1024], "куски сшиты без потерь");
    assert_eq!(
        server.connections(),
        2,
        "произошло ровно одно переподключение"
    );

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// Валидный старт, затем бесконечный мусор: после первого сброса от
/// «декодера» превышение мусорного бюджета закрывает соединение и
/// переподключает; после переподключения поток валидных данных
/// продолжается, процесс не зависает (задача 2.2).
#[test]
fn garbage_budget_exceeded_reconnects_without_hang() {
    let fixture = common::fixture_bytes();
    let prefix = fixture[..8 * 1024].to_vec();
    let after_reconnect = fixture[..16 * 1024].to_vec();
    let mut server = MockServer::start(vec![
        Scenario::garbage_forever(prefix.clone()),
        // Удерживаемое соединение: после выдачи данных сервер молчит —
        // новых переподключений не будет, пока тест не закончит.
        Scenario::paced_then_hold(after_reconnect.clone(), 4 * 1024, Duration::from_millis(5)),
    ]);

    // Ёмкость = 2 × GARBAGE_BUDGET_BYTES: весь мусор до превышения
    // бюджета (бюджет + prefix) помещается в буфер без дрена,
    // backpressure не останавливает reader, и содержимое буфера в момент
    // превышения детерминировано.
    let buffer = RingBuffer::new(2 * GARBAGE_BUDGET_BYTES, READ_CHUNK);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    // Активируем бюджет, как это делает декодер первым распознанным кадром.
    budget.reset();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget.clone(),
        link,
        net_status,
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    // Валидный старт дошёл — «декодер» распознал кадры и сбросил бюджет
    // (ровно это будет делать Mp3Source в задаче 3.1).
    let mut head = vec![0u8; prefix.len()];
    let mut filled = 0usize;
    while filled < head.len() {
        match buffer.read(&mut head[filled..], Duration::from_secs(5)) {
            Ok(n) => filled += n,
            Err(ReadError::Timeout) => panic!("валидный старт не дошёл за 5 с"),
            Err(ReadError::Closed) => panic!("буфер закрыт раньше времени"),
        }
    }
    assert_eq!(head, prefix);
    budget.reset();

    // Ждём переподключение, НЕ вычитывая буфер: мусор помещается целиком
    // (см. ёмкость буфера), поэтому содержимое буфера в момент превышения
    // детерминировано. Если reader при бюджетном исходе чистит буфер —
    // мусор до потребителя не доходит вовсе; если не чистит — он вошьётся
    // в начало свежего потока (находка ревью).
    let deadline = Instant::now() + Duration::from_secs(20);
    while server.connections() < 2 {
        assert!(
            deadline.saturating_duration_since(Instant::now()) > Duration::from_millis(100),
            "мусорный бюджет не сработал за 20 с — процесс завис"
        );
        thread::sleep(Duration::from_millis(10));
    }

    // После переподключения — снова валидные данные; сервер держит
    // соединение, значит первый же таймаут чтения = всё выдано.
    // Чищеный буфер: хвост — ровно свежий поток; нечищенный — свежий
    // поток, сшитый с мусорным хвостом.
    let mut chunk = vec![0u8; READ_CHUNK];
    let mut fresh: Vec<u8> = Vec::new();
    loop {
        match buffer.read(&mut chunk, Duration::from_millis(500)) {
            Ok(n) => fresh.extend_from_slice(&chunk[..n]),
            Err(ReadError::Timeout) => break,
            Err(ReadError::Closed) => panic!("буфер закрыт раньше времени"),
        }
    }
    assert!(
        fresh.starts_with(&after_reconnect),
        "после бюджетного переподключения буфер чист: свежие данные идут \
         без мусорного хвоста превышенной порции"
    );
    assert_eq!(
        server.connections(),
        2,
        "ровно одно бюджетное переподключение"
    );

    stop.store(true, Ordering::SeqCst);
    reader
        .join()
        .expect("reader завершается по флагу без зависания");
    server.stop();
}

/// Остановка на молчуне: reader подключён к серверу без данных; установка
/// флага завершает поток не дольше секунды — блокированное чтение не
/// мешает выходу (задача 2.3).
#[test]
fn stop_flag_terminates_reader_within_second_on_silent_server() {
    let mut server = MockServer::start(vec![Scenario::silence()]);
    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer,
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    // Reader успел подключиться к молчуну (соединение принято).
    let connected = Instant::now() + Duration::from_secs(5);
    while server.connections() < 1 {
        assert!(
            Instant::now() < connected,
            "reader не подключился к молчуну за 5 с"
        );
        thread::sleep(Duration::from_millis(10));
    }

    // Join с ограничением по времени: сигнал о завершении приходит
    // по каналу из потока-наблюдателя.
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = reader.join();
        tx.send(()).expect("наблюдатель жив");
    });
    let started = Instant::now();
    stop.store(true, Ordering::SeqCst);
    rx.recv_timeout(Duration::from_secs(1))
        .expect("reader завершается в течение секунды после флага");
    assert!(
        started.elapsed() <= Duration::from_secs(1),
        "флаг сработал за {:?}",
        started.elapsed()
    );
    server.stop();
}

/// Старт при недоступном сервере: сетевые ошибки первого соединения —
/// бесконечные ретраи с backoff, а не фатальная ошибка (отличимо от
/// «поток не MP3» — задача 3.2): reader жив, предбуфер не накоплен,
/// остановка срабатывает мгновенно.
#[test]
fn unreachable_server_means_retries_not_failure() {
    // Свободный порт без слушателя: соединение гарантированно refused.
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("bind пробы");
    let dead_url = format!("http://{}/stream", probe.local_addr().expect("адрес пробы"));
    drop(probe);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        dead_url,
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    // Первая попытка backoff (привязана к первой ступени лестницы) —
    // reader не упал и не накопил данных: это ожидание сети, а не фаталь.
    std::thread::sleep(sevfm::net::FIRST_BACKOFF_DELAY + Duration::from_millis(500));
    assert!(
        !reader.is_finished(),
        "reader продолжает попытки, а не завершился"
    );
    assert!(!buffer.is_prebuffered(), "данных от мёртвого порта нет");

    stop.store(true, Ordering::SeqCst);
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let _ = reader.join();
        tx.send(()).expect("наблюдатель жив");
    });
    rx.recv_timeout(Duration::from_secs(1))
        .expect("остановка срабатывает и на ретраях");
}

/// Перманентный отказ (редирект 301 и HTTP 404): reader завершается,
/// `Link` поднимает фатальный флаг — повторные попытки бессмысленны
/// (буфер закрывается).
#[test]
fn permanent_status_terminates_reader_with_fatal_flag() {
    for status in [301, 404] {
        let mut server = MockServer::start(vec![Scenario::status(status)]);
        let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
        let stop = Arc::new(AtomicBool::new(false));
        let budget = GarbageBudget::shared();
        let link = Link::shared();
        let net_status = net::SharedNetStatus::default();

        let now_playing = sevfm::icy::SharedNowPlaying::default();
        let reader = net::spawn_reader(
            server.url(),
            buffer,
            stop.clone(),
            budget,
            link.clone(),
            net_status,
            now_playing,
        )
        .expect("поток reader'а запускается в тесте");

        // Завершение с фатальным флагом — без вечных ретраев статуса.
        let deadline = Instant::now() + Duration::from_secs(2);
        while !reader.is_finished() {
            assert!(
                Instant::now() < deadline,
                "reader не завершился на HTTP {status} за 2 с"
            );
            thread::sleep(Duration::from_millis(10));
        }
        reader.join().expect("reader завершился без паники");
        assert!(link.fatal(), "фатальный флаг поднят на HTTP {status}");
        assert!(!link.reader_panicked(), "это не паника reader'а");

        stop.store(true, Ordering::SeqCst);
        server.stop();
    }
}

/// Мусорный лимит декодера закрывает соединение, буфер очищается от
/// байтов закрытого соединения, reader переподключается и пишет данные
/// следующего сценария с чистого листа.
#[test]
fn reconnect_request_closes_connection_and_clears_buffer() {
    let fixture = common::fixture_bytes();
    let first = fixture[..32 * 1024].to_vec();
    let second = fixture[32 * 1024..64 * 1024].to_vec();
    // Первое соединение после выдачи данных держится (нет EOF — без
    // запроса reader не переподключался бы сам) — изолируем путь Link.
    let mut server = MockServer::start(vec![
        Scenario::paced_then_hold(first.clone(), 8 * 1024, Duration::from_millis(5)),
        Scenario::paced_then_hold(second.clone(), 4 * 1024, Duration::from_millis(5)),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link.clone(),
        net_status,
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    // Первое соединение накормило буфер — вычитываем его данные.
    let mut head = vec![0u8; first.len()];
    let mut filled = 0usize;
    while filled < head.len() {
        match buffer.read(&mut head[filled..], Duration::from_secs(5)) {
            Ok(n) => filled += n,
            Err(ReadError::Timeout) => panic!("данные первого соединения не дошли за 5 с"),
            Err(ReadError::Closed) => panic!("буфер закрыт раньше времени"),
        }
    }
    assert_eq!(head, first);

    // «Декодер» исчерпал лимит ошибок: запрос переподключения, штамп
    // текущей эпохи — как это делает Mp3Source (запрос чужой эпохи
    // reader обязан отбрасывать).
    link.request_reconnect(link.epoch());

    // Reader закрыл соединение и переподключился (backoff после данных —
    // минимальный, 1 с).
    let reconnected = Instant::now() + Duration::from_secs(5);
    while server.connections() < 2 {
        assert!(
            Instant::now() < reconnected,
            "запрос не закрыл соединение за 5 с"
        );
        thread::sleep(Duration::from_millis(10));
    }

    // После очистки буфер содержит только данные нового соединения —
    // ровно второй сценарий, байтов первого в нём нет.
    let mut tail = vec![0u8; second.len()];
    let mut got = 0usize;
    while got < tail.len() {
        match buffer.read(&mut tail[got..], Duration::from_secs(5)) {
            Ok(n) => got += n,
            Err(ReadError::Timeout) => panic!("данные второго соединения не дошли за 5 с"),
            Err(ReadError::Closed) => panic!("буфер закрыт раньше времени"),
        }
    }
    assert_eq!(tail, second, "после очистки — только новое соединение");
    assert_eq!(server.connections(), 2, "ровно одно переподключение");

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// «Капающий» сервер: байты идут (по чуть-чуть — ниже минимальной
/// скорости), предбуфер не набирается — соединение закрывается как
/// деградировавшее, reader переподключается; вечного «Подключения» нет
/// (спека: не обрабатывать одно соединение бесконечно без прогресса).
#[test]
fn dripping_server_below_min_rate_reconnects() {
    let fixture = common::fixture_bytes();
    // 512 байт каждые 500 мс = ~1 КиБ/с — заведомо ниже минимума
    // (20 КиБ за 10 с); за окно наберётся ~5 КиБ < предбуфера 16 КиБ.
    let drip = fixture[..32 * 1024].to_vec();
    let mut server = MockServer::start(vec![
        Scenario::paced_then_hold(drip, 512, Duration::from_millis(500)),
        Scenario::silence(),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    // Окно минимальной скорости — 10 с от первого байта: деградация
    // закрывает соединение, происходит переподключение.
    let reconnected = Instant::now() + Duration::from_secs(20);
    while server.connections() < 2 {
        assert!(
            Instant::now() < reconnected,
            "«капающий» сервер не был закрыт за 20 с ({} соединений, {} байтов)",
            server.connections(),
            buffer.available()
        );
        thread::sleep(Duration::from_millis(50));
    }
    // За окно «капель» не набрала и предбуфера — старта декодирования
    // не было (это подключение без прогресса к звуку).
    assert!(
        !buffer.is_prebuffered(),
        "скорость ниже минимальной не должна накопить предбуфер"
    );

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// Статусы reader'а на управляемом сценарии (задача 4.1): медленное
/// первое соединение (данные наблюдаются живьём) → обрыв → окно
/// переподключения на весь backoff → второе соединение. Сводной функцией
/// это даёт Подключение → Воспроизведение → Переподключение →
/// Воспроизведение.
#[test]
fn net_status_tracks_connect_stream_reconnect() {
    use sevfm::net::PlaybackStatus as P;
    use sevfm::net::{NetStatus, playback_status};

    let fixture = common::fixture_bytes();
    // Первое соединение тянется ~2 с (4 чанка по 500 мс): статус Streaming
    // наблюдается заведомо дольше джиттера тестов, затем EOF.
    let first = fixture[..16 * 1024].to_vec();
    let rest = fixture[16 * 1024..48 * 1024].to_vec();
    let mut server = MockServer::start(vec![
        Scenario::paced_then_close(first.clone(), 4 * 1024, Duration::from_millis(500)),
        Scenario::paced_then_hold(rest, 8 * 1024, Duration::from_millis(5)),
    ]);

    // Ёмкость с запасом на повторные подключения (мок повторяет
    // последний сценарий): тест не сливает буфер, и reader, дописывающий
    // сверх ёмкости, завис бы в записи навсегда.
    let buffer = RingBuffer::new(256 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();

    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status.clone(),
        now_playing,
    )
    .expect("поток reader'а запускается в тесте");

    let current = |net_status: &net::SharedNetStatus| {
        *net_status
            .lock()
            .expect("замок статуса не отравлен: присваивание не паникует")
    };
    let wait_for = |net_status: &net::SharedNetStatus, want: NetStatus| {
        let deadline = Instant::now() + Duration::from_secs(10);
        while current(net_status) != want {
            assert!(
                Instant::now() < deadline,
                "статус не дошёл до {want:?}, сейчас {:?}",
                current(net_status)
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    };

    // Старт: первое соединение ещё качает предбуфер — до старта звука
    // любое сетевое состояние сводится в Подключение (underrun-флаг —
    // состояние декодера, на уровне reader'а паузы нет).
    let deadline = Instant::now() + Duration::from_secs(10);
    while !buffer.is_prebuffered() {
        assert!(Instant::now() < deadline, "предбуфер не накоплен за 10 с");
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(!buffer.is_underrun(), "декодера нет — паузы быть не может");
    assert_eq!(
        playback_status(current(&net_status), false, false),
        P::Connecting
    );

    // Соединение живо, данные идут — после старта звука это игра.
    wait_for(&net_status, NetStatus::Streaming);
    assert_eq!(
        playback_status(current(&net_status), true, false),
        P::Playing
    );

    // Обрыв первого соединения (после выдачи всех чанков): весь backoff
    // в 1 с статус — переподключение, буфер кормит.
    wait_for(&net_status, NetStatus::Reconnecting);
    assert!(
        buffer.available() > 0,
        "кратковременный обрыв — буфер кормит"
    );
    assert_eq!(
        playback_status(current(&net_status), true, false),
        P::Reconnecting
    );

    // Переподключение состоялось — снова чтение.
    wait_for(&net_status, NetStatus::Streaming);
    assert_eq!(
        playback_status(current(&net_status), true, false),
        P::Playing
    );

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// Дожидается `want` принятых соединений: переподключение reader'а
/// уходит на backoff (первая ступень 1 с) — ассерты без ожидания гонялись
/// бы с ним.
fn wait_for_connections(server: &MockServer, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while server.connections() < want {
        assert!(
            Instant::now() < deadline,
            "соединений {want} не дождались: {}",
            server.connections()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Метаблоки вырезаются из аудиопути (задача 2.2, спека icy-metadata):
/// байты, дошедшие до буфера, равны чистой фикстуре байт-в-байт, станция
/// и трек появляются в слоте эфира; договорённость запрошена на каждом
/// соединении.
#[test]
fn icy_blocks_are_stripped_and_track_updates() {
    use sevfm::icy::TrackInfo;

    let fixture = common::fixture_bytes();
    let metaint = 16 * 1024;
    let framed = common::icy_frame(
        &fixture[..64 * 1024],
        metaint,
        &["{\"artist\":\"ВЛАДИМИР ТРОШИН\",\"name\":\"ЮНОСТЬ МОЯ\"}"],
    );
    let mut server = MockServer::start(vec![
        Scenario::icy_stream(framed, metaint, "Говорит Севастополь"),
        // Парковка: второе соединение молчит — переподключений нет.
        Scenario::silence(),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();
    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing.clone(),
    )
    .expect("поток reader'а запускается в тесте");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = vec![0u8; 64 * 1024];
    let mut filled = 0usize;
    while filled < got.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "аудио не дошло за 15 с: {filled} байтов"
        );
        match buffer.read(&mut got[filled..], remaining) {
            Ok(n) => filled += n,
            Err(ReadError::Timeout) => {}
            Err(ReadError::Closed) => panic!("буфер закрыт раньше конца потока"),
        }
    }
    // Аудиопуть чист: метаблоки вырезаны до буфера, байт-в-байт фикстура.
    assert_eq!(got, fixture[..64 * 1024], "метаблоки попали в аудиопуть");
    wait_for_connections(&server, 2);
    {
        let ether = now_playing
            .lock()
            .expect("замок эфира не отравлен: присваивание не паникует");
        assert_eq!(ether.station.as_deref(), Some("Говорит Севастополь"));
        assert_eq!(
            ether.track,
            Some(TrackInfo {
                artist: Some("ВЛАДИМИР ТРОШИН".to_owned()),
                title: "ЮНОСТЬ МОЯ".to_owned(),
            })
        );
    }
    assert_eq!(server.connections(), 2, "ровно одно переподключение");
    assert_eq!(server.icy_requests(), 2, "метаданные запрошены в обоих");

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// Смена трека по сценарным метаблокам (задача 2.2): последний блок
/// побеждает — после цикла трек/вставка в слоте соответствует ему.
#[test]
fn icy_track_changes_with_blocks() {
    use sevfm::icy::TrackInfo;

    let fixture = common::fixture_bytes();
    let metaint = 16 * 1024;
    // 4 блока: трек, вставка, трек, вставка — финал «Реклама».
    let framed = common::icy_frame(
        &fixture[..64 * 1024],
        metaint,
        &[
            "{\"artist\":\"ВЛАДИМИР ТРОШИН\",\"name\":\"ЮНОСТЬ МОЯ\"}",
            "Реклама",
        ],
    );
    let mut server = MockServer::start(vec![
        Scenario::icy_stream(framed, metaint, "Говорит Севастополь"),
        Scenario::silence(),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();
    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing.clone(),
    )
    .expect("поток reader'а запускается в тесте");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut sink = vec![0u8; 8 * 1024];
    while now_playing
        .lock()
        .expect("замок эфира не отравлен: присваивание не паникует")
        .track
        != Some(TrackInfo {
            artist: None,
            title: "Реклама".to_owned(),
        })
    {
        assert!(
            Instant::now() < deadline,
            "смена трека не наблюдалась за 15 с"
        );
        // Держим буфер ненаполненным: поток дорабатывает до последнего
        // блока, не застревая в backpressure.
        let _ = buffer.read(&mut sink, Duration::from_millis(10));
    }
    assert_eq!(
        server.connections(),
        1,
        "смены трека не требуют переподключений"
    );

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// Сервер без ICY (нет icy-заголовков в ответе): чистый MP3 без
/// метаданных — воспроизведение как в фазе 1, слот эфира пуст (спека:
/// отсутствие интервала — не ошибка). Запрос метаданных клиент шлёт всё
/// равно — договорённость на каждом соединении.
#[test]
fn no_icy_server_plays_plain_stream() {
    let fixture = common::fixture_bytes();
    let mut server = MockServer::start(vec![
        Scenario::stream(fixture[..64 * 1024].to_vec()),
        Scenario::silence(),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();
    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing.clone(),
    )
    .expect("поток reader'а запускается в тесте");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = vec![0u8; 64 * 1024];
    let mut filled = 0usize;
    while filled < got.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "аудио не дошло за 15 с: {filled} байтов"
        );
        match buffer.read(&mut got[filled..], remaining) {
            Ok(n) => filled += n,
            Err(ReadError::Timeout) => {}
            Err(ReadError::Closed) => panic!("буфер закрыт раньше конца потока"),
        }
    }
    assert_eq!(got, fixture[..64 * 1024]);
    {
        let ether = now_playing
            .lock()
            .expect("замок эфира не отравлен: присваивание не паникует");
        assert!(ether.station.is_none(), "станции нет без icy-name");
        assert!(ether.track.is_none(), "трека нет без метаблоков");
    }
    wait_for_connections(&server, 2);
    assert_eq!(server.icy_requests(), 2, "запрос метаданных шлётся всегда");

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

/// Переподключение переустанавливает договорённость (спека icy-metadata):
/// кадрирование второго соединения начинается с его заголовков заново,
/// аудио обоих кусков сшито без потерь, трек — из последнего блока.
#[test]
fn reconnect_reframes_icy_stream() {
    use sevfm::icy::TrackInfo;

    let fixture = common::fixture_bytes();
    let metaint = 8 * 1024;
    let first = common::icy_frame(
        &fixture[..32 * 1024],
        metaint,
        &["{\"artist\":\"ВЛАДИМИР ТРОШИН\",\"name\":\"ЮНОСТЬ МОЯ\"}"],
    );
    let rest = common::icy_frame(
        &fixture[32 * 1024..64 * 1024],
        metaint,
        &["{\"artist\":\"ИСПОЛНИТЕЛЬ\",\"name\":\"ВТОРОЙ ТРЕК\",\"type\":\"М\"}"],
    );
    let mut server = MockServer::start(vec![
        Scenario::icy_stream(first, metaint, "Говорит Севастополь"),
        Scenario::icy_paced_then_hold(
            rest,
            8 * 1024,
            Duration::from_millis(5),
            metaint,
            "Говорит Севастополь",
        ),
    ]);

    let buffer = RingBuffer::new(64 * 1024, 16 * 1024);
    let stop = Arc::new(AtomicBool::new(false));
    let budget = GarbageBudget::shared();
    let link = Link::shared();
    let net_status = net::SharedNetStatus::default();
    let now_playing = sevfm::icy::SharedNowPlaying::default();
    let reader = net::spawn_reader(
        server.url(),
        buffer.clone(),
        stop.clone(),
        budget,
        link,
        net_status,
        now_playing.clone(),
    )
    .expect("поток reader'а запускается в тесте");

    let deadline = Instant::now() + Duration::from_secs(15);
    let mut got = vec![0u8; 64 * 1024];
    let mut filled = 0usize;
    while filled < got.len() {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "поток не дошёл за 15 с: {filled} байтов"
        );
        match buffer.read(&mut got[filled..], remaining) {
            Ok(n) => filled += n,
            Err(ReadError::Timeout) => {}
            Err(ReadError::Closed) => panic!("буфер закрыт раньше конца потока"),
        }
    }
    assert_eq!(got, fixture[..64 * 1024], "куски сшиты без потерь и мусора");
    wait_for_connections(&server, 2);
    assert_eq!(server.connections(), 2, "ровно одно переподключение");
    assert_eq!(
        server.icy_requests(),
        2,
        "договорённость на каждом соединении"
    );
    // Трек второго соединения может дойти до слота позже последнего
    // байта буфера: поллим до нужного TrackInfo с дедлайном (образец —
    // icy_track_changes_with_blocks).
    let expected_track = Some(TrackInfo {
        artist: Some("ИСПОЛНИТЕЛЬ".to_owned()),
        title: "ВТОРОЙ ТРЕК".to_owned(),
    });
    while now_playing
        .lock()
        .expect("замок эфира не отравлен: присваивание не паникует")
        .track
        != expected_track
    {
        assert!(
            Instant::now() < deadline,
            "трек второго соединения не наблюдался за 15 с"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let ether = now_playing
        .lock()
        .expect("замок эфира не отравлен: присваивание не паникует");
    assert_eq!(ether.station.as_deref(), Some("Говорит Севастополь"));

    stop.store(true, Ordering::SeqCst);
    reader.join().expect("reader завершается по флагу");
    server.stop();
}

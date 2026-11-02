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

//! Ограниченный кольцевой буфер байтов между reader-потоком и трактом
//! вывода (design D4): воспроизведение идёт из буфера, а не из сети.
//!
//! Писатель (`net::reader`) блокируется, пока нет места, — backpressure:
//! сетевое чтение замирает, TCP-окно сжимается, сервер приостанавливает
//! отправку. Читатель (тракт декодирования) ждёт первый байт не дольше
//! таймаута и получает сколько накопилось (возможно, меньше запрошенного).
//! `close()` разблокирует обе стороны: читатель дочитывает накопленное и
//! получает конец потока, писатель — ошибку записи.

use std::cmp::min;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Ёмкость буфера фазы 1 (design D4): 512 КиБ ≈ 26 с аудио при 128 кбит/с.
const PHASE1_CAPACITY: usize = 512 * 1024;
/// Предбуфер фазы 1 (design D4): 64 КиБ ≈ 4 с — старт воспроизведения
/// и возобновление после underrun.
const PHASE1_PREBUFFER: usize = 64 * 1024;

/// Ошибка чтения из буфера.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ReadError {
    /// За таймаут не появился ни один байт (underrun — решение на стороне
    /// читателя: подождать предбуфер или остановиться).
    #[error("чтение буфера: таймаут")]
    Timeout,
    /// Буфер закрыт и опустошён — конец потока.
    #[error("чтение буфера: конец потока")]
    Closed,
}

/// Ошибка записи в буфер.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum WriteError {
    /// Буфер закрыт — данные больше не принимаются.
    #[error("запись буфера: буфер закрыт")]
    Closed,
}

/// Состояние буфера под одним замком.
struct State {
    storage: Vec<u8>,
    /// Начало данных (позиция чтения) в кольце.
    head: usize,
    /// Число байтов в буфере.
    len: usize,
    closed: bool,
}

impl State {
    fn free(&self) -> usize {
        self.storage.len() - self.len
    }

    /// Кладёт `src` в кольцо; вызов гарантирует `src.len() <= free()`.
    fn copy_in(&mut self, src: &[u8]) {
        debug_assert!(src.len() <= self.free(), "в буфере есть место");
        let tail = (self.head + self.len) % self.storage.len();
        let first = min(src.len(), self.storage.len() - tail);
        self.storage[tail..tail + first].copy_from_slice(&src[..first]);
        self.storage[..src.len() - first].copy_from_slice(&src[first..]);
        self.len += src.len();
    }

    /// Забирает до `dst.len()` байтов, возвращая число скопированных.
    fn copy_out(&mut self, dst: &mut [u8]) -> usize {
        let n = min(dst.len(), self.len);
        let first = min(n, self.storage.len() - self.head);
        dst[..first].copy_from_slice(&self.storage[self.head..self.head + first]);
        dst[first..n].copy_from_slice(&self.storage[..n - first]);
        self.head = (self.head + n) % self.storage.len();
        self.len -= n;
        n
    }
}

struct Shared {
    state: Mutex<State>,
    /// Будит читателей: появились данные или буфер закрыт.
    data_ready: Condvar,
    /// Будит писателей: появилось место или буфер закрыт.
    space_ready: Condvar,
    /// Декодер в underrun-паузе (ждёт повторного достижения предбуфера).
    /// После выноса декодирования в поток (design D8) это признак паузы
    /// ДЕКОДЕРА: звук из сэмпл-FIFO доигрывает ещё до ~2,4 с — статус
    /// «Буферизация» появляется чуть раньше фактической тишины.
    underrun: AtomicBool,
    /// Сколько байтов всего отдано читателю за жизнь буфера (design
    /// add-ether-sync D3): монотонная позиция звучащего звука в шкале
    /// «аудиобайты, записанные в буфер». `clear()` выбрасывает только
    /// непрочитанное — счётчик не откатывается.
    total_read: AtomicU64,
}

/// Хэндл кольцевого буфера. `Clone` — разделяемое владение одним буфером:
/// reader-поток пишет, тракт вывода читает.
#[derive(Clone)]
pub struct RingBuffer {
    prebuffer: usize,
    shared: Arc<Shared>,
}

impl RingBuffer {
    /// Строит буфер ёмкости `capacity` с предбуфером `prebuffer` (порог
    /// накопления до старта воспроизведения и возобновления после underrun).
    pub fn new(capacity: usize, prebuffer: usize) -> Self {
        assert!(capacity > 0, "ёмкость буфера должна быть положительной");
        assert!(
            prebuffer <= capacity,
            "предбуфер {prebuffer} больше ёмкости {capacity}"
        );
        Self {
            prebuffer,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    storage: vec![0; capacity],
                    head: 0,
                    len: 0,
                    closed: false,
                }),
                data_ready: Condvar::new(),
                space_ready: Condvar::new(),
                underrun: AtomicBool::new(false),
                total_read: AtomicU64::new(0),
            }),
        }
    }

    /// Буфер с параметрами фазы 1 (design D4): 512 КиБ / 64 КиБ.
    pub fn phase1() -> Self {
        Self::new(PHASE1_CAPACITY, PHASE1_PREBUFFER)
    }

    /// Число байтов, доступных чтению прямо сейчас.
    pub fn available(&self) -> usize {
        self.lock().len
    }

    /// Закрыт ли буфер. Накопленные данные при этом ещё дочитываются.
    pub(crate) fn is_closed(&self) -> bool {
        self.lock().closed
    }

    /// Достигнут ли предбуфер-порог: старт воспроизведения и возобновление
    /// после underrun (design D4 — «тот же порог»).
    pub fn is_prebuffered(&self) -> bool {
        self.available() >= self.prebuffer
    }

    /// Декодер стоит в underrun-паузе (буфер опустошался, ждёт повторного
    /// предбуфера). Отмечает `BufferReader` декод-потока; звук при этом
    /// доигрывает сэмпл-FIFO (≤2,4 с) — мгновенной паузой звука это стало
    /// после D8 не сразу (см. поле `underrun`).
    pub fn is_underrun(&self) -> bool {
        self.shared.underrun.load(Ordering::SeqCst)
    }

    /// Позиция звучащего звука: сколько байтов всего отдано читателю за
    /// жизнь буфера. Шкала та же, что у аудиобайтов, записанных writer'ом:
    /// ворота эфира сверяют смещения метаблоков с этим числом, `clear()`
    /// позицию не откатывает (design add-ether-sync D3). `Relaxed`
    /// достаточен: дренаж читает атомик до захвата замка очереди, гонка
    /// стоит один такт опроса, монотонность гарантирует RMW.
    pub fn total_read(&self) -> u64 {
        self.shared.total_read.load(Ordering::Relaxed)
    }

    /// Отметить вход/выход декодера из underrun-паузы (вызывает
    /// `BufferReader` тракта).
    pub(crate) fn set_underrun(&self, paused: bool) {
        self.shared.underrun.store(paused, Ordering::SeqCst);
    }

    /// Пишет `data` целиком, блокируясь, пока нет места (backpressure).
    ///
    /// Блокировку снимает только появление места или `close()`. После
    /// закрытия возвращает [`WriteError::Closed`]; байты, записанные до
    /// закрытия, остаются доступными читателю.
    pub fn write(&self, data: &[u8]) -> Result<(), WriteError> {
        let mut written = 0;
        let mut state = self.lock();
        loop {
            if state.closed {
                return Err(WriteError::Closed);
            }
            if written < data.len() {
                let n = min(state.free(), data.len() - written);
                state.copy_in(&data[written..written + n]);
                written += n;
                // Уведомляем под замком: пропускаем промежуточное
                // пробуждение читателя между освобождением и записью.
                self.shared.data_ready.notify_all();
            }
            if written == data.len() {
                return Ok(());
            }
            state = self
                .shared
                .space_ready
                .wait_while(state, |s| s.free() == 0 && !s.closed)
                .expect("замок буфера не отравлен: критические секции не паникуют");
        }
    }

    /// Читает до `out.len()` байтов, дожидаясь первого байта не дольше
    /// `timeout`. Отдаёт сколько накопилось — меньше запрошенного, но
    /// больше нуля (кроме пустого `out`).
    pub fn read(&self, out: &mut [u8], timeout: Duration) -> Result<usize, ReadError> {
        if out.is_empty() {
            return Ok(0);
        }
        let deadline = Instant::now() + timeout;
        let mut state = self.lock();
        loop {
            if state.len > 0 {
                let n = state.copy_out(out);
                // Позиция звука растёт только реально отданными байтами
                // (design add-ether-sync D3) — под замком, атомарный RMW.
                self.shared
                    .total_read
                    .fetch_add(n as u64, Ordering::Relaxed);
                self.shared.space_ready.notify_all();
                return Ok(n);
            }
            if state.closed {
                return Err(ReadError::Closed);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(ReadError::Timeout);
            }
            state = self
                .shared
                .data_ready
                .wait_timeout_while(state, remaining, |s| s.len == 0 && !s.closed)
                .expect("замок буфера не отравлен: критические секции не паникуют")
                .0;
        }
    }

    /// Сбрасывает накопленные данные: буфер становится пустым и остаётся
    /// открытым. Разблокирует писателя, стоящего на полном буфере.
    ///
    /// Точка применения — переподключение по мусорному лимиту декодера
    /// (design D4): неподозреваемые байты закрытого соединения не сшиваются
    /// с потоком нового. Сетевые обрывы буфер НЕ сбрасывают: накопленное —
    /// валидное аудио, которое кормит воспроизведение во время backoff.
    ///
    /// `head` не трогаем: при `len == 0` позиция головы не наблюдаема,
    /// следующая запись ляжет с индекса `(head + len) % ёмкость == head`.
    pub fn clear(&self) {
        let mut state = self.lock();
        state.len = 0;
        drop(state);
        self.shared.data_ready.notify_all();
        self.shared.space_ready.notify_all();
    }

    /// Закрывает буфер: писатели получают [`WriteError::Closed`], читатели
    /// дочитывают накопленное и получают [`ReadError::Closed`]. Идемпотентно.
    pub fn close(&self) {
        let mut state = self.lock();
        state.closed = true;
        drop(state);
        self.shared.data_ready.notify_all();
        self.shared.space_ready.notify_all();
    }

    /// Замок с доказуемым инвариантом: паника под замком невозможна
    /// (`copy_in`/`copy_out` не паникуют при согласованном `head`/`len`,
    /// остальные операции под замком — тривиальны), значит и отравления нет.
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.shared
            .state
            .lock()
            .expect("замок буфера не отравлен: критические секции не паникуют")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::mpsc;
    use std::thread;

    /// Пауза, чтобы заблокированная сторона успела войти в ожидание.
    const SETTLE: Duration = Duration::from_millis(150);

    /// Детерминированные псевдослучайные числа (xorshift) — тесты без
    /// недетерминизма rand.
    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
    }

    /// Переплетённые запись/чтение разной длины: порядок байтов сохраняется
    /// при переходе головы кольца через границу хранилища.
    #[test]
    fn interleaved_write_read_preserves_order_across_wraparound() {
        let buffer = RingBuffer::new(16, 4);
        let mut model = VecDeque::new();
        let mut next_value = 0u8;
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
        for _ in 0..300 {
            let w = (rng.next() % 7 + 1) as usize;
            // Проверка свободного места — тест однопоточный: запись не
            // должна блокироваться.
            if buffer.available() + w <= 16 {
                let chunk: Vec<u8> = (0..w)
                    .map(|_| {
                        let v = next_value;
                        next_value = (next_value + 1) % 251;
                        v
                    })
                    .collect();
                buffer.write(&chunk).unwrap();
                model.extend(chunk);
            }
            let r = (rng.next() % 6 + 1) as usize;
            let mut out = vec![0u8; r];
            let n = buffer.read(&mut out, Duration::from_secs(1)).unwrap();
            let expected: Vec<u8> = model.drain(..n).collect();
            assert_eq!(&out[..n], expected.as_slice(), "порядок байтов на выходе");
        }
    }

    /// Чтение пустого буфера: таймаут по истечении интервала, не раньше.
    #[test]
    fn read_empty_times_out() {
        let buffer = RingBuffer::new(16, 8);
        let start = Instant::now();
        let result = buffer.read(&mut [0u8; 4], Duration::from_millis(200));
        assert_eq!(result, Err(ReadError::Timeout));
        assert!(
            start.elapsed() >= Duration::from_millis(180),
            "чтение вернулось раньше таймаута: {:?}",
            start.elapsed()
        );
    }

    /// Чтение отдаёт сколько накопилось — меньше запрошенного, без ожидания
    /// полного объёма.
    #[test]
    fn read_returns_available_bytes_immediately() {
        let buffer = RingBuffer::new(16, 8);
        buffer.write(&[7, 11, 19, 23]).unwrap();
        let mut out = [0u8; 8];
        let start = Instant::now();
        assert_eq!(buffer.read(&mut out, Duration::from_secs(1)).unwrap(), 4);
        assert_eq!(&out[..4], &[7, 11, 19, 23]);
        assert!(
            start.elapsed() < SETTLE,
            "неполное чтение не ждёт полный объём"
        );
    }

    /// Предбуфер-порог: `is_prebuffered` ложен до порога и истинен на нём.
    #[test]
    fn prebuffer_threshold_reached_at_exact_bytes() {
        let buffer = RingBuffer::new(16, 8);
        buffer.write(&[0u8; 7]).unwrap();
        assert!(!buffer.is_prebuffered(), "порог ещё не достигнут");
        buffer.write(&[0u8; 1]).unwrap();
        assert_eq!(buffer.available(), 8);
        assert!(buffer.is_prebuffered(), "порог достигнут ровно");
    }

    /// Полный буфер: писатель блокируется (backpressure) и дописывает
    /// остаток, как только читатель освободил место.
    #[test]
    fn write_on_full_buffer_blocks_until_space_freed() {
        let buffer = RingBuffer::new(8, 4);
        buffer.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        assert_eq!(buffer.available(), 8);

        let (tx, rx) = mpsc::channel();
        let writer = {
            let buffer = buffer.clone();
            thread::spawn(move || tx.send(buffer.write(&[9, 10, 11, 12, 13, 14, 15, 16])))
        };
        thread::sleep(SETTLE);
        assert!(
            rx.try_recv().is_err(),
            "писатель заблокирован на полном буфере"
        );

        // Дочитываем всё: каждое чтение освобождает место, и писатель
        // дописывает остаток порции по мере него.
        let mut all = [0u8; 16];
        let mut filled = 0;
        while filled < all.len() {
            filled += buffer
                .read(&mut all[filled..], SETTLE * 4)
                .expect("чтение после освобождения места");
        }

        assert_eq!(
            rx.recv_timeout(SETTLE * 10).unwrap(),
            Ok(()),
            "писатель дописал остаток после освобождения места"
        );
        let _ = writer.join().expect("поток писателя завершён");

        // Порядок не нарушен: первая порция, затем вторая.
        assert_eq!(all, [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);
    }

    /// Close разблокирует читателя, стоящего в пустом буфере: конец потока
    /// вместо таймаута.
    #[test]
    fn close_unblocks_blocked_reader() {
        let buffer = RingBuffer::new(16, 8);
        let (tx, rx) = mpsc::channel();
        let reader = {
            let buffer = buffer.clone();
            thread::spawn(move || tx.send(buffer.read(&mut [0u8; 4], Duration::from_secs(60))))
        };
        thread::sleep(SETTLE);

        buffer.close();

        assert_eq!(
            rx.recv_timeout(SETTLE * 10).unwrap(),
            Err(ReadError::Closed),
            "заблокированный читатель получает конец потока"
        );
        let _ = reader.join().expect("читатель завершён после close");
    }

    /// Close разблокирует писателя, стоящего на полном буфере: ошибка
    /// записи; данные, записанные до блокировки, читатель дочитывает.
    #[test]
    fn close_unblocks_blocked_writer_and_keeps_written_data() {
        let buffer = RingBuffer::new(8, 4);
        buffer.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();

        let (tx, rx) = mpsc::channel();
        let writer = {
            let buffer = buffer.clone();
            thread::spawn(move || tx.send(buffer.write(&[9, 10])))
        };
        thread::sleep(SETTLE);

        buffer.close();

        assert_eq!(
            rx.recv_timeout(SETTLE * 10).unwrap(),
            Err(WriteError::Closed),
            "заблокированный писатель получает ошибку"
        );
        let _ = writer.join().expect("писатель завершён после close");

        let mut out = [0u8; 8];
        let mut filled = 0;
        while filled < out.len() {
            filled += buffer.read(&mut out[filled..], SETTLE * 4).unwrap();
        }
        assert_eq!(
            out,
            [1, 2, 3, 4, 5, 6, 7, 8],
            "данные до close дочитываются"
        );
        assert_eq!(
            buffer.read(&mut [0u8; 1], SETTLE),
            Err(ReadError::Closed),
            "после данных — конец потока"
        );
    }

    /// Close с накопленными данными: читатель получает всё, затем конец
    /// потока (идемпотентный).
    #[test]
    fn close_drains_pending_data_then_eof() {
        let buffer = RingBuffer::new(16, 8);
        buffer.write(&[5, 6, 7, 8]).unwrap();
        buffer.close();

        let mut out = [0u8; 4];
        assert_eq!(buffer.read(&mut out, SETTLE).unwrap(), 4);
        assert_eq!(out, [5, 6, 7, 8]);
        assert_eq!(buffer.read(&mut out, SETTLE), Err(ReadError::Closed));
        assert_eq!(buffer.read(&mut out, SETTLE), Err(ReadError::Closed));
    }

    /// Запись после close — немедленная ошибка, без блокировки.
    #[test]
    fn write_after_close_fails_immediately() {
        let buffer = RingBuffer::new(16, 8);
        buffer.close();
        let start = Instant::now();
        assert_eq!(buffer.write(&[1]), Err(WriteError::Closed));
        assert!(start.elapsed() < SETTLE);
    }

    /// Clear сбрасывает данные, оставляя буфер открытым: читатель ждёт
    /// новые данные (таймаут, не EOF), писатель с полного буфера
    /// разблокируется, новые записи читаются с чистого листа.
    #[test]
    fn clear_discards_data_and_keeps_buffer_open() {
        let buffer = RingBuffer::new(8, 4);
        buffer.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        buffer.clear();

        assert_eq!(buffer.available(), 0, "данные сброшены");
        assert!(!buffer.is_closed(), "буфер остаётся открытым");
        // Пустой и открытый: чтение — таймаут, а не конец потока.
        assert_eq!(
            buffer.read(&mut [0u8; 4], Duration::from_millis(50)),
            Err(ReadError::Timeout)
        );

        // Писатель, блокированный на полном буфере, разблокирован clear:
        // место появилось мгновенно.
        let buffer_full = RingBuffer::new(8, 4);
        buffer_full.write(&[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
        let (tx, rx) = mpsc::channel();
        let writer = {
            let buffer = buffer_full.clone();
            thread::spawn(move || tx.send(buffer.write(&[9, 10, 11, 12])))
        };
        thread::sleep(SETTLE);
        assert!(rx.try_recv().is_err(), "писатель заблокирован до clear");
        buffer_full.clear();
        assert_eq!(
            rx.recv_timeout(SETTLE * 10).unwrap(),
            Ok(()),
            "clear освободил место — писатель дописал"
        );
        let _ = writer.join().expect("поток писателя завершён");

        // Новые записи читаются с чистого листа: старых байтов нет.
        let mut out = [0u8; 4];
        assert_eq!(buffer_full.read(&mut out, SETTLE * 4).unwrap(), 4);
        assert_eq!(out, [9, 10, 11, 12], "после clear — только новые байты");
    }

    /// Позиция звука (design add-ether-sync D3): растёт ровно на
    /// прочитанное, не двигается записью и таймаутом пустого чтения,
    /// `clear()` не откатывает — новые данные читаются дальше по той же
    /// шкале.
    #[test]
    fn total_read_tracks_read_bytes_and_survives_clear() {
        let buffer = RingBuffer::new(16, 8);
        buffer.write(&[1, 2, 3, 4]).unwrap();
        assert_eq!(buffer.total_read(), 0, "запись не двигает позицию звука");

        let mut out = [0u8; 3];
        assert_eq!(buffer.read(&mut out, SETTLE).unwrap(), 3);
        assert_eq!(buffer.total_read(), 3);
        assert_eq!(buffer.read(&mut out[..1], SETTLE).unwrap(), 1);
        assert_eq!(buffer.total_read(), 4);

        // Пустое чтение — позицию не двигает.
        assert_eq!(
            buffer.read(&mut out, Duration::from_millis(20)),
            Err(ReadError::Timeout)
        );
        assert_eq!(buffer.total_read(), 4, "таймаут не двигает позицию");

        // Clear выбрасывает непрочитанное: прочитанное не отменяется.
        buffer.write(&[5, 6]).unwrap();
        buffer.clear();
        assert_eq!(buffer.total_read(), 4, "clear не откатывает позицию");

        // Новые данные — дальше по той же монотонной шкале.
        buffer.write(&[7, 8]).unwrap();
        assert_eq!(buffer.read(&mut out[..2], SETTLE).unwrap(), 2);
        assert_eq!(buffer.total_read(), 6);
    }

    /// Конкуренция: писатель гонит 1 МиБ случайными порциями сквозь буфер
    /// меньшего размера, читатель вычитывает случайными порциями — все байты
    /// на месте, в исходном порядке, без потерь и дублирования.
    #[test]
    fn concurrent_write_and_read_preserve_byte_sequence() {
        const TOTAL: usize = 1024 * 1024;
        let buffer = RingBuffer::new(4096, 1024);

        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = {
            let buffer = buffer.clone();
            thread::spawn(move || {
                let mut rng = Rng(42);
                let mut chunk = vec![0u8; 512];
                let mut sent = 0usize;
                let result = loop {
                    // Последняя порция обрезается до остатка: писатель
                    // отправляет ровно TOTAL байтов.
                    let len = ((rng.next() % 512 + 1) as usize).min(TOTAL - sent);
                    for byte in &mut chunk[..len] {
                        *byte = (sent % 251) as u8;
                        sent += 1;
                    }
                    if let Err(e) = buffer.write(&chunk[..len]) {
                        break Err(e);
                    }
                    if sent >= TOTAL {
                        buffer.close();
                        break Ok(());
                    }
                };
                writer_tx.send(()).unwrap();
                result
            })
        };

        let (reader_tx, reader_rx) = mpsc::channel();
        let reader = {
            let buffer = buffer.clone();
            thread::spawn(move || {
                let mut rng = Rng(7);
                let mut out = vec![0u8; 512];
                let mut got = 0usize;
                loop {
                    let want = (rng.next() % 512 + 1) as usize;
                    match buffer.read(&mut out[..want], Duration::from_secs(30)) {
                        Ok(n) => {
                            for &byte in &out[..n] {
                                if byte != (got % 251) as u8 {
                                    reader_tx.send(Err(got)).unwrap();
                                    return;
                                }
                                got += 1;
                            }
                        }
                        Err(ReadError::Closed) => {
                            reader_tx.send(Ok(got)).unwrap();
                            return;
                        }
                        Err(ReadError::Timeout) => {
                            reader_tx.send(Err(usize::MAX)).unwrap();
                            return;
                        }
                    }
                }
            })
        };

        let bytes_read = reader_rx
            .recv_timeout(Duration::from_secs(60))
            .expect("читатель уложился в минуту");
        writer_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("писатель завершился");
        assert_eq!(
            writer.join().expect("поток писателя жив").unwrap(),
            (),
            "писатель дописал всё без close-ошибки"
        );
        match bytes_read {
            Ok(got) => assert_eq!(got, TOTAL, "все байты доставлены"),
            Err(usize::MAX) => panic!("читатель упёрся в таймаут"),
            Err(at) => panic!("порядок нарушен на байте #{at}"),
        }
        reader.join().expect("поток читателя жив");
    }
}

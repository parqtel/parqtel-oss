//! TODO

use super::common::{record_block_written, INDEX_DURABILITY_TIMEOUT};
use crate::writer::{BlockMetadata, LogWriter, TraceWriter};
use parqtel_core::storage::PendingIndex;
use parqtel_core::wal::{WalPosition, WalWriter};
use parqtel_core::MemoryBuffer;
use parqtel_core::{
    BlockConfig, ContentionMetrics, LogBlockConfig, LogRecord, Result, SignalType, Span,
};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex};

/// The writer half of a signal that is rotated one block at a time (logs and
/// traces; metrics have their own sharded rotator).
#[doc(hidden)]
pub trait RowWriter: Send + 'static {
    type Row: serde::Serialize + Send + Sync + 'static;
    type Config: Clone + Send + Sync + 'static;
    const SIGNAL: SignalType;
    const NAME: &'static str;

    fn create(config: Self::Config) -> Self;
    fn push_row(&mut self, row: Self::Row) -> Result<Option<BlockMetadata>>;
    fn rows(&self) -> usize;
    fn write_block(&mut self) -> Result<BlockMetadata>;
    /// The WAL span `(from, to]` the next block covers.
    fn set_wal_range(&mut self, range: Option<(u64, u64)>);
    fn max_rows(config: &Self::Config) -> usize;
    fn block_duration(config: &Self::Config) -> Duration;
    /// Adds rows to the memory buffer, in writer order.
    fn feed_buffer<'a>(
        buffer: &'a MemoryBuffer,
        rows: &'a [Self::Row],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
    /// Removes the oldest `n` rows from the memory buffer.
    fn drop_flushed<'a>(
        buffer: &'a MemoryBuffer,
        n: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

impl RowWriter for LogWriter {
    type Row = LogRecord;
    type Config = LogBlockConfig;
    const SIGNAL: SignalType = SignalType::Logs;
    const NAME: &'static str = "log";

    fn create(config: Self::Config) -> Self {
        LogWriter::new(config)
    }
    fn push_row(&mut self, row: Self::Row) -> Result<Option<BlockMetadata>> {
        self.push(row)
    }
    fn rows(&self) -> usize {
        self.len()
    }
    fn write_block(&mut self) -> Result<BlockMetadata> {
        self.flush()
    }
    fn set_wal_range(&mut self, range: Option<(u64, u64)>) {
        LogWriter::set_wal_range(self, range)
    }
    fn max_rows(config: &Self::Config) -> usize {
        config.max_rows_per_block
    }
    fn block_duration(config: &Self::Config) -> Duration {
        Duration::from_secs(config.block_duration_secs)
    }
    fn feed_buffer<'a>(
        buffer: &'a MemoryBuffer,
        rows: &'a [Self::Row],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.push_logs(rows))
    }
    fn drop_flushed<'a>(
        buffer: &'a MemoryBuffer,
        n: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.drop_flushed_logs(n))
    }
}

impl RowWriter for TraceWriter {
    type Row = Span;
    type Config = BlockConfig;
    const SIGNAL: SignalType = SignalType::Traces;
    const NAME: &'static str = "trace";

    fn create(config: Self::Config) -> Self {
        TraceWriter::new(config)
    }
    fn push_row(&mut self, row: Self::Row) -> Result<Option<BlockMetadata>> {
        self.push(row)
    }
    fn rows(&self) -> usize {
        self.len()
    }
    fn write_block(&mut self) -> Result<BlockMetadata> {
        self.flush()
    }
    fn set_wal_range(&mut self, range: Option<(u64, u64)>) {
        TraceWriter::set_wal_range(self, range)
    }
    fn max_rows(config: &Self::Config) -> usize {
        config.max_rows_per_block
    }
    fn block_duration(config: &Self::Config) -> Duration {
        Duration::from_secs(config.block_duration_secs)
    }
    fn feed_buffer<'a>(
        buffer: &'a MemoryBuffer,
        rows: &'a [Self::Row],
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.push_spans(rows))
    }
    fn drop_flushed<'a>(
        buffer: &'a MemoryBuffer,
        n: usize,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(buffer.drop_flushed_spans(n))
    }
}

/// Rotates the blocks of one signal: log or trace rows accumulate in a writer,
/// and a full or old writer is swapped out and encoded in the background.
///
/// # Locking
///
/// The rotator is used behind one mutex, and that lock covers only the ordered
/// part of an ingest: the WAL append (one `write` for the whole request), the
/// memory-buffer push and the writer push. Encoding a block - the slow part - runs
/// on a detached task after the writer has been swapped out, so requests keep
/// flowing while it does. At most one encode is in flight: starting another
/// waits for the gate, which also makes the buffer drop exact (below) and bounds
/// memory when ingest outruns the disk.
///
/// # Buffer
///
/// The buffer is fed under the same lock as the writer, so both hold the rows in
/// the same order. When a block is durable the buffer drops its oldest
/// `row_count` rows - precisely that block's - and rows that arrived during the
/// encode stay queryable.
pub struct SerialRotator<W: RowWriter> {
    writer: W,
    config: W::Config,
    last_flush: Instant,
    max_duration: Duration,
    metadata_tx: mpsc::UnboundedSender<PendingIndex>,
    /// WAL for this signal, when enabled. Appended to under the rotator lock, so
    /// WAL order and writer order are the same total order - the property that
    /// makes the committed position correct.
    wal: Option<Arc<Mutex<WalWriter>>>,
    /// Highest WAL position among the rows currently in `writer`.
    covered: WalPosition,
    /// Where the previous flush's WAL span ended; see `BlockRotator::wal_cursor`.
    wal_cursor: WalPosition,
    /// Held by the one flush task in flight.
    gate: Arc<Mutex<()>>,
    buffer: Option<MemoryBuffer>,
}

/// Handles automatic rotation and flushing of log blocks.
pub type LogRotator = SerialRotator<LogWriter>;
/// Handles automatic rotation and flushing of trace blocks.
pub type TraceRotator = SerialRotator<TraceWriter>;

impl<W: RowWriter> SerialRotator<W> {
    pub fn new(config: W::Config, metadata_tx: mpsc::UnboundedSender<PendingIndex>) -> Self {
        let max_duration = W::block_duration(&config);
        Self {
            writer: W::create(config.clone()),
            config,
            last_flush: Instant::now(),
            max_duration,
            metadata_tx,
            wal: None,
            covered: WalPosition::START,
            wal_cursor: WalPosition::START,
            gate: Arc::new(Mutex::new(())),
            buffer: None,
        }
    }

    /// Attaches a WAL. After this, every accepted row is logged before it
    /// reaches the writer.
    pub fn with_wal(mut self, wal: WalWriter) -> Self {
        self.wal = Some(Arc::new(Mutex::new(wal)));
        self
    }

    /// Gives the rotator the memory buffer it keeps in step with its writer.
    pub fn set_buffer(&mut self, buffer: Option<MemoryBuffer>) {
        self.buffer = buffer;
    }

    /// Whether rows are logged to a WAL.
    pub fn has_wal(&self) -> bool {
        self.wal.is_some()
    }

    /// Bytes this signal's WAL currently occupies.
    pub async fn wal_bytes(&self) -> u64 {
        match &self.wal {
            Some(w) => w.lock().await.size_bytes(),
            None => 0,
        }
    }

    pub(super) fn publish(&self, meta: BlockMetadata, contention: Option<&ContentionMetrics>) {
        record_block_written(&meta, contention, W::SIGNAL);
        let _ = self.metadata_tx.send(PendingIndex::untracked(meta));
    }

    /// Pushes one row, closing the block first when it is full.
    ///
    /// Returns `true` if a block was closed.
    pub async fn push(
        &mut self,
        row: W::Row,
        contention: Option<&Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        let wal_record = match self.wal {
            Some(_) => Some(parqtel_core::wal::encode_record(&row)?),
            None => None,
        };
        self.push_batch(vec![row], wal_record, contention).await
    }

    /// Pushes a request's rows.
    ///
    /// `wal_records` holds the rows already framed with
    /// [`parqtel_core::wal::encode_record_into`], one record per row, in order -
    /// encoded by the caller before taking the rotator lock. Everything else here
    /// is the cheap, ordered part.
    ///
    /// Returns `true` if a block was closed.
    pub async fn push_batch(
        &mut self,
        rows: Vec<W::Row>,
        wal_records: Option<Vec<u8>>,
        contention: Option<&Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        let max_rows = W::max_rows(&self.config).max(1);
        let mut flushed = false;

        // A batch that would overflow the open block closes it first, so the
        // batch is not split across blocks.
        if self.writer.rows() > 0 && self.writer.rows() + rows.len() > max_rows {
            self.start_flush(contention).await;
            flushed = true;
        }
        // A batch bigger than a whole block is split by the writer itself, which
        // writes the full blocks inline. Let any background encode finish first so
        // the buffer's oldest rows are those blocks' rows.
        if rows.len() >= max_rows {
            drop(self.gate.lock().await);
        }

        // Logged first, and failures propagate: acknowledging a row we could not
        // log would make the WAL a lie.
        if let (Some(wal), Some(records)) = (&self.wal, &wal_records) {
            self.covered = wal.lock().await.append_encoded(records)?;
        }
        if let Some(buffer) = &self.buffer {
            W::feed_buffer(buffer, &rows).await;
        }
        for row in rows {
            if let Some(meta) = self.writer.push_row(row)? {
                // The writer closed a full block by itself. Those are the oldest
                // rows in the buffer too.
                if let Some(buffer) = &self.buffer {
                    W::drop_flushed(buffer, meta.row_count).await;
                }
                self.publish(meta, contention.map(|c| c.as_ref()));
                flushed = true;
            }
        }
        Ok(flushed)
    }

    /// Starts a flush when the block duration has elapsed.
    pub async fn check_and_flush(
        &mut self,
        contention: Option<&Arc<ContentionMetrics>>,
    ) -> Result<bool> {
        if Instant::now().duration_since(self.last_flush) >= self.max_duration {
            self.start_flush(contention).await;
            return Ok(true);
        }
        Ok(false)
    }

    /// Waits for the flush in flight, if any, without starting another.
    pub async fn wait_idle(&self) {
        drop(self.gate.lock().await);
    }

    /// Writes the open block and waits until it is done.
    pub async fn flush(&mut self, contention: Option<&Arc<ContentionMetrics>>) -> Result<()> {
        self.start_flush(contention).await;
        // The flush task holds the gate until the block is durable and the WAL
        // committed; taking it here waits for exactly that.
        drop(self.gate.lock().await);
        Ok(())
    }

    /// Swaps the writer out and encodes it on a detached task.
    ///
    /// Waits first for any earlier flush, holding the rotator lock meanwhile:
    /// that is back-pressure for a disk that cannot keep up, not a stall in the
    /// normal case.
    pub(super) async fn start_flush(&mut self, contention: Option<&Arc<ContentionMetrics>>) {
        if self.writer.rows() == 0 {
            tracing::debug!("{} block flush skipped: buffer empty", W::NAME);
            return;
        }
        let permit = Arc::clone(&self.gate).lock_owned().await;
        let rows = self.writer.rows();
        // Snapshot before the writer is taken: the block will contain exactly the
        // rows whose WAL positions are at or below this.
        let covered = std::mem::replace(&mut self.covered, WalPosition::START);
        let mut writer = std::mem::replace(&mut self.writer, W::create(self.config.clone()));
        if self.wal.is_some() {
            writer.set_wal_range(Some((self.wal_cursor.0, covered.0)));
            self.wal_cursor = covered;
        }
        self.last_flush = Instant::now();

        let metadata_tx = self.metadata_tx.clone();
        let wal = self.wal.clone();
        let buffer = self.buffer.clone();
        let contention = contention.cloned();
        tokio::spawn(async move {
            let _permit = permit;
            serial_flush::<W>(writer, covered, rows, metadata_tx, wal, buffer, contention).await;
        });
    }
}

/// Encodes one block, publishes it, commits its WAL range and then removes its
/// rows from the memory buffer - in that order. See [`SerialRotator`].
async fn serial_flush<W: RowWriter>(
    mut writer: W,
    covered: WalPosition,
    row_count: usize,
    metadata_tx: mpsc::UnboundedSender<PendingIndex>,
    wal: Option<Arc<Mutex<WalWriter>>>,
    buffer: Option<MemoryBuffer>,
    contention: Option<Arc<ContentionMetrics>>,
) {
    let started = std::time::Instant::now();
    let flush_guard = contention.as_ref().map(|c| c.flush_started(W::SIGNAL));
    let encoded = tokio::task::spawn_blocking(move || writer.write_block()).await;
    let metadata = match encoded {
        Ok(Ok(meta)) => meta,
        Ok(Err(e)) => {
            // The rows are still in the WAL, so this is recoverable by a replay.
            tracing::error!(rows = row_count, "{} block flush failed: {e}", W::NAME);
            return drop_from_buffer::<W>(&buffer, row_count).await;
        }
        Err(e) => {
            tracing::error!(
                rows = row_count,
                "{} block flush task panicked: {e}",
                W::NAME
            );
            return drop_from_buffer::<W>(&buffer, row_count).await;
        }
    };
    if let Some(guard) = flush_guard {
        guard.count_rows(metadata.row_count as u64);
    }

    // Sidecar durable before the WAL moves; see the metrics flush for why.
    let (pending, durable) = match wal.is_some() {
        true => {
            let (p, r) = PendingIndex::tracked(metadata);
            (p, Some(r))
        }
        false => (PendingIndex::untracked(metadata), None),
    };
    let _ = metadata_tx.send(pending);
    if let Some(durable) = durable {
        // Bounded, and on timeout the WAL stays uncommitted so replay recovers
        // the rows.
        match tokio::time::timeout(INDEX_DURABILITY_TIMEOUT, durable).await {
            Ok(Ok(())) => {
                if let Some(wal) = &wal {
                    if let Err(e) = wal.lock().await.commit(covered) {
                        tracing::error!(
                            "{} WAL commit failed; the rows will be replayed: {e}",
                            W::NAME
                        );
                    }
                }
            }
            Ok(Err(_)) => tracing::warn!(
                "{} index task dropped before the sidecar was persisted; leaving the WAL uncommitted",
                W::NAME
            ),
            Err(_) => tracing::error!(
                "timed out waiting for the {} index to become durable; leaving the WAL uncommitted",
                W::NAME
            ),
        }
    }
    drop_from_buffer::<W>(&buffer, row_count).await;
    tracing::debug!(
        signal = W::NAME,
        rows = row_count,
        duration_ms = started.elapsed().as_millis(),
        "block flushed to parquet"
    );
}

async fn drop_from_buffer<W: RowWriter>(buffer: &Option<MemoryBuffer>, rows: usize) {
    if let Some(buffer) = buffer {
        W::drop_flushed(buffer, rows).await;
    }
}

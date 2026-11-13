//! Write-ahead log for telemetry blocks.
//!
//! # Why
//!
//! A block is only durable once its Parquet file has been written and
//! renamed. Everything accepted into the memory buffer since the last flush is
//! lost on a crash, and with a duration-based block window that window is
//! measured in hours. The WAL narrows it to the sync interval.
//!
//! # Ordering, and why it is this way
//!
//! A block flush and the WAL must agree, or a crash either duplicates or loses
//! data. The sequence is:
//!
//! 1. rows are appended to the WAL, then the request is acknowledged;
//! 2. a flush takes rows, snapshotting the WAL position it is about to cover;
//! 3. the block is written and renamed;
//! 4. the **commit file** is advanced to the snapshotted position;
//! 5. segments entirely below the commit are deleted.
//!
//! The commit file is the single source of truth, and it is advanced *after*
//! the rename, so every crash point is safe:
//!
//! | crash after | result |
//! |---|---|
//! | (1) append | replayed, nothing on disk yet — correct |
//! | (2) snapshot | replayed, no block written — correct |
//! | (3) rename, before (4) | block exists but is not committed, so it is *not* in the index; WAL still holds the rows and replay writes them again. The orphan is invisible, so there is one copy |
//! | (4) commit, before (5) | rows are covered; replay skips everything at or below the commit, so the leftover segments are discarded rather than duplicated |
//!
//! Advancing the commit *before* the rename would instead be able to lose a
//! block, which is the one failure a WAL exists to prevent.
//!
//! # Positions
//!
//! A position is `(segment_sequence << 32) | (byte_offset + 1)`, so it is
//! monotonic across segments and comparable without reading them. Segments are
//! named `%020d.wal`, which also sorts lexically in sequence order. The `+ 1`
//! keeps the first record's position distinct from `START`.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Bumped if the on-disk framing ever changes.
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Bytes of framing per record: `u32` length + `u32` CRC.
const RECORD_HEADER_LEN: u64 = 8;

/// Default size at which the WAL rolls to a new segment.
///
/// A segment is deleted once the commit point passes it, so the on-disk
/// working set is bounded at roughly one segment plus whatever has not been
/// committed yet. Too small and a busy ingest churns segments; too large and
/// deletion lags.
pub const DEFAULT_MAX_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;

/// How durably a WAL append is persisted.
///
/// The default is [`Self::Interval`], not the fastest option: a default that
/// quietly means "do not sync" is a footgun in a durability feature, and the
/// cost of `Interval` is one `fsync` per interval rather than one per batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WalSyncMode {
    /// `write` only, no `fsync`. Survives a **process** crash, because every
    /// append is flushed to the OS and the page cache outlives the process; a
    /// machine loss can still lose whatever the kernel had not written.
    None,
    /// `fsync` every `sync_interval_ms`. The default.
    #[default]
    Interval,
    /// `fsync` every append. Slowest, and for a 100k points/sec ingest it is
    /// one syscall per batch.
    Always,
}

/// A monotonic WAL position: `(segment << 32) | offset`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct WalPosition(pub u64);

impl WalPosition {
    /// Start of the log: before any record.
    pub const START: WalPosition = WalPosition(0);

    fn segment(self) -> u64 {
        self.0 >> 32
    }

    fn offset(self) -> u64 {
        self.0 & 0xffff_ffff
    }

    /// True when a record at this position is already covered by a block.
    pub fn is_covered_by(self, commit: WalPosition) -> bool {
        self <= commit
    }
}

impl std::fmt::Display for WalPosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.segment(), self.offset())
    }
}

/// What a replay found.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReplayStats {
    /// Records handed to the caller.
    pub records: u64,
    /// Records skipped because they were at or below the commit point.
    pub skipped: u64,
    /// Bytes discarded from the tail of the last segment, which is what a
    /// crash mid-append leaves behind.
    pub truncated_bytes: u64,
    /// Segments removed after a successful replay.
    pub segments_removed: usize,
}

/// Directory holding one signal's WAL.
pub fn signal_dir(data_dir: &Path, signal: &str) -> PathBuf {
    data_dir.join("wal").join(signal)
}

/// Appends records to a signal's WAL and advances its commit point.
#[derive(Debug)]
pub struct WalWriter {
    dir: PathBuf,
    segment: u64,
    file: BufWriter<File>,
    offset: u64,
    sync_mode: WalSyncMode,
    sync_interval: std::time::Duration,
    max_segment_bytes: u64,
    last_sync: std::time::Instant,
    /// Set when an append fails, so the writer stops accepting records rather
    /// than acknowledging data it could not log.
    poisoned: Option<String>,
}

impl WalWriter {
    /// Opens (or creates) the WAL for `signal` under `data_dir`.
    pub fn open(
        data_dir: &Path,
        signal: &str,
        sync_mode: WalSyncMode,
        sync_interval: std::time::Duration,
    ) -> Result<Self> {
        Self::open_with_segment_limit(
            data_dir,
            signal,
            sync_mode,
            sync_interval,
            DEFAULT_MAX_SEGMENT_BYTES,
        )
    }

    /// As [`Self::open`], with an explicit segment size limit.
    pub fn open_with_segment_limit(
        data_dir: &Path,
        signal: &str,
        sync_mode: WalSyncMode,
        sync_interval: std::time::Duration,
        max_segment_bytes: u64,
    ) -> Result<Self> {
        let dir = signal_dir(data_dir, signal);
        fs::create_dir_all(&dir)?;
        // Reuse the newest segment rather than always rolling: a restart must
        // not leave an unbounded trail of tiny segments, one per process start.
        let segment = match segments(&dir)?.last().copied() {
            Some(last) => {
                let size = std::fs::metadata(segment_path(&dir, last).unwrap_or_default())
                    .map(|m| m.len())
                    .unwrap_or(0);
                if size >= max_segment_bytes.max(1) {
                    last + 1
                } else {
                    last
                }
            }
            None => 0,
        };
        let (file, offset) = open_or_create_segment(&dir, segment)?;
        Ok(Self {
            dir,
            segment,
            file: BufWriter::with_capacity(64 * 1024, file),
            offset,
            sync_mode,
            sync_interval,
            max_segment_bytes: max_segment_bytes.max(1),
            last_sync: std::time::Instant::now(),
            poisoned: None,
        })
    }

    /// Highest position written so far.
    ///
    /// Positions are **1-based**: the first record of the first segment is 1,
    /// not 0. `WalPosition::START` is 0 and means "nothing committed", so a
    /// 0-based position would make the very first record look already
    /// committed and silently drop it on replay.
    pub fn position(&self) -> WalPosition {
        WalPosition((self.segment << 32) | (self.offset + 1))
    }

    /// Appends one record, returning the position that covers it.
    ///
    /// `T` is serialised as a single JSON value. A request's decoded batch is
    /// therefore one record, which keeps the per-record overhead off the hot
    /// path.
    pub fn append<T: Serialize>(&mut self, value: &T) -> Result<WalPosition> {
        if let Some(err) = &self.poisoned {
            return Err(Error::Internal(format!("WAL writer is poisoned: {err}")));
        }
        let record = match encode_record(value) {
            Ok(r) => r,
            Err(e) => {
                // Serialisation failure must not be retried on every request.
                self.poisoned = Some(e.to_string());
                return Err(e);
            }
        };
        self.append_encoded(&record)
    }

    /// Appends records that [`encode_record`] already framed, with one write.
    ///
    /// Encoding is the CPU-heavy half of an append; doing it before the caller
    /// takes the lock that orders WAL and writer leaves only a copy and a
    /// `write` inside it. `records` may hold several back-to-back records, in
    /// which case the returned position covers the last of them and a request of
    /// N items costs one syscall rather than N.
    pub fn append_encoded(&mut self, records: &[u8]) -> Result<WalPosition> {
        if let Some(err) = &self.poisoned {
            return Err(Error::Internal(format!("WAL writer is poisoned: {err}")));
        }
        if let Err(e) = self.file.write_all(records) {
            let msg = e.to_string();
            self.poisoned = Some(msg.clone());
            return Err(Error::Io(std::io::Error::other(msg)));
        }
        self.offset += records.len() as u64;
        // Flush to the OS on **every** append. The `BufWriter` holds records in
        // userspace, so without this a process kill loses everything still
        // buffered - which is exactly the case the WAL exists for. `write` is
        // one syscall per batch and survives a process crash, because the page
        // cache outlives the process; only a machine loss needs `fsync`, which
        // `WalSyncMode` governs.
        self.file.flush()?;
        let pos = self.position();
        // Roll before the next append so a record is never split across files.
        if self.offset >= self.max_segment_bytes {
            self.roll()?;
        }
        self.maybe_sync()?;
        Ok(pos)
    }

    /// Closes the current segment and starts the next one.
    ///
    /// The commit point is untouched: records already written stay covered by
    /// whatever the last flush committed, and the new segment starts empty and
    /// therefore uncovered.
    fn roll(&mut self) -> Result<()> {
        self.sync()?;
        let next = self.segment + 1;
        let (file, offset) = open_or_create_segment(&self.dir, next)?;
        self.file = BufWriter::with_capacity(64 * 1024, file);
        self.segment = next;
        self.offset = offset;
        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<()> {
        let should = match self.sync_mode {
            WalSyncMode::None => false,
            WalSyncMode::Always => true,
            WalSyncMode::Interval => self.last_sync.elapsed() >= self.sync_interval,
        };
        if !should {
            return Ok(());
        }
        self.sync()?;
        Ok(())
    }

    /// Flushes and `fsync`s the current segment.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file
            .get_ref()
            .sync_data()
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        self.last_sync = std::time::Instant::now();
        Ok(())
    }

    /// Advances the commit point and removes segments it fully covers.
    ///
    /// Must only be called *after* the block covering `position` has been
    /// renamed. See the module docs for why the order matters.
    pub fn commit(&mut self, position: WalPosition) -> Result<()> {
        if position == WalPosition::START {
            return Ok(());
        }
        write_commit(&self.dir, position)?;
        self.remove_covered_segments(position)?;
        Ok(())
    }

    fn remove_covered_segments(&self, commit: WalPosition) -> Result<()> {
        for seq in segments(&self.dir)? {
            // Never delete the segment still being appended to: its tail holds
            // records the commit does not cover.
            if seq >= commit.segment() {
                break;
            }
            if let Some(p) = segment_path(&self.dir, seq) {
                let _ = fs::remove_file(p);
            }
        }
        Ok(())
    }

    /// Bytes currently held by this signal's WAL, for the stats endpoint.
    pub fn size_bytes(&self) -> u64 {
        wal_size_of(&self.dir)
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Best effort: a clean shutdown should not lose the tail.
        let _ = self.file.flush();
    }
}

/// Reads a signal's WAL and hands each record to a callback.
///
/// Stops at the first record that is truncated or fails its CRC — that is what
/// a crash mid-append leaves behind — and truncates the segment there so the
/// next append starts from a clean boundary.
pub fn replay<T, F>(
    data_dir: &Path,
    signal: &str,
    commit: WalPosition,
    mut on_record: F,
) -> Result<ReplayStats>
where
    T: for<'de> Deserialize<'de>,
    F: FnMut(T),
{
    let dir = signal_dir(data_dir, signal);
    if !dir.exists() {
        return Ok(ReplayStats::default());
    }
    let mut stats = ReplayStats::default();

    for seq in segments(&dir)? {
        let Some(path) = segment_path(&dir, seq) else {
            continue;
        };
        let mut bytes = Vec::new();
        File::open(&path)?.read_to_end(&mut bytes)?;

        let mut offset = 0usize;
        let mut bad_at: Option<usize> = None;
        while offset + RECORD_HEADER_LEN as usize <= bytes.len() {
            let len = u32::from_le_bytes(
                bytes[offset..offset + 4]
                    .try_into()
                    .map_err(|_| Error::Internal("bad WAL header".into()))?,
            ) as usize;
            let want_crc = u32::from_le_bytes(
                bytes[offset + 4..offset + 8]
                    .try_into()
                    .map_err(|_| Error::Internal("bad WAL header".into()))?,
            );
            let body_start = offset + RECORD_HEADER_LEN as usize;
            let Some(body_end) = body_start.checked_add(len) else {
                bad_at = Some(offset);
                break;
            };
            if body_end > bytes.len() {
                // Torn tail from a crash mid-append.
                bad_at = Some(offset);
                break;
            }
            let payload = &bytes[body_start..body_end];
            if crc32(payload) != want_crc {
                bad_at = Some(offset);
                break;
            }
            // The position `WalWriter::append` returned for this record: the
            // END of the record, 1-based. Labelling by the start would give the
            // record after a committed one the same number as the commit point,
            // and it would be skipped.
            let pos = WalPosition((seq << 32) | (body_end as u64 + 1));
            if pos.is_covered_by(commit) {
                stats.skipped += 1;
            } else {
                match serde_json::from_slice::<T>(payload) {
                    Ok(v) => {
                        on_record(v);
                        stats.records += 1;
                    }
                    Err(e) => {
                        // A record we cannot decode is not recoverable, but
                        // stopping here would silently drop every later record,
                        // so log and continue.
                        tracing::warn!(position = %pos, error = %e, "skipping undecodable WAL record");
                        stats.skipped += 1;
                    }
                }
            }
            offset = body_end;
        }

        if let Some(at) = bad_at {
            // Drop the unusable tail so the next append has a clean boundary.
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(at as u64)?;
            file.sync_all()?;
            stats.truncated_bytes += (bytes.len() - at) as u64;
        }
    }
    Ok(stats)
}

/// Advances the commit point without holding a writer open.
pub fn commit_position(data_dir: &Path, signal: &str, position: WalPosition) -> Result<()> {
    write_commit(&signal_dir(data_dir, signal), position)
}

/// The current commit point for a signal.
pub fn read_commit(data_dir: &Path, signal: &str) -> WalPosition {
    read_commit_inner(&signal_dir(data_dir, signal)).unwrap_or(WalPosition::START)
}

/// Deletes a signal's WAL. Called after a replay has been re-flushed.
pub fn discard_covered(data_dir: &Path, signal: &str, through: WalPosition) -> Result<usize> {
    let dir = signal_dir(data_dir, signal);
    if !dir.exists() {
        return Ok(0);
    }
    let mut removed = 0;
    for seq in segments(&dir)? {
        if seq > through.segment() {
            break;
        }
        if let Some(p) = segment_path(&dir, seq) {
            if fs::remove_file(p).is_ok() {
                removed += 1;
            }
        }
    }
    Ok(removed)
}

/// Bytes the WAL occupies on disk, summed across signals.
pub fn wal_size(data_dir: &Path) -> u64 {
    let Ok(signals) = fs::read_dir(data_dir.join("wal")) else {
        return 0;
    };
    signals.flatten().map(|e| wal_size_of(&e.path())).sum()
}

fn wal_size_of(dir: &Path) -> u64 {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    entries
        .flatten()
        .filter(|e| e.path().extension().is_some_and(|x| x == "wal"))
        .filter_map(|e| e.metadata().ok().map(|m| m.len()))
        .sum()
}

// --- segment bookkeeping ---------------------------------------------------

fn segments(dir: &Path) -> Result<Vec<u64>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if let Some(seq) = entry
            .file_name()
            .to_str()
            .and_then(|n| n.strip_suffix(".wal"))
            .and_then(|n| n.parse::<u64>().ok())
        {
            out.push(seq);
        }
    }
    out.sort_unstable();
    Ok(out)
}

fn segment_path(dir: &Path, seq: u64) -> Option<PathBuf> {
    let p = dir.join(format!("{seq:020}.wal"));
    p.exists().then_some(p)
}

/// Opens the newest segment for appending, or creates it.
///
/// A pre-existing final segment is reused rather than rolled: rolling on every
/// restart would leave an unbounded tail of tiny segments, since a fresh
/// process always starts at offset 0 of a new file.
fn open_or_create_segment(dir: &Path, segment: u64) -> Result<(File, u64)> {
    let path = dir.join(format!("{segment:020}.wal"));
    let existed = path.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .read(true)
        .open(&path)?;
    // An existing segment may end mid-record after a crash; rewind to the last
    // intact record boundary so appends stay parseable.
    let offset = if existed {
        last_complete_offset(&mut file)?
    } else {
        0
    };
    file.set_len(offset)?;
    Ok((file, offset))
}

/// Offset just past the last intact record, truncating any torn tail.
fn last_complete_offset(file: &mut File) -> Result<u64> {
    let mut bytes = Vec::new();
    file.seek(SeekFrom::Start(0))?;
    file.read_to_end(&mut bytes)?;
    let mut offset = 0usize;
    while offset + RECORD_HEADER_LEN as usize <= bytes.len() {
        let len = u32::from_le_bytes(
            bytes[offset..offset + 4]
                .try_into()
                .map_err(|_| Error::Internal("bad WAL header".into()))?,
        ) as usize;
        let body = offset + RECORD_HEADER_LEN as usize;
        let Some(end) = body.checked_add(len) else {
            break;
        };
        if end > bytes.len() {
            break;
        }
        offset = end;
    }
    Ok(offset as u64)
}

// --- commit point ----------------------------------------------------------

fn commit_path(dir: &Path) -> PathBuf {
    dir.join("commit")
}

fn write_commit(dir: &Path, position: WalPosition) -> Result<()> {
    fs::create_dir_all(dir)?;
    let tmp = dir.join("commit.tmp");
    // Write + rename so the commit point is never observed half-written.
    File::create(&tmp)?.write_all(position.0.to_string().as_bytes())?;
    fs::rename(&tmp, commit_path(dir))?;
    Ok(())
}

fn read_commit_inner(dir: &Path) -> Option<WalPosition> {
    let s = fs::read_to_string(commit_path(dir)).ok()?;
    Some(WalPosition(s.trim().parse().ok()?))
}

// --- Block coverage ----------------------------------------------------------

/// Parquet footer key under which a block records the WAL span it covers.
pub const WAL_RANGE_FOOTER_KEY: &str = "parqtel.wal_range";

/// Footer value for a block covering `(from, to]`.
pub fn encode_wal_range(range: (u64, u64)) -> String {
    format!("{}-{}", range.0, range.1)
}

/// Parses [`encode_wal_range`]'s output.
pub fn decode_wal_range(value: &str) -> Option<(u64, u64)> {
    let (from, to) = value.split_once('-')?;
    let (from, to) = (from.parse().ok()?, to.parse().ok()?);
    (from <= to).then_some((from, to))
}

/// How far the WAL is already durable in blocks, starting from `commit`.
///
/// A block covers `(from, to]`; the blocks of consecutive flushes chain, each
/// one's `from` being the previous one's `to`. Starting at the commit point this
/// follows the chain through every block that is on disk, so a crash between a
/// block becoming durable and its WAL commit does not replay the block's rows
/// into a second block.
///
/// It deliberately walks the chain instead of taking the largest `to`: with
/// several blocks encoding at once a later one can be durable while an earlier
/// one is not, and skipping everything below the later block's end would drop
/// the earlier block's rows. A gap stops the walk there.
pub fn covered_through(commit: WalPosition, ranges: &[(u64, u64)]) -> WalPosition {
    let mut at = commit.0;
    loop {
        let next = ranges
            .iter()
            .filter(|(from, to)| *from <= at && *to > at)
            .map(|(_, to)| *to)
            .max();
        match next {
            Some(to) => at = to,
            None => return WalPosition(at),
        }
    }
}

// --- Record framing --------------------------------------------------------

/// Frames `value` as one WAL record: `u32` length, `u32` CRC-32, JSON payload.
///
/// Pure CPU, no I/O and no state, so callers run it outside any lock and then
/// hand the bytes to [`WalWriter::append_encoded`]. Several encoded records may
/// be concatenated and appended together.
pub fn encode_record<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut record = Vec::with_capacity(256);
    encode_record_into(value, &mut record)?;
    Ok(record)
}

/// As [`encode_record`], appending to `out` so a batch shares one buffer.
pub fn encode_record_into<T: Serialize>(value: &T, out: &mut Vec<u8>) -> Result<()> {
    let start = out.len();
    out.extend_from_slice(&[0u8; RECORD_HEADER_LEN as usize]);
    serde_json::to_writer(&mut *out, value).map_err(Error::Serde)?;
    let payload_len = out.len() - start - RECORD_HEADER_LEN as usize;
    let crc = crc32(&out[start + RECORD_HEADER_LEN as usize..]);
    out[start..start + 4].copy_from_slice(&(payload_len as u32).to_le_bytes());
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
    Ok(())
}

// --- CRC32 -----------------------------------------------------------------

/// CRC-32 (IEEE 802.3), the same checksum every WAL record has always carried.
///
/// Computed with hardware CRC instructions where the CPU has them; the value
/// is identical to the bitwise reference, so existing segments stay readable.
fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

#[cfg(test)]
mod tests;

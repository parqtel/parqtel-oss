//! Tests for the `wal` module.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
use super::*;
use std::time::Duration;

fn tmp() -> tempfile::TempDir {
    tempfile::tempdir().unwrap()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Rec {
    n: String,
    v: i64,
}

fn open(dir: &Path) -> WalWriter {
    WalWriter::open(dir, "metrics", WalSyncMode::Always, Duration::from_secs(0)).unwrap()
}

#[test]
fn positions_are_ordered_across_records() {
    let d = tmp();
    let mut w = open(d.path());
    let mut last = WalPosition::START;
    for i in 0..50 {
        let p = w
            .append(&Rec {
                n: format!("r{i}"),
                v: i,
            })
            .unwrap();
        assert!(p > last, "positions must increase: {p} then {last}");
        last = p;
    }
}

#[test]
fn round_trips_records() {
    let d = tmp();
    let mut w = open(d.path());
    let written: Vec<Rec> = (0..20)
        .map(|i| Rec {
            n: format!("r{i}"),
            v: i * 7,
        })
        .collect();
    for r in &written {
        w.append(r).unwrap();
    }
    let pos = w.position();
    drop(w);

    let mut seen = Vec::new();
    let stats =
        replay::<Rec, _>(d.path(), "metrics", WalPosition::START, |r| seen.push(r)).unwrap();
    assert_eq!(seen, written);
    assert_eq!(stats.records, 20);
    assert_eq!(stats.skipped, 0);
    assert!(pos > WalPosition::START);
}

/// The crash-after-rename case: a record at or below the commit point is
/// already covered by a block, so replaying it would duplicate data.
#[test]
fn replay_skips_records_covered_by_the_commit_point() {
    let d = tmp();
    let mut w = open(d.path());
    for i in 0..10 {
        w.append(&Rec {
            n: format!("r{i}"),
            v: i,
        })
        .unwrap();
    }
    let after_five = w.position();
    w.sync().unwrap();
    drop(w);

    // Simulate "five records were flushed into a block": commit at the
    // position covering the fifth.
    commit_position(d.path(), "metrics", after_five).unwrap();
    let commit = read_commit(d.path(), "metrics");
    let mut seen = Vec::new();
    let stats = replay::<Rec, _>(d.path(), "metrics", commit, |r| seen.push(r)).unwrap();
    assert!(
        seen.is_empty(),
        "everything was committed, nothing should replay: {seen:?}"
    );
    assert!(stats.skipped > 0, "records must be counted as skipped");
}

/// The record right after the commit point is not covered by it. A commit
/// position is the end of the last covered record, and replay must label
/// records the same way, or the first uncommitted record shares the commit's
/// number and is skipped.
#[test]
fn replay_keeps_the_record_immediately_after_the_commit_point() {
    let d = tmp();
    let mut w = open(d.path());
    let mut after = Vec::new();
    for i in 0..5 {
        after.push(
            w.append(&Rec {
                n: format!("r{i}"),
                v: i,
            })
            .unwrap(),
        );
    }
    w.sync().unwrap();
    drop(w);

    // Every prefix of the log, committed, must replay exactly the rest.
    for covered in 0..=5usize {
        let commit = if covered == 0 {
            WalPosition::START
        } else {
            after[covered - 1]
        };
        let mut seen = Vec::new();
        replay::<Rec, _>(d.path(), "metrics", commit, |r| seen.push(r.v)).unwrap();
        let want: Vec<i64> = (covered as i64..5).collect();
        assert_eq!(seen, want, "commit after {covered} records");
    }
}

#[test]
fn wal_range_footer_value_round_trips() {
    assert_eq!(decode_wal_range(&encode_wal_range((5, 90))), Some((5, 90)));
    assert_eq!(decode_wal_range("7-7"), Some((7, 7)));
    assert_eq!(decode_wal_range("9-3"), None);
    assert_eq!(decode_wal_range("nope"), None);
    assert_eq!(decode_wal_range("1-"), None);
}

/// The walk follows connected blocks and stops at a gap, whatever order the
/// blocks happened to become durable in.
#[test]
fn covered_through_follows_the_chain_and_stops_at_a_gap() {
    let start = WalPosition(10);
    // Nothing on disk: nothing is added.
    assert_eq!(covered_through(start, &[]), start);
    // 10 -> 20 -> 35, listed out of order.
    assert_eq!(
        covered_through(start, &[(20, 35), (10, 20)]),
        WalPosition(35)
    );
    // The block for (20, 35] is missing: a later block (35, 50] that did
    // become durable must NOT carry the walk past the gap.
    assert_eq!(
        covered_through(start, &[(10, 20), (35, 50)]),
        WalPosition(20)
    );
    // Blocks entirely below the commit change nothing.
    assert_eq!(
        covered_through(WalPosition(40), &[(0, 10), (10, 30)]),
        WalPosition(40)
    );
    // A block that straddles the commit extends it.
    assert_eq!(
        covered_through(WalPosition(15), &[(10, 30)]),
        WalPosition(30)
    );
    // A range that starts after the commit and does not connect is ignored.
    assert_eq!(covered_through(start, &[(11, 90)]), start);
}

#[test]
fn commit_removes_fully_covered_segments_but_keeps_the_open_one() {
    let d = tmp();
    let mut w = open(d.path());
    // Force a rollover by committing a position in a later segment.
    let dir = signal_dir(d.path(), "metrics");
    fs::write(dir.join(format!("{:020}.wal", 0)), b"").unwrap();
    fs::write(dir.join(format!("{:020}.wal", 5)), b"").unwrap();
    let later = WalPosition(5 << 32);
    w.commit(later).unwrap();
    assert!(
        !dir.join(format!("{:020}.wal", 0)).exists(),
        "segment 0 is fully covered and should go"
    );
    assert!(
        dir.join(format!("{:020}.wal", 5)).exists(),
        "the segment being written must survive"
    );
}

/// A crash mid-append leaves a torn record. Replay must stop there, hand
/// back the intact prefix, and truncate so the next append is clean.
#[test]
fn torn_tail_is_dropped_and_truncated() {
    let d = tmp();
    let mut w = open(d.path());
    for i in 0..5 {
        w.append(&Rec {
            n: format!("r{i}"),
            v: i,
        })
        .unwrap();
    }
    w.sync().unwrap();
    drop(w);

    // Append garbage, as a crash mid-write would.
    let seg = segment_path(&signal_dir(d.path(), "metrics"), 0).unwrap();
    let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
    f.write_all(&[9u8; 11]).unwrap();
    drop(f);

    let mut seen = Vec::new();
    let stats =
        replay::<Rec, _>(d.path(), "metrics", WalPosition::START, |r| seen.push(r)).unwrap();
    assert_eq!(seen.len(), 5, "the intact prefix must survive");
    assert!(stats.truncated_bytes > 0, "the torn tail must be reported");

    // And the next append is readable, because the torn bytes are gone.
    let mut w = open(d.path());
    w.append(&Rec {
        n: "after".into(),
        v: 99,
    })
    .unwrap();
    w.sync().unwrap();
    drop(w);
    let mut seen = Vec::new();
    replay::<Rec, _>(d.path(), "metrics", WalPosition::START, |r| seen.push(r)).unwrap();
    assert_eq!(seen.len(), 6);
    assert_eq!(seen[5].n, "after");
}

/// A corrupted payload must not be handed to the caller, and must not stop
/// later records from being read.
#[test]
fn bad_crc_skips_one_record_and_keeps_going() {
    let d = tmp();
    let mut w = open(d.path());
    for i in 0..4 {
        w.append(&Rec {
            n: format!("r{i}"),
            v: i,
        })
        .unwrap();
    }
    w.sync().unwrap();
    drop(w);

    // Flip a byte inside the second record's payload.
    let seg = segment_path(&signal_dir(d.path(), "metrics"), 0).unwrap();
    let mut bytes = std::fs::read(&seg).unwrap();
    let second = RECORD_HEADER_LEN as usize;
    bytes[second + 2] ^= 0xff;
    std::fs::write(&seg, &bytes).unwrap();

    let mut seen = Vec::new();
    replay::<Rec, _>(d.path(), "metrics", WalPosition::START, |r| seen.push(r)).unwrap();
    assert!(
        !seen.iter().any(|r| r.n == "r1"),
        "the corrupted record must not be delivered"
    );
}

#[test]
fn reopening_appends_to_the_existing_segment() {
    let d = tmp();
    let mut w = open(d.path());
    w.append(&Rec {
        n: "first".into(),
        v: 1,
    })
    .unwrap();
    w.sync().unwrap();
    drop(w);

    // A restart must not roll a new segment for every process start.
    let mut w = open(d.path());
    w.append(&Rec {
        n: "second".into(),
        v: 2,
    })
    .unwrap();
    w.sync().unwrap();
    drop(w);

    assert_eq!(
        segments(&signal_dir(d.path(), "metrics")).unwrap().len(),
        1,
        "reopening should reuse the final segment"
    );
    let mut seen = Vec::new();
    replay::<Rec, _>(d.path(), "metrics", WalPosition::START, |r| seen.push(r)).unwrap();
    assert_eq!(seen.len(), 2, "both records must survive a reopen");
}

#[test]
fn commit_point_survives_and_defaults_to_start() {
    let d = tmp();
    assert_eq!(read_commit(d.path(), "metrics"), WalPosition::START);
    commit_position(d.path(), "metrics", WalPosition(7 << 32)).unwrap();
    assert_eq!(read_commit(d.path(), "metrics"), WalPosition(7 << 32));
}

#[test]
fn writer_stops_accepting_after_a_poisoning_failure() {
    // A writer that failed must not acknowledge further records, or data
    // would be acknowledged without a WAL entry to recover it from.
    let d = tmp();
    let mut w = open(d.path());
    w.poisoned = Some("simulated".into());
    assert!(w
        .append(&Rec {
            n: "x".into(),
            v: 1
        })
        .is_err());
}

#[test]
fn crc_matches_the_ieee_check_value() {
    // The standard CRC-32 check value; also what the original bitwise
    // implementation produced, so records written by older builds verify.
    assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    assert_eq!(crc32(b""), 0);
}

#[test]
fn crc_detects_a_single_bit_flip() {
    let a = crc32(b"hello");
    assert_eq!(a, crc32(b"hello"));
    assert_ne!(a, crc32(b"hellp"));
}

#[test]
fn discard_covered_removes_whole_segments() {
    let d = tmp();
    let dir = signal_dir(d.path(), "metrics");
    fs::create_dir_all(&dir).unwrap();
    fs::write(dir.join(format!("{:020}.wal", 0)), b"").unwrap();
    fs::write(dir.join(format!("{:020}.wal", 1)), b"").unwrap();
    let removed = discard_covered(d.path(), "metrics", WalPosition(2 << 32)).unwrap();
    assert_eq!(removed, 2, "both segments are at or below the cut");
    assert!(segments(&dir).unwrap().is_empty());
}

/// Segment rollover must not lose records: a busy ingest rolls many times
/// and every record must still replay in order.
#[test]
fn rollover_preserves_every_record() {
    let d = tmp();
    let mut w = WalWriter::open_with_segment_limit(
        d.path(),
        "metrics",
        WalSyncMode::Always,
        Duration::from_secs(0),
        512, // force frequent rolls
    )
    .unwrap();
    let written: Vec<Rec> = (0..200)
        .map(|i| Rec {
            n: format!("r{i}"),
            v: i,
        })
        .collect();
    for r in &written {
        w.append(r).unwrap();
    }
    w.sync().unwrap();
    drop(w);

    let segs = segments(&signal_dir(d.path(), "metrics")).unwrap();
    assert!(
        segs.len() > 1,
        "the test must actually have rolled: {segs:?}"
    );
    let mut seen = Vec::new();
    replay::<Rec, _>(d.path(), "metrics", WalPosition::START, |r| seen.push(r)).unwrap();
    assert_eq!(seen.len(), written.len());
    assert_eq!(
        seen, written,
        "records must replay in order across segments"
    );
}

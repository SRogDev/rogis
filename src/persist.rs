//! Persistence: binary snapshots + RESP append-only file.

use crate::resp::{self, Frame, RespError};
use crate::store::{SnapshotEntry, Store, Value};
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

/// Persistence configuration.
pub struct PersistCfg {
    /// Directory holding `dump.rogb` and `appendonly.rogb`.
    pub dir: PathBuf,
    /// Snapshot every N seconds when dirty; 0 disables periodic snapshots.
    pub save_secs: u64,
    /// Enable the append-only file.
    pub appendonly: bool,
}

/// Runtime control handed to the server.
pub struct PersistCtl {
    /// Snapshot every N seconds when dirty; 0 disables periodic snapshots.
    pub save_secs: u64,
}

/// Append-only log. All methods are no-ops when created via `disabled()`.
pub struct Aof {
    dir: PathBuf,
    file: Option<Mutex<BufWriter<File>>>,
    dirty: AtomicBool,
}

impl Aof {
    /// Open (or create) the AOF inside `cfg.dir` when `appendonly` is set.
    pub fn open(cfg: &PersistCfg) -> io::Result<Self> {
        let file = if cfg.appendonly {
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(cfg.dir.join("appendonly.rogb"))?;
            Some(Mutex::new(BufWriter::new(f)))
        } else {
            None
        };
        Ok(Self {
            dir: cfg.dir.clone(),
            file,
            dirty: AtomicBool::new(false),
        })
    }

    /// An AOF that logs nothing; used for tests and AOF replay.
    pub fn disabled() -> Self {
        Self {
            dir: PathBuf::new(),
            file: None,
            dirty: AtomicBool::new(false),
        }
    }

    /// Append a write command (RESP-encoded) to the log. No-op when disabled.
    /// Marks the store dirty so the snapshot scheduler picks it up.
    pub fn log(&self, argv: &[Vec<u8>]) {
        let Some(m) = self.file.as_ref() else {
            return;
        };
        let frame = Frame::Array(Some(
            argv.iter().map(|a| Frame::Bulk(Some(a.clone()))).collect(),
        ));
        let mut buf = Vec::new();
        resp::encode(&frame, &mut buf);
        let mut w = m.lock().unwrap_or_else(|e| e.into_inner());
        // Best-effort: a failed write is reported but must not crash the
        // server; the 1s fsync ticker surfaces persistent failures.
        if let Err(e) = w.write_all(&buf) {
            eprintln!("rogis: AOF write failed: {e}");
        } else {
            self.dirty.store(true, Ordering::Relaxed);
        }
    }

    /// Flush buffered data and fsync. Called by the server's ticker.
    pub fn sync(&self) -> io::Result<()> {
        if let Some(m) = self.file.as_ref() {
            let mut w = m.lock().unwrap_or_else(|e| e.into_inner());
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        Ok(())
    }

    /// Take-and-clear the dirty flag (snapshot scheduler).
    pub fn take_dirty(&self) -> bool {
        self.dirty.swap(false, Ordering::Relaxed)
    }

    /// Atomically snapshot the store and truncate the AOF.
    ///
    /// After this returns, `dump.rogb` holds the full dataset and the AOF
    /// holds only writes newer than the snapshot, so a restart that loads
    /// the snapshot and then replays the AOF cannot apply any write twice.
    /// The file lock is held across snapshot + truncate so no concurrent
    /// `log` can land in between.
    pub fn snapshot_reset(&self, store: &Store) -> io::Result<()> {
        let mut guard = self
            .file
            .as_ref()
            .map(|m| m.lock().unwrap_or_else(|e| e.into_inner()));
        if let Some(w) = guard.as_mut() {
            w.flush()?;
        }
        snapshot(store, &self.dir)?;
        if let Some(w) = guard.as_mut() {
            // O_APPEND is set, so later writes land at the new end (offset 0).
            w.get_ref().set_len(0)?;
        }
        self.dirty.store(false, Ordering::Relaxed);
        Ok(())
    }

    /// Directory snapshots are written to.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

// ---------------------------------------------------------------------------
// Snapshots
// ---------------------------------------------------------------------------

const MAGIC: &[u8; 4] = b"ROGB";
const VERSION: u32 = 1;

/// Write a binary snapshot of `store` to `dir/dump.rogb` (via temp + rename).
///
/// Format (all integers little-endian): magic `ROGB`, version u32 = 1,
/// entry count u64, then per entry: u32 key-len + key + u8 type tag
/// (0 = string, 1 = hash, 2 = list) + i64 expiry unix-ms (-1 = none) +
/// payload (string: u32 len + bytes; hash: u32 pair-count + pairs;
/// list: u32 elem-count + elems).
pub fn snapshot(store: &Store, dir: &Path) -> io::Result<()> {
    // An empty dir means "persistence not configured" (e.g. Aof::disabled()):
    // fail instead of writing dump files into the process working directory.
    if dir.as_os_str().is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "persistence directory not configured",
        ));
    }
    let entries = store.snapshot();
    let tmp = dir.join("dump.rogb.tmp");
    let dst = dir.join("dump.rogb");
    let mut w = BufWriter::new(File::create(&tmp)?);
    w.write_all(MAGIC)?;
    write_u32(&mut w, VERSION)?;
    w.write_all(&(entries.len() as u64).to_le_bytes())?;
    for e in &entries {
        write_u32(&mut w, e.key.len() as u32)?;
        w.write_all(&e.key)?;
        let tag: u8 = match e.value {
            Value::Str(_) => 0,
            Value::Hash(_) => 1,
            Value::List(_) => 2,
        };
        w.write_all(&[tag])?;
        w.write_all(
            &e.expires_at_ms
                .map(|t| t as i64)
                .unwrap_or(-1)
                .to_le_bytes(),
        )?;
        match &e.value {
            Value::Str(b) => {
                write_blob(&mut w, b)?;
            }
            Value::Hash(m) => {
                write_u32(&mut w, m.len() as u32)?;
                for (f, v) in m {
                    write_blob(&mut w, f)?;
                    write_blob(&mut w, v)?;
                }
            }
            Value::List(l) => {
                write_u32(&mut w, l.len() as u32)?;
                for item in l {
                    write_blob(&mut w, item)?;
                }
            }
        }
    }
    w.flush()?;
    w.get_ref().sync_all()?;
    drop(w);
    fs::rename(tmp, dst)?;
    Ok(())
}

fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_blob(w: &mut impl Write, b: &[u8]) -> io::Result<()> {
    write_u32(w, b.len() as u32)?;
    w.write_all(b)
}

/// Load `dump.rogb` (if present) then replay `appendonly.rogb` (if present).
pub fn load(store: &Store, dir: &Path) -> io::Result<()> {
    let dump = dir.join("dump.rogb");
    if dump.exists() {
        let entries = parse_snapshot(&fs::read(&dump)?)?;
        store.restore(entries);
    }
    let aof = dir.join("appendonly.rogb");
    if aof.exists() {
        replay_aof(store, &aof)?;
    }
    Ok(())
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "snapshot: offset overflow")
        })?;
        if end > self.data.len() {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "snapshot: truncated file",
            ));
        }
        let s = &self.data[self.pos..end];
        self.pos = end;
        Ok(s)
    }

    fn u32_le(&mut self) -> io::Result<u32> {
        let b: [u8; 4] = self.take(4)?.try_into().expect("take(4) is 4 bytes");
        Ok(u32::from_le_bytes(b))
    }

    fn u64_le(&mut self) -> io::Result<u64> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("take(8) is 8 bytes");
        Ok(u64::from_le_bytes(b))
    }

    fn i64_le(&mut self) -> io::Result<i64> {
        let b: [u8; 8] = self.take(8)?.try_into().expect("take(8) is 8 bytes");
        Ok(i64::from_le_bytes(b))
    }

    fn blob(&mut self) -> io::Result<Vec<u8>> {
        let n = self.u32_le()? as usize;
        Ok(self.take(n)?.to_vec())
    }
}

fn parse_snapshot(data: &[u8]) -> io::Result<Vec<SnapshotEntry>> {
    let mut c = Cursor { data, pos: 0 };
    if c.take(4)? != MAGIC {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot: bad magic",
        ));
    }
    if c.u32_le()? != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot: unsupported version",
        ));
    }
    let count = c.u64_le()?;
    let mut entries = Vec::new();
    for _ in 0..count {
        let key = c.blob()?;
        let tag = c.take(1)?[0];
        let expires_at_ms = match c.i64_le()? {
            -1 => None,
            t => Some(t as u64),
        };
        let value = match tag {
            0 => Value::Str(c.blob()?),
            1 => {
                let n = c.u32_le()?;
                let mut m = std::collections::HashMap::with_capacity(n as usize);
                for _ in 0..n {
                    let f = c.blob()?;
                    let v = c.blob()?;
                    m.insert(f, v);
                }
                Value::Hash(m)
            }
            2 => {
                let n = c.u32_le()?;
                let mut l = std::collections::VecDeque::with_capacity(n as usize);
                for _ in 0..n {
                    l.push_back(c.blob()?);
                }
                Value::List(l)
            }
            t => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("snapshot: unknown value tag {t}"),
                ))
            }
        };
        entries.push(SnapshotEntry {
            key,
            value,
            expires_at_ms,
        });
    }
    Ok(entries)
}

/// Replay an AOF through the normal command dispatch with a disabled AOF
/// (so replayed writes are not re-logged).
fn replay_aof(store: &Store, path: &Path) -> io::Result<()> {
    use crate::server::PubHub;
    let data = fs::read(path)?;
    let hub = PubHub::new();
    let aof = Aof::disabled();
    let mut pos = 0;
    while pos < data.len() {
        match resp::decode(&data[pos..]) {
            Ok((frame, n)) => {
                pos += n;
                if let Some(argv) = crate::cmd::frame_to_argv(&frame) {
                    // The AOF only ever logs writes; replies are discarded,
                    // so the classic proto-2 shapes are fine here.
                    let _ = crate::cmd::dispatch(&argv, store, &hub, &aof, 2);
                }
            }
            Err(RespError::Incomplete) => {
                eprintln!("rogis: AOF has a truncated tail; replaying what is complete");
                break;
            }
            Err(RespError::Invalid(msg)) => {
                eprintln!("rogis: AOF corrupt at offset {pos} ({msg}); replaying what is complete");
                break;
            }
        }
    }
    Ok(())
}

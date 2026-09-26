//! In-memory sharded key/value store: 16 shards, each a `Mutex<HashMap>`.
//!
//! Each shard behaves like a mini single-threaded Redis: all operations on
//! keys within one shard are serialized by that shard's mutex. Keys are
//! assigned to shards via `DefaultHasher` over the key bytes, mod 16.
//!
//! Expiry is lazy (an expired key behaves as missing on every access) plus
//! an active sweep via [`Store::evict_expired`]. Expiry deadlines are
//! absolute unix-millis timestamps so snapshots round-trip cleanly.

use std::collections::{hash_map::DefaultHasher, HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

/// Number of shards. Must stay a power of two only by convention; the
/// dispatch unit does not depend on the value, only on key hashing.
const NUM_SHARDS: usize = 16;

/// A stored value: Redis string / hash / list.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Str(Vec<u8>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    List(VecDeque<Vec<u8>>),
}

/// Errors the store can return. Mirrors the two Redis runtime errors the
/// command layer needs to distinguish.
#[derive(Debug, Clone, PartialEq)]
pub enum StoreError {
    /// Operation applied to a key holding the wrong value type.
    WrongType,
    /// `INCRBY` on content that is not a valid `i64` (or overflowed).
    NotInteger,
}

/// Shorthand for store operation results.
pub type SResult<T> = Result<T, StoreError>;

/// One entry in a shard: the value plus an optional absolute expiry
/// deadline in unix milliseconds.
struct Entry {
    value: Value,
    expires_at_ms: Option<u64>,
}

/// Snapshot-friendly copy of an entry, used by persistence.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub value: Value,
    pub expires_at_ms: Option<u64>,
}

/// 16-shard concurrent store. `Send + Sync` via the per-shard mutexes.
pub struct Store {
    shards: [Mutex<HashMap<Vec<u8>, Entry>>; NUM_SHARDS],
}

/// Current time as unix milliseconds.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_millis() as u64
}

/// Shard index for a key: `DefaultHasher` over the key bytes, mod 16.
fn shard_of(key: &[u8]) -> usize {
    let mut h = DefaultHasher::new();
    key.hash(&mut h);
    (h.finish() as usize) % NUM_SHARDS
}

fn expired(entry: &Entry, now: u64) -> bool {
    entry.expires_at_ms.is_some_and(|t| t <= now)
}

/// Parse a Redis integer: strict `i64` decimal, no whitespace.
/// Anything else (including overflow of the literal itself) is `NotInteger`.
fn parse_int(bytes: &[u8]) -> SResult<i64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .ok_or(StoreError::NotInteger)
}

impl Store {
    fn shard(&self, key: &[u8]) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Entry>> {
        self.shards[shard_of(key)]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Remove `key` from `shard` if it is expired. Returns true when the
    /// key is absent afterwards (missing or just expired).
    fn purge_if_expired(shard: &mut HashMap<Vec<u8>, Entry>, key: &[u8], now: u64) -> bool {
        match shard.get(key).map(|e| expired(e, now)) {
            None => true,
            Some(true) => {
                shard.remove(key);
                true
            }
            Some(false) => false,
        }
    }
}

impl Store {
    /// Create an empty store.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: [(); NUM_SHARDS].map(|()| Mutex::new(HashMap::new())),
        }
    }

    /// `SET key val [EX ms] [NX|XX]`.
    ///
    /// * `nx && xx` both false: plain overwrite; clears any old TTL unless
    ///   a new `ex_ms` is given.
    /// * `nx`: set only if the key does not exist (non-expired) ->
    ///   `Ok(false)` otherwise.
    /// * `xx`: set only if the key exists -> `Ok(false)` otherwise.
    /// * If both `nx` and `xx` are true, `nx` wins.
    pub fn set(
        &self,
        key: &[u8],
        val: Vec<u8>,
        ex_ms: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> SResult<bool> {
        let now = now_ms();
        let mut shard = self.shard(key);
        let exists = !Self::purge_if_expired(&mut shard, key, now);
        // When both flags are set, NX wins (a key cannot both exist and not exist).
        if nx && exists {
            return Ok(false);
        }
        if xx && !exists {
            return Ok(false);
        }
        shard.insert(
            key.to_vec(),
            Entry {
                value: Value::Str(val),
                expires_at_ms: ex_ms.map(|ms| now.saturating_add(ms)),
            },
        );
        Ok(true)
    }

    /// `GET key`. `WrongType` unless the value is a string.
    pub fn get(&self, key: &[u8]) -> SResult<Option<Vec<u8>>> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(None);
        }
        match shard.get(key).map(|e| &e.value) {
            Some(Value::Str(v)) => Ok(Some(v.clone())),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None), // unreachable: purge returned false
        }
    }

    /// `DEL k...`. Returns the number of keys actually removed.
    pub fn del(&self, keys: &[&[u8]]) -> SResult<usize> {
        let now = now_ms();
        let mut removed = 0;
        // One shard locked at a time: no lock ordering issues, no deadlock.
        for key in keys.iter().copied() {
            let mut shard = self.shard(key);
            if !Self::purge_if_expired(&mut shard, key, now) {
                shard.remove(key);
                removed += 1;
            }
        }
        Ok(removed)
    }

    /// `EXISTS k...`. Returns the number of keys that exist (non-expired).
    pub fn exists(&self, keys: &[&[u8]]) -> SResult<usize> {
        let now = now_ms();
        let mut count = 0;
        for key in keys.iter().copied() {
            let mut shard = self.shard(key);
            if !Self::purge_if_expired(&mut shard, key, now) {
                count += 1;
            }
        }
        Ok(count)
    }

    /// `PEXPIRE key ms`. `Ok(true)` if the timeout was set, `Ok(false)`
    /// if the key does not exist. Works on any value type.
    pub fn expire_ms(&self, key: &[u8], ms: u64) -> SResult<bool> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(false);
        }
        if let Some(entry) = shard.get_mut(key) {
            entry.expires_at_ms = Some(now.saturating_add(ms));
        }
        Ok(true)
    }

    /// `PTTL key`: -2 if missing, -1 if no expiry, else ms remaining.
    pub fn ttl_ms(&self, key: &[u8]) -> SResult<i64> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(-2);
        }
        match shard.get(key).and_then(|e| e.expires_at_ms) {
            None => Ok(-1),
            // Deadline is in the future here (otherwise the key was purged).
            Some(deadline) => Ok(deadline.saturating_sub(now) as i64),
        }
    }

    /// `INCRBY key delta`. Missing keys start at 0 with no TTL.
    /// `NotInteger` on non-integer content or overflow.
    pub fn incrby(&self, key: &[u8], delta: i64) -> SResult<i64> {
        let now = now_ms();
        let mut shard = self.shard(key);
        // (current value, expiry to keep). Missing keys start at 0 with no TTL;
        // an existing key keeps its TTL, matching Redis.
        let (current, keep_expiry) = if Self::purge_if_expired(&mut shard, key, now) {
            (0, None)
        } else {
            match shard.get(key) {
                Some(Entry {
                    value: Value::Str(bytes),
                    expires_at_ms,
                }) => (parse_int(bytes)?, *expires_at_ms),
                _ => return Err(StoreError::WrongType),
            }
        };
        // A failed parse/overflow leaves the old value untouched.
        let next = current.checked_add(delta).ok_or(StoreError::NotInteger)?;
        shard.insert(
            key.to_vec(),
            Entry {
                value: Value::Str(next.to_string().into_bytes()),
                expires_at_ms: keep_expiry,
            },
        );
        Ok(next)
    }

    /// `SETNX key val` — equivalent to `set` with `nx = true`.
    pub fn setnx(&self, key: &[u8], val: Vec<u8>) -> SResult<bool> {
        self.set(key, val, None, true, false)
    }

    /// `HSET key f v ...`. Returns the number of *new* fields added.
    pub fn hset(&self, key: &[u8], pairs: &[(Vec<u8>, Vec<u8>)]) -> SResult<usize> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            // Missing (or expired) key: create the hash.
            // A field repeated within one call counts once, like Redis.
            let mut map = HashMap::with_capacity(pairs.len());
            let mut added = 0;
            for (field, val) in pairs {
                if map.insert(field.clone(), val.clone()).is_none() {
                    added += 1;
                }
            }
            shard.insert(
                key.to_vec(),
                Entry {
                    value: Value::Hash(map),
                    expires_at_ms: None,
                },
            );
            return Ok(added);
        }
        match shard.get_mut(key).map(|e| &mut e.value) {
            Some(Value::Hash(map)) => {
                let mut added = 0;
                for (field, val) in pairs {
                    if map.insert(field.clone(), val.clone()).is_none() {
                        added += 1;
                    }
                }
                Ok(added)
            }
            _ => Err(StoreError::WrongType),
        }
    }

    /// `HGET key field`.
    pub fn hget(&self, key: &[u8], field: &[u8]) -> SResult<Option<Vec<u8>>> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(None);
        }
        match shard.get(key).map(|e| &e.value) {
            Some(Value::Hash(map)) => Ok(map.get(field).cloned()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(None), // unreachable
        }
    }

    /// `HGETALL key`. `Ok(vec![])` when the key is missing.
    pub fn hgetall(&self, key: &[u8]) -> SResult<Vec<(Vec<u8>, Vec<u8>)>> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(Vec::new());
        }
        match shard.get(key).map(|e| &e.value) {
            Some(Value::Hash(map)) => Ok(map.iter().map(|(f, v)| (f.clone(), v.clone())).collect()),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(Vec::new()), // unreachable
        }
    }

    /// `HDEL key f...`. Returns the number of fields actually removed.
    pub fn hdel(&self, key: &[u8], fields: &[&[u8]]) -> SResult<usize> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(0);
        }
        match shard.get_mut(key).map(|e| &mut e.value) {
            Some(Value::Hash(map)) => {
                let mut removed = 0;
                for field in fields.iter().copied() {
                    if map.remove(field).is_some() {
                        removed += 1;
                    }
                }
                Ok(removed)
            }
            _ => Err(StoreError::WrongType),
        }
    }

    /// `LPUSH key e...`. Creates the list when missing; returns new length.
    pub fn lpush(&self, key: &[u8], elems: &[Vec<u8>]) -> SResult<usize> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            let mut list = VecDeque::with_capacity(elems.len());
            for e in elems {
                list.push_front(e.clone());
            }
            let len = list.len();
            shard.insert(
                key.to_vec(),
                Entry {
                    value: Value::List(list),
                    expires_at_ms: None,
                },
            );
            return Ok(len);
        }
        match shard.get_mut(key).map(|e| &mut e.value) {
            Some(Value::List(list)) => {
                for e in elems {
                    list.push_front(e.clone());
                }
                Ok(list.len())
            }
            _ => Err(StoreError::WrongType),
        }
    }

    /// `RPOP key`. Removes the key when the list becomes empty.
    pub fn rpop(&self, key: &[u8]) -> SResult<Option<Vec<u8>>> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(None);
        }
        let (val, became_empty) = match shard.get_mut(key).map(|e| &mut e.value) {
            Some(Value::List(list)) => {
                let val = list.pop_back();
                (val, list.is_empty())
            }
            Some(_) => return Err(StoreError::WrongType),
            None => return Ok(None), // unreachable
        };
        // Like Redis, a list that becomes empty ceases to exist.
        if became_empty {
            shard.remove(key);
        }
        Ok(val)
    }

    /// `LRANGE key start stop` with Redis negative-index semantics
    /// (`stop` inclusive, out-of-range -> empty vec).
    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> SResult<Vec<Vec<u8>>> {
        let now = now_ms();
        let mut shard = self.shard(key);
        if Self::purge_if_expired(&mut shard, key, now) {
            return Ok(Vec::new());
        }
        match shard.get(key).map(|e| &e.value) {
            Some(Value::List(list)) => Ok(redis_range(list, start, stop)),
            Some(_) => Err(StoreError::WrongType),
            None => Ok(Vec::new()), // unreachable
        }
    }

    /// Active expiry sweep: removes every expired key in every shard.
    /// Returns the number of keys removed.
    pub fn evict_expired(&self) -> usize {
        let now = now_ms();
        let mut removed = 0;
        for shard in &self.shards {
            let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            let before = guard.len();
            guard.retain(|_, entry| !expired(entry, now));
            removed += before - guard.len();
        }
        removed
    }

    /// Full copy of all non-expired entries, for persistence.
    /// Expired keys are skipped (they behave as missing everywhere else).
    pub fn snapshot(&self) -> Vec<SnapshotEntry> {
        let now = now_ms();
        let mut out = Vec::new();
        for shard in &self.shards {
            let guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            for (key, entry) in guard.iter() {
                if expired(entry, now) {
                    continue;
                }
                out.push(SnapshotEntry {
                    key: key.clone(),
                    value: entry.value.clone(),
                    expires_at_ms: entry.expires_at_ms,
                });
            }
        }
        out
    }

    /// Replace the store contents with `entries` (e.g. loaded from disk).
    pub fn restore(&self, entries: Vec<SnapshotEntry>) {
        // Bucket by shard first so each shard is locked exactly once.
        let mut by_shard: Vec<Vec<SnapshotEntry>> = (0..NUM_SHARDS).map(|_| Vec::new()).collect();
        for entry in entries {
            by_shard[shard_of(&entry.key)].push(entry);
        }
        for (i, shard) in self.shards.iter().enumerate() {
            let mut guard = shard.lock().unwrap_or_else(|e| e.into_inner());
            guard.clear();
            for entry in by_shard[i].drain(..) {
                guard.insert(
                    entry.key,
                    Entry {
                        value: entry.value,
                        expires_at_ms: entry.expires_at_ms,
                    },
                );
            }
        }
    }
}

/// Slice a list with Redis `LRANGE` semantics: negative indices count from
/// the tail, `stop` is inclusive, out-of-range selections yield an empty vec.
fn redis_range(list: &VecDeque<Vec<u8>>, start: i64, stop: i64) -> Vec<Vec<u8>> {
    let len = list.len() as i64;
    if len == 0 {
        return Vec::new();
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if s >= len {
        return Vec::new();
    }
    if e >= len {
        e = len - 1;
    }
    if e < 0 || s > e {
        return Vec::new();
    }
    list.iter()
        .skip(s as usize)
        .take((e - s + 1) as usize)
        .cloned()
        .collect()
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    fn ms(v: &str) -> Vec<u8> {
        v.as_bytes().to_vec()
    }

    // ---------- set / get ----------

    #[test]
    fn set_get_roundtrip() {
        let s = Store::new();
        assert_eq!(s.set(b"k", ms("v"), None, false, false), Ok(true));
        assert_eq!(s.get(b"k"), Ok(Some(ms("v"))));
    }

    #[test]
    fn get_missing_is_none() {
        let s = Store::new();
        assert_eq!(s.get(b"nope"), Ok(None));
    }

    #[test]
    fn set_overwrites_and_clears_old_ttl() {
        let s = Store::new();
        assert_eq!(s.set(b"k", ms("v1"), Some(60_000), false, false), Ok(true));
        assert!(s.ttl_ms(b"k").unwrap() > 0);
        assert_eq!(s.set(b"k", ms("v2"), None, false, false), Ok(true));
        assert_eq!(s.get(b"k"), Ok(Some(ms("v2"))));
        assert_eq!(s.ttl_ms(b"k"), Ok(-1));
    }

    #[test]
    fn set_keeps_new_ttl() {
        let s = Store::new();
        assert_eq!(s.set(b"k", ms("v"), Some(60_000), false, false), Ok(true));
        let ttl = s.ttl_ms(b"k").unwrap();
        assert!(ttl > 0 && ttl <= 60_000, "ttl={ttl}");
    }

    // ---------- nx / xx matrix ----------

    #[test]
    fn nx_xx_matrix() {
        let s = Store::new();
        // nx on missing -> true, key created
        assert_eq!(s.set(b"a", ms("1"), None, true, false), Ok(true));
        // nx on existing -> false, value untouched
        assert_eq!(s.set(b"a", ms("2"), None, true, false), Ok(false));
        assert_eq!(s.get(b"a"), Ok(Some(ms("1"))));
        // xx on missing -> false, key not created
        assert_eq!(s.set(b"b", ms("1"), None, false, true), Ok(false));
        assert_eq!(s.get(b"b"), Ok(None));
        // xx on existing -> true, value replaced
        assert_eq!(s.set(b"a", ms("3"), None, false, true), Ok(true));
        assert_eq!(s.get(b"a"), Ok(Some(ms("3"))));
    }

    #[test]
    fn setnx_semantics() {
        let s = Store::new();
        assert_eq!(s.setnx(b"k", ms("v1")), Ok(true));
        assert_eq!(s.setnx(b"k", ms("v2")), Ok(false));
        assert_eq!(s.get(b"k"), Ok(Some(ms("v1"))));
    }

    #[test]
    fn nx_on_expired_key_sets() {
        let s = Store::new();
        assert_eq!(s.set(b"k", ms("old"), Some(1), false, false), Ok(true));
        thread::sleep(Duration::from_millis(5));
        // expired == missing, so NX succeeds
        assert_eq!(s.set(b"k", ms("new"), None, true, false), Ok(true));
        assert_eq!(s.get(b"k"), Ok(Some(ms("new"))));
    }

    // ---------- lazy expiry ----------

    #[test]
    fn lazy_expiry_on_access() {
        let s = Store::new();
        assert_eq!(s.set(b"k", ms("v"), Some(1), false, false), Ok(true));
        thread::sleep(Duration::from_millis(5));
        assert_eq!(s.get(b"k"), Ok(None));
        assert_eq!(s.exists(&[b"k".as_slice()]), Ok(0));
        assert_eq!(s.ttl_ms(b"k"), Ok(-2));
    }

    #[test]
    fn expire_ms_and_ttl() {
        let s = Store::new();
        assert_eq!(s.expire_ms(b"missing", 1000), Ok(false));
        assert_eq!(s.set(b"k", ms("v"), None, false, false), Ok(true));
        assert_eq!(s.expire_ms(b"k", 60_000), Ok(true));
        let ttl = s.ttl_ms(b"k").unwrap();
        assert!(ttl > 0 && ttl <= 60_000, "ttl={ttl}");
    }

    #[test]
    fn ttl_missing_and_persistent() {
        let s = Store::new();
        assert_eq!(s.ttl_ms(b"missing"), Ok(-2));
        assert_eq!(s.set(b"k", ms("v"), None, false, false), Ok(true));
        assert_eq!(s.ttl_ms(b"k"), Ok(-1));
    }

    // ---------- del / exists ----------

    #[test]
    fn del_and_exists() {
        let s = Store::new();
        s.set(b"a", ms("1"), None, false, false).unwrap();
        s.set(b"b", ms("2"), None, false, false).unwrap();
        assert_eq!(
            s.exists(&[b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]),
            Ok(2)
        );
        assert_eq!(s.del(&[b"a".as_slice(), b"c".as_slice()]), Ok(1));
        assert_eq!(s.exists(&[b"a".as_slice(), b"b".as_slice()]), Ok(1));
        assert_eq!(s.del(&[b"a".as_slice()]), Ok(0));
    }

    // ---------- incrby ----------

    #[test]
    fn incrby_missing_starts_at_zero_no_ttl() {
        let s = Store::new();
        assert_eq!(s.incrby(b"c", 5), Ok(5));
        assert_eq!(s.get(b"c"), Ok(Some(ms("5"))));
        assert_eq!(s.ttl_ms(b"c"), Ok(-1));
        assert_eq!(s.incrby(b"c", -8), Ok(-3));
        assert_eq!(s.get(b"c"), Ok(Some(ms("-3"))));
    }

    #[test]
    fn incrby_not_integer() {
        let s = Store::new();
        s.set(b"k", ms("notanumber"), None, false, false).unwrap();
        assert_eq!(s.incrby(b"k", 1), Err(StoreError::NotInteger));
    }

    #[test]
    fn incrby_overflow_is_not_integer() {
        let s = Store::new();
        s.set(b"k", ms("9223372036854775807"), None, false, false)
            .unwrap();
        assert_eq!(s.incrby(b"k", 1), Err(StoreError::NotInteger));
        // value unchanged after failed incr
        assert_eq!(s.get(b"k"), Ok(Some(ms("9223372036854775807"))));
    }

    #[test]
    fn incrby_wrong_type() {
        let s = Store::new();
        s.hset(b"h", &[(ms("f"), ms("v"))]).unwrap();
        assert_eq!(s.incrby(b"h", 1), Err(StoreError::WrongType));
    }

    // ---------- wrong type paths ----------

    #[test]
    fn wrong_type_paths() {
        let s = Store::new();
        s.set(b"str", ms("v"), None, false, false).unwrap();
        s.hset(b"hash", &[(ms("f"), ms("v"))]).unwrap();
        s.lpush(b"list", &[ms("e")]).unwrap();

        assert_eq!(s.get(b"hash"), Err(StoreError::WrongType));
        assert_eq!(s.get(b"list"), Err(StoreError::WrongType));
        assert_eq!(s.hget(b"str", b"f"), Err(StoreError::WrongType));
        assert_eq!(
            s.hset(b"str", &[(ms("f"), ms("v"))]),
            Err(StoreError::WrongType)
        );
        assert_eq!(
            s.hdel(b"str", &[b"f".as_slice()]),
            Err(StoreError::WrongType)
        );
        assert_eq!(s.hgetall(b"str"), Err(StoreError::WrongType));
        assert_eq!(s.lpush(b"str", &[ms("e")]), Err(StoreError::WrongType));
        assert_eq!(s.rpop(b"str"), Err(StoreError::WrongType));
        assert_eq!(s.lrange(b"hash", 0, -1), Err(StoreError::WrongType));
        assert_eq!(s.incrby(b"list", 1), Err(StoreError::WrongType));
    }

    // ---------- hashes ----------

    #[test]
    fn hash_ops() {
        let s = Store::new();
        // hgetall on missing -> empty vec
        assert_eq!(s.hgetall(b"missing"), Ok(vec![]));
        assert_eq!(s.hget(b"missing", b"f"), Ok(None));
        assert_eq!(s.hdel(b"missing", &[b"f".as_slice()]), Ok(0));

        assert_eq!(
            s.hset(b"h", &[(ms("a"), ms("1")), (ms("b"), ms("2"))]),
            Ok(2)
        );
        // overwriting existing fields adds 0 new
        assert_eq!(
            s.hset(b"h", &[(ms("a"), ms("10")), (ms("c"), ms("3"))]),
            Ok(1)
        );
        assert_eq!(s.hget(b"h", b"a"), Ok(Some(ms("10"))));
        assert_eq!(s.hget(b"h", b"zzz"), Ok(None));

        let mut all = s.hgetall(b"h").unwrap();
        all.sort();
        assert_eq!(
            all,
            vec![(ms("a"), ms("10")), (ms("b"), ms("2")), (ms("c"), ms("3"))]
        );

        assert_eq!(s.hdel(b"h", &[b"a".as_slice(), b"zzz".as_slice()]), Ok(1));
        assert_eq!(s.hget(b"h", b"a"), Ok(None));
    }

    // ---------- lists ----------

    #[test]
    fn list_push_pop() {
        let s = Store::new();
        assert_eq!(s.rpop(b"missing"), Ok(None));
        assert_eq!(s.lpush(b"l", &[ms("a"), ms("b"), ms("c")]), Ok(3));
        // LPUSH a b c -> head is c
        assert_eq!(s.lrange(b"l", 0, -1), Ok(vec![ms("c"), ms("b"), ms("a")]));
        assert_eq!(s.rpop(b"l"), Ok(Some(ms("a"))));
        assert_eq!(s.rpop(b"l"), Ok(Some(ms("b"))));
        // popping the last element removes the key
        assert_eq!(s.rpop(b"l"), Ok(Some(ms("c"))));
        assert_eq!(s.exists(&[b"l".as_slice()]), Ok(0));
        assert_eq!(s.rpop(b"l"), Ok(None));
    }

    #[test]
    fn lrange_negative_indices() {
        let s = Store::new();
        s.lpush(b"l", &[ms("1"), ms("2"), ms("3"), ms("4"), ms("5")])
            .unwrap(); // list: 5 4 3 2 1
        assert_eq!(
            s.lrange(b"l", 0, -1),
            Ok(vec![ms("5"), ms("4"), ms("3"), ms("2"), ms("1")])
        );
        assert_eq!(s.lrange(b"l", -3, -1), Ok(vec![ms("3"), ms("2"), ms("1")]));
        assert_eq!(s.lrange(b"l", 1, 2), Ok(vec![ms("4"), ms("3")]));
        // stop inclusive
        assert_eq!(s.lrange(b"l", 0, 0), Ok(vec![ms("5")]));
        // out of range -> empty
        assert_eq!(s.lrange(b"l", 10, 20), Ok(vec![]));
        assert_eq!(s.lrange(b"l", 3, 1), Ok(vec![]));
        assert_eq!(s.lrange(b"l", -100, -90), Ok(vec![]));
        // clamping
        assert_eq!(
            s.lrange(b"l", -100, 100),
            Ok(vec![ms("5"), ms("4"), ms("3"), ms("2"), ms("1")])
        );
        // missing key -> empty vec
        assert_eq!(s.lrange(b"missing", 0, -1), Ok(vec![]));
    }

    // ---------- evict_expired ----------

    #[test]
    fn evict_expired_removes_only_expired() {
        let s = Store::new();
        s.set(b"e1", ms("v"), Some(1), false, false).unwrap();
        s.set(b"e2", ms("v"), Some(1), false, false).unwrap();
        s.set(b"keep1", ms("v"), None, false, false).unwrap();
        s.set(b"keep2", ms("v"), Some(60_000), false, false)
            .unwrap();
        thread::sleep(Duration::from_millis(5));
        assert_eq!(s.evict_expired(), 2);
        assert_eq!(
            s.exists(&[
                b"keep1".as_slice(),
                b"keep2".as_slice(),
                b"e1".as_slice(),
                b"e2".as_slice()
            ]),
            Ok(2)
        );
        // second sweep finds nothing
        assert_eq!(s.evict_expired(), 0);
    }

    // ---------- snapshot / restore ----------

    #[test]
    fn snapshot_restore_roundtrip() {
        let s = Store::new();
        s.set(b"str", ms("hello"), Some(60_000), false, false)
            .unwrap();
        s.hset(b"hash", &[(ms("f1"), ms("v1")), (ms("f2"), ms("v2"))])
            .unwrap();
        s.lpush(b"list", &[ms("x"), ms("y")]).unwrap();
        s.set(b"plain", ms("p"), None, false, false).unwrap();
        s.set(b"expired", ms("gone"), Some(1), false, false)
            .unwrap();
        thread::sleep(Duration::from_millis(5));

        let snap = s.snapshot();
        // expired key is skipped
        assert_eq!(snap.len(), 4);
        let mut keys: Vec<Vec<u8>> = snap.iter().map(|e| e.key.clone()).collect();
        keys.sort();
        assert_eq!(keys, vec![ms("hash"), ms("list"), ms("plain"), ms("str")]);

        let s2 = Store::new();
        s2.restore(snap);
        assert_eq!(s2.get(b"str"), Ok(Some(ms("hello"))));
        assert_eq!(s2.get(b"plain"), Ok(Some(ms("p"))));
        assert_eq!(s2.get(b"expired"), Ok(None));
        assert_eq!(s2.hget(b"hash", b"f1"), Ok(Some(ms("v1"))));
        assert_eq!(s2.lrange(b"list", 0, -1), Ok(vec![ms("y"), ms("x")]));
        // TTL preserved (absolute deadline round-trips)
        let ttl = s2.ttl_ms(b"str").unwrap();
        assert!(ttl > 0 && ttl <= 60_000, "ttl={ttl}");
        assert_eq!(s2.ttl_ms(b"plain"), Ok(-1));
    }

    #[test]
    fn restore_replaces_contents() {
        let s = Store::new();
        s.set(b"old", ms("v"), None, false, false).unwrap();
        s.restore(vec![]);
        assert_eq!(s.exists(&[b"old".as_slice()]), Ok(0));
    }

    // ---------- concurrency ----------

    #[test]
    fn concurrent_smoke_no_deadlock() {
        let s = Arc::new(Store::new());
        let mut handles = vec![];
        for t in 0..8 {
            let s = Arc::clone(&s);
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    let k = format!("t{t}:k{i}");
                    let c = format!("t{t}:c");
                    let h = format!("t{t}:h");
                    let l = format!("t{t}:l");
                    s.set(k.as_bytes(), ms("v"), None, false, false).unwrap();
                    assert_eq!(s.get(k.as_bytes()), Ok(Some(ms("v"))));
                    s.incrby(c.as_bytes(), 1).unwrap();
                    s.hset(h.as_bytes(), &[(format!("f{i}").into_bytes(), ms("v"))])
                        .unwrap();
                    s.lpush(l.as_bytes(), &[ms("e")]).unwrap();
                    // occasional cross-key ops
                    if i % 10 == 0 {
                        s.expire_ms(k.as_bytes(), 60_000).unwrap();
                        s.ttl_ms(k.as_bytes()).unwrap();
                    }
                }
            }));
        }
        for h in handles {
            h.join().expect("worker thread panicked");
        }
        // deterministic final state: disjoint key prefixes per thread
        for t in 0..8 {
            let c = format!("t{t}:c");
            assert_eq!(s.incrby(c.as_bytes(), 0), Ok(100));
            let h = format!("t{t}:h");
            assert_eq!(s.hgetall(h.as_bytes()).unwrap().len(), 100);
            let l = format!("t{t}:l");
            assert_eq!(s.lrange(l.as_bytes(), 0, -1).unwrap().len(), 100);
        }
        let keys: Vec<Vec<u8>> = (0..8)
            .flat_map(|t| (0..100).map(move |i| format!("t{t}:k{i}").into_bytes()))
            .collect();
        let refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
        assert_eq!(s.exists(&refs), Ok(800));
    }
}

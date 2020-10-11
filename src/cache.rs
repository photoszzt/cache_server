use log::error;

use crate::lrucache::{LruCache, Meter};
use bytes::Bytes;
use dashmap::DashMap;
use linked_hash_map::LinkedHashMap;
use std::borrow::Borrow;
use std::collections::hash_map::RandomState;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::hash::Hash;

use crate::{Error, Result};
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::hash::BuildHasher;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, Notify};
use tokio::time::{self, Duration, Instant};

pub struct BytesLen;

impl<K> Meter<K, Entry> for BytesLen {
    type Measure = usize;
    fn measure<Q: ?Sized>(&self, _: &Q, v: &Entry) -> usize
    where
        K: Borrow<Q>,
    {
        v.data.len()
    }
}

impl<K: Eq + Hash> LruCache<K, Entry, RandomState, BytesLen> {
    /// Creates an empty cache without capacity.
    /// This will be just a normal cache
    pub fn no_capacity_restriction() -> Self {
        LruCache {
            map: LinkedHashMap::new(),
            current_measure: Default::default(),
            max_capacity: None,
            meter: BytesLen,
        }
    }
}

/// Server state shared across all connections.
///
/// `Db` contains a `LruCache` storing the key/value data and all
/// `broadcast::Sender` values for active pub/sub channels.
///
/// A `Db` instance is a handle to shared state. Cloning `Db` is shallow and
/// only incurs an atomic ref count increment.
///
/// When a `Db` value is created, a background task is spawned. This task is
/// used to expire values after the requested duration has elapsed. The task
/// runs until all instances of `Db` are dropped, at which point the task
/// terminates.
#[derive(Debug, Clone)]
pub(crate) struct Db {
    /// Handle to shared state. The background task will also have an
    /// `Arc<Shared>`.
    shared: Arc<Shared>,
}

#[derive(Debug)]
pub(crate) struct Shared {
    state: RwLock<CacheState>,

    /// Used to store metadata
    metadata: DashMap<String, HashSet<Bytes>>,

    /// Notifies the background task handling entry expiration. The background
    /// task waits on this to be notified, then checks for expired values or the
    /// shutdown signal.
    background_task: Notify,

    /// The root directory to store the evicted value
    /// This value won't change after it's set.
    root: PathBuf,
}

#[derive(Debug)]
pub(crate) struct CacheState<S: BuildHasher = RandomState> {
    /// key-value entries that's orded by lru
    lru: LruCache<OsString, Entry, S, BytesLen>,

    /// meta data hash map that stores the information about the evicted value
    /// with key is the file name and value is the meta data. Currently, we
    /// only support eviting bytes to file
    disk_entries: HashMap<OsString, DiskEntry>,

    /// The pub/sub key-space. Redis uses a **separate** key space for key-value
    /// and pub/sub. `mini-redis` handles this by using a separate `HashMap`.
    pub_sub: HashMap<String, broadcast::Sender<Bytes>>,

    /// Tracks key TTLs.
    ///
    /// A `BTreeMap` is used to maintain expirations sorted by when they expire.
    /// This allows the background task to iterate this map to find the value
    /// expiring next.
    ///
    /// While highly unlikely, it is possible for more than one expiration to be
    /// created for the same instant. Because of this, the `Instant` is
    /// insufficient for the key. A unique expiration identifier (`u64`) is used
    /// to break these ties.
    /// # from miniredis: https://github.com/tokio-rs/mini-redis/blob/master/src/db.rs
    expirations: BTreeMap<(Instant, u64), OsString>,

    /// Identifier to use for the next expiration. Each expiration is associated
    /// with a unique identifier. See above for why.
    next_id: u64,

    /// True when the Db instance is shutting down. This happens when all `Db`
    /// values drop. Setting this to `true` signals to the background task to
    /// exit.
    shutdown: bool,
}

/// Entry in the key-value store
#[derive(Debug)]
struct Entry {
    /// Uniquely identifies this entry.
    id: u64,

    /// Stored data
    data: Bytes,

    /// Instant at which the entry expires and should be removed from the
    /// database.
    expires_at: Option<Instant>,
}

/// DiskEntry in disk_entries map. It's used to store the id and expires_at when the entry is evicted to disk
#[derive(Debug)]
struct DiskEntry {
    /// Uniquely identifies this entry.
    id: u64,

    /// Instant at which the entry expires and should be removed from the
    /// database.
    expires_at: Option<Instant>,
}

impl Db {
    pub fn new<T>(path: T, size: Option<u64>) -> Result<Self>
    where
        PathBuf: From<T>,
    {
        let lru = if let Some(size_v) = size {
            LruCache::with_meter(size_v, BytesLen)
        } else {
            LruCache::no_capacity_restriction()
        };
        let cache = CacheState {
            lru,
            disk_entries: HashMap::new(),
            pub_sub: HashMap::new(),
            expirations: BTreeMap::new(),
            next_id: 0,
            shutdown: false,
        };
        let shared = Arc::new(Shared {
            state: RwLock::new(cache),
            metadata: DashMap::new(),
            root: PathBuf::from(path),
            background_task: Notify::new(),
        });
        fs::create_dir_all(&shared.root)?;
        // Start the background task.
        tokio::spawn(purge_expired_tasks(shared.clone()));

        Ok(Db { shared })
    }

    /// Return the current size of the lru cache in memory
    #[cfg(test)]
    fn mem_size(&self) -> u64 {
        let state = self.shared.state.read().unwrap();
        state.lru.size()
    }

    /// Return the number of entries in the cache
    #[cfg(test)]
    fn mem_len(&self) -> usize {
        let state = self.shared.state.read().unwrap();
        state.lru.len()
    }

    /// Return the maximum size of the cache
    #[cfg(test)]
    fn mem_capacity(&self) -> Option<u64> {
        let state = self.shared.state.read().unwrap();
        state.lru.capacity()
    }

    /// Return the number of entries stored on disk
    #[cfg(test)]
    fn disk_len(&self) -> usize {
        let state = self.shared.state.read().unwrap();
        state.disk_entries.len()
    }

    pub fn insert<K: AsRef<OsStr>>(
        &self,
        key: K,
        value: Bytes,
        expire: Option<Duration>,
    ) -> Result<()> {
        let mut state = self.shared.state.write().unwrap();
        if state.disk_entries.contains_key(key.as_ref()) {
            let path = self.shared.rel_to_abs_path(key.as_ref());
            fs::remove_file(&path).map_err(|e| -> Error {
                error!("Error removing file from cache: `{:?}`: {}", path, e);
                Into::into(e)
            })?;
            state.disk_entries.remove(key.as_ref());
        }
        // Get and increment the next insertion ID. Guarded by the lock, this
        // ensures a unique identifier is associated with each `set` operation.
        let id = state.next_id;
        state.next_id += 1;

        // If this `set` becomes the key that expires **next**, the background
        // task needs to be notified so it can update its state.
        //
        // Whether or not the task needs to be notified is computed during the
        // `set` routine.
        let mut notify = false;

        let expires_at = expire.map(|duration| {
            // `Instant` at which the key expires.
            let when = Instant::now() + duration;

            // Only notify the worker task if the newly inserted expiration is the
            // **next** key to evict. In this case, the worker needs to be woken up
            // to update its state.
            notify = state
                .next_expiration()
                .map(|expiration| expiration > when)
                .unwrap_or(true);

            // Track the expiration.
            state
                .expirations
                .insert((when, id), key.as_ref().to_owned());
            when
        });
        let (prev, evicted) = state.lru.insert(
            key.as_ref().to_owned(),
            Entry {
                id,
                data: value,
                expires_at,
            },
        );
        for (evicted_key, evicted_value) in evicted.iter() {
            let path = self.shared.rel_to_abs_path(evicted_key);
            state.store_evicted(evicted_key, evicted_value, &path)?;
        }
        // If there was a value previously associated with the key **and** it
        // had an expiration time. The associated entry in the `expirations` map
        // must also be removed. This avoids leaking data.
        if let Some(prev_v) = prev {
            if let Some(when) = prev_v.expires_at {
                state.expirations.remove(&(when, prev_v.id));
            }
        }

        // Release the mutex before notifying the background task. This helps
        // reduce contention by avoiding the background task waking up only to
        // be unable to acquire the mutex due to this function still holding it.
        drop(state);

        if notify {
            // Finally, only notify the background task if it needs to update
            // its state to reflect a new expiration.
            self.shared.background_task.notify();
        }
        Ok(())
    }

    pub fn get<K: AsRef<OsStr>>(&self, key: K) -> Result<Bytes> {
        let mut state = self.shared.state.write().unwrap();
        let path = self.shared.rel_to_abs_path(key.as_ref());
        match state.lru.get(key.as_ref()) {
            Some(val) => Ok(val.data.clone()),
            None => {
                if state.disk_entries.contains_key(key.as_ref()) {
                    let content = fs::read(path)?;
                    Ok(Bytes::from(content))
                } else {
                    Err(Error::KeyNotFound)
                }
            }
        }
    }

    #[cfg(test)]
    fn contains_key<K: AsRef<OsStr>>(&self, key: K) -> bool {
        let state = self.shared.state.read().unwrap();
        state.lru.contains_key(key.as_ref()) || state.disk_entries.contains_key(key.as_ref())
    }

    pub fn remove<K: AsRef<OsStr>>(&self, key: K) -> Result<()> {
        let mut state = self.shared.state.write().unwrap();
        match state.lru.remove(key.as_ref()) {
            Some(v) => {
                if let Some(when) = v.expires_at {
                    state.expirations.remove(&(when, v.id));
                }
                Ok(())
            }
            None => {
                if state.disk_entries.contains_key(key.as_ref()) {
                    let abs_path = self.shared.rel_to_abs_path(key.as_ref());
                    state.remove_disk_entry(key.as_ref(), &abs_path)
                } else {
                    Err(Error::KeyNotFound)
                }
            }
        }
    }

    /// Returns a `Receiver` for the requested channel.
    ///
    /// The returned `Receiver` is used to receive values broadcast by `PUBLISH`
    /// commands.
    pub(crate) fn subscribe(&self, key: String) -> broadcast::Receiver<Bytes> {
        use std::collections::hash_map::Entry;

        // Acquire the mutex
        let mut state = self.shared.state.write().unwrap();

        // If there is no entry for the requested channel, then create a new
        // broadcast channel and associate it with the key. If one already
        // exists, return an associated receiver.
        match state.pub_sub.entry(key) {
            Entry::Occupied(e) => e.get().subscribe(),
            Entry::Vacant(e) => {
                // No broadcast channel exists yet, so create one.
                //
                // The channel is created with a capacity of `1024` messages. A
                // message is stored in the channel until **all** subscribers
                // have seen it. This means that a slow subscriber could result
                // in messages being held indefinitely.
                //
                // When the channel's capacity fills up, publishing will result
                // in old messages being dropped. This prevents slow consumers
                // from blocking the entire system.
                let (tx, rx) = broadcast::channel(1024);
                e.insert(tx);
                rx
            }
        }
    }

    /// Publish a message to the channel. Returns the number of subscribers
    /// listening on the channel.
    pub(crate) fn publish(&self, key: &str, value: Bytes) -> usize {
        let state = self.shared.state.read().unwrap();

        state
            .pub_sub
            .get(key)
            // On a successful message send on the broadcast channel, the number
            // of subscribers is returned. An error indicates there are no
            // receivers, in which case, `0` should be returned.
            .map(|tx| tx.send(value).unwrap_or(0))
            // If there is no entry for the channel key, then there are no
            // subscribers. In this case, return `0`.
            .unwrap_or(0)
    }

    /// Add the `value` to the set belongs to `key`. Returns 0 if the value
    /// is already in the set and 1 for adding the value successfully
    pub(crate) fn meta_sadd(&self, key: &str, members: &[Bytes]) -> usize {
        if !self.shared.metadata.contains_key(key) {
            self.shared.metadata.insert(key.to_string(), HashSet::new());
        }
        let mut val_set = self.shared.metadata.get_mut(key).unwrap();
        members
            .iter()
            .map(|member| if val_set.insert(member.clone()) { 1 } else { 0 })
            .sum()
    }

    /// Remove the `value` from the set belongs to `key`. Returns 0 if the value
    /// doesn't exist in the set and 1 for removing a existing value
    pub(crate) fn meta_srem(&self, key: &str, members: &[Bytes]) -> usize {
        if let Some(mut val_set) = self.shared.metadata.get_mut(key) {
            members
                .iter()
                .map(|member| if val_set.remove(member) { 1 } else { 0 })
                .sum()
        } else {
            0
        }
    }

    /// Returns the all members of the set belogs to `key`. If the key doesn't
    /// exists, return the empty set.
    pub(crate) fn meta_smembers(&self, key: &str) -> HashSet<Bytes> {
        if let Some(val_set) = self.shared.metadata.get(key) {
            val_set.clone()
        } else {
            HashSet::new()
        }
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        // If this is the last active `Db` instance, the background task must be
        // notified to shut down.
        //
        // First, determine if this is the last `Db` instance. This is done by
        // checking `strong_count`. The count will be 2. One for this `Db`
        // instance and one for the handle held by the background task.
        if Arc::strong_count(&self.shared) == 2 {
            // The background task must be signaled to shutdown. This is done by
            // setting `State::shutdown` to `true` and signalling the task.
            let mut state = self.shared.state.write().unwrap();
            state.shutdown = true;

            // Drop the lock before signalling the background task. This helps
            // reduce lock contention by ensuring the background task doesn't
            // wake up only to be unable to acquire the mutex.
            drop(state);
            self.shared.background_task.notify();
        }
    }
}

impl Shared {
    /// Purge all expired keys and return the `Instant` at which the **next**
    /// key will expire. The background task will sleep until this instant.
    fn purge_expired_keys(&self) -> Option<Instant> {
        let mut state = self.state.write().unwrap();

        if state.shutdown {
            // The database is shutting down. All handles to the shared state
            // have dropped. The background task should exit.
            return None;
        }

        // This is needed to make the borrow checker happy. In short, `write()`
        // returns a `RwLockWriteGuard` and not a `&mut State`. The borrow checker is
        // not able to see "through" the guard and determine that it is
        // safe to access both `state.expirations` and `state.lru` mutably,
        // so we get a "real" mutable reference to `State` outside of the loop.
        let state = &mut *state;

        // Find all keys scheduled to expire **before** now.
        let now = Instant::now();

        while let Some((&(when, id), key)) = state.expirations.iter().next() {
            if when > now {
                // Done purging, `when` is the instant at which the next key
                // expires. The worker task will wait until this instant.
                return Some(when);
            }

            // The key expired, remove it
            let _ = state.lru.remove(key);
            if state.disk_entries.contains_key(key) {
                let path = self.rel_to_abs_path(key);
                let _ = fs::remove_file(&path).map_err(|e| {
                    error!("Error removing file from cache: `{:?}`: {}", path, e);
                });
                state.disk_entries.remove(key);
            }
            state.expirations.remove(&(when, id));
        }

        None
    }

    /// Returns `true` if the database is shutting down
    ///
    /// The `shutdown` flag is set when all `Db` values have dropped, indicating
    /// that the shared state can no longer be accessed.
    fn is_shutdown(&self) -> bool {
        self.state.read().unwrap().shutdown
    }

    fn rel_to_abs_path<K: AsRef<Path>>(&self, rel_path: K) -> PathBuf {
        self.root.join(rel_path)
    }
}

impl CacheState {
    fn next_expiration(&self) -> Option<Instant> {
        self.expirations
            .keys()
            .next()
            .map(|expiration| expiration.0)
    }

    #[inline]
    fn add_file<K: AsRef<OsStr>>(&mut self, addfile_path: K, value: &Entry) -> Result<()> {
        self.disk_entries.insert(
            addfile_path.as_ref().to_owned(),
            DiskEntry {
                id: value.id,
                expires_at: value.expires_at,
            },
        );
        Ok(())
    }

    fn store_evicted<K: AsRef<OsStr>>(
        &mut self,
        key: K,
        value: &Entry,
        abs_path: &Path,
    ) -> Result<()> {
        let rel_path = key.as_ref();
        fs::create_dir_all(abs_path.parent().expect("Bad path?"))?;
        let mut f = File::create(&abs_path)?;
        f.write_all(&value.data.to_vec())?;
        self.add_file(rel_path, value).map_err(|e| {
            error!(
                "Failed to add key to file `{}`: {}",
                rel_path.to_string_lossy(),
                e
            );
            fs::remove_file(abs_path).expect("Failed to remove file we just created!");
            e
        })
    }

    // /// Return the path in which the evicted value is stored
    // pub fn evicted_path(&self) -> &Path {
    //     self.root.as_path()
    // }

    pub fn remove_disk_entry<K: AsRef<OsStr>>(&mut self, key: K, abs_path: &Path) -> Result<()> {
        fs::remove_file(&abs_path).map_err(|e| -> Error {
            error!("Error removing file from cache: `{:?}`: {}", abs_path, e);
            Into::into(e)
        })?;
        if let Some(entry) = self.disk_entries.remove(key.as_ref()) {
            if let Some(when) = entry.expires_at {
                self.expirations.remove(&(when, entry.id));
            }
        }
        Ok(())
    }
}

/// Routine executed by the background task.
///
/// Wait to be notified. On notification, purge any expired keys from the shared
/// state handle. If `shutdown` is set, terminate the task.
async fn purge_expired_tasks(shared: Arc<Shared>) {
    // If the shutdown flag is set, then the task should exit.
    while !shared.is_shutdown() {
        // Purge all keys that are expired. The function returns the instant at
        // which the **next** key will expire. The worker should wait until the
        // instant has passed then purge again.
        if let Some(when) = shared.purge_expired_keys() {
            // Wait until the next key expires **or** until the background task
            // is notified. If the task is notified, then it must reload its
            // state as new keys have been set to expire early. This is done by
            // looping.
            tokio::select! {
                _ = time::delay_until(when) => {}
                _ = shared.background_task.notified() => {}
            }
        } else {
            // There are no keys expiring in the future. Wait until the task is
            // notified.
            shared.background_task.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Db;

    use bytes::Bytes;
    use std::path::Path;
    use std::time::Duration;
    use tempfile::TempDir;
    use tokio::runtime::Runtime;

    struct TempRoot {
        pub tempdir: TempDir,
    }

    impl TempRoot {
        pub fn new() -> TempRoot {
            TempRoot {
                tempdir: tempfile::Builder::new()
                    .prefix("lru-cache-test")
                    .tempdir()
                    .unwrap(),
            }
        }

        pub fn tmp(&self) -> &Path {
            self.tempdir.path()
        }
    }

    #[test]
    fn test_get_insert() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let f = TempRoot::new();
            let c = Db::new(f.tmp(), Some(20)).unwrap();
            assert_eq!(c.mem_capacity(), Some(20));
            let b1 = Bytes::from(vec![0; 10]);
            let b2 = Bytes::from(vec![1; 9]);
            let _ = c.insert("a##b", b1.clone(), None);
            assert!(c.contains_key("a##b"));
            assert_eq!(c.get("a##b").unwrap(), b1.clone());
            let _ = c.insert("b##c", b2.clone(), None);
            assert_eq!(c.get("b##c").unwrap(), b2.clone());
            assert!(c.contains_key("b##c"));
            assert_eq!(c.mem_size(), 19);
            assert_eq!(c.mem_len(), 2);
            assert_eq!(c.disk_len(), 0);

            let b3 = Bytes::from(vec![1; 2]);
            let _ = c.insert("c##d", b3, None);
            assert_eq!(c.mem_size(), 11);
            assert_eq!(c.mem_len(), 2);
            assert_eq!(c.disk_len(), 1);
            assert!(f.tmp().join("a##b").exists());
            assert!(c.contains_key("a##b"));
            assert_eq!(c.get("a##b").unwrap(), b1);
        });
    }

    #[test]
    fn test_ttl() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let f = TempRoot::new();
            let c = Db::new(f.tmp(), Some(20)).unwrap();
            assert_eq!(c.mem_capacity(), Some(20));
            let b1 = Bytes::from(vec![0; 10]);
            let _ = c.insert("a##b", b1.clone(), Some(Duration::from_millis(200)));
            assert!(c.contains_key("a##b"));
            std::thread::sleep(Duration::from_millis(250));
            assert!(!c.contains_key("a##b"));
        });
    }

    #[test]
    fn test_remove() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let f = TempRoot::new();
            let c = Db::new(f.tmp(), Some(20)).unwrap();
            let b1 = Bytes::from(vec![0; 10]);
            let b2 = Bytes::from(vec![1; 11]);
            let _ = c.insert("a##b", b1.clone(), None);
            assert!(c.contains_key("a##b"));
            let _ = c.remove("a##b");
            assert!(!c.contains_key("a##b"));

            let _ = c.insert("a##b", b1.clone(), None);
            let _ = c.insert("b##c", b2, None);
            assert!(c.contains_key("a##b"));
            assert!(c.contains_key("b##c"));
            assert_eq!(c.disk_len(), 1);
            let on_disk = f.tmp().join("a##b");
            assert!(on_disk.exists());

            let _ = c.remove("a##b");
            assert!(!c.contains_key("a##b"));
            assert!(!on_disk.exists());
            assert_eq!(c.disk_len(), 0);
            assert_eq!(c.mem_len(), 1);
            assert_eq!(c.mem_size(), 11);
        });
    }

    #[test]
    fn test_meta_sadd() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let f = TempRoot::new();
            let c = Db::new(f.tmp(), Some(20)).unwrap();
            let b1 = Bytes::from(vec![0; 10]);
            let b2 = Bytes::from(vec![0; 11]);
            let ret = c.meta_sadd("a##b", &vec![b1.clone()]);
            assert_eq!(ret, 1);
            let ret = c.meta_sadd("a##b", &vec![b2.clone()]);
            assert_eq!(ret, 1);
            let ret = c.meta_smembers("a##b");
            assert_eq!(ret.len(), 2);
            assert!(ret.contains(&b1));
            assert!(ret.contains(&b2));
        });
    }

    #[test]
    fn test_meta_sadd_duplicate() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let f = TempRoot::new();
            let c = Db::new(f.tmp(), Some(20)).unwrap();
            let b1 = Bytes::from(vec![0; 10]);
            let b2 = Bytes::from(vec![0; 10]);
            let ret = c.meta_sadd("a##b", &vec![b1.clone()]);
            assert_eq!(ret, 1);
            let ret = c.meta_sadd("a##b", &vec![b2.clone()]);
            assert_eq!(ret, 0);
        });
    }

    #[test]
    fn test_meta_srem() {
        let mut rt = Runtime::new().unwrap();
        rt.block_on(async {
            let f = TempRoot::new();
            let c = Db::new(f.tmp(), Some(20)).unwrap();
            let b1 = Bytes::from(vec![0; 10]);
            let b2 = Bytes::from(vec![0; 11]);
            let ret = c.meta_sadd("a##b", &vec![b1.clone(), b2.clone()]);
            assert_eq!(ret, 2);
            let ret = c.meta_srem("a##b", &vec![b1.clone()]);
            assert_eq!(ret, 1);
            let ret = c.meta_srem("a##b", &vec![b2.clone()]);
            assert_eq!(ret, 1);

            // remove a value doesn't exists
            let ret = c.meta_srem("a##b", &vec![b1.clone()]);
            assert_eq!(ret, 0);

            // try to remove from a non existing key
            let ret = c.meta_srem("b##b", &vec![b1.clone()]);
            assert_eq!(ret, 0);
        });
    }
}

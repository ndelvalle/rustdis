use bytes::Bytes;
use num_traits::{ToPrimitive, Zero};
use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::ops::AddAssign;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;
use tokio::time::{sleep_until, Duration, Instant};

/// The Store is responsible for managing key-value pairs, with optional time-to-live settings for
/// each key. It automatically handles the expiration and removal of keys when their TTLs elapse.
/// The store is designed to be thread-safe, allowing it to be shared and cloned cheaply using
/// reference counting.
#[derive(Clone)]
pub struct Store {
    inner: Arc<InnerStore>,
}

impl Store {
    pub fn new() -> Store {
        let state = State {
            keys: HashMap::new(),
            ttls: BTreeSet::new(),
        };

        let waker = Notify::new();
        let inner = Arc::new(InnerStore {
            state: Mutex::new(state),
            waker,
        });

        tokio::spawn({
            let inner = inner.clone();
            async move { remove_expired_keys(inner).await }
        });

        Self { inner }
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

pub struct InnerStore {
    state: Mutex<State>,
    waker: Notify,
}

pub struct InnerStoreLocked<'a> {
    state: MutexGuard<'a, State>,
    waker: &'a Notify,
}

impl<'a> InnerStoreLocked<'a> {
    pub fn set(&mut self, key: String, data: Bytes) {
        // Ensure any previous TTL is removed.
        let removed = self.remove(&key);

        let created_at = removed.map(|v| v.created_at).unwrap_or(Instant::now());

        let value = Value {
            data,
            expires_at: None,
            created_at,
        };
        self.state.keys.insert(key, value);
    }

    pub fn set_with_ttl(&mut self, key: Key, data: Bytes, ttl: Duration) {
        // Ensure any previous TTL is removed.
        let removed = self.remove(&key);

        let created_at = removed.map(|v| v.created_at).unwrap_or(Instant::now());

        let expires_at = Instant::now() + ttl;
        let value = Value {
            data,
            expires_at: Some(expires_at),
            created_at,
        };

        self.state.keys.insert(key.clone(), value);
        self.state.ttls.insert((expires_at, key.clone()));

        let next_to_expire = self.state.ttls.iter().next().map(|(_, key)| key);
        let expires_next = next_to_expire == Some(&key);
        if expires_next {
            self.waker.notify_one();
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.state.keys.get(key).map(|v| v.data.clone())
    }

    pub fn get_ttl(&self, key: &str) -> Option<Duration> {
        self.state
            .keys
            .get(key)
            .and_then(|v| v.expires_at)
            .map(|expires_at| {
                let now = Instant::now();
                if expires_at > now {
                    expires_at - now
                } else {
                    Duration::from_secs(0)
                }
            })
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        match self.state.keys.remove(key) {
            None => None,
            Some(value) => match value.expires_at {
                Some(expires_at) => {
                    self.state.ttls.remove(&(expires_at, key.to_string()));
                    Some(value)
                }
                None => Some(value),
            },
        }
    }

    pub fn remove_ttl(&mut self, key: &str) {
        // Drop the immutable reference to `self.state` by cloning.
        if let Some(value) = self.state.keys.get(key).cloned() {
            if let Some(expires_at) = value.expires_at {
                self.state.ttls.remove(&(expires_at, key.to_string()));
                self.set(key.to_string(), value.data);
            }
        }
    }

    pub fn exists(&self, key: &str) -> bool {
        self.state.keys.contains_key(key)
    }

    pub fn size(&self) -> usize {
        self.state.keys.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.state.keys.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.state.keys.iter().map(|(key, value)| (key, value))
    }

    pub fn incr_by<T, R>(&mut self, key: &str, increment: T) -> Result<R, String>
    where
        T: AddAssign + FromStr + Display + Zero + ToPrimitive,
        R: FromStr,
    {
        let err = "value is not an integer or out of range";

        let mut value = match self.get(key) {
            Some(value) => match std::str::from_utf8(value.as_ref())
                .map_err(|_| err.to_string())
                .and_then(|s| s.parse::<T>().map_err(|_| err.to_string()))
            {
                Ok(v) => v,
                Err(e) => return Err(e),
            },
            None => T::zero(),
        };

        value += increment;

        let value = match value.to_f64() {
            // Format as an integer if no fractional part.
            Some(v) if v.fract() == 0.0 => format!("{:.0}", v),
            // Format as a float with up to 17 digits of precision.
            Some(v) => format!("{:.17}", v),
            // This shouldn't happen since we're only using ints and floats, but ideally, a trait
            // would enforce this at compile time.
            None => return Err(err.to_string()),
        };

        self.set(key.to_string(), value.clone().into());

        value.parse::<R>().map_err(|_| err.to_string())
    }

    fn remove_expired_keys(&mut self) -> Option<Instant> {
        let now = Instant::now();

        let expired_keys: Vec<(Instant, String)> = self
            .state
            .ttls
            .iter()
            .take_while(|(expires_at, _)| expires_at <= &now)
            .cloned()
            .collect();

        for (when, key) in expired_keys {
            self.remove(&key);
            self.state.ttls.remove(&(when, key));
        }

        self.state
            .ttls
            .iter()
            .next()
            .map(|&(expires_at, _)| expires_at)
    }
}

impl Deref for Store {
    type Target = InnerStore;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl InnerStore {
    pub fn lock<'a>(&'a self) -> InnerStoreLocked<'a> {
        let state = self.state.lock().unwrap();
        InnerStoreLocked {
            state,
            waker: &self.waker,
        }
    }
}

type Key = String;

#[derive(Debug, Clone)]
pub struct Value {
    pub data: Bytes,
    pub expires_at: Option<Instant>,
    pub created_at: Instant,
}

pub struct State {
    keys: HashMap<Key, Value>,
    ttls: BTreeSet<(Instant, Key)>,
}

async fn remove_expired_keys(store: Arc<InnerStore>) {
    loop {
        let (next_expiration, waker) = {
            let mut store = store.lock();
            let next_expiration = store.remove_expired_keys();
            (next_expiration, store.waker)
        };

        if let Some(next_expiration) = next_expiration {
            tokio::select! {
                _ = sleep_until(next_expiration) => {}
                _ = waker.notified() => {}
            }
        } else {
            waker.notified().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time;
    use tokio::time::Duration;

    #[tokio::test]
    async fn ttl() {
        time::pause();

        let store = Store::new();

        {
            let mut store = store.lock();

            store.set_with_ttl(
                "key1".to_string(),
                Bytes::from("value1"),
                Duration::from_secs(10),
            );

            store.set_with_ttl(
                "key2".to_string(),
                Bytes::from("value2"),
                Duration::from_secs(20),
            );
        }

        assert_eq!(store.lock().keys().count(), 2);

        time::advance(Duration::from_secs(10)).await;
        time::sleep(Duration::from_millis(1)).await;

        assert_eq!(store.lock().keys().count(), 1);
        assert!(store.lock().exists("key2"));

        time::advance(Duration::from_secs(20)).await;
        time::sleep(Duration::from_millis(1)).await;
        assert_eq!(store.lock().keys().count(), 0);

        {
            let mut store = store.lock();

            store.set_with_ttl(
                "key3".to_string(),
                Bytes::from("value3"),
                Duration::from_secs(20),
            );
        }

        assert_eq!(store.lock().keys().count(), 1);

        time::advance(Duration::from_secs(20)).await;
        time::sleep(Duration::from_millis(1)).await;
        assert_eq!(store.lock().keys().count(), 0);
    }

    #[tokio::test]
    async fn ttl_when_removing_keys() {
        time::pause();

        let store = Store::new();

        {
            let mut store = store.lock();

            store.set_with_ttl(
                "key1".to_string(),
                Bytes::from("value1"),
                Duration::from_secs(10),
            );
            store.remove("key1");
            store.set("key1".to_string(), Bytes::from("value2"));
        }

        assert_eq!(store.lock().keys().count(), 1);

        time::advance(Duration::from_secs(10)).await;
        time::sleep(Duration::from_millis(1)).await;

        assert_eq!(store.lock().keys().count(), 1);
        assert!(store.lock().exists("key1"));
    }
}

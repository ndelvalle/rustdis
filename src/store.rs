use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::ops::AddAssign;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;
use tokio::time::{sleep_until, Duration, Instant};

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

impl Deref for Store {
    type Target = InnerStore;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl InnerStore {
    pub fn lock(&self) -> MutexGuard<State> {
        self.state.lock().unwrap()
    }

    pub fn set2(&self, key: Key, value: NewValue) {
        let mut state = self.lock();
        state.set2(key, value);
        self.waker.notify_one();
    }

    pub fn incr_by<T>(&self, key: &str, increment: T) -> Result<T, String>
    where
        T: FromStr + ToString + AddAssign + Default,
    {
        let err = "value is not of the correct type or out of range".to_string();
        let mut state = self.lock();

        let mut value = match state.get(key) {
            Some(value) => match std::str::from_utf8(value.as_ref())
                .map_err(|_| err.clone())
                .and_then(|s| s.parse::<T>().map_err(|_| err.clone()))
            {
                Ok(value) => value,
                Err(e) => return Err(e),
            },
            None => T::default(),
        };

        value += increment;

        state.set(key.to_string(), value.to_string().into());

        Ok(value)
    }

    pub fn remove_expired_keys(&self) -> Option<Instant> {
        let mut state = self.lock();
        let now = Instant::now();

        let expired_keys: Vec<(Instant, String)> = state
            .ttls
            .iter()
            .take_while(|(expires_at, _)| expires_at <= &now)
            .cloned()
            .collect();

        for (when, key) in expired_keys {
            state.remove(&key);
            state.ttls.remove(&(when, key));
        }

        state.ttls.iter().next().map(|&(expires_at, _)| expires_at)
    }
}

type Key = String;

pub struct Value {
    pub data: Bytes,
    pub expires_at: Option<Instant>,
}

pub struct NewValue {
    pub data: Bytes,
    pub ttl: Option<Duration>,
}

impl Value {
    pub fn new(value: Bytes) -> Value {
        Value {
            data: value,
            expires_at: None,
        }
    }
}

pub struct State {
    keys: HashMap<Key, Value>,
    ttls: BTreeSet<(Instant, String)>,
}

impl State {
    pub fn set(&mut self, key: String, value: Bytes) {
        self.keys.insert(key, Value::new(value));
    }

    pub fn set2(&mut self, key: String, value: NewValue) {
        let ttl = value.ttl;
        let expires_at = ttl.map(|ttl| Instant::now() + ttl);
        let value = Value {
            data: value.data,
            expires_at,
        };
        self.keys.insert(key.clone(), value);
        if let Some(expires_at) = expires_at {
            self.ttls.insert((expires_at, key));
        }
    }

    pub fn get(&self, key: &str) -> Option<Bytes> {
        self.keys.get(key).map(|v| v.data.clone())
    }

    pub fn remove(&mut self, key: &str) -> Option<Value> {
        self.keys.remove(key)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.keys.contains_key(key)
    }

    pub fn size(&self) -> usize {
        self.keys.len()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.keys.keys()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Bytes)> {
        self.keys.iter().map(|(key, value)| (key, &value.data))
    }
}

async fn remove_expired_keys(store: Arc<InnerStore>) {
    loop {
        let next_expiration = { store.remove_expired_keys() };

        if let Some(next_expiration) = next_expiration {
            tokio::select! {
                _ = sleep_until(next_expiration) => {}
                _ = store.waker.notified() => {}
            }
        } else {
            store.waker.notified().await;
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

        store.set2(
            "key1".to_string(),
            NewValue {
                data: Bytes::from("value1"),
                ttl: Some(Duration::from_secs(10)),
            },
        );

        store.set2(
            "key2".to_string(),
            NewValue {
                data: Bytes::from("value2"),
                ttl: Some(Duration::from_secs(20)),
            },
        );

        assert_eq!(store.lock().keys().count(), 2);

        time::advance(Duration::from_secs(10)).await;
        time::sleep(Duration::from_millis(1)).await;

        assert_eq!(store.lock().keys().count(), 1);
        assert!(store.lock().exists("key2"));

        time::advance(Duration::from_secs(20)).await;
        time::sleep(Duration::from_millis(1)).await;
        assert_eq!(store.lock().keys().count(), 0);

        store.set2(
            "key3".to_string(),
            NewValue {
                data: Bytes::from("value3"),
                ttl: Some(Duration::from_secs(20)),
            },
        );

        assert_eq!(store.lock().keys().count(), 1);

        time::advance(Duration::from_secs(20)).await;
        time::sleep(Duration::from_millis(1)).await;
        assert_eq!(store.lock().keys().count(), 0);
    }
}

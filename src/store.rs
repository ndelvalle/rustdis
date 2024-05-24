use bytes::Bytes;
use std::collections::{BTreeSet, HashMap};
use std::ops::AddAssign;
use std::str::FromStr;
use std::sync::OnceLock;
use std::sync::{Arc, Mutex};
use tokio::sync::Notify;
use tokio::time::{sleep_until, Instant};

fn ttl_background_job_waker() -> &'static Notify {
    static NOTIFY: OnceLock<Notify> = OnceLock::new();
    NOTIFY.get_or_init(Notify::new)
}

type Key = String;

struct Value {
    data: Bytes,
    ttl: Option<Instant>,
}

impl Value {
    pub fn new(value: Bytes) -> Value {
        Value {
            data: value,
            ttl: None,
        }
    }
}

pub struct Store {
    store: HashMap<Key, Value>,
    ttls: BTreeSet<(Instant, String)>,
}

impl Store {
    pub fn new() -> Store {
        let store = Store {
            store: HashMap::new(),
            ttls: BTreeSet::new(),
        };

        tokio::spawn(async move { expire_keys });

        store
    }

    pub fn remove_expired_keys(&mut self) -> Option<Instant> {
        let now = Instant::now();
        while let Some((ttl, key)) = self.ttls.pop_first() {
            if ttl > now {
                return Some(ttl);
            }
            self.store.remove(&key);
        }
        None
    }

    pub fn set(&mut self, key: String, value: Bytes) {
        self.store.insert(key, Value::new(value));
    }

    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.store.get(key).map(|v| &v.data)
    }

    pub fn remove(&mut self, key: &str) -> Option<Bytes> {
        let value = self.store.remove(key)?;
        if let Some(ttl) = value.ttl {
            self.ttls.remove(&(ttl, key.to_string()));
        }
        Some(value.data)
    }

    pub fn exists(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.store.keys()
    }

    pub fn size(&self) -> usize {
        self.store.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &Bytes)> {
        self.store.iter().map(|(key, value)| (key, &value.data))
    }

    pub fn incr_by<T>(&mut self, key: &str, increment: T) -> Result<T, String>
    where
        T: FromStr + ToString + AddAssign + Default,
    {
        let err = "value is not of the correct type or out of range".to_string();

        let mut value = match self.get(key) {
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

        self.set(key.to_string(), value.to_string().into());

        Ok(value)
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

async fn expire_keys(store: Arc<Mutex<Store>>) {
    let waker = ttl_background_job_waker();
    loop {
        let next_expiration = {
            let mut store = store.lock().unwrap();
            store.remove_expired_keys()
        };

        if let Some(next_expiration) = next_expiration {
            sleep_until(next_expiration).await;
        } else {
            waker.notified().await;
        }
    }
}

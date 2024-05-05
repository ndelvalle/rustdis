use bytes::Bytes;
use std::collections::HashMap;
use std::ops::AddAssign;
use std::str::FromStr;

pub struct Store {
    store: HashMap<String, Bytes>,
}

impl Store {
    pub fn new() -> Store {
        Store {
            store: HashMap::new(),
        }
    }

    pub fn set(&mut self, key: String, value: Bytes) {
        self.store.insert(key, value);
    }

    pub fn get(&self, key: &str) -> Option<&Bytes> {
        self.store.get(key)
    }

    pub fn remove(&mut self, key: &str) -> Option<Bytes> {
        self.store.remove(key)
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
        self.store.iter()
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

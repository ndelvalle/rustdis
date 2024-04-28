use bytes::Bytes;
use std::collections::HashMap;

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

    pub fn incr_by(&mut self, key: &str, increment: i64) -> Result<i64, String> {
        let err = "value is not an integer or out of range".to_string();

        let value = match self.get(key) {
            Some(value) => match std::str::from_utf8(value.as_ref())
                .map_err(|_| err.clone())
                .and_then(|s| s.parse::<i64>().map_err(|_| err))
            {
                Ok(value) => value,
                Err(_) => return Err("value is not an integer or out of range".to_string()),
            },
            None => 0,
        };

        let new_value = value + increment;

        self.set(key.to_string(), new_value.to_string().into());

        Ok(new_value)
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

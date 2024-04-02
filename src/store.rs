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

    pub fn exists(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

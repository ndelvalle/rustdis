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
}
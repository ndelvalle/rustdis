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
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

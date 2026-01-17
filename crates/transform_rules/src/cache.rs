use std::collections::{HashMap, VecDeque};
use std::hash::Hash;

pub struct LruCache<K, V> {
    map: HashMap<K, V>,
    order: VecDeque<K>,
    capacity: usize,
}

impl<K, V> LruCache<K, V>
where
    K: Eq + Hash + Clone,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    pub fn get_cloned(&mut self, key: &K) -> Option<V>
    where
        V: Clone,
    {
        let value = self.map.get(key)?.clone();
        self.touch(key);
        Some(value)
    }

    pub fn insert(&mut self, key: K, value: V) {
        if self.capacity == 0 {
            return;
        }

        if self.map.contains_key(&key) {
            self.map.insert(key.clone(), value);
            self.touch(&key);
            return;
        }

        if self.map.len() >= self.capacity {
            if let Some(evicted) = self.order.pop_front() {
                self.map.remove(&evicted);
            }
        }

        self.order.push_back(key.clone());
        self.map.insert(key, value);
    }

    fn touch(&mut self, key: &K) {
        if let Some(pos) = self.order.iter().position(|k| k == key) {
            self.order.remove(pos);
        }
        self.order.push_back(key.clone());
    }
}

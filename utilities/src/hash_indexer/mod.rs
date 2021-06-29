use std::collections::HashMap;

#[derive(Debug)]
pub struct HashIndexer<T> {
    items: HashMap<u32, T>,
    empty_idx: Vec<u32>,
    top_idx: u32,
}

impl<T> HashIndexer<T> {
    pub fn new() -> Self {
        HashIndexer {
            items: HashMap::new(),
            empty_idx: vec![],
            top_idx: 0,
        }
    }

    pub fn insert(&mut self, item: T) -> u32 {
        let index = if self.empty_idx.is_empty() {
            let index = self.top_idx;
            self.top_idx += 1;
            index
        } else {
            self.empty_idx.pop().unwrap()
        };

        self.items.insert(index, item);
        index
    }

    pub fn remove(&mut self, id: u32) -> Option<T> {
        let item = self.items.remove(&id)?;
        self.empty_idx.push(id);
        Some(item)
    }

    pub fn items(&self) -> &HashMap<u32, T> {
        &self.items
    }

    pub fn into_items(self) -> HashMap<u32, T> {
        self.items
    }
}

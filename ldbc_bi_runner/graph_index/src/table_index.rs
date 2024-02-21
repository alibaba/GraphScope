use std::any::Any;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

use crate::types::*;

pub trait TableIndex: Debug {
    fn get_type(&self) -> DataType;
    fn get(&self, key: u64) -> Option<RefItem>;
    fn insert(&mut self, key: u64, val: Item);
    fn len(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

pub struct Int32TableIndex {
    pub data: HashMap<u64, i32>,
}

impl Int32TableIndex {
    pub fn new() -> Self {
        Int32TableIndex { data: HashMap::<u64, i32>::new() }
    }
}

impl Debug for Int32TableIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int32TableIndex: {:?}", self.data)
    }
}

impl TableIndex for Int32TableIndex {
    fn get_type(&self) -> DataType {
        DataType::Int32
    }

    fn get(&self, key: u64) -> Option<RefItem> {
        self.data.get(&key).map(|x| RefItem::Int32(x))
    }

    fn insert(&mut self, key: u64, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data.insert(key, v);
            }
            _ => {}
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct UInt64TableIndex {
    pub data: HashMap<u64, u64>,
}

impl UInt64TableIndex {
    pub fn new() -> Self {
        UInt64TableIndex { data: HashMap::<u64, u64>::new() }
    }
}

impl Debug for UInt64TableIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int32TableIndex: {:?}", self.data)
    }
}

impl TableIndex for UInt64TableIndex {
    fn get_type(&self) -> DataType {
        DataType::UInt64
    }

    fn get(&self, key: u64) -> Option<RefItem> {
        self.data.get(&key).map(|x| RefItem::UInt64(x))
    }

    fn insert(&mut self, key: u64, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data.insert(key, v);
            }
            _ => {}
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

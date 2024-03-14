use std::any::Any;
use std::fmt::{Debug, Formatter};

use crate::types::*;

pub trait ArrayIndex: Debug {
    fn get_type(&self) -> DataType;
    fn get(&self, index: usize) -> Option<RefItem>;
    fn get_data(&self) -> ArrayDataRef<'_>;
    fn set(&mut self, index: u64, val: Item);
    fn set_unsafe(&self, index: u64, val: Item);
    fn set_batch(&mut self, index: &Vec<usize>, data: ArrayDataRef);
    fn push(&mut self, val: Item);
    fn len(&self) -> usize;
    fn as_any(&self) -> &dyn Any;
}

pub struct Int32ArrayIndex {
    pub data: Vec<i32>,
}

impl Int32ArrayIndex {
    pub fn new(length: usize, default: Option<Item>) -> Self {
        if let Some(Item::Int32(default)) = default {
            Int32ArrayIndex { data: vec![default; length] }
        } else {
            Int32ArrayIndex { data: vec![0; length] }
        }
    }
}

impl Debug for Int32ArrayIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Int32ArrayIndex: {:?}", self.data)
    }
}

impl ArrayIndex for Int32ArrayIndex {
    fn get_type(&self) -> DataType {
        DataType::Int32
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::Int32(x))
    }

    fn get_data(&self) -> ArrayDataRef<'_> {
        ArrayDataRef::Int32Array(&self.data)
    }

    fn set(&mut self, index: u64, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data[index as usize] = v;
            }
            _ => {
                self.data[index as usize] = 0;
            }
        }
    }

    fn set_unsafe(&self, index: u64, val: Item) {
        unsafe {
            let data: &mut Vec<i32> = &mut *(&self.data as *const Vec<i32> as *mut Vec<i32>);
            match val {
                Item::Int32(v) => {
                    data[index as usize] = v;
                }
                _ => {
                    data[index as usize] = 0;
                }
            }
        }
    }

    fn set_batch(&mut self, index: &Vec<usize>, data: ArrayDataRef) {
        match data {
            ArrayDataRef::Int32Array(data) => {
                assert_eq!(index.len(), data.len());
                for i in 0..index.len() {
                    self.data[index[i]] = data[i];
                }
            }
            _ => {}
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::Int32(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

pub struct UInt64ArrayIndex {
    pub data: Vec<u64>,
}

impl UInt64ArrayIndex {
    pub fn new(length: usize) -> Self {
        UInt64ArrayIndex { data: vec![0; length] }
    }
}

impl Debug for UInt64ArrayIndex {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "UInt64ArrayIndex: {:?}", self.data)
    }
}

impl ArrayIndex for UInt64ArrayIndex {
    fn get_type(&self) -> DataType {
        DataType::UInt64
    }

    fn get(&self, index: usize) -> Option<RefItem> {
        self.data.get(index).map(|x| RefItem::UInt64(x))
    }

    fn get_data(&self) -> ArrayDataRef<'_> {
        ArrayDataRef::Uint64Array(&self.data)
    }

    fn set(&mut self, index: u64, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data[index as usize] = v;
            }
            _ => {
                self.data[index as usize] = 0;
            }
        }
    }

    fn set_unsafe(&self, index: u64, val: Item) {
        unsafe {
            let data: &mut Vec<u64> = &mut *(&self.data as *const Vec<u64> as *mut Vec<u64>);
            match val {
                Item::UInt64(v) => {
                    data[index as usize] = v;
                }
                _ => {
                    data[index as usize] = 0;
                }
            }
        }
    }

    fn set_batch(&mut self, index: &Vec<usize>, data: ArrayDataRef) {
        match data {
            ArrayDataRef::Uint64Array(data) => {
                assert_eq!(index.len(), data.len());
                for i in 0..index.len() {
                    self.data[index[i]] = data[i];
                }
            }
            _ => {}
        }
    }

    fn push(&mut self, val: Item) {
        match val {
            Item::UInt64(v) => {
                self.data.push(v);
            }
            _ => {
                self.data.push(0);
            }
        }
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

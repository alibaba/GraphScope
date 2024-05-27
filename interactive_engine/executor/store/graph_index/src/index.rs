use std::any;

use crate::array_index::*;
use crate::table_index::*;
use crate::types::{ArrayDataRef, DataType, Item, LabelId, RefItem};

/*pub struct IndexMap {
    pub vertex_index: ShardedLock<HashMap<u64, VertexIndex>>,
    pub edge_index: ShardedLock<HashMap<u64, EdgeIndex>>,
}*/

pub struct VertexIndex {
    index_map: Option<Box<dyn TableIndex>>,
    pub index_array: Option<Box<dyn ArrayIndex>>,
    data_type: DataType,
    is_internal_id: bool,
}

impl VertexIndex {
    pub fn new(
        data_type: DataType, is_internal_id: bool, length: Option<usize>, default: Option<Item>,
    ) -> Self {
        if is_internal_id {
            match data_type {
                DataType::Int32 => VertexIndex {
                    index_map: None,
                    index_array: Some(Box::new(Int32ArrayIndex::new(length.unwrap(), None))),
                    data_type,
                    is_internal_id,
                },
                DataType::UInt64 => VertexIndex {
                    index_map: None,
                    index_array: Some(Box::new(UInt64ArrayIndex::new(length.unwrap()))),
                    data_type,
                    is_internal_id,
                },
                _ => VertexIndex { index_map: None, index_array: None, data_type, is_internal_id },
            }
        } else {
            match data_type {
                DataType::Int32 => VertexIndex {
                    index_map: Some(Box::new(Int32TableIndex::new())),
                    index_array: None,
                    data_type,
                    is_internal_id,
                },
                DataType::UInt64 => VertexIndex {
                    index_map: Some(Box::new(UInt64TableIndex::new())),
                    index_array: None,
                    data_type,
                    is_internal_id,
                },
                _ => VertexIndex { index_map: None, index_array: None, data_type, is_internal_id },
            }
        }
    }

    pub fn add_index(&mut self, vertex_id: u64, index: Item) {
        if self.is_internal_id {
        } else {
            self.index_map
                .as_mut()
                .unwrap()
                .insert(vertex_id, index);
        }
    }

    pub fn add_index_unsafe(&self, vertex_id: u64, index: Item) {
        if self.is_internal_id {
            self.index_array
                .as_ref()
                .unwrap()
                .set_unsafe(vertex_id, index)
        } else {
            panic!("TableIndex does not support unsafe write");
        }
    }

    pub fn add_index_batch(&mut self, index: &Vec<usize>, data: ArrayDataRef) {
        if self.index_array.is_some() {
            self.index_array
                .as_mut()
                .unwrap()
                .set_batch(index, data);
        } else {
            panic!("Unsupported type for add batch")
        }
    }

    pub fn get_index(&self, vertex_id: usize) -> Option<RefItem> {
        if self.is_internal_id {
            self.index_array
                .as_ref()
                .unwrap()
                .get(vertex_id)
        } else {
            None
        }
    }
}

unsafe impl Send for VertexIndex {}

unsafe impl Sync for VertexIndex {}

pub struct EdgeIndex {
    index_map: Option<Box<dyn TableIndex>>,
    pub index_array: Option<Box<dyn ArrayIndex>>,
    data_type: DataType,
    is_internal_id: bool,
}

impl EdgeIndex {
    pub fn new(data_type: DataType, length: Option<usize>, default: Option<Item>) -> Self {
        if let Some(index_length) = length {
            match data_type {
                DataType::Int32 => EdgeIndex {
                    index_map: None,
                    index_array: Some(Box::new(Int32ArrayIndex::new(index_length, default))),
                    data_type,
                    is_internal_id: true,
                },
                DataType::UInt64 => EdgeIndex {
                    index_map: None,
                    index_array: Some(Box::new(UInt64ArrayIndex::new(index_length))),
                    data_type,
                    is_internal_id: true,
                },
                _ => EdgeIndex { index_map: None, index_array: None, data_type, is_internal_id: true },
            }
        } else {
            match data_type {
                DataType::Int32 => EdgeIndex {
                    index_map: Some(Box::new(Int32TableIndex::new())),
                    index_array: None,
                    data_type,
                    is_internal_id: false,
                },
                DataType::UInt64 => EdgeIndex {
                    index_map: Some(Box::new(UInt64TableIndex::new())),
                    index_array: None,
                    data_type,
                    is_internal_id: false,
                },
                _ => EdgeIndex { index_map: None, index_array: None, data_type, is_internal_id: false },
            }
        }
    }

    pub fn add_index_batch(&mut self, index: &Vec<usize>, data: ArrayDataRef) {
        if self.index_array.is_some() {
            self.index_array
                .as_mut()
                .unwrap()
                .set_batch(index, data);
        } else {
            panic!("Unsupported type for add batch")
        }
    }

    pub fn get_index(&self, offset: usize) -> Option<RefItem> {
        if self.is_internal_id {
            self.index_array.as_ref().unwrap().get(offset)
        } else {
            None
        }
    }

    pub fn get_index_batch(&self) -> ArrayDataRef<'_> {
        self.index_array.as_ref().unwrap().get_data()
    }
}

unsafe impl Send for EdgeIndex {}

unsafe impl Sync for EdgeIndex {}

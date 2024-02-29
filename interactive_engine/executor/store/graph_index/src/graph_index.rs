use std::collections::HashMap;
use std::sync::{RwLock, RwLockReadGuard};

use crossbeam_utils::sync::ShardedLock;

use crate::index::*;
use crate::schema::IndexSchema;
use crate::types::*;

fn encode_vertex_index(vertex_label: LabelId, index_label: LabelId) -> u64 {
    let vertex_index = ((vertex_label as u64) << 32) + index_label as u64;
    vertex_index
}

fn encode_edge_index(
    src_label: LabelId, edge_label: LabelId, dst_label: LabelId, property_id: LabelId,
) -> u64 {
    let edge_index = ((src_label as u64) << 48)
        + ((edge_label as u64) << 32)
        + ((dst_label as u64) << 16)
        + property_id as u64;
    edge_index
}

pub struct GraphIndex {
    pub partition: usize,
    pub index_schema: ShardedLock<IndexSchema>,

    pub vertex_index: ShardedLock<HashMap<u64, VertexIndex>>,
    pub edge_index: ShardedLock<HashMap<u64, EdgeIndex>>,
}

impl GraphIndex {
    pub fn new(partition: usize) -> Self {
        GraphIndex {
            partition,
            index_schema: ShardedLock::new(IndexSchema::new()),
            vertex_index: ShardedLock::new(HashMap::new()),
            edge_index: ShardedLock::new(HashMap::new()),
        }
    }

    pub fn init_vertex_index(
        &self, index_name: String, vertex_label: LabelId, data_type: DataType, use_internal: Option<usize>,
        default: Option<Item>,
    ) -> Option<LabelId> {
        let mut schema_locked = self
            .index_schema
            .write()
            .expect("index_schema lock write poisoned");
        if let Some(index_label) = schema_locked.add_vertex_index(index_name, vertex_label, data_type) {
            let vertex_index_id = encode_vertex_index(vertex_label, index_label);
            let mut index_locked = self
                .vertex_index
                .write()
                .expect("vertex_index lock write poisoned");
            if let Some(index_length) = use_internal {
                index_locked.insert(
                    vertex_index_id,
                    VertexIndex::new(data_type, true, Some(index_length), default),
                );
            } else {
                index_locked.insert(vertex_index_id, VertexIndex::new(data_type, false, None, default));
            }
            Some(index_label)
        } else {
            None
        }
    }

    pub fn init_edge_index(
        &self, index_name: String, src_label: LabelId, dst_label: LabelId, edge_label: LabelId,
        data_type: DataType, use_internal: Option<usize>, default: Option<Item>,
    ) -> Option<LabelId> {
        let mut schema_locked = self
            .index_schema
            .write()
            .expect("index_schema lock write poisoned");
        if let Some(index_label) =
            schema_locked.add_edge_index(index_name, edge_label, src_label, dst_label, data_type)
        {
            let edge_index_id = encode_edge_index(src_label, edge_label, dst_label, index_label);
            let mut index_locked = self
                .edge_index
                .write()
                .expect("edge_index lock write poisoned");
            if let Some(index_length) = use_internal {
                index_locked.insert(edge_index_id, EdgeIndex::new(data_type, Some(index_length), default));
            } else {
                index_locked.insert(edge_index_id, EdgeIndex::new(data_type, None, default));
            }
            Some(index_label)
        } else {
            None
        }
    }

    pub fn add_vertex_index(&self, index_label: (LabelId, LabelId), vertex_id: u64, index: Item) {
        let vertex_index_id = encode_vertex_index(index_label.0, index_label.1);
        let mut locked = self
            .vertex_index
            .write()
            .expect("lock write poisoned");
        if let Some(vertex_index) = locked.get_mut(&vertex_index_id) {
            vertex_index.add_index(vertex_id, index);
        }
    }

    pub fn add_vertex_index_unsafe(&self, index_label: (LabelId, LabelId), vertex_id: u64, index: Item) {
        let vertex_index_id = encode_vertex_index(index_label.0, index_label.1);
        unsafe {
            let index_map_lock: &mut ShardedLock<HashMap<u64, VertexIndex>> = &mut *(&self.vertex_index
                as *const ShardedLock<HashMap<u64, VertexIndex>>
                as *mut ShardedLock<HashMap<u64, VertexIndex>>);
            let index_map = index_map_lock.get_mut().unwrap();
            if let Some(vertex_index) = index_map.get(&vertex_index_id) {
                vertex_index.add_index_unsafe(vertex_id, index);
            }
        }
    }

    pub fn add_vertex_index_batch(
        &self, vertex_label: LabelId, index_name: &String, index: &Vec<usize>, data: ArrayDataRef,
    ) -> Result<(), GraphIndexError> {
        let schema_locked = self
            .index_schema
            .read()
            .expect("index_schema lock write poisoned");
        let index_label = schema_locked
            .get_vertex_index(vertex_label, index_name)
            .expect("index name not found in edge index");
        let encode_label = encode_vertex_index(vertex_label, index_label);
        let mut index_locked = self
            .vertex_index
            .write()
            .expect("edge_index lock write poisoned");
        if let Some(vertex_index) = index_locked.get_mut(&encode_label) {
            vertex_index.add_index_batch(index, data);
            Ok(())
        } else {
            Err(GraphIndexError::UpdateFailure(format!("Failed to update index")))
        }
    }

    pub fn add_edge_index_batch(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, index_name: &String,
        index: &Vec<usize>, data: ArrayDataRef,
    ) -> Result<(), GraphIndexError> {
        let schema_locked = self
            .index_schema
            .read()
            .expect("index_schema lock write poisoned");
        let index_label = schema_locked
            .get_edge_index(edge_label, src_label, dst_label, index_name)
            .expect("index name not found in edge index");
        let encode_label = encode_edge_index(src_label, edge_label, dst_label, index_label);
        let mut index_locked = self
            .edge_index
            .write()
            .expect("edge_index lock write poisoned");
        if let Some(edge_index) = index_locked.get_mut(&encode_label) {
            edge_index.add_index_batch(index, data);
            Ok(())
        } else {
            Err(GraphIndexError::UpdateFailure(format!("Failed to update index")))
        }
    }

    pub fn get_vertex_index_label(
        &self, vertex_label: LabelId, vertex_index_name: &String,
    ) -> Option<LabelId> {
        let schema_locked = self
            .index_schema
            .read()
            .expect("index_schema read poisioned");
        schema_locked.get_vertex_index(vertex_label, vertex_index_name)
    }

    pub fn get_vertex_index(
        &self, vertex_label: LabelId, index_name: &String, offset: usize,
    ) -> Option<Item> {
        let schema_locked = self
            .index_schema
            .read()
            .expect("index_schema lock write poisoned");
        let index_label = schema_locked
            .get_vertex_index(vertex_label, index_name)
            .expect("index name not found in vertex index");
        let encode_label = encode_vertex_index(vertex_label, index_label);
        let index_locked = self
            .vertex_index
            .read()
            .expect("vertex_index lock read poisoned");
        if let Some(index) = index_locked.get(&encode_label) {
            if let Some(data) = index.get_index(offset) {
                Some(data.to_owned())
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn get_edge_index(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, index_name: &String,
        offset: usize,
    ) -> Option<Item> {
        let schema_locked = self
            .index_schema
            .read()
            .expect("index_schema lock write poisoned");
        let index_label = schema_locked
            .get_edge_index(edge_label, src_label, dst_label, index_name)
            .expect("index name not found in edge index");
        let encode_label = encode_edge_index(src_label, edge_label, dst_label, index_label);
        let index_locked = self
            .edge_index
            .read()
            .expect("edge_index lock write poisoned");
        if let Some(edge_index) = index_locked.get(&encode_label) {
            if let Some(data) = edge_index.get_index(offset) {
                Some(data.to_owned())
            } else {
                None
            }
        } else {
            None
        }
    }

    // pub fn get_edge_index_batch<'a>(
    //     &'a self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId, index_name: &String,
    // ) -> Option<ArrayDataRef<'_>> {
    //     let schema_locked = self
    //         .index_schema
    //         .read()
    //         .expect("index_schema lock write poisoned");
    //     let index_label = schema_locked
    //         .get_edge_index(edge_label, src_label, dst_label, index_name)
    //         .expect("index name not found in edge index");
    //     let encode_label = encode_edge_index(src_label, edge_label, dst_label, index_label);
    //     let mut index_locked: RwLockReadGuard<'a, HashMap<u64, EdgeIndex>> = self
    //         .edge_index
    //         .read()
    //         .expect("edge_index lock write poisoned");
    //     if let Some(edge_index) = index_locked.get(&encode_label) {
    //         Some(edge_index.get_index_batch())
    //     } else {
    //         None
    //     }
    // }
}

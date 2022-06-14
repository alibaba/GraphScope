//
//! Copyright 2021 Alibaba Group Holding Limited.
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

use dyn_type::{object, BorrowObject, Object};
use graph_store::common::LabelId;
use graph_store::config::{JsonConf, DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::ldbc::LDBCVertexParser;
use graph_store::prelude::{
    DefaultId, EdgeId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, LDBCGraphSchema,
    LargeGraphDB, LocalEdge, LocalVertex, MutableGraphDB, Row, INVALID_LABEL_ID,
};
use ir_common::{KeyId, NameOrId};
use pegasus::configure_with_default;
use pegasus_common::downcast::*;
use pegasus_common::impl_as_any;

use crate::apis::{
    from_fn, register_graph, DefaultDetails, Details, Direction, DynDetails, Edge, QueryParams, ReadGraph,
    Statement, Vertex, ID,
};
use crate::errors::{GraphProxyError, GraphProxyResult};
use crate::{filter_limit, limit_n};

lazy_static! {
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = _init_graph();
    static ref GRAPH_PROXY: Arc<ExpStore> = initialize();
}

pub struct ExpStore {
    store: &'static LargeGraphDB<DefaultId, InternalId>,
}

fn initialize() -> Arc<ExpStore> {
    lazy_static::initialize(&GRAPH);
    Arc::new(ExpStore { store: &GRAPH })
}

fn _init_graph() -> LargeGraphDB<DefaultId, InternalId> {
    if DATA_PATH.is_empty() {
        info!("Create and use the modern graph for demo.");
        _init_modern_graph()
    } else {
        info!("Read the graph data from {:?} for demo.", *DATA_PATH);
        GraphDBConfig::default()
            .root_dir(&(*DATA_PATH))
            .partition(*PARTITION_ID)
            .schema_file(
                &(DATA_PATH.as_ref() as &Path)
                    .join(DIR_GRAPH_SCHEMA)
                    .join(FILE_SCHEMA),
            )
            .open()
            .expect("Open graph error")
    }
}

fn _init_modern_graph() -> LargeGraphDB<DefaultId, InternalId> {
    let mut mut_graph: MutableGraphDB<DefaultId, InternalId> = GraphDBConfig::default().new();

    let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
    let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
    let v3: DefaultId = LDBCVertexParser::to_global_id(3, 1);
    let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);
    let v5: DefaultId = LDBCVertexParser::to_global_id(5, 1);
    let v6: DefaultId = LDBCVertexParser::to_global_id(6, 0);

    mut_graph.add_vertex(v1, [0, INVALID_LABEL_ID]);
    mut_graph.add_vertex(v2, [0, INVALID_LABEL_ID]);
    mut_graph.add_vertex(v3, [1, INVALID_LABEL_ID]);
    mut_graph.add_vertex(v4, [0, INVALID_LABEL_ID]);
    mut_graph.add_vertex(v5, [1, INVALID_LABEL_ID]);
    mut_graph.add_vertex(v6, [0, INVALID_LABEL_ID]);

    let prop7 = Row::from(vec![object!(0.5)]);
    let prop8 = Row::from(vec![object!(0.4)]);
    let prop9 = Row::from(vec![object!(1.0)]);
    let prop10 = Row::from(vec![object!(0.4)]);
    let prop11 = Row::from(vec![object!(1.0)]);
    let prop12 = Row::from(vec![object!(0.2)]);

    mut_graph
        .add_edge_with_properties(v1, v2, 0, prop7)
        .unwrap();
    mut_graph
        .add_edge_with_properties(v1, v3, 1, prop8)
        .unwrap();
    mut_graph
        .add_edge_with_properties(v1, v4, 0, prop9)
        .unwrap();
    mut_graph
        .add_edge_with_properties(v4, v3, 1, prop10)
        .unwrap();
    mut_graph
        .add_edge_with_properties(v4, v5, 1, prop11)
        .unwrap();
    mut_graph
        .add_edge_with_properties(v6, v3, 1, prop12)
        .unwrap();

    let prop1 = Row::from(vec![object!(1), object!("marko"), object!(29)]);
    let prop2 = Row::from(vec![object!(2), object!("vadas"), object!(27)]);
    let prop3 = Row::from(vec![object!(3), object!("lop"), object!("java")]);
    let prop4 = Row::from(vec![object!(4), object!("josh"), object!(32)]);
    let prop5 = Row::from(vec![object!(5), object!("ripple"), object!("java")]);
    let prop6 = Row::from(vec![object!(6), object!("peter"), object!(35)]);

    mut_graph
        .add_or_update_vertex_properties(v1, prop1)
        .unwrap();
    mut_graph
        .add_or_update_vertex_properties(v2, prop2)
        .unwrap();
    mut_graph
        .add_or_update_vertex_properties(v3, prop3)
        .unwrap();
    mut_graph
        .add_or_update_vertex_properties(v4, prop4)
        .unwrap();
    mut_graph
        .add_or_update_vertex_properties(v5, prop5)
        .unwrap();
    mut_graph
        .add_or_update_vertex_properties(v6, prop6)
        .unwrap();

    let modern_graph_schema = r#"
    {
      "vertex_type_map": {
        "person": 0,
        "software": 1
      },
      "edge_type_map": {
        "knows": 0,
        "created": 1
      },
      "vertex_prop": {
        "person": [
          [
            "id",
            "ID"
          ],
          [
            "name",
            "String"
          ],
          [
            "age",
            "Integer"
          ]
        ],
        "software": [
          [
            "id",
            "ID"
          ],
          [
            "name",
            "String"
          ],
          [
            "lang",
            "String"
          ]
        ]
      },
      "edge_prop": {
        "knows": [
          [
            "start_id",
            "ID"
          ],
          [
            "end_id",
            "ID"
          ],
          [
            "weight",
            "Double"
          ]
        ],
        "created": [
          [
            "start_id",
            "ID"
          ],
          [
            "end_id",
            "ID"
          ],
          [
            "weight",
            "Double"
          ]
        ]
      }
    }
    "#;
    let schema = LDBCGraphSchema::from_json(modern_graph_schema.to_string()).expect("Parse schema error!");

    mut_graph.into_graph(schema)
}

impl ReadGraph for ExpStore {
    fn scan_vertex(
        &self, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        // DemoGraph contains a single graph partition on each server,
        // therefore, there's no need to use the specific partition id for query.
        // Besides, we guarantee only one worker (on each server) is going to scan (with params.partitions.is_some())
        if params.partitions.is_some() {
            let label_ids = encode_storage_vertex_label(&params.labels);
            let store = self.store;
            let props = params.columns.clone();
            let result = self
                .store
                .get_all_vertices(label_ids.as_ref())
                .map(move |v| to_runtime_vertex(v, store, props.clone()));

            Ok(filter_limit!(result, params.filter, params.limit))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn index_scan_vertex(
        &self, _label: &NameOrId, _primary_key_values: &Vec<(NameOrId, Object)>, _params: &QueryParams,
    ) -> GraphProxyResult<Option<Vertex>> {
        Err(GraphProxyError::query_store_error(
            "Experiment storage does not support index_scan_vertex for now",
        ))?
    }

    fn scan_edge(&self, params: &QueryParams) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        if params.partitions.is_some() {
            let label_ids = encode_storage_edge_label(&params.labels);
            let store = self.store;
            let result = self
                .store
                .get_all_edges(label_ids.as_ref())
                .map(move |e| to_runtime_edge(e, store));

            Ok(filter_limit!(result, params.filter, params.limit))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(local_vertex) = self.store.get_vertex(*id as DefaultId) {
                let v = to_runtime_vertex(local_vertex, self.store, params.columns.clone());
                result.push(v);
            }
        }
        Ok(filter_limit!(result.into_iter(), params.filter, params.limit))
    }

    fn get_edge(
        &self, ids: &[ID], params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Iterator<Item = Edge> + Send>> {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let eid = encode_store_e_id(id);
            if let Some(local_edge) = self.store.get_edge(eid) {
                let e = to_runtime_edge(local_edge, self.store);
                result.push(e);
            }
        }
        Ok(filter_limit!(result.into_iter(), params.filter, params.limit))
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Vertex>>> {
        let edge_label_ids = encode_storage_edge_label(params.labels.as_ref());
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let graph = self.store;

        let stmt = from_fn(move |v: ID| {
            let iter = match direction {
                Direction::Out => graph.get_out_vertices(v as DefaultId, edge_label_ids.as_ref()),
                Direction::In => graph.get_in_vertices(v as DefaultId, edge_label_ids.as_ref()),
                Direction::Both => graph.get_both_vertices(v as DefaultId, edge_label_ids.as_ref()),
            }
            .map(move |v| to_runtime_vertex(v, graph, None));
            Ok(filter_limit!(iter, filter, limit))
        });
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams,
    ) -> GraphProxyResult<Box<dyn Statement<ID, Edge>>> {
        let edge_label_ids = encode_storage_edge_label(&params.labels);
        let filter = params.filter.clone();
        let limit = params.limit.clone();
        let graph = self.store;
        let stmt = from_fn(move |v: ID| {
            let iter = match direction {
                Direction::Out => graph.get_out_edges(v as DefaultId, edge_label_ids.as_ref()),
                Direction::In => graph.get_in_edges(v as DefaultId, edge_label_ids.as_ref()),
                Direction::Both => graph.get_both_edges(v as DefaultId, edge_label_ids.as_ref()),
            }
            .map(move |e| to_runtime_edge(e, graph));
            Ok(filter_limit!(iter, filter, limit))
        });
        Ok(stmt)
    }
}

#[allow(dead_code)]
pub fn create_exp_store() {
    lazy_static::initialize(&GRAPH_PROXY);
    register_graph(GRAPH_PROXY.clone());
}

#[inline]
fn to_runtime_vertex(
    v: LocalVertex<DefaultId>, store: &'static LargeGraphDB<DefaultId, InternalId>,
    prop_keys: Option<Vec<NameOrId>>,
) -> Vertex {
    // For vertices, we query properties via vid
    let label = encode_runtime_v_label(&v);
    let details = LazyVertexDetails::new(v.get_id(), prop_keys, store);
    Vertex::new(v.get_id() as ID, Some(label), DynDetails::new(details))
}

#[inline]
fn to_runtime_edge(
    e: LocalEdge<DefaultId, InternalId>, _store: &'static LargeGraphDB<DefaultId, InternalId>,
) -> Edge {
    let id = encode_runtime_e_id(&e);
    let label = encode_runtime_e_label(&e);
    let mut properties = HashMap::new();
    // TODO: For edges, we clone all properties by default for now. But we'd better get properties on demand
    if let Some(mut prop_vals) = e.clone_all_properties() {
        for (prop, obj) in prop_vals.drain() {
            properties.insert(prop.into(), obj as Object);
        }
    }

    Edge::with_from_src(
        id,
        Some(label),
        e.get_src_id() as ID,
        e.get_dst_id() as ID,
        e.is_from_start(),
        DynDetails::new(DefaultDetails::new(properties)),
    )
}

/// LazyVertexDetails is used for local property fetching optimization.
/// That is, the required properties will not be materialized until LazyVertexDetails need to be shuffled.
#[allow(dead_code)]
struct LazyVertexDetails {
    pub id: DefaultId,
    // prop_keys specify the properties we query for,
    // Specifically, Some(vec![]) indicates we need all properties
    // and None indicates we do not need any property,
    prop_keys: Option<Vec<NameOrId>>,
    inner: AtomicPtr<LocalVertex<'static, DefaultId>>,
    store: &'static LargeGraphDB<DefaultId, InternalId>,
}

impl_as_any!(LazyVertexDetails);

impl LazyVertexDetails {
    pub fn new(
        id: DefaultId, prop_keys: Option<Vec<NameOrId>>,
        store: &'static LargeGraphDB<DefaultId, InternalId>,
    ) -> Self {
        LazyVertexDetails { id, prop_keys, inner: AtomicPtr::default(), store }
    }
}

impl fmt::Debug for LazyVertexDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LazyVertexDetails")
            .field("id", &self.id)
            .field("inner", &self.inner)
            .finish()
    }
}

impl Details for LazyVertexDetails {
    fn get_property(&self, key: &NameOrId) -> Option<BorrowObject> {
        if let NameOrId::Str(key) = key {
            let mut ptr = self.inner.load(Ordering::SeqCst);
            if ptr.is_null() {
                if let Some(v) = self.store.get_vertex(self.id) {
                    let v = Box::new(v);
                    let new_ptr = Box::into_raw(v);
                    let swapped = self.inner.swap(new_ptr, Ordering::SeqCst);
                    if swapped.is_null() {
                        ptr = new_ptr;
                    } else {
                        unsafe {
                            std::ptr::drop_in_place(new_ptr);
                        }
                        ptr = swapped
                    };
                } else {
                    return None;
                }
            }

            unsafe { (*ptr).get_property(key) }
        } else {
            info!("Have not support getting property by prop_id in experiments store yet");
            None
        }
    }

    fn get_all_properties(&self) -> Option<HashMap<NameOrId, Object>> {
        let mut all_props = HashMap::new();
        if let Some(prop_keys) = self.prop_keys.as_ref() {
            // the case of get_all_properties from vertex;
            if prop_keys.is_empty() {
                let mut ptr = self.inner.load(Ordering::SeqCst);
                if ptr.is_null() {
                    if let Some(v) = self.store.get_vertex(self.id) {
                        let v = Box::new(v);
                        let new_ptr = Box::into_raw(v);
                        let swapped = self.inner.swap(new_ptr, Ordering::SeqCst);
                        if swapped.is_null() {
                            ptr = new_ptr;
                        } else {
                            unsafe {
                                std::ptr::drop_in_place(new_ptr);
                            }
                            ptr = swapped
                        };
                    } else {
                        return None;
                    }
                }
                unsafe {
                    if let Some(prop_key_vals) = (*ptr).clone_all_properties() {
                        all_props = prop_key_vals
                            .into_iter()
                            .map(|(prop_key, prop_val)| (prop_key.into(), prop_val as Object))
                            .collect();
                    } else {
                        return None;
                    }
                }
            } else {
                // the case of get_all_properties with prop_keys pre-specified
                for key in prop_keys.iter() {
                    if let Some(prop) = self.get_property(&key) {
                        all_props.insert(key.clone(), prop.try_to_owned().unwrap());
                    } else {
                        all_props.insert(key.clone(), Object::None);
                    }
                }
            }
        }
        Some(all_props)
    }
}

impl Drop for LazyVertexDetails {
    fn drop(&mut self) {
        let ptr = self.inner.load(Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe {
                std::ptr::drop_in_place(ptr);
            }
        }
    }
}

/// Edge's ID is encoded by its internal index
fn encode_runtime_e_id(e: &LocalEdge<DefaultId, InternalId>) -> ID {
    let ei = e.get_edge_id();
    ei.1 as ID
}

pub fn encode_store_e_id(e: &ID) -> EdgeId<DefaultId> {
    // TODO(longbin) To only use in current partition
    (0, *e as usize)
}

fn encode_runtime_v_label(v: &LocalVertex<DefaultId>) -> NameOrId {
    NameOrId::Id(v.get_label()[0] as KeyId)
}

fn encode_runtime_e_label(e: &LocalEdge<DefaultId, InternalId>) -> NameOrId {
    NameOrId::Id(e.get_label() as KeyId)
}

/// Transform string-typed labels into a id-typed labels.
/// `is_true_label` records whether the label is an actual label, or already transformed into
/// an id-type.
fn labels_to_ids(labels: &Vec<NameOrId>, is_vertex: bool) -> Option<Vec<LabelId>> {
    if labels.is_empty() {
        None
    } else {
        Some(
            labels
                .iter()
                .map(|label| match label {
                    NameOrId::Str(s) => {
                        let label_id = if is_vertex {
                            (*GRAPH).get_schema().get_vertex_label_id(s)
                        } else {
                            (*GRAPH)
                                .get_schema()
                                .get_edge_label_id(s)
                                .map(|id| id)
                        };
                        label_id.unwrap_or(INVALID_LABEL_ID)
                    }
                    NameOrId::Id(id) => *id as LabelId,
                })
                .collect::<Vec<LabelId>>(),
        )
    }
}

fn encode_storage_vertex_label(labels: &Vec<NameOrId>) -> Option<Vec<LabelId>> {
    labels_to_ids(labels, true)
}

fn encode_storage_edge_label(labels: &Vec<NameOrId>) -> Option<Vec<LabelId>> {
    labels_to_ids(labels, false)
}

#[cfg(test)]
mod tests {
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::{DefaultId, GlobalStoreTrait};

    use super::GRAPH;

    #[test]
    fn it_works() {
        let v1: DefaultId = LDBCVertexParser::to_global_id(1, 0);
        let v2: DefaultId = LDBCVertexParser::to_global_id(2, 0);
        let v4: DefaultId = LDBCVertexParser::to_global_id(4, 0);

        let out_iter = GRAPH.get_out_vertices(v1, Some(&vec![0]));
        let out: Vec<DefaultId> = out_iter.map(|v| v.get_id()).collect();
        assert_eq!(out, vec![v4, v2]);
    }
}

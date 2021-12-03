//
//! Copyright 2020 Alibaba Group Holding Limited.
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

use crate::graph_proxy::from_fn;
use crate::structure::{
    DefaultDetails, Details, Direction, DynDetails, Edge, Label, LabelId, PropKey, QueryParams,
    Statement, Vertex, ID_BITS,
};
use crate::{filter_limit, limit_n};
use crate::{register_graph, DynResult, GraphProxy, ID};
use dyn_type::BorrowObject;
use graph_store::config::{JsonConf, DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::ldbc::LDBCVertexParser;
use graph_store::prelude::{
    DefaultId, EdgeId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId,
    LDBCGraphSchema, LargeGraphDB, LocalEdge, LocalVertex, MutableGraphDB, Row, INVALID_LABEL_ID,
};
use pegasus_common::downcast::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;

lazy_static! {
    pub static ref DATA_PATH: String = configure_with_default!(String, "DATA_PATH", "".to_string());
    pub static ref PARTITION_ID: usize = configure_with_default!(usize, "PARTITION_ID", 0);
    pub static ref GRAPH: LargeGraphDB<DefaultId, InternalId> = _init_graph();
    static ref GRAPH_PROXY: Arc<DemoGraph> = initialize();
}

pub struct DemoGraph {
    store: &'static LargeGraphDB<DefaultId, InternalId>,
}

fn initialize() -> Arc<DemoGraph> {
    lazy_static::initialize(&GRAPH);
    Arc::new(DemoGraph { store: &GRAPH })
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
            .schema_file(&(DATA_PATH.as_ref() as &Path).join(DIR_GRAPH_SCHEMA).join(FILE_SCHEMA))
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

    mut_graph.add_edge_with_properties(v1, v2, 0, prop7).unwrap();
    mut_graph.add_edge_with_properties(v1, v3, 1, prop8).unwrap();
    mut_graph.add_edge_with_properties(v1, v4, 0, prop9).unwrap();
    mut_graph.add_edge_with_properties(v4, v3, 1, prop10).unwrap();
    mut_graph.add_edge_with_properties(v4, v5, 1, prop11).unwrap();
    mut_graph.add_edge_with_properties(v6, v3, 1, prop12).unwrap();

    let prop1 = Row::from(vec![object!(1), object!("marko"), object!(29)]);
    let prop2 = Row::from(vec![object!(2), object!("vadas"), object!(27)]);
    let prop3 = Row::from(vec![object!(3), object!("lop"), object!("java")]);
    let prop4 = Row::from(vec![object!(4), object!("josh"), object!(32)]);
    let prop5 = Row::from(vec![object!(5), object!("ripple"), object!("java")]);
    let prop6 = Row::from(vec![object!(6), object!("peter"), object!(35)]);

    mut_graph.add_or_update_vertex_properties(v1, prop1).unwrap();
    mut_graph.add_or_update_vertex_properties(v2, prop2).unwrap();
    mut_graph.add_or_update_vertex_properties(v3, prop3).unwrap();
    mut_graph.add_or_update_vertex_properties(v4, prop4).unwrap();
    mut_graph.add_or_update_vertex_properties(v5, prop5).unwrap();
    mut_graph.add_or_update_vertex_properties(v6, prop6).unwrap();

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
    let schema =
        LDBCGraphSchema::from_json(modern_graph_schema.to_string()).expect("Parse schema error!");

    mut_graph.into_graph(schema)
}

impl GraphProxy for DemoGraph {
    fn scan_vertex(
        &self, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        // DemoGraph contains a single graph partition on each server,
        // therefore, there's no need to use the specific partition id for query.
        // Besides, we guarantee only one worker (on each server) is going to scan (with params.partitions.is_some())
        if params.partitions.is_some() {
            let label_ids = encode_storage_vertex_label(&params.labels);
            let store = self.store;
            let result = self.store.get_all_vertices(label_ids.as_ref()).map(move |v| {
                // TODO: Only process label[0] for now
                // TODO: change to  to_runtime_vertex_with_property
                to_runtime_vertex(v, store)
                //  to_runtime_vertex_with_property(v, params.props.as_ref())
            });

            Ok(filter_limit!(result, params.filter, params.limit))
        } else {
            Ok(Box::new(std::iter::empty()))
        }
    }

    fn scan_edge(
        &self, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>> {
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
        &self, ids: &[ID], params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(local_vertex) = self.store.get_vertex(*id as DefaultId) {
                let v = if let Some(props) = params.props.as_ref() {
                    to_runtime_vertex_with_property(local_vertex, props)
                } else {
                    to_runtime_vertex(local_vertex, self.store)
                };
                result.push(v);
            }
        }
        Ok(filter_limit!(result.into_iter(), params.filter, None))
    }

    fn get_edge(
        &self, ids: &[ID], params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Iterator<Item = Edge> + Send>> {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            let eid = encode_store_e_id(id);
            if let Some(local_edge) = self.store.get_edge(eid) {
                let e = to_runtime_edge(local_edge, self.store);
                result.push(e);
            }
        }
        Ok(filter_limit!(result.into_iter(), params.filter, None))
    }

    fn prepare_explore_vertex(
        &self, direction: Direction, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Statement<ID, Vertex>>> {
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
            // TODO: change to to_runtime_vertex_with_property
            .map(move |v| to_runtime_vertex(v, graph));
            Ok(filter_limit!(iter, filter, limit))
        });
        Ok(stmt)
    }

    fn prepare_explore_edge(
        &self, direction: Direction, params: &QueryParams<Edge>,
    ) -> DynResult<Box<dyn Statement<ID, Edge>>> {
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
pub fn create_demo_graph() {
    lazy_static::initialize(&GRAPH_PROXY);
    register_graph(GRAPH_PROXY.clone());
}

#[inline]
fn to_runtime_vertex(
    v: LocalVertex<DefaultId>, store: &'static LargeGraphDB<DefaultId, InternalId>,
) -> Vertex {
    // For vertices, we query properties via vid
    let details = LazyVertexDetails::new(v.get_id(), store);
    let id = encode_runtime_v_id(&v);
    let label = encode_runtime_v_label(&v);
    Vertex::new(id, label, details)
}

fn to_runtime_vertex_with_property(v: LocalVertex<DefaultId>, props: &Vec<PropKey>) -> Vertex {
    let id = encode_runtime_v_id(&v);
    let label = encode_runtime_v_label(&v);
    let mut properties = HashMap::new();
    if props.is_empty() {
        if let Some(mut prop_vals) = v.clone_all_properties() {
            for (prop, obj) in prop_vals.drain() {
                properties.insert(prop.into(), obj);
            }
        }
    } else {
        for prop in props {
            if let PropKey::Str(prop) = prop {
                if let Some(val) = v.get_property(prop) {
                    if let Some(obj) = val.try_to_owned() {
                        properties.insert(prop.into(), obj);
                    }
                }
            }
        }
    }

    let details = DefaultDetails::new_with_prop(id, label.clone().unwrap(), properties);
    Vertex::new(id, label, details)
}

#[inline]
fn to_runtime_edge(
    e: LocalEdge<DefaultId, InternalId>, _store: &'static LargeGraphDB<DefaultId, InternalId>,
) -> Edge {
    // TODO: For edges, we clone all properties by default for now. But we'd better get properties on demand
    let id = encode_runtime_e_id(&e);
    let label = encode_runtime_e_label(&e);
    let mut properties = HashMap::new();
    if let Some(mut prop_vals) = e.clone_all_properties() {
        for (prop, obj) in prop_vals.drain() {
            properties.insert(prop.into(), obj);
        }
    }
    Edge::new(
        id,
        label.clone(),
        e.get_src_id() as ID,
        e.get_dst_id() as ID,
        DynDetails::new(DefaultDetails::new_with_prop(id, label.unwrap(), properties)),
    )
}

#[allow(dead_code)]
struct LazyVertexDetails {
    pub id: DefaultId,
    inner: AtomicPtr<LocalVertex<'static, DefaultId>>,
    store: &'static LargeGraphDB<DefaultId, InternalId>,
}

impl_as_any!(LazyVertexDetails);

impl LazyVertexDetails {
    pub fn new(id: DefaultId, store: &'static LargeGraphDB<DefaultId, InternalId>) -> Self {
        LazyVertexDetails { id, inner: AtomicPtr::default(), store }
    }
}

impl Details for LazyVertexDetails {
    fn get_property(&self, key: &PropKey) -> Option<BorrowObject> {
        if let PropKey::Str(key) = key {
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

    fn get_id(&self) -> ID {
        unreachable!()
    }

    fn get_label(&self) -> &Label {
        unreachable!()
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

#[allow(dead_code)]
struct LazyEdgeDetails {
    store: &'static LargeGraphDB<DefaultId, InternalId>,
}

impl_as_any!(LazyEdgeDetails);

impl Details for LazyEdgeDetails {
    fn get_property(&self, _key: &PropKey) -> Option<BorrowObject> {
        unimplemented!()
    }

    fn get_id(&self) -> ID {
        unimplemented!()
    }

    fn get_label(&self) -> &Label {
        unimplemented!()
    }
}

fn encode_runtime_v_id(v: &LocalVertex<DefaultId>) -> ID {
    v.get_id() as ID
}

pub const ID_SHIFT_BITS: usize = ID_BITS >> 1;

/// Given the encoding of an edge, the `ID_MASK` is used to get the lower half part of an edge, which is
/// the src_id. As an edge is indiced by its src_id, one can use edge_id & ID_MASK to route to the
/// machine of the edge.
pub const ID_MASK: ID = ((1 as ID) << (ID_SHIFT_BITS as ID)) - (1 as ID);

/// Edge's ID is encoded by the source vertex's `ID`, and its internal index
fn encode_runtime_e_id(e: &LocalEdge<DefaultId, InternalId>) -> ID {
    let ei = e.get_edge_id();
    ((ei.1 as ID) << ID_SHIFT_BITS) | (ei.0 as ID)
}

pub fn encode_store_e_id(e: &ID) -> EdgeId<DefaultId> {
    let index = (*e >> ID_SHIFT_BITS) as usize;
    let start_id = (*e & ID_MASK) as DefaultId;
    (start_id, index)
}

fn encode_runtime_v_label(v: &LocalVertex<DefaultId>) -> Option<Label> {
    Some(Label::Id(v.get_label()[0]))
}

fn encode_runtime_e_label(e: &LocalEdge<DefaultId, InternalId>) -> Option<Label> {
    Some(Label::Id(e.get_label()))
}

/// Transform string-typed labels into a id-typed labels.
/// `is_true_label` records whether the label is an actual label, or already transformed into
/// an id-type.
fn labels_to_ids(labels: &Vec<Label>, is_vertex: bool) -> Option<Vec<LabelId>> {
    if labels.is_empty() {
        None
    } else {
        Some(
            labels
                .iter()
                .map(|label| match label {
                    Label::Str(s) => {
                        let label_id = if is_vertex {
                            (*GRAPH).get_schema().get_vertex_label_id(s)
                        } else {
                            (*GRAPH).get_schema().get_edge_label_id(s).map(|id| id)
                        };
                        label_id.unwrap_or(INVALID_LABEL_ID)
                    }
                    Label::Id(id) => *id,
                })
                .collect::<Vec<LabelId>>(),
        )
    }
}

fn encode_storage_vertex_label(labels: &Vec<Label>) -> Option<Vec<LabelId>> {
    labels_to_ids(labels, true)
}

fn encode_storage_edge_label(labels: &Vec<Label>) -> Option<Vec<LabelId>> {
    labels_to_ids(labels, false)
}

#[cfg(test)]
mod tests {
    use super::GRAPH;
    use graph_store::ldbc::LDBCVertexParser;
    use graph_store::prelude::{DefaultId, GlobalStoreTrait};

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

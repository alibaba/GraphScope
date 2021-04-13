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

use crate::common::object::BorrowObject;
use crate::structure::{
    DefaultDetails, Details, Direction, DynDetails, Edge, Label, QueryParams, Statement, Vertex,
};
use crate::{register_graph, DynResult, GraphProxy, Object, ID};
use graph_store::config::{JsonConf, DIR_GRAPH_SCHEMA, FILE_SCHEMA};
use graph_store::ldbc::LDBCVertexParser;
use graph_store::prelude::{
    DefaultId, GlobalStoreTrait, GlobalStoreUpdate, GraphDBConfig, InternalId, LDBCGraphSchema,
    LabelId, LargeGraphDB, LocalEdge, LocalVertex, MutableGraphDB, Row, INVALID_LABEL_ID,
};
use pegasus::api::function::DynIter;
use pegasus_common::downcast::*;
use serde_json::{json, Value};
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

    mut_graph.add_edge(v1, v2, 0);
    mut_graph.add_edge(v1, v3, 1);
    mut_graph.add_edge(v1, v4, 0);
    mut_graph.add_edge(v4, v3, 1);
    mut_graph.add_edge(v4, v5, 1);
    mut_graph.add_edge(v6, v3, 1);

    let prop1 = Row::from(vec![json!(1), json!("marko"), json!(29)]);
    let prop2 = Row::from(vec![json!(2), json!("vadas"), json!(27)]);
    let prop3 = Row::from(vec![json!(3), json!("lop"), json!("java")]);
    let prop4 = Row::from(vec![json!(4), json!("josh"), json!(32)]);
    let prop5 = Row::from(vec![json!(5), json!("ripple"), json!("java")]);
    let prop6 = Row::from(vec![json!(6), json!("peter"), json!(35)]);

    mut_graph.add_or_update_vertex_properties(v1, prop1).unwrap();
    mut_graph.add_or_update_vertex_properties(v2, prop2).unwrap();
    mut_graph.add_or_update_vertex_properties(v3, prop3).unwrap();
    mut_graph.add_or_update_vertex_properties(v4, prop4).unwrap();
    mut_graph.add_or_update_vertex_properties(v5, prop5).unwrap();
    mut_graph.add_or_update_vertex_properties(v6, prop6).unwrap();

    let modern_graph_schema = r#"
    {
        "vertex_type_map": {
            "PERSON": 0,
            "SOFTWARE": 1
        },
        "edge_type_map": {
            "KNOWS": 0,
            "CREATED": 1
        },
        "vertex_prop": {
            "PERSON": [
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
            "SOFTWARE": [
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
            "KNOWS": [
                [
                    "start_id",
                    "ID"
                ],
                [
                    "end_id",
                    "ID"
                ]
            ],
            "CREATED": [
                [
                    "start_id",
                    "ID"
                ],
                [
                    "end_id",
                    "ID"
                ]
            ]
        }
    }
    "#;
    let schema =
        LDBCGraphSchema::from_json(modern_graph_schema.to_string()).expect("Parse schema error!");

    mut_graph.into_graph(schema)
}

macro_rules! limit_n {
    ($iter: expr, $n: expr) => {
        if let Some(limit) = $n {
            let r = $iter.take(limit);
            Box::new(r)
        } else {
            Box::new($iter)
        }
    };
}

macro_rules! filter_limit_ok {
    ($iter: expr, $f: expr, $n: expr) => {
        if let Some(ref f) = $f {
            let f = f.clone();
            let r = $iter.filter(move |v| f.test(v).unwrap_or(false)).map(|v| Ok(v));
            limit_n!(r, $n)
        } else {
            let r = $iter.map(|v| Ok(v));
            limit_n!(r, $n)
        }
    };
}

impl GraphProxy for DemoGraph {
    fn scan_vertex(
        &self, params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let label_ids = encode_storage_vertex_label(&params.labels);
        let store = self.store;
        let result = self.store.get_all_vertices(label_ids.as_ref()).map(move |v| {
            // TODO: Only process label[0] for now
            // TODO: change to  to_runtime_vertex_with_property
            to_runtime_vertex(v, store)
            //  to_runtime_vertex_with_property(v, params.props.as_ref())
        });

        if let Some(ref filter) = params.filter {
            let f = filter.clone();
            let result = result.filter(move |v| f.test(v).unwrap_or(false));
            Ok(limit_n!(result, params.limit))
        } else {
            Ok(limit_n!(result, params.limit))
        }
    }

    fn get_vertex(
        &self, ids: &[ID], params: &QueryParams<Vertex>,
    ) -> DynResult<Box<dyn Iterator<Item = Vertex> + Send>> {
        let mut result = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(local_vertex) = self.store.get_vertex(*id as DefaultId) {
                let v = to_runtime_vertex_with_property(local_vertex, params.props.as_ref());
                if let Some(ref filter) = params.filter {
                    if filter.test(&v).unwrap_or(false) {
                        result.push(v);
                    }
                } else {
                    result.push(v);
                }
            }
        }

        DynResult::Ok(Box::new(result.into_iter()))
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
            Ok(filter_limit_ok!(iter, filter, limit))
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
            Ok(filter_limit_ok!(iter, filter, limit))
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

fn to_runtime_vertex_with_property(
    v: LocalVertex<DefaultId>, props: Option<&Vec<String>>,
) -> Vertex {
    let id = encode_runtime_v_id(&v);
    let label = encode_runtime_v_label(&v);
    let mut properties = HashMap::new();
    if let Some(props) = props {
        if props.is_empty() {
            if let Some(prop_vals) = v.clone_all_properties() {
                if prop_vals.is_object() {
                    let prop_val_map = prop_vals.as_object().unwrap();
                    for (prop, val) in prop_val_map {
                        properties.insert(prop.clone(), Object::from(val));
                    }
                } else {
                    unreachable!()
                }
            }
        } else {
            for prop in props {
                if let Some(val) = v.get_property(prop) {
                    properties.insert(prop.clone(), Object::from(val));
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
    if let Some(prop_vals) = e.clone_all_properties() {
        let prop_val_map = prop_vals.as_object().expect("all properties should be stored in a map");
        for (prop, val) in prop_val_map {
            properties.insert(prop.clone(), Object::from(val));
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
    fn get_property(&self, key: &str) -> Option<BorrowObject> {
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

        unsafe { (*ptr).get_property(key) }.and_then(|v| match v {
            Value::Null => None,
            Value::Bool(b) => Some((*b).into()),
            Value::Number(n) => {
                if let Some(x) = n.as_i64() {
                    Some(x.into())
                } else if let Some(x) = n.as_u64() {
                    Some((x as i64).into())
                } else if let Some(x) = n.as_f64() {
                    Some(x.into())
                } else {
                    None
                }
            }
            Value::String(s) => Some(BorrowObject::String(s.as_str())),
            Value::Array(x) => Some(BorrowObject::Unknown(x)),
            Value::Object(x) => Some(BorrowObject::Unknown(x)),
        })
    }

    fn get_id(&self) -> u128 {
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
    fn get_property(&self, _key: &str) -> Option<BorrowObject> {
        unimplemented!()
    }

    fn get_id(&self) -> u128 {
        unimplemented!()
    }

    fn get_label(&self) -> &Label {
        unimplemented!()
    }
}

#[inline]
fn from_fn<I, O, F>(func: F) -> Box<dyn Statement<I, O>>
where
    F: Fn(I) -> DynResult<DynIter<O>> + Send + Sync + 'static,
{
    Box::new(func) as Box<dyn Statement<I, O>>
}

fn encode_runtime_v_id(v: &LocalVertex<DefaultId>) -> ID {
    v.get_id() as ID
}

fn encode_runtime_e_id(e: &LocalEdge<DefaultId, InternalId>) -> ID {
    ((e.get_src_id() as ID) << 64) | (e.get_dst_id() as ID)
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

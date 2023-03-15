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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::path::Path;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::columns::DataType;
use crate::types::*;

/// The starting id field in an edge file
pub const START_ID_FIELD: &'static str = "start_id";
/// The end id field in an edge file
pub const END_ID_FIELD: &'static str = "end_id";

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum PartitionType {
    Dynamic,
    Static,
    Null,
}

impl<'a> From<&'a str> for PartitionType {
    fn from(_token: &'a str) -> Self {
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "DYNAMIC" {
            PartitionType::Dynamic
        } else if token == "STATIC" {
            PartitionType::Static
        } else {
            error!("Unsupported type {:?}", token);
            PartitionType::Null
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum EdgeStrategy {
    Single,
    Multiple,
    Null,
}

impl<'a> From<&'a str> for EdgeStrategy {
    fn from(_token: &'a str) -> Self {
        let token_str = _token.to_uppercase();
        let token = token_str.as_str();
        if token == "SINGLE" {
            EdgeStrategy::Single
        } else if token == "MULTI" {
            EdgeStrategy::Multiple
        } else {
            error!("Unsupported type {:?}", token);
            EdgeStrategy::Null
        }
    }
}
/// An edge's label is consisted of three elements:
/// edge_label, src_vertex_label and dst_vertex_label.
#[derive(Copy, Clone, Debug, Ord, PartialOrd, Eq, PartialEq)]
pub struct EdgeLabelTuple {
    pub edge_label: LabelId,
    pub src_vertex_label: LabelId,
    pub dst_vertex_label: LabelId,
}

pub trait Schema {
    /// Get the header for the certain type of vertex if any
    fn get_vertex_header(&self, vertex_type_id: LabelId) -> Option<&[(String, DataType)]>;

    /// Get the header for the certain type of edge if any
    fn get_edge_header(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    ) -> Option<&[(String, DataType)]>;

    /// Get the schema for the certain type of vertex if any.
    fn get_vertex_schema(&self, vertex_type_id: LabelId) -> Option<&HashMap<String, (DataType, usize)>>;

    /// Get the schema for the certain
    /// type of edge if any.
    fn get_edge_schema(
        &self, edge_type_id: (LabelId, LabelId, LabelId),
    ) -> Option<&HashMap<String, (DataType, usize)>>;

    /// Get a certain vertex type's id if any
    fn get_vertex_label_id(&self, vertex_type: &str) -> Option<LabelId>;

    /// Get a certain edge type's id
    fn get_edge_label_id(&self, edge_type: &str) -> Option<LabelId>;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CsrGraphSchema {
    /// Map from vertex types to labelid
    pub vertex_type_to_id: HashMap<String, LabelId>,
    /// Map from edge types to `EdgeLabelTuple`
    pub edge_type_to_id: HashMap<String, LabelId>,
    /// Map from vertex/edge (labelid) to its property name, data types and index in the row
    vertex_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>>,
    vertex_prop_vec: HashMap<LabelId, Vec<(String, DataType)>>,
    vertex_partition_type: HashMap<LabelId, PartitionType>,

    edge_prop_meta: HashMap<(LabelId, LabelId, LabelId), HashMap<String, (DataType, usize)>>,
    edge_prop_vec: HashMap<(LabelId, LabelId, LabelId), Vec<(String, DataType)>>,
    edge_single_ie: HashSet<(LabelId, LabelId, LabelId)>,
    edge_single_oe: HashSet<(LabelId, LabelId, LabelId)>,
}

impl CsrGraphSchema {
    pub fn vertex_label_names(&self) -> Vec<String> {
        let mut ret = vec![];
        let vertex_label_num = self.vertex_type_to_id.len();
        for _ in 0..vertex_label_num {
            ret.push(String::new());
        }
        for pair in self.vertex_type_to_id.iter() {
            ret[*pair.1 as usize] = pair.0.clone();
        }

        ret
    }

    pub fn edge_label_names(&self) -> Vec<String> {
        let mut ret = vec![];
        let edge_label_num = self.edge_type_to_id.len();
        for _ in 0..edge_label_num {
            ret.push(String::new());
        }
        for pair in self.edge_type_to_id.iter() {
            ret[*pair.1 as usize] = pair.0.clone();
        }

        ret
    }

    pub fn is_static_vertex(&self, vertex_label: LabelId) -> bool {
        *self
            .vertex_partition_type
            .get(&vertex_label)
            .unwrap()
            == PartitionType::Static
    }

    pub fn is_single_ie(&self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId) -> bool {
        if self
            .edge_single_ie
            .contains(&(src_label, edge_label, dst_label))
        {
            true
        } else {
            false
        }
    }

    pub fn is_single_oe(&self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId) -> bool {
        if self
            .edge_single_oe
            .contains(&(src_label, edge_label, dst_label))
        {
            true
        } else {
            false
        }
    }

    pub fn desc(&self) {
        info!(
            "vertex label num: {}, edge label num: {}",
            self.vertex_type_to_id.len(),
            self.edge_type_to_id.len()
        );
        for pair in self.vertex_type_to_id.iter() {
            info!("vertex label: {}, id: {}", pair.0.clone(), pair.1);
        }
        for pair in self.edge_type_to_id.iter() {
            info!("edge label: {}, id: {}", pair.0.clone(), pair.1);
        }
    }

    pub fn from_json_file<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let schema_json =
            serde_json::from_reader::<File, CsrGraphSchemaJson>(file).map_err(std::io::Error::from)?;
        Ok(CsrGraphSchema::from(&schema_json))
    }

    pub fn to_json_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let file = File::create(path)?;
        let schema_json = CsrGraphSchemaJson::from(self);
        serde_json::to_writer_pretty::<File, CsrGraphSchemaJson>(file, &schema_json)
            .map_err(std::io::Error::from)
    }

    /// Get a certain edge type's id, together with its start- and edge- vertices's type
    /// while giving the `full_edge_type` that is "<src_vertex_label>_<edge_label>_<dst_vertex_label>"
    pub fn get_edge_label_tuple(&self, full_edge_type: &str) -> Option<EdgeLabelTuple> {
        let mut parts = full_edge_type.split("_");
        let src_label_id =
            if let Some(src_label) = parts.next() { self.get_vertex_label_id(src_label) } else { None };
        let edge_label_id =
            if let Some(edge_label) = parts.next() { self.get_edge_label_id(edge_label) } else { None };
        let dst_label_id =
            if let Some(dst_label) = parts.next() { self.get_vertex_label_id(dst_label) } else { None };

        if src_label_id.is_some() && edge_label_id.is_some() && dst_label_id.is_some() {
            Some(EdgeLabelTuple {
                edge_label: edge_label_id.unwrap(),
                src_vertex_label: src_label_id.unwrap(),
                dst_vertex_label: dst_label_id.unwrap(),
            })
        } else {
            None
        }
    }
}

fn is_map_eq<K: PartialEq + Ord + Debug + Hash, V: PartialEq + Ord + Debug>(
    map1: &HashMap<K, V>, map2: &HashMap<K, V>,
) -> bool {
    map1.iter().sorted().eq(map2.iter().sorted())
}

impl PartialEq for CsrGraphSchema {
    fn eq(&self, other: &Self) -> bool {
        let mut is_eq = is_map_eq(&self.vertex_type_to_id, &other.vertex_type_to_id)
            && is_map_eq(&self.edge_type_to_id, &other.edge_type_to_id)
            && is_map_eq(&self.vertex_prop_vec, &other.vertex_prop_vec)
            && is_map_eq(&self.edge_prop_vec, &other.edge_prop_vec)
            && self.vertex_prop_meta.len() == other.vertex_prop_meta.len()
            && self.edge_prop_meta.len() == other.edge_prop_meta.len();

        if is_eq {
            for ((k1, v1), (k2, v2)) in self
                .vertex_prop_meta
                .iter()
                .sorted_by(|e1, e2| e1.0.cmp(e2.0))
                .zip(
                    other
                        .vertex_prop_meta
                        .iter()
                        .sorted_by(|e1, e2| e1.0.cmp(e2.0)),
                )
            {
                is_eq = k1 == k2 && is_map_eq(v1, v2);
                if !is_eq {
                    break;
                }
            }

            for ((k1, v1), (k2, v2)) in self
                .edge_prop_meta
                .iter()
                .sorted_by(|e1, e2| e1.0.cmp(e2.0))
                .zip(
                    other
                        .edge_prop_meta
                        .iter()
                        .sorted_by(|e1, e2| e1.0.cmp(e2.0)),
                )
            {
                is_eq = k1 == k2 && is_map_eq(v1, v2);
                if !is_eq {
                    break;
                }
            }
        }

        is_eq
    }
}

impl<'a> From<&'a CsrGraphSchemaJson> for CsrGraphSchema {
    fn from(schema_json: &'a CsrGraphSchemaJson) -> Self {
        let mut vertex_type_to_id = HashMap::new();
        let mut vertex_partition_type = HashMap::new();
        let mut vertex_label = 0 as LabelId;
        for vertex_info in &schema_json.vertex {
            vertex_type_to_id.insert(vertex_info.label.clone(), vertex_label);
            vertex_partition_type.insert(vertex_label, vertex_info.partition_type.clone());
            vertex_label += 1;
        }
        let mut edge_type_to_id = HashMap::new();
        let mut edge_label = 0 as LabelId;
        for edge_info in &schema_json.edge {
            if !edge_type_to_id.contains_key(&edge_info.label) {
                edge_type_to_id.insert(edge_info.label.clone(), edge_label);
                edge_label += 1;
            }
        }
        let mut vertex_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>> =
            HashMap::with_capacity(schema_json.vertex.len());
        let mut vertex_prop_vec: HashMap<LabelId, Vec<(String, DataType)>> =
            HashMap::with_capacity(schema_json.vertex.len());
        let mut edge_prop_meta: HashMap<(LabelId, LabelId, LabelId), HashMap<String, (DataType, usize)>> =
            HashMap::with_capacity(schema_json.edge.len());
        let mut edge_prop_vec: HashMap<(LabelId, LabelId, LabelId), Vec<(String, DataType)>> =
            HashMap::with_capacity(schema_json.edge.len());
        let mut edge_single_ie = HashSet::new();
        let mut edge_single_oe = HashSet::new();

        for vertex_info in &schema_json.vertex {
            let label_id = vertex_type_to_id[&vertex_info.label];
            let vertex_map = vertex_prop_meta
                .entry(label_id)
                .or_insert_with(HashMap::new);
            let vertex_vec = vertex_prop_vec
                .entry(label_id)
                .or_insert_with(Vec::new);

            for (index, column) in vertex_info.properties.iter().enumerate() {
                vertex_map.insert(column.name.clone(), (column.data_type.clone(), index));
                vertex_vec.push((column.name.clone(), column.data_type.clone()));
            }
        }

        for edge_info in &schema_json.edge {
            let src_label_id = vertex_type_to_id[&edge_info.src_label];
            let dst_label_id = vertex_type_to_id[&edge_info.dst_label];
            let label_id = edge_type_to_id[&edge_info.label];
            let edge_map = edge_prop_meta
                .entry((src_label_id, label_id, dst_label_id))
                .or_insert_with(HashMap::new);
            let edge_vec = edge_prop_vec
                .entry((src_label_id, label_id, dst_label_id))
                .or_insert_with(Vec::new);

            if edge_info.ie_strategy.is_some()
                && *edge_info.ie_strategy.as_ref().unwrap() == EdgeStrategy::Single
            {
                edge_single_ie.insert((src_label_id, label_id, dst_label_id));
            }

            if edge_info.oe_strategy.is_some()
                && *edge_info.oe_strategy.as_ref().unwrap() == EdgeStrategy::Single
            {
                edge_single_oe.insert((src_label_id, label_id, dst_label_id));
            }

            if let Some(properties) = &edge_info.properties {
                for (index, column) in properties.iter().enumerate() {
                    edge_map.insert(column.name.clone(), (column.data_type.clone(), index));
                    edge_vec.push((column.name.clone(), column.data_type.clone()));
                }
            }
        }

        Self {
            vertex_type_to_id,
            edge_type_to_id,
            vertex_prop_meta,
            vertex_prop_vec,
            vertex_partition_type,
            edge_prop_meta,
            edge_prop_vec,
            edge_single_ie,
            edge_single_oe,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct InputSchema {
    /// Map from vertex label id to headers in input file
    vertex_headers: HashMap<LabelId, Vec<(String, DataType)>>,
    /// Map from src_vertex, edge, dst_vertex label id to headers in input file
    edge_headers: HashMap<(LabelId, LabelId, LabelId), Vec<(String, DataType)>>,

    /// Map for vertex label id to input file
    vertex_files: HashMap<LabelId, Vec<String>>,
    /// Map for src_vertex, edge, dst_vertex label id to input file
    edge_files: HashMap<(LabelId, LabelId, LabelId), Vec<String>>,
}

impl InputSchema {
    pub fn get_vertex_header(&self, vertex_label: LabelId) -> Option<&[(String, DataType)]> {
        self.vertex_headers
            .get(&vertex_label)
            .map(|vec| vec.as_slice())
    }

    pub fn get_edge_header(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    ) -> Option<&[(String, DataType)]> {
        self.edge_headers
            .get(&(src_label, edge_label, dst_label))
            .map(|vec| vec.as_slice())
    }

    pub fn get_vertex_file(&self, vertex_label: LabelId) -> Option<&Vec<String>> {
        self.vertex_files.get(&vertex_label)
    }

    pub fn get_edge_file(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    ) -> Option<&Vec<String>> {
        self.edge_files
            .get(&(src_label, edge_label, dst_label))
    }

    pub fn from_json_file<P: AsRef<Path>>(path: P, graph_schema: &CsrGraphSchema) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let input_json =
            serde_json::from_reader::<File, InputSchemaJson>(file).map_err(std::io::Error::from)?;
        let mut vertex_headers = HashMap::new();
        let mut vertex_files = HashMap::new();
        for vertex in &input_json.vertex {
            if let Some(vertex_label) = graph_schema
                .vertex_type_to_id
                .get(&vertex.label)
            {
                let mut properties = vec![];
                for column in &vertex.columns {
                    properties.push((column.name.clone(), column.data_type.clone()));
                }
                vertex_headers.insert(*vertex_label, properties);
                vertex_files.insert(*vertex_label, vertex.files.clone());
            }
        }
        let mut edge_headers = HashMap::new();
        let mut edge_files = HashMap::new();
        for edge in &input_json.edge {
            if let (Some(src_label), Some(edge_label), Some(dst_label)) = (
                graph_schema
                    .vertex_type_to_id
                    .get(&edge.src_label),
                graph_schema.edge_type_to_id.get(&edge.label),
                graph_schema
                    .vertex_type_to_id
                    .get(&edge.dst_label),
            ) {
                let mut properties = vec![];
                for column in &edge.columns {
                    properties.push((column.name.clone(), column.data_type.clone()));
                }
                edge_headers.insert((*src_label, *edge_label, *dst_label), properties);
                edge_files.insert((*src_label, *edge_label, *dst_label), edge.files.clone());
            }
        }
        Ok(InputSchema { vertex_headers, edge_headers, vertex_files, edge_files })
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct ColumnInfo {
    name: String,
    data_type: DataType,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct InputVertex {
    label: String,
    columns: Vec<ColumnInfo>,
    files: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct InputEdge {
    src_label: String,
    dst_label: String,
    label: String,
    columns: Vec<ColumnInfo>,
    files: Vec<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct VertexInfo {
    label: String,
    partition_type: PartitionType,
    properties: Vec<ColumnInfo>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct EdgeInfo {
    src_label: String,
    dst_label: String,
    label: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ie_strategy: Option<EdgeStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    oe_strategy: Option<EdgeStrategy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    properties: Option<Vec<ColumnInfo>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct CsrGraphSchemaJson {
    vertex: Vec<VertexInfo>,
    edge: Vec<EdgeInfo>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct InputSchemaJson {
    vertex: Vec<InputVertex>,
    edge: Vec<InputEdge>,
}

impl<'a> From<&'a CsrGraphSchema> for CsrGraphSchemaJson {
    fn from(schema: &'a CsrGraphSchema) -> Self {
        let vertex_label_num = schema.vertex_type_to_id.len();
        let edge_label_num = schema.edge_type_to_id.len();

        let mut vertex_info_vec = vec![
            VertexInfo {
                label: "".to_string(),
                partition_type: PartitionType::Dynamic,
                properties: vec![]
            };
            vertex_label_num
        ];
        let mut edge_info_vec = vec![];

        let mut vertex_names = vec!["".to_string(); vertex_label_num];
        let mut edge_names = vec!["".to_string(); edge_label_num];

        for (vertex_label, label) in &schema.vertex_type_to_id {
            vertex_names[*label as usize] = vertex_label.clone();
            let partition_type = schema.vertex_partition_type.get(label).unwrap();
            if let Some(column) = schema.vertex_prop_vec.get(label) {
                let mut properties = vec![];
                for (col_name, data_type) in column {
                    properties.push(ColumnInfo { name: col_name.clone(), data_type: data_type.clone() });
                }
                vertex_info_vec[*label as usize] = VertexInfo {
                    label: vertex_label.clone(),
                    partition_type: partition_type.clone(),
                    properties: properties,
                }
            }
        }

        for (edge_label, label) in &schema.edge_type_to_id {
            edge_names[*label as usize] = edge_label.clone();
        }

        for ((src_label, label, dst_label), columns) in &schema.edge_prop_vec {
            let src_label_name = vertex_names[*src_label as usize].clone();
            let label_name = edge_names[*label as usize].clone();
            let dst_label_name = vertex_names[*dst_label as usize].clone();
            let ie_strategy = if schema
                .edge_single_ie
                .contains(&(*src_label, *label, *dst_label))
            {
                Some(EdgeStrategy::Single)
            } else {
                None
            };
            let oe_strategy = if schema
                .edge_single_oe
                .contains(&(*src_label, *label, *dst_label))
            {
                Some(EdgeStrategy::Single)
            } else {
                None
            };
            if columns.len() > 0 {
                let mut properties = vec![];
                for (col_name, data_type) in columns {
                    properties.push(ColumnInfo { name: col_name.clone(), data_type: data_type.clone() });
                }
                edge_info_vec.push(EdgeInfo {
                    src_label: src_label_name,
                    dst_label: dst_label_name,
                    label: label_name,
                    ie_strategy: ie_strategy,
                    oe_strategy: oe_strategy,
                    properties: Some(properties),
                });
            } else {
                edge_info_vec.push(EdgeInfo {
                    src_label: src_label_name,
                    dst_label: dst_label_name,
                    label: label_name,
                    ie_strategy: ie_strategy,
                    oe_strategy: oe_strategy,
                    properties: None,
                });
            }
        }

        edge_info_vec
            .sort_by(|a, b| schema.edge_type_to_id[&a.label].cmp(&schema.edge_type_to_id[&b.label]));
        Self { vertex: vertex_info_vec, edge: edge_info_vec }
    }
}

impl Schema for CsrGraphSchema {
    fn get_vertex_header(&self, vertex_type_id: LabelId) -> Option<&[(String, DataType)]> {
        self.vertex_prop_vec
            .get(&vertex_type_id)
            .map(|vec| vec.as_slice())
    }
    fn get_edge_header(
        &self, src_label: LabelId, edge_label: LabelId, dst_label: LabelId,
    ) -> Option<&[(String, DataType)]> {
        self.edge_prop_vec
            .get(&(src_label, edge_label, dst_label))
            .map(|vec| vec.as_slice())
    }

    fn get_vertex_schema(&self, vertex_type_id: LabelId) -> Option<&HashMap<String, (DataType, usize)>> {
        self.vertex_prop_meta.get(&vertex_type_id)
    }

    fn get_edge_schema(
        &self, edge_type_id: (LabelId, LabelId, LabelId),
    ) -> Option<&HashMap<String, (DataType, usize)>> {
        self.edge_prop_meta.get(&edge_type_id)
    }

    fn get_vertex_label_id(&self, vertex_type: &str) -> Option<LabelId> {
        self.vertex_type_to_id.get(vertex_type).cloned()
    }

    fn get_edge_label_id(&self, edge_type: &str) -> Option<LabelId> {
        self.edge_type_to_id.get(edge_type).cloned()
    }
}

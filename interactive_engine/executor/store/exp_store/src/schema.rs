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

use crate::common::LabelId;
use crate::config::JsonConf;
use crate::parser::DataType;
use itertools::Itertools;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::path::Path;

/// The id field in a vertex file
pub const ID_FIELD: &'static str = "id";
/// The label_field in a vertex file
pub const LABEL_FIELD: &'static str = "~LABEL";
/// The starting id field in an edge file
pub const START_ID_FIELD: &'static str = "start_id";
/// The end id field in an edge file
pub const END_ID_FIELD: &'static str = "end_id";

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
    fn get_edge_header(&self, edge_type_id: LabelId) -> Option<&[(String, DataType)]>;

    /// Get the schema for the certain type of vertex if any.
    fn get_vertex_schema(
        &self, vertex_type_id: LabelId,
    ) -> Option<&HashMap<String, (DataType, usize)>>;

    /// Get the schema for the certain
    /// type of edge if any.
    fn get_edge_schema(&self, edge_type_id: LabelId)
        -> Option<&HashMap<String, (DataType, usize)>>;

    /// Get a certain vertex type's id if any
    fn get_vertex_label_id(&self, vertex_type: &str) -> Option<LabelId>;

    /// Get a certain edge type's id
    fn get_edge_label_id(&self, edge_type: &str) -> Option<LabelId>;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LDBCGraphSchema {
    /// Map from vertex types to labelid
    vertex_type_to_id: HashMap<String, LabelId>,
    /// Map from edge types to `EdgeLabelTuple`
    edge_type_to_id: HashMap<String, LabelId>,
    /// Map from vertex/edge (labelid) to its property name, data types and index in the row
    vertex_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>>,
    vertex_prop_vec: HashMap<LabelId, Vec<(String, DataType)>>,
    edge_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>>,
    edge_prop_vec: HashMap<LabelId, Vec<(String, DataType)>>,
}

impl LDBCGraphSchema {
    /// While loading graphs, we will have properties such as LABELs and edge's
    /// ids data recorded in the schema. While these data for now does not actually
    /// maintain in the database, we will trim them in the schema.
    pub(crate) fn trim(&mut self) {
        for (key, value) in &mut self.vertex_prop_meta {
            if let Some((_, index)) = value.remove(LABEL_FIELD) {
                if let Some(vec) = self.vertex_prop_vec.get_mut(key) {
                    vec.remove(index);
                    for (index, (name, dt)) in vec.iter().enumerate() {
                        *value.get_mut(name).unwrap() = (dt.clone(), index);
                    }
                }
            }
        }
        for (key, value) in &mut self.edge_prop_meta {
            let mut indices_to_trim = HashSet::new();
            if let Some((_, index)) = value.remove(START_ID_FIELD) {
                indices_to_trim.insert(index);
            }
            if let Some((_, index)) = value.remove(END_ID_FIELD) {
                indices_to_trim.insert(index);
            }
            let mut vec_trimmed = Vec::<(String, DataType)>::new();
            if let Some(old_vec) = self.edge_prop_vec.get(key) {
                for (index, item) in old_vec.iter().enumerate() {
                    if !indices_to_trim.contains(&index) {
                        vec_trimmed.push(item.clone());
                    }
                }
            }
            for (index, (name, dt)) in vec_trimmed.iter().enumerate() {
                *value.get_mut(name).unwrap() = (dt.clone(), index);
            }
            *self.edge_prop_vec.entry(*key).or_insert_with(Vec::new) = vec_trimmed;
        }
    }

    /// Get a certain edge type's id, together with its start- and edge- vertices's type
    /// while giving the `full_edge_type` that is "<src_vertex_label>_<edge_label>_<dst_vertex_label>"
    pub fn get_edge_label_tuple(&self, full_edge_type: &str) -> Option<EdgeLabelTuple> {
        let mut parts = full_edge_type.split("_");
        let src_label_id = if let Some(src_label) = parts.next() {
            self.get_vertex_label_id(src_label)
        } else {
            None
        };
        let edge_label_id = if let Some(edge_label) = parts.next() {
            self.get_edge_label_id(edge_label)
        } else {
            None
        };
        let dst_label_id = if let Some(dst_label) = parts.next() {
            self.get_vertex_label_id(dst_label)
        } else {
            None
        };

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

impl PartialEq for LDBCGraphSchema {
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
                .zip(other.vertex_prop_meta.iter().sorted_by(|e1, e2| e1.0.cmp(e2.0)))
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
                .zip(other.edge_prop_meta.iter().sorted_by(|e1, e2| e1.0.cmp(e2.0)))
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

#[derive(Serialize, Deserialize, Clone, Debug)]
struct LDBCGraphSchemaJson {
    vertex_type_map: HashMap<String, LabelId>,
    edge_type_map: HashMap<String, LabelId>,
    vertex_prop: HashMap<String, Vec<(String, DataType)>>,
    edge_prop: HashMap<String, Vec<(String, DataType)>>,
}

impl<'a> From<&'a LDBCGraphSchema> for LDBCGraphSchemaJson {
    fn from(schema: &'a LDBCGraphSchema) -> Self {
        let vertex_type_map = schema.vertex_type_to_id.clone();
        let edge_type_map: HashMap<String, LabelId> =
            schema.edge_type_to_id.iter().map(|(name, id)| (name.clone(), *id)).collect();

        let vertex_type_map_rev: HashMap<LabelId, String> =
            vertex_type_map.iter().map(|(name, id)| (*id, name.clone())).collect();
        let edge_type_map_rev: HashMap<LabelId, String> =
            edge_type_map.iter().map(|(name, id)| (*id, name.clone())).collect();

        let mut vertex_prop =
            HashMap::<String, Vec<(String, DataType)>>::with_capacity(schema.vertex_prop_vec.len());
        let mut edge_prop =
            HashMap::<String, Vec<(String, DataType)>>::with_capacity(schema.edge_prop_vec.len());

        for (key, value) in &schema.vertex_prop_vec {
            vertex_prop.insert(vertex_type_map_rev[key].clone(), value.clone());
        }

        for (key, value) in &schema.edge_prop_vec {
            edge_prop.insert(edge_type_map_rev[key].clone(), value.clone());
        }

        Self { vertex_type_map, edge_type_map, vertex_prop, edge_prop }
    }
}

impl<'a> From<&'a LDBCGraphSchemaJson> for LDBCGraphSchema {
    fn from(schema_json: &'a LDBCGraphSchemaJson) -> Self {
        let vertex_type_to_id = schema_json.vertex_type_map.clone();
        let edge_type_to_id: HashMap<String, LabelId> =
            schema_json.edge_type_map.iter().map(|(name, id)| (name.clone(), *id)).collect();
        let mut vertex_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>> =
            HashMap::with_capacity(schema_json.vertex_prop.len());
        let mut vertex_prop_vec: HashMap<LabelId, Vec<(String, DataType)>> =
            HashMap::with_capacity(schema_json.vertex_prop.len());
        let mut edge_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>> =
            HashMap::with_capacity(schema_json.edge_prop.len());
        let mut edge_prop_vec: HashMap<LabelId, Vec<(String, DataType)>> =
            HashMap::with_capacity(schema_json.edge_prop.len());

        for (key, value) in &schema_json.vertex_prop {
            let label_id = vertex_type_to_id[key];
            let vertex_map = vertex_prop_meta.entry(label_id).or_insert_with(HashMap::new);
            let vertex_vec = vertex_prop_vec.entry(label_id).or_insert_with(Vec::new);

            for (index, (name, dt)) in value.iter().enumerate() {
                vertex_map.insert(name.clone(), (dt.clone(), index));
                vertex_vec.push((name.clone(), dt.clone()));
            }
        }

        for (key, value) in &schema_json.edge_prop {
            let label_id = edge_type_to_id[key];
            let edge_map = edge_prop_meta.entry(label_id).or_insert_with(HashMap::new);
            let edge_vec = edge_prop_vec.entry(label_id).or_insert_with(Vec::new);

            for (index, (name, dt)) in value.iter().enumerate() {
                edge_map.insert(name.clone(), (dt.clone(), index));
                edge_vec.push((name.clone(), dt.clone()));
            }
        }

        Self {
            vertex_type_to_id,
            edge_type_to_id,
            vertex_prop_meta,
            vertex_prop_vec,
            edge_prop_meta,
            edge_prop_vec,
        }
    }
}

impl Schema for LDBCGraphSchema {
    fn get_vertex_header(&self, vertex_type_id: LabelId) -> Option<&[(String, DataType)]> {
        self.vertex_prop_vec.get(&vertex_type_id).map(|vec| vec.as_slice())
    }
    fn get_edge_header(&self, edge_type_id: LabelId) -> Option<&[(String, DataType)]> {
        self.edge_prop_vec.get(&edge_type_id).map(|vec| vec.as_slice())
    }

    fn get_vertex_schema(
        &self, vertex_type_id: LabelId,
    ) -> Option<&HashMap<String, (DataType, usize)>> {
        self.vertex_prop_meta.get(&vertex_type_id)
    }

    fn get_edge_schema(
        &self, edge_type_id: LabelId,
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

impl JsonConf<LDBCGraphSchemaJson> for LDBCGraphSchemaJson {}

impl JsonConf<LDBCGraphSchema> for LDBCGraphSchema {
    fn from_json_file<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let schema_json = serde_json::from_reader::<File, LDBCGraphSchemaJson>(file)
            .map_err(std::io::Error::from)?;
        Ok(LDBCGraphSchema::from(&schema_json))
    }

    fn from_json(json: String) -> std::io::Result<Self> {
        let schema_json = serde_json::from_str(&json).map_err(std::io::Error::from)?;
        Ok(LDBCGraphSchema::from(&schema_json))
    }

    fn to_json_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let file = File::create(path)?;
        let schema_json = LDBCGraphSchemaJson::from(self);
        serde_json::to_writer_pretty::<File, LDBCGraphSchemaJson>(file, &schema_json)
            .map_err(std::io::Error::from)
    }

    fn to_json(&self) -> std::io::Result<String> {
        let schema_json = LDBCGraphSchemaJson::from(self);
        serde_json::to_string_pretty(&schema_json).map_err(std::io::Error::from)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_trim_schema() {
        let mut schema =
            LDBCGraphSchema::from_json_file("data/schema.json").expect("Get schema error");

        let vertex_label = schema.get_vertex_label_id("ORGANISATION").unwrap();
        let org_header = schema.get_vertex_header(vertex_label).unwrap();
        assert_eq!(
            org_header,
            &[
                (("id".to_string(), DataType::ID)),
                (("~LABEL".to_string(), DataType::LABEL)),
                (("name".to_string(), DataType::String)),
                (("url".to_string(), DataType::String))
            ]
        );
        let org_schema = schema.get_vertex_schema(vertex_label).unwrap();
        let expected_org_schema: HashMap<String, (DataType, usize)> = vec![
            ("id".to_string(), (DataType::ID, 0)),
            ("~LABEL".to_string(), (DataType::LABEL, 1)),
            ("name".to_string(), (DataType::String, 2)),
            ("url".to_string(), (DataType::String, 3)),
        ]
        .into_iter()
        .collect();

        assert!(is_map_eq(org_schema, &expected_org_schema));

        let label_tuple = schema.get_edge_label_tuple("PERSON_KNOWS_PERSON").unwrap();
        let knows_header = schema.get_edge_header(label_tuple.edge_label).unwrap();
        assert_eq!(
            knows_header,
            &[
                ("start_id".to_string(), DataType::ID),
                ("end_id".to_string(), DataType::ID),
                ("creationDate".to_string(), DataType::Date),
            ]
        );
        let knows_schema = schema.get_edge_schema(label_tuple.edge_label).unwrap();
        let expected_knows_schema: HashMap<String, (DataType, usize)> = vec![
            ("start_id".to_string(), (DataType::ID, 0)),
            ("end_id".to_string(), (DataType::ID, 1)),
            ("creationDate".to_string(), (DataType::Date, 2)),
        ]
        .into_iter()
        .collect();
        assert!(is_map_eq(knows_schema, &expected_knows_schema));

        // after trimming the schema
        schema.trim();

        let vertex_label = schema.get_vertex_label_id("ORGANISATION").unwrap();
        let org_header = schema.get_vertex_header(vertex_label).unwrap();
        assert_eq!(
            org_header,
            &[
                ("id".to_string(), DataType::ID),
                ("name".to_string(), DataType::String),
                ("url".to_string(), DataType::String)
            ]
        );
        let org_schema = schema.get_vertex_schema(vertex_label).unwrap();
        let expected_org_schema: HashMap<String, (DataType, usize)> = vec![
            ("id".to_string(), (DataType::ID, 0)),
            ("name".to_string(), (DataType::String, 1)),
            ("url".to_string(), (DataType::String, 2)),
        ]
        .into_iter()
        .collect();

        assert!(is_map_eq(org_schema, &expected_org_schema));

        let label_tuple = schema.get_edge_label_tuple("PERSON_KNOWS_PERSON").unwrap();
        let knows_header = schema.get_edge_header(label_tuple.edge_label).unwrap();
        assert_eq!(knows_header, &[("creationDate".to_string(), DataType::Date),]);
        let knows_schema = schema.get_edge_schema(label_tuple.edge_label).unwrap();
        let expected_knows_schema: HashMap<String, (DataType, usize)> =
            vec![("creationDate".to_string(), (DataType::Date, 0))].into_iter().collect();

        assert!(is_map_eq(knows_schema, &expected_knows_schema));
    }
}

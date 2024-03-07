use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::hash::Hash;
use std::ops::Index;
use std::path::Path;

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::types::*;

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
    fn get_vertex_schema(&self, vertex_type_id: LabelId) -> Option<&HashMap<String, (DataType, usize)>>;

    /// Get the schema for the certain
    /// type of edge if any.
    fn get_edge_schema(&self, edge_type_id: LabelId) -> Option<&HashMap<String, (DataType, usize)>>;

    /// Get a certain vertex type's id if any
    fn get_vertex_label_id(&self, vertex_type: &str) -> Option<LabelId>;

    /// Get a certain edge type's id
    fn get_edge_label_id(&self, edge_type: &str) -> Option<LabelId>;
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LDBCGraphSchema {
    /// Map from vertex types to labelid
    pub vertex_type_to_id: HashMap<String, LabelId>,
    /// Map from edge types to `EdgeLabelTuple`
    pub edge_type_to_id: HashMap<String, LabelId>,
    /// Map from vertex/edge (labelid) to its property name, data types and index in the row
    vertex_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>>,
    vertex_prop_vec: HashMap<LabelId, Vec<(String, DataType)>>,
    edge_prop_meta: HashMap<LabelId, HashMap<String, (DataType, usize)>>,
    edge_prop_vec: HashMap<LabelId, Vec<(String, DataType)>>,
}

impl LDBCGraphSchema {
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

    pub fn get_edge_data_type(&self, e_label: LabelId) -> Option<DataType> {
        let header = self.get_edge_header(e_label);
        if header.is_some() {
            let header = header.unwrap();
            let mut ret = DataType::NULL;
            for (_, v) in header {
                if *v != DataType::ID {
                    ret = v.clone();
                }
            }
            if ret != DataType::NULL {
                return Some(ret);
            }
        }
        None
    }

    pub fn desc(&self) {
        println!(
            "vertex label num: {}, edge label num: {}",
            self.vertex_type_to_id.len(),
            self.edge_type_to_id.len()
        );
        for pair in self.edge_type_to_id.iter() {
            println!("label: {}, id: {}", pair.0.clone(), pair.1);
        }
    }

    pub fn from_json_file<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let file = File::open(path)?;
        let schema_json =
            serde_json::from_reader::<File, LDBCGraphSchemaJson>(file).map_err(std::io::Error::from)?;
        Ok(LDBCGraphSchema::from(&schema_json))
    }

    pub fn to_json_file<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        let file = File::create(path)?;
        let schema_json = LDBCGraphSchemaJson::from(self);
        serde_json::to_writer_pretty::<File, LDBCGraphSchemaJson>(file, &schema_json)
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

impl<'a> From<&'a LDBCGraphSchemaJson> for LDBCGraphSchema {
    fn from(schema_json: &'a LDBCGraphSchemaJson) -> Self {
        let vertex_type_to_id = schema_json.vertex_type_map.clone();
        let edge_type_to_id: HashMap<String, LabelId> = schema_json
            .edge_type_map
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();
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
            let vertex_map = vertex_prop_meta
                .entry(label_id)
                .or_insert_with(HashMap::new);
            let vertex_vec = vertex_prop_vec
                .entry(label_id)
                .or_insert_with(Vec::new);

            for (index, (name, dt)) in value.iter().enumerate() {
                vertex_map.insert(name.clone(), (dt.clone(), index));
                vertex_vec.push((name.clone(), dt.clone()));
            }
        }

        for (key, value) in &schema_json.edge_prop {
            let label_id = edge_type_to_id[key];
            let edge_map = edge_prop_meta
                .entry(label_id)
                .or_insert_with(HashMap::new);
            let edge_vec = edge_prop_vec
                .entry(label_id)
                .or_insert_with(Vec::new);

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
        let edge_type_map: HashMap<String, LabelId> = schema
            .edge_type_to_id
            .iter()
            .map(|(name, id)| (name.clone(), *id))
            .collect();

        let vertex_type_map_rev: HashMap<LabelId, String> = vertex_type_map
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();
        let edge_type_map_rev: HashMap<LabelId, String> = edge_type_map
            .iter()
            .map(|(name, id)| (*id, name.clone()))
            .collect();

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

impl Schema for LDBCGraphSchema {
    fn get_vertex_header(&self, vertex_type_id: LabelId) -> Option<&[(String, DataType)]> {
        self.vertex_prop_vec
            .get(&vertex_type_id)
            .map(|vec| vec.as_slice())
    }
    fn get_edge_header(&self, edge_type_id: LabelId) -> Option<&[(String, DataType)]> {
        self.edge_prop_vec
            .get(&edge_type_id)
            .map(|vec| vec.as_slice())
    }

    fn get_vertex_schema(&self, vertex_type_id: LabelId) -> Option<&HashMap<String, (DataType, usize)>> {
        self.vertex_prop_meta.get(&vertex_type_id)
    }

    fn get_edge_schema(&self, edge_type_id: LabelId) -> Option<&HashMap<String, (DataType, usize)>> {
        self.edge_prop_meta.get(&edge_type_id)
    }

    fn get_vertex_label_id(&self, vertex_type: &str) -> Option<LabelId> {
        self.vertex_type_to_id.get(vertex_type).cloned()
    }

    fn get_edge_label_id(&self, edge_type: &str) -> Option<LabelId> {
        self.edge_type_to_id.get(edge_type).cloned()
    }
}

pub struct IndexSchema {
    // vertex_index_name, vertex_index_label
    pub vertex_index_name_to_id: HashMap<String, (LabelId, LabelId)>,
    pub vertex_index_count: HashMap<LabelId, i32>,
    // vertex_label_id, index_data_type
    pub vertex_index_name_list: HashMap<LabelId, Vec<String>>,
    pub vertex_index_meta: HashMap<LabelId, HashMap<String, (LabelId, DataType)>>,
    // src_label_id, dst_label_id, direction, index_data_type
    pub edge_index_name_list: HashMap<(LabelId, LabelId, LabelId), Vec<String>>,
    pub edge_index_meta: HashMap<(LabelId, LabelId, LabelId), HashMap<String, (LabelId, DataType)>>,
}

impl IndexSchema {
    pub fn new() -> Self {
        IndexSchema {
            vertex_index_name_to_id: HashMap::new(),
            vertex_index_count: HashMap::new(),
            vertex_index_name_list: HashMap::new(),
            vertex_index_meta: HashMap::new(),
            edge_index_name_list: HashMap::new(),
            edge_index_meta: HashMap::new(),
        }
    }

    pub fn add_vertex_index(
        &mut self, index_name: String, vertex_label: LabelId, data_type: DataType,
    ) -> Option<LabelId> {
        if let Some(mut index_metas) = self.vertex_index_meta.get_mut(&vertex_label) {
            if let Some(mut property_list) = self
                .vertex_index_name_list
                .get_mut(&vertex_label)
            {
                if let Some(property_info) = index_metas.get(&index_name) {
                    Some(property_info.0)
                } else {
                    let property_index = property_list.len() as LabelId;
                    property_list.push(index_name.clone());
                    index_metas.insert(index_name, (property_index, data_type));
                    Some(property_index)
                }
            } else {
                panic!("Vertex index map poisoned")
            }
        } else {
            let mut index_map = HashMap::new();
            let mut property_list = vec![];
            property_list.push(index_name.clone());
            index_map.insert(index_name, (0, data_type));
            self.vertex_index_name_list
                .insert(vertex_label, property_list);
            self.vertex_index_meta
                .insert(vertex_label, index_map);
            return Some(0);
        }
    }

    pub fn add_edge_index(
        &mut self, index_name: String, edge_label: LabelId, src_label: LabelId, dst_label: LabelId,
        data_type: DataType,
    ) -> Option<LabelId> {
        if let Some(mut index_metas) = self
            .edge_index_meta
            .get_mut(&(src_label, edge_label, dst_label))
        {
            if let Some(mut property_list) = self
                .edge_index_name_list
                .get_mut(&(src_label, edge_label, dst_label))
            {
                if let Some(property_info) = index_metas.get(&index_name) {
                    Some(property_info.0)
                } else {
                    let property_index = property_list.len() as LabelId;
                    property_list.push(index_name.clone());
                    index_metas.insert(index_name, (property_index, data_type));
                    Some(property_index)
                }
            } else {
                panic!("Edge index map poisoned")
            }
        } else {
            let mut index_map = HashMap::new();
            let mut property_list = vec![];
            property_list.push(index_name.clone());
            index_map.insert(index_name, (0, data_type));
            self.edge_index_name_list
                .insert((src_label, edge_label, dst_label), property_list);
            self.edge_index_meta
                .insert((src_label, edge_label, dst_label), index_map);
            return Some(0);
        }
    }

    pub fn get_vertex_index(&self, vertex_label: LabelId, vertex_index_name: &String) -> Option<LabelId> {
        if let Some(vertex_index_map) = self.vertex_index_meta.get(&vertex_label) {
            if let Some((index_label, _)) = vertex_index_map.get(vertex_index_name) {
                Some(*index_label)
            } else {
                None
            }
        } else {
            None
        }
    }

    pub fn get_edge_index(
        &self, edge_label: LabelId, src_label: LabelId, dst_label: LabelId, edge_index_name: &String,
    ) -> Option<LabelId> {
        if let Some(edge_index_map) = self
            .edge_index_meta
            .get(&(src_label, edge_label, dst_label))
        {
            if let Some((index_label, _)) = edge_index_map.get(edge_index_name) {
                Some(*index_label)
            } else {
                None
            }
        } else {
            None
        }
    }
}

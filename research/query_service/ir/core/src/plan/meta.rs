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

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::rc::Rc;
use std::sync::RwLock;

use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;
use ir_common::NameOrId;

use crate::error::{IrError, IrResult};
use crate::JsonIO;

pub static INVALID_META_ID: i32 = -1;

lazy_static! {
    pub static ref STORE_META: RwLock<StoreMeta> = RwLock::new(StoreMeta::default());
}

pub fn set_schema_from_json<R: io::Read>(read: R) {
    if let Ok(mut meta) = STORE_META.write() {
        if let Ok(schema) = Schema::from_json(read) {
            meta.schema = Some(schema);
        }
    }
}

/// The simple schema, mapping either label or property name into id.
pub fn set_schema_simple(
    entities: Vec<(String, i32)>, relations: Vec<(String, i32)>, columns: Vec<(String, i32)>,
) {
    if let Ok(mut meta) = STORE_META.write() {
        let schema: Schema = (entities, relations, columns).into();
        meta.schema = Some(schema)
    }
}

pub fn reset_schema() {
    if let Ok(mut meta) = STORE_META.write() {
        meta.schema = None;
    }
}

#[derive(Clone, Debug, Default)]
pub struct StoreMeta {
    pub schema: Option<Schema>,
}

#[derive(Clone, Debug)]
pub struct LabelMeta {
    name: String,
    id: i32,
}

impl Default for LabelMeta {
    fn default() -> Self {
        Self { name: "INVALID".into(), id: INVALID_META_ID }
    }
}

impl From<(String, i32)> for LabelMeta {
    fn from(tuple: (String, i32)) -> Self {
        Self { name: tuple.0, id: tuple.1 }
    }
}

impl From<schema_pb::LabelMeta> for LabelMeta {
    fn from(label: schema_pb::LabelMeta) -> Self {
        Self { name: label.name, id: label.id }
    }
}

impl From<LabelMeta> for schema_pb::LabelMeta {
    fn from(label: LabelMeta) -> Self {
        Self { name: label.name, id: label.id }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum KeyType {
    Entity = 0,
    Relation = 1,
    Column = 2,
}

impl Default for KeyType {
    fn default() -> Self {
        Self::Entity
    }
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    /// A map from table name to its internally encoded id
    /// In the concept of graph database, this is also known as label
    table_map: BTreeMap<String, i32>,
    /// A map from column name to its internally encoded id
    /// In the concept of graph database, this is also known as property
    column_map: BTreeMap<String, i32>,
    /// A reversed map of `id` to `name` mapping
    id_name_rev: BTreeMap<(KeyType, i32), String>,
    /// The source and destination labels of a given relation label's id
    relation_labels: BTreeMap<String, Vec<(LabelMeta, LabelMeta)>>,
    /// Is the table name mapped as id
    is_table_id: bool,
    /// Is the column name mapped as id
    is_column_id: bool,
    /// Entities
    entities: Vec<schema_pb::EntityMeta>,
    /// Relations
    rels: Vec<schema_pb::RelationMeta>,
}

impl Schema {
    pub fn get_table_id(&self, name: &str) -> Option<i32> {
        self.table_map.get(name).cloned()
    }

    pub fn get_table_id_from_pb(&self, name: &common_pb::NameOrId) -> Option<i32> {
        name.item.as_ref().and_then(|item| match item {
            common_pb::name_or_id::Item::Name(name) => self.get_table_id(name),
            common_pb::name_or_id::Item::Id(id) => Some(*id),
        })
    }

    pub fn get_column_id(&self, name: &str) -> Option<i32> {
        self.column_map.get(name).cloned()
    }

    pub fn get_column_id_from_pb(&self, name: &common_pb::NameOrId) -> Option<i32> {
        name.item.as_ref().and_then(|item| match item {
            common_pb::name_or_id::Item::Name(name) => self.get_column_id(name),
            common_pb::name_or_id::Item::Id(id) => Some(*id),
        })
    }

    pub fn get_name(&self, id: i32, ty: KeyType) -> Option<&String> {
        self.id_name_rev.get(&(ty, id))
    }

    pub fn get_relation_labels(&self, relation: &NameOrId) -> Option<&Vec<(LabelMeta, LabelMeta)>> {
        match relation {
            NameOrId::Str(name) => self.relation_labels.get(name),
            NameOrId::Id(id) => self
                .get_name(*id, KeyType::Relation)
                .and_then(|name| self.relation_labels.get(name)),
        }
    }

    pub fn is_column_id(&self) -> bool {
        self.is_column_id
    }

    pub fn is_table_id(&self) -> bool {
        self.is_table_id
    }
}

impl From<(Vec<(String, i32)>, Vec<(String, i32)>, Vec<(String, i32)>)> for Schema {
    fn from(tuple: (Vec<(String, i32)>, Vec<(String, i32)>, Vec<(String, i32)>)) -> Self {
        let (entities, relations, columns) = tuple;
        let mut schema = Schema::default();
        schema.is_table_id = !entities.is_empty() || !relations.is_empty();
        schema.is_column_id = !columns.is_empty();

        if schema.is_table_id {
            for (name, id) in entities.into_iter() {
                schema.table_map.insert(name.clone(), id);
                schema
                    .id_name_rev
                    .insert((KeyType::Entity, id), name);
            }
            for (name, id) in relations.into_iter() {
                schema.table_map.insert(name.clone(), id);
                schema
                    .id_name_rev
                    .insert((KeyType::Relation, id), name);
            }
        }
        if schema.is_column_id {
            for (name, id) in columns.into_iter() {
                schema.column_map.insert(name.clone(), id);
                schema
                    .id_name_rev
                    .insert((KeyType::Column, id), name);
            }
        }

        schema
    }
}

impl JsonIO for Schema {
    fn into_json<W: io::Write>(self, writer: W) -> io::Result<()> {
        let entities_pb: Vec<schema_pb::EntityMeta> = if !self.entities.is_empty() {
            self.entities.clone()
        } else {
            let mut entities = Vec::new();
            for (&(ty, id), name) in &self.id_name_rev {
                if ty == KeyType::Entity {
                    entities.push(schema_pb::EntityMeta {
                        label: Some(schema_pb::LabelMeta { id, name: name.to_string() }),
                        columns: vec![],
                    })
                }
            }
            entities
        };

        let relations_pb: Vec<schema_pb::RelationMeta> = if !self.rels.is_empty() {
            self.rels.clone()
        } else {
            let mut relations = Vec::new();
            for (&(ty, id), name) in &self.id_name_rev {
                if ty == KeyType::Relation {
                    let mut relation_meta = schema_pb::RelationMeta {
                        label: Some(schema_pb::LabelMeta { id, name: name.to_string() }),
                        entity_pairs: vec![],
                        columns: vec![],
                    };
                    if let Some(labels) = self.get_relation_labels(&id.into()) {
                        relation_meta.entity_pairs = labels
                            .iter()
                            .cloned()
                            .map(|(src, dst)| schema_pb::relation_meta::LabelPair {
                                src: Some(src.into()),
                                dst: Some(dst.into()),
                            })
                            .collect();
                    }
                    relations.push(relation_meta);
                }
            }
            relations
        };

        let schema_pb = schema_pb::Schema {
            entities: entities_pb,
            relations: relations_pb,
            is_table_id: self.is_table_id,
            is_column_id: self.is_column_id,
        };
        serde_json::to_writer_pretty(writer, &schema_pb)?;
        Ok(())
    }

    fn from_json<R: io::Read>(reader: R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let schema_pb = serde_json::from_reader::<_, schema_pb::Schema>(reader)?;
        let mut schema = Schema::default();
        schema.entities = schema_pb.entities.clone();
        schema.rels = schema_pb.relations.clone();
        schema.is_table_id = schema_pb.is_table_id;
        schema.is_column_id = schema_pb.is_column_id;
        for entity in schema_pb.entities {
            if schema_pb.is_table_id {
                if let Some(label) = &entity.label {
                    if !schema.table_map.contains_key(&label.name) {
                        schema
                            .table_map
                            .insert(label.name.clone(), label.id);
                        schema
                            .id_name_rev
                            .insert((KeyType::Entity, label.id), label.name.clone());
                    }
                }
            }
            if schema_pb.is_column_id {
                for column in entity.columns {
                    if let Some(key) = &column.key {
                        if !schema.column_map.contains_key(&key.name) {
                            schema
                                .column_map
                                .insert(key.name.clone(), key.id);
                            schema
                                .id_name_rev
                                .insert((KeyType::Column, key.id), key.name.clone());
                        }
                    }
                }
            }
        }

        for rel in schema_pb.relations {
            if schema_pb.is_table_id {
                if let Some(label) = &rel.label {
                    if !schema.table_map.contains_key(&label.name) {
                        schema
                            .table_map
                            .insert(label.name.clone(), label.id);
                        schema
                            .id_name_rev
                            .insert((KeyType::Relation, label.id), label.name.clone());
                    }
                }
            }
            if schema_pb.is_column_id {
                for column in rel.columns {
                    if let Some(key) = &column.key {
                        if !schema.column_map.contains_key(&key.name) {
                            schema
                                .column_map
                                .insert(key.name.clone(), key.id);
                            schema
                                .id_name_rev
                                .insert((KeyType::Column, key.id), key.name.clone());
                        }
                    }
                }
            }
            if let Some(label) = &rel.label {
                let pairs = schema
                    .relation_labels
                    .entry(label.name.clone())
                    .or_default();
                for entity_pair in rel.entity_pairs {
                    if entity_pair.src.is_some() && entity_pair.dst.is_some() {
                        pairs.push((
                            entity_pair.src.clone().unwrap().into(),
                            entity_pair.dst.clone().unwrap().into(),
                        ))
                    }
                }
            }
        }

        Ok(schema)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Record the runtime schema of the node in the logical plan, for it being the vertex/edge
pub struct NodeMeta {
    /// The table names (labels)
    tables: BTreeSet<NameOrId>,
    /// The required columns (columns)
    columns: BTreeSet<NameOrId>,
    /// A flag to indicate that all columns are required
    is_all_columns: bool,
    /// Whether the current node accept columns
    is_add_column: bool,
}

impl NodeMeta {
    pub fn insert_column(&mut self, col: NameOrId) {
        if self.is_add_column {
            self.columns.insert(col);
        }
    }

    pub fn get_columns(&self) -> &BTreeSet<NameOrId> {
        &self.columns
    }

    pub fn insert_table(&mut self, table: NameOrId) {
        self.tables.insert(table);
    }

    pub fn get_tables(&self) -> &BTreeSet<NameOrId> {
        &self.tables
    }
}

#[derive(Clone, Debug)]
pub enum CurrNodeOpt {
    Single(u32),
    Union(Vec<u32>),
}

impl Default for CurrNodeOpt {
    fn default() -> Self {
        CurrNodeOpt::Single(0)
    }
}

#[derive(Clone, Debug)]
pub enum NodeMetaOpt {
    Single(Rc<RefCell<NodeMeta>>),
    Union(Vec<Rc<RefCell<NodeMeta>>>),
}

impl NodeMetaOpt {
    pub fn insert_column(&mut self, col: NameOrId) {
        match self {
            NodeMetaOpt::Single(meta) => meta.borrow_mut().insert_column(col),
            NodeMetaOpt::Union(metas) => {
                for meta in metas {
                    meta.borrow_mut().insert_column(col.clone());
                }
            }
        }
    }

    pub fn set_is_all_columns(&mut self, is_all_columns: bool) {
        match self {
            NodeMetaOpt::Single(meta) => meta.borrow_mut().is_all_columns = is_all_columns,
            NodeMetaOpt::Union(metas) => {
                for meta in metas {
                    meta.borrow_mut().is_all_columns = is_all_columns;
                }
            }
        }
    }

    pub fn is_all_columns(&self) -> bool {
        match self {
            NodeMetaOpt::Single(meta) => meta.borrow().is_all_columns,
            NodeMetaOpt::Union(metas) => {
                for meta in metas {
                    if meta.borrow().is_all_columns {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn set_is_add_column(&mut self, is_add_column: bool) {
        match self {
            NodeMetaOpt::Single(meta) => meta.borrow_mut().is_add_column = is_add_column,
            NodeMetaOpt::Union(metas) => {
                for meta in metas {
                    meta.borrow_mut().is_add_column = is_add_column;
                }
            }
        }
    }

    pub fn get_columns(&self) -> BTreeSet<NameOrId> {
        match self {
            NodeMetaOpt::Single(meta) => meta.borrow().get_columns().clone(),
            NodeMetaOpt::Union(metas) => {
                let mut union_cols = BTreeSet::new();
                for meta in metas {
                    union_cols.append(&mut meta.borrow_mut().columns);
                }
                union_cols
            }
        }
    }
}

/// To record any metadata while processing the logical plan, including:
/// * The tables/columns required by a given node
/// * The tag-node mutual mappings
/// * The tag-id mappings, if preprocessing tag to id
/// * TODO etc.
#[derive(Default, Clone, Debug)]
pub struct PlanMeta {
    /// To record all possible tables/columns of a node, which is typically referred from a tag
    /// while processing projection, selection, groupby, orderby, and etc. For example, when
    /// select the record via an expression "a.name == \"John\"", the tag "a" must refer to
    /// some node in the logical plan, and the node requires the column of \"John\". Such
    /// information is critical in distributed processing, as the computation may not align
    /// with the storage to access the required column. Thus, such information can help
    /// the computation route and fetch columns.
    node_metas: BTreeMap<u32, Rc<RefCell<NodeMeta>>>,
    /// The tag must refer to some valid nodes in the plan.
    tag_nodes: BTreeMap<NameOrId, Vec<u32>>,
    /// To ease the processing, tag may be transformed to an internal id.
    /// This maintains the mappings
    tag_ids: BTreeMap<NameOrId, u32>,
    /// To record the current nodes' id in the logical plan. Note that nodes that have operators that
    /// of `As` or `Selection` does not alter curr_node.
    curr_node: CurrNodeOpt,
    /// The maximal tag id that has been assigned, for mapping tag ids.
    max_tag_id: u32,
    /// Whether to preprocess the table name into id.
    is_table_id: bool,
    /// Whether to preprocess the column name into id.
    is_column_id: bool,
    /// Whether to preprocess the tag name into id.
    is_tag_id: bool,
    /// Whether to partition the task
    is_partition: bool,
}

// Some constructors
impl PlanMeta {
    pub fn new(node_id: u32) -> Self {
        let mut plan_meta = PlanMeta::default();
        plan_meta.curr_node = CurrNodeOpt::Single(node_id);
        plan_meta.node_metas.entry(node_id).or_default();
        plan_meta
    }

    pub fn with_store_conf(mut self, is_table_id: bool, is_column_id: bool) -> Self {
        self.is_table_id = is_table_id;
        self.is_column_id = is_column_id;
        self
    }

    pub fn with_tag_id(mut self) -> Self {
        self.is_tag_id = true;
        self
    }

    pub fn with_partition(mut self) -> Self {
        self.is_partition = true;
        self
    }
}

impl PlanMeta {
    pub fn curr_node_metas_mut(&mut self) -> NodeMetaOpt {
        match &self.curr_node {
            CurrNodeOpt::Single(node) => NodeMetaOpt::Single(
                self.node_metas
                    .entry(*node)
                    .or_default()
                    .clone(),
            ),
            CurrNodeOpt::Union(nodes) => {
                let mut node_metas = vec![];
                for node in nodes {
                    node_metas.push(
                        self.node_metas
                            .entry(*node)
                            .or_default()
                            .clone(),
                    );
                }
                NodeMetaOpt::Union(node_metas)
            }
        }
    }

    pub fn tag_node_metas_mut(&mut self, tag_opt: Option<&NameOrId>) -> IrResult<NodeMetaOpt> {
        if let Some(tag) = tag_opt {
            if let Some(nodes) = self.tag_nodes.get(tag) {
                if nodes.len() == 1 {
                    Ok(NodeMetaOpt::Single(
                        self.node_metas
                            .entry(nodes[0])
                            .or_default()
                            .clone(),
                    ))
                } else {
                    let mut node_metas = vec![];
                    for node in nodes {
                        node_metas.push(
                            self.node_metas
                                .entry(*node)
                                .or_default()
                                .clone(),
                        )
                    }
                    Ok(NodeMetaOpt::Union(node_metas))
                }
            } else {
                Err(IrError::TagNotExist(tag.clone()))
            }
        } else {
            Ok(self.curr_node_metas_mut())
        }
    }

    pub fn get_node_meta(&self, id: u32) -> Option<Rc<RefCell<NodeMeta>>> {
        self.node_metas.get(&id).cloned()
    }

    pub fn curr_node_metas(&self) -> Option<NodeMetaOpt> {
        match &self.curr_node {
            CurrNodeOpt::Single(node) => self
                .node_metas
                .get(node)
                .map(|meta| NodeMetaOpt::Single(meta.clone())),
            CurrNodeOpt::Union(nodes) => {
                let mut node_metas = vec![];
                for node in nodes {
                    if let Some(node_meta) = self.node_metas.get(node) {
                        node_metas.push(node_meta.clone());
                    } else {
                        return None;
                    }
                }
                Some(NodeMetaOpt::Union(node_metas))
            }
        }
    }

    pub fn insert_tag_nodes(&mut self, tag: NameOrId, nodes: Vec<u32>) {
        self.tag_nodes
            .entry(tag)
            .or_default()
            .extend(nodes.into_iter());
    }

    pub fn get_tag_nodes(&self, tag: &NameOrId) -> Option<Vec<u32>> {
        self.tag_nodes.get(tag).cloned()
    }

    /// Get the id (with a `true` indicator) of the given tag if it already presents,
    /// otherwise, set and return the id as `self.max_tag_id` (with a `false` indicator).
    pub fn get_or_set_tag_id(&mut self, tag: NameOrId) -> (bool, u32) {
        let entry = self.tag_ids.entry(tag);
        match entry {
            Entry::Occupied(o) => (true, *o.get()),
            Entry::Vacant(v) => {
                let new_tag_id = self.max_tag_id;
                v.insert(new_tag_id);
                self.max_tag_id += 1;
                (false, new_tag_id)
            }
        }
    }

    pub fn set_curr_node(&mut self, curr_node: u32) {
        self.curr_node = CurrNodeOpt::Single(curr_node);
    }

    pub fn set_union_curr_nodes(&mut self, nodes: Vec<u32>) {
        if nodes.len() == 1 {
            self.curr_node = CurrNodeOpt::Single(nodes[0]);
        } else {
            self.curr_node = CurrNodeOpt::Union(nodes);
        }
    }

    pub fn get_curr_nodes(&self) -> Vec<u32> {
        match &self.curr_node {
            CurrNodeOpt::Single(node) => vec![*node],
            CurrNodeOpt::Union(nodes) => nodes.clone(),
        }
    }

    /// Return a dummy node for maintaining tag that refers to neither vertex nor edge
    pub fn get_dummy_nodes(&self) -> Vec<u32> {
        vec![0xffffffff]
    }

    pub fn is_table_id(&self) -> bool {
        self.is_table_id
    }

    pub fn is_column_id(&self) -> bool {
        self.is_column_id
    }

    pub fn is_tag_id(&self) -> bool {
        self.is_tag_id
    }

    pub fn is_partition(&self) -> bool {
        self.is_partition
    }
}

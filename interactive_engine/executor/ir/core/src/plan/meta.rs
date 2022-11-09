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
use std::fmt::Debug;
use std::io;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::RwLock;

use ir_common::generated::schema as schema_pb;
use ir_common::{KeyId, OneOrMany};
use ir_common::{LabelId, NameOrId};

use crate::error::{IrError, IrResult};
use crate::plan::logical::NodeId;
use crate::JsonIO;

pub static INVALID_META_ID: KeyId = -1;
pub type TagId = u32;

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

pub fn set_schema(schema: Schema) {
    if let Ok(mut meta) = STORE_META.write() {
        meta.schema = Some(schema);
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
    id: KeyId,
}

impl LabelMeta {
    pub fn get_name(&self) -> String {
        self.name.clone()
    }

    pub fn get_id(&self) -> i32 {
        self.id
    }
}

impl Default for LabelMeta {
    fn default() -> Self {
        Self { name: "INVALID".into(), id: INVALID_META_ID }
    }
}

impl From<(String, KeyId)> for LabelMeta {
    fn from(tuple: (String, KeyId)) -> Self {
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
    /// A map from table (Entity or Relation) name to its internally encoded id
    /// In the concept of graph database, this is also known as label
    table_name_to_id: BTreeMap<String, (KeyType, LabelId)>,
    /// A map from column name to its store-encoded id
    /// In the concept of graph database, this is also known as property
    column_name_to_id: BTreeMap<String, KeyId>,
    /// Record the primary keys of each table
    primary_keys: BTreeMap<String, BTreeSet<String>>,
    /// A reversed map of `id` to `name` mapping
    id_to_name: BTreeMap<(KeyType, KeyId), String>,
    /// The entities' labels that are bound a certain type of relations
    relation_bound_labels: BTreeMap<KeyId, Vec<(LabelMeta, LabelMeta)>>,
    /// Is the table name mapped as id
    is_table_id: bool,
    /// Is the column name mapped as id
    is_column_id: bool,
    /// Entities
    entities: Vec<schema_pb::EntityMeta>,
    /// Relations
    relations: Vec<schema_pb::RelationMeta>,
}

impl Schema {
    pub fn new(
        entities: Vec<(String, LabelId)>, relations: Vec<(String, LabelId)>, columns: Vec<(String, KeyId)>,
    ) -> Self {
        let mut schema = Schema::default();
        schema.is_table_id = !entities.is_empty() || !relations.is_empty();
        schema.is_column_id = !columns.is_empty();

        if schema.is_table_id {
            for (name, id) in entities.into_iter() {
                schema
                    .table_name_to_id
                    .insert(name.clone(), (KeyType::Entity, id));
                schema
                    .id_to_name
                    .insert((KeyType::Entity, id), name);
            }
            for (name, id) in relations.into_iter() {
                schema
                    .table_name_to_id
                    .insert(name.clone(), (KeyType::Relation, id));
                schema
                    .id_to_name
                    .insert((KeyType::Relation, id), name);
            }
        }
        if schema.is_column_id {
            for (name, id) in columns.into_iter() {
                schema
                    .column_name_to_id
                    .insert(name.clone(), id);
                schema
                    .id_to_name
                    .insert((KeyType::Column, id), name);
            }
        }

        schema
    }

    pub fn get_table_id(&self, name: &str) -> Option<LabelId> {
        self.table_name_to_id
            .get(name)
            .map(|(_, id)| *id)
    }

    pub fn get_column_id(&self, name: &str) -> Option<KeyId> {
        self.column_name_to_id.get(name).cloned()
    }

    pub fn get_entity_name(&self, id: KeyId) -> Option<&String> {
        self.id_to_name.get(&(KeyType::Entity, id))
    }

    pub fn get_relation_name(&self, id: KeyId) -> Option<&String> {
        self.id_to_name.get(&(KeyType::Relation, id))
    }

    pub fn get_column_name(&self, id: KeyId) -> Option<&String> {
        self.id_to_name.get(&(KeyType::Column, id))
    }

    /// To get the entities' labels that are bound to a relation of given type
    pub fn get_bound_labels(&self, label_id: KeyId) -> Option<&Vec<(LabelMeta, LabelMeta)>> {
        self.relation_bound_labels.get(&label_id)
    }

    pub fn is_column_id(&self) -> bool {
        self.is_column_id
    }

    pub fn is_table_id(&self) -> bool {
        self.is_table_id
    }

    /// Check whether a given table contains a given column as a primary key.
    /// Also return the number of primary keys of the given table.
    pub fn check_primary_key(&self, table: &str, col: &str) -> (bool, usize) {
        if let Some(pks) = self.primary_keys.get(table) {
            (pks.contains(col), pks.len())
        } else {
            (false, 0)
        }
    }
}

impl From<Schema> for schema_pb::Schema {
    fn from(schema: Schema) -> Self {
        let entities_pb: Vec<schema_pb::EntityMeta> = if !schema.entities.is_empty() {
            schema.entities.clone()
        } else {
            let mut entities = Vec::new();
            for (&(ty, id), name) in &schema.id_to_name {
                if ty == KeyType::Entity {
                    entities.push(schema_pb::EntityMeta {
                        label: Some(schema_pb::LabelMeta { id, name: name.to_string() }),
                        columns: vec![],
                    })
                }
            }
            entities
        };

        let relations_pb: Vec<schema_pb::RelationMeta> = if !schema.relations.is_empty() {
            schema.relations.clone()
        } else {
            let mut relations = Vec::new();
            for (&(ty, id), name) in &schema.id_to_name {
                if ty == KeyType::Relation {
                    let mut relation_meta = schema_pb::RelationMeta {
                        label: Some(schema_pb::LabelMeta { id, name: name.to_string() }),
                        entity_pairs: vec![],
                        columns: vec![],
                    };
                    if let Some(labels) = schema.get_bound_labels(id) {
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

        schema_pb::Schema {
            entities: entities_pb,
            relations: relations_pb,
            is_table_id: schema.is_table_id,
            is_column_id: schema.is_column_id,
        }
    }
}

impl From<schema_pb::Schema> for Schema {
    fn from(schema_pb: schema_pb::Schema) -> Self {
        let mut schema = Schema::default();
        schema.entities = schema_pb.entities.clone();
        schema.relations = schema_pb.relations.clone();
        schema.is_table_id = schema_pb.is_table_id;
        schema.is_column_id = schema_pb.is_column_id;
        for entity in schema_pb.entities {
            if schema_pb.is_table_id {
                if let Some(label) = &entity.label {
                    if !schema
                        .table_name_to_id
                        .contains_key(&label.name)
                    {
                        schema
                            .table_name_to_id
                            .insert(label.name.clone(), (KeyType::Entity, label.id));
                        schema
                            .id_to_name
                            .insert((KeyType::Entity, label.id), label.name.clone());
                    }
                }
            }

            for column in entity.columns {
                if let Some(key) = &column.key {
                    if schema_pb.is_column_id {
                        if !schema.column_name_to_id.contains_key(&key.name) {
                            schema
                                .column_name_to_id
                                .insert(key.name.clone(), key.id);
                            schema
                                .id_to_name
                                .insert((KeyType::Column, key.id), key.name.clone());
                        }
                    }
                    if column.is_primary_key {
                        if let Some(label) = &entity.label {
                            schema
                                .primary_keys
                                .entry(label.name.clone())
                                .or_insert_with(BTreeSet::new)
                                .insert(key.name.clone());
                        }
                    }
                }
            }
        }

        for rel in schema_pb.relations {
            if schema_pb.is_table_id {
                if let Some(label) = &rel.label {
                    if !schema
                        .table_name_to_id
                        .contains_key(&label.name)
                    {
                        schema
                            .table_name_to_id
                            .insert(label.name.clone(), (KeyType::Relation, label.id));
                        schema
                            .id_to_name
                            .insert((KeyType::Relation, label.id), label.name.clone());
                    }
                }
            }

            for column in rel.columns {
                if let Some(key) = &column.key {
                    if schema_pb.is_column_id {
                        if !schema.column_name_to_id.contains_key(&key.name) {
                            schema
                                .column_name_to_id
                                .insert(key.name.clone(), key.id);
                            schema
                                .id_to_name
                                .insert((KeyType::Column, key.id), key.name.clone());
                        }
                    }
                    if column.is_primary_key {
                        if let Some(label) = &rel.label {
                            schema
                                .primary_keys
                                .entry(label.name.clone())
                                .or_insert_with(BTreeSet::new)
                                .insert(key.name.clone());
                        }
                    }
                }
            }
            if let Some(label) = &rel.label {
                let pairs = schema
                    .relation_bound_labels
                    .entry(label.id)
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

        schema
    }
}

impl JsonIO for Schema {
    fn into_json<W: io::Write>(self, writer: W) -> io::Result<()> {
        let schema_pb = schema_pb::Schema::from(self);
        serde_json::to_writer_pretty(writer, &schema_pb)?;

        Ok(())
    }

    fn from_json<R: io::Read>(reader: R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let schema_pb = serde_json::from_reader::<_, schema_pb::Schema>(reader)?;
        let schema = Schema::from(schema_pb);
        Ok(schema)
    }
}

/// To define the options of required columns by the computing node of the query plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnsOpt {
    /// Initial state
    Init,
    /// None column is required
    None,
    /// Some columns are required
    Partial(BTreeSet<NameOrId>),
    /// All columns are required, with an integer to indicate the number of columns
    All(usize),
}

impl Default for ColumnsOpt {
    fn default() -> Self {
        Self::Init
    }
}

impl ColumnsOpt {
    pub fn new(cols: BTreeSet<NameOrId>) -> Self {
        Self::Partial(cols)
    }

    pub fn is_init(&self) -> bool {
        match self {
            ColumnsOpt::Init => true,
            _ => false,
        }
    }

    pub fn is_all(&self) -> bool {
        match self {
            ColumnsOpt::All(_) => true,
            _ => false,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnsOpt::Init => 0,
            ColumnsOpt::None => 0,
            ColumnsOpt::Partial(cols) => cols.len(),
            ColumnsOpt::All(size) => *size,
        }
    }

    pub fn insert(&mut self, col: NameOrId) -> bool {
        if self.is_init() {
            let cols = BTreeSet::new();
            *self = Self::Partial(cols)
        }
        match self {
            ColumnsOpt::Partial(cols) => cols.insert(col),
            _ => false,
        }
    }

    pub fn remove(&mut self, col: &NameOrId) -> bool {
        match self {
            ColumnsOpt::Partial(cols) => cols.remove(col),
            _ => false,
        }
    }

    pub fn contains(&self, col: &NameOrId) -> bool {
        match self {
            ColumnsOpt::Init => false,
            ColumnsOpt::None => false,
            ColumnsOpt::Partial(cols) => cols.contains(col),
            ColumnsOpt::All(_) => true,
        }
    }

    pub fn get(&self) -> Vec<NameOrId> {
        match self {
            ColumnsOpt::Partial(cols) => cols.iter().cloned().collect(),
            _ => vec![],
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Record the runtime schema of the node in the logical plan, for it being the vertex/edge
pub struct NodeMeta {
    /// The table names (labels)
    tables: BTreeSet<NameOrId>,
    /// The required columns of current node
    columns: ColumnsOpt,
    /// The required columns of the nodes with certain tags. `None` tag means the columns
    /// for the head nodes that current node refers to.
    tag_columns: BTreeMap<Option<TagId>, ColumnsOpt>,
}

impl NodeMeta {
    pub fn insert_table(&mut self, table: NameOrId) {
        self.tables.insert(table);
    }

    pub fn get_tables(&self) -> &BTreeSet<NameOrId> {
        &self.tables
    }
}

pub struct NodeMetaOpt {
    inner: OneOrMany<Rc<RefCell<NodeMeta>>>,
}

impl AsRef<OneOrMany<Rc<RefCell<NodeMeta>>>> for NodeMetaOpt {
    fn as_ref(&self) -> &OneOrMany<Rc<RefCell<NodeMeta>>> {
        &self.inner
    }
}

impl Deref for NodeMetaOpt {
    type Target = OneOrMany<Rc<RefCell<NodeMeta>>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl From<OneOrMany<Rc<RefCell<NodeMeta>>>> for NodeMetaOpt {
    fn from(inner: OneOrMany<Rc<RefCell<NodeMeta>>>) -> Self {
        Self { inner }
    }
}

impl NodeMetaOpt {
    pub fn set_columns_opt(&mut self, columns_opt: ColumnsOpt) {
        match self.as_ref() {
            // The number 256 is given arbitrarily, which however should be determined by the number
            // of actual columns for the given node.
            OneOrMany::One(meta) => {
                meta[0].borrow_mut().columns = columns_opt;
            }
            OneOrMany::Many(metas) => {
                for meta in metas {
                    meta.borrow_mut().columns = columns_opt.clone();
                }
            }
        }
    }

    pub fn insert_column(&mut self, col: NameOrId) {
        match self.as_ref() {
            OneOrMany::One(meta) => {
                meta[0].borrow_mut().columns.insert(col);
            }
            OneOrMany::Many(metas) => {
                for meta in metas {
                    meta.borrow_mut().columns.insert(col.clone());
                }
            }
        }
    }

    pub fn set_tag_columns_opt(&mut self, tag: Option<TagId>, columns_opt: ColumnsOpt) {
        match self.as_ref() {
            OneOrMany::One(meta) => {
                meta[0]
                    .borrow_mut()
                    .tag_columns
                    .insert(tag, columns_opt);
            }
            OneOrMany::Many(metas) => {
                for meta in metas {
                    meta.borrow_mut()
                        .tag_columns
                        .insert(tag, columns_opt.clone());
                }
            }
        }
    }

    pub fn insert_tag_column(&mut self, tag: Option<TagId>, col: NameOrId) {
        match self.as_ref() {
            OneOrMany::One(meta) => {
                meta[0]
                    .borrow_mut()
                    .tag_columns
                    .entry(tag)
                    .or_default()
                    .insert(col);
            }
            OneOrMany::Many(metas) => {
                for meta in metas {
                    meta.borrow_mut()
                        .tag_columns
                        .entry(tag)
                        .or_default()
                        .insert(col.clone());
                }
            }
        }
    }

    pub fn is_all_columns(&self) -> bool {
        match self.as_ref() {
            OneOrMany::One(meta) => meta[0].borrow().columns.is_all(),
            OneOrMany::Many(metas) => {
                for meta in metas {
                    if meta.borrow().columns.is_all() {
                        return true;
                    }
                }
                false
            }
        }
    }

    pub fn get_columns(&self) -> Vec<NameOrId> {
        self[0].borrow().columns.get()
    }

    pub fn get_tag_columns(&self) -> BTreeMap<Option<TagId>, ColumnsOpt> {
        self[0].borrow().tag_columns.clone()
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
    node_metas: BTreeMap<NodeId, Rc<RefCell<NodeMeta>>>,
    /// Refer a node to the node that points to the head of the plan.
    /// A head of the plan is often determined by an operator that changes the head of the
    /// `Record`. Such operators include Scan, EdgeExpand, PathExpand, GetV, Project, etc.
    referred_nodes: BTreeMap<NodeId, OneOrMany<NodeId>>,
    /// The tag must refer to some valid nodes in the plan.
    tag_nodes: BTreeMap<TagId, Vec<NodeId>>,
    /// To ease the processing, tag may be mapped to an internal id.
    /// This maintains the mappings
    tag_ids: BTreeMap<String, TagId>,
    /// Record the current node that has been processed by the plan
    curr_node: NodeId,
    /// The maximal tag id that has been assigned, for mapping tag ids.
    max_tag_id: TagId,
    /// Whether to partition the task
    is_partition: bool,
}

// Some constructors
impl PlanMeta {
    pub fn new(node_id: NodeId) -> Self {
        let mut plan_meta = PlanMeta::default();
        plan_meta.curr_node = node_id;
        plan_meta.node_metas.entry(node_id).or_default();
        plan_meta
    }

    pub fn with_partition(mut self) -> Self {
        self.is_partition = true;
        self
    }
}

impl PlanMeta {
    pub fn set_tag_nodes(&mut self, tag: TagId, nodes: Vec<NodeId>) {
        *self.tag_nodes.entry(tag).or_default() = nodes;
    }

    pub fn get_tag_nodes(&self, tag: TagId) -> &[NodeId] {
        if let Some(nodes) = self.tag_nodes.get(&tag) {
            nodes.as_slice()
        } else {
            &[]
        }
    }

    pub fn has_tag(&self, tag: TagId) -> bool {
        self.tag_nodes.contains_key(&tag)
    }

    /// Get the id (with a `true` indicator) of the given tag if it already presents,
    /// otherwise, set and return the id as `self.max_tag_id` (with a `false` indicator).
    pub fn get_or_set_tag_id(&mut self, tag: &str) -> (bool, TagId) {
        let entry = self.tag_ids.entry(tag.to_string());
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

    pub fn get_max_tag_id(&self) -> TagId {
        self.max_tag_id
    }

    pub fn set_max_tag_id(&mut self, tag_id: TagId) {
        if self.max_tag_id < tag_id {
            self.max_tag_id = tag_id;
        }
    }

    pub fn get_tag_id_mappings(&self) -> &BTreeMap<String, TagId> {
        &self.tag_ids
    }

    pub fn get_tag_id(&self, tag: &str) -> Option<TagId> {
        self.tag_ids.get(tag).cloned()
    }

    pub fn set_curr_node(&mut self, node: NodeId) {
        self.curr_node = node;
    }

    pub fn get_curr_node(&self) -> NodeId {
        self.curr_node
    }

    pub fn refer_to_nodes(&mut self, node: NodeId, nodes: Vec<NodeId>) {
        self.referred_nodes.insert(
            node,
            if nodes.len() == 1 { OneOrMany::One([nodes[0]]) } else { OneOrMany::Many(nodes) },
        );
    }

    pub fn get_referred_nodes(&self, nodes: &[NodeId]) -> Vec<NodeId> {
        if nodes.len() == 1 {
            if let Some(referred) = self.referred_nodes.get(&nodes[0]) {
                referred.as_ref().to_vec()
            } else {
                vec![]
            }
        } else {
            let mut results = BTreeSet::new();
            for node in nodes {
                if let Some(referred) = self.referred_nodes.get(node) {
                    results.extend(referred.as_ref().iter().cloned())
                }
            }
            results.into_iter().collect()
        }
    }

    /// Get the referred nodes of current node
    pub fn get_curr_referred_nodes(&self) -> &[NodeId] {
        if let Some(nodes) = self.referred_nodes.get(&self.curr_node) {
            nodes.as_ref()
        } else {
            &[]
        }
    }

    /// Get the metadata of one given node. If the metadata does not exist, return `None`.
    pub fn get_node_meta(&self, node: NodeId) -> Option<NodeMetaOpt> {
        self.node_metas
            .get(&node)
            .map(|meta| OneOrMany::One([meta.clone()]).into())
    }

    /// Get the metadata of given nodes. If the metadata does not exist, return `None`.
    pub fn get_nodes_meta(&self, nodes: &[NodeId]) -> Option<NodeMetaOpt> {
        if nodes.len() == 1 {
            self.node_metas
                .get(&nodes[0])
                .map(|meta| OneOrMany::One([meta.clone()]).into())
        } else if nodes.len() > 1 {
            let mut node_metas = vec![];
            for node in nodes {
                if let Some(meta) = self.node_metas.get(node) {
                    node_metas.push(meta.clone());
                } else {
                    return None;
                }
            }
            Some(OneOrMany::Many(node_metas).into())
        } else {
            // empty cases
            None
        }
    }

    pub fn get_curr_node_meta(&self) -> Option<NodeMetaOpt> {
        self.get_node_meta(self.curr_node)
    }

    /// Get or insert the metadata of given nodes.
    pub fn get_or_insert_nodes_meta(&mut self, nodes: &[NodeId]) -> NodeMetaOpt {
        if nodes.len() == 1 {
            OneOrMany::One([self
                .node_metas
                .entry(nodes[0])
                .or_default()
                .clone()])
            .into()
        } else {
            let mut node_metas = vec![];
            for &node in nodes {
                node_metas.push(self.node_metas.entry(node).or_default().clone())
            }
            OneOrMany::Many(node_metas).into()
        }
    }

    /// Get the tag-associated nodes' metadata.
    /// If the tag is `None`, the associated nodes are the referred nodes of current nodes.
    /// If the metadata does not exist, create one and return the newly created.
    /// If there is no node associated with the tag, return `TagNotExist` error.
    pub fn tag_nodes_meta_mut(&mut self, tag_opt: Option<TagId>) -> IrResult<NodeMetaOpt> {
        if let Some(tag) = tag_opt {
            if let Some(nodes) = self.tag_nodes.get(&tag).cloned() {
                Ok(self.get_or_insert_nodes_meta(&nodes))
            } else {
                Ok(self.get_or_insert_nodes_meta(&[]))
            }
        } else {
            let ref_curr_nodes = self.get_curr_referred_nodes().to_vec();
            if !ref_curr_nodes.is_empty() {
                Ok(self.get_or_insert_nodes_meta(&ref_curr_nodes))
            } else {
                Err(IrError::MissingData("the head of the plan refers to empty nodes".to_string()))
            }
        }
    }

    /// Get curr node's metadata. If the metadata does not exist, create one and return.
    pub fn curr_node_meta_mut(&mut self) -> NodeMetaOpt {
        self.get_or_insert_nodes_meta(&vec![self.curr_node])
    }

    pub fn is_partition(&self) -> bool {
        self.is_partition
    }
}

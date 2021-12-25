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

use std::collections::HashMap;
use std::io;
use std::sync::RwLock;

use ir_common::generated::common as common_pb;
use ir_common::generated::schema as schema_pb;

use crate::JsonIO;

lazy_static! {
    pub static ref META_DATA: RwLock<MetaData> = RwLock::new(MetaData::default());
}

pub fn set_schema_from_json<R: io::Read>(read: R) {
    if let Ok(mut meta) = META_DATA.write() {
        if let Ok(schema) = Schema::from_json(read) {
            meta.schema = Some(schema);
        }
    }
}

/// The simple schema, mapping either label or property name into id.
pub fn set_schema_simple(
    entities: Vec<(String, i32)>, relations: Vec<(String, i32)>, columns: Vec<(String, i32)>,
) {
    if let Ok(mut meta) = META_DATA.write() {
        let schema: Schema = (entities, relations, columns).into();
        meta.schema = Some(schema)
    }
}

pub fn reset_schema() {
    if let Ok(mut meta) = META_DATA.write() {
        meta.schema = None;
    }
}

#[derive(Clone, Debug, Default)]
pub struct MetaData {
    pub schema: Option<Schema>,
}

#[derive(Clone, Debug, Default)]
pub struct Column {
    name: String,
    id: i32,
    data_type: common_pb::DataType,
}

#[derive(Clone, Debug, Default)]
pub struct Entity {
    name: String,
    id: i32,
}

#[derive(Clone, Debug, Default)]
pub struct Relation {
    src: Entity,
    dst: Entity,
    edge: Entity,
}

impl From<schema_pb::ColumnKey> for Column {
    fn from(column_pb: schema_pb::ColumnKey) -> Self {
        Column {
            name: column_pb.name.clone(),
            id: column_pb.id,
            data_type: unsafe { std::mem::transmute::<i32, common_pb::DataType>(column_pb.data_type) },
        }
    }
}

fn into_entity(entity_pb: schema_pb::Entity) -> (Entity, Vec<Column>) {
    let entity = Entity { name: entity_pb.name.clone(), id: entity_pb.id };
    let columns = entity_pb
        .columns
        .into_iter()
        .map(|col| col.into())
        .collect();

    (entity, columns)
}

fn into_entity_pb(tuple: (Entity, Vec<Column>)) -> schema_pb::Entity {
    schema_pb::Entity {
        id: tuple.0.id,
        name: tuple.0.name.clone(),
        columns: tuple
            .1
            .into_iter()
            .map(|col| schema_pb::ColumnKey {
                id: col.id,
                name: col.name.clone(),
                data_type: unsafe { std::mem::transmute::<common_pb::DataType, i32>(col.data_type) },
            })
            .collect(),
    }
}

fn into_relation(rel_pb: schema_pb::Relation) -> (Relation, Vec<Column>) {
    let src = Entity { name: rel_pb.src_name.clone(), id: rel_pb.src_id };
    let dst = Entity { name: rel_pb.dst_name.clone(), id: rel_pb.dst_id };
    let edge = Entity { name: rel_pb.name.clone(), id: rel_pb.id };
    let columns = rel_pb
        .columns
        .into_iter()
        .map(|col| col.into())
        .collect();

    (Relation { src, dst, edge }, columns)
}

fn into_relation_pb(tuple: (Relation, Vec<Column>)) -> schema_pb::Relation {
    schema_pb::Relation {
        src_id: tuple.0.src.id,
        src_name: tuple.0.src.name.clone(),
        dst_id: tuple.0.dst.id,
        dst_name: tuple.0.dst.name.clone(),
        id: tuple.0.edge.id,
        name: tuple.0.edge.name.clone(),
        columns: tuple
            .1
            .into_iter()
            .map(|col| schema_pb::ColumnKey {
                id: col.id,
                name: col.name.clone(),
                data_type: unsafe { std::mem::transmute::<common_pb::DataType, i32>(col.data_type) },
            })
            .collect(),
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum TableType {
    Entity = 0,
    Relation = 1,
}

impl Default for TableType {
    fn default() -> Self {
        Self::Entity
    }
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    /// A map from table name to its internally encoded id
    /// In the concept of graph database, this is also known as label
    table_map: HashMap<String, i32>,
    /// A reversed map of `table_map`
    table_map_rev: HashMap<(TableType, i32), String>,
    /// A map from column name to its internally encoded id
    /// In the concept of graph database, this is also known as property
    column_map: HashMap<String, i32>,
    /// A reversed map of `column_map`
    column_map_rev: HashMap<i32, String>,
    /// Is the table name mapped as id
    is_table_id: bool,
    /// Is the column name mapped as id
    is_column_id: bool,
    /// Entities
    entities: Vec<(Entity, Vec<Column>)>,
    /// Relations
    rels: Vec<(Relation, Vec<Column>)>,
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

    pub fn get_table_name(&self, id: i32, ty: TableType) -> Option<&String> {
        self.table_map_rev.get(&(ty, id))
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

    pub fn get_column_name(&self, id: i32) -> Option<&String> {
        self.column_map_rev.get(&id)
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
                    .table_map_rev
                    .insert((TableType::Entity, id), name);
            }
            for (name, id) in relations.into_iter() {
                schema.table_map.insert(name.clone(), id);
                schema
                    .table_map_rev
                    .insert((TableType::Relation, id), name);
            }
        }
        if schema.is_column_id {
            for (name, id) in columns.into_iter() {
                schema.column_map.insert(name.clone(), id);
                schema.column_map_rev.insert(id, name);
            }
        }

        schema
    }
}

impl JsonIO for Schema {
    fn into_json<W: io::Write>(self, writer: W) -> io::Result<()> {
        let entities_pb: Vec<schema_pb::Entity> = if !self.entities.is_empty() {
            self
                .entities
                .clone()
                .into_iter()
                .map(|tuple| into_entity_pb(tuple))
                .collect()
        } else {
            let mut entities = Vec::new();
            for (&(ty, id), name) in &self.table_map_rev {
                if ty == TableType::Entity {
                    entities.push(schema_pb::Entity {
                        id,
                        name: name.clone(),
                        columns: vec![]
                    })
                }
            }
            entities
        };

        let relations_pb: Vec<schema_pb::Relation> = if !self.rels.is_empty() {
            self
                .rels
                .clone()
                .into_iter()
                .map(|tuple| into_relation_pb(tuple))
                .collect()
        } else {
            let mut relations = Vec::new();
            for (&(ty, id), name) in &self.table_map_rev {
                if ty == TableType::Relation {
                    relations.push(schema_pb::Relation {
                        src_id: -1,
                        src_name: "".to_string(),
                        dst_id: -1,
                        dst_name: "".to_string(),
                        id,
                        name: name.clone(),
                        columns: vec![]
                    })
                }
            }
            relations
        };

        let schema_pb = schema_pb::Schema {
            entities: entities_pb,
            rels: relations_pb,
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
        schema.entities = schema_pb
            .entities
            .clone()
            .into_iter()
            .map(|entity_pb| into_entity(entity_pb))
            .collect();
        schema.rels = schema_pb
            .rels
            .clone()
            .into_iter()
            .map(|rel_pb| into_relation(rel_pb))
            .collect();
        schema.is_table_id = schema_pb.is_table_id;
        schema.is_column_id = schema_pb.is_column_id;
        for entity in schema_pb.entities {
            if schema_pb.is_table_id {
                let key = &entity.name;
                if !schema.table_map.contains_key(key) {
                    schema.table_map.insert(key.clone(), entity.id);
                    schema
                        .table_map_rev
                        .insert((TableType::Entity, entity.id), key.clone());
                }
            }
            if schema_pb.is_column_id {
                for column in entity.columns {
                    let key = &column.name;
                    if !schema.column_map.contains_key(key) {
                        schema.column_map.insert(key.clone(), column.id);
                        schema
                            .column_map_rev
                            .insert(column.id, key.clone());
                    }
                }
            }
        }

        for rel in schema_pb.rels {
            if schema_pb.is_table_id {
                let key = &rel.name;
                if !schema.table_map.contains_key(key) {
                    schema.table_map.insert(key.clone(), rel.id);
                    schema.table_map_rev.insert((TableType::Relation, rel.id), key.clone());
                }
            }
            if schema_pb.is_column_id {
                for column in rel.columns {
                    let key = &column.name;
                    if !schema.column_map.contains_key(key) {
                        schema.column_map.insert(key.clone(), column.id);
                        schema
                            .column_map_rev
                            .insert(column.id, key.clone());
                    }
                }
            }
        }

        Ok(schema)
    }
}

/*
#[derive(Clone, Debug, Default)]
pub struct TagMap {
    /// A map from tag to its internally encoded id
    tag_map: HashMap<common_pb::NameOrId, i32>,
    /// A reversed map of `tag_map`
    tag_map_rev: Vec<common_pb::NameOrId>,
    /// The current assigned maximum tag id
    max_tag_id: i32,
}

impl TagMap {
    pub fn get_tag_id(&self, name: NameOrId) -> Option<i32> {
        self.tag_map.get(&name).cloned()
    }

    pub fn get_tag_name(&self, id: i32) -> Option<NameOrId> {
        self.tag_map_rev.get(id as usize).cloned()
    }

    pub fn assign_tag_id(&mut self, tag: NameOrId) -> Option<i32> {
        let curr_id = self.max_tag_id;
        if !self.tag_map.contains_key(&tag) {
            self.tag_map.insert(tag.clone(), curr_id);
            self.tag_map_rev.push(tag);
            self.max_tag_id += 1;

            Some(curr_id)
        } else {
            None
        }
    }
}
 */

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

use ir_common::generated::common as common_pb;
use ir_common::generated::common::name_or_id::Item;
use ir_common::generated::schema as schema_pb;

use crate::JsonIO;

#[derive(Clone, Debug, Default)]
pub struct MetaData {
    pub schema: Option<Schema>,
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    /// A map from table name to its internally encoded id
    /// In the concept of graph database, this is also known as label
    table_map: HashMap<String, i32>,
    /// A reversed map of `table_map`
    table_map_rev: HashMap<i32, String>,
    /// A map from column name to its internally encoded id
    /// In the concept of graph database, this is also known as property
    column_map: HashMap<String, i32>,
    /// A reversed map of `column_map`
    column_map_rev: HashMap<i32, String>,
    /// Is the table name mapped as id
    is_table_id: bool,
    /// Is the column name mapped as id
    is_column_id: bool,
}

impl Schema {
    pub fn get_table_id(&self, name: &str) -> Option<i32> {
        self.table_map.get(name).cloned()
    }

    pub fn get_table_id_from_pb(&self, name: &common_pb::NameOrId) -> Option<i32> {
        name.item.as_ref().and_then(|item| match item {
            Item::Name(name) => self.get_table_id(name),
            Item::Id(id) => Some(*id),
        })
    }

    pub fn get_table_name(&self, id: i32) -> Option<&String> {
        self.table_map_rev.get(&id)
    }

    pub fn get_column_id(&self, name: &str) -> Option<i32> {
        self.column_map.get(name).cloned()
    }

    pub fn get_column_id_from_pb(&self, name: &common_pb::NameOrId) -> Option<i32> {
        name.item.as_ref().and_then(|item| match item {
            Item::Name(name) => self.get_column_id(name),
            Item::Id(id) => Some(*id),
        })
    }

    pub fn get_column_name(&self, id: i32) -> Option<&String> {
        self.column_map_rev.get(&id)
    }
}

#[derive(PartialEq, Copy, Clone)]
pub enum ColumnOrTable {
    None = 0,
    Table = 1,
    Column = 2,
}

impl JsonIO for Schema {
    fn into_json<W: io::Write>(self, _writer: W) -> io::Result<()> {
        todo!()
    }

    fn from_json<R: io::Read>(reader: R) -> io::Result<Self>
    where
        Self: Sized,
    {
        let schema_pb = serde_json::from_reader::<_, schema_pb::Schema>(reader)?;
        let mut schema = Schema::default();
        schema.is_table_id = schema_pb.is_table_id;
        schema.is_column_id = schema_pb.is_column_id;
        for entity in schema_pb.entities {
            if schema_pb.is_table_id {
                let key = &entity.name;
                if !schema.table_map.contains_key(key) {
                    schema.table_map.insert(key.clone(), entity.id);
                    schema
                        .table_map_rev
                        .insert(entity.id, key.clone());
                }
            }
            if schema_pb.is_column_id {
                for column in entity.columns {
                    let key = &column.name;
                    if !schema.column_map.contains_key(key) {
                        schema.column_map.insert(key.clone(), column.id);
                        schema
                            .table_map_rev
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
                    schema.table_map_rev.insert(rel.id, key.clone());
                }
            }
            if schema_pb.is_column_id {
                for column in rel.columns {
                    let key = &column.name;
                    if !schema.column_map.contains_key(key) {
                        schema.column_map.insert(key.clone(), column.id);
                        schema
                            .table_map_rev
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

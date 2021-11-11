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

#![cfg(test)]
use super::data_type::*;
use super::prop_def::*;
use super::relation::*;
use super::schema::*;
use super::type_def::*;
use super::{PropId, LabelId, Schema};
use maxgraph_common::proto::schema::*;
use std::sync::Arc;

pub fn create_prop_def_proto(prop_id: PropId,
                             name: &str,
                             data_type: DataType,
                             comment: &str) -> PropertyDefProto {
    let mut proto = PropertyDefProto::new();
    proto.set_id(prop_id as i32);
    proto.set_name(name.to_owned());
    proto.set_dataType(data_type as i32);
    proto.set_comment(comment.to_owned());
    proto
}

pub fn create_prop_def(prop_id: PropId,
                             name: &str,
                             data_type: DataType,
                             comment: &str) -> PropDef {
    PropDef::from(&create_prop_def_proto(prop_id, name, data_type, comment))
}

pub fn create_vertex_type_def_proto(label: LabelId,
                                    name: &str,
                                    version: u32,
                                    properties: Vec<PropDef>,
                                    is_dimension: bool,
                                    comment: &str) -> TypeDefProto {
    let mut proto = TypeDefProto::new();
    proto.set_id(label as i32);
    proto.set_version(version as i32);
    proto.set_isDimensionType(is_dimension);
    proto.set_label(name.to_owned());
    proto.set_field_type(TypeIdProto_Type::VERTEX);
    for p in properties {
        proto.property.push(p.to_proto());
        proto.gidToPid.insert(p.get_prop_id() as i32, p.get_prop_id() as i32);
    }
    proto.set_comment(comment.to_owned());
    proto
}

pub fn create_vertex_type_def(label: LabelId,
                              name: &str,
                              version: u32,
                              properties: Vec<PropDef>,
                              is_dimension: bool,
                              comment: &str) -> TypeDef {
    let proto = create_vertex_type_def_proto(label, name, version, properties, is_dimension, comment);
    TypeDef::from(&proto)
}

pub fn create_edge_type_def_proto(relation: Relation,
                                    name: &str,
                                    version: u32,
                                    properties: Vec<PropDef>,
                                    is_dimension: bool,
                                    comment: &str) -> TypeDefProto {
    let mut proto = TypeDefProto::new();
    proto.set_id(relation.label as i32);
    proto.set_version(version as i32);
    proto.set_isDimensionType(is_dimension);
    proto.set_label(name.to_owned());
    proto.set_field_type(TypeIdProto_Type::EDGE);
    for p in properties {
        proto.property.push(p.to_proto());
        proto.gidToPid.insert(p.get_prop_id() as i32, p.get_prop_id() as i32);
    }
    proto.set_comment(comment.to_owned());
    proto.relationShip.push(relation.to_proto());
    proto
}

pub fn create_edge_type_def(relation: Relation,
                                  name: &str,
                                  version: u32,
                                  properties: Vec<PropDef>,
                                  is_dimension: bool,
                                  comment: &str) -> TypeDef {
    let proto = create_edge_type_def_proto(relation, name, version, properties, is_dimension, comment);
    TypeDef::from(&proto)
}

pub fn create_schema_proto(partition_num: u32, version: u32, type_defs: Vec<TypeDef>) -> SchemaProto {
    let mut proto = SchemaProto::new();
    proto.set_version(version as i32);
    proto.set_partitionNum(partition_num as i32);
    for t in type_defs {
        proto.field_type.push(t.to_proto());
    }
    proto
}

#[allow(dead_code)]
pub fn create_schema(partition_num: u32, version: u32, type_defs: Vec<TypeDef>) -> Arc<dyn Schema> {
    let proto = create_schema_proto(partition_num, version, type_defs);
    SchemaBuilder::from(&proto).build()
}

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

#![allow(dead_code)]
use super::{Schema, LabelId, PropId};
use super::data_type::DataType;
use std::collections::HashMap;
use std::sync::Arc;
use super::type_def::*;
use super::prop_def::PropDef;
use maxgraph_common::proto::schema::*;
use std::cell::UnsafeCell;
use std::collections::HashSet;
use protobuf::Message;

#[derive(Debug, Default)]
pub struct SchemaImpl {
    label_name_mapping: HashMap<String, LabelId>,
    prop_name_mapping: HashMap<String, PropId>,
    types: HashMap<LabelId, TypeDef>,
    props: HashMap<PropId, HashSet<PropDef>>,
    partition_num: u32,
    version: u32,
}

impl Schema for SchemaImpl {
    #[inline]
    fn get_prop_id(&self, name: &str) -> Option<PropId> {
        self.prop_name_mapping.get(name).map(|x| *x)
    }

    fn get_prop_type(&self, label: u32, prop_id: u32) -> Option<&DataType> {
        if let Some(t) = self.get_type_def(label) {
            t.get_prop_type(prop_id)
        } else {
            None
        }
    }

    #[inline]
    fn get_prop_types(&self, prop_id: PropId) -> Option<Vec<&DataType>> {
        self.props.get(&prop_id).map(|x| {
            x.iter().map(|p| p.get_data_type()).collect()
        })
    }

    #[inline]
    fn get_prop_name(&self, prop_id: PropId) -> Option<&str> {
        self.props.get(&prop_id).map(|x| {
            x.iter().next().unwrap().get_name()
        })
    }

    #[inline]
    fn get_label_id(&self, name: &str) -> Option<u32> {
        self.label_name_mapping.get(name).map(|x| *x)
    }

    #[inline]
    fn get_label_name(&self, label: u32) -> Option<&str> {
        self.get_type_def(label).map(|x| x.get_name())
    }

    #[inline]
    fn get_type_def(&self, label: u32) -> Option<&TypeDef> {
        self.types.get(&label)
    }

    fn get_type_defs(&self) -> Vec<&TypeDef> {
        self.types.values().collect()
    }

    fn get_version(&self) -> u32 {
        self.version
    }

    fn get_partition_num(&self) -> u32 {
        self.partition_num
    }

    fn to_proto(&self) -> Vec<u8> {
        let mut proto = SchemaProto::new();
        proto.set_version(self.version as i32);
        proto.set_partitionNum(self.partition_num as i32);
        for t in self.types.values() {
            proto.field_type.push(t.to_proto());
        }
        let mut buf = Vec::new();
        proto.write_to_vec(&mut buf).unwrap();
        buf
    }
}

impl SchemaImpl {
    #[inline]
    fn add_type_def(&mut self, type_def: TypeDef) {
        for (gid, p) in type_def.get_props() {
            self.prop_name_mapping.insert(p.get_name().to_owned(), *gid);
            self.props.entry(*gid).or_insert_with(|| HashSet::new()).insert(p.clone());
        }
        let label = type_def.get_label();
        self.label_name_mapping.insert(type_def.get_name().to_owned(), label);
        self.types.insert(label, type_def);
    }
}

#[derive(Default)]
pub struct SchemaBuilder {
    inner: UnsafeCell<SchemaImpl>,
}

#[allow(unused_mut)]
impl SchemaBuilder {
    #[inline]
    pub fn partition_num(mut self, partition_num: u32) -> Self {
        self.get_inner().partition_num = partition_num;
        self
    }

    #[inline]
    pub fn version(mut self, version: u32) -> Self {
        self.get_inner().version = version;
        self
    }

    #[inline]
    pub fn add_type_def(mut self, type_def: TypeDef) -> Self {
        self.get_inner().add_type_def(type_def);
        self
    }

    #[inline]
    pub fn build(self) -> Arc<dyn Schema> {
        Arc::new(self.inner.into_inner())
    }

    #[inline]
    fn get_inner(&self) -> &mut SchemaImpl {
        unsafe {
            &mut *self.inner.get()
        }
    }

    pub fn new() -> Self {
        Default::default()
    }
}

impl<'a> From<&'a SchemaProto> for SchemaBuilder {
    fn from(proto: &'a SchemaProto) -> Self {
        let mut builder = Self::new()
            .version(proto.version as u32)
            .partition_num(proto.partitionNum as u32);
        for type_def_proto in proto.get_field_type() {
            builder = builder.add_type_def(TypeDef::from(type_def_proto));
        }
        builder
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_util::*;
    use super::super::schema::*;
    use super::super::relation::*;
    use super::super::prop_def::*;
    use std::sync::Arc;

    #[test]
    fn test_schema_from_proto() {
        let (_, type_defs) = init();
        let proto = create_schema_proto(16, 1, type_defs);
        let schema = SchemaBuilder::from(&proto).build();
        check_schema(schema);
    }

    #[test]
    fn test_schema_builder() {
        let (_, type_defs) = init();
        let mut builder = SchemaBuilder::new()
            .version(1)
            .partition_num(16);
        for t in type_defs {
            builder = builder.add_type_def(t);
        }
        let schema = builder.build();
        check_schema(schema);
    }

    #[test]
    fn test_schema_to_proto() {
        let (_, type_defs) = init();
        let mut builder = SchemaBuilder::new()
            .version(1)
            .partition_num(16);
        for t in type_defs {
            builder = builder.add_type_def(t);
        }
        let schema = builder.build();
        let proto = ::protobuf::parse_from_bytes(&schema.to_proto()).unwrap();
        let schema = SchemaBuilder::from(&proto).build();
        check_schema(schema);
    }

    fn init() -> (Vec<PropDef>, Vec<TypeDef>) {
        let mut props = Vec::new();
        props.push(create_prop_def(1, "1", DataType::String, "11"));
        props.push(create_prop_def(2, "2", DataType::Int, "22"));
        props.push(create_prop_def(3, "3", DataType::Double, "33"));

        let mut type_defs = Vec::new();
        type_defs.push(create_vertex_type_def(1, "v1", 1, props.clone(), false, "v1"));
        type_defs.push(create_vertex_type_def(2, "v2", 2, props.clone(), true, "v2"));
        let relation1 = Relation::new(3, 1, 2);
        let relation2 = Relation::new(4, 2, 1);

        type_defs.push(create_edge_type_def(relation1.clone(), "e3", 3, props.clone(), false, "v3"));
        type_defs.push(create_edge_type_def(relation2.clone(), "e4", 4, props.clone(), true, "v4"));
        (props, type_defs)
    }

    fn check_schema(schema: Arc<dyn Schema>) {
        assert_eq!(schema.get_version(), 1);
        assert_eq!(schema.get_partition_num(), 16);
        assert_eq!(schema.get_prop_id("1").unwrap(), 1);
        assert_eq!(schema.get_prop_id("2").unwrap(), 2);
        assert_eq!(schema.get_prop_id("3").unwrap(), 3);
        assert_eq!(schema.get_prop_name(1).unwrap(), "1");
        assert_eq!(schema.get_prop_name(2).unwrap(), "2");
        assert_eq!(schema.get_prop_name(3).unwrap(), "3");
        assert_eq!(schema.get_label_id("v1").unwrap(), 1);
        assert_eq!(schema.get_label_id("v2").unwrap(), 2);
        assert_eq!(schema.get_label_id("e3").unwrap(), 3);
        assert_eq!(schema.get_label_id("e4").unwrap(), 4);
        assert_eq!(schema.get_label_name(1).unwrap(), "v1");
        assert_eq!(schema.get_label_name(2).unwrap(), "v2");
        assert_eq!(schema.get_label_name(3).unwrap(), "e3");
        assert_eq!(schema.get_label_name(4).unwrap(), "e4");
    }
}

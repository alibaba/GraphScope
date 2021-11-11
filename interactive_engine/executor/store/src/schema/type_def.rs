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
use super::{PropId, DataType};
use std::cell::UnsafeCell;
use maxgraph_common::proto as protos;
use std::collections::HashSet;
use super::relation::Relation;
use super::prop_def::*;
use super::LabelId;

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Type {
    Vertex,
    Edge,
}

impl Default for Type {
    fn default() -> Self {
        Type::Vertex
    }
}

impl<'a> From<&'a protos::schema::TypeIdProto_Type> for Type {
    fn from(proto: &'a protos::schema::TypeIdProto_Type) -> Self {
        match *proto {
            protos::schema::TypeIdProto_Type::VERTEX => Type::Vertex,
            protos::schema::TypeIdProto_Type::EDGE => Type::Edge,
        }
    }
}

impl Type {
    pub fn to_proto(&self) -> protos::schema::TypeIdProto_Type {
        match *self {
            Type::Vertex => protos::schema::TypeIdProto_Type::VERTEX,
            Type::Edge => protos::schema::TypeIdProto_Type::EDGE,
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct TypeDef {
    name: String,
    label: LabelId,
    data_type: Type,
    /// name to global prop id
    prop_name_mapping: HashMap<String, PropId>,
    /// global id to prop def
    props: HashMap<PropId, PropDef>,
    is_dimension_type: bool,
    comment: String,
    version: u32,
    relations: HashSet<Relation>,
    type_option: TypeOption,
}

impl TypeDef {
    pub fn build_def(name: String,
                     label: LabelId,
                     data_type: Type,
                     prop_name_mapping: HashMap<String, PropId>,
                     props: HashMap<PropId, PropDef>,
                     is_dimension_type: bool,
                     comment: String,
                     version: u32,
                     relations: HashSet<Relation>,
                     type_option: TypeOption, ) -> Self {
        TypeDef {
            name,
            label,
            data_type,
            prop_name_mapping,
            props,
            is_dimension_type,
            comment,
            version,
            relations,
            type_option,
        }
    }

    #[inline]
    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    #[inline]
    pub fn get_label(&self) -> LabelId {
        self.label
    }

    #[inline]
    pub fn get_type(&self) -> Type {
        self.data_type
    }

    #[inline]
    pub fn get_prop_id(&self, name: &str) -> Option<PropId> {
        self.prop_name_mapping.get(name).map(|x| *x)
    }

    #[inline]
    pub fn get_prop_type(&self, prop_id: PropId) -> Option<&DataType> {
        self.props.get(&prop_id).map(|x| x.get_data_type())
    }

    #[inline]
    pub fn get_comment(&self) -> &String {
        &self.comment
    }

    #[inline]
    pub fn is_dimension(&self) -> bool {
        self.is_dimension_type
    }

    #[inline]
    pub fn get_version(&self) -> u32 {
        self.version
    }

    #[inline]
    pub fn get_props(&self) -> impl Iterator<Item=(&PropId, &PropDef)> {
        self.props.iter()
    }

    #[inline]
    pub fn get_relations(&self) -> &HashSet<Relation> {
        &self.relations
    }

    pub fn get_type_option(&self) -> &TypeOption {
        &self.type_option
    }

    pub fn new() -> Self {
        Default::default()
    }

    pub fn to_proto(&self) -> protos::schema::TypeDefProto {
        let mut proto = protos::schema::TypeDefProto::new();
        proto.set_version(self.version as i32);
        proto.set_id(self.label as i32);
        proto.set_label(self.name.clone());
        proto.set_comment(self.comment.clone());
        proto.set_isDimensionType(self.is_dimension_type);
        for r in self.relations.iter() {
            proto.relationShip.push(r.to_proto());
        }
        for (gid, p) in self.props.iter() {
            proto.gidToPid.insert(*gid as i32, p.get_prop_id() as i32);
            proto.property.push(p.to_proto());
        }
        proto.set_field_type(self.data_type.to_proto());
        proto.set_option(self.type_option.to_proto());
        proto
    }
}

#[derive(Clone, Debug, Default)]
pub struct TypeOption {
    storage_engine: StorageEngine,
}

impl<'a> From<&'a protos::schema::TypeOptionProto> for TypeOption {
    fn from(proto: &'a protos::schema::TypeOptionProto) -> Self {
        TypeOption {
            storage_engine: StorageEngine::from(&proto.get_storageEngine()),
        }
    }
}

impl TypeOption {
    pub fn new(storage_engine: StorageEngine) -> Self {
        TypeOption {
            storage_engine,
        }
    }

    fn to_proto(&self) -> protos::schema::TypeOptionProto {
        let mut proto = protos::schema::TypeOptionProto::new();
        proto.set_storageEngine(self.storage_engine.to_proto());
        proto
    }

    pub fn get_storage_engine(&self) -> StorageEngine {
        self.storage_engine
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
pub enum StorageEngine {
    Memory,
    Rocksdb,
    Alibtree,
}

impl<'a> From<&'a protos::schema::StorageEngine> for StorageEngine {
    fn from(proto: &'a protos::schema::StorageEngine) -> Self {
        match *proto {
            protos::schema::StorageEngine::MEMORY => StorageEngine::Memory,
            protos::schema::StorageEngine::ROCKSDB => StorageEngine::Rocksdb,
            protos::schema::StorageEngine::ALIBTREE => StorageEngine::Alibtree,
        }
    }
}

impl StorageEngine {
    fn to_proto(&self) -> protos::schema::StorageEngine {
        match *self {
            StorageEngine::Memory => protos::schema::StorageEngine::MEMORY,
            StorageEngine::Rocksdb => protos::schema::StorageEngine::ROCKSDB,
            StorageEngine::Alibtree => protos::schema::StorageEngine::ALIBTREE,
        }
    }
}

impl Default for StorageEngine {
    fn default() -> Self {
        StorageEngine::Memory
    }
}

#[derive(Default)]
pub struct TypeDefBuilder {
    inner: UnsafeCell<TypeDef>,
}

#[allow(unused_mut)]
impl TypeDefBuilder {
    pub fn new() -> Self {
        Default::default()
    }

    #[inline]
    pub fn name(mut self, name: &str) -> Self {
        self.get_inner().name = name.to_owned();
        self
    }

    #[inline]
    pub fn label(mut self, label: LabelId) -> Self {
        self.get_inner().label = label;
        self
    }

    pub fn data_type(mut self, data_type: Type) -> Self {
        self.get_inner().data_type = data_type;
        self
    }

    #[inline]
    pub fn dimension_type(mut self, is_dimension_type: bool) -> Self {
        self.get_inner().is_dimension_type = is_dimension_type;
        self
    }

    #[inline]
    pub fn comment(mut self, comment: &str) -> Self {
        self.get_inner().comment = comment.to_owned();
        self
    }

    #[inline]
    pub fn version(mut self, version: u32) -> Self {
        self.get_inner().version = version;
        self
    }

    #[inline]
    pub fn type_option(mut self, type_option: TypeOption) -> Self {
        self.get_inner().type_option = type_option;
        self
    }

    #[inline]
    pub fn add_prop(mut self, global_prop_id: PropId, prop_def: PropDef) -> Self {
        {
            let inner = self.get_inner();
            inner.prop_name_mapping.insert(prop_def.get_name().to_owned(), global_prop_id);
            inner.props.insert(global_prop_id, prop_def);
        }
        self
    }

    pub fn add_properties(mut self, props: Vec<(&str, DataType)>) -> Self {
        {
            let inner = self.get_inner();
            let mut prop_id = 0;
            for (p, t) in props {
                prop_id += 1;
                inner.prop_name_mapping.insert(p.to_owned(), prop_id);
                inner.props.insert(prop_id, PropDefBuilder::new()
                    .name(p).prop_id(prop_id).data_type(t).build());
            }
        }
        self
    }

    #[inline]
    pub fn add_relation(mut self, relation: Relation) -> Self {
        {
            let inner = self.get_inner();
            inner.relations.insert(relation);
        }
        self
    }

    #[inline]
    pub fn build(self) -> TypeDef {
        self.inner.into_inner()
    }

    #[inline]
    fn get_inner(&self) -> &mut TypeDef {
        unsafe {
            &mut *self.inner.get()
        }
    }
}

impl<'a> From<&'a protos::schema::TypeDefProto> for TypeDef {
    fn from(proto: &'a protos::schema::TypeDefProto) -> Self {
        let mut builder = TypeDefBuilder::new()
            .name(proto.get_label())
            .label(proto.id as u32)
            .data_type(Type::from(&proto.get_field_type()))
            .dimension_type(proto.isDimensionType)
            .comment(proto.get_comment())
            .version(proto.version as u32)
            .type_option(TypeOption::from(proto.get_option()));

        for r in proto.get_relationShip() {
            builder = builder.add_relation(Relation::from(r));
        }

        let mut pid_to_gid = HashMap::new();
        for (k, v) in proto.get_gidToPid() {
            pid_to_gid.insert(*v, *k);
        }

        for p in proto.get_property() {
            let prop_def = PropDef::from(p);
            let global_prop_id = pid_to_gid.get(&(prop_def.get_prop_id() as i32)).expect("pid to gid not found");
            builder = builder.add_prop(*global_prop_id as u32, prop_def);
        }

        builder.build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::test_util::*;

    #[test]
    fn test_type_from_proto() {
        let mut props = Vec::new();
        props.push(create_prop_def(1, "1", DataType::String, "11"));
        props.push(create_prop_def(2, "2", DataType::Int, "22"));
        props.push(create_prop_def(3, "3", DataType::Double, "33"));
        let relation = Relation::new(1, 2, 3);
        let proto = create_edge_type_def_proto(relation.clone(), "ee", 1, props.clone(), false, "ee");
        let type_def = TypeDef::from(&proto);
        check_edge_type_def(type_def, relation);

        let proto = create_vertex_type_def_proto(1, "vv", 1, props.clone(), true, "vv");
        let type_def = TypeDef::from(&proto);
        check_vertex_type_def(type_def);
    }

    #[test]
    fn test_type_def_build() {
        let relation = Relation::new(1, 2, 3);
        let mut props = Vec::new();
        props.push(create_prop_def(1, "1", DataType::String, "11"));
        props.push(create_prop_def(2, "2", DataType::Int, "22"));
        props.push(create_prop_def(3, "3", DataType::Double, "33"));
        let mut builder = TypeDefBuilder::new()
            .name("ee")
            .data_type(Type::Edge)
            .label(1)
            .comment("ee")
            .version(1)
            .dimension_type(false)
            .add_relation(relation.clone());
        for p in props.clone() {
            builder = builder.add_prop(p.get_prop_id(), p);
        }
        let type_def = builder.build();
        check_edge_type_def(type_def, relation);

        let mut builder = TypeDefBuilder::new()
            .name("vv")
            .data_type(Type::Vertex)
            .label(1)
            .comment("vv")
            .version(1)
            .dimension_type(true);
        for p in props.clone() {
            builder = builder.add_prop(p.get_prop_id(), p);
        }
        let type_def = builder.build();
        check_vertex_type_def(type_def);
    }

    #[test]
    fn test_type_def_to_proto() {
        let mut props = Vec::new();
        props.push(create_prop_def(1, "1", DataType::String, "11"));
        props.push(create_prop_def(2, "2", DataType::Int, "22"));
        props.push(create_prop_def(3, "3", DataType::Double, "33"));
        let relation = Relation::new(1, 2, 3);
        let type_def = create_edge_type_def(relation.clone(), "ee", 1, props.clone(), false, "ee");
        let type_def = TypeDef::from(&type_def.to_proto());
        check_edge_type_def(type_def, relation);

        let type_def = create_vertex_type_def(1, "vv", 1, props.clone(), true, "vv");
        let type_def = TypeDef::from(&type_def.to_proto());
        check_vertex_type_def(type_def);
    }

    fn check_edge_type_def(type_def: TypeDef, relation: Relation) {
        assert_eq!(type_def.is_dimension(), false);
        assert_eq!(type_def.get_comment(), "ee");
        assert_eq!(type_def.get_name(), "ee");
        assert_eq!(type_def.get_version(), 1);
        assert_eq!(type_def.get_prop_id("1").unwrap(), 1);
        assert_eq!(type_def.get_prop_id("2").unwrap(), 2);
        assert_eq!(type_def.get_prop_id("3").unwrap(), 3);
        assert_eq!(type_def.get_label(), 1);
        assert_eq!(type_def.get_prop_type(1).unwrap(), &DataType::String);
        assert_eq!(type_def.get_prop_type(2).unwrap(), &DataType::Int);
        assert_eq!(type_def.get_prop_type(3).unwrap(), &DataType::Double);
        assert_eq!(type_def.get_type(), Type::Edge);
        assert_eq!(*type_def.get_relations().iter().next().unwrap(), relation);
    }

    fn check_vertex_type_def(type_def: TypeDef) {
        assert_eq!(type_def.is_dimension(), true);
        assert_eq!(type_def.get_comment(), "vv");
        assert_eq!(type_def.get_name(), "vv");
        assert_eq!(type_def.get_version(), 1);
        assert_eq!(type_def.get_prop_id("1").unwrap(), 1);
        assert_eq!(type_def.get_prop_id("2").unwrap(), 2);
        assert_eq!(type_def.get_prop_id("3").unwrap(), 3);
        assert_eq!(type_def.get_label(), 1);
        assert_eq!(type_def.get_prop_type(1).unwrap(), &DataType::String);
        assert_eq!(type_def.get_prop_type(2).unwrap(), &DataType::Int);
        assert_eq!(type_def.get_prop_type(3).unwrap(), &DataType::Double);
        assert_eq!(type_def.get_type(), Type::Vertex);
        assert!(type_def.get_relations().is_empty());
    }
}

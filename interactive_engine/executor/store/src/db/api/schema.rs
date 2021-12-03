#![allow(dead_code)]
use std::collections::{HashMap, HashSet};

use crate::db::common::bytes::util::parse_pb;
use crate::db::api::property::Value;
use super::{GraphResult, PropertyId};
use super::property::ValueType;
use super::error::*;
use crate::db::proto::model::{TypeDefPb, PropertyDefPb, GraphDefPb, TypeEnumPb, VertexTableIdEntry, EdgeTableIdEntry};
use protobuf::{ProtobufEnum, Message};
use crate::db::api::{LabelId, EdgeKind};

#[derive(Default, Clone)]
pub struct GraphDef {
    version: i64,
    pub label_to_types: HashMap<LabelId, TypeDef>,
    edge_kinds: HashSet<EdgeKind>,
    pub property_name_to_id: HashMap<String, i32>,
    label_idx: i32,
    property_idx: i32,
    vertex_table_ids: HashMap<LabelId, i64>,
    edge_table_ids: HashMap<EdgeKind, i64>,
    table_idx: i64,
}

impl GraphDef {
    pub fn new(version: i64,
               label_to_types: HashMap<LabelId, TypeDef>,
               edge_kinds: HashSet<EdgeKind>,
               property_name_to_id: HashMap<String, i32>,
               label_idx: i32,
               property_idx: i32,
               vertex_table_ids: HashMap<LabelId, i64>,
               edge_table_ids: HashMap<EdgeKind, i64>,
               table_idx: i64
    ) -> Self {
        GraphDef {
            version,
            label_to_types,
            edge_kinds,
            property_name_to_id,
            label_idx,
            property_idx,
            vertex_table_ids,
            edge_table_ids,
            table_idx,
        }
    }

    pub fn add_type(&mut self, label: LabelId, type_def: TypeDef) -> GraphResult<()> {
        if self.label_to_types.contains_key(&label) {
            let msg = format!("labelId {}", label);
            return Err(GraphError::new(GraphErrorCode::TypeAlreadyExists, msg));
        }
        for property in type_def.get_prop_defs() {
            if property.id > self.property_idx {
                self.property_idx = property.id
            }
            self.property_name_to_id.insert(property.name.clone(), property.id);
        }
        self.label_to_types.insert(label, type_def);
        Ok(())
    }

    pub fn put_vertex_table_id(&mut self, label: LabelId, table_id: i64) {
        self.vertex_table_ids.insert(label, table_id);
    }

    pub fn remove_type(&mut self, label_id: &LabelId) {
        if let Some(_typedef) = self.label_to_types.remove(label_id) {
            let mut current_property_names = HashSet::new();
            for t in self.label_to_types.values() {
                for p in t.get_prop_defs() {
                    current_property_names.insert(&p.name);
                }
            }
            self.property_name_to_id.retain(|k, _v| {
                current_property_names.contains(k)
            });
            self.vertex_table_ids.remove(label_id);
        }
    }

    pub fn add_edge_kind(&mut self, edge_kind: EdgeKind) {
        self.edge_kinds.insert(edge_kind);
    }

    pub fn put_edge_table_id(&mut self, edge_kind: EdgeKind, table_id: i64) {
        self.edge_table_ids.insert(edge_kind, table_id);
    }

    pub fn remove_edge_kind(&mut self, edge_kind: &EdgeKind) {
        self.edge_kinds.remove(edge_kind);
        self.edge_table_ids.remove(edge_kind);
    }

    pub fn increase_label_idx(&mut self) {
        self.label_idx = self.label_idx + 1;
    }

    pub fn get_label_idx(&self) -> i32 {
        self.label_idx
    }

    pub fn set_label_idx(&mut self, label_idx: i32) {
        self.label_idx = label_idx;
    }

    pub fn set_table_idx(&mut self, table_idx: i64) {
        self.table_idx = table_idx;
    }

    pub fn increase_version(&mut self) {
        self.version = self.version + 1;
    }

    pub fn get_version(&self) -> i64 {
        self.version
    }

    pub fn to_proto(&self) -> GraphResult<GraphDefPb> {
        let mut pb = GraphDefPb::new();
        pb.set_version(self.version);
        pb.set_labelIdx(self.label_idx);
        pb.set_propertyIdx(self.property_idx);
        for type_def in self.label_to_types.values() {
            pb.mut_typeDefs().push(type_def.to_proto()?);
        }
        for edge_kind in &self.edge_kinds {
            pb.mut_edgeKinds().push(edge_kind.to_proto());
        }
        for (name, id) in &self.property_name_to_id {
            pb.mut_propertyNameToId().insert(name.clone(), *id);
        }
        for (label_id, table_id) in &self.vertex_table_ids {
            let mut vertex_table_id_entry = VertexTableIdEntry::new();
            vertex_table_id_entry.mut_labelId().set_id(*label_id);
            vertex_table_id_entry.set_tableId(*table_id);
            pb.mut_vertexTableIds().push(vertex_table_id_entry);
        }
        for (edge_kind, table_id) in &self.edge_table_ids {
            let mut edge_table_id_entry = EdgeTableIdEntry::new();
            edge_table_id_entry.set_edgeKind(edge_kind.to_proto());
            edge_table_id_entry.set_tableId(*table_id);
            pb.mut_edgeTableIds().push(edge_table_id_entry);
        }
        pb.set_tableIdx(self.table_idx);
        Ok(pb)
    }
}

#[derive(Clone, Default, Debug, PartialEq)]
pub struct TypeDef {
    version: i32,
    label: String,
    label_id: LabelId,
    properties: HashMap<PropertyId, PropDef>,
    type_enum: TypeEnumPb,
}

impl TypeDef {
    pub fn get_version(&self) -> i32 {
        self.version
    }

    pub fn get_prop_defs(&self) -> impl Iterator<Item=&PropDef> {
        self.properties.values()
    }

    pub fn get_prop_def(&self, prop_id: PropertyId) -> Option<&PropDef> {
        self.properties.get(&prop_id)
    }

    pub fn get_label(&self) -> String {
        self.label.clone()
    }

    pub fn get_label_id(&self) -> LabelId {
        return self.label_id;
    }

    pub fn from_proto(proto: &TypeDefPb) -> GraphResult<Self> {
        let version_id = proto.get_versionId();
        let label = proto.get_label();
        let label_id = proto.get_labelId().get_id();
        let mut properties = HashMap::new();
        for propertydef_pb in proto.get_props() {
            let property_def = PropDef::from_proto(propertydef_pb)?;
            properties.insert(property_def.id, property_def);
        }
        let type_enum = proto.get_typeEnum();
        Ok(Self::new(version_id, label.to_string(), label_id, properties, type_enum))
    }

    pub fn to_proto(&self) -> GraphResult<TypeDefPb> {
        let mut typedef_pb = TypeDefPb::new();
        typedef_pb.set_versionId(self.version);
        typedef_pb.set_label(self.label.clone());
        typedef_pb.mut_labelId().set_id(self.label_id);
        for property_def in self.properties.values() {
            typedef_pb.mut_props().push(property_def.to_proto()?);
        }
        typedef_pb.set_typeEnum(self.type_enum);
        Ok(typedef_pb)
    }

    pub fn from_bytes(bytes: &[u8]) -> GraphResult<Self> {
        let typedef_pb = parse_pb::<TypeDefPb>(bytes)?;
        TypeDef::from_proto(&typedef_pb)
    }

    pub fn to_bytes(&self) -> GraphResult<Vec<u8>> {
        let typedef_pb = self.to_proto()?;
        match typedef_pb.write_to_bytes() {
            Ok(b) => Ok(b),
            Err(e) => {
                let msg = format!("{:?}", e);
                Err(gen_graph_err!(GraphErrorCode::InvalidData, msg, to_bytes))
            },
        }
    }

    fn new(version: i32, label: String, label_id: LabelId, properties: HashMap<PropertyId, PropDef>, type_enum: TypeEnumPb) -> Self {
        TypeDef {
            version,
            label,
            label_id,
            properties,
            type_enum,
        }
    }

    #[cfg(test)]
    pub fn new_test() -> Self {
        let mut builder = TypeDefBuilder::new();
        let types = vec![ValueType::Bool, ValueType::Char, ValueType::Short,
                         ValueType::Int, ValueType::Long, ValueType::Float, ValueType::Double,
                         ValueType::String, ValueType::Bytes, ValueType::IntList, ValueType::LongList,
                         ValueType::FloatList, ValueType::DoubleList, ValueType::StringList];
        for i in 0..types.len() {
            let id = i as PropertyId + 112;
            let inner_id = id * 2 + 123;
            let name = format!("type-{}", i);
            builder.add_property(id, inner_id, name, types[i], None, false, "comment".to_string());
        }
        builder.build()
    }
}

pub struct TypeDefBuilder {
    type_def: TypeDef,
}

impl TypeDefBuilder {
    pub fn new() -> Self {
        TypeDefBuilder {
            type_def: TypeDef::default(),
        }
    }

    pub fn add_property(&mut self, id: PropertyId, inner_id: PropertyId, name: String, r#type: ValueType, default_value: Option<Value>, pk: bool, comment: String) -> &mut Self {
        self.type_def.properties.insert(id, PropDef::new(id, inner_id, name, r#type, default_value, pk, comment));
        self
    }

    pub fn version(&mut self, version: i32) -> &mut Self {
        self.type_def.version = version;
        self
    }

    pub fn set_label_id(&mut self, label_id: LabelId) -> &mut Self {
        self.type_def.label_id = label_id;
        self
    }

    pub fn build(self) -> TypeDef {
        self.type_def
    }
}

/// `id` corresponds to property name, and it's global. Some types can have properties with same name
/// but different value types. For example, **Post** have an "ID" property of Long type and **Person**
/// have an "ID" property of String type. For compiler, all "ID" properties have a same property id.
/// So when query g.V().has('ID', eq("12345678")), compiler will tell runtime call v.get_property(123) and
/// check whether the property is equal to "12345678". But in each type this property has a different inner
/// id and it helps to distinguish the property's value type. In addition, think about this case: at
/// first a property's type is String, then user drop this property and later on he add a same name
/// property but with type int. These two property will have a same property id because their name is
/// same but they will have different inner ids to distinguish that they are not the same property.
#[derive(Clone, Debug, PartialEq)]
pub struct PropDef {
    pub id: PropertyId,
    pub inner_id: PropertyId,
    pub name: String,
    pub r#type: ValueType,
    pub default_value: Option<Value>,
    pub pk: bool,
    pub comment: String,
}

impl PropDef {
    fn new(id: PropertyId, inner_id: PropertyId, name: String, r#type: ValueType, default_value: Option<Value>, pk: bool, comment: String) -> Self {
        if let Some(ref v) = default_value {
            if !v.is_type(r#type) {
                panic!("{:?} is not {:?}", v, r#type);
            }
        }
        PropDef {
            id,
            inner_id,
            name,
            r#type,
            default_value,
            pk,
            comment,
        }
    }

    fn from_proto(proto: &PropertyDefPb) -> GraphResult<Self> {
        let id = proto.get_id();
        let inner_id = proto.get_innerId();
        let name = proto.get_name();
        let value_type = ValueType::from_i32(proto.get_dataType().value())?;
        let default_val = match Value::from_proto(proto.get_defaultValue()) {
            Ok(v) => Some(v),
            Err(_) => None,
        };
        let pk = proto.get_pk();
        let comment = proto.get_comment();
        Ok(Self::new(id, inner_id, name.to_string(), value_type, default_val, pk, comment.to_string()))
    }

    fn to_proto(&self) -> GraphResult<PropertyDefPb> {
        let mut pb = PropertyDefPb::new();
        pb.set_id(self.id);
        pb.set_innerId(self.inner_id);
        pb.set_name(self.name.clone());
        pb.set_dataType(self.r#type.to_proto()?);
        if let Some(ref default_value) = self.default_value {
            pb.set_defaultValue(default_value.to_proto()?);
        }
        pb.set_pk(self.pk);
        pb.set_comment(self.comment.clone());
        Ok(pb)
    }

    fn from_bytes(bytes: &[u8]) -> GraphResult<Self> {
        let propertydef_pb = parse_pb::<PropertyDefPb>(bytes)?;
        PropDef::from_proto(&propertydef_pb)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prop_def() {
        for t in ValueType::all_value_types() {
            let prop_id = 11223;
            let inner_id = 3345;
            let prop_def = PropDef::new(prop_id, inner_id, "prop".to_string(),t, None, false, "comment".to_string());
            let bytes = prop_def.to_proto().unwrap().write_to_bytes().unwrap();
            let prop_def2 = PropDef::from_bytes(&bytes).unwrap();
            assert_eq!(prop_def, prop_def2);
        }
    }

    #[test]
    fn test_type_def() {
        let type_def = TypeDef::new_test();
        let bytes = type_def.to_bytes().unwrap();
        let type_def2 = TypeDef::from_bytes(&bytes).unwrap();
        assert_eq!(type_def, type_def2);
    }
}

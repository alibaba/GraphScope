//
//! Copyright 2020 Alibaba Group Holding Limited.
//! 
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//! 
//!     http://www.apache.org/licenses/LICENSE-2.0
//! 
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permissions and
//! limitations under the License.

pub mod primitive;
pub mod subgraph;

use maxgraph_store::api::prelude::{Vertex, Edge, Property};
use maxgraph_common::proto::message::*;
use maxgraph_common::util::hash::murmur_hash64;

use dataflow::message::primitive::Write;
use dataflow::message::primitive::Read;

use abomonation_derive::Abomonation;
use protobuf::{RepeatedField, parse_from_bytes};
use protobuf::Message;
use itertools::Itertools;
use rand::{thread_rng, Rng};
use maxgraph_store::api::graph_partition::GraphPartitionManager;
use std::collections::HashMap;
use std::sync::Arc;
use execution::build_empty_router;

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub enum RawMessageType {
    VERTEX,
    EDGE,
    PROP,
    ENTRY,
    PATHENTRY,
    VALUE,
    DFSCMD,
    LIST,
    MAP,
    ERROR,
}

impl RawMessageType {
    pub fn from_proto(message_type: MessageType) -> Self {
        match message_type {
            MessageType::MSG_VERTEX_TYPE => RawMessageType::VERTEX,
            MessageType::MSG_EDGE_TYPE => RawMessageType::EDGE,
            MessageType::MSG_PROP_TYPE => RawMessageType::PROP,
            MessageType::MSG_ENTRY_TYPE => RawMessageType::ENTRY,
            MessageType::MSG_PATH_ENTRY_TYPE => RawMessageType::PATHENTRY,
            MessageType::MSG_VALUE_TYPE => RawMessageType::VALUE,
            MessageType::MSG_DFS_CMD_TYPE => RawMessageType::DFSCMD,
            MessageType::MSG_LIST_TYPE => RawMessageType::LIST,
            MessageType::MSG_MAP_TYPE => RawMessageType::MAP,
            MessageType::MSG_ERROR_TYPE => RawMessageType::ERROR,
        }
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> MessageType where PR: Fn(&i64) -> i32 + 'static {
        match self {
            RawMessageType::VERTEX => MessageType::MSG_VERTEX_TYPE,
            RawMessageType::EDGE => MessageType::MSG_EDGE_TYPE,
            RawMessageType::PROP => MessageType::MSG_PROP_TYPE,
            RawMessageType::ENTRY => MessageType::MSG_ENTRY_TYPE,
            RawMessageType::PATHENTRY => MessageType::MSG_PATH_ENTRY_TYPE,
            RawMessageType::VALUE => MessageType::MSG_VALUE_TYPE,
            RawMessageType::DFSCMD => MessageType::MSG_DFS_CMD_TYPE,
            RawMessageType::LIST => MessageType::MSG_LIST_TYPE,
            RawMessageType::MAP => MessageType::MSG_MAP_TYPE,
            RawMessageType::ERROR => MessageType::MSG_ERROR_TYPE,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct RawMessage {
    message_type: RawMessageType,
    id: Option<IdMessage>,
    extra: Option<ExtraMessage>,
    bulk: i64,
}

impl RawMessage {
    pub fn new(message_type: RawMessageType) -> Self {
        RawMessage { message_type, id: None, extra: None, bulk: 1 }
    }

    pub fn from_vertex<V: Vertex>(v: V) -> Self {
        let mut message = RawMessage::new(RawMessageType::VERTEX);
        message.set_id(IdMessage::new(v.get_label_id() as i32, v.get_id()));

        message
    }

    pub fn from_vertex_id(label_id: i32, id: i64) -> Self {
        let mut message = RawMessage::new(RawMessageType::VERTEX);
        message.set_id(IdMessage::new(label_id, id));

        message
    }

    pub fn from_edge<E: Edge>(e: E, is_out: bool, fetch_prop_flag: bool) -> Self {
        let mut message = RawMessage::from_edge_id(e.get_edge_id(),
                                                   e.get_label_id() as i32,
                                                   is_out,
                                                   e.get_src_id(),
                                                   e.get_src_label_id() as i32,
                                                   e.get_dst_id(),
                                                   e.get_dst_label_id() as i32);
        if fetch_prop_flag {
            let mut prop_list = vec![];
            for (prop_id, prop_val) in e.get_properties() {
                prop_list.push(PropertyEntity {
                    prop_id: prop_id as i32,
                    value: ValuePayload::from_native(prop_val).0,
                });
            }
            message.set_proplist(prop_list);
        }

        message
    }

    pub fn from_edge_id(id: i64,
                        label_id: i32,
                        is_out: bool,
                        src_id: i64,
                        src_label_id: i32,
                        dst_id: i64,
                        dst_label_id: i32) -> Self {
        let mut message = RawMessage::new(RawMessageType::EDGE);
        message.set_id(IdMessage::new(label_id, id));
        message.set_edge(ExtraEdgeEntity {
            src_label: src_label_id,
            src_id,
            dst_label: dst_label_id,
            dst_id,
            is_out,
        });

        message
    }

    pub fn from_prop(prop: Property) -> Self {
        let (value, value_type) = ValuePayload::from_native(prop);
        RawMessage::from_value_type(value, value_type)
    }

    pub fn from_prop_type(prop: Property, propid: i32) -> Self {
        let mut message = RawMessage::from_value_type(ValuePayload::from_native(prop).0, RawMessageType::PROP);
        message.set_id(IdMessage {
            label_id: 1,
            id: propid as i64,
        });
        message
    }

    pub fn from_vertex_prop_type(prop: Property, propid: i32) -> Self {
        let mut message = RawMessage::from_value_type(ValuePayload::from_native(prop).0, RawMessageType::PROP);
        message.set_id(IdMessage {
            label_id: 0,
            id: propid as i64,
        });
        message
    }

    pub fn from_prop_entity(prop: PropertyEntity) -> Self {
        let mut message = RawMessage::new(RawMessageType::PROP);
        message.set_id(IdMessage {
            label_id: 1,
            id: prop.prop_id as i64,
        });
        message.set_value(prop.value);
        message
    }

    pub fn from_vertex_prop_entity(prop: PropertyEntity) -> Self {
        let mut message = RawMessage::new(RawMessageType::PROP);
        message.set_id(IdMessage {
            label_id: 0,
            id: prop.prop_id as i64,
        });
        message.set_value(prop.value);
        message
    }

    pub fn from_error(error: ErrorCode, str_val: String) -> Self {
        RawMessage::from_value_type(ValuePayload::String(format!("error[{:?}] message[{:?}]", error, str_val)), RawMessageType::ERROR)
    }

    pub fn from_value(value: ValuePayload) -> Self {
        let mut message = RawMessage::new(RawMessageType::VALUE);
        message.set_value(value);

        message
    }

    pub fn from_value_type(value: ValuePayload, message_type: RawMessageType) -> Self {
        let mut message = RawMessage::new(message_type);
        message.set_value(value);

        message
    }

    pub fn from_entry(key: RawMessage, value: RawMessage) -> Self {
        let entry = ExtraEntryEntity::new(key, value);
        RawMessage::from_value_type(ValuePayload::Entry(entry), RawMessageType::ENTRY)
    }

    pub fn from_entry_entity(entry: ExtraEntryEntity) -> Self {
        RawMessage::from_value_type(ValuePayload::Entry(entry), RawMessageType::ENTRY)
    }

    pub fn from_path_entry(label_list: Option<Vec<i32>>, message: RawMessage) -> Self {
        RawMessage::from_value_type(ValuePayload::PathEntry(Box::new(ExtraPathEntity {
            label_list,
            message,
        })), RawMessageType::PATHENTRY)
    }

    pub fn from_raw_base_data_entity(raw_base: &mut RawBaseDataEntity) -> Self {
        let mut message = RawMessage::new(raw_base.get_message_type());
        if let Some(id) = raw_base.take_id() {
            message.set_id(id);
        }
        if let Some(edge) = raw_base.take_edge() {
            message.set_edge(edge);
        }
        if let Some(prop_value) = raw_base.take_prop_value() {
            message.set_prop_value(prop_value);
        }
        if let Some(extra_key) = raw_base.take_extend_key_payload() {
            message.set_extend_key_payload(extra_key);
        }
        if let Some(label_list) = raw_base.take_label_list() {
            message.set_extend_label_list(label_list);
        }
        message
    }

    pub fn from_proto(message_proto: &mut RawMessageProto) -> Self {
        let message_type = RawMessageType::from_proto(message_proto.get_message_type());
        let mut message = RawMessage::new(message_type);
        if message_proto.get_id() != 0 || message_proto.get_type_id() != 0 {
            message.set_id(IdMessage {
                label_id: message_proto.get_type_id(),
                id: message_proto.get_id(),
            });
        }
        message.set_bulk_value(message_proto.get_bulk());
        if message_proto.has_extra() {
            let extra_proto = message_proto.mut_extra();
            message.extra = Some(ExtraMessage {
                edge_entity: {
                    if extra_proto.has_extra_edge() {
                        let edge_proto = extra_proto.get_extra_edge();
                        Some(ExtraEdgeEntity {
                            src_label: edge_proto.get_src_type_id(),
                            src_id: edge_proto.get_src_id(),
                            dst_label: edge_proto.get_dst_type_id(),
                            dst_id: edge_proto.get_dst_id(),
                            is_out: edge_proto.get_is_out(),
                        })
                    } else {
                        None
                    }
                },
                prop_value: {
                    if extra_proto.has_extra_value_prop() {
                        let value_prop = extra_proto.get_extra_value_prop();
                        if value_prop.has_value_entity() {
                            Some(PropertyValueEntity::VALUE(ValuePayload::from_proto(value_prop.get_value_entity(), message_type)))
                        } else {
                            let mut prop_list = Vec::new();
                            for prop_proto in value_prop.get_prop_list() {
                                prop_list.push(PropertyEntity {
                                    prop_id: prop_proto.get_prop_id(),
                                    value: ValuePayload::from_proto(prop_proto.get_prop_value(), RawMessageType::VALUE),
                                });
                            }
                            Some(PropertyValueEntity::PROPLIST(prop_list))
                        }
                    } else {
                        None
                    }
                },
                extra_extend: {
                    let extra_key = extra_proto.get_extra_key().to_vec();
                    if extra_proto.has_extra_path_label() || !extra_key.is_empty() {
                        if !extra_proto.has_extra_path_label() {
                            Some(ExtraExtendEntity {
                                extra_key: {
                                    if extra_key.is_empty() {
                                        None
                                    } else {
                                        Some(extra_key)
                                    }
                                },
                                label_list: None,
                                path_list: None,
                            })
                        } else {
                            let path_label_entity = extra_proto.mut_extra_path_label();
                            Some(ExtraExtendEntity {
                                extra_key: {
                                    if extra_key.is_empty() {
                                        None
                                    } else {
                                        Some(extra_key)
                                    }
                                },
                                label_list: parse_label_list_proto(path_label_entity.mut_label_list()),
                                path_list: parse_path_list_proto(path_label_entity.mut_path_list()),
                            })
                        }
                    } else {
                        None
                    }
                },
            });
        }

        message
    }

    pub fn get_id(&self) -> i64 {
        if let Some(ref id) = self.id {
            id.id
        } else {
            0
        }
    }

    pub fn get_label_id(&self) -> i32 {
        if let Some(ref id) = self.id {
            id.label_id
        } else {
            0
        }
    }

    pub fn take_id(&mut self) -> Option<IdMessage> {
        return self.id.take();
    }

    pub fn get_bulk(&self) -> i64 {
        self.bulk
    }

    pub fn get_message_type(&self) -> RawMessageType {
        self.message_type
    }

    pub fn get_shuffle_id(&self) -> i64 {
        match self.message_type {
            RawMessageType::VERTEX | RawMessageType::EDGE => {
                if let Some(ref id) = self.id {
                    id.id
                } else {
                    0
                }
            }
            _ => {
                if let Some(value) = self.get_value() {
                    let empty_fn = build_empty_router();
                    murmur_hash64(value.to_proto(Some(&empty_fn)).get_payload())
                } else {
                    0
                }
            }
        }
    }

    pub fn set_extend_key_message(&mut self, message: RawMessage, uniq_flag: bool) {
        let extra_key = ExtraKeyEntity::new(message, uniq_flag);
        let empty_fn = build_empty_router();
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                extend.extra_key = Some(extra_key.to_proto(Some(&empty_fn)).write_to_bytes().unwrap());
            } else {
                extra.extra_extend = Some(ExtraExtendEntity {
                    extra_key: Some(extra_key.to_proto(Some(&empty_fn)).write_to_bytes().unwrap()),
                    label_list: None,
                    path_list: None,
                });
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(ExtraExtendEntity {
                    extra_key: Some(extra_key.to_proto(Some(&empty_fn)).write_to_bytes().unwrap()),
                    label_list: None,
                    path_list: None,
                }),
            });
        }
    }

    pub fn get_extend_key_payload(&self) -> Option<&Vec<u8>> {
        if let Some(ref extra) = self.extra {
            if let Some(ref extend) = extra.extra_extend {
                return extend.extra_key.as_ref();
            }
        }
        return None;
    }

    pub fn take_extend_key_payload(&mut self) -> Option<Vec<u8>> {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                return extend.extra_key.take();
            }
        }
        return None;
    }

    pub fn set_extend_key_payload(&mut self, key: Vec<u8>) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                extend.extra_key = Some(key);
            } else {
                extra.extra_extend = Some(ExtraExtendEntity {
                    extra_key: Some(key),
                    label_list: None,
                    path_list: None,
                });
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(ExtraExtendEntity {
                    extra_key: Some(key),
                    label_list: None,
                    path_list: None,
                }),
            });
        }
    }

    pub fn set_id(&mut self, id: IdMessage) {
        self.id = Some(id);
    }

    pub fn set_value(&mut self, value: ValuePayload) {
        if let Some(ref mut extra) = self.extra {
            extra.prop_value = Some(PropertyValueEntity::VALUE(value));
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: Some(PropertyValueEntity::VALUE(value)),
                extra_extend: None,
            });
        }
    }

    pub fn set_edge(&mut self, edge: ExtraEdgeEntity) {
        if let Some(ref mut extra) = self.extra {
            extra.edge_entity = Some(edge);
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: Some(edge),
                prop_value: None,
                extra_extend: None,
            });
        }
    }

    pub fn set_prop_value(&mut self, prop: PropertyValueEntity) {
        if let Some(ref mut extra) = self.extra {
            extra.prop_value = Some(prop);
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: Some(prop),
                extra_extend: None,
            });
        }
    }

    pub fn set_proplist(&mut self, proplist: Vec<PropertyEntity>) {
        if let Some(ref mut extra) = self.extra {
            extra.prop_value = Some(PropertyValueEntity::PROPLIST(proplist));
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: Some(PropertyValueEntity::PROPLIST(proplist)),
                extra_extend: None,
            });
        }
    }

    pub fn set_extend_label_list(&mut self, label_list: Vec<ExtraLabelEntity>) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                extend.label_list = Some(label_list);
            } else {
                extra.extra_extend = Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: Some(label_list),
                    path_list: None,
                });
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: Some(label_list),
                    path_list: None,
                }),
            });
        }
    }

    pub fn set_extend_path_list(&mut self, path_list: Vec<Vec<ExtraPathEntity>>) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                extend.path_list = Some(path_list);
            } else {
                extra.extra_extend = Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: None,
                    path_list: Some(path_list),
                });
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: None,
                    path_list: Some(path_list),
                }),
            });
        }
    }

    pub fn set_bulk_value(&mut self, bulk_value: i64) {
        self.bulk = bulk_value;
    }

    pub fn add_path_entity(&mut self, value: RawMessage, label_list: Option<Vec<i32>>) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                if let Some(ref mut path_list) = extend.path_list {
                    add_path_list_bulk(path_list,
                                       ExtraPathEntity {
                                           label_list,
                                           message: value,
                                       },
                                       self.bulk);
                } else {
                    extend.path_list = Some(build_path_list_bulk(
                        ExtraPathEntity {
                            label_list,
                            message: value,
                        },
                        self.bulk));
                }
            } else {
                extra.extra_extend = Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: None,
                    path_list: Some(build_path_list_bulk(
                        ExtraPathEntity {
                            label_list,
                            message: value,
                        },
                        self.bulk)),
                });
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: None,
                    path_list: Some(build_path_list_bulk(
                        ExtraPathEntity {
                            label_list,
                            message: value,
                        },
                        self.bulk)),
                }),
            });
        }
    }

    pub fn add_label_entity(&mut self, value: RawMessage, label_id: i32) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                if let Some(ref mut label_list) = extend.label_list {
                    label_list.push(
                        ExtraLabelEntity {
                            label_id,
                            message: value,
                        });
                } else {
                    extend.label_list = Some(vec![
                        ExtraLabelEntity {
                            label_id,
                            message: value,
                        }]);
                }
            } else {
                extra.extra_extend = Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: Some(vec![
                        ExtraLabelEntity {
                            label_id,
                            message: value,
                        }]),
                    path_list: None,
                });
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(ExtraExtendEntity {
                    extra_key: None,
                    label_list: Some(vec![
                        ExtraLabelEntity {
                            label_id,
                            message: value,
                        }]),
                    path_list: None,
                }),
            });
        }
    }

    pub fn delete_label_by_id(&mut self, label_id: i32) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                if let Some(ref mut label_list) = extend.label_list {
                    label_list.retain(|x| x.label_id != label_id);
                }
            }
        }
    }

    pub fn get_prop_list(&self) -> Option<&Vec<PropertyEntity>> {
        if let Some(ref extra) = self.extra {
            if let Some(ref prop_value) = extra.prop_value {
                match prop_value {
                    PropertyValueEntity::PROPLIST(proplist) => {
                        return Some(proplist);
                    }
                    _ => {}
                }
            }
        }
        return None;
    }

    pub fn get_property(&self, prop_id: i32) -> Option<&PropertyEntity> {
        if let Some(ref extra) = self.extra {
            if let Some(ref prop_value) = extra.prop_value {
                match prop_value {
                    PropertyValueEntity::PROPLIST(proplist) => {
                        for prop in proplist.iter() {
                            if prop.prop_id == prop_id {
                                return Some(prop);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        return None;
    }

    pub fn get_map_value(&self) -> Option<&Vec<ExtraEntryEntity>> {
        if let Some(value) = self.get_value() {
            if let Ok(map) = value.get_map() {
                return Some(map);
            }
        }

        return None;
    }

    pub fn take_map_value(&mut self) -> Option<Vec<ExtraEntryEntity>> {
        if let Some(value) = self.take_value() {
            if let Ok(map) = value.take_map() {
                return Some(map);
            }
        }

        return None;
    }

    pub fn get_label_entity_list(&self) -> Option<&Vec<ExtraLabelEntity>> {
        if let Some(ref extra) = self.extra {
            if let Some(ref extend) = extra.extra_extend {
                return extend.label_list.as_ref();
            }
        }
        return None;
    }

    pub fn take_label_entity_list(&mut self) -> Option<Vec<ExtraLabelEntity>> {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                return extend.label_list.take();
            }
        }
        return None;
    }

    pub fn get_path_list(&self) -> Option<&Vec<Vec<ExtraPathEntity>>> {
        if let Some(ref extra) = self.extra {
            if let Some(ref extend) = extra.extra_extend {
                return extend.path_list.as_ref();
            }
        }
        return None;
    }

    pub fn take_path_list(&mut self) -> Option<Vec<Vec<ExtraPathEntity>>> {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                return extend.path_list.take();
            }
        }
        return None;
    }

    pub fn take_prop_value(&mut self) -> Option<PropertyValueEntity> {
        if let Some(ref mut extra) = self.extra {
            if let Some(prop_value) = extra.prop_value.take() {
                return Some(prop_value);
            }
        }
        return None;
    }

    pub fn take_prop_entity(&mut self, prop_id: i32) -> Option<PropertyEntity> {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut prop_value) = extra.prop_value {
                match prop_value {
                    PropertyValueEntity::PROPLIST(proplist) => {
                        for i in 0..proplist.len() {
                            if let Some(prop_entity) = proplist.get(i) {
                                if prop_entity.prop_id == prop_id {
                                    return Some(proplist.remove(i));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        return None;
    }

    pub fn take_prop_entity_list(&mut self) -> Option<Vec<PropertyEntity>> {
        if let Some(ref mut extra) = self.extra {
            if let Some(prop_value) = extra.prop_value.take() {
                match prop_value {
                    PropertyValueEntity::PROPLIST(proplist) => {
                        return Some(proplist);
                    }
                    _ => {}
                }
            }
        }

        return None;
    }

    pub fn get_label_entity_list_by_id(&self, label_id: i32) -> Option<Vec<&ExtraLabelEntity>> {
        let mut label_list = Vec::new();
        if let Some(label_entity_list) = self.get_label_entity_list() {
            for label in label_entity_list.iter() {
                if label.label_id == label_id {
                    label_list.push(label);
                }
            }
        }

        return Some(label_list);
    }

    pub fn get_label_entity_by_id(&self, label_id: i32) -> Option<&ExtraLabelEntity> {
        if let Some(mut label_list) = self.get_label_entity_list_by_id(label_id) {
            if !label_list.is_empty() {
                return label_list.pop();
            }
        }

        return None;
    }

    pub fn take_label_entity_by_id(&mut self, label_id: i32) -> Option<ExtraLabelEntity> {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                if let Some(ref mut label_list) = extend.label_list {
                    for i in 0..label_list.len() {
                        let curr_label_id = {
                            label_list.get(i).unwrap().label_id
                        };
                        if curr_label_id == label_id {
                            return Some(label_list.remove(i));
                        }
                    }
                }
            }
        }

        return None;
    }

    pub fn get_label_id_list(&self) -> Option<Vec<i32>> {
        if let Some(label_list) = self.get_label_entity_list() {
            let mut label_id_list = vec![];
            for label in label_list.iter() {
                let label_id = label.label_id;
                if !label_id_list.contains(&label_id) {
                    label_id_list.push(label_id);
                }
            }
            if !label_id_list.is_empty() {
                return Some(label_id_list);
            }
        }
        return None;
    }

    pub fn get_value(&self) -> Option<&ValuePayload> {
        if let Some(ref extra) = self.extra {
            if let Some(ref prop_value) = extra.prop_value {
                match prop_value {
                    PropertyValueEntity::VALUE(val) => {
                        return Some(val);
                    }
                    _ => {}
                }
            }
        }
        return None;
    }

    pub fn get_entry_value(&self) -> Option<&ExtraEntryEntity> {
        if let Some(value) = self.get_value() {
            if let Ok(entry) = value.get_entry() {
                return Some(entry);
            }
        }

        return None;
    }

    pub fn get_list_value(&self) -> Option<&Vec<RawMessage>> {
        if let Some(value) = self.get_value() {
            if let Ok(list) = value.get_list() {
                return Some(list);
            }
        }
        return None;
    }

    pub fn take_value(&mut self) -> Option<ValuePayload> {
        if let Some(ref mut extra) = self.extra {
            if let Some(prop_value) = extra.prop_value.take() {
                match prop_value {
                    PropertyValueEntity::VALUE(val) => {
                        return Some(val);
                    }
                    _ => {}
                }
            }
        }
        return None;
    }

    pub fn get_edge(&self) -> Option<&ExtraEdgeEntity> {
        if let Some(ref extra) = self.extra {
            if let Some(ref edge) = extra.edge_entity {
                return Some(edge);
            }
        }
        return None;
    }

    pub fn take_edge(&mut self) -> Option<ExtraEdgeEntity> {
        if let Some(ref mut extra) = self.extra {
            return extra.edge_entity.take();
        }
        return None;
    }

    pub fn get_extend_entity(&self) -> Option<&ExtraExtendEntity> {
        if let Some(ref extra) = self.extra {
            return extra.extra_extend.as_ref();
        }

        return None;
    }

    pub fn take_extend_entity(&mut self) -> Option<ExtraExtendEntity> {
        if let Some(ref mut extra) = self.extra {
            return extra.extra_extend.take();
        }

        return None;
    }

    pub fn set_extend_entity(&mut self, extend: ExtraExtendEntity) {
        if let Some(ref mut extra) = self.extra {
            extra.extra_extend = Some(extend);
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(extend),
            });
        }
    }

    pub fn merge_extend_entity(&mut self, mut extend: ExtraExtendEntity) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut curr_extend) = extra.extra_extend {
                curr_extend.extra_key = extend.extra_key;
                if let Some(ref mut labellist) = curr_extend.label_list {
                    if let Some(ref mut extend_labellist) = extend.label_list {
                        labellist.append(extend_labellist);
                    }
                } else {
                    curr_extend.label_list = extend.label_list;
                }
                if let Some(extend_path) = extend.path_list {
                    curr_extend.path_list = Some(extend_path);
                }
            } else {
                extra.extra_extend = Some(extend);
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: None,
                extra_extend: Some(extend),
            });
        }
    }

    pub fn add_native_property(&mut self, propid: i32, prop: Property) {
        let prop_entity = PropertyEntity {
            prop_id: propid,
            value: ValuePayload::from_native(prop).0,
        };
        self.add_property_entity(prop_entity);
    }

    pub fn add_property_value(&mut self, propid: i32, value: ValuePayload) {
        let prop_entity = PropertyEntity {
            prop_id: propid,
            value,
        };
        self.add_property_entity(prop_entity);
    }

    fn add_property_entity(&mut self, prop_entity: PropertyEntity) {
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut prop_value) = extra.prop_value {
                match prop_value {
                    PropertyValueEntity::PROPLIST(proplist) => {
                        proplist.push(prop_entity);
                    }
                    _ => {
                        error!("cant add property for value");
                    }
                }
            } else {
                extra.prop_value = Some(PropertyValueEntity::PROPLIST(vec![prop_entity]));
            }
        } else {
            self.extra = Some(ExtraMessage {
                edge_entity: None,
                prop_value: Some(PropertyValueEntity::PROPLIST(vec![prop_entity])),
                extra_extend: None,
            });
        }
    }

    pub fn clean_extend(&mut self) {
        if let Some(ref mut extra) = self.extra {
            extra.extra_extend = None;
        }
    }

    pub fn build_message_key(&self) -> RawMessage {
        let mut key = self.build_message_value();
        if let Some(labellist) = self.get_label_entity_list() {
            for label in labellist {
                key.add_label_entity(label.get_message().clone(), label.label_id);
            }
        }

        return key;
    }

    pub fn build_message_value(&self) -> RawMessage {
        let mut value = RawMessage::new(self.message_type);
        if let Some(ref id) = self.id {
            value.set_id(IdMessage {
                label_id: id.label_id,
                id: id.id,
            });
        }
        if let Some(proplist) = self.get_prop_list() {
            value.set_proplist(proplist.to_vec());
        }
        if let Some(val) = self.get_value() {
            value.set_value(val.clone());
        }
        if let Some(edge) = self.get_edge() {
            value.set_edge(ExtraEdgeEntity {
                src_label: edge.src_label,
                src_id: edge.src_id,
                dst_label: edge.dst_label,
                dst_id: edge.dst_id,
                is_out: edge.is_out,
            });
        }

        value
    }

    pub fn remove_path_bulk_list(&mut self, path_index_list: &Vec<usize>) {
        self.bulk = self.bulk - path_index_list.len() as i64;
        if let Some(ref mut extra) = self.extra {
            if let Some(ref mut extend) = extra.extra_extend {
                if let Some(ref mut path_list) = extend.path_list {
                    for index in 0..path_index_list.len() {
                        let curr_index = path_index_list.get(index).unwrap();
                        path_list.remove(*curr_index);
                    }
                }
            }
        }
    }

    pub fn update_with_bulk(&mut self, bulk_value: i64) {
        if self.bulk > bulk_value {
            self.bulk = bulk_value;
            if let Some(mut path_list) = self.take_path_list() {
                self.set_extend_path_list(path_list.drain(0..bulk_value as usize).collect());
            }
        }
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> RawMessageProto where PR: Fn(&i64) -> i32 + 'static {
        let mut proto = RawMessageProto::new();
        proto.set_message_type(self.message_type.to_proto(process_route));
        if self.message_type == RawMessageType::VERTEX {
            if let Some(pr) = process_route {
                let store_id = pr(&self.get_id());
                proto.set_store_id(store_id);
            }
        }
        if let Some(ref id) = self.id {
            proto.set_id(id.id);
            proto.set_type_id(id.label_id);
        }
        proto.set_bulk(self.get_bulk());
        if let Some(ref extra) = self.extra {
            let mut extra_proto = ExtraMessageProto::new();
            if let Some(ref edge) = extra.edge_entity {
                let mut edge_proto = ExtraEdgeEntityProto::new();
                edge_proto.set_src_id(edge.src_id);
                edge_proto.set_src_type_id(edge.src_label);
                edge_proto.set_dst_id(edge.dst_id);
                edge_proto.set_dst_type_id(edge.dst_label);
                edge_proto.set_is_out(edge.is_out);
                extra_proto.set_extra_edge(edge_proto);
            }
            if let Some(ref prop_value) = extra.prop_value {
                let mut prop_value_proto = ValuePropertyEntityProto::new();
                match prop_value {
                    PropertyValueEntity::VALUE(v) => {
                        prop_value_proto.set_value_entity(v.to_proto(process_route));
                    }
                    PropertyValueEntity::PROPLIST(proplist) => {
                        prop_value_proto.set_prop_list(RepeatedField::from_vec(proplist.iter().map(move |v| v.to_proto(process_route)).collect_vec()));
                    }
                }
                extra_proto.set_extra_value_prop(prop_value_proto);
            }
            if let Some(ref extra_extend) = extra.extra_extend {
                let mut path_label_proto = ExtraPathLabelEntityProto::new();
                if let Some(ref label_list) = extra_extend.label_list {
                    path_label_proto.set_label_list(RepeatedField::from_vec(label_list.iter().map(move |v| v.to_proto(process_route)).collect_vec()));
                }
                if let Some(ref path_list) = extra_extend.path_list {
                    path_label_proto.set_path_list(RepeatedField::from_vec(path_list.iter()
                        .map(move |v| {
                            let mut path_entity_list = PathEntityListProto::new();
                            path_entity_list.set_path_val_list(RepeatedField::from_vec(v.iter().map(|vv|
                                vv.to_proto(process_route)).collect_vec()));
                            path_entity_list
                        })
                        .collect_vec()));
                }
                extra_proto.set_extra_path_label(path_label_proto);

                if let Some(ref extend_key) = extra_extend.extra_key {
                    extra_proto.set_extra_key(extend_key.to_vec());
                }
            }

            proto.set_extra(extra_proto);
        }

        proto
    }
}

fn parse_label_list_proto(label_list_proto: &mut RepeatedField<LabelEntityProto>) -> Option<Vec<ExtraLabelEntity>> {
    let mut label_list = vec![];
    for label_proto in label_list_proto.iter_mut() {
        label_list.push(ExtraLabelEntity {
            label_id: label_proto.get_label_id(),
            message: RawMessage::from_proto(label_proto.mut_message()),
        });
    }

    if label_list.is_empty() {
        return None;
    }
    return Some(label_list);
}

fn parse_path_list_proto(path_list_proto: &mut RepeatedField<PathEntityListProto>) -> Option<Vec<Vec<ExtraPathEntity>>> {
    let path_list = vec![];
    for path_proto_list in path_list_proto.iter_mut() {
        let mut path_value_list = vec![];
        for path_proto in path_proto_list.mut_path_val_list().iter_mut() {
            let message = RawMessage::from_proto(path_proto.mut_message());
            let label_list = {
                let curr_label_list = path_proto.take_label_list();
                if curr_label_list.is_empty() {
                    None
                } else {
                    Some(curr_label_list)
                }
            };
            path_value_list.push(ExtraPathEntity {
                label_list,
                message,
            });
        }
    }
    if path_list.is_empty() {
        return None;
    }

    return Some(path_list);
}

fn add_path_list_bulk(path_list: &mut Vec<Vec<ExtraPathEntity>>, path_entity: ExtraPathEntity, bulk: i64) {
    if bulk > 0 {
        let to_build_count = bulk - path_list.len() as i64;
        for _ in 0..to_build_count {
            path_list.push(vec![]);
        }
        for i in 0..bulk - 1 {
            path_list.get_mut(i as usize).unwrap().push(path_entity.clone());
        }
        path_list.get_mut((bulk - 1) as usize).unwrap().push(path_entity);
    }
}

fn build_path_list_bulk(path_entity: ExtraPathEntity, bulk: i64) -> Vec<Vec<ExtraPathEntity>> {
    let mut path_bulk_list = Vec::with_capacity(bulk as usize);
    for _ in 0..bulk - 1 {
        path_bulk_list.push(vec![path_entity.clone()]);
    }
    path_bulk_list.push(vec![path_entity]);

    return path_bulk_list;
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct IdMessage {
    label_id: i32,
    id: i64,
}

impl IdMessage {
    pub fn new(label_id: i32, id: i64) -> Self {
        IdMessage { label_id, id }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraMessage {
    edge_entity: Option<ExtraEdgeEntity>,
    prop_value: Option<PropertyValueEntity>,
    extra_extend: Option<ExtraExtendEntity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraEdgeEntity {
    src_label: i32,
    src_id: i64,
    dst_label: i32,
    dst_id: i64,
    is_out: bool,
}

impl ExtraEdgeEntity {
    pub fn new() -> Self {
        ExtraEdgeEntity {
            src_label: 0,
            src_id: 0,
            dst_label: 0,
            dst_id: 0,
            is_out: false,
        }
    }

    pub fn from_vertex(src_id: i64,
                       src_label: i32,
                       dst_id: i64,
                       dst_label: i32) -> Self {
        ExtraEdgeEntity {
            src_label,
            src_id,
            dst_label,
            dst_id,
            is_out: true,
        }
    }

    pub fn set_src_label(&mut self, src_label: i32) {
        self.src_label = src_label;
    }

    pub fn get_src_label(&self) -> i32 {
        self.src_label
    }

    pub fn set_src_id(&mut self, src_id: i64) {
        self.src_id = src_id;
    }

    pub fn get_src_id(&self) -> i64 {
        self.src_id
    }

    pub fn set_dst_label(&mut self, dst_label: i32) {
        self.dst_label = dst_label;
    }

    pub fn get_dst_label(&self) -> i32 {
        self.dst_label
    }

    pub fn set_dst_id(&mut self, dst_id: i64) {
        self.dst_id = dst_id;
    }

    pub fn get_dst_id(&self) -> i64 {
        self.dst_id
    }

    pub fn set_is_out(&mut self, is_out: bool) {
        self.is_out = is_out;
    }

    pub fn get_is_out(&self) -> bool {
        self.is_out
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub enum PropertyValueEntity {
    PROPLIST(Vec<PropertyEntity>),
    VALUE(ValuePayload),
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct PropertyEntity {
    prop_id: i32,
    value: ValuePayload,
}

impl PropertyEntity {
    pub fn new(prop_id: i32, value: ValuePayload) -> Self {
        PropertyEntity {
            prop_id,
            value,
        }
    }

    pub fn get_propid(&self) -> i32 {
        self.prop_id
    }

    pub fn get_value(&self) -> &ValuePayload {
        &self.value
    }

    pub fn take_value(self) -> ValuePayload {
        self.value
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> PropertyEntityProto where PR: Fn(&i64) -> i32 + 'static {
        let mut prop_proto = PropertyEntityProto::new();
        prop_proto.set_prop_id(self.prop_id);
        prop_proto.set_prop_value(self.value.to_proto(process_route));

        prop_proto
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub enum ValuePayload {
    Bool(bool),
    Char(u8),
    Short(i16),
    Int(i32),
    Long(i64),
    Float(Vec<u8>),
    Double(Vec<u8>),
    Bytes(Vec<u8>),
    String(String),
    Date(String),
    List(Vec<RawMessage>),
    Map(Vec<ExtraEntryEntity>),
    Entry(ExtraEntryEntity),
    PathEntry(Box<ExtraPathEntity>),
    //for store operator
    ListInt(Vec<i32>),
    ListLong(Vec<i64>),
    ListString(Vec<String>),
}

impl ValuePayload {
    pub fn from_native(prop: Property) -> (Self, RawMessageType) {
        match prop {
            Property::Bool(v) => (ValuePayload::Bool(v), RawMessageType::VALUE),
            Property::Char(v) => (ValuePayload::Char(v), RawMessageType::VALUE),
            Property::Short(v) => (ValuePayload::Short(v), RawMessageType::VALUE),
            Property::Int(v) => (ValuePayload::Int(v), RawMessageType::VALUE),
            Property::Long(v) => (ValuePayload::Long(v), RawMessageType::VALUE),
            Property::Float(v) => (ValuePayload::Float(v.into_bytes()), RawMessageType::VALUE),
            Property::Double(v) => (ValuePayload::Double(v.into_bytes()), RawMessageType::VALUE),
            Property::Bytes(v) => (ValuePayload::Bytes(v), RawMessageType::VALUE),
            Property::String(v) => (ValuePayload::String(v), RawMessageType::VALUE),
            Property::Date(v) => (ValuePayload::Date(v), RawMessageType::VALUE),
            Property::ListInt(v) => {
                let list = v.into_iter()
                    .map(move |v| RawMessage::from_value(ValuePayload::Int(v))).collect_vec();
                (ValuePayload::List(list), RawMessageType::LIST)
            }
            Property::ListLong(v) => {
                let list = v.into_iter()
                    .map(move |v| RawMessage::from_value(ValuePayload::Long(v))).collect_vec();
                (ValuePayload::List(list), RawMessageType::LIST)
            }
            Property::ListString(v) => {
                let list = v.into_iter()
                    .map(move |v| RawMessage::from_value(ValuePayload::String(v))).collect_vec();
                (ValuePayload::List(list), RawMessageType::LIST)
            }
            Property::ListFloat(v) => {
                let list = v.into_iter()
                    .map(move |v| RawMessage::from_value(ValuePayload::Float(v.into_bytes()))).collect_vec();
                (ValuePayload::List(list), RawMessageType::LIST)
            }
            Property::ListDouble(v) => {
                let list = v.into_iter()
                    .map(move |v| RawMessage::from_value(ValuePayload::Double(v.into_bytes()))).collect_vec();
                (ValuePayload::List(list), RawMessageType::LIST)
            }
            Property::ListBytes(v) => {
                let list = v.into_iter()
                    .map(move |v| RawMessage::from_value(ValuePayload::Bytes(v))).collect_vec();
                (ValuePayload::List(list), RawMessageType::LIST)
            }
            _ => (ValuePayload::Bytes(vec![]), RawMessageType::ERROR),
        }
    }


    /// get boolean value
    pub fn get_bool(&self) -> Result<bool, String> {
        match self {
            &ValuePayload::Bool(b) => { Ok(b) }
            _ => { Err(format!("get bool value fail from property=>{:?}", self)) }
        }
    }

    /// get char value
    pub fn get_char(&self) -> Result<u8, String> {
        match self {
            &ValuePayload::Char(c) => { Ok(c) }
            _ => { Err(format!("get char value fail from property=>{:?}", self)) }
        }
    }

    /// get short value
    pub fn get_short(&self) -> Result<i16, String> {
        match self {
            &ValuePayload::Short(c) => { Ok(c) }
            _ => { Err(format!("get short value fail from property=>{:?}", self)) }
        }
    }

    /// get int value
    pub fn get_int(&self) -> Result<i32, String> {
        match self {
            &ValuePayload::Int(i) => { Ok(i) }
            _ => { Err(format!("get int value fail from property=>{:?}", self)) }
        }
    }

    /// get long value
    pub fn get_long(&self) -> Result<i64, String> {
        match self {
            &ValuePayload::Int(l) => { Ok(l as i64) }
            &ValuePayload::Long(l) => { Ok(l) }
            _ => { Err(format!("get long value fail from property=>{:?}", self)) }
        }
    }

    /// get float value
    pub fn get_float(&self) -> Result<f32, String> {
        match self {
            &ValuePayload::Int(i) => { Ok(i as f32) }
            &ValuePayload::Float(ref f) => { Ok(f32::parse_bytes(f)) }
            _ => { Err(format!("get float value fail from property=>{:?}", self)) }
        }
    }

    /// get double value
    pub fn get_double(&self) -> Result<f64, String> {
        match self {
            &ValuePayload::Int(d) => { Ok(d as f64) }
            &ValuePayload::Long(d) => { Ok(d as f64) }
            &ValuePayload::Float(ref d) => { Ok(f32::parse_bytes(d) as f64) }
            &ValuePayload::Double(ref d) => { Ok(f64::parse_bytes(d)) }
            _ => { Err(format!("get double value fail from property=>{:?}", self)) }
        }
    }

    /// get string ref value
    pub fn get_string(&self) -> Result<&String, String> {
        match self {
            &ValuePayload::String(ref s) => { Ok(s) }
            &ValuePayload::Date(ref s) => { Ok(s) }
            _ => { Err(format!("get string ref value fail from property=>{:?}", self)) }
        }
    }

    /// get bytes
    pub fn get_bytes(&self) -> Result<&Vec<u8>, String> {
        match self {
            &ValuePayload::Bytes(ref bytes) => { Ok(bytes) }
            _ => { Err(format!("get bytes fail from property=>{:?}", self)) }
        }
    }

    pub fn get_list(&self) -> Result<&Vec<RawMessage>, String> {
        match self {
            &ValuePayload::List(ref list) => { Ok(list) }
            _ => { Err(format!("get list fail from property=>{:?}", self)) }
        }
    }

    pub fn take_list(self) -> Result<Vec<RawMessage>, String> {
        match self {
            ValuePayload::List(list) => { Ok(list) }
            _ => { Err(format!("get list fail from property=>{:?}", self)) }
        }
    }

    pub fn take_list_long(self) -> Result<Vec<i64>, String> {
        match self {
            ValuePayload::ListLong(list) => { Ok(list) }
            _ => { Err(format!("get long list fail=>{:?}", self)) }
        }
    }

    pub fn get_map(&self) -> Result<&Vec<ExtraEntryEntity>, String> {
        match self {
            &ValuePayload::Map(ref map) => { Ok(map) }
            _ => { Err(format!("get map fail from property=>{:?}", self)) }
        }
    }

    pub fn take_map(self) -> Result<Vec<ExtraEntryEntity>, String> {
        match self {
            ValuePayload::Map(map) => { Ok(map) }
            _ => { Err(format!("get map fail from property=>{:?}", self)) }
        }
    }

    pub fn get_entry(&self) -> Result<&ExtraEntryEntity, String> {
        match self {
            &ValuePayload::Entry(ref entry) => Ok(entry),
            _ => Err(format!("get entry fail from property=>{:?}", self))
        }
    }

    pub fn take_entry(self) -> Result<ExtraEntryEntity, String> {
        match self {
            ValuePayload::Entry(entry) => Ok(entry),
            _ => Err(format!("get entry fail from property=>{:?}", self))
        }
    }

    pub fn take_path_entry(self) -> Result<Box<ExtraPathEntity>, String> {
        match self {
            ValuePayload::PathEntry(entry) => Ok(entry),
            _ => Err(format!("get path entry fail from property=>{:?}", self))
        }
    }

    pub fn from_proto(value_proto: &ValueEntityProto, message_type: RawMessageType) -> Self {
        match value_proto.get_value_type() {
            VariantType::VT_BOOL => {
                return ValuePayload::Bool(bool::parse_bytes(value_proto.get_payload()));
            }
            VariantType::VT_CHAR => {
                return ValuePayload::Char(u8::parse_bytes(value_proto.get_payload()));
            }
            VariantType::VT_SHORT => {
                return ValuePayload::Short(i16::parse_bytes(value_proto.get_payload()));
            }
            VariantType::VT_INT => {
                return ValuePayload::Int(i32::parse_bytes(value_proto.get_payload()));
            }
            VariantType::VT_LONG => {
                return ValuePayload::Long(i64::parse_bytes(value_proto.get_payload()));
            }
            VariantType::VT_FLOAT => {
                return ValuePayload::Float(value_proto.get_payload().to_vec());
            }
            VariantType::VT_DOUBLE => {
                return ValuePayload::Double(value_proto.get_payload().to_vec());
            }
            VariantType::VT_BINARY => {
                match message_type {
                    RawMessageType::LIST => {
                        let mut value_list = Vec::new();
                        let mut list_proto = parse_from_bytes::<ListProto>(value_proto.get_payload()).expect("parse map proto");
                        for mut value_proto in list_proto.take_value().into_iter() {
                            value_list.push(RawMessage::from_proto(&mut value_proto));
                        }
                        return ValuePayload::List(value_list);
                    }
                    RawMessageType::MAP => {
                        let mut map_value_list = Vec::new();
                        let mut map_proto = parse_from_bytes::<MapProto>(value_proto.get_payload()).expect("parse map proto");
                        for mut entry_proto in map_proto.take_entry_list().into_iter() {
                            map_value_list.push(ExtraEntryEntity::new(RawMessage::from_proto(entry_proto.mut_key()),
                                                                      RawMessage::from_proto(entry_proto.mut_value())));
                        }
                        return ValuePayload::Map(map_value_list);
                    }
                    RawMessageType::ENTRY => {
                        let mut entry_proto = parse_from_bytes::<EntryProto>(value_proto.get_payload()).expect("parse entry proto");
                        let entry_entity = ExtraEntryEntity::new(RawMessage::from_proto(entry_proto.mut_key()),
                                                                 RawMessage::from_proto(entry_proto.mut_value()));
                        return ValuePayload::Entry(entry_entity);
                    }
                    RawMessageType::PATHENTRY => {
                        let mut path_entry_proto = parse_from_bytes::<PathEntityProto>(value_proto.get_payload()).expect("parse path entry proto");
                        let label_id_list = path_entry_proto.get_label_list().to_vec();
                        let path_entry = ExtraPathEntity {
                            label_list: {
                                if label_id_list.is_empty() {
                                    None
                                } else {
                                    Some(label_id_list)
                                }
                            },
                            message: RawMessage::from_proto(path_entry_proto.mut_message()),
                        };
                        return ValuePayload::PathEntry(Box::new(path_entry));
                    }
                    _ => {
                        return ValuePayload::Bytes(value_proto.get_payload().to_vec());
                    }
                }
            }
            VariantType::VT_STRING => {
                return ValuePayload::String(String::parse_bytes(value_proto.get_payload()));
            }
            VariantType::VT_DATE => {
                return ValuePayload::Date(String::parse_bytes(value_proto.get_payload()));
            }
            _ => {
                error!("cant parse value");
                return ValuePayload::Bool(false);
            }
        }
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> ValueEntityProto where PR: Fn(&i64) -> i32 + 'static {
        let mut value_proto = ValueEntityProto::new();
        match self {
            ValuePayload::Bool(v) => {
                value_proto.set_value_type(VariantType::VT_BOOL);
                value_proto.set_payload(v.into_bytes());
            }
            ValuePayload::Char(v) => {
                value_proto.set_value_type(VariantType::VT_CHAR);
                value_proto.set_payload(v.into_bytes());
            }
            ValuePayload::Short(v) => {
                value_proto.set_value_type(VariantType::VT_SHORT);
                value_proto.set_payload(v.into_bytes());
            }
            ValuePayload::Int(v) => {
                value_proto.set_value_type(VariantType::VT_INT);
                value_proto.set_payload(v.into_bytes());
            }
            ValuePayload::Long(v) => {
                value_proto.set_value_type(VariantType::VT_LONG);
                value_proto.set_payload(v.into_bytes());
            }
            ValuePayload::Float(v) => {
                value_proto.set_value_type(VariantType::VT_FLOAT);
                value_proto.set_payload(v.to_vec());
            }
            ValuePayload::Double(v) => {
                value_proto.set_value_type(VariantType::VT_DOUBLE);
                value_proto.set_payload(v.to_vec());
            }
            ValuePayload::Bytes(v) => {
                value_proto.set_value_type(VariantType::VT_BINARY);
                value_proto.set_payload(v.to_vec());
            }
            ValuePayload::String(v) => {
                value_proto.set_value_type(VariantType::VT_STRING);
                value_proto.set_payload(v.to_owned().into_bytes());
            }
            ValuePayload::Date(v) => {
                value_proto.set_value_type(VariantType::VT_DATE);
                value_proto.set_payload(v.to_owned().into_bytes());
            }
            ValuePayload::List(v) => {
                let mut list_proto = ListProto::new();
                for m in v {
                    list_proto.mut_value().push(m.to_proto(process_route));
                }
                value_proto.set_value_type(VariantType::VT_BINARY);
                value_proto.set_payload(list_proto.write_to_bytes().unwrap());
            }
            ValuePayload::Map(v) => {
                let mut map_proto = MapProto::new();
                for m in v {
                    map_proto.mut_entry_list().push(m.to_proto(process_route));
                }
                value_proto.set_value_type(VariantType::VT_BINARY);
                value_proto.set_payload(map_proto.write_to_bytes().unwrap());
            }
            ValuePayload::Entry(v) => {
                value_proto.set_value_type(VariantType::VT_BINARY);
                value_proto.set_payload(v.to_proto(process_route).write_to_bytes().unwrap());
            }
            ValuePayload::PathEntry(v) => {
                value_proto.set_value_type(VariantType::VT_BINARY);
                value_proto.set_payload(v.to_proto(process_route).write_to_bytes().unwrap());
            }
            _ => {
                error!("cant support to proto");
            }
        }
        value_proto
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraEntryEntity {
    key: Box<RawMessage>,
    value: Box<RawMessage>,
}

impl ExtraEntryEntity {
    pub fn new(key: RawMessage, value: RawMessage) -> Self {
        ExtraEntryEntity {
            key: Box::new(key),
            value: Box::new(value),
        }
    }

    pub fn get_key(&self) -> &Box<RawMessage> {
        &self.key
    }

    pub fn take_key(self) -> RawMessage {
        *self.key
    }

    pub fn take_value(self) -> RawMessage {
        *self.value
    }

    pub fn get_value(&self) -> &Box<RawMessage> {
        &self.value
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> EntryProto where PR: Fn(&i64) -> i32 + 'static {
        let mut entry = EntryProto::new();
        entry.set_key(self.key.to_proto(process_route));
        entry.set_value(self.value.to_proto(process_route));
        entry
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraExtendEntity {
    extra_key: Option<Vec<u8>>,
    label_list: Option<Vec<ExtraLabelEntity>>,
    path_list: Option<Vec<Vec<ExtraPathEntity>>>,
}

impl ExtraExtendEntity {
    pub fn valid_entity(&self) -> bool {
        self.extra_key.is_some() || self.label_list.is_some() || self.path_list.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraKeyEntity {
    rand_key: i64,
    message: RawMessage,
}

impl ExtraKeyEntity {
    pub fn new(message: RawMessage, uniq_flag: bool) -> Self {
        let rand_key = {
            if uniq_flag {
                let mut rng = thread_rng();
                rng.gen::<i64>()
            } else {
                0
            }
        };
        ExtraKeyEntity {
            rand_key: rand_key,
            message,
        }
    }

    pub fn from_payload(payload: &Vec<u8>) -> Self {
        let mut extra_proto = parse_from_bytes::<ExtraKeyEntityProto>(payload).expect("parse extra key proto");
        let rand_key = extra_proto.get_key_rand();
        let key_message = RawMessage::from_proto(extra_proto.mut_message());
        ExtraKeyEntity {
            rand_key,
            message: key_message,
        }
    }

    pub fn take_message(self) -> RawMessage {
        self.message
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> ExtraKeyEntityProto where PR: Fn(&i64) -> i32 + 'static {
        let mut extra_key_entity = ExtraKeyEntityProto::new();
        extra_key_entity.set_key_rand(self.rand_key);
        extra_key_entity.set_message(self.message.to_proto(process_route));

        extra_key_entity
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraLabelEntity {
    label_id: i32,
    message: RawMessage,
}

impl ExtraLabelEntity {
    pub fn get_message(&self) -> &RawMessage {
        &self.message
    }

    pub fn take_message(self) -> RawMessage {
        self.message
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> LabelEntityProto where PR: Fn(&i64) -> i32 + 'static {
        let mut label_proto = LabelEntityProto::new();
        label_proto.set_label_id(self.label_id);
        label_proto.set_message(self.message.to_proto(process_route));

        label_proto
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct ExtraPathEntity {
    label_list: Option<Vec<i32>>,
    message: RawMessage,
}

impl ExtraPathEntity {
    pub fn take_message(self) -> RawMessage {
        self.message
    }

    pub fn take_label_list(&mut self) -> Option<Vec<i32>> {
        self.label_list.take()
    }

    pub fn get_message(&self) -> &RawMessage {
        &self.message
    }

    pub fn to_proto<PR>(&self, process_route: Option<&PR>) -> PathEntityProto where PR: Fn(&i64) -> i32 + 'static {
        let mut path_proto = PathEntityProto::new();
        if let Some(ref labels) = self.label_list {
            path_proto.set_label_list(labels.to_vec());
        }
        path_proto.set_message(self.message.to_proto(process_route));

        path_proto
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, Abomonation)]
pub struct RawBaseDataEntity {
    message_type: RawMessageType,
    id: Option<IdMessage>,
    edge_entity: Option<ExtraEdgeEntity>,
    prop_value: Option<PropertyValueEntity>,
    extra_key: Option<Vec<u8>>,
    label_list: Option<Vec<ExtraLabelEntity>>,
}

impl RawBaseDataEntity {
    pub fn new(message_type: RawMessageType) -> Self {
        RawBaseDataEntity { message_type, id: None, edge_entity: None, prop_value: None, extra_key: None, label_list: None }
    }

    pub fn from_message(message: &mut RawMessage) -> Self {
        let mut raw_base = RawBaseDataEntity::new(message.get_message_type());
        raw_base.set_id(message.take_id());
        raw_base.set_edge(message.take_edge());
        raw_base.set_prop_value(message.take_prop_value());
        raw_base.set_extra_key(message.take_extend_key_payload());
        raw_base.set_label_list(message.take_label_entity_list());
        raw_base
    }

    pub fn set_id(&mut self, id: Option<IdMessage>) {
        self.id = id;
    }

    pub fn set_edge(&mut self, edge: Option<ExtraEdgeEntity>) {
        self.edge_entity = edge;
    }

    pub fn set_prop_value(&mut self, prop: Option<PropertyValueEntity>) {
        self.prop_value = prop;
    }

    pub fn set_extra_key(&mut self, key: Option<Vec<u8>>) {
        self.extra_key = key;
    }

    pub fn set_label_list(&mut self, label_list: Option<Vec<ExtraLabelEntity>>) {
        self.label_list = label_list;
    }

    pub fn get_message_type(&self) -> RawMessageType {
        self.message_type
    }

    pub fn get_id_message(&self) -> Option<&IdMessage> {
        if let Some(ref id) = self.id {
            return Some(id);
        }
        return None;
    }

    pub fn take_id(&mut self) -> Option<IdMessage> {
        return self.id.take();
    }

    pub fn get_edge(&self) -> Option<&ExtraEdgeEntity> {
        if let Some(ref edge_entity) = self.edge_entity {
            return Some(edge_entity);
        }
        return None;
    }

    pub fn take_edge(&mut self) -> Option<ExtraEdgeEntity> {
        if let Some(edge_entity) = self.edge_entity.take() {
            return Some(edge_entity);
        }
        return None;
    }

    pub fn get_prop_list(&self) -> Option<&Vec<PropertyEntity>> {
        if let Some(ref prop_value) = self.prop_value {
            match prop_value {
                PropertyValueEntity::PROPLIST(proplist) => {
                    return Some(proplist);
                }
                _ => {}
            }
        }
        return None;
    }

    pub fn take_prop_value(&mut self) -> Option<PropertyValueEntity> {
        return self.prop_value.take();
    }

    pub fn get_extend_key_payload(&mut self) -> Option<&Vec<u8>> {
        if let Some(ref extend) = self.extra_key {
            return self.extra_key.as_ref();
        }
        return None;
    }

    pub fn take_extend_key_payload(&mut self) -> Option<Vec<u8>> {
        return self.extra_key.take();
    }

    pub fn take_label_list(&mut self) -> Option<Vec<ExtraLabelEntity>> {
        return self.label_list.take();
    }
}

pub struct BulkExtraEntity {
    path_bulk_list: Option<Vec<Vec<ExtraPathEntity>>>,
    bulk_value: i64,
}

impl BulkExtraEntity {
    pub fn new() -> Self {
        BulkExtraEntity { path_bulk_list: None, bulk_value: 0 }
    }

    pub fn add_path_entity(&mut self, value: &mut RawMessage) {
        if let Some(ref mut path_bulk_list) = self.path_bulk_list {
            if let Some(path_list) = value.take_path_list() {
                for path in path_list {
                    path_bulk_list.push(path);
                }
            }
        } else {
            if let Some(path_list) = value.take_path_list() {
                let mut tmp_path_list = vec![];
                for path in path_list {
                    tmp_path_list.push(path);
                }
                self.path_bulk_list = Some(tmp_path_list);
            }
        }
    }

    pub fn increase_bulk_value(&mut self, inc_bulk: i64) {
        self.bulk_value += inc_bulk;
    }

    pub fn get_bulk_value(&self) -> i64 {
        return self.bulk_value;
    }

    pub fn take_path_bulk_list(&mut self) -> Option<Vec<Vec<ExtraPathEntity>>> {
        return self.path_bulk_list.take();
    }
}

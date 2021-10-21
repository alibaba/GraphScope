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

use dataflow::message::{RawMessage, RawMessageType, PropertyEntity, ValuePayload, ExtraLabelEntity};

use maxgraph_common::proto::message::{OrderComparatorList, OrderComparator, OrderType};
use maxgraph_common::proto::message;
use maxgraph_common::util::hash::murmur_hash64;

use std::cmp::Ordering;
use utils::{PROP_VALUE, PROP_ID, PROP_KEY};
use protobuf::Message;
use execution::build_empty_router;
use std::sync::Arc;

pub struct OrderManager {
    val_list: Vec<RawMessage>,
    comparator_list: OrderComparatorList,
    range_limit: usize,
    has_limit: bool,
    order_property_ids: Vec<i32>,
}

impl OrderManager {
    pub fn new(comparator_list: OrderComparatorList,
               range_limit: usize,
               has_limit: bool) -> Self {
        let mut order_property_ids = vec![];
        for order_comparator in comparator_list.get_order_comparator() {
            let prop_id = order_comparator.get_prop_id();
            if prop_id > 0 && !order_property_ids.contains(&prop_id) {
                order_property_ids.push(prop_id);
            }
        }
        OrderManager {
            val_list: Vec::new(),
            comparator_list,
            range_limit,
            has_limit,
            order_property_ids,
        }
    }

    pub fn get_order_property_id(&self) -> &Vec<i32> {
        &self.order_property_ids
    }

    pub fn add_message(&mut self, message: RawMessage) {
        order_message_list(&mut self.val_list,
                           message,
                           self.comparator_list.get_order_comparator(),
                           self.range_limit,
                           self.has_limit);
    }

    pub fn get_result_list(&mut self) -> Vec<RawMessage> {
        ::std::mem::replace(&mut self.val_list, Vec::new())
    }

    pub fn get_range_result_list(&mut self, range_start: usize) -> Vec<RawMessage> {
        let mut result_list = Vec::with_capacity(self.range_limit - range_start);
        let mut curr_range = 0;
        for mut message in self.val_list.drain(..) {
            let next_range = curr_range + message.get_bulk();
            if (next_range as usize) < range_start {
                curr_range = next_range;
                continue;
            } else if (curr_range as usize) < range_start {
                if (next_range as usize) < self.range_limit {
                    let left_bulk = next_range - range_start as i64;
                    if left_bulk > 0 {
                        message.update_with_bulk(left_bulk);
                        result_list.push(message);
                    }
                } else {
                    let left_bulk = self.range_limit - range_start;
                    if left_bulk > 0 {
                        message.update_with_bulk(left_bulk as i64);
                        result_list.push(message);
                    }
                    return result_list;
                }
            } else {
                if (next_range as usize) < self.range_limit {
                    result_list.push(message);
                } else {
                    let left_bulk = (self.range_limit as i64) - curr_range;
                    if left_bulk > 0 {
                        message.update_with_bulk(left_bulk);
                        result_list.push(message);
                    }
                    return result_list;
                }
            }
            curr_range = next_range;
        }
        return result_list;
    }
}

pub fn order_message_list(val_list: &mut Vec<RawMessage>,
                          message: RawMessage,
                          order_list: &[OrderComparator],
                          range_limit: usize,
                          has_limit: bool) {
    val_list.push(message);
    val_list.sort_by(|a, b|
        cmp_vertex_edge(a, b, order_list));
    if has_limit && val_list.len() > range_limit {
        val_list.pop();
    }
}

#[inline]
pub fn cmp_vertex_edge(a: &RawMessage, b: &RawMessage, orders: &[OrderComparator]) -> Ordering {
    if a.get_message_type() == b.get_message_type() {
        match a.get_message_type() {
            RawMessageType::VERTEX | RawMessageType::EDGE => {
                cmp_vertex_edge_class_value(a, b, orders)
            }
            RawMessageType::ERROR => {
                error!("not support: {:?}", a.get_message_type());
                Ordering::Equal
            }
            _ => {
                cmp_value_entity_orders(a, b, orders)
            }
        }
    } else {
        error!("class type is not equal, {:?} vs {:?}", a.get_message_type(), b.get_message_type());
        Ordering::Equal
    }
}

#[inline]
pub fn cmp_prop_entity(a: Option<&PropertyEntity>, b: Option<&PropertyEntity>, order: &OrderComparator) -> Ordering {
    if a.is_none() && b.is_none() {
        return Ordering::Equal;
    }

    match order.get_order_type() {
        OrderType::INCR | OrderType::ASC => {
            cmp_prop_entity_ordering(a, b)
        }
        OrderType::DECR | OrderType::DESC => {
            let order = cmp_prop_entity_ordering(a, b);
            match order {
                Ordering::Less => {
                    Ordering::Greater
                }
                Ordering::Greater => {
                    Ordering::Less
                }
                _ => {
                    Ordering::Equal
                }
            }
        }
        OrderType::SHUFFLE => {
            error!("should not shuffle here");
            Ordering::Equal
        }
    }
}

#[inline]
pub fn cmp_prop_entity_ordering(a: Option<&PropertyEntity>, b: Option<&PropertyEntity>) -> Ordering {
    if a.is_none() {
        return Ordering::Less;
    }
    if b.is_none() {
        return Ordering::Greater;
    }

    let a = a.unwrap();
    let b = b.unwrap();
    cmp_value_entity(a.get_value(), b.get_value())
}

#[inline]
fn cmp_value_entity(a: &ValuePayload, b: &ValuePayload) -> Ordering {
    match a {
        ValuePayload::Int(_) | ValuePayload::Long(_) => {
            if let Ok(va) = a.get_long() {
                if let Ok(vb) = b.get_long() {
                    return va.cmp(&vb);
                }
            }
        }
        ValuePayload::Float(_) | ValuePayload::Double(_) => {
            if let Ok(va) = a.get_double() {
                if let Ok(vb) = b.get_double() {
                    if va > vb {
                        return Ordering::Greater;
                    } else if va < vb {
                        return Ordering::Less;
                    }
                }
            }
        }
        ValuePayload::String(_) | ValuePayload::Date(_) => {
            if let Ok(va) = a.get_string() {
                if let Ok(vb) = b.get_string() {
                    return va.cmp(vb);
                }
            }
        }
        _ => {
            let empty_fn = Arc::new(build_empty_router());
            let ahash = murmur_hash64(&a.to_proto(Some(empty_fn.as_ref())).write_to_bytes().unwrap());
            let bhash = murmur_hash64(&b.to_proto(Some(empty_fn.as_ref())).write_to_bytes().unwrap());
            return ahash.cmp(&bhash);
        }
    }
    return Ordering::Equal;
}

#[inline]
pub fn cmp_label_entity_value(la: Option<&ExtraLabelEntity>, lb: Option<&ExtraLabelEntity>, order: &OrderComparator) -> Ordering {
    if let Some(lav) = la {
        if let Some(lbv) = lb {
            if let Some(lav_entity) = lav.get_message().get_value() {
                if let Some(lbv_entity) = lbv.get_message().get_value() {
                    return cmp_value_entity_ordering(lav_entity, lbv_entity, order);
                }
            }
        }
    }

    Ordering::Equal
}

#[inline]
fn cmp_value_entity_ordering(a: &ValuePayload, b: &ValuePayload, order: &OrderComparator) -> Ordering {
    let ordering = cmp_value_entity(a, b);
    match order.get_order_type() {
        OrderType::DECR | OrderType::DESC => {
            match ordering {
                Ordering::Less => {
                    Ordering::Greater
                }
                Ordering::Greater => {
                    Ordering::Less
                }
                _ => {
                    ordering
                }
            }
        }
        OrderType::INCR | OrderType::ASC | OrderType::SHUFFLE => {
            ordering
        }
    }
}

#[inline]
pub fn cmp_vertex_edge_class_value(va: &RawMessage, vb: &RawMessage, orders: &[OrderComparator]) -> Ordering {
    let mut ordering = Ordering::Equal;
    for order in orders {
        let prop_id = order.get_prop_id();
        if prop_id > 0 {
            let pa = va.get_property(prop_id);
            let pb = vb.get_property(prop_id);
            ordering = cmp_prop_entity(pa, pb, order);
        } else if prop_id < PROP_VALUE {
            ordering = cmp_label_entity_value(va.get_label_entity_by_id(prop_id), vb.get_label_entity_by_id(prop_id), order);
        } else if prop_id == PROP_ID
            || order.get_prop_id() == 0 {
            ordering = va.get_id().cmp(&vb.get_id());
        } else {
            RawMessage::from_error(message::ErrorCode::UNIMPLEMENT, "not support to compare label/key/value yet".to_string());
        }
        if ordering != Ordering::Equal {
            break;
        }
    }
    ordering
}

#[inline]
pub fn cmp_entry_key_ordering(a: &RawMessage, b: &RawMessage, order: &OrderComparator) -> Ordering {
    if let Some(va) = a.get_value() {
        if let Some(vb) = b.get_value() {
            if let Ok(entry_a) = va.get_entry() {
                if let Ok(entry_b) = vb.get_entry() {
                    if let Some(av) = entry_a.get_key().get_value() {
                        if let Some(bv) = entry_b.get_key().get_value() {
                            return cmp_value_entity_ordering(av, bv, order);
                        }
                    }
                }
            }
        }
    }
    return Ordering::Equal;
}

#[inline]
pub fn cmp_entry_value_ordering(a: &RawMessage, b: &RawMessage, order: &OrderComparator) -> Ordering {
    if let Some(va) = a.get_value() {
        if let Some(vb) = b.get_value() {
            if let Ok(entry_a) = va.get_entry() {
                if let Ok(entry_b) = vb.get_entry() {
                    if let Some(av) = entry_a.get_value().get_value() {
                        if let Some(bv) = entry_b.get_value().get_value() {
                            return cmp_value_entity_ordering(av, bv, order);
                        }
                    }
                }
            }
        }
    }
    return Ordering::Equal;
}

#[inline]
pub fn cmp_value_entity_orders(a: &RawMessage, b: &RawMessage, orders: &[OrderComparator]) -> Ordering {
    for order in orders {
        let ordering = {
            if order.get_prop_id() == PROP_KEY {
                match a.get_message_type() {
                    RawMessageType::ENTRY => {
                        cmp_entry_key_ordering(a, b, order)
                    }
                    _ => {
                        Ordering::Equal
                    }
                }
            } else if order.get_prop_id() == PROP_VALUE {
                match a.get_message_type() {
                    RawMessageType::ENTRY => {
                        cmp_entry_value_ordering(a, b, order)
                    }
                    _ => {
                        Ordering::Equal
                    }
                }
            } else if order.get_prop_id() < PROP_VALUE {
                let label_id = order.get_prop_id();
                cmp_label_entity_value(a.get_label_entity_by_id(label_id), b.get_label_entity_by_id(label_id), order)
            } else if order.get_prop_id() > 0 {
                let prop_id = order.get_prop_id();
                if let Some(a_prop) = a.get_property(prop_id) {
                    if let Some(b_prop) = b.get_property(prop_id) {
                        cmp_value_entity_ordering(a_prop.get_value(), b_prop.get_value(), order)
                    } else {
                        Ordering::Equal
                    }
                } else {
                    Ordering::Equal
                }
            } else if order.get_prop_id() == 0 {
                if a.get_message_type() == b.get_message_type() {
                    match a.get_message_type() {
                        RawMessageType::VALUE => {
                            if let Some(avalue_entity) = a.get_value() {
                                if let Some(bvalue_entity) = b.get_value() {
                                    cmp_value_entity_ordering(avalue_entity, bvalue_entity, order)
                                } else {
                                    Ordering::Equal
                                }
                            } else {
                                Ordering::Equal
                            }
                        }
                        _ => {
                            let empty_fn = Arc::new(build_empty_router());
                            murmur_hash64(&a.to_proto(Some(empty_fn.as_ref())).write_to_bytes().unwrap())
                                .cmp(&murmur_hash64(&b.to_proto(Some(empty_fn.as_ref())).write_to_bytes().unwrap()))
                        }
                    }
                } else {
                    Ordering::Equal
                }
            } else {
                Ordering::Equal
            }
        };
        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

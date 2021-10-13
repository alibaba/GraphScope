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

extern crate regex;

use maxgraph_common::proto::message::{VariantType, CompareType, Value, LogicalCompare};
use maxgraph_store::api::prelude::{Vertex, Edge};
use maxgraph_store::api::prelude::Property;
use maxgraph_store::schema::{PropId, Schema};

use regex::Regex;
use std::collections::HashSet;
use std::sync::Arc;
use crate::filter::regex::RegexFilter;
use crate::filter::*;

pub struct FilterManager {
    filters: Vec<LogicalCompare>,
    regex_filters: Vec<RegexFilter>,
    propid_list: HashSet<i32>,
    schema: Arc<dyn Schema>,
}

#[inline]
fn parse_compare_propid_list(logical_compare: &LogicalCompare) -> (HashSet<i32>, HashSet<i32>) {
    let mut prop_list = HashSet::new();
    let mut label_list = HashSet::new();
    match logical_compare.get_compare() {
        CompareType::OR_RELATION | CompareType::AND_RELATION => {
            for child_compare in logical_compare.get_child_compare_list() {
                let (child_propid_list, child_labelid_list) = parse_compare_propid_list(child_compare);
                for propid in child_propid_list.into_iter() {
                    prop_list.insert(propid);
                }
                for labelid in child_labelid_list.into_iter() {
                    label_list.insert(labelid);
                }
            }
        }
        _ => {
            if logical_compare.get_prop_id() > 0 {
                prop_list.insert(logical_compare.get_prop_id());
            } else if logical_compare.get_prop_id() < 0 {
                label_list.insert(logical_compare.get_prop_id());
            }
        }
    }

    return (prop_list, label_list);
}

impl FilterManager {
    pub fn new(filters: &[LogicalCompare],
               schema: Arc<dyn Schema>) -> Self {
        let mut logic_filters = vec![];
        let mut regex_filters = vec![];
        let mut propid_list = HashSet::new();
        let mut labelid_list = HashSet::new();
        for filter in filters.to_vec() {
            let (curr_propid_list, curr_labelid_list) = parse_compare_propid_list(&filter);
            for propid in curr_propid_list {
                propid_list.insert(propid);
            }
            for labelid in curr_labelid_list {
                labelid_list.insert(labelid);
            }

            if filter.compare == CompareType::REGEX {
                regex_filters.push(RegexFilter {
                    regex: Regex::new(filter.get_value().get_str_value()).unwrap(),
                    logical_compare: filter,
                });
            } else {
                logic_filters.push(filter);
            }
        }

        FilterManager { filters: logic_filters, regex_filters, propid_list, schema }
    }

    pub fn get_related_propid_list(&self) -> &HashSet<i32> {
        &self.propid_list
    }

    pub fn get_filters(&self) -> &Vec<LogicalCompare> {
        &self.filters
    }

    pub fn filter_native_vertex<V: Vertex>(&self, v: &V) -> bool {
        if self.filters.is_empty() && self.regex_filters.is_empty() {
            return true;
        }

        for filter in self.filters.iter() {
            if !self.filter_every_native_vertex(filter, v) {
                return false;
            }
        }
        for regex_filter in self.regex_filters.iter() {
            if !regex_filter.filter_native_vertex(v) {
                return false;
            }
        }

        return true;
    }

    fn filter_every_native_vertex<V: Vertex>(&self, filter: &LogicalCompare, v: &V) -> bool {
        let negate_flag = filter.get_value().get_bool_flag();
        let filter_flag = self.filter_every_native_vertex_positive(filter, v);
        if negate_flag {
            return !filter_flag;
        } else {
            return filter_flag;
        }
    }

    fn filter_every_native_vertex_positive<V: Vertex>(&self, filter: &LogicalCompare, v: &V) -> bool {
        let prop_id = filter.get_prop_id();
        let value = filter.get_value();
        let compare = filter.get_compare();
        match compare {
            CompareType::AND_RELATION => {
                let child_list = filter.get_child_compare_list();
                for child_compare in child_list {
                    if !self.filter_every_native_vertex(child_compare, v) {
                        return false;
                    }
                }
                return true;
            }
            CompareType::OR_RELATION => {
                let child_list = filter.get_child_compare_list();
                for child_compare in child_list {
                    if self.filter_every_native_vertex(child_compare, v) {
                        return true;
                    }
                }
                return false;
            }
            CompareType::WITHIN | CompareType::WITHOUT => {
                match value.get_value_type() {
                    VariantType::VT_INT_LIST => {
                        if let Some(val) = get_vertex_int_prop_value(prop_id, v) {
                            let flag = value.get_int_value_list().contains(&val);
                            if compare == CompareType::WITHIN {
                                return flag;
                            } else {
                                return !flag;
                            }
                        }
                    }
                    VariantType::VT_LONG_LIST => {
                        if let Some(val) = get_vertex_long_prop_value(prop_id, v) {
                            let flag = value.get_long_value_list().contains(&val);
                            if compare == CompareType::WITHIN {
                                return flag;
                            } else {
                                return !flag;
                            }
                        }
                    }
                    VariantType::VT_STRING_LIST => {
                        if let Some(val) = get_vertex_string_prop_value(prop_id, v) {
                            let flag = value.get_str_value_list().contains(&val);
                            if compare == CompareType::WITHIN {
                                return flag;
                            } else {
                                return !flag;
                            }
                        }
                    }
                    _ => {
                        error!("Not support within or without for type {:?}", value.get_value_type());
                    }
                }
                return false;
            }
            CompareType::STARTSWITH => {
                match value.get_value_type() {
                    VariantType::VT_STRING => {
                        let str_val = value.get_str_value();
                        if let Some(prop_value) = get_vertex_string_prop_value(prop_id, v) {
                            return prop_value.starts_with(str_val);
                        }
                    }
                    _ => {
                        return false;
                    }
                }
                return false;
            }
            CompareType::ENDSWITH => {
                match value.get_value_type() {
                    VariantType::VT_STRING => {
                        let str_val = value.get_str_value();
                        if let Some(prop_value) = get_vertex_string_prop_value(prop_id, v) {
                            return prop_value.ends_with(str_val);
                        }
                    }
                    _ => {
                        return false;
                    }
                }
                return false;
            }
            CompareType::CONTAINS => {
                match value.get_value_type() {
                    VariantType::VT_STRING => {
                        let str_val = value.get_str_value();
                        if let Some(prop_value) = get_vertex_string_prop_value(prop_id, v) {
                            return prop_value.contains(str_val);
                        }
                    }
                    _ => {
                        return false;
                    }
                }
                return false;
            }
            CompareType::EQ |
            CompareType::NEQ |
            CompareType::GT |
            CompareType::GTE |
            CompareType::LT |
            CompareType::LTE => {
                match value.get_value_type() {
                    VariantType::VT_BOOL => {
                        if let Some(v) = get_vertex_bool_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &value.get_bool_value());
                        }
                    }
                    VariantType::VT_SHORT |
                    VariantType::VT_INT => {
                        if let Some(v) = get_vertex_long_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &(value.get_int_value() as i64));
                        }
                    }
                    VariantType::VT_LONG => {
                        if let Some(v) = get_vertex_long_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &value.get_long_value());
                        }
                    }
                    VariantType::VT_FLOAT => {
                        if let Some(v) = get_vertex_double_prop_value(prop_id, v) {
                            return filter_double_value(&compare, &v, &(value.get_float_value() as f64));
                        }
                    }
                    VariantType::VT_DOUBLE => {
                        if let Some(v) = get_vertex_double_prop_value(prop_id, v) {
                            return filter_double_value(&compare, &v, &value.get_double_value());
                        }
                    }
                    VariantType::VT_STRING | VariantType::VT_DATE => {
                        if let Some(v) = get_vertex_string_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &value.get_str_value().to_owned());
                        }
                    }
                    _ => {
                        error!("not support to compare value {:?}", value.get_value_type());
                    }
                }
                return false;
            }
            CompareType::LIST_CONTAINS |
            CompareType::LIST_CONTAINS_ANY |
            CompareType::LIST_CONTAINS_ALL => {
                if let Some(prop) = v.get_property(prop_id as PropId) {
                    return filter_list_native_prop(&compare, prop, &value);
                } else {
                    error!("cant get property {:?} from vertex", prop_id);
                    return false;
                }
            }
            CompareType::EXIST => {
                if let Some(_) = v.get_property(prop_id as PropId) {
                    return true;
                } else {
                    return false;
                }
            }
            _ => {
                error!("Not support filter compare {:?}", compare);
                return false;
            }
        }
    }

    pub fn filter_native_edge<E: Edge>(&self, e: &E) -> bool {
        if self.filters.is_empty() && self.regex_filters.is_empty() {
            return true;
        }

        for filter in self.filters.iter() {
            if !self.filter_every_native_edge(filter, e) {
                return false;
            }
        }
        for regex_filter in self.regex_filters.iter() {
            if !regex_filter.filter_native_edge(e) {
                return false;
            }
        }

        return true;
    }

    fn filter_every_native_edge<E: Edge>(&self, filter: &LogicalCompare, v: &E) -> bool {
        let negate_flag = filter.get_value().get_bool_flag();
        let filter_flag = self.filter_every_native_edge_positive(filter, v);
        if negate_flag {
            return !filter_flag;
        } else {
            return filter_flag;
        }
    }

    fn filter_every_native_edge_positive<E: Edge>(&self, filter: &LogicalCompare, v: &E) -> bool {
        let prop_id = filter.get_prop_id();
        let value = filter.get_value();
        let compare = filter.get_compare();
        match compare {
            CompareType::AND_RELATION => {
                let child_list = filter.get_child_compare_list();
                for child_compare in child_list {
                    if !self.filter_every_native_edge(child_compare, v) {
                        return false;
                    }
                }
            }
            CompareType::OR_RELATION => {
                let child_list = filter.get_child_compare_list();
                for child_compare in child_list {
                    if self.filter_every_native_edge(child_compare, v) {
                        return true;
                    }
                }
                return false;
            }
            CompareType::WITHIN | CompareType::WITHOUT => {
                match value.get_value_type() {
                    VariantType::VT_INT_LIST => {
                        if let Some(val) = get_edge_int_prop_value(prop_id, v) {
                            let flag = value.get_int_value_list().contains(&val);
                            if compare == CompareType::WITHIN {
                                return flag;
                            } else {
                                return !flag;
                            }
                        }
                    }
                    VariantType::VT_LONG_LIST => {
                        if let Some(val) = get_edge_long_prop_value(prop_id, v) {
                            let flag = value.get_long_value_list().contains(&val);
                            if compare == CompareType::WITHIN {
                                return flag;
                            } else {
                                return !flag;
                            }
                        }
                    }
                    VariantType::VT_STRING_LIST => {
                        if let Some(val) = get_edge_string_prop_value(prop_id, v) {
                            let flag = value.get_str_value_list().contains(&val);
                            if compare == CompareType::WITHIN {
                                return flag;
                            } else {
                                return !flag;
                            }
                        }
                    }
                    _ => {
                        error!("Not support within or without for type {:?}", value.get_value_type());
                    }
                }
                return false;
            }
            CompareType::STARTSWITH => {
                match value.get_value_type() {
                    VariantType::VT_STRING => {
                        let str_val = value.get_str_value();
                        if let Some(prop_value) = get_edge_string_prop_value(prop_id, v) {
                            return prop_value.starts_with(str_val);
                        }
                    }
                    _ => {
                        return false;
                    }
                }
                return false;
            }
            CompareType::ENDSWITH => {
                match value.get_value_type() {
                    VariantType::VT_STRING => {
                        let str_val = value.get_str_value();
                        if let Some(prop_value) = get_edge_string_prop_value(prop_id, v) {
                            return prop_value.ends_with(str_val);
                        }
                    }
                    _ => {
                        return false;
                    }
                }
                return false;
            }
            CompareType::CONTAINS => {
                match value.get_value_type() {
                    VariantType::VT_STRING => {
                        let str_val = value.get_str_value();
                        if let Some(prop_value) = get_edge_string_prop_value(prop_id, v) {
                            return prop_value.contains(str_val);
                        }
                    }
                    _ => {
                        return false;
                    }
                }
                return false;
            }
            CompareType::EQ |
            CompareType::NEQ |
            CompareType::GT |
            CompareType::GTE |
            CompareType::LT |
            CompareType::LTE => {
                match value.get_value_type() {
                    VariantType::VT_BOOL => {
                        if let Some(v) = get_edge_bool_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &value.get_bool_value());
                        }
                    }
                    VariantType::VT_SHORT |
                    VariantType::VT_INT => {
                        if let Some(v) = get_edge_long_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &(value.get_int_value() as i64));
                        }
                    }
                    VariantType::VT_LONG => {
                        if let Some(v) = get_edge_long_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &value.get_long_value());
                        }
                    }
                    VariantType::VT_FLOAT => {
                        if let Some(v) = get_edge_double_prop_value(prop_id, v) {
                            return filter_double_value(&compare, &v, &(value.get_float_value() as f64));
                        }
                    }
                    VariantType::VT_DOUBLE => {
                        if let Some(v) = get_edge_double_prop_value(prop_id, v) {
                            return filter_double_value(&compare, &v, &value.get_double_value());
                        }
                    }
                    VariantType::VT_STRING | VariantType::VT_DATE => {
                        if let Some(v) = get_edge_string_prop_value(prop_id, v) {
                            return filter_value(&compare, &v, &value.get_str_value().to_owned());
                        }
                    }
                    _ => {
                        error!("not support to compare value {:?}", value.get_value_type());
                    }
                }
                return false;
            }
            CompareType::LIST_CONTAINS |
            CompareType::LIST_CONTAINS_ANY |
            CompareType::LIST_CONTAINS_ALL => {
                if let Some(prop) = v.get_property(prop_id as PropId) {
                    return filter_list_native_prop(&compare, prop, &value);
                } else {
                    error!("cant get property {:?} from vertex", prop_id);
                    return false;
                }
            }
            CompareType::EXIST => {
                if let Some(_) = v.get_property(prop_id as PropId) {
                    return true;
                } else {
                    return false;
                }
            }
            _ => {
                error!("Not support filter compare {:?}", compare);
                return false;
            }
        }

        return true;
    }
}


#[inline]
pub fn filter_value<D: Ord>(compare: &CompareType, x: &D, y: &D) -> bool {
    match compare {
        &CompareType::EQ => {
            x == y
        }
        &CompareType::LT => {
            x < y
        }
        &CompareType::GT => {
            x > y
        }
        &CompareType::LTE => {
            x <= y
        }
        &CompareType::GTE => {
            x >= y
        }
        &CompareType::NEQ => {
            x != y
        }
        _ => {
            error!("not support {:?} in filter value", compare);
            false
        }
    }
}

#[inline]
pub fn filter_double_value(compare: &CompareType, x: &f64, y: &f64) -> bool {
    match compare {
        &CompareType::EQ => {
            x == y
        }
        &CompareType::LT => {
            x < y
        }
        &CompareType::GT => {
            x > y
        }
        &CompareType::LTE => {
            x <= y
        }
        &CompareType::GTE => {
            x >= y
        }
        &CompareType::NEQ => {
            x != y
        }
        _ => {
            error!("not support {:?} in filter value", compare);
            false
        }
    }
}

#[inline]
fn filter_int_list(vallist: &Vec<i32>, compare: &CompareType, value: &Value) -> bool {
    match compare {
        CompareType::LIST_CONTAINS => {
            return vallist.contains(&value.get_int_value());
        }
        CompareType::LIST_CONTAINS_ANY => {
            for val in value.get_int_value_list() {
                if vallist.contains(val) {
                    return true;
                }
            }
        }
        CompareType::LIST_CONTAINS_ALL => {
            for val in value.get_int_value_list() {
                if !vallist.contains(val) {
                    return false;
                }
            }
            return true;
        }
        _ => {}
    }

    return false;
}

#[inline]
fn filter_long_list(vallist: &Vec<i64>, compare: &CompareType, value: &Value) -> bool {
    match compare {
        CompareType::LIST_CONTAINS => {
            return vallist.contains(&value.get_long_value());
        }
        CompareType::LIST_CONTAINS_ANY => {
            for val in value.get_long_value_list() {
                if vallist.contains(val) {
                    return true;
                }
            }
        }
        CompareType::LIST_CONTAINS_ALL => {
            for val in value.get_long_value_list() {
                if !vallist.contains(val) {
                    return false;
                }
            }
            return true;
        }
        _ => {}
    }

    return false;
}

#[inline]
fn filter_string_list(vallist: &Vec<String>, compare: &CompareType, value: &Value) -> bool {
    match compare {
        CompareType::LIST_CONTAINS => {
            return vallist.contains(&value.get_str_value().to_owned());
        }
        CompareType::LIST_CONTAINS_ANY => {
            for val in value.get_str_value_list() {
                if vallist.contains(val) {
                    return true;
                }
            }
        }
        CompareType::LIST_CONTAINS_ALL => {
            for val in value.get_str_value_list() {
                if !vallist.contains(val) {
                    return false;
                }
            }
            return true;
        }
        _ => {}
    }

    return false;
}

#[inline]
fn filter_list_native_prop(compare: &CompareType, prop: Property, value: &Value) -> bool {
    match value.get_value_type() {
        VariantType::VT_INT_LIST | VariantType::VT_INT => {
            if let Ok(vallist) = prop.get_list() {
                return filter_int_list(vallist, compare, value);
            }
        }
        VariantType::VT_LONG_LIST | VariantType::VT_LONG => {
            if let Ok(vallist) = prop.get_long_list() {
                return filter_long_list(vallist, compare, value);
            }
        }
        VariantType::VT_STRING_LIST | VariantType::VT_STRING => {
            if let Ok(vallist) = prop.get_string_list() {
                return filter_string_list(vallist, compare, value);
            }
        }
        _ => {}
    }

    return false;
}

#[inline]
pub fn filter_within_or_not<D: PartialEq>(compare: &CompareType, val: &D, list: &Vec<D>) -> bool {
    match compare {
        CompareType::WITHIN => list.contains(val),
        CompareType::WITHOUT => !list.contains(val),
        _ => false,
    }
}

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

use crate::generated::gremlin as pb;
use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::functions::CompareFunction;
use crate::process::traversal::step::order_by::CompareFunctionGen;
use crate::process::traversal::step::util::result_downcast::{
    try_downcast_group_count_value, try_downcast_group_key,
};
use crate::process::traversal::traverser::Traverser;
use crate::structure::codec::ParseError;
use crate::structure::{Details, GraphElement, Token};
use crate::{str_to_dyn_error, DynResult, Element, FromPb};
use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub enum Order {
    Shuffle = 0,
    Asc = 1,
    Desc = 2,
}

impl FromPb<pb::order_by_compare_pair::Order> for Order {
    fn from_pb(order: pb::order_by_compare_pair::Order) -> Result<Self, ParseError>
    where
        Self: Sized,
    {
        match order {
            pb::order_by_compare_pair::Order::Shuffle => Ok(Order::Shuffle),
            pb::order_by_compare_pair::Order::Asc => Ok(Order::Asc),
            pb::order_by_compare_pair::Order::Desc => Ok(Order::Desc),
        }
    }
}

struct OrderStep {
    tag_key_order: Vec<(TagKey, Order)>,
}

impl OrderStep {
    fn compare_element_traverser_with_token_opt(
        &self, left_element: Option<&GraphElement>, right_element: Option<&GraphElement>,
        token: &Token,
    ) -> Option<Ordering> {
        let mut ordering = None;
        if left_element.is_some() && right_element.is_some() {
            let (left_element, right_element) = (left_element.unwrap(), right_element.unwrap());
            ordering = match token {
                // by select("a").by(id) or select(id)
                Token::Id => left_element.id().partial_cmp(&right_element.id()),
                // by select("a").by(label) or select(label)
                Token::Label => left_element.label().partial_cmp(&right_element.label()),
                // by select("a").by("name") or select("name")
                Token::Property(prop) => {
                    let left_prop_val = left_element.details().get_property(prop);
                    let right_prop_val = right_element.details().get_property(prop);
                    left_prop_val.partial_cmp(&right_prop_val)
                }
            };
        }
        ordering
    }
}

// TODO(bingqing): In the case that the values can not compare, for example, 1. tag "a" or head should be an element or attached with a value, but it is not as expected. 2. None with not None or the value itself does not implement Ord (only partially comparable). We now by default return Ordering::Equal, this must be further investigated.
impl CompareFunction<Traverser> for OrderStep {
    fn compare(&self, left: &Traverser, right: &Traverser) -> Ordering {
        let mut result = Ordering::Equal;
        for (tag_key, order) in self.tag_key_order.iter() {
            let (tag, key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
            let mut ordering = None;
            if let Some(key) = key {
                match key {
                    // "a" or head should be a graph_element
                    ByStepOption::OptToken(token) => {
                        let (left_element, right_element) =
                            (left.select_as_element(tag), right.select_as_element(tag));
                        ordering = self.compare_element_traverser_with_token_opt(
                            left_element,
                            right_element,
                            token,
                        );
                    }
                    // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                    ByStepOption::OptGroupKeys(opt_group_keys) => {
                        let (left, right) = if let Some(tag) = tag {
                            (left.select_as_value(tag), right.select_as_value(tag))
                        } else {
                            (left.get_object(), right.get_object())
                        };
                        if left.is_some() && right.is_some() {
                            let (left, right) = (left.unwrap(), right.unwrap());
                            let left_key_traverser = try_downcast_group_key(&left);
                            let right_key_traverser = try_downcast_group_key(&right);
                            if left_key_traverser.is_some() && right_key_traverser.is_some() {
                                if let Some(opt_group_keys_token) = opt_group_keys {
                                    let left_element = left_key_traverser.unwrap().get_element();
                                    let right_element = right_key_traverser.unwrap().get_element();
                                    ordering = self.compare_element_traverser_with_token_opt(
                                        left_element,
                                        right_element,
                                        opt_group_keys_token,
                                    );
                                } else {
                                    let left_value =
                                        left_key_traverser.as_ref().unwrap().get_object();
                                    let right_value =
                                        right_key_traverser.as_ref().unwrap().get_object();
                                    ordering = left_value.partial_cmp(&right_value);
                                }
                            }
                        }
                    }
                    ByStepOption::OptGroupValues(_) => {
                        let (left, right) = if let Some(tag) = tag {
                            (left.select_as_value(tag), right.select_as_value(tag))
                        } else {
                            (left.get_object(), right.get_object())
                        };
                        if left.is_some() && right.is_some() {
                            let (left, right) = (left.unwrap(), right.unwrap());
                            let left_value = try_downcast_group_count_value(&left);
                            let right_value = try_downcast_group_count_value(&right);
                            if left_value.is_some() && right_value.is_some() {
                                ordering = left_value.partial_cmp(&right_value);
                            } else {
                                // TODO: support more group value types
                            };
                        }
                    }
                    // by a value precomputed in sub_traversal which is attached in head graph_element
                    ByStepOption::OptSubtraversal => {
                        let (left_value, right_value) =
                            (left.get_element_attached(), right.get_element_attached());
                        // TODO: only support count() computed by engine for now
                        ordering = left_value.partial_cmp(&right_value);
                    }
                    _ => {}
                }
            } else {
                // by a precomputed value which is in head or saved in path and attached with a tag
                let (left_value, right_value) = if let Some(tag) = tag {
                    (left.select_as_value(tag), right.select_as_value(tag))
                } else {
                    (left.get_object(), right.get_object())
                };
                ordering = left_value.partial_cmp(&right_value);
            }
            if let Some(ordering) = ordering {
                if Ordering::Equal != ordering {
                    result = {
                        match order {
                            Order::Desc => ordering.reverse(),
                            _ => ordering,
                        }
                    };
                    break;
                }
            }
        }
        result
    }
}

impl CompareFunctionGen for pb::OrderByStep {
    fn gen_cmp(self) -> DynResult<Box<dyn CompareFunction<Traverser>>> {
        let mut order_keys = vec![];
        for cmp in self.pairs {
            let tag_key = if let Some(tag_key_pb) = cmp.key {
                TagKey::from_pb(tag_key_pb)?
            } else {
                TagKey::default()
            };
            let order_type_pb = unsafe { std::mem::transmute(cmp.order) };
            let order_type = Order::from_pb(order_type_pb)?;
            order_keys.push((tag_key, order_type));
        }

        for (tag_key, _) in order_keys.iter() {
            let (tag, key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
            if let Some(key) = key {
                match key {
                    ByStepOption::OptProperties(_) => {
                        Err(str_to_dyn_error("Do not support order by valueMap"))?;
                    }
                    ByStepOption::OptSubtraversal => {
                        if tag.is_some() {
                            Err(str_to_dyn_error(
                                "Do not support tag when OptSubtraversal in order by",
                            ))?;
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(Box::new(OrderStep { tag_key_order: order_keys }))
    }
}

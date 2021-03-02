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

use crate::process::traversal::step::by_key::{ByStepOption, TagKey};
use crate::process::traversal::step::order_by::CompareFunctionGen;
use crate::process::traversal::step::util::result_downcast::{
    try_downcast_count, try_downcast_group_count_value,
};
use crate::process::traversal::step::util::StepSymbol;
use crate::process::traversal::step::Step;
use crate::process::traversal::traverser::{ShadeSync, Traverser};
use crate::structure::Details;
use crate::structure::Token;
use crate::{Element, Object};
use pegasus::api::function::CompareFunction;
use std::cmp::Ordering;

#[derive(Clone)]
#[repr(i32)]
pub enum Order {
    Shuffle = 0,
    Asc = 1,
    Desc = 2,
}

pub struct OrderStep {
    tag_key_order: Vec<(TagKey, Order)>,
}

impl OrderStep {
    pub fn new(tag_key_order: Vec<(TagKey, Order)>) -> Self {
        OrderStep { tag_key_order }
    }
}

struct OrderBy {
    tag_key_order: Vec<(TagKey, Order)>,
}

impl Step for OrderStep {
    fn get_symbol(&self) -> StepSymbol {
        StepSymbol::Order
    }

    fn add_tag(&mut self, _label: String) {
        unimplemented!()
    }

    fn tags(&self) -> &[String] {
        unimplemented!()
    }
}

// TODO(bingqing): throw exception instead of panic(), expect() or unwrap()
impl CompareFunction<Traverser> for OrderBy {
    fn compare(&self, left: &Traverser, right: &Traverser) -> Ordering {
        let mut result = Ordering::Equal;
        for (tag_key, order) in self.tag_key_order.iter() {
            let (tag, key) = (tag_key.tag.as_ref(), tag_key.by_key.as_ref());
            let mut ordering = Ordering::Equal;
            if let Some(key) = key {
                match key {
                    // "a" or head should be a graph_element
                    ByStepOption::OptToken(token) => {
                        let (left_element, right_element) = if let Some(tag) = tag {
                            (
                                left.select_as_element(tag)
                                    .expect(&format!("Get tag {:?} as element error!", tag)),
                                right
                                    .select_as_element(tag)
                                    .expect(&format!("Get tag {:?} as element error!", tag)),
                            )
                        } else {
                            (
                                left.get_element().expect("should be graph element"),
                                right.get_element().expect("should be graph element"),
                            )
                        };
                        let (left_obj, right_obj) = match token {
                            // by select("a").by(id) or select(id)
                            Token::Id => (left_element.id().into(), right_element.id().into()),
                            // by select("a").by(label) or select(label)
                            Token::Label => {
                                (left_element.label().into(), right_element.label().into())
                            }
                            // by select("a").by("name") or select("name")
                            Token::Property(prop) => {
                                let left_prop_val = left_element
                                    .details()
                                    .get_property(prop)
                                    .expect(&format!("Get property {:?} error!", prop))
                                    .try_to_owned()
                                    .expect("Can't get owned property value");
                                let right_prop_val = right_element
                                    .details()
                                    .get_property(prop)
                                    .expect(&format!("Get property {:?} error!", prop))
                                    .try_to_owned()
                                    .expect("Can't get owned property value");
                                (left_prop_val, right_prop_val)
                            }
                        };
                        ordering = left_obj.partial_cmp(&right_obj).expect("cannot compare");
                    }
                    ByStepOption::OptProperties(_) => panic!("Do not support order by valueMap"),
                    // "a" should be a pair of (k,v), or head should attach with a pair of (k,v)
                    ByStepOption::OptGroupKeys => unimplemented!(),
                    ByStepOption::OptGroupValues => {
                        let (left, right) = if let Some(tag) = tag {
                            (
                                left.select_as_value(tag)
                                    .expect(&format!("Select tag {:?} as value error!", tag)),
                                right
                                    .select_as_value(tag)
                                    .expect(&format!("Select tag {:?} as value error!", tag)),
                            )
                        } else {
                            (
                                left.get_object().expect("left should be object"),
                                right.get_object().expect("right should be object"),
                            )
                        };
                        // TODO: support more value types
                        let left_value = try_downcast_group_count_value(&left);
                        let right_value = try_downcast_group_count_value(&right);
                        if left_value.is_some() && right_value.is_some() {
                            ordering =
                                left_value.partial_cmp(&right_value).expect("cannot compare");
                        } else {
                            panic!(
                                "unsupported order by(values) with values {:?}, {:?}",
                                left, right
                            )
                        };
                    }
                    // by a value precomputed in sub_traversal which is attached in head graph_element
                    ByStepOption::OptSubtraversal => {
                        if tag.is_some() {
                            panic!("do not support tag when by_key is SubKey")
                        }
                        let (left_obj, right_obj) = {
                            (
                                left.get_element()
                                    .expect("should be graph_element")
                                    .get_attached()
                                    .expect("should with attached object"),
                                right
                                    .get_element()
                                    .expect("should be graph_element")
                                    .get_attached()
                                    .expect("should with attached object"),
                            )
                        };
                        // TODO: only support count() computed by engine for now
                        if let Object::UnknownOwned(left) = left_obj.clone() {
                            if let Object::UnknownOwned(right) = right_obj.clone() {
                                let left_count = left.try_downcast_ref::<ShadeSync<u64>>();
                                let right_count = right.try_downcast_ref::<ShadeSync<u64>>();
                                if left_count.is_some() && right_count.is_some() {
                                    let (left_obj, right_obj) =
                                        (left_count.unwrap().inner, right_count.unwrap().inner);
                                    ordering =
                                        left_obj.partial_cmp(&right_obj).expect("cannot compare");
                                }
                            }
                        } else {
                            ordering = left_obj.partial_cmp(right_obj).expect("cannot compare");
                        }
                    }
                    ByStepOption::OptInvalid => panic!("Invalid by option"),
                }
            } else {
                // by a precomputed value which is saved in path and attached with a tag
                if let Some(tag) = tag {
                    let (left_value, right_value) = (
                        left.select_as_value(tag)
                            .expect(&format!("Select tag {:?} as value error!", tag)),
                        right
                            .select_as_value(tag)
                            .expect(&format!("Select tag {:?} as value error!", tag)),
                    );
                    // value may be generated count()
                    let left_count = try_downcast_count(&left_value);
                    let right_count = try_downcast_count(&right_value);
                    ordering = if left_count.is_some() && right_count.is_some() {
                        (left_count.unwrap().inner)
                            .partial_cmp(&right_count.unwrap().inner)
                            .expect("cannot compare")
                    } else {
                        left_value.partial_cmp(right_value).expect("cannot compare")
                    };
                } else {
                    panic!("no tag key is provided");
                }
            }
            if ordering != Ordering::Equal {
                result = {
                    match order {
                        Order::Desc => ordering.reverse(),
                        _ => ordering,
                    }
                };
                break;
            }
        }
        result
    }
}

impl CompareFunctionGen for OrderStep {
    fn gen(&self) -> Box<dyn CompareFunction<Traverser>> {
        Box::new(OrderBy { tag_key_order: self.tag_key_order.clone() })
    }
}
